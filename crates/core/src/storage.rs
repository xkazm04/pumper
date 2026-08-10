use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::config::StorageConfig;
use crate::datasets::DerivedSpec;
use crate::job::{Job, JobStatus};
use crate::{Error, Result};

const JOB_COLUMNS: &str = "id, app, params, status, attempts, max_attempts, priority, \
                           callback_url, callback_secret, budget_usd, schedule_id, trigger_id, \
                           result, error, created_at, available_at, started_at, finished_at";

/// Options for enqueuing a job. Defaults: 1 attempt, no delay, priority 0.
#[derive(Debug, Clone, Default)]
pub struct EnqueueOptions {
    pub params: Value,
    pub max_attempts: i64,
    pub delay_secs: u64,
    pub priority: i64,
    pub callback_url: Option<String>,
    pub callback_secret: Option<String>,
    /// Spend ceiling for the whole job (metered Claude calls abort past it).
    pub budget_usd: Option<f64>,
    /// Client-supplied dedup key: an enqueue with a key that already exists
    /// returns the original job instead of creating a duplicate.
    pub idempotency_key: Option<String>,
    /// Set by the scheduler so overlapping runs of one schedule can be skipped.
    pub schedule_id: Option<String>,
    /// Set by trigger evaluation: which trigger fired this job (lineage).
    pub trigger_id: Option<String>,
    /// Set by trigger evaluation: which job's OUTCOME fired this one. The
    /// complement of `trigger_id` (which trigger) and what makes "the hops this
    /// run caused" an index seek rather than a scan of the jobs table.
    pub source_job_id: Option<String>,
}

/// A standing subscription: deliver a `dataset.changed` event whenever a job
/// leaves fresh revisions in the watched dataset (`"*"` = all datasets of the
/// app). `sink` selects the delivery connector; `"webhook"` (POST at `url`)
/// is the default and the original behavior.
#[derive(Debug, Clone, Serialize)]
pub struct Watch {
    pub id: String,
    pub app: String,
    pub dataset: String,
    pub url: String,
    /// HMAC-SHA256 signing secret for delivery bodies (never serialized).
    #[serde(skip_serializing)]
    pub secret: Option<String>,
    /// Delivery connector: `"webhook"` | `"file"` | `"slack"` (0031).
    pub sink: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

impl Watch {
    /// True when this watch covers `dataset`.
    pub fn covers(&self, dataset: &str) -> bool {
        self.dataset == "*" || self.dataset == dataset
    }
}

/// A recurring schedule that fires an app on a cron cadence.
#[derive(Debug, Clone, Serialize)]
pub struct Schedule {
    pub id: String,
    pub app: String,
    pub cron: String,
    pub params: Value,
    pub enabled: bool,
    pub priority: i64,
    /// IANA timezone name (chrono-tz) the cron expression is evaluated in;
    /// `None` = UTC.
    pub timezone: Option<String>,
    /// How firings missed while the scheduler was down are handled:
    /// `"fire_once"` (default) runs one catch-up; `"skip"` runs none.
    pub misfire_policy: String,
    /// Attempt budget for jobs this schedule enqueues; `None` = server default.
    pub max_attempts: Option<i64>,
    /// Which controller owns this row: `None` = hand-made / code-seeded (sacred —
    /// the catalog reconciler never touches these); `Some("catalog")` = driven by
    /// `catalog/data-sources.toml` via the reconciler.
    pub managed_by: Option<String>,
    pub last_run: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Column list shared by every `schedules` SELECT (kept in sync with `ScheduleRow`).
const SCHEDULE_COLUMNS: &str =
    "id, app, cron, params, enabled, priority, timezone, misfire_policy, max_attempts, \
     managed_by, last_run, created_at";

/// Create-time fields for a schedule (borrowed; storage assigns id/enabled/time).
#[derive(Debug, Clone)]
pub struct NewSchedule<'a> {
    pub app: &'a str,
    pub cron: &'a str,
    pub params: Value,
    pub priority: i64,
    /// IANA timezone name (chrono-tz); `None` = UTC.
    pub timezone: Option<&'a str>,
    /// `"fire_once"` | `"skip"`.
    pub misfire_policy: &'a str,
    /// `None` = server default attempt budget.
    pub max_attempts: Option<i64>,
}

/// The compile-time-embedded migration chain (`crates/core/migrations`,
/// `0001_init.sql` …). Exposed so the pre-migration backup can count pending
/// versions and the replay test can assert the chain's shape.
pub fn migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}

/// Durable job store on SQLite (WAL). Jobs survive restarts; `recover_stuck`
/// re-queues anything that was mid-flight when the process died.
#[derive(Clone)]
pub struct Storage {
    pool: SqlitePool,
    pub artifacts_dir: PathBuf,
    /// Monotonic version of the `triggers` table, bumped by every mutation
    /// (create / enable-toggle / delete) **after** the write commits. Callers
    /// that cache an evaluation set stamp it with the value they read *before*
    /// their SELECT and re-read on any change — see
    /// [`Storage::trigger_generation`].
    ///
    /// `Arc` because `Storage` is `Clone` and clones (the test harness makes
    /// one) must share one counter — a per-clone counter would be a silent
    /// lost-invalidation hole.
    trigger_generation: Arc<AtomicU64>,
}

impl Storage {
    pub async fn connect(cfg: &StorageConfig) -> Result<Self> {
        if let Some(parent) = cfg.database_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::create_dir_all(&cfg.artifacts_dir)?;

        let options = SqliteConnectOptions::new()
            .filename(&cfg.database_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        // Codified operator ritual: snapshot the database before advancing the
        // schema. No-ops for fresh/up-to-date/in-memory databases, so the test
        // harness (`testing::TempStore`) never writes a backup — see
        // `backup::backup_decision`.
        let migrator = migrator();
        crate::backup::backup_before_migrations(&pool, &cfg.database_path, &migrator).await;
        migrator
            .run(&pool)
            .await
            .map_err(|e| Error::Storage(sqlx::Error::Migrate(Box::new(e))))?;

        Ok(Self {
            pool,
            artifacts_dir: cfg.artifacts_dir.clone(),
            trigger_generation: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Shares the underlying pool with sibling stores (cache, datasets) so they
    /// run against the same SQLite database and migrations.
    pub fn pool(&self) -> SqlitePool {
        self.pool.clone()
    }

    pub async fn enqueue(&self, app: &str, opts: EnqueueOptions) -> Result<Job> {
        self.enqueue_dedup(app, opts).await.map(|(job, _)| job)
    }

    /// Enqueues a job; when `opts.idempotency_key` matches an existing job, the
    /// original is returned instead. The bool reports whether a job was created.
    pub async fn enqueue_dedup(&self, app: &str, opts: EnqueueOptions) -> Result<(Job, bool)> {
        if let Some(key) = &opts.idempotency_key {
            if let Some(existing) = self.get_by_idempotency_key(key).await? {
                return Ok((existing, false));
            }
        }
        let id = Uuid::new_v4();
        let created = Utc::now();
        let available = created + chrono::Duration::seconds(opts.delay_secs as i64);
        let insert = sqlx::query(
            "INSERT INTO jobs (id, app, params, status, attempts, max_attempts, priority, \
             callback_url, callback_secret, budget_usd, idempotency_key, schedule_id, \
             trigger_id, source_job_id, created_at, available_at) \
             VALUES (?1, ?2, ?3, 'queued', 0, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        )
        .bind(id.to_string())
        .bind(app)
        .bind(opts.params.to_string())
        .bind(opts.max_attempts.max(1))
        .bind(opts.priority)
        .bind(opts.callback_url)
        .bind(opts.callback_secret)
        .bind(opts.budget_usd)
        .bind(&opts.idempotency_key)
        .bind(&opts.schedule_id)
        .bind(&opts.trigger_id)
        .bind(&opts.source_job_id)
        .bind(ts(created))
        .bind(ts(available))
        .execute(&self.pool)
        .await;
        if let Err(e) = insert {
            // Lost a concurrent race on the unique key — return the winner.
            if let Some(key) = &opts.idempotency_key {
                if let Some(existing) = self.get_by_idempotency_key(key).await? {
                    return Ok((existing, false));
                }
            }
            return Err(e.into());
        }
        let job = self
            .get(id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))?;
        Ok((job, true))
    }

    async fn get_by_idempotency_key(&self, key: &str) -> Result<Option<Job>> {
        let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE idempotency_key = ?1");
        let row: Option<JobRow> = sqlx::query_as(&sql)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        row.map(Job::try_from).transpose()
    }

    /// Atomically claims the highest-priority due job and flips it to `running`.
    /// Apps listed in `blocked` are skipped, which is how the worker enforces
    /// per-app concurrency limits (fairness across many apps' queues).
    ///
    /// `aging_coeff` is the priority-aging starvation guard (`WorkerConfig::
    /// priority_aging_coefficient_secs`): the claim orders by *effective*
    /// priority = `priority + waited_secs / aging_coeff`, so a long-waiting
    /// low-priority job overtakes fresh high-priority work instead of starving.
    /// `0.0` (or negative) restores the plain `priority DESC, created_at` order.
    /// The `created_at` tiebreak keeps equal-(effective-)priority claims FIFO.
    pub async fn claim_next(&self, blocked: &[String], aging_coeff: f64) -> Result<Option<Job>> {
        let exclusion = if blocked.is_empty() {
            String::new()
        } else {
            let marks: Vec<String> = (0..blocked.len()).map(|i| format!("?{}", i + 2)).collect();
            format!(" AND app NOT IN ({})", marks.join(", "))
        };
        // Effective-priority expression. The coefficient is a trusted config
        // f64 (not user input), so inlining it is safe; the bind slots (?1, ?2…)
        // stay reserved for the timestamp and the blocked-app list.
        let order = if aging_coeff > 0.0 {
            format!(
                "(priority + (julianday(?1) - julianday(created_at)) * 86400.0 / {aging_coeff}) \
                 DESC, created_at"
            )
        } else {
            "priority DESC, created_at".to_string()
        };
        let sql = format!(
            "UPDATE jobs SET status = 'running', attempts = attempts + 1, started_at = ?1, \
             heartbeat_at = ?1 \
             WHERE id = (SELECT id FROM jobs WHERE status = 'queued' AND available_at <= ?1{exclusion} \
                         ORDER BY {order} LIMIT 1) \
             RETURNING {JOB_COLUMNS}"
        );
        let mut query = sqlx::query_as::<_, JobRow>(&sql).bind(now());
        for app in blocked {
            query = query.bind(app);
        }
        let row = query.fetch_optional(&self.pool).await?;
        row.map(Job::try_from).transpose()
    }

    /// Marks a running job succeeded. Guarded on `(status, attempts)`: only the
    /// worker task that currently owns the running row may complete it, so a
    /// stale task whose job was reset/reaped and re-claimed (advancing the
    /// attempt number) can't overwrite the live row. Returns whether the write
    /// landed (`false` = discarded as stale).
    pub async fn complete(&self, id: Uuid, attempt: i64, result: Value) -> Result<bool> {
        let r = sqlx::query(
            "UPDATE jobs SET status = 'succeeded', result = ?2, error = NULL, finished_at = ?3 \
             WHERE id = ?1 AND status = 'running' AND attempts = ?4",
        )
        .bind(id.to_string())
        .bind(result.to_string())
        .bind(now())
        .bind(attempt)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() > 0)
    }

    /// Records a running job's failure, guarded on `(status, attempts)` like
    /// `complete`. Re-queues with exponential backoff while attempts remain,
    /// else fails permanently. Returns the resulting status, or `None` when the
    /// write was discarded as stale (the job had already moved on).
    pub async fn fail(&self, id: Uuid, attempt: i64, error: &str) -> Result<Option<JobStatus>> {
        let Some(job) = self.get(id).await? else {
            return Ok(None);
        };
        // Fence: only fail the row this task is still running.
        if job.status != JobStatus::Running || job.attempts != attempt {
            return Ok(None);
        }
        if job.attempts < job.max_attempts {
            let backoff_secs = 10u64
                .saturating_mul(2u64.saturating_pow(job.attempts.max(0) as u32))
                .min(3600);
            let available = Utc::now() + chrono::Duration::seconds(backoff_secs as i64);
            let r = sqlx::query(
                "UPDATE jobs SET status = 'queued', error = ?2, available_at = ?3 \
                 WHERE id = ?1 AND status = 'running' AND attempts = ?4",
            )
            .bind(id.to_string())
            .bind(error)
            .bind(ts(available))
            .bind(attempt)
            .execute(&self.pool)
            .await?;
            Ok((r.rows_affected() > 0).then_some(JobStatus::Queued))
        } else {
            let ok = self.fail_permanently(id, attempt, error).await?;
            Ok(ok.then_some(JobStatus::Failed))
        }
    }

    /// Marks a running job permanently failed, guarded on `(status, attempts)`.
    /// Returns whether the write landed (`false` = stale, discarded).
    pub async fn fail_permanently(&self, id: Uuid, attempt: i64, error: &str) -> Result<bool> {
        let r = sqlx::query(
            "UPDATE jobs SET status = 'failed', error = ?2, finished_at = ?3 \
             WHERE id = ?1 AND status = 'running' AND attempts = ?4",
        )
        .bind(id.to_string())
        .bind(error)
        .bind(now())
        .bind(attempt)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() > 0)
    }

    /// Cancels a job that has not started yet.
    pub async fn cancel(&self, id: Uuid) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE jobs SET status = 'cancelled', finished_at = ?2 \
             WHERE id = ?1 AND status = 'queued'",
        )
        .bind(id.to_string())
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Marks a `running` job cancelled, guarded on `(status, attempts)`. The
    /// worker calls this when a per-job cancellation token fires for an in-flight
    /// job (`DELETE /jobs/{id}` on a running job). Returns whether it landed.
    pub async fn cancel_running(&self, id: Uuid, attempt: i64) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE jobs SET status = 'cancelled', finished_at = ?2 \
             WHERE id = ?1 AND status = 'running' AND attempts = ?3",
        )
        .bind(id.to_string())
        .bind(now())
        .bind(attempt)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Re-queues a `running` job (e.g. hung/stuck) with a fresh attempt budget.
    /// The orphaned worker task's late completion is discarded by the
    /// `(status, attempts)` fence on `complete`/`fail`: once this row is
    /// re-claimed its attempt advances past what the stale task holds, so the
    /// stale write matches no row. Returns the refreshed job, or None when the
    /// job doesn't exist or isn't running.
    pub async fn reset(&self, id: Uuid) -> Result<Option<Job>> {
        let r = sqlx::query(
            "UPDATE jobs SET status = 'queued', error = NULL, finished_at = NULL, \
             available_at = ?2, max_attempts = MAX(max_attempts, attempts + 1) \
             WHERE id = ?1 AND status = 'running'",
        )
        .bind(id.to_string())
        .bind(now())
        .execute(&self.pool)
        .await?;
        if r.rows_affected() == 0 {
            return Ok(None);
        }
        self.get(id).await
    }

    /// Bulk re-queue: re-queues up to `cap` jobs in the given terminal state
    /// (`Failed` | `Cancelled`), optionally scoped to one app, each granted one
    /// more attempt — the per-job `retry` semantics applied to a filtered batch,
    /// oldest first. Returns the ids re-queued.
    pub async fn retry_bulk(
        &self,
        status: JobStatus,
        app: Option<&str>,
        cap: i64,
    ) -> Result<Vec<Uuid>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "UPDATE jobs SET status = 'queued', error = NULL, finished_at = NULL, \
             available_at = ?1, max_attempts = MAX(max_attempts, attempts + 1) \
             WHERE id IN (SELECT id FROM jobs WHERE status = ?2 AND (?3 IS NULL OR app = ?3) \
                          ORDER BY created_at LIMIT ?4) \
             RETURNING id",
        )
        .bind(now())
        .bind(status.as_str())
        .bind(app)
        .bind(cap.max(0))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(s,)| Uuid::parse_str(&s).map_err(|e| Error::Parse(format!("job id: {e}"))))
            .collect()
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Job>> {
        let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = ?1");
        let row: Option<JobRow> = sqlx::query_as(&sql)
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(Job::try_from).transpose()
    }

    pub async fn list(
        &self,
        app: Option<&str>,
        status: Option<JobStatus>,
        limit: i64,
    ) -> Result<Vec<Job>> {
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM jobs \
             WHERE (?1 IS NULL OR app = ?1) AND (?2 IS NULL OR status = ?2) \
             ORDER BY created_at DESC LIMIT ?3"
        );
        let rows: Vec<JobRow> = sqlx::query_as(&sql)
            .bind(app)
            .bind(status.map(JobStatus::as_str))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Job::try_from).collect()
    }

    /// Keyset page of jobs ordered (created_at DESC, id DESC). `after` is the
    /// previous page's last (created_at-as-stored, id); None starts at the top.
    pub async fn list_page(
        &self,
        app: Option<&str>,
        status: Option<JobStatus>,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<Job>> {
        let (after_ts, after_id) = after
            .map(|(t, i)| (Some(t), Some(i)))
            .unwrap_or((None, None));
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM jobs \
             WHERE (?1 IS NULL OR app = ?1) AND (?2 IS NULL OR status = ?2) \
             AND (?3 IS NULL OR created_at < ?3 OR (created_at = ?3 AND id < ?4)) \
             ORDER BY created_at DESC, id DESC LIMIT ?5"
        );
        let rows: Vec<JobRow> = sqlx::query_as(&sql)
            .bind(app)
            .bind(status.map(JobStatus::as_str))
            .bind(after_ts)
            .bind(after_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Job::try_from).collect()
    }

    /// Counts jobs grouped by status — for the metrics endpoint.
    pub async fn status_counts(&self) -> Result<Vec<(String, i64)>> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT status, COUNT(*) FROM jobs GROUP BY status")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows)
    }

    /// Permanently-failed job count per app — the DB-derived source for the
    /// `pumper_job_failures_total{app}` metric. Reflects the current number of
    /// rows in the `failed` state (a job later retried leaves the set), so it is
    /// not a strictly monotonic process counter.
    pub async fn failure_counts(&self) -> Result<Vec<(String, i64)>> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT app, COUNT(*) FROM jobs WHERE status = 'failed' GROUP BY app")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows)
    }

    /// Execution-duration (started→finished) and queue-wait (created→started)
    /// aggregates for the metrics endpoint, computed in one pass. Durations come
    /// from `julianday` deltas over the fixed-width RFC-3339 timestamps (× 86400
    /// → seconds). Rows missing an endpoint are excluded from that aggregate.
    pub async fn job_timing_stats(&self) -> Result<JobTimingStats> {
        let row: JobTimingStats = sqlx::query_as(
            "SELECT \
               COALESCE(SUM(CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL \
                 THEN (julianday(finished_at) - julianday(started_at)) * 86400.0 END), 0.0) AS duration_sum, \
               COUNT(CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL THEN 1 END) AS duration_count, \
               COALESCE(MAX(CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL \
                 THEN (julianday(finished_at) - julianday(started_at)) * 86400.0 END), 0.0) AS duration_max, \
               COALESCE(SUM(CASE WHEN started_at IS NOT NULL \
                 THEN (julianday(started_at) - julianday(created_at)) * 86400.0 END), 0.0) AS wait_sum, \
               COUNT(CASE WHEN started_at IS NOT NULL THEN 1 END) AS wait_count, \
               COALESCE(MAX(CASE WHEN started_at IS NOT NULL \
                 THEN (julianday(started_at) - julianday(created_at)) * 86400.0 END), 0.0) AS wait_max \
             FROM jobs",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// True when a schedule already has a job queued or running — the overlap
    /// guard the scheduler consults before firing.
    pub async fn schedule_has_active_job(&self, schedule_id: &str) -> Result<bool> {
        let found: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM jobs WHERE schedule_id = ?1 AND status IN ('queued', 'running') LIMIT 1",
        )
        .bind(schedule_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(found.is_some())
    }

    /// The most recent job this schedule enqueued: `(job_id, status)`, or `None`
    /// if it has never fired. Backs the schedule-observability API (`last_job_id`
    /// / `last_status`); uses the same `schedule_id` index as the overlap guard.
    pub async fn latest_job_for_schedule(
        &self,
        schedule_id: &str,
    ) -> Result<Option<(String, String)>> {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT id, status FROM jobs WHERE schedule_id = ?1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(schedule_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Manually re-queues a failed or cancelled job: clears the terminal state
    /// and grants one more attempt. Returns the refreshed job, or None when the
    /// job doesn't exist or isn't in a retryable state.
    pub async fn retry(&self, id: Uuid) -> Result<Option<Job>> {
        let result = sqlx::query(
            "UPDATE jobs SET status = 'queued', error = NULL, finished_at = NULL, \
             available_at = ?2, max_attempts = MAX(max_attempts, attempts + 1) \
             WHERE id = ?1 AND status IN ('failed', 'cancelled')",
        )
        .bind(id.to_string())
        .bind(now())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }
        self.get(id).await
    }

    /// Re-queues jobs left in `running` by a previous crash/shutdown.
    pub async fn recover_stuck(&self) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE jobs SET status = 'queued', available_at = ?1 WHERE status = 'running'",
        )
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Stamps a liveness heartbeat on a running job, guarded on `(status,
    /// attempts)` so a stale task can't refresh a row it no longer owns. Returns
    /// whether the write landed.
    pub async fn heartbeat(&self, id: Uuid, attempt: i64) -> Result<bool> {
        let r = sqlx::query(
            "UPDATE jobs SET heartbeat_at = ?2 \
             WHERE id = ?1 AND status = 'running' AND attempts = ?3",
        )
        .bind(id.to_string())
        .bind(now())
        .bind(attempt)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() > 0)
    }

    /// The stuck-job reaper: re-queues (or permanently fails) every running job
    /// whose last heartbeat is older than `stale_secs`. Staleness is measured
    /// from `heartbeat_at`, falling back to `started_at`/`created_at` for rows
    /// predating the heartbeat column. Each stale job goes through `fail`, so a
    /// hung lease is treated exactly like a failure — attempts and backoff apply,
    /// and an attempts-exhausted job fails permanently. Returns `(id, app,
    /// resulting status)` per reaped job. A job the worker completes between the
    /// scan and the `fail` is skipped by the `(status, attempts)` fence.
    pub async fn reap_stale(&self, stale_secs: i64) -> Result<Vec<(Uuid, String, JobStatus)>> {
        let cutoff = ts(Utc::now() - chrono::Duration::seconds(stale_secs));
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE status = 'running' \
             AND COALESCE(heartbeat_at, started_at, created_at) < ?1"
        );
        let rows: Vec<JobRow> = sqlx::query_as(&sql)
            .bind(&cutoff)
            .fetch_all(&self.pool)
            .await?;
        let mut reaped = Vec::new();
        for row in rows {
            let job = Job::try_from(row)?;
            if let Some(status) = self
                .fail(job.id, job.attempts, "lease expired (heartbeat stale)")
                .await?
            {
                reaped.push((job.id, job.app, status));
            }
        }
        Ok(reaped)
    }

    // ---- Schedules --------------------------------------------------------

    pub async fn create_schedule(&self, s: NewSchedule<'_>) -> Result<Schedule> {
        let id = Uuid::new_v4().to_string();
        self.insert_schedule(&id, &s).await?;
        self.get_schedule(&id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    /// Seeds a code-declared schedule idempotently (stable id per app+cron), so
    /// static `ScrapeApp::schedule()` values become editable DB rows.
    pub async fn seed_schedule(&self, app: &str, cron: &str) -> Result<()> {
        let id = format!("static-{app}");
        sqlx::query(
            "INSERT INTO schedules (id, app, cron, params, enabled, priority, created_at) \
             VALUES (?1, ?2, ?3, '{}', 1, 0, ?4) ON CONFLICT(id) DO NOTHING",
        )
        .bind(id)
        .bind(app)
        .bind(cron)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn insert_schedule(&self, id: &str, s: &NewSchedule<'_>) -> Result<()> {
        sqlx::query(
            "INSERT INTO schedules \
             (id, app, cron, params, enabled, priority, timezone, misfire_policy, max_attempts, created_at) \
             VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(id)
        .bind(s.app)
        .bind(s.cron)
        .bind(s.params.to_string())
        .bind(s.priority)
        .bind(s.timezone)
        .bind(s.misfire_policy)
        .bind(s.max_attempts)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        let rows: Vec<ScheduleRow> = sqlx::query_as(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules ORDER BY app"
        ))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Schedule::try_from).collect()
    }

    /// Keyset page of schedules ordered (created_at DESC, id DESC). `after` is
    /// the previous page's last (created_at-as-stored, id); None starts at the top.
    pub async fn list_schedules_page(
        &self,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<Schedule>> {
        let (after_ts, after_id) = split_after(after);
        let rows: Vec<ScheduleRow> = sqlx::query_as(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules \
             WHERE (?1 IS NULL OR created_at < ?1 OR (created_at = ?1 AND id < ?2)) \
             ORDER BY created_at DESC, id DESC LIMIT ?3"
        ))
        .bind(after_ts)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Schedule::try_from).collect()
    }

    pub async fn get_schedule(&self, id: &str) -> Result<Option<Schedule>> {
        let row: Option<ScheduleRow> = sqlx::query_as(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE id = ?1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Schedule::try_from).transpose()
    }

    pub async fn set_schedule_enabled(&self, id: &str, enabled: bool) -> Result<bool> {
        let result = sqlx::query("UPDATE schedules SET enabled = ?2 WHERE id = ?1")
            .bind(id)
            .bind(enabled as i64)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn touch_schedule(&self, id: &str, when: DateTime<Utc>) -> Result<()> {
        sqlx::query("UPDATE schedules SET last_run = ?2 WHERE id = ?1")
            .bind(id)
            .bind(ts(when))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM schedules WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    // ---- Dataset watches ---------------------------------------------------

    /// `sink` is the delivery connector (`"webhook"` | `"file"` | `"slack"`);
    /// callers validate the value — storage stores it verbatim.
    pub async fn create_watch(
        &self,
        app: &str,
        dataset: &str,
        url: &str,
        secret: Option<&str>,
        sink: &str,
    ) -> Result<Watch> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO watches (id, app, dataset, url, secret, sink, enabled, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7)",
        )
        .bind(&id)
        .bind(app)
        .bind(dataset)
        .bind(url)
        .bind(secret)
        .bind(sink)
        .bind(now())
        .execute(&self.pool)
        .await?;
        self.get_watch(&id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    pub async fn get_watch(&self, id: &str) -> Result<Option<Watch>> {
        let row: Option<WatchRow> = sqlx::query_as(
            "SELECT id, app, dataset, url, secret, sink, enabled, created_at FROM watches WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Watch::try_from).transpose()
    }

    /// Watches for an app (all watches when `app` is None).
    pub async fn list_watches(&self, app: Option<&str>) -> Result<Vec<Watch>> {
        let rows: Vec<WatchRow> = sqlx::query_as(
            "SELECT id, app, dataset, url, secret, sink, enabled, created_at FROM watches \
             WHERE (?1 IS NULL OR app = ?1) ORDER BY app, dataset",
        )
        .bind(app)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Watch::try_from).collect()
    }

    /// Keyset page of watches ordered (created_at DESC, id DESC), optionally
    /// filtered by app. `after` is the previous page's last (created_at, id).
    pub async fn list_watches_page(
        &self,
        app: Option<&str>,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<Watch>> {
        let (after_ts, after_id) = split_after(after);
        let rows: Vec<WatchRow> = sqlx::query_as(
            "SELECT id, app, dataset, url, secret, sink, enabled, created_at FROM watches \
             WHERE (?1 IS NULL OR app = ?1) \
             AND (?2 IS NULL OR created_at < ?2 OR (created_at = ?2 AND id < ?3)) \
             ORDER BY created_at DESC, id DESC LIMIT ?4",
        )
        .bind(app)
        .bind(after_ts)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Watch::try_from).collect()
    }

    /// Enabled watches for an app — the delivery set for change webhooks.
    pub async fn enabled_watches(&self, app: &str) -> Result<Vec<Watch>> {
        let rows: Vec<WatchRow> = sqlx::query_as(
            "SELECT id, app, dataset, url, secret, sink, enabled, created_at FROM watches \
             WHERE app = ?1 AND enabled = 1",
        )
        .bind(app)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Watch::try_from).collect()
    }

    pub async fn set_watch_enabled(&self, id: &str, enabled: bool) -> Result<bool> {
        let result = sqlx::query("UPDATE watches SET enabled = ?2 WHERE id = ?1")
            .bind(id)
            .bind(enabled as i64)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_watch(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM watches WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    // ---- Reactive triggers ---------------------------------------------------

    /// Current version of the `triggers` table. Any change to the table (a
    /// create, an enable-toggle, a delete) makes this differ from every value
    /// read before it, which is the whole contract: a cached evaluation set
    /// stamped with an older value is stale by definition.
    ///
    /// The **read-before-query** discipline is what closes the invalidation
    /// window. A reader samples the generation, THEN runs its SELECT, THEN
    /// stamps the result with the sampled value. Writers bump only after their
    /// statement has committed. So a reader that observes generation `g` is
    /// guaranteed to see every write that produced `g`, and a reader racing a
    /// write either observes the old `g` (and its entry is invalidated the
    /// moment anybody re-reads) or the new one (and its SELECT saw the write).
    /// There is no interleaving that lets a post-mutation reader be served a
    /// pre-mutation set.
    pub fn trigger_generation(&self) -> u64 {
        self.trigger_generation.load(Ordering::Acquire)
    }

    /// Invalidates every cached trigger evaluation set. Called by (and only by)
    /// the mutating trigger methods, always after the write commits.
    fn bump_trigger_generation(&self) {
        self.trigger_generation.fetch_add(1, Ordering::Release);
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_trigger(&self, t: &NewTrigger<'_>) -> Result<Trigger> {
        let id = Uuid::new_v4().to_string();
        let filters_json = t
            .filters
            .filter(|f| !f.is_empty())
            .map(|f| serde_json::to_string(f).unwrap_or_else(|_| "[]".into()));
        // NULL when neither hook is set — an all-empty hooks object is no hooks.
        let hooks_json = t
            .plugin_hooks
            .filter(|h| h.predicate.is_some() || h.transform.is_some())
            .map(|h| serde_json::to_string(h).unwrap_or_else(|_| "{}".into()));
        sqlx::query(
            "INSERT INTO triggers (id, name, source_kind, source_app, source_dataset, on_change, \
             on_status, target_app, params, budget_usd, priority, max_attempts, enabled, created_at, \
             filters, plugin_hooks) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1, ?13, ?14, ?15)",
        )
        .bind(&id)
        .bind(t.name)
        .bind(t.source_kind)
        .bind(t.source_app)
        .bind(t.source_dataset)
        .bind(t.on_change)
        .bind(t.on_status)
        .bind(t.target_app)
        .bind(t.params.to_string())
        .bind(t.budget_usd)
        .bind(t.priority)
        .bind(t.max_attempts.max(1))
        .bind(now())
        .bind(filters_json)
        .bind(hooks_json)
        .execute(&self.pool)
        .await?;
        self.bump_trigger_generation();
        self.get_trigger(&id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    pub async fn get_trigger(&self, id: &str) -> Result<Option<Trigger>> {
        let row: Option<TriggerRow> = sqlx::query_as(&format!(
            "SELECT {TRIGGER_COLUMNS} FROM triggers WHERE id = ?1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Trigger::try_from).transpose()
    }

    /// All triggers, optionally filtered by source app.
    pub async fn list_triggers(&self, source_app: Option<&str>) -> Result<Vec<Trigger>> {
        let rows: Vec<TriggerRow> = sqlx::query_as(&format!(
            "SELECT {TRIGGER_COLUMNS} FROM triggers \
             WHERE (?1 IS NULL OR source_app = ?1) ORDER BY created_at"
        ))
        .bind(source_app)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Trigger::try_from).collect()
    }

    /// Keyset page of triggers ordered (created_at DESC, id DESC), optionally
    /// filtered by source app. `after` is the previous page's last (created_at, id).
    pub async fn list_triggers_page(
        &self,
        source_app: Option<&str>,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<Trigger>> {
        let (after_ts, after_id) = split_after(after);
        let rows: Vec<TriggerRow> = sqlx::query_as(&format!(
            "SELECT {TRIGGER_COLUMNS} FROM triggers \
             WHERE (?1 IS NULL OR source_app = ?1) \
             AND (?2 IS NULL OR created_at < ?2 OR (created_at = ?2 AND id < ?3)) \
             ORDER BY created_at DESC, id DESC LIMIT ?4"
        ))
        .bind(source_app)
        .bind(after_ts)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Trigger::try_from).collect()
    }

    /// Enabled triggers of one source kind for an app — the evaluation set.
    pub async fn enabled_triggers(
        &self,
        source_kind: &str,
        source_app: &str,
    ) -> Result<Vec<Trigger>> {
        let rows: Vec<TriggerRow> = sqlx::query_as(&format!(
            "SELECT {TRIGGER_COLUMNS} FROM triggers \
             WHERE source_kind = ?1 AND source_app = ?2 AND enabled = 1"
        ))
        .bind(source_kind)
        .bind(source_app)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Trigger::try_from).collect()
    }

    pub async fn set_trigger_enabled(&self, id: &str, enabled: bool) -> Result<bool> {
        let result = sqlx::query("UPDATE triggers SET enabled = ?2 WHERE id = ?1")
            .bind(id)
            .bind(enabled as i64)
            .execute(&self.pool)
            .await?;
        self.bump_trigger_generation();
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_trigger(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM triggers WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        self.bump_trigger_generation();
        Ok(result.rows_affected() > 0)
    }

    /// Jobs a trigger fired, newest first (the lineage view).
    pub async fn jobs_by_trigger(&self, trigger_id: &str, limit: i64) -> Result<Vec<Job>> {
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE trigger_id = ?1 \
             ORDER BY created_at DESC LIMIT ?2"
        );
        let rows: Vec<JobRow> = sqlx::query_as(&sql)
            .bind(trigger_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Job::try_from).collect()
    }

    // ---- Trigger decision ledger --------------------------------------------

    /// Records ONE trigger decision (migration 0036). Callers are fail-open:
    /// a decision that could not be recorded must never hold up the hop it
    /// describes, so this returns the error rather than acting on it.
    pub async fn record_trigger_run(&self, run: &NewTriggerRun<'_>) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO trigger_runs (id, trigger_id, outcome, source_kind, source_job_id, \
             dataset, event_id, job_id, detail, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )
        .bind(&id)
        .bind(run.trigger_id)
        .bind(run.outcome)
        .bind(run.source_kind)
        .bind(run.source_job_id)
        .bind(run.dataset)
        .bind(run.event_id)
        .bind(run.job_id)
        .bind(run.detail)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// Keyset page of one trigger's decisions, newest first. `after` is the
    /// previous page's last (created_at, id).
    pub async fn list_trigger_runs_page(
        &self,
        trigger_id: &str,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<TriggerRun>> {
        let (after_ts, after_id) = split_after(after);
        let rows: Vec<TriggerRunRow> = sqlx::query_as(
            "SELECT id, trigger_id, outcome, source_kind, source_job_id, dataset, event_id, \
             job_id, detail, created_at FROM trigger_runs \
             WHERE trigger_id = ?1 \
             AND (?2 IS NULL OR created_at < ?2 OR (created_at = ?2 AND id < ?3)) \
             ORDER BY created_at DESC, id DESC LIMIT ?4",
        )
        .bind(trigger_id)
        .bind(after_ts)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(TriggerRun::try_from).collect()
    }

    /// Drops decisions older than `days`. The ledger is diagnostic — one row per
    /// candidate edge per source event, negatives included — so unlike the
    /// evidence ledgers in [`LEDGER_TABLES`] it is bounded by default and this
    /// is called from the worker's reaper tick rather than by an operator knob.
    pub async fn prune_trigger_runs(&self, days: u64) -> Result<u64> {
        if days == 0 {
            return Ok(0);
        }
        Ok(
            sqlx::query("DELETE FROM trigger_runs WHERE created_at < ?1")
                .bind(ts(cutoff(days)))
                .execute(&self.pool)
                .await?
                .rows_affected(),
        )
    }

    // ---- DataHub governance audit trail (migration 0037) --------------------

    /// Records ONE executed governance action. Callers are fail-open: the action
    /// has already happened when this is called, so a failed audit write is a
    /// warn, never a reason to pretend it didn't.
    pub async fn record_datahub_govern_action(
        &self,
        a: &NewDatahubGovernAction<'_>,
    ) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO datahub_govern_actions \
             (id, action, target, dataset, subject, evidence, detail, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(&id)
        .bind(a.action)
        .bind(a.target)
        .bind(a.dataset)
        .bind(a.subject)
        .bind(a.evidence)
        .bind(a.detail)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// The newest governance actions, newest first — the durable answer to "why
    /// is this schedule disabled?" that survives the restart `GovernState.last`
    /// does not.
    pub async fn list_datahub_govern_actions(
        &self,
        limit: i64,
    ) -> Result<Vec<DatahubGovernAction>> {
        let rows: Vec<DatahubGovernActionRow> = sqlx::query_as(
            "SELECT id, action, target, dataset, subject, evidence, detail, created_at \
             FROM datahub_govern_actions ORDER BY created_at DESC, id DESC LIMIT ?1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(DatahubGovernAction::try_from)
            .collect()
    }

    /// Drops governance actions older than `days`. Diagnostic like
    /// [`prune_trigger_runs`](Self::prune_trigger_runs): the *effects* (a
    /// disabled schedule, an enqueued job) are durable in their own tables, so
    /// an aged-out audit row loses explanation, not state.
    pub async fn prune_datahub_govern_actions(&self, days: u64) -> Result<u64> {
        if days == 0 {
            return Ok(0);
        }
        Ok(
            sqlx::query("DELETE FROM datahub_govern_actions WHERE created_at < ?1")
                .bind(ts(cutoff(days)))
                .execute(&self.pool)
                .await?
                .rows_affected(),
        )
    }

    // ---- DataHub governance level memory (migration 0038) -------------------

    /// The last-acted remote level of `signal`, per target. Absent target =
    /// never acted on (treated as "off"), so a first sighting is a transition.
    ///
    /// This is what makes governance a *transition* follower: without it, a
    /// standing deprecation re-disabled an operator's re-enabled schedule on
    /// every poll, and a restart re-disabled it once more.
    pub async fn datahub_govern_levels(
        &self,
        signal: &str,
    ) -> Result<std::collections::HashMap<String, bool>> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT target, level FROM datahub_govern_levels WHERE signal = ?1")
                .bind(signal)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|(t, l)| (t, l != 0)).collect())
    }

    /// Records the level governance has now acted on for `(signal, target)`.
    pub async fn set_datahub_govern_level(
        &self,
        signal: &str,
        target: &str,
        level: bool,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO datahub_govern_levels (signal, target, level, updated_at) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(signal, target) DO UPDATE SET level = excluded.level, \
             updated_at = excluded.updated_at",
        )
        .bind(signal)
        .bind(target)
        .bind(level as i64)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ---- Saved searches -----------------------------------------------------

    pub async fn create_saved_search(
        &self,
        query: &str,
        app: Option<&str>,
        dataset: Option<&str>,
        url: &str,
        secret: Option<&str>,
        materialize: Option<&SearchMaterialize>,
    ) -> Result<SavedSearch> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO saved_searches (id, query, app, dataset, url, secret, enabled, \
             materialize_app, materialize_dataset, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?8, ?9)",
        )
        .bind(&id)
        .bind(query)
        .bind(app)
        .bind(dataset)
        .bind(url)
        .bind(secret)
        .bind(materialize.map(|m| m.app.as_str()))
        .bind(materialize.map(|m| m.dataset.as_str()))
        .bind(now())
        .execute(&self.pool)
        .await?;
        self.get_saved_search(&id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    pub async fn get_saved_search(&self, id: &str) -> Result<Option<SavedSearch>> {
        let row: Option<SavedSearchRow> = sqlx::query_as(
            "SELECT id, query, app, dataset, url, secret, enabled, \
             materialize_app, materialize_dataset, created_at \
             FROM saved_searches WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(SavedSearch::try_from).transpose()
    }

    pub async fn list_saved_searches(&self, enabled_only: bool) -> Result<Vec<SavedSearch>> {
        let rows: Vec<SavedSearchRow> = sqlx::query_as(
            "SELECT id, query, app, dataset, url, secret, enabled, \
             materialize_app, materialize_dataset, created_at \
             FROM saved_searches WHERE (?1 = 0 OR enabled = 1) ORDER BY created_at",
        )
        .bind(enabled_only as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(SavedSearch::try_from).collect()
    }

    /// Keyset page of saved searches ordered (created_at DESC, id DESC). `after`
    /// is the previous page's last (created_at, id); None starts at the top.
    pub async fn list_saved_searches_page(
        &self,
        enabled_only: bool,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<SavedSearch>> {
        let (after_ts, after_id) = split_after(after);
        let rows: Vec<SavedSearchRow> = sqlx::query_as(
            "SELECT id, query, app, dataset, url, secret, enabled, \
             materialize_app, materialize_dataset, created_at \
             FROM saved_searches WHERE (?1 = 0 OR enabled = 1) \
             AND (?2 IS NULL OR created_at < ?2 OR (created_at = ?2 AND id < ?3)) \
             ORDER BY created_at DESC, id DESC LIMIT ?4",
        )
        .bind(enabled_only as i64)
        .bind(after_ts)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(SavedSearch::try_from).collect()
    }

    pub async fn set_saved_search_enabled(&self, id: &str, enabled: bool) -> Result<bool> {
        let result = sqlx::query("UPDATE saved_searches SET enabled = ?2 WHERE id = ?1")
            .bind(id)
            .bind(enabled as i64)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_saved_search(&self, id: &str) -> Result<bool> {
        sqlx::query("DELETE FROM saved_search_seen WHERE search_id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        let result = sqlx::query("DELETE FROM saved_searches WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// The subset of `doc_ids` this search has never alerted on, marked seen
    /// atomically-enough for the single-writer worker: insert-or-ignore, then
    /// report which inserts landed.
    pub async fn claim_unseen(&self, search_id: &str, doc_ids: &[String]) -> Result<Vec<String>> {
        let mut unseen = Vec::new();
        for doc_id in doc_ids {
            let result = sqlx::query(
                "INSERT OR IGNORE INTO saved_search_seen (search_id, doc_id, created_at) \
                 VALUES (?1, ?2, ?3)",
            )
            .bind(search_id)
            .bind(doc_id)
            .bind(now())
            .execute(&self.pool)
            .await?;
            if result.rows_affected() > 0 {
                unseen.push(doc_id.clone());
            }
        }
        Ok(unseen)
    }

    // ---- Webhook delivery log ----------------------------------------------

    /// Records an outbound delivery as pending; returns its id.
    pub async fn create_delivery(
        &self,
        kind: &str,
        ref_id: &str,
        url: &str,
        event: &str,
        body: &str,
    ) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO webhook_deliveries (id, kind, ref_id, url, event, body, status, \
             attempts, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', 0, ?7, ?7)",
        )
        .bind(&id)
        .bind(kind)
        .bind(ref_id)
        .bind(url)
        .bind(event)
        .bind(body)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// Marks a delivery delivered — clears any pending retry so the drain won't
    /// re-send it. (The failed path is [`fail_delivery`], which schedules a retry
    /// or marks the row `dead`.)
    pub async fn finish_delivery(
        &self,
        id: &str,
        delivered: bool,
        attempts: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE webhook_deliveries SET status = ?2, attempts = attempts + ?3, \
             last_error = ?4, next_retry_at = NULL, updated_at = ?5 WHERE id = ?1",
        )
        .bind(id)
        .bind(if delivered { "delivered" } else { "failed" })
        .bind(attempts)
        .bind(last_error)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Records a failed delivery outcome and either schedules the next auto-drain
    /// retry (exponential backoff from the row's current `retry_count`, indexing
    /// `backoff_secs` with mild jitter) or, once `retry_count >= max_retries`,
    /// marks the row `dead` so the DLQ view stays meaningful and the drain stops
    /// picking it up. No-op if the row vanished.
    pub async fn fail_delivery(
        &self,
        id: &str,
        attempts: i64,
        last_error: Option<&str>,
        max_retries: i64,
        backoff_secs: &[i64],
    ) -> Result<()> {
        let Some(retry_count): Option<i64> =
            sqlx::query_scalar("SELECT retry_count FROM webhook_deliveries WHERE id = ?1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
        else {
            return Ok(());
        };
        if retry_count >= max_retries || backoff_secs.is_empty() {
            sqlx::query(
                "UPDATE webhook_deliveries SET status = 'dead', attempts = attempts + ?2, \
                 last_error = ?3, next_retry_at = NULL, updated_at = ?4 WHERE id = ?1",
            )
            .bind(id)
            .bind(attempts)
            .bind(last_error)
            .bind(now())
            .execute(&self.pool)
            .await?;
            return Ok(());
        }
        let idx = (retry_count as usize).min(backoff_secs.len() - 1);
        let base = backoff_secs[idx].max(1);
        // Jitter up to +25% to de-sync a herd of deliveries that all failed during
        // the same receiver outage. Deterministic seed (no wall-clock RNG): the id
        // bytes plus the retry count.
        let seed = id.bytes().fold(retry_count as u64, |a, b| {
            a.wrapping_mul(31).wrapping_add(b as u64)
        });
        let jitter = (crate::jitter::lcg_fraction(seed) * (base as f64) * 0.25) as i64;
        let next = Utc::now() + chrono::Duration::seconds(base + jitter);
        sqlx::query(
            "UPDATE webhook_deliveries SET status = 'failed', attempts = attempts + ?2, \
             last_error = ?3, next_retry_at = ?4, updated_at = ?5 WHERE id = ?1",
        )
        .bind(id)
        .bind(attempts)
        .bind(last_error)
        .bind(ts(next))
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Failed deliveries whose scheduled retry is due (`next_retry_at <= now`),
    /// soonest first — the auto-drain's work list. Includes the body so the drain
    /// can re-send without a second read.
    pub async fn due_deliveries(&self, limit: i64) -> Result<Vec<Delivery>> {
        let rows: Vec<DeliveryRow> = sqlx::query_as(
            "SELECT id, kind, ref_id, url, event, body, status, attempts, last_error, \
             created_at, updated_at FROM webhook_deliveries \
             WHERE status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= ?1 \
             ORDER BY next_retry_at ASC LIMIT ?2",
        )
        .bind(now())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Delivery::try_from).collect()
    }

    /// Atomically claims a due delivery for a retry: flips `failed` → `pending`
    /// and bumps `retry_count`, so a concurrent drain tick can't double-send it.
    /// Returns `false` if another tick already claimed it (row no longer `failed`).
    pub async fn begin_delivery_retry(&self, id: &str) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE webhook_deliveries SET status = 'pending', retry_count = retry_count + 1, \
             next_retry_at = NULL, updated_at = ?2 WHERE id = ?1 AND status = 'failed'",
        )
        .bind(id)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Atomically claims a delivery for a **manual** replay. Deliberately a
    /// separate claim from [`Storage::begin_delivery_retry`] rather than a
    /// widened one: the auto-drain must never be able to resurrect a `dead` row
    /// (that is what "dead" means — the ladder gave up and stopped), while an
    /// operator asking for a replay by id explicitly may.
    ///
    /// Claimable states: `failed` and `dead`; `delivered` only under `force`,
    /// so a re-send of something the receiver already accepted is never
    /// accidental. `pending` is never claimable — the row is in flight, and a
    /// second sender would duplicate it *and* race its outcome write.
    ///
    /// Returns `false` when the row is gone or in a state this claim doesn't
    /// cover — the caller answers 409 rather than pretending it scheduled work.
    pub async fn begin_delivery_replay(&self, id: &str, force: bool) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE webhook_deliveries SET status = 'pending', retry_count = retry_count + 1, \
             next_retry_at = NULL, updated_at = ?2 WHERE id = ?1 \
             AND (status IN ('failed', 'dead') OR (?3 = 1 AND status = 'delivered'))",
        )
        .bind(id)
        .bind(now())
        .bind(force as i64)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Returns crash-interrupted deliveries to the retry ladder.
    ///
    /// A `pending` row is one of two things: a delivery created by
    /// `create_delivery` whose send is still running, or a row a drain tick
    /// claimed via [`Storage::begin_delivery_retry`]. Either way the *only*
    /// thing that ever moves it out of `pending` is the in-process outcome
    /// write. Kill the process in that window and the row is stranded forever:
    /// `due_deliveries` scans `failed` only, so nothing re-sends it, and
    /// `prune_ledgers` touches only `delivered`/`dead`, so nothing reclaims it
    /// either — an unbounded leak of undelivered payloads.
    ///
    /// This flips such rows back to `failed` and marks them immediately due, so
    /// the next drain tick picks them up and they walk the normal ladder to
    /// `delivered` or `dead`. `retry_count` is deliberately NOT bumped here —
    /// the subsequent drain claim bumps it, so a crash-looping row still reaches
    /// `dead` after the usual number of retries instead of cycling forever.
    ///
    /// `older_than_secs` must be comfortably longer than the worst-case
    /// in-process delivery, or a live send gets a duplicate sender. See
    /// `crate::webhook::STALE_PENDING_SECS` in the server for the shipped value
    /// and its margin.
    ///
    /// `last_error` is overwritten with the reclaim reason: for a stranded row
    /// "no outcome was ever recorded" is the actionable fact, and the attempt
    /// history remains in `attempts`/`retry_count`.
    pub async fn reclaim_stale_deliveries(&self, older_than_secs: i64) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than_secs.max(1));
        let stamp = now();
        let res = sqlx::query(
            "UPDATE webhook_deliveries SET status = 'failed', next_retry_at = ?2, \
             last_error = 'interrupted: reclaimed after no delivery outcome was recorded', \
             updated_at = ?2 WHERE status = 'pending' AND updated_at < ?1",
        )
        .bind(ts(cutoff))
        .bind(stamp)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    /// Whole-log delivery health in ONE aggregate pass — the source for the
    /// `pumper_webhook_*` metrics.
    ///
    /// Deliberately one query rather than four counts plus a min: `/metrics` is
    /// scraped on an interval, and the numbers must describe the same instant to
    /// be comparable (a `dead` count from after a transition next to a `failed`
    /// count from before it is worse than no gauge at all).
    pub async fn delivery_health(&self) -> Result<DeliveryHealth> {
        let row: DeliveryHealthRow = sqlx::query_as(
            "SELECT \
               COALESCE(SUM(status = 'pending'), 0)   AS pending, \
               COALESCE(SUM(status = 'delivered'), 0) AS delivered, \
               COALESCE(SUM(status = 'failed'), 0)    AS failed, \
               COALESCE(SUM(status = 'dead'), 0)      AS dead, \
               COALESCE(SUM(attempts), 0)             AS attempts, \
               MIN(CASE WHEN status IN ('pending', 'failed') THEN created_at END) \
                 AS oldest_undelivered \
             FROM webhook_deliveries",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(DeliveryHealth {
            pending: row.pending,
            delivered: row.delivered,
            failed: row.failed,
            dead: row.dead,
            attempts: row.attempts,
            oldest_undelivered: row
                .oldest_undelivered
                .as_deref()
                .map(parse_ts)
                .transpose()?,
        })
    }

    /// Deliveries, newest first, optionally filtered by status. The four states
    /// are `pending` (in flight), `delivered` (accepted), `failed` (**still on
    /// the retry ladder**, with a scheduled `next_retry_at`) and `dead` (the
    /// ladder gave up — this is the dead-letter view an operator wants).
    /// Bodies excluded — fetch one by id for the payload.
    ///
    /// `status` is validated by the caller (the route answers 400 on anything
    /// outside those four); an unknown value here simply matches no rows.
    pub async fn list_deliveries(&self, status: Option<&str>, limit: i64) -> Result<Vec<Delivery>> {
        let rows: Vec<DeliveryRow> = sqlx::query_as(
            "SELECT id, kind, ref_id, url, event, '' AS body, status, attempts, last_error, \
             created_at, updated_at FROM webhook_deliveries \
             WHERE (?1 IS NULL OR status = ?1) ORDER BY created_at DESC LIMIT ?2",
        )
        .bind(status)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Delivery::try_from).collect()
    }

    /// Keyset page of deliveries ordered (created_at DESC, id DESC), optionally
    /// filtered by status. Bodies excluded (same as `list_deliveries`). `after`
    /// is the previous page's last (created_at, id).
    pub async fn list_deliveries_page(
        &self,
        status: Option<&str>,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<Delivery>> {
        let (after_ts, after_id) = split_after(after);
        let rows: Vec<DeliveryRow> = sqlx::query_as(
            "SELECT id, kind, ref_id, url, event, '' AS body, status, attempts, last_error, \
             created_at, updated_at FROM webhook_deliveries \
             WHERE (?1 IS NULL OR status = ?1) \
             AND (?2 IS NULL OR created_at < ?2 OR (created_at = ?2 AND id < ?3)) \
             ORDER BY created_at DESC, id DESC LIMIT ?4",
        )
        .bind(status)
        .bind(after_ts)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Delivery::try_from).collect()
    }

    pub async fn get_delivery(&self, id: &str) -> Result<Option<Delivery>> {
        let row: Option<DeliveryRow> = sqlx::query_as(
            "SELECT id, kind, ref_id, url, event, body, status, attempts, last_error, \
             created_at, updated_at FROM webhook_deliveries WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Delivery::try_from).transpose()
    }

    // ── ledger retention ─────────────────────────────────────────────────────
    // Four append-only tables had no prune path at all: `cost_events` (one row
    // per metered engine call), `webhook_deliveries` (one row per outbound POST,
    // body included), `job_yield` (one row per dataset per job) and
    // `saved_search_seen` (one row per alerted doc, forever). On a machine with
    // cron schedules they are the tables that grow while nobody is looking.
    //
    // Every knob is OFF by default and every prune is scoped by a predicate that
    // protects something still in use — deleting a ledger is data loss, so the
    // precedent set by `revision_retention_days` (opt-in, with the reason stated)
    // is followed exactly rather than softened.

    /// Prunes the four unbounded ledgers according to `retention`. Each `0` day
    /// count skips its table entirely, so an unconfigured deployment does nothing.
    ///
    /// The scoping predicates, and why each exists:
    ///
    /// - **cost_events** — only events of jobs that have already reached a
    ///   terminal state. A running job's budget ceiling is enforced against the
    ///   SUM of its own events; pruning under it would silently hand it more
    ///   money than the operator granted. Guarded by
    ///   `prune_cost_events_spares_a_running_jobs_events`.
    /// - **webhook_deliveries** — only `delivered` (and, under a separate knob,
    ///   `dead`) rows. `pending` and `failed` are the live retry queue and the
    ///   replayable dead-letter queue; pruning either would drop an undelivered
    ///   payload on the floor.
    /// - **job_yield** — plain age. It is derived from job results, which remain.
    /// - **saved_search_seen** — plain age, and the sharpest edge in this list:
    ///   a pruned `seen` row makes an already-alerted document look new again, so
    ///   a still-matching doc re-fires its webhook. Off unless the operator
    ///   deliberately accepts that.
    pub async fn prune_ledgers(&self, retention: &LedgerRetention) -> Result<LedgerPruned> {
        let mut out = LedgerPruned::default();
        if retention.cost_event_days > 0 {
            out.cost_events = sqlx::query(
                "DELETE FROM cost_events WHERE created_at < ?1 \
                 AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = cost_events.job_id \
                                 AND j.status IN ('queued', 'running'))",
            )
            .bind(ts(cutoff(retention.cost_event_days)))
            .execute(&self.pool)
            .await?
            .rows_affected();
        }
        if retention.delivered_webhook_days > 0 {
            out.webhook_deliveries += sqlx::query(
                "DELETE FROM webhook_deliveries WHERE status = 'delivered' AND created_at < ?1",
            )
            .bind(ts(cutoff(retention.delivered_webhook_days)))
            .execute(&self.pool)
            .await?
            .rows_affected();
        }
        if retention.dead_webhook_days > 0 {
            out.webhook_deliveries += sqlx::query(
                "DELETE FROM webhook_deliveries WHERE status = 'dead' AND created_at < ?1",
            )
            .bind(ts(cutoff(retention.dead_webhook_days)))
            .execute(&self.pool)
            .await?
            .rows_affected();
        }
        if retention.job_yield_days > 0 {
            out.job_yield = sqlx::query("DELETE FROM job_yield WHERE created_at < ?1")
                .bind(ts(cutoff(retention.job_yield_days)))
                .execute(&self.pool)
                .await?
                .rows_affected();
        }
        if retention.saved_search_seen_days > 0 {
            out.saved_search_seen =
                sqlx::query("DELETE FROM saved_search_seen WHERE created_at < ?1")
                    .bind(ts(cutoff(retention.saved_search_seen_days)))
                    .execute(&self.pool)
                    .await?
                    .rows_affected();
        }
        Ok(out)
    }

    /// Row counts of the tables that have no natural bound, for the read-only
    /// store report. Cheap `COUNT(*)`s, but still a table scan each — on-demand.
    pub async fn ledger_row_counts(&self) -> Result<Vec<(String, i64)>> {
        Ok(self
            .ledger_stats()
            .await?
            .into_iter()
            .map(|s| (s.table, s.rows))
            .collect())
    }

    /// Per-table growth of the append-only stores: rows plus the age of the
    /// oldest row. The age is what separates "big because it is busy" from "big
    /// because nothing ever bounded it" — a table with rows going back a year and
    /// retention off is accruing, not merely large.
    ///
    /// Read-only, one `COUNT(*)` + one `MIN(created_at)` per table. On-demand.
    pub async fn ledger_stats(&self) -> Result<Vec<LedgerStat>> {
        let mut out = Vec::new();
        for table in LEDGER_TABLES {
            // `table` comes from a crate constant, never from a caller.
            let row: (i64, Option<String>) =
                sqlx::query_as(&format!("SELECT COUNT(*), MIN(created_at) FROM {table}"))
                    .fetch_one(&self.pool)
                    .await?;
            out.push(LedgerStat {
                table: table.to_string(),
                rows: row.0,
                oldest: row.1.as_deref().and_then(|s| parse_ts(s).ok()),
            });
        }
        Ok(out)
    }

    /// Tables named `*_new` still present in `sqlite_master`.
    ///
    /// SQLite cannot `ALTER` a `CHECK` constraint, so a migration that needs one
    /// rebuilds the table: `CREATE TABLE x_new` → copy → `DROP TABLE x` →
    /// `ALTER TABLE x_new RENAME TO x` (migration 0021 does exactly this to
    /// `triggers`). The scaffold is transient and each migration runs in a
    /// transaction, so on any correctly-migrated database this returns EMPTY —
    /// which is the point of checking. A leftover means a rebuild did not
    /// complete, and the live table may be the pre-rebuild one.
    pub async fn stale_rebuild_tables(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT name FROM sqlite_master \
             WHERE type = 'table' AND name LIKE '%\\_new' ESCAPE '\\' ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    // ── ingress ──────────────────────────────────────────────────────────────
    // Inbound event ingress sources: per-caller credentials for POST /ingest/{id}.

    pub async fn create_ingress_source(&self, name: &str, secret: &str) -> Result<IngressSource> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO ingress_sources (id, name, secret, enabled, created_at) \
             VALUES (?1, ?2, ?3, 1, ?4)",
        )
        .bind(&id)
        .bind(name)
        .bind(secret)
        .bind(now())
        .execute(&self.pool)
        .await?;
        self.get_ingress_source(&id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    pub async fn get_ingress_source(&self, id: &str) -> Result<Option<IngressSource>> {
        let row: Option<IngressSourceRow> = sqlx::query_as(
            "SELECT id, name, secret, enabled, created_at FROM ingress_sources WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(IngressSource::try_from).transpose()
    }

    pub async fn list_ingress_sources(&self) -> Result<Vec<IngressSource>> {
        let rows: Vec<IngressSourceRow> = sqlx::query_as(
            "SELECT id, name, secret, enabled, created_at FROM ingress_sources ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(IngressSource::try_from).collect()
    }

    pub async fn set_ingress_source_enabled(&self, id: &str, enabled: bool) -> Result<bool> {
        let result = sqlx::query("UPDATE ingress_sources SET enabled = ?2 WHERE id = ?1")
            .bind(id)
            .bind(enabled as i64)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_ingress_source(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM ingress_sources WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Enabled external-kind triggers for one ingress source — the evaluation
    /// set for an inbound event. `source_app = '*'` triggers match every source.
    pub async fn enabled_external_triggers(&self, source_id: &str) -> Result<Vec<Trigger>> {
        let rows: Vec<TriggerRow> = sqlx::query_as(&format!(
            "SELECT {TRIGGER_COLUMNS} FROM triggers \
             WHERE source_kind = 'external' AND (source_app = ?1 OR source_app = '*') \
             AND enabled = 1"
        ))
        .bind(source_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Trigger::try_from).collect()
    }

    // ── reconcile ───────────────────────────────────────────────────────────
    // Catalog GitOps reconciler (M19). Every mutating method here is fenced
    // with `AND managed_by = ?tag` in SQL so an untagged (hand-made or
    // code-seeded) schedule can never be touched, even by a buggy plan.

    /// Creates (or re-syncs) the catalog-managed schedule for `app`, tagged
    /// `managed_by = tag`. Id is the stable `catalog-<app>` so re-applying a plan
    /// is idempotent; a conflicting re-apply updates cron and re-enables rather
    /// than duplicating. Params stay `{}` — the scheduler falls back to the
    /// app's `default_params()` at fire time.
    pub async fn create_managed_schedule(
        &self,
        app: &str,
        cron: &str,
        tag: &str,
    ) -> Result<Schedule> {
        let id = format!("catalog-{app}");
        sqlx::query(
            "INSERT INTO schedules (id, app, cron, params, enabled, priority, managed_by, created_at) \
             VALUES (?1, ?2, ?3, '{}', 1, 0, ?4, ?5) \
             ON CONFLICT(id) DO UPDATE SET cron = excluded.cron, enabled = 1 \
             WHERE schedules.managed_by = excluded.managed_by",
        )
        .bind(&id)
        .bind(app)
        .bind(cron)
        .bind(tag)
        .bind(now())
        .execute(&self.pool)
        .await?;
        self.get_schedule(&id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    /// Updates the cron of a schedule owned by `tag` (and re-enables it, since
    /// the desired state of a cron-bearing catalog row is "running"). Returns
    /// `false` when the row doesn't exist *or isn't owned by `tag`* — the fence.
    pub async fn set_managed_schedule_cron(&self, id: &str, cron: &str, tag: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE schedules SET cron = ?2, enabled = 1 WHERE id = ?1 AND managed_by = ?3",
        )
        .bind(id)
        .bind(cron)
        .bind(tag)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Enables/disables a schedule owned by `tag`. Returns `false` when the row
    /// doesn't exist *or isn't owned by `tag`* — the fence.
    pub async fn set_managed_schedule_enabled(
        &self,
        id: &str,
        enabled: bool,
        tag: &str,
    ) -> Result<bool> {
        let result =
            sqlx::query("UPDATE schedules SET enabled = ?2 WHERE id = ?1 AND managed_by = ?3")
                .bind(id)
                .bind(enabled as i64)
                .bind(tag)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }

    // ── checkpoints ──────────────────────────────────────────────────────────

    /// Persists a job's durable-execution checkpoint, guarded by the same
    /// attempts-lineage rule as [`complete`](Self::complete): the write only
    /// lands while `(id, attempt)` still owns the `running` row, so a stale task
    /// whose job was reset/reaped and re-claimed can never overwrite the live
    /// attempt's checkpoint. Returns whether the write landed (`false` = stale,
    /// discarded). Oversized blobs (> [`MAX_CHECKPOINT_BYTES`]) are rejected
    /// with an error — a checkpoint that big is a bug, not state.
    pub async fn save_checkpoint(&self, id: Uuid, attempt: i64, state: &Value) -> Result<bool> {
        let blob = state.to_string();
        if blob.len() > MAX_CHECKPOINT_BYTES {
            return Err(Error::App(format!(
                "checkpoint too large: {} bytes (cap {MAX_CHECKPOINT_BYTES})",
                blob.len()
            )));
        }
        // INSERT..SELECT so the lineage guard and the upsert are one atomic
        // statement; `resume_failures` is preserved across overwrites (it counts
        // restores handed out, not writes). The SELECT's WHERE clause also
        // disambiguates the UPSERT grammar for SQLite's parser.
        let r = sqlx::query(
            "INSERT INTO checkpoints (job_id, state, attempt, resume_failures, updated_at) \
             SELECT ?1, ?2, ?3, 0, ?4 FROM jobs \
             WHERE id = ?1 AND status = 'running' AND attempts = ?3 \
             ON CONFLICT(job_id) DO UPDATE SET \
               state = excluded.state, attempt = excluded.attempt, \
               updated_at = excluded.updated_at",
        )
        .bind(id.to_string())
        .bind(blob)
        .bind(attempt)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() > 0)
    }

    /// A job's stored checkpoint as `(state, resume_failures)`, or `None`. A
    /// blob that no longer parses is treated as absent (never resumed from
    /// silently) — the caller decides whether to clear it.
    pub async fn load_checkpoint(&self, id: Uuid) -> Result<Option<(Value, i64)>> {
        let row: Option<(String, i64)> =
            sqlx::query_as("SELECT state, resume_failures FROM checkpoints WHERE job_id = ?1")
                .bind(id.to_string())
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.and_then(|(state, failures)| {
            serde_json::from_str(&state)
                .ok()
                .map(|v: Value| (v, failures))
        }))
    }

    /// Counts one restore handed out from a job's checkpoint (called at claim
    /// time, before the attempt runs) and returns the new count. The counter is
    /// the poisoned-checkpoint escape: attempts that *complete* clear the row,
    /// so a count that keeps growing means every restored attempt has failed.
    pub async fn bump_checkpoint_resumes(&self, id: Uuid) -> Result<i64> {
        let n: Option<i64> = sqlx::query_scalar(
            "UPDATE checkpoints SET resume_failures = resume_failures + 1 \
             WHERE job_id = ?1 RETURNING resume_failures",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        Ok(n.unwrap_or(0))
    }

    /// Drops a job's checkpoint (on terminal completion, or when the blob is
    /// judged poisoned). Idempotent.
    pub async fn clear_checkpoint(&self, id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM checkpoints WHERE job_id = ?1")
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ── api recipes ──────────────────────────────────────────────────────────

    /// Handle to the API X-ray recipe store (`api_recipes`, migration 0025) on
    /// this database. The SQL lives on [`crate::recipes::RecipeStore`] (the
    /// `TierMemory` pattern: a small pool-wrapping handle shared on
    /// `AppContext`); this is the one constructor the server/routes reach it by.
    pub fn recipes(&self) -> crate::recipes::RecipeStore {
        crate::recipes::RecipeStore::new(self.pool.clone())
    }

    // ── job yield ────────────────────────────────────────────────────────────

    /// Persists the yield entries parsed from one completed job's result
    /// ([`crate::costs::extract_yields`]) — one row per summary the result
    /// carried. `None` counts store as NULL (the result didn't report that
    /// number), never 0. Best-effort telemetry: callers log-and-continue on
    /// error, a job's outcome must never hinge on its accounting.
    pub async fn record_job_yield(
        &self,
        job_id: Uuid,
        app: &str,
        entries: &[crate::costs::YieldEntry],
    ) -> Result<()> {
        let at = now();
        for e in entries {
            sqlx::query(
                "INSERT INTO job_yield (job_id, app, dataset, new_count, changed_count, \
                 unchanged_count, removed_count, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )
            .bind(job_id.to_string())
            .bind(app)
            .bind(&e.dataset)
            .bind(e.new)
            .bind(e.changed)
            .bind(e.unchanged)
            .bind(e.removed)
            .bind(&at)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// Yield grouped by (app, dataset) since a cutoff — the economics window
    /// query. `SUM` over SQL NULLs stays NULL, so an app whose results never
    /// reported a count comes back `None` ("unknown"), distinct from `Some(0)`
    /// ("reported zero") — the /economics math turns `None` into JSON null, not
    /// $0 or a division.
    pub async fn yield_summary(&self, since: DateTime<Utc>) -> Result<Vec<YieldSummary>> {
        let rows = sqlx::query_as::<_, YieldSummary>(
            "SELECT app, dataset, COUNT(DISTINCT job_id) AS jobs, \
                    SUM(new_count) AS new, SUM(changed_count) AS changed, \
                    SUM(unchanged_count) AS unchanged, SUM(removed_count) AS removed \
             FROM job_yield WHERE created_at > ?1 \
             GROUP BY app, dataset ORDER BY app, dataset",
        )
        .bind(crate::datasets::ts(since))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    // ── job stages ───────────────────────────────────────────────────────────

    /// Stamps where one run's wall-clock went (`job_stages`, migration 0034).
    /// One row per job — a retried job's row is replaced by the attempt that
    /// produced its current outcome. Best-effort telemetry, exactly like
    /// [`Self::record_job_yield`]: the caller logs and continues on error.
    pub async fn record_job_stages(&self, job_id: Uuid, app: &str, s: &JobStages) -> Result<()> {
        sqlx::query(
            "INSERT INTO job_stages (job_id, app, attempt, run_ms, index_ms, hooks_ms, \
             alerts_ms, total_ms, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
             ON CONFLICT(job_id) DO UPDATE SET app = ?2, attempt = ?3, run_ms = ?4, \
             index_ms = ?5, hooks_ms = ?6, alerts_ms = ?7, total_ms = ?8, created_at = ?9",
        )
        .bind(job_id.to_string())
        .bind(app)
        .bind(s.attempt)
        .bind(s.run_ms)
        .bind(s.index_ms)
        .bind(s.hooks_ms)
        .bind(s.alerts_ms)
        .bind(s.total_ms)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// One job's stage timings, or `None` when the job has none — a job that
    /// ran before this table existed, or one that never reached its fan-out.
    /// `None` is an honest "unknown", never a zeroed row.
    pub async fn job_stages(&self, job_id: Uuid) -> Result<Option<JobStages>> {
        let row = sqlx::query_as::<_, JobStages>(
            "SELECT attempt, run_ms, index_ms, hooks_ms, alerts_ms, total_ms \
             FROM job_stages WHERE job_id = ?1",
        )
        .bind(job_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    // ── job receipt joins ────────────────────────────────────────────────────
    // Job-scoped reads behind `GET /jobs/{id}/receipt`. Each is an index seek on
    // one job id (migration 0035), never a corpus scan — a receipt is a
    // per-job audit view, not a metrics query.

    /// This job's persisted yield rows (`job_yield`), one per dataset summary
    /// its result reported. Counts stay `Option`: NULL means the result did not
    /// report that number, which is not the same as zero.
    pub async fn job_yield_entries(&self, job_id: Uuid) -> Result<Vec<crate::costs::YieldEntry>> {
        let rows: Vec<YieldRow> = sqlx::query_as(
            "SELECT dataset, new_count, changed_count, unchanged_count, removed_count \
             FROM job_yield WHERE job_id = ?1 ORDER BY id",
        )
        .bind(job_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::costs::YieldEntry {
                dataset: r.dataset,
                new: r.new_count,
                changed: r.changed_count,
                unchanged: r.unchanged_count,
                removed: r.removed_count,
            })
            .collect())
    }

    /// Revisions this job actually wrote, grouped by `(app, dataset, change)`.
    ///
    /// Counted from `record_revisions.job_id` — the provenance stamp (0030), so
    /// this is attribution by *identity*, not the time-window approximation the
    /// worker's own push path uses. Revisions written by a path that doesn't
    /// stamp a job (or before 0030) carry NULL and are therefore invisible here;
    /// the receipt says so rather than folding them in.
    ///
    /// Lives here rather than on `Datasets` because it is a job-scoped read for
    /// the job receipt; `Datasets` owns the revision write path and the
    /// record-scoped chain reads.
    pub async fn job_revision_counts(&self, job_id: Uuid) -> Result<Vec<RevisionCount>> {
        let rows = sqlx::query_as::<_, RevisionCount>(
            "SELECT app, dataset, change, COUNT(*) AS count FROM record_revisions \
             WHERE job_id = ?1 GROUP BY app, dataset, change ORDER BY app, dataset, change",
        )
        .bind(job_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Outbound deliveries logged against this job id — its own result callback
    /// (`kind = 'job'`) and the global failure firehose (`kind = 'failure'`).
    ///
    /// Watch and saved-search deliveries are deliberately NOT here: they are
    /// logged against the watch / search id, so the log cannot attribute them to
    /// a job. The receipt names that gap instead of guessing.
    pub async fn job_deliveries(&self, job_id: Uuid) -> Result<Vec<Delivery>> {
        let rows: Vec<DeliveryRow> = sqlx::query_as(
            "SELECT id, kind, ref_id, url, event, '' AS body, status, attempts, last_error, \
             created_at, updated_at FROM webhook_deliveries \
             WHERE ref_id = ?1 AND kind IN ('job', 'failure') ORDER BY created_at DESC",
        )
        .bind(job_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Delivery::try_from).collect()
    }

    /// The extraction-health verdicts THIS run produced (`source_runs`, keyed
    /// `(source_id, job_id)`) — the honest at-run-time answer, as opposed to
    /// the source's state right now, which a later run may have changed.
    /// Empty when health detection was off for the run.
    pub async fn job_health_verdicts(&self, job_id: Uuid) -> Result<Vec<JobHealthVerdict>> {
        let rows = sqlx::query_as::<_, JobHealthVerdict>(
            "SELECT source_id, verdict, diagnosis, score, state_after \
             FROM source_runs WHERE job_id = ?1 ORDER BY source_id",
        )
        .bind(job_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Jobs this job's outcome caused a trigger to enqueue (`source_job_id`,
    /// migration 0035). Empty for a run that fired nothing — and also for a run
    /// that predates the column, which the receipt reports as unknown.
    pub async fn triggered_hops(&self, job_id: Uuid) -> Result<Vec<Job>> {
        let sql =
            format!("SELECT {JOB_COLUMNS} FROM jobs WHERE source_job_id = ?1 ORDER BY created_at");
        let rows: Vec<JobRow> = sqlx::query_as(&sql)
            .bind(job_id.to_string())
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Job::try_from).collect()
    }

    // ── derived ──────────────────────────────────────────────────────────────
    // CRUD for derived-dataset specs (M11). The hot-path read (enabled specs
    // for one source) and the recompute/backfill mechanics live on `Datasets`;
    // this is the management surface. Cycle rejection happens at create time
    // here — the one write path a spec can enter through.

    /// Creates a derived spec, assigning its id. Rejects a spec that would
    /// close a cycle through the existing specs of the same app (including the
    /// self-loop `source == target`) — acyclic chains stay bounded at runtime
    /// by the depth cap, cycles never terminate and are refused at the door.
    pub async fn create_derived_spec(&self, n: &NewDerivedSpec<'_>) -> Result<DerivedSpec> {
        let existing = self.list_derived_specs(Some(n.source_app)).await?;
        if crate::datasets::derived_would_cycle(&existing, n.source_dataset, n.target_dataset) {
            return Err(Error::BadRequest(format!(
                "derived spec '{}' -> '{}' would create a cycle",
                n.source_dataset, n.target_dataset
            )));
        }
        // Aggregate specs (v2) are group-shaped, not row-shaped: they cannot
        // carry a per-row lookup or projection, and they share the stored
        // `lookup` column — validated here, the one write path a spec enters
        // through, so every stored spec is evaluable.
        if let Some(group) = n.group {
            if n.lookup.is_some() {
                return Err(Error::BadRequest(
                    "a derived spec cannot combine aggregates with lookup".into(),
                ));
            }
            if !n.project.is_empty() {
                return Err(Error::BadRequest(
                    "a derived spec cannot combine aggregates with project \
                     (group rows are synthesized, not projected per record)"
                        .into(),
                ));
            }
            crate::datasets::validate_group(group)?;
        }
        let id = Uuid::new_v4().to_string();
        let lookup_json = match (n.lookup, n.group) {
            (Some(l), _) => Some(serde_json::to_string(l).unwrap_or_else(|_| "null".into())),
            (None, Some(g)) => Some(serde_json::to_string(g).unwrap_or_else(|_| "null".into())),
            (None, None) => None,
        };
        sqlx::query(
            "INSERT INTO derived (id, source_app, source_dataset, target_dataset, filters, \
             project, lookup, enabled, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8)",
        )
        .bind(&id)
        .bind(n.source_app)
        .bind(n.source_dataset)
        .bind(n.target_dataset)
        .bind(serde_json::to_string(n.filters).unwrap_or_else(|_| "[]".into()))
        .bind(serde_json::to_string(n.project).unwrap_or_else(|_| "{}".into()))
        .bind(lookup_json)
        .bind(now())
        .execute(&self.pool)
        .await?;
        self.get_derived_spec(&id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    pub async fn get_derived_spec(&self, id: &str) -> Result<Option<DerivedSpec>> {
        let row: Option<crate::datasets::DerivedRow> = sqlx::query_as(&format!(
            "SELECT {} FROM derived WHERE id = ?1",
            crate::datasets::DERIVED_COLUMNS
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(DerivedSpec::try_from).transpose()
    }

    /// All derived specs, optionally filtered by source app.
    pub async fn list_derived_specs(&self, app: Option<&str>) -> Result<Vec<DerivedSpec>> {
        let rows: Vec<crate::datasets::DerivedRow> = sqlx::query_as(&format!(
            "SELECT {} FROM derived WHERE (?1 IS NULL OR source_app = ?1) \
             ORDER BY created_at, id",
            crate::datasets::DERIVED_COLUMNS
        ))
        .bind(app)
        .fetch_all(&self.pool)
        .await?;
        // An unreadable row is logged and skipped rather than failing the whole
        // listing — CRUD (and cycle detection, which reads this) must keep
        // working around one corrupt spec. `get_derived_spec` still errors when
        // that exact spec is asked for by id.
        Ok(crate::datasets::specs_from_rows(rows, "list_derived_specs"))
    }

    /// Per-spec kill-switch.
    pub async fn set_derived_enabled(&self, id: &str, enabled: bool) -> Result<bool> {
        let result = sqlx::query("UPDATE derived SET enabled = ?2 WHERE id = ?1")
            .bind(id)
            .bind(enabled as i64)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_derived_spec(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM derived WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

/// Create-time fields for a derived spec (borrowed; storage assigns
/// id/enabled/time). Filters are `$.path:op:value` specs (validated by the
/// caller via `datasets::parse_filter_specs`); `project` maps
/// `out_field -> "$.path"`.
pub struct NewDerivedSpec<'a> {
    pub source_app: &'a str,
    pub source_dataset: &'a str,
    pub target_dataset: &'a str,
    pub filters: &'a [String],
    pub project: &'a std::collections::BTreeMap<String, String>,
    pub lookup: Option<&'a crate::datasets::DerivedLookup>,
    /// Aggregate half (M11 v2); mutually exclusive with `lookup`/`project`.
    pub group: Option<&'a crate::datasets::DerivedGroup>,
}

/// Hard cap on one checkpoint blob (bytes). Generous enough for a 100k-URL
/// crawl frontier (~a few MB) while keeping a runaway app from turning the jobs
/// database into a blob store.
pub const MAX_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;

/// A `job_yield` row as stored (the `*_count` column names), mapped to the
/// public [`crate::costs::YieldEntry`] on read.
#[derive(sqlx::FromRow)]
struct YieldRow {
    dataset: String,
    new_count: Option<i64>,
    changed_count: Option<i64>,
    unchanged_count: Option<i64>,
    removed_count: Option<i64>,
}

/// One source's extraction-health verdict from a single run
/// ([`Storage::job_health_verdicts`]). `diagnosis` is `None` when the detector
/// had nothing to say — unknown, not "healthy".
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct JobHealthVerdict {
    pub source_id: String,
    pub verdict: String,
    pub diagnosis: Option<String>,
    pub score: f64,
    pub state_after: String,
}

/// One `(app, dataset, change)` group of the revisions a single job wrote
/// ([`Storage::job_revision_counts`]). `change` is the revision kind — `new`,
/// `changed` or `removed`.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct RevisionCount {
    pub app: String,
    pub dataset: String,
    pub change: String,
    pub count: i64,
}

/// Where one job run's wall-clock went (`job_stages`, migration 0034).
///
/// Every duration is `Option`: `None` means the run never reached that stage
/// (it failed first, or the stage was skipped), which is deliberately distinct
/// from `Some(0)` ("the stage ran and took under a millisecond"). Readers must
/// render `None` as unknown — an invented zero would claim the stage was free.
///
/// `total_ms` spans claim → end of fan-out and is **not** the sum of the named
/// stages: the queue's own bookkeeping (completion write, checkpoint clear,
/// yield record) sits between them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct JobStages {
    /// The attempt these numbers describe.
    pub attempt: i64,
    /// The app's own `run()`, i.e. the scraping itself.
    pub run_ms: Option<i64>,
    /// Building and committing this run's search documents (+ deletions).
    pub index_ms: Option<i64>,
    /// Loading the run's revisions, the health/contract gates, watch webhooks
    /// and dataset triggers.
    pub hooks_ms: Option<i64>,
    /// Saved-search evaluation: the forced index flush, materialization, and
    /// standing-alert dispatch.
    pub alerts_ms: Option<i64>,
    /// Claim → end of fan-out.
    pub total_ms: Option<i64>,
}

/// One (app, dataset) group of the trailing-window yield rollup
/// ([`Storage::yield_summary`]). Counts are `Option`: `None` means no result in
/// the window reported that number — unknown, not zero.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct YieldSummary {
    pub app: String,
    /// `""` = the result-root summary; else the result's key path
    /// (`"unified"`, `"datasets.velocity"`) — the closest thing the job-result
    /// convention has to a dataset name.
    pub dataset: String,
    /// Distinct jobs that contributed rows to this group in the window.
    pub jobs: i64,
    pub new: Option<i64>,
    pub changed: Option<i64>,
    pub unchanged: Option<i64>,
    pub removed: Option<i64>,
}

/// Job timing aggregates (seconds) for the metrics endpoint: execution duration
/// (started→finished) and queue wait (created→started), each as sum/count/max so
/// callers can expose Prometheus summaries and derive averages.
#[derive(Debug, Clone, Default, sqlx::FromRow)]
pub struct JobTimingStats {
    pub duration_sum: f64,
    pub duration_count: i64,
    pub duration_max: f64,
    pub wait_sum: f64,
    pub wait_count: i64,
    pub wait_max: f64,
}

#[derive(sqlx::FromRow)]
struct JobRow {
    id: String,
    app: String,
    params: String,
    status: String,
    attempts: i64,
    max_attempts: i64,
    priority: i64,
    callback_url: Option<String>,
    callback_secret: Option<String>,
    budget_usd: Option<f64>,
    schedule_id: Option<String>,
    trigger_id: Option<String>,
    result: Option<String>,
    error: Option<String>,
    created_at: String,
    available_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
}

impl TryFrom<JobRow> for Job {
    type Error = Error;

    fn try_from(r: JobRow) -> Result<Job> {
        Ok(Job {
            id: Uuid::parse_str(&r.id).map_err(|e| Error::Parse(format!("job id: {e}")))?,
            app: r.app,
            params: serde_json::from_str(&r.params).unwrap_or(Value::Null),
            status: JobStatus::parse(&r.status)
                .ok_or_else(|| Error::Parse(format!("unknown job status '{}'", r.status)))?,
            attempts: r.attempts,
            max_attempts: r.max_attempts,
            priority: r.priority,
            callback_url: r.callback_url,
            callback_secret: r.callback_secret,
            budget_usd: r.budget_usd,
            schedule_id: r.schedule_id,
            trigger_id: r.trigger_id,
            result: r
                .result
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok()),
            error: r.error,
            created_at: parse_ts(&r.created_at)?,
            available_at: parse_ts(&r.available_at)?,
            started_at: r.started_at.as_deref().map(parse_ts).transpose()?,
            finished_at: r.finished_at.as_deref().map(parse_ts).transpose()?,
        })
    }
}

const TRIGGER_COLUMNS: &str = "id, name, source_kind, source_app, source_dataset, on_change, \
                               on_status, target_app, params, budget_usd, priority, \
                               max_attempts, enabled, created_at, filters, plugin_hooks";

/// One sandboxed WASM hook on a trigger: the plugin to run plus the `params`
/// half of the `extract_v2` envelope it receives (so one module serves many
/// triggers with different config, exactly like extraction plugins).
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct PluginHook {
    /// Loaded plugin name (file stem under the plugins dir).
    pub plugin: String,
    /// Params envelope passed to the plugin (default `{}`).
    #[serde(default = "default_hook_params")]
    pub params: Value,
    /// Predicate hook only — what to do when the plugin errors/traps/returns
    /// garbage: `"fire"` (default; fail-open) or `"skip"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_error: Option<String>,
}

fn default_hook_params() -> Value {
    Value::Object(serde_json::Map::new())
}

/// M15 "WASM everywhere" v1 — a trigger's optional plugin hooks, stored as one
/// JSON column (`plugin_hooks`, migration 0032). Both hooks receive the delta
/// (`_trigger`) object as the envelope's `doc` and BOTH fail open with a loud
/// log — a broken plugin never wedges the pipeline.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct TriggerPluginHooks {
    /// Decides fire/skip: must return `{"pass": bool}` (or a bare bool).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<PluginHook>,
    /// Shapes the `_trigger` object before it is merged into target params.
    /// Must return a JSON object; provenance keys are re-stamped by the host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<PluginHook>,
}

/// A reactive-pipeline edge: (source event) → (enqueue target app). The set of
/// triggers is the pipeline DAG.
#[derive(Debug, Clone, Serialize)]
pub struct Trigger {
    pub id: String,
    pub name: Option<String>,
    /// 'dataset' | 'job'
    pub source_kind: String,
    pub source_app: String,
    /// '*' or dataset name (dataset kind only).
    pub source_dataset: Option<String>,
    /// 'new'|'changed'|'removed'|'fresh'|'any' (dataset kind only).
    pub on_change: Option<String>,
    /// 'succeeded'|'failed'|'any' (job kind only).
    pub on_status: Option<String>,
    pub target_app: String,
    /// Static params template; `_trigger` is merged over it at fire time.
    pub params: Value,
    pub budget_usd: Option<f64>,
    pub priority: i64,
    pub max_attempts: i64,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    /// JSON-path predicate specs (`$.path:op:value`, the `?filter=` grammar)
    /// ANDed against the inbound payload. External kind only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<String>>,
    /// Sandboxed WASM predicate/transform hooks (M15 v1). All source kinds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_hooks: Option<TriggerPluginHooks>,
}

impl Trigger {
    /// True when this dataset trigger covers `dataset` (`'*'` = all).
    pub fn covers_dataset(&self, dataset: &str) -> bool {
        matches!(self.source_dataset.as_deref(), Some("*") | None)
            || self.source_dataset.as_deref() == Some(dataset)
    }
}

/// Create-time fields for a trigger (borrowed; storage assigns id/enabled/time).
pub struct NewTrigger<'a> {
    pub name: Option<&'a str>,
    pub source_kind: &'a str,
    pub source_app: &'a str,
    pub source_dataset: Option<&'a str>,
    pub on_change: Option<&'a str>,
    pub on_status: Option<&'a str>,
    pub target_app: &'a str,
    pub params: &'a Value,
    pub budget_usd: Option<f64>,
    pub priority: i64,
    pub max_attempts: i64,
    /// External kind only: `$.path:op:value` predicate specs (ANDed).
    pub filters: Option<&'a [String]>,
    /// Sandboxed WASM predicate/transform hooks (M15 v1).
    pub plugin_hooks: Option<&'a TriggerPluginHooks>,
}

#[derive(sqlx::FromRow)]
struct TriggerRow {
    id: String,
    name: Option<String>,
    source_kind: String,
    source_app: String,
    source_dataset: Option<String>,
    on_change: Option<String>,
    on_status: Option<String>,
    target_app: String,
    params: String,
    budget_usd: Option<f64>,
    priority: i64,
    max_attempts: i64,
    enabled: i64,
    created_at: String,
    filters: Option<String>,
    plugin_hooks: Option<String>,
}

impl TryFrom<TriggerRow> for Trigger {
    type Error = Error;

    fn try_from(r: TriggerRow) -> Result<Trigger> {
        Ok(Trigger {
            id: r.id,
            name: r.name,
            source_kind: r.source_kind,
            source_app: r.source_app,
            source_dataset: r.source_dataset,
            on_change: r.on_change,
            on_status: r.on_status,
            target_app: r.target_app,
            params: serde_json::from_str(&r.params).unwrap_or(Value::Null),
            budget_usd: r.budget_usd,
            priority: r.priority,
            max_attempts: r.max_attempts,
            enabled: r.enabled != 0,
            created_at: parse_ts(&r.created_at)?,
            filters: r
                .filters
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok()),
            plugin_hooks: r
                .plugin_hooks
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok()),
        })
    }
}

// ---- Trigger decision ledger (migration 0036) -------------------------------

/// One recorded trigger decision: what the evaluation of one edge against one
/// source event concluded, fires and skips alike.
#[derive(Debug, Clone, Serialize)]
pub struct TriggerRun {
    pub id: String,
    /// The evaluated trigger, or [`TRIGGER_SET_ID`] for a decision about the
    /// whole edge set rather than one trigger.
    pub trigger_id: String,
    /// `fired`, or one of the skip reasons in [`TRIGGER_OUTCOMES`].
    pub outcome: String,
    /// `dataset` | `job` | `external`.
    pub source_kind: String,
    pub source_job_id: Option<String>,
    pub dataset: Option<String>,
    pub event_id: Option<String>,
    /// The hop that was enqueued (outcome `fired`).
    pub job_id: Option<String>,
    /// Free-text context: an error message, a plugin name, an idempotency key.
    pub detail: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// The `trigger_id` a decision carries when it is about the evaluation SET, not
/// about one trigger — the only such case is the set failing to load, which
/// drops every edge of that source event at once.
pub const TRIGGER_SET_ID: &str = "*";

/// Every value `trigger_runs.outcome` may take. The API documents these as the
/// skip-reason vocabulary, so the list is a contract, not a hint.
pub const TRIGGER_OUTCOMES: &[&str] = &[
    // The hop was enqueued.
    "fired",
    // The evaluation set could not be loaded — every edge of this source event
    // was dropped (transient DB error). Recorded against `TRIGGER_SET_ID`.
    "eval_set_error",
    // No revision in this dataset's batch passed the trigger's `on_change`.
    "no_change_match",
    // The source job's terminal status did not pass `on_status`.
    "status_mismatch",
    // The inbound payload did not satisfy the trigger's JSON-path filters.
    "filter_miss",
    // The trigger's stored filter specs no longer parse.
    "bad_filters",
    // A predicate plugin returned `pass=false` (or failed with `on_error=skip`).
    "predicate_veto",
    // A CONFIGURED plugin hook names a module the host has not loaded, so the
    // hook did nothing: the predicate did not gate and the transform did not
    // shape. The hop itself still took the fail-open path — this row exists
    // because "the gate passed" and "there was no gate" are otherwise
    // indistinguishable. Usually means the build/install step never ran
    // (`just plugins-install`). `detail` is the missing plugin name.
    "plugin_missing",
    // The trigger already appears in the source's provenance chain.
    "cycle",
    // `[triggers] max_depth` reached.
    "depth",
    // `target_app` is not a registered app.
    "target_unregistered",
    // A job already exists for this hop's idempotency key.
    "dedup",
    // The enqueue itself failed.
    "enqueue_failed",
];

/// Insert shape for [`Storage::record_trigger_run`]. Borrowed and `Default`ing
/// so each decision path names only the fields it actually knows.
#[derive(Debug, Default)]
pub struct NewTriggerRun<'a> {
    pub trigger_id: &'a str,
    pub outcome: &'a str,
    pub source_kind: &'a str,
    pub source_job_id: Option<&'a str>,
    pub dataset: Option<&'a str>,
    pub event_id: Option<&'a str>,
    pub job_id: Option<&'a str>,
    pub detail: Option<&'a str>,
}

#[derive(sqlx::FromRow)]
struct TriggerRunRow {
    id: String,
    trigger_id: String,
    outcome: String,
    source_kind: String,
    source_job_id: Option<String>,
    dataset: Option<String>,
    event_id: Option<String>,
    job_id: Option<String>,
    detail: Option<String>,
    created_at: String,
}

impl TryFrom<TriggerRunRow> for TriggerRun {
    type Error = Error;

    fn try_from(r: TriggerRunRow) -> Result<TriggerRun> {
        Ok(TriggerRun {
            id: r.id,
            trigger_id: r.trigger_id,
            outcome: r.outcome,
            source_kind: r.source_kind,
            source_job_id: r.source_job_id,
            dataset: r.dataset,
            event_id: r.event_id,
            job_id: r.job_id,
            detail: r.detail,
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

// ---- DataHub governance audit trail (migration 0037) ------------------------

/// One executed DataHub governance action, with the remote evidence that caused
/// it. Durable — unlike the in-memory last-poll summary, which a restart erases
/// while the disabled schedule it explains stays disabled.
#[derive(Debug, Clone, Serialize)]
pub struct DatahubGovernAction {
    pub id: String,
    /// One of [`DATAHUB_GOVERN_ACTIONS`].
    pub action: String,
    /// The Pumper app acted on.
    pub target: String,
    /// The dataset whose remote state was the evidence, when the action came
    /// from one dataset's signal.
    pub dataset: Option<String>,
    /// The row produced or changed: a schedule id, a job id.
    pub subject: Option<String>,
    /// The remote signal: one of [`DATAHUB_GOVERN_EVIDENCE`].
    pub evidence: String,
    pub detail: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Every value `datahub_govern_actions.action` may take. Documented on
/// `GET /datahub/status`, so the list is a contract, not a hint.
pub const DATAHUB_GOVERN_ACTIONS: &[&str] = &[
    // A catalog-managed schedule was disabled (dataset deprecated in DataHub).
    "disable_schedule",
    // An immediate sync job was enqueued (failing assertion in DataHub).
    "enqueue_sync",
    // An app entered the paused set (`cost:pause` tag) — new jobs run free
    // tiers only.
    "pause_app",
    // An app left the paused set (tag removed) — normal budgets resume.
    "resume_app",
    // The paused set was dropped because governance had been blind for longer
    // than the staleness window: pauses expire loudly instead of freezing at $0.
    "expire_pause",
];

/// Every value `datahub_govern_actions.evidence` may take — what an operator
/// should go and look at in DataHub.
pub const DATAHUB_GOVERN_EVIDENCE: &[&str] = &[
    "deprecation",
    "cost:pause",
    "assertions",
    // Not a remote signal: the ABSENCE of one for too long (see `expire_pause`).
    "stale",
];

/// Insert shape for [`Storage::record_datahub_govern_action`]. Borrowed and
/// `Default`ing so each action names only the fields it knows.
#[derive(Debug, Default)]
pub struct NewDatahubGovernAction<'a> {
    pub action: &'a str,
    pub target: &'a str,
    pub dataset: Option<&'a str>,
    pub subject: Option<&'a str>,
    pub evidence: &'a str,
    pub detail: Option<&'a str>,
}

#[derive(sqlx::FromRow)]
struct DatahubGovernActionRow {
    id: String,
    action: String,
    target: String,
    dataset: Option<String>,
    subject: Option<String>,
    evidence: String,
    detail: Option<String>,
    created_at: String,
}

impl TryFrom<DatahubGovernActionRow> for DatahubGovernAction {
    type Error = Error;

    fn try_from(r: DatahubGovernActionRow) -> Result<DatahubGovernAction> {
        Ok(DatahubGovernAction {
            id: r.id,
            action: r.action,
            target: r.target,
            dataset: r.dataset,
            subject: r.subject,
            evidence: r.evidence,
            detail: r.detail,
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

/// A standing full-text query that webhooks NEW matches exactly once each.
/// With `materialize` set, each run also snapshots the result set into that
/// dataset (M13 "queries as datasets") so the change feed / watches / triggers /
/// `?filter=` / export compose over full-text semantics.
#[derive(Debug, Clone, Serialize)]
pub struct SavedSearch {
    pub id: String,
    pub query: String,
    pub app: Option<String>,
    pub dataset: Option<String>,
    pub url: String,
    #[serde(skip_serializing)]
    pub secret: Option<String>,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialize: Option<SearchMaterialize>,
    pub created_at: DateTime<Utc>,
}

/// Target dataset a saved search materializes its result set into.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct SearchMaterialize {
    pub app: String,
    pub dataset: String,
}

#[derive(sqlx::FromRow)]
struct SavedSearchRow {
    id: String,
    query: String,
    app: Option<String>,
    dataset: Option<String>,
    url: String,
    secret: Option<String>,
    enabled: i64,
    materialize_app: Option<String>,
    materialize_dataset: Option<String>,
    created_at: String,
}

impl TryFrom<SavedSearchRow> for SavedSearch {
    type Error = Error;

    fn try_from(r: SavedSearchRow) -> Result<SavedSearch> {
        Ok(SavedSearch {
            id: r.id,
            query: r.query,
            app: r.app,
            dataset: r.dataset,
            url: r.url,
            secret: r.secret,
            enabled: r.enabled != 0,
            // Half-set columns (hand-edited DB) degrade to "not materialized"
            // rather than a phantom target with an empty app or dataset.
            materialize: match (r.materialize_app, r.materialize_dataset) {
                (Some(app), Some(dataset)) => Some(SearchMaterialize { app, dataset }),
                _ => None,
            },
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

/// One logged webhook delivery. `body` is only populated by `get_delivery`.
#[derive(Debug, Clone, Serialize)]
pub struct Delivery {
    pub id: String,
    pub kind: String,
    pub ref_id: String,
    pub url: String,
    pub event: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub body: String,
    pub status: String,
    pub attempts: i64,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Whole-log delivery health, all read at one instant — see
/// [`Storage::delivery_health`].
#[derive(Debug, Clone, Default, Serialize)]
pub struct DeliveryHealth {
    /// In flight right now (a first send, a drain retry, or a manual replay).
    pub pending: i64,
    /// Accepted by the receiver.
    pub delivered: i64,
    /// Failed and **still on the retry ladder** — not the dead-letter queue.
    pub failed: i64,
    /// The ladder gave up: the dead-letter queue.
    pub dead: i64,
    /// Send attempts summed across the whole log, retries included.
    pub attempts: i64,
    /// Creation time of the oldest delivery the receiver has not accepted.
    ///
    /// Over `pending` + `failed` only. `dead` is deliberately excluded: it is
    /// terminal, so including it would pin the derived age gauge at the age of
    /// the oldest dead row forever and destroy the only signal the operator
    /// actually watches — "is my undelivered backlog getting older right now".
    pub oldest_undelivered: Option<DateTime<Utc>>,
}

impl DeliveryHealth {
    /// Age in whole seconds of [`DeliveryHealth::oldest_undelivered`], or 0 when
    /// nothing is undelivered. Takes `now` rather than reading the clock so the
    /// arithmetic is directly testable, and clamps at 0 — a row stamped in the
    /// future (clock skew) is "brand new", never negative age.
    pub fn oldest_undelivered_secs(&self, now: DateTime<Utc>) -> i64 {
        self.oldest_undelivered
            .map_or(0, |at| (now - at).num_seconds().max(0))
    }
}

#[derive(sqlx::FromRow)]
struct DeliveryHealthRow {
    pending: i64,
    delivered: i64,
    failed: i64,
    dead: i64,
    attempts: i64,
    oldest_undelivered: Option<String>,
}

#[derive(sqlx::FromRow)]
struct DeliveryRow {
    id: String,
    kind: String,
    ref_id: String,
    url: String,
    event: String,
    body: String,
    status: String,
    attempts: i64,
    last_error: Option<String>,
    created_at: String,
    updated_at: String,
}

impl TryFrom<DeliveryRow> for Delivery {
    type Error = Error;

    fn try_from(r: DeliveryRow) -> Result<Delivery> {
        Ok(Delivery {
            id: r.id,
            kind: r.kind,
            ref_id: r.ref_id,
            url: r.url,
            event: r.event,
            body: r.body,
            status: r.status,
            attempts: r.attempts,
            last_error: r.last_error,
            created_at: parse_ts(&r.created_at)?,
            updated_at: parse_ts(&r.updated_at)?,
        })
    }
}

#[derive(sqlx::FromRow)]
struct WatchRow {
    id: String,
    app: String,
    dataset: String,
    url: String,
    secret: Option<String>,
    sink: String,
    enabled: i64,
    created_at: String,
}

impl TryFrom<WatchRow> for Watch {
    type Error = Error;

    fn try_from(r: WatchRow) -> Result<Watch> {
        Ok(Watch {
            id: r.id,
            app: r.app,
            dataset: r.dataset,
            url: r.url,
            secret: r.secret,
            sink: r.sink,
            enabled: r.enabled != 0,
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

#[derive(sqlx::FromRow)]
struct ScheduleRow {
    id: String,
    app: String,
    cron: String,
    params: String,
    enabled: i64,
    priority: i64,
    timezone: Option<String>,
    misfire_policy: String,
    max_attempts: Option<i64>,
    managed_by: Option<String>,
    last_run: Option<String>,
    created_at: String,
}

impl TryFrom<ScheduleRow> for Schedule {
    type Error = Error;

    fn try_from(r: ScheduleRow) -> Result<Schedule> {
        Ok(Schedule {
            id: r.id,
            app: r.app,
            cron: r.cron,
            params: serde_json::from_str(&r.params).unwrap_or(Value::Null),
            enabled: r.enabled != 0,
            priority: r.priority,
            timezone: r.timezone,
            misfire_policy: r.misfire_policy,
            max_attempts: r.max_attempts,
            managed_by: r.managed_by,
            last_run: r.last_run.as_deref().map(parse_ts).transpose()?,
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

/// An inbound-event ingress source: the per-caller credential for
/// `POST /ingest/{id}`. The secret signs every inbound body (HMAC-SHA256) and
/// is never serialized into list/read responses.
#[derive(Debug, Clone, Serialize)]
pub struct IngressSource {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing)]
    pub secret: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct IngressSourceRow {
    id: String,
    name: String,
    secret: String,
    enabled: i64,
    created_at: String,
}

impl TryFrom<IngressSourceRow> for IngressSource {
    type Error = Error;

    fn try_from(r: IngressSourceRow) -> Result<IngressSource> {
        Ok(IngressSource {
            id: r.id,
            name: r.name,
            secret: r.secret,
            enabled: r.enabled != 0,
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

/// Splits an optional keyset cursor pair into two bind-ready Options, so a
/// single SQL `WHERE (?1 IS NULL OR ...)` clause covers the first-page case.
/// The append-only tables with no natural bound, in report order. Kept as one
/// list so `ledger_row_counts` and the store report cannot drift from what
/// [`Storage::prune_ledgers`] actually knows how to bound.
pub const LEDGER_TABLES: &[&str] = &[
    "cost_events",
    "webhook_deliveries",
    "job_yield",
    "saved_search_seen",
    "record_revisions",
];

/// Day-count retention for the four unbounded ledgers. `0` means OFF, which is
/// the default for every field: each of these deletions is data loss of a
/// different kind (spend history, delivery evidence, yield telemetry, alert
/// suppression), so retention is something an operator turns on, never something
/// that happens to them. See [`Storage::prune_ledgers`] for the per-table
/// scoping predicates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LedgerRetention {
    pub cost_event_days: u64,
    /// `delivered` rows — the successful-delivery log.
    pub delivered_webhook_days: u64,
    /// `dead` rows — the exhausted dead-letter tail. Separate knob because a DLQ
    /// entry is evidence of a failure someone may still want to see.
    pub dead_webhook_days: u64,
    pub job_yield_days: u64,
    pub saved_search_seen_days: u64,
}

impl LedgerRetention {
    /// True when at least one table is bounded — the janitor's "is there work"
    /// check, so a fully unconfigured deployment never even opens a transaction.
    pub fn any_enabled(&self) -> bool {
        *self != Self::default()
    }
}

/// Growth of one append-only table: how many rows, and how far back they go.
#[derive(Debug, Clone, Serialize)]
pub struct LedgerStat {
    pub table: String,
    pub rows: i64,
    /// `created_at` of the oldest row, or `None` when the table is empty.
    pub oldest: Option<DateTime<Utc>>,
}

/// Rows removed by one [`Storage::prune_ledgers`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct LedgerPruned {
    pub cost_events: u64,
    pub webhook_deliveries: u64,
    pub job_yield: u64,
    pub saved_search_seen: u64,
}

impl LedgerPruned {
    pub fn total(&self) -> u64 {
        self.cost_events + self.webhook_deliveries + self.job_yield + self.saved_search_seen
    }
}

fn split_after(after: Option<(String, String)>) -> (Option<String>, Option<String>) {
    after
        .map(|(t, i)| (Some(t), Some(i)))
        .unwrap_or((None, None))
}

/// Fixed-width RFC 3339 UTC ("...Z", µs precision) so that lexicographic
/// comparison in SQL matches chronological order.
fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn now() -> String {
    ts(Utc::now())
}

/// Retention cutoff `days` before now. Named so every ledger prune derives its
/// boundary the same way (and so the janitor's log and the SQL can never drift).
fn cutoff(days: u64) -> DateTime<Utc> {
    Utc::now() - chrono::Duration::days(days as i64)
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| Error::Parse(format!("bad timestamp '{s}': {e}")))
}
