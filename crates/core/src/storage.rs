use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::config::StorageConfig;
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
}

/// A standing subscription: POST a webhook whenever a job leaves fresh
/// revisions in the watched dataset (`"*"` = all datasets of the app).
#[derive(Debug, Clone, Serialize)]
pub struct Watch {
    pub id: String,
    pub app: String,
    pub dataset: String,
    pub url: String,
    /// HMAC-SHA256 signing secret for delivery bodies (never serialized).
    #[serde(skip_serializing)]
    pub secret: Option<String>,
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
    pub last_run: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Column list shared by every `schedules` SELECT (kept in sync with `ScheduleRow`).
const SCHEDULE_COLUMNS: &str =
    "id, app, cron, params, enabled, priority, timezone, misfire_policy, max_attempts, \
     last_run, created_at";

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

/// Durable job store on SQLite (WAL). Jobs survive restarts; `recover_stuck`
/// re-queues anything that was mid-flight when the process died.
pub struct Storage {
    pool: SqlitePool,
    pub artifacts_dir: PathBuf,
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
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| Error::Storage(sqlx::Error::Migrate(Box::new(e))))?;

        Ok(Self { pool, artifacts_dir: cfg.artifacts_dir.clone() })
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
             trigger_id, created_at, available_at) \
             VALUES (?1, ?2, ?3, 'queued', 0, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
        let (after_ts, after_id) = after.map(|(t, i)| (Some(t), Some(i))).unwrap_or((None, None));
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
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT app, COUNT(*) FROM jobs WHERE status = 'failed' GROUP BY app",
        )
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
        let result =
            sqlx::query("UPDATE jobs SET status = 'queued', available_at = ?1 WHERE status = 'running'")
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
        let rows: Vec<JobRow> = sqlx::query_as(&sql).bind(&cutoff).fetch_all(&self.pool).await?;
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

    pub async fn create_watch(
        &self,
        app: &str,
        dataset: &str,
        url: &str,
        secret: Option<&str>,
    ) -> Result<Watch> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO watches (id, app, dataset, url, secret, enabled, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
        )
        .bind(&id)
        .bind(app)
        .bind(dataset)
        .bind(url)
        .bind(secret)
        .bind(now())
        .execute(&self.pool)
        .await?;
        self.get_watch(&id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    pub async fn get_watch(&self, id: &str) -> Result<Option<Watch>> {
        let row: Option<WatchRow> = sqlx::query_as(
            "SELECT id, app, dataset, url, secret, enabled, created_at FROM watches WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Watch::try_from).transpose()
    }

    /// Watches for an app (all watches when `app` is None).
    pub async fn list_watches(&self, app: Option<&str>) -> Result<Vec<Watch>> {
        let rows: Vec<WatchRow> = sqlx::query_as(
            "SELECT id, app, dataset, url, secret, enabled, created_at FROM watches \
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
            "SELECT id, app, dataset, url, secret, enabled, created_at FROM watches \
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
            "SELECT id, app, dataset, url, secret, enabled, created_at FROM watches \
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

    #[allow(clippy::too_many_arguments)]
    pub async fn create_trigger(&self, t: &NewTrigger<'_>) -> Result<Trigger> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO triggers (id, name, source_kind, source_app, source_dataset, on_change, \
             on_status, target_app, params, budget_usd, priority, max_attempts, enabled, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1, ?13)",
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
        .execute(&self.pool)
        .await?;
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
    pub async fn enabled_triggers(&self, source_kind: &str, source_app: &str) -> Result<Vec<Trigger>> {
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
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_trigger(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM triggers WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
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

    // ---- Saved searches -----------------------------------------------------

    pub async fn create_saved_search(
        &self,
        query: &str,
        app: Option<&str>,
        dataset: Option<&str>,
        url: &str,
        secret: Option<&str>,
    ) -> Result<SavedSearch> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO saved_searches (id, query, app, dataset, url, secret, enabled, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7)",
        )
        .bind(&id)
        .bind(query)
        .bind(app)
        .bind(dataset)
        .bind(url)
        .bind(secret)
        .bind(now())
        .execute(&self.pool)
        .await?;
        self.get_saved_search(&id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    pub async fn get_saved_search(&self, id: &str) -> Result<Option<SavedSearch>> {
        let row: Option<SavedSearchRow> = sqlx::query_as(
            "SELECT id, query, app, dataset, url, secret, enabled, created_at \
             FROM saved_searches WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(SavedSearch::try_from).transpose()
    }

    pub async fn list_saved_searches(&self, enabled_only: bool) -> Result<Vec<SavedSearch>> {
        let rows: Vec<SavedSearchRow> = sqlx::query_as(
            "SELECT id, query, app, dataset, url, secret, enabled, created_at \
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
            "SELECT id, query, app, dataset, url, secret, enabled, created_at \
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
        let seed = id.bytes().fold(retry_count as u64, |a, b| a.wrapping_mul(31).wrapping_add(b as u64));
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

    /// Deliveries, newest first, optionally filtered by status (`failed` is the
    /// dead-letter view). Bodies excluded — fetch one by id for the payload.
    pub async fn list_deliveries(
        &self,
        status: Option<&str>,
        limit: i64,
    ) -> Result<Vec<Delivery>> {
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
                               max_attempts, enabled, created_at";

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
        })
    }
}

/// A standing full-text query that webhooks NEW matches exactly once each.
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
    pub created_at: DateTime<Utc>,
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
            last_run: r.last_run.as_deref().map(parse_ts).transpose()?,
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

/// Splits an optional keyset cursor pair into two bind-ready Options, so a
/// single SQL `WHERE (?1 IS NULL OR ...)` clause covers the first-page case.
fn split_after(after: Option<(String, String)>) -> (Option<String>, Option<String>) {
    after.map(|(t, i)| (Some(t), Some(i))).unwrap_or((None, None))
}

/// Fixed-width RFC 3339 UTC ("...Z", µs precision) so that lexicographic
/// comparison in SQL matches chronological order.
fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn now() -> String {
    ts(Utc::now())
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| Error::Parse(format!("bad timestamp '{s}': {e}")))
}
