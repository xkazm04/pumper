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
                           callback_url, callback_secret, result, error, created_at, \
                           available_at, started_at, finished_at";

/// Options for enqueuing a job. Defaults: 1 attempt, no delay, priority 0.
#[derive(Debug, Clone, Default)]
pub struct EnqueueOptions {
    pub params: Value,
    pub max_attempts: i64,
    pub delay_secs: u64,
    pub priority: i64,
    pub callback_url: Option<String>,
    pub callback_secret: Option<String>,
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
    pub last_run: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
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
        let id = Uuid::new_v4();
        let created = Utc::now();
        let available = created + chrono::Duration::seconds(opts.delay_secs as i64);
        sqlx::query(
            "INSERT INTO jobs (id, app, params, status, attempts, max_attempts, priority, \
             callback_url, callback_secret, created_at, available_at) \
             VALUES (?1, ?2, ?3, 'queued', 0, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(id.to_string())
        .bind(app)
        .bind(opts.params.to_string())
        .bind(opts.max_attempts.max(1))
        .bind(opts.priority)
        .bind(opts.callback_url)
        .bind(opts.callback_secret)
        .bind(ts(created))
        .bind(ts(available))
        .execute(&self.pool)
        .await?;
        self.get(id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))
    }

    /// Atomically claims the highest-priority due job and flips it to `running`.
    /// Apps listed in `blocked` are skipped, which is how the worker enforces
    /// per-app concurrency limits (fairness across many apps' queues).
    pub async fn claim_next(&self, blocked: &[String]) -> Result<Option<Job>> {
        let exclusion = if blocked.is_empty() {
            String::new()
        } else {
            let marks: Vec<String> = (0..blocked.len()).map(|i| format!("?{}", i + 2)).collect();
            format!(" AND app NOT IN ({})", marks.join(", "))
        };
        let sql = format!(
            "UPDATE jobs SET status = 'running', attempts = attempts + 1, started_at = ?1 \
             WHERE id = (SELECT id FROM jobs WHERE status = 'queued' AND available_at <= ?1{exclusion} \
                         ORDER BY priority DESC, created_at LIMIT 1) \
             RETURNING {JOB_COLUMNS}"
        );
        let mut query = sqlx::query_as::<_, JobRow>(&sql).bind(now());
        for app in blocked {
            query = query.bind(app);
        }
        let row = query.fetch_optional(&self.pool).await?;
        row.map(Job::try_from).transpose()
    }

    pub async fn complete(&self, id: Uuid, result: Value) -> Result<()> {
        sqlx::query(
            "UPDATE jobs SET status = 'succeeded', result = ?2, error = NULL, finished_at = ?3 \
             WHERE id = ?1",
        )
        .bind(id.to_string())
        .bind(result.to_string())
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Marks failed, or re-queues with exponential backoff while attempts
    /// remain. Returns the status the job ended up in.
    pub async fn fail(&self, id: Uuid, error: &str) -> Result<JobStatus> {
        let job = self
            .get(id)
            .await?
            .ok_or(Error::Storage(sqlx::Error::RowNotFound))?;
        if job.attempts < job.max_attempts {
            let backoff_secs = 10u64
                .saturating_mul(2u64.saturating_pow(job.attempts.max(0) as u32))
                .min(3600);
            let available = Utc::now() + chrono::Duration::seconds(backoff_secs as i64);
            sqlx::query(
                "UPDATE jobs SET status = 'queued', error = ?2, available_at = ?3 WHERE id = ?1",
            )
            .bind(id.to_string())
            .bind(error)
            .bind(ts(available))
            .execute(&self.pool)
            .await?;
            Ok(JobStatus::Queued)
        } else {
            self.fail_permanently(id, error).await?;
            Ok(JobStatus::Failed)
        }
    }

    pub async fn fail_permanently(&self, id: Uuid, error: &str) -> Result<()> {
        sqlx::query(
            "UPDATE jobs SET status = 'failed', error = ?2, finished_at = ?3 WHERE id = ?1",
        )
        .bind(id.to_string())
        .bind(error)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
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

    /// Counts jobs grouped by status — for the metrics endpoint.
    pub async fn status_counts(&self) -> Result<Vec<(String, i64)>> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT status, COUNT(*) FROM jobs GROUP BY status")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows)
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

    // ---- Schedules --------------------------------------------------------

    pub async fn create_schedule(
        &self,
        app: &str,
        cron: &str,
        params: Value,
        priority: i64,
    ) -> Result<Schedule> {
        let id = Uuid::new_v4().to_string();
        self.insert_schedule(&id, app, cron, params, priority, true).await?;
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

    async fn insert_schedule(
        &self,
        id: &str,
        app: &str,
        cron: &str,
        params: Value,
        priority: i64,
        enabled: bool,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO schedules (id, app, cron, params, enabled, priority, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(id)
        .bind(app)
        .bind(cron)
        .bind(params.to_string())
        .bind(enabled as i64)
        .bind(priority)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        let rows: Vec<ScheduleRow> = sqlx::query_as(
            "SELECT id, app, cron, params, enabled, priority, last_run, created_at \
             FROM schedules ORDER BY app",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Schedule::try_from).collect()
    }

    pub async fn get_schedule(&self, id: &str) -> Result<Option<Schedule>> {
        let row: Option<ScheduleRow> = sqlx::query_as(
            "SELECT id, app, cron, params, enabled, priority, last_run, created_at \
             FROM schedules WHERE id = ?1",
        )
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

#[derive(sqlx::FromRow)]
struct ScheduleRow {
    id: String,
    app: String,
    cron: String,
    params: String,
    enabled: i64,
    priority: i64,
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
            last_run: r.last_run.as_deref().map(parse_ts).transpose()?,
            created_at: parse_ts(&r.created_at)?,
        })
    }
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
