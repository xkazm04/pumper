//! Cost ledger: meters every engine call a job makes, so spend is queryable
//! per job, per app, and per engine tier. The Claude tier is where real money
//! goes (the CLI reports `total_cost_usd`); http/browser events are recorded
//! at 0.0 for call-count and ROI accounting. Written by the metered
//! `AppContext::fetch` / `AppContext::research` wrappers.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{Error, Result};

/// One metered engine call.
#[derive(Debug, Clone, Serialize)]
pub struct CostEvent {
    pub job_id: String,
    pub app: String,
    pub engine: String,
    pub url: Option<String>,
    pub cost_usd: f64,
    pub detail: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Aggregated spend for one (app, engine) pair.
#[derive(Debug, Clone, Serialize)]
pub struct CostSummary {
    pub app: String,
    pub engine: String,
    pub calls: i64,
    pub cost_usd: f64,
}

pub struct CostLedger {
    pool: SqlitePool,
}

impl CostLedger {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Records one engine call. Never fails the caller's job over accounting —
    /// callers may ignore the Result, but the write itself is cheap and local.
    pub async fn record(
        &self,
        job_id: Uuid,
        app: &str,
        engine: &str,
        url: Option<&str>,
        cost_usd: f64,
        detail: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO cost_events (job_id, app, engine, url, cost_usd, detail, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(job_id.to_string())
        .bind(app)
        .bind(engine)
        .bind(url)
        .bind(cost_usd)
        .bind(detail)
        .bind(ts(Utc::now()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Everything one job spent, oldest first.
    pub async fn job_events(&self, job_id: Uuid) -> Result<Vec<CostEvent>> {
        let rows: Vec<CostEventRow> = sqlx::query_as(
            "SELECT job_id, app, engine, url, cost_usd, detail, created_at \
             FROM cost_events WHERE job_id = ?1 ORDER BY id",
        )
        .bind(job_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(CostEvent::try_from).collect()
    }

    /// Total USD one job has spent so far — the budget-ceiling check.
    pub async fn job_total(&self, job_id: Uuid) -> Result<f64> {
        let total: Option<f64> =
            sqlx::query_scalar("SELECT SUM(cost_usd) FROM cost_events WHERE job_id = ?1")
                .bind(job_id.to_string())
                .fetch_one(&self.pool)
                .await?;
        Ok(total.unwrap_or(0.0))
    }

    /// Spend grouped by (app, engine), optionally filtered to one app and/or a
    /// time window — the ROI overview.
    pub async fn summary(
        &self,
        app: Option<&str>,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<CostSummary>> {
        let rows: Vec<(String, String, i64, f64)> = sqlx::query_as(
            "SELECT app, engine, COUNT(*), COALESCE(SUM(cost_usd), 0) FROM cost_events \
             WHERE (?1 IS NULL OR app = ?1) AND (?2 IS NULL OR created_at > ?2) \
             GROUP BY app, engine ORDER BY app, engine",
        )
        .bind(app)
        .bind(since.map(ts))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(app, engine, calls, cost_usd)| CostSummary { app, engine, calls, cost_usd })
            .collect())
    }
}

#[derive(sqlx::FromRow)]
struct CostEventRow {
    job_id: String,
    app: String,
    engine: String,
    url: Option<String>,
    cost_usd: f64,
    detail: Option<String>,
    created_at: String,
}

impl TryFrom<CostEventRow> for CostEvent {
    type Error = Error;

    fn try_from(r: CostEventRow) -> Result<CostEvent> {
        Ok(CostEvent {
            job_id: r.job_id,
            app: r.app,
            engine: r.engine,
            url: r.url,
            cost_usd: r.cost_usd,
            detail: r.detail,
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| Error::Parse(format!("bad timestamp '{s}': {e}")))
}
