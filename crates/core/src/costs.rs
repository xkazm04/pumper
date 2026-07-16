//! Cost ledger: meters every engine call a job makes, so spend is queryable
//! per job, per app, and per engine tier. The Claude tier is where real money
//! goes (the CLI reports `total_cost_usd`); http/browser events are recorded
//! at 0.0 for call-count and ROI accounting.
//!
//! Everything reaches the ledger through `AppContext::meter` — the metered
//! `fetch` / `research` wrappers call it for you, and apps that must drive an
//! engine raw (the crawler) call it directly. [`SpentTotal`] mirrors a job's
//! total in memory so the per-call budget check doesn't re-aggregate the ledger.

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

/// A job's running spend total, held in the job's `AppContext`.
///
/// The pre-flight budget check runs on every metered call, and reading spend
/// from the ledger meant re-`SUM`-ing the job's entire cost history each time —
/// O(n) per call, O(n²) over a job. This mirrors the same number in memory:
/// seeded once from the ledger at context construction (so a *retried* job still
/// counts its prior attempts' spend), then advanced by each metered seam as it
/// records.
///
/// The ledger stays the source of truth — this is a read cache for one job's
/// lifetime, and is rebuilt from the ledger on restart. `f64` is bit-cast into
/// an `AtomicU64` so concurrent metered calls within a job can advance it
/// without a lock.
#[derive(Debug, Default)]
pub struct SpentTotal(std::sync::atomic::AtomicU64);

impl SpentTotal {
    /// Seeds the total, normally from [`CostLedger::job_total`].
    pub fn new(seed_usd: f64) -> Self {
        Self(std::sync::atomic::AtomicU64::new(seed_usd.max(0.0).to_bits()))
    }

    /// USD recorded against this job so far.
    pub fn get(&self) -> f64 {
        f64::from_bits(self.0.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Adds a recorded cost. Non-positive and non-finite deltas are ignored —
    /// engine costs are `Option<f64>` defaulted to 0.0, and a NaN must never be
    /// able to poison a budget ceiling into never tripping.
    pub fn add(&self, delta_usd: f64) {
        if !delta_usd.is_finite() || delta_usd <= 0.0 {
            return;
        }
        let mut cur = self.0.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(cur) + delta_usd).to_bits();
            match self.0.compare_exchange_weak(
                cur,
                next,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeds_and_accumulates() {
        let s = SpentTotal::new(1.5);
        assert_eq!(s.get(), 1.5);
        s.add(0.25);
        s.add(0.25);
        assert_eq!(s.get(), 2.0);
    }

    #[test]
    fn default_starts_at_zero() {
        assert_eq!(SpentTotal::default().get(), 0.0);
    }

    #[test]
    fn ignores_non_positive_and_non_finite_deltas() {
        // Engine costs arrive as Option<f64> defaulted to 0.0, and a NaN must
        // never be able to poison the total — a NaN budget comparison is always
        // false, which would silently disable the ceiling forever.
        let s = SpentTotal::new(1.0);
        s.add(0.0);
        s.add(-5.0);
        s.add(f64::NAN);
        s.add(f64::INFINITY);
        assert_eq!(s.get(), 1.0);
    }

    #[test]
    fn a_negative_seed_floors_at_zero() {
        assert_eq!(SpentTotal::new(-3.0).get(), 0.0);
    }

    #[test]
    fn concurrent_adds_do_not_lose_updates() {
        // The CAS loop is the whole reason this isn't a plain store; a job's
        // metered calls can run concurrently.
        let s = std::sync::Arc::new(SpentTotal::default());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = s.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    s.add(0.001);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!((s.get() - 8.0).abs() < 1e-6, "lost updates: {}", s.get());
    }
}
