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
        Self(std::sync::atomic::AtomicU64::new(
            seed_usd.max(0.0).to_bits(),
        ))
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
            .map(|(app, engine, calls, cost_usd)| CostSummary {
                app,
                engine,
                calls,
                cost_usd,
            })
            .collect())
    }
}

// ── job yield ───────────────────────────────────────────────────────────────

/// One yield observation parsed out of a completed job's result JSON: the
/// `UpsertSummary`-shaped counts apps already report (`"new"`, `"changed"`,
/// `"unchanged"`, `"removed"`). Persisted to `job_yield` so the cost ledger can
/// be joined against what the spend actually produced (`GET /economics`).
///
/// Every count is `Option`: a result that doesn't report a number stores NULL —
/// never 0, which would be a claim ("this run produced nothing") the result
/// didn't make.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct YieldEntry {
    /// Where in the result the summary sat: `""` for the result root, else the
    /// dot-joined key path (`"datasets.velocity"`, `"unified"`). Apps that write
    /// several datasets nest one summary per dataset; the path is the closest
    /// thing the result convention has to a dataset name.
    pub dataset: String,
    pub new: Option<i64>,
    pub changed: Option<i64>,
    pub unchanged: Option<i64>,
    pub removed: Option<i64>,
}

/// Ceiling on entries parsed from one result — a pathological result (records
/// that themselves carry `new` fields) must not turn into thousands of rows.
const MAX_YIELD_ENTRIES: usize = 16;
/// How deep the walk descends. Observed conventions sit at the root or one to
/// two objects down (`datasets.formations`); anything deeper is record data.
const MAX_YIELD_DEPTH: usize = 3;

/// Extracts yield summaries from a job-result JSON, worker-side, no app changes.
///
/// Conventions found in the fleet (2026-07): most apps report
/// `{"new": n, "changed": n, "unchanged": n}` numbers at the result root
/// (hackernews, extractor, cordis, eu-sedia, plugin, …); multi-dataset apps nest
/// the same shape under dataset-named keys (`census-bfs`'s
/// `datasets.{formations,velocity}`, homewyse/valuation's `"unified"`,
/// census-density's saturation block); crawl reports numeric `new`/`changed`
/// page counts at the root. An object counts as a summary when it carries a
/// numeric (or array-valued — the raw `UpsertSummary` shape, counted by length)
/// `new` or `changed`; other counts are recorded when parseable and left `None`
/// when not. Objects with no such field (e.g. cms-fee-schedule's
/// `change_since_last_run: "new"` *string*) yield nothing.
pub fn extract_yields(result: &serde_json::Value) -> Vec<YieldEntry> {
    let mut out = Vec::new();
    walk_yields(result, String::new(), 0, &mut out);
    out
}

fn walk_yields(v: &serde_json::Value, path: String, depth: usize, out: &mut Vec<YieldEntry>) {
    if out.len() >= MAX_YIELD_ENTRIES || depth > MAX_YIELD_DEPTH {
        return;
    }
    let Some(obj) = v.as_object() else { return };
    let new = yield_count(obj.get("new"));
    let changed = yield_count(obj.get("changed"));
    if new.is_some() || changed.is_some() {
        out.push(YieldEntry {
            dataset: path.clone(),
            new,
            changed,
            unchanged: yield_count(obj.get("unchanged")),
            removed: yield_count(obj.get("removed")),
        });
    }
    // Keep walking below a match: a root summary and a nested `"unified"` block
    // are different datasets, both real. Arrays (`records`, …) are never
    // descended — their elements are data, not summaries.
    for (key, child) in obj {
        if child.is_object() {
            let child_path = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            walk_yields(child, child_path, depth + 1, out);
        }
    }
}

/// A count field: a non-negative integer, or an array counted by length (the
/// raw `UpsertSummary` serialization carries key *lists*). Anything else —
/// strings, floats, negatives, absent — is "not reported" (`None`), never 0.
fn yield_count(v: Option<&serde_json::Value>) -> Option<i64> {
    match v? {
        serde_json::Value::Number(n) => n.as_i64().filter(|&n| n >= 0),
        serde_json::Value::Array(a) => i64::try_from(a.len()).ok(),
        _ => None,
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

    // ── job yield extraction ──

    #[test]
    fn extracts_root_summary_counts() {
        // The dominant convention: numeric new/changed/unchanged at the root
        // (hackernews, extractor, cordis, eu-sedia, plugin, …).
        let result = serde_json::json!({
            "count": 30, "new": 5, "changed": 2, "unchanged": 23, "records": [{"id": 1}]
        });
        let out = extract_yields(&result);
        assert_eq!(
            out,
            vec![YieldEntry {
                dataset: String::new(),
                new: Some(5),
                changed: Some(2),
                unchanged: Some(23),
                removed: None,
            }]
        );
    }

    #[test]
    fn extracts_nested_dataset_summaries() {
        // census-bfs shape: per-dataset summaries under `datasets.*`, no root counts.
        let result = serde_json::json!({
            "rows": 120,
            "datasets": {
                "formations": { "new": 10, "changed": 0, "unchanged": 110 },
                "velocity": { "new": 3, "changed": 1, "unchanged": 0 },
            }
        });
        let mut out = extract_yields(&result);
        out.sort_by(|a, b| a.dataset.cmp(&b.dataset));
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].dataset, "datasets.formations");
        assert_eq!(out[0].new, Some(10));
        assert_eq!(out[1].dataset, "datasets.velocity");
        assert_eq!(out[1].changed, Some(1));
    }

    #[test]
    fn root_and_nested_summaries_are_both_captured() {
        // homewyse/valuation shape: own dataset at the root PLUS a nested
        // `unified` cross-source summary — different datasets, both real.
        let result = serde_json::json!({
            "new": 4, "changed": 1, "unchanged": 40,
            "unified": { "new": 4, "changed": 1 },
        });
        let out = extract_yields(&result);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].dataset, "");
        assert_eq!(out[1].dataset, "unified");
        assert_eq!(
            out[1].unchanged, None,
            "unified reports no unchanged — stays None"
        );
    }

    #[test]
    fn array_valued_counts_use_length_and_strings_yield_nothing() {
        // Raw UpsertSummary serialization carries key LISTS; count by length.
        let raw = serde_json::json!({ "new": ["a", "b"], "changed": [], "unchanged": 7 });
        let out = extract_yields(&raw);
        assert_eq!(out[0].new, Some(2));
        assert_eq!(out[0].changed, Some(0));
        // cms-fee-schedule reports `change_since_last_run: "new"` — a string
        // VALUE under a different key. No numeric new/changed → no entry.
        let strings = serde_json::json!({ "change_since_last_run": "new", "release": "26A" });
        assert!(extract_yields(&strings).is_empty());
        // A string under the `new` key itself is unparseable → not a summary.
        assert!(extract_yields(&serde_json::json!({ "new": "yes" })).is_empty());
    }

    #[test]
    fn negatives_floats_and_non_objects_are_not_counts() {
        assert!(extract_yields(&serde_json::json!({ "new": -3 })).is_empty());
        assert!(extract_yields(&serde_json::json!({ "new": 1.5 })).is_empty());
        assert!(extract_yields(&serde_json::json!(null)).is_empty());
        assert!(extract_yields(&serde_json::json!([{"new": 1}])).is_empty());
        assert!(extract_yields(&serde_json::json!("new")).is_empty());
    }

    #[test]
    fn entry_count_and_depth_are_bounded() {
        // 100 summary-shaped children must not become 100 rows.
        let mut children = serde_json::Map::new();
        for i in 0..100 {
            children.insert(
                format!("d{i:03}"),
                serde_json::json!({ "new": 1, "changed": 0 }),
            );
        }
        let out = extract_yields(&serde_json::Value::Object(children));
        assert_eq!(out.len(), 16);
        // A summary buried past the depth cap is record data, not telemetry.
        let deep =
            serde_json::json!({ "a": { "b": { "c": { "d": { "new": 1, "changed": 0 } } } } });
        assert!(extract_yields(&deep).is_empty());
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
