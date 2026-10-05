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

/// Re-exported here because the ledger is where it is read; it is DECLARED in
/// `crate::error` beside `ClaudeSpend::ledger_event`, which is the one producer
/// of a non-trivial value and is compiled without the `storage` feature.
pub use crate::error::SpendCertainty;
use crate::{Error, Result};

/// How a **summed** total should be read, given how many of its rows carry a
/// floor instead of a price: `"exact"` or `"at_least"`.
///
/// Extracted and named because three surfaces need the same sentence
/// (`GET /jobs/{id}/costs`, `GET /costs`, `GET /economics`) and a hand-rolled
/// ternary at each one is how two of them end up disagreeing.
pub fn total_certainty(uncertain_calls: i64) -> &'static str {
    if uncertain_calls > 0 {
        "at_least"
    } else {
        "exact"
    }
}

/// The mirror of [`total_certainty`] for remaining budget headroom. Spend is a
/// lower bound, so the headroom derived from it is an **upper** bound: `"at_most"`.
pub fn headroom_certainty(uncertain_calls: i64) -> &'static str {
    if uncertain_calls > 0 {
        "at_most"
    } else {
        "exact"
    }
}

/// One metered engine call.
#[derive(Debug, Clone, Serialize)]
pub struct CostEvent {
    pub job_id: String,
    pub app: String,
    pub engine: String,
    pub url: Option<String>,
    pub cost_usd: f64,
    /// Whether `cost_usd` is the bill, a floor, or nothing at all (0045).
    pub spend_certainty: SpendCertainty,
    pub detail: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Aggregated spend for one (app, engine) pair.
#[derive(Debug, Clone, Serialize)]
pub struct CostSummary {
    pub app: String,
    pub engine: String,
    pub calls: i64,
    /// Sum of `cost_usd` — the **billed** money only. With
    /// `unmetered_calls + unknown_calls > 0` this is a floor, not a price.
    pub cost_usd: f64,
    /// Calls in this group that were billed and whose amount is unreadable.
    pub unmetered_calls: i64,
    /// Calls in this group that may or may not have been billed at all.
    pub unknown_calls: i64,
}

impl CostSummary {
    /// Calls whose `cost_usd` is a floor rather than a price.
    pub fn uncertain_calls(&self) -> i64 {
        self.unmetered_calls + self.unknown_calls
    }

    /// `"exact"` or `"at_least"` for this group's `cost_usd`.
    pub fn total_certainty(&self) -> &'static str {
        total_certainty(self.uncertain_calls())
    }
}

/// One job's ledger, split by what we actually know about the money — the
/// per-EVENT counterpart of [`CostSummary`], which is the per-GROUP one.
///
/// **The anti-pattern this replaces** was a single line —
/// `let total: f64 = events.iter().map(|e| e.cost_usd).sum();` — which treated
/// `$0`-because-free and `$0`-because-the-run-was-killed as the same number.
/// A job that burned to its ceiling and was killed reported
/// `{"total_usd": 0.0, "calls": 1}`, byte-identical to a job that served one
/// free cache hit, and `cost_per_fresh_record_usd` then divided that zero by
/// the record count and published a confident `$0.00` ROI that `GET /economics`
/// advises budgets from.
///
/// **Why it lives in core rather than in the route that first needed it.** It
/// began as a `pub(crate)` helper inside one route module, so the second
/// surface in the same crate — the job receipt — wrote its own sum and dropped
/// the certainty. Registry technique `measurement-honesty/lower-bound-disclosure`:
/// declare the bound at the source of truth, not at the render site; a bound
/// documented only in the component that draws it survives exactly until the
/// second consumer appears.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CostRollup {
    /// Sum of the money we can price. Never presented without its certainty.
    pub billed_usd: f64,
    /// Calls that WERE billed and whose amount is unreadable.
    pub unmetered_calls: usize,
    /// Calls where even "was this billed?" has no answer.
    pub unknown_calls: usize,
}

impl CostRollup {
    /// Calls whose `cost_usd` is a floor rather than a price.
    pub fn uncertain_calls(&self) -> i64 {
        (self.unmetered_calls + self.unknown_calls) as i64
    }

    /// `"exact"` or `"at_least"` — how `billed_usd` must be read.
    pub fn total_certainty(&self) -> &'static str {
        total_certainty(self.uncertain_calls())
    }
}

/// Splits a job's cost events into [`CostRollup`].
///
/// Takes an iterator rather than a slice so a caller that has already grouped
/// its events — the receipt's per-engine breakdown — can roll each group up
/// through this one function instead of folding `cost_usd` by hand.
pub fn roll_up<'a>(events: impl IntoIterator<Item = &'a CostEvent>) -> CostRollup {
    let mut out = CostRollup::default();
    for e in events {
        match e.spend_certainty {
            SpendCertainty::Unmetered => out.unmetered_calls += 1,
            SpendCertainty::Unknown => out.unknown_calls += 1,
            // `Billed` carries the real amount; `Free` carries a real zero.
            // Both are prices, so both are summable.
            SpendCertainty::Billed | SpendCertainty::Free => {}
        }
        out.billed_usd += e.cost_usd;
    }
    out
}

// ── the ceiling's view of a job's spend ─────────────────────────────────────

/// What one unpriceable call should be charged against a ceiling, given the
/// calls in the same window we COULD price: the mean of the priced ones, times
/// the number we could not price.
///
/// `None` is the degenerate case and it is the whole point of the function:
/// **uncertain calls with no priced call to extrapolate from**. There is no
/// honest number there, and returning `Some(0.0)` would hand the caller the
/// platform's blindness dressed up as the job's headroom. Registry law
/// `nullable-never-zero` (llm-observability/usage-limit-governance/
/// cost-evidence-and-imputation): a cap that cannot be measured is not a cap,
/// so an enforcing cap REFUSES in this state. See [`budget_verdict`].
///
/// `Some(0.0)` means something different and is the common case: there is
/// nothing uncertain to charge for. A fresh job with an empty ledger is
/// `Some(0.0)`, not unpriceable — reading an empty window as blind is how this
/// policy would otherwise refuse every job on its first metered call.
///
/// Non-finite and negative inputs are unpriceable rather than clamped: a
/// `budget - NaN` comparison is false against every bound, so a poisoned sum
/// is a ceiling that can never trip, and silently treating it as `0.0` would
/// restore exactly the hole this exists to close.
pub fn impute_uncertain(billed_usd: f64, billed_calls: i64, uncertain_calls: i64) -> Option<f64> {
    if billed_calls < 0 {
        return None;
    }
    if uncertain_calls <= 0 {
        return Some(0.0);
    }
    if !billed_usd.is_finite() || billed_usd < 0.0 || billed_calls == 0 {
        return None;
    }
    let mean = billed_usd / billed_calls as f64;
    let imputed = mean * uncertain_calls as f64;
    imputed.is_finite().then_some(imputed)
}

/// One job's spend as the **enforcement point** must read it: the money we can
/// price, how many calls priced it, and how many calls did not.
///
/// This is the third member of the family, and the three are deliberately
/// distinct rather than one widened struct:
///
/// | type | grain | who reads it |
/// | --- | --- | --- |
/// | [`CostSummary`] | one `(app, engine)` group | the ROI/fleet surfaces |
/// | [`CostRollup`] | one job's events, already loaded | the per-job REPORT surfaces |
/// | [`JobSpend`] | one job, as a single query | the per-job CEILING |
///
/// `CostRollup` splits `unmetered` from `unknown` because a reader needs to
/// know WHICH kind of silence it is looking at. The ceiling does not: both are
/// "a call whose bill we cannot read", and the one number it is missing is the
/// one `CostRollup` does not carry — `billed_calls`, the denominator of the
/// mean. Hence a sibling type, not a reshaped one.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct JobSpend {
    /// Sum of `cost_usd` over the rows that carry a price (`billed` + `free`).
    /// Identical to [`CostLedger::job_total`] — the ceiling's dollars have not
    /// changed, only what else it knows about them.
    pub billed_usd: f64,
    /// Rows whose amount the engine actually reported: `spend_certainty =
    /// 'billed'` only.
    ///
    /// **`free` is deliberately NOT counted here.** It is the denominator of
    /// the imputation mean, and a job doing a hundred free http fetches
    /// alongside two unreadable Claude bills would otherwise compute a mean of
    /// `$0.40 / 102` and charge its blind spots a third of a cent — re-opening
    /// the hole from inside the fix. A free call is a real `$0`, not a price
    /// sample for what a paid call costs.
    pub billed_calls: i64,
    /// Rows whose `cost_usd` is a floor: `unmetered` + `unknown`.
    pub uncertain_calls: i64,
}

impl JobSpend {
    /// The same triple from events already in hand, so a surface that has
    /// loaded the ledger does not issue a second query for the counts.
    pub fn from_events<'a>(events: impl IntoIterator<Item = &'a CostEvent>) -> Self {
        let mut out = Self::default();
        for e in events {
            match e.spend_certainty {
                SpendCertainty::Billed => out.billed_calls += 1,
                SpendCertainty::Unmetered | SpendCertainty::Unknown => out.uncertain_calls += 1,
                SpendCertainty::Free => {}
            }
            out.billed_usd += e.cost_usd;
        }
        out
    }

    /// USD this job's unpriceable calls are charged under `policy`. `None` =
    /// unpriceable (see [`impute_uncertain`]); `Off` always imputes nothing.
    pub fn imputed_usd(&self, policy: crate::config::BudgetImputation) -> Option<f64> {
        match policy {
            crate::config::BudgetImputation::Off => Some(0.0),
            crate::config::BudgetImputation::WindowMean => {
                impute_uncertain(self.billed_usd, self.billed_calls, self.uncertain_calls)
            }
        }
    }

    /// Everything charged against the ceiling: billed plus imputed. `None`
    /// when the imputation has nothing to work from.
    pub fn charged_usd(&self, policy: crate::config::BudgetImputation) -> Option<f64> {
        let imputed = self.imputed_usd(policy)?;
        let charged = self.billed_usd.max(0.0) + imputed;
        charged.is_finite().then_some(charged)
    }

    /// `"exact"` or `"at_least"` — how `billed_usd` must be read.
    pub fn total_certainty(&self) -> &'static str {
        total_certainty(self.uncertain_calls)
    }
}

/// A job's standing against its ceiling — one closed vocabulary, so the
/// enforcement point and every surface that reports it answer from the same
/// three cases instead of each re-deriving "is there room".
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BudgetVerdict {
    /// No ceiling (or a non-finite one, which has never been a ceiling). This
    /// outranks [`Unpriceable`](Self::Unpriceable): the refusal exists to
    /// protect a ceiling, so a job without one can never reach it.
    Unlimited,
    /// USD still spendable, floored at zero. `Headroom(0.0)` is exhausted.
    Headroom(f64),
    /// The job has calls we could not price and nothing priced to estimate
    /// from, so its spend has no upper OR lower bound anyone can act on. The
    /// operator's fix is "find why the bill is unreadable", never "raise the
    /// budget".
    Unpriceable { uncertain_calls: i64 },
}

/// The ceiling, as one pure function of a budget, a [`JobSpend`] and a policy.
///
/// Extracted rather than written inline in `AppContext::remaining_budget_usd`
/// because FOUR call sites need the same boundary — the hard refusal in
/// `research`, the tier downgrade in `fetch`, the reported headroom, and
/// `GET /jobs/{id}/costs` — and a boundary re-typed at four sites is a boundary
/// that ends up meaning four things.
pub fn budget_verdict(
    budget_usd: Option<f64>,
    spend: JobSpend,
    policy: crate::config::BudgetImputation,
) -> BudgetVerdict {
    let Some(budget) = budget_usd.filter(|b| b.is_finite()) else {
        return BudgetVerdict::Unlimited;
    };
    match spend.charged_usd(policy) {
        Some(charged) => BudgetVerdict::Headroom((budget - charged).max(0.0)),
        None => BudgetVerdict::Unpriceable {
            uncertain_calls: spend.uncertain_calls,
        },
    }
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
///
/// **It mirrors the ledger's CERTAINTY as well as its dollars.** It used to be
/// a single `f64`, which made the enforcement point structurally unable to see
/// the one column migration 0045 added: a Claude run killed mid-generation
/// advanced this total by `$0.00`, so the ceiling was blind exactly where the
/// only money-spending tier is. The two counters beside the dollars are what
/// [`budget_verdict`] needs, and the imputation policy travels with them so
/// every reader of this total answers from one configured policy.
#[derive(Debug, Default)]
pub struct SpentTotal {
    usd: std::sync::atomic::AtomicU64,
    billed_calls: std::sync::atomic::AtomicI64,
    uncertain_calls: std::sync::atomic::AtomicI64,
    imputation: crate::config::BudgetImputation,
}

impl SpentTotal {
    /// Seeds the dollars only, with no certainty counts — for callers that have
    /// nothing but a number. [`Self::from_spend`] is what the worker uses.
    pub fn new(seed_usd: f64) -> Self {
        Self::from_spend(
            JobSpend {
                billed_usd: seed_usd,
                ..JobSpend::default()
            },
            crate::config::BudgetImputation::default(),
        )
    }

    /// Seeds from one ledger read ([`CostLedger::job_spend`]) under the
    /// configured imputation policy — the worker's path, so a RETRIED job
    /// carries its prior attempts' unreadable bills as well as their dollars.
    pub fn from_spend(spend: JobSpend, imputation: crate::config::BudgetImputation) -> Self {
        Self {
            usd: std::sync::atomic::AtomicU64::new(spend.billed_usd.max(0.0).to_bits()),
            billed_calls: std::sync::atomic::AtomicI64::new(spend.billed_calls.max(0)),
            uncertain_calls: std::sync::atomic::AtomicI64::new(spend.uncertain_calls.max(0)),
            imputation,
        }
    }

    /// USD recorded against this job so far — the **billed** money only, as it
    /// always was. Read [`Self::spend`] for the number the ceiling acts on.
    pub fn get(&self) -> f64 {
        f64::from_bits(self.usd.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// The whole triple, as the enforcement point reads it.
    pub fn spend(&self) -> JobSpend {
        JobSpend {
            billed_usd: self.get(),
            billed_calls: self.billed_calls.load(std::sync::atomic::Ordering::Relaxed),
            uncertain_calls: self
                .uncertain_calls
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// This job's standing against `budget_usd` under the configured policy.
    pub fn verdict(&self, budget_usd: Option<f64>) -> BudgetVerdict {
        budget_verdict(budget_usd, self.spend(), self.imputation)
    }

    /// The configured imputation policy, for the surfaces that must disclose it.
    pub fn imputation(&self) -> crate::config::BudgetImputation {
        self.imputation
    }

    /// Adds a recorded cost. Non-positive and non-finite deltas are ignored —
    /// engine costs are `Option<f64>` defaulted to 0.0, and a NaN must never be
    /// able to poison a budget ceiling into never tripping.
    ///
    /// Records no certainty, so it cannot advance the counters the ceiling
    /// reads. [`Self::add_call`] is the metered seams' entry point.
    pub fn add(&self, delta_usd: f64) {
        if !delta_usd.is_finite() || delta_usd <= 0.0 {
            return;
        }
        let mut cur = self.usd.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(cur) + delta_usd).to_bits();
            match self.usd.compare_exchange_weak(
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

    /// [`Self::add`] plus the certainty of the row being recorded: the one seam
    /// that keeps this mirror and `cost_events` saying the same thing about a
    /// job's money. `AppContext::meter_with_certainty` is its only caller.
    pub fn add_call(&self, delta_usd: f64, certainty: SpendCertainty) {
        self.add(delta_usd);
        let counter = match certainty {
            // A reported `$0.00` IS a price, and it IS a sample of what this
            // job's calls cost — so it counts in the denominator.
            SpendCertainty::Billed => &self.billed_calls,
            SpendCertainty::Unmetered | SpendCertainty::Unknown => &self.uncertain_calls,
            // Real zero, no price information. See `JobSpend::billed_calls`.
            SpendCertainty::Free => return,
        };
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
    /// One argument per column, as the other row-shaped inserts in this crate
    /// are written (see `datasets.rs`): a parameter struct here would be a
    /// second place for the column list to drift from the `INSERT`.
    #[allow(clippy::too_many_arguments)]
    pub async fn record(
        &self,
        job_id: Uuid,
        app: &str,
        engine: &str,
        url: Option<&str>,
        cost_usd: f64,
        certainty: SpendCertainty,
        detail: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO cost_events \
             (job_id, app, engine, url, cost_usd, spend_certainty, detail, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(job_id.to_string())
        .bind(app)
        .bind(engine)
        .bind(url)
        .bind(cost_usd)
        .bind(certainty.as_str())
        .bind(detail)
        .bind(ts(Utc::now()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Everything one job spent, oldest first.
    pub async fn job_events(&self, job_id: Uuid) -> Result<Vec<CostEvent>> {
        let rows: Vec<CostEventRow> = sqlx::query_as(
            "SELECT job_id, app, engine, url, cost_usd, spend_certainty, detail, created_at \
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

    /// One job's spend as the **ceiling** must read it, in one query: the
    /// priced dollars, the count of calls that priced them, and the count of
    /// calls that could not.
    ///
    /// Beside [`Self::job_total`], never instead of it. `job_total` is a
    /// documented contract (pinned as the plain `SUM(cost_usd)` by
    /// `crates/core/tests/migrations.rs`) and several callers want exactly that
    /// number; this adds the two counts the enforcement point was missing
    /// without moving the dollars.
    pub async fn job_spend(&self, job_id: Uuid) -> Result<JobSpend> {
        let (billed_usd, billed_calls, uncertain_calls): (f64, i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(cost_usd), 0), \
             COALESCE(SUM(spend_certainty = 'billed'), 0), \
             COALESCE(SUM(spend_certainty IN ('unmetered', 'unknown')), 0) \
             FROM cost_events WHERE job_id = ?1",
        )
        .bind(job_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(JobSpend {
            billed_usd,
            billed_calls,
            uncertain_calls,
        })
    }

    /// Spend grouped by (app, engine), optionally filtered to one app and/or a
    /// time window — the ROI overview.
    pub async fn summary(
        &self,
        app: Option<&str>,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<CostSummary>> {
        self.summary_filtered(app, since, None).await
    }

    /// [`Self::summary`] with a certainty filter: `Some(c)` keeps only the
    /// (app, engine) groups holding **at least one** call of that certainty.
    ///
    /// The filter is a `HAVING`, not a `WHERE`, deliberately: an operator
    /// asking "where is my unmetered spend" wants the affected group's *whole*
    /// picture — its billed total alongside the calls that make that total a
    /// floor — not a synthetic group containing only the bad rows.
    pub async fn summary_filtered(
        &self,
        app: Option<&str>,
        since: Option<DateTime<Utc>>,
        certainty: Option<SpendCertainty>,
    ) -> Result<Vec<CostSummary>> {
        let rows: Vec<(String, String, i64, f64, i64, i64)> = sqlx::query_as(
            "SELECT app, engine, COUNT(*), COALESCE(SUM(cost_usd), 0), \
             COALESCE(SUM(spend_certainty = 'unmetered'), 0), \
             COALESCE(SUM(spend_certainty = 'unknown'), 0) \
             FROM cost_events \
             WHERE (?1 IS NULL OR app = ?1) AND (?2 IS NULL OR created_at > ?2) \
             GROUP BY app, engine \
             HAVING (?3 IS NULL OR SUM(spend_certainty = ?3) > 0) \
             ORDER BY app, engine",
        )
        .bind(app)
        .bind(since.map(ts))
        .bind(certainty.map(SpendCertainty::as_str))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(app, engine, calls, cost_usd, unmetered_calls, unknown_calls)| CostSummary {
                    app,
                    engine,
                    calls,
                    cost_usd,
                    unmetered_calls,
                    unknown_calls,
                },
            )
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
    spend_certainty: String,
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
            // A value outside the vocabulary degrades to `Unknown`, never to
            // `Free`: an unreadable certainty is exactly the case where
            // claiming "no money moved" is the expensive mistake.
            spend_certainty: SpendCertainty::parse(&r.spend_certainty)
                .unwrap_or(SpendCertainty::Unknown),
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
        .map_err(|e| Error::parse_from(format!("bad timestamp '{s}': {e}"), e))
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
