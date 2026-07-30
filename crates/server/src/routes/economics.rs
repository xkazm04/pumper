//! Information economics (M04): `GET /economics` — what the spend actually
//! buys, per app, over trailing windows.
//!
//! Joins the cost ledger (`cost_events`, what each run spent, by engine tier)
//! with per-job yield (`job_yield`, the new/changed/unchanged counts the worker
//! parses out of every completed result) into $/new-record and $/changed-record
//! per app over 7d and 30d windows, a per-app Claude-tier "was the escalation
//! worth it" score, and an ADVISORY planner block recommending a `budget_usd`
//! and cadence direction per app.
//!
//! Advisory only. `[economics] enforce` is a deferred seam (the scheduler
//! reading planner budgets for scheduled runs); nothing acts on this payload.
//! Division-by-zero discipline throughout: an unknown or zero denominator is
//! JSON `null` — never `$0`, never infinity — because "we don't know what a
//! record costs here" is itself the signal.

use std::collections::BTreeMap;

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::routes::error::ApiError;
use crate::state::AppState;

/// The trailing windows reported: a recent slice and a baseline. The advice
/// heuristic compares fresh-record *rates* between exactly these two.
const WINDOWS: &[(&str, i64)] = &[("7d", 7), ("30d", 30)];

/// Engine tier name real money flows through (see `FetchTier::Claude.as_str()`).
const CLAUDE_ENGINE: &str = "claude";

/// One app's window aggregate: ledger side (cost, calls, the Claude share) plus
/// yield side (counts, per-dataset breakdown).
#[derive(Debug, Default, Clone)]
struct AppWindow {
    /// Distinct jobs that reported yield, taken as the max across this app's
    /// (app, dataset) groups — exact when every job reports each of its
    /// datasets, a lower bound otherwise.
    jobs_with_yield: i64,
    calls: i64,
    cost_usd: f64,
    claude_calls: i64,
    claude_cost_usd: f64,
    new: Option<i64>,
    changed: Option<i64>,
    unchanged: Option<i64>,
    /// Per-dataset yield rows (counts only: the ledger attributes cost to apps,
    /// not datasets, and prorating would fabricate precision).
    datasets: Vec<Value>,
}

/// `None`-preserving addition: unknown + unknown stays unknown; a known count
/// treats the unknown side as contributing nothing (not as zeroing the total).
fn opt_add(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (None, None) => None,
        (a, b) => Some(a.unwrap_or(0) + b.unwrap_or(0)),
    }
}

/// $/record. `None` (JSON null) when the count is unknown or zero — a price per
/// record that nobody produced is not $0 and not infinity, it is undefined.
fn per_record(cost_usd: f64, count: Option<i64>) -> Option<f64> {
    match count {
        Some(n) if n > 0 && cost_usd.is_finite() && cost_usd >= 0.0 => Some(cost_usd / n as f64),
        _ => None,
    }
}

/// Weighted fresh records per dollar — the planner's value metric. `None` when
/// the spend is zero (free records have no per-dollar rate) or yield is unknown.
fn value_per_dollar(weight: f64, fresh: Option<i64>, cost_usd: f64) -> Option<f64> {
    match fresh {
        Some(f) if cost_usd > 0.0 && cost_usd.is_finite() => Some(weight * f as f64 / cost_usd),
        _ => None,
    }
}

/// Claude-tier worth-it score (step 5): fresh records produced per Claude
/// dollar, plus a coarse verdict. Both `null` when the app never escalated to
/// the paid tier in the window (nothing to judge) or when yield is unknown.
fn claude_worth(fresh: Option<i64>, claude_cost_usd: f64) -> (Value, Value) {
    if claude_cost_usd <= 0.0 || !claude_cost_usd.is_finite() {
        return (Value::Null, Value::Null);
    }
    match fresh {
        Some(f) => (json!(f as f64 / claude_cost_usd), json!(f > 0)),
        None => (Value::Null, Value::Null),
    }
}

/// A recommendable dollar figure: rounded to 4 decimals, and only if the result
/// is finite — a ledger corrupted into `f64::MAX` must surface as "no
/// recommendation", not as an absurd budget.
fn budget_round(v: f64) -> Option<f64> {
    let r = (v * 10_000.0).round() / 10_000.0;
    r.is_finite().then_some(r)
}

/// The advisory planner's output for one app.
#[derive(Debug, Clone, PartialEq)]
struct Advice {
    /// Recommended per-job `budget_usd`; `None` when there is no spend history
    /// to derive one from.
    budget_usd: Option<f64>,
    /// Cadence direction: `"increase"` | `"keep"` | `"decrease"`.
    cadence: &'static str,
    reason: String,
}

/// Deterministic advisory heuristic — pure arithmetic over the two windows, no
/// clock, no randomness (same inputs, same advice, testably).
///
/// - No yield telemetry: no recommendation beyond "keep" — a planner that
///   guesses without data is worse than none. Spend-without-telemetry is called
///   out explicitly (it's the instrumentation gap /economics exists to close).
/// - Zero fresh records over 30d with real spend: halve the observed per-job
///   budget and slow down — the money is buying `unchanged`.
/// - Yielding: budget = observed 30d cost/job × 1.2 headroom; cadence compares
///   the 7d fresh-record daily rate to the 30d baseline rate (≥1.5× → increase,
///   ≤0.5× → decrease, else keep). An exploration floor is deliberate: even
///   "decrease" never recommends zero — starving a source hides its yield.
fn advise(
    jobs_with_yield: i64,
    cost_30d: f64,
    fresh_7d: Option<i64>,
    fresh_30d: Option<i64>,
) -> Advice {
    let avg_cost_per_job = (jobs_with_yield > 0 && cost_30d.is_finite() && cost_30d >= 0.0)
        .then(|| cost_30d / jobs_with_yield as f64);
    match fresh_30d {
        None => Advice {
            budget_usd: None,
            cadence: "keep",
            reason: if cost_30d > 0.0 {
                "spend recorded but results report no yield counts — cannot price records; \
                 keep until instrumented"
                    .into()
            } else {
                "no activity in the last 30d".into()
            },
        },
        Some(0) => Advice {
            budget_usd: avg_cost_per_job
                .filter(|_| cost_30d > 0.0)
                .and_then(|avg| budget_round(avg * 0.5)),
            cadence: "decrease",
            reason: if cost_30d > 0.0 {
                "30d spend produced zero fresh records — halve the per-job budget and slow \
                 the cadence (never to zero: a starved source hides its yield)"
                    .into()
            } else {
                "runs complete but produce no fresh records — cadence likely faster than \
                 the source changes"
                    .into()
            },
        },
        Some(f30) => {
            let budget_usd = avg_cost_per_job.and_then(|avg| budget_round(avg * 1.2));
            let r30 = f30 as f64 / 30.0;
            let (cadence, reason) = match fresh_7d {
                Some(f7) => {
                    let r7 = f7 as f64 / 7.0;
                    if f7 > 0 && r7 >= 1.5 * r30 {
                        (
                            "increase",
                            format!(
                                "recent fresh-record rate {r7:.2}/day is ≥1.5× the 30d \
                                 baseline {r30:.2}/day — source is hot"
                            ),
                        )
                    } else if r7 <= 0.5 * r30 {
                        (
                            "decrease",
                            format!(
                                "recent fresh-record rate {r7:.2}/day is ≤0.5× the 30d \
                                 baseline {r30:.2}/day — source has cooled"
                            ),
                        )
                    } else {
                        (
                            "keep",
                            format!(
                                "recent fresh-record rate {r7:.2}/day tracks the 30d \
                                 baseline {r30:.2}/day"
                            ),
                        )
                    }
                }
                None => (
                    "keep",
                    "yielding over 30d but no 7d yield telemetry to compare against".into(),
                ),
            };
            Advice {
                budget_usd,
                cadence,
                reason,
            }
        }
    }
}

/// One trailing window's per-app aggregates: cost summary (grouped app×engine)
/// merged with the yield rollup (grouped app×dataset). The map's key set is the
/// UNION — an app with spend but no yield rows stays visible (that's the
/// zero-yield spender the planner most wants to flag), as does yield recorded
/// against an all-free run.
async fn window(state: &AppState, days: i64) -> Result<BTreeMap<String, AppWindow>, ApiError> {
    let since = chrono::Utc::now() - chrono::Duration::days(days);
    let costs = state.costs.summary(None, Some(since)).await?;
    let yields = state.storage.yield_summary(since).await?;

    let mut apps: BTreeMap<String, AppWindow> = BTreeMap::new();
    for c in costs {
        let e = apps.entry(c.app.clone()).or_default();
        e.calls += c.calls;
        e.cost_usd += c.cost_usd;
        if c.engine == CLAUDE_ENGINE {
            e.claude_calls += c.calls;
            e.claude_cost_usd += c.cost_usd;
        }
    }
    for y in yields {
        let e = apps.entry(y.app.clone()).or_default();
        e.jobs_with_yield = e.jobs_with_yield.max(y.jobs);
        e.new = opt_add(e.new, y.new);
        e.changed = opt_add(e.changed, y.changed);
        e.unchanged = opt_add(e.unchanged, y.unchanged);
        e.datasets.push(json!({
            "dataset": y.dataset,
            "jobs": y.jobs,
            "new": y.new,
            "changed": y.changed,
            "unchanged": y.unchanged,
            "removed": y.removed,
        }));
    }
    Ok(apps)
}

/// One app's JSON row for a window.
fn app_json(app: &str, w: &AppWindow, weight: f64) -> Value {
    let fresh = opt_add(w.new, w.changed);
    let (claude_records_per_dollar, claude_worth_it) = claude_worth(fresh, w.claude_cost_usd);
    json!({
        "app": app,
        "weight": weight,
        "jobs_with_yield": w.jobs_with_yield,
        "engine_calls": w.calls,
        "cost_usd": w.cost_usd,
        "new": w.new,
        "changed": w.changed,
        "unchanged": w.unchanged,
        "cost_per_new_usd": per_record(w.cost_usd, w.new),
        "cost_per_changed_usd": per_record(w.cost_usd, w.changed),
        "weighted_fresh_per_dollar": value_per_dollar(weight, fresh, w.cost_usd),
        "claude": {
            "cost_usd": w.claude_cost_usd,
            "calls": w.claude_calls,
            "records_per_dollar": claude_records_per_dollar,
            "worth_it": claude_worth_it,
        },
        "datasets": w.datasets,
    })
}

/// The information-economics report: cost × yield per app over trailing 7d/30d
/// windows, the Claude-tier worth-it score, and the ADVISORY planner block.
/// Unknown or zero denominators are `null` throughout — never `$0`, never
/// infinity.
#[utoipa::path(
    get,
    path = "/economics",
    tag = "costs",
    responses((status = 200, description = "`{enforce, windows: {7d: {days, apps: [App]}, 30d: …}, advice: [{app, weight, recommended_budget_usd, cadence, reason}]}` — App = `{app, weight, jobs_with_yield, engine_calls, cost_usd, new, changed, unchanged, cost_per_new_usd, cost_per_changed_usd, weighted_fresh_per_dollar, claude: {cost_usd, calls, records_per_dollar, worth_it}, datasets}`. Advisory only; unknown/zero denominators are null."))
)]
pub(crate) async fn economics_report(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    debug_assert_eq!(
        WINDOWS.len(),
        2,
        "advice compares exactly recent vs baseline"
    );
    let recent = window(&state, WINDOWS[0].1).await?;
    let baseline = window(&state, WINDOWS[1].1).await?;

    let mut windows = serde_json::Map::new();
    for ((label, days), apps) in WINDOWS.iter().zip([&recent, &baseline]) {
        let rows: Vec<Value> = apps
            .iter()
            .map(|(app, w)| app_json(app, w, state.config.economics.weight(app)))
            .collect();
        windows.insert(label.to_string(), json!({ "days": days, "apps": rows }));
    }

    // Advice over the baseline window's apps (a superset of the recent one for
    // any app active at all in the last week).
    let advice: Vec<Value> = baseline
        .iter()
        .map(|(app, w30)| {
            let fresh_7d = recent.get(app).and_then(|w| opt_add(w.new, w.changed));
            let fresh_30d = opt_add(w30.new, w30.changed);
            let a = advise(w30.jobs_with_yield, w30.cost_usd, fresh_7d, fresh_30d);
            json!({
                "app": app,
                "weight": state.config.economics.weight(app),
                "recommended_budget_usd": a.budget_usd,
                "cadence": a.cadence,
                "reason": a.reason,
            })
        })
        .collect();

    Ok(Json(json!({
        // The deferred enforcement seam, surfaced so a dashboard can show
        // whether this report is advisory (today: always) or acted on.
        "enforce": state.config.economics.enforce,
        "windows": windows,
        "advice": advice,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── economics math: unknown/zero denominators are None, never 0 or ∞ ──

    #[test]
    fn per_record_divides_and_refuses_zero_or_unknown_counts() {
        assert_eq!(per_record(4.0, Some(8)), Some(0.5));
        assert_eq!(
            per_record(0.0, Some(8)),
            Some(0.0),
            "free records genuinely cost $0"
        );
        assert_eq!(
            per_record(4.0, Some(0)),
            None,
            "zero records: undefined, not infinity"
        );
        assert_eq!(per_record(4.0, None), None, "unknown count: unknown price");
        assert_eq!(per_record(f64::NAN, Some(8)), None);
        assert_eq!(per_record(-1.0, Some(8)), None);
    }

    #[test]
    fn opt_add_keeps_unknown_unknown() {
        assert_eq!(opt_add(None, None), None);
        assert_eq!(opt_add(Some(2), None), Some(2));
        assert_eq!(opt_add(None, Some(3)), Some(3));
        assert_eq!(opt_add(Some(2), Some(3)), Some(5));
    }

    #[test]
    fn value_per_dollar_needs_spend_and_yield() {
        assert_eq!(value_per_dollar(1.0, Some(10), 2.0), Some(5.0));
        assert_eq!(
            value_per_dollar(2.0, Some(10), 2.0),
            Some(10.0),
            "weight scales value"
        );
        assert_eq!(
            value_per_dollar(1.0, Some(10), 0.0),
            None,
            "no spend: no per-dollar rate"
        );
        assert_eq!(value_per_dollar(1.0, None, 2.0), None);
    }

    #[test]
    fn claude_worth_is_null_without_claude_spend_or_yield() {
        assert_eq!(claude_worth(Some(10), 0.0), (Value::Null, Value::Null));
        assert_eq!(claude_worth(None, 2.0), (Value::Null, Value::Null));
        let (rate, worth) = claude_worth(Some(10), 2.0);
        assert_eq!(rate, json!(5.0));
        assert_eq!(worth, json!(true));
        let (rate, worth) = claude_worth(Some(0), 2.0);
        assert_eq!(rate, json!(0.0));
        assert_eq!(worth, json!(false), "paid escalations that bought nothing");
    }

    // ── advice heuristic: deterministic, every branch ──

    #[test]
    fn advise_is_deterministic() {
        let a = advise(10, 5.0, Some(21), Some(30));
        let b = advise(10, 5.0, Some(21), Some(30));
        assert_eq!(a, b);
    }

    #[test]
    fn no_telemetry_means_no_recommendation() {
        let a = advise(0, 0.0, None, None);
        assert_eq!((a.budget_usd, a.cadence), (None, "keep"));
        // Spend with no yield counts is the instrumentation gap, called out.
        let a = advise(0, 3.0, None, None);
        assert_eq!((a.budget_usd, a.cadence), (None, "keep"));
        assert!(a.reason.contains("no yield counts"));
    }

    #[test]
    fn zero_yield_spender_halves_budget_and_slows() {
        // 10 jobs, $4 → $0.40/job observed, recommend half.
        let a = advise(10, 4.0, Some(0), Some(0));
        assert_eq!(a.cadence, "decrease");
        assert_eq!(a.budget_usd, Some(0.2));
        // Free zero-yielder: still slow down, but no budget to recommend.
        let a = advise(10, 0.0, Some(0), Some(0));
        assert_eq!((a.budget_usd, a.cadence), (None, "decrease"));
    }

    #[test]
    fn hot_source_increases_and_cooled_source_decreases() {
        // 30d: 30 fresh (1/day). 7d: 21 fresh (3/day) → ≥1.5× → increase.
        let a = advise(10, 5.0, Some(21), Some(30));
        assert_eq!(a.cadence, "increase");
        assert_eq!(a.budget_usd, Some(0.6), "cost/job 0.5 × 1.2 headroom");
        // 7d: 1 fresh (0.14/day) vs 1/day baseline → ≤0.5× → decrease.
        let a = advise(10, 5.0, Some(1), Some(30));
        assert_eq!(a.cadence, "decrease");
        // 7d: 7 fresh (1/day) tracks baseline → keep.
        let a = advise(10, 5.0, Some(7), Some(30));
        assert_eq!(a.cadence, "keep");
        // Yielding but no recent telemetry → keep, honestly worded.
        let a = advise(10, 5.0, None, Some(30));
        assert_eq!(a.cadence, "keep");
        assert!(a.reason.contains("no 7d"));
    }

    #[test]
    fn recommended_budgets_are_always_finite() {
        for &(jobs, cost, f7, f30) in &[
            (1i64, f64::MAX, Some(1i64), Some(1i64)),
            (1, f64::INFINITY, Some(0), Some(0)),
            (0, 5.0, Some(0), Some(0)),
        ] {
            if let Some(b) = advise(jobs, cost, f7, f30).budget_usd {
                assert!(b.is_finite(), "non-finite budget from ({jobs}, {cost})");
            }
        }
    }
}
