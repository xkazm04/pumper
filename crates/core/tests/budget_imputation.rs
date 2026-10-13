//! The per-job budget ceiling, charged for the calls it could not price.
//!
//! Migration 0045 taught `cost_events` to say whether money actually moved.
//! Four READ surfaces learned to repeat it. The one surface that *acts* on
//! money — the per-job ceiling — still summed `cost_usd` and only `cost_usd`,
//! so a Claude run killed mid-generation with an unreadable bill contributed
//! exactly `$0.00` to its own ceiling. The run that spent the most was the run
//! the ceiling could not see, and the Claude tier is the only tier that spends
//! real money AND the only tier that can produce an `unmetered`/`unknown` row.
//!
//! Registry: `llm-observability/usage-limit-governance/cost-evidence-and-imputation`
//! — charge each unpriceable call the mean of the *priced* calls in the same
//! window, hold the estimate in the EVALUATION and never in the stored row,
//! and disclose the imputed share beside the current value. Its degenerate
//! case is the sharper half, and it is the policy this file pins: a window with
//! uncertain calls and **no** billed call has nothing to impute from, and an
//! enforcing cap must REFUSE there rather than present the platform's
//! blindness as the job's headroom.

use std::sync::Arc;

use pumper_core::config::BudgetImputation;
use pumper_core::costs::{
    budget_verdict, impute_uncertain, BudgetVerdict, CostLedger, JobSpend, SpendCertainty,
    SpentTotal,
};
use pumper_core::testing::{engines_with, Dead, ScriptedResearcher, TempStore, TestContext};
use pumper_core::{Error, ResearchRequest};
use uuid::Uuid;

const ON: BudgetImputation = BudgetImputation::WindowMean;
const OFF: BudgetImputation = BudgetImputation::Off;

fn job() -> Uuid {
    Uuid::new_v4()
}

/// A context whose researcher ANSWERS, so "admitted" and "refused" are told
/// apart by whether the engine was reached rather than by which error came
/// back. [`Dead`] panics on any call, which cannot express "this job still
/// runs".
fn scripted(
    store: &TempStore,
    budget_usd: Option<f64>,
    engine: Arc<ScriptedResearcher>,
) -> pumper_core::AppContext {
    let mut b = TestContext::new(&store.storage, "demo").engines(engines_with(
        Arc::new(Dead),
        Arc::new(Dead),
        engine,
    ));
    if let Some(budget) = budget_usd {
        b = b.budget_usd(budget);
    }
    b.build()
}

/// Seeds a ledger with `(cost_usd, certainty)` rows and returns the job's
/// `JobSpend` as the ENFORCEMENT path reads it.
async fn seed(ledger: &CostLedger, job_id: Uuid, rows: &[(f64, SpendCertainty)]) -> JobSpend {
    for (cost, certainty) in rows {
        ledger
            .record(job_id, "demo", "claude", None, *cost, *certainty, None)
            .await
            .expect("record cost event");
    }
    ledger.job_spend(job_id).await.expect("job_spend")
}

// ── case 1 — the pure imputation ────────────────────────────────────────────

/// ACCEPTANCE 1. The mean of the priced calls, and `None` — never `0.0` —
/// when there is nothing priced to take a mean of.
#[test]
fn impute_is_the_mean_of_the_priced_calls_not_a_zero() {
    // Four billed calls averaging $0.10; two calls we could not price.
    assert_eq!(impute_uncertain(0.40, 4, 2), Some(0.20));
    // Nothing uncertain: nothing imputed. Not `None` — this job is fully priced.
    assert_eq!(impute_uncertain(0.40, 4, 0), Some(0.0));
    // The degenerate case: uncertain calls and no price to extrapolate from.
    // `None` is "unpriceable", and the caller must refuse rather than read it
    // as $0 of imputed charge (registry law `nullable-never-zero`).
    assert_eq!(impute_uncertain(0.0, 0, 3), None);
    // A fresh job is EMPTY, not unpriceable — this is the case that would
    // otherwise refuse every job on its first metered call.
    assert_eq!(impute_uncertain(0.0, 0, 0), Some(0.0));
}

/// A NaN/∞ billed sum must never reach the ceiling arithmetic: `budget - NaN`
/// compares false against every bound, so a poisoned sum is a ceiling that can
/// never trip. `SpentTotal::add` already refuses non-finite deltas; this is the
/// same refusal one layer up, for a sum read back out of SQLite.
#[test]
fn a_non_finite_billed_sum_is_unpriceable_not_a_poisoned_ceiling() {
    assert_eq!(impute_uncertain(f64::NAN, 4, 2), None);
    assert_eq!(impute_uncertain(f64::INFINITY, 4, 2), None);
    assert_eq!(impute_uncertain(f64::NEG_INFINITY, 4, 2), None);
    // A negative count is nonsense from a corrupt row, not a credit.
    assert_eq!(impute_uncertain(0.40, -4, 2), None);
    assert_eq!(impute_uncertain(0.40, 4, -2), Some(0.0));
}

// ── case 2 — the ceiling now charges the floor ──────────────────────────────

/// ACCEPTANCE 2. $1.00 budget, 4 billed rows summing $0.40, 2 `unmetered`
/// rows. Today's answer is $0.60 remaining. The imputed answer is $0.40:
/// $0.40 billed + $0.20 imputed at the job's own $0.10 mean.
#[tokio::test]
async fn the_ceiling_charges_the_calls_it_could_not_price() {
    let store = TempStore::new("budget-imputation-charges").await;
    let ledger = CostLedger::new(store.storage.pool());
    let job_id = job();
    let spend = seed(
        &ledger,
        job_id,
        &[
            (0.10, SpendCertainty::Billed),
            (0.10, SpendCertainty::Billed),
            (0.10, SpendCertainty::Billed),
            (0.10, SpendCertainty::Billed),
            (0.0, SpendCertainty::Unmetered),
            (0.0, SpendCertainty::Unmetered),
        ],
    )
    .await;

    assert_eq!(spend.billed_calls, 4, "four rows carried a readable price");
    assert_eq!(spend.uncertain_calls, 2, "two rows carried a floor");
    assert!((spend.billed_usd - 0.40).abs() < 1e-9);
    assert_eq!(spend.imputed_usd(ON), Some(0.20));

    let mut ctx = TestContext::new(&store.storage, "demo")
        .budget_usd(1.00)
        .build();
    ctx.job_id = job_id;
    ctx.spent_usd = Arc::new(SpentTotal::from_spend(spend, ON));

    let remaining = ctx
        .remaining_budget_usd()
        .await
        .expect("headroom")
        .expect("a budgeted job has a headroom");
    assert!(
        (remaining - 0.40).abs() < 1e-9,
        "headroom must charge the imputed $0.20, got {remaining}"
    );
    assert!(
        (remaining - 0.60).abs() > 1e-9,
        "$0.60 is the OLD blind answer — the two unmetered calls were free money"
    );
}

/// The in-memory mirror advances the two COUNTS as well as the dollars, so the
/// imputation works for the calls of the current attempt and not only for the
/// ledger-seeded ones. `meter_with_certainty` is the single seam.
#[tokio::test]
async fn a_floor_metered_in_this_attempt_counts_against_the_ceiling() {
    let store = TempStore::new("budget-imputation-live").await;
    let mut ctx = TestContext::new(&store.storage, "demo")
        .budget_usd(1.00)
        .build();
    ctx.spent_usd = Arc::new(SpentTotal::from_spend(JobSpend::default(), ON));

    ctx.meter_with_certainty("claude", None, 0.40, SpendCertainty::Billed, None)
        .await;
    assert_eq!(ctx.remaining_budget_usd().await.unwrap(), Some(0.60));

    // One killed run: the bill is a floor, and the ceiling charges the mean of
    // what it CAN price ($0.40 over one billed call).
    ctx.meter_with_certainty("claude", None, 0.0, SpendCertainty::Unmetered, None)
        .await;
    let remaining = ctx.remaining_budget_usd().await.unwrap().unwrap();
    assert!(
        (remaining - 0.20).abs() < 1e-9,
        "one unmetered call at the $0.40 mean leaves $0.20, got {remaining}"
    );

    // A free row is not a price sample: it must not dilute the mean toward $0,
    // which is how a job with a hundred free http fetches would otherwise read
    // its unreadable Claude bills as costing nothing.
    ctx.meter_with_certainty("http", None, 0.0, SpendCertainty::Free, None)
        .await;
    let after_free = ctx.remaining_budget_usd().await.unwrap().unwrap();
    assert!(
        (after_free - remaining).abs() < 1e-9,
        "a free call changed the imputed charge: {remaining} -> {after_free}"
    );
}

// ── case 3 — the unpriceable state refuses ──────────────────────────────────

/// ACCEPTANCE 3. A budgeted job whose ledger holds only floors has nothing to
/// impute from. It must REFUSE the next metered call, naming the unpriceable
/// state, rather than be admitted against a confident `$0` total.
#[tokio::test]
async fn an_unpriceable_job_refuses_instead_of_reading_blindness_as_headroom() {
    let store = TempStore::new("budget-imputation-unpriceable").await;
    let ledger = CostLedger::new(store.storage.pool());
    let job_id = job();
    let spend = seed(
        &ledger,
        job_id,
        &[
            (0.0, SpendCertainty::Unmetered),
            (0.0, SpendCertainty::Unknown),
        ],
    )
    .await;
    assert_eq!(spend.billed_calls, 0);
    assert_eq!(spend.uncertain_calls, 2);
    assert_eq!(spend.imputed_usd(ON), None, "nothing to impute from");

    let engine = Arc::new(ScriptedResearcher::new().always_text("expensive answer"));
    let mut ctx = scripted(&store, Some(1.00), engine.clone());
    ctx.job_id = job_id;
    ctx.spent_usd = Arc::new(SpentTotal::from_spend(spend, ON));

    // The REPORTED headroom is zero, never the whole budget.
    assert_eq!(
        ctx.remaining_budget_usd().await.unwrap(),
        Some(0.0),
        "an unpriceable job must not report its untouched budget as headroom"
    );

    let err = ctx
        .research(ResearchRequest::new("one more expensive question"))
        .await
        .expect_err("an unpriceable job must not reach the engine");
    let Error::BudgetExhausted(msg) = &err else {
        panic!("expected BudgetExhausted, got {err:?}");
    };
    assert!(
        msg.contains("unpriceable"),
        "the refusal must name the unpriceable state so the operator's fix is \
         'find why the bill is unreadable', never 'raise the budget': {msg}"
    );
    assert!(
        msg.contains('2'),
        "the refusal must say how many calls could not be priced: {msg}"
    );
    assert!(
        err.is_terminal_for_job(),
        "the refusal is deterministic — retrying re-reads the same ledger"
    );
    assert_eq!(
        engine.call_count(),
        0,
        "the refusal must land BEFORE the engine — a call that reaches the CLI          has already spent the money the cap exists to withhold"
    );
}

/// The other half of ACCEPTANCE 3: this tightening touches budgeted jobs only.
/// An UNBUDGETED job in the identical state still runs — there is no ceiling
/// to be blind about, so there is nothing to refuse.
#[tokio::test]
async fn an_unbudgeted_job_in_the_same_state_still_runs() {
    let store = TempStore::new("budget-imputation-unbudgeted").await;
    let engine = Arc::new(ScriptedResearcher::new().always_text("an answer"));
    let mut ctx = scripted(&store, None, engine.clone());
    ctx.spent_usd = Arc::new(SpentTotal::from_spend(
        JobSpend {
            billed_usd: 0.0,
            billed_calls: 0,
            uncertain_calls: 7,
        },
        ON,
    ));

    assert_eq!(
        ctx.remaining_budget_usd().await.unwrap(),
        None,
        "no ceiling means unlimited, not unpriceable"
    );
    ctx.research(ResearchRequest::new("an unbudgeted question"))
        .await
        .expect("an unbudgeted job must still run");
    assert_eq!(engine.call_count(), 1, "the engine was never reached");
}

// ── case 4 — the unlimited path stays free of the ledger ────────────────────

/// ACCEPTANCE 4. `budget_usd: None` is `None` headroom, no imputation, and NO
/// ledger read on the pre-flight path of every metered call.
///
/// The card asked for this as a SQL call count through the `store_instrument`
/// chokepoint; the cost ledger is not wired to that instrument (it owns no
/// `StoreOp` family), so this proves the stronger property directly: the pool
/// is CLOSED before the question is asked, so any query at all would error.
#[tokio::test]
async fn the_preflight_path_never_queries_the_ledger() {
    let store = TempStore::new("budget-imputation-noquery").await;
    let pool = store.storage.pool();
    let ledger = CostLedger::new(pool.clone());
    let job_id = job();
    let spend = seed(
        &ledger,
        job_id,
        &[
            (0.30, SpendCertainty::Billed),
            (0.0, SpendCertainty::Unmetered),
        ],
    )
    .await;

    let mut unlimited = TestContext::new(&store.storage, "demo").build();
    unlimited.spent_usd = Arc::new(SpentTotal::from_spend(spend, ON));
    let mut budgeted = TestContext::new(&store.storage, "demo")
        .budget_usd(1.00)
        .build();
    budgeted.spent_usd = Arc::new(SpentTotal::from_spend(spend, ON));

    pool.close().await;

    assert_eq!(
        unlimited.remaining_budget_usd().await.unwrap(),
        None,
        "an unbudgeted job reads nothing and answers 'unlimited'"
    );
    let remaining = budgeted.remaining_budget_usd().await.unwrap().unwrap();
    assert!(
        (remaining - 0.40).abs() < 1e-9,
        "the whole verdict comes from the in-memory mirror: {remaining}"
    );
}

// ── case 5 — `off` is byte-identical to today ───────────────────────────────

/// ACCEPTANCE 5. `[costs] budget_imputation = "off"` restores today's answers
/// exactly: the 1.00/4-billed/2-unmetered job reports $0.60 remaining, and the
/// unpriceable job is admitted against its $0 total. The knob IS the rollback,
/// and it is proven by the same table the tightening is proven by.
#[tokio::test]
async fn off_is_todays_blind_ceiling_exactly() {
    let store = TempStore::new("budget-imputation-off").await;
    let ledger = CostLedger::new(store.storage.pool());

    let priced = seed(
        &ledger,
        job(),
        &[
            (0.10, SpendCertainty::Billed),
            (0.10, SpendCertainty::Billed),
            (0.10, SpendCertainty::Billed),
            (0.10, SpendCertainty::Billed),
            (0.0, SpendCertainty::Unmetered),
            (0.0, SpendCertainty::Unmetered),
        ],
    )
    .await;
    assert_eq!(priced.imputed_usd(OFF), Some(0.0), "off imputes nothing");

    let mut ctx = TestContext::new(&store.storage, "demo")
        .budget_usd(1.00)
        .build();
    ctx.spent_usd = Arc::new(SpentTotal::from_spend(priced, OFF));
    let remaining = ctx.remaining_budget_usd().await.unwrap().unwrap();
    assert!(
        (remaining - 0.60).abs() < 1e-9,
        "off must return the pre-change $0.60: {remaining}"
    );

    let blind = seed(
        &ledger,
        job(),
        &[
            (0.0, SpendCertainty::Unmetered),
            (0.0, SpendCertainty::Unknown),
        ],
    )
    .await;
    let engine = Arc::new(ScriptedResearcher::new().always_text("admitted answer"));
    let mut ctx = scripted(&store, Some(1.00), engine.clone());
    ctx.spent_usd = Arc::new(SpentTotal::from_spend(blind, OFF));
    assert_eq!(
        ctx.remaining_budget_usd().await.unwrap(),
        Some(1.0),
        "off reads blindness as the full budget — which is exactly the defect, \
         kept reachable on purpose as the no-deploy rollback"
    );
    ctx.research(ResearchRequest::new("admitted under off"))
        .await
        .expect("off must admit the unpriceable job");
    assert_eq!(engine.call_count(), 1, "the engine was never reached");
}

/// The verdict is one closed vocabulary, and `Unlimited` outranks
/// `Unpriceable`: the refusal exists to protect a CEILING, so a job without
/// one can never reach it.
#[test]
fn the_verdict_vocabulary_puts_unlimited_above_unpriceable() {
    let blind = JobSpend {
        billed_usd: 0.0,
        billed_calls: 0,
        uncertain_calls: 3,
    };
    assert_eq!(budget_verdict(None, blind, ON), BudgetVerdict::Unlimited);
    assert_eq!(
        budget_verdict(Some(1.0), blind, ON),
        BudgetVerdict::Unpriceable { uncertain_calls: 3 }
    );
    assert_eq!(
        budget_verdict(Some(1.0), blind, OFF),
        BudgetVerdict::Headroom(1.0)
    );
    // A non-finite CEILING is not a ceiling (it is how `budget_headroom` has
    // always treated one), so it is unlimited rather than unpriceable.
    assert_eq!(
        budget_verdict(Some(f64::NAN), blind, ON),
        BudgetVerdict::Unlimited
    );
    // Overspend floors at zero rather than reporting negative headroom.
    assert_eq!(
        budget_verdict(
            Some(1.0),
            JobSpend {
                billed_usd: 5.0,
                billed_calls: 1,
                uncertain_calls: 0,
            },
            ON
        ),
        BudgetVerdict::Headroom(0.0)
    );
}

// ── the guard: `job_total` is untouched ─────────────────────────────────────

/// GUARD, declared before the build. `CostLedger::job_total` is a documented
/// contract (`crates/core/tests/migrations.rs` pins it as the plain
/// `SUM(cost_usd)`) and several callers still want exactly that number. The
/// certainty-aware read is a SECOND query beside it, never a reshaping of it.
#[tokio::test]
async fn job_spend_is_beside_job_total_not_a_reshaping_of_it() {
    let store = TempStore::new("budget-imputation-guard").await;
    let ledger = CostLedger::new(store.storage.pool());
    let job_id = job();
    let spend = seed(
        &ledger,
        job_id,
        &[
            (0.30, SpendCertainty::Billed),
            (0.0, SpendCertainty::Unmetered),
            (0.0, SpendCertainty::Unknown),
            (0.0, SpendCertainty::Free),
            (0.12, SpendCertainty::Billed),
        ],
    )
    .await;

    let total = ledger.job_total(job_id).await.expect("job_total");
    assert!(
        (total - 0.42).abs() < 1e-9,
        "job_total must still be the plain SUM(cost_usd): {total}"
    );
    assert!((spend.billed_usd - total).abs() < 1e-9, "same dollars");
    assert_eq!(spend.billed_calls, 2, "free is not a price sample");
    assert_eq!(spend.uncertain_calls, 2, "unmetered + unknown");
}
