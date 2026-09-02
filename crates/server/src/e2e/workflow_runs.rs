//! N03 workflow runs, end to end over the real worker: a 3-step diamond (two
//! sources, one join).
//!
//! The three things this pins, each of which is the way a workflow engine goes
//! wrong in practice:
//!
//! 1. **The join fires exactly once.** Both upstreams complete, each one
//!    reaching `finalize_with_stages` and therefore `on_step_terminal`; only the
//!    completion that finds the barrier fully satisfied may enqueue, and only
//!    one of them may win the claim. The failure mode is a join that runs once
//!    per upstream — duplicate work, two receipts for one plan.
//! 2. **The receipt sums the run, not one job.** Cost comes from `cost_events`
//!    over the run's whole job set, so the plan has one bill.
//! 3. **Cancel leaves no orphan.** A run cancelled mid-flight closes every open
//!    step, and no queued step job survives to run afterwards.

use std::sync::Arc;

use serde_json::{json, Value};

use pumper_core::{AppContext, Result, ScrapeApp};

use super::harness::{test_state, FakeApp};
use crate::state::AppState;
use crate::{worker, workflow};

/// An app whose result carries `UpsertSummary`-shaped counts, so the run
/// receipt's yield rollup has something real to sum. `FakeApp`'s result does
/// not (it reports `synced`), and a rollup asserted against an app that reports
/// nothing would pass while summing zero rows.
struct YieldApp;

#[async_trait::async_trait]
impl ScrapeApp for YieldApp {
    fn name(&self) -> &'static str {
        "yielder"
    }
    fn description(&self) -> &'static str {
        "reports UpsertSummary-shaped counts"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let n = ctx.params.get("n").and_then(Value::as_i64).unwrap_or(1);
        Ok(json!({ "new": n, "changed": 0, "note": ctx.params.get("note") }))
    }
}

/// A diamond: `a` and `b` in parallel, `join` behind both. The join's params are
/// templated off BOTH upstream results, so a join that fired before the second
/// upstream landed could not even render.
fn diamond_spec() -> Value {
    json!({
        "steps": {
            "a": { "app": "yielder", "params": { "n": 2 } },
            "b": { "app": "yielder", "params": { "n": 3 } },
            "join": {
                "app": "yielder",
                "after": { "all_of": ["a", "b"] },
                "params": { "note": "a={{steps.a.result.new}} b={{steps.b.result.new}}" }
            }
        }
    })
}

/// Every step job of a run, as `(step, status)`.
async fn step_jobs(state: &AppState, run_id: &str) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT workflow_step, status FROM jobs WHERE workflow_run_id = ?1 ORDER BY workflow_step",
    )
    .bind(run_id)
    .fetch_all(&state.storage.pool())
    .await
    .unwrap();
    rows.sort();
    rows
}

async fn states(state: &AppState, run_id: &str) -> Vec<(String, String)> {
    state
        .storage
        .workflow_steps(run_id)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.step, r.status))
        .collect()
}

#[tokio::test]
async fn a_diamonds_join_runs_exactly_once_and_the_run_reports_one_receipt() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp), Arc::new(YieldApp)]).await;
    let def = state
        .storage
        .create_workflow("diamond", &diamond_spec(), None)
        .await
        .unwrap();

    let (run, created) = workflow::start_run(&state, &def, None, None, None)
        .await
        .unwrap();
    assert!(created);
    assert_eq!(
        states(&state, &run.id).await,
        vec![
            ("a".to_string(), "queued".to_string()),
            ("b".to_string(), "queued".to_string()),
            ("join".to_string(), "pending".to_string()),
        ],
        "opening a run enqueues its ROOT steps only — the join waits on its barrier"
    );

    // Run the first upstream. Its terminal event reaches the barrier, which is
    // NOT yet satisfied: the join must stay pending, with no job.
    assert!(worker::run_one(&state).await);
    let after_first = states(&state, &run.id).await;
    let join_state = after_first
        .iter()
        .find(|(s, _)| s == "join")
        .map(|(_, st)| st.as_str())
        .unwrap();
    assert_eq!(
        join_state, "pending",
        "the join fired on ONE upstream — this is the duplicate-work bug the \
         barrier exists to prevent: {after_first:?}"
    );

    // Second upstream: now the barrier is satisfied and the join is enqueued.
    assert!(worker::run_one(&state).await);
    let jobs = step_jobs(&state, &run.id).await;
    assert_eq!(
        jobs.iter().filter(|(s, _)| s == "join").count(),
        1,
        "exactly one join job, whichever upstream landed last: {jobs:?}"
    );

    // The join's params were rendered from both upstream results.
    let params: (String,) =
        sqlx::query_as("SELECT params FROM jobs WHERE workflow_run_id = ?1 AND workflow_step = ?2")
            .bind(&run.id)
            .bind("join")
            .fetch_one(&state.storage.pool())
            .await
            .unwrap();
    let params: Value = serde_json::from_str(&params.0).unwrap();
    assert_eq!(
        params["note"], "a=2 b=3",
        "the join's template read BOTH upstream results: {params}"
    );

    // Price the two upstreams, then run the join and read the rolled-up bill.
    for (step, cost) in [("a", 0.25), ("b", 0.75)] {
        let job: (String,) =
            sqlx::query_as("SELECT id FROM jobs WHERE workflow_run_id = ?1 AND workflow_step = ?2")
                .bind(&run.id)
                .bind(step)
                .fetch_one(&state.storage.pool())
                .await
                .unwrap();
        state
            .costs
            .record(
                job.0.parse().unwrap(),
                "yielder",
                "http",
                None,
                cost,
                Some("test"),
            )
            .await
            .unwrap();
    }

    assert!(worker::run_one(&state).await);
    assert!(
        !worker::run_one(&state).await,
        "no further work: a join that ran twice would show up here"
    );

    let run = state
        .storage
        .get_workflow_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "succeeded", "{:?}", run.error);

    let report = workflow::run_report(&state, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        report["receipt"]["cost_usd"].as_f64().unwrap(),
        1.0,
        "one receipt for the plan: both upstream costs summed, not one job's: {}",
        report["receipt"]
    );
    assert_eq!(report["receipt"]["steps_total"], 3);
    assert_eq!(report["receipt"]["steps_priced"], 3);
    assert_eq!(
        report["receipt"]["yield"]["new"], 6,
        "yield rolls up across the run's WHOLE job set (2 + 3 + 1), not one job's: {}",
        report["receipt"]
    );
    assert!(
        report["unknown"].as_array().unwrap().is_empty(),
        "nothing was unknowable about this run: {}",
        report["unknown"]
    );
}

/// A failing upstream must not leave the join `pending` forever with the run
/// stuck `running`. Under the default `fail_fast` the whole remainder is closed
/// as `skipped` and the run fails, naming what never ran.
#[tokio::test]
async fn a_failed_upstream_closes_the_run_rather_than_parking_the_join_forever() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let spec = json!({
        "steps": {
            "a": { "app": "fake", "params": { "fail": "boom" } },
            "join": { "app": "fake", "after": ["a"] }
        }
    });
    let def = state
        .storage
        .create_workflow("failing", &spec, None)
        .await
        .unwrap();
    let (run, _) = workflow::start_run(&state, &def, None, None, None)
        .await
        .unwrap();

    assert!(worker::run_one(&state).await);
    assert!(!worker::run_one(&state).await, "the join must never run");

    let run = state
        .storage
        .get_workflow_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "failed");
    let error = run.error.unwrap_or_default();
    assert!(error.contains("failed: a"), "{error}");
    assert!(
        error.contains("never ran: join"),
        "the verdict names the step that was skipped, not just the one that broke: {error}"
    );
    assert_eq!(
        states(&state, &run.id).await,
        vec![
            ("a".to_string(), "failed".to_string()),
            ("join".to_string(), "skipped".to_string()),
        ]
    );
}

/// Cancelling mid-run closes every open cell and leaves nothing claimable — the
/// orphan-step failure mode is a queued job that runs after the run is over.
#[tokio::test]
async fn cancelling_mid_run_leaves_no_orphan_step() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp), Arc::new(YieldApp)]).await;
    let def = state
        .storage
        .create_workflow("cancelme", &diamond_spec(), None)
        .await
        .unwrap();
    let (run, _) = workflow::start_run(&state, &def, None, None, None)
        .await
        .unwrap();

    // One upstream done, one still queued, the join still pending.
    assert!(worker::run_one(&state).await);

    let cancelled_jobs = workflow::cancel_run(&state, &run.id).await.unwrap();
    assert_eq!(cancelled_jobs, 1, "the one queued step job was cancelled");

    let run = state
        .storage
        .get_workflow_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "cancelled");
    assert!(
        state
            .storage
            .open_workflow_steps(&run.id)
            .await
            .unwrap()
            .is_empty(),
        "no step may still be open after a cancel: {:?}",
        states(&state, &run.id).await
    );
    assert!(
        !worker::run_one(&state).await,
        "an orphan step ran after its run was cancelled"
    );
}

/// The envelope is a hard ceiling on the SUM, not a per-step hint: a step
/// enqueued after the envelope is spent is refused rather than run uncapped.
#[tokio::test]
async fn an_exhausted_envelope_fails_the_downstream_step_rather_than_running_it_uncapped() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let spec = json!({
        "budget_usd": 0.5,
        "steps": {
            "a": { "app": "fake" },
            "b": { "app": "fake", "after": ["a"] }
        }
    });
    let def = state
        .storage
        .create_workflow("envelope", &spec, None)
        .await
        .unwrap();
    let (run, _) = workflow::start_run(&state, &def, None, None, None)
        .await
        .unwrap();

    // Spend the whole envelope on the first step before it finishes.
    let job: (String,) =
        sqlx::query_as("SELECT id FROM jobs WHERE workflow_run_id = ?1 AND workflow_step = 'a'")
            .bind(&run.id)
            .fetch_one(&state.storage.pool())
            .await
            .unwrap();
    state
        .costs
        .record(job.0.parse().unwrap(), "fake", "claude", None, 0.5, None)
        .await
        .unwrap();

    assert!(worker::run_one(&state).await);
    assert!(
        !worker::run_one(&state).await,
        "step b must never have been enqueued"
    );
    let cells = states(&state, &run.id).await;
    assert_eq!(
        cells,
        vec![
            ("a".to_string(), "succeeded".to_string()),
            ("b".to_string(), "failed".to_string()),
        ],
        "the starved step is recorded as failed, not silently dropped"
    );
    let b = state
        .storage
        .workflow_steps(&run.id)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.step == "b")
        .unwrap();
    assert!(
        b.error.unwrap_or_default().contains("envelope"),
        "the refusal says WHY the step never ran"
    );
}
