//! The scheduler **tick loop** itself — first coverage of `scheduler::run`.
//!
//! Every other scheduler test drives `reconcile` directly, which leaves the loop
//! around it untested. That loop is the process heartbeat: cron firing, the
//! stuck-job reaper, the webhook dead-letter drain, the cache refresher and the
//! DataHub governance poll all ride it, and it is spawned as one task. Three
//! properties therefore have to hold, and none of them could be observed before:
//!
//! 1. a tick really does fire a due schedule (the loop is wired, not just
//!    `reconcile`);
//! 2. an unwind INSIDE a tick is contained — the loop survives it and the other
//!    schedules of the same pass still get their turn;
//! 3. the loop exits on the shutdown token, which is what `main`'s new join is
//!    allowed to wait on.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use pumper_core::{AppContext, NewSchedule, Result, ScrapeApp};
use serde_json::{json, Value};

use super::harness::{test_state_with, wait_for, FakeApp, SchedulerLoop};
use crate::state::AppState;

/// An app that unwinds where the scheduler reaches it INLINE: `default_params`
/// is called on the fire path through `validate_schedule_params`, inside the
/// tick's own task. Before containment this panic killed the scheduler task
/// outright — cron, the reaper and the DLQ drain with it — while the HTTP
/// server kept answering.
///
/// Named to sort BEFORE `fake`: `list_schedules` returns rows `ORDER BY app`,
/// so the healthy schedule is reached only if the pass survived this one.
struct PanicsInDefaultParams;

#[async_trait::async_trait]
impl ScrapeApp for PanicsInDefaultParams {
    fn name(&self) -> &'static str {
        "boom"
    }
    fn default_params(&self) -> Value {
        panic!("default_params exploded");
    }
    async fn run(&self, _ctx: AppContext) -> Result<Value> {
        Ok(json!({}))
    }
}

/// A schedule that is already overdue: every-second cron, `created_at` backdated
/// so the very first pass finds it due.
async fn due_schedule(state: &AppState, app: &str) -> String {
    let schedule = state
        .storage
        .create_schedule(NewSchedule {
            app,
            cron: "* * * * * *",
            params: json!({}),
            priority: 0,
            timezone: None,
            misfire_policy: "fire_once",
            max_attempts: Some(1),
            budget_usd: None,
        })
        .await
        .expect("create schedule");
    sqlx::query("UPDATE schedules SET created_at = ?1 WHERE id = ?2")
        .bind((Utc::now() - chrono::Duration::minutes(5)).to_rfc3339())
        .bind(&schedule.id)
        .execute(&state.storage.pool())
        .await
        .expect("backdate schedule");
    schedule.id
}

async fn jobs_for(state: &AppState, app: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE app = ?1")
        .bind(app)
        .fetch_one(&state.storage.pool())
        .await
        .expect("count jobs")
}

/// The loop is wired end to end: spawn the real `run`, and a due schedule turns
/// into a job with nobody calling `reconcile` by hand. Then the token stops it.
#[tokio::test]
async fn the_real_tick_loop_fires_a_due_schedule_and_exits_on_the_token() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.worker.schedule_tick_secs = 1;
    })
    .await;
    due_schedule(&state, "fake").await;

    let loop_ = SchedulerLoop::start(&state);
    wait_for(
        "the tick to fire the due schedule",
        Duration::from_secs(10),
        || async { jobs_for(&state, "fake").await > 0 },
    )
    .await;

    // Exiting on the token is what `main`'s join is allowed to wait on.
    loop_.shutdown(&state, Duration::from_secs(10)).await;
}

/// Containment, at the level that matters: the panicking schedule is stepped
/// FIRST (rows come back `ORDER BY app`, and `boom` < `fake`), so before
/// per-schedule containment the pass died on it and `fake` never fired — for the
/// rest of the process's life, because the whole task was gone.
#[tokio::test]
async fn a_panicking_schedule_does_not_kill_the_tick_or_starve_the_rest() {
    let (state, _store) = test_state_with(
        vec![Arc::new(PanicsInDefaultParams), Arc::new(FakeApp)],
        |c| {
            c.worker.schedule_tick_secs = 1;
        },
    )
    .await;
    due_schedule(&state, "boom").await;
    due_schedule(&state, "fake").await;

    let loop_ = SchedulerLoop::start(&state);
    wait_for(
        "the healthy schedule to fire despite the panicking one ahead of it",
        Duration::from_secs(10),
        || async { jobs_for(&state, "fake").await > 0 },
    )
    .await;
    assert_eq!(
        jobs_for(&state, "boom").await,
        0,
        "the panicking schedule must not have enqueued anything"
    );

    // And the loop is still alive afterwards: it stops on the token rather than
    // having already died on the unwind.
    loop_.shutdown(&state, Duration::from_secs(10)).await;
}

/// The shutdown check inside the reconcile loop: once the token is already
/// fired, a pass enqueues nothing at all — a job created into a draining queue
/// would come back on the next boot with a `last_run` claiming it had run.
#[tokio::test]
async fn a_cancelled_token_stops_the_pass_before_it_enqueues() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.worker.schedule_tick_secs = 1;
    })
    .await;
    due_schedule(&state, "fake").await;

    state.shutdown.cancel();
    let mut cron_cache = std::collections::HashMap::new();
    let tally = crate::scheduler::reconcile(&state, &mut cron_cache, None, Utc::now())
        .await
        .expect("pass reads the table");

    assert!(tally.stopped_early, "the pass must report stopping early");
    assert_eq!(tally.fired, 0);
    assert_eq!(
        jobs_for(&state, "fake").await,
        0,
        "no scheduled work may be enqueued once shutdown is signalled"
    );
}
