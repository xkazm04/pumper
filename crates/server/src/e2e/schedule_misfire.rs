//! `misfire_policy = "skip"` must eat only what was genuinely missed.
//!
//! Three ways it used to eat more than that, all driven here against the real
//! reconcile pass:
//!
//! 1. **The shared tick.** Classification ran over the whole pending batch from
//!    its OLDEST member, so an hourly schedule that missed 11:00 while the
//!    process was down and came back at 12:00:05 skipped BOTH firings — the
//!    12:00 one the process was up and due for included.
//! 2. **No gate parity.** The `Skip` branch never consulted the registry or the
//!    params schema, so a schedule pointing at an unregistered app accrued
//!    `skipped_count` forever while `GET /schedules` reported
//!    `health: "unregistered_app"` — a count of dropped work for work that could
//!    never have run.
//! 3. **The stale snapshot.** A pass reads the whole table once; a disable
//!    landing between that read and this row's enqueue was invisible, so the
//!    tick fired a schedule the operator had already stopped and stamped
//!    `last_run` on it.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use pumper_core::NewSchedule;
use serde_json::json;

use super::harness::{test_state, FakeApp};
use crate::scheduler;
use crate::state::AppState;

/// An hourly schedule for `app`, with `created_at` placed explicitly so both the
/// backlog and the observation instant are deterministic.
async fn hourly(
    state: &AppState,
    app: &str,
    policy: &str,
    created_at: chrono::DateTime<Utc>,
) -> String {
    let schedule = state
        .storage
        .create_schedule(NewSchedule {
            app,
            cron: "0 0 * * * *",
            params: json!({}),
            priority: 0,
            timezone: None,
            misfire_policy: policy,
            max_attempts: Some(1),
            budget_usd: None,
        })
        .await
        .expect("create schedule");
    sqlx::query("UPDATE schedules SET created_at = ?1 WHERE id = ?2")
        .bind(created_at.to_rfc3339())
        .bind(&schedule.id)
        .execute(&state.storage.pool())
        .await
        .expect("backdate schedule");
    schedule.id
}

async fn jobs_for_schedule(state: &AppState, id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE schedule_id = ?1")
        .bind(id)
        .fetch_one(&state.storage.pool())
        .await
        .expect("count jobs")
}

/// The load-bearing invariant, end to end: an on-time firing runs under `skip`
/// even when it shares a tick with an older missed one. Both facts land on the
/// row in the same pass — the backlog is advanced past AND the due run happened.
#[tokio::test]
async fn a_skip_schedule_runs_the_on_time_firing_that_shares_the_tick() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    // Last accounted-for point 10:00; 11:00 missed while down; back at 12:00:05.
    // Default tick is 15s, so the boot grace floor is 30s: 12:00:00 is on-time,
    // 11:00 is not.
    let created = Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 5).unwrap();
    let id = hourly(&state, "fake", "skip", created).await;

    let tally = scheduler::reconcile(&state, &mut HashMap::new(), None, now)
        .await
        .expect("pass runs");
    assert_eq!(tally.fired, 1, "the due 12:00 firing must run: {tally:?}");
    assert_eq!(
        tally.skipped, 1,
        "and 11:00 must be advanced past: {tally:?}"
    );

    assert_eq!(
        jobs_for_schedule(&state, &id).await,
        1,
        "exactly the on-time firing was enqueued"
    );
    let row = state
        .storage
        .get_schedule(&id)
        .await
        .unwrap()
        .expect("schedule row");
    assert_eq!(row.last_run, Some(now), "the run that happened is recorded");
    assert_eq!(
        row.skipped_count, 1,
        "one firing was genuinely missed — not two"
    );
    assert_eq!(row.last_skipped_at, Some(now));
}

/// Gate parity, the case with teeth: a `skip` schedule on an app that isn't
/// registered. Nothing it points at could ever have run, so nothing may be
/// recorded as eaten — and, exactly like the fire path, fixing the row must make
/// it fire rather than leaving it having silently advanced past its own backlog.
#[tokio::test]
async fn an_unregistered_app_does_not_accrue_skips_it_could_never_have_run() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let created = Utc.with_ymd_and_hms(2026, 7, 13, 7, 30, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 30, 0).unwrap();
    let id = hourly(&state, "ghost", "skip", created).await;

    let tally = scheduler::reconcile(&state, &mut HashMap::new(), None, now)
        .await
        .expect("pass runs");
    assert_eq!(
        tally.refused, 1,
        "the row is refused, not skipped: {tally:?}"
    );
    assert_eq!(tally.skipped, 0);

    let row = state
        .storage
        .get_schedule(&id)
        .await
        .unwrap()
        .expect("schedule row");
    assert_eq!(
        row.skipped_count, 0,
        "a schedule whose app does not exist has eaten nothing"
    );
    assert!(
        row.last_skipped_at.is_none(),
        "and nothing advanced, so pointing it at a real app makes it fire"
    );

    // Point it at the registered app: the SAME backlog is now genuinely its own
    // to skip, which is the contract the fire path already kept.
    sqlx::query("UPDATE schedules SET app = 'fake' WHERE id = ?1")
        .bind(&id)
        .execute(&state.storage.pool())
        .await
        .unwrap();
    let tally = scheduler::reconcile(&state, &mut HashMap::new(), None, now)
        .await
        .expect("pass runs");
    assert_eq!(tally.skipped, 1, "{tally:?}");
    let row = state.storage.get_schedule(&id).await.unwrap().unwrap();
    assert_eq!(
        row.skipped_count, 5,
        "the backlog was still there to advance past once the row was fixed"
    );
}

/// The disable race, at the only layer that can reach it: the pass holds a
/// snapshot, and the row is turned off before the step enqueues. This is the
/// governance/API path — `POST /schedules/{id}/enabled {false}`, a catalog
/// reconcile, a DataHub deprecation — racing a tick that is already walking the
/// table. Firing anyway spends money on a stopped schedule.
#[tokio::test]
async fn a_disable_racing_the_pass_stops_the_enqueue_not_just_the_next_tick() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let created = Utc.with_ymd_and_hms(2026, 7, 13, 7, 30, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 30, 0).unwrap();
    let id = hourly(&state, "fake", "fire_once", created).await;

    // The snapshot this pass would be working from.
    let snapshot = state.storage.get_schedule(&id).await.unwrap().unwrap();
    assert!(snapshot.enabled);
    let cron = cron::Schedule::from_str("0 0 * * * *").unwrap();

    // ...and the disable lands after that read, before the step acts.
    assert!(state
        .storage
        .set_schedule_enabled(&id, false)
        .await
        .unwrap());

    let outcome =
        scheduler::reconcile_one(&state, &snapshot, &cron, now - Duration::minutes(1), now)
            .await
            .expect("the step runs");
    assert_eq!(
        outcome,
        scheduler::StepOutcome::Refused,
        "a schedule disabled since the snapshot must not enqueue"
    );
    assert_eq!(jobs_for_schedule(&state, &id).await, 0);
    let row = state.storage.get_schedule(&id).await.unwrap().unwrap();
    assert!(
        row.last_run.is_none(),
        "and no `last_run` may claim the stopped schedule ran: {row:?}"
    );
}
