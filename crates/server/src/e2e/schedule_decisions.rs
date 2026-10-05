//! The schedule decision ledger, driven through the real reconcile pass and
//! the real router: a firing that did NOT happen must leave durable evidence
//! naming why, and a reconcile loop that has stopped completing passes must be
//! distinguishable from a loop with nothing to do.
//!
//! The four silences this pins, all of them real before migration 0047:
//!
//! 1. **The overlap hold.** The most common non-fire recorded nothing at all,
//!    by design — recording a skip there would move the cron reference past the
//!    very firing the guard just promised to keep due. So the ledger row is the
//!    only place it can be written, and writing it must not move the reference.
//! 2. **The misfire count, per occurrence.** `skipped_count` does accumulate
//!    `missed` -- the card's premise that it moves by +1 is FALSE, and the test
//!    below pins the true behaviour. What a lifetime total cannot answer is WHEN
//!    and WHY: four firings eaten in one tick and one eaten in each of four
//!    ticks are the same `4`.
//! 3. **The reason.** A retirement skip and a misfire skip wrote the identical
//!    two columns (`last_skipped_at`, `skipped_count`), so they were
//!    indistinguishable in the store.
//! 4. **The wedged loop.** `last_pass` was a loop-local, so a pass failing at
//!    `list_schedules` forever looked exactly like "nothing was due".
//!
//! The vocabulary is NOT new: `outcome` is `StepOutcome` (the scheduler's own
//! six verdicts plus the absorbed failure) and `reason` is
//! `ScheduleAdmission::health()` verbatim — the same closed list `GET /schedules`
//! publishes. The ledger adds no second authority on "why isn't this firing?".

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Duration, TimeZone, Utc};
use http_body_util::BodyExt;
use pumper_core::resilience::source_id;
use pumper_core::storage::{NewScheduleRun, SCHEDULE_OUTCOMES, SCHEDULE_PASS_ID};
use pumper_core::{AppContext, JobStatus, NewSchedule, Result, ScrapeApp, SourceState};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, test_state_with, FakeApp};
use crate::state::AppState;
use crate::{routes, scheduler};

// ---- fixtures ---------------------------------------------------------------

/// An `extractor` app, so the retirement gate applies to its schedules at all
/// (`scheduler::retirement_dataset` maps only this app).
struct Extractor;

#[async_trait::async_trait]
impl ScrapeApp for Extractor {
    fn name(&self) -> &'static str {
        "extractor"
    }
    fn description(&self) -> &'static str {
        "retirable test app"
    }
    async fn run(&self, _ctx: AppContext) -> Result<Value> {
        Ok(json!({ "ok": true }))
    }
}

/// A schedule on `cron`, backdated to `created_at` so both its backlog and the
/// observation instant are deterministic.
async fn schedule_at(
    state: &AppState,
    app: &str,
    cron: &str,
    policy: &str,
    params: Value,
    created_at: DateTime<Utc>,
) -> String {
    let schedule = state
        .storage
        .create_schedule(NewSchedule {
            app,
            cron,
            params,
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

/// Every decision recorded against one schedule, newest first.
async fn decisions(state: &AppState, id: &str) -> Vec<pumper_core::storage::ScheduleRun> {
    state
        .storage
        .list_schedule_runs_page(id, None, 500)
        .await
        .expect("read the ledger")
}

/// `(outcome, reason)` pairs, newest first — the shape every assertion below
/// reads, so a row that gained an outcome but lost its reason is visible.
async fn verdicts(state: &AppState, id: &str) -> Vec<(String, Option<String>)> {
    decisions(state, id)
        .await
        .into_iter()
        .map(|r| (r.outcome, r.reason))
        .collect()
}

async fn get_json(router: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn set_source_state(state: &AppState, app: &str, dataset: &str, to: SourceState) {
    let hs = state.health.store().expect("health store");
    hs.ensure_source(app, dataset).await.unwrap();
    hs.set_state_manual(&source_id(app, dataset), to, "test")
        .await
        .unwrap();
}

// ---- 1. the overlap hold ----------------------------------------------------

/// Case 1. Three ticks held by the overlap guard leave three rows naming the
/// job that holds the slot and the firing that is still due — and `last_run`
/// does not move, which is the invariant the hold exists to protect.
#[tokio::test]
async fn three_held_ticks_leave_three_rows_and_do_not_move_the_reference() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let created = Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap();
    let id = schedule_at(
        &state,
        "fake",
        "0 * * * * *",
        "fire_once",
        json!({}),
        created,
    )
    .await;
    let mut cache = HashMap::new();

    // Fire once, so a run owns the slot.
    let t0 = Utc.with_ymd_and_hms(2026, 7, 13, 10, 5, 0).unwrap();
    scheduler::reconcile(&state, &mut cache, None, t0)
        .await
        .expect("first pass");
    let after_fire = state.storage.get_schedule(&id).await.unwrap().unwrap();
    assert_eq!(after_fire.last_run, Some(t0));

    // Three more ticks while that job is still queued.
    for minute in [6, 7, 8] {
        let t = Utc.with_ymd_and_hms(2026, 7, 13, 10, minute, 0).unwrap();
        let tally = scheduler::reconcile(&state, &mut cache, None, t)
            .await
            .expect("held pass");
        assert_eq!(tally.held, 1, "tick at :{minute} must hold");
    }

    let rows = decisions(&state, &id).await;
    let held: Vec<_> = rows.iter().filter(|r| r.outcome == "held").collect();
    assert_eq!(held.len(), 3, "three holds, three rows: {rows:?}");
    for row in &held {
        assert_eq!(
            row.reason.as_deref(),
            Some("overlapping"),
            "the reason is the admission vocabulary's own word"
        );
        assert!(
            row.job_id.is_some(),
            "a hold names the job that owns the slot — the one to go look at"
        );
        assert!(row.due_at.is_some(), "a hold names the firing still due");
    }
    // The same firing stayed due across all three holds. That is the whole
    // reason the hold may not record a skip advance.
    let due: Vec<_> = held.iter().map(|r| r.due_at).collect();
    assert!(
        due.windows(2).all(|w| w[0] == w[1]),
        "the held firing must be the SAME firing each tick: {due:?}"
    );
    assert_eq!(
        state
            .storage
            .get_schedule(&id)
            .await
            .unwrap()
            .unwrap()
            .last_run,
        Some(t0),
        "writing the ledger must not advance the cron reference"
    );
}

// ---- 2. the misfire count ---------------------------------------------------

/// Case 2. `skip` advancing past four missed firings and running the on-time
/// one in the SAME tick leaves two rows: the skip carries the `4` that used to
/// exist only in a log line, and the fire carries its job.
#[tokio::test]
async fn a_skip_then_fire_tick_records_the_missed_count_the_log_line_threw_away() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    // Hourly, accounted for at 07:00; 08/09/10/11 missed; back at 12:00:05, so
    // 12:00 is on-time (the boot grace floor is two 15s ticks).
    let created = Utc.with_ymd_and_hms(2026, 7, 13, 7, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 5).unwrap();
    let id = schedule_at(&state, "fake", "0 0 * * * *", "skip", json!({}), created).await;

    let tally = scheduler::reconcile(&state, &mut HashMap::new(), None, now)
        .await
        .expect("pass runs");
    assert_eq!((tally.skipped, tally.fired), (1, 1), "{tally:?}");

    let rows = decisions(&state, &id).await;
    assert_eq!(
        rows.len(),
        2,
        "one row per fact, not one per tick: {rows:?}"
    );
    let skipped = rows
        .iter()
        .find(|r| r.outcome == "skipped")
        .expect("a skip row");
    assert_eq!(
        skipped.missed, 4,
        "the four genuinely-missed firings are ON the row: {rows:?}"
    );
    let fired = rows
        .iter()
        .find(|r| r.outcome == "fired")
        .expect("a fire row");
    assert!(fired.job_id.is_some(), "the fire names its job: {fired:?}");
    assert_eq!(
        fired.missed, 0,
        "the fire is the on-time firing; it missed nothing"
    );
    // The store's own counter is NOT reason-free about the number: it
    // accumulates `missed`, so it reads 4 here. The card claimed +1 and that is
    // false -- pinned, so nobody "fixes" the ledger to match a wrong premise.
    let row = state.storage.get_schedule(&id).await.unwrap().unwrap();
    assert_eq!(
        row.skipped_count, 4,
        "skipped_count accumulates the missed count rather than incrementing"
    );
    // The gap the row actually closes: a lifetime total has no window and no
    // reason, so this `4` is indistinguishable from four separate ticks that
    // each ate one, and from four firings a RETIREMENT ate. The ledger row
    // carries the occurrence, its reason and its due time.
    assert_eq!(
        skipped.reason.as_deref(),
        Some("ok"),
        "the misfire skip was admitted; no gate refused it: {rows:?}"
    );
    assert!(
        skipped.due_at.is_some(),
        "and the occurrence names the firing it is about"
    );
}

// ---- 3. the reason ----------------------------------------------------------

/// Case 3. A retirement skip and a misfire skip on the SAME schedule are two
/// rows with the same `outcome` and different `reason`s. Before this they were
/// byte-identical in the store: both a `record_schedule_skip` call writing
/// `last_skipped_at` + `skipped_count`.
#[tokio::test]
async fn a_retirement_skip_and_a_misfire_skip_differ_by_reason_not_only_in_the_log() {
    let (state, _store) = test_state_with(vec![Arc::new(Extractor)], |c| {
        // Retirement is a SCHEDULING decision, so it must hold in soak mode.
        c.resilience.enabled = true;
        c.resilience.enforce = false;
    })
    .await;
    let created = Utc.with_ymd_and_hms(2026, 7, 13, 7, 0, 0).unwrap();
    let id = schedule_at(
        &state,
        "extractor",
        "0 0 * * * *",
        "skip",
        json!({ "dataset": "retired-ds" }),
        created,
    )
    .await;
    set_source_state(&state, "extractor", "retired-ds", SourceState::Retired).await;

    // Pass one: the source is retired, so the firing is skipped for THAT reason.
    let t1 = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 5).unwrap();
    scheduler::reconcile(&state, &mut HashMap::new(), None, t1)
        .await
        .expect("retired pass");

    // Restore the source and put a real backlog back under the row, so the next
    // pass skips for the misfire policy instead.
    set_source_state(&state, "extractor", "retired-ds", SourceState::Healthy).await;
    sqlx::query("UPDATE schedules SET last_skipped_at = ?1 WHERE id = ?2")
        .bind(
            Utc.with_ymd_and_hms(2026, 7, 13, 13, 0, 0)
                .unwrap()
                .to_rfc3339(),
        )
        .bind(&id)
        .execute(&state.storage.pool())
        .await
        .unwrap();
    // 14:00..17:00 missed, observed long after, so nothing is on-time.
    let t2 = Utc.with_ymd_and_hms(2026, 7, 13, 17, 30, 0).unwrap();
    scheduler::reconcile(&state, &mut HashMap::new(), None, t2)
        .await
        .expect("misfire pass");

    let rows = verdicts(&state, &id).await;
    let skips: Vec<_> = rows.iter().filter(|(o, _)| o == "skipped").collect();
    assert_eq!(skips.len(), 2, "two skips: {rows:?}");
    let reasons: Vec<_> = skips.iter().filter_map(|(_, r)| r.as_deref()).collect();
    assert!(
        reasons.contains(&"retired"),
        "the retirement skip names the gate that stopped it: {rows:?}"
    );
    assert!(
        reasons.contains(&"ok"),
        "the misfire skip was ADMITTED — nothing refused it, the policy ate the \
         backlog — and `ok` is what the one admission vocabulary calls that: {rows:?}"
    );
    assert_ne!(
        reasons[0], reasons[1],
        "the two skips must differ: {rows:?}"
    );
}

// ---- 4. the absorbed failure -----------------------------------------------

/// Case 4. A per-schedule storage error inside the pass is a row, not only an
/// increment of `PassTally::failed` — so the schedule whose step silently did
/// nothing can say so over the API.
#[tokio::test]
async fn a_per_schedule_failure_inside_the_pass_is_a_row_not_only_a_counter() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let created = Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap();
    let id = schedule_at(
        &state,
        "fake",
        "0 * * * * *",
        "fire_once",
        json!({}),
        created,
    )
    .await;

    // Break exactly the read the step makes (the overlap guard's `latest_run`),
    // leaving the ledger itself writable.
    sqlx::query("DROP TABLE jobs")
        .execute(&state.storage.pool())
        .await
        .unwrap();

    let now = Utc.with_ymd_and_hms(2026, 7, 13, 10, 5, 0).unwrap();
    let tally = scheduler::reconcile(&state, &mut HashMap::new(), None, now)
        .await
        .expect("the pass itself must still complete");
    assert_eq!(
        tally.failed, 1,
        "the step failed and was absorbed: {tally:?}"
    );

    let rows = decisions(&state, &id).await;
    assert_eq!(rows.len(), 1, "the silence is one row: {rows:?}");
    assert_eq!(rows[0].outcome, "failed");
    assert!(
        rows[0].detail.as_deref().is_some_and(|d| !d.is_empty()),
        "the error text travels with the row: {rows:?}"
    );
}

// ---- 5. the pass heartbeat --------------------------------------------------

/// Case 5. A completed pass stamps a durable heartbeat; a loop wedged at
/// `list_schedules` never does, and `GET /schedules` can tell the two apart.
#[tokio::test]
async fn a_wedged_loop_is_distinguishable_from_nothing_being_due() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());

    // No pass has ever completed: there is nothing to be confident about.
    let (status, body) = get_json(&router, "/schedules?cursor=").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["scheduler"]["last_pass_at"].is_null(),
        "no heartbeat yet: {body}"
    );
    assert_eq!(
        body["scheduler"]["stale"], true,
        "a loop that has never completed a pass is not healthy: {body}"
    );

    // An empty pass — nothing due, nothing to do — still completes, and that is
    // exactly the fact the heartbeat exists to carry.
    let now = Utc::now();
    let tally = scheduler::reconcile(&state, &mut HashMap::new(), None, now)
        .await
        .expect("empty pass");
    assert_eq!(
        (tally.fired, tally.skipped, tally.held, tally.failed),
        (0, 0, 0, 0),
        "nothing was due: {tally:?}"
    );
    state
        .storage
        .mark_schedule_pass(now)
        .await
        .expect("heartbeat");

    let (_, body) = get_json(&router, "/schedules?cursor=").await;
    assert_eq!(
        body["scheduler"]["stale"], false,
        "a just-completed pass is fresh even though it did nothing: {body}"
    );
    assert!(!body["scheduler"]["last_pass_at"].is_null());

    // The heartbeat is ONE row, upserted — not one row per pass. A row per pass
    // would paginate the newest-first page out of reach on a busy edge, which
    // is the shape the trigger ledger's static facts got wrong.
    for _ in 0..50 {
        state.storage.mark_schedule_pass(Utc::now()).await.unwrap();
    }
    let beats: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = ?1")
            .bind(SCHEDULE_PASS_ID)
            .fetch_one(&state.storage.pool())
            .await
            .unwrap();
    assert_eq!(beats, 1, "51 passes, one heartbeat row");

    // And a stale heartbeat reads as stale.
    state
        .storage
        .mark_schedule_pass(Utc::now() - Duration::hours(6))
        .await
        .unwrap();
    let (_, body) = get_json(&router, "/schedules?cursor=").await;
    assert_eq!(
        body["scheduler"]["stale"], true,
        "six hours without a completed pass is the wedge: {body}"
    );
}

/// The staleness predicate itself, away from the clock: a pure function of the
/// last heartbeat, now, and the configured tick.
#[test]
fn pass_staleness_is_measured_against_the_configured_tick() {
    let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap();
    assert!(
        scheduler::pass_is_stale(None, now, 15),
        "never having completed a pass is not fresh"
    );
    assert!(!scheduler::pass_is_stale(
        Some(now - Duration::seconds(5)),
        now,
        15
    ));
    assert!(
        !scheduler::pass_is_stale(Some(now - Duration::seconds(45)), now, 15),
        "a 15s tick tolerates a minute, so one slow pass is not an alarm"
    );
    assert!(scheduler::pass_is_stale(
        Some(now - Duration::hours(1)),
        now,
        15
    ));
    // A long tick widens the window rather than crying wolf...
    assert!(!scheduler::pass_is_stale(
        Some(now - Duration::minutes(30)),
        now,
        3600
    ));
    // ...and a sub-second tick does not make the window flap.
    assert!(!scheduler::pass_is_stale(
        Some(now - Duration::seconds(30)),
        now,
        1
    ));
}

// ---- 6. bounded at birth ----------------------------------------------------

/// Case 6. The ledger is bounded before the first row exists: the reaper's
/// prune drops rows past the retention age, the read path keyset-pages
/// newest-first, and the heartbeat survives its own staleness (pruning the
/// evidence of a wedge would be the one deletion that destroys the answer).
#[tokio::test]
async fn the_ledger_is_age_bounded_and_pages_newest_first() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let id = "sched-ledger";
    for n in 0..1000 {
        state
            .storage
            .record_schedule_run(&NewScheduleRun {
                schedule_id: id,
                outcome: "held",
                reason: Some("overlapping"),
                missed: n % 3,
                ..Default::default()
            })
            .await
            .expect("record");
    }
    assert_eq!(decisions(&state, id).await.len(), 500, "the page is capped");

    // Newest-first, and the keyset page continues without repeating a row.
    let first = state
        .storage
        .list_schedule_runs_page(id, None, 10)
        .await
        .unwrap();
    assert!(
        first.windows(2).all(|w| w[0].created_at >= w[1].created_at),
        "newest first"
    );
    let last = first.last().expect("a page");
    let next = state
        .storage
        .list_schedule_runs_page(
            id,
            Some((pumper_core::datasets::ts(last.created_at), last.id.clone())),
            10,
        )
        .await
        .unwrap();
    assert_eq!(next.len(), 10);
    assert!(
        !next.iter().any(|r| r.id == last.id),
        "the cursor does not repeat its own row"
    );

    // A heartbeat older than the retention age must NOT be pruned.
    state
        .storage
        .mark_schedule_pass(Utc::now() - Duration::days(400))
        .await
        .unwrap();
    // Age half the decisions past the window.
    sqlx::query("UPDATE schedule_runs SET created_at = ?1 WHERE schedule_id = ?2 AND missed = 0")
        .bind(pumper_core::datasets::ts(Utc::now() - Duration::days(400)))
        .bind(id)
        .execute(&state.storage.pool())
        .await
        .unwrap();

    let before = decisions(&state, id).await.len();
    let pruned = state
        .storage
        .prune_schedule_runs(30)
        .await
        .expect("prune runs");
    assert!(pruned > 0, "the sweep drops the aged rows");
    assert!(
        decisions(&state, id).await.len() < before || before == 500,
        "the table does not grow without bound"
    );
    let aged: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = ?1 AND created_at < ?2",
    )
    .bind(id)
    .bind(pumper_core::datasets::ts(Utc::now() - Duration::days(60)))
    .fetch_one(&state.storage.pool())
    .await
    .unwrap();
    assert_eq!(aged, 0, "nothing older than the window survives");
    assert!(
        state
            .storage
            .schedule_pass_heartbeat()
            .await
            .unwrap()
            .is_some(),
        "the heartbeat is exempt: pruning it would delete the evidence OF the \
         wedge it exists to prove"
    );
    // `days == 0` is "never prune", exactly like the trigger ledger.
    assert_eq!(state.storage.prune_schedule_runs(0).await.unwrap(), 0);
}

// ---- 7. fail-open -----------------------------------------------------------

/// Case 7. A ledger write that fails must never hold up the firing it
/// describes. The same fail-open contract `record_trigger_run` states.
#[tokio::test]
async fn a_ledger_write_failure_never_holds_up_the_firing_it_describes() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let created = Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap();
    let id = schedule_at(
        &state,
        "fake",
        "0 * * * * *",
        "fire_once",
        json!({}),
        created,
    )
    .await;

    // Take the ledger away entirely.
    sqlx::query("DROP TABLE schedule_runs")
        .execute(&state.storage.pool())
        .await
        .unwrap();

    let now = Utc.with_ymd_and_hms(2026, 7, 13, 10, 5, 0).unwrap();
    let tally = scheduler::reconcile(&state, &mut HashMap::new(), None, now)
        .await
        .expect("the pass completes without the ledger");
    assert_eq!(tally.fired, 1, "the job is enqueued regardless: {tally:?}");
    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE schedule_id = ?1")
        .bind(&id)
        .fetch_one(&state.storage.pool())
        .await
        .unwrap();
    assert_eq!(jobs, 1);
    let row = state.storage.get_schedule(&id).await.unwrap().unwrap();
    assert_eq!(
        row.last_run,
        Some(now),
        "and the firing is accounted for, so it is not re-run forever"
    );
}

// ---- the HTTP surface -------------------------------------------------------

/// The endpoint itself, and the 404 parity `GET /triggers/{id}/runs` already
/// has: an unknown id must not answer `200 {count: 0}`, which reads as "this
/// schedule has never done anything" — the exact wrong answer here.
#[tokio::test]
async fn the_runs_endpoint_answers_the_ledger_and_404s_an_unknown_schedule() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());
    let created = Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap();
    let id = schedule_at(
        &state,
        "fake",
        "0 * * * * *",
        "fire_once",
        json!({}),
        created,
    )
    .await;

    let (status, _) = get_json(&router, "/schedules/nope/runs").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an unknown schedule is a 404, not an empty ledger"
    );

    let t0 = Utc.with_ymd_and_hms(2026, 7, 13, 10, 5, 0).unwrap();
    scheduler::reconcile(&state, &mut HashMap::new(), None, t0)
        .await
        .unwrap();
    let t1 = Utc.with_ymd_and_hms(2026, 7, 13, 10, 6, 0).unwrap();
    scheduler::reconcile(&state, &mut HashMap::new(), None, t1)
        .await
        .unwrap();

    let (status, body) = get_json(&router, &format!("/schedules/{id}/runs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["schedule_id"], json!(id));
    let outs: Vec<&str> = body["decisions"]
        .as_array()
        .expect("decisions array")
        .iter()
        .map(|d| d["outcome"].as_str().expect("outcome"))
        .collect();
    assert_eq!(outs, vec!["held", "fired"], "newest first: {body}");
    for out in &outs {
        assert!(
            SCHEDULE_OUTCOMES.contains(out),
            "`{out}` is outside the published vocabulary"
        );
    }
    assert!(
        body["scheduler"]["stale"].is_boolean(),
        "the liveness answer travels with the ledger too: {body}"
    );
    // The job lineage is answered alongside the decisions, like the trigger
    // endpoint: the jobs this schedule actually enqueued.
    assert_eq!(body["count"], json!(1), "{body}");
    let status_of = body["runs"][0]["status"].as_str().expect("a job row");
    assert_eq!(status_of, JobStatus::Queued.as_str());
}
