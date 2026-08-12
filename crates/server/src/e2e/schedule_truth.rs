//! `GET /schedules` exists to answer one question — "why isn't this firing?" —
//! and it is only worth having if its answer is the scheduler's answer.
//!
//! Two ways it used to lie, both driven here over the real router against the
//! real reconcile loop:
//!
//! 1. **The retry wedge.** The overlap guard was existential over ALL of a
//!    schedule's jobs (`status IN ('queued','running')`) while health read only
//!    the NEWEST one. `POST /jobs/{id}/retry` on an OLD job of the schedule
//!    re-queues it without touching `created_at`, and nothing clears
//!    `schedule_id` — so the guard stayed satisfied forever, the schedule
//!    silently never fired again, and the API kept answering `health: "ok"`.
//!    `POST /jobs/retry {app}` could do that to every schedule of an app in one
//!    call.
//! 2. **The lying skip.** Under `misfire_policy = "skip"` the scheduler stamped
//!    `last_run` when NO job had run, so one response carried a recent
//!    `last_run` next to a null `last_job_id`, and the count of firings the
//!    policy ate lived only in a log line.
//!
//! The `invalid_params` health is covered by `enqueue_door_parity.rs`; this file
//! deliberately does not restate it.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{Duration, Utc};
use http_body_util::BodyExt;
use pumper_core::{JobStatus, NewSchedule};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::state::AppState;
use crate::{routes, scheduler};

// ---- HTTP helpers -----------------------------------------------------------

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

async fn post_empty(router: &axum::Router, uri: &str) -> StatusCode {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

/// The one schedule row of the store, as `GET /schedules` renders it.
async fn only_schedule(router: &axum::Router) -> Value {
    let (status, body) = get_json(router, "/schedules").await;
    assert_eq!(status, StatusCode::OK);
    let items = body.as_array().expect("bare-array mode");
    assert_eq!(items.len(), 1, "expected exactly one schedule: {body}");
    items[0].clone()
}

// ---- store helpers ----------------------------------------------------------

/// An every-minute schedule for `fake`, already due (created five minutes ago).
async fn due_schedule(state: &AppState, misfire_policy: &str) -> String {
    let schedule = state
        .storage
        .create_schedule(NewSchedule {
            app: "fake",
            cron: "0 * * * * *",
            params: json!({}),
            priority: 0,
            timezone: None,
            misfire_policy,
            max_attempts: Some(1),
        })
        .await
        .expect("create schedule");
    sqlx::query("UPDATE schedules SET created_at = ?1 WHERE id = ?2")
        .bind((Utc::now() - Duration::minutes(5)).to_rfc3339())
        .bind(&schedule.id)
        .execute(&state.storage.pool())
        .await
        .unwrap();
    schedule.id
}

async fn jobs_for_schedule(state: &AppState, id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE schedule_id = ?1")
        .bind(id)
        .fetch_one(&state.storage.pool())
        .await
        .unwrap()
}

/// Claims the one queued job and drives it to `status`. Returns its id.
async fn run_queued_job(state: &AppState, succeed: bool) -> uuid::Uuid {
    let claimed = state
        .storage
        .claim_next(&[], 0.0)
        .await
        .unwrap()
        .expect("a queued job to claim");
    if succeed {
        state
            .storage
            .complete(claimed.id, claimed.attempts, json!({ "ok": true }))
            .await
            .unwrap();
    } else {
        state
            .storage
            .fail_permanently(claimed.id, claimed.attempts, "boom")
            .await
            .unwrap();
    }
    claimed.id
}

// ---- the retry wedge --------------------------------------------------------

/// The refuted case, end to end: fire A and let it fail, fire B and let it
/// succeed, then manually retry the OLD job A. The schedule must keep firing,
/// and the API must have been telling the truth about it the whole time.
///
/// Under the existential guard, retrying A held the slot forever: the third
/// reconcile enqueued nothing while `GET /schedules` still said `ok` — the API
/// and the scheduler answering the same question two different ways.
#[tokio::test]
async fn a_retried_older_job_does_not_wedge_the_schedule_forever() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());
    let id = due_schedule(&state, "fire_once").await;
    let mut cron_cache = HashMap::new();

    // Firing A: fails permanently (max_attempts = 1).
    scheduler::reconcile(&state, &mut cron_cache, Utc::now())
        .await
        .unwrap();
    let job_a = run_queued_job(&state, false).await;
    assert_eq!(
        state.storage.get(job_a).await.unwrap().unwrap().status,
        JobStatus::Failed
    );

    // Firing B, two minutes later: succeeds.
    let t2 = Utc::now() + Duration::minutes(2);
    scheduler::reconcile(&state, &mut cron_cache, t2)
        .await
        .unwrap();
    assert_eq!(jobs_for_schedule(&state, &id).await, 2, "B fired");
    let job_b = run_queued_job(&state, true).await;
    assert_ne!(job_a, job_b);

    // The operator retries the OLD failed job over the real HTTP door.
    assert_eq!(
        post_empty(&router, &format!("/jobs/{job_a}/retry")).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        state.storage.get(job_a).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "the retry really did re-queue the older job"
    );

    // The API's answer, with a re-queued old job of this schedule outstanding.
    let row = only_schedule(&router).await;
    assert_eq!(
        row["health"], "ok",
        "the newest run finished, so this schedule is healthy: {row}"
    );
    assert_eq!(
        row["last_job_id"],
        json!(job_b.to_string()),
        "health is derived from the NEWEST run, not from whatever is queued"
    );

    // ...and the scheduler must agree with it. This is the wedge.
    let t3 = Utc::now() + Duration::minutes(4);
    scheduler::reconcile(&state, &mut cron_cache, t3)
        .await
        .unwrap();
    assert_eq!(
        jobs_for_schedule(&state, &id).await,
        3,
        "a manually retried OLD job must not hold this schedule's firing slot — \
         the API said `ok`, so the scheduler has to fire"
    );
}

/// The guarantee the overlap guard exists for, which the wedge fix must not
/// cost: while the schedule's own current run is still outstanding, a due firing
/// is HELD, `last_run` is not advanced, and the API says so.
#[tokio::test]
async fn a_live_run_still_holds_the_slot_and_reports_overlapping() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());
    let id = due_schedule(&state, "fire_once").await;
    let mut cron_cache = HashMap::new();

    scheduler::reconcile(&state, &mut cron_cache, Utc::now())
        .await
        .unwrap();
    assert_eq!(jobs_for_schedule(&state, &id).await, 1);
    let last_run_1 = only_schedule(&router).await["last_run"].clone();
    assert!(!last_run_1.is_null(), "a real firing stamps last_run");

    // The job is still queued: the next due tick must not stack a second run.
    let row = only_schedule(&router).await;
    assert_eq!(row["health"], "overlapping", "{row}");

    let t2 = Utc::now() + Duration::minutes(2);
    scheduler::reconcile(&state, &mut cron_cache, t2)
        .await
        .unwrap();
    assert_eq!(
        jobs_for_schedule(&state, &id).await,
        1,
        "no stacked run while this schedule's own run is outstanding"
    );
    assert_eq!(
        only_schedule(&router).await["last_run"],
        last_run_1,
        "a held firing must not advance last_run — it stays due"
    );

    // Finishing it releases the held firing on the next tick.
    run_queued_job(&state, true).await;
    assert_eq!(only_schedule(&router).await["health"], "ok");
    scheduler::reconcile(&state, &mut cron_cache, t2)
        .await
        .unwrap();
    assert_eq!(jobs_for_schedule(&state, &id).await, 2);
}

// ---- the lying skip ---------------------------------------------------------

/// `misfire_policy = "skip"` advances past firings missed while the scheduler
/// was down. It used to record that advance in `last_run` — so the row claimed a
/// run that never happened, next to a null `last_job_id`, and the number of
/// eaten firings existed only in the logs.
#[tokio::test]
async fn a_misfire_skip_is_recorded_as_a_skip_not_as_a_run() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());
    let id = due_schedule(&state, "skip").await;

    // Five minutes of every-minute firings missed while "down".
    scheduler::reconcile(&state, &mut HashMap::new(), Utc::now())
        .await
        .unwrap();

    assert_eq!(
        jobs_for_schedule(&state, &id).await,
        0,
        "the whole point of `skip` is that nothing runs"
    );
    let row = only_schedule(&router).await;
    assert!(
        row["last_run"].is_null(),
        "nothing ran, so `last_run` must still be null: {row}"
    );
    assert!(
        row["last_job_id"].is_null(),
        "and the two can no longer contradict each other: {row}"
    );
    assert!(
        !row["last_skipped_at"].is_null(),
        "the advance is recorded where it belongs: {row}"
    );
    let skipped = row["skipped_count"].as_i64().expect("skipped_count");
    assert!(
        skipped >= 4,
        "the eaten firings are counted on the row, not only in a log line \
         (five minutes of an every-minute cron): {row}"
    );

    // And the projection followed the skip: the schedule is not stuck re-scanning
    // the same backlog, it is waiting for a firing in the future.
    let next_run = row["next_run"].as_str().expect("next_run");
    let next: chrono::DateTime<Utc> = next_run.parse().expect("rfc3339 next_run");
    assert!(
        next > Utc::now() - Duration::seconds(90),
        "a skip must move the cron reference forward, or every tick re-scans the \
         same backlog forever: {row}"
    );

    // A second tick right after eats nothing more.
    scheduler::reconcile(&state, &mut HashMap::new(), Utc::now())
        .await
        .unwrap();
    assert_eq!(
        only_schedule(&router).await["skipped_count"],
        json!(skipped),
        "the backlog was already advanced past; re-counting it would inflate the gauge"
    );

    // A real firing after the skip stamps `last_run` — and leaves the skip
    // history intact, because both facts are true and both are worth keeping.
    // (The policy is switched first: while it stays `skip`, a firing more than
    // the grace window behind is by definition skipped again — that is the
    // policy working, not the row lying.)
    sqlx::query("UPDATE schedules SET misfire_policy = 'fire_once' WHERE id = ?1")
        .bind(&id)
        .execute(&state.storage.pool())
        .await
        .unwrap();
    scheduler::reconcile(
        &state,
        &mut HashMap::new(),
        Utc::now() + Duration::minutes(2),
    )
    .await
    .unwrap();
    assert_eq!(jobs_for_schedule(&state, &id).await, 1);
    let row = only_schedule(&router).await;
    assert!(!row["last_run"].is_null(), "the real run stamps last_run");
    assert_eq!(
        row["skipped_count"],
        json!(skipped),
        "a later real run does not erase what the skip policy already ate: {row}"
    );
    assert!(
        !row["last_skipped_at"].is_null(),
        "both facts survive on one row: this schedule has run AND skipped: {row}"
    );
}

// ---- the remaining health answers -------------------------------------------

/// The two configuration-level answers, over HTTP. `disabled` outranks
/// everything (a disabled schedule's cron is never even parsed), and an
/// unparseable cron is named rather than reported as a schedule that simply
/// never fires.
#[tokio::test]
async fn disabled_and_invalid_cron_are_named_not_reported_as_ok() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());
    let id = due_schedule(&state, "fire_once").await;

    assert_eq!(only_schedule(&router).await["health"], "ok");

    // Disabled.
    assert!(state
        .storage
        .set_schedule_enabled(&id, false)
        .await
        .unwrap());
    assert_eq!(only_schedule(&router).await["health"], "disabled");
    assert!(
        only_schedule(&router).await["next_run"].is_null()
            || only_schedule(&router).await["health"] == "disabled"
    );
    assert!(state.storage.set_schedule_enabled(&id, true).await.unwrap());

    // An unparseable cron (only reachable by editing the row — the create door
    // refuses it — which is exactly why the read side has to name it).
    sqlx::query("UPDATE schedules SET cron = 'not a cron' WHERE id = ?1")
        .bind(&id)
        .execute(&state.storage.pool())
        .await
        .unwrap();
    let row = only_schedule(&router).await;
    assert_eq!(row["health"], "invalid_cron", "{row}");
    assert!(row["next_run"].is_null(), "{row}");

    // The scheduler agrees: nothing fires.
    scheduler::reconcile(&state, &mut HashMap::new(), Utc::now())
        .await
        .unwrap();
    assert_eq!(jobs_for_schedule(&state, &id).await, 0);
}
