//! A schedule's spend ceiling, from the door to the enqueued job.
//!
//! Schedules were the LAST work-creator without one. The jobs door and the
//! trigger door both got the budget floor in round 12, but the fire path built
//! its `EnqueueOptions` from `Default`, so `budget_usd` was `None` — "no
//! ceiling" — for every scheduled run. The user moment that names the bug: *"I
//! put a $2 ceiling on my research jobs, then scheduled them nightly — and the
//! scheduled ones ran unlimited."*
//!
//! Driven here end to end: the ceiling reaches the job row, the door refuses the
//! values that would silently *become* unlimited, a row that was never created
//! through `POST /schedules` (code-seeded, catalog-managed) can still be given
//! one, and a catalog reconcile — which has no budget vocabulary at all — cannot
//! wipe it back off.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{Duration, Utc};
use http_body_util::BodyExt;
use pumper_core::CATALOG_MANAGED_BY;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::routes;
use crate::scheduler;
use crate::state::AppState;

/// Every minute, on the minute — the cadence the other schedule e2e tests use,
/// paired with a backdated `created_at` so the first pass finds a firing due.
const EVERY_MINUTE: &str = "0 * * * * *";

async fn send(router: &axum::Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn get(router: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Places the row's `created_at` in the past so the next pass has a firing to
/// act on, whatever wall-clock second the test runs at.
async fn backdate(state: &AppState, id: &str) {
    sqlx::query("UPDATE schedules SET created_at = ?1 WHERE id = ?2")
        .bind((Utc::now() - Duration::minutes(5)).to_rfc3339())
        .bind(id)
        .execute(&state.storage.pool())
        .await
        .expect("backdate schedule");
}

/// The job a schedule's firing produced, read back exactly as the worker sees it.
async fn fired_job(state: &AppState, schedule_id: &str) -> pumper_core::Job {
    let (id, _) = state
        .storage
        .latest_job_for_schedule(schedule_id)
        .await
        .expect("read the schedule's latest run")
        .expect("the schedule fired");
    state
        .storage
        .get(uuid::Uuid::parse_str(&id).expect("job id is a uuid"))
        .await
        .expect("read job")
        .expect("job row")
}

async fn budget_of(state: &AppState, schedule_id: &str) -> Option<f64> {
    state
        .storage
        .get_schedule(schedule_id)
        .await
        .expect("read schedule")
        .expect("schedule row")
        .budget_usd
}

/// The headline: a scheduled run is byte-identical to the same app enqueued at
/// the jobs door with that ceiling — and a schedule that set none still gets the
/// documented `null`, because this feature invents no default.
#[tokio::test]
async fn a_scheduled_run_carries_the_schedules_ceiling_not_an_unlimited_default() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());

    let (status, capped) = send(
        &router,
        "POST",
        "/schedules",
        json!({ "app": "fake", "cron": EVERY_MINUTE, "budget_usd": 2.0 }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{capped}");
    assert_eq!(
        capped["budget_usd"], 2.0,
        "the ceiling is stored verbatim, not clamped: {capped}"
    );
    let (status, uncapped) = send(
        &router,
        "POST",
        "/schedules",
        json!({ "app": "fake", "cron": EVERY_MINUTE }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{uncapped}");
    assert!(
        uncapped["budget_usd"].is_null(),
        "omitting the field stays 'no ceiling': {uncapped}"
    );

    let capped = capped["id"].as_str().unwrap().to_string();
    let uncapped = uncapped["id"].as_str().unwrap().to_string();
    backdate(&state, &capped).await;
    backdate(&state, &uncapped).await;

    let tally = scheduler::reconcile(&state, &mut HashMap::new(), None, Utc::now())
        .await
        .expect("pass runs");
    assert_eq!(tally.fired, 2, "both schedules were due: {tally:?}");

    assert_eq!(
        fired_job(&state, &capped).await.budget_usd,
        Some(2.0),
        "the scheduled run must carry the schedule's ceiling — the whole bug was \
         that the fire path built its EnqueueOptions from Default, i.e. unlimited"
    );
    assert!(
        fired_job(&state, &uncapped).await.budget_usd.is_none(),
        "a schedule with no ceiling still enqueues an uncapped job (unchanged default)"
    );

    // And the field is visible on the observability surface, not only in SQL:
    // the enrichment path rebuilds each row as JSON, so a stripped field there
    // would make an operator's ceiling invisible.
    let (status, listed) = get(&router, "/schedules").await;
    assert_eq!(status, StatusCode::OK);
    let rows = listed.as_array().expect("bare array mode");
    assert!(
        rows.iter().any(|s| s["budget_usd"] == json!(2.0)),
        "GET /schedules must surface the ceiling it stored: {listed}"
    );
    assert!(
        rows.iter()
            .any(|s| s.get("budget_usd").is_some_and(Value::is_null)),
        "...and an explicit null (not a dropped key) for the row that has none: {listed}"
    );
}

/// The rows that actually spend are the ones nobody created over `POST
/// /schedules`: a code-seeded `ScrapeApp::schedule` row and a catalog-managed
/// one are both born `NULL`. `POST /schedules/{id}/budget` is the only way they
/// can ever get a ceiling — and it has to bind the NEXT firing, not just be
/// stored.
#[tokio::test]
async fn a_ceiling_set_after_creation_binds_the_next_firing() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());

    // Exactly what `main` seeds from `ScrapeApp::schedule()` at boot.
    state
        .storage
        .seed_schedule("fake", EVERY_MINUTE)
        .await
        .expect("seed the code-declared schedule");
    let id = "static-fake";
    assert_eq!(
        budget_of(&state, id).await,
        None,
        "a code-seeded row starts with no ceiling"
    );

    let (status, body) = send(
        &router,
        "POST",
        &format!("/schedules/{id}/budget"),
        json!({ "budget_usd": 1.5 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["budget_usd"], 1.5, "{body}");
    assert_eq!(budget_of(&state, id).await, Some(1.5), "and it stuck");

    backdate(&state, id).await;
    scheduler::reconcile(&state, &mut HashMap::new(), None, Utc::now())
        .await
        .expect("pass runs");
    assert_eq!(
        fired_job(&state, id).await.budget_usd,
        Some(1.5),
        "the ceiling set over the API must reach the run it was set for"
    );

    // Unknown row: a 404, not a silently-swallowed write.
    let (status, _) = send(
        &router,
        "POST",
        "/schedules/nope/budget",
        json!({ "budget_usd": 1.0 }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// The floor, at both schedule doors, with the jobs door's own contract: `0` is
/// refused rather than reinterpreted. A schedule is the worst place for that
/// reinterpretation — the value is stored on the row and replayed into every run
/// forever, so one silent decay is a standing unlimited-spend order.
#[tokio::test]
async fn a_zero_ceiling_is_refused_at_both_schedule_doors_not_stored_as_unlimited() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());

    for refused in [json!(0.0), json!(-2.5)] {
        let (status, body) = send(
            &router,
            "POST",
            "/schedules",
            json!({ "app": "fake", "cron": EVERY_MINUTE, "budget_usd": refused }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "budget_usd {refused} must be refused at the create door: {body}"
        );
        let msg = body["error"].as_str().unwrap_or_default();
        assert!(
            msg.contains("NO spend ceiling"),
            "the 422 must explain what the value would have meant — the same \
             message the jobs door answers: {msg}"
        );
    }
    assert!(
        state.storage.list_schedules().await.unwrap().is_empty(),
        "a refused budget must not leave a schedule behind"
    );

    // The same floor on the set door, where a refusal must also leave the
    // ceiling the row already had alone.
    let (status, created) = send(
        &router,
        "POST",
        "/schedules",
        json!({ "app": "fake", "cron": EVERY_MINUTE, "budget_usd": 0.5 }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["id"].as_str().unwrap().to_string();
    for refused in [json!(0.0), json!(-1.0)] {
        let (status, body) = send(
            &router,
            "POST",
            &format!("/schedules/{id}/budget"),
            json!({ "budget_usd": refused }),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    }
    assert_eq!(
        budget_of(&state, &id).await,
        Some(0.5),
        "a refused write must not have lifted the ceiling that was already there"
    );

    // `null` is the documented way to remove one, and is not the same request as
    // `0`: it asks for no ceiling explicitly instead of pretending to cap.
    let (status, body) = send(
        &router,
        "POST",
        &format!("/schedules/{id}/budget"),
        json!({ "budget_usd": Value::Null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(budget_of(&state, &id).await, None);
}

/// The fire-time re-read, for money: a ceiling that lands *while the pass is
/// walking the table* was placed before this enqueue happened, so it must bind
/// this firing. The pass holds a snapshot from one `list_schedules`, and the row
/// this step is about may be several rows further down.
#[tokio::test]
async fn a_ceiling_set_while_the_pass_walks_the_table_binds_that_firing() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());

    let (status, created) = send(
        &router,
        "POST",
        "/schedules",
        json!({ "app": "fake", "cron": EVERY_MINUTE }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["id"].as_str().unwrap().to_string();
    backdate(&state, &id).await;

    // The snapshot this pass would be working from — no ceiling on it.
    let snapshot = state.storage.get_schedule(&id).await.unwrap().unwrap();
    assert!(snapshot.budget_usd.is_none());

    // ...and the operator's ceiling lands after that read, before the step acts.
    let (status, body) = send(
        &router,
        "POST",
        &format!("/schedules/{id}/budget"),
        json!({ "budget_usd": 0.75 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let now = Utc::now();
    let cron = cron::Schedule::from_str(EVERY_MINUTE).unwrap();
    let outcome =
        scheduler::reconcile_one(&state, &snapshot, &cron, now - Duration::minutes(1), now)
            .await
            .expect("the step runs");
    assert_eq!(outcome, scheduler::StepOutcome::Fired);
    assert_eq!(
        fired_job(&state, &id).await.budget_usd,
        Some(0.75),
        "the firing must honour the live row's ceiling; replaying the snapshot's \
         would let one more unbounded run out after the call meant to stop it"
    );
}

/// `catalog/data-sources.toml` has no budget vocabulary — a catalog-managed row
/// is born `NULL` and the only ceiling it can ever have is one an operator set
/// over the API. So the reconciler must not carry a ceiling *away* either: its
/// writes touch `cron`/`enabled` and nothing else, and a re-apply that rewrote
/// the whole row would silently return a governed schedule to unlimited spend.
#[tokio::test]
async fn a_catalog_reconcile_does_not_wipe_an_operator_set_ceiling() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());

    let managed = state
        .storage
        .create_managed_schedule("fake", "0 0 * * * *", CATALOG_MANAGED_BY)
        .await
        .expect("catalog-managed schedule");
    assert_eq!(
        managed.budget_usd, None,
        "the catalog cannot express a budget, so its rows start with none"
    );

    let (status, body) = send(
        &router,
        "POST",
        &format!("/schedules/{}/budget", managed.id),
        json!({ "budget_usd": 3.0 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Re-apply the catalog with a changed cron, through both write paths the
    // reconciler uses (the idempotent create's ON CONFLICT branch, and the
    // explicit cron update), plus a governance disable/enable round trip.
    state
        .storage
        .create_managed_schedule("fake", "0 30 * * * *", CATALOG_MANAGED_BY)
        .await
        .expect("re-apply is idempotent");
    assert!(state
        .storage
        .set_managed_schedule_cron(&managed.id, "0 45 * * * *", CATALOG_MANAGED_BY)
        .await
        .expect("cron update"));
    assert!(state
        .storage
        .set_managed_schedule_enabled(&managed.id, false, CATALOG_MANAGED_BY)
        .await
        .expect("disable"));
    assert!(state
        .storage
        .set_managed_schedule_enabled(&managed.id, true, CATALOG_MANAGED_BY)
        .await
        .expect("re-enable"));

    let row = state
        .storage
        .get_schedule(&managed.id)
        .await
        .unwrap()
        .expect("row survives");
    assert_eq!(row.cron, "0 45 * * * *", "the reconcile really did rewrite");
    assert_eq!(
        row.budget_usd,
        Some(3.0),
        "the operator's ceiling is not catalog state, so no reconcile may clear it"
    );
}
