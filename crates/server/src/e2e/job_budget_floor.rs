//! The enqueue door's budget floor: `budget_usd` is a ceiling, never a wish.
//!
//! The trap this pins closed: `budget_usd: 0` reads as "spend nothing", but an
//! *absent* `budget_usd` is what the runtime calls "no ceiling". The door used
//! to filter a non-positive value away to `None`, so the most cautious request
//! enqueued the one job shape that can spend without limit — on a paid path
//! (Claude research, the paid fetch tiers) that is real money.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::routes;

async fn enqueue(router: &axum::Router, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri("/apps/fake/jobs")
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

#[tokio::test]
async fn budget_zero_is_rejected_not_unlimited() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());

    for refused in [json!(0.0), json!(-2.5)] {
        let (status, body) = enqueue(&router, json!({ "budget_usd": refused })).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "budget_usd {refused} must be refused at the door: {body}"
        );
        let msg = body["error"].as_str().unwrap_or_default();
        assert!(
            msg.contains("NO spend ceiling"),
            "the 422 must explain what the value would have meant: {msg}"
        );
    }

    // The refusal is a refusal: nothing was enqueued behind it.
    let jobs = state.storage.list(None, None, 50).await.unwrap();
    assert!(
        jobs.is_empty(),
        "a refused budget must not leave a job in the queue: {jobs:?}"
    );
}

/// Same floor at the trigger door — where the dropped value was WORSE than one
/// bad job: `budget_usd` is stored on the trigger row and replayed into every
/// hop it fires, so `0` silently becoming `None` was a standing unlimited-spend
/// generator.
#[tokio::test]
async fn trigger_budget_zero_is_rejected_not_a_standing_unlimited_generator() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());

    let post = |body: Value| {
        let router = router.clone();
        async move {
            let req = Request::builder()
                .method("POST")
                .uri("/triggers")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();
            let status = resp.status();
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            (status, body)
        }
    };

    let trigger = |budget: Value| {
        json!({
            "source_kind": "job",
            "source_app": "fake",
            "target_app": "fake",
            "budget_usd": budget,
        })
    };

    let (status, body) = post(trigger(json!(0.0))).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "a zero trigger budget must be refused, not stored as unlimited: {body}"
    );
    let triggers = state.storage.list_triggers(None).await.unwrap();
    assert!(
        triggers.is_empty(),
        "the refused trigger must not exist: {triggers:?}"
    );

    // A real ceiling still creates, stored verbatim for every future hop.
    let (status, body) = post(trigger(json!(0.5))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["budget_usd"], 0.5, "{body}");
}

#[tokio::test]
async fn a_real_ceiling_and_an_omitted_one_both_still_enqueue() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());

    let (status, body) = enqueue(&router, json!({ "budget_usd": 0.25 })).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(
        body["budget_usd"], 0.25,
        "a positive ceiling is stored verbatim, not clamped: {body}"
    );

    // Omitting the field is the documented "no ceiling" request; this fix
    // narrows nothing, it only refuses the values that used to *become* it.
    let (status, body) = enqueue(&router, json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert!(body["budget_usd"].is_null(), "{body}");
}
