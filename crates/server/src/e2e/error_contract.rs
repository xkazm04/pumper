//! The error envelope over the wire: `{error, code}` with the `code` a client
//! is told to branch on.
//!
//! The unit tests in `routes::error` pin the map and the redaction; this file
//! pins that real responses from the real router carry it — the gap that let
//! `403`, `429` and the five detection-off `503`s all ship as `"internal"`
//! while a doc sentence claimed the map was "kept in lockstep".

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::test_state_with;
use crate::routes;

async fn send(router: &axum::Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// All five health routes answer `503` when `[resilience] enabled = false` —
/// documented, deliberate, and previously indistinguishable from a crash.
///
/// "Detection is off" is a *configuration* answer: the caller should stop asking
/// (or tell the operator to switch it on), not retry, not page anyone. A
/// `"code": "internal"` said the opposite.
#[tokio::test]
async fn the_detection_off_503s_report_unavailable_not_internal() {
    let (state, _store) = test_state_with(vec![], |c| c.resilience.enabled = false).await;
    assert!(
        state.health.store().is_none(),
        "detection must really be off, or these routes answer 200 and prove nothing"
    );
    let router = routes::router(state);

    let gets = [
        "/sources",
        "/sources/fake%2Fitems",
        "/sources/fake%2Fitems/runs",
        "/enforcement/preview",
    ];
    for uri in gets {
        let (status, body) = send(&router, "GET", uri, Value::Null).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{uri}: {body}");
        assert_eq!(body["code"], "unavailable", "{uri}: {body}");
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|e| e.contains("[resilience] enabled")),
            "{uri}: the message must name the switch to flip: {body}"
        );
    }

    let (status, body) = send(
        &router,
        "POST",
        "/sources/fake%2Fitems/state",
        json!({ "state": "healthy" }),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], "unavailable", "{body}");
}

/// `unknown` is a state the API **renders** and never **accepts**.
///
/// It is what a health read that failed reports — the honest replacement for the
/// `healthy` a failed read used to claim. Letting an operator POST it would
/// write the string into `sources.state`, manufacturing the very unreadable row
/// the value exists to describe, and the next read would report `unknown` about
/// a row that is perfectly readable and says so.
#[tokio::test]
async fn unknown_is_a_rendered_state_the_operator_cannot_set() {
    let (state, _store) = test_state_with(vec![], |_| {}).await;
    assert!(
        state.health.store().is_some(),
        "detection must be on, or this route answers 503 and proves nothing"
    );
    let router = routes::router(state);

    for bad in ["unknown", "not-a-state", ""] {
        let (status, body) = send(
            &router,
            "POST",
            "/sources/fake%2Fitems/state",
            json!({ "state": bad }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "state {bad:?} was not refused: {body}"
        );
        assert!(
            body["error"].as_str().is_some_and(|e| e.contains(bad)),
            "the refusal must echo what was sent: {body}"
        );
    }

    // …and a real rung is still refused only for the right reason (no such
    // source), not for being unparseable — otherwise the guard above would pass
    // by rejecting everything.
    let (status, body) = send(
        &router,
        "POST",
        "/sources/fake%2Fitems/state",
        json!({ "state": "quarantined" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

/// The codes that were already right stay right — the widened map must not have
/// renamed anything a consumer already branches on (`@pumper/sync` reads
/// `.code` and nothing else).
#[tokio::test]
async fn the_pre_existing_codes_are_unchanged() {
    let (state, _store) = test_state_with(vec![], |_| {}).await;
    let router = routes::router(state);

    let (status, body) = send(&router, "GET", "/jobs/not-a-uuid", Value::Null).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, body) = send(
        &router,
        "GET",
        "/jobs/00000000-0000-0000-0000-000000000000",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "not_found", "{body}");

    // Every envelope carries BOTH keys — a client that reads `code` must never
    // find it absent, which is what makes branching on it safe.
    assert!(body["error"].is_string(), "{body}");
}
