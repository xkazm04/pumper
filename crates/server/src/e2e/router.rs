//! The HTTP surface, driven through the real router with `oneshot` — no bound
//! port. Pins the behaviors the spec-inventory test cannot see: cursor paging
//! over a page boundary, the error envelope, and preview validation.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::EnqueueOptions;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::routes;

async fn get_json(router: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

#[tokio::test]
async fn jobs_cursor_paging_returns_every_row_exactly_once() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    for i in 0..5 {
        state
            .storage
            .enqueue("fake", EnqueueOptions { params: json!({ "i": i }), ..Default::default() })
            .await
            .unwrap();
    }
    let router = routes::router(state);

    let mut seen: Vec<String> = Vec::new();
    let mut cursor = String::new();
    for _page in 0..4 {
        let uri = format!("/jobs?cursor={cursor}&limit=2");
        let (status, body) = get_json(&router, &uri).await;
        assert_eq!(status, StatusCode::OK);
        let items = body["items"].as_array().expect("cursor mode returns {items, next_cursor}");
        assert!(items.len() <= 2, "page size respects the limit");
        seen.extend(items.iter().map(|j| j["id"].as_str().unwrap().to_string()));
        match body["next_cursor"].as_str() {
            Some(next) => cursor = urlencoding_encode(next),
            None => break,
        }
    }
    assert_eq!(seen.len(), 5, "every job appears exactly once across pages");
    let mut dedup = seen.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(dedup.len(), 5, "no duplicates across page boundaries");
}

/// Minimal percent-encoding for the cursor's `|` separator (enough for tests).
fn urlencoding_encode(s: &str) -> String {
    s.replace('|', "%7C").replace('+', "%2B")
}

#[tokio::test]
async fn missing_job_returns_the_error_envelope_not_a_bare_status() {
    let (state, _store) = test_state(vec![]).await;
    let router = routes::router(state);
    let (status, body) =
        get_json(&router, "/jobs/00000000-0000-0000-0000-000000000000").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].is_string(), "error envelope carries a message: {body}");
}

#[tokio::test]
async fn extract_preview_rejects_a_bad_ruleset_with_400() {
    let (state, _store) = test_state(vec![]).await;
    let router = routes::router(state);
    let req = Request::builder()
        .method("POST")
        .uri("/extract/preview")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "html": "<p>x</p>",
                "rules": { "broken": { "type": "regex", "pattern": "(" } }
            }))
            .unwrap(),
        ))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "client-input rejection is 400 (Error::BadRequest), never 500: {}",
        String::from_utf8_lossy(&body)
    );
}
