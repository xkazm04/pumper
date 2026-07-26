//! The request-body ceiling, driven through the real router with `oneshot`.
//!
//! Two properties, both easy to get silently wrong: an over-limit body must be
//! *rejected* (413) rather than truncated into a body the handler then parses as
//! valid, and the scoped `/extract/preview` override must actually survive the
//! global layer rather than being clobbered by it.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::routes::{self, BODY_LIMIT_BYTES, PREVIEW_BODY_LIMIT_BYTES};

async fn post(router: &axum::Router, uri: &str, body: Vec<u8>) -> (StatusCode, String) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// A syntactically valid enqueue body whose `params` blob is `filler` bytes of
/// padding, so size is the only variable between the two cases below.
fn enqueue_body(filler: usize) -> Vec<u8> {
    serde_json::to_vec(&json!({ "params": { "pad": "x".repeat(filler) } })).unwrap()
}

/// The anti-pattern: a body past the ceiling silently truncated to the limit and
/// handed to the handler, which then acts on a partial request. axum's
/// `DefaultBodyLimit` must refuse it outright with 413 instead.
#[tokio::test]
async fn oversized_body_rejected_not_truncated() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state);

    let (status, _) = post(
        &router,
        "/apps/fake/jobs",
        enqueue_body(BODY_LIMIT_BYTES + 1),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "a body over BODY_LIMIT_BYTES must be rejected, never accepted or truncated"
    );
}

/// The other half of the guard: the ceiling is only useful if it is set well
/// clear of what real clients send. A normal enqueue — and a chunky-but-plausible
/// one — must still succeed.
#[tokio::test]
async fn legitimate_body_accepted_under_the_limit() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state);

    for filler in [0, 64 * 1024] {
        let (status, body) = post(&router, "/apps/fake/jobs", enqueue_body(filler)).await;
        assert!(
            status.is_success(),
            "a {filler}-byte-padded enqueue is legitimate traffic: {status} {body}"
        );
    }
}

/// `/extract/preview` takes a whole web page, so it carries a scoped override.
/// The anti-pattern this defends is the override being applied *outside* the
/// global layer, where the global 1 MiB would win and a legitimate page would
/// 413 through `html` while the same page previewed fine through `url`.
#[tokio::test]
async fn preview_override_outlives_the_global_limit_not_clobbered_by_it() {
    let (state, _store) = test_state(vec![]).await;
    let router = routes::router(state);

    // Comfortably over the global ceiling, comfortably under the preview one.
    let html = format!("<p>{}</p>", "x".repeat(2 * 1024 * 1024));
    let body = serde_json::to_vec(&json!({
        "html": html,
        "rules": { "t": { "type": "css", "selector": "p" } }
    }))
    .unwrap();
    assert!(body.len() > BODY_LIMIT_BYTES && body.len() < PREVIEW_BODY_LIMIT_BYTES);

    let (status, out) = post(&router, "/extract/preview", body).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a page-sized preview must clear the global limit via its scoped override: {out}"
    );
}

/// And the override is a *ceiling*, not an amnesty: past its own limit the
/// preview route rejects too.
#[tokio::test]
async fn preview_override_still_bounded_not_unlimited() {
    let (state, _store) = test_state(vec![]).await;
    let router = routes::router(state);

    let html = "x".repeat(PREVIEW_BODY_LIMIT_BYTES + 1);
    let body = serde_json::to_vec(&json!({ "html": html, "rules": {} })).unwrap();
    let (status, _) = post(&router, "/extract/preview", body).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}
