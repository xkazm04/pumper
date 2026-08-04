//! `POST /ingest/{id}` from the outside: every refusal gate in order, and the
//! replay posture of the bare (timestamp-less, GitHub-style) signature scheme.
//!
//! This is the only route designed for non-localhost callers, so each gate is
//! pinned by status code — a gate that silently stops gating would otherwise be
//! invisible until someone abused it.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::config::Config;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state_with, FakeApp};
use crate::routes;
use crate::state::AppState;

const SECRET: &str = "hush";

/// A test state with ingress ON, plus any per-test config tweak.
async fn ingress_state(
    tweak: impl FnOnce(&mut Config),
) -> (AppState, pumper_core::testing::TempStore) {
    test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.ingress.enabled = true;
        tweak(c);
    })
    .await
}

/// GitHub's scheme: `HMAC-SHA256(secret, body)`, hex, `sha256=`-prefixed.
fn bare_signature(secret: &str, body: &[u8]) -> String {
    use hmac::{Mac, SimpleHmac};
    let mut mac = <SimpleHmac<sha2::Sha256>>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// POSTs to `/ingest/{id}` with the given headers.
async fn ingest(
    router: &axum::Router,
    id: &str,
    body: &[u8],
    headers: &[(&str, String)],
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/ingest/{id}"))
        .header("content-type", "application/json");
    for (name, value) in headers {
        req = req.header(*name, value);
    }
    let resp = router
        .clone()
        .oneshot(req.body(Body::from(body.to_vec())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Signed with the bare scheme, which is the one with no replay clock.
async fn ingest_bare(router: &axum::Router, id: &str, body: &[u8]) -> (StatusCode, Value) {
    ingest(
        router,
        id,
        body,
        &[("x-pumper-signature", bare_signature(SECRET, body))],
    )
    .await
}

async fn source(state: &AppState) -> pumper_core::IngressSource {
    state
        .storage
        .create_ingress_source("github", SECRET)
        .await
        .expect("create ingress source")
}

#[tokio::test]
async fn ingest_refuses_before_it_verifies_anything() {
    // 409 — the master switch, checked before the source even loads.
    let (state, _s) = test_state_with(vec![Arc::new(FakeApp)], |_| {}).await;
    let src = source(&state).await;
    let id = src.id.clone();
    let router = routes::router(state);
    let (status, body) = ingest_bare(&router, &id, b"{}").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    // 404 — unknown source id.
    let (state, _s) = ingress_state(|_| {}).await;
    let router = routes::router(state);
    let (status, _) = ingest_bare(&router, "no-such-source", b"{}").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // 403 — a real but disabled source.
    let (state, _s) = ingress_state(|_| {}).await;
    let src = source(&state).await;
    state
        .storage
        .set_ingress_source_enabled(&src.id, false)
        .await
        .unwrap();
    let id = src.id.clone();
    let router = routes::router(state);
    let (status, _) = ingest_bare(&router, &id, b"{}").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn oversized_body_is_413_before_the_signature_is_checked() {
    let (state, _s) = ingress_state(|c| c.ingress.max_body_bytes = 16).await;
    let src = source(&state).await;
    let id = src.id.clone();
    let router = routes::router(state);
    let big = json!({ "pad": "x".repeat(64) }).to_string().into_bytes();
    // Deliberately unsigned: the size gate must not depend on crypto running.
    let (status, _) = ingest(&router, &id, &big, &[]).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn per_source_rate_limit_answers_429_once_the_burst_is_spent() {
    let (state, _s) = ingress_state(|c| c.ingress.rate_limit_per_min = 2).await;
    let src = source(&state).await;
    let id = src.id.clone();
    let router = routes::router(state);
    // Distinct bodies so nothing is refused for being a replay.
    for i in 0..2 {
        let body = json!({ "i": i }).to_string().into_bytes();
        let (status, b) = ingest_bare(&router, &id, &body).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{b}");
    }
    let body = json!({ "i": 99 }).to_string().into_bytes();
    let (status, _) = ingest_bare(&router, &id, &body).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn a_wrong_signature_and_a_stale_timestamp_are_both_401() {
    let (state, _s) = ingress_state(|c| c.ingress.max_skew_secs = 60).await;
    let src = source(&state).await;
    let id = src.id.clone();
    let router = routes::router(state);
    let body = br#"{"ok":true}"#;

    // No signature at all.
    let (status, _) = ingest(&router, &id, body, &[]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // A signature over a DIFFERENT body (the capture-and-edit case).
    let (status, _) = ingest(
        &router,
        &id,
        body,
        &[("x-pumper-signature", bare_signature(SECRET, b"{}"))],
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Right secret, right body — but the pumper scheme's clock says this
    // capture is hours old. Rejected before the MAC is even computed.
    let stale = chrono::Utc::now().timestamp() - 7200;
    let sig = crate::webhook::sign(SECRET.as_bytes(), stale, "d-1", body);
    let (status, err) = ingest(
        &router,
        &id,
        body,
        &[
            ("x-pumper-signature", format!("sha256={sig}")),
            ("x-pumper-timestamp", stale.to_string()),
            ("x-pumper-delivery-id", "d-1".to_string()),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(
        err["error"].as_str().unwrap_or_default().contains("skew"),
        "the refusal names the clock, not the key: {err}"
    );
    // The same signature INSIDE the window is accepted, so the test above is
    // about staleness rather than about a broken signing helper.
    let fresh = chrono::Utc::now().timestamp();
    let sig = crate::webhook::sign(SECRET.as_bytes(), fresh, "d-1", body);
    let (status, _) = ingest(
        &router,
        &id,
        body,
        &[
            ("x-pumper-signature", format!("sha256={sig}")),
            ("x-pumper-timestamp", fresh.to_string()),
            ("x-pumper-delivery-id", "d-1".to_string()),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
}

/// The replay this direction exists for: the bare scheme has no timestamp and
/// no nonce, so a captured signed body verifies forever. It used to mint a
/// fresh random event id each time and enqueue a fresh job on every replay.
#[tokio::test]
async fn an_unidentified_replay_dedupes_instead_of_enqueuing_forever() {
    let (state, _s) = ingress_state(|_| {}).await;
    let src = source(&state).await;
    state
        .storage
        .create_trigger(&pumper_core::NewTrigger {
            name: Some("any-event"),
            source_kind: "external",
            source_app: &src.id,
            source_dataset: None,
            on_change: None,
            on_status: None,
            target_app: "fake",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: None,
        })
        .await
        .unwrap();
    let id = src.id.clone();
    let storage = state.storage.clone();
    let router = routes::router(state);

    let body = br#"{"ref":"refs/heads/main"}"#;
    let (status, first) = ingest_bare(&router, &id, body).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(first["triggers_fired"], 1);

    // Byte-identical capture, replayed twice more.
    for _ in 0..2 {
        let (status, again) = ingest_bare(&router, &id, body).await;
        assert_eq!(status, StatusCode::ACCEPTED, "a replay still verifies");
        assert_eq!(
            again["event_id"], first["event_id"],
            "…and is the SAME event, derived from the body"
        );
        assert_eq!(
            again["triggers_fired"], 0,
            "so it enqueues nothing the second time"
        );
    }
    let jobs = storage.list(Some("fake"), None, 100).await.unwrap();
    assert_eq!(jobs.len(), 1, "one job for one distinct event, not three");

    // A genuinely different body is a different event and does fire.
    let other = br#"{"ref":"refs/heads/dev"}"#;
    let (_, new_event) = ingest_bare(&router, &id, other).await;
    assert_ne!(new_event["event_id"], first["event_id"]);
    assert_eq!(new_event["triggers_fired"], 1);
}
