//! N20 identity gate, driven through the REAL router in both modes.
//!
//! The unit tests in `crate::auth` pin the pure route->scope map; these pin what
//! a client actually sees — the only thing that can prove `open` still behaves
//! as it did and that `keys` refuses with this service's error envelope rather
//! than a bare status.

use super::harness::{test_state, test_state_with, FakeApp};
use crate::auth::{generate_key, hash_key, OPERATOR_PRINCIPAL_ID, PUBLIC_PATHS};
use crate::routes;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use http_body_util::BodyExt;
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

async fn call(
    router: &axum::Router,
    method: &str,
    uri: &str,
    key: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(key) = key {
        builder = builder.header("authorization", format!("Bearer {key}"));
    }
    if method != "GET" {
        builder = builder.header("content-type", "application/json");
    }
    let body = if method == "GET" {
        Body::empty()
    } else {
        Body::from("{}")
    };
    let resp = router
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// The default. Every route answers with no credential at all, and the
/// caller the layer stamps is the synthetic operator — which is what makes
/// this a no-op for a node that never edits its config.
#[tokio::test]
async fn open_mode_needs_no_key_and_resolves_the_operator() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state);

    let (status, _) = call(&router, "GET", "/health", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&router, "GET", "/jobs", None).await;
    assert_eq!(status, StatusCode::OK, "reads stay open");
    let (status, body) = call(&router, "GET", "/principals", None).await;
    assert_eq!(status, StatusCode::OK, "even the identity surface");
    assert_eq!(body["mode"], "open");
    assert_eq!(body["caller"]["id"], OPERATOR_PRINCIPAL_ID);
    assert_eq!(
        body["caller"]["synthetic"], true,
        "an identity this server invented must not be rendered as an authenticated one"
    );
    let (status, _) = call(&router, "POST", "/apps/fake/jobs", None).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "the enqueue door is unchanged in open mode"
    );
}

fn keys_mode(config: &mut pumper_core::config::Config) {
    config.auth.mode = "keys".to_string();
}

/// The gate the card names: `mode = keys`, a scoped key refused on an
/// out-of-scope route with the error-code map, and allowed on its own.
#[tokio::test]
async fn keys_mode_refuses_out_of_scope_and_admits_in_scope() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], keys_mode).await;
    let key = generate_key();
    state
        .storage
        .create_principal(
            "fleet",
            &hash_key(&key),
            &["enqueue:fake".to_string()],
            None,
            None,
        )
        .await
        .unwrap();
    let router = routes::router(state);

    // In scope: its own app's enqueue door.
    let (status, _) = call(&router, "POST", "/apps/fake/jobs", Some(&key)).await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // Out of scope: a read it was never granted.
    let (status, body) = call(&router, "GET", "/jobs", Some(&key)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        body["code"], "forbidden",
        "the stable code map, not a bare status"
    );

    // Out of scope: another app's enqueue door.
    let (status, body) = call(&router, "POST", "/apps/other/jobs", Some(&key)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "forbidden");

    // Out of scope: the identity surface itself.
    let (status, body) = call(&router, "GET", "/principals", Some(&key)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "forbidden");
}

#[tokio::test]
async fn keys_mode_refuses_missing_and_unknown_keys_but_never_the_public_trio() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], keys_mode).await;
    let router = routes::router(state);

    let (status, body) = call(&router, "GET", "/jobs", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthorized");

    let (status, body) = call(&router, "GET", "/jobs", Some("not-a-real-key")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthorized");

    for path in PUBLIC_PATHS {
        let (status, _) = call(&router, "GET", path, None).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{path} must answer without a credential in keys mode too"
        );
    }
}

/// A disabled key is refused as `forbidden`, not `unauthorized`: it exists
/// and was recognised, which is a different fact for the caller than "who
/// are you" — the same distinction the ingress surface already makes.
#[tokio::test]
async fn disabled_key_is_forbidden_not_unauthorized() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], keys_mode).await;
    let key = generate_key();
    let principal = state
        .storage
        .create_principal(
            "retired",
            &hash_key(&key),
            &["admin".to_string()],
            None,
            None,
        )
        .await
        .unwrap();
    state
        .storage
        .set_principal_enabled(&principal.id, false)
        .await
        .unwrap();
    let router = routes::router(state);

    let (status, body) = call(&router, "GET", "/jobs", Some(&key)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "forbidden");
}

/// Rotation must actually revoke: the old key stops working the moment the
/// new one is issued. A rotation that left the previous key valid would be
/// a no-op dressed as a revocation.
#[tokio::test]
async fn rotation_revokes_the_previous_key() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], keys_mode).await;
    let old = generate_key();
    let principal = state
        .storage
        .create_principal(
            "rotating",
            &hash_key(&old),
            &["admin".to_string()],
            None,
            None,
        )
        .await
        .unwrap();
    let router = routes::router(state);

    let (status, body) = call(
        &router,
        "POST",
        &format!("/principals/{}/rotate", principal.id),
        Some(&old),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let new = body["key"]
        .as_str()
        .expect("the new key, shown once")
        .to_string();
    assert_ne!(new, old);

    let (status, _) = call(&router, "GET", "/principals", Some(&new)).await;
    assert_eq!(status, StatusCode::OK, "the new key works");
    let (status, _) = call(&router, "GET", "/principals", Some(&old)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "the old key does not");
}

/// Every mutating verb lands in the audit ledger — including one that was
/// REFUSED, which is the row an operator most wants after the fact.
#[tokio::test]
async fn mutating_verbs_are_audited_including_refusals() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], keys_mode).await;
    let key = generate_key();
    state
        .storage
        .create_principal(
            "fleet",
            &hash_key(&key),
            &["enqueue:fake".to_string()],
            None,
            None,
        )
        .await
        .unwrap();
    let router = routes::router(state.clone());

    call(&router, "POST", "/apps/fake/jobs", Some(&key)).await;
    call(&router, "POST", "/apps/other/jobs", Some(&key)).await;
    call(&router, "GET", "/jobs", Some(&key)).await;

    let entries = state.storage.list_audit(None, None, 50).await.unwrap();
    let actions: Vec<&str> = entries.iter().map(|e| e.action.as_str()).collect();
    assert!(
        actions.contains(&"POST /apps/fake/jobs"),
        "the accepted enqueue is audited: {actions:?}"
    );
    assert!(
        actions.contains(&"POST /apps/other/jobs"),
        "the REFUSED enqueue is audited too: {actions:?}"
    );
    assert!(
        !actions.iter().any(|a| a.starts_with("GET ")),
        "reads are not mutations and must not flood the ledger: {actions:?}"
    );
}

/// `open` mode audits too — "who deleted the dataset" is a question a
/// single-operator node also wants answered — but it must not INVENT a
/// principal id for the synthetic operator.
#[tokio::test]
async fn open_mode_audits_without_fabricating_a_principal() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state.clone());
    call(&router, "POST", "/apps/fake/jobs", None).await;

    let entries = state.storage.list_audit(None, None, 10).await.unwrap();
    let row = entries
        .iter()
        .find(|e| e.action == "POST /apps/fake/jobs")
        .expect("the enqueue is audited in open mode");
    assert_eq!(
        row.principal_id, None,
        "the operator is not a stored principal; a fabricated id would make an \
         unattributed node look attributed"
    );
}

/// The per-principal throttle refuses with `rate_limited`, and the refusal
/// is a fact about the KEY, not about the route.
#[tokio::test]
async fn exhausted_bucket_is_rate_limited() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], keys_mode).await;
    let key = generate_key();
    state
        .storage
        .create_principal(
            "throttled",
            &hash_key(&key),
            &["admin".to_string()],
            None,
            Some(2),
        )
        .await
        .unwrap();
    let router = routes::router(state);

    let mut sawteeth = Vec::new();
    for _ in 0..6 {
        let (status, body) = call(&router, "GET", "/jobs", Some(&key)).await;
        sawteeth.push((status, body["code"].as_str().unwrap_or("").to_string()));
    }
    assert!(
        sawteeth
            .iter()
            .any(|(s, c)| *s == StatusCode::TOO_MANY_REQUESTS && c == "rate_limited"),
        "a 2/min key must be throttled within six calls: {sawteeth:?}"
    );
}
