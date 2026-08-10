//! Host-weather (M01 v1) over the real router: the export floor, the
//! dry-run-by-default purity of import, and the conservative apply merge.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::test_state;
use crate::routes;
use crate::state::AppState;

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

async fn post_json(router: &axum::Router, uri: &str, body: &Value) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Pins `pinned.example` (3 losses) and gives `thin.example` a single loss —
/// 3 vs 1 observations, so the default export floor separates them.
async fn seed(state: &AppState) {
    for _ in 0..3 {
        state
            .tiers
            .record("pinned.example", "browser", true)
            .await
            .unwrap();
    }
    state
        .tiers
        .record("thin.example", "browser", true)
        .await
        .unwrap();
}

fn bundle(entries: Value) -> Value {
    json!({
        "schema": "pumper.host-weather/1",
        "node_id": "peer-node",
        "generated_at": "2026-07-31T00:00:00Z",
        "entries": entries,
    })
}

#[tokio::test]
async fn export_applies_the_observation_floor_and_carries_the_schema() {
    let (state, _store) = test_state(vec![]).await;
    seed(&state).await;
    let router = routes::router(state);

    // Default floor (3): only the well-observed host travels.
    let (status, body) = get_json(&router, "/host-weather/export").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["schema"], "pumper.host-weather/1");
    assert!(body["node_id"].is_string() && body["generated_at"].is_string());
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "thin host must not travel: {body}");
    assert_eq!(entries[0]["host"], "pinned.example");
    assert_eq!(entries[0]["preferred_tier"], "browser");
    assert_eq!(entries[0]["observations"], 3);
    assert_eq!(entries[0]["challenge_fingerprints"], json!([]));

    // Floor 1 includes the thin host too.
    let (_, body) = get_json(&router, "/host-weather/export?min_observations=1").await;
    assert_eq!(body["entries"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn import_is_a_dry_run_by_default_and_writes_nothing() {
    let (state, _store) = test_state(vec![]).await;
    let tiers = state.tiers.clone();
    let governor = state.governor.clone();
    let router = routes::router(state);

    let b = bundle(json!([{
        "host": "new.example",
        "preferred_tier": "browser",
        "http_strikes": 3,
        "penalty_ms": 5000,
        "observations": 10,
    }]));
    let (status, body) = post_json(&router, "/host-weather/import", &b).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["applied"], false, "apply must default to false");
    assert_eq!(body["source_node_id"], "peer-node");
    assert_eq!(body["changed"], 1);
    assert_eq!(body["actions"][0]["adopt_pin"], true);
    assert_eq!(body["actions"][0]["raise_penalty_ms"], 5000);

    // Purity: the dry run left no trace in tier memory or the governor.
    assert!(tiers.get("new.example").await.unwrap().is_none());
    assert!(governor.penalty("new.example").await.is_zero());
    // And a wrong schema is rejected outright.
    let (status, _) = post_json(
        &router,
        "/host-weather/import",
        &json!({"schema": "pumper.host-weather/99", "entries": []}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn apply_merges_conservatively() {
    let (state, _store) = test_state(vec![]).await;
    seed(&state).await; // pinned.example: pin + 3 obs; thin.example: 1 strike + 1 obs
    let tiers = state.tiers.clone();
    let governor = state.governor.clone();
    let router = routes::router(state);

    let b = bundle(json!([
        // Better-observed remote pin over an unknown host: adopted.
        {"host": "NEW.example", "preferred_tier": "browser", "http_strikes": 3,
         "penalty_ms": 600000, "observations": 10},
        // Remote says our pinned host is fine — a local pin is never downgraded.
        {"host": "pinned.example", "preferred_tier": null, "http_strikes": 0,
         "penalty_ms": 0, "observations": 999},
        // Fewer-observed remote pin over our thin host: strikes rise (capped
        // below the pin threshold), the pin itself is NOT adopted (1 obs each).
        {"host": "thin.example", "preferred_tier": "browser", "http_strikes": 5,
         "penalty_ms": 0, "observations": 1},
    ]));
    let (status, body) = post_json(&router, "/host-weather/import?apply=true", &b).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["applied"], true);
    assert_eq!(body["changed"], 2, "pinned.example is a no-op: {body}");

    // Adopted pin, with the imported penalty capped at the 60s severity cap.
    let new = tiers.get("new.example").await.unwrap().unwrap();
    assert_eq!(new.preferred_tier.as_deref(), Some("browser"));
    assert_eq!(
        new.observations, 0,
        "imports never fabricate local evidence"
    );
    assert_eq!(
        governor.penalty("new.example").await,
        std::time::Duration::from_secs(60),
        "imported penalty must be capped"
    );
    assert_eq!(
        new.penalty_ms, 60_000,
        "capped penalty persisted to the snapshot"
    );

    // The local pin survived the remote all-clear.
    let pinned = tiers.get("pinned.example").await.unwrap().unwrap();
    assert_eq!(pinned.preferred_tier.as_deref(), Some("browser"));

    // Strikes rose to the sub-threshold cap; no pin without local confirmation.
    let thin = tiers.get("thin.example").await.unwrap().unwrap();
    assert_eq!(thin.http_strikes, 2);
    assert_eq!(
        thin.preferred_tier, None,
        "imported strikes alone must not pin"
    );

    // Re-importing the same bundle is idempotent: everything is now dominated.
    let (_, body) = post_json(&router, "/host-weather/import?apply=true", &b).await;
    assert_eq!(body["changed"], 0, "second apply must be a no-op: {body}");
}
