//! The trigger decision ledger through the HTTP surface: a trigger that did
//! nothing must be able to say WHY, and two different "nothing"s must not look
//! alike.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::NewTrigger;
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
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Outcomes recorded against one trigger, newest first.
fn outcomes(body: &Value) -> Vec<&str> {
    body["decisions"]
        .as_array()
        .expect("decisions array")
        .iter()
        .map(|d| d["outcome"].as_str().expect("outcome string"))
        .collect()
}

async fn external_trigger(
    state: &crate::state::AppState,
    name: &str,
    source_id: &str,
    filters: Option<&[String]>,
) -> pumper_core::Trigger {
    state
        .storage
        .create_trigger(&NewTrigger {
            name: Some(name),
            source_kind: "external",
            source_app: source_id,
            source_dataset: None,
            on_change: None,
            on_status: None,
            target_app: "fake",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters,
            plugin_hooks: None,
        })
        .await
        .expect("create trigger")
}

/// Both triggers "did not fire" for the second event — one because its filter
/// did not match, one because the hop already existed. Before the ledger the
/// API reported an identical empty `runs` for the first and an unexplained
/// single job for the second.
#[tokio::test]
async fn a_filter_miss_is_distinguishable_from_a_dedup_suppression() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let src = state
        .storage
        .create_ingress_source("github", "hush")
        .await
        .unwrap();
    let picky = external_trigger(
        &state,
        "only-main",
        &src.id,
        Some(&["$.ref:eq:refs/heads/main".to_string()]),
    )
    .await;
    let open = external_trigger(&state, "any-push", &src.id, None).await;

    // One event that the picky trigger's filter rejects, delivered twice under
    // the SAME event id (a redelivery).
    let payload = json!({ "ref": "refs/heads/dev" });
    for _ in 0..2 {
        crate::triggers::fire_external_triggers(&state, &src.id, &src.name, "ev-1", &payload).await;
    }

    let router = routes::router(state);
    let (status, body) = get_json(&router, &format!("/triggers/{}/runs", picky.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 0, "the picky trigger enqueued nothing");
    assert_eq!(
        outcomes(&body),
        vec!["filter_miss", "filter_miss"],
        "…and says so, per delivery"
    );

    let (_, body) = get_json(&router, &format!("/triggers/{}/runs", open.id)).await;
    assert_eq!(body["count"], 1, "the open trigger fired exactly one hop");
    assert_eq!(
        outcomes(&body),
        vec!["dedup", "fired"],
        "the redelivery is recorded as a suppression, not as silence"
    );
    let fired = body["decisions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["outcome"] == "fired")
        .unwrap();
    assert_eq!(fired["event_id"], "ev-1");
    assert_eq!(fired["source_kind"], "external");
    assert!(
        fired["job_id"].as_str().is_some(),
        "a fire names the hop it enqueued: {fired}"
    );
}

/// The anti-pattern: a deleted or mistyped trigger answering `200 {count: 0}`,
/// which reads as "this trigger has never fired" rather than "no such trigger".
#[tokio::test]
async fn unknown_trigger_runs_is_404_not_an_empty_200() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state);
    let (status, body) = get_json(&router, "/triggers/no-such-trigger/runs").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].is_string(), "error envelope: {body}");
}

/// Decisions page like every other keyset list on this surface.
#[tokio::test]
async fn decisions_page_by_cursor_without_repeating_rows() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let src = state
        .storage
        .create_ingress_source("github", "hush")
        .await
        .unwrap();
    let picky = external_trigger(
        &state,
        "only-main",
        &src.id,
        Some(&["$.ref:eq:refs/heads/main".to_string()]),
    )
    .await;
    let payload = json!({ "ref": "refs/heads/dev" });
    for i in 0..5 {
        crate::triggers::fire_external_triggers(
            &state,
            &src.id,
            &src.name,
            &format!("ev-{i}"),
            &payload,
        )
        .await;
    }

    let router = routes::router(state);
    let mut seen: Vec<String> = Vec::new();
    let mut cursor = String::new();
    for _page in 0..4 {
        let uri = format!("/triggers/{}/runs?cursor={cursor}&limit=2", picky.id);
        let (status, body) = get_json(&router, &uri).await;
        assert_eq!(status, StatusCode::OK);
        let items = body["decisions"].as_array().unwrap();
        assert!(items.len() <= 2);
        seen.extend(items.iter().map(|d| d["id"].as_str().unwrap().to_string()));
        match body["next_cursor"].as_str() {
            Some(next) => cursor = next.replace('|', "%7C").replace('+', "%2B"),
            None => break,
        }
    }
    seen.sort();
    let total = seen.len();
    seen.dedup();
    assert_eq!(total, 5, "every decision appears across the pages");
    assert_eq!(seen.len(), 5, "and none of them twice");
}
