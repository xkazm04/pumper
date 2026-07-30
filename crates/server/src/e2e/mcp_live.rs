//! MCP live-surface tests: the `GET /mcp` SSE notification stream (handshake,
//! bridged bus events, replay + per-connection filters), the research sugar
//! tools' gated enqueue wiring, and `wait_job`'s terminal/timeout semantics.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::util::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::events::JobEvent;
use crate::mcp::handle_rpc;
use crate::state::AppState;

async fn mcp_state(allow_enqueue: bool) -> (AppState, pumper_core::testing::TempStore) {
    let (mut state, store) = test_state(vec![
        Arc::new(FakeApp),
        Arc::new(app_readable::Readable),
        Arc::new(app_research::Research),
    ])
    .await;
    let mut config = (*state.config).clone();
    config.mcp.enabled = true;
    config.mcp.allow_enqueue = allow_enqueue;
    config.mcp.max_job_budget_usd = 1.0;
    state.config = Arc::new(config);
    (state, store)
}

fn call(name: &str, args: Value) -> Value {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": name, "arguments": args }
    })
}

fn structured(resp: &Value) -> &Value {
    assert_eq!(
        resp["result"]["isError"], false,
        "tool errored: {}",
        resp["result"]["content"][0]["text"]
    );
    &resp["result"]["structuredContent"]
}

/// Reads SSE body frames until `needle` shows up (or panics on timeout);
/// returns everything read so far.
async fn read_until(body: &mut Body, needle: &str) -> String {
    let mut seen = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let frame = tokio::time::timeout_at(deadline, body.frame())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for '{needle}'; saw: {seen}"))
            .expect("stream ended before the expected frame")
            .expect("frame error");
        if let Some(data) = frame.data_ref() {
            seen.push_str(&String::from_utf8_lossy(data));
        }
        if seen.contains(needle) {
            return seen;
        }
    }
}

#[tokio::test]
async fn get_mcp_replays_filtered_jsonrpc_notifications() {
    let (state, _store) = mcp_state(false).await;
    // Two buffered events, different apps; the connection filters to `fake`.
    state
        .events
        .emit(JobEvent::new(uuid::Uuid::nil(), "fake", "queued"));
    state
        .events
        .emit(JobEvent::new(uuid::Uuid::nil(), "other", "queued"));

    let router = crate::mcp::router().with_state(state.clone());
    let resp = router
        .oneshot(
            Request::get("/mcp?app=fake")
                .header("accept", "text/event-stream")
                .header("last-event-id", "0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp.headers()["content-type"].to_str().unwrap().to_string();
    assert!(ct.starts_with("text/event-stream"), "{ct}");

    let mut body = resp.into_body();
    let seen = read_until(&mut body, "notifications/pumper/job").await;
    // The bridged frame is a JSON-RPC notification with the bus seq as SSE id.
    assert!(seen.contains("id: 1"), "{seen}");
    assert!(seen.contains(r#""jsonrpc":"2.0""#), "{seen}");
    assert!(seen.contains(r#""app":"fake""#), "{seen}");
    // The other app's event was filtered out of the replay burst.
    assert!(!seen.contains(r#""app":"other""#), "{seen}");
}

#[tokio::test]
async fn get_mcp_bridges_live_events_and_kind_filter_applies() {
    let (state, _store) = mcp_state(false).await;
    let router = crate::mcp::router().with_state(state.clone());
    // Connect first (no resume point): the handler subscribes when it runs.
    let resp = router
        .oneshot(
            Request::get("/mcp?kind=succeeded,failed")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Emitted after the subscription: the filtered-out kind first, then a keeper.
    state
        .events
        .emit(JobEvent::new(uuid::Uuid::nil(), "fake", "running"));
    state
        .events
        .emit(JobEvent::new(uuid::Uuid::nil(), "fake", "succeeded"));
    let mut body = resp.into_body();
    let seen = read_until(&mut body, r#""status":"succeeded""#).await;
    assert!(!seen.contains(r#""status":"running""#), "{seen}");
}

#[tokio::test]
async fn research_tools_are_gated_and_enqueue_through_the_clamped_path() {
    // Gated: without allow_enqueue the tools are withheld and calls name the switch.
    let (state, _store) = mcp_state(false).await;
    let resp = handle_rpc(
        &state,
        &json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
    )
    .await
    .unwrap();
    let names: Vec<&str> = resp["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert!(
        names.contains(&"wait_job"),
        "wait_job is read-only: always offered"
    );
    assert!(!names.contains(&"fetch_readable"));
    assert!(!names.contains(&"deep_research"));
    for tool in ["fetch_readable", "deep_research"] {
        let resp = handle_rpc(&state, &call(tool, json!({ "url": "x", "query": "x" })))
            .await
            .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("allow_enqueue"), "{text}");
    }

    // Opted in: fetch_readable enqueues a readable job carrying the url.
    let (state, _store) = mcp_state(true).await;
    let resp = handle_rpc(
        &state,
        &call("fetch_readable", json!({ "url": "https://example.com/a" })),
    )
    .await
    .unwrap();
    let out = structured(&resp);
    assert_eq!(out["created"], true);
    assert_eq!(out["job"]["app"], "readable");
    assert_eq!(out["job"]["params"]["url"], "https://example.com/a");

    // deep_research: the $100 ask is clamped to the $1 rail on the stored row
    // AND inside the app's own max_budget_usd param.
    let resp = handle_rpc(
        &state,
        &call(
            "deep_research",
            json!({ "query": "czech vat thresholds", "budget_usd": 100.0 }),
        ),
    )
    .await
    .unwrap();
    let out = structured(&resp);
    assert_eq!(out["job"]["app"], "research");
    assert_eq!(out["budget_usd"], 1.0);
    let job_id: uuid::Uuid = out["job"]["id"].as_str().unwrap().parse().unwrap();
    let job = state.storage.get(job_id).await.unwrap().unwrap();
    assert_eq!(job.budget_usd, Some(1.0));
    assert_eq!(job.params["query"], "czech vat thresholds");
    assert_eq!(job.params["max_budget_usd"], 1.0);

    // A missing required arg is a readable tool error, not a job.
    let resp = handle_rpc(&state, &call("deep_research", json!({})))
        .await
        .unwrap();
    assert_eq!(resp["result"]["isError"], true);
}

#[tokio::test]
async fn wait_job_times_out_at_the_config_cap_and_resolves_on_terminal_event() {
    let (mut state, _store) = mcp_state(true).await;
    let mut config = (*state.config).clone();
    config.mcp.wait_job_max_secs = 1; // cap every wait to 1s for the test
    state.config = Arc::new(config);

    // A job that will never run (no worker): a 999s ask is clamped to the cap.
    let resp = handle_rpc(
        &state,
        &call(
            "fetch_readable",
            json!({ "url": "https://example.com/slow" }),
        ),
    )
    .await
    .unwrap();
    let job_id = structured(&resp)["job"]["id"].as_str().unwrap().to_string();
    let started = tokio::time::Instant::now();
    let resp = handle_rpc(
        &state,
        &call("wait_job", json!({ "job_id": job_id, "timeout_secs": 999 })),
    )
    .await
    .unwrap();
    let out = structured(&resp);
    assert_eq!(out["timed_out"], true);
    assert_eq!(out["waited_secs"], 1);
    assert_eq!(out["status"], "queued");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "cap must bound the wait: {:?}",
        started.elapsed()
    );

    // A terminal transition on the bus resolves the wait early.
    let id: uuid::Uuid = out["job"]["id"].as_str().unwrap().parse().unwrap();
    let events = state.events.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        events.emit(JobEvent::new(id, "readable", "succeeded"));
    });
    let resp = handle_rpc(
        &state,
        &call("wait_job", json!({ "job_id": id.to_string() })),
    )
    .await
    .unwrap();
    let out = structured(&resp);
    assert_eq!(out["timed_out"], false);
    assert_eq!(out["status"], "succeeded");

    // Unknown ids and garbage ids are tool errors.
    let resp = handle_rpc(
        &state,
        &call(
            "wait_job",
            json!({ "job_id": uuid::Uuid::new_v4().to_string() }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(resp["result"]["isError"], true);
    let resp = handle_rpc(&state, &call("wait_job", json!({ "job_id": "not-a-uuid" })))
        .await
        .unwrap();
    assert_eq!(resp["result"]["isError"], true);
}
