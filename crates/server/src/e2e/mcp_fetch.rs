//! N15 end-to-end: the self-hosted agent loop's `fetch` MCP tool.
//!
//! What these prove, in the order the feature's trust chain runs: a job token
//! is required and its every refusal is typed; a token that resolves puts the
//! fetch through the calling job's own metered `AppContext::fetch`; and the
//! spend plus the marker land on that job — so `GET /jobs/{id}/receipt` reports
//! `self_hosted_fetches` instead of the zero-by-construction it reported for
//! every research run before this item.
//!
//! No `claude` subprocess and no network: the HTTP engine is scripted, which is
//! also the only way to assert on *which* tier answered.

use std::collections::HashMap;
use std::sync::Arc;

use pumper_core::agent_tools;
use pumper_core::config::{Config, GovernorConfig};
use pumper_core::testing::{engines_with, Dead, TempStore};
use pumper_core::{
    EnqueueOptions, Governor, HttpClient, HttpRequest, HttpResponse, JobStatus, NoPlugins,
    NoSearch, Result,
};
use serde_json::{json, Value};

use crate::mcp::{handle_rpc_as, jobtoken::McpCaller};
use crate::state::{AppState, AppStateParts};

/// An HTTP engine that answers every request with one scripted body — enough
/// for the tiered fetcher to declare the http tier the winner, and enough to
/// tell "the tool fetched" from "the tool returned something plausible".
struct ScriptedHttp {
    body: String,
}

#[async_trait::async_trait]
impl HttpClient for ScriptedHttp {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: self.body.clone(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// A headless state whose http tier serves `body`. Deliberately not the shared
/// `test_state` helper: that one wires `Dead` engines, and a fetch tool tested
/// against an engine that panics on use can only ever test its refusals.
async fn fetch_state(body: &str) -> (AppState, TempStore) {
    let store = TempStore::new("mcp-fetch-e2e").await;
    let mut config = Config::default();
    config.storage.database_path = store.path().join("pumper.db");
    config.storage.artifacts_dir = store.path().join("artifacts");
    config.mcp.enabled = true;
    // The tool is not behind `allow_enqueue`: it enqueues nothing, and the job
    // token is the gate. Left OFF here on purpose, so this suite also proves
    // that.
    config.mcp.allow_enqueue = false;
    let engines = engines_with(
        Arc::new(ScriptedHttp {
            body: body.to_string(),
        }),
        Arc::new(Dead),
        Arc::new(Dead),
    );
    let state = AppState::from_parts(AppStateParts {
        config,
        storage: Arc::new(store.storage.clone()),
        governor: Arc::new(Governor::new(&GovernorConfig::default())),
        engines,
        plugins: Arc::new(NoPlugins),
        search: Arc::new(NoSearch),
        registry: HashMap::new(),
    })
    .expect("assemble AppState");
    (state, store)
}

fn call(args: Value) -> Value {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "fetch", "arguments": args }
    })
}

/// A `running` job to spend against, the way the worker leaves one.
async fn running_job(state: &AppState) -> uuid::Uuid {
    let job = state
        .storage
        .enqueue("research", EnqueueOptions::default())
        .await
        .expect("enqueue");
    let claimed = state
        .storage
        .claim_next(&[], 0.0)
        .await
        .expect("claim")
        .expect("a queued job");
    assert_eq!(claimed.id, job.id);
    assert_eq!(claimed.status, JobStatus::Running);
    job.id
}

fn tool_error(resp: &Value) -> String {
    assert_eq!(
        resp["result"]["isError"], true,
        "expected a tool error, got {resp}"
    );
    resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// The tool is advertised, so a self-hosted subprocess can discover the one
/// network tool it has — and its schema says a job token is required.
#[tokio::test]
async fn the_fetch_tool_is_listed_even_with_enqueue_off() {
    let (state, _store) = fetch_state("<p>hello</p>").await;
    let resp = handle_rpc_as(
        &state,
        &json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
        &McpCaller::anonymous(),
    )
    .await
    .expect("response");
    let tools = resp["result"]["tools"].as_array().expect("tools");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"fetch"), "{names:?}");
    assert_eq!(
        names.last(),
        Some(&"fetch"),
        "the wave-2 rule is that J's tool is appended LAST: {names:?}"
    );
    let def = tools.iter().find(|t| t["name"] == "fetch").expect("fetch");
    assert!(
        def["description"]
            .as_str()
            .unwrap_or_default()
            .contains("job token"),
        "the schema must say what it needs: {def}"
    );
}

/// THE refusal: an ordinary MCP client — a person's Claude Desktop pointed at
/// this node — must not be able to spend a job's budget by calling this tool.
/// Every non-valid token shape is refused, and each says which fact it is.
#[tokio::test]
async fn a_fetch_without_a_live_job_token_is_refused() {
    let (state, _store) = fetch_state("<p>hello</p>").await;

    let missing = tool_error(
        &handle_rpc_as(
            &state,
            &call(json!({ "url": "https://example.test/a" })),
            &McpCaller::anonymous(),
        )
        .await
        .expect("response"),
    );
    assert!(missing.starts_with("[unauthorized]"), "{missing}");
    assert!(missing.contains("x-pumper-job-token"), "{missing}");

    let unknown = tool_error(
        &handle_rpc_as(
            &state,
            &call(json!({ "url": "https://example.test/a" })),
            &McpCaller::with_token("not-a-real-token"),
        )
        .await
        .expect("response"),
    );
    assert!(unknown.starts_with("[unauthorized]"), "{unknown}");
    assert!(unknown.contains("unknown job token"), "{unknown}");

    // A token minted with a zero TTL is born expired — the same code path a
    // token that outlived its run takes, without sleeping through a TTL.
    let expired = agent_tools::mint(uuid::Uuid::new_v4(), std::time::Duration::from_secs(0));
    let text = tool_error(
        &handle_rpc_as(
            &state,
            &call(json!({ "url": "https://example.test/a" })),
            &McpCaller::with_token(expired.secret()),
        )
        .await
        .expect("response"),
    );
    assert!(text.starts_with("[unauthorized]"), "{text}");
    assert!(text.contains("expired job token"), "{text}");
}

/// A token whose job has been finalized (or never started) must not buy a
/// fetch: the receipt for that run is already written, and its budget has
/// already been reported as final.
#[tokio::test]
async fn a_token_for_a_job_that_is_not_running_is_refused() {
    let (state, _store) = fetch_state("<p>hello</p>").await;
    let job = state
        .storage
        .enqueue("research", EnqueueOptions::default())
        .await
        .expect("enqueue");
    let token = agent_tools::mint(job.id, std::time::Duration::from_secs(60));
    let text = tool_error(
        &handle_rpc_as(
            &state,
            &call(json!({ "url": "https://example.test/a" })),
            &McpCaller::with_token(token.secret()),
        )
        .await
        .expect("response"),
    );
    assert!(text.starts_with("[conflict]"), "{text}");
    assert!(text.contains("queued"), "{text}");

    // And a token naming a job that no longer exists at all.
    let ghost = agent_tools::mint(uuid::Uuid::new_v4(), std::time::Duration::from_secs(60));
    let text = tool_error(
        &handle_rpc_as(
            &state,
            &call(json!({ "url": "https://example.test/a" })),
            &McpCaller::with_token(ghost.secret()),
        )
        .await
        .expect("response"),
    );
    assert!(text.starts_with("[not_found]"), "{text}");
}

/// The whole point: a fetch through the tool goes down THIS host's ladder and
/// is metered on the calling job — so the run's receipt can finally say how
/// much of its egress the model drove. Before N15 that number was zero by
/// construction, because the CLI's `WebFetch` left no row anywhere.
#[tokio::test]
async fn a_self_hosted_fetch_is_metered_on_the_calling_job() {
    let (state, _store) = fetch_state(
        "<html><body><p>the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog. the quick brown fox jumps over the lazy dog.</p></body></html>",
    )
    .await;
    let job_id = running_job(&state).await;
    let token = agent_tools::mint(job_id, std::time::Duration::from_secs(60));

    let resp = handle_rpc_as(
        &state,
        &call(json!({ "url": "https://example.test/page", "to_markdown": true })),
        &McpCaller::with_token(token.secret()),
    )
    .await
    .expect("response");
    assert_eq!(
        resp["result"]["isError"], false,
        "tool errored: {}",
        resp["result"]["content"][0]["text"]
    );
    let out = &resp["result"]["structuredContent"];
    assert_eq!(out["engine"], "http", "the scripted http tier won: {out}");
    assert_eq!(out["job_id"], job_id.to_string());
    assert!(
        out["content"].as_str().unwrap_or_default().contains("fox"),
        "{out}"
    );

    // The ledger: the priced row `AppContext::fetch` writes, plus the zero-cost
    // marker that makes it countable as a model-driven fetch.
    let events = state.costs.job_events(job_id).await.expect("events");
    assert_eq!(events.len(), 2, "{events:?}");
    assert!(
        events.iter().any(|e| e.engine == "http"),
        "the metered seam wrote its row: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| e.detail.as_deref() == Some(crate::mcp::SELF_HOSTED_FETCH_DETAIL)),
        "{events:?}"
    );

    // And the receipt counts it.
    let receipt = crate::routes::receipt::job_receipt(
        axum::extract::State(state.clone()),
        axum::extract::Path(job_id),
    )
    .await
    .expect("receipt");
    assert_eq!(receipt.0["cost"]["self_hosted_fetches"], 1);
}

/// A research tier that can ask for another research tier is an unbounded spend
/// loop with the job budget as its only brake. The refusal happens before any
/// job lookup, so it is not something a valid token unlocks.
#[tokio::test]
async fn the_agent_cannot_ask_the_ladder_to_re_enter_the_claude_tier() {
    let (state, _store) = fetch_state("<p>hello</p>").await;
    let job_id = running_job(&state).await;
    let token = agent_tools::mint(job_id, std::time::Duration::from_secs(60));
    let text = tool_error(
        &handle_rpc_as(
            &state,
            &call(json!({
                "url": "https://example.test/a",
                "strategy": "auto_with_research"
            })),
            &McpCaller::with_token(token.secret()),
        )
        .await
        .expect("response"),
    );
    assert!(text.contains("research tier"), "{text}");
    // Nothing was fetched, so nothing was billed.
    assert!(state
        .costs
        .job_events(job_id)
        .await
        .expect("events")
        .is_empty());
}
