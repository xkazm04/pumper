//! MCP surface + manifest-enforcement tests: the JSON-RPC dispatch over a
//! headless state, the enqueue tool's gating and budget rail, and the HTTP
//! enqueue path's 422 on schema-violating params.

use std::sync::Arc;

use pumper_core::{AppContext, AppManifest, CostClass, ManifestExample, Result, ScrapeApp};
use serde_json::{json, Value};

use super::harness::{test_state, FakeApp};
use crate::mcp::handle_rpc;
use crate::state::AppState;

/// A test app with a strict params schema, for the enforcement paths.
struct SchemaApp;

#[async_trait::async_trait]
impl ScrapeApp for SchemaApp {
    fn name(&self) -> &'static str {
        "schematic"
    }
    fn default_params(&self) -> Value {
        json!({ "rows": 10 })
    }
    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": { "type": "string", "minLength": 1 },
                    "rows": { "type": "integer", "maximum": 100 }
                }
            })),
            examples: vec![ManifestExample {
                description: "minimal",
                params: json!({ "query": "x" }),
            }],
            output_shape: None,
            cost_class: CostClass::Free,
        }
    }
    async fn run(&self, _ctx: AppContext) -> Result<Value> {
        Ok(json!({ "ok": true }))
    }
}

async fn mcp_state(allow_enqueue: bool) -> (AppState, pumper_core::testing::TempStore) {
    let (state, store, _) = mcp_state_recording(allow_enqueue).await;
    (state, store)
}

/// A `Search` that answers nothing and remembers the last `SearchRequest` it was
/// handed — the only way to assert that a tool's arguments reached the query
/// layer intact rather than being silently dropped on the floor.
#[derive(Default)]
struct RecordingSearch {
    last: std::sync::Mutex<Option<pumper_core::SearchRequest>>,
}

#[async_trait::async_trait]
impl pumper_core::Search for RecordingSearch {
    async fn index(&self, _docs: Vec<pumper_core::SearchDoc>) -> Result<()> {
        Ok(())
    }
    async fn query(&self, req: pumper_core::SearchRequest) -> Result<pumper_core::SearchResponse> {
        *self.last.lock().unwrap() = Some(req);
        Ok(pumper_core::SearchResponse::default())
    }
    async fn delete_ids(&self, _ids: &[String]) -> Result<()> {
        Ok(())
    }
    async fn delete_dataset(&self, _app: &str, _dataset: &str) -> Result<()> {
        Ok(())
    }
    async fn doc_count(&self) -> Result<u64> {
        Ok(0)
    }
}

async fn mcp_state_recording(
    allow_enqueue: bool,
) -> (
    AppState,
    pumper_core::testing::TempStore,
    Arc<RecordingSearch>,
) {
    let search = Arc::new(RecordingSearch::default());
    let (mut state, store) = super::harness::test_state_indexed(
        vec![Arc::new(FakeApp), Arc::new(SchemaApp)],
        search.clone(),
        |_| {},
    )
    .await;
    let mut config = (*state.config).clone();
    config.mcp.enabled = true;
    config.mcp.allow_enqueue = allow_enqueue;
    config.mcp.max_job_budget_usd = 1.0;
    state.config = Arc::new(config);
    (state, store, search)
}

fn call(name: &str, args: Value) -> Value {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": name, "arguments": args }
    })
}

/// The structured result of a successful (non-isError) tool call.
fn structured(resp: &Value) -> &Value {
    assert_eq!(
        resp["result"]["isError"], false,
        "tool errored: {}",
        resp["result"]["content"][0]["text"]
    );
    &resp["result"]["structuredContent"]
}

#[tokio::test]
async fn initialize_negotiates_a_supported_protocol_version() {
    let (state, _store) = mcp_state(false).await;
    // A supported requested version is echoed back.
    let resp = handle_rpc(
        &state,
        &json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": "2025-03-26" }
        }),
    )
    .await
    .expect("requests get responses");
    assert_eq!(resp["result"]["protocolVersion"], "2025-03-26");
    assert!(resp["result"]["capabilities"]["tools"].is_object());
    // An unknown one gets the newest we speak, not an error.
    let resp = handle_rpc(
        &state,
        &json!({
            "jsonrpc": "2.0", "id": 2, "method": "initialize",
            "params": { "protocolVersion": "1999-01-01" }
        }),
    )
    .await
    .unwrap();
    assert_eq!(resp["result"]["protocolVersion"], "2025-06-18");
    // notifications/initialized is a notification: no response at all.
    assert!(handle_rpc(
        &state,
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })
    )
    .await
    .is_none());
}

#[tokio::test]
async fn tools_list_is_read_only_until_enqueue_is_opted_in() {
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
    // wait_job is read-only (awaits status, spends nothing) so it is always
    // offered; the actuating tools are not.
    assert_eq!(
        names,
        vec!["list_apps", "query_dataset", "search", "wait_job"]
    );

    // Calling the withheld tool is a readable tool error naming the switch.
    let resp = handle_rpc(&state, &call("enqueue_job", json!({ "app": "fake" })))
        .await
        .unwrap();
    assert_eq!(resp["result"]["isError"], true);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("allow_enqueue"), "{text}");

    // Opted in, the tool appears.
    let (state, _store2) = mcp_state(true).await;
    let resp = handle_rpc(
        &state,
        &json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
    )
    .await
    .unwrap();
    let tools = resp["result"]["tools"].as_array().unwrap();
    assert!(tools.iter().any(|t| t["name"] == "enqueue_job"));
}

#[tokio::test]
async fn list_apps_tool_serves_manifest_backed_tool_definitions() {
    let (state, _store) = mcp_state(false).await;
    let resp = handle_rpc(&state, &call("list_apps", json!({})))
        .await
        .unwrap();
    let tools = structured(&resp)["tools"].as_array().unwrap().clone();
    let schematic = tools.iter().find(|t| t["name"] == "schematic").unwrap();
    assert_eq!(schematic["inputSchema"]["required"][0], "query");
    assert_eq!(schematic["cost_class"], "free");
    assert_eq!(schematic["examples"][0]["params"]["query"], "x");
    // An app with no declared schema still presents a permissive object schema.
    let fake = tools.iter().find(|t| t["name"] == "fake").unwrap();
    assert_eq!(fake["inputSchema"]["type"], "object");
}

#[tokio::test]
async fn enqueue_tool_validates_clamps_and_enqueues() {
    let (state, _store) = mcp_state(true).await;
    // Schema violation (defaults merge leaves `query` missing) = tool error
    // with pointer-path detail, and no job row.
    let resp = handle_rpc(
        &state,
        &call(
            "enqueue_job",
            json!({ "app": "schematic", "params": { "rows": 500 } }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(resp["result"]["isError"], true);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("params/rows"), "{text}");
    assert!(text.contains("query"), "{text}");
    assert!(state.storage.list(None, None, 10).await.unwrap().is_empty());

    // Valid params: job lands, and the requested $100 budget is clamped to the
    // operator's $1 ceiling — on the stored row, not just the reply.
    let resp = handle_rpc(
        &state,
        &call(
            "enqueue_job",
            json!({ "app": "schematic", "params": { "query": "grants" }, "budget_usd": 100.0 }),
        ),
    )
    .await
    .unwrap();
    let out = structured(&resp);
    assert_eq!(out["created"], true);
    assert_eq!(out["budget_usd"], 1.0);
    let job_id: uuid::Uuid = out["job"]["id"].as_str().unwrap().parse().unwrap();
    let job = state.storage.get(job_id).await.unwrap().unwrap();
    assert_eq!(job.budget_usd, Some(1.0));
    assert_eq!(job.params["query"], "grants");
    assert_eq!(
        job.params["rows"], 10,
        "defaults shallow-merge under overrides"
    );
}

#[tokio::test]
async fn query_dataset_tool_reuses_the_filter_grammar() {
    let (state, _store) = mcp_state(false).await;
    state
        .datasets
        .upsert("fake", "d", "a", &json!({ "state": "CA", "n": 5 }))
        .await
        .unwrap();
    state
        .datasets
        .upsert("fake", "d", "b", &json!({ "state": "TX", "n": 50 }))
        .await
        .unwrap();
    let resp = handle_rpc(
        &state,
        &call(
            "query_dataset",
            json!({ "app": "fake", "dataset": "d", "filter": ["$.state:eq:CA"] }),
        ),
    )
    .await
    .unwrap();
    let out = structured(&resp);
    assert_eq!(out["count"], 1);
    assert_eq!(out["records"][0]["key"], "a");
    // A malformed spec surfaces the shared parser's message as a tool error.
    let resp = handle_rpc(
        &state,
        &call(
            "query_dataset",
            json!({ "app": "fake", "dataset": "d", "filter": ["state:eq:CA"] }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(resp["result"]["isError"], true);

    // Unknown methods and unknown tools are protocol errors, not tool results.
    let resp = handle_rpc(
        &state,
        &json!({ "jsonrpc": "2.0", "id": 9, "method": "nope" }),
    )
    .await
    .unwrap();
    assert_eq!(resp["error"]["code"], -32601);
    let resp = handle_rpc(&state, &call("nope", json!({}))).await.unwrap();
    assert_eq!(resp["error"]["code"], -32602);
}

/// The `search` tool's declared params. The anti-pattern this pins: the tool
/// advertising a strict subset of `GET /search` (it used to offer only
/// q/limit/app/dataset), so an agent could not page, sort, or use the entity
/// filters the REST surface has had since M14. EXPECTED-diff idiom — changing
/// the tool's surface must change this list.
const EXPECTED_SEARCH_PARAMS: &[&str] = &[
    "amount_gte",
    "amount_lte",
    "app",
    "dataset",
    "date_after",
    "date_before",
    "fuzzy",
    "limit",
    "offset",
    "q",
    "since",
    "sort",
];

#[tokio::test]
async fn search_tool_advertises_the_whole_http_query_surface_not_a_subset() {
    let (state, _store) = mcp_state(false).await;
    let resp = handle_rpc(
        &state,
        &json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
    )
    .await
    .unwrap();
    let tool = resp["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "search")
        .unwrap()
        .clone();
    let mut params: Vec<&str> = tool["inputSchema"]["properties"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    params.sort_unstable();
    assert_eq!(params, EXPECTED_SEARCH_PARAMS);
    // The vocabulary an agent must not have to guess at.
    assert_eq!(
        tool["inputSchema"]["properties"]["sort"]["enum"][0],
        "score"
    );
    assert_eq!(
        tool["inputSchema"]["properties"]["sort"]["enum"][1],
        "newest"
    );
    assert_eq!(
        tool["inputSchema"]["properties"]["offset"]["maximum"],
        crate::routes::SEARCH_MAX_OFFSET,
        "the advertised offset ceiling is the one the builder enforces"
    );
}

/// The anti-pattern: the tool accepting the new arguments in its schema but
/// dropping them on the way to the index (`fuzzy: false, since: None, …`
/// hardcoded), so the agent's filter silently does nothing.
#[tokio::test]
async fn search_tool_arguments_reach_the_query_not_the_floor() {
    let (state, _store, search) = mcp_state_recording(false).await;
    let resp = handle_rpc(
        &state,
        &call(
            "search",
            json!({
                "q": "rural health", "limit": 5, "offset": 10, "app": "grants",
                "dataset": "unified", "fuzzy": true, "sort": "newest",
                "since": 1_700_000_000i64, "amount_gte": 100_000, "amount_lte": 5_000_000,
                "date_after": 1_800_000_000i64, "date_before": 1_900_000_000i64
            }),
        ),
    )
    .await
    .unwrap();
    structured(&resp);
    let req = search.last.lock().unwrap().clone().expect("query ran");
    assert_eq!(req.q, "rural health");
    assert_eq!((req.limit, req.offset), (5, 10));
    assert_eq!(req.app.as_deref(), Some("grants"));
    assert_eq!(req.dataset.as_deref(), Some("unified"));
    assert!(req.fuzzy);
    assert_eq!(req.sort, pumper_core::SearchSort::Newest);
    assert_eq!(req.since, Some(1_700_000_000));
    assert_eq!(req.amount_gte, Some(100_000));
    assert_eq!(req.amount_lte, Some(5_000_000));
    assert_eq!(req.date_after, Some(1_800_000_000));
    assert_eq!(req.date_before, Some(1_900_000_000));
    assert!(
        !req.facets,
        "the tool returns hits only — facets cost a >=1000-doc sample"
    );

    // Defaults match the HTTP route's, because they come from the same builder.
    handle_rpc(&state, &call("search", json!({ "q": "grants" })))
        .await
        .unwrap();
    let req = search.last.lock().unwrap().clone().unwrap();
    assert_eq!((req.limit, req.offset), (20, 0));
    assert_eq!(req.sort, pumper_core::SearchSort::Score);
}

/// The shared builder's rejections must surface as readable tool errors, not as
/// a silent fallback or a protocol error.
#[tokio::test]
async fn search_tool_rejects_a_bad_sort_and_a_blank_query_like_the_http_route() {
    let (state, _store, search) = mcp_state_recording(false).await;
    for (args, needle) in [
        (json!({ "q": "x", "sort": "sideways" }), "sideways"),
        (json!({ "q": "   " }), "q"),
    ] {
        let resp = handle_rpc(&state, &call("search", args)).await.unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains(needle), "{text}");
    }
    assert!(
        search.last.lock().unwrap().is_none(),
        "a refused request never reaches the index"
    );
}

#[tokio::test]
async fn resources_serve_catalog_and_app_manifests() {
    let (state, _store) = mcp_state(false).await;
    let resp = handle_rpc(
        &state,
        &json!({ "jsonrpc": "2.0", "id": 1, "method": "resources/list" }),
    )
    .await
    .unwrap();
    let uris: Vec<&str> = resp["result"]["resources"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["uri"].as_str())
        .collect();
    assert!(uris.contains(&"pumper://catalog/sources"));
    assert!(uris.contains(&"pumper://apps/schematic/manifest"));

    let resp = handle_rpc(
        &state,
        &json!({
            "jsonrpc": "2.0", "id": 2, "method": "resources/read",
            "params": { "uri": "pumper://apps/schematic/manifest" }
        }),
    )
    .await
    .unwrap();
    let text = resp["result"]["contents"][0]["text"].as_str().unwrap();
    let manifest: Value = serde_json::from_str(text).unwrap();
    assert_eq!(manifest["name"], "schematic");
    assert_eq!(manifest["inputSchema"]["required"][0], "query");

    let resp = handle_rpc(
        &state,
        &json!({
            "jsonrpc": "2.0", "id": 3, "method": "resources/read",
            "params": { "uri": "pumper://nope" }
        }),
    )
    .await
    .unwrap();
    assert_eq!(resp["error"]["code"], -32002);
}

// ---- HTTP enqueue enforcement (the same schema, over the REST surface) ------

#[tokio::test]
async fn http_enqueue_422s_on_schema_violation_with_pointer_paths() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;

    let (state, _store) = test_state(vec![Arc::new(SchemaApp)]).await;
    let app = crate::routes::router(state.clone());

    // Violating params → 422 naming the pointer, and no job row.
    let resp = app
        .clone()
        .oneshot(
            Request::post("/apps/schematic/jobs")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"params":{"rows":500}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body: Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let msg = body["error"].as_str().unwrap();
    assert!(msg.contains("params/rows"), "{msg}");
    assert_eq!(body["code"], "unprocessable");
    assert!(state.storage.list(None, None, 10).await.unwrap().is_empty());

    // Valid params → 202 as before.
    let resp = app
        .oneshot(
            Request::post("/apps/schematic/jobs")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"params":{"query":"ok"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}
