//! MCP server: the registry, datasets, and search as native agent tools.
//!
//! Implements the Model Context Protocol's **streamable-HTTP** transport by
//! hand rather than through the `rmcp` crate: the surface Pumper needs is a
//! small, stable JSON-RPC vocabulary (`initialize`, `tools/list`,
//! `tools/call`, `resources/list`, `resources/read`), the MCP spec explicitly
//! permits a stateless server that answers each `POST /mcp` with a single
//! `application/json` response (no SSE required), and hand-rolling those five
//! methods over the existing `AppState` is less code — and far less version
//! churn — than adapting rmcp's transport layer to this router. The whole
//! protocol lives in this module; swapping in a crate later is a local change.
//!
//! Mounted only when `[mcp] enabled = true` (default OFF), and read-mostly by
//! default: the one actuating tool, `enqueue_job`, sits behind its own
//! `[mcp] allow_enqueue` switch and clamps every job budget to
//! `[mcp] max_job_budget_usd`.
//!
//! **Notifications seam (deliberate)**: the EventBus replay ring could feed
//! MCP `notifications/*` to subscribed clients, but server-initiated messages
//! require the transport's SSE half (a `GET /mcp` stream + session ids).
//! Stateless POST keeps the surface trivially correct, so `GET /mcp` returns
//! 405 and agents poll `tools/call query_dataset` / `GET /events` (SSE)
//! instead. If subscriptions earn their keep, implement the GET stream here —
//! `EventBus::subscribe_with_replay` already provides resume semantics.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::state::AppState;

/// Protocol revisions this server speaks. The client's requested version is
/// echoed when supported; otherwise the newest supported one is offered.
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26"];

/// Rows a `query_dataset` tool call may return (also the default when the
/// agent asks for nothing) — mirrors the HTTP route's clamp.
const QUERY_LIMIT_CAP: i64 = 1000;
/// Hits a `search` tool call may return — mirrors `GET /search`.
const SEARCH_LIMIT_CAP: usize = 100;

/// The `/mcp` routes. Only merged into the main router when `[mcp] enabled`.
pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/mcp", post(handle_post).get(handle_get))
}

/// The transport's server→client stream half is deliberately not implemented
/// (see the module doc's notifications seam).
async fn handle_get() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [("allow", "POST")],
        "this MCP server is stateless: POST JSON-RPC messages to /mcp",
    )
        .into_response()
}

/// One streamable-HTTP exchange: a JSON-RPC request, notification, or batch in;
/// a JSON response (or 202 for notification-only input) out.
async fn handle_post(State(state): State<AppState>, Json(payload): Json<Value>) -> Response {
    match payload {
        Value::Array(msgs) => {
            let mut responses = Vec::new();
            for msg in &msgs {
                if let Some(resp) = handle_rpc(&state, msg).await {
                    responses.push(resp);
                }
            }
            if responses.is_empty() {
                StatusCode::ACCEPTED.into_response()
            } else {
                Json(Value::Array(responses)).into_response()
            }
        }
        msg => match handle_rpc(&state, &msg).await {
            Some(resp) => Json(resp).into_response(),
            None => StatusCode::ACCEPTED.into_response(),
        },
    }
}

/// Dispatches one JSON-RPC message. `None` = notification (nothing to send).
pub(crate) async fn handle_rpc(state: &AppState, msg: &Value) -> Option<Value> {
    let id = msg.get("id").cloned();
    let Some(method) = msg.get("method").and_then(Value::as_str) else {
        // A message with neither method nor id is garbage; with an id it is an
        // invalid request the client can correlate.
        return id
            .filter(|id| !id.is_null())
            .map(|id| rpc_error(id, -32600, "invalid request: missing 'method'"));
    };
    let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));

    // Notifications get handled (all are no-ops here) and produce no response.
    let Some(id) = id.filter(|id| !id.is_null()) else {
        return None;
    };

    let result = match method {
        "initialize" => Ok(initialize_result(&params)),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": server_tools(state) })),
        "tools/call" => return Some(tools_call(state, id, &params).await),
        "resources/list" => Ok(json!({ "resources": resources_list(state) })),
        "resources/read" => resources_read(state, &params),
        other => Err((-32601, format!("method '{other}' not found"))),
    };
    Some(match result {
        Ok(result) => rpc_result(id, result),
        Err((code, msg)) => rpc_error(id, code, &msg),
    })
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn initialize_result(params: &Value) -> Value {
    let requested = params.get("protocolVersion").and_then(Value::as_str);
    let version = requested
        .filter(|v| SUPPORTED_PROTOCOL_VERSIONS.contains(v))
        .unwrap_or(SUPPORTED_PROTOCOL_VERSIONS[0]);
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": {}, "resources": {} },
        "serverInfo": { "name": "pumper", "version": env!("CARGO_PKG_VERSION") },
        "instructions": "Local scraping / data-product service. Start with the list_apps tool \
            (every app's params schema, examples, and cost class), query stored data with \
            query_dataset (`$.path:op:value` filters) and search (full text). enqueue_job is \
            only offered when the operator has enabled [mcp] allow_enqueue; its budget_usd is \
            clamped to [mcp] max_job_budget_usd. Catalog + app manifests are resources.",
    })
}

// ---- Tools ------------------------------------------------------------------

/// The server's own MCP tools. `enqueue_job` — the only one that can spend
/// money and load targets — is offered only when the operator opted in.
fn server_tools(state: &AppState) -> Vec<Value> {
    let mut tools = vec![
        json!({
            "name": "list_apps",
            "description": "List every registered scraping app as an agent-ready tool \
                definition: params JSON Schema, worked examples, output shape, cost class \
                (free|metered|claude), schedule, and readiness (unmet credential \
                preconditions).",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false }
        }),
        json!({
            "name": "query_dataset",
            "description": "Query one app's stored dataset with optional `$.path:op:value` \
                filters (ops: eq | contains | gte | lte | numgte; all ANDed). Returns \
                change-detected records (key, data, first/last seen).",
            "inputSchema": {
                "type": "object",
                "required": ["app", "dataset"],
                "properties": {
                    "app": { "type": "string" },
                    "dataset": { "type": "string" },
                    "filter": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "e.g. [\"$.state:eq:CA\", \"$.award_ceiling:numgte:100000\"]"
                    },
                    "limit": { "type": "integer", "minimum": 1, "maximum": QUERY_LIMIT_CAP }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "search",
            "description": "Full-text search (BM25) across everything indexed from job \
                results, with highlighted snippets. Scope with app/dataset.",
            "inputSchema": {
                "type": "object",
                "required": ["q"],
                "properties": {
                    "q": { "type": "string", "minLength": 1 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": SEARCH_LIMIT_CAP },
                    "app": { "type": "string" },
                    "dataset": { "type": "string" }
                },
                "additionalProperties": false
            }
        }),
    ];
    if state.config.mcp.allow_enqueue {
        tools.push(json!({
            "name": "enqueue_job",
            "description": format!(
                "Enqueue one job for a registered app (see list_apps for each app's params \
                 schema and cost class). params shallow-merge over the app's defaults and are \
                 validated against its schema. budget_usd is clamped to the operator's \
                 [mcp] max_job_budget_usd ceiling (${:.2}); omitted = that ceiling.",
                state.config.mcp.max_job_budget_usd
            ),
            "inputSchema": {
                "type": "object",
                "required": ["app"],
                "properties": {
                    "app": { "type": "string" },
                    "params": { "type": "object" },
                    "budget_usd": { "type": "number", "minimum": 0 },
                    "idempotency_key": { "type": "string" }
                },
                "additionalProperties": false
            }
        }));
    }
    tools
}

/// `tools/call`: runs a tool and wraps the outcome per MCP — a *tool* failure
/// is a `result` with `isError: true` (the agent can read and react), while an
/// unknown tool or unusable arguments are protocol errors.
async fn tools_call(state: &AppState, id: Value, params: &Value) -> Value {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return rpc_error(id, -32602, "tools/call needs a 'name'");
    };
    let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
    let outcome = match name {
        "list_apps" => Ok(tool_list_apps(state)),
        "query_dataset" => tool_query_dataset(state, &args).await,
        "search" => tool_search(state, &args).await,
        "enqueue_job" if state.config.mcp.allow_enqueue => tool_enqueue(state, &args).await,
        "enqueue_job" => Err(
            "enqueue is disabled on this MCP surface — the operator must set \
             [mcp] allow_enqueue = true"
                .to_string(),
        ),
        other => return rpc_error(id, -32602, &format!("unknown tool '{other}'")),
    };
    let result = match outcome {
        Ok(value) => json!({
            "content": [{ "type": "text", "text": value.to_string() }],
            "structuredContent": value,
            "isError": false,
        }),
        Err(message) => json!({
            "content": [{ "type": "text", "text": message }],
            "isError": true,
        }),
    };
    rpc_result(id, result)
}

fn tool_list_apps(state: &AppState) -> Value {
    let mut apps: Vec<_> = state.registry.values().collect();
    apps.sort_by_key(|app| app.name());
    let tools: Vec<Value> = apps
        .into_iter()
        .map(|app| crate::registry::tool_definition(app.as_ref()))
        .collect();
    json!({ "tools": tools })
}

async fn tool_query_dataset(state: &AppState, args: &Value) -> Result<Value, String> {
    let app = require_str(args, "app")?;
    let dataset = require_str(args, "dataset")?;
    let specs: Vec<String> = args
        .get("filter")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    // The exact `?filter=` grammar the HTTP surface uses — one parser, no drift.
    let filters = crate::routes::parse_filters(&specs).map_err(|e| e.1)?;
    let limit = args
        .get("limit")
        .and_then(Value::as_i64)
        .unwrap_or(100)
        .clamp(1, QUERY_LIMIT_CAP);
    let records = if filters.is_empty() {
        state
            .datasets
            .list(app, dataset, limit)
            .await
            .map_err(|e| e.to_string())?
    } else {
        state
            .datasets
            .list_filtered(app, dataset, &filters, None, limit)
            .await
            .map_err(|e| e.to_string())?
    };
    Ok(json!({
        "app": app,
        "dataset": dataset,
        "count": records.len(),
        "records": records,
    }))
}

async fn tool_search(state: &AppState, args: &Value) -> Result<Value, String> {
    let q = require_str(args, "q")?;
    if q.trim().is_empty() {
        return Err("'q' must be non-empty".into());
    }
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(20)
        .clamp(1, SEARCH_LIMIT_CAP as u64) as usize;
    let req = pumper_core::SearchRequest {
        q: q.to_string(),
        limit,
        app: args.get("app").and_then(Value::as_str).map(String::from),
        dataset: args
            .get("dataset")
            .and_then(Value::as_str)
            .map(String::from),
        fuzzy: false,
        sort: pumper_core::SearchSort::Score,
        since: None,
        offset: 0,
        facets: false,
    };
    let results = state.search.query(req).await.map_err(|e| e.to_string())?;
    Ok(json!({
        "query": q,
        "total": results.total,
        "count": results.hits.len(),
        "hits": results.hits,
    }))
}

/// The budget rail: whatever the agent asks for, the job's spend ceiling is
/// `min(requested, [mcp] max_job_budget_usd)`; an absent request gets the
/// ceiling itself. A ceiling of 0 pins jobs to the free tiers.
fn clamp_budget(requested: Option<f64>, ceiling: f64) -> f64 {
    requested.map_or(ceiling, |b| b.max(0.0).min(ceiling))
}

async fn tool_enqueue(state: &AppState, args: &Value) -> Result<Value, String> {
    let name = require_str(args, "app")?;
    let Some(app) = state.registry.get(name) else {
        return Err(format!("unknown app '{name}' — call list_apps first"));
    };
    let over = args.get("params").cloned();
    if let Some(over) = &over {
        if !over.is_object() {
            return Err("'params' must be an object".into());
        }
    }
    let params = crate::routes::merge_params(app.default_params(), over);
    if let Some(schema) = &app.manifest().params_schema {
        validate_params(schema, &params)?;
    }
    let budget = clamp_budget(
        args.get("budget_usd").and_then(Value::as_f64),
        state.config.mcp.max_job_budget_usd,
    );
    let opts = pumper_core::EnqueueOptions {
        params,
        max_attempts: 1,
        delay_secs: 0,
        priority: 0,
        callback_url: None,
        callback_secret: None,
        // 0 is a real ceiling here (free tiers only), not "unlimited".
        budget_usd: Some(budget),
        idempotency_key: args
            .get("idempotency_key")
            .and_then(Value::as_str)
            .map(String::from)
            .filter(|k| !k.trim().is_empty()),
        schedule_id: None,
        trigger_id: None,
    };
    let (job, created) = state
        .storage
        .enqueue_dedup(name, opts)
        .await
        .map_err(|e| e.to_string())?;
    if created {
        state.notify.notify_one();
    }
    let note = format!("poll GET /jobs/{} (or query_dataset) for the result", job.id);
    Ok(json!({
        "job": job,
        "created": created,
        "budget_usd": budget,
        "note": note,
    }))
}

fn require_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing required string argument '{key}'"))
}

// ---- Resources --------------------------------------------------------------

const CATALOG_URI: &str = "pumper://catalog/sources";

fn manifest_uri(app: &str) -> String {
    format!("pumper://apps/{app}/manifest")
}

fn resources_list(state: &AppState) -> Vec<Value> {
    let mut resources = vec![json!({
        "uri": CATALOG_URI,
        "name": "Data-source catalog",
        "description": "catalog/data-sources.toml: every pipeline's market, category, \
            cadence, status, and serving app.",
        "mimeType": "application/json",
    })];
    let mut names: Vec<&str> = state.registry.keys().map(String::as_str).collect();
    names.sort_unstable();
    for name in names {
        resources.push(json!({
            "uri": manifest_uri(name),
            "name": format!("{name} manifest"),
            "description": format!("Agent-ready manifest for the '{name}' app: params schema, examples, output shape, cost class."),
            "mimeType": "application/json",
        }));
    }
    resources
}

fn resources_read(state: &AppState, params: &Value) -> Result<Value, (i64, String)> {
    let Some(uri) = params.get("uri").and_then(Value::as_str) else {
        return Err((-32602, "resources/read needs a 'uri'".into()));
    };
    let text = if uri == CATALOG_URI {
        let catalog = pumper_core::Catalog::load()
            .map_err(|e| (-32603_i64, format!("catalog load: {e}")))?;
        json!({ "sources": catalog.sources }).to_string()
    } else if let Some(app) = uri
        .strip_prefix("pumper://apps/")
        .and_then(|rest| rest.strip_suffix("/manifest"))
        .and_then(|name| state.registry.get(name))
    {
        crate::registry::tool_definition(app.as_ref()).to_string()
    } else {
        return Err((-32002, format!("unknown resource uri '{uri}'")));
    };
    Ok(json!({
        "contents": [{ "uri": uri, "mimeType": "application/json", "text": text }]
    }))
}

// ---- Params-schema validation (shared with the HTTP enqueue path) -----------

/// Validates a params object against an app's declared JSON Schema. `Err` is a
/// single human/agent-readable message carrying every violation as
/// `params<json-pointer>: <detail>`.
///
/// A schema that itself fails to compile is a manifest bug, not the caller's —
/// it is warn-logged and validation is skipped, so a bad schema can never brick
/// enqueue (the registry test keeps this path theoretical).
pub(crate) fn validate_params(schema: &Value, params: &Value) -> Result<(), String> {
    let validator = match jsonschema::validator_for(schema) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("unusable params_schema (skipping validation): {e}");
            return Ok(());
        }
    };
    let errors: Vec<String> = validator
        .iter_errors(params)
        .map(|e| format!("params{}: {e}", e.instance_path))
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "params failed the app's schema: {}",
            errors.join("; ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{clamp_budget, validate_params};
    use serde_json::json;

    #[test]
    fn validate_params_reports_pointer_paths() {
        let schema = json!({
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": { "type": "string" },
                "rows": { "type": "integer", "maximum": 1000 }
            }
        });
        // Missing required + nested violation, both named by pointer.
        let err = validate_params(&schema, &json!({ "rows": 5000 })).unwrap_err();
        assert!(err.contains("params:") || err.contains("params/"), "{err}");
        assert!(err.contains("query"), "missing-required names the field: {err}");
        assert!(err.contains("params/rows"), "violation carries its pointer: {err}");
        // Valid params pass.
        validate_params(&schema, &json!({ "query": "x", "rows": 10 })).unwrap();
    }

    #[test]
    fn unusable_schema_skips_validation_instead_of_bricking_enqueue() {
        let broken = json!({ "type": "definitely-not-a-type" });
        assert!(validate_params(&broken, &json!({})).is_ok());
    }

    #[test]
    fn budget_rail_clamps_and_defaults_to_the_ceiling() {
        assert_eq!(clamp_budget(None, 1.0), 1.0);
        assert_eq!(clamp_budget(Some(100.0), 1.0), 1.0);
        assert_eq!(clamp_budget(Some(0.25), 1.0), 0.25);
        assert_eq!(clamp_budget(Some(-5.0), 1.0), 0.0);
        // Ceiling 0 = free tiers only, even when the agent asks for spend.
        assert_eq!(clamp_budget(Some(3.0), 0.0), 0.0);
    }
}
