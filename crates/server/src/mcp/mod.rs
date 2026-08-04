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
//! default: the actuating tools — `enqueue_job` and its research sugar
//! `fetch_readable` / `deep_research` — sit behind the `[mcp] allow_enqueue`
//! switch and clamp every job budget to `[mcp] max_job_budget_usd`.
//!
//! **Notifications** (the transport's SSE half) live in [`live`]: `GET /mcp`
//! opens an SSE stream of JSON-RPC `notifications/pumper/*` messages bridged
//! read-only from the EventBus (subscribe + replay ring, `Last-Event-ID`
//! resume, per-connection `?app=`/`?kind=` filters, lag-tolerant bounded
//! buffering). POST stays stateless — the stream is a one-way event feed, not
//! a session.

mod live;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::state::AppState;
use axum::http::StatusCode;
// Deepest `search` page an agent may ask for — the HTTP route's own cap, so the
// tool schema advertises the same ceiling the request builder enforces.
use crate::routes::SEARCH_MAX_OFFSET;

/// Protocol revisions this server speaks. The client's requested version is
/// echoed when supported; otherwise the newest supported one is offered.
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26"];

/// Rows a `query_dataset` tool call may return (also the default when the
/// agent asks for nothing) — mirrors the HTTP route's clamp.
const QUERY_LIMIT_CAP: i64 = 1000;
/// Hits a `search` tool call may return — mirrors `GET /search`.
const SEARCH_LIMIT_CAP: usize = 100;

/// The `/mcp` routes. Only merged into the main router when `[mcp] enabled`.
/// POST = stateless JSON-RPC exchanges; GET = the SSE notification stream.
pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/mcp", post(handle_post).get(live::handle_get))
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
            query_dataset (`$.path:op:value` filters) and search (full text). enqueue_job, \
            fetch_readable, and deep_research are only offered when the operator has enabled \
            [mcp] allow_enqueue; every budget_usd is clamped to [mcp] max_job_budget_usd. Await \
            a job with wait_job (timeout capped by [mcp] wait_job_max_secs), or open GET /mcp \
            (SSE, optional ?app=/?kind= filters, Last-Event-ID resume) for live \
            notifications/pumper/* events. Catalog + app manifests are resources.",
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
                results, with highlighted snippets. Scope with app/dataset, page with \
                offset, order by relevance or index time, and filter on the entity fields \
                extracted at index time (money amount, deadline date). Same query surface \
                as GET /search; app/dataset facets are the one thing this tool does not \
                return.",
            "inputSchema": {
                "type": "object",
                "required": ["q"],
                "properties": {
                    "q": { "type": "string", "minLength": 1 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": SEARCH_LIMIT_CAP },
                    "app": { "type": "string", "description": "Restrict hits to one app." },
                    "dataset": {
                        "type": "string",
                        "description": "Restrict hits to one dataset. Job-result documents \
                            live under the reserved '_job' / '_records' names."
                    },
                    "offset": {
                        "type": "integer", "minimum": 0, "maximum": SEARCH_MAX_OFFSET,
                        "description": "Skip this many ranked hits before `limit` \
                            (page 2 = offset equal to limit). Clamped."
                    },
                    "fuzzy": {
                        "type": "boolean",
                        "description": "Typo tolerance (edit distance 1). Quoted phrases \
                            stay exact."
                    },
                    "sort": {
                        "type": "string", "enum": ["score", "newest"],
                        "description": "Ordering: 'score' (BM25 relevance, default) or \
                            'newest' (most recently indexed first)."
                    },
                    "since": {
                        "type": "integer",
                        "description": "Only hits indexed at/after this unix-seconds \
                            instant — a \"what's new\" feed."
                    },
                    "amount_gte": {
                        "type": "integer", "minimum": 0,
                        "description": "Only hits whose index-time-extracted money amount \
                            (whole US dollars) is >= this. Documents with no extracted \
                            amount never match."
                    },
                    "amount_lte": {
                        "type": "integer", "minimum": 0,
                        "description": "Only hits whose extracted amount is <= this \
                            (whole US dollars)."
                    },
                    "date_after": {
                        "type": "integer",
                        "description": "Only hits whose extracted deadline (unix seconds) \
                            is at/after this. Documents with no extracted deadline never \
                            match."
                    },
                    "date_before": {
                        "type": "integer",
                        "description": "Only hits whose extracted deadline is at/before \
                            this (unix seconds)."
                    }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "wait_job",
            "description": format!(
                "Wait for a job to reach a terminal status (succeeded | failed | cancelled), \
                 watching the live event stream. timeout_secs is clamped to the operator's \
                 [mcp] wait_job_max_secs cap ({}s; omitted = that cap). Hitting the deadline \
                 returns timed_out: true with the job's current snapshot — call again to keep \
                 waiting.",
                state.config.mcp.wait_job_max_secs
            ),
            "inputSchema": {
                "type": "object",
                "required": ["job_id"],
                "properties": {
                    "job_id": { "type": "string", "format": "uuid" },
                    "timeout_secs": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }
        }),
    ];
    if state.config.mcp.allow_enqueue {
        tools.push(json!({
            "name": "fetch_readable",
            "description": "Fetch one URL as clean Markdown via the tiered fetcher: enqueues \
                a 'readable' job and returns its job id (then wait_job for the result; the \
                document lands in the job's page.md artifact). Same operator gates as \
                enqueue_job.",
            "inputSchema": {
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }
        }));
        tools.push(json!({
            "name": "deep_research",
            "description": format!(
                "Agentic web research (search, read, synthesize) via the Claude engine: \
                 enqueues a 'research' job and returns its job id (then wait_job for the \
                 result). budget_usd is the run's spend ceiling, clamped to the operator's \
                 [mcp] max_job_budget_usd rail (${:.2}); omitted = that rail.",
                state.config.mcp.max_job_budget_usd
            ),
            "inputSchema": {
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": { "type": "string", "minLength": 1 },
                    "budget_usd": { "type": "number", "minimum": 0 }
                },
                "additionalProperties": false
            }
        }));
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
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let outcome = match name {
        "list_apps" => Ok(tool_list_apps(state)),
        "query_dataset" => tool_query_dataset(state, &args).await,
        "search" => tool_search(state, &args).await,
        "wait_job" => live::wait_job(state, &args).await,
        "enqueue_job" if state.config.mcp.allow_enqueue => tool_enqueue(state, &args).await,
        "fetch_readable" if state.config.mcp.allow_enqueue => {
            tool_fetch_readable(state, &args).await
        }
        "deep_research" if state.config.mcp.allow_enqueue => tool_deep_research(state, &args).await,
        "enqueue_job" | "fetch_readable" | "deep_research" => Err(
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

/// The MCP `search` tool. Every param maps through the HTTP route's own
/// [`crate::routes::build_search_request`] — the tool used to expose a strict
/// subset (q/limit/app/dataset), so an agent could not page, sort, or filter
/// what the REST surface has filtered on since M14. Facets stay off: this tool
/// returns hits only, and computing them costs a ≥1000-doc sample.
async fn tool_search(state: &AppState, args: &Value) -> Result<Value, String> {
    let q = require_str(args, "q")?.to_string();
    let str_arg = |key: &str| args.get(key).and_then(Value::as_str).map(String::from);
    let req = crate::routes::build_search_request(crate::routes::SearchInput {
        q: q.clone(),
        // The tool schema's own cap; `build_search_request` clamps again.
        limit: args
            .get("limit")
            .and_then(Value::as_u64)
            .map(|v| v.min(SEARCH_LIMIT_CAP as u64) as usize),
        app: str_arg("app"),
        dataset: str_arg("dataset"),
        fuzzy: args.get("fuzzy").and_then(Value::as_bool).unwrap_or(false),
        sort: str_arg("sort"),
        since: args.get("since").and_then(Value::as_i64),
        offset: args
            .get("offset")
            .and_then(Value::as_u64)
            .map(|v| v as usize),
        amount_gte: args.get("amount_gte").and_then(Value::as_u64),
        amount_lte: args.get("amount_lte").and_then(Value::as_u64),
        date_after: args.get("date_after").and_then(Value::as_i64),
        date_before: args.get("date_before").and_then(Value::as_i64),
        facets: false,
    })?;
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
    let over = args.get("params").cloned();
    if let Some(over) = &over {
        if !over.is_object() {
            return Err("'params' must be an object".into());
        }
    }
    let budget = clamp_budget(
        args.get("budget_usd").and_then(Value::as_f64),
        state.config.mcp.max_job_budget_usd,
    );
    let idempotency_key = args
        .get("idempotency_key")
        .and_then(Value::as_str)
        .map(String::from)
        .filter(|k| !k.trim().is_empty());
    enqueue_app(state, name, over, budget, idempotency_key).await
}

/// `fetch_readable`: sugar over enqueueing the `readable` app — one URL in,
/// clean Markdown out (as the job's `page.md` artifact). Rides the exact gated
/// enqueue path: allow_enqueue is checked by the dispatcher, budget clamped.
async fn tool_fetch_readable(state: &AppState, args: &Value) -> Result<Value, String> {
    let url = require_str(args, "url")?;
    if url.trim().is_empty() {
        return Err("'url' must be non-empty".into());
    }
    let budget = clamp_budget(None, state.config.mcp.max_job_budget_usd);
    enqueue_app(state, "readable", Some(json!({ "url": url })), budget, None).await
}

/// `deep_research`: sugar over enqueueing the `research` app. The clamped
/// budget is BOTH the job's spend ceiling and the app's own `max_budget_usd`
/// param, so the Claude engine enforces the same rail mid-run.
async fn tool_deep_research(state: &AppState, args: &Value) -> Result<Value, String> {
    let query = require_str(args, "query")?;
    if query.trim().is_empty() {
        return Err("'query' must be non-empty".into());
    }
    let budget = clamp_budget(
        args.get("budget_usd").and_then(Value::as_f64),
        state.config.mcp.max_job_budget_usd,
    );
    let params = json!({ "query": query, "max_budget_usd": budget });
    enqueue_app(state, "research", Some(params), budget, None).await
}

/// The one gated enqueue path every actuating tool funnels through: params
/// shallow-merge over the app's defaults, schema-validate, budget already
/// clamped by the caller, dedup + worker wake exactly like the HTTP surface.
async fn enqueue_app(
    state: &AppState,
    name: &str,
    over: Option<Value>,
    budget: f64,
    idempotency_key: Option<String>,
) -> Result<Value, String> {
    let Some(app) = state.registry.get(name) else {
        return Err(format!("unknown app '{name}' — call list_apps first"));
    };
    let params = crate::routes::merge_params(app.default_params(), over);
    if let Some(schema) = &app.manifest().params_schema {
        validate_params(schema, &params)?;
    }
    let opts = pumper_core::EnqueueOptions {
        params,
        max_attempts: 1,
        delay_secs: 0,
        priority: 0,
        callback_url: None,
        callback_secret: None,
        // 0 is a real ceiling here (free tiers only), not "unlimited".
        budget_usd: Some(budget),
        idempotency_key,
        schedule_id: None,
        trigger_id: None,
        source_job_id: None,
    };
    let (job, created) = state
        .storage
        .enqueue_dedup(name, opts)
        .await
        .map_err(|e| e.to_string())?;
    if created {
        state.notify.notify_one();
    }
    let note = format!(
        "wait_job {{\"job_id\": \"{0}\"}} for the terminal status, or poll GET /jobs/{0}",
        job.id
    );
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
        let catalog =
            pumper_core::Catalog::load().map_err(|e| (-32603_i64, format!("catalog load: {e}")))?;
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
        assert!(
            err.contains("query"),
            "missing-required names the field: {err}"
        );
        assert!(
            err.contains("params/rows"),
            "violation carries its pointer: {err}"
        );
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
