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
//! `approve_transaction` (N01) sits behind a THIRD switch, `[mcp]
//! allow_approve`, and is additionally inert unless `[transact] allow_live` is
//! on. It is not folded into `allow_enqueue` because the two authorities are
//! not the same size: an enqueue spends money a ceiling bounds, while an
//! approval submits a form on a live site under the operator's logged-in
//! identity and cannot be undone by anything this process controls.
//!
//! **Notifications** (the transport's SSE half) live in [`live`]: `GET /mcp`
//! opens an SSE stream of JSON-RPC `notifications/pumper/*` messages bridged
//! read-only from the EventBus (subscribe + replay ring, `Last-Event-ID`
//! resume, per-connection `?app=`/`?kind=` filters, lag-tolerant bounded
//! buffering). POST stays stateless — the stream is a one-way event feed, not
//! a session.

pub(crate) mod jobtoken;
mod live;

use axum::extract::State;
use axum::http::HeaderMap;
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
async fn handle_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    // N15: the only thing read off the request itself is the job token a
    // self-hosted Claude subprocess presents. Every other client is anonymous
    // here and unaffected; the API key, when `[auth] mode = "keys"`, was
    // already resolved by the identity layer this route sits behind.
    let caller = jobtoken::McpCaller::from_headers(&headers);
    match payload {
        Value::Array(msgs) => {
            let mut responses = Vec::new();
            for msg in &msgs {
                if let Some(resp) = handle_rpc_as(&state, msg, &caller).await {
                    responses.push(resp);
                }
            }
            if responses.is_empty() {
                StatusCode::ACCEPTED.into_response()
            } else {
                Json(Value::Array(responses)).into_response()
            }
        }
        msg => match handle_rpc_as(&state, &msg, &caller).await {
            Some(resp) => Json(resp).into_response(),
            None => StatusCode::ACCEPTED.into_response(),
        },
    }
}

/// [`handle_rpc`] for a caller that presented something about itself — today,
/// only a job token (N15). Kept as a separate entry point so every existing
/// call site keeps its two-argument shape.
pub(crate) async fn handle_rpc_as(
    state: &AppState,
    msg: &Value,
    caller: &jobtoken::McpCaller,
) -> Option<Value> {
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
    let id = id.filter(|id| !id.is_null())?;

    let result = match method {
        "initialize" => Ok(initialize_result(&params)),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": server_tools(state) })),
        "tools/call" => return Some(tools_call(state, id, &params, caller).await),
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
            a job with wait_job (timeout capped by [mcp] wait_job_max_secs) - it settles on \
            a terminal status OR on 'waiting', a job asking YOU for input, which you answer \
            with resume_job - or open GET /mcp \
            (SSE, optional ?app=/?kind= filters, Last-Event-ID resume) for live \
            notifications/pumper/* events. list_pending_transactions is the approval inbox             for transact runs parked on a human decision; approve_transaction releases one and             is offered ONLY when the operator enabled both [mcp] allow_approve and [transact]             allow_live, because it performs a live, irreversible web action under their             logged-in profile. Catalog + app manifests are resources.",
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
                return. Every result carries an `index` block ({enabled, doc_count, degraded, \
                reason}) — when `degraded` is true the index is disabled or empty, so an empty \
                `hits` list is NOT evidence the records do not exist; read `reason` before \
                concluding anything from zero hits.",
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
                "Wait for a job to SETTLE, watching the live event stream. It settles on a \
                 terminal status (succeeded | failed | cancelled) OR on 'waiting' - a job that \
                 parked to ask YOU for something, in which case the result carries input_request: \
                 answer it with resume_job, then call wait_job again. timeout_secs is clamped to \
                 the operator's [mcp] wait_job_max_secs cap ({}s; omitted = that cap). Hitting \
                 the deadline returns timed_out: true with the job's current snapshot - call \
                 again to keep waiting.",
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
            "name": "resume_job",
            "description": "Answer a job that is 'waiting' (its request is in wait_job's input_request) \
                and let it continue: it resumes from the checkpoint it parked at, reads `input` \
                back through ctx.restore_input(), and burns no retry attempt. 409-equivalent \
                refusal if the job is not waiting - including a second resume of one you have \
                already answered. Same operator gate as enqueue_job, because resuming lets a job \
                spend the rest of its budget.",
            "inputSchema": {
                "type": "object",
                "required": ["job_id"],
                "properties": {
                    "job_id": { "type": "string", "format": "uuid" },
                    "input": {
                        "description": "The answer, in whatever shape the job's input_request \
                            asked for. Omitted = null, which is a fine answer to a pure \
                            'proceed?' gate."
                    }
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
        // N03 workflow runs. Appended at the END of the tool table (three wave-2
        // items add tools here); no existing entry is reordered.
        tools.push(json!({
            "name": "run_workflow",
            "description": "Start a run of a DECLARED multi-step workflow (see GET /workflows) \
                and return its run id. A workflow is a DAG of ordinary jobs with fan-in join \
                barriers, so a crawl -> extract -> research pipeline is ONE call and ONE \
                receipt instead of N enqueue_job/wait_job round-trips. budget_usd is the \
                envelope for the WHOLE run: each step's ceiling is clamped to what is left of \
                it, and it is clamped to the operator's [mcp] max_job_budget_usd rail. Then \
                wait_workflow for the outcome. Same operator gate as enqueue_job.",
            "inputSchema": {
                "type": "object",
                "required": ["workflow"],
                "properties": {
                    "workflow": { "type": "string", "description": "Workflow id or name." },
                    "budget_usd": { "type": "number", "minimum": 0 },
                    "idempotency_key": {
                        "type": "string",
                        "description": "Replaying a start with the same key returns the ORIGINAL \
                            run instead of executing the plan twice."
                    }
                },
                "additionalProperties": false
            }
        }));
    }
    tools.push(json!({
        "name": "wait_workflow",
        "description": format!(
            "Wait for a workflow run to settle (succeeded | failed | cancelled) and return its \
             step matrix plus the rolled-up receipt: cost summed over the run's job set, yield \
             from job_yield. timeout_secs is clamped to the operator's [mcp] wait_job_max_secs \
             cap ({}s; omitted = that cap). Hitting the deadline returns timed_out: true with \
             the run's current step matrix - call again to keep waiting. A step that never \
             became a job reports cost_usd: null, not $0.",
            state.config.mcp.wait_job_max_secs
        ),
        "inputSchema": {
            "type": "object",
            "required": ["run_id"],
            "properties": {
                "run_id": { "type": "string" },
                "timeout_secs": { "type": "integer", "minimum": 1 }
            },
            "additionalProperties": false
        }
    }));
    // N01 Transact v2. `list_pending_transactions` is a READ and rides the same
    // posture as the other reads — an agent may always see what is waiting on a
    // human. `approve_transaction` is the only tool on this surface that can
    // release an irreversible action, so it has its own switch: it is offered
    // only when the operator set BOTH `[mcp] allow_approve` and
    // `[transact] allow_live`, because a tool that is offered and then always
    // refuses is worse than one that was never listed.
    tools.push(json!({
        "name": "list_pending_transactions",
        "description": "List transact transactions awaiting a human decision: the ledger row \
            (id, idempotency_key, profile, state, evidence_sha, expires_at) plus the operator's \
            allow_live switch. Read the full evidence bundle a run parked on through \
            wait_job's input_request, or GET /transactions/{id}. Approving is a SEPARATE tool \
            the operator must enable.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": { "type": "integer", "minimum": 1, "maximum": TRANSACTION_LIMIT_CAP }
            },
            "additionalProperties": false
        }
    }));
    if state.config.mcp.allow_approve && state.config.transact.allow_live {
        tools.push(json!({
            "name": "approve_transaction",
            "description": "Approve one pending transaction and let its parked job perform the \
                irreversible action it staged — a live form submission under the operator's \
                logged-in profile, which nothing in this process can undo. You MUST quote \
                evidence_sha, the digest of the bundle you actually read, so an approval cannot \
                be given for evidence you never saw; a mismatch, an expired request, a \
                non-pending row or the profile's daily cap is a refusal. The commit re-probes \
                the live page and refuses again if it drifted. One idempotency key submits at \
                most once, ever.",
            "inputSchema": {
                "type": "object",
                "required": ["transaction_id", "evidence_sha"],
                "properties": {
                    "transaction_id": { "type": "string" },
                    "evidence_sha": {
                        "type": "string",
                        "description": "The evidence digest from the transaction row or the \
                            job's input_request. Required here even though the HTTP door \
                            treats it as optional: an agent approving without naming what it \
                            read is the case this gate exists for."
                    }
                },
                "additionalProperties": false
            }
        }));
    }
    // N04: the pipeline-AUTHORING tools. Everything above lets an agent run a
    // job; these let it wire the edge that runs jobs by itself, which is a
    // standing commitment rather than one enqueue — so all five ride
    // `allow_enqueue`, including the two reads, because a decision ledger and a
    // dry-run are only useful to something that can author. Appended before
    // `fetch`, which stays LAST (the e2e inventory pins it).
    if state.config.mcp.allow_enqueue {
        tools.push(json!({
            "name": "create_trigger",
            "description": "Create a standing reactive EDGE: when a source event happens, \
                enqueue a target app. source_kind is 'dataset' (a run's change batch), 'job' (a \
                terminal status) or 'external' (an inbound signed webhook on /ingest). Use \
                `bind` to steer the target's OWN params from the event — a map of {target \
                param: JSON pointer} resolved against the {template, _trigger} view, e.g. \
                {\"url\": \"/_trigger/payload/repository/html_url\"} — and `each` (a JSON \
                pointer to an array) to fan ONE event out into one job per element, which then \
                read `/_trigger/item`. A pointer that resolves to nothing does NOT enqueue: it \
                records the `bind_miss` decision, which trigger_decisions shows. Dry-run the \
                edge with test_trigger before you rely on it. Same door, same validation as \
                POST /triggers.",
            "inputSchema": {
                "type": "object",
                "required": ["source_kind", "source_app", "target_app"],
                "properties": {
                    "name": { "type": "string" },
                    "source_kind": { "type": "string", "enum": ["dataset", "job", "external"] },
                    "source_app": {
                        "type": "string",
                        "description": "The source app or namespace; for 'external', an ingress \
                            source id (see create_ingress_source) or '*' for any source."
                    },
                    "source_dataset": { "type": "string" },
                    "on_change": {
                        "type": "string",
                        "enum": ["new", "changed", "removed", "fresh", "any"]
                    },
                    "on_status": { "type": "string", "enum": ["succeeded", "failed", "any"] },
                    "target_app": { "type": "string" },
                    "params": {
                        "type": "object",
                        "description": "Static params template. `_trigger` is merged over it, \
                            and `bind` is applied on top of that."
                    },
                    "bind": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                        "description": "{target param: JSON pointer}. Pointers are RFC 6901 \
                            (start with '/'), NOT the dotted '$.path' form `filters` uses."
                    },
                    "each": {
                        "type": "string",
                        "description": "JSON pointer to an array; one job per element, capped by \
                            the operator's [triggers] fan_out_cap. Not valid for source_kind \
                            'job'."
                    },
                    "filters": {
                        "type": "array", "items": { "type": "string" },
                        "description": "source_kind 'external' only: '$.path:op:value' specs \
                            ANDed against the inbound payload."
                    },
                    "budget_usd": { "type": "number", "exclusiveMinimum": 0 },
                    "priority": { "type": "integer" },
                    "max_attempts": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }
        }));
        tools.push(json!({
            "name": "test_trigger",
            "description": "DRY-RUN one trigger against its most recent matching source job and \
                return what it would do: would_fire, the fully resolved target params (binding \
                and fan-out applied exactly as the live path applies them), the fan-out shape \
                when `each` is set, and any hook incidents. Nothing is enqueued unless you pass \
                fire: true, which enqueues every planned hop with the idempotency key bypassed \
                (so it is repeatable) and is refused if the resolved params fail the target \
                app's schema. External triggers have no source job to preview — exercise those \
                by POSTing a signed event to /ingest/{source}.",
            "inputSchema": {
                "type": "object",
                "required": ["trigger_id"],
                "properties": {
                    "trigger_id": { "type": "string" },
                    "fire": {
                        "type": "boolean",
                        "description": "Actually enqueue the planned hops (default false)."
                    }
                },
                "additionalProperties": false
            }
        }));
        tools.push(json!({
            "name": "trigger_decisions",
            "description": "Why an edge did or did not fire: one page of the DECISION LEDGER for \
                one trigger, newest first, plus the jobs it enqueued. Every evaluation is \
                recorded, skips included — `fired`, `bind_miss` (a bind/each pointer resolved to \
                nothing), `fan_out_empty`, `filter_miss`, `no_change_match`, `status_mismatch`, \
                `bad_params`, `dedup`, `cycle`, `depth`, `predicate_veto`, the hook faults, \
                `target_unregistered`, `enqueue_failed`. This is the tool that answers 'I wired \
                it and nothing happened'. Page with the returned next_cursor.",
            "inputSchema": {
                "type": "object",
                "required": ["trigger_id"],
                "properties": {
                    "trigger_id": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 500 },
                    "cursor": { "type": "string" }
                },
                "additionalProperties": false
            }
        }));
        tools.push(json!({
            "name": "create_watch",
            "description": "Subscribe an external destination to a dataset's changes: every new \
                or changed record under app/dataset is delivered to `url` (sink 'webhook', the \
                default, or 'slack'), appended to data/sinks/<id>.ndjson (sink 'file'), or run \
                through an installed WASM connector (sink 'plugin:<name>'). `app` is the \
                NAMESPACE records land under, which is not always the app that produced them \
                (grant sources publish into 'grants'); an (app, dataset) pair that could never \
                fire is refused with the namespace that would. Same door as POST /watches.",
            "inputSchema": {
                "type": "object",
                "required": ["app"],
                "properties": {
                    "app": { "type": "string" },
                    "dataset": { "type": "string", "description": "'*' (default) = every dataset." },
                    "url": { "type": "string" },
                    "secret": {
                        "type": "string",
                        "description": "Delivery bodies are HMAC-SHA256 signed with it."
                    },
                    "sink": { "type": "string", "description": "webhook | file | slack | plugin:<name>" }
                },
                "additionalProperties": false
            }
        }));
        tools.push(json!({
            "name": "create_ingress_source",
            "description": "Create a named credential an EXTERNAL system can POST signed events \
                to at /ingest/{id}, which is what a source_kind 'external' trigger reacts to. \
                The signing secret is returned by this call and NEVER again — hand it to the \
                sender now or delete the source and make another. GitHub-style \
                x-hub-signature-256 is accepted as-is. The CRUD works while [ingress] enabled = \
                false, so sources can be staged before the operator flips the switch; until \
                then /ingest returns 409.",
            "inputSchema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": { "type": "string", "minLength": 1 },
                    "secret": {
                        "type": "string",
                        "description": "Bring your own signing secret; omitted = generated and \
                            returned once."
                    }
                },
                "additionalProperties": false
            }
        }));
    }
    // N15 (appended last, per the wave-2 shared-surface rule). Advertised
    // unconditionally: the token, not a config switch, is what makes it usable,
    // and hiding it would leave the self-hosted subprocess unable to discover
    // the one network tool it has.
    tools.push(json!({
        "name": "fetch",
        "description": "Fetch one URL through THIS host's tiered fetcher and return the content \
            synchronously. Requires the job token pumper writes into a self-hosted Claude run's \
            MCP config, and the fetch is metered against that job: politeness governor, response \
            cache, learned tier router, session profile, archive tier, budget ceiling and cost \
            ledger all apply. Unlike fetch_readable this does not enqueue a job — it answers in \
            the same call. Returns {url, engine (archive|api_recipe|http|browser|claude), status, \
            content, chars, cost_usd, escalations, trace}.",
        "inputSchema": {
            "type": "object",
            "required": ["url"],
            "properties": {
                "url": { "type": "string", "minLength": 1 },
                "strategy": {
                    "type": "string", "enum": ["http", "browser", "auto"],
                    "description": "Ladder entry point; default 'auto' (http, escalating to a \
                        browser render when the result is thin or blocked). The paid claude tier \
                        is never reachable from here — a research run must not recurse into \
                        itself."
                },
                "profile": {
                    "type": "string",
                    "description": "Named session profile (cookie jar / browser user-data-dir) \
                        to fetch under."
                },
                "archive_max_age": {
                    "type": "integer", "minimum": 0,
                    "description": "Accept an archived snapshot no older than this many seconds \
                        instead of going live."
                },
                "to_markdown": {
                    "type": "boolean",
                    "description": "Convert the fetched page to clean Markdown (default true)."
                }
            },
            "additionalProperties": false
        }
    }));
    tools
}

/// `tools/call`: runs a tool and wraps the outcome per MCP — a *tool* failure
/// is a `result` with `isError: true` (the agent can read and react), while an
/// unknown tool or unusable arguments are protocol errors.
async fn tools_call(
    state: &AppState,
    id: Value,
    params: &Value,
    caller: &jobtoken::McpCaller,
) -> Value {
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
        "resume_job" if state.config.mcp.allow_enqueue => tool_resume_job(state, &args).await,
        "fetch_readable" if state.config.mcp.allow_enqueue => {
            tool_fetch_readable(state, &args).await
        }
        "deep_research" if state.config.mcp.allow_enqueue => tool_deep_research(state, &args).await,
        // N03: appended at the END of the dispatch, no arm reordered.
        "wait_workflow" => tool_wait_workflow(state, &args).await,
        "run_workflow" if state.config.mcp.allow_enqueue => tool_run_workflow(state, &args).await,
        "enqueue_job" | "fetch_readable" | "deep_research" | "resume_job" | "run_workflow" => Err(
            "enqueue is disabled on this MCP surface — the operator must set \
             [mcp] allow_enqueue = true"
                .to_string(),
        ),
        // N01, appended last: see `server_tools`.
        "list_pending_transactions" => tool_list_pending_transactions(state, &args).await,
        "approve_transaction"
            if state.config.mcp.allow_approve && state.config.transact.allow_live =>
        {
            tool_approve_transaction(state, &args).await
        }
        "approve_transaction" => Err(
            "approving live transactions is disabled on this MCP surface — the operator must \
             set BOTH [mcp] allow_approve = true and [transact] allow_live = true. Nothing \
             was submitted."
                .to_string(),
        ),
        // N04: the pipeline-authoring tools, appended before `fetch`.
        "create_trigger" if state.config.mcp.allow_enqueue => {
            tool_create_trigger(state, &args).await
        }
        "test_trigger" if state.config.mcp.allow_enqueue => tool_test_trigger(state, &args).await,
        "trigger_decisions" if state.config.mcp.allow_enqueue => {
            tool_trigger_decisions(state, &args).await
        }
        "create_watch" if state.config.mcp.allow_enqueue => tool_create_watch(state, &args).await,
        "create_ingress_source" if state.config.mcp.allow_enqueue => {
            tool_create_ingress_source(state, &args).await
        }
        "create_trigger"
        | "test_trigger"
        | "trigger_decisions"
        | "create_watch"
        | "create_ingress_source" => Err(
            "authoring reactive pipelines is disabled on this MCP surface — the operator must \
             set [mcp] allow_enqueue = true. A trigger is a STANDING commitment to enqueue \
             work, so it rides the same switch as a single enqueue"
                .to_string(),
        ),
        // N15, appended last per the wave-2 shared-surface rule.
        "fetch" => tool_fetch(state, &args, caller).await,
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

/// The `resume_job` tool: answers a parked job (N02).
///
/// One store call, through the same `status = 'waiting'` guard the HTTP door
/// uses - so the two surfaces cannot disagree about what a stale resume does,
/// and an agent that retries a `resume_job` it already sent gets a refusal
/// instead of restarting a lineage that has moved on.
async fn tool_resume_job(state: &AppState, args: &Value) -> Result<Value, String> {
    let id: uuid::Uuid = require_str(args, "job_id")?
        .parse()
        .map_err(|e| format!("invalid job_id: {e}"))?;
    let input = args.get("input").cloned().unwrap_or(Value::Null);
    match state.storage.resume(id, &input).await {
        Ok(Some(job)) => {
            state.events.emit(crate::events::JobEvent::new(
                job.id,
                job.app.clone(),
                "queued",
            ));
            state.notify.notify_one();
            Ok(json!({ "resumed": true, "job": job }))
        }
        // Deliberately not split into `unknown job` vs `wrong state`: the store
        // cannot tell them apart in one guarded statement, and inventing the
        // distinction with a second read would be a race, not a fact.
        Ok(None) => Err(format!(
            "job '{id}' is not waiting for input (unknown, still running, already \
            resumed, or terminal) - read its status with wait_job before resuming"
        )),
        Err(e) => Err(e.to_string()),
    }
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
///
/// The body itself is rendered by [`crate::routes::run_search`], the same
/// renderer `GET /search` uses — so the `index` degraded-state block reaches
/// the agent too. An agent is the caller most likely to be fooled by a wiped
/// index: it reads `total: 0` and reports back that the data does not exist.
async fn tool_search(state: &AppState, args: &Value) -> Result<Value, String> {
    let q = require_str(args, "q")?.to_string();
    let str_arg = |key: &str| args.get(key).and_then(Value::as_str).map(String::from);
    let req = crate::routes::build_search_request(crate::routes::SearchInput {
        q,
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
    // Same renderer `GET /search` uses (`facets: false` is what keeps this
    // result facet-free), so the agent gets the `index` degraded-state block
    // too: an empty page from a wiped index must not read to an agent as
    // "this data does not exist".
    crate::routes::run_search(state, req)
        .await
        .map_err(|e| e.to_string())
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
    validate_app_params(&state.registry, name, &params)?;
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
        workflow_run_id: None,
        workflow_step: None,
        root_id: None,
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

/// The `run_workflow` tool (N03): opens a run of a declared plan.
///
/// Goes through `workflow::start_run`, the same door `POST /workflows/{id}/runs`
/// uses, so an agent and an operator cannot get different validation, different
/// idempotence or a different envelope.
async fn tool_run_workflow(state: &AppState, args: &Value) -> Result<Value, String> {
    let name = require_str(args, "workflow")?;
    let Some(def) = state
        .storage
        .get_workflow(name)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Err(format!(
            "unknown workflow '{name}' — GET /workflows lists the declared plans"
        ));
    };
    let budget = clamp_budget(
        args.get("budget_usd").and_then(Value::as_f64),
        state.config.mcp.max_job_budget_usd,
    );
    let key = args
        .get("idempotency_key")
        .and_then(Value::as_str)
        .map(str::to_string);
    let (run, created) = crate::workflow::start_run(
        state,
        &def,
        // 0 is a real ceiling on this surface (free tiers only), exactly as on
        // `enqueue_app` — so it is passed through rather than dropped to "no
        // envelope", which is what `None` would mean.
        Some(budget),
        key.as_deref(),
        None,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(json!({
        "run": run,
        "created": created,
        "budget_usd": budget,
        "note": format!(
            "wait_workflow {{\"run_id\": \"{}\"}} for the outcome and the rolled-up receipt",
            run.id
        ),
    }))
}

/// The `wait_workflow` tool: settle on a run, or report the deadline honestly.
///
/// Polls rather than riding the event bus: a run's lifecycle events are emitted
/// on the same bus, but a run can also be advanced by a step that finished
/// before this call started, and a poll cannot miss that. The deadline is the
/// operator's `wait_job_max_secs` rail — the same one `wait_job` honours, so an
/// agent cannot buy a longer hold by waiting on a workflow instead of a job.
async fn tool_wait_workflow(state: &AppState, args: &Value) -> Result<Value, String> {
    let run_id = require_str(args, "run_id")?.to_string();
    let cap = state.config.mcp.wait_job_max_secs.max(1);
    let secs = args
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .unwrap_or(cap)
        .clamp(1, cap);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    loop {
        let Some(run) = state
            .storage
            .get_workflow_run(&run_id)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Err(format!("unknown workflow run '{run_id}'"));
        };
        let settled = run.status != "running";
        if settled || tokio::time::Instant::now() >= deadline {
            let report = crate::workflow::run_report(state, &run_id)
                .await
                .map_err(|e| e.to_string())?
                .unwrap_or_else(|| json!({ "run": run }));
            let mut out = report;
            if let Value::Object(obj) = &mut out {
                obj.insert("timed_out".into(), json!(!settled));
            }
            return Ok(out);
        }
        tokio::select! {
            _ = state.shutdown.cancelled() => {
                return Err("server is shutting down; the run is durable — call wait_workflow \
                            again after the restart".into());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }
    }
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

// ---- Params-schema validation (shared by every door that creates work) ------

/// The one params check **every door that creates future work** performs, so a
/// job's effective params are judged identically no matter which door made it.
///
/// The anti-pattern this closes: `POST /apps/{name}/jobs` enforced the app's
/// declared schema (422 with pointer paths) while `POST /schedules` stored
/// whatever it was handed, the scheduler enqueued it hours later, and the
/// trigger fire paths never looked at all. Same app, same params, three
/// different answers — and the two silent ones surfaced as a failed job with a
/// message nobody connects back to the schedule row or the trigger template.
///
/// Unknown app and no declared schema are both `Ok`: the caller owns the
/// "unknown app" answer (404 vs a skip + ledger row), and an app without a
/// schema declares no contract to check. Validation runs on the EFFECTIVE
/// params — what the job would actually run with, after the defaults merge.
pub(crate) fn validate_app_params(
    registry: &std::collections::HashMap<String, std::sync::Arc<dyn pumper_core::ScrapeApp>>,
    app: &str,
    params: &Value,
) -> Result<(), String> {
    let Some(entry) = registry.get(app) else {
        return Ok(());
    };
    let Some(schema) = &entry.manifest().params_schema else {
        return Ok(());
    };
    validate_params(schema, params)
}

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

/// The doors that create future work, and the shared check they must run.
///
/// Inventory in the house EXPECTED-diff style (`routes::mod`'s spec coverage,
/// `routes::error`'s status contract): the scan walks the server sources for
/// call sites that create work — `enqueue`, `enqueue_dedup`, `create_schedule`
/// — and diffs them against these two lists, so a NEW door cannot be added
/// without either wiring the check or writing down why it doesn't need one.
///
/// Test-only enqueues (everything after a file's first `#[cfg(test)]`) are not
/// doors and are excluded from the scan.
/// Each entry is `(file, the symbol that file must call)` — either the shared
/// check itself or the schedule-shaped wrapper around it
/// (`scheduler::validate_schedule_params`, which resolves the effective params
/// first and then calls [`validate_app_params`]).
/// Max rows `list_pending_transactions` will return.
const TRANSACTION_LIMIT_CAP: i64 = 200;

/// The `list_pending_transactions` tool (N01): the approval inbox, read-only.
///
/// Stale mandates are swept before the listing, exactly as the HTTP door does
/// it, so an agent is never shown a pending row the approve door would refuse.
async fn tool_list_pending_transactions(state: &AppState, args: &Value) -> Result<Value, String> {
    let limit = args
        .get("limit")
        .and_then(Value::as_i64)
        .unwrap_or(TRANSACTION_LIMIT_CAP)
        .clamp(1, TRANSACTION_LIMIT_CAP);
    let pool = state.storage.pool();
    pumper_core::transactions::expire_stale(&pool, state.config.transact.approval_ttl_secs)
        .await
        .map_err(|e| e.to_string())?;
    let rows = pumper_core::transactions::list(
        &pool,
        Some(pumper_core::transactions::TransactionState::Pending),
        limit,
    )
    .await
    .map_err(|e| e.to_string())?;
    let transactions: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "transaction_id": r.id,
                "idempotency_key": r.idempotency_key,
                "app": r.app,
                "job_id": r.job_id,
                "profile": r.profile,
                "state": r.state,
                "evidence_sha": r.evidence_sha,
                "expires_at": state.config.transact.approval_deadline(r.created_at),
                "created_at": r.created_at,
            })
        })
        .collect();
    Ok(json!({
        "count": transactions.len(),
        // The honest half: on a node with `allow_live = false` every one of
        // these is unapprovable, and an agent that did not know that would keep
        // trying. Absence of the approve tool says it too; this says it in data.
        "allow_live": state.config.transact.allow_live,
        "approve_enabled": state.config.mcp.allow_approve && state.config.transact.allow_live,
        "transactions": transactions,
    }))
}

/// The `approve_transaction` tool (N01): the one tool on this surface that can
/// release an irreversible action.
///
/// It goes through the SAME pure decision function and the same SQL guard as
/// the HTTP door — not a second implementation of the rules — so an agent and a
/// human cannot get different answers about a stale approval, and a race
/// between them cannot submit twice.
async fn tool_approve_transaction(state: &AppState, args: &Value) -> Result<Value, String> {
    let id = require_str(args, "transaction_id")?.to_string();
    let quoted = require_str(args, "evidence_sha")?.to_string();
    let pool = state.storage.pool();
    pumper_core::transactions::expire_stale(&pool, state.config.transact.approval_ttl_secs)
        .await
        .map_err(|e| e.to_string())?;
    let row = pumper_core::transactions::get(&pool, &id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no transaction '{id}' (see list_pending_transactions)"))?;
    let submitted_today = pumper_core::transactions::submitted_since(
        &pool,
        row.profile.as_deref(),
        chrono::Utc::now() - chrono::Duration::hours(24),
    )
    .await
    .map_err(|e| e.to_string())?;
    pumper_core::approve_decision(
        state.config.transact.allow_live,
        row.state,
        state.config.transact.approval_deadline(row.created_at),
        chrono::Utc::now(),
        &row.evidence_sha,
        Some(quoted.as_str()),
        submitted_today,
        state.config.transact.daily_cap(),
    )
    .map_err(|refusal| refusal.message())?;
    if !pumper_core::transactions::mark_approved(&pool, &row.id, None)
        .await
        .map_err(|e| e.to_string())?
    {
        return Err(
            "the transaction stopped being pending while this approval was being decided \
             (another approver, or the expiry sweep) — nothing was submitted"
                .to_string(),
        );
    }
    // Release the parked run. Its authority is the ledger row, not this input.
    let resumed = match row.job_id.as_ref().and_then(|j| j.parse().ok()) {
        Some(job_id) => {
            let input = json!({ "transaction_id": row.id, "evidence_sha": row.evidence_sha });
            match state.storage.resume(job_id, &input).await {
                Ok(Some(job)) => {
                    state.events.emit(crate::events::JobEvent::new(
                        job.id,
                        job.app.clone(),
                        "queued",
                    ));
                    state.notify.notify_one();
                    true
                }
                _ => false,
            }
        }
        None => false,
    };
    Ok(json!({
        "transaction_id": row.id,
        "state": "approved",
        "job_id": row.job_id,
        "resumed": resumed,
        "note": "the parked job was released; it re-probes the live page and submits only if \
                 it still hashes to the approved evidence. Poll it with wait_job.",
    }))
}

// -- N04: the pipeline-authoring tools ---------------------------------------

/// Turns one HTTP handler's refusal into a readable tool error.
///
/// The status is carried into the text rather than dropped, because an agent
/// that cannot tell "unknown target app" (404) from "invalid on_change" (400)
/// from "budget_usd must be > 0" (422) will retry the wrong half of its body.
fn door_error(e: crate::routes::ApiError) -> String {
    let crate::routes::ApiError(status, message) = e;
    format!("[{}] {message}", status.as_u16())
}

/// Deserializes an agent's `arguments` into one of the HTTP request bodies.
///
/// The bodies ARE the schema: reusing them is what makes "same door, same
/// validation" true rather than aspirational, and a field the REST surface
/// gains is a field this tool gains with it.
fn tool_body<T: serde::de::DeserializeOwned>(args: &Value) -> Result<T, String> {
    serde_json::from_value(args.clone()).map_err(|e| format!("invalid arguments: {e}"))
}

/// The `create_trigger` tool (N04): authors one reactive edge through the same
/// handler `POST /triggers` runs — kind-aware validation, the pointer-syntax
/// check on `bind`/`each`, the `budget_usd > 0` floor, all of it.
async fn tool_create_trigger(state: &AppState, args: &Value) -> Result<Value, String> {
    let body: crate::routes::CreateTriggerBody = tool_body(args)?;
    let (_, axum::Json(trigger)) =
        crate::routes::create_trigger(axum::extract::State(state.clone()), axum::Json(body))
            .await
            .map_err(door_error)?;
    Ok(json!({
        "created": true,
        "trigger": trigger,
        "note": "the edge is live. Dry-run it with test_trigger, and read why it did or did \
                 not fire with trigger_decisions.",
    }))
}

/// The `test_trigger` tool (N04): the dry-run, wrapped.
async fn tool_test_trigger(state: &AppState, args: &Value) -> Result<Value, String> {
    let id = require_str(args, "trigger_id")?.to_string();
    let fire = args.get("fire").and_then(Value::as_bool).unwrap_or(false);
    let query: crate::routes::TestTriggerQuery = tool_body(&json!({ "fire": fire }))?;
    let axum::Json(out) = crate::routes::test_trigger(
        axum::extract::State(state.clone()),
        axum::extract::Path(id),
        axum::extract::Query(query),
    )
    .await
    .map_err(door_error)?;
    Ok(out)
}

/// The `trigger_decisions` tool (N04): one page of the ledger.
async fn tool_trigger_decisions(state: &AppState, args: &Value) -> Result<Value, String> {
    let id = require_str(args, "trigger_id")?.to_string();
    let mut q = serde_json::Map::new();
    if let Some(limit) = args.get("limit") {
        q.insert("limit".into(), limit.clone());
    }
    if let Some(cursor) = args.get("cursor") {
        q.insert("cursor".into(), cursor.clone());
    }
    let query: crate::routes::RunsQuery = tool_body(&Value::Object(q))?;
    let axum::Json(out) = crate::routes::trigger_runs(
        axum::extract::State(state.clone()),
        axum::extract::Path(id),
        axum::extract::Query(query),
    )
    .await
    .map_err(door_error)?;
    Ok(out)
}

/// The `create_watch` tool (N04): a dataset subscription, through the same
/// namespace gate `POST /watches` applies — so an agent cannot create the
/// `(app, dataset)` pair that could never fire and that the HTTP door refuses.
async fn tool_create_watch(state: &AppState, args: &Value) -> Result<Value, String> {
    let body: crate::routes::CreateWatchBody = tool_body(args)?;
    let (_, axum::Json(watch)) =
        crate::routes::create_watch(axum::extract::State(state.clone()), axum::Json(body))
            .await
            .map_err(door_error)?;
    Ok(json!({ "created": true, "watch": watch }))
}

/// The `create_ingress_source` tool (N04): the secret is in the response and
/// nowhere else, ever again — exactly as `POST /ingress/sources` behaves.
async fn tool_create_ingress_source(state: &AppState, args: &Value) -> Result<Value, String> {
    let body: crate::routes::CreateIngressSourceBody = tool_body(args)?;
    let (_, axum::Json(mut out)) =
        crate::routes::create_ingress_source(axum::extract::State(state.clone()), axum::Json(body))
            .await
            .map_err(door_error)?;
    if let Value::Object(map) = &mut out {
        map.insert(
            "note".into(),
            Value::String(
                "this is the ONLY time the secret is returned — hand it to the sender now. \
                 The sender POSTs signed bodies to /ingest/{source.id}; a source_kind \
                 'external' trigger on that id then reacts to them."
                    .into(),
            ),
        );
        map.insert(
            "ingress_enabled".into(),
            Value::Bool(state.config.ingress.enabled),
        );
    }
    Ok(out)
}

#[cfg(test)]
const EXPECTED_VALIDATING_DOORS: &[(&str, &str)] = &[
    // POST /apps/{name}/jobs — 422 with pointer paths.
    ("routes/jobs.rs", "validate_app_params"),
    // POST /schedules — 422, on the merged effective params.
    ("routes/schedules.rs", "validate_schedule_params"),
    // POST /triggers/{id}/test?fire=true — 422, same resolution as a live hop.
    ("routes/triggers.rs", "validate_app_params"),
    // The cron fire path — skips the row, `GET /schedules` shows invalid_params.
    ("scheduler.rs", "validate_app_params"),
    // Dataset/terminal/external trigger hops — records the `bad_params` outcome.
    ("triggers.rs", "validate_app_params"),
    // The MCP `enqueue_job` tool and its research sugar.
    ("mcp/mod.rs", "validate_app_params"),
    // N03 workflow steps — each step's RENDERED params are validated before the
    // step is claimed, so a template that resolves to something the app refuses
    // fails that step at its own door instead of minutes later.
    ("workflow.rs", "validate_app_params"),
];

/// Work-creating call sites that deliberately do NOT run the check, each with
/// the reason it cannot carry caller-supplied params.
#[cfg(test)]
const EXPECTED_EXEMPT_DOORS: &[(&str, &str)] = &[(
    "datahub.rs",
    "the governance actuator enqueues the app's OWN default_params verbatim (no caller input), \
     and `registry::scheduled_apps_default_params_pass_their_schema` pins those",
)];

/// Dispatches one JSON-RPC message from an anonymous caller. `None` =
/// notification (nothing to send).
///
/// The live surface goes through [`handle_rpc_as`] (it carries the request's
/// job token); this two-argument shape is what the e2e suite drives, so it is
/// test-only rather than a second production entry point that could drift.
///
/// It lives HERE, below the enqueue call sites, deliberately: the door
/// inventory (`every_door_that_creates_work_runs_the_shared_params_check`)
/// reads each file's production half as "everything before the first
/// `#[cfg(test)]`", so a test-gated item placed near the top of this file
/// hides the `enqueue` doors below it from the very test that polices them.
#[cfg(test)]
pub(crate) async fn handle_rpc(state: &AppState, msg: &Value) -> Option<Value> {
    handle_rpc_as(state, msg, &jobtoken::McpCaller::anonymous()).await
}

// ── N15: the self-hosted agent loop's `fetch` tool ───────────────────────────

/// The cost-event `detail` marking one fetch the Claude subprocess made for
/// itself. Zero-cost audit marker beside the real ledger row `AppContext::fetch`
/// already wrote — the money is counted once, and the receipt can still say how
/// many of a run's fetches came from the model rather than from the app.
pub(crate) const SELF_HOSTED_FETCH_DETAIL: &str = "claude_subfetch";

/// The strategy an agent may ask for, and the one it may not.
///
/// `auto_with_research` is deliberately unreachable: the caller of this tool
/// **is** the Claude tier, and letting it request a ladder that ends in another
/// Claude run is an unbounded spend loop wearing a per-job budget as its only
/// brake. The anti-pattern: `research_strategy_not_reachable_from_the_agent_loop`.
fn agent_fetch_strategy(name: Option<&str>) -> Result<pumper_core::FetchStrategy, String> {
    use pumper_core::FetchStrategy as S;
    match name {
        None | Some("auto") => Ok(S::Auto),
        Some("http") => Ok(S::Http),
        Some("browser") => Ok(S::Browser),
        Some("auto_with_research") | Some("claude") => Err(
            "strategy 'auto_with_research' is not available to this tool: you ARE the research \
             tier, and a research tier that can re-enter itself has no bound but the job budget. \
             Use 'auto' (http, escalating to a browser render)."
                .to_string(),
        ),
        Some(other) => Err(format!(
            "unknown strategy '{other}': use 'http', 'browser' or 'auto'"
        )),
    }
}

/// The body an agent gets back. Prefers Markdown (what the model asked the
/// ladder for), falls back to extracted text, then to raw HTML — and says which
/// via `content_kind`, so an empty `content` is never mistaken for an empty page.
fn fetch_body(outcome: &pumper_core::FetchOutcome, to_markdown: bool) -> (&'static str, String) {
    if to_markdown {
        if let Some(md) = outcome.markdown.as_deref().filter(|m| !m.is_empty()) {
            return ("markdown", md.to_string());
        }
    }
    if let Some(text) = outcome.text.as_deref().filter(|t| !t.is_empty()) {
        return ("text", text.to_string());
    }
    match outcome.html.as_deref().filter(|h| !h.is_empty()) {
        Some(html) => ("html", html.to_string()),
        None => ("none", String::new()),
    }
}

/// The `fetch` tool: one synchronous fetch through the calling job's own
/// metered [`pumper_core::AppContext::fetch`].
///
/// This is the whole point of N15. `fetch_readable` enqueues a job and hands
/// back an id, which is useless to a model mid-turn; the CLI's own `WebFetch`
/// answers in-turn but leaves the ladder entirely — no politeness spacing, no
/// response cache, no session profile, no archive tier, no tier-router
/// learning, and no ledger row, on the most expensive tier in the system. This
/// answers in-turn *and* stays inside the ladder.
async fn tool_fetch(
    state: &AppState,
    args: &Value,
    caller: &jobtoken::McpCaller,
) -> Result<Value, String> {
    let url = require_str(args, "url")?.to_string();
    let strategy = agent_fetch_strategy(args.get("strategy").and_then(Value::as_str))?;
    let to_markdown = args
        .get("to_markdown")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let ctx = jobtoken::job_context(state, caller).await?;

    let mut req = pumper_core::FetchRequest::new(url.clone());
    req.strategy = strategy;
    req.to_markdown = to_markdown;
    req.profile = args
        .get("profile")
        .and_then(Value::as_str)
        .map(str::to_string);
    req.archive_max_age = args.get("archive_max_age").and_then(Value::as_u64);

    let outcome = ctx.fetch(req).await.map_err(|e| e.to_string())?;
    // The zero-cost marker beside the row `ctx.fetch` already wrote: the money
    // is metered once (there), and this row is what lets `GET /jobs/{id}/receipt`
    // count how much of a run's egress the model drove. Accounting must never
    // fail the call — the fetch already happened.
    if let Err(e) = state
        .costs
        .record(
            ctx.job_id,
            &ctx.app,
            "mcp_fetch",
            Some(&url),
            0.0,
            Some(SELF_HOSTED_FETCH_DETAIL),
        )
        .await
    {
        tracing::warn!(job = %ctx.job_id, "self-hosted fetch marker not recorded: {e}");
    }
    let (content_kind, content) = fetch_body(&outcome, to_markdown);
    Ok(json!({
        "url": outcome.url,
        "engine": outcome.engine,
        "status": outcome.status,
        "content_kind": content_kind,
        "chars": content.chars().count(),
        "content": content,
        "cost_usd": outcome.cost_usd,
        "escalations": outcome.escalations,
        "trace": outcome.trace,
        "job_id": ctx.job_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::{
        agent_fetch_strategy, clamp_budget, fetch_body, validate_app_params, validate_params,
    };
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::path::Path;
    use std::sync::Arc;

    /// An app that declares a schema, so the shared door check has something to
    /// enforce.
    struct SchemaApp;

    #[async_trait::async_trait]
    impl pumper_core::ScrapeApp for SchemaApp {
        fn name(&self) -> &'static str {
            "schema-app"
        }
        fn default_params(&self) -> serde_json::Value {
            json!({ "query": "default" })
        }
        fn manifest(&self) -> pumper_core::AppManifest {
            pumper_core::AppManifest {
                params_schema: Some(json!({
                    "type": "object",
                    "required": ["query"],
                    "properties": { "rows": { "type": "integer", "maximum": 10 } }
                })),
                ..Default::default()
            }
        }
        async fn run(
            &self,
            _ctx: pumper_core::AppContext,
        ) -> pumper_core::Result<serde_json::Value> {
            Ok(json!({}))
        }
    }

    /// An app with no declared schema — the majority case.
    struct BareApp;

    #[async_trait::async_trait]
    impl pumper_core::ScrapeApp for BareApp {
        fn name(&self) -> &'static str {
            "bare-app"
        }
        async fn run(
            &self,
            _ctx: pumper_core::AppContext,
        ) -> pumper_core::Result<serde_json::Value> {
            Ok(json!({}))
        }
    }

    fn registry() -> HashMap<String, Arc<dyn pumper_core::ScrapeApp>> {
        let mut registry: HashMap<String, Arc<dyn pumper_core::ScrapeApp>> = HashMap::new();
        registry.insert("schema-app".into(), Arc::new(SchemaApp));
        registry.insert("bare-app".into(), Arc::new(BareApp));
        registry
    }

    /// The shared door check refuses exactly what the job door refuses, and is
    /// silent about the two cases it is not the authority on.
    #[test]
    fn shared_door_check_refuses_bad_params_and_passes_the_undeclared() {
        let registry = registry();
        let err = validate_app_params(&registry, "schema-app", &json!({ "rows": 99 }))
            .expect_err("a schema violation must be refused at every door");
        assert!(err.contains("params/rows"), "pointer path preserved: {err}");
        assert!(err.contains("query"), "missing required named: {err}");
        validate_app_params(&registry, "schema-app", &json!({ "query": "x", "rows": 3 }))
            .expect("valid params pass");
        // No declared schema = no contract to enforce.
        validate_app_params(&registry, "bare-app", &json!({ "anything": true }))
            .expect("no schema");
        // Unknown app: the CALLER owns that answer (404, or a ledger row), so the
        // check must not turn it into a params complaint.
        validate_app_params(&registry, "nope", &json!({})).expect("unknown app is not our answer");
    }

    /// Work-creating call sites in production code, by file (relative to `src`).
    fn work_creating_files() -> BTreeMap<String, BTreeSet<String>> {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        scan(&src, &src, &mut found);
        assert!(
            found.len() > 2,
            "the scan found almost nothing — it is looking in the wrong place, and a test that \
             cannot see the doors cannot police them"
        );
        found
    }

    fn scan(root: &Path, dir: &Path, found: &mut BTreeMap<String, BTreeSet<String>>) {
        for entry in
            std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                // `e2e/` is test-only by construction (its whole module tree is
                // `#[cfg(test)]`), so nothing in it is a production door.
                if path.file_name().and_then(|n| n.to_str()) == Some("e2e") {
                    continue;
                }
                scan(root, &path, found);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("read source");
            // Production half only: a test that enqueues is not a door.
            let production = match source.find("#[cfg(test)]") {
                Some(at) => &source[..at],
                None => &source[..],
            };
            let rel = path
                .strip_prefix(root)
                .expect("under src")
                .to_string_lossy()
                .replace('\\', "/");
            for line in production
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
            {
                // `.enqueue_dedup_as(` earns its own marker rather than being
                // caught by a prefix of `.enqueue_dedup(`: it is a DIFFERENT
                // symbol, and the day `routes/jobs.rs` switched to it (N20
                // principal stamping) this scan stopped seeing the platform's
                // primary enqueue door entirely. An inventory that silently
                // loses its most important row is worse than no inventory.
                for marker in [
                    ".enqueue(",
                    ".enqueue_dedup(",
                    ".enqueue_dedup_as(",
                    ".create_schedule(",
                ] {
                    if line.contains(marker) {
                        found.entry(rel.clone()).or_default().insert(marker.into());
                    }
                }
            }
        }
    }

    /// The anti-pattern: `POST /apps/{name}/jobs` validated params while
    /// `POST /schedules`, the cron fire path and every trigger hop did not — so
    /// the same app ran with params one door had already refused. Any new way to
    /// create work has to join the list (or be exempted, with a reason).
    #[test]
    fn every_door_that_creates_work_runs_the_shared_params_check() {
        let doors = work_creating_files();
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let validating: BTreeSet<&str> = super::EXPECTED_VALIDATING_DOORS
            .iter()
            .map(|(f, _)| *f)
            .collect();
        let exempt: BTreeSet<&str> = super::EXPECTED_EXEMPT_DOORS
            .iter()
            .map(|(f, _)| *f)
            .collect();

        let unlisted: Vec<&String> = doors
            .keys()
            .filter(|f| !validating.contains(f.as_str()) && !exempt.contains(f.as_str()))
            .collect();
        assert!(
            unlisted.is_empty(),
            "these files create work without being listed as doors — call \
             `mcp::validate_app_params` and add them to EXPECTED_VALIDATING_DOORS, or add them to \
             EXPECTED_EXEMPT_DOORS with the reason: {unlisted:?}"
        );

        for (door, symbol) in super::EXPECTED_VALIDATING_DOORS {
            assert!(
                doors.contains_key(*door),
                "{door} is listed as a door but no longer creates work — drop it from the list"
            );
            let source = std::fs::read_to_string(src.join(door))
                .unwrap_or_else(|e| panic!("read listed door {door}: {e}"));
            let production = match source.find("#[cfg(test)]") {
                Some(at) => &source[..at],
                None => &source[..],
            };
            assert!(
                production.contains(symbol),
                "{door} is listed as a validating door but never calls {symbol}"
            );
        }
        // The exemptions have to stay real doors: one that stopped creating work
        // is stale scaffolding pretending to be a reviewed decision.
        for (file, reason) in super::EXPECTED_EXEMPT_DOORS {
            assert!(
                doors.contains_key(*file),
                "{file} is exempted but no longer creates work — drop the exemption"
            );
            assert!(!reason.is_empty(), "{file}'s exemption needs a reason");
        }
    }

    /// The anti-pattern: the research tier handed a ladder that ends in another
    /// research run. Nothing but the job budget would bound the recursion, and
    /// a budget is a ceiling on the damage, not a design.
    #[test]
    fn research_strategy_not_reachable_from_the_agent_loop() {
        use pumper_core::FetchStrategy as S;
        assert_eq!(agent_fetch_strategy(None).unwrap(), S::Auto);
        assert_eq!(agent_fetch_strategy(Some("auto")).unwrap(), S::Auto);
        assert_eq!(agent_fetch_strategy(Some("http")).unwrap(), S::Http);
        assert_eq!(agent_fetch_strategy(Some("browser")).unwrap(), S::Browser);
        for refused in ["auto_with_research", "claude"] {
            let err = agent_fetch_strategy(Some(refused)).unwrap_err();
            assert!(err.contains("research tier"), "{err}");
        }
        assert!(agent_fetch_strategy(Some("teleport")).is_err());
    }

    /// An empty answer must be labelled, not silently returned as ordinary
    /// empty content: a model that reads `content: ""` with no `content_kind`
    /// cannot tell a blocked page from a genuinely blank one.
    #[test]
    fn an_empty_outcome_is_labelled_rather_than_returned_as_content() {
        let mut outcome = pumper_core::FetchOutcome {
            url: "https://example.com".into(),
            engine: "http",
            status: Some(200),
            html: None,
            markdown: None,
            text: None,
            escalations: Vec::new(),
            trace: Vec::new(),
            cost_usd: None,
            snapshot: None,
            network: Vec::new(),
        };
        assert_eq!(fetch_body(&outcome, true), ("none", String::new()));
        outcome.html = Some("<p>hi</p>".into());
        assert_eq!(fetch_body(&outcome, true), ("html", "<p>hi</p>".into()));
        outcome.text = Some("hi".into());
        assert_eq!(fetch_body(&outcome, true), ("text", "hi".into()));
        outcome.markdown = Some("# hi".into());
        assert_eq!(fetch_body(&outcome, true), ("markdown", "# hi".into()));
        // An empty markdown string is not an answer; fall through to text.
        outcome.markdown = Some(String::new());
        assert_eq!(fetch_body(&outcome, true), ("text", "hi".into()));
        // And a caller that did not ask for Markdown never gets it.
        outcome.markdown = Some("# hi".into());
        assert_eq!(fetch_body(&outcome, false), ("text", "hi".into()));
    }

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
