use std::convert::Infallible;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use pumper_core::{EnqueueOptions, HostProfile, Job, JobStatus, Schedule};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use uuid::Uuid;

use crate::events::JobEvent;
use crate::state::AppState;

/// Top-level metadata for the generated OpenAPI document. Route operations are
/// collected from each `#[utoipa::path]` handler by `OpenApiRouter` (see
/// `openapi_router`), so this only carries document-level info and tags.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "pumper HTTP API",
        description = "Local scraping / data-product service. Machine-readable surface \
                       for the routes documented in docs/features/http-api.md.",
        version = "0.1.0",
    ),
    tags(
        (name = "health", description = "Liveness and Prometheus metrics"),
        (name = "apps", description = "Registered scraping apps and enqueue"),
        (name = "jobs", description = "Job queue lifecycle"),
        (name = "costs", description = "Engine spend ledger"),
        (name = "schedules", description = "Cron schedules"),
        (name = "datasets", description = "Change-detected dataset records, export, history"),
        (name = "grants", description = "Filtered query surface over the cross-source grants corpus"),
        (name = "watches", description = "Dataset change webhooks"),
        (name = "triggers", description = "Reactive pipelines"),
        (name = "webhooks", description = "Outbound delivery log"),
        (name = "search", description = "Full-text search and saved searches"),
        (name = "extract", description = "Declarative RuleSet preview / dry-run"),
        (name = "plugins", description = "WASM plugin host"),
        (name = "events", description = "Server-sent event streams"),
        (name = "hosts", description = "Learned per-host tier memory and politeness"),
        (name = "profiles", description = "Session vault: named login profiles"),
        (name = "meta", description = "The OpenAPI document itself"),
    )
)]
struct ApiDoc;

/// Builds the router with every route registered through its `#[utoipa::path]`
/// annotation, so the axum routing table and the OpenAPI document are generated
/// from a single source and cannot drift. Registering a route without an
/// annotation fails to compile; the path-coverage test guards the inverse.
fn openapi_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(health))
        .routes(routes!(metrics))
        .routes(routes!(stream_events))
        .routes(routes!(list_apps))
        .routes(routes!(enqueue_job))
        .routes(routes!(list_datasets))
        .routes(routes!(list_jobs))
        .routes(routes!(get_job, cancel_job))
        .routes(routes!(retry_job))
        .routes(routes!(bulk_retry_jobs))
        .routes(routes!(reset_job))
        .routes(routes!(stream_job))
        .routes(routes!(job_costs))
        .routes(routes!(cost_summary))
        .routes(routes!(list_schedules, create_schedule))
        .routes(routes!(delete_schedule))
        .routes(routes!(set_schedule_enabled))
        .routes(routes!(list_records))
        .routes(routes!(export_records))
        .routes(routes!(dataset_duplicates))
        .routes(routes!(dataset_changes))
        .routes(routes!(record_history))
        .routes(routes!(list_watches, create_watch))
        .routes(routes!(delete_watch))
        .routes(routes!(set_watch_enabled))
        .routes(routes!(list_triggers, create_trigger))
        .routes(routes!(delete_trigger))
        .routes(routes!(set_trigger_enabled))
        .routes(routes!(test_trigger))
        .routes(routes!(trigger_runs))
        .routes(routes!(list_deliveries))
        .routes(routes!(get_delivery))
        .routes(routes!(replay_delivery))
        .routes(routes!(list_hosts))
        .routes(routes!(get_host))
        .routes(routes!(delete_host_memory))
        .routes(routes!(list_profiles))
        .routes(routes!(list_plugins))
        .routes(routes!(reload_plugins))
        .routes(routes!(search))
        .routes(routes!(delete_search_docs))
        .routes(routes!(list_saved_searches, create_saved_search))
        .routes(routes!(delete_saved_search))
        .routes(routes!(set_saved_search_enabled))
        .routes(routes!(delete_search_dataset))
        .routes(routes!(extract_preview))
        .routes(routes!(list_grants))
        .routes(routes!(closing_soon))
        .routes(routes!(openapi_json))
}

pub fn router(state: AppState) -> Router {
    let (router, _api) = openapi_router().split_for_parts();
    let router = router.layer(tower_http::trace::TraceLayer::new_for_http());
    // CORS is OFF by default (same-origin only). A permissive allow-all on an
    // unauthenticated, mutating, data-bearing API lets any site the operator
    // visits drive it cross-origin (DNS-rebinding defeats the localhost
    // assumption). A trusted local UI opts in via [server] cors_allowed_origins.
    let origins: Vec<axum::http::HeaderValue> = state
        .config
        .server
        .cors_allowed_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();
    let router = if origins.is_empty() {
        router
    } else {
        router.layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(origins)
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any),
        )
    };
    router.with_state(state)
}

/// Upper bound on a client-supplied `max_attempts`, so a job/schedule/trigger
/// can't request a practically-non-terminating retry loop.
const MAX_ATTEMPTS_CAP: i64 = 20;

/// Parses an optional RFC-3339 `since` query param. A malformed value is the
/// client's mistake, so it is a 400 — not the blanket 500 a bare `?` would give.
fn parse_since(since: Option<&str>) -> Result<Option<chrono::DateTime<chrono::Utc>>, ApiError> {
    since
        .map(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&chrono::Utc))
                .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("invalid 'since': {e}")))
        })
        .transpose()
}

/// Serves the generated OpenAPI 3.1 document. The spec is rebuilt from the same
/// route registration used by `router`, so it always matches what is served.
#[utoipa::path(
    get,
    path = "/openapi.json",
    tag = "meta",
    responses((status = 200, description = "OpenAPI 3.1 document for this API"))
)]
async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(openapi_router().split_for_parts().1)
}

struct ApiError(StatusCode, String);

/// Stable machine-readable code derived from the HTTP status, sent alongside the
/// human `error` string so consumers can branch without string-matching. Kept in
/// lockstep with the statuses the handlers actually emit.
fn error_code(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "bad_request",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::CONFLICT => "conflict",
        StatusCode::PAYLOAD_TOO_LARGE => "too_large",
        StatusCode::UNPROCESSABLE_ENTITY => "unprocessable",
        _ => "internal",
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = error_code(self.0);
        (self.0, Json(json!({ "error": self.1, "code": code }))).into_response()
    }
}

impl From<pumper_core::Error> for ApiError {
    fn from(e: pumper_core::Error) -> Self {
        // A BadRequest is the one core error that is definitionally the client's
        // fault (a malformed query/filter/rule) → 400. Everything else is
        // unexpected at the request boundary → 500; the client-distinguishable
        // outcomes (404/409/400) are otherwise raised explicitly by the handlers.
        match e {
            pumper_core::Error::BadRequest(msg) => Self(StatusCode::BAD_REQUEST, msg),
            other => Self(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        }
    }
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses((status = 200, description = "Service is up (`{\"status\":\"ok\"}`)"))
)]
async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

// ---- Observability --------------------------------------------------------

/// How long a rendered `/metrics` body is served from cache before the aggregate
/// queries are re-run. Short enough that a scrape is never meaningfully stale.
const METRICS_TTL: std::time::Duration = std::time::Duration::from_secs(5);

fn metrics_response(body: String) -> Response {
    ([("content-type", "text/plain; version=0.0.4")], body).into_response()
}

/// Prometheus-style text exposition of queue + platform gauges. Cached for
/// `METRICS_TTL` so a burst of scrapes doesn't re-run the aggregate queries each
/// time (the render touches jobs, costs, schedules, and timing in one pass).
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "health",
    responses((status = 200, description = "Prometheus text exposition (content-type text/plain; version=0.0.4)", content_type = "text/plain"))
)]
async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    {
        let cached = state.metrics_cache.lock().await;
        if let Some((at, body)) = cached.as_ref() {
            if at.elapsed() < METRICS_TTL {
                return Ok(metrics_response(body.clone()));
            }
        }
    }

    let counts = state.storage.status_counts().await?;
    let failures = state.storage.failure_counts().await?;
    let timing = state.storage.job_timing_stats().await?;
    let schedules = state.storage.list_schedules().await?;
    let mut out = String::new();
    out.push_str("# HELP pumper_jobs Jobs by status\n# TYPE pumper_jobs gauge\n");
    for status in ["queued", "running", "succeeded", "failed", "cancelled"] {
        let n = counts.iter().find(|(s, _)| s == status).map_or(0, |(_, n)| *n);
        out.push_str(&format!("pumper_jobs{{status=\"{status}\"}} {n}\n"));
    }
    // Permanent failures per app. DB-derived (current `failed` row count per app),
    // so not strictly monotonic — a retried job leaves the failed set.
    out.push_str(
        "# HELP pumper_job_failures_total Permanently-failed jobs by app (DB-derived count)\n\
         # TYPE pumper_job_failures_total counter\n",
    );
    for (app, n) in &failures {
        out.push_str(&format!("pumper_job_failures_total{{app=\"{app}\"}} {n}\n"));
    }
    out.push_str(
        "# HELP pumper_job_duration_seconds Job execution time (started -> finished)\n\
         # TYPE pumper_job_duration_seconds summary\n",
    );
    out.push_str(&format!(
        "pumper_job_duration_seconds_sum {}\npumper_job_duration_seconds_count {}\n",
        timing.duration_sum, timing.duration_count
    ));
    out.push_str(
        "# HELP pumper_job_duration_seconds_max Longest job execution time\n\
         # TYPE pumper_job_duration_seconds_max gauge\n",
    );
    out.push_str(&format!("pumper_job_duration_seconds_max {}\n", timing.duration_max));
    out.push_str(
        "# HELP pumper_job_queue_wait_seconds Queue wait (created -> started)\n\
         # TYPE pumper_job_queue_wait_seconds summary\n",
    );
    out.push_str(&format!(
        "pumper_job_queue_wait_seconds_sum {}\npumper_job_queue_wait_seconds_count {}\n",
        timing.wait_sum, timing.wait_count
    ));
    out.push_str(
        "# HELP pumper_job_queue_wait_seconds_max Longest queue wait\n\
         # TYPE pumper_job_queue_wait_seconds_max gauge\n",
    );
    out.push_str(&format!("pumper_job_queue_wait_seconds_max {}\n", timing.wait_max));
    out.push_str("# HELP pumper_cost_usd Total engine spend by app\n# TYPE pumper_cost_usd gauge\n");
    for entry in state.costs.summary(None, None).await? {
        out.push_str(&format!(
            "pumper_cost_usd{{app=\"{}\",engine=\"{}\"}} {}\n",
            entry.app, entry.engine, entry.cost_usd
        ));
    }
    out.push_str("# HELP pumper_apps Registered apps\n# TYPE pumper_apps gauge\n");
    out.push_str(&format!("pumper_apps {}\n", state.registry.len()));
    out.push_str("# HELP pumper_schedules Configured schedules\n# TYPE pumper_schedules gauge\n");
    let enabled = schedules.iter().filter(|s| s.enabled).count();
    out.push_str(&format!("pumper_schedules{{enabled=\"true\"}} {enabled}\n"));
    out.push_str(&format!(
        "pumper_schedules{{enabled=\"false\"}} {}\n",
        schedules.len() - enabled
    ));

    *state.metrics_cache.lock().await = Some((std::time::Instant::now(), out.clone()));
    Ok(metrics_response(out))
}

/// SSE stream of all job status transitions.
///
/// Every event carries a monotonic id. A client reconnecting with a
/// `Last-Event-ID` header is replayed the events it missed from the in-memory
/// ring; if the gap is older than the ring retains, a single `reset` event is
/// emitted first so the client knows to resync its view. Live subscribers that
/// fall behind the broadcast buffer recover the same way instead of dropping
/// events silently.
#[utoipa::path(
    get,
    path = "/events",
    tag = "events",
    responses((status = 200, description = "SSE stream of job status transitions. Each event carries a monotonic `id`; reconnect with a `Last-Event-ID` header to replay the missed gap (or receive a `reset` event when it is too old).", content_type = "text/event-stream"))
)]
async fn stream_events(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let after = last_event_id(&headers);
    let mut rx = state.events.subscribe();
    let (initial, mut last_seq) = resume(&state, after, |_| true);
    let stream = async_stream::stream! {
        for ev in initial {
            yield Ok(ev);
        }
        loop {
            match rx.recv().await {
                Ok((seq, event)) => {
                    if seq <= last_seq {
                        continue; // already replayed (overlap window)
                    }
                    last_seq = seq;
                    yield Ok(sse_event(seq, &event));
                }
                Err(RecvError::Lagged(_)) => {
                    for ev in recover(&state, &mut last_seq, |_| true) {
                        yield Ok(ev);
                    }
                }
                Err(RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// SSE stream scoped to one job; closes once the job reaches a terminal state.
/// Supports the same `Last-Event-ID` resume as `/events`, filtered to this job.
#[utoipa::path(
    get,
    path = "/jobs/{id}/stream",
    tag = "events",
    params(("id" = Uuid, Path, description = "Job id")),
    responses((status = 200, description = "SSE stream scoped to one job; replays current state on connect, closes at terminal. Same `Last-Event-ID` resume as `/events`.", content_type = "text/event-stream"))
)]
async fn stream_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: axum::http::HeaderMap,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let after = last_event_id(&headers);
    // Subscribe before snapshotting so no transition slips through the gap.
    let mut rx = state.events.subscribe();
    // A fresh connect (no resume point) gets the current state up front; a
    // resuming client already has it and only wants the gap.
    let snapshot = if after.is_none() {
        state.storage.get(id).await.ok().flatten()
    } else {
        None
    };
    let (replayed, mut last_seq) = resume(&state, after, move |ev| ev.job_id == id);
    let stream = async_stream::stream! {
        for ev in replayed {
            yield Ok(ev);
        }
        if let Some(job) = snapshot {
            let mut event = JobEvent::new(job.id, job.app.clone(), job.status.as_str());
            event.result = job.result.clone();
            event.error = job.error.clone();
            yield Ok(snapshot_event(&event));
            if is_terminal(job.status) {
                return;
            }
        }
        loop {
            match rx.recv().await {
                Ok((seq, event)) => {
                    if seq <= last_seq {
                        continue;
                    }
                    last_seq = seq;
                    if event.job_id != id {
                        continue;
                    }
                    let done = matches!(event.status.as_str(), "succeeded" | "failed" | "cancelled");
                    yield Ok(sse_event(seq, &event));
                    if done {
                        break;
                    }
                }
                Err(RecvError::Lagged(_)) => {
                    for ev in recover(&state, &mut last_seq, |ev| ev.job_id == id) {
                        yield Ok(ev);
                    }
                }
                Err(RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Parses a `Last-Event-ID` header into the sequence id the client last saw.
fn last_event_id(headers: &axum::http::HeaderMap) -> Option<u64> {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse().ok())
}

/// Builds the connect-time replay for a resuming client: the buffered events it
/// missed (filtered by `keep`), preceded by a `reset` marker when the gap is too
/// old. Returns the events plus the highest sequence id now delivered, which the
/// live loop uses to dedup the broadcast overlap window.
fn resume(
    state: &AppState,
    after: Option<u64>,
    keep: impl Fn(&JobEvent) -> bool,
) -> (Vec<Event>, u64) {
    let Some(after) = after else {
        return (Vec::new(), 0);
    };
    match state.events.replay(after) {
        crate::events::Replay::Reset => {
            let latest = state.events.latest_seq();
            (vec![reset_event(latest)], latest)
        }
        crate::events::Replay::Events(events) => {
            let mut last = after;
            let mut out = Vec::new();
            for (seq, event) in events {
                last = seq;
                if keep(&event) {
                    out.push(sse_event(seq, &event));
                }
            }
            (out, last)
        }
    }
}

/// Recovers a live subscriber that lagged past the broadcast buffer: replays the
/// ring past `last_seq`, advancing it, or emits a single `reset` when the gap is
/// unrecoverable.
fn recover(state: &AppState, last_seq: &mut u64, keep: impl Fn(&JobEvent) -> bool) -> Vec<Event> {
    match state.events.replay(*last_seq) {
        crate::events::Replay::Reset => {
            let latest = state.events.latest_seq();
            *last_seq = latest;
            vec![reset_event(latest)]
        }
        crate::events::Replay::Events(events) => {
            let mut out = Vec::new();
            for (seq, event) in events {
                *last_seq = seq;
                if keep(&event) {
                    out.push(sse_event(seq, &event));
                }
            }
            out
        }
    }
}

fn sse_event(seq: u64, event: &JobEvent) -> Event {
    Event::default()
        .id(seq.to_string())
        .event("job")
        .json_data(event)
        .unwrap_or_else(|_| Event::default().comment("serialize error"))
}

/// Connect-time snapshot of a job's current state (no sequence id — it is a
/// synthesized view, not a buffered transition).
fn snapshot_event(event: &JobEvent) -> Event {
    Event::default()
        .event("job")
        .json_data(event)
        .unwrap_or_else(|_| Event::default().comment("serialize error"))
}

/// Signals a resuming client that its requested id fell out of the replay ring;
/// it should discard assumptions and resync. Carries the latest id so the client
/// can advance its `Last-Event-ID` pointer.
fn reset_event(latest: u64) -> Event {
    Event::default()
        .id(latest.to_string())
        .event("reset")
        .data("replay gap: reconnect point too old, resync state")
}

fn is_terminal(status: JobStatus) -> bool {
    matches!(
        status,
        JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled
    )
}

// ---- Apps & jobs ----------------------------------------------------------

#[utoipa::path(
    get,
    path = "/apps",
    tag = "apps",
    responses((status = 200, description = "`{apps: [{name, description, schedule}]}`"))
)]
async fn list_apps(State(state): State<AppState>) -> Json<Value> {
    let mut apps: Vec<_> = state.registry.values().collect();
    apps.sort_by_key(|app| app.name());
    let apps: Vec<_> = apps
        .into_iter()
        .map(|app| {
            json!({
                "name": app.name(),
                "description": app.description(),
                "schedule": app.schedule(),
            })
        })
        .collect();
    Json(json!({ "apps": apps }))
}

#[derive(Deserialize, Default, ToSchema)]
struct EnqueueBody {
    params: Option<Value>,
    max_attempts: Option<i64>,
    delay_secs: Option<u64>,
    priority: Option<i64>,
    /// POST the finished job here on terminal state.
    callback_url: Option<String>,
    /// If set, the callback body is HMAC-SHA256 signed with this secret.
    callback_secret: Option<String>,
    /// Spend ceiling for the whole job; metered Claude calls abort past it.
    budget_usd: Option<f64>,
    /// Dedup key: retrying an enqueue with the same key returns the original
    /// job (200) instead of creating a duplicate. The `Idempotency-Key`
    /// header takes precedence over this field.
    idempotency_key: Option<String>,
}

#[utoipa::path(
    post,
    path = "/apps/{name}/jobs",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    request_body = EnqueueBody,
    responses(
        (status = 202, description = "Job enqueued", body = Object),
        (status = 200, description = "Idempotency-Key replay: the original job", body = Object),
        (status = 404, description = "Unknown app", body = Object),
    )
)]
async fn enqueue_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: axum::http::HeaderMap,
    body: Option<Json<EnqueueBody>>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    let Some(app) = state.registry.get(&name) else {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("unknown app '{name}'")));
    };
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .or(body.idempotency_key)
        .filter(|k| !k.trim().is_empty());
    let opts = EnqueueOptions {
        params: body.params.unwrap_or_else(|| app.default_params()),
        max_attempts: body.max_attempts.unwrap_or(1).clamp(1, MAX_ATTEMPTS_CAP),
        delay_secs: body.delay_secs.unwrap_or(0),
        priority: body.priority.unwrap_or(0),
        callback_url: body.callback_url,
        callback_secret: body.callback_secret,
        budget_usd: body.budget_usd.filter(|b| *b > 0.0),
        idempotency_key,
        schedule_id: None,
        trigger_id: None,
    };
    let (job, created) = state.storage.enqueue_dedup(&name, opts).await?;
    if created {
        state.notify.notify_one();
        Ok((StatusCode::ACCEPTED, Json(job)))
    } else {
        // Replayed request: the original job, not a new one.
        Ok((StatusCode::OK, Json(job)))
    }
}

#[derive(Deserialize, IntoParams)]
struct ListQuery {
    app: Option<String>,
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor. Presence (even empty, for page 1) switches the
    /// response to `{items, next_cursor}`; absent keeps the legacy bare array.
    cursor: Option<String>,
}

fn default_limit() -> i64 {
    50
}

/// Cursors are `<sort-timestamp>|<tiebreak-id>` — decode back to the pair.
fn parse_cursor(cursor: &str) -> Option<(String, String)> {
    let trimmed = cursor.trim();
    if trimmed.is_empty() {
        return None; // first page
    }
    trimmed.split_once('|').map(|(t, k)| (t.to_string(), k.to_string()))
}

/// Cursor variant for revision feeds whose tiebreak is numeric (a rowid or a
/// per-key revision number). A malformed or empty cursor pages from the top.
fn parse_cursor_i64(cursor: &str) -> Option<(String, i64)> {
    parse_cursor(cursor).and_then(|(t, k)| k.parse().ok().map(|n| (t, n)))
}

/// Next-page cursor for a keyset page: `Some` only when the page came back full
/// (so more rows may remain), built from the last item. Mirrors the inline
/// pattern on `/jobs` and `/datasets/...`.
fn keyset_cursor<T>(items: &[T], limit: i64, encode: impl Fn(&T) -> String) -> Option<String> {
    ((items.len() as i64) == limit)
        .then(|| items.last())
        .flatten()
        .map(encode)
}

#[utoipa::path(
    get,
    path = "/jobs",
    tag = "jobs",
    params(ListQuery),
    responses((status = 200, description = "Dual-mode: without `cursor` a bare `[Job]` array; with `cursor` present (even empty) `{items: [Job], next_cursor}` paged by keyset."))
)]
async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let status = query
        .status
        .as_deref()
        .map(|s| {
            JobStatus::parse(s)
                .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, format!("invalid status '{s}'")))
        })
        .transpose()?;
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        let jobs = state.storage.list(query.app.as_deref(), status, limit).await?;
        return Ok(Json(json!(jobs)));
    };
    let after = parse_cursor(cursor);
    let jobs = state
        .storage
        .list_page(query.app.as_deref(), status, after, limit)
        .await?;
    let next_cursor = keyset_cursor(&jobs, limit, |j| {
        format!("{}|{}", pumper_core::datasets::ts(j.created_at), j.id)
    });
    Ok(Json(json!({ "items": jobs, "next_cursor": next_cursor })))
}

#[utoipa::path(
    get,
    path = "/jobs/{id}",
    tag = "jobs",
    params(("id" = Uuid, Path, description = "Job id")),
    responses(
        (status = 200, description = "The job", body = Object),
        (status = 404, description = "Job not found", body = Object),
    )
)]
async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let job = state
        .storage
        .get(id)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "job not found".into()))?;
    let mut body = serde_json::to_value(&job).unwrap_or_else(|_| json!({}));
    // A running long job's latest live-progress snapshot (in-memory; absent once
    // the job finalizes or after a restart). Additive — the job fields are
    // unchanged.
    if let (Value::Object(map), Some(snapshot)) = (&mut body, state.progress.snapshot(&id)) {
        map.insert("progress".into(), snapshot);
    }
    Ok(Json(body))
}

/// Re-queues a failed or cancelled job with one more attempt.
#[utoipa::path(
    post,
    path = "/jobs/{id}/retry",
    tag = "jobs",
    params(("id" = Uuid, Path, description = "Job id")),
    responses(
        (status = 202, description = "Re-queued job", body = Object),
        (status = 404, description = "Job not found", body = Object),
        (status = 409, description = "Job not in a retryable (failed/cancelled) state", body = Object),
    )
)]
async fn retry_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    match state.storage.retry(id).await? {
        Some(job) => {
            state.events.emit(JobEvent::new(job.id, job.app.clone(), "queued"));
            state.notify.notify_one();
            Ok((StatusCode::ACCEPTED, Json(job)))
        }
        None => Err(job_state_error(
            &state,
            id,
            "job is not in a retryable state (failed/cancelled)",
        )
        .await),
    }
}

#[derive(Deserialize, ToSchema, Default)]
struct BulkRetryBody {
    /// Terminal state to resurrect: `failed` (default) or `cancelled`.
    status: Option<String>,
    /// Restrict the batch to one app.
    app: Option<String>,
    /// Max jobs to re-queue (clamped 1..=500, default 500).
    limit: Option<i64>,
}

/// Bulk re-queue: re-queues every job in the given terminal state (default
/// `failed`), optionally scoped to one app, up to a cap — each with one more
/// attempt. Returns the count and the ids re-queued.
#[utoipa::path(
    post,
    path = "/jobs/retry",
    tag = "jobs",
    request_body = BulkRetryBody,
    responses(
        (status = 200, description = "`{retried: <count>, ids: [uuid]}`"),
        (status = 400, description = "status must be failed|cancelled", body = Object),
    )
)]
async fn bulk_retry_jobs(
    State(state): State<AppState>,
    body: Option<Json<BulkRetryBody>>,
) -> Result<Json<Value>, ApiError> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let status = match body.status.as_deref().unwrap_or("failed") {
        "failed" => JobStatus::Failed,
        "cancelled" => JobStatus::Cancelled,
        other => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!("status must be failed|cancelled, got '{other}'"),
            ))
        }
    };
    let cap = body.limit.unwrap_or(500).clamp(1, 500);
    let ids = state.storage.retry_bulk(status, body.app.as_deref(), cap).await?;
    for id in &ids {
        state.events.emit(JobEvent::new(*id, "", "queued"));
    }
    if !ids.is_empty() {
        state.notify.notify_one();
    }
    Ok(Json(json!({ "retried": ids.len(), "ids": ids })))
}

/// Re-queues a `running` job (e.g. one stuck on a hung task) with a fresh
/// attempt budget. The orphaned task's late completion is discarded by the
/// `(status, attempts)` fence on the worker's finish/fail writes.
#[utoipa::path(
    post,
    path = "/jobs/{id}/reset",
    tag = "jobs",
    params(("id" = Uuid, Path, description = "Job id")),
    responses(
        (status = 202, description = "Re-queued job", body = Object),
        (status = 404, description = "Job not found", body = Object),
        (status = 409, description = "Job not in `running` state", body = Object),
    )
)]
async fn reset_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    match state.storage.reset(id).await? {
        Some(job) => {
            state.events.emit(JobEvent::new(job.id, job.app.clone(), "queued"));
            state.notify.notify_one();
            Ok((StatusCode::ACCEPTED, Json(job)))
        }
        None => Err(job_state_error(&state, id, "job is not in 'running' state").await),
    }
}

/// Distinguishes a missing job (404) from a job in the wrong state (409) after a
/// state-guarded mutation reported no rows changed — one extra lookup to give the
/// caller an actionable status instead of a blanket conflict.
async fn job_state_error(state: &AppState, id: Uuid, wrong_state: &str) -> ApiError {
    match state.storage.get(id).await {
        Ok(Some(_)) => ApiError(StatusCode::CONFLICT, wrong_state.into()),
        Ok(None) => ApiError(StatusCode::NOT_FOUND, "job not found".into()),
        Err(e) => e.into(),
    }
}

/// Cancels a job. A `queued` job is cancelled synchronously; a `running` job
/// has its cancellation token fired so the worker aborts the app future and
/// marks it `cancelled` (the response reports `running: true`). A terminal job
/// is `409`, an unknown one `404`.
#[utoipa::path(
    delete,
    path = "/jobs/{id}",
    tag = "jobs",
    params(("id" = Uuid, Path, description = "Job id")),
    responses(
        (status = 200, description = "Cancelled (`{cancelled: true}`; `running: true` when it was in-flight)"),
        (status = 404, description = "Job not found", body = Object),
        (status = 409, description = "Job already terminal (succeeded/failed/cancelled)", body = Object),
    )
)]
async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    // Queued job: cancel synchronously.
    if state.storage.cancel(id).await? {
        state.events.emit(JobEvent::new(id, "", "cancelled"));
        return Ok(Json(json!({ "cancelled": true })));
    }
    // Otherwise it may be running here: fire its cancellation token. The worker
    // task races it against the app future and persists `cancelled` + emits the
    // terminal event, so we don't touch storage or emit from the request path.
    let token = state
        .job_cancels
        .lock()
        .unwrap()
        .get(&id)
        .map(|(_, t)| t.clone());
    if let Some(token) = token {
        token.cancel();
        return Ok(Json(json!({ "cancelled": true, "running": true })));
    }
    Err(job_state_error(&state, id, "job is already terminal (succeeded/failed/cancelled)").await)
}

// ---- Costs ------------------------------------------------------------------

/// A job's cost events + total, with cost-per-fresh-record yield when the
/// job's result exposes new/changed counts (the upsert-summary convention).
#[utoipa::path(
    get,
    path = "/jobs/{id}/costs",
    tag = "costs",
    params(("id" = Uuid, Path, description = "Job id")),
    responses(
        (status = 200, description = "`{job_id, app, total_usd, calls, fresh_records, cost_per_fresh_record_usd, events}`"),
        (status = 404, description = "Job not found", body = Object),
    )
)]
async fn job_costs(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let Some(job) = state.storage.get(id).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, "job not found".into()));
    };
    let events = state.costs.job_events(id).await?;
    let total: f64 = events.iter().map(|e| e.cost_usd).sum();
    let fresh = job.result.as_ref().map(|r| {
        r.get("new").and_then(Value::as_u64).unwrap_or(0)
            + r.get("changed").and_then(Value::as_u64).unwrap_or(0)
    });
    let cost_per_fresh_record = match fresh {
        Some(n) if n > 0 => Some(total / n as f64),
        _ => None,
    };
    Ok(Json(json!({
        "job_id": id,
        "app": job.app,
        "total_usd": total,
        "calls": events.len(),
        "fresh_records": fresh,
        "cost_per_fresh_record_usd": cost_per_fresh_record,
        "events": events,
    })))
}

#[derive(Deserialize, IntoParams)]
struct CostSummaryQuery {
    app: Option<String>,
    /// RFC 3339 lower bound for the window.
    since: Option<String>,
}

/// Spend grouped by (app, engine) — the ROI overview.
#[utoipa::path(
    get,
    path = "/costs",
    tag = "costs",
    params(CostSummaryQuery),
    responses((status = 200, description = "`{total_usd, by_app_engine: [{app, engine, cost_usd}]}`"))
)]
async fn cost_summary(
    State(state): State<AppState>,
    Query(query): Query<CostSummaryQuery>,
) -> Result<Json<Value>, ApiError> {
    let since = parse_since(query.since.as_deref())?;
    let summary = state.costs.summary(query.app.as_deref(), since).await?;
    let total: f64 = summary.iter().map(|s| s.cost_usd).sum();
    Ok(Json(json!({ "total_usd": total, "by_app_engine": summary })))
}

// ---- Schedules ------------------------------------------------------------

#[derive(Deserialize, IntoParams)]
struct SchedulesQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/schedules",
    tag = "schedules",
    params(SchedulesQuery),
    responses((status = 200, description = "Dual-mode: bare `[Schedule]` array, or `{items, next_cursor}` when `cursor` is present."))
)]
async fn list_schedules(
    State(state): State<AppState>,
    Query(query): Query<SchedulesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        // Legacy bare-array mode is still capped: an uncursored list must not
        // stream an entire table.
        let items = state.storage.list_schedules_page(None, limit).await?;
        return Ok(Json(json!(items)));
    };
    let after = parse_cursor(cursor);
    let items = state.storage.list_schedules_page(after, limit).await?;
    let next_cursor = keyset_cursor(&items, limit, |s| {
        format!("{}|{}", pumper_core::datasets::ts(s.created_at), s.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(Deserialize, ToSchema)]
struct CreateScheduleBody {
    app: String,
    /// 6-field cron with seconds: "sec min hour day month weekday".
    cron: String,
    params: Option<Value>,
    priority: Option<i64>,
    /// IANA timezone the cron is evaluated in (e.g. "America/New_York"); omitted
    /// = UTC. An unknown name is rejected with 400 `bad_request`.
    timezone: Option<String>,
    /// Catch-up policy for firings missed while the scheduler was down:
    /// "fire_once" (default) runs a single job; "skip" runs none.
    misfire_policy: Option<String>,
    /// Attempt budget for jobs this schedule enqueues; omitted = server default (3).
    max_attempts: Option<i64>,
}

#[utoipa::path(
    post,
    path = "/schedules",
    tag = "schedules",
    request_body = CreateScheduleBody,
    responses(
        (status = 201, description = "Created schedule", body = Object),
        (status = 400, description = "Invalid cron, timezone, or misfire_policy", body = Object),
        (status = 404, description = "Unknown app", body = Object),
    )
)]
async fn create_schedule(
    State(state): State<AppState>,
    Json(body): Json<CreateScheduleBody>,
) -> Result<(StatusCode, Json<Schedule>), ApiError> {
    if !state.registry.contains_key(&body.app) {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("unknown app '{}'", body.app)));
    }
    // Validate the cron expression up front.
    use std::str::FromStr;
    cron::Schedule::from_str(&body.cron)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("invalid cron: {e}")))?;

    // Validate the timezone against the chrono-tz database.
    if let Some(tz) = &body.timezone {
        chrono_tz::Tz::from_str(tz)
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, format!("unknown timezone '{tz}'")))?;
    }

    // Validate the misfire policy.
    let misfire_policy = body.misfire_policy.as_deref().unwrap_or("fire_once");
    if !matches!(misfire_policy, "fire_once" | "skip") {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("unknown misfire_policy '{misfire_policy}' (expected 'fire_once' or 'skip')"),
        ));
    }

    let schedule = state
        .storage
        .create_schedule(pumper_core::NewSchedule {
            app: &body.app,
            cron: &body.cron,
            params: body.params.unwrap_or(Value::Null),
            priority: body.priority.unwrap_or(0),
            timezone: body.timezone.as_deref(),
            misfire_policy,
            max_attempts: body.max_attempts.map(|n| n.clamp(1, MAX_ATTEMPTS_CAP)),
        })
        .await?;
    Ok((StatusCode::CREATED, Json(schedule)))
}

#[utoipa::path(
    delete,
    path = "/schedules/{id}",
    tag = "schedules",
    params(("id" = String, Path, description = "Schedule id")),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`)"),
        (status = 404, description = "Schedule not found", body = Object),
    )
)]
async fn delete_schedule(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_schedule(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "schedule not found".into()))
    }
}

#[derive(Deserialize, ToSchema)]
struct EnabledBody {
    enabled: bool,
}

#[utoipa::path(
    post,
    path = "/schedules/{id}/enabled",
    tag = "schedules",
    params(("id" = String, Path, description = "Schedule id")),
    request_body = EnabledBody,
    responses(
        (status = 200, description = "`{id, enabled}`"),
        (status = 404, description = "Schedule not found", body = Object),
    )
)]
async fn set_schedule_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.set_schedule_enabled(&id, body.enabled).await? {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "schedule not found".into()))
    }
}

// ---- Datasets -------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/apps/{name}/datasets",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    responses((status = 200, description = "`{app, datasets: [name]}`"))
)]
async fn list_datasets(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let names = state.datasets.datasets(&name).await?;
    Ok(Json(json!({ "app": name, "datasets": names })))
}

#[derive(Deserialize, IntoParams)]
struct RecordsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        RecordsQuery,
    ),
    responses((status = 200, description = "Dual-mode: bare `[Record]` array, or `{items, next_cursor}` when `cursor` is present."))
)]
async fn list_records(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<RecordsQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 1000);
    let Some(cursor) = &query.cursor else {
        let records = state.datasets.list(&app, &dataset, limit).await?;
        return Ok(Json(json!(records)));
    };
    let after = parse_cursor(cursor);
    let records = state.datasets.list_page(&app, &dataset, after, limit).await?;
    let next_cursor = keyset_cursor(&records, limit, |r| {
        format!("{}|{}", pumper_core::datasets::ts(r.updated_at), r.key)
    });
    Ok(Json(json!({ "items": records, "next_cursor": next_cursor })))
}

#[derive(Deserialize, IntoParams)]
struct ExportQuery {
    /// 'json' (default) | 'ndjson' | 'csv'. All three stream in constant memory.
    format: Option<String>,
}

#[derive(Clone, Copy)]
enum ExportFormat {
    /// A single streamed JSON array — `[{record},{record},...]`.
    Json,
    /// One JSON object per line.
    Ndjson,
    /// RFC-4180 rows with a fixed header.
    Csv,
}

impl ExportFormat {
    fn extension(self) -> &'static str {
        match self {
            ExportFormat::Json => "json",
            ExportFormat::Ndjson => "ndjson",
            ExportFormat::Csv => "csv",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            ExportFormat::Json => "application/json",
            ExportFormat::Ndjson => "application/x-ndjson",
            ExportFormat::Csv => "text/csv; charset=utf-8",
        }
    }
}

#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}/export",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        ExportQuery,
    ),
    responses(
        (status = 200, description = "Streamed export as a JSON array, NDJSON, or CSV (per `format`); constant memory, no row cap. `content-disposition: attachment`."),
        (status = 400, description = "Unknown format", body = Object),
    )
)]
async fn export_records(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<ExportQuery>,
) -> Result<Response, ApiError> {
    let format = match query.format.as_deref().unwrap_or("json") {
        "json" => ExportFormat::Json,
        "ndjson" => ExportFormat::Ndjson,
        "csv" => ExportFormat::Csv,
        other => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!("unknown format '{other}' (json | ndjson | csv)"),
            ))
        }
    };
    Ok(stream_export(state, app, dataset, format))
}

/// Streams the whole dataset in keyset-paged batches — constant memory
/// regardless of dataset size, with no row cap or silent truncation. `json`
/// frames the batches as one array (`[`, comma-separated records, `]`); `ndjson`
/// and `csv` stream line-oriented output.
fn stream_export(state: AppState, app: String, dataset: String, format: ExportFormat) -> Response {
    const BATCH: i64 = 1_000;
    let filename = format!("attachment; filename=\"{dataset}.{}\"", format.extension());
    let content_type = format.content_type();
    let stream = async_stream::stream! {
        match format {
            ExportFormat::Csv => yield Ok::<_, Infallible>(axum::body::Bytes::from_static(
                b"key,first_seen,last_seen,updated_at,removed_at,data\n",
            )),
            ExportFormat::Json => yield Ok(axum::body::Bytes::from_static(b"[")),
            ExportFormat::Ndjson => {}
        }
        let mut after: Option<(String, String)> = None;
        let mut first = true;
        loop {
            let batch = match state.datasets.list_page(&app, &dataset, after.clone(), BATCH).await {
                Ok(batch) => batch,
                Err(e) => {
                    tracing::warn!(app = %app, dataset = %dataset, "export stream aborted: {e}");
                    break;
                }
            };
            let Some(last) = batch.last() else { break };
            after = Some((pumper_core::datasets::ts(last.updated_at), last.key.clone()));
            let short = (batch.len() as i64) < BATCH;
            let mut chunk = String::new();
            for record in &batch {
                match format {
                    ExportFormat::Csv => csv_row(&mut chunk, record),
                    ExportFormat::Ndjson => {
                        if let Ok(line) = serde_json::to_string(record) {
                            chunk.push_str(&line);
                            chunk.push('\n');
                        }
                    }
                    ExportFormat::Json => {
                        if let Ok(line) = serde_json::to_string(record) {
                            if !first {
                                chunk.push(',');
                            }
                            first = false;
                            chunk.push_str(&line);
                        }
                    }
                }
            }
            yield Ok(axum::body::Bytes::from(chunk));
            if short {
                break;
            }
        }
        if let ExportFormat::Json = format {
            yield Ok(axum::body::Bytes::from_static(b"]"));
        }
    };
    (
        [
            ("content-type", content_type.to_string()),
            ("content-disposition", filename),
        ],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

/// Appends one CSV row: fixed columns, RFC-4180 quoting for key and data.
fn csv_row(out: &mut String, record: &pumper_core::Record) {
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    out.push_str(&format!(
        "{},{},{},{},{},{}\n",
        quote(&record.key),
        record.first_seen.to_rfc3339(),
        record.last_seen.to_rfc3339(),
        record.updated_at.to_rfc3339(),
        record.removed_at.map(|d| d.to_rfc3339()).unwrap_or_default(),
        quote(&record.data.to_string()),
    ));
}

#[derive(Deserialize, IntoParams)]
struct DupQuery {
    #[serde(default = "default_distance")]
    distance: u32,
}

fn default_distance() -> u32 {
    3
}

/// Upper bound on dataset size for the duplicate scan. The comparison is an
/// O(n²) pairwise SimHash sweep held in memory, so a dataset past this size is
/// rejected (413) rather than pinning a core; page or narrow the dataset, or run
/// the scan offline. 10k rows ≈ 50M Hamming comparisons — sub-second, bounded.
const DUP_SCAN_MAX: i64 = 10_000;

/// Near-duplicate record pairs (SimHash Hamming distance ≤ `distance`).
#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}/duplicates",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        DupQuery,
    ),
    responses(
        (status = 200, description = "`{app, dataset, max_distance, pairs}`"),
        (status = 413, description = "Dataset over the 10k O(n^2) scan cap", body = Object),
    )
)]
async fn dataset_duplicates(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<DupQuery>,
) -> Result<Json<Value>, ApiError> {
    let count = state.datasets.record_count(&app, &dataset).await?;
    if count > DUP_SCAN_MAX {
        return Err(ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "dataset has {count} records; the duplicate scan is O(n²) and capped at \
                 {DUP_SCAN_MAX}. Narrow the dataset or run the scan offline."
            ),
        ));
    }
    let distance = query.distance.min(20);
    let pairs = state.datasets.duplicate_pairs(&app, &dataset, distance).await?;
    Ok(Json(json!({
        "app": app,
        "dataset": dataset,
        "max_distance": distance,
        "pairs": pairs,
    })))
}

// ---- Change intelligence ---------------------------------------------------

#[derive(Deserialize, IntoParams)]
struct ChangesQuery {
    /// RFC 3339 lower bound; only revisions after this instant are returned.
    since: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    /// Pages the full feed past the legacy 1000-row clamp; `since` still applies.
    cursor: Option<String>,
}

/// Change feed for a dataset: new/changed/removed revisions, newest first,
/// each carrying the field-level diff versus its previous revision.
#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}/changes",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        ChangesQuery,
    ),
    responses((status = 200, description = "Dual-mode: `{app, dataset, count, changes}` (clamped 1000), or `{items, next_cursor}` when `cursor` is present (pages the full feed)."))
)]
async fn dataset_changes(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<ChangesQuery>,
) -> Result<Json<Value>, ApiError> {
    let since = parse_since(query.since.as_deref())?;
    let Some(cursor) = &query.cursor else {
        let changes = state
            .datasets
            .changes_since(&app, Some(&dataset), since, query.limit.clamp(1, 1000))
            .await?;
        return Ok(Json(json!({
            "app": app,
            "dataset": dataset,
            "count": changes.len(),
            "changes": changes,
        })));
    };
    let after = parse_cursor_i64(cursor);
    let page = state
        .datasets
        .changes_page(&app, Some(&dataset), since, after, query.limit.clamp(1, 1000))
        .await?;
    Ok(Json(json!({ "items": page.items, "next_cursor": page.next_cursor })))
}

#[derive(Deserialize, IntoParams)]
struct HistoryQuery {
    /// Record key (query param, since keys may contain URL-hostile characters).
    key: String,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    /// Pages the full history past the legacy 500-row clamp.
    cursor: Option<String>,
}

/// A single record's revision history, newest first.
#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}/history",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        HistoryQuery,
    ),
    responses((status = 200, description = "Dual-mode: `{app, dataset, key, count, revisions}` (clamped 500), or `{items, next_cursor}` when `cursor` is present."))
)]
async fn record_history(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Value>, ApiError> {
    let Some(cursor) = &query.cursor else {
        let revisions = state
            .datasets
            .history(&app, &dataset, &query.key, query.limit.clamp(1, 500))
            .await?;
        return Ok(Json(json!({
            "app": app,
            "dataset": dataset,
            "key": query.key,
            "count": revisions.len(),
            "revisions": revisions,
        })));
    };
    let after = parse_cursor_i64(cursor);
    let page = state
        .datasets
        .history_page(&app, &dataset, &query.key, after, query.limit.clamp(1, 500))
        .await?;
    Ok(Json(json!({ "items": page.items, "next_cursor": page.next_cursor })))
}

// ---- Dataset watches --------------------------------------------------------

#[derive(Deserialize, IntoParams)]
struct WatchesQuery {
    app: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/watches",
    tag = "watches",
    params(WatchesQuery),
    responses((status = 200, description = "Dual-mode: `{watches: [Watch]}`, or `{items, next_cursor}` when `cursor` is present."))
)]
async fn list_watches(
    State(state): State<AppState>,
    Query(query): Query<WatchesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        // Legacy bare-array mode is still capped: an uncursored list must not
        // stream an entire table.
        let watches = state
            .storage
            .list_watches_page(query.app.as_deref(), None, limit)
            .await?;
        return Ok(Json(json!({ "watches": watches })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .storage
        .list_watches_page(query.app.as_deref(), after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |w| {
        format!("{}|{}", pumper_core::datasets::ts(w.created_at), w.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(Deserialize, ToSchema)]
struct CreateWatchBody {
    app: String,
    /// Dataset to watch; "*" (default) watches every dataset of the app.
    dataset: Option<String>,
    /// URL that receives `dataset.changed` POSTs.
    url: String,
    /// If set, delivery bodies are HMAC-SHA256 signed with this secret.
    secret: Option<String>,
}

#[utoipa::path(
    post,
    path = "/watches",
    tag = "watches",
    request_body = CreateWatchBody,
    responses(
        (status = 201, description = "Created watch", body = Object),
        (status = 400, description = "url must be http(s)", body = Object),
        (status = 404, description = "Unknown app", body = Object),
    )
)]
async fn create_watch(
    State(state): State<AppState>,
    Json(body): Json<CreateWatchBody>,
) -> Result<(StatusCode, Json<pumper_core::Watch>), ApiError> {
    if !state.registry.contains_key(&body.app) {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("unknown app '{}'", body.app)));
    }
    if !body.url.starts_with("http://") && !body.url.starts_with("https://") {
        return Err(ApiError(StatusCode::BAD_REQUEST, "url must be http(s)".into()));
    }
    let watch = state
        .storage
        .create_watch(
            &body.app,
            body.dataset.as_deref().unwrap_or("*"),
            &body.url,
            body.secret.as_deref(),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(watch)))
}

#[utoipa::path(
    delete,
    path = "/watches/{id}",
    tag = "watches",
    params(("id" = String, Path, description = "Watch id")),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`)"),
        (status = 404, description = "Watch not found", body = Object),
    )
)]
async fn delete_watch(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_watch(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "watch not found".into()))
    }
}

#[utoipa::path(
    post,
    path = "/watches/{id}/enabled",
    tag = "watches",
    params(("id" = String, Path, description = "Watch id")),
    request_body = EnabledBody,
    responses(
        (status = 200, description = "`{id, enabled}`"),
        (status = 404, description = "Watch not found", body = Object),
    )
)]
async fn set_watch_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.set_watch_enabled(&id, body.enabled).await? {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "watch not found".into()))
    }
}

// ---- Reactive triggers -------------------------------------------------------

#[derive(Deserialize, IntoParams)]
struct TriggersQuery {
    app: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/triggers",
    tag = "triggers",
    params(TriggersQuery),
    responses((status = 200, description = "Dual-mode: `{triggers: [Trigger]}`, or `{items, next_cursor}` when `cursor` is present."))
)]
async fn list_triggers(
    State(state): State<AppState>,
    Query(query): Query<TriggersQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        // Legacy bare-array mode is still capped: an uncursored list must not
        // stream an entire table.
        let triggers = state
            .storage
            .list_triggers_page(query.app.as_deref(), None, limit)
            .await?;
        return Ok(Json(json!({ "triggers": triggers })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .storage
        .list_triggers_page(query.app.as_deref(), after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |t| {
        format!("{}|{}", pumper_core::datasets::ts(t.created_at), t.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(Deserialize, ToSchema)]
struct CreateTriggerBody {
    name: Option<String>,
    /// 'dataset' (change-feed events) | 'job' (terminal events).
    source_kind: String,
    source_app: String,
    /// Dataset kind only: dataset name or '*' (default).
    source_dataset: Option<String>,
    /// Dataset kind only: new|changed|removed|fresh|any (default fresh).
    on_change: Option<String>,
    /// Job kind only: succeeded|failed|any (default succeeded).
    on_status: Option<String>,
    target_app: String,
    /// Static params template; `_trigger` is merged over it at fire time.
    params: Option<Value>,
    /// The TARGET's spend ceiling (never inherited from the source).
    budget_usd: Option<f64>,
    priority: Option<i64>,
    max_attempts: Option<i64>,
}

#[utoipa::path(
    post,
    path = "/triggers",
    tag = "triggers",
    request_body = CreateTriggerBody,
    responses(
        (status = 201, description = "Created trigger", body = Object),
        (status = 400, description = "Invalid source_kind/on_change/on_status", body = Object),
        (status = 404, description = "Unknown target app", body = Object),
    )
)]
async fn create_trigger(
    State(state): State<AppState>,
    Json(body): Json<CreateTriggerBody>,
) -> Result<(StatusCode, Json<pumper_core::Trigger>), ApiError> {
    let bad = |msg: String| ApiError(StatusCode::BAD_REQUEST, msg);
    if !state.registry.contains_key(&body.target_app) {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("unknown target app '{}'", body.target_app)));
    }
    // source_app may be a virtual namespace (e.g. cross-source 'grants'), so
    // only the target is required to be a registered app.
    let (source_dataset, on_change, on_status) = match body.source_kind.as_str() {
        "dataset" => {
            let on_change = body.on_change.as_deref().unwrap_or("fresh");
            if !matches!(on_change, "new" | "changed" | "removed" | "fresh" | "any") {
                return Err(bad(format!("invalid on_change '{on_change}'")));
            }
            if body.on_status.is_some() {
                return Err(bad("on_status is only valid for source_kind 'job'".into()));
            }
            (
                Some(body.source_dataset.as_deref().unwrap_or("*")),
                Some(on_change),
                None,
            )
        }
        "job" => {
            let on_status = body.on_status.as_deref().unwrap_or("succeeded");
            if !matches!(on_status, "succeeded" | "failed" | "any") {
                return Err(bad(format!("invalid on_status '{on_status}'")));
            }
            if body.source_dataset.is_some() || body.on_change.is_some() {
                return Err(bad("source_dataset/on_change are only valid for source_kind 'dataset'".into()));
            }
            (None, None, Some(on_status))
        }
        other => return Err(bad(format!("invalid source_kind '{other}' (dataset | job)"))),
    };
    let params = body.params.unwrap_or_else(|| json!({}));
    let trigger = state
        .storage
        .create_trigger(&pumper_core::NewTrigger {
            name: body.name.as_deref(),
            source_kind: &body.source_kind,
            source_app: &body.source_app,
            source_dataset,
            on_change,
            on_status,
            target_app: &body.target_app,
            params: &params,
            budget_usd: body.budget_usd.filter(|b| *b > 0.0),
            priority: body.priority.unwrap_or(0),
            max_attempts: body.max_attempts.unwrap_or(1).clamp(1, MAX_ATTEMPTS_CAP),
        })
        .await?;
    Ok((StatusCode::CREATED, Json(trigger)))
}

#[utoipa::path(
    delete,
    path = "/triggers/{id}",
    tag = "triggers",
    params(("id" = String, Path, description = "Trigger id")),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`)"),
        (status = 404, description = "Trigger not found", body = Object),
    )
)]
async fn delete_trigger(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_trigger(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "trigger not found".into()))
    }
}

#[utoipa::path(
    post,
    path = "/triggers/{id}/enabled",
    tag = "triggers",
    params(("id" = String, Path, description = "Trigger id")),
    request_body = EnabledBody,
    responses(
        (status = 200, description = "`{id, enabled}`"),
        (status = 404, description = "Trigger not found", body = Object),
    )
)]
async fn set_trigger_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.set_trigger_enabled(&id, body.enabled).await? {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "trigger not found".into()))
    }
}

#[derive(Deserialize, IntoParams)]
struct TestTriggerQuery {
    /// When true, actually enqueue the resolved hop (repeatable — the
    /// idempotency key is bypassed for testing). Default: dry-run only.
    #[serde(default)]
    fire: bool,
}

/// Dry-runs a trigger against its most recent matching source job: shows
/// whether it would fire, the resolved target params, and why not otherwise.
/// `?fire=true` enqueues the hop for real.
#[utoipa::path(
    post,
    path = "/triggers/{id}/test",
    tag = "triggers",
    params(("id" = String, Path, description = "Trigger id"), TestTriggerQuery),
    responses(
        (status = 200, description = "Dry-run decision `{would_fire, ...}` or, with `?fire=true`, `{fired, job}`"),
        (status = 404, description = "Trigger not found", body = Object),
    )
)]
async fn test_trigger(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<TestTriggerQuery>,
) -> Result<Json<Value>, ApiError> {
    let Some(trigger) = state.storage.get_trigger(&id).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, "trigger not found".into()));
    };
    let no_fire = |reason: &str| json!({ "would_fire": false, "reason": reason });

    // Most recent source job of the trigger's source app.
    let Some(source) = state
        .storage
        .list(Some(&trigger.source_app), None, 1)
        .await?
        .into_iter()
        .next()
    else {
        return Ok(Json(no_fire("no source job of that app exists yet")));
    };

    let decision = crate::triggers::decide(&trigger.id, &source.params, &state.config.triggers);
    let (depth, chain) = match decision {
        crate::triggers::FireDecision::Fire { depth, chain } => (depth, chain),
        crate::triggers::FireDecision::SkipCycle => {
            return Ok(Json(no_fire("cycle: trigger already in the source job's chain")))
        }
        crate::triggers::FireDecision::SkipDepth => {
            return Ok(Json(no_fire("max chain depth reached")))
        }
    };

    let obj = if trigger.source_kind == "dataset" {
        let changes = state
            .datasets
            .changes_since(&trigger.source_app, None, source.started_at, 1000)
            .await?;
        let matching: Vec<&pumper_core::Revision> = changes
            .iter()
            .filter(|r| trigger.covers_dataset(&r.dataset))
            .filter(|r| crate::triggers::change_matches(trigger.on_change.as_deref(), &r.change))
            .collect();
        if matching.is_empty() {
            return Ok(Json(no_fire("latest source run produced no matching changes")));
        }
        let dataset = matching[0].dataset.clone();
        crate::triggers::dataset_trigger_obj(
            &trigger, &source, &dataset, &matching, depth, &chain, &state.config.triggers,
        )
    } else {
        if !crate::triggers::status_matches(trigger.on_status.as_deref(), source.status.as_str()) {
            return Ok(Json(no_fire("latest source job's status does not match on_status")));
        }
        crate::triggers::terminal_trigger_obj(&trigger, &source, depth, &chain)
    };
    let resolved_params = crate::triggers::merged_params(&trigger.params, obj);

    if !query.fire {
        return Ok(Json(json!({
            "would_fire": true,
            "target_app": trigger.target_app,
            "source_job_id": source.id,
            "resolved_params": resolved_params,
        })));
    }
    // Real fire: no idempotency key so tests are repeatable.
    let opts = EnqueueOptions {
        params: resolved_params,
        max_attempts: trigger.max_attempts,
        priority: trigger.priority,
        budget_usd: trigger.budget_usd,
        trigger_id: Some(trigger.id.clone()),
        ..Default::default()
    };
    let job = state.storage.enqueue(&trigger.target_app, opts).await?;
    state.notify.notify_one();
    Ok(Json(json!({ "fired": true, "job": job })))
}

#[derive(Deserialize, IntoParams)]
struct RunsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
}

/// Jobs this trigger fired, newest first — the lineage view.
#[utoipa::path(
    get,
    path = "/triggers/{id}/runs",
    tag = "triggers",
    params(("id" = String, Path, description = "Trigger id"), RunsQuery),
    responses((status = 200, description = "`{trigger_id, count, runs: [Job]}`"))
)]
async fn trigger_runs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<RunsQuery>,
) -> Result<Json<Value>, ApiError> {
    let jobs = state
        .storage
        .jobs_by_trigger(&id, query.limit.clamp(1, 500))
        .await?;
    Ok(Json(json!({ "trigger_id": id, "count": jobs.len(), "runs": jobs })))
}

// ---- Webhook delivery log ----------------------------------------------------

#[derive(Deserialize, IntoParams)]
struct DeliveriesQuery {
    /// 'pending' | 'delivered' | 'failed' — `failed` is the dead-letter view.
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/webhooks/deliveries",
    tag = "webhooks",
    params(DeliveriesQuery),
    responses((status = 200, description = "Dual-mode: `{count, deliveries}`, or `{items, next_cursor}` when `cursor` is present. `?status=failed` is the dead-letter view."))
)]
async fn list_deliveries(
    State(state): State<AppState>,
    Query(query): Query<DeliveriesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        let deliveries = state
            .storage
            .list_deliveries(query.status.as_deref(), limit)
            .await?;
        return Ok(Json(json!({ "count": deliveries.len(), "deliveries": deliveries })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .storage
        .list_deliveries_page(query.status.as_deref(), after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |d| {
        format!("{}|{}", pumper_core::datasets::ts(d.created_at), d.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[utoipa::path(
    get,
    path = "/webhooks/deliveries/{id}",
    tag = "webhooks",
    params(("id" = String, Path, description = "Delivery id")),
    responses(
        (status = 200, description = "The delivery, including body", body = Object),
        (status = 404, description = "Delivery not found", body = Object),
    )
)]
async fn get_delivery(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<pumper_core::Delivery>, ApiError> {
    state
        .storage
        .get_delivery(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "delivery not found".into()))
}

/// Re-sends a logged delivery, re-signing with the source's current secret
/// (job callback secret or watch secret) when it still exists.
#[utoipa::path(
    post,
    path = "/webhooks/deliveries/{id}/replay",
    tag = "webhooks",
    params(("id" = String, Path, description = "Delivery id")),
    responses(
        (status = 202, description = "Replay scheduled (`{id, replaying: true}`)"),
        (status = 404, description = "Delivery not found", body = Object),
    )
)]
async fn replay_delivery(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let Some(delivery) = state.storage.get_delivery(&id).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, "delivery not found".into()));
    };
    let secret = match delivery.kind.as_str() {
        "job" => {
            let job_id = Uuid::parse_str(&delivery.ref_id)
                .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            state.storage.get(job_id).await?.and_then(|j| j.callback_secret)
        }
        _ => state
            .storage
            .get_watch(&delivery.ref_id)
            .await?
            .and_then(|w| w.secret),
    };
    crate::webhook::replay(
        state.webhook_client.clone(),
        state.storage.clone(),
        delivery.id.clone(),
        delivery.url.clone(),
        delivery.event.clone(),
        delivery.body.into_bytes(),
        secret,
    );
    Ok((StatusCode::ACCEPTED, Json(json!({ "id": id, "replaying": true }))))
}

// ---- Full-text search -----------------------------------------------------

#[derive(Deserialize, IntoParams)]
struct SearchQuery {
    q: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
    /// Restrict hits to one app.
    app: Option<String>,
    /// Restrict hits to one dataset.
    dataset: Option<String>,
    /// Typo tolerance (edit distance 1). Quoted phrases stay exact.
    #[serde(default)]
    fuzzy: bool,
}

fn default_search_limit() -> usize {
    20
}

/// Full-text search across everything indexed from job results (BM25 ranked),
/// with highlighted snippets and app/dataset facets over the matching set.
#[utoipa::path(
    get,
    path = "/search",
    tag = "search",
    params(SearchQuery),
    responses(
        (status = 200, description = "`{query, count, hits, facets}` (BM25 ranked, highlighted snippets)"),
        (status = 400, description = "Empty query", body = Object),
    )
)]
async fn search(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<Value>, ApiError> {
    if query.q.trim().is_empty() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "query 'q' is required".into()));
    }
    let req = pumper_core::SearchRequest {
        q: query.q.clone(),
        limit: query.limit.clamp(1, 100),
        app: query.app,
        dataset: query.dataset,
        fuzzy: query.fuzzy,
    };
    let results = state.search.query(req).await?;
    Ok(Json(json!({
        "query": query.q,
        "count": results.hits.len(),
        "hits": results.hits,
        "facets": results.facets,
    })))
}

// ---- Saved searches (standing alerts) ---------------------------------------

#[derive(Deserialize, IntoParams)]
struct SavedSearchesQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/searches",
    tag = "search",
    params(SavedSearchesQuery),
    responses((status = 200, description = "Dual-mode: `{searches: [SavedSearch]}`, or `{items, next_cursor}` when `cursor` is present."))
)]
async fn list_saved_searches(
    State(state): State<AppState>,
    Query(query): Query<SavedSearchesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        // Legacy bare-array mode is still capped: an uncursored list must not
        // stream an entire table.
        let searches = state
            .storage
            .list_saved_searches_page(false, None, limit)
            .await?;
        return Ok(Json(json!({ "searches": searches })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .storage
        .list_saved_searches_page(false, after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |s| {
        format!("{}|{}", pumper_core::datasets::ts(s.created_at), s.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(Deserialize, ToSchema)]
struct CreateSavedSearchBody {
    /// Full-text query (same syntax as GET /search).
    query: String,
    /// Optional scope: only this app / dataset.
    app: Option<String>,
    dataset: Option<String>,
    /// Webhook that receives `search.matched` events for NEW matches.
    url: String,
    /// If set, delivery bodies are HMAC-SHA256 signed with this secret.
    secret: Option<String>,
}

#[utoipa::path(
    post,
    path = "/searches",
    tag = "search",
    request_body = CreateSavedSearchBody,
    responses(
        (status = 201, description = "Created saved search", body = Object),
        (status = 400, description = "Empty query or url not http(s)", body = Object),
    )
)]
async fn create_saved_search(
    State(state): State<AppState>,
    Json(body): Json<CreateSavedSearchBody>,
) -> Result<(StatusCode, Json<pumper_core::SavedSearch>), ApiError> {
    if body.query.trim().is_empty() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "'query' is required".into()));
    }
    if !body.url.starts_with("http://") && !body.url.starts_with("https://") {
        return Err(ApiError(StatusCode::BAD_REQUEST, "url must be http(s)".into()));
    }
    let search = state
        .storage
        .create_saved_search(
            body.query.trim(),
            body.app.as_deref(),
            body.dataset.as_deref(),
            &body.url,
            body.secret.as_deref(),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(search)))
}

#[utoipa::path(
    delete,
    path = "/searches/{id}",
    tag = "search",
    params(("id" = String, Path, description = "Saved search id")),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`)"),
        (status = 404, description = "Saved search not found", body = Object),
    )
)]
async fn delete_saved_search(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_saved_search(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "saved search not found".into()))
    }
}

#[utoipa::path(
    post,
    path = "/searches/{id}/enabled",
    tag = "search",
    params(("id" = String, Path, description = "Saved search id")),
    request_body = EnabledBody,
    responses(
        (status = 200, description = "`{id, enabled}`"),
        (status = 404, description = "Saved search not found", body = Object),
    )
)]
async fn set_saved_search_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.set_saved_search_enabled(&id, body.enabled).await? {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "saved search not found".into()))
    }
}

#[derive(Deserialize, ToSchema)]
struct DeleteDocsBody {
    ids: Vec<String>,
}

/// Removes specific documents from the search index by id.
#[utoipa::path(
    delete,
    path = "/search/docs",
    tag = "search",
    request_body = DeleteDocsBody,
    responses(
        (status = 200, description = "`{deleted: <count>}`"),
        (status = 400, description = "`ids` must be non-empty", body = Object),
    )
)]
async fn delete_search_docs(
    State(state): State<AppState>,
    Json(body): Json<DeleteDocsBody>,
) -> Result<Json<Value>, ApiError> {
    if body.ids.is_empty() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "'ids' must be non-empty".into()));
    }
    let count = body.ids.len();
    state.search.delete_ids(&body.ids).await?;
    Ok(Json(json!({ "deleted": count })))
}

/// Removes every indexed document of one app's dataset.
#[utoipa::path(
    delete,
    path = "/search/datasets/{app}/{dataset}",
    tag = "search",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses((status = 200, description = "`{app, dataset, deleted: true}`"))
)]
async fn delete_search_dataset(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    state.search.delete_dataset(&app, &dataset).await?;
    Ok(Json(json!({ "app": app, "dataset": dataset, "deleted": true })))
}

// ---- Host profiles (learned tier memory + politeness) -----------------------

#[derive(Deserialize, IntoParams)]
struct HostsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

/// Serializes a stored profile with the **live** governor penalty merged in
/// (the row's `penalty_ms` is only the last write-behind snapshot; the
/// in-memory value is authoritative and fresher).
async fn host_json(state: &AppState, mut profile: HostProfile) -> Value {
    let live = state.governor.penalty(&profile.host).await;
    profile.penalty_ms = live.as_millis().min(i64::MAX as u128) as i64;
    json!(profile)
}

/// Paginated list of learned host state: preferred tier, HTTP strikes, live
/// politeness penalty, and last-outcome timestamps. Most-recently-active first.
#[utoipa::path(
    get,
    path = "/hosts",
    tag = "hosts",
    params(HostsQuery),
    responses((status = 200, description = "Dual-mode: `{hosts: [...]}` without `cursor=`, \
        `{items, next_cursor}` with it. Each host: `{host, preferred_tier, http_strikes, \
        penalty_ms (live), updated_at, penalty_updated_at}`"))
)]
async fn list_hosts(
    State(state): State<AppState>,
    Query(query): Query<HostsQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let after = query.cursor.as_deref().and_then(parse_cursor);
    let profiles = state.tiers.list_page(after, limit).await?;
    let next_cursor = keyset_cursor(&profiles, limit, |p| {
        format!("{}|{}", p.updated_at, p.host)
    });
    let mut items = Vec::with_capacity(profiles.len());
    for p in profiles {
        items.push(host_json(&state, p).await);
    }
    // Dual-mode, matching every other list endpoint: no cursor ⇒ legacy
    // `{hosts: [...]}` shape; cursor present ⇒ `{items, next_cursor}`.
    if query.cursor.is_none() {
        Ok(Json(json!({ "hosts": items })))
    } else {
        Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
    }
}

/// One host's learned profile. 404 when the host has no learned state (no tier
/// memory row and no live penalty).
#[utoipa::path(
    get,
    path = "/hosts/{host}",
    tag = "hosts",
    params(("host" = String, Path, description = "Hostname (case-insensitive)")),
    responses(
        (status = 200, description = "`{host, preferred_tier, http_strikes, penalty_ms (live), \
            updated_at, penalty_updated_at}`"),
        (status = 404, description = "No learned state for this host", body = Object),
    )
)]
async fn get_host(
    State(state): State<AppState>,
    Path(host): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let host = host.to_lowercase();
    if let Some(profile) = state.tiers.get(&host).await? {
        return Ok(Json(host_json(&state, profile).await));
    }
    // No stored row, but a live penalty may exist ahead of the next snapshot.
    let live = state.governor.penalty(&host).await;
    if !live.is_zero() {
        return Ok(Json(json!({
            "host": host,
            "preferred_tier": Value::Null,
            "http_strikes": 0,
            "penalty_ms": live.as_millis().min(i64::MAX as u128) as i64,
            "updated_at": Value::Null,
            "penalty_updated_at": Value::Null,
        })));
    }
    Err(ApiError(StatusCode::NOT_FOUND, "unknown host".into()))
}

/// Resets a host's learned state: drops its tier memory (strikes + browser pin +
/// persisted penalty) and clears the live governor penalty. 404 when unknown.
#[utoipa::path(
    delete,
    path = "/hosts/{host}/memory",
    tag = "hosts",
    params(("host" = String, Path, description = "Hostname (case-insensitive)")),
    responses(
        (status = 200, description = "`{host, reset: true}`"),
        (status = 404, description = "No learned state for this host", body = Object),
    )
)]
async fn delete_host_memory(
    State(state): State<AppState>,
    Path(host): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let host = host.to_lowercase();
    let forgot = state.tiers.forget(&host).await?;
    let cleared = state.governor.clear(&host);
    if forgot || cleared {
        Ok(Json(json!({ "host": host, "reset": true })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "unknown host".into()))
    }
}

// ---- Session profiles -----------------------------------------------------

/// One profile of the session vault (`[fetcher] profiles_dir`), as it exists on
/// disk. Profiles are created implicitly by the first fetch that names them —
/// there is no create/delete API in phase 1.
#[derive(serde::Serialize, ToSchema)]
struct ProfileInfo {
    /// Directory name — exactly the string a request's `profile` field takes.
    name: String,
    /// A persistent HTTP cookie jar exists (`cookies.json`).
    has_cookies: bool,
    /// A Chrome user-data-dir exists (`browser/`).
    has_browser_dir: bool,
    /// Most recent mtime across the profile dir, its jar, and its browser dir
    /// (RFC 3339). `None` when no mtime is readable.
    last_used: Option<String>,
}

/// Lists the profiles in the session vault — see [fetching.md]. Read-only
/// diagnostics: it reports what is on disk, it does not create anything.
#[utoipa::path(
    get,
    path = "/profiles",
    tag = "profiles",
    responses((
        status = 200,
        description = "`{profiles: [{name, has_cookies, has_browser_dir, last_used}]}`, \
                       alphabetical. Empty (not an error) when the vault dir does not exist yet.",
        body = Object,
    ))
)]
async fn list_profiles(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let root = state.config.fetcher.profiles_dir.clone();
    let mut entries = match tokio::fs::read_dir(&root).await {
        Ok(entries) => entries,
        // No vault dir yet simply means no profiles — it is created on the first
        // profiled fetch, so this is an empty list, not a failure.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Json(json!({ "profiles": [] })));
        }
        Err(e) => {
            return Err(ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("reading {}: {e}", root.display()),
            ))
        }
    };

    let mut profiles: Vec<ProfileInfo> = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(|e| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("reading {}: {e}", root.display()))
    })? {
        let Ok(name) = entry.file_name().into_string() else { continue };
        // Only directories whose names are valid profiles — anything else in the
        // vault dir isn't ours and can't be named by a request anyway.
        if pumper_core::validate_profile_name(&name).is_err() {
            continue;
        }
        if !entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let dir = entry.path();
        let cookies = dir.join(pumper_core::PROFILE_COOKIES_FILE);
        let browser = dir.join(pumper_core::PROFILE_BROWSER_DIR);
        let has_cookies = tokio::fs::metadata(&cookies).await.map(|m| m.is_file()).unwrap_or(false);
        let has_browser_dir =
            tokio::fs::metadata(&browser).await.map(|m| m.is_dir()).unwrap_or(false);
        // Last use ≈ the newest mtime among the profile dir and its artifacts:
        // the jar is rewritten after cookie-setting responses, and Chrome churns
        // its user-data-dir on every render.
        let mut newest: Option<std::time::SystemTime> = None;
        for path in [&dir, &cookies, &browser] {
            if let Ok(mtime) = tokio::fs::metadata(path).await.and_then(|m| m.modified()) {
                newest = Some(newest.map_or(mtime, |cur: std::time::SystemTime| cur.max(mtime)));
            }
        }
        let last_used = newest.map(|t| chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339());
        profiles.push(ProfileInfo { name, has_cookies, has_browser_dir, last_used });
    }
    profiles.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(json!({ "profiles": profiles })))
}

// ---- WASM plugins ---------------------------------------------------------

#[utoipa::path(
    get,
    path = "/plugins",
    tag = "plugins",
    responses((status = 200, description = "`{plugins: [...]}`"))
)]
async fn list_plugins(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "plugins": state.plugins.list() }))
}

/// Hot-swap: rescan the plugin directory and reload every `.wasm` module.
#[utoipa::path(
    post,
    path = "/plugins/reload",
    tag = "plugins",
    responses((status = 200, description = "`{loaded: <count>}`"))
)]
async fn reload_plugins(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let loaded = state.plugins.reload().await?;
    Ok(Json(json!({ "loaded": loaded })))
}

// ---- Declarative extraction preview -----------------------------------------

/// Time budget for a preview `url` fetch. A preview must stay interactive, so a
/// slow origin is abandoned rather than blocking the request.
const PREVIEW_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Body budget for a preview `url` fetch. Past this the document is rejected
/// (413) instead of parsed — previews validate rules, they are not a bulk pull.
const PREVIEW_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

#[derive(Deserialize, ToSchema)]
struct PreviewBody {
    /// A `RuleSet`: a bare `{field: rule}` map (the same shape apps take), e.g.
    /// `{"title": {"type": "css", "selector": "h1"}}`.
    #[schema(value_type = Object)]
    rules: Value,
    /// Inline document to run the rules against. Provide exactly one of
    /// `html` or `url`.
    html: Option<String>,
    /// URL to fetch (HTTP tier only — no browser/Claude escalation) and run the
    /// rules against. Provide exactly one of `html` or `url`.
    url: Option<String>,
}

/// Compiles a `RuleSet` and runs it against one document without enqueuing a
/// job — the fast feedback loop for authoring selectors. Rules are compiled
/// field-by-field so every bad field is reported at once (not just the first);
/// the response pairs the extracted values with the per-field match report
/// (matched | empty | error), so a selector that silently matches nothing is
/// visible before a job fetches anything.
///
/// `url` mode fetches through the shared HTTP tier only (`FetchStrategy::Http`):
/// a preview never spends money on the Claude tier or waits on a browser render,
/// and is bounded by a modest time and body budget.
#[utoipa::path(
    post,
    path = "/extract/preview",
    tag = "extract",
    request_body = PreviewBody,
    responses(
        (status = 200, description = "`{values, report, fields_matched, fields_total}` — extracted values plus the per-field match report (each field `matched`|`empty`|`error`)."),
        (status = 400, description = "Bad request: not exactly one of html|url, non-object `rules`, non-http(s) url, fetch failure/timeout, or rule compile errors — the body then carries a `fields: [{field, error}]` list covering every bad field.", body = Object),
        (status = 413, description = "Fetched body over the preview size budget", body = Object),
    )
)]
async fn extract_preview(
    State(state): State<AppState>,
    Json(body): Json<PreviewBody>,
) -> Result<Response, ApiError> {
    // Exactly one document source.
    let doc = match (body.html, body.url) {
        (Some(_), Some(_)) => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "provide exactly one of 'html' or 'url', not both".into(),
            ))
        }
        (None, None) => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "provide exactly one of 'html' or 'url'".into(),
            ))
        }
        (Some(html), None) => html,
        (None, Some(url)) => fetch_preview_doc(&state, &url).await?,
    };

    // Compile field-by-field so ALL bad fields are reported, not just the first.
    // `rules` must be an object mapping field -> rule; each value is deserialized
    // into a `FieldRule` and then compiled on its own (as a single-field
    // `RuleSet`), collecting both deserialize and compile-time errors per field.
    let Value::Object(map) = body.rules else {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "'rules' must be a JSON object mapping field -> rule".into(),
        ));
    };
    let mut fields: std::collections::BTreeMap<String, pumper_core::FieldRule> =
        std::collections::BTreeMap::new();
    let mut errors: Vec<Value> = Vec::new();
    for (name, rule_val) in map {
        match serde_json::from_value::<pumper_core::FieldRule>(rule_val) {
            Ok(field_rule) => {
                let one = std::collections::BTreeMap::from([(name.clone(), field_rule.clone())]);
                match (pumper_core::RuleSet { fields: one }).compile() {
                    Ok(_) => {
                        fields.insert(name, field_rule);
                    }
                    Err(e) => errors.push(json!({ "field": name, "error": e.to_string() })),
                }
            }
            Err(e) => errors.push(json!({ "field": name, "error": e.to_string() })),
        }
    }
    if !errors.is_empty() {
        // Structured compile diagnostics: the same `{error, code}` envelope as
        // ApiError, plus a per-field list so every bad selector is fixed at once.
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "rule compilation failed",
                "code": error_code(StatusCode::BAD_REQUEST),
                "fields": errors,
            })),
        )
            .into_response());
    }

    // Every field compiled on its own, so the combined compile cannot fail.
    let compiled = (pumper_core::RuleSet { fields })
        .compile()
        .map_err(ApiError::from)?;
    let (values, report) = pumper_core::extract_one_with_report(&compiled, &doc);
    let fields_total = report.fields.len();
    let fields_matched = report
        .fields
        .values()
        .filter(|s| matches!(s, pumper_core::FieldStatus::Matched))
        .count();
    Ok(Json(json!({
        "values": values,
        "report": report,
        "fields_matched": fields_matched,
        "fields_total": fields_total,
    }))
    .into_response())
}

/// Fetches a preview document through the shared HTTP tier only, under a modest
/// time and size budget. No browser/Claude escalation — a preview must stay
/// cheap and never spend money.
async fn fetch_preview_doc(state: &AppState, url: &str) -> Result<String, ApiError> {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(ApiError(StatusCode::BAD_REQUEST, "'url' must be http(s)".into()));
    }
    let mut req = pumper_core::FetchRequest::new(url);
    req.strategy = pumper_core::FetchStrategy::Http;
    let outcome = tokio::time::timeout(PREVIEW_FETCH_TIMEOUT, state.engines.fetch.fetch(req))
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::BAD_REQUEST,
                format!("fetch exceeded the {}s preview budget", PREVIEW_FETCH_TIMEOUT.as_secs()),
            )
        })?
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("failed to fetch url: {e}")))?;
    let html = outcome.html.unwrap_or_default();
    if html.len() > PREVIEW_MAX_BODY_BYTES {
        return Err(ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "fetched body is {} bytes; the preview budget is {PREVIEW_MAX_BODY_BYTES} bytes",
                html.len()
            ),
        ));
    }
    Ok(html)
}

// ---------------------------------------------------------------------------
// Grants query surface
//
// `grants/unified` is the cross-source corpus that grants-gov, ca-grants, and
// eu-sedia all normalize into (see the `grants-common` crate, which owns these
// two names).
// Until now it was reachable only through the generic dataset API, so every
// consumer had to export the whole corpus and filter client-side. These two
// routes push the filters into SQL.
// ---------------------------------------------------------------------------

/// Virtual app namespace holding the cross-source grants datasets. Mirrors
/// `grants_common::{UNIFIED_APP, UNIFIED_DATASET}`; duplicated as literals rather
/// than taking a server dependency on a library crate for two strings.
const GRANTS_APP: &str = "grants";
const GRANTS_DATASET: &str = "unified";

/// Upper bound on `GET /grants?limit=`. The default is `default_limit` (50).
const GRANTS_MAX_LIMIT: i64 = 500;

/// Default closing-soon window, in days, matching the grants-gov digest.
const CLOSING_SOON_DEFAULT_DAYS: i64 = 14;
/// Rows the closing-soon view pulls before sorting. Sorting is by `close_date`,
/// not by the `updated_at` order the store returns, so the whole window has to be
/// in hand before it can be truncated — this bounds that read.
const CLOSING_SOON_SCAN: i64 = 1000;
/// Rows the closing-soon view returns. `count` reports the full window size.
const CLOSING_SOON_CAP: usize = 200;

/// Filters over `grants/unified`. All optional, all ANDed; with none set the
/// route lists the whole live corpus.
#[derive(Deserialize, IntoParams)]
struct GrantsQuery {
    /// Normalized status, exact match: `open` | `forecasted` | `closed`.
    status: Option<String>,
    /// Case-insensitive substring of the agency name (e.g. `health`).
    agency: Option<String>,
    /// Source app, exact match: `grants-gov` | `ca-grants` | `eu-sedia`.
    source: Option<String>,
    /// Closes on or before this `YYYY-MM-DD`. Records with no close date are excluded.
    closing_before: Option<String>,
    /// Closes on or after this `YYYY-MM-DD`. Records with no close date are excluded.
    closing_after: Option<String>,
    /// Minimum money: keeps records whose `award_ceiling` OR `total_funding` is >= this.
    min_award: Option<f64>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

/// A blank query param (`?status=`) means "unset", not "match the empty string" —
/// otherwise a UI that always serializes its filter form would match nothing.
fn filter_value(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

/// Grant dates are canonical `YYYY-MM-DD`, which sorts lexicographically — that is
/// what lets the closing-window filters compare as text. Reject anything else
/// rather than silently comparing a malformed string.
fn parse_grant_date(value: &str, field: &str) -> Result<chrono::NaiveDate, ApiError> {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("'{field}' must be a YYYY-MM-DD date, got '{value}'"),
        )
    })
}

/// Translates the query params into store-level JSON predicates.
fn grant_filters(query: &GrantsQuery) -> Result<Vec<pumper_core::datasets::JsonFilter>, ApiError> {
    use pumper_core::datasets::JsonFilter;
    let mut filters = Vec::new();
    if let Some(status) = filter_value(&query.status) {
        filters.push(JsonFilter::Eq { path: "$.status".into(), value: status.into() });
    }
    if let Some(source) = filter_value(&query.source) {
        filters.push(JsonFilter::Eq { path: "$.source".into(), value: source.into() });
    }
    if let Some(agency) = filter_value(&query.agency) {
        filters.push(JsonFilter::Contains { path: "$.agency".into(), value: agency.into() });
    }
    if let Some(before) = filter_value(&query.closing_before) {
        parse_grant_date(before, "closing_before")?;
        filters.push(JsonFilter::Lte { path: "$.close_date".into(), value: before.into() });
    }
    if let Some(after) = filter_value(&query.closing_after) {
        parse_grant_date(after, "closing_after")?;
        filters.push(JsonFilter::Gte { path: "$.close_date".into(), value: after.into() });
    }
    // A grant's "size" is reported inconsistently across sources: some publish a
    // per-award ceiling, some only a program total. Matching either keeps a
    // funder's largest number in play instead of demanding one specific field.
    if let Some(min) = query.min_award {
        filters.push(JsonFilter::NumGteAny {
            paths: vec!["$.award_ceiling".into(), "$.total_funding".into()],
            value: min,
        });
    }
    Ok(filters)
}

#[utoipa::path(
    get,
    path = "/grants",
    tag = "grants",
    params(GrantsQuery),
    responses(
        (status = 200, description = "Live records from `grants/unified` matching every filter, newest-updated first. Dual-mode: `{grants: [Record]}`, or `{items, next_cursor}` when `cursor` is present (even empty)."),
        (status = 400, description = "Malformed `closing_before` / `closing_after` date", body = Object),
    )
)]
async fn list_grants(
    State(state): State<AppState>,
    Query(query): Query<GrantsQuery>,
) -> Result<Json<Value>, ApiError> {
    let filters = grant_filters(&query)?;
    let limit = query.limit.clamp(1, GRANTS_MAX_LIMIT);
    let Some(cursor) = &query.cursor else {
        let grants = state
            .datasets
            .list_filtered(GRANTS_APP, GRANTS_DATASET, &filters, None, limit)
            .await?;
        return Ok(Json(json!({ "grants": grants })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .datasets
        .list_filtered(GRANTS_APP, GRANTS_DATASET, &filters, after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |r| {
        format!("{}|{}", pumper_core::datasets::ts(r.updated_at), r.key)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(Deserialize, IntoParams)]
struct ClosingSoonQuery {
    /// Window size in days from today. Default 14, clamped to 1..=365.
    days: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/grants/closing-soon",
    tag = "grants",
    params(ClosingSoonQuery),
    responses((status = 200, description = "`{days, count, grants}` — live open grants closing within the window, soonest first. Each grant is its unified record `data` plus `key` and `days_left`. `count` is the window total; `grants` is capped at 200."))
)]
async fn closing_soon(
    State(state): State<AppState>,
    Query(query): Query<ClosingSoonQuery>,
) -> Result<Json<Value>, ApiError> {
    use pumper_core::datasets::JsonFilter;
    let days = query.days.unwrap_or(CLOSING_SOON_DEFAULT_DAYS).clamp(1, 365);
    let today = chrono::Utc::now().date_naive();
    let until = today + chrono::Duration::days(days);

    // Computed on read rather than materialized as a dataset: the corpus is small
    // enough to scan, and a read view can never go stale between syncs — which a
    // "closing soon" list, whose membership changes with the calendar and not with
    // the data, absolutely would if it were snapshotted.
    let filters = vec![
        JsonFilter::Eq { path: "$.status".into(), value: "open".into() },
        JsonFilter::Gte { path: "$.close_date".into(), value: today.to_string() },
        JsonFilter::Lte { path: "$.close_date".into(), value: until.to_string() },
    ];
    let records = state
        .datasets
        .list_filtered(GRANTS_APP, GRANTS_DATASET, &filters, None, CLOSING_SOON_SCAN)
        .await?;

    let mut window: Vec<(i64, Value)> = records
        .into_iter()
        .filter_map(|r| {
            let close = r.data.get("close_date").and_then(Value::as_str)?;
            let close = chrono::NaiveDate::parse_from_str(close, "%Y-%m-%d").ok()?;
            let days_left = (close - today).num_days();
            let mut grant = r.data.as_object()?.clone();
            grant.insert("key".into(), json!(r.key));
            grant.insert("days_left".into(), json!(days_left));
            Some((days_left, Value::Object(grant)))
        })
        .collect();
    window.sort_by_key(|(days_left, _)| *days_left);

    let count = window.len();
    let grants: Vec<Value> =
        window.into_iter().take(CLOSING_SOON_CAP).map(|(_, grant)| grant).collect();
    Ok(Json(json!({ "days": days, "count": count, "grants": grants })))
}

#[cfg(test)]
mod api_spec_tests {
    use std::collections::BTreeSet;

    /// Every `(METHOD, path)` operation the router serves. Because `router` and
    /// the OpenAPI document are both generated from `openapi_router`, this set is
    /// literally the routing table — so this list doubles as the canonical route
    /// inventory. Adding or removing a route changes the spec and fails this test
    /// until the list is updated, which is the point: drift can't land silently.
    const EXPECTED: &[&str] = &[
        "GET /health",
        "GET /metrics",
        "GET /events",
        "GET /apps",
        "POST /apps/{name}/jobs",
        "GET /apps/{name}/datasets",
        "GET /jobs",
        "GET /jobs/{id}",
        "DELETE /jobs/{id}",
        "POST /jobs/{id}/retry",
        "POST /jobs/retry",
        "POST /jobs/{id}/reset",
        "GET /jobs/{id}/stream",
        "GET /jobs/{id}/costs",
        "GET /costs",
        "GET /schedules",
        "POST /schedules",
        "DELETE /schedules/{id}",
        "POST /schedules/{id}/enabled",
        "GET /datasets/{app}/{dataset}",
        "GET /datasets/{app}/{dataset}/export",
        "GET /datasets/{app}/{dataset}/duplicates",
        "GET /datasets/{app}/{dataset}/changes",
        "GET /datasets/{app}/{dataset}/history",
        "GET /watches",
        "POST /watches",
        "DELETE /watches/{id}",
        "POST /watches/{id}/enabled",
        "GET /triggers",
        "POST /triggers",
        "DELETE /triggers/{id}",
        "POST /triggers/{id}/enabled",
        "POST /triggers/{id}/test",
        "GET /triggers/{id}/runs",
        "GET /webhooks/deliveries",
        "GET /webhooks/deliveries/{id}",
        "POST /webhooks/deliveries/{id}/replay",
        "GET /hosts",
        "GET /hosts/{host}",
        "DELETE /hosts/{host}/memory",
        "GET /profiles",
        "GET /plugins",
        "POST /plugins/reload",
        "GET /search",
        "DELETE /search/docs",
        "GET /searches",
        "POST /searches",
        "DELETE /searches/{id}",
        "POST /searches/{id}/enabled",
        "DELETE /search/datasets/{app}/{dataset}",
        "POST /extract/preview",
        "GET /grants",
        "GET /grants/closing-soon",
        "GET /openapi.json",
    ];

    /// The `(METHOD, path)` operations actually present in the generated spec.
    fn spec_operations() -> BTreeSet<String> {
        let api = super::openapi_router().split_for_parts().1;
        let json = serde_json::to_value(&api).expect("spec serializes");
        let methods = ["get", "post", "put", "delete", "patch", "head", "options", "trace"];
        let mut ops = BTreeSet::new();
        for (path, item) in json["paths"].as_object().expect("paths object") {
            for method in item.as_object().expect("path item object").keys() {
                if methods.contains(&method.as_str()) {
                    ops.insert(format!("{} {}", method.to_uppercase(), path));
                }
            }
        }
        ops
    }

    #[test]
    fn spec_covers_exactly_the_registered_routes() {
        let spec = spec_operations();
        let expected: BTreeSet<String> = EXPECTED.iter().map(|s| s.to_string()).collect();
        let missing: Vec<_> = expected.difference(&spec).collect();
        let undocumented: Vec<_> = spec.difference(&expected).collect();
        assert!(
            missing.is_empty(),
            "routes missing from the OpenAPI spec: {missing:?}"
        );
        assert!(
            undocumented.is_empty(),
            "spec has operations not in the expected inventory (update EXPECTED): {undocumented:?}"
        );
    }

    #[test]
    fn spec_is_valid_openapi_document() {
        let api = super::openapi_router().split_for_parts().1;
        let json = serde_json::to_value(&api).unwrap();
        assert!(json["openapi"].as_str().unwrap().starts_with("3."));
        assert_eq!(json["info"]["title"], "pumper HTTP API");
        // Typed request bodies land in components.schemas (e.g. the enqueue body).
        assert!(json["components"]["schemas"]["EnqueueBody"].is_object());
    }
}
