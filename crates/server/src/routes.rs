use std::convert::Infallible;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use pumper_core::{EnqueueOptions, HostProfile, Job, JobStatus, Schedule};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

use crate::events::JobEvent;
use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/events", get(stream_events))
        .route("/apps", get(list_apps))
        .route("/apps/{name}/jobs", post(enqueue_job))
        .route("/apps/{name}/datasets", get(list_datasets))
        .route("/jobs", get(list_jobs))
        .route("/jobs/{id}", get(get_job).delete(cancel_job))
        .route("/jobs/{id}/retry", post(retry_job))
        .route("/jobs/{id}/stream", get(stream_job))
        .route("/jobs/{id}/costs", get(job_costs))
        .route("/costs", get(cost_summary))
        .route("/schedules", get(list_schedules).post(create_schedule))
        .route("/schedules/{id}", axum::routing::delete(delete_schedule))
        .route("/schedules/{id}/enabled", post(set_schedule_enabled))
        .route("/datasets/{app}/{dataset}", get(list_records))
        .route("/datasets/{app}/{dataset}/export", get(export_records))
        .route("/datasets/{app}/{dataset}/duplicates", get(dataset_duplicates))
        .route("/datasets/{app}/{dataset}/changes", get(dataset_changes))
        .route("/datasets/{app}/{dataset}/history", get(record_history))
        .route("/watches", get(list_watches).post(create_watch))
        .route("/watches/{id}", axum::routing::delete(delete_watch))
        .route("/watches/{id}/enabled", post(set_watch_enabled))
        .route("/triggers", get(list_triggers).post(create_trigger))
        .route("/triggers/{id}", axum::routing::delete(delete_trigger))
        .route("/triggers/{id}/enabled", post(set_trigger_enabled))
        .route("/triggers/{id}/test", post(test_trigger))
        .route("/triggers/{id}/runs", get(trigger_runs))
        .route("/webhooks/deliveries", get(list_deliveries))
        .route("/webhooks/deliveries/{id}", get(get_delivery))
        .route("/webhooks/deliveries/{id}/replay", post(replay_delivery))
        .route("/hosts", get(list_hosts))
        .route("/hosts/{host}", get(get_host))
        .route("/hosts/{host}/memory", axum::routing::delete(delete_host_memory))
        .route("/plugins", get(list_plugins))
        .route("/plugins/reload", post(reload_plugins))
        .route("/search", get(search))
        .route("/search/docs", axum::routing::delete(delete_search_docs))
        .route("/searches", get(list_saved_searches).post(create_saved_search))
        .route("/searches/{id}", axum::routing::delete(delete_saved_search))
        .route("/searches/{id}/enabled", post(set_saved_search_enabled))
        .route(
            "/search/datasets/{app}/{dataset}",
            axum::routing::delete(delete_search_dataset),
        )
        .layer(tower_http::trace::TraceLayer::new_for_http())
        // Local power mode: any localhost web app may call this API directly.
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state)
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
        // Engine/storage/parse/config failures are all unexpected at the request
        // boundary. The client-distinguishable outcomes — missing resource (404),
        // wrong state (409), bad input (400) — are raised explicitly by the
        // handlers, which know the semantics a bare `Error` cannot express.
        Self(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

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
    let timing = state.storage.job_timing_stats().await?;
    let schedules = state.storage.list_schedules().await?;
    let mut out = String::new();
    out.push_str("# HELP pumper_jobs Jobs by status\n# TYPE pumper_jobs gauge\n");
    for status in ["queued", "running", "succeeded", "failed", "cancelled"] {
        let n = counts.iter().find(|(s, _)| s == status).map_or(0, |(_, n)| *n);
        out.push_str(&format!("pumper_jobs{{status=\"{status}\"}} {n}\n"));
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

#[derive(Deserialize, Default)]
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
        max_attempts: body.max_attempts.unwrap_or(1),
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

#[derive(Deserialize)]
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
    let next_cursor = ((jobs.len() as i64) == limit)
        .then(|| jobs.last())
        .flatten()
        .map(|j| format!("{}|{}", pumper_core::datasets::ts(j.created_at), j.id));
    Ok(Json(json!({ "items": jobs, "next_cursor": next_cursor })))
}

async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, ApiError> {
    state
        .storage
        .get(id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "job not found".into()))
}

/// Re-queues a failed or cancelled job with one more attempt.
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

async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.cancel(id).await? {
        state.events.emit(JobEvent::new(id, "", "cancelled"));
        Ok(Json(json!({ "cancelled": true })))
    } else {
        Err(job_state_error(&state, id, "job is not in 'queued' state (already running or terminal)").await)
    }
}

// ---- Costs ------------------------------------------------------------------

/// A job's cost events + total, with cost-per-fresh-record yield when the
/// job's result exposes new/changed counts (the upsert-summary convention).
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

#[derive(Deserialize)]
struct CostSummaryQuery {
    app: Option<String>,
    /// RFC 3339 lower bound for the window.
    since: Option<String>,
}

/// Spend grouped by (app, engine) — the ROI overview.
async fn cost_summary(
    State(state): State<AppState>,
    Query(query): Query<CostSummaryQuery>,
) -> Result<Json<Value>, ApiError> {
    let since = query
        .since
        .as_deref()
        .map(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&chrono::Utc))
                .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("invalid 'since': {e}")))
        })
        .transpose()?;
    let summary = state.costs.summary(query.app.as_deref(), since).await?;
    let total: f64 = summary.iter().map(|s| s.cost_usd).sum();
    Ok(Json(json!({ "total_usd": total, "by_app_engine": summary })))
}

// ---- Schedules ------------------------------------------------------------

#[derive(Deserialize)]
struct SchedulesQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

async fn list_schedules(
    State(state): State<AppState>,
    Query(query): Query<SchedulesQuery>,
) -> Result<Json<Value>, ApiError> {
    let Some(cursor) = &query.cursor else {
        return Ok(Json(json!(state.storage.list_schedules().await?)));
    };
    let limit = query.limit.clamp(1, 500);
    let after = parse_cursor(cursor);
    let items = state.storage.list_schedules_page(after, limit).await?;
    let next_cursor = keyset_cursor(&items, limit, |s| {
        format!("{}|{}", pumper_core::datasets::ts(s.created_at), s.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(Deserialize)]
struct CreateScheduleBody {
    app: String,
    /// 6-field cron with seconds: "sec min hour day month weekday".
    cron: String,
    params: Option<Value>,
    priority: Option<i64>,
}

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

    let schedule = state
        .storage
        .create_schedule(
            &body.app,
            &body.cron,
            body.params.unwrap_or(Value::Null),
            body.priority.unwrap_or(0),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(schedule)))
}

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

#[derive(Deserialize)]
struct EnabledBody {
    enabled: bool,
}

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

async fn list_datasets(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let names = state.datasets.datasets(&name).await?;
    Ok(Json(json!({ "app": name, "datasets": names })))
}

#[derive(Deserialize)]
struct RecordsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

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
    let next_cursor = ((records.len() as i64) == limit)
        .then(|| records.last())
        .flatten()
        .map(|r| format!("{}|{}", pumper_core::datasets::ts(r.updated_at), r.key));
    Ok(Json(json!({ "items": records, "next_cursor": next_cursor })))
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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
async fn dataset_changes(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<ChangesQuery>,
) -> Result<Json<Value>, ApiError> {
    let since = query
        .since
        .as_deref()
        .map(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&chrono::Utc))
                .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("invalid 'since': {e}")))
        })
        .transpose()?;
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct WatchesQuery {
    app: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

async fn list_watches(
    State(state): State<AppState>,
    Query(query): Query<WatchesQuery>,
) -> Result<Json<Value>, ApiError> {
    let Some(cursor) = &query.cursor else {
        let watches = state.storage.list_watches(query.app.as_deref()).await?;
        return Ok(Json(json!({ "watches": watches })));
    };
    let limit = query.limit.clamp(1, 500);
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

#[derive(Deserialize)]
struct CreateWatchBody {
    app: String,
    /// Dataset to watch; "*" (default) watches every dataset of the app.
    dataset: Option<String>,
    /// URL that receives `dataset.changed` POSTs.
    url: String,
    /// If set, delivery bodies are HMAC-SHA256 signed with this secret.
    secret: Option<String>,
}

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

#[derive(Deserialize)]
struct TriggersQuery {
    app: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

async fn list_triggers(
    State(state): State<AppState>,
    Query(query): Query<TriggersQuery>,
) -> Result<Json<Value>, ApiError> {
    let Some(cursor) = &query.cursor else {
        let triggers = state.storage.list_triggers(query.app.as_deref()).await?;
        return Ok(Json(json!({ "triggers": triggers })));
    };
    let limit = query.limit.clamp(1, 500);
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

#[derive(Deserialize)]
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
            max_attempts: body.max_attempts.unwrap_or(1),
        })
        .await?;
    Ok((StatusCode::CREATED, Json(trigger)))
}

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

#[derive(Deserialize)]
struct TestTriggerQuery {
    /// When true, actually enqueue the resolved hop (repeatable — the
    /// idempotency key is bypassed for testing). Default: dry-run only.
    #[serde(default)]
    fire: bool,
}

/// Dry-runs a trigger against its most recent matching source job: shows
/// whether it would fire, the resolved target params, and why not otherwise.
/// `?fire=true` enqueues the hop for real.
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

#[derive(Deserialize)]
struct RunsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
}

/// Jobs this trigger fired, newest first — the lineage view.
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

#[derive(Deserialize)]
struct DeliveriesQuery {
    /// 'pending' | 'delivered' | 'failed' — `failed` is the dead-letter view.
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct SavedSearchesQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

async fn list_saved_searches(
    State(state): State<AppState>,
    Query(query): Query<SavedSearchesQuery>,
) -> Result<Json<Value>, ApiError> {
    let Some(cursor) = &query.cursor else {
        let searches = state.storage.list_saved_searches(false).await?;
        return Ok(Json(json!({ "searches": searches })));
    };
    let limit = query.limit.clamp(1, 500);
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct DeleteDocsBody {
    ids: Vec<String>,
}

/// Removes specific documents from the search index by id.
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
async fn delete_search_dataset(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    state.search.delete_dataset(&app, &dataset).await?;
    Ok(Json(json!({ "app": app, "dataset": dataset, "deleted": true })))
}

// ---- Host profiles (learned tier memory + politeness) -----------------------

#[derive(Deserialize)]
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

// ---- WASM plugins ---------------------------------------------------------

async fn list_plugins(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "plugins": state.plugins.list() }))
}

/// Hot-swap: rescan the plugin directory and reload every `.wasm` module.
async fn reload_plugins(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let loaded = state.plugins.reload().await?;
    Ok(Json(json!({ "loaded": loaded })))
}
