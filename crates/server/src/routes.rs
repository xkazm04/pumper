use std::convert::Infallible;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use pumper_core::{EnqueueOptions, Job, JobStatus, Record, Schedule};
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
        .route("/plugins", get(list_plugins))
        .route("/plugins/reload", post(reload_plugins))
        .route("/search", get(search))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        // Local power mode: any localhost web app may call this API directly.
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state)
}

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<pumper_core::Error> for ApiError {
    fn from(e: pumper_core::Error) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

// ---- Observability --------------------------------------------------------

/// Prometheus-style text exposition of queue + platform gauges.
async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    let counts = state.storage.status_counts().await?;
    let schedules = state.storage.list_schedules().await?;
    let mut out = String::new();
    out.push_str("# HELP pumper_jobs Jobs by status\n# TYPE pumper_jobs gauge\n");
    for status in ["queued", "running", "succeeded", "failed", "cancelled"] {
        let n = counts.iter().find(|(s, _)| s == status).map_or(0, |(_, n)| *n);
        out.push_str(&format!("pumper_jobs{{status=\"{status}\"}} {n}\n"));
    }
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
    Ok(([("content-type", "text/plain; version=0.0.4")], out).into_response())
}

/// SSE stream of all job status transitions.
async fn stream_events(State(state): State<AppState>) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.events.subscribe();
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(event) => yield Ok(sse_event(&event)),
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// SSE stream scoped to one job; closes once the job reaches a terminal state.
async fn stream_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    // Subscribe before snapshotting so no transition slips through the gap.
    let mut rx = state.events.subscribe();
    let snapshot = state.storage.get(id).await.ok().flatten();
    let stream = async_stream::stream! {
        if let Some(job) = snapshot {
            let mut event = JobEvent::new(job.id, job.app.clone(), job.status.as_str());
            event.result = job.result.clone();
            event.error = job.error.clone();
            yield Ok(sse_event(&event));
            if is_terminal(job.status) {
                return;
            }
        }
        loop {
            match rx.recv().await {
                Ok(event) if event.job_id == id => {
                    let done = matches!(event.status.as_str(), "succeeded" | "failed" | "cancelled");
                    yield Ok(sse_event(&event));
                    if done {
                        break;
                    }
                }
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn sse_event(event: &JobEvent) -> Event {
    Event::default()
        .event("job")
        .json_data(event)
        .unwrap_or_else(|_| Event::default().comment("serialize error"))
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
}

async fn enqueue_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Option<Json<EnqueueBody>>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    let Some(app) = state.registry.get(&name) else {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("unknown app '{name}'")));
    };
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let opts = EnqueueOptions {
        params: body.params.unwrap_or_else(|| app.default_params()),
        max_attempts: body.max_attempts.unwrap_or(1),
        delay_secs: body.delay_secs.unwrap_or(0),
        priority: body.priority.unwrap_or(0),
        callback_url: body.callback_url,
        callback_secret: body.callback_secret,
    };
    let job = state.storage.enqueue(&name, opts).await?;
    state.notify.notify_one();
    Ok((StatusCode::ACCEPTED, Json(job)))
}

#[derive(Deserialize)]
struct ListQuery {
    app: Option<String>,
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_limit() -> i64 {
    50
}

async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<Job>>, ApiError> {
    let status = query
        .status
        .as_deref()
        .map(|s| {
            JobStatus::parse(s)
                .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, format!("invalid status '{s}'")))
        })
        .transpose()?;
    let jobs = state
        .storage
        .list(query.app.as_deref(), status, query.limit.clamp(1, 500))
        .await?;
    Ok(Json(jobs))
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

async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.cancel(id).await? {
        state
            .events
            .send(JobEvent::new(id, "", "cancelled"))
            .ok();
        Ok(Json(json!({ "cancelled": true })))
    } else {
        Err(ApiError(
            StatusCode::CONFLICT,
            "job not found or not in 'queued' state".into(),
        ))
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

async fn list_schedules(State(state): State<AppState>) -> Result<Json<Vec<Schedule>>, ApiError> {
    Ok(Json(state.storage.list_schedules().await?))
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
}

async fn list_records(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<RecordsQuery>,
) -> Result<Json<Vec<Record>>, ApiError> {
    let records = state
        .datasets
        .list(&app, &dataset, query.limit.clamp(1, 1000))
        .await?;
    Ok(Json(records))
}

async fn export_records(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let records = state.datasets.list(&app, &dataset, 100_000).await?;
    Ok(Json(json!({
        "app": app,
        "dataset": dataset,
        "count": records.len(),
        "records": records,
    })))
}

#[derive(Deserialize)]
struct DupQuery {
    #[serde(default = "default_distance")]
    distance: u32,
}

fn default_distance() -> u32 {
    3
}

/// Near-duplicate record pairs (SimHash Hamming distance ≤ `distance`).
async fn dataset_duplicates(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<DupQuery>,
) -> Result<Json<Value>, ApiError> {
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
    let changes = state
        .datasets
        .changes_since(&app, Some(&dataset), since, query.limit.clamp(1, 1000))
        .await?;
    Ok(Json(json!({
        "app": app,
        "dataset": dataset,
        "count": changes.len(),
        "changes": changes,
    })))
}

#[derive(Deserialize)]
struct HistoryQuery {
    /// Record key (query param, since keys may contain URL-hostile characters).
    key: String,
    #[serde(default = "default_limit")]
    limit: i64,
}

/// A single record's revision history, newest first.
async fn record_history(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Value>, ApiError> {
    let revisions = state
        .datasets
        .history(&app, &dataset, &query.key, query.limit.clamp(1, 500))
        .await?;
    Ok(Json(json!({
        "app": app,
        "dataset": dataset,
        "key": query.key,
        "count": revisions.len(),
        "revisions": revisions,
    })))
}

// ---- Dataset watches --------------------------------------------------------

#[derive(Deserialize)]
struct WatchesQuery {
    app: Option<String>,
}

async fn list_watches(
    State(state): State<AppState>,
    Query(query): Query<WatchesQuery>,
) -> Result<Json<Value>, ApiError> {
    let watches = state.storage.list_watches(query.app.as_deref()).await?;
    Ok(Json(json!({ "watches": watches })))
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

// ---- Full-text search -----------------------------------------------------

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

fn default_search_limit() -> usize {
    20
}

/// Full-text search across everything indexed from job results (BM25 ranked).
async fn search(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<Value>, ApiError> {
    if query.q.trim().is_empty() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "query 'q' is required".into()));
    }
    let hits = state.search.query(&query.q, query.limit.clamp(1, 100)).await?;
    Ok(Json(json!({ "query": query.q, "count": hits.len(), "hits": hits })))
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
