//! Job queue lifecycle and the engine spend ledger: enqueue, list, inspect,
//! retry (single + bulk), reset, cancel, and per-job / aggregate costs.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::{EnqueueOptions, Job, JobStatus};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::events::JobEvent;
use crate::routes::error::{
    default_limit, keyset_cursor, parse_cursor, parse_since, ApiError, MAX_ATTEMPTS_CAP,
};
use crate::state::AppState;

/// Merge a request's `params` over the app's defaults. A POST that sets one key
/// must not silently drop the rest of the defaults (which the scheduler still
/// runs with) — so an object body **shallow-merges** over the object defaults.
/// A non-object body (or non-object defaults) can't be merged key-wise, so it
/// replaces, matching the prior behaviour for those shapes.
pub(crate) fn merge_params(defaults: Value, over: Option<Value>) -> Value {
    match (defaults, over) {
        (defaults, None) => defaults,
        (Value::Object(mut base), Some(Value::Object(top))) => {
            base.extend(top);
            Value::Object(base)
        }
        (_, Some(over)) => over,
    }
}

#[derive(Deserialize, Default, ToSchema)]
pub(crate) struct EnqueueBody {
    /// Job params. An object here **shallow-merges over the app's
    /// `default_params`** (see `GET /apps`), so setting one key keeps the rest of
    /// the defaults — matching what the scheduler runs. A non-object value
    /// replaces the defaults wholesale.
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
        (status = 409, description = "The name is a discovered dynamic WASM app (`GET /apps` lists it with `dynamic: true, runnable: false`) — listed but not runnable in this build; the message carries the reason", body = Object),
        (status = 422, description = "Merged params fail the app's declared JSON Schema (message carries JSON-pointer paths)", body = Object),
    )
)]
pub(crate) async fn enqueue_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: axum::http::HeaderMap,
    body: Option<Json<EnqueueBody>>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    let Some(app) = state.registry.get(&name) else {
        // Not compiled in — but if discovery listed it as a dynamic app, say so
        // precisely (409 + the listing's own reason) instead of a blank 404:
        // dynamic apps are read-only manifests until the component-model host
        // lands, and nothing here may pretend otherwise.
        if let Some(entry) = state
            .dynamic_apps
            .iter()
            .find(|e| e.get("name").and_then(serde_json::Value::as_str) == Some(name.as_str()))
        {
            let reason = entry
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(crate::registry::DYNAMIC_NOT_RUNNABLE_REASON);
            return Err(ApiError(
                StatusCode::CONFLICT,
                format!("dynamic app '{name}' is not runnable: {reason}"),
            ));
        }
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("unknown app '{name}'"),
        ));
    };
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .or(body.idempotency_key)
        .filter(|k| !k.trim().is_empty());
    let params = merge_params(app.default_params(), body.params);
    // Manifest enforcement: an app that declares a params schema gets it
    // enforced at the door — the silent wrong-params job that fails (or worse,
    // half-runs) minutes later becomes an immediate 422 with pointer paths.
    // Validated on the MERGED params, i.e. exactly what the job would run with.
    // The check itself is `mcp::validate_app_params`, shared with every other
    // door that creates work (schedules, the scheduler's fire path, trigger
    // hops) so this door's answer is the only answer.
    if let Err(msg) = crate::mcp::validate_app_params(&state.registry, &name, &params) {
        return Err(ApiError(StatusCode::UNPROCESSABLE_ENTITY, msg));
    }
    let opts = EnqueueOptions {
        params,
        max_attempts: body.max_attempts.unwrap_or(1).clamp(1, MAX_ATTEMPTS_CAP),
        delay_secs: body.delay_secs.unwrap_or(0),
        priority: body.priority.unwrap_or(0),
        callback_url: body.callback_url,
        callback_secret: body.callback_secret,
        budget_usd: body.budget_usd.filter(|b| *b > 0.0),
        idempotency_key,
        schedule_id: None,
        trigger_id: None,
        source_job_id: None,
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
pub(crate) struct ListQuery {
    app: Option<String>,
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor. Presence (even empty, for page 1) switches the
    /// response to `{items, next_cursor}`; absent keeps the legacy bare array.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/jobs",
    tag = "jobs",
    params(ListQuery),
    responses((status = 200, description = "Dual-mode: without `cursor` a bare `[Job]` array; with `cursor` present (even empty) `{items: [Job], next_cursor}` paged by keyset."))
)]
pub(crate) async fn list_jobs(
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
        let jobs = state
            .storage
            .list(query.app.as_deref(), status, limit)
            .await?;
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
pub(crate) async fn get_job(
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
pub(crate) async fn retry_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    match state.storage.retry(id).await? {
        Some(job) => {
            state
                .events
                .emit(JobEvent::new(job.id, job.app.clone(), "queued"));
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
pub(crate) struct BulkRetryBody {
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
pub(crate) async fn bulk_retry_jobs(
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
    let ids = state
        .storage
        .retry_bulk(status, body.app.as_deref(), cap)
        .await?;
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
pub(crate) async fn reset_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    match state.storage.reset(id).await? {
        Some(job) => {
            state
                .events
                .emit(JobEvent::new(job.id, job.app.clone(), "queued"));
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
pub(crate) async fn cancel_job(
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
    let token = super::error::lock_advisory(&state.job_cancels, "job_cancels")
        .get(&id)
        .map(|(_, t)| t.clone());
    if let Some(token) = token {
        token.cancel();
        return Ok(Json(json!({ "cancelled": true, "running": true })));
    }
    Err(job_state_error(
        &state,
        id,
        "job is already terminal (succeeded/failed/cancelled)",
    )
    .await)
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
pub(crate) async fn job_costs(
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
pub(crate) struct CostSummaryQuery {
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
pub(crate) async fn cost_summary(
    State(state): State<AppState>,
    Query(query): Query<CostSummaryQuery>,
) -> Result<Json<Value>, ApiError> {
    let since = parse_since(query.since.as_deref())?;
    let summary = state.costs.summary(query.app.as_deref(), since).await?;
    let total: f64 = summary.iter().map(|s| s.cost_usd).sum();
    Ok(Json(
        json!({ "total_usd": total, "by_app_engine": summary }),
    ))
}

#[cfg(test)]
mod merge_tests {
    use super::merge_params;
    use serde_json::json;

    #[test]
    fn no_override_keeps_all_defaults() {
        let out = merge_params(json!({ "year": "2021", "naics": ["2382"] }), None);
        assert_eq!(out, json!({ "year": "2021", "naics": ["2382"] }));
    }

    #[test]
    fn object_override_shallow_merges_and_preserves_other_defaults() {
        // The bug this fixes: a one-key override used to drop `year`.
        let out = merge_params(
            json!({ "year": "2021", "naics": ["2382"] }),
            Some(json!({ "naics": "23" })),
        );
        assert_eq!(out, json!({ "year": "2021", "naics": "23" }));
    }

    #[test]
    fn non_object_override_replaces() {
        // A scalar/array body can't merge key-wise, so it replaces (prior behaviour).
        let out = merge_params(json!({ "a": 1 }), Some(json!([1, 2, 3])));
        assert_eq!(out, json!([1, 2, 3]));
    }
}
