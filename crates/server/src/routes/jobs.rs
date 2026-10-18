//! Job queue lifecycle and the engine spend ledger: enqueue, list, inspect,
//! retry (single + bulk), reset, cancel, and per-job / aggregate costs.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::config::BudgetImputation;
use pumper_core::costs::{
    budget_verdict, headroom_certainty, total_certainty, BudgetVerdict, JobSpend, SpendCertainty,
};
use pumper_core::{EnqueueOptions, Job, JobStatus};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::auth::CallerPrincipal;
use crate::events::JobEvent;
use crate::routes::error::{
    default_limit, keyset_cursor, parse_cursor, parse_rfc3339_param, parse_since, ApiError,
    MAX_ATTEMPTS_CAP,
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

/// The spend ceiling a job may carry, or the caller-facing refusal.
///
/// The anti-pattern this replaces: `budget_usd.filter(|b| *b > 0.0)`. A caller
/// sending `0.0` ("spend nothing") or a negative number had it silently dropped
/// to `None` — and `None` at this door means **no ceiling at all**, i.e.
/// unlimited spend on the paid paths (Claude research, the paid fetch tiers).
/// The most cautious input produced the least cautious job, silently.
///
/// So a non-positive (or non-finite) budget is refused *at the door*, with a
/// message that names what it would have meant, instead of being reinterpreted.
/// `None` still passes through unchanged: omitting the field is the documented
/// "no ceiling" request, and this function does not invent one.
pub(crate) fn validate_budget_usd(requested: Option<f64>) -> Result<Option<f64>, String> {
    match requested {
        None => Ok(None),
        Some(b) if b.is_finite() && b > 0.0 => Ok(Some(b)),
        Some(b) => Err(format!(
            "budget_usd must be a positive number of dollars (got {b}). It is not a way to say \
             'spend nothing': an omitted budget_usd means this job runs with NO spend ceiling, so \
             honouring {b} here would have turned the most cautious request into the least \
             limited job. Pass a real ceiling (e.g. 0.25) to cap the job, or — to run a paid app \
             on free tiers only — pass that app's own zero budget param (the research app takes \
             \"max_budget_usd\": 0), or enqueue over MCP, where the [mcp] max_job_budget_usd rail \
             treats 0 as a real $0 ceiling."
        )),
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
    /// Must be **> 0** — see [`validate_budget_usd`]: omitting it means "no
    /// ceiling", so `0` cannot also mean "spend nothing" and is refused (422)
    /// rather than silently reinterpreted as unlimited.
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
        (status = 202, description = "Job enqueued", body = crate::routes::dto::JobDto),
        (status = 200, description = "Idempotency-Key replay: the original job", body = crate::routes::dto::JobDto),
        (status = 404, description = "Unknown app", body = crate::routes::dto::ErrorEnvelope),
        (status = 409, description = "The name is a discovered dynamic WASM app (`GET /apps` lists it with `dynamic: true, runnable: false`) — listed but not runnable in this build; the message carries the reason", body = crate::routes::dto::ErrorEnvelope),
        (status = 422, description = "Merged params fail the app's declared JSON Schema (message carries JSON-pointer paths), or `budget_usd` is not a positive number of dollars", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn enqueue_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: axum::http::HeaderMap,
    caller: Option<axum::Extension<CallerPrincipal>>,
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
    // Budget floor: a non-positive ceiling is refused here rather than dropped
    // to `None` (= unlimited). See `validate_budget_usd`.
    let budget_usd = validate_budget_usd(body.budget_usd)
        .map_err(|msg| ApiError(StatusCode::UNPROCESSABLE_ENTITY, msg))?;
    let target_key = crate::mcp::target_key_for(&state.registry, &name, &params);
    let opts = EnqueueOptions {
        params,
        max_attempts: body.max_attempts.unwrap_or(1).clamp(1, MAX_ATTEMPTS_CAP),
        delay_secs: body.delay_secs.unwrap_or(0),
        priority: body.priority.unwrap_or(0),
        callback_url: body.callback_url,
        callback_secret: body.callback_secret,
        budget_usd,
        idempotency_key,
        schedule_id: None,
        trigger_id: None,
        source_job_id: None,
        target_key,
        workflow_run_id: None,
        workflow_step: None,
        root_id: None,
    };
    // N20 attribution: the principal the identity layer resolved is stamped onto
    // the job row, which is what makes `cost_events.principal_id` (copied from
    // the job at metering time) anything other than NULL. `stored_id()` is
    // `None` for the synthetic `open`-mode operator — an identity this server
    // invented rather than authenticated — so an `open` node keeps reporting its
    // spend as `(unattributed)`, which is the truth about it.
    let principal_id = caller
        .as_ref()
        .and_then(|axum::Extension(c)| c.stored_id())
        .map(str::to_string);
    let (job, created) = state
        .storage
        .enqueue_dedup_as(&name, opts, principal_id.as_deref())
        .await?;
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
    /// The schedule that enqueued the job (`jobs.schedule_id`, migration 0012).
    /// The run history `GET /schedules` only ever showed one row of.
    schedule_id: Option<String>,
    /// The trigger hop that enqueued the job (`jobs.trigger_id`, 0014).
    trigger_id: Option<String>,
    /// The dedup target (`jobs.target_key`, 0041). Deliberately NOT scoped to
    /// an app: a target is shared across apps, which is the whole point of it.
    target_key: Option<String>,
    /// Inclusive RFC-3339 lower bound on `created_at`.
    since: Option<String>,
    /// **Exclusive** RFC-3339 upper bound on `created_at`, so adjacent windows
    /// partition rather than overlap.
    until: Option<String>,
    /// `status` — answer `{total, counts}` from one `GROUP BY` over the same
    /// filters instead of the rows themselves.
    rollup: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor. Presence (even empty, for page 1) switches the
    /// response to `{items, next_cursor}`; absent keeps the legacy bare array.
    cursor: Option<String>,
}

/// Every query param `GET /jobs` understands — the inventory
/// [`unknown_list_params`] refuses against, and the list its refusal prints.
///
/// It is a declared constant rather than something derived from `ListQuery`
/// because serde has no runtime field list; the paired test
/// `an_unknown_query_param_is_refused_not_dropped` walks BOTH directions (every
/// name here is accepted, three near-misses are refused), which is what keeps
/// the two in step.
pub(crate) const JOB_LIST_PARAMS: &[&str] = &[
    "app",
    "status",
    "schedule_id",
    "trigger_id",
    "target_key",
    "since",
    "until",
    "rollup",
    "limit",
    "cursor",
];

/// The query-string keys this endpoint does not understand, in the order they
/// appeared.
///
/// Why this exists at all: `ListQuery` is a plain serde `Deserialize`, and
/// serde **silently drops** a field it has no slot for. Before the lineage
/// params landed, `?schedule_id=S` therefore answered `200` with the
/// *unfiltered* list — an operator who assumed the obvious param existed got a
/// wrong answer wearing a right answer's status code. Adding the five params
/// fixes the five; refusing the unknown closes the class, so the next
/// `?scheduleId=` or `?source_job_id=` is a 400 the caller can act on instead
/// of a plausible lie.
///
/// Keys only — a value is never inspected and never echoed, so a refusal
/// cannot reflect caller-supplied content back. `?x` with no `=` counts as the
/// key `x`; a bare `&` contributes nothing.
pub(crate) fn unknown_list_params(raw: Option<&str>) -> Vec<String> {
    raw.unwrap_or("")
        .split('&')
        .map(|pair| pair.split('=').next().unwrap_or(""))
        .filter(|key| !key.is_empty() && !JOB_LIST_PARAMS.contains(key))
        .map(str::to_string)
        .collect()
}

/// `?rollup=` is a closed vocabulary of one. A typo must be a 400, not a
/// silent fall-through to the row list — the same posture `parse_removed` and
/// `parse_feed_order` already take on this API.
pub(crate) fn parse_rollup(raw: Option<&str>) -> Result<bool, ApiError> {
    match raw {
        None => Ok(false),
        Some("status") => Ok(true),
        Some(other) => Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("invalid 'rollup': '{other}' — the only rollup is 'status'"),
        )),
    }
}

/// `ListQuery` → the store's predicate set, with every value validated.
///
/// Extracted so the two response modes and the rollup cannot build three
/// slightly different queries out of one request — the divergence that made
/// `GET /datasets/{app}/{ds}` answer `trust=` three ways.
pub(crate) fn job_query(query: &ListQuery) -> Result<pumper_core::storage::JobQuery, ApiError> {
    let status = query
        .status
        .as_deref()
        .map(|s| {
            JobStatus::parse(s)
                .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, format!("invalid status '{s}'")))
        })
        .transpose()?;
    Ok(pumper_core::storage::JobQuery {
        app: query.app.clone(),
        status,
        schedule_id: query.schedule_id.clone(),
        trigger_id: query.trigger_id.clone(),
        target_key: query.target_key.clone(),
        since: parse_since(query.since.as_deref())?,
        until: parse_rfc3339_param("until", query.until.as_deref())?,
    })
}

/// `{total, counts}` out of the store's `(status, count)` rows.
///
/// `total` is summed from the same rows the counts come from, so the two halves
/// of the answer cannot disagree. A status the window does not hold is
/// **absent**, not `0`: five invented zeroes per response would make "no
/// failures last night" and "no runs last night" read identically.
pub(crate) fn rollup_body(counts: &[(String, i64)]) -> Value {
    let total: i64 = counts.iter().map(|(_, n)| *n).sum();
    let map: serde_json::Map<String, Value> = counts
        .iter()
        .map(|(status, n)| (status.clone(), json!(n)))
        .collect();
    json!({ "total": total, "counts": map })
}

#[utoipa::path(
    get,
    path = "/jobs",
    tag = "jobs",
    params(ListQuery),
    responses(
        (status = 200, description = "Dual-mode: without `cursor` a bare `[Job]` array; with `cursor` present (even empty) `{items: [Job], next_cursor}` paged by keyset. With `rollup=status`, `{total, counts}` from one GROUP BY over the same filters.", body = crate::routes::dto::JobsResponse),
        (status = 400, description = "A query param this endpoint does not understand, an unparseable `since`/`until`, an unknown `status`, or a `rollup` other than `status`", body = Object),
    )
)]
pub(crate) async fn list_jobs(
    State(state): State<AppState>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let unknown = unknown_list_params(raw.as_deref());
    if !unknown.is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "unknown query param(s) {unknown:?} on GET /jobs — understood: {}",
                JOB_LIST_PARAMS.join(", ")
            ),
        ));
    }
    let q = job_query(&query)?;
    if parse_rollup(query.rollup.as_deref())? {
        let counts = state.storage.status_counts_filtered(&q).await?;
        return Ok(Json(rollup_body(&counts)));
    }
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        let jobs = state.storage.list(&q, limit).await?;
        return Ok(Json(json!(jobs)));
    };
    let after = parse_cursor(cursor);
    let jobs = state.storage.list_page(&q, after, limit).await?;
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
        (status = 200, description = "The job", body = crate::routes::dto::JobDetail),
        (status = 404, description = "Job not found", body = crate::routes::dto::ErrorEnvelope),
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
        (status = 202, description = "Re-queued job", body = crate::routes::dto::JobDto),
        (status = 404, description = "Job not found", body = crate::routes::dto::ErrorEnvelope),
        (status = 409, description = "Job not in a retryable (failed/cancelled) state", body = crate::routes::dto::ErrorEnvelope),
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

/// The `queued` events a bulk retry has to announce — one per job, each
/// carrying that job's **real** app.
///
/// The anti-pattern: `JobEvent::new(id, "", "queued")`. `mcp::live::LiveFilter`
/// (`GET /mcp?app=…`) keeps an event only when its `app` matches exactly, so an
/// app-scoped watcher never saw a bulk retry re-queue its jobs — the work
/// restarted and the one surface watching it showed nothing at all.
pub(crate) fn requeued_events(requeued: &[(Uuid, String)]) -> Vec<JobEvent> {
    requeued
        .iter()
        .map(|(id, app)| JobEvent::new(*id, app.clone(), "queued"))
        .collect()
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
        (status = 200, description = "`{retried: <count>, ids: [uuid]}`", body = crate::routes::dto::BulkRetryResponse),
        (status = 400, description = "status must be failed|cancelled", body = crate::routes::dto::ErrorEnvelope),
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
    let requeued = state
        .storage
        .retry_bulk(status, body.app.as_deref(), cap)
        .await?;
    for event in requeued_events(&requeued) {
        state.events.emit(event);
    }
    if !requeued.is_empty() {
        state.notify.notify_one();
    }
    // The wire shape is unchanged: `ids` stays a bare uuid array; the app rides
    // the events, which is where it was missing.
    let ids: Vec<Uuid> = requeued.iter().map(|(id, _)| *id).collect();
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
        (status = 202, description = "Re-queued job", body = crate::routes::dto::JobDto),
        (status = 404, description = "Job not found", body = crate::routes::dto::ErrorEnvelope),
        (status = 409, description = "Job not in `running` state", body = crate::routes::dto::ErrorEnvelope),
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

#[derive(Deserialize, ToSchema, Default)]
pub(crate) struct ResumeBody {
    /// The answer the parked job asked for (`input_request` on `GET
    /// /jobs/{id}`). Any JSON; the app is what interprets it. Omitted = `null`,
    /// which is a legitimate answer for a pure "proceed" gate.
    #[serde(default)]
    input: Value,
}

/// Answers a `waiting` job and re-queues it (N02).
///
/// The job resumes from the checkpoint `ctx.await_input(..)` forced before it
/// parked, and reads `input` back through `ctx.restore_input()`. **No attempt is
/// burned**: the re-queue uses `reset`'s `max_attempts = MAX(max_attempts,
/// attempts + 1)` headroom, so a job that waited an hour for a human still has
/// every retry it started with.
///
/// 409 on anything that is not `waiting` — including a second resume of a job
/// the first one already re-queued. That guard IS the `(status, attempts)` fence
/// applied at the door: a stale resume (the operator's tab was open across the
/// answer, the expiry sweep already fired) matches no row, so it cannot restart
/// a lineage that has moved on.
#[utoipa::path(
    post,
    path = "/jobs/{id}/resume",
    tag = "jobs",
    params(("id" = Uuid, Path, description = "Job id")),
    request_body = ResumeBody,
    responses(
        (status = 202, description = "Re-queued job", body = crate::routes::dto::JobDto),
        (status = 404, description = "Job not found", body = crate::routes::dto::ErrorEnvelope),
        (status = 409, description = "Job not in `waiting` state", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn resume_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    body: Option<Json<ResumeBody>>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    let input = body.map(|Json(b)| b.input).unwrap_or(Value::Null);
    match state.storage.resume(id, &input).await? {
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
            "job is not in 'waiting' state (nothing is awaiting input on it)",
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

/// Cancels a job. A `queued` job — or a `waiting` one, parked on external
/// input with no executor to interrupt — is cancelled synchronously; a
/// `running` job
/// has its cancellation token fired so the worker aborts the app future and
/// marks it `cancelled` (the response reports `running: true`). A terminal job
/// is `409`, an unknown one `404`.
///
/// **During a graceful shutdown** the drain fires those same tokens to mean
/// *suspend* (checkpoint + re-queue). A cancel that reaches a run before it
/// resolves still wins — user intent outranks the drain. A cancel that arrives
/// after the run committed to a suspend cannot win, and says so
/// (`{cancelled: false, suspended: true}`) instead of claiming otherwise.
#[utoipa::path(
    delete,
    path = "/jobs/{id}",
    tag = "jobs",
    params(("id" = Uuid, Path, description = "Job id")),
    responses(
        (status = 200, description = "Cancelled (`{cancelled: true}`; `running: true` when it was in-flight). During a graceful shutdown a run that already committed to a checkpoint suspend answers `{cancelled: false, running: true, suspended: true, note}` — it was re-queued, not cancelled.", body = crate::routes::dto::CancelJobResponse),
        (status = 404, description = "Job not found", body = crate::routes::dto::ErrorEnvelope),
        (status = 409, description = "Job already terminal (succeeded/failed/cancelled)", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    // Queued job: cancel synchronously. The event carries the job's real app —
    // `storage.cancel` returns it from the same statement — because a blank app
    // is filtered out of every app-scoped watcher (`GET /mcp?app=…`), so the
    // cancellation used to land invisibly for exactly the clients watching it.
    if let Some(app) = state.storage.cancel(id).await? {
        state.events.emit(JobEvent::new(id, app, "cancelled"));
        return Ok(Json(json!({ "cancelled": true })));
    }
    // Otherwise it may be running here: fire its cancellation token. The worker
    // task races it against the app future and persists `cancelled` + emits the
    // terminal event, so we don't touch storage or emit from the request path.
    //
    // The intent is claimed *under the token registry's mutex, immediately
    // before the fire*: the drain reads that same registry, so this ordering is
    // what stops the worker resolving the token in the gap between the fire and
    // the mark. See `worker::claim_user_cancel`.
    let outcome = {
        let registry = super::error::lock_advisory(&state.job_cancels, "job_cancels");
        registry.get(&id).map(|(_, token)| {
            let kind = crate::worker::claim_user_cancel(id);
            token.cancel();
            kind
        })
    };
    match outcome {
        Some(crate::worker::CancelKind::User) => {
            Ok(Json(json!({ "cancelled": true, "running": true })))
        }
        // Lost the race with the drain by microseconds. Saying `cancelled: true`
        // here would be a lie the queue itself contradicts a moment later.
        Some(crate::worker::CancelKind::ShutdownSuspend) => Ok(Json(json!({
            "cancelled": false,
            "running": true,
            "suspended": true,
            "note": "the server is shutting down and this run had already committed to a \
                     checkpoint suspend, so it was re-queued rather than cancelled — it resumes \
                     on the next boot. Cancel it again once the server is back up.",
        }))),
        None => Err(job_state_error(
            &state,
            id,
            "job is already terminal (succeeded/failed/cancelled)",
        )
        .await),
    }
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
        (status = 200, description = "`{job_id, app, total_usd, billed_usd, unmetered_calls, unknown_calls, total_certainty, calls, fresh_records, cost_per_fresh_record_usd, cost_per_fresh_record_unavailable, budget_usd, budget_remaining_usd, budget_remaining_certainty, budget_imputed_usd, budget_imputation, unpriceable, breached, events}`. `billed_usd` is the money we can price; `total_certainty` is `exact` or `at_least`. `budget_remaining_usd` is charged for the calls we could not price and `budget_imputed_usd` says how much of that charge is an estimate; it is `null` with `unpriceable: true` when the job has unreadable bills and no readable one, where the ceiling refuses further metered spend.", body = crate::routes::dto::JobCostsResponse),
        (status = 404, description = "Job not found", body = crate::routes::dto::ErrorEnvelope),
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
    let roll = roll_up(&events);
    let fresh = job.result.as_ref().map(|r| {
        r.get("new").and_then(Value::as_u64).unwrap_or(0)
            + r.get("changed").and_then(Value::as_u64).unwrap_or(0)
    });
    let (cost_per_fresh_record, unavailable) = fresh_record_cost(&roll, fresh);
    // The ceiling's own view of the same events — `JobSpend` carries the
    // `billed_calls` denominator `CostRollup` does not, and no second query is
    // issued for it.
    let spend = JobSpend::from_events(&events);
    let mut body = json!({
        "job_id": id,
        "app": job.app,
        // Kept for compatibility, and it is the SAME number as `billed_usd`:
        // the sum of what we can price. Read `total_certainty` before
        // treating either as the bill.
        "total_usd": roll.billed_usd,
        "calls": events.len(),
        "fresh_records": fresh,
        "cost_per_fresh_record_usd": cost_per_fresh_record,
        "cost_per_fresh_record_unavailable": unavailable,
        "events": events,
    });
    merge_certainty_fields(&mut body, &roll);
    if let Some(obj) = body.as_object_mut() {
        obj.extend(budget_block(
            job.budget_usd,
            &spend,
            state.config.costs.budget_imputation,
        ));
    }
    Ok(Json(body))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct CostSummaryQuery {
    app: Option<String>,
    /// RFC 3339 lower bound for the window.
    since: Option<String>,
    /// Restrict the ledger to one caller (N20). Pass the literal
    /// `(unattributed)` for the rows that carry no caller — every legacy row,
    /// and every row an `open`-mode node or an internal producer (scheduler,
    /// trigger hop, workflow step of an unauthenticated run) creates.
    principal: Option<String>,
    /// Keep only (app, engine) groups holding at least one call of this
    /// certainty: `billed` | `unmetered` | `unknown` | `free`.
    certainty: Option<String>,
}

/// Parses the `?certainty=` filter. An unknown value is a 400 naming the
/// vocabulary, never an empty page — "no unmetered spend" and "you asked a
/// question this API does not understand" are opposite operational answers,
/// and the reassuring one must not be returned for the other.
fn parse_certainty(raw: Option<&str>) -> Result<Option<SpendCertainty>, ApiError> {
    let Some(raw) = raw else { return Ok(None) };
    SpendCertainty::parse(raw).map(Some).ok_or_else(|| {
        let known: Vec<&str> = SpendCertainty::ALL.iter().map(|c| c.as_str()).collect();
        ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "unknown certainty '{raw}' — expected one of {}",
                known.join(", ")
            ),
        )
    })
}

/// Spend grouped by (app, engine) — the ROI overview.
#[utoipa::path(
    get,
    path = "/costs",
    tag = "costs",
    params(CostSummaryQuery),
    responses(
        (status = 200, description = "`{total_usd, billed_usd, unmetered_calls, unknown_calls, total_certainty, by_app_engine: [{app, engine, calls, cost_usd, unmetered_calls, unknown_calls}], principal}`. With `?principal=` the ledger is restricted to that caller (`(unattributed)` selects the rows with no caller)", body = crate::routes::dto::CostSummaryResponse),
        (status = 400, description = "Unknown `certainty` value", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn cost_summary(
    State(state): State<AppState>,
    Query(query): Query<CostSummaryQuery>,
) -> Result<Json<Value>, ApiError> {
    let since = parse_since(query.since.as_deref())?;
    let certainty = parse_certainty(query.certainty.as_deref())?;
    let principal = query.principal.as_deref().map(principal_filter);
    // `Option<Option<&str>>`: absent = no filter, `Some(None)` = the rows with
    // no caller, `Some(Some(id))` = that caller. See `summary_filtered`.
    let summary = state
        .costs
        .summary_filtered(query.app.as_deref(), since, certainty, principal)
        .await?;
    let total: f64 = summary.iter().map(|s| s.cost_usd).sum();
    let unmetered: i64 = summary.iter().map(|s| s.unmetered_calls).sum();
    let unknown: i64 = summary.iter().map(|s| s.unknown_calls).sum();
    Ok(Json(json!({
        "total_usd": total,
        "billed_usd": total,
        "unmetered_calls": unmetered,
        "unknown_calls": unknown,
        "total_certainty": total_certainty(unmetered + unknown),
        // Echoed so a caller can see the filter was applied — an empty ledger
        // under a mistyped principal must not read as "this caller spent $0".
        "principal": query.principal,
        "by_app_engine": summary,
    })))
}

/// Maps the caller-facing `principal=` value onto the ledger's own encoding.
///
/// The by-principal report renders NULL rows as `(unattributed)`
/// ([`pumper_core::costs::UNATTRIBUTED_PRINCIPAL`]), so that label has to be
/// accepted back as a filter — otherwise the one row an operator is most likely
/// to click on is the one row that cannot be drilled into. Anything else is a
/// literal principal id.
pub(crate) fn principal_filter(requested: &str) -> Option<&str> {
    (requested != pumper_core::costs::UNATTRIBUTED_PRINCIPAL).then_some(requested)
}

#[cfg(test)]
mod principal_filter_tests {
    use super::principal_filter;

    /// The anti-pattern: `(unattributed)` — the label the by-principal report
    /// prints for rows with no caller — read as an ordinary principal id. The
    /// filter would then match no row, and the one line an operator is most
    /// likely to click on would answer "$0 spent" about the largest bucket on
    /// most deployments.
    #[test]
    fn the_unattributed_label_selects_null_rows_not_a_principal_named_that() {
        assert_eq!(principal_filter("(unattributed)"), None);
        assert_eq!(principal_filter("p_abc"), Some("p_abc"));
        assert_eq!(principal_filter(""), Some(""));
    }
}

#[cfg(test)]
mod list_query_tests {
    use super::{parse_rollup, rollup_body, unknown_list_params, JOB_LIST_PARAMS};
    use serde_json::json;

    /// The anti-pattern: serde drops a field it has no slot for, so a query
    /// param this endpoint cannot honour used to come back as a 200 with the
    /// *unfiltered* list. Every near-miss of a real param is the shape that
    /// actually bites.
    #[test]
    fn a_near_miss_of_a_real_param_is_not_silently_dropped() {
        for raw in [
            "scheduleId=S",
            "schedule=S",
            "Schedule_Id=S",
            "source_job_id=x",
            "app=a&scheduleid=S",
            "limit=10&foo",
        ] {
            assert!(
                !unknown_list_params(Some(raw)).is_empty(),
                "'{raw}' holds a param GET /jobs cannot honour and must be refused, \
                 not answered with an unfiltered list"
            );
        }
    }

    /// The other direction, which is what keeps the inventory honest: a guard
    /// that refuses everything would pass the test above. Every name the
    /// endpoint declares must still be accepted, in every position.
    #[test]
    fn every_declared_param_is_accepted() {
        for name in JOB_LIST_PARAMS {
            assert!(
                unknown_list_params(Some(&format!("{name}=x"))).is_empty(),
                "'{name}' is in JOB_LIST_PARAMS and must be understood"
            );
        }
        let all: Vec<String> = JOB_LIST_PARAMS.iter().map(|p| format!("{p}=x")).collect();
        assert!(unknown_list_params(Some(&all.join("&"))).is_empty());
        // No query string at all, and an empty one, are both fine.
        assert!(unknown_list_params(None).is_empty());
        assert!(unknown_list_params(Some("")).is_empty());
        assert!(unknown_list_params(Some("&&")).is_empty());
    }

    /// Keys only: a value is never inspected, so a value that happens to look
    /// like a param name cannot be mistaken for one — and nothing the caller
    /// supplied is echoed back out of a value.
    #[test]
    fn only_keys_are_inspected_never_values() {
        assert!(unknown_list_params(Some("app=nonsense=more")).is_empty());
        assert_eq!(
            unknown_list_params(Some("cursor=a&b")),
            vec!["b".to_string()],
            "a second pair is a second KEY, not part of the first value"
        );
        assert_eq!(
            unknown_list_params(Some("nope=app")),
            vec!["nope".to_string()],
            "the OFFENDING KEY is what comes back, not the value beside it"
        );
    }

    /// `rollup` is a closed vocabulary of one; a typo is the caller's mistake,
    /// not a silent fall-through to the row list (which would answer a
    /// completely different question with a 200).
    #[test]
    fn rollup_is_a_closed_vocabulary_not_a_fall_through() {
        assert!(!parse_rollup(None).expect("absent is fine"));
        assert!(parse_rollup(Some("status")).expect("the one rollup"));
        for bad in ["Status", "statuses", "state", "", "status "] {
            let Err(err) = parse_rollup(Some(bad)) else {
                panic!("'{bad}' must not parse as a rollup")
            };
            assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);
            assert!(
                err.1.contains("rollup") && err.1.contains("status"),
                "the refusal must name the param and the one legal value: {}",
                err.1
            );
        }
    }

    /// `total` is summed from the very rows the counts come from, so the two
    /// halves of one answer cannot disagree — and a status the window does not
    /// hold is ABSENT rather than an invented zero.
    #[test]
    fn rollup_total_is_summed_from_its_own_counts_and_zeroes_are_not_invented() {
        let body = rollup_body(&[("failed".into(), 3), ("succeeded".into(), 12)]);
        assert_eq!(
            body,
            json!({"total": 15, "counts": {"failed": 3, "succeeded": 12}})
        );
        assert!(
            body["counts"].get("queued").is_none(),
            "an unheld status is absent: {body}"
        );
        assert_eq!(
            rollup_body(&[]),
            json!({"total": 0, "counts": {}}),
            "an empty window is a total of zero over no counts, not an error"
        );
    }
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

#[cfg(test)]
mod control_event_tests {
    use super::requeued_events;
    use uuid::Uuid;

    /// The anti-pattern: `JobEvent::new(id, "", "queued")` on the bulk-retry
    /// path. An app-scoped watcher (`GET /mcp?app=…`, whose `LiveFilter::keep`
    /// compares the event's app exactly) never saw a bulk retry re-queue its
    /// jobs — work restarted, and the surface watching for it stayed silent.
    #[test]
    fn control_events_carry_app_not_blank() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let events = requeued_events(&[(a, "grants-gov".into()), (b, "hackernews".into())]);
        assert_eq!(events.len(), 2, "one event per re-queued job");
        for ev in &events {
            assert!(
                !ev.app.is_empty(),
                "a blank app is invisible to every app-scoped watcher: {ev:?}"
            );
            assert_eq!(ev.status, "queued");
        }
        assert_eq!(events[0].job_id, a);
        assert_eq!(events[0].app, "grants-gov");
        assert_eq!(
            events[1].app, "hackernews",
            "apps are not shared across ids"
        );
        assert!(
            requeued_events(&[]).is_empty(),
            "an empty batch says nothing"
        );
    }
}

#[cfg(test)]
mod budget_tests {
    use super::validate_budget_usd;

    /// Convention guard: the idiom this fix replaced must be EXTINCT, not just
    /// fixed where it was found — `.filter(|b| *b > 0.0)` silently rewrites a
    /// caller's "spend nothing" into "no ceiling", and it already spread once
    /// (jobs door → triggers door) before being caught. Whitespace is stripped
    /// before matching so rustfmt wrapping cannot hide a site (the round-11
    /// chokepoint-guard lesson).
    #[test]
    fn budget_filter_antipattern_is_extinct() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        // Built from parts so this definition cannot match itself in the scan.
        let needle = format!("budget_usd.{}", "filter(|b|*b>0.0)");
        let needle = needle.as_str();
        let mut offenders = Vec::new();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("readable src dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let source = std::fs::read_to_string(&path).expect("readable source");
                    // Comment-stripped view, then whitespace-stripped: doc
                    // comments legitimately QUOTE the anti-pattern (this
                    // module's own docs do), and rustfmt wrapping must not
                    // hide a real site — the round-11 chokepoint-guard lesson,
                    // both halves.
                    let flat: String = source
                        .lines()
                        .filter(|l| !l.trim_start().starts_with("//"))
                        .flat_map(str::split_whitespace)
                        .collect();
                    if flat.contains(needle) {
                        offenders.push(path);
                    }
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "budget_usd may only pass a door through validate_budget_usd — \
             the silent zero-means-unlimited filter is back in: {offenders:?}"
        );
    }

    /// The anti-pattern: `budget_usd: 0.0` filtered away to `None`, which at
    /// this door means NO ceiling — so "spend nothing" enqueued the one job
    /// shape that can spend without limit.
    #[test]
    fn budget_zero_is_rejected_not_unlimited() {
        let err = validate_budget_usd(Some(0.0)).unwrap_err();
        assert!(
            err.contains("positive") && err.contains("NO spend ceiling"),
            "the refusal must say what a zero budget would have meant: {err}"
        );
    }

    /// Same class, other side of zero: a negative ceiling is nonsense, and
    /// nonsense must not decay into "unlimited".
    #[test]
    fn negative_budget_is_rejected_not_unlimited() {
        assert!(validate_budget_usd(Some(-1.5)).is_err());
        // NaN/∞ are not ceilings either — a NaN comparison is false everywhere,
        // so `> 0.0` alone would have let them through as `Some(NaN)`.
        assert!(validate_budget_usd(Some(f64::NAN)).is_err());
        assert!(validate_budget_usd(Some(f64::INFINITY)).is_err());
    }

    /// The two legitimate shapes are untouched: omitted stays "no ceiling"
    /// (this fix narrows nothing), a positive number passes through verbatim.
    #[test]
    fn absent_stays_none_and_positive_passes_through() {
        assert_eq!(validate_budget_usd(None).unwrap(), None);
        assert_eq!(validate_budget_usd(Some(0.25)).unwrap(), Some(0.25));
    }
}

// ---- Spend certainty: the arithmetic the cost surfaces are allowed to do ----

/// The per-event rollup and its summer live in [`pumper_core::costs`], beside
/// `CostSummary` — the per-group rollup that already exposed the identical
/// `uncertain_calls()` / `total_certainty()` pair. Re-exported here because
/// this module is where the ledger's HTTP surfaces are written, and because a
/// `pub(crate)` copy in this file is precisely how the receipt ended up with
/// its own certainty-blind sum.
pub(crate) use pumper_core::costs::{roll_up, CostRollup};

/// The four fields every surface that publishes a cost-ledger total owes its
/// reader: the money we can price, the two counts that make it a floor, and the
/// one word that says which it is.
///
/// A `Map` rather than four `json!` lines at each call site, because four lines
/// copied to a second surface is exactly how `GET /jobs/{id}/costs` and
/// `GET /jobs/{id}/receipt` came to answer differently about the same job.
pub(crate) fn certainty_fields(roll: &CostRollup) -> serde_json::Map<String, Value> {
    let mut out = serde_json::Map::new();
    out.insert("billed_usd".into(), json!(roll.billed_usd));
    out.insert("unmetered_calls".into(), json!(roll.unmetered_calls));
    out.insert("unknown_calls".into(), json!(roll.unknown_calls));
    out.insert("total_certainty".into(), json!(roll.total_certainty()));
    out
}

/// Folds [`certainty_fields`] into an object a surface is about to publish.
/// A non-object is left alone rather than silently replaced.
pub(crate) fn merge_certainty_fields(body: &mut Value, roll: &CostRollup) {
    if let Some(obj) = body.as_object_mut() {
        obj.extend(certainty_fields(roll));
    }
}

/// `(cost_per_fresh_record_usd, cost_per_fresh_record_unavailable)`.
///
/// A job holding any call whose bill we cannot read has **no** $/record figure:
/// the numerator is a floor, so the quotient would be a floor presented as a
/// price. `null` with a named reason is the honest answer, and it is the same
/// discipline `routes::economics` already applies to an unknown denominator —
/// this closes the matching hole in the numerator.
pub(crate) fn fresh_record_cost(
    roll: &CostRollup,
    fresh: Option<u64>,
) -> (Option<f64>, Option<&'static str>) {
    if roll.unmetered_calls > 0 {
        return (None, Some("unmetered_calls"));
    }
    if roll.unknown_calls > 0 {
        return (None, Some("unknown_calls"));
    }
    match fresh {
        Some(n) if n > 0 => (Some(roll.billed_usd / n as f64), None),
        _ => (None, None),
    }
}

/// The whole budget block of `GET /jobs/{id}/costs`, as one pure function of
/// the ceiling, the job's spend and the configured imputation policy.
///
/// It reports the SAME verdict the enforcement point acts on
/// (`pumper_core::costs::budget_verdict`, which `AppContext::remaining_budget_usd`
/// and the Claude-tier downgrade also call) rather than re-deriving headroom
/// here. A report that computes its own answer is a report that will one day
/// tell an operator there is money left while the worker is refusing calls.
///
/// Four keys, and each answers a question the previous shape could not:
///
/// - `budget_remaining_usd` — headroom charged for the calls we could not
///   price, or **`null`** in the unpriceable state. Not `0.0`: zero is a
///   measurement, and this is the absence of one (registry law
///   `nullable-never-zero`).
/// - `budget_imputed_usd` — how much of the charge is an ESTIMATE. The
///   estimate must announce itself; a headroom that silently includes imputed
///   dollars is a number an operator cannot audit.
/// - `unpriceable` — true when the job has calls we could not price and none
///   we could, so its spend has no actionable bound. The fix is "find why the
///   bill is unreadable", never "raise the budget".
/// - `breached` — the ceiling was actually *reached* by charged spend, which is
///   a different fact from `unpriceable` and must not be conflated with it: a
///   blind job has not been shown to have overspent anything.
pub(crate) fn budget_block(
    budget_usd: Option<f64>,
    spend: &JobSpend,
    policy: BudgetImputation,
) -> serde_json::Map<String, Value> {
    let verdict = budget_verdict(budget_usd, *spend, policy);
    let (remaining, unpriceable) = match verdict {
        BudgetVerdict::Unlimited => (None, false),
        BudgetVerdict::Headroom(r) => (Some(r), false),
        BudgetVerdict::Unpriceable { .. } => (None, true),
    };
    let mut out = serde_json::Map::new();
    out.insert("budget_usd".into(), json!(budget_usd));
    out.insert("budget_remaining_usd".into(), json!(remaining));
    out.insert(
        "budget_remaining_certainty".into(),
        json!(headroom_certainty(spend.uncertain_calls)),
    );
    out.insert(
        "budget_imputed_usd".into(),
        json!(spend.imputed_usd(policy)),
    );
    out.insert("budget_imputation".into(), json!(policy.as_str()));
    out.insert("unpriceable".into(), json!(unpriceable));
    out.insert(
        "breached".into(),
        json!(matches!(verdict, BudgetVerdict::Headroom(r) if r <= 0.0)),
    );
    out
}

/// Every place in the server crate that turns `cost_events.cost_usd` into a
/// user-visible number, enumerated — the EXPECTED-diff idiom this repo uses to
/// enforce a convention (`routes/mod.rs`), because a convention stated only in
/// a doc comment is a convention until the next surface is written.
///
/// The rule: a cost total is published with its certainty, and the only code
/// allowed to produce one is `pumper_core::costs::roll_up` (per-event) or a
/// `CostSummary` row (per-group, which carries the counts itself). A new
/// hand-rolled `map(|e| e.cost_usd).sum()` adds a line here and fails the build.
#[cfg(test)]
mod cost_sum_inventory {
    use std::path::{Path, PathBuf};

    /// Sites that sum `cost_usd`, as `"<path>: <line>"`, sorted.
    ///
    /// Every survivor folds summary GROUPS (`CostSummary`,
    /// `PrincipalCostSummary`), not raw events: the group already carries
    /// `unmetered_calls` / `unknown_calls` and the route publishes them beside
    /// the money. Nothing here may sum a `CostEvent`.
    const EXPECTED: &[&str] = &[
        "routes/economics.rs: e.claude_cost_usd += c.cost_usd;",
        "routes/economics.rs: e.cost_usd += c.cost_usd;",
        "routes/jobs.rs: let total: f64 = summary.iter().map(|s| s.cost_usd).sum();",
        "routes/principals.rs: let total: f64 = rows.iter().map(|r| r.cost_usd).sum();",
    ];

    /// Reduces a source file to the code a reader would call *executable*:
    /// `//` line comments and `/* */` blocks removed, and every string
    /// literal's CONTENTS blanked while its quotes stay.
    ///
    /// Both halves are load-bearing. A gate that matches inside comments passes
    /// a file that merely TALKS about the rule above code that breaks it. A gate
    /// that matches inside string literals red-lines on its own error message —
    /// and a gate that treats `"https://x"` as the start of a comment blinds
    /// itself to the rest of the line.
    fn code_only(src: &str) -> String {
        let mut out = String::with_capacity(src.len());
        let mut chars = src.chars().peekable();
        let (mut in_string, mut in_block) = (false, false);
        while let Some(c) = chars.next() {
            if in_block {
                if c == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    in_block = false;
                }
                continue;
            }
            if in_string {
                match c {
                    // A `\n` inside a literal must not become a real newline in
                    // the stripped text, or half a literal lands on its own line.
                    '\\' => {
                        chars.next();
                    }
                    '"' => {
                        in_string = false;
                        out.push(c);
                    }
                    '\n' => out.push(c),
                    _ => {}
                }
                continue;
            }
            match c {
                '"' => {
                    in_string = true;
                    out.push(c);
                }
                '/' if chars.peek() == Some(&'/') => {
                    for c in chars.by_ref() {
                        if c == '\n' {
                            out.push('\n');
                            break;
                        }
                    }
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    in_block = true;
                }
                _ => out.push(c),
            }
        }
        out
    }

    /// Does this (comment-stripped) line accumulate `cost_usd` into a total?
    fn sums_cost_usd(line: &str) -> bool {
        line.contains("cost_usd")
            && (line.contains("+=")
                || line.contains(".sum(")
                || line.contains(".sum::")
                || line.contains(".fold("))
    }

    fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("readable source dir") {
            let path = entry.expect("readable entry").path();
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }

    #[test]
    fn every_cost_total_in_this_crate_goes_through_the_shared_rollup() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        rust_files(&root, &mut files);
        files.sort();

        let mut found: Vec<String> = Vec::new();
        for file in &files {
            let rel = file
                .strip_prefix(&root)
                .expect("under src")
                .to_string_lossy()
                .replace('\\', "/");
            let src = std::fs::read_to_string(file).expect("readable source file");
            for line in code_only(&src).lines() {
                let line = line.trim();
                if sums_cost_usd(line) {
                    found.push(format!("{rel}: {line}"));
                }
            }
        }
        found.sort();

        let expected: Vec<String> = EXPECTED.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            found, expected,
            "a cost total is summed somewhere this inventory does not know about. If it folds \
             CostEvent rows it must call pumper_core::costs::roll_up — a bare \
             `map(|e| e.cost_usd).sum()` drops spend_certainty and republishes a floor as a \
             price. If it folds CostSummary rows (which carry the counts), add it to EXPECTED."
        );
    }

    /// Seeded violation: the matcher must go red on a real hand-rolled sum and
    /// must NOT go red on a comment that merely quotes one, nor on the error
    /// message of this very gate.
    #[test]
    fn the_matcher_reads_code_not_the_comment_above_it_nor_its_own_prose() {
        let talks_about_it = code_only(
            "// let total: f64 = events.iter().map(|e| e.cost_usd).sum();
let x = 1;
",
        );
        assert!(
            !talks_about_it.lines().any(sums_cost_usd),
            "a comment quoting the anti-pattern is not the anti-pattern: {talks_about_it:?}"
        );

        // A block comment spanning the anti-pattern is still not the code.
        let blocked = code_only(
            "/* acc += e.cost_usd; */ let x = 1;
",
        );
        assert!(!blocked.lines().any(sums_cost_usd), "{blocked:?}");

        // Neither is a string literal that names it — which is how this gate
        // avoids flagging its own EXPECTED list and its own failure message.
        let quoted = code_only(
            "let msg = \"acc += e.cost_usd;\";
",
        );
        assert!(!quoted.lines().any(sums_cost_usd), "{quoted:?}");

        // The real thing, with a trailing comment and with a URL on the line:
        // stripping either must not hide the sum.
        let does_it = code_only(
            "let total: f64 = events.iter().map(|e| e.cost_usd).sum(); // honest
",
        );
        assert!(does_it.lines().any(sums_cost_usd), "{does_it:?}");
        let with_url = code_only(
            "log(\"https://x/y\"); let t: f64 = v.iter().map(|e| e.cost_usd).sum();
",
        );
        assert!(with_url.lines().any(sums_cost_usd), "{with_url:?}");
        assert!(code_only("acc += e.cost_usd;").lines().any(sums_cost_usd));
    }
}

#[cfg(test)]
mod cost_certainty_tests {
    use super::*;
    use chrono::Utc;
    use pumper_core::costs::CostEvent;

    fn event(cost_usd: f64, spend_certainty: SpendCertainty) -> CostEvent {
        CostEvent {
            job_id: "j".into(),
            app: "demo".into(),
            engine: "claude".into(),
            url: None,
            cost_usd,
            spend_certainty,
            detail: None,
            created_at: Utc::now(),
        }
    }

    /// THE anti-pattern, stated as a test: the killed $4 run and the free cache
    /// hit used to produce byte-identical payloads. They must now differ in a
    /// field a dashboard, a sum or a filter can reach.
    #[test]
    fn a_killed_paid_run_does_not_read_like_a_free_cache_hit() {
        let killed = roll_up(&[event(0.0, SpendCertainty::Unmetered)]);
        let cached = roll_up(&[event(0.0, SpendCertainty::Free)]);

        assert_eq!(killed.billed_usd, 0.0);
        assert_eq!(cached.billed_usd, 0.0, "both still sum to zero dollars");
        assert_ne!(
            (killed.unmetered_calls, killed.total_certainty()),
            (cached.unmetered_calls, cached.total_certainty()),
            "the two $0 rows are indistinguishable again — the whole defect"
        );
        assert_eq!(
            (killed.unmetered_calls, killed.total_certainty()),
            (1, "at_least")
        );
        assert_eq!(
            (cached.unmetered_calls, cached.total_certainty()),
            (0, "exact")
        );
    }

    /// A failure whose envelope DID report its spend is exact, not a floor —
    /// over-labelling is its own dishonesty.
    #[test]
    fn a_reported_failure_spend_stays_exact() {
        let roll = roll_up(&[event(0.42, SpendCertainty::Billed)]);
        assert_eq!(roll.billed_usd, 0.42);
        assert_eq!(roll.uncertain_calls(), 0);
        assert_eq!(roll.total_certainty(), "exact");
    }

    /// `unmetered` ("you were billed, we cannot read the amount") and `unknown`
    /// ("we cannot even say you were billed") are counted apart, because the
    /// operational follow-up differs.
    #[test]
    fn unmetered_and_unknown_are_counted_apart() {
        let roll = roll_up(&[
            event(0.30, SpendCertainty::Billed),
            event(0.0, SpendCertainty::Unmetered),
            event(0.0, SpendCertainty::Unknown),
            event(0.0, SpendCertainty::Free),
        ]);
        assert_eq!(roll.unmetered_calls, 1);
        assert_eq!(roll.unknown_calls, 1);
        assert_eq!(roll.uncertain_calls(), 2);
        assert!((roll.billed_usd - 0.30).abs() < 1e-12);
    }

    /// 10 fresh records, one $0.30 billed call and one unmetered call: the
    /// yield is `null` with a named reason, NOT a confident $0.03.
    #[test]
    fn roi_is_null_not_wrong_when_a_call_was_unmetered() {
        let roll = roll_up(&[
            event(0.30, SpendCertainty::Billed),
            event(0.0, SpendCertainty::Unmetered),
        ]);
        assert_eq!(
            fresh_record_cost(&roll, Some(10)),
            (None, Some("unmetered_calls")),
            "$0.03/record would be a price derived from a floor"
        );
        // An `unknown` call blocks it too, under its own name.
        let roll = roll_up(&[
            event(0.30, SpendCertainty::Billed),
            event(0.0, SpendCertainty::Unknown),
        ]);
        assert_eq!(
            fresh_record_cost(&roll, Some(10)),
            (None, Some("unknown_calls"))
        );
    }

    /// The guard half: a fully-priced job still publishes its $/record, and no
    /// reason string appears to explain an absence that is not there.
    #[test]
    fn roi_still_divides_when_every_call_is_priced() {
        let roll = roll_up(&[event(0.30, SpendCertainty::Billed)]);
        assert_eq!(fresh_record_cost(&roll, Some(10)), (Some(0.03), None));
        // Zero / unknown fresh records stay `null` with NO reason — that is the
        // pre-existing "undefined denominator" case, not a certainty problem.
        assert_eq!(fresh_record_cost(&roll, Some(0)), (None, None));
        assert_eq!(fresh_record_cost(&roll, None), (None, None));
    }

    /// Headroom is CHARGED for the calls we could not price, and is labelled an
    /// upper bound exactly when a floor went into it.
    ///
    /// **This assertion was inverted, on purpose.** It used to pin
    /// `Some(0.70)` for a job with an unreadable bill with the comment "the
    /// ceiling arithmetic is deliberately UNCHANGED" — that WAS the defect, and
    /// the guard made it a promise. The new expression is pinned here and the
    /// old one is explicitly forbidden, so the blind answer cannot come back
    /// quietly under either policy.
    #[test]
    fn headroom_is_labelled_an_upper_bound_when_spend_is_a_floor() {
        let certain = JobSpend::from_events(&[event(0.30, SpendCertainty::Billed)]);
        let block = budget_block(Some(1.0), &certain, BudgetImputation::WindowMean);
        assert_eq!(block["budget_remaining_usd"], json!(0.70));
        assert_eq!(block["budget_remaining_certainty"], json!("exact"));
        assert_eq!(block["budget_imputed_usd"], json!(0.0));
        assert_eq!(block["unpriceable"], json!(false));
        assert_eq!(block["breached"], json!(false));

        let floored = JobSpend::from_events(&[
            event(0.30, SpendCertainty::Billed),
            event(0.0, SpendCertainty::Unmetered),
        ]);
        let block = budget_block(Some(1.0), &floored, BudgetImputation::WindowMean);
        assert_eq!(
            block["budget_remaining_usd"],
            json!(0.40),
            "the unmetered call is charged the job's own $0.30 mean"
        );
        assert_ne!(
            block["budget_remaining_usd"], json!(0.70),
            "FORBIDDEN: $0.70 is the blind answer that read an unreadable bill              as free money"
        );
        assert_eq!(
            block["budget_imputed_usd"],
            json!(0.30),
            "the estimate announces itself"
        );
        assert_eq!(
            block["budget_remaining_certainty"],
            json!("at_most"),
            "a headroom derived from a floor is an upper bound, and must say so"
        );

        // `off` is the no-deploy rollback and must restore the old number here.
        let block = budget_block(Some(1.0), &floored, BudgetImputation::Off);
        assert_eq!(block["budget_remaining_usd"], json!(0.70));
        assert_eq!(block["budget_imputed_usd"], json!(0.0));
    }

    /// A job with unreadable bills and NO readable one has no headroom anybody
    /// can act on: `null`, not `0.0` (that is a measurement) and certainly not
    /// the untouched budget. `breached` stays false — a blind job has not been
    /// shown to have overspent anything.
    #[test]
    fn an_unpriceable_job_reports_no_headroom_rather_than_its_whole_budget() {
        let blind = JobSpend::from_events(&[
            event(0.0, SpendCertainty::Unmetered),
            event(0.0, SpendCertainty::Unknown),
        ]);
        let block = budget_block(Some(1.0), &blind, BudgetImputation::WindowMean);
        assert_eq!(block["unpriceable"], json!(true));
        assert_eq!(block["budget_remaining_usd"], Value::Null);
        assert_ne!(
            block["budget_remaining_usd"],
            json!(1.0),
            "FORBIDDEN: the platform's blindness presented as the job's headroom"
        );
        assert_eq!(block["budget_imputed_usd"], Value::Null);
        assert_eq!(block["breached"], json!(false));

        // Unbudgeted: nothing to be blind about, so nothing to flag.
        let block = budget_block(None, &blind, BudgetImputation::WindowMean);
        assert_eq!(block["unpriceable"], json!(false));
        assert_eq!(block["budget_remaining_usd"], Value::Null);

        // `off` admits it against its $0 total — today's behaviour, kept.
        let block = budget_block(Some(1.0), &blind, BudgetImputation::Off);
        assert_eq!(block["unpriceable"], json!(false));
        assert_eq!(block["budget_remaining_usd"], json!(1.0));
    }

    /// No ceiling means unlimited, not exhausted; overspend floors at zero
    /// rather than reporting negative headroom, and THAT is `breached`.
    #[test]
    fn no_ceiling_is_not_zero_headroom() {
        let spend = JobSpend {
            billed_usd: 5.0,
            billed_calls: 1,
            uncertain_calls: 0,
        };
        let on = BudgetImputation::WindowMean;
        assert_eq!(
            budget_block(None, &spend, on)["budget_remaining_usd"],
            Value::Null
        );
        assert_eq!(budget_block(None, &spend, on)["breached"], json!(false));
        assert_eq!(
            budget_block(Some(1.0), &spend, on)["budget_remaining_usd"],
            json!(0.0)
        );
        assert_eq!(budget_block(Some(1.0), &spend, on)["breached"], json!(true));
        // A non-finite ceiling has never been a ceiling.
        assert_eq!(
            budget_block(Some(f64::NAN), &spend, on)["budget_remaining_usd"],
            Value::Null
        );
    }

    /// The report and the ENFORCEMENT point answer from one function. If this
    /// drifts, `GET /jobs/{id}/costs` tells an operator there is money left
    /// while the worker is refusing the next call.
    #[test]
    fn the_report_and_the_ceiling_cannot_disagree() {
        let cases = [
            JobSpend::default(),
            JobSpend::from_events(&[event(0.40, SpendCertainty::Billed)]),
            JobSpend::from_events(&[
                event(0.40, SpendCertainty::Billed),
                event(0.0, SpendCertainty::Unmetered),
            ]),
            JobSpend::from_events(&[event(0.0, SpendCertainty::Unknown)]),
        ];
        for policy in [BudgetImputation::WindowMean, BudgetImputation::Off] {
            for spend in &cases {
                let block = budget_block(Some(1.0), spend, policy);
                let expected = match budget_verdict(Some(1.0), *spend, policy) {
                    BudgetVerdict::Headroom(r) => json!(r),
                    _ => Value::Null,
                };
                assert_eq!(
                    block["budget_remaining_usd"], expected,
                    "report disagreed with the verdict for {spend:?} under {policy:?}"
                );
            }
        }
    }

    /// An unknown `?certainty=` is a 400 naming the vocabulary — never a
    /// reassuring empty page.
    #[test]
    fn an_unknown_certainty_filter_is_rejected_not_silently_empty() {
        assert_eq!(parse_certainty(None).unwrap(), None);
        assert_eq!(
            parse_certainty(Some("unmetered")).unwrap(),
            Some(SpendCertainty::Unmetered)
        );
        let err = parse_certainty(Some("maybe")).expect_err("must not be accepted");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(
            err.1.contains("unmetered"),
            "the 400 names the vocabulary: {}",
            err.1
        );
    }
}
