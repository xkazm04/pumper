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
        (status = 202, description = "Job enqueued", body = Object),
        (status = 200, description = "Idempotency-Key replay: the original job", body = Object),
        (status = 404, description = "Unknown app", body = Object),
        (status = 409, description = "The name is a discovered dynamic WASM app (`GET /apps` lists it with `dynamic: true, runnable: false`) — listed but not runnable in this build; the message carries the reason", body = Object),
        (status = 422, description = "Merged params fail the app's declared JSON Schema (message carries JSON-pointer paths), or `budget_usd` is not a positive number of dollars", body = Object),
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
        (status = 200, description = "Cancelled (`{cancelled: true}`; `running: true` when it was in-flight). During a graceful shutdown a run that already committed to a checkpoint suspend answers `{cancelled: false, running: true, suspended: true, note}` — it was re-queued, not cancelled."),
        (status = 404, description = "Job not found", body = Object),
        (status = 409, description = "Job already terminal (succeeded/failed/cancelled)", body = Object),
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
