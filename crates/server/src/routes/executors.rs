//! N18 — the executor plane's HTTP doors, coordinator side.
//!
//! Five POSTs an outbound executor drives and one GET an operator reads:
//!
//! | route | what it does |
//! | --- | --- |
//! | `POST /executors/claim` | long-poll for one job (≤ `[executors] claim_wait_secs`), `204` when the queue has nothing this executor may run |
//! | `POST /jobs/{id}/heartbeat` | refresh the lease; the answer says whether the executor still owns the job |
//! | `POST /jobs/{id}/checkpoint` | persist the running job's resumable state |
//! | `POST /jobs/{id}/progress` | publish a live progress snapshot |
//! | `POST /jobs/{id}/finish` | report the outcome; the coordinator runs the whole post-run fan-out |
//! | `GET /executors` | who is out there, what they can run, what they are doing |
//!
//! **Three guards, in this order, on every one of them:**
//! 1. `[executors] enabled` + a non-blank `secret` — otherwise `404`. A disabled
//!    plane does not exist as far as a caller is concerned; there is no
//!    "configured but open" state.
//! 2. The shared secret in [`EXECUTOR_SECRET_HEADER`], compared as SHA-256
//!    digests (`crate::executors::secret_matches`) exactly as the remote fetch
//!    fabric compares its own.
//! 3. In `[auth] mode = "keys"`, the `admin` scope — for free, because
//!    `auth::required_scope` classifies every unknown mutating path as `Admin`
//!    and `GET /executors` as `Read`. The plane secret and the principal key are
//!    deliberately two different credentials in two different headers, so
//!    "wrong secret" and "wrong key" stay two distinguishable `401`s.
//!
//! **And one fence under all of them.** Every write names `(job id, attempt,
//! executor_id)` and is refused unless the row still says
//! `status='running' AND attempts = ? AND executor_id = ?`
//! ([`pumper_core::Storage::job_claimed_by`]). This is what makes an executor's
//! death cheap and its *resurrection* harmless: the reaper re-queues the job on
//! its stale heartbeat exactly as it does for a local task, the next claim
//! advances `attempts`, and the late `finish` from the process everyone thought
//! was dead matches no row and is refused with a `409` instead of overwriting a
//! run that has already moved on.

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use pumper_core::config::EXECUTOR_SECRET_HEADER;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::executors::{age_secs, blocked_over_cap, eligible_apps, executor_state, secret_matches};
use crate::routes::error::ApiError;
use crate::state::AppState;

/// The three guards, applied once. `Ok(())` means this request may act.
fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let cfg = &state.config.executors;
    if !cfg.servable() {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "the executor plane is disabled on this node ([executors] enabled)".into(),
        ));
    }
    let presented = headers
        .get(EXECUTOR_SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !secret_matches(presented, &cfg.secret) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            format!("missing or invalid {EXECUTOR_SECRET_HEADER} header"),
        ));
    }
    Ok(())
}

/// What every executor-driven write names: the job's attempt and the executor
/// that claims to hold it. Both are required — the attempt is the fence the
/// local worker already lives under, and the executor id is what makes a
/// *reaped* process's late write refusable rather than merely unlucky.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct ExecutorWrite {
    /// The executor reporting. Must equal `jobs.executor_id` for this row.
    executor_id: String,
    /// The attempt this executor was handed. Must equal `jobs.attempts`.
    attempt: i64,
    /// `checkpoint` / `progress`: the payload. Ignored by `heartbeat`.
    #[serde(default)]
    state: Value,
    /// `finish`: the job result, when the run succeeded.
    #[serde(default)]
    result: Option<Value>,
    /// `finish`: the failure, when it did not. Exactly one of `result`/`error`.
    #[serde(default)]
    error: Option<String>,
    /// `finish`: how long the app's own `run()` took on the executor, so the
    /// coordinator's stage row is not silently missing the only span it cannot
    /// measure. Optional; absent means unmeasured, never zero.
    #[serde(default)]
    run_ms: Option<i64>,
}

/// The one refusal an executor gets when the fence says the job is no longer
/// its business. Extracted so every door refuses with the same status and the
/// same sentence, whichever of them noticed first.
fn not_owned(id: Uuid, w: &ExecutorWrite) -> ApiError {
    ApiError(
        StatusCode::CONFLICT,
        format!(
            "job {id} is not running as attempt {} on executor '{}' — it was reset, reaped, \
             cancelled or already finished, and another attempt now owns the outcome",
            w.attempt, w.executor_id
        ),
    )
}

/// The fence, applied at the door. `Err` carries the `409` a refused write gets.
async fn owned(state: &AppState, id: Uuid, w: &ExecutorWrite) -> Result<(), ApiError> {
    let ok = state
        .storage
        .job_claimed_by(id, w.attempt, &w.executor_id)
        .await?;
    if ok {
        return Ok(());
    }
    Err(not_owned(id, w))
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct ClaimBody {
    /// This executor's stable identity. Stamped onto the claimed row and
    /// required by every later write about it.
    executor_id: String,
    /// The apps this executor offers to run. Empty = "whatever you'll give me".
    /// Always intersected with the coordinator's executor-eligible set, so it
    /// can only narrow.
    #[serde(default)]
    capabilities: Vec<String>,
}

/// One claimed job, as the executor needs it: everything `AppContext`
/// construction takes, and nothing the coordinator keeps to itself (no callback
/// secret, no principal, no resumed-input on a job that never parked).
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ClaimedJob {
    job_id: Uuid,
    app: String,
    params: Value,
    /// The attempt number this claim advanced to — the fence value for every
    /// write about this job.
    attempt: i64,
    /// The budget the coordinator resolved (including a DataHub `cost:pause`
    /// forcing `$0`), not the raw enqueue value.
    budget_usd: Option<f64>,
    /// The last durable checkpoint, if the job has one, run through the same
    /// poisoned-checkpoint escape the local worker applies.
    restored: Option<Value>,
    /// The answer a `POST /jobs/{id}/resume` stored for a previously parked job.
    resumed_input: Option<Value>,
}

/// Long-poll for one job this executor may run.
///
/// **The poll is the clock and the backpressure boundary.** An executor asks
/// only when it has a free slot, so the coordinator never has to model remote
/// capacity: there is no push, no queue-per-executor and no lease to hand out
/// speculatively. An empty queue is held open for `[executors] claim_wait_secs`
/// (default 30) and then answered `204`, which keeps an idle fleet at one
/// request per executor per 30s instead of one per poll interval.
#[utoipa::path(
    post,
    path = "/executors/claim",
    tag = "executors",
    request_body = ClaimBody,
    responses(
        (status = 200, description = "A claimed job", body = ClaimedJob),
        (status = 204, description = "Nothing to run for this executor right now"),
        (status = 401, description = "Missing or wrong `x-pumper-executor-secret`"),
        (status = 404, description = "`[executors]` disabled on this node"),
    )
)]
pub(crate) async fn claim_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ClaimBody>,
) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse;
    authorize(&state, &headers)?;
    if body.executor_id.trim().is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "executor_id is required — an anonymous executor cannot be fenced, reaped or \
             reported on"
                .into(),
        ));
    }
    let eligible = eligible_apps(&state.registry, &body.capabilities);
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(state.config.executors.claim_wait_secs.max(1));
    let claimed = loop {
        // Cluster-wide caps, from the DB. The worker's in-memory map counts one
        // process's jobs; with N executors it admitted N× the configured cap.
        let counts = state.storage.executor_running_counts().await?;
        let blocked = blocked_over_cap(&counts, |app| crate::worker::app_limit(&state, app));
        let job = state
            .storage
            .claim_next_for_executor(
                &body.executor_id,
                &eligible,
                &blocked,
                state.config.worker.priority_aging_coefficient_secs,
            )
            .await?;
        if job.is_some() || tokio::time::Instant::now() >= deadline {
            break job;
        }
        // Sleep until an enqueue wakes us, the long poll expires, or the node
        // shuts down. Same `notify` the local worker waits on, so a job posted
        // one millisecond ago does not wait out the poll interval.
        tokio::select! {
            _ = state.shutdown.cancelled() => break None,
            _ = state.notify.notified() => {}
            _ = tokio::time::sleep_until(deadline) => {}
        }
    };
    // Recorded on EVERY poll, claim or not: "idle but alive" and "gone" are
    // different facts and `GET /executors` must be able to tell them apart.
    if let Err(e) = state
        .storage
        .record_executor_poll(&body.executor_id, &body.capabilities, claimed.is_some())
        .await
    {
        tracing::warn!(executor = %body.executor_id, "executor poll record failed: {e}");
    }
    let Some(job) = claimed else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    // The SAME checkpoint hand-out the local worker performs, including the
    // poisoned-checkpoint escape — a remote attempt must not be handed a blob
    // that has already killed `max_resume_failures` attempts.
    let restored = crate::worker::load_restore(&state, &job).await;
    let budget_usd = crate::datahub::effective_budget(&state, &job.app, job.budget_usd);
    tracing::info!(
        job = %job.id, app = %job.app, attempt = job.attempts,
        executor = %body.executor_id, "job claimed by executor"
    );
    crate::worker::publish_running(&state, &job);
    Ok(Json(ClaimedJob {
        job_id: job.id,
        app: job.app,
        params: job.params,
        attempt: job.attempts,
        budget_usd,
        restored,
        resumed_input: job.resumed_input,
    })
    .into_response())
}

/// Refreshes a remote job's lease.
///
/// The answer's `owned` field is the executor's **stop signal**: `false` means
/// the coordinator no longer considers this executor the owner (reaped, reset,
/// cancelled, or finished by someone else), and the executor must abandon the
/// run rather than keep spending on a result that will be refused.
#[utoipa::path(
    post,
    path = "/jobs/{id}/heartbeat",
    tag = "executors",
    params(("id" = Uuid, Path, description = "Job id")),
    request_body = ExecutorWrite,
    responses(
        (status = 200, description = "`{owned: true}` — lease refreshed", body = Object),
        (status = 401, description = "Missing or wrong `x-pumper-executor-secret`"),
        (status = 404, description = "`[executors]` disabled on this node"),
        (status = 409, description = "`{owned: false}` — this executor no longer holds the job", body = Object),
    )
)]
pub(crate) async fn executor_heartbeat(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<ExecutorWrite>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let ok = state
        .storage
        .heartbeat_from(id, body.attempt, &body.executor_id)
        .await?;
    if !ok {
        // Deliberately the same 409 the other writes get, with the same prose:
        // one refusal shape for "you do not own this job", whichever door found
        // out first. The fenced UPDATE above IS the ownership check, so this
        // costs no extra round trip on the healthy path.
        return Err(not_owned(id, &body));
    }
    Ok(Json(json!({ "owned": true })))
}

/// Persists a remote job's resumable state, through the same
/// attempts-lineage-fenced write the local `ctx.checkpoint()` uses.
#[utoipa::path(
    post,
    path = "/jobs/{id}/checkpoint",
    tag = "executors",
    params(("id" = Uuid, Path, description = "Job id")),
    request_body = ExecutorWrite,
    responses(
        (status = 200, description = "`{saved: true}`", body = Object),
        (status = 401, description = "Missing or wrong `x-pumper-executor-secret`"),
        (status = 404, description = "`[executors]` disabled on this node"),
        (status = 409, description = "This executor no longer holds the job", body = Object),
    )
)]
pub(crate) async fn executor_checkpoint(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<ExecutorWrite>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    owned(&state, id, &body).await?;
    let saved = state
        .storage
        .save_checkpoint(id, body.attempt, &body.state)
        .await?;
    Ok(Json(json!({ "saved": saved })))
}

/// Publishes a remote job's live progress snapshot — the same store and the same
/// `progress` SSE event a local run produces, so `GET /jobs/{id}` and
/// `/jobs/{id}/stream` cannot tell where the job is running.
#[utoipa::path(
    post,
    path = "/jobs/{id}/progress",
    tag = "executors",
    params(("id" = Uuid, Path, description = "Job id")),
    request_body = ExecutorWrite,
    responses(
        (status = 200, description = "`{reported: true}`", body = Object),
        (status = 401, description = "Missing or wrong `x-pumper-executor-secret`"),
        (status = 404, description = "`[executors]` disabled on this node"),
        (status = 409, description = "This executor no longer holds the job", body = Object),
    )
)]
pub(crate) async fn executor_progress(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<ExecutorWrite>,
) -> Result<Json<Value>, ApiError> {
    use pumper_core::ProgressReporter;
    authorize(&state, &headers)?;
    owned(&state, id, &body).await?;
    let app = state
        .storage
        .get(id)
        .await?
        .map(|j| j.app)
        .unwrap_or_default();
    // A fresh reporter per POST: the *executor* owns the throttle (it is the
    // side that knows how chatty its app is), so the coordinator persists and
    // emits every snapshot it is actually sent rather than throttling twice.
    state
        .progress
        .reporter(id, app, state.events.clone())
        .report(body.state.clone());
    Ok(Json(json!({ "reported": true })))
}

/// Reports a remote job's outcome. **The coordinator finalizes.**
///
/// Everything after this point — the completion write, search indexing, the
/// health and contract gates, watches, dataset triggers, saved searches, the
/// terminal event, the result webhook — runs here, exactly as it does for a
/// locally-executed job, through the same `finalize_fanout`. That is the whole
/// architectural claim of N18: only `execute` moves.
#[utoipa::path(
    post,
    path = "/jobs/{id}/finish",
    tag = "executors",
    params(("id" = Uuid, Path, description = "Job id")),
    request_body = ExecutorWrite,
    responses(
        (status = 200, description = "`{outcome: \"succeeded\"|\"queued\"|\"failed\"}`", body = Object),
        (status = 400, description = "Neither (or both) of `result` / `error`", body = Object),
        (status = 401, description = "Missing or wrong `x-pumper-executor-secret`"),
        (status = 404, description = "`[executors]` disabled on this node"),
        (status = 409, description = "This executor no longer holds the job — the report is \
            refused rather than overwriting the attempt that does", body = Object),
    )
)]
pub(crate) async fn executor_finish(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<ExecutorWrite>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let outcome = match (body.result.clone(), body.error.clone()) {
        (Some(result), None) => Ok(result),
        (None, Some(error)) => Err(error),
        _ => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "exactly one of `result` or `error` must be present — a finish that says \
                 neither (or both) has not reported an outcome"
                    .into(),
            ))
        }
    };
    owned(&state, id, &body).await?;
    let Some(job) = state.storage.get(id).await? else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("job {id} not found"),
        ));
    };
    let outcome = crate::worker::finish_from_executor(&state, job, outcome, body.run_ms).await;
    Ok(Json(json!({ "outcome": outcome })))
}

/// Who is out there: every executor the coordinator has seen, what it declared
/// it can run, when it last polled, and what it is running now.
#[utoipa::path(
    get,
    path = "/executors",
    tag = "executors",
    responses(
        (status = 200, description = "`{executors: [{id, state, capabilities, running, \
            claimed_total, last_poll_at, last_poll_age_secs, first_seen_at}], eligible_apps}`",
            body = Object),
        (status = 401, description = "Missing or wrong `x-pumper-executor-secret`"),
        (status = 404, description = "`[executors]` disabled on this node"),
    )
)]
pub(crate) async fn list_executors(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let now = chrono::Utc::now();
    let rows = state.storage.list_executors().await?;
    let offline_after = state.config.executors.offline_after_secs;
    let executors: Vec<Value> = rows
        .iter()
        .map(|e| {
            let age = age_secs(&e.last_poll_at, now);
            json!({
                "id": e.id,
                "state": executor_state(age, e.running, offline_after),
                "capabilities": e.capabilities,
                "running": e.running,
                "claimed_total": e.claimed_total,
                "last_poll_at": e.last_poll_at,
                "last_poll_age_secs": age,
                "first_seen_at": e.first_seen_at,
            })
        })
        .collect();
    Ok(Json(json!({
        "executors": executors,
        // What this coordinator would hand out at all — the honest companion to
        // the list above, since an executor declaring a capability the cluster
        // does not consider eligible would otherwise look idle for no visible
        // reason.
        "eligible_apps": eligible_apps(&state.registry, &[]),
    })))
}
