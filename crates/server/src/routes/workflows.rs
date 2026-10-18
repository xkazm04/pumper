//! Workflow runs (N03): the HTTP surface over `crate::workflow`.
//!
//! A workflow is a *declared plan* — a named DAG of steps, each an ordinary job
//! — as opposed to a trigger, which is a standing reactive edge. The engine
//! (barriers, templating, budget envelope) lives in `crate::workflow`; this file
//! is the door: validate at create, open runs, report them, cancel them.
//!
//! **Validation happens where the facts are.** A step's `app` and its literal
//! params are checked at create through the same `validate_app_params` every
//! other door uses, so a typo is a 422 with pointer paths rather than a run that
//! fails minutes later. A step whose params carry `{{steps.…}}` templates cannot
//! be schema-checked until it is rendered — the create response says so per step
//! (`params_validated: false`) instead of implying a check that did not happen.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::auth::CallerPrincipal;
use crate::routes::error::{default_limit, ApiError};
use crate::state::AppState;
use crate::workflow;

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateWorkflowBody {
    /// Unique, human-chosen name. Also accepted wherever an id is, so callers
    /// can `POST /workflows/nightly-refresh/runs`.
    name: String,
    /// `{steps: {name: {app, params?, after?, budget_usd?, priority?,
    /// max_attempts?}}, budget_usd?, on_failure?}`.
    spec: Value,
    /// Optional 5/6-field cron. Present = the scheduler fires a run per firing,
    /// held while the newest run of this plan is still open.
    cron: Option<String>,
}

#[utoipa::path(
    post,
    path = "/workflows",
    tag = "workflows",
    request_body = CreateWorkflowBody,
    responses(
        (status = 201, description = "Created. `steps[]` reports, per step, whether its params could be schema-validated now (`params_validated: false` = the step is templated and is validated when it is rendered)", body = crate::routes::dto::WorkflowCreated),
        (status = 409, description = "The name is taken", body = crate::routes::dto::ErrorEnvelope),
        (status = 422, description = "The spec does not validate: an unknown step in an `after` barrier, a cycle, an unregistered app, params failing the app's schema (pointer paths), a non-positive budget, or step budgets summing past the run envelope", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn create_workflow(
    State(state): State<AppState>,
    Json(body): Json<CreateWorkflowBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let name = body.name.trim().to_string();
    if name.is_empty() {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "name: required, and may not be blank".into(),
        ));
    }
    let spec = workflow::parse_spec(&body.spec)
        .map_err(|errs| ApiError(StatusCode::UNPROCESSABLE_ENTITY, errs.join("; ")))?;
    if let Some(cron) = body.cron.as_deref() {
        <cron::Schedule as std::str::FromStr>::from_str(cron).map_err(|e| {
            ApiError(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("cron: '{cron}' is not a valid cron expression ({e})"),
            )
        })?;
    }
    // Per-step door validation, the same one the enqueue path runs. An
    // unregistered app is always checkable; params only when they carry no
    // template.
    let mut step_report = Vec::with_capacity(spec.steps.len());
    for (step_name, step) in &spec.steps {
        let Some(app) = state.registry.get(&step.app) else {
            return Err(ApiError(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "spec/steps/{step_name}/app: '{}' is not a registered app on this node",
                    step.app
                ),
            ));
        };
        let templated = workflow::has_template(&step.params);
        if !templated {
            let merged = super::merge_params(app.default_params(), Some(step.params.clone()));
            if let Err(msg) = crate::mcp::validate_app_params(&state.registry, &step.app, &merged) {
                return Err(ApiError(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!("spec/steps/{step_name}/params: {msg}"),
                ));
            }
        }
        step_report.push(json!({
            "step": step_name,
            "app": step.app,
            "after": step.after,
            // Honest about which check actually ran.
            "params_validated": !templated,
        }));
    }
    if state.storage.get_workflow(&name).await?.is_some() {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!("a workflow named '{name}' already exists"),
        ));
    }
    let def = state
        .storage
        .create_workflow(&name, &body.spec, body.cron.as_deref())
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "workflow": def, "steps": step_report })),
    ))
}

#[utoipa::path(
    get,
    path = "/workflows",
    tag = "workflows",
    responses((status = 200, description = "Every declared plan", body = crate::routes::dto::WorkflowListResponse))
)]
pub(crate) async fn list_workflows(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let defs = state.storage.list_workflows().await?;
    Ok(Json(json!({ "workflows": defs })))
}

#[utoipa::path(
    get,
    path = "/workflows/{id}",
    tag = "workflows",
    params(("id" = String, Path, description = "Workflow id or name")),
    responses(
        (status = 200, description = "The plan", body = crate::routes::dto::WorkflowResponse),
        (status = 404, description = "Unknown workflow", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn get_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let def = load(&state, &id).await?;
    Ok(Json(json!({ "workflow": def })))
}

#[utoipa::path(
    delete,
    path = "/workflows/{id}",
    tag = "workflows",
    params(("id" = String, Path, description = "Workflow id or name")),
    responses(
        (status = 200, description = "`{deleted}`. Runs already open are NOT cancelled — they finish against the spec they started with, then report the plan as gone", body = crate::routes::dto::DeletedResponse),
        (status = 404, description = "Unknown workflow", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn delete_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let def = load(&state, &id).await?;
    let deleted = state.storage.delete_workflow(&def.id).await?;
    Ok(Json(json!({ "deleted": deleted })))
}

#[derive(Deserialize, Default, ToSchema)]
pub(crate) struct StartRunBody {
    /// Envelope for this run, overriding the spec's own `budget_usd`. Each
    /// step's ceiling is clamped to what is left of it, so the steps can never
    /// collectively spend more than this.
    budget_usd: Option<f64>,
    /// Dedup key: a replayed start returns the original run (200) instead of a
    /// second execution. The `Idempotency-Key` header takes precedence.
    idempotency_key: Option<String>,
}

#[utoipa::path(
    post,
    path = "/workflows/{id}/runs",
    tag = "workflows",
    params(("id" = String, Path, description = "Workflow id or name")),
    request_body = StartRunBody,
    responses(
        (status = 202, description = "Run opened; its root steps are enqueued", body = crate::routes::dto::WorkflowRunStarted),
        (status = 200, description = "Idempotency-Key replay: the original run", body = crate::routes::dto::WorkflowRunStarted),
        (status = 404, description = "Unknown workflow", body = crate::routes::dto::ErrorEnvelope),
        (status = 422, description = "`budget_usd` is not a positive number, or the stored spec no longer validates", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn start_workflow_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
    caller: Option<axum::Extension<CallerPrincipal>>,
    body: Option<Json<StartRunBody>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let def = load(&state, &id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let budget_usd = super::jobs::validate_budget_usd(body.budget_usd)
        .map_err(|msg| ApiError(StatusCode::UNPROCESSABLE_ENTITY, msg))?;
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .or(body.idempotency_key)
        .filter(|k| !k.trim().is_empty());
    // The caller the identity layer resolved (N20). Every step job of this run
    // inherits it, so `GET /costs?principal=` can price a whole plan to whoever
    // asked for it — `stored_id()` is `None` for the synthetic `open`-mode
    // operator, which keeps "unattributed" honest.
    let principal = caller.and_then(|axum::Extension(c)| c.stored_id().map(str::to_string));
    let (run, created) = workflow::start_run(
        &state,
        &def,
        budget_usd,
        idempotency_key.as_deref(),
        principal.as_deref(),
    )
    .await
    .map_err(|e| ApiError(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;
    let status = if created {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(json!({ "run": run, "created": created }))))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct RunsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
}

#[utoipa::path(
    get,
    path = "/workflows/{id}/runs",
    tag = "workflows",
    params(("id" = String, Path, description = "Workflow id or name"), RunsQuery),
    responses(
        (status = 200, description = "This plan's runs, newest first", body = crate::routes::dto::WorkflowRunListResponse),
        (status = 404, description = "Unknown workflow", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn list_workflow_runs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<RunsQuery>,
) -> Result<Json<Value>, ApiError> {
    let def = load(&state, &id).await?;
    let runs = state
        .storage
        .list_workflow_runs(&def.id, query.limit)
        .await?;
    Ok(Json(json!({ "runs": runs })))
}

#[utoipa::path(
    get,
    path = "/workflow-runs/{run_id}",
    tag = "workflows",
    params(("run_id" = String, Path, description = "Workflow run id")),
    responses(
        (status = 200, description = "`{run, workflow, steps, receipt, unknown}` — the step matrix plus one rolled-up receipt: cost summed from `cost_events` over the run's job set, yield from `job_yield`. A step that never became a job has `cost_usd: null`, not `$0`", body = crate::routes::dto::WorkflowRunReport),
        (status = 404, description = "Unknown run", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn get_workflow_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    workflow::run_report(&state, &run_id)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map(Json)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "workflow run not found".into()))
}

#[utoipa::path(
    delete,
    path = "/workflow-runs/{run_id}",
    tag = "workflows",
    params(("run_id" = String, Path, description = "Workflow run id")),
    responses(
        (status = 200, description = "`{cancelled, jobs_cancelled}` — every open step is closed and each one that already had a job goes through the ordinary `DELETE /jobs/{id}` door", body = crate::routes::dto::WorkflowRunCancelled),
        (status = 404, description = "Unknown run", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn cancel_workflow_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.get_workflow_run(&run_id).await?.is_none() {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "workflow run not found".into(),
        ));
    }
    let jobs_cancelled = workflow::cancel_run(&state, &run_id)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(
        json!({ "cancelled": true, "jobs_cancelled": jobs_cancelled }),
    ))
}

/// A plan by id or name, or a 404 that names what was asked for.
async fn load(state: &AppState, id_or_name: &str) -> Result<pumper_core::WorkflowDef, ApiError> {
    state
        .storage
        .get_workflow(id_or_name)
        .await?
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_FOUND,
                format!("unknown workflow '{id_or_name}'"),
            )
        })
}
