//! Cron schedules: list (health-enriched), create (with cron/timezone/misfire
//! validation), delete, and enable/disable.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::Schedule;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::routes::error::{
    default_limit, keyset_cursor, parse_cursor, ApiError, EnabledBody, MAX_ATTEMPTS_CAP,
};
// The one budget floor, shared with the jobs and trigger doors: `budget_usd`
// must be a real positive ceiling, because an omitted one means "no ceiling"
// and so `0` cannot also mean "spend nothing".
use crate::routes::jobs::validate_budget_usd;
use crate::state::AppState;

#[derive(Deserialize, IntoParams)]
pub(crate) struct SchedulesQuery {
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
    responses((status = 200, description = "Dual-mode: bare `[Schedule]` array, or `{items, next_cursor}` when `cursor` is present. Each schedule is enriched with `next_run` (computed next firing), `last_job_id` / `last_status` (its most recent run), and `health` (`ok` | `disabled` | `invalid_cron` | `unregistered_app` | `invalid_params` | `overlapping`) — so a silently-wedged schedule is visible over the API. `last_run` means *a job was enqueued* and is null until one was; firings eaten by `misfire_policy = \"skip\"` are reported separately as `last_skipped_at` + `skipped_count` (cumulative), so the two can no longer contradict each other. `budget_usd` is the spend ceiling replayed into every run this schedule enqueues (`null` = no ceiling)."))
)]
pub(crate) async fn list_schedules(
    State(state): State<AppState>,
    Query(query): Query<SchedulesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        // Legacy bare-array mode is still capped: an uncursored list must not
        // stream an entire table.
        let items = state.storage.list_schedules_page(None, limit).await?;
        let enriched = enrich_schedules(&state, items).await?;
        return Ok(Json(json!(enriched)));
    };
    let after = parse_cursor(cursor);
    let items = state.storage.list_schedules_page(after, limit).await?;
    let next_cursor = keyset_cursor(&items, limit, |s| {
        format!("{}|{}", pumper_core::datasets::ts(s.created_at), s.id)
    });
    let enriched = enrich_schedules(&state, items).await?;
    Ok(Json(
        json!({ "items": enriched, "next_cursor": next_cursor }),
    ))
}

/// Enriches each schedule with the observability fields the raw row can't carry:
/// the computed `next_run`, its most recent job (`last_job_id` / `last_status`),
/// and a `health` reason so "why isn't this firing?" is answerable over the API
/// instead of only in server logs.
///
/// `health` is derived from the same conditions the scheduler checks — and for
/// the overlap condition from the same read and the same predicate
/// (`scheduler::latest_run` → `run_holds_slot`), not a second interpretation of
/// a second query. The previous arrangement had exactly that second
/// interpretation, and it reported `ok` for schedules the scheduler had already
/// stopped firing (see `run_holds_slot`).
async fn enrich_schedules(
    state: &AppState,
    schedules: Vec<Schedule>,
) -> Result<Vec<Value>, ApiError> {
    let mut out = Vec::with_capacity(schedules.len());
    for schedule in schedules {
        let next_run = crate::scheduler::project_next_run(&schedule);
        let last = crate::scheduler::latest_run(state, &schedule.id)
            .await
            .map_err(|e| {
                ApiError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to read the schedule's latest run: {e}"),
                )
            })?;
        let (last_job_id, last_status) = (last.job_id, last.status);
        // Precedence mirrors the scheduler's own short-circuits: a disabled or
        // mis-configured schedule never reaches the overlap check.
        let health = if !schedule.enabled {
            "disabled"
        } else if next_run.is_none() {
            // project_next_run only returns None on an unparseable/exhausted cron.
            "invalid_cron"
        } else if !state.registry.contains_key(&schedule.app) {
            "unregistered_app"
        } else if crate::scheduler::validate_schedule_params(
            &state.registry,
            &schedule.app,
            &schedule.params,
        )
        .is_err()
        {
            // The scheduler skips this row at the fire step; say so here rather
            // than reporting "ok" for a schedule that will never enqueue again.
            // Derived (not stored) on purpose: it is computed by the SAME
            // function the fire path calls, so fixing the row clears the marker
            // in the same instant the scheduler starts firing it again — a
            // stored flag would have to be un-set by somebody.
            "invalid_params"
        } else if last.holds_slot {
            "overlapping"
        } else {
            "ok"
        };
        let mut value = serde_json::to_value(&schedule).unwrap_or_else(|_| json!({}));
        if let Value::Object(map) = &mut value {
            map.insert("next_run".into(), json!(next_run));
            map.insert("last_job_id".into(), json!(last_job_id));
            map.insert("last_status".into(), json!(last_status));
            map.insert("health".into(), json!(health));
        }
        out.push(value);
    }
    Ok(out)
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateScheduleBody {
    app: String,
    /// 6-field cron with seconds: "sec min hour day month weekday".
    cron: String,
    /// Params for the jobs this schedule enqueues. An object **shallow-merges
    /// over the app's `default_params`**, exactly like `POST /apps/{name}/jobs`
    /// — so setting one key keeps the rest of the defaults. Validated here
    /// against the app's declared schema (422), on the merged result.
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
    /// Spend ceiling replayed into **every** job this schedule enqueues; metered
    /// Claude calls abort past it. Must be **> 0** — validated by the same
    /// `jobs::validate_budget_usd` the enqueue and trigger doors use, so `0`,
    /// negative, NaN and ∞ are refused with the identical 422 rather than
    /// silently becoming "no ceiling". Omitted = no ceiling (unchanged
    /// behaviour, and the only thing catalog-managed schedules can have).
    budget_usd: Option<f64>,
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
        (status = 422, description = "The effective params (app defaults + this body's `params`) fail the app's declared JSON Schema — same shape as the enqueue door — or `budget_usd` is not a positive number of dollars", body = Object),
    )
)]
pub(crate) async fn create_schedule(
    State(state): State<AppState>,
    Json(body): Json<CreateScheduleBody>,
) -> Result<(StatusCode, Json<Schedule>), ApiError> {
    if !state.registry.contains_key(&body.app) {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("unknown app '{}'", body.app),
        ));
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

    // Door parity: validate the EFFECTIVE params (the app's defaults with this
    // body's params merged over them — exactly what the scheduler will enqueue)
    // against the app's declared schema, with the same 422 + pointer-path shape
    // `POST /apps/{name}/jobs` answers. Before this, a schedule accepted params
    // the job door would have refused and the failure only surfaced on the
    // first cron firing, as a failed job with no link back to this row.
    if let Err(msg) = crate::scheduler::validate_schedule_params(
        &state.registry,
        &body.app,
        body.params.as_ref().unwrap_or(&Value::Null),
    ) {
        return Err(ApiError(StatusCode::UNPROCESSABLE_ENTITY, msg));
    }

    // The SAME floor the jobs and trigger doors apply. A schedule is the worst
    // place for `0` to decay into "no ceiling": the value is stored on the row
    // and replayed into every run forever, so one silent reinterpretation is a
    // standing unlimited-spend order rather than one bad job.
    let budget_usd = validate_budget_usd(body.budget_usd)
        .map_err(|msg| ApiError(StatusCode::UNPROCESSABLE_ENTITY, msg))?;

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
            budget_usd,
        })
        .await?;
    Ok((StatusCode::CREATED, Json(schedule)))
}

/// Body of `POST /schedules/{id}/budget`. `null` clears the ceiling.
#[derive(Deserialize, ToSchema)]
pub(crate) struct ScheduleBudgetBody {
    /// New spend ceiling, or `null` to remove it. Must be **> 0** when present —
    /// same `validate_budget_usd` contract as `POST /schedules`.
    budget_usd: Option<f64>,
}

#[utoipa::path(
    post,
    path = "/schedules/{id}/budget",
    tag = "schedules",
    params(("id" = String, Path, description = "Schedule id")),
    request_body = ScheduleBudgetBody,
    responses(
        (status = 200, description = "`{id, budget_usd}`"),
        (status = 404, description = "Schedule not found", body = Object),
        (status = 422, description = "`budget_usd` is not a positive number of dollars", body = Object),
    )
)]
pub(crate) async fn set_schedule_budget(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<ScheduleBudgetBody>,
) -> Result<Json<Value>, ApiError> {
    // Without this door a ceiling could only be set at create time, which would
    // leave it unreachable for exactly the schedules that spend: code-seeded
    // rows (`ScrapeApp::schedule`) and catalog-managed ones are both created
    // with `budget_usd = NULL` and are not created through `POST /schedules`.
    let budget_usd = validate_budget_usd(body.budget_usd)
        .map_err(|msg| ApiError(StatusCode::UNPROCESSABLE_ENTITY, msg))?;
    if state.storage.set_schedule_budget(&id, budget_usd).await? {
        Ok(Json(json!({ "id": id, "budget_usd": budget_usd })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "schedule not found".into()))
    }
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
pub(crate) async fn delete_schedule(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_schedule(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "schedule not found".into()))
    }
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
pub(crate) async fn set_schedule_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state
        .storage
        .set_schedule_enabled(&id, body.enabled)
        .await?
    {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "schedule not found".into()))
    }
}
