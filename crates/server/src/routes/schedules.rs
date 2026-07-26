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
    responses((status = 200, description = "Dual-mode: bare `[Schedule]` array, or `{items, next_cursor}` when `cursor` is present. Each schedule is enriched with `next_run` (computed next firing), `last_job_id` / `last_status` (its most recent run), and `health` (`ok` | `disabled` | `invalid_cron` | `unregistered_app` | `overlapping`) — so a silently-wedged schedule is visible over the API."))
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
    Ok(Json(json!({ "items": enriched, "next_cursor": next_cursor })))
}

/// Enriches each schedule with the observability fields the raw row can't carry:
/// the computed `next_run`, its most recent job (`last_job_id` / `last_status`),
/// and a `health` reason so "why isn't this firing?" is answerable over the API
/// instead of only in server logs. `health` is derived from the same conditions
/// the scheduler checks, so the API and the reconcile loop can't disagree.
async fn enrich_schedules(
    state: &AppState,
    schedules: Vec<Schedule>,
) -> Result<Vec<Value>, ApiError> {
    let mut out = Vec::with_capacity(schedules.len());
    for schedule in schedules {
        let next_run = crate::scheduler::project_next_run(&schedule);
        let last = state.storage.latest_job_for_schedule(&schedule.id).await?;
        let (last_job_id, last_status) = match &last {
            Some((id, status)) => (Some(id.clone()), Some(status.clone())),
            None => (None, None),
        };
        // Precedence mirrors the scheduler's own short-circuits: a disabled or
        // mis-configured schedule never reaches the overlap check.
        let last_active = matches!(last_status.as_deref(), Some("queued") | Some("running"));
        let health = if !schedule.enabled {
            "disabled"
        } else if next_run.is_none() {
            // project_next_run only returns None on an unparseable/exhausted cron.
            "invalid_cron"
        } else if !state.registry.contains_key(&schedule.app) {
            "unregistered_app"
        } else if last_active {
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
pub(crate) async fn create_schedule(
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
    if state.storage.set_schedule_enabled(&id, body.enabled).await? {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "schedule not found".into()))
    }
}
