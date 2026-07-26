//! Dataset change webhooks: list, create, delete, and enable/disable the
//! `dataset.changed` delivery subscriptions.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::routes::error::{default_limit, keyset_cursor, parse_cursor, ApiError, EnabledBody};
use crate::state::AppState;

#[derive(Deserialize, IntoParams)]
pub(crate) struct WatchesQuery {
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
pub(crate) async fn list_watches(
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
pub(crate) struct CreateWatchBody {
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
pub(crate) async fn create_watch(
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
pub(crate) async fn delete_watch(
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
pub(crate) async fn set_watch_enabled(
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
