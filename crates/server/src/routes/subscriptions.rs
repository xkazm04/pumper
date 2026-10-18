//! Cursor subscriptions (N05): create, list, delete, and the per-subscription
//! delivery log.
//!
//! A subscription is the general form of every push consumer this service has:
//! an event *selector*, a *sink*, and a *cursor*. Where a watch can only ask for
//! "this app's dataset changed" and only survives as long as the process does, a
//! subscription can name any kind in the durable log — `job.succeeded`,
//! `external`, `transaction.submitted`, `source.repair_promoted` — and picks up
//! from its own `cursor_seq` after a restart, a disable, or a week of downtime.
//!
//! The sink vocabulary is the watch vocabulary (`webhook` | `slack` | `file` |
//! `plugin:<name>`), validated by the same gate, delivered by the same
//! transport, retried and dead-lettered by the same ladder.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::auth::CallerPrincipal;
use crate::routes::error::{default_limit, keyset_cursor, parse_cursor, ApiError};
use crate::routes::watches::validate_sink;
use crate::state::AppState;
use crate::subscriptions::parse_selector;

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateSubscriptionBody {
    /// Operator-facing label. Not an identity — the id is.
    pub name: Option<String>,
    /// `{kinds: [..], app, dataset, filters: [{pointer, equals}]}`. Every field
    /// narrows; `{}` (or omitted) subscribes to the whole log.
    #[schema(value_type = Object)]
    pub selector: Option<Value>,
    /// `webhook` (default) | `slack` | `file` | `plugin:<name>`.
    pub sink: Option<String>,
    /// The delivery endpoint (or, for a `plugin:` sink, the connector's target).
    pub url: Option<String>,
    /// HMAC-SHA256 signing secret for delivery bodies. Never read back.
    pub secret: Option<String>,
    /// Where the cursor starts. Omitted = **now**: the subscription receives
    /// what happens next, not the retained backlog. `0` replays the whole
    /// retained log — which is the point of a durable cursor, and is also a
    /// burst, so it is opt-in rather than the default.
    pub from_seq: Option<i64>,
}

#[utoipa::path(
    post,
    path = "/subscriptions",
    tag = "subscriptions",
    request_body = CreateSubscriptionBody,
    responses(
        (status = 201, description = "Created subscription", body = crate::routes::dto::SubscriptionDto),
        (status = 400, description = "Malformed selector, unknown sink, or a url that is not http(s)", body = crate::routes::dto::ErrorEnvelope),
        (status = 409, description = "The durable event log is off (`[events] log_enabled = false`), so nothing would ever be delivered", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn create_subscription(
    State(state): State<AppState>,
    caller: Option<axum::Extension<CallerPrincipal>>,
    Json(body): Json<CreateSubscriptionBody>,
) -> Result<(StatusCode, Json<pumper_core::Subscription>), ApiError> {
    // Refusing here is the same rule `watch_target_refusal` follows: a
    // subscription that could never fire must not be accepted and left sitting
    // `enabled` forever. With the log off there is no outbox to drain at all.
    if !state.events.logs() {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "the durable event log is disabled (`[events] log_enabled = false`), so a \
             subscription could never be delivered — enable it and restart"
                .into(),
        ));
    }
    let selector = parse_selector(body.selector.as_ref().unwrap_or(&Value::Null))
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    let sink = body.sink.as_deref().unwrap_or("webhook");
    let url = validate_sink(&state, sink, body.url.as_deref()).await?;
    // "From now" is the honest default for a fresh consumer; `from_seq: 0` is
    // the explicit "replay everything you still have".
    let from_seq = body
        .from_seq
        .unwrap_or_else(|| state.events.latest_seq() as i64)
        .max(0);
    let sub = state
        .storage
        .create_subscription(
            body.name.as_deref(),
            &selector.to_json(),
            sink,
            url,
            body.secret.as_deref(),
            from_seq,
            caller.as_ref().and_then(|c| c.stored_id()),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(sub)))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct ListSubscriptionsQuery {
    /// `true` = only enabled rows (the drain's view). Default: every row.
    enabled: Option<bool>,
}

#[utoipa::path(
    get,
    path = "/subscriptions",
    tag = "subscriptions",
    params(ListSubscriptionsQuery),
    responses((status = 200, description = "`{count, latest_seq, subscriptions}`. `latest_seq` is the log's head, so the gap between it and a row's `cursor_seq` is that subscription's backlog.", body = crate::routes::dto::SubscriptionListResponse))
)]
pub(crate) async fn list_subscriptions(
    State(state): State<AppState>,
    Query(query): Query<ListSubscriptionsQuery>,
) -> Result<Json<Value>, ApiError> {
    let subs = state
        .storage
        .list_subscriptions(query.enabled.unwrap_or(false))
        .await?;
    Ok(Json(json!({
        "count": subs.len(),
        // Rendered beside the rows on purpose: "cursor_seq = 41" means nothing
        // without the head. 41 of 41 is caught up; 41 of 9000 is a backlog.
        "latest_seq": state.events.latest_seq(),
        "subscriptions": subs,
    })))
}

#[utoipa::path(
    delete,
    path = "/subscriptions/{id}",
    tag = "subscriptions",
    params(("id" = String, Path, description = "Subscription id")),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`)", body = crate::routes::dto::DeletedResponse),
        (status = 404, description = "Subscription not found", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn delete_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_subscription(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            "subscription not found".into(),
        ))
    }
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct SubscriptionDeliveriesQuery {
    /// `pending` | `delivered` | `failed` (still retrying) | `dead` (the ladder
    /// gave up). Anything else is a 400 — same vocabulary and validator as
    /// `GET /webhooks/deliveries`.
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor over this subscription's deliveries.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/subscriptions/{id}/deliveries",
    tag = "subscriptions",
    params(("id" = String, Path, description = "Subscription id"), SubscriptionDeliveriesQuery),
    responses(
        (status = 200, description = "Dual-mode: `{subscription_id, cursor_seq, count, deliveries}`, or `{items, next_cursor}` when `cursor` is present. Bodies excluded — fetch one from `GET /webhooks/deliveries/{id}`.", body = crate::routes::dto::SubscriptionDeliveryFeed),
        (status = 400, description = "Unknown `status` (allowed: pending, delivered, failed, dead)", body = crate::routes::dto::ErrorEnvelope),
        (status = 404, description = "Subscription not found", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn subscription_deliveries(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<SubscriptionDeliveriesQuery>,
) -> Result<Json<Value>, ApiError> {
    // A deleted or mistyped id must not answer `200 {count: 0}` — that reads as
    // "this subscription has never delivered", the exact wrong answer here.
    let Some(sub) = state.storage.get_subscription(&id).await? else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "subscription not found".into(),
        ));
    };
    let limit = query.limit.clamp(1, 500);
    let status = super::triggers::validate_delivery_status(query.status.as_deref())
        .map_err(|msg| ApiError(StatusCode::BAD_REQUEST, msg))?;
    let after = query.cursor.as_deref().and_then(parse_cursor);
    let items = state
        .storage
        .list_deliveries_for_ref_page(
            pumper_core::storage::DELIVERY_KIND_SUBSCRIPTION,
            &id,
            status,
            after,
            limit,
        )
        .await?;
    if query.cursor.is_none() {
        return Ok(Json(json!({
            "subscription_id": id,
            "cursor_seq": sub.cursor_seq,
            "latest_seq": state.events.latest_seq(),
            "last_error": sub.last_error,
            "count": items.len(),
            "deliveries": items,
        })));
    }
    let next_cursor = keyset_cursor(&items, limit, |d| {
        format!("{}|{}", pumper_core::datasets::ts(d.created_at), d.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}
