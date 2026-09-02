//! The approval surface for live (irreversible) browser actions — N01 Transact v2.
//!
//! A `transact` job enqueued with `submit: true` runs its dry run, stages a
//! `pending` row in the transactions ledger, and **parks** (N02 `waiting`) with
//! the evidence bundle as its `input_request`. These routes are the only door
//! that can release it.
//!
//! ## The gates, in the order they are applied
//!
//! 1. **`[transact] allow_live`** — off by default, and checked FIRST, so a node
//!    that has not opted in never even reports which of the later rules a
//!    request broke.
//! 2. **`admin` scope** — enforced by the identity layer, not by this file:
//!    `required_scope` maps every non-enqueue mutation to `Requirement::Admin`,
//!    so `POST /transactions/{id}/approve` needs an admin key under
//!    `[auth] mode = "keys"` and is audited like every other mutating verb.
//! 3. **The state machine** — [`pumper_core::approve_decision`], the same pure
//!    function the MCP tool consults, over the row's state, its derived
//!    deadline, the digest the caller quoted, and the profile's daily cap.
//! 4. **The SQL guard** — `state = 'pending'` on the write itself, so two
//!    approvals racing for one row cannot both win.
//!
//! Only then is the parked job resumed, and only the resumed attempt can reach
//! `Browser::commit` — which re-probes the live page and refuses unless it still
//! hashes to the digest that was reviewed.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{Duration, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use pumper_core::transactions::{
    approve_decision, expire_stale, get, list, mark_approved, mark_rejected, reject_decision,
    ApprovalRefusal, Transaction, TransactionState,
};

use crate::auth::CallerPrincipal;
use crate::routes::error::ApiError;
use crate::state::AppState;
use crate::webhook::dispatch_event;

/// The window the per-profile submit cap is measured over.
const CAP_WINDOW_HOURS: i64 = 24;

#[derive(Deserialize, IntoParams)]
pub(crate) struct ListQuery {
    // `pub(crate)` so the e2e can construct the extractor directly.
    /// `pending` | `approved` | `submitted` | `rejected` | `expired`. Omitted =
    /// every state, newest first.
    pub(crate) state: Option<String>,
}

#[derive(Deserialize, ToSchema, Default)]
pub(crate) struct ApproveBody {
    /// The digest of the evidence bundle the approver actually read
    /// (`input_request.evidence_sha`, also on `GET /transactions/{id}`).
    ///
    /// Optional, and a mismatch is a refusal. Quoting it is how an approval
    /// proves it is an approval *of something*: a client that re-fetched the
    /// row after a re-stage refreshed the evidence would otherwise release an
    /// action against a bundle nobody looked at.
    #[serde(default)]
    evidence_sha: Option<String>,
}

/// The JSON one ledger row serialises as, with the two facts that are derived
/// rather than stored: the approval deadline (`created_at + [transact]
/// approval_ttl_secs`) and whether it has passed.
fn render(state: &AppState, row: &Transaction) -> Value {
    let deadline = state.config.transact.approval_deadline(row.created_at);
    let now = Utc::now();
    json!({
        "id": row.id,
        "idempotency_key": row.idempotency_key,
        "app": row.app,
        "job_id": row.job_id,
        "profile": row.profile,
        "state": row.state,
        "evidence_sha": row.evidence_sha,
        "approved_by": row.approved_by,
        "approved_at": row.approved_at,
        "submitted_at": row.submitted_at,
        "receipt_path": row.receipt_path,
        "expires_at": deadline,
        "expired": deadline.is_some_and(|d| d <= now) && row.state == TransactionState::Pending,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        // The operator switch, on every row: a reader looking at a pending
        // transaction on a node that cannot submit should learn that here,
        // rather than from a 409 after clicking approve.
        "allow_live": state.config.transact.allow_live,
    })
}

#[utoipa::path(
    get,
    path = "/transactions",
    tag = "transactions",
    params(ListQuery),
    responses(
        (status = 200, description = "`{count, allow_live, transactions}`", body = crate::routes::dto::TransactionListResponse),
        (status = 400, description = "Unknown `state` filter", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn list_transactions(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    // Retire stale mandates before listing them: a `pending` row past its TTL
    // is not pending, and showing it as such invites an approval the door would
    // then refuse.
    let pool = state.storage.pool();
    expire_stale(&pool, state.config.transact.approval_ttl_secs).await?;
    let filter = query
        .state
        .as_deref()
        .map(TransactionState::parse)
        .transpose()
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    let rows = list(&pool, filter, state.config.transact.list_limit).await?;
    Ok(Json(json!({
        "count": rows.len(),
        "allow_live": state.config.transact.allow_live,
        "transactions": rows.iter().map(|r| render(&state, r)).collect::<Vec<_>>(),
    })))
}

#[utoipa::path(
    get,
    path = "/transactions/{id}",
    tag = "transactions",
    params(("id" = String, Path, description = "Transaction id")),
    responses(
        (status = 200, description = "One ledger row", body = crate::routes::dto::TransactionDto),
        (status = 404, description = "No such transaction", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn get_transaction(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let pool = state.storage.pool();
    expire_stale(&pool, state.config.transact.approval_ttl_secs).await?;
    let row = get(&pool, &id).await?.ok_or_else(not_found)?;
    Ok(Json(render(&state, &row)))
}

#[utoipa::path(
    post,
    path = "/transactions/{id}/approve",
    tag = "transactions",
    params(("id" = String, Path, description = "Transaction id")),
    request_body = ApproveBody,
    responses(
        (status = 202, description = "Approved; the parked job was resumed", body = crate::routes::dto::TransactionApproved),
        (status = 404, description = "No such transaction", body = crate::routes::dto::ErrorEnvelope),
        (status = 409, description = "Refused: live submission off, not pending, expired, evidence mismatch, or the profile's daily cap", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn approve_transaction(
    State(state): State<AppState>,
    Path(id): Path<String>,
    caller: Option<axum::Extension<CallerPrincipal>>,
    body: Option<Json<ApproveBody>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let quoted = body.and_then(|Json(b)| b.evidence_sha);
    let pool = state.storage.pool();
    expire_stale(&pool, state.config.transact.approval_ttl_secs).await?;
    let row = get(&pool, &id).await?.ok_or_else(not_found)?;

    let submitted_today = pumper_core::transactions::submitted_since(
        &pool,
        row.profile.as_deref(),
        Utc::now() - Duration::hours(CAP_WINDOW_HOURS),
    )
    .await?;
    approve_decision(
        state.config.transact.allow_live,
        row.state,
        state.config.transact.approval_deadline(row.created_at),
        Utc::now(),
        &row.evidence_sha,
        quoted.as_deref(),
        submitted_today,
        state.config.transact.daily_cap(),
    )
    .map_err(refusal)?;

    // The principal that released it. `stored_id` is `None` for the synthetic
    // `open`-mode operator — an approval this server never authenticated is
    // recorded as unattributed rather than credited to an invented identity.
    let approved_by = caller
        .as_ref()
        .and_then(|axum::Extension(c)| c.stored_id())
        .map(str::to_string);
    if !mark_approved(&pool, &row.id, approved_by.as_deref()).await? {
        // The SQL guard, not the decision function: two approvals raced and the
        // other one won. Re-read so the caller learns which state it lost to.
        let now = get(&pool, &row.id).await?.ok_or_else(not_found)?;
        return Err(refusal(ApprovalRefusal::NotPending(now.state)));
    }

    // Release the parked job. Its resume input is informational — the app
    // re-reads the ledger for authority — so a resume that cannot land is a
    // reported gap, never a silent approval that nothing acts on.
    let resumed = resume_parked_job(&state, &row).await;
    let row = get(&pool, &row.id).await?.ok_or_else(not_found)?;
    let payload = json!({
        "event": "transaction.approved",
        "transaction": render(&state, &row),
        "resumed": resumed,
    });
    notify(&state, "transaction.approved", &row.id, &payload).await;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "transaction": render(&state, &row),
            "resumed": resumed,
            "note": if resumed {
                "the parked job was resumed; it re-probes the live page and submits only if it \
                 still hashes to the approved evidence"
            } else {
                "the transaction is approved but no parked job could be resumed (it may have \
                 been cancelled or expired) — nothing will be submitted until one is"
            },
        })),
    ))
}

#[utoipa::path(
    post,
    path = "/transactions/{id}/reject",
    tag = "transactions",
    params(("id" = String, Path, description = "Transaction id")),
    responses(
        (status = 200, description = "Rejected", body = crate::routes::dto::TransactionRejected),
        (status = 404, description = "No such transaction", body = crate::routes::dto::ErrorEnvelope),
        (status = 409, description = "Not pending", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn reject_transaction(
    State(state): State<AppState>,
    Path(id): Path<String>,
    caller: Option<axum::Extension<CallerPrincipal>>,
) -> Result<Json<Value>, ApiError> {
    let pool = state.storage.pool();
    let row = get(&pool, &id).await?.ok_or_else(not_found)?;
    // Deliberately NOT gated on `allow_live`: saying no must work on every
    // node, including one that has since turned live submission off. A refusal
    // to record a refusal is the wrong way round.
    reject_decision(row.state).map_err(refusal)?;
    let by = caller
        .as_ref()
        .and_then(|axum::Extension(c)| c.stored_id())
        .map(str::to_string);
    if !mark_rejected(&pool, &row.id, by.as_deref()).await? {
        let now = get(&pool, &row.id).await?.ok_or_else(not_found)?;
        return Err(refusal(ApprovalRefusal::NotPending(now.state)));
    }
    // The parked job is cancelled, not resumed: there is nothing for it to do,
    // and leaving it parked would hold its schedule slot forever.
    let cancelled = match &row.job_id {
        Some(job) => match job.parse() {
            Ok(id) => state
                .storage
                .cancel(id)
                .await
                .map(|c| c.is_some())
                .unwrap_or(false),
            Err(_) => false,
        },
        None => false,
    };
    let row = get(&pool, &row.id).await?.ok_or_else(not_found)?;
    let payload = json!({
        "event": "transaction.rejected",
        "transaction": render(&state, &row),
    });
    notify(&state, "transaction.rejected", &row.id, &payload).await;
    Ok(Json(json!({
        "transaction": render(&state, &row),
        "job_cancelled": cancelled,
    })))
}

/// Resumes the job parked on this transaction, reporting whether it landed.
///
/// The input is informational only. Authority lives in the ledger row the app
/// re-reads — which is what makes a hand-posted `POST /jobs/{id}/resume`
/// harmless — so this payload exists to tell the run which row released it, not
/// to tell it that it may act.
async fn resume_parked_job(state: &AppState, row: &Transaction) -> bool {
    let Some(job_id) = row.job_id.as_ref().and_then(|j| j.parse().ok()) else {
        return false;
    };
    let input = json!({ "transaction_id": row.id, "evidence_sha": row.evidence_sha });
    match state.storage.resume(job_id, &input).await {
        Ok(Some(job)) => {
            state.events.emit(crate::events::JobEvent::new(
                job.id,
                job.app.clone(),
                "queued",
            ));
            state.notify.notify_one();
            true
        }
        Ok(None) => false,
        Err(e) => {
            tracing::warn!(error = %e, transaction = %row.id, "approved transaction could not resume its job");
            false
        }
    }
}

/// Best-effort webhook for a ledger transition. Subscribed through
/// `[transact] webhook_url`; absent = no delivery, never a silent failure.
async fn notify(state: &AppState, event: &str, id: &str, payload: &Value) {
    let Some(url) = state.config.transact.webhook_url.clone() else {
        return;
    };
    dispatch_event(
        state,
        "transaction",
        id,
        &url,
        event,
        payload,
        state.config.transact.webhook_secret.clone(),
    )
    .await;
}

fn not_found() -> ApiError {
    ApiError(
        StatusCode::NOT_FOUND,
        "no such transaction (see GET /transactions)".into(),
    )
}

/// Every approval refusal is a 409: the request was well-formed and the caller
/// is allowed to ask — the ledger's state is what says no.
fn refusal(refusal: ApprovalRefusal) -> ApiError {
    ApiError(StatusCode::CONFLICT, refusal.message())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumper_core::config::TransactConfig;

    /// The listing and the door must derive the SAME deadline from the same
    /// row, or a transaction shown as live is refused as expired (or worse, the
    /// reverse). One config method is the single producer.
    #[test]
    fn the_deadline_is_derived_once_from_created_at() {
        let cfg = TransactConfig {
            approval_ttl_secs: 3600,
            ..Default::default()
        };
        let created = Utc::now() - Duration::hours(2);
        let deadline = cfg
            .approval_deadline(created)
            .expect("a TTL yields a deadline");
        assert_eq!(deadline, created + Duration::hours(1));
        assert!(
            deadline <= Utc::now(),
            "a two-hour-old row is past a one-hour TTL"
        );
        // Zero means never, not "expired at created_at" — the difference
        // between "wait for a human" and "refuse every approval".
        let never = TransactConfig {
            approval_ttl_secs: 0,
            ..Default::default()
        };
        assert!(never.approval_deadline(created).is_none());
    }

    /// A node that has not opted in must refuse before consulting anything
    /// else, and the refusal must name the switch — an operator reading a 409
    /// needs to know which key to flip, not that "something" said no.
    #[test]
    fn a_default_node_refuses_every_approval_and_names_the_switch() {
        let cfg = TransactConfig::default();
        assert!(!cfg.allow_live, "live submission is OFF on a default node");
        let err = approve_decision(
            cfg.allow_live,
            TransactionState::Pending,
            None,
            Utc::now(),
            "sha",
            Some("sha"),
            0,
            cfg.daily_cap(),
        )
        .unwrap_err();
        assert_eq!(err, ApprovalRefusal::LiveDisabled);
        assert!(err.message().contains("allow_live"));
        assert_eq!(refusal(err).0, StatusCode::CONFLICT);
    }

    /// Rejecting is not gated on `allow_live`: an operator who turned live
    /// submission off must still be able to close out the requests it left
    /// behind, or the ledger fills with rows nobody can resolve.
    #[test]
    fn reject_is_not_gated_on_the_live_switch() {
        assert!(reject_decision(TransactionState::Pending).is_ok());
        assert!(reject_decision(TransactionState::Submitted).is_err());
    }
}
