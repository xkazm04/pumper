//! Identity & tenancy surface (N20): principal CRUD, the audit ledger, and
//! spend attributed to the caller who created the work.
//!
//! Every route here requires the `admin` scope (see `crate::auth`), including
//! the reads — this is the list of who may call the node at all.
//!
//! The key is shown **once**, at creation and at rotation, exactly like
//! `create_ingress_source`'s signing secret: only a SHA-256 digest is stored, so
//! there is no later call that could return it.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::auth::{generate_key, hash_key, validate_scope, CallerPrincipal};
use crate::routes::error::{parse_since, ApiError};
use crate::state::AppState;

/// Default and maximum page size for `GET /audit`.
const AUDIT_DEFAULT_LIMIT: i64 = 100;
const AUDIT_MAX_LIMIT: i64 = 500;

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreatePrincipalBody {
    name: String,
    /// Scope strings: `"admin"`, `"read"`, `"enqueue:<app>"`, `"enqueue:*"`.
    /// An unknown scope is a 400, never a silently powerless key.
    scopes: Vec<String>,
    /// Daily spend ceiling in USD across every job this key enqueues. Omitted =
    /// **no ceiling**, the same convention `budget_usd` uses at every other
    /// door — so `0`/negative is refused rather than reinterpreted.
    budget_usd_per_day: Option<f64>,
    /// Requests/minute (also the burst). Omitted = the `[auth]`
    /// `default_rate_limit_per_min` fallback.
    rate_limit_per_min: Option<i64>,
}

/// The ceiling a principal may carry, or the caller-facing refusal.
///
/// The same anti-pattern `validate_budget_usd` defends at the job door, one
/// level up: `0` here would silently become `None`, and `None` means *no
/// ceiling* — the most cautious input producing the least limited key.
pub(crate) fn validate_daily_budget(requested: Option<f64>) -> Result<Option<f64>, String> {
    match requested {
        None => Ok(None),
        Some(b) if b.is_finite() && b > 0.0 => Ok(Some(b)),
        Some(b) => Err(format!(
            "budget_usd_per_day must be a positive number of dollars (got {b}). Omitting it means \
             this key runs with NO daily ceiling, so {b} cannot also mean 'spend nothing' — pass a \
             real ceiling, or disable the key."
        )),
    }
}

/// The scope list a principal may carry, or the refusal. An empty list is
/// refused: a key that satisfies no requirement is indistinguishable from a
/// disabled one, and minting it silently is how an operator ends up debugging
/// 403s on a key they believe is correct.
pub(crate) fn validate_scopes(scopes: &[String]) -> Result<(), String> {
    if scopes.is_empty() {
        return Err(
            "scopes must name at least one grant ('admin', 'read', 'enqueue:<app>', \
             'enqueue:*') — a key with no scopes can call nothing"
                .to_string(),
        );
    }
    for scope in scopes {
        validate_scope(scope)?;
    }
    Ok(())
}

#[utoipa::path(
    get,
    path = "/principals",
    tag = "principals",
    responses((status = 200, description = "`{count, principals}` — key digests are never listed"))
)]
pub(crate) async fn list_principals(
    State(state): State<AppState>,
    caller: Option<axum::Extension<CallerPrincipal>>,
) -> Result<Json<Value>, ApiError> {
    let principals = state.storage.list_principals().await?;
    // Who this request resolved as, read back out of the extension the identity
    // layer stamped. `synthetic` is the honest half: in `open` mode the caller
    // is an identity this server invented, not one it authenticated, and a
    // surface that rendered it identically to a real key would be claiming an
    // authentication that never happened.
    let caller = caller.map(|axum::Extension(c)| {
        json!({
            "id": c.id,
            "name": c.name,
            "scopes": c.scopes,
            "synthetic": c.synthetic,
        })
    });
    Ok(Json(json!({
        "count": principals.len(),
        "mode": state.config.auth.effective_mode(),
        "caller": caller,
        "principals": principals,
    })))
}

#[utoipa::path(
    post,
    path = "/principals",
    tag = "principals",
    request_body = CreatePrincipalBody,
    responses(
        (status = 201, description = "`{principal, key}` — the key is shown ONCE", body = Object),
        (status = 400, description = "Empty name, empty/unknown scope", body = Object),
        (status = 422, description = "`budget_usd_per_day` is not a positive number of dollars", body = Object),
    )
)]
pub(crate) async fn create_principal(
    State(state): State<AppState>,
    Json(body): Json<CreatePrincipalBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let name = body.name.trim();
    if name.is_empty() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "name is required".into()));
    }
    validate_scopes(&body.scopes).map_err(|m| ApiError(StatusCode::BAD_REQUEST, m))?;
    let budget = validate_daily_budget(body.budget_usd_per_day)
        .map_err(|m| ApiError(StatusCode::UNPROCESSABLE_ENTITY, m))?;
    let key = generate_key();
    let principal = state
        .storage
        .create_principal(
            name,
            &hash_key(&key),
            &body.scopes,
            budget,
            body.rate_limit_per_min,
        )
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "principal": principal, "key": key })),
    ))
}

#[utoipa::path(
    post,
    path = "/principals/{id}/disable",
    tag = "principals",
    params(("id" = String, Path, description = "Principal id")),
    responses(
        (status = 200, description = "`{id, enabled: false}`"),
        (status = 404, description = "Principal not found", body = Object),
    )
)]
pub(crate) async fn disable_principal(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.set_principal_enabled(&id, false).await? {
        Ok(Json(json!({ "id": id, "enabled": false })))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            "principal not found".into(),
        ))
    }
}

#[utoipa::path(
    post,
    path = "/principals/{id}/rotate",
    tag = "principals",
    params(("id" = String, Path, description = "Principal id")),
    responses(
        (status = 200, description = "`{id, key}` — the NEW key, shown once; the old one stops working immediately", body = Object),
        (status = 404, description = "Principal not found", body = Object),
    )
)]
pub(crate) async fn rotate_principal(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let key = generate_key();
    if state
        .storage
        .rotate_principal_key(&id, &hash_key(&key))
        .await?
    {
        Ok(Json(json!({ "id": id, "key": key })))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            "principal not found".into(),
        ))
    }
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct AuditQuery {
    /// Restrict to one principal id.
    principal: Option<String>,
    /// Opaque keyset cursor (the previous page's `next_cursor`).
    cursor: Option<String>,
    limit: Option<i64>,
}

/// The audit cursor is the last row id seen — the ledger's own monotonic key, so
/// a page boundary cannot skip or repeat a row the way a timestamp cursor can
/// when two rows share a microsecond.
fn parse_audit_cursor(cursor: &str) -> Option<i64> {
    let trimmed = cursor.trim();
    (!trimmed.is_empty())
        .then(|| trimmed.parse().ok())
        .flatten()
}

#[utoipa::path(
    get,
    path = "/audit",
    tag = "principals",
    params(AuditQuery),
    responses((status = 200, description = "`{items, next_cursor}` — newest first, keyset-paged on the row id"))
)]
pub(crate) async fn list_audit(
    State(state): State<AppState>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query
        .limit
        .unwrap_or(AUDIT_DEFAULT_LIMIT)
        .clamp(1, AUDIT_MAX_LIMIT);
    let after = query.cursor.as_deref().and_then(parse_audit_cursor);
    let entries = state
        .storage
        .list_audit(query.principal.as_deref(), after, limit)
        .await?;
    // A full page means there may be more; a short page is the last one.
    let next_cursor = (entries.len() as i64 == limit)
        .then(|| entries.last().map(|e| e.id.to_string()))
        .flatten();
    Ok(Json(
        json!({ "items": entries, "next_cursor": next_cursor }),
    ))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct PrincipalCostsQuery {
    /// RFC3339 instant; only spend after it is counted.
    since: Option<String>,
}

#[utoipa::path(
    get,
    path = "/principals/costs",
    tag = "principals",
    params(PrincipalCostsQuery),
    responses((status = 200, description = "`{total_usd, by_principal}` — spend grouped by the caller who enqueued the work; rows with no caller appear as `(unattributed)` so the parts sum to the total")),
)]
pub(crate) async fn principal_costs(
    State(state): State<AppState>,
    Query(query): Query<PrincipalCostsQuery>,
) -> Result<Json<Value>, ApiError> {
    let since = parse_since(query.since.as_deref())?;
    let rows = state.costs.summary_by_principal(since).await?;
    let total: f64 = rows.iter().map(|r| r.cost_usd).sum();
    let by_principal: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "principal_id": r.principal_id,
                "principal": r.label(),
                "calls": r.calls,
                "cost_usd": r.cost_usd,
            })
        })
        .collect();
    Ok(Json(
        json!({ "total_usd": total, "by_principal": by_principal }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daily_budget_refuses_zero_rather_than_reading_it_as_unlimited() {
        assert_eq!(validate_daily_budget(None), Ok(None));
        assert_eq!(validate_daily_budget(Some(2.5)), Ok(Some(2.5)));
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(
                validate_daily_budget(Some(bad)).is_err(),
                "{bad} must be refused, not silently dropped to 'no ceiling'"
            );
        }
    }

    #[test]
    fn scopes_must_be_non_empty_and_known() {
        assert!(validate_scopes(&["admin".to_string()]).is_ok());
        assert!(
            validate_scopes(&[]).is_err(),
            "a key with no scopes can call nothing — refuse it at creation"
        );
        assert!(validate_scopes(&["write".to_string()]).is_err());
        assert!(validate_scopes(&["read".to_string(), "bogus".to_string()]).is_err());
    }

    #[test]
    fn audit_cursor_parses_a_row_id_and_ignores_junk() {
        assert_eq!(parse_audit_cursor("42"), Some(42));
        assert_eq!(parse_audit_cursor(" 42 "), Some(42));
        // Page 1 is signalled by an EMPTY cursor, which must not become an id.
        assert_eq!(parse_audit_cursor(""), None);
        assert_eq!(parse_audit_cursor("not-a-number"), None);
    }
}
