//! Inbound event ingress: `POST /ingest/{id}` turns external webhooks into
//! `external` events on the EventBus (visible on `/events` + the replay ring)
//! and inputs to external-kind triggers — plus the ingress-source CRUD that
//! issues the per-caller signing secrets.
//!
//! This is pumper's first write surface designed for non-localhost callers, so
//! the defaults are hostile: `[ingress] enabled = false`, per-source HMAC
//! secrets are mandatory, bodies are size-capped, and each source is
//! token-bucket rate-limited. Verification reuses the outbound `sign()` scheme
//! inverted (see `crate::webhook::verify_signature`), and additionally accepts
//! GitHub's bare `HMAC(secret, body)` digest so a GitHub webhook can point
//! straight at `/ingest/{id}`.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::events::JobEvent;
use crate::routes::error::{ApiError, EnabledBody};
use crate::state::AppState;

// ── per-source token buckets ─────────────────────────────────────────────────

/// Bucket state per source id: (tokens remaining, last refill instant).
/// Process-global, not persisted — a restart refills every bucket, which for a
/// politeness rail is fine (the SQL surface stays bounded either way).
static BUCKETS: OnceLock<Mutex<HashMap<String, (f64, Instant)>>> = OnceLock::new();

/// Pure token-bucket step: refills `tokens` for `elapsed_secs` at
/// `per_min`/60 tokens per second (capped at the burst = `per_min`), then takes
/// one if available. Returns (new_tokens, allowed).
fn bucket_step(tokens: f64, elapsed_secs: f64, per_min: u32) -> (f64, bool) {
    let cap = per_min as f64;
    let refilled = (tokens + elapsed_secs * cap / 60.0).min(cap);
    if refilled >= 1.0 {
        (refilled - 1.0, true)
    } else {
        (refilled, false)
    }
}

/// Deterministic event id for a non-UUID sender delivery id: the first 16
/// bytes of SHA-256 over `"{source_id}:{delivery_id}"`. Stable per delivery
/// (so redeliveries dedupe) and distinct across sources.
fn derived_event_id(source_id: &str, delivery_id: &str) -> Uuid {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("{source_id}:{delivery_id}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// Consumes one token for `source_id`, creating a full bucket on first sight.
fn rate_limit_allow(source_id: &str, per_min: u32) -> bool {
    let now = Instant::now();
    let mut buckets = BUCKETS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    let entry = buckets
        .entry(source_id.to_string())
        .or_insert((per_min as f64, now));
    let elapsed = now.duration_since(entry.1).as_secs_f64();
    let (tokens, allowed) = bucket_step(entry.0, elapsed, per_min);
    *entry = (tokens, now);
    allowed
}

// ── source CRUD ──────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/ingress/sources",
    tag = "ingress",
    responses((status = 200, description = "`{count, sources}` — secrets are never listed"))
)]
pub(crate) async fn list_ingress_sources(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let sources = state.storage.list_ingress_sources().await?;
    Ok(Json(json!({ "count": sources.len(), "sources": sources })))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateIngressSourceBody {
    name: String,
    /// Signing secret; generated (and returned ONCE) when omitted.
    secret: Option<String>,
}

/// Creates an ingress source. The response is the only time the secret is ever
/// returned — list/read responses omit it.
#[utoipa::path(
    post,
    path = "/ingress/sources",
    tag = "ingress",
    request_body = CreateIngressSourceBody,
    responses(
        (status = 201, description = "`{source, secret}` — the secret is shown once", body = Object),
        (status = 400, description = "Empty name or empty secret", body = Object),
    )
)]
pub(crate) async fn create_ingress_source(
    State(state): State<AppState>,
    Json(body): Json<CreateIngressSourceBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let name = body.name.trim();
    if name.is_empty() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "name is required".into()));
    }
    // An unsigned ingress source would be an open POST-to-trigger surface, so a
    // secret always exists: caller-supplied (non-empty) or generated here.
    let secret = match body.secret {
        Some(s) if s.trim().is_empty() => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "secret must be non-empty when supplied".into(),
            ))
        }
        Some(s) => s,
        None => format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple()),
    };
    let source = state.storage.create_ingress_source(name, &secret).await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "source": source, "secret": secret })),
    ))
}

#[utoipa::path(
    delete,
    path = "/ingress/sources/{id}",
    tag = "ingress",
    params(("id" = String, Path, description = "Ingress source id")),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`)"),
        (status = 404, description = "Source not found", body = Object),
    )
)]
pub(crate) async fn delete_ingress_source(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_ingress_source(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            "ingress source not found".into(),
        ))
    }
}

#[utoipa::path(
    post,
    path = "/ingress/sources/{id}/enabled",
    tag = "ingress",
    params(("id" = String, Path, description = "Ingress source id")),
    request_body = EnabledBody,
    responses(
        (status = 200, description = "`{id, enabled}`"),
        (status = 404, description = "Source not found", body = Object),
    )
)]
pub(crate) async fn set_ingress_source_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state
        .storage
        .set_ingress_source_enabled(&id, body.enabled)
        .await?
    {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            "ingress source not found".into(),
        ))
    }
}

// ── the ingest endpoint ──────────────────────────────────────────────────────

/// Accepts one signed external event and stamps it onto the EventBus.
///
/// Auth: `x-pumper-signature: sha256=<hex>` over one of two bases —
/// - with `x-pumper-timestamp` (and optional `x-pumper-delivery-id`): the full
///   outbound scheme `HMAC(secret, "{ts}.{id}." ++ body)`, timestamp checked
///   against `[ingress] max_skew_secs`;
/// - without: bare `HMAC(secret, body)` — GitHub's `x-hub-signature-256`
///   scheme, which is also accepted from that header directly.
///
/// The delivery id (when sent) becomes the event id, which scopes trigger
/// idempotency: a redelivered webhook re-verifies but cannot double-fire.
#[utoipa::path(
    post,
    path = "/ingest/{id}",
    tag = "ingress",
    params(("id" = String, Path, description = "Ingress source id")),
    responses(
        (status = 202, description = "`{event_id, seq, triggers_fired}`"),
        (status = 400, description = "Body is not JSON", body = Object),
        (status = 401, description = "Missing/invalid signature or stale timestamp", body = Object),
        (status = 403, description = "Source disabled", body = Object),
        (status = 404, description = "Unknown source", body = Object),
        (status = 409, description = "`[ingress] enabled = false`", body = Object),
        (status = 413, description = "Body exceeds `[ingress] max_body_bytes`", body = Object),
        (status = 429, description = "Per-source rate limit exceeded", body = Object),
    )
)]
pub(crate) async fn ingest(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    // Raw body as a String: the signature covers the exact bytes, and JSON is
    // UTF-8 by definition (non-UTF-8 is rejected by the extractor as 400).
    body: String,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let body = body.into_bytes();
    let cfg = &state.config.ingress;
    if !cfg.enabled {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "inbound ingress is disabled ([ingress] enabled = false)".into(),
        ));
    }
    let Some(source) = state.storage.get_ingress_source(&id).await? else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "ingress source not found".into(),
        ));
    };
    if !source.enabled {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "ingress source is disabled".into(),
        ));
    }
    // Rate-limit before crypto: cheap rejection first, and a flood of bad
    // signatures burns the same bucket as good traffic.
    if !rate_limit_allow(&source.id, cfg.rate_limit_per_min) {
        return Err(ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "rate limit exceeded ({} events/min per source)",
                cfg.rate_limit_per_min
            ),
        ));
    }
    if body.len() > cfg.max_body_bytes {
        return Err(ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "body exceeds [ingress] max_body_bytes ({})",
                cfg.max_body_bytes
            ),
        ));
    }

    // Signature: x-pumper-signature, or GitHub's x-hub-signature-256 verbatim.
    let header_str = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
    };
    let Some(sig) = header_str("x-pumper-signature").or_else(|| header_str("x-hub-signature-256"))
    else {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "missing x-pumper-signature".into(),
        ));
    };
    let sig = sig.strip_prefix("sha256=").unwrap_or(sig);
    let delivery_id = header_str("x-pumper-delivery-id").unwrap_or("");
    let context = match header_str("x-pumper-timestamp") {
        Some(raw) => {
            let Ok(ts) = raw.parse::<i64>() else {
                return Err(ApiError(
                    StatusCode::UNAUTHORIZED,
                    "invalid x-pumper-timestamp".into(),
                ));
            };
            // Skew gate BEFORE the MAC: a stale-but-correctly-signed capture is
            // exactly the replay this bound exists to stop.
            if (chrono::Utc::now().timestamp() - ts).abs() > cfg.max_skew_secs {
                return Err(ApiError(
                    StatusCode::UNAUTHORIZED,
                    "timestamp outside the accepted skew window".into(),
                ));
            }
            Some((ts, delivery_id))
        }
        None => None,
    };
    if !crate::webhook::verify_signature(source.secret.as_bytes(), context, &body, sig) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "signature verification failed".into(),
        ));
    }

    let payload: Value = serde_json::from_slice(&body)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("body is not JSON: {e}")))?;

    // Event id: the sender's delivery id when given (stable across redeliveries
    // — that stability is what makes trigger idempotency hold), else fresh.
    let event_uuid = Uuid::parse_str(delivery_id).unwrap_or_else(|_| {
        if delivery_id.is_empty() {
            Uuid::new_v4()
        } else {
            // Non-UUID delivery ids (GitHub's are UUIDs, others may not be)
            // still need a stable mapping: derive one from source + id.
            derived_event_id(&source.id, delivery_id)
        }
    });
    let event_id = event_uuid.to_string();
    let seq = state.events.emit(JobEvent::external(
        event_uuid,
        &source.name,
        payload.clone(),
    ));
    let fired = crate::triggers::fire_external_triggers(
        &state,
        &source.id,
        &source.name,
        &event_id,
        &payload,
    )
    .await;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "event_id": event_id, "seq": seq, "triggers_fired": fired })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_allows_burst_then_refuses_until_refill() {
        // Burst of 3 at 3/min: three pass, the fourth is refused.
        let mut tokens = 3.0;
        for _ in 0..3 {
            let (t, ok) = bucket_step(tokens, 0.0, 3);
            assert!(ok);
            tokens = t;
        }
        let (t, ok) = bucket_step(tokens, 0.0, 3);
        assert!(!ok, "burst exhausted");
        // 20s at 3/min refills exactly one token.
        let (_, ok) = bucket_step(t, 20.0, 3);
        assert!(ok, "refill admits again");
    }

    #[test]
    fn bucket_refill_is_capped_at_burst() {
        // An hour idle must not bank 180 tokens at 3/min — cap is the burst.
        let (tokens, ok) = bucket_step(0.0, 3600.0, 3);
        assert!(ok);
        assert!(tokens <= 3.0, "refill capped at per_min, got {tokens}");
    }

    #[test]
    fn stable_event_id_for_non_uuid_delivery_ids() {
        // The derivation used by `ingest` must be deterministic per
        // (source, delivery id) and distinct across either changing.
        let a = derived_event_id("s1", "delivery-1");
        let b = derived_event_id("s1", "delivery-1");
        let c = derived_event_id("s1", "delivery-2");
        let d = derived_event_id("s2", "delivery-1");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
    }
}
