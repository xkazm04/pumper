//! Remote fetch fabric, serving side (M17 v1): `POST /fetch-proxy`.
//!
//! A peer coordinator POSTs a serialized [`HttpRequest`]; this node runs it
//! through its **local** fetch stack — the real HTTP engine, which means the
//! politeness governor, response cache, retries, and body caps all apply
//! exactly as they would for a locally-originated fetch — and returns the
//! [`pumper_core::HttpResponse`] as JSON (the envelope the coordinator-side
//! `pumper-engine-remote` decodes).
//!
//! Guardrails:
//! - the route answers 404 unless `[remote] enabled` (and a secret is set —
//!   `Config::validate` enforces that pairing at boot),
//! - the shared secret must arrive in [`REMOTE_SECRET_HEADER`] (401 otherwise;
//!   compared as SHA-256 digests so the comparison doesn't leak a prefix),
//! - the inner request's `max_body_bytes` / `timeout_secs` are clamped to the
//!   `[remote]` caps — a peer can lower them, never raise them,
//! - a local engine failure is 502 (the coordinator treats any non-2xx as
//!   "this node failed" and falls back to its own local engine).

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use pumper_core::HttpRequest;
use pumper_engine_remote::REMOTE_SECRET_HEADER;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::routes::error::ApiError;
use crate::state::AppState;

/// Constant-shape secret comparison: hash both sides, compare digests. Two
/// fixed-length digests make the `==` timing independent of where the presented
/// value diverges from the real secret.
fn secret_matches(presented: &str, expected: &str) -> bool {
    Sha256::digest(presented.as_bytes()) == Sha256::digest(expected.as_bytes())
}

/// Run a peer's serialized fetch through this node's local stack.
#[utoipa::path(
    post,
    path = "/fetch-proxy",
    tag = "remote",
    request_body(
        content = Value,
        description = "A serialized `HttpRequest` (same serde shape the engines use): \
            `{url, method?, headers?, body?, no_cache?, ttl_override?, profile?, \
            max_body_bytes?, timeout_secs?, ...}`. Body/timeout caps are clamped to \
            the node's `[remote]` limits."
    ),
    responses(
        (status = 200, description = "The `HttpResponse` envelope: \
            `{status, headers, body, final_url, cache_hit}`"),
        (status = 401, description = "Missing or wrong `x-pumper-remote-secret`"),
        (status = 404, description = "`[remote]` disabled on this node"),
        (status = 502, description = "The local fetch itself failed"),
    )
)]
pub(crate) async fn fetch_proxy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut req): Json<HttpRequest>,
) -> Result<Json<Value>, ApiError> {
    let cfg = &state.config.remote;
    // Disabled (or secret-less, which validate() rejects at boot for real
    // configs but a hand-assembled state could still carry): the route does
    // not exist as far as callers are concerned — never an open proxy.
    if !cfg.enabled || cfg.secret.trim().is_empty() {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "remote fetch fabric is disabled on this node ([remote] enabled)".into(),
        ));
    }
    let presented = headers
        .get(REMOTE_SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !secret_matches(presented, cfg.secret.trim()) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            format!("missing or invalid {REMOTE_SECRET_HEADER} header"),
        ));
    }

    // Size/time caps: the peer may tighten them per request, never exceed the
    // node's own ceilings.
    req.max_body_bytes = Some(
        req.max_body_bytes
            .map_or(cfg.max_body_bytes, |b| b.min(cfg.max_body_bytes)),
    );
    req.timeout_secs = Some(
        req.timeout_secs
            .map_or(cfg.timeout_secs, |t| t.min(cfg.timeout_secs)),
    );

    // The LOCAL stack: `engines.http` is the real HttpEngine — governor
    // spacing, learned penalties, cache, retries, profile jars all included.
    let resp = state.engines.http.fetch(req).await.map_err(|e| {
        ApiError(StatusCode::BAD_GATEWAY, format!("proxied fetch failed: {e}"))
    })?;
    let value = serde_json::to_value(&resp)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(value))
}

#[cfg(test)]
mod tests {
    use super::secret_matches;

    #[test]
    fn secret_comparison_is_exact() {
        assert!(secret_matches("sesame", "sesame"));
        assert!(!secret_matches("", "sesame"));
        assert!(!secret_matches("sesam", "sesame"));
        assert!(!secret_matches("sesamee", "sesame"));
    }
}
