//! Remote fetch fabric, serving side (M17 v1): `POST /fetch-proxy`.
//!
//! A peer coordinator POSTs a serialized [`HttpRequest`]; this node runs it
//! through its **local** fetch stack — the real HTTP engine, which means the
//! politeness governor, response cache, retries, and body caps all apply
//! exactly as they would for a locally-originated fetch — and returns the
//! [`pumper_core::HttpResponse`] as JSON (the envelope the coordinator-side
//! `pumper-engine-remote` decodes).
//!
//! Guardrails, in the order they are applied:
//! - the route answers 404 unless `[remote] enabled` (and a secret is set —
//!   `Config::validate` enforces that pairing at boot),
//! - the shared secret must arrive in [`REMOTE_SECRET_HEADER`] (401 otherwise;
//!   compared as SHA-256 digests so the comparison doesn't leak a prefix),
//! - **target policy** ([`blocked_target`]): loopback / link-local / private /
//!   CGNAT addresses and non-http(s) schemes are refused with 403 unless
//!   `[remote] allow_private_targets`. This is the only route in the service that
//!   turns a caller-supplied string into an arbitrary outbound request, and it is
//!   reachable *because* a fabric node must bind off loopback — the precondition
//!   `docs/deployment.md` now states in the `[remote]` section,
//! - **session policy** ([`absent_profile`]): a fetch naming a profile this node
//!   does not hold is refused with 422 rather than served from an empty cookie
//!   jar (see the doc comment there — this is the correctness half),
//! - the inner request's `max_body_bytes` / `timeout_secs` are clamped to the
//!   `[remote]` caps — a peer can lower them, never raise them,
//! - a local engine failure is [`PROXY_FETCH_FAILED`] (the coordinator treats
//!   any non-2xx as "this node failed" and moves to the next peer, then to its
//!   own local engine).

use std::path::Path;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use pumper_core::fetcher::REMOTE_TARGET_HEADER;
use pumper_core::HttpRequest;
use pumper_engine_remote::{blocked_target, REMOTE_SECRET_HEADER};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::routes::error::ApiError;
use crate::state::AppState;

/// What this node answers when the fetch it ran on a peer's behalf failed.
///
/// **Deliberately not `502`.** The coordinator dispatches its proxy call through
/// its own `HttpEngine`, whose `[http] retryable_statuses` defaults to
/// `[429, 502, 503, 504]` and whose `[http] retries` defaults to 3 — so a `502`
/// here meant one deterministic peer-side failure cost the coordinator **four**
/// full proxy attempts with exponential backoff, each paying this node's whole
/// fetch time, before its own failover ladder even learned the node was bad. The
/// fabric already owns a retry ladder (next peer, then local, with a per-node
/// cooldown); a second one underneath it multiplies a dead peer's cost for no
/// benefit.
///
/// `422` is the repo's existing answer to exactly this shape: `Browser::transact`
/// capability refusals became a 422 rather than a retryable 502 precisely because
/// a job "burned its whole backoff ladder producing the same sentence four
/// times". The meaning here is the same — *do not re-ask me this*; the
/// coordinator's move is a different node, not the same one again.
///
/// `every_status_a_handler_emits_has_a_code` (routes::error) keeps this inside
/// the documented status inventory, and
/// `the_proxy_failure_status_is_not_one_a_coordinator_will_retry` below keeps it
/// out of the retryable set.
const PROXY_FETCH_FAILED: StatusCode = StatusCode::UNPROCESSABLE_ENTITY;

/// Constant-shape secret comparison: hash both sides, compare digests. Two
/// fixed-length digests make the `==` timing independent of where the presented
/// value diverges from the real secret.
fn secret_matches(presented: &str, expected: &str) -> bool {
    Sha256::digest(presented.as_bytes()) == Sha256::digest(expected.as_bytes())
}

/// Why this node cannot serve a fetch under the profile a peer named — `None`
/// when the request carries no profile, or when this node genuinely holds it.
///
/// A profile is a persistent cookie jar at `<profiles_dir>/<name>/cookies.json`.
/// `engine-http` treats a **missing** jar as "start an empty one" (a deliberate
/// create-on-first-use default for local, interactive onboarding), and says
/// nothing — its one `warn!` covers an *unreadable* jar, not an absent one. On
/// this route that default is a data-integrity failure: the peer answers `200`
/// with the logged-out or login-wall page, the coordinator has no field to tell
/// the difference, and the row is extracted and stored as real content.
///
/// So the node refuses instead, and the coordinator falls back like it would for
/// any node failure. That fallback costs a second fetch — the correct trade,
/// because the alternative is silently wrong data with no detectable trace.
/// (In practice a fixed coordinator never sends one: `must_serve_locally` keeps
/// profiled fetches at home. This guard is what protects a node from an older or
/// hostile peer.)
///
/// Same anti-pattern as `pumper_core::require_existing_profile`, which closed
/// this hole for browser transact flows: "an empty profile is a LOGGED-OUT
/// browser".
fn absent_profile(profiles_dir: &Path, profile: Option<&str>) -> Option<String> {
    let name = profile?;
    let jar = match pumper_core::profile_cookies_path(profiles_dir, name) {
        Ok(jar) => jar,
        // An unusable name (traversal, too long, illegal chars) is refused for
        // the same reason — this node cannot hold that session either.
        Err(e) => return Some(format!("session profile '{name}' is unusable here: {e}")),
    };
    if jar.is_file() {
        return None;
    }
    Some(format!(
        "session profile '{name}' is not present on this node (no jar at {}). Serving a \
         profiled fetch from a jar this node does not have would run it through an EMPTY \
         cookie jar and return the logged-out page with a 200 — indistinguishable from real \
         content by the time it is extracted and stored. Profiled fetches belong on the node \
         that holds the session.",
        jar.display()
    ))
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
        (status = 403, description = "Target out of policy: a loopback / link-local / private / \
            CGNAT address, or a non-http(s) scheme. Relax with `[remote] allow_private_targets`"),
        (status = 404, description = "`[remote]` disabled on this node"),
        (status = 422, description = "This node will not produce a result for this request — \
            either the local fetch itself failed, or the request names a session profile this \
            node does not hold (serving that would return the logged-out page with a 200). \
            Deliberately NOT 502: a coordinator's transport retries 502 by default, which \
            multiplied every deterministic peer-side failure by `[http] retries`"),
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

    // Target policy. Authentication says *who* may ask; this says *what* may be
    // asked for. Holding the cluster secret must not amount to holding a shell
    // on every node's private network.
    if let Some(reason) = blocked_target(&req.url, cfg.allow_private_targets) {
        return Err(ApiError(StatusCode::FORBIDDEN, reason));
    }
    // Session policy: refuse a profile this node does not hold rather than fetch
    // logged out and answer 200.
    if let Some(reason) = absent_profile(&state.config.fetcher.profiles_dir, req.profile.as_deref())
    {
        return Err(ApiError(StatusCode::UNPROCESSABLE_ENTITY, reason));
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
    let target = req.url.clone();
    let started = std::time::Instant::now();
    let mut resp = state.engines.http.fetch(req).await.map_err(|e| {
        // A node whose IP gets banned has to be able to reconstruct what it
        // fetched for peers, and until now the serving side logged NOTHING —
        // success and failure alike. `warn!` for the failure half, at the level
        // the rest of the fetch stack already uses.
        tracing::warn!(target_url = %target, error = %e, "fetch-proxy: fetch on a peer's behalf failed");
        ApiError(PROXY_FETCH_FAILED, format!("proxied fetch failed: {e}"))
    })?;
    // The success half. `info!` deliberately, not `debug!`: this is the record of
    // an outbound request this node made for someone else, and it is the only
    // place that record exists. Target URLs are a privacy surface, so it stays at
    // the level the fetch stack already logs URLs at, not above it.
    tracing::info!(
        target_url = %target,
        status = resp.status,
        bytes = resp.body.len(),
        cache_hit = resp.cache_hit,
        ms = started.elapsed().as_millis() as u64,
        "fetch-proxy: served a fetch on a peer's behalf"
    );
    // Bind the envelope to the request: echo the URL this node was ASKED for, so
    // the coordinator can refuse an answer to a different question. Deliberately
    // not `final_url`, which legitimately differs after a redirect.
    resp.headers
        .insert(REMOTE_TARGET_HEADER.to_string(), target);
    let value = serde_json::to_value(&resp)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(value))
}

#[cfg(test)]
mod tests {
    use super::{absent_profile, secret_matches, PROXY_FETCH_FAILED};
    use std::path::PathBuf;

    /// The anti-pattern: **two retry ladders stacked on the same failure**. The
    /// coordinator POSTs its proxy call through its own `HttpEngine`, so the
    /// status this node picks decides whether that call is retried underneath the
    /// fabric's own failover. With `502` it was — `[http] retries` = 3 — so a
    /// deterministic peer-side failure cost four full proxy attempts (each paying
    /// this node's whole fetch time, with exponential backoff between) before the
    /// coordinator's ladder even started.
    ///
    /// This is the guard, not a comment: it fails if someone puts the status back
    /// to 502, **and** if someone adds this status to the shipped retryable set.
    #[test]
    fn the_proxy_failure_status_is_not_one_a_coordinator_will_retry() {
        let shipped = pumper_core::config::HttpConfig::default().retryable_statuses;
        assert!(
            !shipped.contains(&PROXY_FETCH_FAILED.as_u16()),
            "/fetch-proxy answers {} for a failed proxied fetch, but the default \
             [http] retryable_statuses is {shipped:?} — a coordinator's transport would retry \
             this node {} times before its own failover ladder ran",
            PROXY_FETCH_FAILED.as_u16(),
            pumper_core::config::HttpConfig::default().retries + 1,
        );
        // And it is still an error status, so the coordinator's `!is_success()`
        // check keeps treating it as "this node failed".
        assert!(PROXY_FETCH_FAILED.is_client_error() || PROXY_FETCH_FAILED.is_server_error());
    }

    #[test]
    fn secret_comparison_is_exact() {
        assert!(secret_matches("sesame", "sesame"));
        assert!(!secret_matches("", "sesame"));
        assert!(!secret_matches("sesam", "sesame"));
        assert!(!secret_matches("sesamee", "sesame"));
    }

    /// A scratch profiles root that is removed when the test ends.
    struct Vault(PathBuf);
    impl Vault {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("pumper-remote-vault-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create scratch vault");
            Self(dir)
        }
        fn with_profile(self, name: &str) -> Self {
            let dir = self.0.join(name);
            std::fs::create_dir_all(&dir).expect("create profile dir");
            std::fs::write(dir.join("cookies.json"), "[]").expect("write jar");
            self
        }
    }
    impl Drop for Vault {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The anti-pattern: **create-on-first-use applied to someone else's
    /// session**. `engine-http` opens a missing `cookies.json` as an empty jar
    /// and says nothing, which is the right default for a local operator logging
    /// in by hand and a data-integrity failure on a route a *peer* drives: the
    /// node answers 200 with the logged-out page and nothing downstream can tell.
    #[test]
    fn an_unknown_profile_is_refused_not_served_from_an_empty_jar() {
        let vault = Vault::new("unknown").with_profile("acme");

        // Held here: serving it is legitimate.
        assert_eq!(absent_profile(&vault.0, Some("acme")), None);
        // No profile at all: nothing to be wrong about.
        assert_eq!(absent_profile(&vault.0, None), None);

        let why = absent_profile(&vault.0, Some("other")).expect("an unheld profile is refused");
        assert!(why.contains("not present on this node"), "{why}");
        assert!(
            why.contains("EMPTY"),
            "the refusal must say what serving it would actually return: {why}"
        );
        // A profile dir with no jar is still not a session.
        std::fs::create_dir_all(vault.0.join("empty-shell")).unwrap();
        assert!(absent_profile(&vault.0, Some("empty-shell")).is_some());
    }

    /// A name that cannot even be turned into a path is refused as such, rather
    /// than falling through to "no profile" and fetching anonymously.
    #[test]
    fn an_unusable_profile_name_is_refused_rather_than_ignored() {
        let vault = Vault::new("badname");
        for name in ["../../etc", "", "a/b"] {
            assert!(
                absent_profile(&vault.0, Some(name)).is_some(),
                "{name:?} must be refused"
            );
        }
    }
}
