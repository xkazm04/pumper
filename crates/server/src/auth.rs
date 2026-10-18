//! N20 identity & tenancy plane: the one tower layer that turns an anonymous
//! request into a **principal**.
//!
//! Until this landed the HTTP surface had no identity concept at all — the only
//! credential anywhere was the ingress HMAC, which authenticates a *webhook
//! sender*, not an operator (`routes/datasets.rs`'s delete gate says so in
//! prose). Everything mutating, including the paths that spend real money
//! through the Claude engine, was open to anything that could reach the
//! listener.
//!
//! **The default is still exactly that.** `[auth] mode = "open"` (the default,
//! and what an absent `[auth]` section means) resolves a synthetic `operator`
//! principal carrying every scope and no ceiling, consults no table and
//! throttles nothing, so a node that never edits its config behaves byte for
//! byte as it did before. Auth exists only once an operator flips the key —
//! the same posture `[ingress]`, `[remote]` and `[mcp]` already take.
//!
//! In `keys` mode every non-public request must present a key
//! (`Authorization: Bearer <key>` or `x-pumper-key: <key>`), which is resolved
//! by SHA-256 digest, checked against the route's required scope, run through
//! the principal's token bucket, and — for the enqueue door, the one that
//! spends — checked against the principal's daily ceiling. The resolved
//! principal is stamped into a request extension so the doors downstream can
//! attribute the work they create.
//!
//! Three routes stay unauthenticated in BOTH modes: `/health`, `/metrics` and
//! `/openapi.json`. A liveness probe that needs a credential is a liveness
//! probe that reports the credential's health, and a spec you cannot read
//! without a key cannot be used to obtain one.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;
use chrono::{Duration, Utc};

use crate::state::AppState;

/// The id the synthetic `open`-mode principal reports. Deliberately NOT a UUID:
/// it can never collide with a real `principals.id`, and a row that somehow
/// carried it would be visibly not a stored principal.
pub(crate) const OPERATOR_PRINCIPAL_ID: &str = "operator";

/// Routes that answer without a credential in every mode.
///
/// Kept as an explicit list rather than a prefix rule: a prefix (`/health*`)
/// would silently exempt any future route that happened to start the same way,
/// and this list is the whole unauthenticated surface of the server.
pub(crate) const PUBLIC_PATHS: &[&str] = &["/health", "/metrics", "/openapi.json"];

// ── scopes ───────────────────────────────────────────────────────────────────

/// The scope string granting everything.
pub(crate) const SCOPE_ADMIN: &str = "admin";
/// The scope string granting read-only access to the non-admin surface.
pub(crate) const SCOPE_READ: &str = "read";
/// Prefix of the per-app enqueue scope (`enqueue:hackernews`, `enqueue:*`).
pub(crate) const SCOPE_ENQUEUE_PREFIX: &str = "enqueue:";

/// What a given route demands of a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Requirement {
    /// No credential at all — the three public routes.
    Public,
    /// Any read of the non-admin surface.
    Read,
    /// Creating work for one named app (the door that spends).
    Enqueue(String),
    /// Every other mutation, plus the identity surface itself.
    Admin,
}

/// The scope a route requires, from its method and path alone.
///
/// Pure and total: an unknown path is **not** unguarded. A route this function
/// has never heard of falls into `Read` when it only reads and `Admin` when it
/// mutates, so a route added by another change is protected before anyone
/// remembers this file exists. The anti-pattern is an allow-list of guarded
/// paths, where the default for anything new is "open".
pub(crate) fn required_scope(method: &Method, path: &str) -> Requirement {
    if is_public(path) {
        return Requirement::Public;
    }
    let identity_surface =
        path == "/audit" || path == "/principals" || path.starts_with("/principals/");
    if !is_mutating(method) {
        return if identity_surface {
            Requirement::Admin
        } else {
            Requirement::Read
        };
    }
    match enqueue_app(path) {
        Some(app) => Requirement::Enqueue(app.to_string()),
        None => Requirement::Admin,
    }
}

/// The app named by the enqueue door's path (`/apps/{name}/jobs`), if this is
/// that path. Anything else — including `/apps/{name}/datasets` — is `None`.
fn enqueue_app(path: &str) -> Option<&str> {
    let mut segments = path.split('/').filter(|s| !s.is_empty());
    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("apps"), Some(app), Some("jobs"), None) if !app.is_empty() => Some(app),
        _ => None,
    }
}

/// Whether a method changes state. `OPTIONS`/`HEAD` sit with `GET`: a CORS
/// preflight that had to carry a key could never be sent by a browser.
pub(crate) fn is_mutating(method: &Method) -> bool {
    !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// Whether a path is served without any credential.
pub(crate) fn is_public(path: &str) -> bool {
    PUBLIC_PATHS.contains(&path)
}

/// Whether a principal's scopes satisfy a requirement.
///
/// `admin` implies everything; `read` implies only reads; an `enqueue:` grant
/// implies **only** that app's enqueue door — deliberately not read, so a key
/// minted to run one pipeline cannot also export every dataset on the node.
pub(crate) fn scope_satisfies(scopes: &[String], requirement: &Requirement) -> bool {
    let has = |want: &str| scopes.iter().any(|s| s == want);
    match requirement {
        Requirement::Public => true,
        Requirement::Admin => has(SCOPE_ADMIN),
        Requirement::Read => has(SCOPE_ADMIN) || has(SCOPE_READ),
        Requirement::Enqueue(app) => {
            has(SCOPE_ADMIN)
                || has(&format!("{SCOPE_ENQUEUE_PREFIX}*"))
                || has(&format!("{SCOPE_ENQUEUE_PREFIX}{app}"))
        }
    }
}

/// The scope set the synthetic `open`-mode operator carries: `admin`, which by
/// the rule above satisfies every requirement there is.
pub(crate) fn operator_scopes() -> Vec<String> {
    vec![SCOPE_ADMIN.to_string()]
}

/// Refuses a scope string this server would never grant, so a typo becomes a
/// 400 at creation instead of a key that silently authorizes nothing.
///
/// The anti-pattern: accepting free-form scope strings. `enqueue-*`, `Admin`
/// and `write` all *look* like grants and all grant nothing, and the failure
/// surfaces later as an inexplicable 403 on a key the operator believes is
/// correct.
pub(crate) fn validate_scope(scope: &str) -> Result<(), String> {
    if scope == SCOPE_ADMIN || scope == SCOPE_READ {
        return Ok(());
    }
    match scope.strip_prefix(SCOPE_ENQUEUE_PREFIX) {
        Some(app) if !app.is_empty() => Ok(()),
        _ => Err(format!(
            "unknown scope '{scope}'. Accepted: '{SCOPE_ADMIN}', '{SCOPE_READ}', \
             '{SCOPE_ENQUEUE_PREFIX}<app>' or '{SCOPE_ENQUEUE_PREFIX}*'"
        )),
    }
}

// ── keys ─────────────────────────────────────────────────────────────────────

/// SHA-256 hex digest of a presented key. The only form of a key this service
/// ever stores.
pub(crate) fn hash_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

/// A fresh 256-bit key, in the same shape `create_ingress_source` mints its
/// signing secrets (two UUIDs' worth of entropy, hex, no separators).
pub(crate) fn generate_key() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// The key a request presents, from `Authorization: Bearer <key>` or
/// `x-pumper-key: <key>`.
///
/// `Authorization` wins when both are present — it is the standard header, and
/// silently preferring the vendor one would make a correct `Authorization`
/// unusable behind a proxy that injects its own `x-pumper-key`. The scheme
/// match is case-insensitive (`bearer` is legal); an empty or whitespace-only
/// credential is `None`, never an empty-string key that could match an empty
/// digest.
pub(crate) fn presented_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        let (scheme, rest) = value.split_once(' ')?;
        if scheme.eq_ignore_ascii_case("bearer") {
            let key = rest.trim();
            return (!key.is_empty()).then(|| key.to_string());
        }
        return None;
    }
    let key = headers.get("x-pumper-key")?.to_str().ok()?.trim();
    (!key.is_empty()).then(|| key.to_string())
}

// ── the resolved caller ──────────────────────────────────────────────────────

/// The principal a request resolved to, stamped into the request extensions.
///
/// Downstream doors read this to attribute the work they create. In `open` mode
/// it is the synthetic operator and `id` is [`OPERATOR_PRINCIPAL_ID`] — which is
/// **not** a `principals.id`, so [`Self::stored_id`] is what a foreign column
/// must be given: `None` there means "no caller was recorded", which is the
/// truth about every job an `open`-mode node enqueues.
#[derive(Debug, Clone)]
pub(crate) struct CallerPrincipal {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) scopes: Vec<String>,
    /// True for the `open`-mode operator: an identity this server invented, not
    /// one it authenticated.
    pub(crate) synthetic: bool,
}

impl CallerPrincipal {
    /// The synthetic operator every request resolves to in `open` mode.
    pub(crate) fn operator() -> Self {
        Self {
            id: OPERATOR_PRINCIPAL_ID.to_string(),
            name: OPERATOR_PRINCIPAL_ID.to_string(),
            scopes: operator_scopes(),
            synthetic: true,
        }
    }

    /// The id to write into a `principal_id` column — `None` for the synthetic
    /// operator, because inventing a caller row for it would make every legacy
    /// and every `open`-mode job look attributed when it is not.
    pub(crate) fn stored_id(&self) -> Option<&str> {
        (!self.synthetic).then_some(self.id.as_str())
    }
}

// ── per-principal throttle ───────────────────────────────────────────────────

/// Bucket state per principal id: (tokens remaining, last refill instant).
/// Process-global and not persisted, exactly like the ingress buckets: a
/// restart refills every bucket, which for a politeness rail is fine.
static BUCKETS: OnceLock<Mutex<HashMap<String, (f64, Instant)>>> = OnceLock::new();

/// Consumes one token for `principal_id`, creating a full bucket on first
/// sight. `per_min == 0` means "no throttle" and always allows.
fn rate_limit_allow(principal_id: &str, per_min: u32) -> bool {
    if per_min == 0 {
        return true;
    }
    let now = Instant::now();
    let mut buckets = crate::routes::lock_advisory(
        BUCKETS.get_or_init(|| Mutex::new(HashMap::new())),
        "auth_rate_buckets",
    );
    let entry = buckets
        .entry(principal_id.to_string())
        .or_insert((per_min as f64, now));
    let elapsed = now.duration_since(entry.1).as_secs_f64();
    let (tokens, allowed) = crate::routes::bucket_step(entry.0, elapsed, per_min);
    *entry = (tokens, now);
    allowed
}

/// The throttle that applies to a principal: its own, else the configured
/// fallback. A stored `0`/negative is treated as "no throttle" rather than as
/// "refuse everything" — a rate limit of zero requests is a disable switch, and
/// `enabled` is already that switch.
pub(crate) fn effective_rate_limit(row: Option<i64>, fallback: u32) -> u32 {
    match row {
        Some(n) if n > 0 => u32::try_from(n).unwrap_or(u32::MAX),
        Some(_) => 0,
        None => fallback,
    }
}

// ── the layer ────────────────────────────────────────────────────────────────

/// Wraps a router in the identity layer. Applied in `routes::router` so every
/// caller of it — the server and every e2e test — drives the same stack.
pub fn with_auth(router: Router<AppState>, state: AppState) -> Router<AppState> {
    router.layer(axum::middleware::from_fn_with_state(state, auth_layer))
}

/// One refusal, in the service's standard `{error, code}` envelope.
fn refuse(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        axum::Json(serde_json::json!({
            "error": message.into(),
            "code": crate::routes::error_code(status),
        })),
    )
        .into_response()
}

async fn auth_layer(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let requirement = required_scope(&method, &path);

    // The public trio, in both modes, before anything is read or looked up.
    if requirement == Requirement::Public {
        return next.run(req).await;
    }

    let (caller, refusal) = if state.config.auth.keys_required() {
        match resolve(&state, req.headers(), &requirement).await {
            Ok(caller) => (Some(caller), None),
            Err(response) => (None, Some(response)),
        }
    } else {
        (Some(CallerPrincipal::operator()), None)
    };

    let audited = state.config.auth.audit && is_mutating(&method);
    let principal_id = caller
        .as_ref()
        .and_then(|c| c.stored_id())
        .map(String::from);
    let principal_name = caller.as_ref().map(|c| c.name.clone());

    let response = match refusal {
        Some(response) => response,
        None => {
            if let Some(caller) = caller {
                req.extensions_mut().insert(caller);
            }
            next.run(req).await
        }
    };

    if audited {
        let detail = serde_json::json!({
            "status": response.status().as_u16(),
            "mode": state.config.auth.effective_mode(),
            "principal_name": principal_name,
        })
        .to_string();
        // Auditing must never be able to fail the request it is recording: the
        // work already happened, and answering 500 because the ledger write
        // failed would turn an observability gap into a data-loss report.
        if let Err(e) = state
            .storage
            .record_audit(
                principal_id.as_deref(),
                &format!("{method} {path}"),
                Some(&path),
                Some(&detail),
            )
            .await
        {
            tracing::warn!(error = %e, action = %format!("{method} {path}"), "audit row not recorded");
        }
    }
    response
}

/// Resolves the presented credential in `keys` mode, or the refusal it earns.
async fn resolve(
    state: &AppState,
    headers: &HeaderMap,
    requirement: &Requirement,
) -> Result<CallerPrincipal, Response> {
    let Some(key) = presented_key(headers) else {
        return Err(refuse(
            StatusCode::UNAUTHORIZED,
            "missing API key — send 'Authorization: Bearer <key>' or 'x-pumper-key: <key>'",
        ));
    };
    let digest = hash_key(&key);
    let principal = match state.storage.principal_by_key_hash(&digest).await {
        Ok(Some(p)) => p,
        Ok(None) => {
            return Err(refuse(StatusCode::UNAUTHORIZED, "unknown API key"));
        }
        Err(e) => {
            // A store that cannot answer "who is this" must not be read as
            // "nobody", which would be a 401 storm, nor as "anybody".
            tracing::error!(error = %e, "principal lookup failed");
            return Err(refuse(
                StatusCode::INTERNAL_SERVER_ERROR,
                crate::routes::INTERNAL_MESSAGE,
            ));
        }
    };
    if !principal.enabled {
        return Err(refuse(StatusCode::FORBIDDEN, "API key is disabled"));
    }
    let per_min = effective_rate_limit(
        principal.rate_limit_per_min,
        state.config.auth.default_rate_limit_per_min,
    );
    if !rate_limit_allow(&principal.id, per_min) {
        return Err(refuse(
            StatusCode::TOO_MANY_REQUESTS,
            format!("rate limit exceeded ({per_min}/min for this key) — back off and retry"),
        ));
    }
    if !scope_satisfies(&principal.scopes, requirement) {
        return Err(refuse(
            StatusCode::FORBIDDEN,
            format!(
                "this key does not carry the scope this route requires ({})",
                describe(requirement)
            ),
        ));
    }
    // The daily ceiling is checked at the door that spends, beside the job's own
    // `validate_budget_usd`: a read cannot exhaust a budget, and charging the
    // check to every GET would put a ledger aggregate on the hot read path.
    if matches!(requirement, Requirement::Enqueue(_)) {
        if let Some(cap) = principal.budget_usd_per_day {
            let since = Utc::now() - Duration::hours(24);
            match state
                .costs
                .principal_total_since(&principal.id, since)
                .await
            {
                Ok(spent) if spent >= cap => {
                    return Err(refuse(
                        StatusCode::PAYMENT_REQUIRED,
                        format!(
                            "this key has spent ${spent:.4} of its ${cap:.4} daily ceiling in the \
                             last 24h — deterministic, retrying re-reads the same ledger"
                        ),
                    ));
                }
                Ok(_) => {}
                Err(e) => {
                    // Unknown spend must not be read as zero spend: a ceiling
                    // that fails open is not a ceiling.
                    tracing::error!(error = %e, "principal spend lookup failed");
                    return Err(refuse(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        crate::routes::INTERNAL_MESSAGE,
                    ));
                }
            }
        }
    }
    Ok(CallerPrincipal {
        id: principal.id,
        name: principal.name,
        scopes: principal.scopes,
        synthetic: false,
    })
}

/// The scope string a requirement asks for, for the 403 message.
fn describe(requirement: &Requirement) -> String {
    match requirement {
        Requirement::Public => "none".to_string(),
        Requirement::Read => SCOPE_READ.to_string(),
        Requirement::Admin => SCOPE_ADMIN.to_string(),
        Requirement::Enqueue(app) => format!("{SCOPE_ENQUEUE_PREFIX}{app}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn scopes(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    // ── the route → scope map ────────────────────────────────────────────────

    #[test]
    fn public_paths_need_no_credential_in_either_mode() {
        for path in PUBLIC_PATHS {
            assert_eq!(
                required_scope(&Method::GET, path),
                Requirement::Public,
                "{path} must stay unauthenticated"
            );
        }
    }

    /// The anti-pattern: a prefix rule. `/health` is public; a future
    /// `/health/secrets` must not inherit that.
    #[test]
    fn public_is_exact_not_a_prefix() {
        assert_eq!(
            required_scope(&Method::GET, "/health/secrets"),
            Requirement::Read
        );
        assert_eq!(
            required_scope(&Method::GET, "/metrics-internal"),
            Requirement::Read
        );
    }

    #[test]
    fn enqueue_door_requires_that_apps_scope() {
        assert_eq!(
            required_scope(&Method::POST, "/apps/hackernews/jobs"),
            Requirement::Enqueue("hackernews".to_string())
        );
    }

    /// The anti-pattern this defends: `path.starts_with("/apps/")` as the
    /// enqueue test, which would have handed the app-scoped key every other
    /// route under `/apps` too.
    #[test]
    fn only_the_jobs_door_is_an_enqueue_not_every_apps_path() {
        assert_eq!(
            required_scope(&Method::GET, "/apps/hackernews/datasets"),
            Requirement::Read
        );
        assert_eq!(
            required_scope(&Method::POST, "/apps/hackernews/jobs/extra"),
            Requirement::Admin
        );
    }

    /// A route this map has never heard of is guarded, not open: reads land on
    /// `read`, mutations on `admin`.
    #[test]
    fn unknown_route_is_guarded_not_open() {
        assert_eq!(
            required_scope(&Method::GET, "/some/future/route"),
            Requirement::Read
        );
        assert_eq!(
            required_scope(&Method::DELETE, "/some/future/route"),
            Requirement::Admin
        );
        assert_eq!(
            required_scope(&Method::PATCH, "/some/future/route"),
            Requirement::Admin
        );
    }

    #[test]
    fn identity_surface_is_admin_even_to_read() {
        for path in ["/principals", "/principals/abc", "/audit"] {
            assert_eq!(
                required_scope(&Method::GET, path),
                Requirement::Admin,
                "{path} lists who may call this node"
            );
        }
    }

    // ── scope satisfaction ───────────────────────────────────────────────────

    #[test]
    fn admin_satisfies_everything() {
        let s = scopes(&["admin"]);
        assert!(scope_satisfies(&s, &Requirement::Read));
        assert!(scope_satisfies(&s, &Requirement::Admin));
        assert!(scope_satisfies(&s, &Requirement::Enqueue("any".into())));
    }

    #[test]
    fn read_does_not_satisfy_admin_or_enqueue() {
        let s = scopes(&["read"]);
        assert!(scope_satisfies(&s, &Requirement::Read));
        assert!(!scope_satisfies(&s, &Requirement::Admin));
        assert!(!scope_satisfies(&s, &Requirement::Enqueue("hn".into())));
    }

    /// A key minted to run one pipeline must not also be able to export every
    /// dataset on the node.
    #[test]
    fn enqueue_scope_is_not_a_read_scope() {
        let s = scopes(&["enqueue:hackernews"]);
        assert!(scope_satisfies(
            &s,
            &Requirement::Enqueue("hackernews".into())
        ));
        assert!(!scope_satisfies(
            &s,
            &Requirement::Enqueue("grants-gov".into())
        ));
        assert!(!scope_satisfies(&s, &Requirement::Read));
        assert!(!scope_satisfies(&s, &Requirement::Admin));
    }

    #[test]
    fn enqueue_wildcard_covers_every_app_but_nothing_else() {
        let s = scopes(&["enqueue:*"]);
        assert!(scope_satisfies(
            &s,
            &Requirement::Enqueue("anything".into())
        ));
        assert!(!scope_satisfies(&s, &Requirement::Read));
        assert!(!scope_satisfies(&s, &Requirement::Admin));
    }

    #[test]
    fn empty_scopes_satisfy_nothing_but_public() {
        let s: Vec<String> = Vec::new();
        assert!(scope_satisfies(&s, &Requirement::Public));
        assert!(!scope_satisfies(&s, &Requirement::Read));
        assert!(!scope_satisfies(&s, &Requirement::Admin));
        assert!(!scope_satisfies(&s, &Requirement::Enqueue("x".into())));
    }

    #[test]
    fn operator_satisfies_every_requirement() {
        let s = operator_scopes();
        assert!(scope_satisfies(&s, &Requirement::Read));
        assert!(scope_satisfies(&s, &Requirement::Admin));
        assert!(scope_satisfies(&s, &Requirement::Enqueue("x".into())));
    }

    // ── scope validation ─────────────────────────────────────────────────────

    #[test]
    fn validate_scope_accepts_the_vocabulary_and_refuses_lookalikes() {
        for good in ["admin", "read", "enqueue:*", "enqueue:hackernews"] {
            assert!(validate_scope(good).is_ok(), "{good} is a real scope");
        }
        for bad in ["Admin", "write", "enqueue", "enqueue:", "enqueue-*", ""] {
            assert!(
                validate_scope(bad).is_err(),
                "{bad:?} grants nothing and must be refused at creation"
            );
        }
    }

    // ── credential extraction ────────────────────────────────────────────────

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            let name = axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap();
            h.insert(name, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn reads_bearer_and_vendor_headers() {
        assert_eq!(
            presented_key(&headers(&[("authorization", "Bearer abc123")])).as_deref(),
            Some("abc123")
        );
        assert_eq!(
            presented_key(&headers(&[("authorization", "bearer abc123")])).as_deref(),
            Some("abc123"),
            "the scheme is case-insensitive"
        );
        assert_eq!(
            presented_key(&headers(&[("x-pumper-key", "abc123")])).as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn authorization_wins_over_the_vendor_header() {
        let h = headers(&[("authorization", "Bearer real"), ("x-pumper-key", "proxy")]);
        assert_eq!(presented_key(&h).as_deref(), Some("real"));
    }

    /// The anti-pattern: an empty credential becoming `Some("")`, which then
    /// hashes to a fixed digest a row could be created for.
    #[test]
    fn blank_credentials_are_absent_not_empty_keys() {
        assert!(presented_key(&headers(&[("authorization", "Bearer   ")])).is_none());
        assert!(presented_key(&headers(&[("x-pumper-key", "  ")])).is_none());
        assert!(presented_key(&headers(&[("authorization", "Basic abc")])).is_none());
        assert!(presented_key(&HeaderMap::new()).is_none());
    }

    // ── digests ──────────────────────────────────────────────────────────────

    #[test]
    fn hash_is_sha256_hex_and_key_specific() {
        // The published SHA-256 of "abc".
        assert_eq!(
            hash_key("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_ne!(hash_key("abc"), hash_key("abd"));
        assert_eq!(hash_key("abc").len(), 64);
    }

    #[test]
    fn generated_keys_are_long_and_distinct() {
        let a = generate_key();
        let b = generate_key();
        assert_ne!(a, b);
        assert_eq!(a.len(), 64, "two UUIDs' worth of hex");
    }

    // ── throttle ─────────────────────────────────────────────────────────────

    #[test]
    fn row_limit_wins_over_fallback_and_zero_means_unthrottled() {
        assert_eq!(effective_rate_limit(Some(30), 60), 30);
        assert_eq!(effective_rate_limit(None, 60), 60);
        assert_eq!(effective_rate_limit(None, 0), 0);
        // A stored 0/negative is "no throttle", not "refuse everything" —
        // `enabled` is the disable switch, a rate limit is not.
        assert_eq!(effective_rate_limit(Some(0), 60), 0);
        assert_eq!(effective_rate_limit(Some(-5), 60), 0);
    }

    #[test]
    fn unthrottled_principal_is_never_refused() {
        for _ in 0..100 {
            assert!(rate_limit_allow("unthrottled-principal", 0));
        }
    }

    // ── the caller stamp ─────────────────────────────────────────────────────

    /// The synthetic operator must never be written into a `principal_id`
    /// column: an `open`-mode node's jobs are genuinely unattributed, and a
    /// fabricated caller would make the cost ledger claim otherwise.
    #[test]
    fn synthetic_operator_stores_no_principal_id() {
        assert_eq!(CallerPrincipal::operator().stored_id(), None);
        let real = CallerPrincipal {
            id: "p-1".into(),
            name: "ledgerline".into(),
            scopes: scopes(&["read"]),
            synthetic: false,
        };
        assert_eq!(real.stored_id(), Some("p-1"));
    }
}
