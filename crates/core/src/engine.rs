//! Engine capability traits. Apps depend only on these; concrete engines
//! (`engine-http`, `engine-browser`, `engine-claude`) implement them, and the
//! server wires everything together into an [`EngineSet`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Result};

// ---- Session vault: named login profiles (phase 1) -------------------------
//
// A *profile* is a named, persistent identity a fetch can run under. It lives in
// its own directory under `[fetcher] profiles_dir` (default `data/profiles`),
// created on first use:
//
//   data/profiles/<name>/cookies.json   persistent HTTP cookie jar (engine-http)
//   data/profiles/<name>/browser/       Chrome user-data-dir      (engine-browser)
//
// The name is the only untrusted input in that path, so it is validated to a
// path-safe alphabet before it is ever joined onto a directory. Phase 1 stores
// session state only — there is no credential management or at-rest encryption.

/// Cookie-jar file inside a profile dir.
pub const PROFILE_COOKIES_FILE: &str = "cookies.json";
/// Chrome user-data-dir inside a profile dir.
pub const PROFILE_BROWSER_DIR: &str = "browser";
/// Max profile-name length (keeps paths sane on every platform).
pub const PROFILE_NAME_MAX_LEN: usize = 64;

/// Accepts only path-safe profile names: 1..=64 chars of ASCII alphanumerics,
/// `-`, or `_`. Everything else — separators, `.`/`..`, drive letters, spaces,
/// non-ASCII — is rejected with a typed [`Error::Profile`], so a name can never
/// escape `profiles_dir`.
pub fn validate_profile_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Profile("name must not be empty".into()));
    }
    if name.len() > PROFILE_NAME_MAX_LEN {
        return Err(Error::Profile(format!(
            "name '{name}' is longer than {PROFILE_NAME_MAX_LEN} chars"
        )));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '-' || *c == '_'))
    {
        return Err(Error::Profile(format!(
            "name '{name}' contains {bad:?}; only ASCII letters, digits, '-' and '_' are allowed"
        )));
    }
    Ok(())
}

/// `<profiles_dir>/<name>` for a validated name.
pub fn profile_dir(profiles_dir: &Path, name: &str) -> Result<PathBuf> {
    validate_profile_name(name)?;
    Ok(profiles_dir.join(name))
}

/// `<profiles_dir>/<name>/cookies.json` — the HTTP tier's persistent jar.
pub fn profile_cookies_path(profiles_dir: &Path, name: &str) -> Result<PathBuf> {
    Ok(profile_dir(profiles_dir, name)?.join(PROFILE_COOKIES_FILE))
}

/// `<profiles_dir>/<name>/browser` — the browser tier's Chrome user-data-dir.
pub fn profile_browser_dir(profiles_dir: &Path, name: &str) -> Result<PathBuf> {
    Ok(profile_dir(profiles_dir, name)?.join(PROFILE_BROWSER_DIR))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    #[default]
    Get,
    Post,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRequest {
    pub url: String,
    #[serde(default)]
    pub method: HttpMethod,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
    /// Skip the response cache for this request (always hit the network).
    #[serde(default)]
    pub no_cache: bool,
    /// Override the response-cache TTL (seconds). On write it sets how long the
    /// stored response stays fresh; on read it also caps accepted staleness, so a
    /// caller asking for `<=N`-second-old content is never served a longer-lived
    /// entry another caller wrote. `None` uses the configured `[cache] ttl_secs`.
    /// Not part of the cache key (it shapes freshness, not the answer) and ignored
    /// when uncacheable.
    #[serde(default)]
    pub ttl_override: Option<u64>,
    /// Conditional GET validator: sent as `If-None-Match` so the origin can
    /// answer `304 Not Modified` (empty body) when the resource is unchanged.
    /// Powers incremental recrawl / change-monitoring. Usually paired with
    /// `no_cache` so the request actually revalidates instead of being served
    /// from the local TTL cache.
    #[serde(default)]
    pub etag: Option<String>,
    /// Conditional GET validator: sent as `If-Modified-Since` (an HTTP-date
    /// string, typically the origin's prior `Last-Modified`). Same 304 contract
    /// as `etag`.
    #[serde(default)]
    pub if_modified_since: Option<String>,
    /// Per-request response body cap (bytes). Overrides `[http] max_body_bytes`.
    /// A response whose streamed body exceeds this is rejected with a typed error
    /// naming the cap and URL (guards against unbounded/hostile bodies). `None`
    /// uses the configured default.
    #[serde(default)]
    pub max_body_bytes: Option<u64>,
    /// Per-request timeout (seconds) applied to each attempt. Overrides the
    /// client-global `[http] timeout_secs` for this request only. `None` uses the
    /// global timeout.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Per-request proxy override (`http`/`https`/`socks5` URL, optional
    /// `user:pass@` auth). Routes just this request through the given proxy
    /// instead of `[http] proxy`. Served from a small bounded client pool since
    /// reqwest binds a proxy at client-build time. `None` uses the configured
    /// default (or no proxy).
    #[serde(default)]
    pub proxy: Option<String>,
    /// Session-vault profile to run this request under: it is served by a client
    /// bound to `<profiles_dir>/<name>/cookies.json`, a **persistent** cookie jar
    /// that survives restarts (the default client's jar is in-memory and dies
    /// with the process). `None` = exactly the previous behavior. An invalid name
    /// yields a typed [`Error::Profile`].
    #[serde(default)]
    pub profile: Option<String>,
}

impl HttpRequest {
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: HttpMethod::Get,
            headers: HashMap::new(),
            body: None,
            no_cache: false,
            ttl_override: None,
            etag: None,
            if_modified_since: None,
            max_body_bytes: None,
            timeout_secs: None,
            proxy: None,
            profile: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
    pub final_url: String,
    /// Whether this response was served from the HTTP cache rather than the
    /// network. Set by the engine; surfaced in the tiered-fetch trace so callers
    /// can distinguish a cache hit from a live fetch.
    pub cache_hit: bool,
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderRequest {
    pub url: String,
    /// CSS selector to wait for before capturing the DOM.
    #[serde(default)]
    pub wait_for_selector: Option<String>,
    /// Extra settle time; falls back to the configured default.
    #[serde(default)]
    pub extra_wait_ms: Option<u64>,
    /// JS expression evaluated after load; its JSON result lands in
    /// [`RenderedPage::evaluated`].
    #[serde(default)]
    pub evaluate: Option<String>,
    /// Opt this render out of resource blocking (`[browser] block_resources`):
    /// load images/fonts/media too. Ignored when blocking is disabled globally.
    #[serde(default)]
    pub load_all_resources: bool,
    /// Session-vault profile to render under: Chrome is acquired with
    /// `<profiles_dir>/<name>/browser` as its user-data-dir, so that profile's
    /// logins/cookies are in effect. `None` renders on the shared default
    /// instance (`[browser] user_data_dir`) — exactly the previous behavior.
    #[serde(default)]
    pub profile: Option<String>,
    /// Cap on the captured HTML size (bytes); over-cap renders fail instead of
    /// buffering an unbounded DOM. `None` falls back to `[browser] max_html_bytes`
    /// — the browser-tier mirror of `HttpRequest.max_body_bytes`.
    #[serde(default)]
    pub max_body_bytes: Option<u64>,
}

impl RenderRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            wait_for_selector: None,
            extra_wait_ms: None,
            evaluate: None,
            load_all_resources: false,
            profile: None,
            max_body_bytes: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RenderedPage {
    pub html: String,
    pub final_url: Option<String>,
    pub evaluated: Option<Value>,
    /// `true` when the navigation-wait deadline elapsed and the DOM was captured
    /// mid-load — the HTML may be partial. Distinguishes an honest timeout from a
    /// clean load.
    pub nav_timed_out: bool,
    /// Outcome of a `wait_for_selector`: `Some(true)` the selector appeared,
    /// `Some(false)` it never did before the deadline, `None` no selector was
    /// requested.
    pub selector_found: Option<bool>,
    /// Count of subresources (images/fonts/media) dropped by request interception
    /// for this render. `0` when blocking is off or the render opted out.
    pub blocked_resources: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResearchRequest {
    pub prompt: String,
    #[serde(default)]
    pub append_system_prompt: Option<String>,
    #[serde(default)]
    pub max_turns: Option<u32>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Named preset from `[claude.roles]` — e.g. "research" or "compose".
    #[serde(default)]
    pub role: Option<String>,
    /// Explicit model id/alias; overrides the role and config default.
    #[serde(default)]
    pub model: Option<String>,
    /// Explicit reasoning effort (low|medium|high|xhigh|max); overrides role.
    #[serde(default)]
    pub effort: Option<String>,
    /// Hard spend ceiling for this run.
    #[serde(default)]
    pub max_budget_usd: Option<f64>,
    /// Resume a prior CLI session id for multi-step research pipelines.
    #[serde(default)]
    pub resume_session: Option<String>,
    /// Constrain the final answer to this JSON schema (`--json-schema`).
    #[serde(default)]
    pub json_schema: Option<Value>,
}

impl ResearchRequest {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self { prompt: prompt.into(), ..Default::default() }
    }

    /// Selects a named role preset (e.g. "research", "compose").
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResearchOutput {
    /// Final response text from the agent.
    pub text: String,
    /// Populated when the response parses as JSON (fenced JSON is unwrapped).
    pub json: Option<Value>,
    pub cost_usd: Option<f64>,
    pub duration_ms: Option<u64>,
    pub num_turns: Option<u64>,
    pub session_id: Option<String>,
}

/// Plain HTTP fetching — fast path for server-rendered pages and APIs.
#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse>;
}

/// Headless-browser rendering — JS-heavy pages, logged-in sessions.
#[async_trait]
pub trait Browser: Send + Sync {
    async fn render(&self, req: RenderRequest) -> Result<RenderedPage>;
}

/// Agentic web research via Claude Code CLI.
#[async_trait]
pub trait Researcher: Send + Sync {
    async fn research(&self, req: ResearchRequest) -> Result<ResearchOutput>;
}

/// Everything an app can scrape with, handed over via [`crate::AppContext`].
pub struct EngineSet {
    pub http: Arc<dyn HttpClient>,
    pub browser: Arc<dyn Browser>,
    pub claude: Arc<dyn Researcher>,
    /// Tiered fetcher that picks/escalates engines automatically.
    pub fetch: crate::fetcher::Fetcher,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_request_conditional_validators_are_serde_defaulted() {
        // Older payloads (and the common case) omit the conditional fields.
        let req: HttpRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(req.etag.is_none());
        assert!(req.if_modified_since.is_none());
        // When present they round-trip.
        let req2: HttpRequest = serde_json::from_str(
            r#"{"url":"https://x/","etag":"\"abc\"","if_modified_since":"Wed, 21 Oct 2025 07:28:00 GMT"}"#,
        )
        .unwrap();
        assert_eq!(req2.etag.as_deref(), Some("\"abc\""));
        assert_eq!(req2.if_modified_since.as_deref(), Some("Wed, 21 Oct 2025 07:28:00 GMT"));
        // The convenience constructor leaves them unset.
        assert!(HttpRequest::get("https://x/").etag.is_none());
    }

    #[test]
    fn profile_is_serde_defaulted_on_every_request_type() {
        // None = today's behavior; omitted from older payloads.
        let h: HttpRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(h.profile.is_none());
        let r: RenderRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(r.profile.is_none());
        // Present => round-trips.
        let h2: HttpRequest =
            serde_json::from_str(r#"{"url":"https://x/","profile":"acme_login"}"#).unwrap();
        assert_eq!(h2.profile.as_deref(), Some("acme_login"));
        let r2: RenderRequest =
            serde_json::from_str(r#"{"url":"https://x/","profile":"acme_login"}"#).unwrap();
        assert_eq!(r2.profile.as_deref(), Some("acme_login"));
        assert!(HttpRequest::get("https://x/").profile.is_none());
        assert!(RenderRequest::new("https://x/").profile.is_none());
    }

    #[test]
    fn profile_names_accept_only_the_path_safe_alphabet() {
        for ok in ["a", "acme", "acme-login", "acme_login_2", "A1", &"x".repeat(64)] {
            assert!(validate_profile_name(ok).is_ok(), "{ok:?} should be accepted");
        }
        // Traversal, separators, and anything else are typed errors.
        for bad in [
            "", "..", ".", "a/b", "a\\b", "a.b", "C:", "a b", "naïve", "a:b", "-*-",
            &"x".repeat(65),
        ] {
            let err = validate_profile_name(bad).unwrap_err();
            assert!(matches!(err, Error::Profile(_)), "{bad:?} => {err:?}");
        }
    }

    #[test]
    fn profile_paths_stay_inside_the_profiles_dir() {
        let root = Path::new("data/profiles");
        assert_eq!(profile_dir(root, "acme").unwrap(), root.join("acme"));
        assert_eq!(
            profile_cookies_path(root, "acme").unwrap(),
            root.join("acme").join("cookies.json")
        );
        assert_eq!(
            profile_browser_dir(root, "acme").unwrap(),
            root.join("acme").join("browser")
        );
        // A traversal attempt never produces a path at all.
        assert!(profile_cookies_path(root, "../../etc").is_err());
        assert!(profile_browser_dir(root, "..").is_err());
    }
}
