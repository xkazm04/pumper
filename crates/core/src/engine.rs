//! Engine capability traits. Apps depend only on these; concrete engines
//! (`engine-http`, `engine-browser`, `engine-claude`) implement them, and the
//! server wires everything together into an [`EngineSet`].

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Result;

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
    /// Override the response-cache TTL (seconds) when this response is stored.
    /// `None` uses the configured `[cache] ttl_secs`. Not part of the cache key
    /// (it shapes freshness, not the answer) and ignored when uncacheable.
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
    /// can distinguish a cache hit from a live fetch. `#[serde(default)]` keeps
    /// older serialized responses (which predate the field) deserializable.
    #[serde(default)]
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
}

impl RenderRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            wait_for_selector: None,
            extra_wait_ms: None,
            evaluate: None,
            load_all_resources: false,
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
    /// clean load. Serde-defaulted so older payloads deserialize.
    #[serde(default)]
    pub nav_timed_out: bool,
    /// Outcome of a `wait_for_selector`: `Some(true)` the selector appeared,
    /// `Some(false)` it never did before the deadline, `None` no selector was
    /// requested. Serde-defaulted for compatibility.
    #[serde(default)]
    pub selector_found: Option<bool>,
    /// Count of subresources (images/fonts/media) dropped by request interception
    /// for this render. `0` when blocking is off or the render opted out. Serde-
    /// defaulted for compatibility.
    #[serde(default)]
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
}
