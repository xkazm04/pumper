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
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
    pub final_url: String,
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
}

impl RenderRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), wait_for_selector: None, extra_wait_ms: None, evaluate: None }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RenderedPage {
    pub html: String,
    pub final_url: Option<String>,
    pub evaluated: Option<Value>,
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
