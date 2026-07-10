//! Tiered fetching with automatic escalation. Starts on the cheapest engine
//! that can plausibly work and climbs only when the result looks insufficient:
//!
//!   http  ──(too little content / blocked)──▶  browser  ──(still thin)──▶  claude
//!
//! Apps call `ctx.engines.fetch.fetch(...)` and get back whichever tier
//! succeeded, plus a trail of why each escalation happened.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::engine::{Browser, HttpClient, HttpRequest, RenderRequest, Researcher};
use crate::markdown::html_to_markdown;
use crate::{Error, ResearchRequest, Result};

const DEFAULT_MIN_CONTENT_CHARS: usize = 250;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchStrategy {
    /// Plain HTTP only — never escalate.
    Http,
    /// Headless browser only.
    Browser,
    /// HTTP first, escalate to the browser if the result is thin. (default)
    #[default]
    Auto,
    /// HTTP -> browser -> Claude research if both come back thin.
    AutoWithResearch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub url: String,
    #[serde(default)]
    pub strategy: FetchStrategy,
    /// Browser tier: CSS selector to wait for before capturing.
    #[serde(default)]
    pub wait_for_selector: Option<String>,
    /// Escalate when the extracted text is shorter than this. Defaults to 250.
    #[serde(default)]
    pub min_content_chars: Option<usize>,
    /// Claude tier prompt; defaults to a fetch-and-extract instruction.
    #[serde(default)]
    pub research_prompt: Option<String>,
    /// Spend ceiling for the Claude tier of this fetch (`--max-budget-usd`).
    #[serde(default)]
    pub max_budget_usd: Option<f64>,
    /// Skip the HTTP tier and start at the browser (set by the learned tier
    /// router for hosts where HTTP persistently fails/thins out). Ignored for
    /// the explicit `Http` strategy.
    #[serde(default)]
    pub skip_http: bool,
    /// Also produce clean Markdown alongside the raw HTML.
    #[serde(default)]
    pub to_markdown: bool,
}

impl FetchRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            strategy: FetchStrategy::Auto,
            wait_for_selector: None,
            min_content_chars: None,
            research_prompt: None,
            max_budget_usd: None,
            skip_http: false,
            to_markdown: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchOutcome {
    pub url: String,
    /// Tier that produced the result: "http", "browser", or "claude".
    pub engine: &'static str,
    pub status: Option<u16>,
    pub html: Option<String>,
    pub markdown: Option<String>,
    /// Extracted plain text (Claude tier stores its answer here).
    pub text: Option<String>,
    /// One line per escalation explaining why the previous tier was rejected.
    pub escalations: Vec<String>,
    /// Real money spent on this fetch (Claude tier only; None elsewhere).
    pub cost_usd: Option<f64>,
}

/// Holds clones of the three engines and orchestrates escalation. Cheap to
/// clone (just `Arc`s), so it lives directly inside [`crate::EngineSet`].
#[derive(Clone)]
pub struct Fetcher {
    http: Arc<dyn HttpClient>,
    browser: Arc<dyn Browser>,
    claude: Arc<dyn Researcher>,
}

impl Fetcher {
    pub fn new(
        http: Arc<dyn HttpClient>,
        browser: Arc<dyn Browser>,
        claude: Arc<dyn Researcher>,
    ) -> Self {
        Self { http, browser, claude }
    }

    pub async fn fetch(&self, req: FetchRequest) -> Result<FetchOutcome> {
        let min_chars = req.min_content_chars.unwrap_or(DEFAULT_MIN_CONTENT_CHARS);
        let mut escalations: Vec<String> = Vec::new();

        // --- HTTP tier --- (skip_http only applies to escalating strategies;
        // an explicit Http strategy is the caller's call.)
        let try_http = req.strategy == FetchStrategy::Http
            || (!req.skip_http
                && matches!(req.strategy, FetchStrategy::Auto | FetchStrategy::AutoWithResearch));
        if try_http {
            match self.http.fetch(HttpRequest::get(&req.url)).await {
                Ok(resp) => {
                    let markdown = html_to_markdown(&resp.body);
                    let enough = resp.is_success() && markdown.chars().count() >= min_chars;
                    if enough || req.strategy == FetchStrategy::Http {
                        return Ok(outcome("http", &req, Some(resp.status), resp.body, markdown, escalations));
                    }
                    escalations.push(format!(
                        "http tier thin: status {}, {} chars of text",
                        resp.status,
                        markdown.chars().count()
                    ));
                }
                Err(e) if req.strategy == FetchStrategy::Http => return Err(e),
                Err(e) => escalations.push(format!("http tier failed: {e}")),
            }
        }

        // --- Browser tier ---
        if matches!(req.strategy, FetchStrategy::Browser | FetchStrategy::Auto | FetchStrategy::AutoWithResearch) {
            let mut render = RenderRequest::new(&req.url);
            render.wait_for_selector = req.wait_for_selector.clone();
            match self.browser.render(render).await {
                Ok(page) => {
                    let markdown = html_to_markdown(&page.html);
                    let enough = markdown.chars().count() >= min_chars;
                    if enough || req.strategy != FetchStrategy::AutoWithResearch {
                        return Ok(outcome("browser", &req, None, page.html, markdown, escalations));
                    }
                    escalations.push(format!(
                        "browser tier thin: {} chars of text",
                        markdown.chars().count()
                    ));
                }
                Err(e) if req.strategy == FetchStrategy::Browser => return Err(e),
                Err(e) => escalations.push(format!("browser tier failed: {e}")),
            }
        }

        // --- Claude research tier ---
        if req.strategy == FetchStrategy::AutoWithResearch {
            let prompt = req.research_prompt.clone().unwrap_or_else(|| {
                format!(
                    "Fetch {} and extract its main textual content as clean Markdown. \
                     Respond with only the content, no commentary.",
                    req.url
                )
            });
            let mut research = ResearchRequest::new(prompt);
            research.max_budget_usd = req.max_budget_usd;
            let out = self.claude.research(research).await?;
            return Ok(FetchOutcome {
                url: req.url,
                engine: "claude",
                status: None,
                html: None,
                markdown: req.to_markdown.then(|| out.text.clone()),
                text: Some(out.text),
                escalations,
                cost_usd: out.cost_usd,
            });
        }

        Err(Error::App(format!(
            "all fetch tiers exhausted for {}: {}",
            req.url,
            escalations.join("; ")
        )))
    }
}

fn outcome(
    engine: &'static str,
    req: &FetchRequest,
    status: Option<u16>,
    html: String,
    markdown: String,
    escalations: Vec<String>,
) -> FetchOutcome {
    FetchOutcome {
        url: req.url.clone(),
        engine,
        status,
        markdown: req.to_markdown.then_some(markdown),
        text: None,
        html: Some(html),
        escalations,
        cost_usd: None,
    }
}
