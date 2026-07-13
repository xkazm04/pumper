//! Tiered fetching with automatic escalation. Starts on the cheapest engine
//! that can plausibly work and climbs only when the result looks insufficient:
//!
//!   http  ──(too little content / blocked)──▶  browser  ──(still thin)──▶  claude
//!
//! Apps call `ctx.engines.fetch.fetch(...)` and get back whichever tier
//! succeeded, plus a trail of why each escalation happened.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::FetcherConfig;
use crate::engine::{Browser, HttpClient, HttpRequest, RenderRequest, Researcher};
use crate::markdown::html_to_markdown;
use crate::{Error, ResearchRequest, Result};

/// Case-insensitive marker phrases that identify a bot-wall / interstitial
/// challenge page rather than real content. Conservative and specific enough
/// to rarely fire on genuine articles; only the page's leading window is
/// scanned (challenge markup lives at the top). Extend deliberately + with a test.
const CHALLENGE_MARKERS: &[&str] = &[
    "checking your browser",       // Cloudflare IUAM
    "cf-browser-verification",     // Cloudflare challenge widget
    "just a moment",               // Cloudflare interstitial title
    "attention required",          // Cloudflare block page
    "enable javascript",           // JS-gate interstitials
    "please enable cookies",       // Cloudflare cookie gate
    "verify you are human",        // generic challenge prompt
    "captcha",                     // hCaptcha / reCAPTCHA gates
    "ddos protection by",          // anti-DDoS interstitials
];

/// Only the first N chars are scanned for challenge markers — cheap, and
/// interstitial markup is front-loaded.
const CHALLENGE_SCAN_CHARS: usize = 4096;

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
    /// Bypass the HTTP response cache — always hit the network. Monitors (e.g.
    /// the `watch` app) set this to avoid serving up-to-TTL-stale bodies.
    #[serde(default)]
    pub no_cache: bool,
    /// Override the HTTP response-cache TTL (seconds) for this fetch. `None`
    /// uses the configured `[cache] ttl_secs`. A short value caps staleness
    /// without a full cache bypass. Only affects the HTTP tier.
    #[serde(default)]
    pub ttl_override: Option<u64>,
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
            no_cache: false,
            ttl_override: None,
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
    /// Default escalation threshold from `[fetcher] min_content_chars`; a
    /// per-request `min_content_chars` overrides it.
    min_content_chars: usize,
}

impl Fetcher {
    pub fn new(
        http: Arc<dyn HttpClient>,
        browser: Arc<dyn Browser>,
        claude: Arc<dyn Researcher>,
        cfg: &FetcherConfig,
    ) -> Self {
        Self { http, browser, claude, min_content_chars: cfg.min_content_chars }
    }

    pub async fn fetch(&self, req: FetchRequest) -> Result<FetchOutcome> {
        let min_chars = req.min_content_chars.unwrap_or(self.min_content_chars);
        let mut escalations: Vec<String> = Vec::new();

        // --- HTTP tier --- (skip_http only applies to escalating strategies;
        // an explicit Http strategy is the caller's call.)
        let try_http = req.strategy == FetchStrategy::Http
            || (!req.skip_http
                && matches!(req.strategy, FetchStrategy::Auto | FetchStrategy::AutoWithResearch));
        if try_http {
            let mut http_req = HttpRequest::get(&req.url);
            http_req.no_cache = req.no_cache;
            http_req.ttl_override = req.ttl_override;
            match self.http.fetch(http_req).await {
                Ok(resp) => {
                    // Convert to Markdown at most once, and only when a decision
                    // (escalation) or the caller (to_markdown) actually needs it.
                    // The `Http` strategy returns regardless, so it skips the
                    // conversion entirely unless Markdown was requested.
                    let needs_count = matches!(
                        req.strategy,
                        FetchStrategy::Auto | FetchStrategy::AutoWithResearch
                    );
                    // Bot-wall / challenge detection only matters when there's a
                    // higher tier to escalate to (the `Http` strategy hands the
                    // body back for the caller to inspect).
                    let wall = needs_count
                        .then(|| http_bot_wall(resp.status, &resp.body))
                        .flatten();
                    let markdown =
                        (req.to_markdown || needs_count).then(|| html_to_markdown(&resp.body));
                    let text_len = markdown.as_ref().map(|m| m.chars().count());
                    let enough = wall.is_none()
                        && resp.is_success()
                        && text_len.map_or(true, |n| n >= min_chars);
                    if enough || req.strategy == FetchStrategy::Http {
                        return Ok(outcome("http", &req, Some(resp.status), resp.body, markdown, escalations));
                    }
                    match wall {
                        Some(reason) => escalations.push(format!(
                            "http tier blocked: {reason} (status {})",
                            resp.status
                        )),
                        None => escalations.push(format!(
                            "http tier thin: status {}, {} chars of text",
                            resp.status,
                            text_len.unwrap_or(0)
                        )),
                    }
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
                    // Only AutoWithResearch escalates past the browser, so the
                    // char count only decides anything there; every other
                    // strategy returns the render as-is. Convert once, and only
                    // when the decision or the caller needs Markdown.
                    let needs_count = req.strategy == FetchStrategy::AutoWithResearch;
                    // A rendered page can still be a challenge/error wall (the
                    // browser has no HTTP status), so add a marker heuristic
                    // beyond char count before handing off to Claude.
                    let wall = needs_count.then(|| challenge_marker(&page.html)).flatten();
                    let markdown =
                        (req.to_markdown || needs_count).then(|| html_to_markdown(&page.html));
                    let text_len = markdown.as_ref().map(|m| m.chars().count());
                    let enough = wall.is_none() && text_len.map_or(true, |n| n >= min_chars);
                    if enough || req.strategy != FetchStrategy::AutoWithResearch {
                        return Ok(outcome("browser", &req, None, page.html, markdown, escalations));
                    }
                    match wall {
                        Some(reason) => {
                            escalations.push(format!("browser tier blocked: {reason}"))
                        }
                        None => escalations.push(format!(
                            "browser tier thin: {} chars of text",
                            text_len.unwrap_or(0)
                        )),
                    }
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
    markdown: Option<String>,
    escalations: Vec<String>,
) -> FetchOutcome {
    FetchOutcome {
        url: req.url.clone(),
        engine,
        status,
        // `markdown` is only computed when needed; surface it solely when asked.
        markdown: if req.to_markdown { markdown } else { None },
        text: None,
        html: Some(html),
        escalations,
        cost_usd: None,
    }
}

/// Classifies an HTTP-tier response as a bot-wall / challenge that should
/// escalate rather than pass off as content. Returns a short reason for the
/// escalation trail, or `None` when the response looks like real content.
///
/// Two signals: hard block/challenge statuses (403/429/503), and conservative
/// challenge-page text markers in the body's leading window (a 200 "enable
/// JavaScript" or Cloudflare interstitial that would otherwise pass a char
/// count).
fn http_bot_wall(status: u16, body: &str) -> Option<String> {
    match status {
        403 => return Some("challenge/block status 403".into()),
        429 => return Some("rate-limited status 429".into()),
        503 => return Some("unavailable/challenge status 503".into()),
        _ => {}
    }
    challenge_marker(body)
}

/// Scans the leading window of a document for a known challenge/interstitial
/// marker. Shared by the HTTP and browser tiers (the browser has no status, so
/// markers are its only bot-wall signal).
fn challenge_marker(body: &str) -> Option<String> {
    let head: String = body.chars().take(CHALLENGE_SCAN_CHARS).collect::<String>().to_lowercase();
    CHALLENGE_MARKERS
        .iter()
        .find(|m| head.contains(**m))
        .map(|m| format!("challenge marker: {m:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_statuses_are_bot_walls() {
        assert!(http_bot_wall(403, "whatever").is_some());
        assert!(http_bot_wall(429, "whatever").is_some());
        assert!(http_bot_wall(503, "whatever").is_some());
    }

    #[test]
    fn ok_status_with_real_content_is_not_a_wall() {
        let body = "<html><body><h1>Quarterly report</h1>\
            <p>Revenue rose across every region this year.</p></body></html>";
        assert!(http_bot_wall(200, body).is_none());
        // 404s are returned to the caller, not treated as walls here.
        assert!(http_bot_wall(404, body).is_none());
    }

    #[test]
    fn ok_status_challenge_page_is_a_wall() {
        let cf = "<html><head><title>Just a moment...</title></head><body>\
            <div class=\"cf-browser-verification\">Checking your browser before accessing.</div>\
            </body></html>";
        assert!(http_bot_wall(200, cf).is_some(), "cloudflare interstitial must escalate");

        let js = "<html><body><noscript>Please enable JavaScript to view this page.</noscript></body></html>";
        assert!(http_bot_wall(200, js).is_some(), "js-gate must escalate");

        let captcha = "<html><body>Please complete the CAPTCHA to continue.</body></html>";
        assert!(http_bot_wall(200, captcha).is_some(), "captcha gate must escalate");
    }

    #[test]
    fn challenge_markers_only_scan_the_leading_window() {
        // A marker buried past the scan window doesn't trip the heuristic —
        // keeps long real articles that mention these phrases from escalating.
        let mut body = "x".repeat(CHALLENGE_SCAN_CHARS + 10);
        body.push_str("enable javascript");
        assert!(challenge_marker(&body).is_none());
    }

    #[test]
    fn browser_challenge_marker_detects_walls() {
        let html = "<html><body>Verify you are human by completing the action below.</body></html>";
        assert!(challenge_marker(html).is_some());
        let real = "<html><body><article>A long, ordinary news story with no gates.</article></body></html>";
        assert!(challenge_marker(real).is_none());
    }
}
