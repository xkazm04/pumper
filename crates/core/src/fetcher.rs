//! Tiered fetching with automatic escalation. Starts on the cheapest engine
//! that can plausibly work and climbs only when the result looks insufficient:
//!
//!   http  ──(too little content / blocked)──▶  browser  ──(still thin)──▶  claude
//!
//! Apps call `ctx.engines.fetch.fetch(...)` and get back whichever tier
//! succeeded, plus a trail of why each escalation happened.

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::config::FetcherConfig;
use crate::engine::{Browser, HttpClient, HttpRequest, RenderRequest, Researcher};
use crate::governor::Governor;
use crate::markdown::{html_to_markdown, text_len_capped};
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
    /// Session-vault profile this fetch runs under, threaded to **both** tiers:
    /// the HTTP tier uses that profile's persistent cookie jar, the browser tier
    /// a Chrome bound to that profile's user-data-dir. `None` = today's behavior
    /// (in-memory jar + the shared default browser profile).
    #[serde(default)]
    pub profile: Option<String>,
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
            profile: None,
        }
    }
}

/// The three fetch tiers, cheapest first. Serializes to `http`/`browser`/`claude`
/// — the same strings the winning `FetchOutcome.engine` uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchTier {
    Http,
    Browser,
    Claude,
}

impl FetchTier {
    /// The `&'static str` tier name (matches `FetchOutcome.engine`).
    pub fn as_str(self) -> &'static str {
        match self {
            FetchTier::Http => "http",
            FetchTier::Browser => "browser",
            FetchTier::Claude => "claude",
        }
    }
}

/// Why a tier's attempt ended — the structured replacement for string-matching
/// the free-text escalation trail. Consumers branch on this instead of parsing
/// prose; the tier router keys on it to detect HTTP losses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierVerdict {
    /// This tier produced the returned result (the winner).
    Ok,
    /// Too little content to trust — escalated to the next tier.
    Thin,
    /// A bot-wall / challenge / block (status 403/429/503 or a challenge
    /// marker) — escalated.
    Blocked,
    /// The tier itself errored (network/render/research failure) — escalated.
    Error,
    /// The router skipped this tier before attempting it (learned `skip_http`
    /// preference, or the Claude tier dropped because the job budget is spent).
    SkippedByRouter,
}

/// One tier's contribution to a fetch: what it did, why it ended, and its cost
/// in latency and money. Every attempted tier (including the winner) gets an
/// entry; the human-readable `FetchOutcome.escalations` lines are kept alongside.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierTrace {
    pub tier: FetchTier,
    pub verdict: TierVerdict,
    /// HTTP status (http tier only; the browser/claude tiers have none).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    /// Extracted-text length in chars when it was measured (escalation
    /// decisions and the claude answer measure it; a straight `Http`-strategy
    /// return that skips counting leaves it `None`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_chars: Option<usize>,
    /// http tier only: whether the response was served from the HTTP cache.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_hit: Option<bool>,
    /// Wall-clock time this tier took. Zero for a `skipped_by_router` entry
    /// (nothing ran).
    pub latency_ms: u64,
    /// Real money spent (claude tier only; `None` for the free tiers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    /// Short human reason (challenge marker, error text, skip cause). `None`
    /// when the tier + verdict already say everything (e.g. a thin http tier,
    /// whose status and char count are their own explanation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
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
    /// Preserved for existing consumers and cost-event detail; the structured
    /// equivalent (and the winning tier's entry) lives in `trace`.
    pub escalations: Vec<String>,
    /// Structured, serde-serializable per-tier trace: one entry per attempted
    /// tier (incl. the winner), with verdict, per-tier latency, http status,
    /// content size, cache hit, and Claude spend. Consumers branch on
    /// `verdict` rather than parsing `escalations`.
    pub trace: Vec<TierTrace>,
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
    /// The same per-host politeness governor the HTTP engine uses. The HTTP tier
    /// is governed inside `HttpEngine::send` (so raw-HTTP callers like the crawler
    /// are still spaced); the browser tier has no such internal seam, so the
    /// Fetcher governs it here — sharing this one instance keeps per-host spacing
    /// coherent across an http -> browser escalation.
    governor: Arc<Governor>,
    /// Default escalation threshold from `[fetcher] min_content_chars`; a
    /// per-request `min_content_chars` overrides it.
    min_content_chars: usize,
}

impl Fetcher {
    pub fn new(
        http: Arc<dyn HttpClient>,
        browser: Arc<dyn Browser>,
        claude: Arc<dyn Researcher>,
        governor: Arc<Governor>,
        cfg: &FetcherConfig,
    ) -> Self {
        Self { http, browser, claude, governor, min_content_chars: cfg.min_content_chars }
    }

    pub async fn fetch(&self, req: FetchRequest) -> Result<FetchOutcome> {
        let min_chars = req.min_content_chars.unwrap_or(self.min_content_chars);
        let mut escalations: Vec<String> = Vec::new();
        let mut trace: Vec<TierTrace> = Vec::new();

        // --- HTTP tier --- (skip_http only applies to escalating strategies;
        // an explicit Http strategy is the caller's call.)
        let try_http = req.strategy == FetchStrategy::Http
            || (!req.skip_http
                && matches!(req.strategy, FetchStrategy::Auto | FetchStrategy::AutoWithResearch));
        if try_http {
            let mut http_req = HttpRequest::get(&req.url);
            http_req.no_cache = req.no_cache;
            http_req.ttl_override = req.ttl_override;
            http_req.profile = req.profile.clone();
            let started = Instant::now();
            match self.http.fetch(http_req).await {
                Ok(resp) => {
                    let latency_ms = elapsed_ms(started);
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
                    // Build the Markdown document only when the caller wants it.
                    // For the escalation decision alone, count text with an
                    // early-exit capped counter instead of materializing (then
                    // discarding) a full-page Markdown String.
                    let markdown = req.to_markdown.then(|| html_to_markdown(&resp.body));
                    let text_len = match &markdown {
                        Some(md) => Some(md.chars().count()),
                        None if needs_count => Some(text_len_capped(&resp.body, min_chars)),
                        None => None,
                    };
                    let cache_hit = Some(resp.cache_hit);
                    let enough = wall.is_none()
                        && resp.is_success()
                        && text_len.map_or(true, |n| n >= min_chars);
                    if enough || req.strategy == FetchStrategy::Http {
                        trace.push(TierTrace {
                            tier: FetchTier::Http,
                            verdict: TierVerdict::Ok,
                            http_status: Some(resp.status),
                            content_chars: text_len,
                            cache_hit,
                            latency_ms,
                            cost_usd: None,
                            detail: None,
                        });
                        return Ok(outcome(
                            "http",
                            &req,
                            Some(resp.status),
                            resp.body,
                            markdown,
                            escalations,
                            trace,
                        ));
                    }
                    let (verdict, detail) = match wall {
                        Some(reason) => {
                            escalations.push(format!(
                                "http tier blocked: {reason} (status {})",
                                resp.status
                            ));
                            (TierVerdict::Blocked, Some(reason))
                        }
                        None => {
                            escalations.push(format!(
                                "http tier thin: status {}, {} chars of text",
                                resp.status,
                                text_len.unwrap_or(0)
                            ));
                            (TierVerdict::Thin, None)
                        }
                    };
                    trace.push(TierTrace {
                        tier: FetchTier::Http,
                        verdict,
                        http_status: Some(resp.status),
                        content_chars: text_len,
                        cache_hit,
                        latency_ms,
                        cost_usd: None,
                        detail,
                    });
                }
                Err(e) if req.strategy == FetchStrategy::Http => return Err(e),
                Err(e) => trace_tier_error(
                    &mut escalations,
                    &mut trace,
                    FetchTier::Http,
                    "http",
                    &e,
                    started,
                ),
            }
        }

        // --- Browser tier ---
        if matches!(req.strategy, FetchStrategy::Browser | FetchStrategy::Auto | FetchStrategy::AutoWithResearch) {
            let mut render = RenderRequest::new(&req.url);
            render.wait_for_selector = req.wait_for_selector.clone();
            render.profile = req.profile.clone();
            // Space the browser render per-host, exactly as the HTTP tier is
            // spaced inside its engine. Critical because the learned tier router
            // pins repeatedly-blocked hosts to the browser tier — so without this
            // the hosts already hostile to us would receive *unlimited* renders.
            let host = url::Url::parse(&req.url)
                .ok()
                .and_then(|u| u.host_str().map(str::to_lowercase));
            if let Some(host) = &host {
                self.governor.acquire(host).await;
            }
            let started = Instant::now();
            match self.browser.render(render).await {
                Ok(page) => {
                    let latency_ms = elapsed_ms(started);
                    // Only AutoWithResearch escalates past the browser, so the
                    // char count only decides anything there; every other
                    // strategy returns the render as-is. Convert once, and only
                    // when the decision or the caller needs Markdown.
                    let needs_count = req.strategy == FetchStrategy::AutoWithResearch;
                    // A rendered page can still be a challenge/error wall (the
                    // browser has no HTTP status), so add a marker heuristic
                    // beyond char count before handing off to Claude.
                    let wall = needs_count.then(|| challenge_marker(&page.html)).flatten();
                    // Build Markdown only for the caller; the escalation decision
                    // uses the capped text counter (no full-page String built and
                    // thrown away when to_markdown is false).
                    let markdown = req.to_markdown.then(|| html_to_markdown(&page.html));
                    let text_len = match &markdown {
                        Some(md) => Some(md.chars().count()),
                        None if needs_count => Some(text_len_capped(&page.html, min_chars)),
                        None => None,
                    };
                    let enough = wall.is_none() && text_len.map_or(true, |n| n >= min_chars);
                    if enough || req.strategy != FetchStrategy::AutoWithResearch {
                        // A healthy browser fetch decays any learned penalty on the
                        // host (no-op when unpenalized) — the recovery half of the
                        // loop, mirroring the HTTP tier's reward-on-success.
                        if let Some(host) = &host {
                            self.governor.reward(host).await;
                        }
                        trace.push(TierTrace {
                            tier: FetchTier::Browser,
                            verdict: TierVerdict::Ok,
                            http_status: None,
                            content_chars: text_len,
                            cache_hit: None,
                            latency_ms,
                            cost_usd: None,
                            detail: None,
                        });
                        return Ok(outcome(
                            "browser",
                            &req,
                            None,
                            page.html,
                            markdown,
                            escalations,
                            trace,
                        ));
                    }
                    let (verdict, detail) = match wall {
                        Some(reason) => {
                            // A browser-tier bot-wall teaches the governor to back
                            // off this host — previously the adaptive penalty was
                            // blind on the browser tier, exactly where the router
                            // concentrates blocked-host traffic. No status here, so
                            // no server Retry-After to honor.
                            if let Some(host) = &host {
                                self.governor.penalize(host, None).await;
                            }
                            escalations.push(format!("browser tier blocked: {reason}"));
                            (TierVerdict::Blocked, Some(reason))
                        }
                        None => {
                            escalations.push(format!(
                                "browser tier thin: {} chars of text",
                                text_len.unwrap_or(0)
                            ));
                            (TierVerdict::Thin, None)
                        }
                    };
                    trace.push(TierTrace {
                        tier: FetchTier::Browser,
                        verdict,
                        http_status: None,
                        content_chars: text_len,
                        cache_hit: None,
                        latency_ms,
                        cost_usd: None,
                        detail,
                    });
                }
                Err(e) if req.strategy == FetchStrategy::Browser => return Err(e),
                Err(e) => trace_tier_error(
                    &mut escalations,
                    &mut trace,
                    FetchTier::Browser,
                    "browser",
                    &e,
                    started,
                ),
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
            let started = Instant::now();
            let out = self.claude.research(research).await?;
            trace.push(TierTrace {
                tier: FetchTier::Claude,
                verdict: TierVerdict::Ok,
                http_status: None,
                content_chars: Some(out.text.chars().count()),
                cache_hit: None,
                latency_ms: elapsed_ms(started),
                cost_usd: out.cost_usd,
                detail: None,
            });
            return Ok(FetchOutcome {
                url: req.url,
                engine: "claude",
                status: None,
                html: None,
                markdown: req.to_markdown.then(|| out.text.clone()),
                text: Some(out.text),
                escalations,
                trace,
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

/// Milliseconds since `started`, saturating into a `u64` for the trace.
/// Records a tier that errored out: the human-readable escalation line plus the
/// machine-readable `Error` trace entry. Identical for every tier bar its name,
/// so it lives here rather than being re-typed in each tier's error arm.
///
/// The tier *bodies* stay deliberately explicit: each tier's "good enough"
/// criteria genuinely differ (HTTP weighs status + bot-wall, the browser weighs
/// challenge markers, and the return-early condition differs per strategy), and
/// that per-tier judgement is the whole point of a tiered fetcher.
fn trace_tier_error(
    escalations: &mut Vec<String>,
    trace: &mut Vec<TierTrace>,
    tier: FetchTier,
    name: &str,
    err: &Error,
    started: Instant,
) {
    escalations.push(format!("{name} tier failed: {err}"));
    trace.push(TierTrace {
        tier,
        verdict: TierVerdict::Error,
        http_status: None,
        content_chars: None,
        cache_hit: None,
        latency_ms: elapsed_ms(started),
        cost_usd: None,
        detail: Some(err.to_string()),
    });
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

#[allow(clippy::too_many_arguments)]
fn outcome(
    engine: &'static str,
    req: &FetchRequest,
    status: Option<u16>,
    html: String,
    markdown: Option<String>,
    escalations: Vec<String>,
    trace: Vec<TierTrace>,
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
        trace,
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
pub(crate) fn http_bot_wall(status: u16, body: &str) -> Option<String> {
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
    fn verdict_and_tier_serialize_to_stable_snake_case() {
        // The trace is a serialized API contract: verdicts are snake_case
        // strings and a tier's name matches the winning `engine` string.
        assert_eq!(
            serde_json::to_string(&TierVerdict::SkippedByRouter).unwrap(),
            "\"skipped_by_router\""
        );
        assert_eq!(serde_json::to_string(&TierVerdict::Ok).unwrap(), "\"ok\"");
        assert_eq!(serde_json::to_string(&FetchTier::Claude).unwrap(), "\"claude\"");
        assert_eq!(FetchTier::Http.as_str(), "http");
        assert_eq!(FetchTier::Browser.as_str(), "browser");
        assert_eq!(FetchTier::Claude.as_str(), "claude");
    }

    #[test]
    fn trace_entry_omits_empty_optionals_but_keeps_latency() {
        // Optional fields drop out when None; latency_ms is always present.
        let t = TierTrace {
            tier: FetchTier::Http,
            verdict: TierVerdict::Thin,
            http_status: Some(200),
            content_chars: Some(12),
            cache_hit: Some(false),
            latency_ms: 7,
            cost_usd: None,
            detail: None,
        };
        let v: serde_json::Value = serde_json::to_value(&t).unwrap();
        assert_eq!(v["tier"], "http");
        assert_eq!(v["verdict"], "thin");
        assert_eq!(v["http_status"], 200);
        assert_eq!(v["content_chars"], 12);
        assert_eq!(v["cache_hit"], false);
        assert_eq!(v["latency_ms"], 7);
        assert!(v.get("cost_usd").is_none(), "None cost_usd is omitted");
        assert!(v.get("detail").is_none(), "None detail is omitted");
    }

    #[test]
    fn fetch_request_profile_is_serde_defaulted_and_threads_to_both_tiers() {
        // Omitted => None => today's behavior.
        let req: FetchRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(req.profile.is_none());
        assert!(FetchRequest::new("https://x/").profile.is_none());

        // Present => both tier requests carry it (mirrors what `fetch` builds).
        let req: FetchRequest =
            serde_json::from_str(r#"{"url":"https://x/","profile":"acme"}"#).unwrap();
        let mut http_req = HttpRequest::get(&req.url);
        http_req.profile = req.profile.clone();
        let mut render = RenderRequest::new(&req.url);
        render.profile = req.profile.clone();
        assert_eq!(http_req.profile.as_deref(), Some("acme"));
        assert_eq!(render.profile.as_deref(), Some("acme"));
    }

    #[test]
    fn browser_challenge_marker_detects_walls() {
        let html = "<html><body>Verify you are human by completing the action below.</body></html>";
        assert!(challenge_marker(html).is_some());
        let real = "<html><body><article>A long, ordinary news story with no gates.</article></body></html>";
        assert!(challenge_marker(real).is_none());
    }

    // --- Browser-tier governor integration ---

    use std::time::Duration;

    use crate::config::GovernorConfig;
    use crate::engine::{HttpResponse, RenderedPage, ResearchOutput};
    use async_trait::async_trait;

    /// Browser stub that returns a fixed HTML body for every render.
    struct StubBrowser {
        html: String,
    }
    #[async_trait]
    impl Browser for StubBrowser {
        async fn render(&self, _req: RenderRequest) -> Result<RenderedPage> {
            Ok(RenderedPage { html: self.html.clone(), ..Default::default() })
        }
    }

    struct DeadHttp;
    #[async_trait]
    impl HttpClient for DeadHttp {
        async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
            panic!("http tier must not be called: these tests skip_http");
        }
    }

    /// Researcher stub — the Claude tier the AutoWithResearch strategy falls
    /// through to after a blocked/thin browser render.
    struct StubResearcher;
    #[async_trait]
    impl Researcher for StubResearcher {
        async fn research(&self, _req: ResearchRequest) -> Result<ResearchOutput> {
            Ok(ResearchOutput {
                text: "researched content".into(),
                json: None,
                cost_usd: Some(0.0),
                duration_ms: None,
                num_turns: None,
                session_id: None,
            })
        }
    }

    fn fetcher_with(browser: StubBrowser, governor: Arc<Governor>) -> Fetcher {
        Fetcher::new(
            Arc::new(DeadHttp),
            Arc::new(browser),
            Arc::new(StubResearcher),
            governor,
            &FetcherConfig { min_content_chars: 100, ..FetcherConfig::default() },
        )
    }

    fn enabled_governor() -> Arc<Governor> {
        // Politeness spacing disabled (rps huge, no jitter) so the test never
        // sleeps; only the learned penalty behaviour is under test.
        let cfg = GovernorConfig {
            enabled: true,
            default_rps: 1_000_000.0,
            jitter_ms: 0,
            ..GovernorConfig::default()
        };
        Arc::new(Governor::new(&cfg))
    }

    #[tokio::test]
    async fn browser_tier_bot_wall_penalizes_the_host() {
        // A challenge wall reached via the browser tier must teach the governor
        // to back off — the learning hole this change closes (previously the
        // browser tier never called penalize).
        let governor = enabled_governor();
        let wall = "<html><head><title>Just a moment...</title></head><body>\
            <div class=\"cf-browser-verification\">Checking your browser before accessing.</div>\
            </body></html>";
        let fetcher = fetcher_with(StubBrowser { html: wall.into() }, governor.clone());

        assert_eq!(governor.penalty("blocked.example").await, Duration::ZERO);

        let mut req = FetchRequest::new("https://blocked.example/page");
        req.strategy = FetchStrategy::AutoWithResearch;
        req.skip_http = true; // straight to the browser tier
        let outcome = fetcher.fetch(req).await.unwrap();

        // The wall drove escalation to the Claude tier...
        assert_eq!(outcome.engine, "claude");
        assert!(outcome.trace.iter().any(|t| t.tier == FetchTier::Browser
            && t.verdict == TierVerdict::Blocked));
        // ...and the governor learned a penalty for the host.
        assert!(
            governor.penalty("blocked.example").await > Duration::ZERO,
            "browser bot-wall must penalize the host"
        );
    }

    #[tokio::test]
    async fn healthy_browser_render_rewards_the_host() {
        // A clean browser fetch decays a pre-existing learned penalty (the
        // recovery half of the loop), mirroring the HTTP tier's reward-on-success.
        let governor = enabled_governor();
        governor.penalize("recovering.example", Some(Duration::from_secs(4))).await;
        assert_eq!(governor.penalty("recovering.example").await, Duration::from_secs(4));

        let good = "<html><body><article>A perfectly ordinary page with plenty of \
            real readable content, well past the hundred-character threshold used \
            for escalation decisions in this test.</article></body></html>";
        let fetcher = fetcher_with(StubBrowser { html: good.into() }, governor.clone());

        let mut req = FetchRequest::new("https://recovering.example/page");
        req.strategy = FetchStrategy::Browser; // browser-only: returns the render as-is
        let outcome = fetcher.fetch(req).await.unwrap();
        assert_eq!(outcome.engine, "browser");

        // reward() halves the learned penalty.
        assert_eq!(
            governor.penalty("recovering.example").await,
            Duration::from_secs(2),
            "healthy browser render must decay the penalty"
        );
    }
}
