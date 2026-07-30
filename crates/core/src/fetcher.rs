//! Tiered fetching with automatic escalation. Starts on the cheapest engine
//! that can plausibly work and climbs only when the result looks insufficient:
//!
//!   http  ──(too little content / blocked)──▶  browser  ──(still thin)──▶  claude
//!
//! Apps call `ctx.engines.fetch.fetch(...)` and get back whichever tier
//! succeeded, plus a trail of why each escalation happened.
//!
//! **Tier zero (opt-in): the archive tier.** When a request declares an
//! `archive_max_age` freshness window AND an archive engine is wired
//! ([`Fetcher::with_archive`], `[archive] enabled`), a stored web-archive
//! snapshot is tried BEFORE any live tier — zero load on the target site, zero
//! politeness budget, zero ban risk. Archive coverage is patchy by nature, so
//! the archive tier is strictly opportunistic: a miss, a stale-only snapshot, a
//! thin body, or an engine error always falls through to the live ladder.

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::config::{FetcherConfig, RecipesConfig};
use crate::engine::{Browser, HttpClient, HttpRequest, RenderRequest, Researcher};
use crate::governor::Governor;
use crate::markdown::{html_to_markdown, text_len_capped};
use crate::recipes::{payload_overlaps, RecipeSource};
use crate::{Error, ResearchRequest, Result};

/// Case-insensitive marker phrases that identify a bot-wall / interstitial
/// challenge page rather than real content. Conservative and specific enough
/// to rarely fire on genuine articles; only the page's leading window is
/// scanned (challenge markup lives at the top). Extend deliberately + with a test.
const CHALLENGE_MARKERS: &[&str] = &[
    "checking your browser",   // Cloudflare IUAM
    "cf-browser-verification", // Cloudflare challenge widget
    "just a moment",           // Cloudflare interstitial title
    "attention required",      // Cloudflare block page
    "enable javascript",       // JS-gate interstitials
    "please enable cookies",   // Cloudflare cookie gate
    "verify you are human",    // generic challenge prompt
    "captcha",                 // hCaptcha / reCAPTCHA gates
    "ddos protection by",      // anti-DDoS interstitials
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
    /// Browser tier: scripted page actions (scroll/click/wait) run before
    /// capture — drives infinite-scroll / "load more" listings. Empty = one-shot.
    #[serde(default)]
    pub actions: Vec<crate::engine::PageAction>,
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
    /// Archive freshness window (seconds): when set (and an archive engine is
    /// wired), a stored web-archive snapshot captured no longer than this many
    /// seconds ago is tried BEFORE the live tiers. An absent/older/thin snapshot
    /// falls through to the live ladder — the archive tier is never terminal.
    /// `None` = live-only, exactly the previous behavior.
    #[serde(default)]
    pub archive_max_age: Option<u64>,
    /// Try a learned API recipe (M05, see [`crate::recipes`]) ahead of every
    /// other tier for this fetch, even when the global `[recipes] enabled`
    /// switch is off. Default-OFF; with neither this nor the config switch set,
    /// recipes are never consulted.
    #[serde(default)]
    pub use_recipes: bool,
}

impl FetchRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            strategy: FetchStrategy::Auto,
            wait_for_selector: None,
            actions: Vec::new(),
            min_content_chars: None,
            research_prompt: None,
            max_budget_usd: None,
            skip_http: false,
            to_markdown: false,
            no_cache: false,
            ttl_override: None,
            profile: None,
            archive_max_age: None,
            use_recipes: false,
        }
    }
}

/// The fetch tiers, cheapest first. Serializes to
/// `archive`/`http`/`browser`/`claude` — the same strings the winning
/// `FetchOutcome.engine` uses. `Archive` is tier zero: opt-in (only attempted
/// when `archive_max_age` is set and an archive engine is wired) and strictly
/// opportunistic — it can win but never terminate the ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchTier {
    /// Learned API-recipe replay (M05) — tried even before the archive tier,
    /// double-opt-in (`FetchRequest.use_recipes` / `[recipes] enabled`) and,
    /// like the archive tier, never terminal on failure.
    ApiRecipe,
    Archive,
    Http,
    Browser,
    Claude,
}

impl FetchTier {
    /// The `&'static str` tier name (matches `FetchOutcome.engine`).
    pub fn as_str(self) -> &'static str {
        match self {
            FetchTier::ApiRecipe => "api_recipe",
            FetchTier::Archive => "archive",
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
    /// HTTP status (archive/http tiers only; the browser/claude tiers have none).
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
    /// Tier-zero archive engine (`[archive]`, default OFF => `None`). Attempted
    /// before the live ladder only when a request sets `archive_max_age`. It is
    /// an `HttpClient` like the http tier, but serves stored web-archive
    /// snapshots; its own outbound requests (CDX + snapshot body) run through
    /// the real HTTP engine, so archive.org is governed like any host.
    archive: Option<Arc<dyn HttpClient>>,
    /// Learned API-recipe source (`[recipes]`, M05). `None` (the default) or a
    /// fetch with neither opt-in flag leaves the ladder untouched. Replays run
    /// through the real HTTP engine, so recipe hosts stay governed/cached.
    recipes: Option<Arc<dyn RecipeSource>>,
    /// `[recipes] enabled`: consult recipes on every fetch (per-request
    /// `use_recipes` opts a single fetch in regardless).
    recipes_enabled: bool,
    /// `[recipes] auto_validate`: also try unvalidated recipes, and let a
    /// successful overlapping replay promote them via `record_success`.
    recipes_auto_validate: bool,
    /// `[recipes] max_failures`: consecutive strikes that un-validate.
    recipes_max_failures: u32,
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
        Self {
            http,
            browser,
            claude,
            archive: None,
            recipes: None,
            recipes_enabled: false,
            recipes_auto_validate: false,
            recipes_max_failures: RecipesConfig::default().max_failures,
            governor,
            min_content_chars: cfg.min_content_chars,
        }
    }

    /// Wires the (optional) learned API-recipe source and its `[recipes]`
    /// tuning. `None` — the default — leaves the ladder exactly as it was;
    /// even when wired, a fetch consults recipes only when it sets
    /// `use_recipes` or `[recipes] enabled` is on.
    pub fn with_recipes(
        mut self,
        recipes: Option<Arc<dyn RecipeSource>>,
        cfg: &RecipesConfig,
    ) -> Self {
        self.recipes = recipes;
        self.recipes_enabled = cfg.enabled;
        self.recipes_auto_validate = cfg.auto_validate;
        self.recipes_max_failures = cfg.max_failures;
        self
    }

    /// Wires the (optional) tier-zero archive engine. `None` — the default —
    /// leaves the ladder exactly as it was; requests that set `archive_max_age`
    /// then simply skip straight to the live tiers.
    pub fn with_archive(mut self, archive: Option<Arc<dyn HttpClient>>) -> Self {
        self.archive = archive;
        self
    }

    pub async fn fetch(&self, req: FetchRequest) -> Result<FetchOutcome> {
        let min_chars = req.min_content_chars.unwrap_or(self.min_content_chars);
        let mut escalations: Vec<String> = Vec::new();
        let mut trace: Vec<TierTrace> = Vec::new();

        // --- API-recipe tier (pre-archive, double-opt-in) --- a validated
        // recipe for the request's host replays the page's discovered JSON API
        // instead of touching the page at all: one governed HTTP call, a
        // structured body, no render. Strictly opportunistic like the archive
        // tier: any miss/thin/error records a strike and falls through. The
        // browser-only strategy is excluded — the caller explicitly wants a JS
        // render, which an API payload cannot be.
        if (req.use_recipes || self.recipes_enabled) && req.strategy != FetchStrategy::Browser {
            if let Some(out) = self.try_recipe(&req, &mut escalations, &mut trace).await {
                return Ok(out);
            }
        }

        // --- Archive tier (tier zero, opt-in) --- tried BEFORE any live tier,
        // but only when the caller declared a freshness window and an archive
        // engine is wired. Strictly opportunistic: every failure mode (no
        // snapshot, snapshot older than the window, thin body, archived
        // challenge page, engine error) falls through to the live ladder. The
        // browser-only strategy is excluded — the caller explicitly wants a
        // JS render, which an archived static body cannot be.
        if req.archive_max_age.is_some() && req.strategy != FetchStrategy::Browser {
            if let Some(archive) = &self.archive {
                let mut arch_req = HttpRequest::get(&req.url);
                arch_req.archive_max_age = req.archive_max_age;
                arch_req.no_cache = req.no_cache;
                arch_req.ttl_override = req.ttl_override;
                let started = Instant::now();
                match archive.fetch(arch_req).await {
                    Ok(resp) => {
                        let latency_ms = elapsed_ms(started);
                        // Same acceptance bar as the http tier: a snapshot can be
                        // an archived bot-wall or a thin shell page, and serving
                        // that as content would be worse than a live fetch.
                        let wall = http_bot_wall(resp.status, &resp.body);
                        let markdown = req.to_markdown.then(|| html_to_markdown(&resp.body));
                        let text_len = match &markdown {
                            Some(md) => md.chars().count(),
                            None => text_len_capped(&resp.body, min_chars),
                        };
                        let enough = wall.is_none() && resp.is_success() && text_len >= min_chars;
                        if enough {
                            trace.push(TierTrace {
                                tier: FetchTier::Archive,
                                verdict: TierVerdict::Ok,
                                http_status: Some(resp.status),
                                content_chars: Some(text_len),
                                cache_hit: Some(resp.cache_hit),
                                latency_ms,
                                cost_usd: None,
                                detail: None,
                            });
                            return Ok(outcome(
                                "archive",
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
                                    "archive tier blocked: archived challenge page ({reason})"
                                ));
                                (TierVerdict::Blocked, Some(reason))
                            }
                            None => {
                                escalations.push(format!(
                                    "archive tier thin: status {}, {} chars of text",
                                    resp.status, text_len
                                ));
                                (TierVerdict::Thin, None)
                            }
                        };
                        trace.push(TierTrace {
                            tier: FetchTier::Archive,
                            verdict,
                            http_status: Some(resp.status),
                            content_chars: Some(text_len),
                            cache_hit: Some(resp.cache_hit),
                            latency_ms,
                            cost_usd: None,
                            detail,
                        });
                    }
                    // A miss (no snapshot / stale-only) surfaces as an engine
                    // error here — never terminal, always fall through to live.
                    Err(e) => trace_tier_error(
                        &mut escalations,
                        &mut trace,
                        FetchTier::Archive,
                        "archive",
                        &e,
                        started,
                    ),
                }
            }
        }

        // --- HTTP tier --- (skip_http only applies to escalating strategies;
        // an explicit Http strategy is the caller's call.)
        let try_http = req.strategy == FetchStrategy::Http
            || (!req.skip_http
                && matches!(
                    req.strategy,
                    FetchStrategy::Auto | FetchStrategy::AutoWithResearch
                ));
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
                        && text_len.is_none_or(|n| n >= min_chars);
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
        if matches!(
            req.strategy,
            FetchStrategy::Browser | FetchStrategy::Auto | FetchStrategy::AutoWithResearch
        ) {
            let mut render = RenderRequest::new(&req.url);
            render.wait_for_selector = req.wait_for_selector.clone();
            render.actions = req.actions.clone();
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
                    let enough = wall.is_none() && text_len.is_none_or(|n| n >= min_chars);
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

    /// One attempt at the API-recipe tier. `Some(outcome)` means the recipe
    /// replay won (structured JSON in `text`, engine `api_recipe`); `None`
    /// means no usable recipe or a failed/thin replay — the strike is recorded
    /// and the caller falls through to the archive/live ladder.
    async fn try_recipe(
        &self,
        req: &FetchRequest,
        escalations: &mut Vec<String>,
        trace: &mut Vec<TierTrace>,
    ) -> Option<FetchOutcome> {
        let source = self.recipes.as_ref()?;
        let host = url::Url::parse(&req.url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_lowercase))?;
        // Unvalidated recipes are only tried opportunistically when
        // auto-validation is on; otherwise validated-only.
        let recipe = match source
            .best_for_host(&host, self.recipes_auto_validate)
            .await
        {
            Ok(Some(r)) => r,
            Ok(None) => return None,
            Err(e) => {
                escalations.push(format!("api_recipe tier failed: recipe lookup: {e}"));
                return None;
            }
        };
        let Some(api_url) = recipe.replay_url() else {
            return None; // un-replayable template (unfilled placeholder)
        };
        let mut api_req = HttpRequest::get(&api_url);
        api_req.no_cache = req.no_cache;
        api_req.ttl_override = req.ttl_override;
        api_req.profile = req.profile.clone();
        let started = Instant::now();
        match self.http.fetch(api_req).await {
            Ok(resp) => {
                let latency_ms = elapsed_ms(started);
                let parsed = serde_json::from_str::<serde_json::Value>(&resp.body).ok();
                let overlaps = parsed
                    .as_ref()
                    .is_some_and(|v| payload_overlaps(&recipe.json_paths, v));
                if resp.is_success() && overlaps {
                    // A successful overlapping replay resets the strike counter
                    // and — under auto_validate — proves an unvalidated recipe.
                    let validate = self.recipes_auto_validate && !recipe.validated;
                    if let Err(e) = source.record_success(&recipe.id, validate).await {
                        escalations.push(format!("api_recipe tier: recording success failed: {e}"));
                    }
                    trace.push(TierTrace {
                        tier: FetchTier::ApiRecipe,
                        verdict: TierVerdict::Ok,
                        http_status: Some(resp.status),
                        content_chars: Some(resp.body.chars().count()),
                        cache_hit: Some(resp.cache_hit),
                        latency_ms,
                        cost_usd: None,
                        detail: Some(format!("recipe {}", recipe.id)),
                    });
                    return Some(FetchOutcome {
                        url: req.url.clone(),
                        engine: "api_recipe",
                        status: Some(resp.status),
                        html: None,
                        markdown: None,
                        // Structured JSON body — deliberately `text`, not
                        // `html`: this is API data, not a document.
                        text: Some(resp.body),
                        escalations: std::mem::take(escalations),
                        trace: std::mem::take(trace),
                        cost_usd: None,
                    });
                }
                // Thin/failed replay → strike (may un-validate) → fall through.
                let demoted = source
                    .record_failure(&recipe.id, self.recipes_max_failures)
                    .await
                    .unwrap_or(false);
                let why = if !resp.is_success() {
                    "non-success status"
                } else if parsed.is_none() {
                    "non-JSON payload"
                } else {
                    "payload lost the expected field paths"
                };
                escalations.push(format!(
                    "api_recipe tier thin: status {}, {why}{}",
                    resp.status,
                    if demoted {
                        " (recipe un-validated)"
                    } else {
                        ""
                    }
                ));
                trace.push(TierTrace {
                    tier: FetchTier::ApiRecipe,
                    verdict: TierVerdict::Thin,
                    http_status: Some(resp.status),
                    content_chars: Some(resp.body.chars().count()),
                    cache_hit: Some(resp.cache_hit),
                    latency_ms,
                    cost_usd: None,
                    detail: Some(why.into()),
                });
                None
            }
            Err(e) => {
                // Engine error is a strike too — a recipe pointing at a dead
                // endpoint must eventually un-validate itself.
                let _ = source
                    .record_failure(&recipe.id, self.recipes_max_failures)
                    .await;
                trace_tier_error(
                    escalations,
                    trace,
                    FetchTier::ApiRecipe,
                    "api_recipe",
                    &e,
                    started,
                );
                None
            }
        }
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
    let head: String = body
        .chars()
        .take(CHALLENGE_SCAN_CHARS)
        .collect::<String>()
        .to_lowercase();
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
        assert!(
            http_bot_wall(200, cf).is_some(),
            "cloudflare interstitial must escalate"
        );

        let js = "<html><body><noscript>Please enable JavaScript to view this page.</noscript></body></html>";
        assert!(http_bot_wall(200, js).is_some(), "js-gate must escalate");

        let captcha = "<html><body>Please complete the CAPTCHA to continue.</body></html>";
        assert!(
            http_bot_wall(200, captcha).is_some(),
            "captcha gate must escalate"
        );
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
        assert_eq!(
            serde_json::to_string(&FetchTier::Claude).unwrap(),
            "\"claude\""
        );
        assert_eq!(
            serde_json::to_string(&FetchTier::Archive).unwrap(),
            "\"archive\""
        );
        assert_eq!(
            serde_json::to_string(&FetchTier::ApiRecipe).unwrap(),
            "\"api_recipe\""
        );
        assert_eq!(FetchTier::ApiRecipe.as_str(), "api_recipe");
        assert_eq!(FetchTier::Archive.as_str(), "archive");
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
            Ok(RenderedPage {
                html: self.html.clone(),
                ..Default::default()
            })
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
            &FetcherConfig {
                min_content_chars: 100,
                ..FetcherConfig::default()
            },
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

    // --- Archive tier (tier zero) ---

    const GOOD_PAGE: &str = "<html><body><article>A perfectly ordinary page with plenty of \
        real readable content, well past the hundred-character threshold used \
        for escalation decisions in these tests.</article></body></html>";

    /// Archive stub that always serves a snapshot body.
    struct StubArchive {
        body: String,
    }
    #[async_trait]
    impl HttpClient for StubArchive {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            assert!(
                req.archive_max_age.is_some(),
                "the fetcher must thread archive_max_age to the archive engine"
            );
            Ok(HttpResponse {
                status: 200,
                headers: std::collections::HashMap::new(),
                body: self.body.clone(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    /// Archive stub that always misses (no snapshot within the window).
    struct MissArchive;
    #[async_trait]
    impl HttpClient for MissArchive {
        async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
            Err(Error::Http("no archive snapshot within window".into()))
        }
    }

    /// HTTP stub that serves a healthy live page.
    struct StubHttp;
    #[async_trait]
    impl HttpClient for StubHttp {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            Ok(HttpResponse {
                status: 200,
                headers: std::collections::HashMap::new(),
                body: GOOD_PAGE.into(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    struct DeadBrowser;
    #[async_trait]
    impl Browser for DeadBrowser {
        async fn render(&self, _req: RenderRequest) -> Result<RenderedPage> {
            panic!("browser tier must not be reached in these tests");
        }
    }

    fn archive_fetcher(http: Arc<dyn HttpClient>, archive: Option<Arc<dyn HttpClient>>) -> Fetcher {
        Fetcher::new(
            http,
            Arc::new(DeadBrowser),
            Arc::new(StubResearcher),
            enabled_governor(),
            &FetcherConfig {
                min_content_chars: 100,
                ..FetcherConfig::default()
            },
        )
        .with_archive(archive)
    }

    #[tokio::test]
    async fn archive_tier_wins_before_live_http_when_window_set() {
        // With a snapshot available, the live HTTP tier is never touched
        // (DeadHttp panics if called) — the whole point of tier zero.
        let fetcher = archive_fetcher(
            Arc::new(DeadHttp),
            Some(Arc::new(StubArchive {
                body: GOOD_PAGE.into(),
            })),
        );
        let mut req = FetchRequest::new("https://example.test/page");
        req.archive_max_age = Some(86_400);
        let out = fetcher.fetch(req).await.unwrap();
        assert_eq!(out.engine, "archive");
        assert_eq!(out.trace.len(), 1);
        assert_eq!(out.trace[0].tier, FetchTier::Archive);
        assert_eq!(out.trace[0].verdict, TierVerdict::Ok);
        assert_eq!(out.trace[0].http_status, Some(200));
    }

    #[tokio::test]
    async fn archive_miss_falls_through_to_live_http() {
        let fetcher = archive_fetcher(Arc::new(StubHttp), Some(Arc::new(MissArchive)));
        let mut req = FetchRequest::new("https://example.test/page");
        req.archive_max_age = Some(3600);
        let out = fetcher.fetch(req).await.unwrap();
        assert_eq!(out.engine, "http", "a miss must fall through to live");
        assert!(out
            .trace
            .iter()
            .any(|t| t.tier == FetchTier::Archive && t.verdict == TierVerdict::Error));
        assert!(out.escalations.iter().any(|e| e.contains("archive tier")));
    }

    #[tokio::test]
    async fn archive_thin_snapshot_falls_through_to_live_http() {
        // A snapshot that exists but is a thin shell must not be served.
        let fetcher = archive_fetcher(
            Arc::new(StubHttp),
            Some(Arc::new(StubArchive {
                body: "<html><body>tiny</body></html>".into(),
            })),
        );
        let mut req = FetchRequest::new("https://example.test/page");
        req.archive_max_age = Some(3600);
        let out = fetcher.fetch(req).await.unwrap();
        assert_eq!(out.engine, "http");
        assert!(out
            .trace
            .iter()
            .any(|t| t.tier == FetchTier::Archive && t.verdict == TierVerdict::Thin));
    }

    #[tokio::test]
    async fn archive_tier_requires_opt_in_window() {
        // No archive_max_age => the archive engine is never called, even when
        // wired (StubArchive's assert would fire on a None window; instead the
        // live tier serves as before).
        struct PanicArchive;
        #[async_trait]
        impl HttpClient for PanicArchive {
            async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
                panic!("archive engine must not be called without archive_max_age");
            }
        }
        let fetcher = archive_fetcher(Arc::new(StubHttp), Some(Arc::new(PanicArchive)));
        let out = fetcher
            .fetch(FetchRequest::new("https://example.test/page"))
            .await
            .unwrap();
        assert_eq!(out.engine, "http");
    }

    #[tokio::test]
    async fn archive_window_without_wired_engine_is_a_noop() {
        // [archive] disabled => Fetcher.archive is None => the window is inert.
        let fetcher = archive_fetcher(Arc::new(StubHttp), None);
        let mut req = FetchRequest::new("https://example.test/page");
        req.archive_max_age = Some(3600);
        let out = fetcher.fetch(req).await.unwrap();
        assert_eq!(out.engine, "http");
        assert!(out.trace.iter().all(|t| t.tier != FetchTier::Archive));
    }

    #[tokio::test]
    async fn browser_strategy_skips_the_archive_tier() {
        // An explicit Browser strategy wants a JS render; an archived static
        // body can't be that, so tier zero is skipped even with a window set.
        let governor = enabled_governor();
        let fetcher = Fetcher::new(
            Arc::new(DeadHttp),
            Arc::new(StubBrowser {
                html: GOOD_PAGE.into(),
            }),
            Arc::new(StubResearcher),
            governor,
            &FetcherConfig {
                min_content_chars: 100,
                ..FetcherConfig::default()
            },
        )
        .with_archive(Some(Arc::new(StubArchive {
            body: GOOD_PAGE.into(),
        })));
        let mut req = FetchRequest::new("https://example.test/page");
        req.strategy = FetchStrategy::Browser;
        req.archive_max_age = Some(3600);
        let out = fetcher.fetch(req).await.unwrap();
        assert_eq!(out.engine, "browser");
        assert!(out.trace.iter().all(|t| t.tier != FetchTier::Archive));
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
        assert!(outcome
            .trace
            .iter()
            .any(|t| t.tier == FetchTier::Browser && t.verdict == TierVerdict::Blocked));
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
        governor
            .penalize("recovering.example", Some(Duration::from_secs(4)))
            .await;
        assert_eq!(
            governor.penalty("recovering.example").await,
            Duration::from_secs(4)
        );

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

    // --- API-recipe tier (pre-archive, double-opt-in) ---

    use std::sync::Mutex;

    use crate::recipes::ApiRecipe;
    use serde_json::json;

    const API_URL: &str = "https://example.test/api/search?q=grants&page=1";
    const PAGE_URL: &str = "https://example.test/page";

    fn test_recipe(validated: bool) -> ApiRecipe {
        ApiRecipe {
            id: "r1".into(),
            host: "example.test".into(),
            url_template: "https://example.test/api/search?q={q}&page={page}".into(),
            params: json!({"q": "grants", "page": "1"}),
            json_paths: vec!["$.results[*].title".into()],
            score: 0.9,
            validated,
        }
    }

    /// Scripted [`RecipeSource`]: one recipe, call recording, no storage.
    #[derive(Default)]
    struct ScriptedRecipes {
        recipe: Option<ApiRecipe>,
        lookups: Mutex<Vec<(String, bool)>>,
        successes: Mutex<Vec<(String, bool)>>,
        failures: Mutex<Vec<(String, u32)>>,
    }
    #[async_trait]
    impl RecipeSource for ScriptedRecipes {
        async fn best_for_host(
            &self,
            host: &str,
            include_unvalidated: bool,
        ) -> Result<Option<ApiRecipe>> {
            self.lookups
                .lock()
                .unwrap()
                .push((host.to_string(), include_unvalidated));
            Ok(self
                .recipe
                .clone()
                .filter(|r| r.host == host && (r.validated || include_unvalidated)))
        }
        async fn record_success(&self, id: &str, validate: bool) -> Result<()> {
            self.successes
                .lock()
                .unwrap()
                .push((id.to_string(), validate));
            Ok(())
        }
        async fn record_failure(&self, id: &str, unvalidate_after: u32) -> Result<bool> {
            self.failures
                .lock()
                .unwrap()
                .push((id.to_string(), unvalidate_after));
            Ok(false)
        }
    }

    /// Never-called guards for gating tests.
    struct PanicRecipes;
    #[async_trait]
    impl RecipeSource for PanicRecipes {
        async fn best_for_host(&self, _: &str, _: bool) -> Result<Option<ApiRecipe>> {
            panic!("recipes must not be consulted without an opt-in");
        }
        async fn record_success(&self, _: &str, _: bool) -> Result<()> {
            unreachable!()
        }
        async fn record_failure(&self, _: &str, _: u32) -> Result<bool> {
            unreachable!()
        }
    }

    struct PanicArchive;
    #[async_trait]
    impl HttpClient for PanicArchive {
        async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
            panic!("archive tier must not run when the recipe tier wins");
        }
    }

    /// HTTP stub that routes by URL: JSON for the recipe's API URL, a healthy
    /// page for everything else (the live-HTTP fallback).
    struct RouteHttp {
        api_body: String,
        api_status: u16,
    }
    #[async_trait]
    impl HttpClient for RouteHttp {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            let (status, body) = if req.url == API_URL {
                (self.api_status, self.api_body.clone())
            } else {
                (200, GOOD_PAGE.to_string())
            };
            Ok(HttpResponse {
                status,
                headers: std::collections::HashMap::new(),
                body,
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    fn recipe_fetcher(
        http: Arc<dyn HttpClient>,
        source: Arc<dyn RecipeSource>,
        cfg: &RecipesConfig,
    ) -> Fetcher {
        Fetcher::new(
            http,
            Arc::new(DeadBrowser),
            Arc::new(StubResearcher),
            enabled_governor(),
            &FetcherConfig {
                min_content_chars: 100,
                ..FetcherConfig::default()
            },
        )
        .with_recipes(Some(source), cfg)
    }

    const OVERLAPPING_JSON: &str =
        r#"{"results": [{"title": "Alpha Grant"}, {"title": "Beta Grant"}]}"#;

    #[tokio::test]
    async fn recipe_tier_wins_before_archive_and_live() {
        // Validated recipe + overlapping JSON replay: the fetch never touches
        // the archive (PanicArchive) even though a window is set, and the trace
        // carries a single winning `api_recipe` entry.
        let source = Arc::new(ScriptedRecipes {
            recipe: Some(test_recipe(true)),
            ..Default::default()
        });
        let fetcher = recipe_fetcher(
            Arc::new(RouteHttp {
                api_body: OVERLAPPING_JSON.into(),
                api_status: 200,
            }),
            source.clone(),
            &RecipesConfig::default(),
        )
        .with_archive(Some(Arc::new(PanicArchive)));

        let mut req = FetchRequest::new(PAGE_URL);
        req.use_recipes = true;
        req.archive_max_age = Some(3600);
        let out = fetcher.fetch(req).await.unwrap();

        assert_eq!(out.engine, "api_recipe");
        assert_eq!(out.status, Some(200));
        let body: serde_json::Value = serde_json::from_str(out.text.as_deref().unwrap()).unwrap();
        assert_eq!(body["results"][0]["title"], "Alpha Grant");
        assert_eq!(out.trace.len(), 1);
        assert_eq!(out.trace[0].tier, FetchTier::ApiRecipe);
        assert_eq!(out.trace[0].verdict, TierVerdict::Ok);
        // Validated-only lookup (auto_validate off), success without promotion.
        assert_eq!(
            source.lookups.lock().unwrap().as_slice(),
            &[("example.test".to_string(), false)]
        );
        assert_eq!(
            source.successes.lock().unwrap().as_slice(),
            &[("r1".to_string(), false)]
        );
        assert!(source.failures.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn recipe_mismatch_strikes_and_falls_through_to_live() {
        // The API answered but lost the expected field paths: strike recorded
        // (with the configured threshold), fall through to the live HTTP tier.
        let source = Arc::new(ScriptedRecipes {
            recipe: Some(test_recipe(true)),
            ..Default::default()
        });
        let cfg = RecipesConfig {
            enabled: true, // config switch (no per-request flag) also gates in
            max_failures: 5,
            ..RecipesConfig::default()
        };
        let fetcher = recipe_fetcher(
            Arc::new(RouteHttp {
                api_body: r#"{"items": [{"name": "renamed shape"}]}"#.into(),
                api_status: 200,
            }),
            source.clone(),
            &cfg,
        );

        let out = fetcher.fetch(FetchRequest::new(PAGE_URL)).await.unwrap();
        assert_eq!(out.engine, "http", "a thin replay must fall through");
        assert!(out
            .trace
            .iter()
            .any(|t| t.tier == FetchTier::ApiRecipe && t.verdict == TierVerdict::Thin));
        assert!(out
            .escalations
            .iter()
            .any(|e| e.contains("api_recipe tier thin")));
        assert_eq!(
            source.failures.lock().unwrap().as_slice(),
            &[("r1".to_string(), 5)],
            "strike must carry the configured un-validate threshold"
        );
        assert!(source.successes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn recipe_engine_error_strikes_and_falls_through() {
        // The API endpoint errors outright: strike + Error trace entry, then
        // the live ladder serves as usual.
        struct ApiFailsHttp;
        #[async_trait]
        impl HttpClient for ApiFailsHttp {
            async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
                if req.url == API_URL {
                    return Err(Error::Http("connection refused".into()));
                }
                Ok(HttpResponse {
                    status: 200,
                    headers: std::collections::HashMap::new(),
                    body: GOOD_PAGE.into(),
                    final_url: req.url,
                    cache_hit: false,
                })
            }
        }
        let source = Arc::new(ScriptedRecipes {
            recipe: Some(test_recipe(true)),
            ..Default::default()
        });
        let mut req = FetchRequest::new(PAGE_URL);
        req.use_recipes = true;
        let out = recipe_fetcher(
            Arc::new(ApiFailsHttp),
            source.clone(),
            &RecipesConfig::default(),
        )
        .fetch(req)
        .await
        .unwrap();
        assert_eq!(out.engine, "http");
        assert!(out
            .trace
            .iter()
            .any(|t| t.tier == FetchTier::ApiRecipe && t.verdict == TierVerdict::Error));
        assert_eq!(source.failures.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unvalidated_recipe_needs_auto_validate_and_then_promotes() {
        // auto_validate OFF: the unvalidated recipe is invisible (validated-only
        // lookup) — the live tier serves and no recipe call is recorded.
        let source = Arc::new(ScriptedRecipes {
            recipe: Some(test_recipe(false)),
            ..Default::default()
        });
        let http = Arc::new(RouteHttp {
            api_body: OVERLAPPING_JSON.into(),
            api_status: 200,
        });
        let mut req = FetchRequest::new(PAGE_URL);
        req.use_recipes = true;
        let out = recipe_fetcher(http.clone(), source.clone(), &RecipesConfig::default())
            .fetch(req.clone())
            .await
            .unwrap();
        assert_eq!(out.engine, "http");
        assert!(out.trace.iter().all(|t| t.tier != FetchTier::ApiRecipe));
        assert!(source.successes.lock().unwrap().is_empty());

        // auto_validate ON: tried opportunistically, and the successful
        // overlapping replay promotes it (record_success validate: true).
        let cfg = RecipesConfig {
            auto_validate: true,
            ..RecipesConfig::default()
        };
        let out = recipe_fetcher(http, source.clone(), &cfg)
            .fetch(req)
            .await
            .unwrap();
        assert_eq!(out.engine, "api_recipe");
        assert_eq!(
            source.successes.lock().unwrap().as_slice(),
            &[("r1".to_string(), true)],
            "a successful overlapping replay must validate the recipe"
        );
    }

    #[tokio::test]
    async fn recipes_are_never_consulted_without_an_opt_in() {
        // Neither `use_recipes` nor `[recipes] enabled`: the wired source is
        // never even looked up (PanicRecipes) — default behavior is untouched.
        let fetcher = recipe_fetcher(
            Arc::new(StubHttp),
            Arc::new(PanicRecipes),
            &RecipesConfig::default(),
        );
        let out = fetcher.fetch(FetchRequest::new(PAGE_URL)).await.unwrap();
        assert_eq!(out.engine, "http");

        // The browser-only strategy also skips the recipe tier even opted in.
        let governor = enabled_governor();
        let fetcher = Fetcher::new(
            Arc::new(DeadHttp),
            Arc::new(StubBrowser {
                html: GOOD_PAGE.into(),
            }),
            Arc::new(StubResearcher),
            governor,
            &FetcherConfig {
                min_content_chars: 100,
                ..FetcherConfig::default()
            },
        )
        .with_recipes(
            Some(Arc::new(PanicRecipes)),
            &RecipesConfig {
                enabled: true,
                ..RecipesConfig::default()
            },
        );
        let mut req = FetchRequest::new(PAGE_URL);
        req.strategy = FetchStrategy::Browser;
        let out = fetcher.fetch(req).await.unwrap();
        assert_eq!(out.engine, "browser");
    }
}
