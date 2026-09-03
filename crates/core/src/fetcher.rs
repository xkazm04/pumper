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

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::config::{FetcherConfig, RecipesConfig};
use crate::engine::{
    anonymous_profile, anonymous_profile_note, snapshot_provenance, Browser, HttpClient,
    HttpRequest, RenderRequest, Researcher, SnapshotProvenance,
};
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

// ---- Remote-fabric egress attribution ---------------------------------------
//
// The remote fetch fabric's entire product claim is "this fetch left from a
// different IP/geography", and until now nothing in the product could confirm
// it happened: `engine` is the literal string `"http"` whether the body came
// from this machine or a peer in another country, and the fabric's total
// observability was one coordinator-side `warn!` on the FAILURE path. A
// misconfigured secret made every peer answer 401 → warn → silent local
// fallback, forever, with a log line that reads identically whether one fetch
// or a million fell back.
//
// The carrier is a reserved response header rather than a new field on
// `HttpResponse`, following `engine::FETCHED_VIA_HEADER` exactly: the header map
// is the only channel that survives an engine boundary, and `FetchOutcome` has
// never had one. The constants live HERE, next to the only reader, because
// `pumper-core` cannot depend on `pumper-engine-remote` (engines depend on core,
// never the reverse) while the reader must be in the fetcher.

/// Reserved response header naming the peer node that served a fetch. Written by
/// the coordinator-side `RemoteEngine` after it has verified the envelope, read
/// once at the fetcher's HTTP-tier seam.
///
/// Namespaced under `x-pumper-` so it cannot collide with a real target-site
/// header, and — like [`crate::engine::FETCHED_VIA_HEADER`] — read **only where
/// the fabric is wired**, so an origin that echoes it on an ordinary live fetch
/// cannot forge "a peer served this".
pub const REMOTE_NODE_HEADER: &str = "x-pumper-remote-node";

/// Reserved response header carrying the URL a serving node was **asked** for,
/// echoed back in the envelope so the coordinator can bind the answer to the
/// question.
///
/// Distinct from `HttpResponse.final_url`, which is where the fetch *ended* and
/// legitimately differs after a redirect. This is a wire artifact: the
/// coordinator verifies it and strips it, so it never reaches a consumer.
pub const REMOTE_TARGET_HEADER: &str = "x-pumper-remote-target";

/// Prefix of the escalation-trail line a peer-served fetch leaves, so the fact
/// reaches the job's `cost_events.detail` (via `fetch_cost_detail`) and from
/// there the receipt.
///
/// One writer ([`Fetcher::try_http_tier`]) and one reader (the job receipt),
/// both through this constant — the same single-constant discipline the archive
/// provenance headers use, and the reason this is not "parsing the prose".
pub const EGRESS_TRAIL_PREFIX: &str = "egress via remote node ";

/// The one rendering of "a peer served this" — used by the tier trace's `detail`
/// and the escalation trail, so the two surfaces cannot drift into describing
/// the same fetch differently. Mirrors `SnapshotProvenance::note()`.
pub fn egress_note(node: &str) -> String {
    format!("{EGRESS_TRAIL_PREFIX}{node}")
}

/// Joins the http tier's notes into one `TierTrace.detail`.
///
/// One renderer for both the winning and the losing entry, so the two can never
/// drift into describing the same fetch differently — and so a third note (the
/// anonymous-profile marker) did not have to be threaded through two hand-rolled
/// `match` arms. `None` when there is nothing to say, which is the common case:
/// a clean local http win's tier and status already say everything.
fn http_tier_detail(
    why: Option<String>,
    served_by: Option<&str>,
    anonymous: Option<&str>,
) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(why) = why {
        parts.push(why);
    }
    if let Some(node) = served_by {
        parts.push(egress_note(node));
    }
    if let Some(profile) = anonymous {
        parts.push(anonymous_profile_note(profile));
    }
    (!parts.is_empty()).then(|| parts.join("; "))
}

/// Lifts the serving node off a response header map, or `None` when the body
/// egressed locally.
///
/// ASCII-case-insensitive (a header map that round-tripped a real HTTP stack has
/// no guaranteed casing) and an empty value counts as absent, because a marker
/// that says nothing is not attribution.
pub fn remote_egress(headers: &std::collections::HashMap<String, String>) -> Option<&str> {
    headers.iter().find_map(|(name, value)| {
        (name.eq_ignore_ascii_case(REMOTE_NODE_HEADER) && !value.trim().is_empty())
            .then(|| value.trim())
    })
}

/// Process-wide count of how the live-HTTP tier actually egressed while the
/// remote fabric was wired.
///
/// Counts, not just log lines: "a peer served it" and "we fell back" were
/// previously distinguishable only by grepping for a `warn!` that reads the same
/// for one fetch and for a million. Held behind an `Arc` because [`Fetcher`] is
/// cloned into every `EngineSet` and the numbers must be the same numbers.
#[derive(Debug, Default)]
pub struct EgressCounters {
    peer_served: AtomicU64,
    local_fallback: AtomicU64,
}

impl EgressCounters {
    fn record(&self, peer_served: bool) {
        let counter = if peer_served {
            &self.peer_served
        } else {
            &self.local_fallback
        };
        counter.fetch_add(1, AtomicOrdering::Relaxed);
    }
    /// Live-HTTP-tier fetches a peer node served.
    pub fn peer_served(&self) -> u64 {
        self.peer_served.load(AtomicOrdering::Relaxed)
    }
    /// Live-HTTP-tier fetches that egressed from this coordinator despite the
    /// fabric being configured — every one of these left from the IP the
    /// operator deployed the fabric to stop using.
    pub fn local_fallback(&self) -> u64 {
        self.local_fallback.load(AtomicOrdering::Relaxed)
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
    /// Set **only** when the body came out of a stored capture instead of the
    /// live site — i.e. the archive tier won — carrying which store served it
    /// and when the page was captured. `None` on every live tier.
    ///
    /// This is the field to branch on: `engine == "archive"` says *which tier*
    /// answered, but the capture time is the variable the tier actually trades
    /// (freshness for availability), and it used to be dropped at the engine
    /// boundary — a row extracted from a 2019 snapshot was byte-identical to
    /// one extracted from today's page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<crate::engine::SnapshotProvenance>,
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
    /// Remote fetch fabric (M17, `[remote]`, default OFF => `None`). When
    /// wired, the **live-HTTP tier** routes through this client instead of the
    /// plain local engine: the remote engine round-robins the configured peer
    /// nodes' `/fetch-proxy` endpoints and internally falls back to the local
    /// engine on any node error — so this substitution can change *where* a
    /// fetch egresses from, never whether it succeeds. The recipe tier and the
    /// archive engine's inner transport deliberately stay local.
    remote: Option<Arc<dyn HttpClient>>,
    /// Peer-served vs fell-back-to-local counts for the live-HTTP tier, so the
    /// fabric's success is a number an operator can read rather than the absence
    /// of a warning. Shared across clones of this `Fetcher`.
    egress: Arc<EgressCounters>,
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
            remote: None,
            egress: Arc::new(EgressCounters::default()),
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

    /// Wires the (optional) remote fetch-fabric client (M17, `[remote]`) into
    /// the **live-HTTP tier position**. `None` — the default — leaves the
    /// ladder exactly as it was. The wired client is expected to fall back to
    /// the local engine itself on node failure (see `pumper-engine-remote`),
    /// so from the ladder's perspective it is just an HTTP tier that may
    /// egress elsewhere.
    pub fn with_remote(mut self, remote: Option<Arc<dyn HttpClient>>) -> Self {
        self.remote = remote;
        self
    }

    /// Live-HTTP-tier egress counts (peer-served vs local fallback) while the
    /// remote fabric is wired. Read by `/metrics`; both stay `0` when `[remote]`
    /// is off, because nothing is being substituted.
    pub fn egress_counters(&self) -> &Arc<EgressCounters> {
        &self.egress
    }

    /// The client serving the live-HTTP tier: the remote fabric when wired,
    /// the plain local engine otherwise.
    fn live_http(&self) -> &Arc<dyn HttpClient> {
        self.remote.as_ref().unwrap_or(&self.http)
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
                            // Provenance is lifted **here** — inside the branch
                            // where the archive engine is the one that answered.
                            // The header is forgeable by any origin, so reading
                            // it on the live tiers would let a hostile host
                            // stamp its own page "archived".
                            let snapshot = archive_snapshot(&resp.headers);
                            trace.push(TierTrace {
                                tier: FetchTier::Archive,
                                verdict: TierVerdict::Ok,
                                http_status: Some(resp.status),
                                content_chars: Some(text_len),
                                cache_hit: Some(resp.cache_hit),
                                latency_ms,
                                cost_usd: None,
                                // The trace is the human-facing half of a fetch;
                                // a `detail: None` archive win read there
                                // exactly like a live one.
                                detail: Some(snapshot.note()),
                            });
                            return Ok(outcome(
                                "archive",
                                &req,
                                Some(resp.status),
                                resp.body,
                                markdown,
                                escalations,
                                trace,
                                Some(snapshot),
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
                        e,
                        started,
                    )?,
                }
            }
        }

        // --- HTTP tier --- (skip_http only applies to escalating strategies;
        // an explicit Http strategy is the caller's call.)
        if http_tier_attempted(req.strategy, req.skip_http) {
            if let Some(out) = self
                .try_http_tier(&req, min_chars, &mut escalations, &mut trace)
                .await?
            {
                return Ok(out);
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
                            // A live render, by definition.
                            None,
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
                Err(e) => {
                    trace_tier_error(
                        &mut escalations,
                        &mut trace,
                        FetchTier::Browser,
                        "browser",
                        e,
                        started,
                    )?;
                    // A dead browser must not take the whole ladder down with
                    // it — least of all on the hosts the learned router pinned
                    // to the browser, which is exactly where traffic is
                    // concentrated. Un-skip the http tier and try it now.
                    if browser_failure_falls_back_to_http(
                        req.strategy,
                        req.skip_http,
                        TierVerdict::Error,
                    ) {
                        escalations.push(
                            "http tier un-skipped: browser engine failed, retrying the tier the \
                             router had skipped"
                                .to_string(),
                        );
                        if let Some(out) = self
                            .try_http_tier(&req, min_chars, &mut escalations, &mut trace)
                            .await?
                        {
                            return Ok(out);
                        }
                    }
                }
            }
        }

        // --- Claude research tier ---
        let mut claude_spend = None;
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
            // Every other tier traces its engine error and lets the ladder end
            // on the exhaustion error; the Claude tier used to `?` the raw
            // engine error out instead, so the last tier's failure erased the
            // whole trail of what the earlier tiers found.
            match self.claude.research(research).await {
                Ok(out) => {
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
                        snapshot: None,
                    });
                }
                Err(e) => {
                    // The paid tier can fail *after* spending. That money is
                    // real and has to reach the job's ledger, and this error is
                    // the only channel out — see `ladder_exhausted`.
                    claude_spend = e.claude_spend();
                    trace_tier_error(
                        &mut escalations,
                        &mut trace,
                        FetchTier::Claude,
                        "claude",
                        e,
                        started,
                    )?;
                }
            }
        }

        Err(ladder_exhausted(
            &req.url,
            &trace,
            &escalations,
            claude_spend,
        ))
    }

    /// One attempt at the HTTP tier, at either of its two positions in the
    /// ladder: its normal cheap-first slot, or the fallback slot after a browser
    /// engine failure on a host the router had pinned past it.
    ///
    /// `Ok(Some(outcome))` = this tier produced the result; `Ok(None)` = it was
    /// thin/blocked/errored and the caller climbs (both the human trail line and
    /// the structured trace entry are already appended). `Err` only for the
    /// explicit `Http` strategy, which has nothing to climb to.
    ///
    /// Extracted so the fallback runs the *same* attempt — acceptance bar,
    /// Markdown handling, trace shape and all — rather than a second, subtly
    /// different copy. The request is still governed inside `HttpEngine::send`,
    /// so a fallback attempt is spaced like any other.
    async fn try_http_tier(
        &self,
        req: &FetchRequest,
        min_chars: usize,
        escalations: &mut Vec<String>,
        trace: &mut Vec<TierTrace>,
    ) -> Result<Option<FetchOutcome>> {
        let mut http_req = HttpRequest::get(&req.url);
        http_req.no_cache = req.no_cache;
        http_req.ttl_override = req.ttl_override;
        http_req.profile = req.profile.clone();
        let started = Instant::now();
        match self.live_http().fetch(http_req).await {
            Ok(resp) => {
                let latency_ms = elapsed_ms(started);
                // Egress attribution. The remote fabric marks a peer-served body
                // with `REMOTE_NODE_HEADER`; this is the seam that lifts it off
                // the header map (which does not survive a tiered fetch) and
                // into the trail + trace + counters, the same way the archive
                // tier's provenance header is lifted. Read only when the fabric
                // is actually wired, so a hostile origin cannot stamp its own
                // page "served by a peer" — the same forgery rule the archive
                // provenance follows.
                let served_by = self
                    .remote
                    .is_some()
                    .then(|| remote_egress(&resp.headers))
                    .flatten()
                    .map(str::to_string);
                if self.remote.is_some() {
                    self.egress.record(served_by.is_some());
                }
                if let Some(node) = &served_by {
                    // Into the human trail, so it reaches this fetch's
                    // `cost_events.detail` through `fetch_cost_detail` — the
                    // same path `SnapshotProvenance::note()` takes.
                    escalations.push(format!("{EGRESS_TRAIL_PREFIX}{node}"));
                }
                // "This fetch named a login and went out anonymous" travels the
                // same way, and for the same reason: the header map is the only
                // channel that survives an engine boundary. Read ONLY when the
                // caller actually asked for a profile, so an origin cannot stamp
                // an ordinary fetch — the forgery rule the archive provenance
                // and the egress marker both follow.
                let anonymous = req
                    .profile
                    .is_some()
                    .then(|| anonymous_profile(&resp.headers))
                    .flatten()
                    .map(str::to_string);
                if let Some(name) = &anonymous {
                    escalations.push(anonymous_profile_note(name));
                }
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
                let enough =
                    wall.is_none() && resp.is_success() && text_len.is_none_or(|n| n >= min_chars);
                if enough || req.strategy == FetchStrategy::Http {
                    trace.push(TierTrace {
                        tier: FetchTier::Http,
                        verdict: TierVerdict::Ok,
                        http_status: Some(resp.status),
                        content_chars: text_len,
                        cache_hit,
                        latency_ms,
                        cost_usd: None,
                        // A clean local http win says everything with its tier
                        // and status; a peer-served one does not, because "this
                        // left from another IP" is the whole product claim and
                        // was previously invisible everywhere past the header
                        // map. Neither does a WINNING profiled fetch that
                        // carried no session — that is precisely the fetch whose
                        // 200 is a login wall about to be stored as data.
                        detail: http_tier_detail(None, served_by.as_deref(), anonymous.as_deref()),
                    });
                    return Ok(Some(outcome(
                        "http",
                        req,
                        Some(resp.status),
                        resp.body,
                        markdown,
                        std::mem::take(escalations),
                        std::mem::take(trace),
                        // The live web: no stored capture, whatever headers the
                        // origin chose to send.
                        None,
                    )));
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
                    // A LOSING peer-served tier is the one an operator most
                    // needs attributed: "the http tier came back blocked" reads
                    // very differently once you know which node's IP it came
                    // back blocked at — or that the login it named was empty.
                    detail: http_tier_detail(detail, served_by.as_deref(), anonymous.as_deref()),
                });
                Ok(None)
            }
            Err(e) if req.strategy == FetchStrategy::Http => Err(e),
            Err(e) => {
                trace_tier_error(escalations, trace, FetchTier::Http, "http", e, started)?;
                Ok(None)
            }
        }
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
                        // A recipe replay is a live API call, not a stored body.
                        snapshot: None,
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
                // The one tier that does NOT take the break arm: a recipe is a
                // learned artifact, and its failure un-validates it (the strike
                // above) instead of indicting the ladder. `Option` here is the
                // contract — this tier can only fall through.
                let _ = trace_tier_error(
                    escalations,
                    trace,
                    FetchTier::ApiRecipe,
                    "api_recipe",
                    e,
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
///
/// **`Err` is the ladder's second break arm.** A tier that failed for a reason
/// that is *ours* ([`Error::is_router_failure`] — a broken `[section]`, a
/// pre-flight refusal, an unloadable plugin) reproduces identically on every
/// remaining tier, so this returns it instead of escalating: one tier tried, one
/// failure reported, and the job row names the origin (`config: …`) rather than
/// the ladder's exhaustion prose. It also keeps the browser tier's http un-skip
/// from overturning a correct routing decision on evidence about pumper.
/// `Ok(())` is a candidate failure and escalates exactly as it always has.
fn trace_tier_error(
    escalations: &mut Vec<String>,
    trace: &mut Vec<TierTrace>,
    tier: FetchTier,
    name: &str,
    err: Error,
    started: Instant,
) -> Result<()> {
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
    if err.is_router_failure() {
        return Err(err);
    }
    Ok(())
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

/// The error that ends the ladder when every tier has been tried: the exhaustion
/// message carrying the whole trail.
///
/// **Why the variant depends on `claude_spend`.** The paid tier can spend money
/// and *then* fail, and the fetcher has no cost ledger to write that to — the
/// error is its only channel to [`crate::app::AppContext::fetch`], which does.
/// So when (and only when) the Claude tier reports something the ledger must
/// record, the same message is minted as the cost-carrying `Error::Claude`;
/// otherwise it stays `Error::App`, which is what the ladder has always
/// returned. **The text is identical either way** — the trail is the payload for
/// humans, the variant is the payload for the ledger.
fn ladder_exhausted(
    url: &str,
    trace: &[TierTrace],
    escalations: &[String],
    claude_spend: Option<crate::error::ClaudeSpend>,
) -> Error {
    let message = format!(
        "all fetch tiers exhausted for {} (attempted: {}): {}",
        url,
        attempted_tiers(trace),
        escalations.join("; ")
    );
    match claude_spend.filter(|s| s.ledger_event().is_some()) {
        Some(spend) => Error::Claude { message, spend },
        None => Error::App(message),
    }
}

/// Whether the ladder attempts the http tier at its normal cheap-first slot.
/// `skip_http` (set by the learned tier router for hosts where HTTP keeps
/// losing) only applies to the escalating strategies — an explicit `Http`
/// strategy is the caller's call and always runs.
fn http_tier_attempted(strategy: FetchStrategy, skip_http: bool) -> bool {
    strategy == FetchStrategy::Http
        || (!skip_http
            && matches!(
                strategy,
                FetchStrategy::Auto | FetchStrategy::AutoWithResearch
            ))
}

/// Whether a failed browser attempt should fall back to the http tier that was
/// skipped before it.
///
/// The anti-pattern (`browser_down_does_not_kill_pinned_hosts`): with Chrome
/// down, every escalating fetch failed outright — and hosts the learned router
/// had pinned to the browser skipped the *working* http tier on the way, so the
/// router amplified the outage exactly where it concentrates traffic. A dead
/// engine is not evidence that the cheap tier is dead too.
///
/// Deliberately narrow:
/// - **Engine errors only.** A `Blocked` or `Thin` browser verdict is a real
///   observation about the page (the router pinned this host to the browser
///   *because* http kept losing), so re-running http there would just spend a
///   politeness slot to re-learn what we already know.
/// - **Escalating strategies only.** An explicit `Browser` strategy asked for a
///   JS render; a static body is not that, so it keeps its fail-fast.
/// - The fetcher cannot tell a router-set `skip_http` from a caller-set one —
///   nothing in `FetchRequest` records who set it — so both fall back. That is
///   the safe direction: the alternative is a caller-pinned host losing its
///   whole ladder to an unrelated engine outage, and the cost of being wrong is
///   one extra governed HTTP request on a fetch that was about to fail anyway.
fn browser_failure_falls_back_to_http(
    strategy: FetchStrategy,
    skip_http: bool,
    verdict: TierVerdict,
) -> bool {
    verdict == TierVerdict::Error
        && matches!(
            strategy,
            FetchStrategy::Auto | FetchStrategy::AutoWithResearch
        )
        && !http_tier_attempted(strategy, skip_http)
}

/// The tiers that actually ran, in trace order, for the exhaustion error — so a
/// failed fetch names the ladder it climbed instead of only the last reason.
fn attempted_tiers(trace: &[TierTrace]) -> String {
    let mut names: Vec<&'static str> = Vec::new();
    for t in trace {
        let name = t.tier.as_str();
        if !names.contains(&name) {
            names.push(name);
        }
    }
    if names.is_empty() {
        return "none".to_string();
    }
    names.join(", ")
}

/// The provenance an **archive-tier win** carries: whatever the serving engine
/// marked on the response ([`snapshot_provenance`]), falling back to the fact
/// the fetcher knows on its own — this body came out of the archive tier, and
/// its capture time was not reported.
///
/// The fallback is what stops [`FetchOutcome::snapshot`] and
/// `FetchOutcome::engine == "archive"` from ever disagreeing. Without it an
/// archive engine that forgot the header (a wrapper, a second snapshot source,
/// a stub) would mint an outcome that names the archive tier and simultaneously
/// reports a live body — the exact indistinguishability this field exists to
/// end, reintroduced through the back door.
fn archive_snapshot(headers: &std::collections::HashMap<String, String>) -> SnapshotProvenance {
    snapshot_provenance(headers).unwrap_or_else(|| SnapshotProvenance {
        via: FetchTier::Archive.as_str().to_string(),
        captured_at: None,
    })
}

/// `snapshot` is a required argument rather than a defaulted field so every
/// present and future tier has to answer "did this body come from the live site
/// or out of a store?" at the one place an outcome is minted. A tier that serves
/// the live web passes `None`.
#[allow(clippy::too_many_arguments)]
fn outcome(
    engine: &'static str,
    req: &FetchRequest,
    status: Option<u16>,
    html: String,
    markdown: Option<String>,
    escalations: Vec<String>,
    trace: Vec<TierTrace>,
    snapshot: Option<crate::engine::SnapshotProvenance>,
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
        snapshot,
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

    /// Archive stub that serves a snapshot **and marks it** exactly as the real
    /// `ArchiveEngine` does — the two provenance headers on the response.
    struct MarkedArchive {
        captured_at: &'static str,
    }
    #[async_trait]
    impl HttpClient for MarkedArchive {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            let mut headers = std::collections::HashMap::new();
            headers.insert(
                crate::engine::FETCHED_VIA_HEADER.to_string(),
                "archive".to_string(),
            );
            headers.insert(
                crate::engine::SNAPSHOT_TS_HEADER.to_string(),
                self.captured_at.to_string(),
            );
            Ok(HttpResponse {
                status: 200,
                headers,
                body: GOOD_PAGE.into(),
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
            Err(Error::http("no archive snapshot within window"))
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

    // ── remote-fabric egress attribution ────────────────────────────────────

    /// A live-HTTP stub that answers as the remote fabric does: a body plus the
    /// reserved node marker.
    struct PeerServedHttp(&'static str);
    #[async_trait]
    impl HttpClient for PeerServedHttp {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            Ok(HttpResponse {
                status: 200,
                headers: std::collections::HashMap::from([(
                    REMOTE_NODE_HEADER.to_string(),
                    self.0.to_string(),
                )]),
                body: GOOD_PAGE.into(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    fn remote_fetcher(remote: Option<Arc<dyn HttpClient>>, local: Arc<dyn HttpClient>) -> Fetcher {
        Fetcher::new(
            local,
            Arc::new(DeadBrowser),
            Arc::new(StubResearcher),
            enabled_governor(),
            &FetcherConfig {
                min_content_chars: 100,
                ..FetcherConfig::default()
            },
        )
        .with_remote(remote)
    }

    /// The anti-pattern: **an unattributable substitution**. `engine` reads
    /// `"http"` whether a body came off this machine or a peer in another
    /// country, and no field on the outcome, the trace or the receipt said
    /// otherwise — so the fabric's one product claim was unverifiable from
    /// inside the product.
    #[tokio::test]
    async fn a_peer_served_fetch_names_its_node_where_a_local_one_says_nothing() {
        let fetcher = remote_fetcher(
            Some(Arc::new(PeerServedHttp("http://node-b:8088"))),
            Arc::new(DeadHttp),
        );
        let out = fetcher
            .fetch(FetchRequest::new("https://example.test/page"))
            .await
            .unwrap();
        assert_eq!(out.engine, "http");
        assert_eq!(
            out.trace[0].detail.as_deref(),
            Some("egress via remote node http://node-b:8088"),
            "the winning tier has to say which node served it"
        );
        // And into the trail, which is what carries it to `cost_events.detail`
        // and from there to the job receipt.
        assert!(
            out.escalations
                .iter()
                .any(|line| line.starts_with(EGRESS_TRAIL_PREFIX)),
            "{:?}",
            out.escalations
        );
        assert_eq!(fetcher.egress_counters().peer_served(), 1);
        assert_eq!(fetcher.egress_counters().local_fallback(), 0);

        // A local-egress fetch through the SAME wired fabric is distinguishable
        // without reading a log: no detail, and it lands on the other counter.
        let fell_back = remote_fetcher(Some(Arc::new(StubHttp)), Arc::new(DeadHttp));
        let out = fell_back
            .fetch(FetchRequest::new("https://example.test/page"))
            .await
            .unwrap();
        assert_eq!(out.trace[0].detail, None);
        assert!(out.escalations.is_empty());
        assert_eq!(fell_back.egress_counters().peer_served(), 0);
        assert_eq!(fell_back.egress_counters().local_fallback(), 1);
    }

    /// The anti-pattern: **provenance a hostile origin can forge**. The archive
    /// tier's marker is read only inside the archive branch for exactly this
    /// reason; the egress marker follows it. With `[remote]` off, a target site
    /// that echoes `x-pumper-remote-node` must not be able to claim its page
    /// left from somewhere else — and the counters must stay silent rather than
    /// counting fetches on a deployment that has no fabric.
    #[tokio::test]
    async fn an_origin_cannot_stamp_itself_as_peer_served_when_the_fabric_is_off() {
        let fetcher = remote_fetcher(None, Arc::new(PeerServedHttp("http://attacker.example")));
        let out = fetcher
            .fetch(FetchRequest::new("https://example.test/page"))
            .await
            .unwrap();
        assert_eq!(out.trace[0].detail, None, "an origin cannot forge egress");
        assert!(out.escalations.is_empty());
        assert_eq!(fetcher.egress_counters().peer_served(), 0);
        assert_eq!(
            fetcher.egress_counters().local_fallback(),
            0,
            "with no fabric wired there is no fallback to count"
        );
    }

    // ── anonymous-profile provenance ────────────────────────────────────────

    /// A live-HTTP stub answering the way `engine-http` answers a profiled fetch
    /// whose cookie jar turned out to be empty: a perfectly healthy 200, plus
    /// the reserved marker.
    struct AnonymousProfileHttp(&'static str);
    #[async_trait]
    impl HttpClient for AnonymousProfileHttp {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            Ok(HttpResponse {
                status: 200,
                headers: std::collections::HashMap::from([(
                    crate::engine::ANONYMOUS_PROFILE_HEADER.to_string(),
                    self.0.to_string(),
                )]),
                body: GOOD_PAGE.into(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    fn profiled_request(url: &str, profile: Option<&str>) -> FetchRequest {
        let mut req = FetchRequest::new(url);
        req.profile = profile.map(str::to_string);
        req
    }

    /// The anti-pattern: **a login that silently is not one**. A mistyped
    /// `profile` fetched the login wall with a 200; it cleared
    /// `min_content_chars`, the tier recorded `TierVerdict::Ok`, and the
    /// extractor stored the login page as a real dataset revision. Nothing
    /// downstream could tell — the http engine mapped a missing `cookies.json`
    /// to an empty jar with no signal at all.
    ///
    /// The fact has to reach the WINNING entry, because the winning fetch is
    /// exactly the one about to be stored as data.
    #[tokio::test]
    async fn a_profiled_fetch_that_carried_no_session_says_so_on_the_winning_tier() {
        let fetcher = archive_fetcher(Arc::new(AnonymousProfileHttp("acme_portl")), None);
        let out = fetcher
            .fetch(profiled_request(
                "https://example.test/page",
                Some("acme_portl"),
            ))
            .await
            .unwrap();
        assert_eq!(out.engine, "http");
        let detail = out.trace[0].detail.as_deref().unwrap_or_default();
        assert!(
            detail.contains("acme_portl") && detail.contains("ANONYMOUS"),
            "the winning tier must name the profile that carried nothing: {detail:?}"
        );
        // And into the trail, which is what carries it to `cost_events.detail`
        // and from there to the job receipt.
        assert!(
            out.escalations
                .iter()
                .any(|line| line.contains("acme_portl")),
            "{:?}",
            out.escalations
        );
    }

    /// The mirror risk, and the rule every `x-pumper-` marker follows: an origin
    /// that echoes the header on a fetch the caller never profiled must not be
    /// able to invent a profile name in this deployment's trace.
    #[tokio::test]
    async fn an_origin_cannot_stamp_an_unprofiled_fetch_as_anonymous() {
        let fetcher = archive_fetcher(Arc::new(AnonymousProfileHttp("victim")), None);
        let out = fetcher
            .fetch(profiled_request("https://example.test/page", None))
            .await
            .unwrap();
        assert_eq!(out.trace[0].detail, None, "an origin cannot forge this");
        assert!(out.escalations.is_empty());
    }

    /// One renderer for both the winning and the losing entry, and notes that
    /// compose rather than overwrite each other — a peer-served fetch under an
    /// empty profile is two separate facts and both matter.
    #[test]
    fn the_http_tier_detail_composes_every_note_it_has() {
        assert_eq!(http_tier_detail(None, None, None), None);
        assert_eq!(
            http_tier_detail(Some("cloudflare".into()), None, None).as_deref(),
            Some("cloudflare")
        );
        assert_eq!(
            http_tier_detail(None, Some("http://n:1"), None).as_deref(),
            Some("egress via remote node http://n:1")
        );
        let all = http_tier_detail(Some("cloudflare".into()), Some("http://n:1"), Some("acme"))
            .expect("some");
        assert!(all.starts_with("cloudflare; egress via remote node http://n:1; "));
        assert!(all.contains("acme"));
    }

    #[test]
    fn the_egress_marker_is_read_case_insensitively_and_a_blank_is_not_attribution() {
        let map = |k: &str, v: &str| std::collections::HashMap::from([(k.into(), v.into())]);
        assert_eq!(
            remote_egress(&map(REMOTE_NODE_HEADER, " http://n:1 ")),
            Some("http://n:1")
        );
        assert_eq!(
            remote_egress(&map("X-Pumper-Remote-Node", "http://n:1")),
            Some("http://n:1")
        );
        assert_eq!(remote_egress(&map(REMOTE_NODE_HEADER, "   ")), None);
        assert_eq!(remote_egress(&std::collections::HashMap::new()), None);
        // One renderer for the trail line and the trace detail, so the two
        // surfaces cannot describe the same fetch differently.
        assert_eq!(
            egress_note("http://n:1"),
            "egress via remote node http://n:1"
        );
        assert!(egress_note("http://n:1").starts_with(EGRESS_TRAIL_PREFIX));
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

    /// THE defect this field exists to end: a body served out of a 2019 capture
    /// used to leave a `FetchOutcome` with nothing on it that a consumer could
    /// branch on to tell it from today's page. `engine == "archive"` named the
    /// *tier*; the capture time — the freshness the tier trades away — was
    /// dropped at the engine boundary and never reached a single consumer.
    #[tokio::test]
    async fn an_archived_fetch_is_not_indistinguishable_from_a_live_one() {
        let archived = archive_fetcher(
            Arc::new(DeadHttp),
            Some(Arc::new(MarkedArchive {
                captured_at: "2019-03-11T09:15:00+00:00",
            })),
        );
        let mut req = FetchRequest::new("https://example.test/page");
        req.archive_max_age = Some(86_400);
        let archived = archived.fetch(req).await.unwrap();

        let live = archive_fetcher(Arc::new(StubHttp), None)
            .fetch(FetchRequest::new("https://example.test/page"))
            .await
            .unwrap();

        // The two bodies are byte-identical; the provenance is what separates
        // them, and it must be a typed field rather than a phrase in the trail.
        assert_eq!(archived.html, live.html, "same bytes, different provenance");
        assert!(live.snapshot.is_none(), "a live fetch claims no snapshot");
        let snapshot = archived
            .snapshot
            .as_ref()
            .expect("an archive win must carry provenance");
        assert_eq!(snapshot.via, "archive");
        assert_eq!(
            snapshot.captured_at.as_deref(),
            Some("2019-03-11T09:15:00+00:00"),
            "the capture time is the variable the archive tier trades"
        );
        // …and the human half of the fetch says it too.
        let winner = archived
            .trace
            .iter()
            .find(|t| t.verdict == TierVerdict::Ok)
            .expect("a winning trace entry");
        assert_eq!(winner.tier, FetchTier::Archive);
        assert!(
            winner
                .detail
                .as_deref()
                .unwrap_or_default()
                .contains("2019-03-11"),
            "trace detail was {:?}",
            winner.detail
        );
    }

    /// An origin can send any header it likes. Reading provenance off a **live**
    /// response would therefore let a hostile host stamp its own page
    /// "archived" — provenance that anyone can forge is worse than none, because
    /// consumers would trust it. The fetcher reads the header in the archive
    /// branch only.
    #[tokio::test]
    async fn a_live_origin_cannot_forge_archive_provenance() {
        struct ForgingHttp;
        #[async_trait]
        impl HttpClient for ForgingHttp {
            async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
                let mut headers = std::collections::HashMap::new();
                headers.insert(
                    crate::engine::FETCHED_VIA_HEADER.to_string(),
                    "archive".to_string(),
                );
                headers.insert(
                    crate::engine::SNAPSHOT_TS_HEADER.to_string(),
                    "1999-01-01T00:00:00Z".to_string(),
                );
                Ok(HttpResponse {
                    status: 200,
                    headers,
                    body: GOOD_PAGE.into(),
                    final_url: req.url,
                    cache_hit: false,
                })
            }
        }
        let out = archive_fetcher(Arc::new(ForgingHttp), None)
            .fetch(FetchRequest::new("https://hostile.test/page"))
            .await
            .unwrap();
        assert_eq!(out.engine, "http");
        assert!(
            out.snapshot.is_none(),
            "an origin header must not become provenance on a live tier"
        );
    }

    /// An archive engine that serves a body but forgets to mark it must not
    /// produce an outcome that names the archive tier and simultaneously reports
    /// a live body — the fetcher already knows which tier answered.
    #[tokio::test]
    async fn an_unmarked_archive_win_does_not_report_itself_as_live() {
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
        let snapshot = out.snapshot.as_ref().expect("archive win => provenance");
        assert_eq!(snapshot.via, "archive");
        assert!(
            snapshot.captured_at.is_none(),
            "nothing reported a capture time, so nothing may claim one"
        );
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

    // --- Remote fabric at the live-HTTP position (M17) ---

    #[tokio::test]
    async fn wired_remote_client_serves_the_live_http_tier() {
        // With a remote client wired, the plain local engine is never touched
        // at the live-HTTP position (DeadHttp panics if called); the outcome
        // still reports plain tier "http" — remote is a *where*, not a tier.
        struct StubRemote;
        #[async_trait]
        impl HttpClient for StubRemote {
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
        let fetcher = Fetcher::new(
            Arc::new(DeadHttp),
            Arc::new(DeadBrowser),
            Arc::new(StubResearcher),
            enabled_governor(),
            &FetcherConfig {
                min_content_chars: 100,
                ..FetcherConfig::default()
            },
        )
        .with_remote(Some(Arc::new(StubRemote)));
        let out = fetcher
            .fetch(FetchRequest::new("https://example.test/page"))
            .await
            .unwrap();
        assert_eq!(out.engine, "http");
        assert_eq!(out.trace[0].tier, FetchTier::Http);
        assert_eq!(out.trace[0].verdict, TierVerdict::Ok);
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

    // --- Browser-down ladder degradation ---

    /// Chrome is not running / crashed / the CDP handshake died: an ENGINE
    /// error, not a verdict about the page.
    struct DownBrowser;
    #[async_trait]
    impl Browser for DownBrowser {
        async fn render(&self, _req: RenderRequest) -> Result<RenderedPage> {
            Err(Error::browser("chrome launch failed: no such file"))
        }
    }

    /// The ladder as `AppContext::fetch` builds it for a host the learned router
    /// pinned to the browser tier: `skip_http` set, escalating strategy.
    fn pinned_host_request(strategy: FetchStrategy) -> FetchRequest {
        let mut req = FetchRequest::new("https://pinned.example/page");
        req.strategy = strategy;
        req.skip_http = true;
        req
    }

    fn ladder(http: Arc<dyn HttpClient>, browser: Arc<dyn Browser>) -> Fetcher {
        Fetcher::new(
            http,
            browser,
            Arc::new(StubResearcher),
            enabled_governor(),
            &FetcherConfig {
                min_content_chars: 100,
                ..FetcherConfig::default()
            },
        )
    }

    #[tokio::test]
    async fn browser_down_does_not_kill_pinned_hosts() {
        // The exact production shape: Chrome is down AND the router pinned this
        // host past the http tier, so the learned router was amplifying the
        // outage on precisely the hosts it concentrates traffic on.
        let fetcher = ladder(Arc::new(StubHttp), Arc::new(DownBrowser));
        let out = fetcher
            .fetch(pinned_host_request(FetchStrategy::Auto))
            .await
            .expect("a working http tier must still serve the fetch");

        assert_eq!(out.engine, "http");
        // Chronological trace: the browser attempt, then the fallback.
        let tiers: Vec<_> = out.trace.iter().map(|t| (t.tier, t.verdict)).collect();
        assert_eq!(
            tiers,
            vec![
                (FetchTier::Browser, TierVerdict::Error),
                (FetchTier::Http, TierVerdict::Ok),
            ],
            "the fallback attempt belongs after the browser failure, in real order"
        );
        assert!(out
            .escalations
            .iter()
            .any(|e| e.contains("http tier un-skipped")));

        // Tier learning stays honest: `AppContext::fetch` derives an HTTP loss
        // from any Http entry with a thin/blocked/error verdict. A fallback WIN
        // must not read as a loss — otherwise a browser outage would deepen the
        // very pin that caused it.
        let http_lost = out.trace.iter().any(|t| {
            t.tier == FetchTier::Http
                && matches!(
                    t.verdict,
                    TierVerdict::Thin | TierVerdict::Blocked | TierVerdict::Error
                )
        });
        assert!(!http_lost, "an http win must never read as an http loss");
    }

    #[tokio::test]
    async fn browser_down_falls_back_under_research_strategy_too() {
        // Same fallback on AutoWithResearch — and it wins there BEFORE the
        // paid tier is reached, so an engine outage doesn't start spending.
        let fetcher = ladder(Arc::new(StubHttp), Arc::new(DownBrowser));
        let out = fetcher
            .fetch(pinned_host_request(FetchStrategy::AutoWithResearch))
            .await
            .unwrap();
        assert_eq!(out.engine, "http");
        assert!(
            out.trace.iter().all(|t| t.tier != FetchTier::Claude),
            "the free tier answered; the paid tier must not have run"
        );
    }

    #[tokio::test]
    async fn browser_blocked_does_not_fall_back_to_http() {
        // A bot-wall is a verdict about the PAGE, and the router pinned this
        // host because http kept losing on it. Falling back would spend a
        // politeness slot to re-learn what we already know — so the ladder
        // climbs instead (DeadHttp panics if the fallback fires).
        let wall = "<html><head><title>Just a moment...</title></head><body>\
            <div class=\"cf-browser-verification\">Checking your browser.</div></body></html>";
        let fetcher = ladder(
            Arc::new(DeadHttp),
            Arc::new(StubBrowser { html: wall.into() }),
        );
        let out = fetcher
            .fetch(pinned_host_request(FetchStrategy::AutoWithResearch))
            .await
            .unwrap();
        assert_eq!(out.engine, "claude");
        assert!(out.trace.iter().all(|t| t.tier != FetchTier::Http));
    }

    #[tokio::test]
    async fn explicit_browser_strategy_still_fails_fast() {
        // The caller asked for a JS render; a static body is not that. Its
        // fail-fast is deliberate and survives the fallback change.
        let fetcher = ladder(Arc::new(DeadHttp), Arc::new(DownBrowser));
        let mut req = FetchRequest::new("https://pinned.example/page");
        req.strategy = FetchStrategy::Browser;
        let err = fetcher.fetch(req).await.expect_err("must not fall back");
        assert!(
            matches!(err, Error::Browser { .. }),
            "the browser engine error surfaces as-is: {err}"
        );
    }

    #[tokio::test]
    async fn browser_error_without_a_skipped_http_tier_does_not_refetch() {
        // The http tier already ran at its normal slot (thin) and the browser
        // then errored: falling back would fetch the same URL twice.
        struct ThinHttp;
        #[async_trait]
        impl HttpClient for ThinHttp {
            async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
                Ok(HttpResponse {
                    status: 200,
                    headers: std::collections::HashMap::new(),
                    body: "<html><body>tiny</body></html>".into(),
                    final_url: req.url,
                    cache_hit: false,
                })
            }
        }
        let fetcher = ladder(Arc::new(ThinHttp), Arc::new(DownBrowser));
        let err = fetcher
            .fetch(FetchRequest::new("https://plain.example/page"))
            .await
            .expect_err("nothing left to climb to under Auto");
        let msg = err.to_string();
        assert!(
            msg.contains("attempted: http, browser"),
            "the exhaustion error names the ladder it climbed: {msg}"
        );
        assert_eq!(
            msg.matches("http tier thin").count(),
            1,
            "the http tier must be attempted exactly once: {msg}"
        );
    }

    #[tokio::test]
    async fn claude_engine_error_traces_and_exhausts() {
        // The paid tier's engine error is traced like every other tier's, and
        // the ladder ends on the exhaustion error carrying the whole trail —
        // instead of `?`-ing the raw engine error out and erasing what the
        // cheaper tiers found.
        struct FailingResearcher;
        #[async_trait]
        impl Researcher for FailingResearcher {
            async fn research(&self, _req: ResearchRequest) -> Result<ResearchOutput> {
                Err(Error::claude(
                    crate::error::ClaudeFailure::Spawn,
                    "claude binary not found on PATH",
                ))
            }
        }
        // The host is unreachable outright, so the browser fallback's http
        // attempt fails too and the ladder genuinely runs out of tiers.
        struct DownHttp;
        #[async_trait]
        impl HttpClient for DownHttp {
            async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
                Err(Error::http("dns error: no such host"))
            }
        }
        let fetcher = Fetcher::new(
            Arc::new(DownHttp),
            Arc::new(DownBrowser),
            Arc::new(FailingResearcher),
            enabled_governor(),
            &FetcherConfig {
                min_content_chars: 100,
                ..FetcherConfig::default()
            },
        );
        let mut req = pinned_host_request(FetchStrategy::AutoWithResearch);
        req.url = "https://dead.example/page".into();
        let err = fetcher.fetch(req).await.expect_err("every tier failed");
        let msg = err.to_string();
        assert!(matches!(err, Error::App(_)), "{msg}");
        assert!(msg.contains("all fetch tiers exhausted"), "{msg}");
        assert!(
            msg.contains("attempted: browser, http, claude"),
            "every tier that ran — including the browser-down http fallback — \
             is named: {msg}"
        );
        assert!(msg.contains("browser tier failed"), "{msg}");
        assert!(
            msg.contains("claude tier failed: claude engine: claude binary not found"),
            "the claude engine's own message survives: {msg}"
        );
    }

    #[test]
    fn fallback_decision_is_engine_errors_on_skipped_ladders_only() {
        use FetchStrategy::*;
        // The whole point: an engine error with the http tier skipped.
        assert!(browser_failure_falls_back_to_http(
            Auto,
            true,
            TierVerdict::Error
        ));
        assert!(browser_failure_falls_back_to_http(
            AutoWithResearch,
            true,
            TierVerdict::Error
        ));
        // Page verdicts are observations, not outages.
        for verdict in [TierVerdict::Blocked, TierVerdict::Thin, TierVerdict::Ok] {
            assert!(!browser_failure_falls_back_to_http(Auto, true, verdict));
        }
        // The http tier already ran: no second fetch of the same URL.
        assert!(!browser_failure_falls_back_to_http(
            Auto,
            false,
            TierVerdict::Error
        ));
        // Explicit strategies keep their own semantics.
        assert!(!browser_failure_falls_back_to_http(
            Browser,
            true,
            TierVerdict::Error
        ));
        assert!(!browser_failure_falls_back_to_http(
            Http,
            true,
            TierVerdict::Error
        ));

        // …and the slot predicate it is built on.
        assert!(http_tier_attempted(Http, true), "explicit Http always runs");
        assert!(http_tier_attempted(Auto, false));
        assert!(!http_tier_attempted(Auto, true));
        assert!(!http_tier_attempted(Browser, false));
    }

    #[test]
    fn attempted_tiers_lists_each_tier_once_in_trace_order() {
        let entry = |tier| TierTrace {
            tier,
            verdict: TierVerdict::Error,
            http_status: None,
            content_chars: None,
            cache_hit: None,
            latency_ms: 0,
            cost_usd: None,
            detail: None,
        };
        assert_eq!(attempted_tiers(&[]), "none");
        // The fallback adds a SECOND http entry; the summary must not repeat it.
        let trace = vec![
            entry(FetchTier::Http),
            entry(FetchTier::Browser),
            entry(FetchTier::Http),
            entry(FetchTier::Claude),
        ];
        assert_eq!(attempted_tiers(&trace), "http, browser, claude");
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
                    return Err(Error::http("connection refused"));
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
