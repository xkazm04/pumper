//! High-concurrency broad crawler. A bounded, deduplicated URL frontier feeds a
//! pool of concurrent fetch tasks (tokio holds thousands cheaply, at ~KB per
//! task); page bodies are written to disk as they arrive rather than
//! accumulated. Politeness comes from the shared per-domain governor (inside the
//! http engine) plus robots.txt; near-duplicate pages are dropped via SimHash.
//!
//! This is the shape asyncio struggles with: high connection concurrency with
//! GIL-free body parsing under backpressure.
//!
//! Memory: page bodies stream to disk (never held), per-page fingerprints stream
//! to the dataset via a [`PageSink`] (never accumulated in the result), and
//! near-dup detection uses a banded SimHash index (candidate lookup, not an
//! O(n) scan per page). What DOES grow with the crawl are the frontier seen-set
//! (capped at `MAX_FRONTIER`) and the kept-page SimHash fingerprints (8 bytes
//! each) — both bounded, neither the page bodies.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::engine::{HttpClient, HttpRequest};
use crate::simhash::{hamming, simhash};
use crate::Result;

const MAX_FRONTIER: usize = 100_000;

/// Characters of extracted page text kept as the record excerpt.
const EXCERPT_CHARS: usize = 300;

/// Kept pages are flushed to the [`PageSink`] in batches of this size during the
/// crawl (not one giant batch at the end) so dataset writes stay incremental and
/// per-page metadata never accumulates in memory.
const PAGE_SINK_STRIDE: usize = 50;

/// Compact, queryable fingerprint of one KEPT page — the unit the crawl streams
/// to a [`PageSink`] (e.g. the app's dataset writer). Bodies are artifacts
/// (`artifact_path`), never stored here; this carries only what supports
/// query/diff/trigger. Keyed downstream by `url` (canonical).
#[derive(Debug, Clone, Serialize)]
pub struct CrawlPageRecord {
    /// Canonical URL — the stable external id / dataset key.
    pub url: String,
    /// `<title>` text, when present.
    pub title: Option<String>,
    pub status: u16,
    /// Visible-text character count (script/style excluded).
    pub content_chars: usize,
    /// SimHash of the body (same fingerprint used for near-dup detection).
    pub simhash: u64,
    /// First ~300 chars of extracted text.
    pub excerpt: String,
    /// Basename of the page body written under the job's artifacts dir
    /// (`page-NNNN.html`), or empty when bodies aren't being written.
    pub artifact_path: String,
    pub depth: u32,
    /// Response `ETag`, when the origin sent one — stored so a later revisit can
    /// send `If-None-Match` and get a cheap `304`.
    pub etag: Option<String>,
    /// Response `Last-Modified`, when present — the `If-Modified-Since` validator
    /// for a later revisit.
    pub last_modified: Option<String>,
    /// Set on a revisit when the page returned `404`/`410` — a removal signal.
    /// Normal kept pages carry `false`. Gone markers carry only `url`, `status`
    /// and this flag; the rest is empty.
    pub gone: bool,
    /// Set on a revisit when the conditional GET answered `304 Not Modified`:
    /// a check-only marker carrying just `url` + the bumped `cadence` (no body
    /// was downloaded). The sink merges the cadence into the stored record so
    /// the estimator improves every run — without a new table.
    pub unchanged: bool,
    /// Learned change-cadence counters for this URL, updated every revisit (and
    /// initialized on first sighting). `None` outside revisit bookkeeping.
    pub cadence: Option<RevisitCadence>,
    /// Outbound links extracted from this page — canonicalized and already
    /// scheme/`same_domain`/robots-independent filtered exactly as the frontier
    /// saw them (a `same_domain` crawl therefore carries a truncated,
    /// same-host-only view). Surfaced so a sink can persist the link graph the
    /// crawler otherwise computes and discards. Empty on `gone`/`unchanged`
    /// markers (no body was parsed).
    pub links: Vec<String>,
}

/// Per-URL change-cadence counters, persisted ON the page record (M07 — no new
/// table). Every revisit is a labeled observation: a `304` bumps `checks`; a
/// changed body bumps `changes` by a SimHash-distance-graded weight (boilerplate
/// churn — rotating ads, timestamps — must not make a page look hot) and feeds
/// the EWMA inter-change interval. `due_score` turns these into "probability
/// this page changed since we last looked".
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RevisitCadence {
    /// Revisit checks observed (304s and changed bodies alike).
    #[serde(default)]
    pub checks: u64,
    /// Graded change mass: each changed body adds its `change_weight` (0..=1),
    /// so ad-rotation noise accumulates slowly while real edits count fully.
    #[serde(default)]
    pub changes: f64,
    /// Epoch seconds of the last check (any outcome). The due clock.
    #[serde(default)]
    pub last_checked_at: Option<i64>,
    /// Epoch seconds of the last WEIGHTED change (first sighting counts as the
    /// baseline). Anchors the inter-change gap measurement.
    #[serde(default)]
    pub last_change_at: Option<i64>,
    /// EWMA of observed inter-change gaps (seconds). `None` until the second
    /// weighted change — the host-level prior fills in for scoring.
    #[serde(default)]
    pub interval_secs: Option<f64>,
}

impl RevisitCadence {
    /// First sighting of a page: one check, change baseline anchored at `now`.
    fn first_seen(now: i64) -> Self {
        Self {
            checks: 1,
            changes: 0.0,
            last_checked_at: Some(now),
            last_change_at: Some(now),
            interval_secs: None,
        }
    }

    /// A revisit answered `304` (or an unweighted near-identical body): count
    /// the check, move the due clock, leave the change model untouched.
    fn observe_unchanged(&self, now: i64) -> Self {
        let mut next = self.clone();
        next.checks += 1;
        next.last_checked_at = Some(now);
        next
    }

    /// A revisit found a changed body with `weight` (0..=1, SimHash-graded).
    /// Zero weight degrades to an unchanged observation.
    fn observe_changed(&self, now: i64, weight: f64) -> Self {
        if weight <= 0.0 {
            return self.observe_unchanged(now);
        }
        let mut next = self.observe_unchanged(now);
        next.changes += weight;
        if let Some(prev) = self.last_change_at {
            let gap = (now - prev).max(1) as f64;
            next.interval_secs = Some(match self.interval_secs {
                None => gap,
                Some(p) => CADENCE_EWMA_ALPHA * gap + (1.0 - CADENCE_EWMA_ALPHA) * p,
            });
        }
        next.last_change_at = Some(now);
        next
    }
}

/// EWMA smoothing for observed inter-change gaps (mirrors the cache mirror's
/// estimator): newest gap weighs 0.3 so a cadence shift tracks within a few
/// observations without one outlier whipsawing the estimate.
const CADENCE_EWMA_ALPHA: f64 = 0.3;

/// Cold-start prior for a URL (and host) with no learned interval: one week.
/// Deliberately long — an unknown page starts "cool" and earns a hotter cadence
/// only by actually changing; the never-checked case bypasses the prior with a
/// due score of 1.0 (a baseline must be established).
const DEFAULT_CADENCE_PRIOR_SECS: f64 = 7.0 * 86_400.0;

/// SimHash Hamming distances at or below this are boilerplate churn (rotating
/// ads, counters, timestamps) — same scale as the crawler's default
/// `dedup_distance = 3` for "near-identical page".
const BOILERPLATE_DISTANCE: u32 = 3;

/// Grades how much a page really changed from the SimHash Hamming distance
/// between the old and new body fingerprints: `0.0` at or below
/// [`BOILERPLATE_DISTANCE`] (cosmetic churn must not look hot), ramping
/// linearly to `1.0` at distance 16 (a solidly different document). An unknown
/// old fingerprint (0) grades as a full change — no evidence to discount it.
pub fn change_weight(old_simhash: u64, new_simhash: u64) -> f64 {
    if old_simhash == 0 {
        return 1.0;
    }
    let d = hamming(old_simhash, new_simhash);
    if d <= BOILERPLATE_DISTANCE {
        return 0.0;
    }
    (f64::from(d - BOILERPLATE_DISTANCE) / f64::from(16 - BOILERPLATE_DISTANCE)).min(1.0)
}

/// Probability this URL has changed since its last check, under a Poisson
/// change process with the learned (or prior) inter-change interval:
/// `1 - exp(-elapsed / T̂)`. A never-checked URL scores `1.0` — the estimator
/// has no baseline and must establish one. Monotonic in elapsed time, so a
/// stale page always eventually becomes due no matter how long its interval.
pub fn due_score(cadence: &RevisitCadence, now_epoch: i64, prior_interval_secs: f64) -> f64 {
    let Some(last_checked) = cadence.last_checked_at else {
        return 1.0;
    };
    let interval = cadence
        .interval_secs
        .filter(|t| t.is_finite() && *t > 0.0)
        .unwrap_or(
            if prior_interval_secs.is_finite() && prior_interval_secs > 0.0 {
                prior_interval_secs
            } else {
                DEFAULT_CADENCE_PRIOR_SECS
            },
        );
    let elapsed = (now_epoch - last_checked).max(0) as f64;
    1.0 - (-elapsed / interval).exp()
}

/// Host-level cadence priors for cold-start seeds: the mean learned interval of
/// this host's URLs that HAVE one, so a new URL inherits its host's rhythm
/// instead of the global one-week default.
fn host_cadence_priors(seeds: &[RevisitSeed]) -> HashMap<String, f64> {
    let mut sums: HashMap<String, (f64, usize)> = HashMap::new();
    for seed in seeds {
        if let Some(interval) = seed
            .cadence
            .interval_secs
            .filter(|t| t.is_finite() && *t > 0.0)
        {
            if let Some(host) = host_of(&seed.url) {
                let entry = sums.entry(host).or_insert((0.0, 0));
                entry.0 += interval;
                entry.1 += 1;
            }
        }
    }
    sums.into_iter()
        .map(|(host, (sum, n))| (host, sum / n as f64))
        .collect()
}

/// One existing page handed back by a [`PageSource`] to seed a revisit: the
/// canonical URL plus whatever conditional-GET validators were stored last time.
#[derive(Debug, Clone)]
pub struct RevisitSeed {
    pub url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// Stored body SimHash from the last kept fetch (0 = unknown) — grades how
    /// much a changed revisit really changed.
    pub simhash: u64,
    /// Learned change-cadence counters read back off the stored page record
    /// (default for records that predate the counters).
    pub cadence: RevisitCadence,
}

impl RevisitSeed {
    /// A seed carrying only validators (pre-cadence records, tests).
    pub fn bare(
        url: impl Into<String>,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Self {
        Self {
            url: url.into(),
            etag,
            last_modified,
            simhash: 0,
            cadence: RevisitCadence::default(),
        }
    }
}

/// Reads existing page records to seed an incremental recrawl. The app layer
/// implements this over `ctx.datasets` (the `pages` dataset written by
/// [`PageSink`]); core stays storage-agnostic — the read-side mirror of the
/// write-side `PageSink`. Implementations must not fail the crawl — return an
/// empty vec and log on error.
#[async_trait]
pub trait PageSource: Send {
    /// Existing pages to revisit (canonical URL + stored validators).
    async fn seeds(&self) -> Vec<RevisitSeed>;
}

/// A streaming consumer of KEPT-page fingerprints, called in batches during the
/// crawl. The app layer implements this over `ctx.datasets` (upsert to the
/// `pages` dataset); core stays storage-agnostic. Implementations must not fail
/// the crawl — swallow and log their own errors.
#[async_trait]
pub trait PageSink: Send {
    async fn emit(&mut self, batch: Vec<CrawlPageRecord>);
}

#[derive(Debug, Clone)]
pub struct CrawlConfig {
    pub seeds: Vec<String>,
    pub max_pages: usize,
    pub max_depth: u32,
    pub concurrency: usize,
    /// Max pages fetched per host before the frontier stops handing out that
    /// host's URLs (`None` = no per-host cap). With the round-robin frontier this
    /// keeps one large seed from consuming the whole `max_pages` budget and
    /// starving other seeds — multi-seed / off-domain crawls stay broad.
    pub max_pages_per_host: Option<usize>,
    /// Restrict to the seed hosts.
    pub same_domain: bool,
    /// Drop pages within this SimHash distance of one already kept (0 disables).
    pub dedup_distance: u32,
    pub respect_robots: bool,
    /// Regexes a discovered URL must match (any of) to be enqueued. Empty =
    /// everything allowed. Seeds are exempt — the user asked for them.
    pub include_patterns: Vec<String>,
    /// Regexes that drop a discovered URL (any match). Applied after include.
    pub exclude_patterns: Vec<String>,
    /// Expand seeds from each seed host's sitemaps (robots.txt `Sitemap:`
    /// directives, falling back to /sitemap.xml).
    pub sitemap_seeds: bool,
    /// Frontier state persisted by a prior run (the JSON a checkpoint sink was
    /// handed), restored at start — so an interrupted or page-capped crawl
    /// resumes where it left off instead of refetching everything. Advisory: an
    /// incompatible/unparseable value is discarded for a clean fresh start
    /// (surfaced as `checkpoint_reset`), never resumed silently-wrong.
    pub resume_state: Option<serde_json::Value>,
    /// Incremental recrawl / site-change sentinel mode. When true the frontier is
    /// seeded from existing `pages` records (via the [`PageSource`] seam) and each
    /// known page is fetched with a conditional GET using its stored
    /// `etag`/`last_modified`: a `304` is counted `unchanged_304` (cheap, not
    /// re-fingerprinted), a changed body is re-fingerprinted + upserted, and a
    /// `404`/`410` flags the page `gone`. Does NOT follow links unless `discover`.
    pub revisit: bool,
    /// In revisit mode, opt in to link-following (expand the frontier with newly
    /// discovered URLs). Ignored outside revisit mode (normal crawls always
    /// follow links within the depth budget).
    pub discover: bool,
    /// Revisit mode: max known pages fetched this run, spent on the URLs with
    /// the highest [`due_score`] — the learned-cadence frontier. Seeds beyond
    /// the budget are counted `skipped_not_due`. `None` = revisit every seed
    /// (the flat pre-M07 schedule).
    pub revisit_budget: Option<usize>,
    /// Revisit mode: seeds scoring below this due probability (0..=1) are
    /// skipped this run and counted `skipped_not_due`. `0.0` disables the
    /// filter (every seed is at least eligible; the budget still ranks).
    pub min_due_score: f64,
}

/// Compiled include/exclude filter.
struct UrlFilter {
    include: Vec<regex::Regex>,
    exclude: Vec<regex::Regex>,
}

impl UrlFilter {
    fn compile(cfg: &CrawlConfig) -> Result<Self> {
        let compile = |patterns: &[String]| -> Result<Vec<regex::Regex>> {
            patterns
                .iter()
                .map(|p| {
                    regex::Regex::new(p)
                        .map_err(|e| crate::Error::Parse(format!("bad url pattern '{p}': {e}")))
                })
                .collect()
        };
        Ok(Self {
            include: compile(&cfg.include_patterns)?,
            exclude: compile(&cfg.exclude_patterns)?,
        })
    }

    fn allows(&self, url: &str) -> bool {
        if !self.include.is_empty() && !self.include.iter().any(|re| re.is_match(url)) {
            return false;
        }
        !self.exclude.iter().any(|re| re.is_match(url))
    }
}

/// Compact live-progress snapshot emitted periodically DURING a crawl (not just
/// at the end) via the [`ProgressFn`] seam, so a 100k-page crawl is observable
/// mid-run instead of a black box until completion.
#[derive(Debug, Clone, Serialize)]
pub struct CrawlProgressSnapshot {
    pub crawled: usize,
    pub kept: usize,
    pub failed: usize,
    /// URLs still queued in the frontier.
    pub frontier: usize,
    /// Distinct hosts touched so far.
    pub hosts: usize,
}

/// Periodic progress callback. Invoked every [`PROGRESS_STRIDE`] crawled pages
/// (and once at the end) with a live snapshot. The app layer bridges it to the
/// runtime's `ProgressReporter` (persist latest + emit a `progress` event);
/// core stays runtime-agnostic. Cheap and non-blocking — the runtime throttles.
pub type ProgressFn = Arc<dyn Fn(&CrawlProgressSnapshot) + Send + Sync>;

/// How often (in crawled pages) the progress seam is invoked. The runtime
/// throttles the actual persist/emit, so a tight stride here is cheap.
const PROGRESS_STRIDE: usize = 20;

/// A minimal removal marker for a page a revisit found `404`/`410`. Carries only
/// `url`, `status` and `gone: true`; the app upserts it so the record flips to a
/// `gone` state (a changed revision that triggers/watches fire on).
fn gone_record(url: String, status: u16) -> CrawlPageRecord {
    CrawlPageRecord {
        url,
        title: None,
        status,
        content_chars: 0,
        simhash: 0,
        excerpt: String::new(),
        artifact_path: String::new(),
        depth: 0,
        etag: None,
        last_modified: None,
        gone: true,
        unchanged: false,
        cadence: None,
        links: Vec::new(),
    }
}

/// A check-only marker for a revisit answered `304 Not Modified`: carries the
/// URL and the bumped cadence counters, nothing else (no body was downloaded).
/// The sink merges the cadence into the stored record.
fn unchanged_record(url: String, cadence: RevisitCadence) -> CrawlPageRecord {
    CrawlPageRecord {
        url,
        title: None,
        status: 304,
        content_chars: 0,
        simhash: 0,
        excerpt: String::new(),
        artifact_path: String::new(),
        depth: 0,
        etag: None,
        last_modified: None,
        gone: false,
        unchanged: true,
        cadence: Some(cadence),
        links: Vec::new(),
    }
}

/// Epoch seconds now — the cadence clock (core stays chrono-free).
fn epoch_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().min(i64::MAX as u64) as i64)
        .unwrap_or(0)
}

fn emit_progress(progress: &Option<ProgressFn>, stats: &CrawlStats, frontier: usize, hosts: usize) {
    if let Some(cb) = progress {
        cb(&CrawlProgressSnapshot {
            crawled: stats.crawled,
            kept: stats.kept,
            failed: stats.failed,
            frontier,
            hosts,
        });
    }
}

#[derive(Debug, Default, Serialize)]
pub struct CrawlStats {
    pub crawled: usize,
    pub kept: usize,
    pub skipped_duplicates: usize,
    pub skipped_robots: usize,
    /// Discovered links dropped by include/exclude URL patterns.
    pub skipped_filtered: usize,
    /// Discovered URLs refused because the frontier hit its `MAX_FRONTIER` cap —
    /// coverage was truncated (0 = the whole discovered graph fit).
    pub frontier_dropped: usize,
    /// Queued URLs skipped because their host had already reached
    /// `max_pages_per_host` — host-fairness truncation, reported honestly rather
    /// than letting one big site silently consume the whole `max_pages` budget.
    pub skipped_host_budget: usize,
    /// URLs seeded into the frontier from sitemaps.
    pub sitemap_seeded: usize,
    /// Fetches that failed at the transport layer (DNS/TLS/connection/timeout) —
    /// previously swallowed silently.
    pub failed: usize,
    /// Failure counts by host, capped to the top 20 offenders at the end.
    pub failed_by_host: HashMap<String, usize>,
    /// Responses classified as a bot-wall / challenge (status 403/429/503 or a
    /// challenge marker) and therefore NOT kept — see `fetcher::http_bot_wall`.
    pub skipped_botwall: usize,
    /// robots.txt fetches that failed at the transport layer (fail-open to
    /// allow-all, but surfaced rather than hidden). Equals the number of origins
    /// crawled without verified rules — see `robots_unverified_hosts`.
    pub robots_fetch_failures: usize,
    /// The origins (`scheme://host[:port]`) whose robots.txt could not be fetched
    /// at all, and which were therefore crawled under a fail-open **assumption**
    /// rather than under verified rules. Empty = every host's rules were read.
    ///
    /// Failing open on a transport failure is the right default and is not up for
    /// change — but a run that fails open and then reports `respect_robots: true`
    /// with `skipped_robots: 0` *looks* compliant when it is not, and politeness
    /// is the one bug class in a scraper whose cost lands on someone else's
    /// server. A bare count reads as noise; naming the origins is the point.
    ///
    /// A non-2xx robots response (e.g. `404` "no robots") is a legitimate
    /// allow-all and deliberately does NOT appear here. Sorted and capped at
    /// [`MAX_UNVERIFIED_HOSTS`]; `robots_fetch_failures` carries the full count.
    pub robots_unverified_hosts: Vec<String>,
    /// Checkpoint saves that failed to persist (write/rename error).
    pub checkpoint_errors: usize,
    /// True when this run restored frontier state from a checkpoint.
    pub resumed: bool,
    /// True when a checkpoint existed but was an incompatible (older) format and
    /// was discarded for a clean fresh start rather than a silently-wrong resume.
    pub checkpoint_reset: bool,
    pub hosts: usize,
    pub frontier_remaining: usize,
    /// Revisit mode: known pages fetched with a conditional GET (200 + 304 + gone).
    pub revisited: usize,
    /// Revisit mode: conditional GETs answered `304 Not Modified` (unchanged,
    /// not re-fingerprinted).
    pub unchanged_304: usize,
    /// Revisit mode: known pages that returned `404`/`410` and were flagged gone.
    pub gone: usize,
    /// Revisit mode: known pages NOT fetched this run because their due score
    /// fell below `min_due_score` or they ranked past `revisit_budget` — honest
    /// coverage accounting for the learned-cadence frontier.
    pub skipped_not_due: usize,
}

/// Bounded, deduplicated, **host-fair** URL frontier.
///
/// URLs are bucketed per host and handed out round-robin, so one large seed
/// can't monopolize the `max_pages` budget and starve other seeds (a plain FIFO
/// would). An optional `max_pages_per_host` caps how many a single host yields.
/// A polite (crawl-delayed) host rotating to the back no longer sits behind a
/// fast host's entire backlog. The `seen` set (global dedup + `MAX_FRONTIER`
/// cap) and `dropped` counter keep their prior semantics.
struct Frontier {
    /// Per-host FIFO of `(url, depth)`.
    per_host: HashMap<String, VecDeque<(String, u32)>>,
    /// Round-robin cursor: hosts with a non-empty queue, rotated on each pop.
    order: VecDeque<String>,
    seen: HashSet<String>,
    /// New URLs refused because the seen-set hit `MAX_FRONTIER` (coverage was
    /// truncated). Tracked so a capped crawl is reported honestly rather than
    /// silently dropping discovered URLs.
    dropped: usize,
    /// Total queued URLs across all host buckets.
    len: usize,
    /// Pages handed out per host (budget accounting; a requeue is refunded).
    taken: HashMap<String, usize>,
    /// Per-host page cap; `None` = unlimited.
    max_pages_per_host: Option<usize>,
    /// Queued URLs dropped because their host hit `max_pages_per_host`.
    skipped_host_budget: usize,
}

impl Frontier {
    fn new(max_pages_per_host: Option<usize>) -> Self {
        Self {
            per_host: HashMap::new(),
            order: VecDeque::new(),
            seen: HashSet::new(),
            dropped: 0,
            len: 0,
            taken: HashMap::new(),
            max_pages_per_host: max_pages_per_host.filter(|&n| n > 0),
            skipped_host_budget: 0,
        }
    }

    /// Enqueues `(url, depth)` into its host bucket, registering the host in the
    /// round-robin order if newly non-empty. Skips already-seen URLs and enforces
    /// the global `MAX_FRONTIER` cap.
    fn push(&mut self, url: String, depth: u32) {
        if self.seen.contains(&url) {
            return; // already discovered — normal dedup, not a coverage drop
        }
        if self.seen.len() >= MAX_FRONTIER {
            self.dropped += 1;
            return;
        }
        self.seen.insert(url.clone());
        self.enqueue(url, depth);
    }

    /// Routes `(url, depth)` into its host bucket without touching `seen` — used
    /// by both [`push`] (after the dedup check) and checkpoint restore.
    fn enqueue(&mut self, url: String, depth: u32) {
        let host = host_of(&url).unwrap_or_default();
        let q = self.per_host.entry(host.clone()).or_default();
        let was_empty = q.is_empty();
        q.push_back((url, depth));
        self.len += 1;
        if was_empty && !self.order.contains(&host) {
            self.order.push_back(host);
        }
    }

    /// Count of discovered URLs refused because the frontier cap was reached.
    fn dropped(&self) -> usize {
        self.dropped
    }

    /// Queued URLs dropped because their host hit its per-host page budget.
    fn skipped_host_budget(&self) -> usize {
        self.skipped_host_budget
    }

    /// Pops the next URL round-robin across hosts. A host that has reached
    /// `max_pages_per_host` has its remaining queue dropped (counted in
    /// `skipped_host_budget`) and leaves the rotation.
    fn pop(&mut self) -> Option<(String, u32)> {
        for _ in 0..self.order.len() {
            let Some(host) = self.order.pop_front() else {
                break;
            };
            // Over budget? Drop this host's remaining backlog, honestly counted.
            if let Some(cap) = self.max_pages_per_host {
                if self.taken.get(&host).copied().unwrap_or(0) >= cap {
                    if let Some(q) = self.per_host.remove(&host) {
                        self.skipped_host_budget += q.len();
                        self.len -= q.len();
                    }
                    continue; // host left the rotation
                }
            }
            let Some(q) = self.per_host.get_mut(&host) else {
                continue;
            };
            let Some(item) = q.pop_front() else {
                self.per_host.remove(&host);
                continue;
            };
            self.len -= 1;
            *self.taken.entry(host.clone()).or_insert(0) += 1;
            if q.is_empty() {
                self.per_host.remove(&host); // drop empty host from rotation
            } else {
                self.order.push_back(host); // rotate to the back
            }
            return Some(item);
        }
        None
    }

    /// Puts an already-seen URL back for a later tick (crawl-delay rotation). The
    /// budget increment from the matching [`pop`] is refunded — a requeue is not a
    /// consumed fetch.
    fn requeue(&mut self, url: String, depth: u32) {
        let host = host_of(&url).unwrap_or_default();
        if let Some(c) = self.taken.get_mut(&host) {
            *c = c.saturating_sub(1);
        }
        self.enqueue(url, depth);
    }

    fn len(&self) -> usize {
        self.len
    }

    /// Flattens the queued URLs for checkpointing (host grouping is rederived from
    /// the URL on restore, so the persisted shape stays a flat `(url, depth)` list
    /// — checkpoint-compatible with the pre-host-fairness format).
    fn queued(&self) -> Vec<(String, u32)> {
        self.per_host
            .values()
            .flat_map(|q| q.iter().cloned())
            .collect()
    }

    /// Per-host pages already handed out, for checkpointing.
    fn taken(&self) -> &HashMap<String, usize> {
        &self.taken
    }

    /// Restores queued URLs + seen-set + per-host `taken` counts from a
    /// checkpoint (bypasses the dedup check; `seen` is authoritative).
    ///
    /// Anti-pattern the `taken` half defends — *the budget that resets on every
    /// resume*. `max_pages_per_host` is documented as host fairness, but on a
    /// long crawl its real job is politeness, and durable execution silently
    /// multiplied it by the retry count: a `max_pages_per_host: 100` crawl reaped
    /// and re-claimed four times fetched up to 500 pages from one host.
    fn restore(
        &mut self,
        queue: Vec<(String, u32)>,
        seen: Vec<String>,
        taken: HashMap<String, usize>,
    ) {
        self.seen = seen.into_iter().collect();
        self.taken = taken;
        for (url, depth) in queue {
            self.enqueue(url, depth);
        }
    }
}

/// The crawler's near-duplicate gate: a thin wrapper over the shared
/// [`BandedIndex`](crate::simhash::BandedIndex) that keeps the crawler's own
/// vocabulary (`is_near_dup` + a checkpointable list of kept hashes).
///
/// The banding itself deliberately does NOT live here: the dataset store's
/// `duplicate_pairs` needs the same buckets, and two copies of the band
/// arithmetic is exactly where an off-by-one becomes a silent false negative.
struct SimHashIndex {
    inner: crate::simhash::BandedIndex<()>,
    /// Every kept hash, in insert order — persisted to the checkpoint so dedup
    /// survives a resume (8 bytes each: bounded by kept count, not bodies).
    all: Vec<u64>,
}

impl SimHashIndex {
    fn new(distance: u32) -> Self {
        Self {
            inner: crate::simhash::BandedIndex::new(distance),
            all: Vec::new(),
        }
    }

    /// Rebuilds an index (e.g. after a checkpoint resume) from kept hashes.
    fn from_hashes(distance: u32, hashes: Vec<u64>) -> Self {
        let mut idx = Self::new(distance);
        for h in hashes {
            idx.insert(h);
        }
        idx
    }

    /// True when some already-kept hash is within `distance` Hamming bits of
    /// `hash`. Identical decision to `all.iter().any(|h| hamming(*h, hash) <= d)`.
    fn is_near_dup(&self, hash: u64) -> bool {
        self.inner.is_near_dup(hash)
    }

    fn insert(&mut self, hash: u64) {
        self.inner.insert(hash, ());
        self.all.push(hash);
    }

    fn hashes(&self) -> &[u64] {
        &self.all
    }
}

/// Stored state of one known page during a revisit: the conditional-GET
/// validators plus the fingerprint + cadence the change grading needs.
#[derive(Debug, Clone)]
struct KnownPage {
    etag: Option<String>,
    last_modified: Option<String>,
    simhash: u64,
    cadence: RevisitCadence,
}

struct Fetched {
    url: String,
    depth: u32,
    status: u16,
    body: String,
    links: Vec<String>,
    title: Option<String>,
    content_chars: usize,
    excerpt: String,
    /// Response `ETag` / `Last-Modified` (case-insensitive header lookup),
    /// stored into the page record so a later revisit can revalidate.
    etag: Option<String>,
    last_modified: Option<String>,
}

/// Crawls from the seeds, writing kept page bodies under `output_dir` (if set).
///
/// `sink`, when provided, receives batches of [`CrawlPageRecord`] for KEPT pages
/// during the crawl — the seam the app layer uses to upsert per-page
/// fingerprints into the `pages` dataset without core knowing about storage.
///
/// `checkpointer`, when provided, receives the serialized frontier state
/// (queue + seen-set + kept hashes) periodically and at the end — the durable
/// -execution seam (`AppContext::checkpoints`). The same JSON comes back as
/// `cfg.resume_state` on a resumed attempt.
pub async fn crawl(
    http: Arc<dyn HttpClient>,
    cfg: CrawlConfig,
    output_dir: Option<PathBuf>,
    mut sink: Option<Box<dyn PageSink>>,
    source: Option<Box<dyn PageSource>>,
    progress: Option<ProgressFn>,
    checkpointer: Option<Arc<dyn crate::app::CheckpointSink>>,
) -> Result<CrawlStats> {
    let concurrency = cfg.concurrency.clamp(1, 256);
    // Buffer of kept-page fingerprints awaiting the next batched sink flush.
    let mut sink_buf: Vec<CrawlPageRecord> = Vec::new();
    // Revisit: per-known-URL stored state (validators + fingerprint + cadence).
    // Presence in this map marks a URL as "known" — it gets a conditional GET
    // and 304/gone handling; discovered URLs are absent and fetched normally.
    let mut conditional: HashMap<String, KnownPage> = HashMap::new();
    let filter = UrlFilter::compile(&cfg)?;
    let mut frontier = Frontier::new(cfg.max_pages_per_host);
    let mut dedup_index = SimHashIndex::new(cfg.dedup_distance);
    let mut resumed = false;
    let mut checkpoint_reset = false;

    // Restore a prior run's frontier + dedup state before seeding, so already
    // -seen URLs (including the seeds) aren't re-enqueued. An incompatible
    // (older-format) checkpoint is discarded for a clean fresh start — never a
    // silently-wrong partial resume.
    if let Some(state) = &cfg.resume_state {
        match Checkpoint::from_value(state) {
            CheckpointLoad::Loaded(cp) => {
                frontier.restore(cp.queue, cp.seen, cp.taken);
                dedup_index = SimHashIndex::from_hashes(cfg.dedup_distance, cp.kept_hashes);
                resumed = true;
            }
            CheckpointLoad::Incompatible => {
                checkpoint_reset = true;
                tracing::warn!(
                    "crawl: restored checkpoint format incompatible — discarding for a fresh start"
                );
            }
            CheckpointLoad::None => {}
        }
    }

    // Origins (scheme+host+port), not bare hosts: robots.txt and /sitemap.xml
    // both belong to an ORIGIN, and probing them over a scheme the crawl is not
    // using is how an http-only site ends up crawled with no rules at all.
    let mut seed_origins: HashSet<String> = HashSet::new();
    for seed in &cfg.seeds {
        if let Some(origin) = origin_of(seed) {
            seed_origins.insert(origin);
        }
        frontier.push(canonicalize_str(seed), 0);
    }
    // Revisit: seed the frontier from existing page records — but spend the
    // budget where change is likely. Each seed's learned cadence yields a
    // due_score(now) (cold-start URLs inherit their host's mean interval as the
    // prior); seeds below `min_due_score` or ranked past `revisit_budget` are
    // skipped this run and counted honestly in `skipped_not_due`.
    let mut skipped_not_due = 0usize;
    if cfg.revisit {
        if let Some(source) = source {
            let seeds = source.seeds().await;
            let priors = host_cadence_priors(&seeds);
            let now = epoch_now();
            let mut scored: Vec<(f64, RevisitSeed)> = Vec::with_capacity(seeds.len());
            for seed in seeds {
                let prior = seed
                    .cadence
                    .interval_secs
                    .or_else(|| host_of(&seed.url).and_then(|h| priors.get(&h).copied()))
                    .unwrap_or(DEFAULT_CADENCE_PRIOR_SECS);
                let score = due_score(&seed.cadence, now, prior);
                if score < cfg.min_due_score {
                    skipped_not_due += 1;
                    continue;
                }
                scored.push((score, seed));
            }
            // Most-due first; ties broken by URL for determinism.
            scored.sort_by(|a, b| {
                b.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.1.url.cmp(&b.1.url))
            });
            if let Some(budget) = cfg.revisit_budget {
                if scored.len() > budget {
                    skipped_not_due += scored.len() - budget;
                    scored.truncate(budget);
                }
            }
            for (_, seed) in scored {
                let url = canonicalize_str(&seed.url);
                if let Some(origin) = origin_of(&url) {
                    seed_origins.insert(origin);
                }
                conditional.insert(
                    url.clone(),
                    KnownPage {
                        etag: seed.etag,
                        last_modified: seed.last_modified,
                        simhash: seed.simhash,
                        cadence: seed.cadence,
                    },
                );
                frontier.push(url, 0);
            }
        }
    }
    if let Some(dir) = &output_dir {
        if let Err(e) = tokio::fs::create_dir_all(dir).await {
            tracing::warn!(dir = %dir.display(), "crawl: output dir create failed: {e}");
        }
    }

    let mut robots: HashMap<String, RobotRules> = HashMap::new();
    let mut robots_audit = RobotsAudit::default();
    let mut hosts: HashSet<String> = HashSet::new();
    let mut stats = CrawlStats {
        resumed,
        checkpoint_reset,
        skipped_not_due,
        ..Default::default()
    };
    let mut in_flight = FuturesUnordered::new();
    // URL → depth of every fetch currently in flight. `pop` takes a URL OUT of the
    // queue and INTO `seen` in one step, so between the pop and its outcome the
    // URL lives nowhere a checkpoint can see it — see [`checkpoint_queue`], which
    // merges this set back in on every save.
    let mut in_flight_urls: HashMap<String, u32> = HashMap::new();
    // Per-host earliest-next-fetch, driven by robots.txt Crawl-delay.
    let mut next_allowed: HashMap<String, tokio::time::Instant> = HashMap::new();
    // Last intermediate checkpoint save. Time-based, not page-based (see below).
    let mut last_checkpoint = tokio::time::Instant::now();

    // Expand seeds from each seed host's sitemaps before crawling.
    if cfg.sitemap_seeds {
        let origins: Vec<String> = seed_origins.iter().cloned().collect();
        for origin in origins {
            let declared = robots_for(&mut robots, &http, &origin, &mut robots_audit)
                .await
                .sitemaps
                .clone();
            let budget = MAX_SITEMAP_SEEDS.saturating_sub(stats.sitemap_seeded);
            if budget == 0 {
                break;
            }
            stats.sitemap_seeded +=
                seed_from_sitemaps(&http, &origin, &declared, &mut frontier, &filter, budget).await;
        }
    }

    loop {
        // Top up in-flight fetches from the frontier. `rotations` guards the
        // crawl-delay requeue path against spinning through a queue where
        // every remaining URL is still inside its host's delay window.
        let mut rotations = 0;
        while in_flight.len() < concurrency && rotations <= frontier.len() {
            let Some((url, depth)) = frontier.pop() else {
                break;
            };
            let host = host_of(&url).unwrap_or_default();
            let mut crawl_delay = None;
            // Robots are looked up by ORIGIN, so the probe uses the very scheme
            // (and port) this fetch is about to use — see [`robots_url`].
            if let Some(origin) = cfg.respect_robots.then(|| origin_of(&url)).flatten() {
                let rules = robots_for(&mut robots, &http, &origin, &mut robots_audit).await;
                let allowed = rules.allowed(&url);
                crawl_delay = rules.crawl_delay;
                if !allowed {
                    stats.skipped_robots += 1;
                    continue;
                }
            }
            if let Some(delay) = crawl_delay {
                let now = tokio::time::Instant::now();
                if next_allowed.get(&host).is_some_and(|&t| now < t) {
                    frontier.requeue(url, depth);
                    rotations += 1;
                    continue;
                }
                // Cap silly delays; a 3600s crawl-delay would stall the run.
                let delay = std::time::Duration::from_secs_f64(delay.min(30.0));
                next_allowed.insert(host.clone(), now + delay);
            }
            hosts.insert(host);
            let http = http.clone();
            let same_domain = cfg.same_domain;
            // A known page (in `conditional`) gets a revalidating conditional GET.
            let cond = if cfg.revisit {
                conditional
                    .get(&url)
                    .map(|k| (k.etag.clone(), k.last_modified.clone()))
            } else {
                None
            };
            in_flight_urls.insert(url.clone(), depth);
            in_flight.push(async move { fetch_one(http, url, depth, same_domain, cond).await });
        }

        // Periodic checkpoint, gated by wall-clock rather than page count, and
        // evaluated ONCE PER LOOP TURN — i.e. after EVERY fetch outcome, not only
        // after a kept page. It used to sit inside the kept-page branch, below the
        // `continue`s for Failed / BotWall / NotModified / Gone / duplicate, so a
        // revisit sweep over 10k mostly-`304` pages produced zero intermediate
        // checkpoints: killed at 95% it lost 95%.
        //
        // `save_checkpoint` serializes the WHOLE frontier (up to MAX_FRONTIER
        // seen-strings + queue + kept hashes) — O(frontier), not O(delta) — so
        // firing it every N pages made total checkpoint work O(pages/N × frontier):
        // a 100k-page crawl did thousands of full ~10 MB rewrites (tens of GB of
        // write amplification) for state that moved by a handful of pages, and each
        // inline save stalled every in-flight fetch. A minimum interval decouples
        // save count from crawl size; the final save below still captures the true
        // end state, and the frontier's own seen-set makes a resume idempotent.
        if checkpointer.is_some() && last_checkpoint.elapsed() >= CHECKPOINT_MIN_INTERVAL {
            if !flush_then_checkpoint(
                &mut sink,
                &mut sink_buf,
                checkpointer.as_ref(),
                &frontier,
                &in_flight_urls,
                dedup_index.hashes(),
                false,
            )
            .await
            {
                stats.checkpoint_errors += 1;
            }
            last_checkpoint = tokio::time::Instant::now();
        }

        if in_flight.is_empty() {
            if frontier.len() == 0 {
                break; // frontier drained and nothing in flight
            }
            // Everything left is crawl-delayed; wait out the shortest window.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        let Some(result) = in_flight.next().await else {
            break;
        };
        // Retire the URL from the in-flight set: from here its outcome is either
        // handed to the sink or deliberately dropped, so the next checkpoint must
        // not re-queue it.
        in_flight_urls.remove(fetch_outcome_url(&result));
        let fetched = match result {
            CrawlFetch::Page(f) => f,
            CrawlFetch::Failed(url) => {
                stats.failed += 1;
                if let Some(host) = host_of(&url) {
                    *stats.failed_by_host.entry(host).or_default() += 1;
                }
                tracing::debug!(url = %url, "crawl: fetch failed");
                continue;
            }
            CrawlFetch::BotWall(url, reason) => {
                stats.skipped_botwall += 1;
                tracing::debug!(url = %url, reason = %reason, "crawl: skipped bot-wall");
                continue;
            }
            CrawlFetch::NotModified(url) => {
                // Cheap unchanged: no body downloaded, not re-fingerprinted.
                stats.revisited += 1;
                stats.unchanged_304 += 1;
                tracing::debug!(url = %url, "crawl: 304 unchanged");
                // The 304 is still a cadence observation: bump the check
                // counters and stream a check-only marker so the estimator
                // improves every run (the sink merges it into the record).
                if sink.is_some() {
                    if let Some(known) = conditional.get(&url) {
                        let cadence = known.cadence.observe_unchanged(epoch_now());
                        sink_buf.push(unchanged_record(url, cadence));
                        if sink_buf.len() >= PAGE_SINK_STRIDE {
                            flush_page_sink(&mut sink, &mut sink_buf).await;
                        }
                    }
                }
                continue;
            }
            CrawlFetch::Gone(url, status) => {
                stats.revisited += 1;
                stats.gone += 1;
                tracing::debug!(url = %url, status, "crawl: page gone");
                // Emit a gone marker through the sink so the dataset reflects the
                // removal (explicit per-key `gone` field, NOT a sync_many snapshot
                // removal — a revisit is a partial view).
                if sink.is_some() {
                    sink_buf.push(gone_record(url, status));
                    if sink_buf.len() >= PAGE_SINK_STRIDE {
                        flush_page_sink(&mut sink, &mut sink_buf).await;
                    }
                }
                continue;
            }
        };
        stats.crawled += 1;
        // A known page fetched with a conditional GET that came back 200 (a
        // discovered link is absent from `conditional`): count the revisit. The
        // same flag decides whether the cross-page dedup gate applies at all.
        let known_page = cfg.revisit && conditional.contains_key(&fetched.url);
        if known_page {
            stats.revisited += 1;
        }

        let hash = simhash(&fetched.body);
        let duplicate =
            dedup_applies(cfg.dedup_distance, known_page) && dedup_index.is_near_dup(hash);

        if duplicate {
            stats.skipped_duplicates += 1;
        } else {
            dedup_index.insert(hash);
            stats.kept += 1;
            // URL-addressed, NOT the per-run `stats.kept` counter: that counter
            // restarts at 0 on a checkpoint resume, so a resumed crawl would write
            // page-0001.html over the prior run's page-0001.html — a different URL's
            // body — leaving earlier `pages` records' `artifact_path` pointing at
            // the wrong content. Keying the file on the (canonical, frontier-unique)
            // URL makes the name stable across runs: each URL owns one file, and a
            // revisit updates it in place.
            let artifact_name = artifact_name(&fetched.url);
            if let Some(dir) = &output_dir {
                let file = dir.join(&artifact_name);
                if let Err(e) = tokio::fs::write(&file, &fetched.body).await {
                    tracing::warn!(path = %file.display(), "crawl: page write failed: {e}");
                }
            }
            // Stream this kept page's compact fingerprint to the sink (batched).
            if sink.is_some() {
                // Cadence bookkeeping: a KNOWN page's change is graded by SimHash
                // distance from its stored fingerprint (boilerplate churn weighs
                // ~0, a real edit ~1) and feeds the EWMA interval; an unknown URL
                // starts a fresh baseline. Authoritative in revisit mode — a full
                // fresh recrawl restarts baselines (it can't know prior state).
                let now = epoch_now();
                let cadence = match conditional.get(&fetched.url) {
                    Some(known) => known
                        .cadence
                        .observe_changed(now, change_weight(known.simhash, hash)),
                    None => RevisitCadence::first_seen(now),
                };
                sink_buf.push(CrawlPageRecord {
                    url: fetched.url.clone(),
                    title: fetched.title.clone(),
                    status: fetched.status,
                    content_chars: fetched.content_chars,
                    simhash: hash,
                    excerpt: fetched.excerpt.clone(),
                    artifact_path: if output_dir.is_some() {
                        artifact_name
                    } else {
                        String::new()
                    },
                    depth: fetched.depth,
                    etag: fetched.etag.clone(),
                    last_modified: fetched.last_modified.clone(),
                    gone: false,
                    unchanged: false,
                    cadence: Some(cadence),
                    links: fetched.links.clone(),
                });
                if sink_buf.len() >= PAGE_SINK_STRIDE {
                    flush_page_sink(&mut sink, &mut sink_buf).await;
                }
            }
        }

        // Enqueue newly discovered links within the depth budget — for BOTH kept
        // and near-duplicate pages. A page being a content near-dup of another
        // does NOT mean its outbound links are already known; following them only
        // from kept pages silently under-crawls subtrees (pagination / faceted
        // nav) reachable only via a near-dup page. The frontier's own URL seen-set
        // still prevents re-fetching. (Revisit mode does not expand unless
        // `discover` is set — a sentinel recrawl re-checks, it doesn't expand.)
        let expand = !cfg.revisit || cfg.discover;
        if expand && fetched.depth < cfg.max_depth {
            for link in &fetched.links {
                if !filter.allows(link) {
                    stats.skipped_filtered += 1;
                    continue;
                }
                frontier.push(link.clone(), fetched.depth + 1);
            }
        }

        // Per-page metadata is NOT accumulated in memory (it streams to the
        // dataset via the sink); the result keeps only counters + the artifacts
        // dir + `pages` dataset as pointers.

        // Live progress: cheap seam call every stride; the runtime throttles the
        // actual persist/emit so a huge crawl stays observable without spamming.
        if stats.crawled.is_multiple_of(PROGRESS_STRIDE) {
            emit_progress(&progress, &stats, frontier.len(), hosts.len());
        }

        if stats.kept >= cfg.max_pages {
            break;
        }
    }

    stats.hosts = hosts.len();
    // Unresolved in-flight URLs are still WORK, not coverage: the `max_pages`
    // break abandons up to `concurrency - 1` of them, and they are already in
    // `seen`, so the resume point is the queue plus that set. Reporting only
    // `frontier.len()` here understated the remaining work by exactly the URLs
    // the run had buried.
    stats.frontier_remaining = frontier.len() + in_flight_urls.len();
    stats.frontier_dropped = frontier.dropped();
    stats.skipped_host_budget = frontier.skipped_host_budget();
    // Compliance evidence: how many origins failed open, and WHICH ones.
    stats.robots_fetch_failures = robots_audit.unverified_count();
    stats.robots_unverified_hosts = robots_audit.unverified_hosts();
    // Final, unthrottled save so the persisted state reflects the true end of the
    // run (an incomplete crawl's remaining frontier is the resume point). Flushes
    // the sink first — see [`flush_then_checkpoint`].
    if !flush_then_checkpoint(
        &mut sink,
        &mut sink_buf,
        checkpointer.as_ref(),
        &frontier,
        &in_flight_urls,
        dedup_index.hashes(),
        true,
    )
    .await
    {
        stats.checkpoint_errors += 1;
    }
    // Final snapshot so a subscriber's last progress event reflects the true end
    // state (the throttle may have suppressed the last periodic tick).
    emit_progress(&progress, &stats, stats.frontier_remaining, stats.hosts);
    stats.failed_by_host = top_n_by_count(stats.failed_by_host, MAX_FAILED_HOSTS);
    Ok(stats)
}

/// Minimum wall-clock between intermediate checkpoint saves. Each save is a full
/// O(frontier) serialize, so this bounds total checkpoint work by crawl *duration*
/// instead of page count. The final save on exit is unconditional, so this only
/// affects mid-crawl resume granularity (a few seconds of re-crawl, which the
/// seen-set makes idempotent).
#[cfg(not(test))]
const CHECKPOINT_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Test seam. What the tests here assert about checkpointing is the **placement**
/// of the interval check — it used to sit inside the kept-page branch, so a run
/// of 304s/duplicates/failures never checkpointed at all — not the pacing value,
/// which is one comparison. A millisecond interval lets a test drive the real
/// `crawl()` loop across several intermediate saves in a tenth of a second
/// instead of half a minute. (`tokio`'s `test-util` clock, which would let the
/// production value stand, is not enabled for this crate.)
#[cfg(test)]
const CHECKPOINT_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);

/// Stable, filesystem-safe artifact filename for a page, addressed by its URL
/// rather than a per-run sequence number. The frontier de-duplicates URLs, so
/// this is unique within a crawl; being a pure function of the URL, it is also
/// stable across resumes and revisits (the `pages` record's `artifact_path` and
/// the file on disk can never disagree). 16 bytes of SHA-256 (128 bits) is far
/// beyond collision range for any single crawl's URL set.
fn artifact_name(url: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(url.as_bytes());
    let hex: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
    format!("page-{hex}.html")
}

/// Cap on the per-host failure map surfaced in the result — only the worst
/// offenders are useful; the total lives in `failed`.
const MAX_FAILED_HOSTS: usize = 20;

/// Keeps the `n` highest-count entries of a host→count map (ties broken by host
/// name for determinism), dropping the long tail so the result stays compact.
fn top_n_by_count(map: HashMap<String, usize>, n: usize) -> HashMap<String, usize> {
    if map.len() <= n {
        return map;
    }
    let mut entries: Vec<(String, usize)> = map.into_iter().collect();
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    entries.into_iter().take(n).collect()
}

/// Current checkpoint schema version. Bumped when the persisted shape changes;
/// a mismatch triggers a clean fresh start rather than a silently-wrong resume.
/// v2 added the per-host `taken` budget counts.
const CHECKPOINT_VERSION: u32 = 2;

/// Result of attempting to restore a checkpoint.
enum CheckpointLoad {
    /// No checkpoint state present — start fresh, not an error.
    None,
    /// A compatible checkpoint restored.
    Loaded(Checkpoint),
    /// State existed but was an incompatible version / unparseable format;
    /// discarded for a fresh start (surfaced as `checkpoint_reset`).
    Incompatible,
}

/// Persisted frontier state: what is still queued, what has been seen, and the
/// SimHash fingerprints of kept pages (so dedup survives the resume too).
#[derive(Serialize, Deserialize)]
struct Checkpoint {
    /// Schema version; `#[serde(default)]` makes pre-versioning files parse as
    /// version 0, which then fails the compatibility check → fresh start.
    #[serde(default)]
    version: u32,
    queue: Vec<(String, u32)>,
    seen: Vec<String>,
    kept_hashes: Vec<u64>,
    /// Pages already handed out per host, so `max_pages_per_host` is a budget for
    /// the CRAWL rather than for each attempt of it.
    #[serde(default)]
    taken: HashMap<String, usize>,
}

impl Checkpoint {
    /// Interprets restored state. `Null` is "nothing stored" (fresh start); a
    /// present value that doesn't parse as the current version is an
    /// incompatible/corrupt checkpoint — never resumed from silently.
    fn from_value(state: &serde_json::Value) -> CheckpointLoad {
        if state.is_null() {
            return CheckpointLoad::None;
        }
        match serde_json::from_value::<Checkpoint>(state.clone()) {
            Ok(cp) if cp.version == CHECKPOINT_VERSION => CheckpointLoad::Loaded(cp),
            _ => CheckpointLoad::Incompatible,
        }
    }
}

/// Whether the cross-page near-duplicate gate applies to this fetch.
///
/// Cross-page dedup answers *"is this the same content we already kept under a
/// different URL?"* — the right question for a FRESH crawl, where twenty URLs of
/// one templated page are twenty copies of one document. A revisit asks the
/// opposite question: a sentinel recrawl re-checks each KNOWN page against **its
/// own history**, not against its siblings. Two product pages that share a
/// template are not duplicates of one another for monitoring purposes.
///
/// Anti-pattern this defends — *the frozen revisit record*. The gate used to be
/// unconditional, and the app ships `dedup_distance: 3` in every mode, so this
/// was the default behavior rather than an edge case. A known page whose body
/// landed within 3 bits of a sibling already fetched this run bumped
/// `revisited`, then returned without ever touching the sink: its fresh
/// `etag`/`last_modified` were discarded, its [`RevisitCadence`] never advanced,
/// and its stored record froze — stale validators and all. The next run sent the
/// same stale validator, got another full `200`, and was dropped again. Forever,
/// over exactly the paginated/templated population a revisit sweep exists for,
/// while `skipped_duplicates` climbed and the run reported success.
///
/// A URL *discovered* during a revisit (`discover`) is not a known page, so it
/// keeps the fresh-crawl gate.
fn dedup_applies(dedup_distance: u32, known_page: bool) -> bool {
    dedup_distance > 0 && !known_page
}

/// The queue a checkpoint must persist: everything still in the frontier PLUS
/// every URL currently in flight.
///
/// Anti-pattern this defends — *an in-flight URL is not a crawled URL*.
/// [`Frontier::pop`] removes a URL from its host bucket and inserts it into
/// `seen` in one step, so while its fetch is outstanding the URL exists in
/// neither the queue nor the sink. Persisting only `frontier.queued()` wrote
/// those URLs as seen-but-not-queued, and [`Frontier::restore`] treats `seen` as
/// authoritative — so every `max_pages` break (which abandons up to
/// `concurrency - 1` outstanding fetches) and every kill buried them
/// permanently, in a resume that then reported success. Merging them back in
/// costs at most `concurrency` extra queue entries and makes the resume
/// idempotent: they are re-fetched, and the seen-set stops nothing else.
///
/// Duplicates are impossible: a URL in flight was popped out of its bucket, and
/// `push` early-returns on anything already in `seen`, so it cannot be back in
/// the queue while its fetch is outstanding.
fn checkpoint_queue(frontier: &Frontier, in_flight: &HashMap<String, u32>) -> Vec<(String, u32)> {
    let mut queue = frontier.queued();
    queue.extend(in_flight.iter().map(|(url, depth)| (url.clone(), *depth)));
    queue
}

/// The URL of a fetch outcome, whatever its disposition — the key that retires
/// it from the in-flight set.
fn fetch_outcome_url(fetch: &CrawlFetch) -> &str {
    match fetch {
        CrawlFetch::Page(f) => &f.url,
        CrawlFetch::Failed(url)
        | CrawlFetch::BotWall(url, _)
        | CrawlFetch::NotModified(url)
        | CrawlFetch::Gone(url, _) => url,
    }
}

/// Hands every buffered page record to the [`PageSink`]. The single drain point,
/// used both at [`PAGE_SINK_STRIDE`] and — mandatorily — before every checkpoint.
async fn flush_page_sink(sink: &mut Option<Box<dyn PageSink>>, buf: &mut Vec<CrawlPageRecord>) {
    if buf.is_empty() {
        return;
    }
    match sink.as_mut() {
        Some(s) => s.emit(std::mem::take(buf)).await,
        // No sink to hand them to; don't let the buffer grow unbounded.
        None => buf.clear(),
    }
}

/// The ONE path that persists crawl state. Invariant, stated once and enforced
/// here: **the checkpoint never claims a page the sink has not been handed.**
///
/// Anti-pattern this defends — *checkpoint-before-flush*. Kept pages reach the
/// `pages` dataset only every [`PAGE_SINK_STRIDE`] records, while the checkpoint
/// fires on a wall clock and serializes `frontier.seen` *and* the kept
/// fingerprints — both of which already contain the still-buffered page. A kill
/// in that window left the body orphaned on disk with no record pointing at it,
/// its URL marked seen (so a resume never re-fetched it), and its fingerprint
/// live in the restored dup index (so near-dups of a page that no longer exists
/// in the dataset kept being suppressed). Flushing first collapses the window:
/// worst case a page is emitted twice, which the sink upserts idempotently.
///
/// The same ordering is what makes the `gone` / `unchanged_304` / `revisited`
/// counters honest — those markers ride the same buffer, so a run could report
/// `gone: 40` with zero `gone: true` rows written. No second mechanism needed.
///
/// Best-effort: checkpointing must never fail the crawl, but a failure is not
/// swallowed — returns `false` (warn-logged) so the caller can surface a
/// `checkpoint_errors` count in the result.
async fn flush_then_checkpoint(
    sink: &mut Option<Box<dyn PageSink>>,
    sink_buf: &mut Vec<CrawlPageRecord>,
    checkpointer: Option<&Arc<dyn crate::app::CheckpointSink>>,
    frontier: &Frontier,
    in_flight: &HashMap<String, u32>,
    kept_hashes: &[u64],
    force: bool,
) -> bool {
    flush_page_sink(sink, sink_buf).await;
    match checkpointer {
        Some(cp) => save_checkpoint(cp, frontier, in_flight, kept_hashes, force).await,
        None => true,
    }
}

/// Serializes the frontier state and hands it to the durable checkpoint sink.
/// Callers go through [`flush_then_checkpoint`], never here directly — the sink
/// flush has to happen first. The sink throttles its own persistence; `force`
/// bypasses that for the final end-of-run snapshot.
async fn save_checkpoint(
    sink: &Arc<dyn crate::app::CheckpointSink>,
    frontier: &Frontier,
    in_flight: &HashMap<String, u32>,
    kept_hashes: &[u64],
    force: bool,
) -> bool {
    let cp = Checkpoint {
        version: CHECKPOINT_VERSION,
        queue: checkpoint_queue(frontier, in_flight),
        seen: frontier.seen.iter().cloned().collect(),
        kept_hashes: kept_hashes.to_vec(),
        taken: frontier.taken().clone(),
    };
    match serde_json::to_value(&cp) {
        Ok(state) => sink.save(state, force).await,
        Err(e) => {
            tracing::warn!("crawl: checkpoint serialize failed: {e}");
            false
        }
    }
}

/// Disposition of one fetch attempt. Previously a bare `Option<Fetched>` that
/// collapsed transport failures and bot-walls into an indistinguishable `None`,
/// which the loop dropped silently. Now each outcome is counted honestly.
enum CrawlFetch {
    /// A real content response, ready to dedup / keep.
    Page(Fetched),
    /// Transport-layer failure (DNS/TLS/connection/timeout). Carries the URL for
    /// per-host attribution.
    Failed(String),
    /// Classified as a bot-wall / challenge (see `fetcher::http_bot_wall`) — not
    /// stored as content. Carries the URL and the classification reason.
    BotWall(String, String),
    /// Revisit only: a conditional GET answered `304 Not Modified` — the page is
    /// unchanged and was not re-downloaded/re-fingerprinted.
    NotModified(String),
    /// Revisit only: a known page returned `404`/`410` — flag it gone. Carries
    /// the URL and the status.
    Gone(String, u16),
}

/// Case-insensitive header lookup returning a non-empty value.
fn header_value(headers: &HashMap<String, String>, name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
        .filter(|v| !v.is_empty())
}

async fn fetch_one(
    http: Arc<dyn HttpClient>,
    url: String,
    depth: u32,
    same_domain: bool,
    // `Some` ⇒ this is a revisit of a KNOWN page: send its stored validators as a
    // conditional GET (bypassing the TTL cache so it actually revalidates) and
    // resolve `304`/`404`/`410` specially. `None` ⇒ a normal full fetch.
    conditional: Option<(Option<String>, Option<String>)>,
) -> CrawlFetch {
    let mut req = HttpRequest::get(&url);
    if let Some((etag, last_modified)) = &conditional {
        // Force a network revalidation; the TTL cache would otherwise serve a
        // 200 and defeat the whole point of the conditional GET.
        req.no_cache = true;
        req.etag = etag.clone();
        req.if_modified_since = last_modified.clone();
    }
    let resp = match http.fetch(req).await {
        Ok(resp) => resp,
        Err(_) => return CrawlFetch::Failed(url),
    };
    // Known-page revisit outcomes take priority over content parsing.
    if conditional.is_some() {
        if resp.status == 304 {
            return CrawlFetch::NotModified(url);
        }
        if matches!(resp.status, 404 | 410) {
            return CrawlFetch::Gone(url, resp.status);
        }
    }
    // A challenge/block response (403/429/503 or a Cloudflare/JS/CAPTCHA marker
    // on a 200) is not content — reuse the fetcher's shared classifier.
    if let Some(reason) = crate::fetcher::http_bot_wall(resp.status, &resp.body) {
        return CrawlFetch::BotWall(url, reason);
    }
    let etag = header_value(&resp.headers, "etag");
    let last_modified = header_value(&resp.headers, "last-modified");
    let parsed = parse_page(&resp.body, &url, same_domain);
    CrawlFetch::Page(Fetched {
        url,
        depth,
        status: resp.status,
        body: resp.body,
        links: parsed.links,
        title: parsed.title,
        content_chars: parsed.content_chars,
        excerpt: parsed.excerpt,
        etag,
        last_modified,
    })
}

/// Everything derived from one parse of a page body: outbound links plus a
/// compact content fingerprint (title / visible-text chars / excerpt). Parsed
/// once, off the main loop, inside the concurrent fetch task.
struct ParsedPage {
    links: Vec<String>,
    title: Option<String>,
    content_chars: usize,
    excerpt: String,
}

fn parse_page(html: &str, base: &str, same_domain: bool) -> ParsedPage {
    let doc = Html::parse_document(html);
    let links = extract_links(&doc, base, same_domain);
    let title = extract_title(&doc);
    let text = extract_text(&doc);
    let content_chars = text.chars().count();
    let excerpt: String = text.chars().take(EXCERPT_CHARS).collect();
    ParsedPage {
        links,
        title,
        content_chars,
        excerpt,
    }
}

fn extract_links(doc: &Html, base: &str, same_domain: bool) -> Vec<String> {
    let Ok(base_url) = Url::parse(base) else {
        return Vec::new();
    };
    let base_host = base_url.host_str().map(str::to_owned);
    let selector = Selector::parse("a[href]").expect("valid selector");
    let mut out = Vec::new();
    for el in doc.select(&selector) {
        let Some(href) = el.value().attr("href") else {
            continue;
        };
        let Ok(joined) = base_url.join(href) else {
            continue;
        };
        if !matches!(joined.scheme(), "http" | "https") {
            continue;
        }
        if same_domain && joined.host_str().map(str::to_owned) != base_host {
            continue;
        }
        out.push(canonicalize(joined));
    }
    out
}

/// `<title>` text (whitespace-collapsed), or `None` when absent/empty.
fn extract_title(doc: &Html) -> Option<String> {
    let selector = Selector::parse("title").expect("valid selector");
    let raw: String = doc.select(&selector).next()?.text().collect();
    let title: String = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    (!title.is_empty()).then_some(title)
}

/// Visible page text, script/style/noscript excluded, whitespace-collapsed. Used
/// only for compact fingerprints (char count + excerpt), so approximate is fine.
fn extract_text(doc: &Html) -> String {
    let mut out = String::new();
    for node in doc.tree.nodes() {
        let Some(text) = node.value().as_text() else {
            continue;
        };
        let in_non_content = node.ancestors().any(|a| {
            a.value().as_element().is_some_and(|e| {
                matches!(
                    e.name(),
                    "script" | "style" | "noscript" | "template" | "head" | "title"
                )
            })
        });
        if in_non_content {
            continue;
        }
        for word in text.split_whitespace() {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(word);
        }
    }
    out
}

/// Query parameters that never change page content — dropped so the frontier's
/// seen-set doesn't treat `?utm_source=x` variants as distinct pages.
const TRACKING_PARAMS: &[&str] = &[
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "msclkid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
];

/// Canonical form of a URL for frontier dedup: fragment stripped, tracking
/// params dropped, remaining query pairs sorted, trailing slash trimmed off
/// non-root paths. `Url` itself already lowercases scheme/host and drops
/// default ports.
fn canonicalize(mut url: Url) -> String {
    url.set_fragment(None);
    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| !TRACKING_PARAMS.contains(&k.as_ref()))
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.sort();
    if pairs.is_empty() {
        url.set_query(None);
    } else {
        let query: Vec<String> = pairs
            .into_iter()
            .map(|(k, v)| if v.is_empty() { k } else { format!("{k}={v}") })
            .collect();
        url.set_query(Some(&query.join("&")));
    }
    if url.path().len() > 1 && url.path().ends_with('/') {
        let trimmed = url.path().trim_end_matches('/').to_string();
        url.set_path(&trimmed);
    }
    url.to_string()
}

/// Canonicalizes a raw URL string; passes through unparseable input unchanged.
fn canonicalize_str(url: &str) -> String {
    Url::parse(url)
        .map(canonicalize)
        .unwrap_or_else(|_| url.to_string())
}

fn host_of(url: &str) -> Option<String> {
    Url::parse(url).ok()?.host_str().map(str::to_owned)
}

/// Scheme + host + port of a URL — the identity a robots.txt actually belongs to,
/// and therefore the robots cache key. `None` for anything that is not an
/// http(s) URL. `Url` normalizes default ports away, so `https://x` and
/// `https://x:443` are the same origin while `http://x` and `https://x` are not.
fn origin_of(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    let host = parsed.host_str()?;
    Some(match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    })
}

/// The robots.txt URL governing a page URL: **the scheme the crawl is actually
/// using for that host**, that host, and its port.
///
/// Anti-pattern this defends — *the https-only robots probe*. The URL used to be
/// `https://{host}/robots.txt` with the seed's own scheme ignored and the bare
/// host as the cache key. For an `http`-only origin that probe fails at the
/// transport layer, which fails open to allow-all, and the crawl then walks every
/// `Disallow:` path while reporting `respect_robots: true` and
/// `skipped_robots: 0`. The site owner finds out before the operator does.
fn robots_url(page_url: &str) -> Option<String> {
    Some(format!("{}/robots.txt", origin_of(page_url)?))
}

/// Cap on the named unverified-robots origins carried in the result; the full
/// count stays in `robots_fetch_failures`. Same idiom as [`MAX_FAILED_HOSTS`].
const MAX_UNVERIFIED_HOSTS: usize = 50;

/// Evidence about how robots.txt compliance was actually achieved this run, so
/// the result can distinguish hosts crawled under **verified** rules from hosts
/// crawled under a **failed-open assumption**.
#[derive(Default)]
struct RobotsAudit {
    /// Origins whose robots.txt fetch failed at the transport layer. A
    /// `BTreeSet` so the reported list is deduped and deterministic.
    unverified: BTreeSet<String>,
}

impl RobotsAudit {
    /// Records that `origin` was crawled without its rules ever being read.
    fn failed_open(&mut self, origin: &str) {
        self.unverified.insert(origin.to_string());
    }

    /// How many origins were crawled without verified rules (one fetch per
    /// origin, so this is also the robots fetch-failure count).
    fn unverified_count(&self) -> usize {
        self.unverified.len()
    }

    /// The named origins, deterministic and capped.
    fn unverified_hosts(&self) -> Vec<String> {
        self.unverified
            .iter()
            .take(MAX_UNVERIFIED_HOSTS)
            .cloned()
            .collect()
    }
}

async fn robots_for<'a>(
    cache: &'a mut HashMap<String, RobotRules>,
    http: &Arc<dyn HttpClient>,
    origin: &str,
    audit: &mut RobotsAudit,
) -> &'a RobotRules {
    if !cache.contains_key(origin) {
        // `origin` is already an origin string, so this is `{origin}/robots.txt`;
        // the fallback is unreachable for anything the callers pass.
        let url = robots_url(origin).unwrap_or_else(|| format!("{origin}/robots.txt"));
        let rules = match http.fetch(HttpRequest::get(&url)).await {
            Ok(resp) if resp.is_success() => RobotRules::parse(&resp.body),
            // A non-2xx (e.g. 404 "no robots") is a legitimate allow-all.
            Ok(_) => RobotRules::allow_all(),
            // A transport failure is NOT "no robots" — fail open, but name the
            // origin instead of silently pretending it allowed everything.
            Err(e) => {
                audit.failed_open(origin);
                tracing::debug!(%origin, "crawl: robots.txt fetch failed: {e}");
                RobotRules::allow_all()
            }
        };
        cache.insert(origin.to_string(), rules);
    }
    cache.get(origin).unwrap()
}

/// robots.txt rules for the `*` user-agent: ordered Allow/Disallow patterns
/// (with `*`/`$` wildcards and longest-match precedence), plus the `Crawl-delay`
/// for that group and the (group-independent) `Sitemap:` directives.
struct RobotRules {
    /// `(is_allow, pattern)` in file order. A path is matched against every
    /// pattern; the longest match wins, and an `Allow` beats a `Disallow` on an
    /// equal-length tie (the common Google robots precedence).
    rules: Vec<(bool, String)>,
    crawl_delay: Option<f64>,
    sitemaps: Vec<String>,
}

impl RobotRules {
    fn allow_all() -> Self {
        Self {
            rules: Vec::new(),
            crawl_delay: None,
            sitemaps: Vec::new(),
        }
    }

    fn parse(text: &str) -> Self {
        let mut rules = Self::allow_all();
        let mut in_star_group = false;
        for raw in text.lines() {
            let line = raw.split('#').next().unwrap_or("").trim();
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            let key = key.trim().to_ascii_lowercase();
            // `Sitemap:` values are absolute URLs — re-join the split colon.
            let value = if key == "sitemap" {
                line.split_once(':').map(|x| x.1).unwrap_or("").trim()
            } else {
                value.trim()
            };
            match key.as_str() {
                "user-agent" => in_star_group = value == "*",
                "disallow" if in_star_group && !value.is_empty() => {
                    rules.rules.push((false, value.to_string()));
                }
                "allow" if in_star_group && !value.is_empty() => {
                    rules.rules.push((true, value.to_string()));
                }
                "crawl-delay" if in_star_group => {
                    rules.crawl_delay = value.parse::<f64>().ok().filter(|d| *d > 0.0);
                }
                "sitemap" if !value.is_empty() => rules.sitemaps.push(value.to_string()),
                _ => {}
            }
        }
        rules
    }

    fn allowed(&self, url: &str) -> bool {
        let path = Url::parse(url)
            .ok()
            .map(|u| match u.query() {
                Some(q) => format!("{}?{}", u.path(), q),
                None => u.path().to_string(),
            })
            .unwrap_or_else(|| "/".to_string());
        // Longest matching pattern wins; Allow beats Disallow on an equal-length
        // tie; no match at all → allowed.
        let mut best: Option<(usize, bool)> = None; // (specificity, is_allow)
        for (is_allow, pattern) in &self.rules {
            if let Some(len) = robots_match_len(pattern, &path) {
                let better = match best {
                    None => true,
                    Some((blen, ballow)) => len > blen || (len == blen && *is_allow && !ballow),
                };
                if better {
                    best = Some((len, *is_allow));
                }
            }
        }
        best.map(|(_, is_allow)| is_allow).unwrap_or(true)
    }
}

/// Matches a robots path pattern against `path`, returning the pattern's
/// specificity (byte length, minus a trailing `$`) when it matches, else `None`.
/// Robots patterns match from the START of the path; `*` matches any run
/// (including empty) and a trailing `$` anchors the match to the path end.
fn robots_match_len(pattern: &str, path: &str) -> Option<usize> {
    let anchored = pattern.ends_with('$');
    let pat = if anchored {
        &pattern[..pattern.len() - 1]
    } else {
        pattern
    };
    let mut pos = 0usize;
    for (i, seg) in pat.split('*').enumerate() {
        if seg.is_empty() {
            continue;
        }
        if i == 0 {
            // The first literal segment is anchored to the path start.
            if !path[pos..].starts_with(seg) {
                return None;
            }
            pos += seg.len();
        } else {
            match path[pos..].find(seg) {
                Some(idx) => pos += idx + seg.len(),
                None => return None,
            }
        }
    }
    // `$` requires the match to reach the end of the path (unless the pattern
    // ends with `*`, which already permits any suffix).
    if anchored && !pat.ends_with('*') && pos != path.len() {
        return None;
    }
    Some(pat.len())
}

/// Hard caps for sitemap seeding: nested sitemaps followed per index, and total
/// URLs pushed — a big site's sitemap must not replace the crawl itself.
const MAX_SITEMAPS_PER_HOST: usize = 10;
const MAX_SITEMAP_SEEDS: usize = 2_000;

/// `<loc>` values from a sitemap or sitemap-index document.
/// One sitemap entry: the URL and its optional `<lastmod>` (W3C datetime), which
/// the crawler uses to spend a `max_pages`-capped budget on the freshest URLs.
struct SitemapEntry {
    loc: String,
    lastmod: Option<String>,
}

/// Parses `<url>`/`<sitemap>` blocks, pulling each block's `<loc>` and optional
/// `<lastmod>`. Falls back to bare `<loc>` scanning for sitemaps without wrappers.
fn parse_sitemap_entries(xml: &str) -> Vec<SitemapEntry> {
    let block_re =
        regex::Regex::new(r"(?s)<(?:url|sitemap)\b[^>]*>(.*?)</(?:url|sitemap)>").expect("valid");
    let loc_re = regex::Regex::new(r"<loc>\s*([^<]+?)\s*</loc>").expect("valid");
    let lastmod_re = regex::Regex::new(r"<lastmod>\s*([^<]+?)\s*</lastmod>").expect("valid");
    let mut out = Vec::new();
    for block in block_re.captures_iter(xml) {
        let body = &block[1];
        if let Some(loc) = loc_re.captures(body) {
            out.push(SitemapEntry {
                loc: loc[1].replace("&amp;", "&"),
                lastmod: lastmod_re.captures(body).map(|c| c[1].trim().to_string()),
            });
        }
    }
    // Fallback: bare <loc> entries with no <url> wrapper.
    if out.is_empty() {
        for loc in loc_re.captures_iter(xml) {
            out.push(SitemapEntry {
                loc: loc[1].replace("&amp;", "&"),
                lastmod: None,
            });
        }
    }
    out
}

/// Seeds the frontier from a host's sitemaps (robots `Sitemap:` directives,
/// falling back to `/sitemap.xml`). Sitemap-index files are followed one level
/// deep. Returns how many URLs were pushed.
async fn seed_from_sitemaps(
    http: &Arc<dyn HttpClient>,
    origin: &str,
    declared: &[String],
    frontier: &mut Frontier,
    filter: &UrlFilter,
    budget: usize,
) -> usize {
    // The fallback probe uses the seed's own scheme/port, for the same reason
    // [`robots_url`] does: an http-only origin has no https sitemap either.
    let roots: Vec<String> = if declared.is_empty() {
        vec![format!("{origin}/sitemap.xml")]
    } else {
        declared
            .iter()
            .take(MAX_SITEMAPS_PER_HOST)
            .cloned()
            .collect()
    };
    // Collect all in-scope URL entries first, then push the freshest by `<lastmod>`
    // — a `max_pages`-capped crawl should spend its budget on URLs that changed most
    // recently. Mis-ordering is harmless (self-reported lastmod), so prioritization
    // is unconditional; `budget` still bounds how many land in the frontier.
    let mut entries: Vec<SitemapEntry> = Vec::new();
    for root in roots {
        let Ok(resp) = http.fetch(HttpRequest::get(&root)).await else {
            continue;
        };
        if !resp.is_success() {
            continue;
        }
        let parsed = parse_sitemap_entries(&resp.body);
        if resp.body.contains("<sitemapindex") {
            // A sitemap index lists further sitemaps; follow one level.
            for sm in parsed.into_iter().take(MAX_SITEMAPS_PER_HOST) {
                let Ok(resp) = http.fetch(HttpRequest::get(&sm.loc)).await else {
                    continue;
                };
                if resp.is_success() {
                    entries.extend(parse_sitemap_entries(&resp.body));
                }
            }
        } else {
            entries.extend(parsed);
        }
    }

    entries.retain(|e| filter.allows(&e.loc));
    // Newest `lastmod` first; entries without a lastmod sort last (unknown freshness).
    entries.sort_by(|a, b| b.lastmod.cmp(&a.lastmod));

    let mut pushed = 0;
    for entry in entries {
        if pushed >= budget {
            break;
        }
        frontier.push(canonicalize_str(&entry.loc), 0);
        pushed += 1;
    }
    pushed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::HttpResponse;
    use std::sync::Mutex as SyncMutex;

    #[test]
    fn artifact_name_is_url_addressed_stable_and_collision_free() {
        // Same URL always maps to the same file — no dependence on a per-run
        // counter, so a resumed crawl (stats.kept restarts at 0) can't overwrite a
        // prior run's page with a different URL's body.
        let a1 = artifact_name("https://example.com/a");
        let a2 = artifact_name("https://example.com/a");
        assert_eq!(a1, a2, "stable per URL");
        // Distinct URLs get distinct names.
        assert_ne!(a1, artifact_name("https://example.com/b"));
        // Filesystem-safe: page-<32 hex>.html, no path separators.
        assert!(a1.starts_with("page-") && a1.ends_with(".html"), "{a1}");
        assert!(!a1.contains('/') && !a1.contains('\\'), "{a1}");
        assert_eq!(a1.len(), "page-".len() + 32 + ".html".len());
    }

    /// Serves canned `(status, body)` per URL; URLs in `fail` return a transport
    /// error; unknown URLs → 404 empty. Honors conditional GETs: a request whose
    /// `If-None-Match` (`req.etag`) equals the URL's `etags` entry gets a bare
    /// `304`; `resp_etags` entries are echoed as an `ETag` header on 200s.
    #[derive(Default)]
    struct MockHttp {
        pages: HashMap<String, (u16, String)>,
        fail: HashSet<String>,
        /// Current server-side validator per URL — a matching `If-None-Match`
        /// yields 304.
        etags: HashMap<String, String>,
        /// `ETag` header value returned on a 200 (stored into the page record).
        resp_etags: HashMap<String, String>,
        /// Simulated per-fetch latency, so a test can put real (small) wall-clock
        /// duration on a run and cross a time-gated code path such as
        /// [`CHECKPOINT_MIN_INTERVAL`].
        delay: Option<std::time::Duration>,
    }

    #[async_trait]
    impl HttpClient for MockHttp {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            if let Some(d) = self.delay {
                tokio::time::sleep(d).await;
            }
            if self.fail.contains(&req.url) {
                return Err(crate::Error::App(format!(
                    "simulated transport failure: {}",
                    req.url
                )));
            }
            // Conditional GET: matching validator ⇒ 304 Not Modified, empty body.
            if let Some(sent) = &req.etag {
                if self.etags.get(&req.url) == Some(sent) {
                    return Ok(HttpResponse {
                        status: 304,
                        headers: HashMap::new(),
                        body: String::new(),
                        final_url: req.url,
                        cache_hit: false,
                    });
                }
            }
            let (status, body) = self
                .pages
                .get(&req.url)
                .cloned()
                .unwrap_or((404, String::new()));
            let mut headers = HashMap::new();
            if status == 200 {
                if let Some(tag) = self.resp_etags.get(&req.url) {
                    headers.insert("ETag".to_string(), tag.clone());
                }
            }
            Ok(HttpResponse {
                status,
                headers,
                body,
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    /// One thing the crawl did, in the order it did it. End-state assertions
    /// cannot see an ORDERING defect: the checkpoint-before-flush window is
    /// invisible unless a test can ask "at the moment THIS state was persisted,
    /// had the sink already been handed the pages it claims?".
    enum CrawlEvent {
        /// SimHashes of the records handed to the [`PageSink`] in one batch.
        Emitted(Vec<u64>),
        /// The raw state blob handed to the checkpoint seam.
        Saved(serde_json::Value),
    }

    type EventLog = Arc<SyncMutex<Vec<CrawlEvent>>>;

    /// A [`PageSink`] that accumulates every emitted record for assertions, and
    /// optionally appends to a shared ordered [`CrawlEvent`] trace.
    #[derive(Default)]
    struct CollectSink {
        records: Arc<SyncMutex<Vec<CrawlPageRecord>>>,
        log: Option<EventLog>,
    }

    #[async_trait]
    impl PageSink for CollectSink {
        async fn emit(&mut self, batch: Vec<CrawlPageRecord>) {
            if let Some(log) = &self.log {
                log.lock().unwrap().push(CrawlEvent::Emitted(
                    batch.iter().map(|r| r.simhash).collect(),
                ));
            }
            self.records.lock().unwrap().extend(batch);
        }
    }

    /// Minimal config for tests: robots + sitemaps off, single-threaded, no dedup.
    fn test_cfg(seeds: &[&str]) -> CrawlConfig {
        CrawlConfig {
            seeds: seeds.iter().map(|s| s.to_string()).collect(),
            max_pages: 50,
            max_depth: 3,
            concurrency: 4,
            max_pages_per_host: None,
            same_domain: true,
            dedup_distance: 0,
            respect_robots: false,
            include_patterns: vec![],
            exclude_patterns: vec![],
            sitemap_seeds: false,
            resume_state: None,
            revisit: false,
            discover: false,
            revisit_budget: None,
            min_due_score: 0.0,
        }
    }

    #[tokio::test]
    async fn crawl_streams_kept_pages_to_sink() {
        let mut pages = HashMap::new();
        pages.insert(
            "https://ex.com/".to_string(),
            (
                200,
                "<html><head><title>Home</title></head><body><h1>Hi</h1>\
                   <a href=\"/about\">about</a></body></html>"
                    .to_string(),
            ),
        );
        pages.insert(
            "https://ex.com/about".to_string(),
            (
                200,
                "<html><head><title>About</title></head><body>\
                   <p>About us page content.</p></body></html>"
                    .to_string(),
            ),
        );
        let http = Arc::new(MockHttp {
            pages,
            ..Default::default()
        });
        let records = Arc::new(SyncMutex::new(Vec::new()));
        let sink = Box::new(CollectSink {
            records: records.clone(),
            log: None,
        });

        let stats = crawl(
            http,
            test_cfg(&["https://ex.com/"]),
            None,
            Some(sink),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(stats.kept, 2, "both distinct pages kept");
        let recs = records.lock().unwrap();
        assert_eq!(
            recs.len(),
            2,
            "each kept page streamed to the sink exactly once"
        );
        let home = recs.iter().find(|r| r.url == "https://ex.com/").unwrap();
        assert_eq!(home.title.as_deref(), Some("Home"));
        assert_eq!(home.status, 200);
        assert!(home.content_chars > 0);
        assert_ne!(home.simhash, 0, "body simhash recorded");
        assert!(recs
            .iter()
            .any(|r| r.url == "https://ex.com/about" && r.title.as_deref() == Some("About")));
    }

    #[tokio::test]
    async fn crawl_reports_progress_snapshots() {
        let mut pages = HashMap::new();
        pages.insert(
            "https://ex.com/".to_string(),
            (
                200,
                "<html><body><a href=\"/a\">a</a></body></html>".to_string(),
            ),
        );
        pages.insert(
            "https://ex.com/a".to_string(),
            (
                200,
                "<html><body><p>distinct content</p></body></html>".to_string(),
            ),
        );
        let http = Arc::new(MockHttp {
            pages,
            ..Default::default()
        });
        let seen: Arc<SyncMutex<Vec<CrawlProgressSnapshot>>> = Arc::new(SyncMutex::new(Vec::new()));
        let sink_seen = seen.clone();
        let progress: ProgressFn =
            Arc::new(move |snap| sink_seen.lock().unwrap().push(snap.clone()));

        let stats = crawl(
            http,
            test_cfg(&["https://ex.com/"]),
            None,
            None,
            None,
            Some(progress),
            None,
        )
        .await
        .unwrap();

        let snaps = seen.lock().unwrap();
        assert!(
            !snaps.is_empty(),
            "at least the final progress snapshot is emitted"
        );
        let last = snaps.last().unwrap();
        assert_eq!(
            last.crawled, stats.crawled,
            "final snapshot mirrors end stats"
        );
        assert_eq!(last.kept, stats.kept);
        assert_eq!(last.hosts, stats.hosts);
    }

    /// A [`PageSource`] that hands back a fixed seed list.
    struct SeedSource(Vec<RevisitSeed>);

    #[async_trait]
    impl PageSource for SeedSource {
        async fn seeds(&self) -> Vec<RevisitSeed> {
            self.0.clone()
        }
    }

    #[tokio::test]
    async fn revisit_counts_unchanged_changed_and_gone() {
        // Three known pages: one 304-unchanged, one changed (200 + new body/etag),
        // one gone (404). Revisit does NOT follow links (discover off).
        let mut pages = HashMap::new();
        pages.insert(
            "https://ex.com/changed".to_string(),
            (
                200,
                "<html><body><p>brand new content this run</p></body></html>".to_string(),
            ),
        );
        // /stable is 304 (validator matches); /gone is unknown → 404.
        let mut etags = HashMap::new();
        etags.insert("https://ex.com/stable".to_string(), "v1".to_string());
        let mut resp_etags = HashMap::new();
        resp_etags.insert("https://ex.com/changed".to_string(), "new-tag".to_string());
        let http = Arc::new(MockHttp {
            pages,
            etags,
            resp_etags,
            ..Default::default()
        });

        let source = Box::new(SeedSource(vec![
            RevisitSeed::bare("https://ex.com/stable", Some("v1".into()), None),
            RevisitSeed::bare("https://ex.com/changed", Some("stale".into()), None),
            RevisitSeed::bare("https://ex.com/gone", None, None),
        ]));

        let records = Arc::new(SyncMutex::new(Vec::new()));
        let sink = Box::new(CollectSink {
            records: records.clone(),
            log: None,
        });

        let mut cfg = test_cfg(&[]);
        cfg.revisit = true;

        let stats = crawl(http, cfg, None, Some(sink), Some(source), None, None)
            .await
            .unwrap();

        assert_eq!(stats.revisited, 3, "all three known pages revisited");
        assert_eq!(
            stats.unchanged_304, 1,
            "the matching-validator page is a cheap 304"
        );
        assert_eq!(stats.gone, 1, "the 404 page is flagged gone");
        assert_eq!(
            stats.kept, 1,
            "only the changed page is re-fingerprinted/kept"
        );
        assert_eq!(stats.crawled, 1, "only the 200 counts as crawled");

        let recs = records.lock().unwrap();
        let live: Vec<_> = recs.iter().filter(|r| !r.gone && !r.unchanged).collect();
        let gone: Vec<_> = recs.iter().filter(|r| r.gone).collect();
        let checks: Vec<_> = recs.iter().filter(|r| r.unchanged).collect();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].url, "https://ex.com/changed");
        assert_eq!(
            live[0].etag.as_deref(),
            Some("new-tag"),
            "response ETag stored"
        );
        // The changed page carries updated cadence counters (weight 1.0: no
        // stored fingerprint means the change can't be discounted).
        let cad = live[0].cadence.as_ref().expect("cadence attached");
        assert_eq!(cad.checks, 1);
        assert!((cad.changes - 1.0).abs() < 1e-9);
        assert_eq!(gone.len(), 1);
        assert_eq!(gone[0].url, "https://ex.com/gone");
        assert_eq!(gone[0].status, 404);
        // The 304 emitted a check-only marker with bumped counters.
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].url, "https://ex.com/stable");
        let cad = checks[0].cadence.as_ref().expect("cadence on marker");
        assert_eq!(cad.checks, 1);
        assert_eq!(cad.changes, 0.0, "a 304 is not a change");
    }

    #[tokio::test]
    async fn revisit_does_not_follow_links_without_discover() {
        // A known page links to a NEW url; without discover the frontier must not
        // expand to it.
        let mut pages = HashMap::new();
        pages.insert(
            "https://ex.com/hub".to_string(),
            (
                200,
                "<html><body><a href=\"/newly-linked\">new</a></body></html>".to_string(),
            ),
        );
        pages.insert(
            "https://ex.com/newly-linked".to_string(),
            (
                200,
                "<html><body><p>should not be crawled</p></body></html>".to_string(),
            ),
        );
        let http = Arc::new(MockHttp {
            pages,
            ..Default::default()
        });
        let source = Box::new(SeedSource(vec![RevisitSeed::bare(
            "https://ex.com/hub",
            None,
            None,
        )]));
        let mut cfg = test_cfg(&[]);
        cfg.revisit = true; // discover stays false

        let stats = crawl(http, cfg, None, None, Some(source), None, None)
            .await
            .unwrap();
        assert_eq!(
            stats.crawled, 1,
            "only the seeded hub is fetched; no link-following"
        );
        assert_eq!(stats.revisited, 1);
    }

    #[tokio::test]
    async fn crawl_counts_failures_and_botwalls() {
        // Seed links to four children: one good, one transport failure, one 403
        // block, one 200 Cloudflare challenge page.
        let seed = "<html><body>\
            <a href=\"/ok\">ok</a><a href=\"/dead\">dead</a>\
            <a href=\"/blocked\">blocked</a><a href=\"/cf\">cf</a></body></html>";
        let mut pages = HashMap::new();
        pages.insert("https://ex.com/".to_string(), (200, seed.to_string()));
        pages.insert(
            "https://ex.com/ok".to_string(),
            (
                200,
                "<html><body><p>real content here</p></body></html>".to_string(),
            ),
        );
        // 403 hard block.
        pages.insert(
            "https://ex.com/blocked".to_string(),
            (403, "denied".to_string()),
        );
        // 200 with a Cloudflare interstitial marker — must classify as bot-wall.
        pages.insert(
            "https://ex.com/cf".to_string(),
            (
                200,
                "<html><head><title>Just a moment...</title></head><body>\
                   <div class=\"cf-browser-verification\">Checking your browser\
                   </div></body></html>"
                    .to_string(),
            ),
        );
        let mut fail = HashSet::new();
        fail.insert("https://ex.com/dead".to_string());

        let http = Arc::new(MockHttp {
            pages,
            fail,
            ..Default::default()
        });
        let stats = crawl(
            http,
            test_cfg(&["https://ex.com/"]),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // Kept: seed + /ok. /dead failed, /blocked + /cf are bot-walls.
        assert_eq!(stats.kept, 2, "only real-content pages kept");
        assert_eq!(stats.crawled, 2, "crawled counts only real responses");
        assert_eq!(stats.failed, 1, "transport failure counted, not swallowed");
        assert_eq!(stats.failed_by_host.get("ex.com").copied(), Some(1));
        assert_eq!(
            stats.skipped_botwall, 2,
            "403 block + CF challenge both bot-walls"
        );
    }

    #[test]
    fn simhash_index_matches_linear_scan() {
        // A cheap deterministic PRNG (xorshift) so the fixture is reproducible
        // without a rand dependency.
        let mut state: u64 = 0x9e3779b97f4a7c15;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        // Cover several distances, incl. 0 (exact) and a wide band.
        for &distance in &[0u32, 1, 3, 5, 12, 20] {
            let mut linear: Vec<u64> = Vec::new();
            let mut index = SimHashIndex::new(distance);
            for _ in 0..600 {
                // Mix fully-random hashes with near-neighbours of existing kept
                // ones (flip a few bits) so near-dups actually occur.
                let base = if !linear.is_empty() && next() % 2 == 0 {
                    let pick = linear[(next() as usize) % linear.len()];
                    let flips = (next() % (distance as u64 + 3)) as u32;
                    let mut h = pick;
                    for _ in 0..flips {
                        h ^= 1u64 << (next() % 64);
                    }
                    h
                } else {
                    next()
                };

                let linear_dup =
                    distance > 0 && linear.iter().any(|&h| hamming(h, base) <= distance);
                let index_dup = distance > 0 && index.is_near_dup(base);
                assert_eq!(
                    linear_dup, index_dup,
                    "distance {distance}: banded index disagreed with linear scan on {base:#x}"
                );
                // Mirror the crawl's keep policy in both structures.
                if !linear_dup {
                    linear.push(base);
                    index.insert(base);
                }
            }
            assert_eq!(index.hashes().len(), linear.len());
        }
    }

    #[test]
    fn simhash_index_from_hashes_roundtrips() {
        let hashes = vec![0x1u64, 0xFFu64, 0xDEAD_BEEFu64];
        let index = SimHashIndex::from_hashes(3, hashes.clone());
        assert_eq!(index.hashes(), hashes.as_slice());
        // Exact members are trivially within distance 3.
        assert!(index.is_near_dup(0x1));
        // A bit-flip within distance is caught; far-away is not.
        assert!(index.is_near_dup(0x1 ^ 0b110));
        assert!(!index.is_near_dup(!0u64));
    }

    /// Collects every `(state, force)` pair handed to the checkpoint seam, so
    /// tests can assert both on what the crawl persists AND on whether a save was
    /// an intermediate one (`force == false`) or the final snapshot.
    #[derive(Default)]
    struct CollectCheckpoints {
        saves: Arc<SyncMutex<Vec<(serde_json::Value, bool)>>>,
        log: Option<EventLog>,
    }

    #[async_trait]
    impl crate::app::CheckpointSink for CollectCheckpoints {
        async fn save(&self, state: serde_json::Value, force: bool) -> bool {
            if let Some(log) = &self.log {
                log.lock().unwrap().push(CrawlEvent::Saved(state.clone()));
            }
            self.saves.lock().unwrap().push((state, force));
            true
        }
    }

    /// The crawl-driving fixture all the resume/dedup/politeness tests share: a
    /// [`MockHttp`] site, a collecting [`PageSink`] and a collecting checkpoint
    /// sink, so a test can assert on the RECORDS a run emitted (not just its
    /// counters) and on the state it persisted. The bug class these directions
    /// close — pages that are counted but never handed to the sink — is invisible
    /// to any test that drives `crawl()` with `sink: None`, which is what every
    /// resume test used to do.
    struct CrawlHarness {
        records: Arc<SyncMutex<Vec<CrawlPageRecord>>>,
        checkpoints: Arc<SyncMutex<Vec<(serde_json::Value, bool)>>>,
        /// Interleaved sink-emit / checkpoint-save trace, in happened-before order.
        log: EventLog,
    }

    impl CrawlHarness {
        fn new() -> Self {
            Self {
                records: Arc::new(SyncMutex::new(Vec::new())),
                checkpoints: Arc::new(SyncMutex::new(Vec::new())),
                log: Arc::new(SyncMutex::new(Vec::new())),
            }
        }

        fn sink(&self) -> Box<dyn PageSink> {
            Box::new(CollectSink {
                records: self.records.clone(),
                log: Some(self.log.clone()),
            })
        }

        fn checkpointer(&self) -> Arc<dyn crate::app::CheckpointSink> {
            Arc::new(CollectCheckpoints {
                saves: self.checkpoints.clone(),
                log: Some(self.log.clone()),
            })
        }

        /// URLs of every record handed to the sink so far, in emit order.
        fn record_urls(&self) -> Vec<String> {
            self.records
                .lock()
                .unwrap()
                .iter()
                .map(|r| r.url.clone())
                .collect()
        }

        /// The last state the crawl persisted — the blob a resume is handed.
        fn last_state(&self) -> serde_json::Value {
            self.checkpoints
                .lock()
                .unwrap()
                .last()
                .map(|(state, _)| state.clone())
                .expect("at least the final checkpoint was saved")
        }

        /// True when at least one save was an INTERMEDIATE (non-forced) one, i.e.
        /// the run was resumable before it ended.
        fn saved_mid_run(&self) -> bool {
            self.checkpoints
                .lock()
                .unwrap()
                .iter()
                .any(|(_, force)| !*force)
        }
    }

    /// A site of `n` distinct child pages hanging off `https://ex.com/`.
    fn linked_site(n: usize) -> HashMap<String, (u16, String)> {
        let mut pages = HashMap::new();
        let links: String = (0..n)
            .map(|i| format!("<a href=\"/p{i}\">p{i}</a>"))
            .collect();
        pages.insert(
            "https://ex.com/".to_string(),
            (200, format!("<html><body>{links}</body></html>")),
        );
        for i in 0..n {
            pages.insert(
                format!("https://ex.com/p{i}"),
                (
                    200,
                    format!(
                        "<html><body><p>the unique body of child page number {i}</p></body></html>"
                    ),
                ),
            );
        }
        pages
    }

    #[tokio::test]
    async fn checkpoint_version_mismatch_forces_fresh_start() {
        // Null restored state is a fresh start, not a reset.
        assert!(matches!(
            Checkpoint::from_value(&serde_json::Value::Null),
            CheckpointLoad::None
        ));

        // Pre-versioning state (no `version` field) parses as version 0 and is
        // rejected as incompatible rather than resumed silently-wrong.
        let old: serde_json::Value = serde_json::from_str(
            r#"{"queue":[["https://x.com/",0]],"seen":["https://x.com/"],"kept_hashes":[1,2]}"#,
        )
        .unwrap();
        assert!(matches!(
            Checkpoint::from_value(&old),
            CheckpointLoad::Incompatible
        ));

        // A current-version checkpoint round-trips through the sink seam.
        let mut frontier = Frontier::new(None);
        frontier.push("https://x.com/".into(), 0);
        let saved = Arc::new(SyncMutex::new(Vec::new()));
        let sink: Arc<dyn crate::app::CheckpointSink> = Arc::new(CollectCheckpoints {
            saves: saved.clone(),
            log: None,
        });
        assert!(save_checkpoint(&sink, &frontier, &HashMap::new(), &[7u64], true).await);
        let (state, forced) = saved.lock().unwrap().pop().expect("one checkpoint saved");
        assert!(forced, "the explicit save is a forced one");
        match Checkpoint::from_value(&state) {
            CheckpointLoad::Loaded(cp) => {
                assert_eq!(cp.version, CHECKPOINT_VERSION);
                assert_eq!(cp.kept_hashes, vec![7]);
                assert_eq!(cp.queue, vec![("https://x.com/".to_string(), 0)]);
            }
            _ => panic!("expected a compatible checkpoint to load"),
        }
    }

    #[tokio::test]
    async fn crawl_persists_and_resumes_frontier_state() {
        // Run 1: a page-capped crawl leaves work in the frontier and hands its
        // end state to the checkpoint sink.
        let mut pages = HashMap::new();
        pages.insert(
            "https://ex.com/".to_string(),
            (
                200,
                "<html><body><a href=\"/a\">a</a><a href=\"/b\">b</a></body></html>".to_string(),
            ),
        );
        pages.insert(
            "https://ex.com/a".to_string(),
            (
                200,
                "<html><body><p>page a body</p></body></html>".to_string(),
            ),
        );
        pages.insert(
            "https://ex.com/b".to_string(),
            (
                200,
                "<html><body><p>page b body</p></body></html>".to_string(),
            ),
        );
        let http = Arc::new(MockHttp {
            pages,
            ..Default::default()
        });
        let mut cfg = test_cfg(&["https://ex.com/"]);
        cfg.max_pages = 1;
        cfg.concurrency = 1;
        let harness = CrawlHarness::new();
        let stats = crawl(
            http.clone(),
            cfg,
            None,
            None,
            None,
            None,
            Some(harness.checkpointer()),
        )
        .await
        .unwrap();
        assert_eq!(stats.kept, 1);
        assert!(!stats.resumed);
        let state = harness.last_state();

        // Run 2: restoring that state resumes (seen-set intact — the seed is not
        // re-enqueued) and reports `resumed`.
        let mut cfg = test_cfg(&["https://ex.com/"]);
        cfg.concurrency = 1;
        cfg.resume_state = Some(state);
        let stats = crawl(http, cfg, None, None, None, None, None)
            .await
            .unwrap();
        assert!(stats.resumed, "restored state marks the run resumed");
        assert!(!stats.checkpoint_reset);
        assert_eq!(
            stats.kept, 2,
            "resume crawls only the remaining frontier, not the already-seen seed"
        );
    }

    #[test]
    fn checkpoint_queue_merges_in_flight_not_only_the_queue() {
        // `pop` takes a URL out of the queue AND into `seen` in one step, so a
        // checkpoint built from `queued()` alone writes outstanding fetches as
        // seen-but-not-queued — unreachable on every future resume.
        let mut frontier = Frontier::new(None);
        frontier.push("https://ex.com/queued".into(), 0);
        frontier.push("https://ex.com/flying".into(), 2);
        let (flying, depth) = {
            // Pop until we get the URL we want to simulate as in flight.
            let mut popped = frontier.pop().unwrap();
            if popped.0 != "https://ex.com/flying" {
                frontier.push("https://ex.com/other".into(), 0); // keep the queue non-empty
                popped = frontier.pop().unwrap();
            }
            popped
        };
        let mut in_flight = HashMap::new();
        in_flight.insert(flying.clone(), depth);

        let queue = checkpoint_queue(&frontier, &in_flight);
        assert!(
            queue.iter().any(|(u, d)| *u == flying && *d == depth),
            "the in-flight URL (and its depth) is persisted: {queue:?}"
        );
        assert_eq!(
            queue.len(),
            frontier.queued().len() + 1,
            "exactly the in-flight set is added, no duplicates"
        );
    }

    #[tokio::test]
    async fn a_page_capped_crawl_does_not_bury_the_urls_it_had_in_flight() {
        // The `max_pages` break abandons up to `concurrency - 1` outstanding
        // fetches. Those URLs are already in `seen`, so unless the checkpoint
        // hands them back the resume can NEVER reach them — the incremental
        // "max_pages: 50, run it five times" sweep silently loses coverage.
        //
        // AC5's assertion: records emitted before the stop ∪ records emitted
        // after the resume == every page of the site.
        let http = Arc::new(MockHttp {
            pages: linked_site(8),
            ..Default::default()
        });

        // Run 1: stop at 3 kept pages with 4 concurrent fetches in flight.
        let run1 = CrawlHarness::new();
        let mut cfg = test_cfg(&["https://ex.com/"]);
        cfg.max_pages = 3;
        cfg.concurrency = 4;
        let stats1 = crawl(
            http.clone(),
            cfg,
            None,
            Some(run1.sink()),
            None,
            None,
            Some(run1.checkpointer()),
        )
        .await
        .unwrap();
        assert_eq!(stats1.kept, 3, "the page cap stopped the run");
        assert_eq!(
            run1.record_urls().len(),
            stats1.kept,
            "every kept page reached the sink before the run ended"
        );

        // Run 2: resume from the persisted state and drain the rest.
        let run2 = CrawlHarness::new();
        let mut cfg = test_cfg(&["https://ex.com/"]);
        cfg.concurrency = 4;
        cfg.resume_state = Some(run1.last_state());
        let stats2 = crawl(
            http,
            cfg,
            None,
            Some(run2.sink()),
            None,
            None,
            Some(run2.checkpointer()),
        )
        .await
        .unwrap();
        assert!(stats2.resumed);

        let mut seen: Vec<String> = run1.record_urls();
        seen.extend(run2.record_urls());
        seen.sort();
        seen.dedup();
        let mut expected: Vec<String> = std::iter::once("https://ex.com/".to_string())
            .chain((0..8).map(|i| format!("https://ex.com/p{i}")))
            .collect();
        expected.sort();
        assert_eq!(
            seen, expected,
            "no page is fetched-then-buried: the two runs' records cover the whole site"
        );
    }

    #[tokio::test]
    async fn a_mostly_304_run_is_not_left_without_an_intermediate_checkpoint() {
        // The interval save used to live inside the kept-page branch, below the
        // `continue`s for Failed / BotWall / NotModified / Gone / duplicate. A
        // revisit sweep whose outcomes are all `304` therefore produced ZERO
        // intermediate checkpoints: killed at 95% it lost 95% of its progress.
        //
        // A small per-fetch delay puts real wall-clock on the run so it crosses
        // the (test-shortened) CHECKPOINT_MIN_INTERVAL several times.
        let n = 80usize;
        let urls: Vec<String> = (0..n).map(|i| format!("https://ex.com/k{i}")).collect();
        let etags: HashMap<String, String> =
            urls.iter().map(|u| (u.clone(), "v1".to_string())).collect();
        let http = Arc::new(MockHttp {
            etags,
            delay: Some(std::time::Duration::from_millis(5)),
            ..Default::default()
        });
        let source = Box::new(SeedSource(
            urls.iter()
                .map(|u| RevisitSeed::bare(u.clone(), Some("v1".into()), None))
                .collect(),
        ));

        let harness = CrawlHarness::new();
        let mut cfg = test_cfg(&[]);
        cfg.revisit = true;
        cfg.concurrency = 4;
        let stats = crawl(
            http,
            cfg,
            None,
            Some(harness.sink()),
            Some(source),
            None,
            Some(harness.checkpointer()),
        )
        .await
        .unwrap();

        assert_eq!(stats.unchanged_304, n, "every known page answered 304");
        assert_eq!(stats.kept, 0, "no body was downloaded");
        assert!(
            harness.saved_mid_run(),
            "a long run of non-kept outcomes still checkpoints mid-run"
        );
        let intermediate = harness
            .checkpoints
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, force)| !*force)
            .count();
        assert!(
            intermediate >= 2,
            "checkpoints keep firing THROUGHOUT a non-kept run, not once: {intermediate}"
        );
        // Marker honesty: the counter can't exceed the rows the sink was handed.
        assert_eq!(
            harness.record_urls().len(),
            stats.unchanged_304,
            "every counted 304 produced a cadence marker in the sink"
        );
    }

    #[tokio::test]
    async fn a_checkpoint_is_not_saved_before_its_pages_reach_the_sink() {
        // Kept pages reach the dataset only every PAGE_SINK_STRIDE records, while
        // the checkpoint fires on a wall clock and serializes `seen` + the kept
        // fingerprints — both of which ALREADY contain the still-buffered page. A
        // kill in that window orphaned the body on disk (no record points at it),
        // marked the URL seen so no resume ever refetched it, and left its
        // fingerprint suppressing near-dups of a page the dataset never got.
        //
        // The invariant, checked at every save in happened-before order: the
        // checkpoint never claims a page the sink has not been handed.
        let http = Arc::new(MockHttp {
            pages: linked_site(40),
            delay: Some(std::time::Duration::from_millis(5)),
            ..Default::default()
        });
        let harness = CrawlHarness::new();
        let mut cfg = test_cfg(&["https://ex.com/"]);
        cfg.concurrency = 4;
        let stats = crawl(
            http,
            cfg,
            None,
            Some(harness.sink()),
            None,
            None,
            Some(harness.checkpointer()),
        )
        .await
        .unwrap();
        assert_eq!(stats.kept, 41, "seed + 40 children, all distinct");
        // The whole run stays under PAGE_SINK_STRIDE, so nothing flushes on its
        // own — every flush that happens is one a checkpoint forced.
        assert!(stats.kept < PAGE_SINK_STRIDE);

        let mut handed: HashSet<u64> = HashSet::new();
        let mut saves = 0usize;
        for event in harness.log.lock().unwrap().iter() {
            match event {
                CrawlEvent::Emitted(hashes) => handed.extend(hashes),
                CrawlEvent::Saved(state) => {
                    saves += 1;
                    let CheckpointLoad::Loaded(cp) = Checkpoint::from_value(state) else {
                        panic!("every persisted state must load");
                    };
                    for h in &cp.kept_hashes {
                        assert!(
                            handed.contains(h),
                            "checkpoint #{saves} claims fingerprint {h:#x}, \
                             but the sink had not been handed that page yet"
                        );
                    }
                }
            }
        }
        assert!(
            harness.saved_mid_run(),
            "the assertion above must cover at least one INTERMEDIATE save"
        );
        assert_eq!(
            harness.records.lock().unwrap().len(),
            stats.kept,
            "and every kept page ends up in the sink exactly once"
        );
    }

    #[test]
    fn dedup_gate_is_skipped_for_known_pages_not_for_new_ones() {
        // Fresh crawl: the gate is the whole point of `dedup_distance`.
        assert!(dedup_applies(3, false));
        // Revisit of a KNOWN page: never gated against a sibling's body.
        assert!(!dedup_applies(3, true));
        // `dedup_distance: 0` still disables it everywhere.
        assert!(!dedup_applies(0, false));
        assert!(!dedup_applies(0, true));
    }

    /// Two URLs serving byte-identical bodies (SimHash distance 0, so the near-dup
    /// verdict is deterministic at any `dedup_distance > 0`), plus their fresh
    /// `ETag`s.
    fn twin_pages() -> MockHttp {
        let twin = "<html><head><title>Product</title></head><body>\
            <p>the same templated body served at two different urls</p></body></html>";
        let mut pages = HashMap::new();
        let mut resp_etags = HashMap::new();
        for path in ["one", "two"] {
            pages.insert(format!("https://ex.com/{path}"), (200, twin.to_string()));
            resp_etags.insert(format!("https://ex.com/{path}"), format!("fresh-{path}"));
        }
        MockHttp {
            pages,
            resp_etags,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_revisited_page_is_not_dropped_as_a_near_dup_of_its_sibling() {
        // Both known pages share a template. Under the old unconditional gate the
        // second one counted as `revisited`, then returned before the sink: fresh
        // validators discarded, cadence frozen, record never updated — so the next
        // run re-downloaded it and dropped it again, forever.
        let http = Arc::new(twin_pages());
        let source = Box::new(SeedSource(vec![
            RevisitSeed::bare("https://ex.com/one", Some("stale-one".into()), None),
            RevisitSeed::bare("https://ex.com/two", Some("stale-two".into()), None),
        ]));
        let harness = CrawlHarness::new();
        let mut cfg = test_cfg(&[]);
        cfg.revisit = true;
        cfg.dedup_distance = 3; // the app's default, in every mode

        let stats = crawl(
            http,
            cfg,
            None,
            Some(harness.sink()),
            Some(source),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(stats.revisited, 2);
        assert_eq!(
            stats.skipped_duplicates, 0,
            "a known page is never a duplicate OF ANOTHER PAGE"
        );
        assert_eq!(stats.kept, 2, "both records are refreshed");

        let records = harness.records.lock().unwrap();
        for path in ["one", "two"] {
            let url = format!("https://ex.com/{path}");
            let rec = records
                .iter()
                .find(|r| r.url == url)
                .unwrap_or_else(|| panic!("{url} reached the sink"));
            assert_eq!(
                rec.etag.as_deref(),
                Some(format!("fresh-{path}").as_str()),
                "the fresh validator is stored, so the next run can revalidate"
            );
            let cadence = rec.cadence.as_ref().expect("cadence advanced");
            assert_eq!(cadence.checks, 1, "the check was observed");
        }
    }

    #[tokio::test]
    async fn a_fresh_crawl_still_drops_near_duplicate_new_pages() {
        // The guard for the fix above: removing the gate outright (rather than
        // scoping it to non-known pages) would keep all three pages here.
        let mut mock = twin_pages();
        mock.pages.insert(
            "https://ex.com/".to_string(),
            (
                200,
                "<html><head><title>Index</title></head><body><h1>Catalogue index</h1>\
                 <a href=\"/one\">one</a><a href=\"/two\">two</a></body></html>"
                    .to_string(),
            ),
        );
        let http = Arc::new(mock);
        let harness = CrawlHarness::new();
        let mut cfg = test_cfg(&["https://ex.com/"]);
        cfg.dedup_distance = 3;

        let stats = crawl(http, cfg, None, Some(harness.sink()), None, None, None)
            .await
            .unwrap();

        assert_eq!(stats.crawled, 3, "all three pages were fetched");
        assert_eq!(
            stats.skipped_duplicates, 1,
            "the second copy of the templated body is still suppressed"
        );
        assert_eq!(stats.kept, 2, "index + one copy");
        assert_eq!(harness.records.lock().unwrap().len(), 2);
    }

    #[test]
    fn frontier_round_robins_across_hosts() {
        // Two hosts, host A pushed first with 3 URLs, then host B with 2. A FIFO
        // would drain all of A before B; the round-robin interleaves them.
        let mut f = Frontier::new(None);
        for i in 0..3 {
            f.push(format!("https://a.com/{i}"), 0);
        }
        for i in 0..2 {
            f.push(format!("https://b.com/{i}"), 0);
        }
        let mut hosts_in_order = Vec::new();
        while let Some((url, _)) = f.pop() {
            hosts_in_order.push(host_of(&url).unwrap());
        }
        // First two pops alternate hosts (A, B), proving no single-host monopoly.
        assert_eq!(&hosts_in_order[0], "a.com");
        assert_eq!(&hosts_in_order[1], "b.com");
        assert_eq!(hosts_in_order.len(), 5);
    }

    #[test]
    fn frontier_enforces_per_host_budget_and_reports_it() {
        // Host A has 5 URLs but a per-host cap of 2 — only 2 come out, the rest
        // are counted as budget-skipped. Host B (under cap) is unaffected.
        let mut f = Frontier::new(Some(2));
        for i in 0..5 {
            f.push(format!("https://a.com/{i}"), 0);
        }
        f.push("https://b.com/x".into(), 0);
        let mut a = 0;
        let mut b = 0;
        while let Some((url, _)) = f.pop() {
            match host_of(&url).unwrap().as_str() {
                "a.com" => a += 1,
                "b.com" => b += 1,
                _ => {}
            }
        }
        assert_eq!(a, 2, "host A capped at 2");
        assert_eq!(b, 1, "host B under cap, unaffected");
        assert_eq!(
            f.skipped_host_budget(),
            3,
            "the 3 over-budget A URLs are reported"
        );
    }

    #[test]
    fn frontier_requeue_refunds_host_budget() {
        // A crawl-delay requeue must not burn budget: pop then requeue, and the
        // URL is still reachable under a cap of 1.
        let mut f = Frontier::new(Some(1));
        f.push("https://a.com/1".into(), 0);
        let (url, depth) = f.pop().unwrap();
        f.requeue(url, depth);
        assert!(
            f.pop().is_some(),
            "requeue refunded the budget so the URL pops again"
        );
    }

    #[test]
    fn top_n_by_count_keeps_worst_offenders() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 5);
        map.insert("c".to_string(), 3);
        let top = top_n_by_count(map, 2);
        assert_eq!(top.len(), 2);
        assert_eq!(top.get("b").copied(), Some(5));
        assert_eq!(top.get("c").copied(), Some(3));
        assert!(!top.contains_key("a"), "smallest dropped");
    }

    #[test]
    fn robots_is_probed_over_the_pages_own_scheme_not_always_https() {
        // The scheme the crawl is USING decides where robots.txt lives.
        assert_eq!(
            robots_url("http://ex.com/a/b").as_deref(),
            Some("http://ex.com/robots.txt")
        );
        assert_eq!(
            robots_url("https://ex.com/a/b").as_deref(),
            Some("https://ex.com/robots.txt")
        );
        // A non-default port is part of the origin: :8080 has its own robots.txt.
        assert_eq!(
            robots_url("http://ex.com:8080/x").as_deref(),
            Some("http://ex.com:8080/robots.txt")
        );
        // `Url` normalizes default ports, so both spellings are one origin...
        assert_eq!(
            robots_url("https://ex.com:443/x"),
            robots_url("https://ex.com/x")
        );
        // ...while the two schemes are DIFFERENT origins, i.e. different cache
        // keys — one host can serve different rules (or nothing) on each.
        assert_ne!(origin_of("http://ex.com/x"), origin_of("https://ex.com/x"));
        // Nothing to probe for a non-http(s) or unparseable URL.
        assert_eq!(robots_url("ftp://ex.com/x"), None);
        assert_eq!(robots_url("not a url"), None);
    }

    #[tokio::test]
    async fn an_http_only_host_is_not_crawled_under_a_failed_open_robots_assumption_silently() {
        // The seed is http://, but robots was probed at https:// regardless. For
        // an http-only origin that fails at the transport layer, which fails open
        // to allow-all — so the crawl walked every `Disallow:` path while
        // reporting `respect_robots: true` and `skipped_robots: 0`.
        let mut pages = HashMap::new();
        pages.insert(
            "http://ex.com/".to_string(),
            (
                200,
                "<html><body><a href=\"/admin/secret\">a</a><a href=\"/pub\">b</a></body></html>"
                    .to_string(),
            ),
        );
        pages.insert(
            "http://ex.com/admin/secret".to_string(),
            (
                200,
                "<html><body><p>private area</p></body></html>".to_string(),
            ),
        );
        pages.insert(
            "http://ex.com/pub".to_string(),
            (
                200,
                "<html><body><p>public area</p></body></html>".to_string(),
            ),
        );
        pages.insert(
            "http://ex.com/robots.txt".to_string(),
            (200, "User-agent: *\nDisallow: /admin\n".to_string()),
        );
        // https:// is simply not reachable for this origin.
        let mut fail = HashSet::new();
        fail.insert("https://ex.com/robots.txt".to_string());
        let http = Arc::new(MockHttp {
            pages,
            fail,
            ..Default::default()
        });

        let mut cfg = test_cfg(&["http://ex.com/"]);
        cfg.respect_robots = true;
        let stats = crawl(http, cfg, None, None, None, None, None)
            .await
            .unwrap();

        assert_eq!(
            stats.skipped_robots, 1,
            "the Disallow: /admin rule was actually read and obeyed"
        );
        assert_eq!(stats.kept, 2, "index + /pub, never /admin/secret");
        assert_eq!(
            stats.robots_fetch_failures, 0,
            "nothing failed open — the rules were verified over http"
        );
        assert!(
            stats.robots_unverified_hosts.is_empty(),
            "a verified host is not reported as unverified: {:?}",
            stats.robots_unverified_hosts
        );
    }

    #[tokio::test]
    async fn a_failed_open_robots_fetch_is_named_not_just_counted() {
        // Failing open on a transport failure is correct and stays. What must not
        // survive is the run LOOKING compliant afterwards.
        let mut pages = HashMap::new();
        pages.insert(
            "http://ex.com/".to_string(),
            (
                200,
                "<html><body><p>only page</p></body></html>".to_string(),
            ),
        );
        let mut fail = HashSet::new();
        fail.insert("http://ex.com/robots.txt".to_string());
        let http = Arc::new(MockHttp {
            pages,
            fail,
            ..Default::default()
        });
        let mut cfg = test_cfg(&["http://ex.com/"]);
        cfg.respect_robots = true;
        let stats = crawl(http, cfg, None, None, None, None, None)
            .await
            .unwrap();

        assert_eq!(stats.kept, 1, "fail-open still allows the crawl");
        assert_eq!(stats.skipped_robots, 0);
        assert_eq!(stats.robots_fetch_failures, 1);
        assert_eq!(
            stats.robots_unverified_hosts,
            vec!["http://ex.com".to_string()],
            "the origin crawled without rules is NAMED, not just counted"
        );

        // A 404 "no robots" is a legitimate allow-all and must NOT be reported as
        // an unverified assumption — that distinction is the whole point.
        let mut pages = HashMap::new();
        pages.insert(
            "https://ok.com/".to_string(),
            (
                200,
                "<html><body><p>only page</p></body></html>".to_string(),
            ),
        );
        let http = Arc::new(MockHttp {
            pages,
            ..Default::default()
        });
        let mut cfg = test_cfg(&["https://ok.com/"]);
        cfg.respect_robots = true;
        let stats = crawl(http, cfg, None, None, None, None, None)
            .await
            .unwrap();
        assert_eq!(stats.robots_fetch_failures, 0);
        assert!(stats.robots_unverified_hosts.is_empty());
    }

    #[tokio::test]
    async fn a_resumed_crawl_does_not_hand_a_host_a_fresh_page_budget() {
        // `taken` was not persisted, so a job reaped and re-claimed four times
        // fetched up to 4x `max_pages_per_host` from one host. The cap is
        // documented as host fairness, but on a long crawl its real job is
        // politeness — and durable execution silently multiplied it.
        let mut frontier = Frontier::new(Some(2));
        for i in 0..5 {
            frontier.push(format!("https://a.com/{i}"), 0);
        }
        assert!(frontier.pop().is_some());
        assert!(frontier.pop().is_some());
        assert!(
            frontier.pop().is_none(),
            "budget of 2 is spent for this run"
        );

        // Re-push the dropped backlog so the resumed run has work to refuse.
        let mut frontier = Frontier::new(Some(2));
        for i in 0..5 {
            frontier.push(format!("https://a.com/{i}"), 0);
        }
        frontier.pop();
        frontier.pop();

        let saved = Arc::new(SyncMutex::new(Vec::new()));
        let sink: Arc<dyn crate::app::CheckpointSink> = Arc::new(CollectCheckpoints {
            saves: saved.clone(),
            log: None,
        });
        assert!(save_checkpoint(&sink, &frontier, &HashMap::new(), &[], true).await);
        let (state, _) = saved.lock().unwrap().pop().expect("saved");
        let CheckpointLoad::Loaded(cp) = Checkpoint::from_value(&state) else {
            panic!("the checkpoint must load at the current version");
        };
        assert_eq!(cp.taken.get("a.com").copied(), Some(2), "budget persisted");

        let mut resumed = Frontier::new(Some(2));
        resumed.restore(cp.queue, cp.seen, cp.taken);
        assert!(
            resumed.pop().is_none(),
            "the resumed run does not get a fresh allowance for a host it already spent"
        );
        assert_eq!(
            resumed.skipped_host_budget(),
            3,
            "the refused backlog is still reported honestly"
        );
    }

    #[test]
    fn robots_parses_crawl_delay_and_sitemaps() {
        let rules = RobotRules::parse(
            "User-agent: googlebot\nCrawl-delay: 9\n\nUser-agent: *\nDisallow: /admin\n\
             Crawl-delay: 2.5\nSitemap: https://x.com/sitemap.xml\nSitemap: https://x.com/news.xml",
        );
        assert_eq!(rules.crawl_delay, Some(2.5));
        assert_eq!(rules.sitemaps.len(), 2);
        assert_eq!(rules.sitemaps[0], "https://x.com/sitemap.xml");
        assert!(!rules.allowed("https://x.com/admin/x"));
        assert!(rules.allowed("https://x.com/pub"));
    }

    #[test]
    fn robots_allow_overrides_and_wildcards_match() {
        let r = RobotRules::parse(
            "User-agent: *\nDisallow: /private\nAllow: /private/public\nDisallow: /*.pdf$\n",
        );
        // A longer Allow beats the shorter Disallow it sits under.
        assert!(!r.allowed("https://x.test/private/secret"));
        assert!(r.allowed("https://x.test/private/public/page"));
        // `$`-anchored wildcard blocks only exact `.pdf` endings.
        assert!(!r.allowed("https://x.test/files/doc.pdf"));
        assert!(r.allowed("https://x.test/files/doc.pdfx"));
        // No matching rule → allowed.
        assert!(r.allowed("https://x.test/anything"));
    }

    #[test]
    fn sitemap_entries_parse_unescape_and_capture_lastmod() {
        let xml = "<urlset>\
                   <url><loc> https://x.com/a </loc><lastmod>2026-07-16</lastmod></url>\
                   <url><loc>https://x.com/b?x=1&amp;y=2</loc></url></urlset>";
        let entries = parse_sitemap_entries(xml);
        let locs: Vec<&str> = entries.iter().map(|e| e.loc.as_str()).collect();
        assert_eq!(locs, vec!["https://x.com/a", "https://x.com/b?x=1&y=2"]);
        assert_eq!(entries[0].lastmod.as_deref(), Some("2026-07-16"));
        assert_eq!(entries[1].lastmod, None);

        // Bare-<loc> fallback (no <url> wrappers).
        let bare = parse_sitemap_entries("<loc>https://x.com/c</loc>");
        assert_eq!(bare.len(), 1);
        assert_eq!(bare[0].loc, "https://x.com/c");
    }

    #[test]
    fn url_filter_include_then_exclude() {
        let cfg = CrawlConfig {
            seeds: vec![],
            max_pages: 1,
            max_depth: 1,
            concurrency: 1,
            max_pages_per_host: None,
            same_domain: true,
            dedup_distance: 0,
            respect_robots: false,
            include_patterns: vec!["/blog/".into()],
            exclude_patterns: vec!["\\.pdf$".into()],
            sitemap_seeds: false,
            resume_state: None,
            revisit: false,
            discover: false,
            revisit_budget: None,
            min_due_score: 0.0,
        };
        let f = UrlFilter::compile(&cfg).unwrap();
        assert!(f.allows("https://x.com/blog/post"));
        assert!(!f.allows("https://x.com/shop/item"));
        assert!(!f.allows("https://x.com/blog/file.pdf"));
        assert!(UrlFilter::compile(&CrawlConfig {
            include_patterns: vec!["(".into()],
            ..cfg
        })
        .is_err());
    }

    #[test]
    fn parse_page_extracts_title_text_and_excerpt() {
        let html = "<html><head><title>  Weekly  Report </title>\
            <style>.a{color:red}</style></head><body>\
            <script>var x = 'ignore me';</script>\
            <h1>Revenue</h1><p>Sales rose sharply this quarter.</p>\
            <noscript>enable javascript</noscript></body></html>";
        let parsed = parse_page(html, "https://x.com/", true);
        assert_eq!(parsed.title.as_deref(), Some("Weekly Report"));
        // script/style/noscript text is excluded; visible text is collapsed.
        assert_eq!(parsed.excerpt, "Revenue Sales rose sharply this quarter.");
        assert_eq!(parsed.content_chars, parsed.excerpt.chars().count());
        assert!(!parsed.excerpt.contains("ignore me"));
        assert!(!parsed.excerpt.contains("enable javascript"));
    }

    #[test]
    fn parse_page_excerpt_is_capped() {
        let body = "word ".repeat(400);
        let html = format!("<html><body><p>{body}</p></body></html>");
        let parsed = parse_page(&html, "https://x.com/", true);
        assert_eq!(parsed.excerpt.chars().count(), EXCERPT_CHARS);
        assert!(parsed.content_chars > EXCERPT_CHARS);
        assert!(parsed.title.is_none());
    }

    #[test]
    fn change_weight_grades_by_simhash_distance() {
        // Unknown old fingerprint: can't discount, full weight.
        assert_eq!(change_weight(0, 0xDEAD), 1.0);
        // Identical and boilerplate-close: zero weight (churn isn't change).
        assert_eq!(change_weight(0xFF, 0xFF), 0.0);
        assert_eq!(change_weight(0b111, 0b000), 0.0, "distance 3 = boilerplate");
        // Ramps between the boilerplate floor and the full-change ceiling.
        let w4 = change_weight(0b1111, 0b0000); // distance 4
        assert!(w4 > 0.0 && w4 < 0.5, "{w4}");
        // Distance >= 16 is a full change.
        assert_eq!(change_weight(u64::MAX, u64::MAX << 16), 1.0);
    }

    #[test]
    fn due_score_never_checked_is_one_and_grows_with_elapsed() {
        let never = RevisitCadence::default();
        assert_eq!(due_score(&never, 1_000_000, 3600.0), 1.0);

        // Learned hourly interval: after ~1 interval, P ≈ 1 - 1/e ≈ 0.63.
        let cadence = RevisitCadence {
            checks: 5,
            changes: 4.0,
            last_checked_at: Some(0),
            last_change_at: Some(0),
            interval_secs: Some(3600.0),
        };
        let p1 = due_score(&cadence, 3600, 999.0);
        assert!((p1 - 0.632).abs() < 0.01, "{p1}");
        // Monotonic: more elapsed, more due; freshly checked, barely due.
        assert!(due_score(&cadence, 7200, 999.0) > p1);
        assert!(due_score(&cadence, 60, 999.0) < 0.05);
        // No learned interval: the (host) prior drives the clock.
        let cold = RevisitCadence {
            last_checked_at: Some(0),
            ..RevisitCadence::default()
        };
        assert!(due_score(&cold, 3600, 3600.0) > due_score(&cold, 3600, 86_400.0));
    }

    #[test]
    fn cadence_observations_update_counters_and_ewma() {
        let first = RevisitCadence::first_seen(100);
        assert_eq!(first.checks, 1);
        assert_eq!(first.changes, 0.0);
        assert_eq!(first.last_change_at, Some(100));

        // A change 1000s later seeds the EWMA with the first gap.
        let changed = first.observe_changed(1100, 1.0);
        assert_eq!(changed.checks, 2);
        assert_eq!(changed.interval_secs, Some(1000.0));
        assert_eq!(changed.last_change_at, Some(1100));
        // A second gap of 2000s smooths: 0.3*2000 + 0.7*1000 = 1300.
        let again = changed.observe_changed(3100, 1.0);
        assert_eq!(again.interval_secs, Some(1300.0));
        // Unchanged moves only the due clock; zero weight degrades to a check.
        let checked = again.observe_unchanged(4000);
        assert_eq!(checked.last_checked_at, Some(4000));
        assert_eq!(checked.last_change_at, Some(3100));
        assert_eq!(checked.interval_secs, again.interval_secs);
        let cosmetic = again.observe_changed(4000, 0.0);
        assert_eq!(cosmetic.changes, again.changes, "weight 0 is not a change");
    }

    #[test]
    fn host_priors_average_learned_intervals_per_host() {
        let seed = |url: &str, interval: Option<f64>| RevisitSeed {
            url: url.into(),
            etag: None,
            last_modified: None,
            simhash: 0,
            cadence: RevisitCadence {
                interval_secs: interval,
                ..RevisitCadence::default()
            },
        };
        let seeds = vec![
            seed("https://a.com/1", Some(100.0)),
            seed("https://a.com/2", Some(300.0)),
            seed("https://a.com/3", None), // cold — contributes nothing
            seed("https://b.com/1", None), // host with no signal at all
        ];
        let priors = host_cadence_priors(&seeds);
        assert_eq!(priors.get("a.com").copied(), Some(200.0));
        assert!(!priors.contains_key("b.com"));
    }

    #[tokio::test]
    async fn revisit_budget_and_min_due_score_skip_seeds_honestly() {
        // Three known pages, all served 200. One is freshly-checked (cold, low
        // due score) and must be filtered by min_due_score; of the two due
        // ones, revisit_budget = 1 keeps only the MORE due (never-checked)
        // seed. skipped_not_due reports both skips.
        let now = epoch_now();
        let mut pages = HashMap::new();
        for path in ["hot", "warm", "cold"] {
            pages.insert(
                format!("https://ex.com/{path}"),
                (
                    200,
                    format!("<html><body><p>content of {path} page</p></body></html>"),
                ),
            );
        }
        let http = Arc::new(MockHttp {
            pages,
            ..Default::default()
        });
        let cadence = |checked: i64, interval: f64| RevisitCadence {
            checks: 3,
            changes: 2.0,
            last_checked_at: Some(checked),
            last_change_at: Some(checked),
            interval_secs: Some(interval),
        };
        let seed = |url: &str, cad: RevisitCadence| RevisitSeed {
            url: url.into(),
            etag: None,
            last_modified: None,
            simhash: 0,
            cadence: cad,
        };
        let source = Box::new(SeedSource(vec![
            // Never checked: score 1.0 — the most due.
            RevisitSeed::bare("https://ex.com/hot", None, None),
            // Checked 2 intervals ago: due (score ~0.86) but ranks second.
            seed("https://ex.com/warm", cadence(now - 7200, 3600.0)),
            // Checked seconds ago against a week-long interval: score ~0.
            seed("https://ex.com/cold", cadence(now - 5, 604_800.0)),
        ]));
        let mut cfg = test_cfg(&[]);
        cfg.revisit = true;
        cfg.min_due_score = 0.5;
        cfg.revisit_budget = Some(1);

        let stats = crawl(http, cfg, None, None, Some(source), None, None)
            .await
            .unwrap();
        assert_eq!(stats.revisited, 1, "only the budgeted most-due seed ran");
        assert_eq!(
            stats.skipped_not_due, 2,
            "one below min_due_score + one past the budget"
        );
    }

    #[test]
    fn canonicalize_drops_tracking_sorts_query_and_trims_slash() {
        assert_eq!(
            canonicalize_str("https://x.com/a/?b=2&utm_source=tw&a=1#frag"),
            "https://x.com/a?a=1&b=2"
        );
        assert_eq!(canonicalize_str("https://x.com/"), "https://x.com/");
        assert_eq!(
            canonicalize_str("https://x.com/p/?fbclid=abc"),
            "https://x.com/p"
        );
        assert_eq!(canonicalize_str("not a url"), "not a url");
    }
}
