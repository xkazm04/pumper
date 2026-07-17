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

use std::collections::{HashMap, HashSet, VecDeque};
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
}

/// One existing page handed back by a [`PageSource`] to seed a revisit: the
/// canonical URL plus whatever conditional-GET validators were stored last time.
#[derive(Debug, Clone)]
pub struct RevisitSeed {
    pub url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
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
    /// Persist frontier state here (JSON): loaded at start when present, saved
    /// periodically and at the end — so an interrupted or page-capped crawl
    /// resumes where it left off instead of refetching everything.
    pub checkpoint: Option<PathBuf>,
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
    }
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
    /// allow-all, but surfaced rather than hidden).
    pub robots_fetch_failures: usize,
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
            let Some(host) = self.order.pop_front() else { break };
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
            let Some(q) = self.per_host.get_mut(&host) else { continue };
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
        self.per_host.values().flat_map(|q| q.iter().cloned()).collect()
    }

    /// Restores queued URLs + seen-set from a checkpoint (bypasses the dedup
    /// check; `seen` is authoritative). Per-host `taken` counts are not persisted,
    /// so the per-host budget restarts for the resumed run.
    fn restore(&mut self, queue: Vec<(String, u32)>, seen: Vec<String>) {
        self.seen = seen.into_iter().collect();
        for (url, depth) in queue {
            self.enqueue(url, depth);
        }
    }
}

/// Banded SimHash index for near-duplicate detection — SAME Hamming-distance
/// semantics as a linear scan (`any kept within distance d`), but candidate
/// lookup instead of an O(n) scan per page.
///
/// Pigeonhole: two 64-bit hashes within Hamming distance `d` differ in at most
/// `d` bits, so across `b = d + 1` contiguous bit-bands at least one band is
/// bit-identical. Each kept hash is bucketed by every band value; a query
/// gathers candidates from its own band buckets and verifies the true Hamming
/// distance. That guarantees no false negatives (a true near-dup always shares
/// a band) — the exact same decision the linear scan would make, only faster.
struct SimHashIndex {
    distance: u32,
    /// Per-band `(shift, mask)` to extract that band's value from a hash.
    segs: Vec<(u32, u64)>,
    /// Per-band bucket: band value -> hashes carrying it.
    buckets: Vec<HashMap<u64, Vec<u64>>>,
    /// Every kept hash, in insert order — persisted to the checkpoint so dedup
    /// survives a resume (8 bytes each: bounded by kept count, not bodies).
    all: Vec<u64>,
}

impl SimHashIndex {
    fn new(distance: u32) -> Self {
        // b = d + 1 bands guarantee a shared band for any pair within distance d.
        let bands = (distance + 1).clamp(1, 64) as usize;
        let base = 64 / bands;
        let rem = 64 % bands;
        let mut segs = Vec::with_capacity(bands);
        let mut shift = 0u32;
        for i in 0..bands {
            let width = base + if i < rem { 1 } else { 0 };
            let mask = if width == 64 { u64::MAX } else { (1u64 << width) - 1 };
            segs.push((shift, mask));
            shift += width as u32;
        }
        Self { distance, segs, buckets: vec![HashMap::new(); bands], all: Vec::new() }
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
        for (i, (shift, mask)) in self.segs.iter().enumerate() {
            let band = (hash >> shift) & mask;
            if let Some(cands) = self.buckets[i].get(&band) {
                if cands.iter().any(|&h| hamming(h, hash) <= self.distance) {
                    return true;
                }
            }
        }
        false
    }

    fn insert(&mut self, hash: u64) {
        for (i, (shift, mask)) in self.segs.iter().enumerate() {
            let band = (hash >> shift) & mask;
            self.buckets[i].entry(band).or_default().push(hash);
        }
        self.all.push(hash);
    }

    fn hashes(&self) -> &[u64] {
        &self.all
    }
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
pub async fn crawl(
    http: Arc<dyn HttpClient>,
    cfg: CrawlConfig,
    output_dir: Option<PathBuf>,
    mut sink: Option<Box<dyn PageSink>>,
    source: Option<Box<dyn PageSource>>,
    progress: Option<ProgressFn>,
) -> Result<CrawlStats> {
    let concurrency = cfg.concurrency.clamp(1, 256);
    // Buffer of kept-page fingerprints awaiting the next batched sink flush.
    let mut sink_buf: Vec<CrawlPageRecord> = Vec::new();
    // Revisit: per-known-URL stored validators (etag, last_modified). Presence in
    // this map marks a URL as "known" — it gets a conditional GET and 304/gone
    // handling; discovered URLs are absent and fetched normally.
    let mut conditional: HashMap<String, (Option<String>, Option<String>)> = HashMap::new();
    let filter = UrlFilter::compile(&cfg)?;
    let mut frontier = Frontier::new(cfg.max_pages_per_host);
    let mut dedup_index = SimHashIndex::new(cfg.dedup_distance);
    let mut resumed = false;
    let mut checkpoint_reset = false;

    // Restore a prior run's frontier + dedup state before seeding, so already
    // -seen URLs (including the seeds) aren't re-enqueued. An incompatible
    // (older-format) checkpoint is discarded for a clean fresh start — never a
    // silently-wrong partial resume.
    if let Some(path) = &cfg.checkpoint {
        match Checkpoint::load(path).await {
            CheckpointLoad::Loaded(cp) => {
                frontier.restore(cp.queue, cp.seen);
                dedup_index = SimHashIndex::from_hashes(cfg.dedup_distance, cp.kept_hashes);
                resumed = true;
            }
            CheckpointLoad::Incompatible => {
                checkpoint_reset = true;
                tracing::warn!(
                    path = %path.display(),
                    "crawl: checkpoint format incompatible — discarding for a fresh start"
                );
            }
            CheckpointLoad::None => {}
        }
    }

    let mut seed_hosts: HashSet<String> = HashSet::new();
    for seed in &cfg.seeds {
        if let Some(host) = host_of(seed) {
            seed_hosts.insert(host);
        }
        frontier.push(canonicalize_str(seed), 0);
    }
    // Revisit: seed the frontier from existing page records and remember each
    // one's stored validators for the conditional GET.
    if cfg.revisit {
        if let Some(source) = source {
            for seed in source.seeds().await {
                let url = canonicalize_str(&seed.url);
                if let Some(host) = host_of(&url) {
                    seed_hosts.insert(host);
                }
                conditional.insert(url.clone(), (seed.etag, seed.last_modified));
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
    let mut hosts: HashSet<String> = HashSet::new();
    let mut stats = CrawlStats::default();
    stats.resumed = resumed;
    stats.checkpoint_reset = checkpoint_reset;
    let mut in_flight = FuturesUnordered::new();
    // Per-host earliest-next-fetch, driven by robots.txt Crawl-delay.
    let mut next_allowed: HashMap<String, tokio::time::Instant> = HashMap::new();
    // Last intermediate checkpoint save. Time-based, not page-based (see below).
    let mut last_checkpoint = tokio::time::Instant::now();

    // Expand seeds from each seed host's sitemaps before crawling.
    if cfg.sitemap_seeds {
        let hosts: Vec<String> = seed_hosts.iter().cloned().collect();
        for host in hosts {
            let declared = robots_for(&mut robots, &http, &host, &mut stats.robots_fetch_failures)
                .await
                .sitemaps
                .clone();
            let budget = MAX_SITEMAP_SEEDS.saturating_sub(stats.sitemap_seeded);
            if budget == 0 {
                break;
            }
            stats.sitemap_seeded +=
                seed_from_sitemaps(&http, &host, &declared, &mut frontier, &filter, budget).await;
        }
    }

    loop {
        // Top up in-flight fetches from the frontier. `rotations` guards the
        // crawl-delay requeue path against spinning through a queue where
        // every remaining URL is still inside its host's delay window.
        let mut rotations = 0;
        while in_flight.len() < concurrency && rotations <= frontier.len() {
            let Some((url, depth)) = frontier.pop() else { break };
            let host = host_of(&url).unwrap_or_default();
            let mut crawl_delay = None;
            if cfg.respect_robots && !host.is_empty() {
                let rules =
                    robots_for(&mut robots, &http, &host, &mut stats.robots_fetch_failures).await;
                if !rules.allowed(&url) {
                    stats.skipped_robots += 1;
                    continue;
                }
                crawl_delay = rules.crawl_delay;
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
            let cond = if cfg.revisit { conditional.get(&url).cloned() } else { None };
            in_flight.push(async move { fetch_one(http, url, depth, same_domain, cond).await });
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
                        if let Some(s) = sink.as_mut() {
                            s.emit(std::mem::take(&mut sink_buf)).await;
                        }
                    }
                }
                continue;
            }
        };
        stats.crawled += 1;
        // A known page fetched with a conditional GET that came back 200 (or a
        // discovered link is absent from `conditional`): count the revisit.
        if cfg.revisit && conditional.contains_key(&fetched.url) {
            stats.revisited += 1;
        }

        let hash = simhash(&fetched.body);
        let duplicate = cfg.dedup_distance > 0 && dedup_index.is_near_dup(hash);

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
                });
                if sink_buf.len() >= PAGE_SINK_STRIDE {
                    if let Some(s) = sink.as_mut() {
                        s.emit(std::mem::take(&mut sink_buf)).await;
                    }
                }
            }
            // Periodic checkpoint, gated by wall-clock rather than page count.
            // `Checkpoint::save` serializes the WHOLE frontier (up to MAX_FRONTIER
            // seen-strings + queue + kept hashes) — O(frontier), not O(delta) — so
            // firing it every N kept pages made total checkpoint work
            // O(pages/N × frontier): a 100k-page crawl did thousands of full ~10 MB
            // rewrites (tens of GB of write amplification) for state that moved by a
            // handful of pages, and each inline save stalled every in-flight fetch.
            // A minimum interval decouples save count from crawl size; the final
            // save below still captures the true end state, and the frontier's own
            // seen-set makes a resume idempotent, so widening the worst-case resume
            // loss from N pages to a few seconds is safe.
            if cfg.checkpoint.is_some() && last_checkpoint.elapsed() >= CHECKPOINT_MIN_INTERVAL {
                if let Some(path) = &cfg.checkpoint {
                    if !Checkpoint::save(path, &frontier, dedup_index.hashes()).await {
                        stats.checkpoint_errors += 1;
                    }
                }
                last_checkpoint = tokio::time::Instant::now();
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
        if stats.crawled % PROGRESS_STRIDE == 0 {
            emit_progress(&progress, &stats, frontier.len(), hosts.len());
        }

        if stats.kept >= cfg.max_pages {
            break;
        }
    }

    // Flush any kept pages still buffered below the batch stride.
    if let Some(s) = sink.as_mut() {
        if !sink_buf.is_empty() {
            s.emit(std::mem::take(&mut sink_buf)).await;
        }
    }

    stats.hosts = hosts.len();
    stats.frontier_remaining = frontier.len();
    stats.frontier_dropped = frontier.dropped();
    stats.skipped_host_budget = frontier.skipped_host_budget();
    if let Some(path) = &cfg.checkpoint {
        if !Checkpoint::save(path, &frontier, dedup_index.hashes()).await {
            stats.checkpoint_errors += 1;
        }
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
const CHECKPOINT_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

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
const CHECKPOINT_VERSION: u32 = 1;

/// Result of attempting to restore a checkpoint.
enum CheckpointLoad {
    /// No checkpoint file present (or unreadable) — start fresh, not an error.
    None,
    /// A compatible checkpoint restored.
    Loaded(Checkpoint),
    /// A file existed but was an incompatible version / unparseable format;
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
}

impl Checkpoint {
    async fn load(path: &PathBuf) -> CheckpointLoad {
        // A missing/unreadable file is a fresh start, not a reset signal.
        let Ok(bytes) = tokio::fs::read(path).await else {
            return CheckpointLoad::None;
        };
        // A file that IS present but doesn't parse as the current version is an
        // incompatible/corrupt checkpoint — never resume from it silently.
        match serde_json::from_slice::<Checkpoint>(&bytes) {
            Ok(cp) if cp.version == CHECKPOINT_VERSION => CheckpointLoad::Loaded(cp),
            _ => CheckpointLoad::Incompatible,
        }
    }

    /// Best-effort save; checkpointing must never fail the crawl, but a failure
    /// is no longer swallowed — returns `false` (and warn-logs) so the caller can
    /// surface a `checkpoint_errors` count in the result.
    async fn save(path: &PathBuf, frontier: &Frontier, kept_hashes: &[u64]) -> bool {
        let cp = Checkpoint {
            version: CHECKPOINT_VERSION,
            queue: frontier.queued(),
            seen: frontier.seen.iter().cloned().collect(),
            kept_hashes: kept_hashes.to_vec(),
        };
        let bytes = match serde_json::to_vec(&cp) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(path = %path.display(), "crawl: checkpoint serialize failed: {e}");
                return false;
            }
        };
        if let Some(parent) = path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                tracing::warn!(path = %path.display(), "crawl: checkpoint dir create failed: {e}");
                return false;
            }
        }
        // Write-then-rename so a crash mid-write can't corrupt the checkpoint.
        let tmp = path.with_extension("json.tmp");
        if let Err(e) = tokio::fs::write(&tmp, bytes).await {
            tracing::warn!(path = %tmp.display(), "crawl: checkpoint write failed: {e}");
            return false;
        }
        if let Err(e) = tokio::fs::rename(&tmp, path).await {
            tracing::warn!(path = %path.display(), "crawl: checkpoint rename failed: {e}");
            return false;
        }
        true
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
    ParsedPage { links, title, content_chars, excerpt }
}

fn extract_links(doc: &Html, base: &str, same_domain: bool) -> Vec<String> {
    let Ok(base_url) = Url::parse(base) else {
        return Vec::new();
    };
    let base_host = base_url.host_str().map(str::to_owned);
    let selector = Selector::parse("a[href]").expect("valid selector");
    let mut out = Vec::new();
    for el in doc.select(&selector) {
        let Some(href) = el.value().attr("href") else { continue };
        let Ok(joined) = base_url.join(href) else { continue };
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
        let Some(text) = node.value().as_text() else { continue };
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
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "msclkid", "mc_cid", "mc_eid", "ref", "ref_src",
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
    Url::parse(url).map(canonicalize).unwrap_or_else(|_| url.to_string())
}

fn host_of(url: &str) -> Option<String> {
    Url::parse(url).ok()?.host_str().map(str::to_owned)
}

async fn robots_for<'a>(
    cache: &'a mut HashMap<String, RobotRules>,
    http: &Arc<dyn HttpClient>,
    host: &str,
    fetch_failures: &mut usize,
) -> &'a RobotRules {
    if !cache.contains_key(host) {
        let url = format!("https://{host}/robots.txt");
        let rules = match http.fetch(HttpRequest::get(&url)).await {
            Ok(resp) if resp.is_success() => RobotRules::parse(&resp.body),
            // A non-2xx (e.g. 404 "no robots") is a legitimate allow-all.
            Ok(_) => RobotRules::allow_all(),
            // A transport failure is NOT "no robots" — fail open, but count it
            // instead of silently pretending the host allowed everything.
            Err(e) => {
                *fetch_failures += 1;
                tracing::debug!(%host, "crawl: robots.txt fetch failed: {e}");
                RobotRules::allow_all()
            }
        };
        cache.insert(host.to_string(), rules);
    }
    cache.get(host).unwrap()
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
        Self { rules: Vec::new(), crawl_delay: None, sitemaps: Vec::new() }
    }

    fn parse(text: &str) -> Self {
        let mut rules = Self::allow_all();
        let mut in_star_group = false;
        for raw in text.lines() {
            let line = raw.split('#').next().unwrap_or("").trim();
            let Some((key, value)) = line.split_once(':') else { continue };
            let key = key.trim().to_ascii_lowercase();
            // `Sitemap:` values are absolute URLs — re-join the split colon.
            let value = if key == "sitemap" {
                line.splitn(2, ':').nth(1).unwrap_or("").trim()
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
    let pat = if anchored { &pattern[..pattern.len() - 1] } else { pattern };
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
            out.push(SitemapEntry { loc: loc[1].replace("&amp;", "&"), lastmod: None });
        }
    }
    out
}

/// Seeds the frontier from a host's sitemaps (robots `Sitemap:` directives,
/// falling back to `/sitemap.xml`). Sitemap-index files are followed one level
/// deep. Returns how many URLs were pushed.
async fn seed_from_sitemaps(
    http: &Arc<dyn HttpClient>,
    host: &str,
    declared: &[String],
    frontier: &mut Frontier,
    filter: &UrlFilter,
    budget: usize,
) -> usize {
    let roots: Vec<String> = if declared.is_empty() {
        vec![format!("https://{host}/sitemap.xml")]
    } else {
        declared.iter().take(MAX_SITEMAPS_PER_HOST).cloned().collect()
    };
    // Collect all in-scope URL entries first, then push the freshest by `<lastmod>`
    // — a `max_pages`-capped crawl should spend its budget on URLs that changed most
    // recently. Mis-ordering is harmless (self-reported lastmod), so prioritization
    // is unconditional; `budget` still bounds how many land in the frontier.
    let mut entries: Vec<SitemapEntry> = Vec::new();
    for root in roots {
        let Ok(resp) = http.fetch(HttpRequest::get(&root)).await else { continue };
        if !resp.is_success() {
            continue;
        }
        let parsed = parse_sitemap_entries(&resp.body);
        if resp.body.contains("<sitemapindex") {
            // A sitemap index lists further sitemaps; follow one level.
            for sm in parsed.into_iter().take(MAX_SITEMAPS_PER_HOST) {
                let Ok(resp) = http.fetch(HttpRequest::get(&sm.loc)).await else { continue };
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
    }

    #[async_trait]
    impl HttpClient for MockHttp {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            if self.fail.contains(&req.url) {
                return Err(crate::Error::App(format!("simulated transport failure: {}", req.url)));
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
            let (status, body) =
                self.pages.get(&req.url).cloned().unwrap_or((404, String::new()));
            let mut headers = HashMap::new();
            if status == 200 {
                if let Some(tag) = self.resp_etags.get(&req.url) {
                    headers.insert("ETag".to_string(), tag.clone());
                }
            }
            Ok(HttpResponse { status, headers, body, final_url: req.url, cache_hit: false })
        }
    }

    /// A [`PageSink`] that accumulates every emitted record for assertions.
    struct CollectSink {
        records: Arc<SyncMutex<Vec<CrawlPageRecord>>>,
    }

    #[async_trait]
    impl PageSink for CollectSink {
        async fn emit(&mut self, batch: Vec<CrawlPageRecord>) {
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
            checkpoint: None,
            revisit: false,
            discover: false,
        }
    }

    #[tokio::test]
    async fn crawl_streams_kept_pages_to_sink() {
        let mut pages = HashMap::new();
        pages.insert(
            "https://ex.com/".to_string(),
            (200, "<html><head><title>Home</title></head><body><h1>Hi</h1>\
                   <a href=\"/about\">about</a></body></html>"
                .to_string()),
        );
        pages.insert(
            "https://ex.com/about".to_string(),
            (200, "<html><head><title>About</title></head><body>\
                   <p>About us page content.</p></body></html>"
                .to_string()),
        );
        let http = Arc::new(MockHttp { pages, ..Default::default() });
        let records = Arc::new(SyncMutex::new(Vec::new()));
        let sink = Box::new(CollectSink { records: records.clone() });

        let stats = crawl(http, test_cfg(&["https://ex.com/"]), None, Some(sink), None, None)
            .await
            .unwrap();

        assert_eq!(stats.kept, 2, "both distinct pages kept");
        let recs = records.lock().unwrap();
        assert_eq!(recs.len(), 2, "each kept page streamed to the sink exactly once");
        let home = recs.iter().find(|r| r.url == "https://ex.com/").unwrap();
        assert_eq!(home.title.as_deref(), Some("Home"));
        assert_eq!(home.status, 200);
        assert!(home.content_chars > 0);
        assert_ne!(home.simhash, 0, "body simhash recorded");
        assert!(recs.iter().any(|r| r.url == "https://ex.com/about"
            && r.title.as_deref() == Some("About")));
    }

    #[tokio::test]
    async fn crawl_reports_progress_snapshots() {
        let mut pages = HashMap::new();
        pages.insert(
            "https://ex.com/".to_string(),
            (200, "<html><body><a href=\"/a\">a</a></body></html>".to_string()),
        );
        pages.insert(
            "https://ex.com/a".to_string(),
            (200, "<html><body><p>distinct content</p></body></html>".to_string()),
        );
        let http = Arc::new(MockHttp { pages, ..Default::default() });
        let seen: Arc<SyncMutex<Vec<CrawlProgressSnapshot>>> = Arc::new(SyncMutex::new(Vec::new()));
        let sink_seen = seen.clone();
        let progress: ProgressFn = Arc::new(move |snap| sink_seen.lock().unwrap().push(snap.clone()));

        let stats = crawl(http, test_cfg(&["https://ex.com/"]), None, None, None, Some(progress))
            .await
            .unwrap();

        let snaps = seen.lock().unwrap();
        assert!(!snaps.is_empty(), "at least the final progress snapshot is emitted");
        let last = snaps.last().unwrap();
        assert_eq!(last.crawled, stats.crawled, "final snapshot mirrors end stats");
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
            (200, "<html><body><p>brand new content this run</p></body></html>".to_string()),
        );
        // /stable is 304 (validator matches); /gone is unknown → 404.
        let mut etags = HashMap::new();
        etags.insert("https://ex.com/stable".to_string(), "v1".to_string());
        let mut resp_etags = HashMap::new();
        resp_etags.insert("https://ex.com/changed".to_string(), "new-tag".to_string());
        let http = Arc::new(MockHttp { pages, etags, resp_etags, ..Default::default() });

        let source = Box::new(SeedSource(vec![
            RevisitSeed {
                url: "https://ex.com/stable".into(),
                etag: Some("v1".into()),
                last_modified: None,
            },
            RevisitSeed {
                url: "https://ex.com/changed".into(),
                etag: Some("stale".into()),
                last_modified: None,
            },
            RevisitSeed { url: "https://ex.com/gone".into(), etag: None, last_modified: None },
        ]));

        let records = Arc::new(SyncMutex::new(Vec::new()));
        let sink = Box::new(CollectSink { records: records.clone() });

        let mut cfg = test_cfg(&[]);
        cfg.revisit = true;

        let stats = crawl(http, cfg, None, Some(sink), Some(source), None).await.unwrap();

        assert_eq!(stats.revisited, 3, "all three known pages revisited");
        assert_eq!(stats.unchanged_304, 1, "the matching-validator page is a cheap 304");
        assert_eq!(stats.gone, 1, "the 404 page is flagged gone");
        assert_eq!(stats.kept, 1, "only the changed page is re-fingerprinted/kept");
        assert_eq!(stats.crawled, 1, "only the 200 counts as crawled");

        let recs = records.lock().unwrap();
        let live: Vec<_> = recs.iter().filter(|r| !r.gone).collect();
        let gone: Vec<_> = recs.iter().filter(|r| r.gone).collect();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].url, "https://ex.com/changed");
        assert_eq!(live[0].etag.as_deref(), Some("new-tag"), "response ETag stored");
        assert_eq!(gone.len(), 1);
        assert_eq!(gone[0].url, "https://ex.com/gone");
        assert_eq!(gone[0].status, 404);
    }

    #[tokio::test]
    async fn revisit_does_not_follow_links_without_discover() {
        // A known page links to a NEW url; without discover the frontier must not
        // expand to it.
        let mut pages = HashMap::new();
        pages.insert(
            "https://ex.com/hub".to_string(),
            (200, "<html><body><a href=\"/newly-linked\">new</a></body></html>".to_string()),
        );
        pages.insert(
            "https://ex.com/newly-linked".to_string(),
            (200, "<html><body><p>should not be crawled</p></body></html>".to_string()),
        );
        let http = Arc::new(MockHttp { pages, ..Default::default() });
        let source = Box::new(SeedSource(vec![RevisitSeed {
            url: "https://ex.com/hub".into(),
            etag: None,
            last_modified: None,
        }]));
        let mut cfg = test_cfg(&[]);
        cfg.revisit = true; // discover stays false

        let stats = crawl(http, cfg, None, None, Some(source), None).await.unwrap();
        assert_eq!(stats.crawled, 1, "only the seeded hub is fetched; no link-following");
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
            (200, "<html><body><p>real content here</p></body></html>".to_string()),
        );
        // 403 hard block.
        pages.insert("https://ex.com/blocked".to_string(), (403, "denied".to_string()));
        // 200 with a Cloudflare interstitial marker — must classify as bot-wall.
        pages.insert(
            "https://ex.com/cf".to_string(),
            (200, "<html><head><title>Just a moment...</title></head><body>\
                   <div class=\"cf-browser-verification\">Checking your browser\
                   </div></body></html>"
                .to_string()),
        );
        let mut fail = HashSet::new();
        fail.insert("https://ex.com/dead".to_string());

        let http = Arc::new(MockHttp { pages, fail, ..Default::default() });
        let stats = crawl(http, test_cfg(&["https://ex.com/"]), None, None, None, None).await.unwrap();

        // Kept: seed + /ok. /dead failed, /blocked + /cf are bot-walls.
        assert_eq!(stats.kept, 2, "only real-content pages kept");
        assert_eq!(stats.crawled, 2, "crawled counts only real responses");
        assert_eq!(stats.failed, 1, "transport failure counted, not swallowed");
        assert_eq!(stats.failed_by_host.get("ex.com").copied(), Some(1));
        assert_eq!(stats.skipped_botwall, 2, "403 block + CF challenge both bot-walls");
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

    #[tokio::test]
    async fn checkpoint_version_mismatch_forces_fresh_start() {
        let dir = std::env::temp_dir().join(format!("pumper-crawl-cp-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("cp.json");

        // A missing file is a fresh start, not a reset.
        assert!(matches!(Checkpoint::load(&path).await, CheckpointLoad::None));

        // A pre-versioning file (no `version` field) parses as version 0 and is
        // rejected as incompatible rather than resumed silently-wrong.
        tokio::fs::write(
            &path,
            br#"{"queue":[["https://x.com/",0]],"seen":["https://x.com/"],"kept_hashes":[1,2]}"#,
        )
        .await
        .unwrap();
        assert!(matches!(Checkpoint::load(&path).await, CheckpointLoad::Incompatible));

        // A current-version checkpoint round-trips.
        let mut frontier = Frontier::new(None);
        frontier.push("https://x.com/".into(), 0);
        assert!(Checkpoint::save(&path, &frontier, &[7u64]).await);
        match Checkpoint::load(&path).await {
            CheckpointLoad::Loaded(cp) => {
                assert_eq!(cp.version, CHECKPOINT_VERSION);
                assert_eq!(cp.kept_hashes, vec![7]);
            }
            _ => panic!("expected a compatible checkpoint to load"),
        }
        tokio::fs::remove_dir_all(&dir).await.ok();
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
        assert_eq!(f.skipped_host_budget(), 3, "the 3 over-budget A URLs are reported");
    }

    #[test]
    fn frontier_requeue_refunds_host_budget() {
        // A crawl-delay requeue must not burn budget: pop then requeue, and the
        // URL is still reachable under a cap of 1.
        let mut f = Frontier::new(Some(1));
        f.push("https://a.com/1".into(), 0);
        let (url, depth) = f.pop().unwrap();
        f.requeue(url, depth);
        assert!(f.pop().is_some(), "requeue refunded the budget so the URL pops again");
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
            checkpoint: None,
            revisit: false,
            discover: false,
        };
        let f = UrlFilter::compile(&cfg).unwrap();
        assert!(f.allows("https://x.com/blog/post"));
        assert!(!f.allows("https://x.com/shop/item"));
        assert!(!f.allows("https://x.com/blog/file.pdf"));
        assert!(UrlFilter::compile(&CrawlConfig { include_patterns: vec!["(".into()], ..cfg })
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
    fn canonicalize_drops_tracking_sorts_query_and_trims_slash() {
        assert_eq!(
            canonicalize_str("https://x.com/a/?b=2&utm_source=tw&a=1#frag"),
            "https://x.com/a?a=1&b=2"
        );
        assert_eq!(canonicalize_str("https://x.com/"), "https://x.com/");
        assert_eq!(canonicalize_str("https://x.com/p/?fbclid=abc"), "https://x.com/p");
        assert_eq!(canonicalize_str("not a url"), "not a url");
    }
}
