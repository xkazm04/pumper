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

#[derive(Debug, Default, Serialize)]
pub struct CrawlStats {
    pub crawled: usize,
    pub kept: usize,
    pub skipped_duplicates: usize,
    pub skipped_robots: usize,
    /// Discovered links dropped by include/exclude URL patterns.
    pub skipped_filtered: usize,
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
}

/// Bounded, deduplicated URL queue.
struct Frontier {
    queue: VecDeque<(String, u32)>,
    seen: HashSet<String>,
}

impl Frontier {
    fn new() -> Self {
        Self { queue: VecDeque::new(), seen: HashSet::new() }
    }
    fn push(&mut self, url: String, depth: u32) {
        if self.seen.len() >= MAX_FRONTIER || self.seen.contains(&url) {
            return;
        }
        self.seen.insert(url.clone());
        self.queue.push_back((url, depth));
    }
    fn pop(&mut self) -> Option<(String, u32)> {
        self.queue.pop_front()
    }
    /// Puts an already-seen URL back at the tail (crawl-delay rotation).
    fn requeue(&mut self, url: String, depth: u32) {
        self.queue.push_back((url, depth));
    }
    fn len(&self) -> usize {
        self.queue.len()
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
) -> Result<CrawlStats> {
    let concurrency = cfg.concurrency.clamp(1, 256);
    // Buffer of kept-page fingerprints awaiting the next batched sink flush.
    let mut sink_buf: Vec<CrawlPageRecord> = Vec::new();
    let filter = UrlFilter::compile(&cfg)?;
    let mut frontier = Frontier::new();
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
                frontier.queue = cp.queue.into_iter().collect();
                frontier.seen = cp.seen.into_iter().collect();
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
            in_flight.push(async move { fetch_one(http, url, depth, same_domain).await });
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
        };
        stats.crawled += 1;

        let hash = simhash(&fetched.body);
        let duplicate = cfg.dedup_distance > 0 && dedup_index.is_near_dup(hash);

        if duplicate {
            stats.skipped_duplicates += 1;
        } else {
            dedup_index.insert(hash);
            stats.kept += 1;
            let artifact_name = format!("page-{:04}.html", stats.kept);
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
                });
                if sink_buf.len() >= PAGE_SINK_STRIDE {
                    if let Some(s) = sink.as_mut() {
                        s.emit(std::mem::take(&mut sink_buf)).await;
                    }
                }
            }
            // Periodic checkpoint so a killed process loses at most one stride.
            if stats.kept % CHECKPOINT_STRIDE == 0 {
                if let Some(path) = &cfg.checkpoint {
                    if !Checkpoint::save(path, &frontier, dedup_index.hashes()).await {
                        stats.checkpoint_errors += 1;
                    }
                }
            }
            // Enqueue newly discovered links within the depth budget.
            if fetched.depth < cfg.max_depth {
                for link in &fetched.links {
                    if !filter.allows(link) {
                        stats.skipped_filtered += 1;
                        continue;
                    }
                    frontier.push(link.clone(), fetched.depth + 1);
                }
            }
        }

        // Per-page metadata is NOT accumulated in memory (it streams to the
        // dataset via the sink); the result keeps only counters + the artifacts
        // dir + `pages` dataset as pointers.

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
    stats.frontier_remaining = frontier.queue.len();
    if let Some(path) = &cfg.checkpoint {
        if !Checkpoint::save(path, &frontier, dedup_index.hashes()).await {
            stats.checkpoint_errors += 1;
        }
    }
    stats.failed_by_host = top_n_by_count(stats.failed_by_host, MAX_FAILED_HOSTS);
    Ok(stats)
}

const CHECKPOINT_STRIDE: usize = 25;

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
            queue: frontier.queue.iter().cloned().collect(),
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
}

async fn fetch_one(
    http: Arc<dyn HttpClient>,
    url: String,
    depth: u32,
    same_domain: bool,
) -> CrawlFetch {
    let resp = match http.fetch(HttpRequest::get(&url)).await {
        Ok(resp) => resp,
        Err(_) => return CrawlFetch::Failed(url),
    };
    // A challenge/block response (403/429/503 or a Cloudflare/JS/CAPTCHA marker
    // on a 200) is not content — reuse the fetcher's shared classifier.
    if let Some(reason) = crate::fetcher::http_bot_wall(resp.status, &resp.body) {
        return CrawlFetch::BotWall(url, reason);
    }
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

/// Minimal robots.txt rules for the `*` user-agent: Disallow-prefix matching,
/// plus the `Crawl-delay` for that group and the (group-independent)
/// `Sitemap:` directives.
struct RobotRules {
    disallows: Vec<String>,
    crawl_delay: Option<f64>,
    sitemaps: Vec<String>,
}

impl RobotRules {
    fn allow_all() -> Self {
        Self { disallows: Vec::new(), crawl_delay: None, sitemaps: Vec::new() }
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
                    rules.disallows.push(value.to_string());
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
            .map(|u| u.path().to_string())
            .unwrap_or_else(|| "/".to_string());
        !self.disallows.iter().any(|d| path.starts_with(d))
    }
}

/// Hard caps for sitemap seeding: nested sitemaps followed per index, and total
/// URLs pushed — a big site's sitemap must not replace the crawl itself.
const MAX_SITEMAPS_PER_HOST: usize = 10;
const MAX_SITEMAP_SEEDS: usize = 2_000;

/// `<loc>` values from a sitemap or sitemap-index document.
fn parse_sitemap_locs(xml: &str) -> Vec<String> {
    let re = regex::Regex::new(r"<loc>\s*([^<]+?)\s*</loc>").expect("valid regex");
    re.captures_iter(xml)
        .map(|c| c[1].replace("&amp;", "&"))
        .collect()
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
    let mut pushed = 0;
    for root in roots {
        let Ok(resp) = http.fetch(HttpRequest::get(&root)).await else { continue };
        if !resp.is_success() {
            continue;
        }
        let locs = parse_sitemap_locs(&resp.body);
        // A sitemap index lists further sitemaps; follow one level.
        let nested: Vec<String> = if resp.body.contains("<sitemapindex") {
            locs.into_iter().take(MAX_SITEMAPS_PER_HOST).collect()
        } else {
            for loc in locs {
                if pushed >= budget {
                    return pushed;
                }
                if filter.allows(&loc) {
                    frontier.push(canonicalize_str(&loc), 0);
                    pushed += 1;
                }
            }
            continue;
        };
        for sm in nested {
            let Ok(resp) = http.fetch(HttpRequest::get(&sm)).await else { continue };
            if !resp.is_success() {
                continue;
            }
            for loc in parse_sitemap_locs(&resp.body) {
                if pushed >= budget {
                    return pushed;
                }
                if filter.allows(&loc) {
                    frontier.push(canonicalize_str(&loc), 0);
                    pushed += 1;
                }
            }
        }
    }
    pushed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::HttpResponse;
    use std::sync::Mutex as SyncMutex;

    /// Serves canned `(status, body)` per URL; URLs in `fail` return a transport
    /// error; unknown URLs → 404 empty.
    #[derive(Default)]
    struct MockHttp {
        pages: HashMap<String, (u16, String)>,
        fail: HashSet<String>,
    }

    #[async_trait]
    impl HttpClient for MockHttp {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            if self.fail.contains(&req.url) {
                return Err(crate::Error::App(format!("simulated transport failure: {}", req.url)));
            }
            let (status, body) =
                self.pages.get(&req.url).cloned().unwrap_or((404, String::new()));
            Ok(HttpResponse {
                status,
                headers: HashMap::new(),
                body,
                final_url: req.url,
                cache_hit: false,
            })
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
            same_domain: true,
            dedup_distance: 0,
            respect_robots: false,
            include_patterns: vec![],
            exclude_patterns: vec![],
            sitemap_seeds: false,
            checkpoint: None,
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

        let stats = crawl(http, test_cfg(&["https://ex.com/"]), None, Some(sink))
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

        let http = Arc::new(MockHttp { pages, fail });
        let stats = crawl(http, test_cfg(&["https://ex.com/"]), None, None).await.unwrap();

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
        let mut frontier = Frontier::new();
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
    fn sitemap_locs_parse_and_unescape() {
        let xml = "<urlset><url><loc> https://x.com/a </loc></url>\
                   <url><loc>https://x.com/b?x=1&amp;y=2</loc></url></urlset>";
        let locs = parse_sitemap_locs(xml);
        assert_eq!(locs, vec!["https://x.com/a", "https://x.com/b?x=1&y=2"]);
    }

    #[test]
    fn url_filter_include_then_exclude() {
        let cfg = CrawlConfig {
            seeds: vec![],
            max_pages: 1,
            max_depth: 1,
            concurrency: 1,
            same_domain: true,
            dedup_distance: 0,
            respect_robots: false,
            include_patterns: vec!["/blog/".into()],
            exclude_patterns: vec!["\\.pdf$".into()],
            sitemap_seeds: false,
            checkpoint: None,
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
