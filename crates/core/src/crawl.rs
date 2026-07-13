//! High-concurrency broad crawler. A bounded, deduplicated URL frontier feeds a
//! pool of concurrent fetch tasks (tokio holds thousands cheaply, at ~KB per
//! task); page bodies are written to disk as they arrive rather than
//! accumulated, so memory stays bounded by the concurrency level, not the crawl
//! size. Politeness comes from the shared per-domain governor (inside the http
//! engine) plus robots.txt; near-duplicate pages are dropped via SimHash.
//!
//! This is the shape asyncio struggles with: high connection concurrency with
//! GIL-free body parsing and constant memory under backpressure.

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

#[derive(Debug, Clone, Serialize)]
pub struct CrawlPage {
    pub url: String,
    pub depth: u32,
    pub status: u16,
    pub bytes: usize,
    pub links: usize,
    pub duplicate: bool,
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
    /// True when this run restored frontier state from a checkpoint.
    pub resumed: bool,
    pub hosts: usize,
    pub frontier_remaining: usize,
    pub pages: Vec<CrawlPage>,
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
    let mut kept_hashes: Vec<u64> = Vec::new();
    let mut resumed = false;

    // Restore a prior run's frontier + dedup state before seeding, so already
    // -seen URLs (including the seeds) aren't re-enqueued.
    if let Some(path) = &cfg.checkpoint {
        if let Some(cp) = Checkpoint::load(path).await {
            frontier.queue = cp.queue.into_iter().collect();
            frontier.seen = cp.seen.into_iter().collect();
            kept_hashes = cp.kept_hashes;
            resumed = true;
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
        tokio::fs::create_dir_all(dir).await.ok();
    }

    let mut robots: HashMap<String, RobotRules> = HashMap::new();
    let mut hosts: HashSet<String> = HashSet::new();
    let mut stats = CrawlStats::default();
    stats.resumed = resumed;
    let mut in_flight = FuturesUnordered::new();
    // Per-host earliest-next-fetch, driven by robots.txt Crawl-delay.
    let mut next_allowed: HashMap<String, tokio::time::Instant> = HashMap::new();

    // Expand seeds from each seed host's sitemaps before crawling.
    if cfg.sitemap_seeds {
        let hosts: Vec<String> = seed_hosts.iter().cloned().collect();
        for host in hosts {
            let declared = robots_for(&mut robots, &http, &host).await.sitemaps.clone();
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
                let rules = robots_for(&mut robots, &http, &host).await;
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
        let Some(fetched) = result else {
            continue; // fetch failed; skip
        };
        stats.crawled += 1;

        let hash = simhash(&fetched.body);
        let duplicate = cfg.dedup_distance > 0
            && kept_hashes.iter().any(|h| hamming(*h, hash) <= cfg.dedup_distance);

        if duplicate {
            stats.skipped_duplicates += 1;
        } else {
            kept_hashes.push(hash);
            stats.kept += 1;
            let artifact_name = format!("page-{:04}.html", stats.kept);
            if let Some(dir) = &output_dir {
                let file = dir.join(&artifact_name);
                let _ = tokio::fs::write(file, &fetched.body).await;
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
                    Checkpoint::save(path, &frontier, &kept_hashes).await;
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

        stats.pages.push(CrawlPage {
            url: fetched.url,
            depth: fetched.depth,
            status: fetched.status,
            bytes: fetched.body.len(),
            links: fetched.links.len(),
            duplicate,
        });

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
        Checkpoint::save(path, &frontier, &kept_hashes).await;
    }
    Ok(stats)
}

const CHECKPOINT_STRIDE: usize = 25;

/// Persisted frontier state: what is still queued, what has been seen, and the
/// SimHash fingerprints of kept pages (so dedup survives the resume too).
#[derive(Serialize, Deserialize)]
struct Checkpoint {
    queue: Vec<(String, u32)>,
    seen: Vec<String>,
    kept_hashes: Vec<u64>,
}

impl Checkpoint {
    async fn load(path: &PathBuf) -> Option<Self> {
        let bytes = tokio::fs::read(path).await.ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Best-effort save; checkpointing must never fail the crawl.
    async fn save(path: &PathBuf, frontier: &Frontier, kept_hashes: &[u64]) {
        let cp = Checkpoint {
            queue: frontier.queue.iter().cloned().collect(),
            seen: frontier.seen.iter().cloned().collect(),
            kept_hashes: kept_hashes.to_vec(),
        };
        let Ok(bytes) = serde_json::to_vec(&cp) else { return };
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        // Write-then-rename so a crash mid-write can't corrupt the checkpoint.
        let tmp = path.with_extension("json.tmp");
        if tokio::fs::write(&tmp, bytes).await.is_ok() {
            tokio::fs::rename(&tmp, path).await.ok();
        }
    }
}

async fn fetch_one(
    http: Arc<dyn HttpClient>,
    url: String,
    depth: u32,
    same_domain: bool,
) -> Option<Fetched> {
    let resp = http.fetch(HttpRequest::get(&url)).await.ok()?;
    let parsed = parse_page(&resp.body, &url, same_domain);
    Some(Fetched {
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
) -> &'a RobotRules {
    if !cache.contains_key(host) {
        let url = format!("https://{host}/robots.txt");
        let rules = match http.fetch(HttpRequest::get(&url)).await {
            Ok(resp) if resp.is_success() => RobotRules::parse(&resp.body),
            _ => RobotRules::allow_all(),
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

    /// Serves canned `(status, body)` per URL; unknown URLs → 404 empty.
    struct MockHttp {
        pages: HashMap<String, (u16, String)>,
    }

    #[async_trait]
    impl HttpClient for MockHttp {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
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
        let http = Arc::new(MockHttp { pages });
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
