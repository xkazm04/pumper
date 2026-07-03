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

use futures::stream::{FuturesUnordered, StreamExt};
use scraper::{Html, Selector};
use serde::Serialize;
use url::Url;

use crate::engine::{HttpClient, HttpRequest};
use crate::simhash::{hamming, simhash};
use crate::Result;

const MAX_FRONTIER: usize = 100_000;

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
}

struct Fetched {
    url: String,
    depth: u32,
    status: u16,
    body: String,
    links: Vec<String>,
}

/// Crawls from the seeds, writing kept page bodies under `output_dir` (if set).
pub async fn crawl(
    http: Arc<dyn HttpClient>,
    cfg: CrawlConfig,
    output_dir: Option<PathBuf>,
) -> Result<CrawlStats> {
    let concurrency = cfg.concurrency.clamp(1, 256);
    let mut frontier = Frontier::new();
    let mut seed_hosts: HashSet<String> = HashSet::new();
    for seed in &cfg.seeds {
        if let Some(host) = host_of(seed) {
            seed_hosts.insert(host);
        }
        frontier.push(seed.clone(), 0);
    }
    if let Some(dir) = &output_dir {
        tokio::fs::create_dir_all(dir).await.ok();
    }

    let mut robots: HashMap<String, RobotRules> = HashMap::new();
    let mut kept_hashes: Vec<u64> = Vec::new();
    let mut hosts: HashSet<String> = HashSet::new();
    let mut stats = CrawlStats::default();
    let mut in_flight = FuturesUnordered::new();

    loop {
        // Top up in-flight fetches from the frontier.
        while in_flight.len() < concurrency {
            let Some((url, depth)) = frontier.pop() else { break };
            let host = host_of(&url).unwrap_or_default();
            if cfg.respect_robots && !host.is_empty() {
                let rules = robots_for(&mut robots, &http, &host).await;
                if !rules.allowed(&url) {
                    stats.skipped_robots += 1;
                    continue;
                }
            }
            hosts.insert(host);
            let http = http.clone();
            let same_domain = cfg.same_domain;
            in_flight.push(async move { fetch_one(http, url, depth, same_domain).await });
        }

        let Some(result) = in_flight.next().await else {
            break; // frontier drained and nothing in flight
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
            if let Some(dir) = &output_dir {
                let file = dir.join(format!("page-{:04}.html", stats.kept));
                let _ = tokio::fs::write(file, &fetched.body).await;
            }
            // Enqueue newly discovered links within the depth budget.
            if fetched.depth < cfg.max_depth {
                for link in &fetched.links {
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

    stats.hosts = hosts.len();
    stats.frontier_remaining = frontier.queue.len();
    Ok(stats)
}

async fn fetch_one(
    http: Arc<dyn HttpClient>,
    url: String,
    depth: u32,
    same_domain: bool,
) -> Option<Fetched> {
    let resp = http.fetch(HttpRequest::get(&url)).await.ok()?;
    let links = extract_links(&resp.body, &url, same_domain);
    Some(Fetched { url, depth, status: resp.status, body: resp.body, links })
}

fn extract_links(html: &str, base: &str, same_domain: bool) -> Vec<String> {
    let Ok(base_url) = Url::parse(base) else {
        return Vec::new();
    };
    let base_host = base_url.host_str().map(str::to_owned);
    let doc = Html::parse_document(html);
    let selector = Selector::parse("a[href]").expect("valid selector");
    let mut out = Vec::new();
    for el in doc.select(&selector) {
        let Some(href) = el.value().attr("href") else { continue };
        let Ok(mut joined) = base_url.join(href) else { continue };
        if !matches!(joined.scheme(), "http" | "https") {
            continue;
        }
        if same_domain && joined.host_str().map(str::to_owned) != base_host {
            continue;
        }
        joined.set_fragment(None);
        out.push(joined.to_string());
    }
    out
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

/// Minimal robots.txt rules for the `*` user-agent (Disallow-prefix matching).
struct RobotRules {
    disallows: Vec<String>,
}

impl RobotRules {
    fn allow_all() -> Self {
        Self { disallows: Vec::new() }
    }

    fn parse(text: &str) -> Self {
        let mut disallows = Vec::new();
        let mut in_star_group = false;
        for raw in text.lines() {
            let line = raw.split('#').next().unwrap_or("").trim();
            let Some((key, value)) = line.split_once(':') else { continue };
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim();
            match key.as_str() {
                "user-agent" => in_star_group = value == "*",
                "disallow" if in_star_group && !value.is_empty() => {
                    disallows.push(value.to_string());
                }
                _ => {}
            }
        }
        Self { disallows }
    }

    fn allowed(&self, url: &str) -> bool {
        let path = Url::parse(url)
            .ok()
            .map(|u| u.path().to_string())
            .unwrap_or_else(|| "/".to_string());
        !self.disallows.iter().any(|d| path.starts_with(d))
    }
}
