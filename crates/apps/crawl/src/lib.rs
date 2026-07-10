//! Broad crawler app: seed a set of URLs and crawl outward with bounded
//! concurrency, depth, and page count — respecting robots.txt and the per-domain
//! governor, dropping near-duplicate pages, and streaming page bodies to the
//! job's artifact directory.

use async_trait::async_trait;
use pumper_core::{crawl, AppContext, CrawlConfig, Error, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct Crawl;

#[async_trait]
impl ScrapeApp for Crawl {
    fn name(&self) -> &'static str {
        "crawl"
    }

    fn description(&self) -> &'static str {
        "High-concurrency broad crawler. Params: {\"seeds\": [..], \"max_pages\": 50, \
         \"max_depth\": 2, \"concurrency\": 16, \"same_domain\": true, \
         \"dedup_distance\": 3, \"respect_robots\": true, \
         \"include_patterns\": [\"regex\", ..], \"exclude_patterns\": [\"regex\", ..]}"
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let str_array = |key: &str| -> Vec<String> {
            ctx.params
                .get(key)
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default()
        };
        let seeds = str_array("seeds");
        if seeds.is_empty() {
            return Err(Error::App("param 'seeds' must be a non-empty array".into()));
        }

        let usize_param = |key: &str, default: usize| {
            ctx.params.get(key).and_then(Value::as_u64).map(|n| n as usize).unwrap_or(default)
        };
        let u32_param = |key: &str, default: u32| {
            ctx.params.get(key).and_then(Value::as_u64).map(|n| n as u32).unwrap_or(default)
        };
        let bool_param = |key: &str, default: bool| {
            ctx.params.get(key).and_then(Value::as_bool).unwrap_or(default)
        };

        let cfg = CrawlConfig {
            seeds,
            max_pages: usize_param("max_pages", 50),
            max_depth: u32_param("max_depth", 2),
            concurrency: usize_param("concurrency", 16),
            same_domain: bool_param("same_domain", true),
            dedup_distance: u32_param("dedup_distance", 3),
            respect_robots: bool_param("respect_robots", true),
            include_patterns: str_array("include_patterns"),
            exclude_patterns: str_array("exclude_patterns"),
            sitemap_seeds: bool_param("sitemap_seeds", false),
        };

        let stats = crawl(ctx.engines.http.clone(), cfg, Some(ctx.artifacts_dir.clone())).await?;
        Ok(json!({
            "crawled": stats.crawled,
            "kept": stats.kept,
            "skipped_duplicates": stats.skipped_duplicates,
            "skipped_robots": stats.skipped_robots,
            "skipped_filtered": stats.skipped_filtered,
            "sitemap_seeded": stats.sitemap_seeded,
            "hosts": stats.hosts,
            "frontier_remaining": stats.frontier_remaining,
            "pages": stats.pages,
        }))
    }
}
