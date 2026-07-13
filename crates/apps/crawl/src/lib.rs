//! Broad crawler app: seed a set of URLs and crawl outward with bounded
//! concurrency, depth, and page count — respecting robots.txt and the per-domain
//! governor, dropping near-duplicate pages, and streaming page bodies to the
//! job's artifact directory.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::{
    crawl, AppContext, CrawlConfig, CrawlPageRecord, Datasets, Error, PageSink, PageSource, Result,
    RevisitSeed, ScrapeApp,
};
use serde_json::{json, Value};

pub struct Crawl;

/// Max existing `pages` records loaded as revisit seeds per run (bounds the
/// dataset read and the frontier). A larger known set is revisited across runs.
const REVISIT_SEED_LIMIT: i64 = 10_000;

/// Running new/changed/unchanged tallies shared between the [`DatasetPageSink`]
/// (which adds to them as batches upsert) and the app (which reads them into the
/// job result once the crawl returns). Atomics avoid holding a lock across the
/// sink's `.await`.
#[derive(Default)]
struct PageCounts {
    new: AtomicUsize,
    changed: AtomicUsize,
    unchanged: AtomicUsize,
}

/// [`PageSink`] that upserts each batch of kept-page fingerprints into the
/// `pages` dataset (key = canonical URL). Uses `upsert_many` — partial-batch
/// semantics, never `sync_many` (a crawl is a partial view, not a full snapshot,
/// so absent keys must NOT be marked removed). Errors are logged, never fatal:
/// dataset side-effects must not fail the crawl.
struct DatasetPageSink {
    datasets: Arc<Datasets>,
    app: String,
    job_id: String,
    counts: Arc<PageCounts>,
}

#[async_trait]
impl PageSink for DatasetPageSink {
    async fn emit(&mut self, batch: Vec<CrawlPageRecord>) {
        // Split live fingerprints from revisit `gone` markers: gone records upsert
        // a `{gone: true}` value (an explicit per-key removal → a `changed`
        // revision that triggers/watches fire on) and are NOT counted as changed.
        let mut live: Vec<(String, Value)> = Vec::new();
        let mut gone: Vec<(String, Value)> = Vec::new();
        for p in batch {
            if p.gone {
                gone.push((
                    p.url.clone(),
                    json!({ "url": p.url, "status": p.status, "gone": true, "job_id": self.job_id }),
                ));
            } else {
                live.push((
                    p.url.clone(),
                    json!({
                        "url": p.url,
                        "title": p.title,
                        "status": p.status,
                        "content_chars": p.content_chars,
                        "simhash": p.simhash,
                        "excerpt": p.excerpt,
                        "artifact_path": p.artifact_path,
                        "depth": p.depth,
                        // Conditional-GET validators, stored so the next revisit
                        // can send If-None-Match / If-Modified-Since.
                        "etag": p.etag,
                        "last_modified": p.last_modified,
                        "job_id": self.job_id,
                    }),
                ));
            }
        }
        if !live.is_empty() {
            match self.datasets.upsert_many(&self.app, "pages", &live).await {
                Ok(summary) => {
                    self.counts.new.fetch_add(summary.new.len(), Ordering::Relaxed);
                    self.counts.changed.fetch_add(summary.changed.len(), Ordering::Relaxed);
                    self.counts.unchanged.fetch_add(summary.unchanged, Ordering::Relaxed);
                }
                Err(e) => tracing::warn!(job = %self.job_id, "crawl pages upsert failed: {e}"),
            }
        }
        if !gone.is_empty() {
            if let Err(e) = self.datasets.upsert_many(&self.app, "pages", &gone).await {
                tracing::warn!(job = %self.job_id, "crawl gone-marker upsert failed: {e}");
            }
        }
    }
}

/// [`PageSource`] that reads existing live `pages` records to seed a revisit —
/// the read-side mirror of [`DatasetPageSink`]. Skips already-removed and
/// already-`gone` records so a sentinel doesn't keep re-probing dead URLs.
struct DatasetPageSource {
    datasets: Arc<Datasets>,
    app: String,
    limit: i64,
}

#[async_trait]
impl PageSource for DatasetPageSource {
    async fn seeds(&self) -> Vec<RevisitSeed> {
        match self.datasets.list(&self.app, "pages", self.limit).await {
            Ok(records) => records
                .into_iter()
                .filter(|r| {
                    r.removed_at.is_none()
                        && !r.data.get("gone").and_then(Value::as_bool).unwrap_or(false)
                })
                .map(|r| RevisitSeed {
                    etag: r.data.get("etag").and_then(Value::as_str).map(String::from),
                    last_modified: r
                        .data
                        .get("last_modified")
                        .and_then(Value::as_str)
                        .map(String::from),
                    // The record key is the canonical URL (see DatasetPageSink).
                    url: r.key,
                })
                .collect(),
            Err(e) => {
                tracing::warn!(app = %self.app, "crawl revisit seed load failed: {e}");
                Vec::new()
            }
        }
    }
}

#[async_trait]
impl ScrapeApp for Crawl {
    fn name(&self) -> &'static str {
        "crawl"
    }

    fn description(&self) -> &'static str {
        "High-concurrency broad crawler. Params: {\"seeds\": [..], \"max_pages\": 50, \
         \"max_depth\": 2, \"concurrency\": 16, \"same_domain\": true, \
         \"dedup_distance\": 3, \"respect_robots\": true, \
         \"include_patterns\": [\"regex\", ..], \"exclude_patterns\": [\"regex\", ..], \
         \"sitemap_seeds\": false, \"checkpoint\": \"name\" (resumable frontier), \
         \"mode\": \"revisit\" (incremental recrawl of the `pages` dataset via \
         conditional GETs; \"discover\": true opts into link-following)}"
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let str_array = |key: &str| -> Vec<String> {
            ctx.params
                .get(key)
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default()
        };
        // Revisit mode seeds the frontier from the `pages` dataset, so `seeds` is
        // optional there (it stays required for a normal fresh crawl).
        let revisit = ctx.params.get("mode").and_then(Value::as_str) == Some("revisit");
        let seeds = str_array("seeds");
        if seeds.is_empty() && !revisit {
            return Err(Error::App(
                "param 'seeds' must be a non-empty array (or set mode:\"revisit\")".into(),
            ));
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
            // Named checkpoints live beside (not inside) the per-job artifacts
            // dir, so a later job with the same name resumes the crawl.
            checkpoint: ctx
                .params
                .get("checkpoint")
                .and_then(Value::as_str)
                .map(|name| {
                    let safe: String = name
                        .chars()
                        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
                        .collect();
                    ctx.artifacts_dir
                        .parent()
                        .unwrap_or(&ctx.artifacts_dir)
                        .join("checkpoints")
                        .join(format!("{safe}.json"))
                }),
            revisit,
            discover: bool_param("discover", false),
        };

        // Per-page fingerprints stream into the `pages` dataset as the crawl
        // runs (key = canonical URL), so crawled pages become queryable/diffable
        // and dataset triggers + watches fire per-page.
        let counts = Arc::new(PageCounts::default());
        let sink: Box<dyn PageSink> = Box::new(DatasetPageSink {
            datasets: ctx.datasets.clone(),
            app: ctx.app.clone(),
            job_id: ctx.job_id.to_string(),
            counts: counts.clone(),
        });

        // Revisit mode reads existing page records to seed the frontier.
        let source: Option<Box<dyn PageSource>> = revisit.then(|| {
            Box::new(DatasetPageSource {
                datasets: ctx.datasets.clone(),
                app: ctx.app.clone(),
                limit: REVISIT_SEED_LIMIT,
            }) as Box<dyn PageSource>
        });

        // Bridge core's crawl progress seam to the runtime reporter: each live
        // snapshot is persisted (visible on GET /jobs/{id}) and emitted as a
        // `progress` SSE event. The runtime throttles; this closure is cheap.
        let reporter = ctx.progress.clone();
        let progress: pumper_core::ProgressFn = Arc::new(move |snap| {
            reporter.report(serde_json::to_value(snap).unwrap_or_default());
        });

        let stats = crawl(
            ctx.engines.http.clone(),
            cfg,
            Some(ctx.artifacts_dir.clone()),
            Some(sink),
            source,
            Some(progress),
        )
        .await?;

        let pages_new = counts.new.load(Ordering::Relaxed);
        let pages_changed = counts.changed.load(Ordering::Relaxed);
        let pages_unchanged = counts.unchanged.load(Ordering::Relaxed);
        Ok(json!({
            "crawled": stats.crawled,
            "kept": stats.kept,
            "skipped_duplicates": stats.skipped_duplicates,
            "skipped_robots": stats.skipped_robots,
            "skipped_filtered": stats.skipped_filtered,
            "sitemap_seeded": stats.sitemap_seeded,
            // Honest failure/bot-wall accounting (previously swallowed silently).
            "failed": stats.failed,
            "failed_by_host": stats.failed_by_host,
            "skipped_botwall": stats.skipped_botwall,
            "robots_fetch_failures": stats.robots_fetch_failures,
            "checkpoint_errors": stats.checkpoint_errors,
            "resumed": stats.resumed,
            "checkpoint_reset": stats.checkpoint_reset,
            "hosts": stats.hosts,
            "frontier_remaining": stats.frontier_remaining,
            // Per-page metadata lives in the `pages` dataset (streamed during the
            // crawl), not in the result — only the write outcome is echoed here.
            "pages_dataset": "pages",
            "pages_new": pages_new,
            "pages_changed": pages_changed,
            "pages_unchanged": pages_unchanged,
            // Incremental-recrawl accounting (all 0 for a normal fresh crawl).
            "revisit": revisit,
            "revisited": stats.revisited,
            "unchanged_304": stats.unchanged_304,
            // `changed`/`new` = live pages re-fingerprinted / first-seen this run.
            "changed": pages_changed,
            "new": pages_new,
            "gone": stats.gone,
        }))
    }
}
