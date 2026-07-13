//! Generic scheduled change-watch: point it at any URL and it tells you when
//! (and how) the page changed. Each run fetches the page via the tiered
//! fetcher, reduces it to a compact fingerprint record (title, char count,
//! content hash, excerpt), and upserts it keyed by the URL — so the dataset
//! store's change detection + revision history do the heavy lifting. Pair a
//! run with a cron schedule (`POST /schedules`) and a dataset watch
//! (`POST /watches`) for a Visualping-style monitor with webhook alerts.

use async_trait::async_trait;
use pumper_core::{AppContext, FetchRequest, FetchStrategy, Result, ScrapeApp};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub struct Watch;

/// Excerpt length stored in the record. Long enough that the field-level diff
/// shows *what* changed near the top of the page, short enough to keep
/// revisions compact (the full markdown is saved as a job artifact).
const EXCERPT_CHARS: usize = 600;

#[async_trait]
impl ScrapeApp for Watch {
    fn name(&self) -> &'static str {
        "watch"
    }

    fn description(&self) -> &'static str {
        "Watch any URL for content changes. Fetches the page as Markdown, \
         fingerprints it into the `pages` dataset (keyed by URL), and reports \
         new/changed/unchanged with the field-level diff. Params: \
         {\"url\": \"...\", \"strategy\": \"http|browser|auto|auto_with_research\", \
         \"wait_for_selector\": \".main\", \"min_content_chars\": 250, \
         \"cache_ttl_secs\": 60}. Bypasses the HTTP cache by default so it sees \
         live bodies; set `cache_ttl_secs` to cap staleness instead. \
         Schedule it via POST /schedules and subscribe via POST /watches."
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let url = ctx.require_str("url")?.to_string();
        let strategy = match ctx.params.get("strategy").and_then(Value::as_str) {
            Some("http") => FetchStrategy::Http,
            Some("browser") => FetchStrategy::Browser,
            Some("auto_with_research") => FetchStrategy::AutoWithResearch,
            _ => FetchStrategy::Auto,
        };

        let mut req = FetchRequest::new(&url);
        req.strategy = strategy;
        req.to_markdown = true;
        req.wait_for_selector = ctx
            .params
            .get("wait_for_selector")
            .and_then(Value::as_str)
            .map(String::from);
        req.min_content_chars = ctx
            .params
            .get("min_content_chars")
            .and_then(Value::as_u64)
            .map(|n| n as usize);
        // Monitors need live bodies, not up-to-TTL-stale cached ones. Default to
        // a full cache bypass; a `cache_ttl_secs` param instead caps staleness
        // to a short TTL (useful when several watches share one hot endpoint).
        match ctx.params.get("cache_ttl_secs").and_then(Value::as_u64) {
            Some(secs) => req.ttl_override = Some(secs),
            None => req.no_cache = true,
        }

        let outcome = ctx.fetch(req).await?;
        let markdown = outcome
            .markdown
            .clone()
            .or_else(|| outcome.text.clone())
            .unwrap_or_default();
        ctx.save_artifact("page.md", markdown.as_bytes()).await?;

        // Compact fingerprint: change detection runs on this record, so keep it
        // small but informative — the excerpt makes diffs human-readable.
        let record = json!({
            "url": outcome.url,
            "title": first_heading(&markdown),
            "chars": markdown.chars().count(),
            "content_sha256": hex_sha256(markdown.as_bytes()),
            "excerpt": markdown.chars().take(EXCERPT_CHARS).collect::<String>(),
        });
        let change = ctx.upsert("pages", &url, &record).await?;

        // Surface what actually changed straight in the job result.
        let diff = ctx
            .datasets
            .history(&ctx.app, "pages", &url, 1)
            .await?
            .into_iter()
            .next()
            .and_then(|rev| rev.diff);

        Ok(json!({
            "url": outcome.url,
            "engine": outcome.engine,
            "status": outcome.status,
            "change": change,
            "chars": markdown.chars().count(),
            "diff": diff,
        }))
    }
}

/// First markdown heading, as a cheap page title.
fn first_heading(markdown: &str) -> Option<String> {
    markdown
        .lines()
        .find(|l| l.starts_with('#'))
        .map(|l| l.trim_start_matches('#').trim().to_string())
        .filter(|t| !t.is_empty())
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}
