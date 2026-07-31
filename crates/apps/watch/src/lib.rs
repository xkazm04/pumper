//! Generic scheduled change-watch: point it at any URL and it tells you when
//! (and how) the page changed. Each run fetches the page via the tiered
//! fetcher, reduces it to a compact fingerprint record (title, char count,
//! content hash, excerpt), and upserts it keyed by the URL — so the dataset
//! store's change detection + revision history do the heavy lifting. Pair a
//! run with a cron schedule (`POST /schedules`) and a dataset watch
//! (`POST /watches`) for a Visualping-style monitor with webhook alerts.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, FetchRequest, FetchStrategy, ManifestExample, Provenance,
    Result, ScrapeApp,
};
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

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": {
                        "type": "string",
                        "pattern": "^https?://",
                        "description": "Page to monitor; it is also the record key in the `pages` dataset."
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["http", "browser", "auto", "auto_with_research"],
                        "description": "Fetch ladder entry point (default \"auto\")."
                    },
                    "wait_for_selector": {
                        "type": "string",
                        "description": "Browser tier only: CSS selector to await before capturing — use it to fingerprint the content region rather than a spinner."
                    },
                    "min_content_chars": { "type": "integer", "minimum": 1 },
                    "cache_ttl_secs": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Cap cache staleness at N seconds instead of bypassing the HTTP cache entirely (the default). Useful when several watches share one hot endpoint."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Watch a release-notes page for changes (schedule this + a dataset watch for webhook alerts)",
                    params: json!({ "url": "https://example.com/releases" }),
                },
                ManifestExample {
                    description: "JS-rendered status page: render in the browser, fingerprint only the content region, and share one cached body across sibling watches",
                    params: json!({
                        "url": "https://status.example.com/",
                        "strategy": "browser",
                        "wait_for_selector": "main .incidents",
                        "cache_ttl_secs": 60
                    }),
                },
            ],
            output_shape: Some(
                "{url, engine, status, change: new|changed|unchanged, chars, diff} — the \
                 field-level diff of this run's `pages` record versus the previous revision",
            ),
            cost_class: CostClass::Metered,
        }
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
        let body_sha = hex_sha256(markdown.as_bytes());
        let record = json!({
            "url": outcome.url,
            "title": first_heading(&markdown),
            "chars": markdown.chars().count(),
            "content_sha256": body_sha.clone(),
            "excerpt": markdown.chars().take(EXCERPT_CHARS).collect::<String>(),
        });
        // Provenance (M12): this app knows both derivation facts for the record
        // it is about to write — the exact URL the body came from (the post-
        // redirect `outcome.url`, not the requested one) and the sha256 of the
        // body it saved as `page.md`. No RuleSet is involved (the fingerprint is
        // computed in code), so `rules_hash` stays Null rather than invented.
        let change = ctx
            .upsert_with_provenance(
                "pages",
                &url,
                &record,
                Provenance {
                    source_url: Some(outcome.url.clone()),
                    artifact_sha: Some(body_sha),
                    ..Provenance::default()
                },
            )
            .await?;

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

#[cfg(test)]
mod tests {
    use super::*;

    // Realistic slice of a page fetched as markdown: nav/preamble text before
    // the first real heading, which is not an h1.
    const PAGE: &str =
        "Skip to content\n\nSign in | Register\n\n## Release Notes\n\nv2.1 shipped today.\n";

    #[test]
    fn first_heading_is_the_stripped_title_not_the_preamble() {
        assert_eq!(first_heading(PAGE).as_deref(), Some("Release Notes"));
    }

    #[test]
    fn hashes_only_line_yields_none_not_an_empty_title() {
        assert_eq!(first_heading("body text\n##\nmore body"), None);
    }

    #[test]
    fn page_without_headings_has_no_title() {
        assert_eq!(first_heading("just a paragraph of text"), None);
    }

    #[test]
    fn hex_sha256_matches_the_nist_vector_not_a_double_hash() {
        // NIST test vectors — guard against double-hashing or hex-casing drift,
        // which would flip every stored fingerprint into a false "changed".
        assert_eq!(
            hex_sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex_sha256(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
