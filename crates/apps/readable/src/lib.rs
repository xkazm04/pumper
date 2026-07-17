//! Example app: fetch any URL as clean Markdown, using the tiered fetcher.
//! Demonstrates automatic escalation (http -> browser -> claude) and the
//! HTML-to-Markdown preprocessing pipeline in one call.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, FetchRequest, FetchStrategy, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct Readable;

#[async_trait]
impl ScrapeApp for Readable {
    fn name(&self) -> &'static str {
        "readable"
    }

    fn description(&self) -> &'static str {
        "Fetch a URL as clean Markdown via the tiered fetcher. The document is saved \
         to the `page.md` artifact; the result JSON is compact (set \"inline\": true \
         to also return the Markdown in the result). Params: \
         {\"url\": \"...\", \"strategy\": \"http|browser|auto|auto_with_research\", \
         \"wait_for_selector\": \".article\", \"min_content_chars\": 250, \"inline\": false}"
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

        let mut outcome = ctx.fetch(req).await?;

        // Move the document out of the outcome rather than cloning it twice.
        let markdown = outcome.markdown.take().or_else(|| outcome.text.take()).unwrap_or_default();
        if markdown.trim().is_empty() {
            // A successful fetch that yields no readable content is a failed
            // extraction, not an empty-but-valid result — don't report it as OK.
            return Err(Error::App(format!(
                "readable: extracted no content from {} (engine {}, status {:?})",
                outcome.url, outcome.engine, outcome.status
            )));
        }
        ctx.save_artifact("page.md", markdown.as_bytes()).await?;
        let markdown_chars = markdown.chars().count();

        // Compact result by default (the "big payloads to artifacts" convention the
        // artifact pipeline demonstrates): the document lives in the `page.md`
        // artifact, not inlined into jobs.result — which would store it a SECOND
        // time in SQLite and bloat every job listing that hydrates results. An
        // interactive caller can opt into inline return with `inline: true`; the
        // scheduled path never pays.
        let mut out = json!({
            "url": outcome.url,
            "engine": outcome.engine,
            "status": outcome.status,
            "escalations": outcome.escalations,
            "markdown_chars": markdown_chars,
            "artifact": "page.md",
        });
        if ctx.params.get("inline").and_then(Value::as_bool).unwrap_or(false) {
            if let Value::Object(map) = &mut out {
                map.insert("markdown".into(), Value::String(markdown));
            }
        }
        Ok(out)
    }
}
