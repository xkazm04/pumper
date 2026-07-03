//! Example app: fetch any URL as clean Markdown, using the tiered fetcher.
//! Demonstrates automatic escalation (http -> browser -> claude) and the
//! HTML-to-Markdown preprocessing pipeline in one call.

use async_trait::async_trait;
use pumper_core::{AppContext, FetchRequest, FetchStrategy, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct Readable;

#[async_trait]
impl ScrapeApp for Readable {
    fn name(&self) -> &'static str {
        "readable"
    }

    fn description(&self) -> &'static str {
        "Fetch a URL as clean Markdown via the tiered fetcher. Params: \
         {\"url\": \"...\", \"strategy\": \"http|browser|auto|auto_with_research\", \
         \"wait_for_selector\": \".article\", \"min_content_chars\": 250}"
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

        let outcome = ctx.engines.fetch.fetch(req).await?;

        let markdown = outcome
            .markdown
            .clone()
            .or_else(|| outcome.text.clone())
            .unwrap_or_default();
        ctx.save_artifact("page.md", markdown.as_bytes()).await?;

        Ok(json!({
            "url": outcome.url,
            "engine": outcome.engine,
            "status": outcome.status,
            "escalations": outcome.escalations,
            "markdown_chars": markdown.chars().count(),
            "markdown": markdown,
        }))
    }
}
