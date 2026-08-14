//! Example app: fetch any URL as clean Markdown, using the tiered fetcher.
//! Demonstrates automatic escalation (http -> browser -> claude) and the
//! HTML-to-Markdown preprocessing pipeline in one call.

use async_trait::async_trait;
use pumper_core::extract::extracted_nothing;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, FetchRequest, FetchStrategy, ManifestExample,
    Result, ScrapeApp,
};
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
         \"wait_for_selector\": \".article\", \"min_content_chars\": 250, \"inline\": false, \
         \"archive_max_age\": 604800 (accept a web-archive snapshot no older than N \
         seconds instead of touching the live site), \"use_recipes\": false (try a \
         learned JSON-API recipe for this host first)}"
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": { "type": "string", "pattern": "^https?://" },
                    "strategy": {
                        "type": "string",
                        "enum": ["http", "browser", "auto", "auto_with_research"],
                        "description": "Fetch ladder entry point (default \"auto\": http → browser)."
                    },
                    "wait_for_selector": {
                        "type": "string",
                        "description": "Browser tier only: CSS selector to await before capturing."
                    },
                    "min_content_chars": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Below this many extracted chars the tier is judged thin and the ladder escalates."
                    },
                    "inline": {
                        "type": "boolean",
                        "description": "Also return the Markdown in the job result (default false — it always lands in the page.md artifact)."
                    },
                    "archive_max_age": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Archive tier (default off): accept a web-archive snapshot captured within this many seconds INSTEAD of fetching the live page. Only for reads where a slightly stale body is fine (research/backfill); never for change detection."
                    },
                    "use_recipes": {
                        "type": "boolean",
                        "description": "Try a learned JSON-API recipe for this host ahead of the live tiers (default false). Useful on JS-heavy hosts the x-ray has already profiled."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Read one page as Markdown, escalating http → browser as needed",
                    params: json!({ "url": "https://example.com/post", "strategy": "auto" }),
                },
                ManifestExample {
                    description: "Cheap/polite historical read: accept a web-archive snapshot up \
                                  to a week old rather than hitting the live site",
                    params: json!({
                        "url": "https://example.com/docs/pricing",
                        "archive_max_age": 604800,
                        "inline": true
                    }),
                },
            ],
            output_shape: Some(
                "{url, engine (archive|http|browser|claude|api_recipe), status, escalations, \
                 markdown_chars, artifact: \"page.md\", markdown?} — the document lives in the \
                 page.md artifact unless `inline` was set",
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
        // Archive tier (M18) + API recipes (M05), both per-request opt-in and
        // absent by default — an unset param leaves the live ladder exactly as
        // it was. `readable` is the one app in this group where a stale-but-cheap
        // body is legitimately acceptable: it is a one-shot reader, not a monitor,
        // so the caller decides how old a body they will take.
        req.archive_max_age = ctx.params.get("archive_max_age").and_then(Value::as_u64);
        req.use_recipes = ctx
            .params
            .get("use_recipes")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let mut outcome = ctx.fetch(req).await?;

        // Move the document out of the outcome rather than cloning it twice.
        let markdown = outcome
            .markdown
            .take()
            .or_else(|| outcome.text.take())
            .unwrap_or_default();
        if extracted_nothing(&markdown) {
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
        if ctx
            .params
            .get("inline")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            if let Value::Object(map) = &mut out {
                map.insert("markdown".into(), Value::String(markdown));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The predicate this app used to own now lives in
    /// `pumper_core::extract::extracted_nothing` — `watch` needed the same check
    /// and apps may not depend on each other, so a shared home in core was the
    /// only way to avoid a third copy. This test stays here because `readable`
    /// is the app whose contract it defines.
    #[test]
    fn whitespace_only_extraction_is_a_failure_not_an_empty_success() {
        assert!(extracted_nothing(""));
        // Realistic failure mode: the markdown converter reduces a JS-only
        // page to bare whitespace/newlines.
        assert!(extracted_nothing("  \n\t\n   \n"));
    }

    #[test]
    fn real_content_is_not_flagged_as_failed_extraction() {
        assert!(!extracted_nothing("# Title\n\nBody paragraph."));
    }
}
