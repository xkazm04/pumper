//! Generic extraction app: fetch a list of URLs (tiered) and run a declarative
//! rule set over all of them in parallel across every CPU core. Showcases the
//! no-GIL, SIMD extraction engine — the fetched documents are parsed and
//! extracted concurrently in one process, then deduped into a dataset.

use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::{
    extract_batch, AppContext, Error, FetchRequest, FetchStrategy, Result, RuleSet, ScrapeApp,
};
use serde_json::{json, Value};

pub struct Extractor;

#[async_trait]
impl ScrapeApp for Extractor {
    fn name(&self) -> &'static str {
        "extractor"
    }

    fn description(&self) -> &'static str {
        "Fetch many URLs and extract fields in parallel via a declarative rule set. Params: \
         {\"urls\": [..], \"rules\": {\"field\": {\"type\": \"css|regex|json|const\", ..}}, \
         \"strategy\": \"http|browser|auto\", \"dataset\": \"extracted\"}"
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let urls: Vec<String> = ctx
            .params
            .get("urls")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        if urls.is_empty() {
            return Err(Error::App("param 'urls' must be a non-empty array".into()));
        }
        let rules: RuleSet = ctx
            .params
            .get("rules")
            .cloned()
            .ok_or_else(|| Error::App("param 'rules' is required".into()))
            .and_then(|v| serde_json::from_value(v).map_err(|e| Error::App(format!("bad rules: {e}"))))?;
        // Compile (and validate selectors/regex) once, before the fan-out.
        let compiled = Arc::new(rules.compile()?);

        let strategy = match ctx.params.get("strategy").and_then(Value::as_str) {
            Some("browser") => FetchStrategy::Browser,
            Some("auto") => FetchStrategy::Auto,
            Some("auto_with_research") => FetchStrategy::AutoWithResearch,
            _ => FetchStrategy::Http,
        };
        let dataset = ctx
            .params
            .get("dataset")
            .and_then(Value::as_str)
            .unwrap_or("extracted")
            .to_string();

        // Fetch all URLs concurrently (the governor handles per-host politeness).
        // The Fetcher is just Arcs — clone it out so the async tasks don't
        // capture `ctx`, which we still need for the dataset upsert below.
        let fetcher = ctx.engines.fetch.clone();
        let fetches = urls.iter().map(|url| {
            let f = fetcher.clone();
            let mut req = FetchRequest::new(url);
            req.strategy = strategy;
            async move {
                match f.fetch(req).await {
                    Ok(out) => out.html.or(out.text).unwrap_or_default(),
                    Err(_) => String::new(), // failed fetch → empty doc → null fields
                }
            }
        });
        let docs: Vec<String> = futures::future::join_all(fetches).await;
        let fetched = docs.iter().filter(|d| !d.is_empty()).count();

        // Extract the whole batch in parallel across all cores, off the async
        // runtime so we don't block a tokio worker.
        let compiled_for_task = compiled.clone();
        let extract_docs = docs.clone();
        let mut records = tokio::task::spawn_blocking(move || {
            extract_batch(&compiled_for_task, &extract_docs)
        })
        .await
        .map_err(|e| Error::App(format!("extract task failed: {e}")))?;

        // Tag each record with its source URL and upsert for dedup.
        let items: Vec<(String, Value)> = urls
            .iter()
            .zip(records.iter_mut())
            .map(|(url, rec)| {
                if let Value::Object(map) = rec {
                    map.insert("_url".into(), Value::String(url.clone()));
                }
                (url.clone(), rec.clone())
            })
            .collect();
        let summary = ctx.upsert_many(&dataset, &items).await?;

        Ok(json!({
            "requested": urls.len(),
            "fetched": fetched,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "records": records,
        }))
    }
}
