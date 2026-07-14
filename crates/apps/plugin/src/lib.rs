//! Run a sandboxed WASM plugin over a set of URLs. Fetches each URL (tiered),
//! hands the document to the named plugin (fuel + memory limited), and dedupes
//! the JSON results into a dataset. The extraction logic lives in the .wasm
//! module — swappable at runtime without recompiling the service, and safe to
//! run even if untrusted.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, FetchRequest, FetchStrategy, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct Plugin;

#[async_trait]
impl ScrapeApp for Plugin {
    fn name(&self) -> &'static str {
        "plugin"
    }

    fn description(&self) -> &'static str {
        "Run a sandboxed WASM plugin over URLs. Params: {\"plugin\": \"title\", \
         \"urls\": [..], \"strategy\": \"http|browser|auto\", \"dataset\": \"plugin_out\"}"
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let plugin = ctx.require_str("plugin")?.to_string();
        let urls: Vec<String> = ctx
            .params
            .get("urls")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        if urls.is_empty() {
            return Err(Error::App("param 'urls' must be a non-empty array".into()));
        }
        let strategy = match ctx.params.get("strategy").and_then(Value::as_str) {
            Some("browser") => FetchStrategy::Browser,
            Some("auto") => FetchStrategy::Auto,
            _ => FetchStrategy::Http,
        };
        let dataset = ctx
            .params
            .get("dataset")
            .and_then(Value::as_str)
            .unwrap_or("plugin_out")
            .to_string();

        // Clone the handles so the async tasks don't capture `ctx`.
        let fetcher = ctx.engines.fetch.clone();
        let plugins = ctx.plugins.clone();
        let tasks = urls.iter().map(|url| {
            let f = fetcher.clone();
            let p = plugins.clone();
            let name = plugin.clone();
            let mut req = FetchRequest::new(url);
            req.strategy = strategy;
            async move {
                let doc = match f.fetch(req).await {
                    Ok(out) => out.html.or(out.text).unwrap_or_default(),
                    Err(e) => return json!({ "error": format!("fetch: {e}") }),
                };
                if doc.is_empty() {
                    return json!({ "error": "empty document" });
                }
                p.run(&name, &doc)
                    .await
                    .unwrap_or_else(|e| json!({ "error": e.to_string() }))
            }
        });
        let mut results: Vec<Value> = futures::future::join_all(tasks).await;

        let ran = results.iter().filter(|r| r.get("error").is_none()).count();
        let items: Vec<(String, Value)> = urls
            .iter()
            .zip(results.iter_mut())
            .filter_map(|(url, rec)| {
                // Fetch/plugin failures are reported in the summary, not written
                // into the output dataset as if they were extracted records.
                if rec.get("error").is_some() {
                    return None;
                }
                if let Value::Object(map) = rec {
                    map.insert("_url".into(), Value::String(url.clone()));
                }
                Some((url.clone(), rec.clone()))
            })
            .collect();
        let summary = ctx.upsert_many(&dataset, &items).await?;

        Ok(json!({
            "plugin": plugin,
            "requested": urls.len(),
            "ran": ran,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "records": results,
        }))
    }
}
