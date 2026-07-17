//! Run a sandboxed WASM plugin over documents (fuel + memory limited), deduping
//! the JSON results into a dataset. The extraction logic lives in the .wasm
//! module — swappable at runtime without recompiling the service, and safe to run
//! even if untrusted. Two input modes, mirroring `extractor`: fetch live `urls`,
//! or read stored bodies from a crawl→dataset `source` (no re-fetch).

use async_trait::async_trait;
use futures::StreamExt;
use pumper_core::{AppContext, Error, FetchRequest, FetchStrategy, Record, Result, ScrapeApp};
use serde_json::{json, Value};

/// Default in-flight cap for the URL/record fan-out, matching `CrawlConfig.concurrency`.
const DEFAULT_CONCURRENCY: usize = 16;

/// Read the `concurrency` param (max in-flight fetch+run tasks), clamped to `>= 1`
/// and defaulting to [`DEFAULT_CONCURRENCY`]. Uses ordered buffering so the
/// positional `zip` of keys against results stays correct.
fn concurrency(ctx: &AppContext) -> usize {
    parse_concurrency(&ctx.params)
}

/// The per-job `plugin_params` envelope forwarded to the plugin (`Null` when
/// absent). Lets one plugin be configured per job (e.g. a different selector)
/// instead of recompiling a module per variation.
fn plugin_params(ctx: &AppContext) -> Value {
    ctx.params.get("plugin_params").cloned().unwrap_or(Value::Null)
}

/// Pure param parse for [`concurrency`] — clamps `concurrency` to `>= 1`,
/// defaulting to [`DEFAULT_CONCURRENCY`].
fn parse_concurrency(params: &Value) -> usize {
    params
        .get("concurrency")
        .and_then(Value::as_u64)
        .map(|n| n.max(1) as usize)
        .unwrap_or(DEFAULT_CONCURRENCY)
}

pub struct Plugin;

/// Max live records pulled from a source dataset when no explicit `keys` (and no
/// `_trigger.keys`) narrow the set — bounds the dataset read and the fan-out.
const SOURCE_LIST_LIMIT: i64 = 10_000;

#[async_trait]
impl ScrapeApp for Plugin {
    fn name(&self) -> &'static str {
        "plugin"
    }

    fn description(&self) -> &'static str {
        "Run a sandboxed WASM plugin over documents. Params: {\"plugin\": \"title\", \
         \"urls\": [..] OR \"source\": {\"app\": .., \"dataset\": .., \"keys\": [..]?}, \
         \"strategy\": \"http|browser|auto|auto_with_research\", \"concurrency\": 16 \
         (max in-flight fetch+run tasks), \"plugin_params\": {..} (forwarded to a \
         params-aware plugin's extract_v2 envelope), \"dataset\": \"plugin_out\"}. \
         Source mode reads each record's stored body (artifact_path under the origin job's \
         dir) instead of re-fetching; keys default to the firing trigger's _trigger.keys, \
         else all live records."
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let plugin = ctx.require_str("plugin")?.to_string();
        let dataset = ctx
            .params
            .get("dataset")
            .and_then(Value::as_str)
            .unwrap_or("plugin_out")
            .to_string();

        // Two input modes: fetch live `urls`, or read stored bodies from a
        // crawl→dataset `source`. Exactly one is required.
        if ctx.params.get("source").is_some() {
            self.run_source_mode(&ctx, &plugin, &dataset).await
        } else {
            self.run_urls_mode(&ctx, &plugin, &dataset).await
        }
    }
}

impl Plugin {
    /// URLs mode: fetch each URL (tiered) and run the plugin over it — fetch and
    /// plugin execution pipelined per URL.
    async fn run_urls_mode(&self, ctx: &AppContext, plugin: &str, dataset: &str) -> Result<Value> {
        let urls: Vec<String> = ctx
            .params
            .get("urls")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        if urls.is_empty() {
            return Err(Error::App(
                "param 'urls' must be a non-empty array (or provide 'source')".into(),
            ));
        }
        let strategy = match ctx.params.get("strategy").and_then(Value::as_str) {
            Some("browser") => FetchStrategy::Browser,
            Some("auto") => FetchStrategy::Auto,
            Some("auto_with_research") => FetchStrategy::AutoWithResearch,
            _ => FetchStrategy::Http,
        };

        // Bounded fetch+run fan-out: the governor serializes same-host fetches but
        // caps nothing globally, so a large `urls` list would open one socket per
        // URL at once. `buffered` preserves order for the positional zip below.
        let concurrency = concurrency(ctx);
        let plugin_params = plugin_params(ctx);
        let fetcher = ctx.engines.fetch.clone();
        let plugins = ctx.plugins.clone();
        let tasks = urls.iter().cloned().map(|url| {
            let f = fetcher.clone();
            let p = plugins.clone();
            let name = plugin.to_string();
            let pp = plugin_params.clone();
            let mut req = FetchRequest::new(&url);
            req.strategy = strategy;
            async move {
                let doc = match f.fetch(req).await {
                    Ok(out) => out.html.or(out.text).unwrap_or_default(),
                    Err(e) => return json!({ "error": format!("fetch: {e}") }),
                };
                if doc.is_empty() {
                    return json!({ "error": "empty document" });
                }
                p.run(&name, &doc, &pp).await.unwrap_or_else(|e| json!({ "error": e.to_string() }))
            }
        });
        let mut results: Vec<Value> =
            futures::stream::iter(tasks).buffered(concurrency).collect().await;

        let ran = results.iter().filter(|r| r.get("error").is_none()).count();
        let items = upsert_items(urls.iter().map(String::as_str), &mut results);
        let summary = ctx.upsert_many(dataset, &items).await?;

        Ok(json!({
            "mode": "urls",
            "plugin": plugin,
            "requested": urls.len(),
            "ran": ran,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "records": results,
        }))
    }

    /// Source mode: run the plugin over already-crawled bodies (no re-fetch).
    /// Key precedence mirrors `extractor`: explicit `source.keys` → the firing
    /// trigger's `_trigger.keys` → all live records in the source dataset.
    async fn run_source_mode(&self, ctx: &AppContext, plugin: &str, dataset: &str) -> Result<Value> {
        let source = ctx.params.get("source").and_then(Value::as_object).ok_or_else(|| {
            Error::App("param 'source' must be an object {app, dataset, keys?}".into())
        })?;
        let src_app = source
            .get("app")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::App("source.app is required".into()))?
            .to_string();
        let src_dataset = source
            .get("dataset")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::App("source.dataset is required".into()))?
            .to_string();

        let str_array = |v: Option<&Value>| -> Option<Vec<String>> {
            v.and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        };
        let explicit_keys = str_array(source.get("keys"))
            .or_else(|| str_array(ctx.params.pointer("/_trigger/keys")));

        // Resolve (key, stored-body) pairs; a missing record or unreadable artifact
        // is reported per key, not run.
        let mut keyed: Vec<(String, String)> = Vec::new();
        let mut missing: Vec<Value> = Vec::new();
        let requested: usize;

        if let Some(keys) = explicit_keys {
            requested = keys.len();
            for key in keys {
                match ctx.datasets.get(&src_app, &src_dataset, &key).await? {
                    Some(r) => match ctx.read_source_artifact(&src_app, &r).await {
                        Ok(body) => keyed.push((key, body)),
                        Err(reason) => missing.push(json!({ "key": key, "reason": reason })),
                    },
                    None => missing
                        .push(json!({ "key": key, "reason": "no record in source dataset" })),
                }
            }
        } else {
            let records: Vec<Record> = ctx
                .datasets
                .list(&src_app, &src_dataset, SOURCE_LIST_LIMIT)
                .await?
                .into_iter()
                .filter(|r| {
                    r.removed_at.is_none()
                        && !r.data.get("gone").and_then(Value::as_bool).unwrap_or(false)
                })
                .collect();
            requested = records.len();
            for r in &records {
                match ctx.read_source_artifact(&src_app, r).await {
                    Ok(body) => keyed.push((r.key.clone(), body)),
                    Err(reason) => missing.push(json!({ "key": r.key, "reason": reason })),
                }
            }
        }

        // Split keys from bodies without cloning either (bodies are moved into the
        // plugin tasks); zip the keys back against the results.
        let (keys, docs): (Vec<String>, Vec<String>) = keyed.into_iter().unzip();
        let loaded = keys.len();
        let concurrency = concurrency(ctx);
        let plugin_params = plugin_params(ctx);
        let plugins = ctx.plugins.clone();
        let tasks = docs.into_iter().map(|doc| {
            let p = plugins.clone();
            let name = plugin.to_string();
            let pp = plugin_params.clone();
            async move {
                if doc.is_empty() {
                    return json!({ "error": "empty document" });
                }
                p.run(&name, &doc, &pp).await.unwrap_or_else(|e| json!({ "error": e.to_string() }))
            }
        });
        // Bounded run fan-out; `buffered` keeps order for the positional zip below.
        let mut results: Vec<Value> =
            futures::stream::iter(tasks).buffered(concurrency).collect().await;

        let ran = results.iter().filter(|r| r.get("error").is_none()).count();
        let items = upsert_items(keys.iter().map(String::as_str), &mut results);
        let summary = ctx.upsert_many(dataset, &items).await?;

        Ok(json!({
            "mode": "source",
            "plugin": plugin,
            "source": { "app": src_app, "dataset": src_dataset },
            "requested": requested,
            "loaded": loaded,
            "ran": ran,
            "missing": missing.len(),
            "missing_keys": missing,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "records": results,
        }))
    }
}

/// Builds the upsert items from `(key, result)` pairs: skip plugin/fetch failures
/// (reported in the summary, not written as records), and tag each record with its
/// source key as `_url`.
fn upsert_items<'a>(
    keys: impl Iterator<Item = &'a str>,
    results: &mut [Value],
) -> Vec<(String, Value)> {
    keys.zip(results.iter_mut())
        .filter_map(|(key, rec)| {
            if rec.get("error").is_some() {
                return None;
            }
            if let Value::Object(map) = rec {
                map.insert("_url".into(), Value::String(key.to_string()));
            }
            Some((key.to_string(), rec.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{parse_concurrency, DEFAULT_CONCURRENCY};
    use serde_json::json;

    #[test]
    fn concurrency_defaults_clamps_and_overrides() {
        assert_eq!(parse_concurrency(&json!({})), DEFAULT_CONCURRENCY);
        assert_eq!(parse_concurrency(&json!({ "concurrency": 8 })), 8);
        assert_eq!(parse_concurrency(&json!({ "concurrency": 0 })), 1);
        assert_eq!(parse_concurrency(&json!({ "concurrency": "lots" })), DEFAULT_CONCURRENCY);
    }
}
