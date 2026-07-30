//! Run a sandboxed WASM plugin over documents (fuel + memory limited), deduping
//! the JSON results into a dataset. The extraction logic lives in the .wasm
//! module — swappable at runtime without recompiling the service, and safe to run
//! even if untrusted. Two input modes, mirroring `extractor`: fetch live `urls`,
//! or read stored bodies from a crawl→dataset `source` (no re-fetch).

use async_trait::async_trait;
use futures::StreamExt;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, FetchRequest, FetchStrategy, ManifestExample,
    Record, Result, ScrapeApp,
};
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
    ctx.params
        .get("plugin_params")
        .cloned()
        .unwrap_or(Value::Null)
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
/// Backfill mode also pages through `page_versions` in batches of this size.
const SOURCE_LIST_LIMIT: i64 = 10_000;

/// The crawl app's versioned archive dataset (see the crawl app): one record per
/// CHANGED revision of a page, keyed `{url}#{revision}`, carrying
/// `{url, revision, artifact_path, job_id, simhash, fetched_at}` — the same
/// artifact contract as `pages`, so `read_source_artifact` resolves historical
/// bodies unchanged.
const VERSIONS_DATASET: &str = "page_versions";

/// Cap on the per-key `missing_keys` echo in a backfill result — a large archive
/// could otherwise blow up the stored job result; `missing` keeps the full count.
const MISSING_ECHO_LIMIT: usize = 100;

/// Output-record identity for one input document: the record key, the natural
/// source URL, and (for archived versions) the observation timestamp. Historical
/// records are keyed `{natural_key}@{observed_at_date}` so change detection
/// treats backfill rows as distinct time-series points, not churn.
struct DocMeta {
    key: String,
    url: String,
    observed_at: Option<String>,
}

impl DocMeta {
    /// A present-day document: key IS the natural key, no observation tag.
    fn live(key: String) -> Self {
        Self {
            url: key.clone(),
            key,
            observed_at: None,
        }
    }
}

/// Key for a record derived from an archived observation:
/// `{natural_key}@{YYYY-MM-DD}` (date part of the version's `fetched_at`).
fn versioned_key(url: &str, observed_at: &str) -> String {
    let date = observed_at.get(..10).unwrap_or(observed_at);
    format!("{url}@{date}")
}

/// Index of the newest `observed` timestamp at or before `as_of` (both RFC3339).
/// Unparseable candidate timestamps are skipped; a bad `as_of` is an error.
fn pick_as_of(observed: &[String], as_of: &str) -> std::result::Result<Option<usize>, String> {
    let cutoff = chrono::DateTime::parse_from_rfc3339(as_of)
        .map_err(|e| format!("bad as_of '{as_of}' (want RFC3339): {e}"))?;
    let mut best: Option<(usize, chrono::DateTime<chrono::FixedOffset>)> = None;
    for (i, ts) in observed.iter().enumerate() {
        let Ok(t) = chrono::DateTime::parse_from_rfc3339(ts) else {
            continue;
        };
        if t <= cutoff && best.map_or(true, |(_, b)| t > b) {
            best = Some((i, t));
        }
    }
    Ok(best.map(|(i, _)| i))
}

/// All archived versions of one URL from the source app's [`VERSIONS_DATASET`],
/// via a bound (never interpolated) JSON filter on `$.url`.
async fn versions_for(ctx: &AppContext, src_app: &str, url: &str) -> Result<Vec<Record>> {
    ctx.datasets
        .list_filtered(
            src_app,
            VERSIONS_DATASET,
            &[pumper_core::datasets::JsonFilter::Eq {
                path: "$.url".into(),
                value: url.into(),
            }],
            None,
            SOURCE_LIST_LIMIT,
        )
        .await
}

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
         else all live records. The crawl's versioned archive is reachable via \
         source.as_of (RFC3339 snapshot), source.versions: \"all\" (every archived revision \
         + current), or source.backfill: true + url_pattern (batched fan over the whole \
         page_versions archive); historical records are keyed {url}@{date} and tagged \
         _url + _observed_at."
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["plugin"],
                "properties": {
                    "plugin": { "type": "string", "minLength": 1, "description": "Registered WASM plugin name (see GET /plugins)." },
                    "urls": {
                        "type": "array",
                        "items": { "type": "string", "pattern": "^https?://" },
                        "minItems": 1,
                        "description": "URL mode: fetch these and run the plugin. Mutually exclusive with `source`."
                    },
                    "source": {
                        "type": "object",
                        "required": ["app", "dataset"],
                        "properties": {
                            "app": { "type": "string" },
                            "dataset": { "type": "string" },
                            "keys": { "type": "array", "items": { "type": "string" } },
                            "as_of": {
                                "type": "string",
                                "description": "RFC3339 timestamp: resolve each key to the newest archived version (crawl page_versions) observed at or before this instant. Mutually exclusive with `versions`."
                            },
                            "versions": {
                                "type": "string",
                                "enum": ["all"],
                                "description": "Fan over every archived version of each key plus the current body; output keyed {url}@{date}."
                            },
                            "backfill": {
                                "type": "boolean",
                                "description": "Fan over the source app's whole page_versions archive in batches (ignores keys); combine with url_pattern."
                            },
                            "url_pattern": {
                                "type": "string",
                                "description": "Backfill only: regex a version's URL must match to be run."
                            }
                        },
                        "description": "Source mode: run over stored record bodies (no re-fetch)."
                    },
                    "strategy": { "type": "string", "enum": ["http", "browser", "auto", "auto_with_research"] },
                    "concurrency": { "type": "integer", "minimum": 1, "maximum": 64 },
                    "plugin_params": { "type": "object", "description": "Forwarded to a params-aware plugin's extract_v2 envelope." },
                    "dataset": { "type": "string", "description": "Output dataset name (default \"plugin_out\")." }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Run the `title` plugin over two live URLs",
                    params: json!({
                        "plugin": "title",
                        "urls": ["https://example.com/a", "https://example.com/b"]
                    }),
                },
                ManifestExample {
                    description: "Run a configured plugin over stored crawl bodies",
                    params: json!({
                        "plugin": "title",
                        "source": { "app": "crawl", "dataset": "pages" },
                        "plugin_params": { "selector": "h1" },
                        "concurrency": 8
                    }),
                },
                ManifestExample {
                    description: "Retroactive backfill: run the plugin over every archived \
                                  version of matching pages, producing a time-series dataset",
                    params: json!({
                        "plugin": "title",
                        "source": {
                            "app": "crawl",
                            "dataset": "pages",
                            "backfill": true,
                            "url_pattern": "^https://example\\.com/blog/"
                        },
                        "dataset": "title_history"
                    }),
                },
            ],
            output_shape: Some(
                "{ran, errors, dataset, new, changed, unchanged} — per-document plugin results \
                 deduped into the output dataset",
            ),
            cost_class: CostClass::Metered,
        }
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
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
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
        // clippy::redundant_iter_cloned — the `cloned()` looks redundant (the body
        // only ever takes `&url`), but it is load-bearing for inference: with
        // `Item = &String`/`&str` the closure must implement `FnOnce` for ANY
        // lifetime to satisfy the `buffered()` Send bound, and rustc rejects it
        // with "implementation of FnOnce is not general enough". Owning the item
        // removes the lifetime from the closure signature. Verified: both
        // `.iter()` and `.iter().map(String::as_str)` fail to compile.
        #[allow(clippy::redundant_iter_cloned)]
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
                p.run(&name, &doc, &pp)
                    .await
                    .unwrap_or_else(|e| json!({ "error": e.to_string() }))
            }
        });
        let mut results: Vec<Value> = futures::stream::iter(tasks)
            .buffered(concurrency)
            .collect()
            .await;

        let ran = results.iter().filter(|r| r.get("error").is_none()).count();
        let metas: Vec<DocMeta> = urls.iter().map(|u| DocMeta::live(u.clone())).collect();
        let items = upsert_items(&metas, &mut results);
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
    async fn run_source_mode(
        &self,
        ctx: &AppContext,
        plugin: &str,
        dataset: &str,
    ) -> Result<Value> {
        let source = ctx
            .params
            .get("source")
            .and_then(Value::as_object)
            .ok_or_else(|| {
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
            v.and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
        };
        let explicit_keys = str_array(source.get("keys"))
            .or_else(|| str_array(ctx.params.pointer("/_trigger/keys")));

        // Versioned-archive resolution (crawl `page_versions`): `backfill` fans the
        // plugin over ALL archived versions matching a URL pattern (its own batched
        // runner); `as_of` / `versions:"all"` resolve the chosen keys through the
        // archive instead of the live record.
        if source
            .get("backfill")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return self.run_backfill(ctx, plugin, dataset, &src_app).await;
        }
        let as_of = source
            .get("as_of")
            .and_then(Value::as_str)
            .map(str::to_string);
        let versions_all = source.get("versions").and_then(Value::as_str) == Some("all");
        if as_of.is_some() && versions_all {
            return Err(Error::App(
                "source.as_of and source.versions are mutually exclusive".into(),
            ));
        }

        // Resolve (meta, stored-body) pairs; a missing record or unreadable artifact
        // is reported per key, not run.
        let mut keyed: Vec<(DocMeta, String)> = Vec::new();
        let mut missing: Vec<Value> = Vec::new();
        let requested: usize;

        // Same key-selection precedence as before (explicit / trigger keys, else
        // the live sweep); the modes differ only in WHICH stored body each key
        // resolves to. The sweep carries its records instead of re-fetching.
        let selected: Vec<(String, Option<Record>)> = if let Some(keys) = explicit_keys {
            requested = keys.len();
            keys.into_iter().map(|k| (k, None)).collect()
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
            records
                .into_iter()
                .map(|r| (r.key.clone(), Some(r)))
                .collect()
        };

        for (key, pre_fetched) in selected {
            let live = match pre_fetched {
                Some(r) => Some(r),
                None => ctx.datasets.get(&src_app, &src_dataset, &key).await?,
            };
            if as_of.is_none() && !versions_all {
                // Present-day mode (unchanged behavior): the live record's body.
                match live {
                    Some(r) => match ctx.read_source_artifact(&src_app, &r).await {
                        Ok(body) => keyed.push((DocMeta::live(key), body)),
                        Err(reason) => missing.push(json!({ "key": key, "reason": reason })),
                    },
                    None => {
                        missing.push(json!({ "key": key, "reason": "no record in source dataset" }))
                    }
                }
                continue;
            }
            // Historical modes: candidates = archived versions + the live body
            // (observed at the live record's updated_at) — the archive holds only
            // CHANGED revisions, so a never-changed page still resolves.
            let mut candidates: Vec<(String, Record)> = Vec::new();
            for v in versions_for(ctx, &src_app, &key).await? {
                if let Some(ts) = v.data.get("fetched_at").and_then(Value::as_str) {
                    candidates.push((ts.to_string(), v));
                }
            }
            if let Some(r) = live {
                if r.removed_at.is_none()
                    && !r.data.get("gone").and_then(Value::as_bool).unwrap_or(false)
                {
                    candidates.push((r.updated_at.to_rfc3339(), r));
                }
            }
            if candidates.is_empty() {
                missing.push(json!({ "key": key, "reason": "no record or archived version" }));
                continue;
            }
            let chosen: Vec<&(String, Record)> = if let Some(as_of) = &as_of {
                let observed: Vec<String> = candidates.iter().map(|(ts, _)| ts.clone()).collect();
                match pick_as_of(&observed, as_of).map_err(Error::App)? {
                    Some(i) => vec![&candidates[i]],
                    None => {
                        missing.push(json!({
                            "key": key,
                            "reason": format!("no version observed at or before {as_of}"),
                        }));
                        continue;
                    }
                }
            } else {
                candidates.iter().collect()
            };
            for (ts, record) in chosen {
                match ctx.read_source_artifact(&src_app, record).await {
                    Ok(body) => keyed.push((
                        DocMeta {
                            key: versioned_key(&key, ts),
                            url: key.clone(),
                            observed_at: Some(ts.clone()),
                        },
                        body,
                    )),
                    Err(reason) => missing.push(json!({ "key": record.key, "reason": reason })),
                }
            }
        }

        let (metas, mut results) = self.run_plugin_batch(ctx, plugin, keyed).await;
        let loaded = metas.len();
        let ran = results.iter().filter(|r| r.get("error").is_none()).count();
        let items = upsert_items(&metas, &mut results);
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

    /// Runs the plugin over one batch of `(meta, body)` pairs with the bounded,
    /// order-preserving fan-out (bodies are moved into the tasks, never cloned);
    /// returns the metas re-paired positionally with the results.
    async fn run_plugin_batch(
        &self,
        ctx: &AppContext,
        plugin: &str,
        keyed: Vec<(DocMeta, String)>,
    ) -> (Vec<DocMeta>, Vec<Value>) {
        let (metas, docs): (Vec<DocMeta>, Vec<String>) = keyed.into_iter().unzip();
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
                p.run(&name, &doc, &pp)
                    .await
                    .unwrap_or_else(|e| json!({ "error": e.to_string() }))
            }
        });
        // Bounded run fan-out; `buffered` keeps order for the positional zip.
        let results: Vec<Value> = futures::stream::iter(tasks)
            .buffered(concurrency)
            .collect()
            .await;
        (metas, results)
    }

    /// Backfill mode: fan the plugin over ALL archived versions in the source
    /// app's `page_versions` dataset (optionally narrowed by a `url_pattern`
    /// regex), paging in [`SOURCE_LIST_LIMIT`] batches and upserting per batch so
    /// a large archive never accumulates in memory. Records are keyed
    /// `{url}@{observed_at_date}` and tagged `_url`/`_observed_at`. Only the
    /// archive is fanned — a plain `source` run covers the present-day bodies.
    async fn run_backfill(
        &self,
        ctx: &AppContext,
        plugin: &str,
        dataset: &str,
        src_app: &str,
    ) -> Result<Value> {
        let pattern = ctx
            .params
            .pointer("/source/url_pattern")
            .and_then(Value::as_str)
            .map(|p| {
                regex::Regex::new(p).map_err(|e| Error::App(format!("bad url_pattern '{p}': {e}")))
            })
            .transpose()?;

        let mut after: Option<(String, String)> = None;
        let mut scanned = 0usize;
        let mut skipped_pattern = 0usize;
        let mut loaded = 0usize;
        let mut ran = 0usize;
        let mut batches = 0usize;
        let mut missing: Vec<Value> = Vec::new();
        let (mut new, mut changed, mut unchanged) = (0usize, 0usize, 0usize);
        loop {
            let batch = ctx
                .datasets
                .list_page(
                    src_app,
                    VERSIONS_DATASET,
                    after.clone(),
                    SOURCE_LIST_LIMIT,
                    None,
                )
                .await?;
            let Some(last) = batch.last() else { break };
            after = Some((pumper_core::datasets::ts(last.updated_at), last.key.clone()));
            let short = (batch.len() as i64) < SOURCE_LIST_LIMIT;

            let mut keyed: Vec<(DocMeta, String)> = Vec::new();
            for v in &batch {
                if v.removed_at.is_some() {
                    continue;
                }
                scanned += 1;
                let Some(url) = v.data.get("url").and_then(Value::as_str) else {
                    missing.push(json!({ "key": v.key, "reason": "version record has no url" }));
                    continue;
                };
                if pattern.as_ref().is_some_and(|re| !re.is_match(url)) {
                    skipped_pattern += 1;
                    continue;
                }
                let Some(ts) = v.data.get("fetched_at").and_then(Value::as_str) else {
                    missing.push(
                        json!({ "key": v.key, "reason": "version record has no fetched_at" }),
                    );
                    continue;
                };
                match ctx.read_source_artifact(src_app, v).await {
                    Ok(body) => keyed.push((
                        DocMeta {
                            key: versioned_key(url, ts),
                            url: url.to_string(),
                            observed_at: Some(ts.to_string()),
                        },
                        body,
                    )),
                    Err(reason) => missing.push(json!({ "key": v.key, "reason": reason })),
                }
            }
            if !keyed.is_empty() {
                loaded += keyed.len();
                batches += 1;
                let (metas, mut results) = self.run_plugin_batch(ctx, plugin, keyed).await;
                ran += results.iter().filter(|r| r.get("error").is_none()).count();
                let items = upsert_items(&metas, &mut results);
                let summary = ctx.upsert_many(dataset, &items).await?;
                new += summary.new.len();
                changed += summary.changed.len();
                unchanged += summary.unchanged;
            }
            if short {
                break;
            }
        }
        // Bound the per-key echo; the full count is still reported.
        let missing_count = missing.len();
        missing.truncate(MISSING_ECHO_LIMIT);
        Ok(json!({
            "mode": "backfill",
            "plugin": plugin,
            "source": { "app": src_app, "dataset": VERSIONS_DATASET },
            "scanned": scanned,
            "skipped_pattern": skipped_pattern,
            "loaded": loaded,
            "ran": ran,
            "batches": batches,
            "missing": missing_count,
            "missing_keys": missing,
            "new": new,
            "changed": changed,
            "unchanged": unchanged,
        }))
    }
}

/// Builds the upsert items from `(meta, result)` pairs: skip plugin/fetch failures
/// (reported in the summary, not written as records), tag each record with its
/// natural source URL as `_url` and, for archived versions, its `_observed_at`.
fn upsert_items(metas: &[DocMeta], results: &mut [Value]) -> Vec<(String, Value)> {
    metas
        .iter()
        .zip(results.iter_mut())
        .filter_map(|(meta, rec)| {
            if rec.get("error").is_some() {
                return None;
            }
            if let Value::Object(map) = rec {
                map.insert("_url".into(), Value::String(meta.url.clone()));
                if let Some(ts) = &meta.observed_at {
                    map.insert("_observed_at".into(), Value::String(ts.clone()));
                }
            }
            Some((meta.key.clone(), rec.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{parse_concurrency, pick_as_of, versioned_key, DEFAULT_CONCURRENCY};
    use serde_json::json;

    #[test]
    fn versioned_key_uses_date_part() {
        assert_eq!(
            versioned_key("https://a/x", "2026-07-30T10:11:12+00:00"),
            "https://a/x@2026-07-30"
        );
        assert_eq!(versioned_key("https://a/x", "2026"), "https://a/x@2026");
    }

    #[test]
    fn pick_as_of_selects_newest_at_or_before_cutoff() {
        let observed = vec![
            "2026-01-01T00:00:00+00:00".to_string(),
            "2026-03-01T00:00:00+00:00".to_string(),
            "2026-06-01T00:00:00+00:00".to_string(),
            "not-a-timestamp".to_string(), // skipped, never picked
        ];
        assert_eq!(
            pick_as_of(&observed, "2026-04-15T00:00:00Z").unwrap(),
            Some(1)
        );
        assert_eq!(
            pick_as_of(&observed, "2026-06-01T00:00:00Z").unwrap(),
            Some(2)
        );
        assert_eq!(pick_as_of(&observed, "2025-12-31T23:59:59Z").unwrap(), None);
        assert!(pick_as_of(&observed, "yesterday").is_err());
    }

    #[test]
    fn concurrency_defaults_clamps_and_overrides() {
        assert_eq!(parse_concurrency(&json!({})), DEFAULT_CONCURRENCY);
        assert_eq!(parse_concurrency(&json!({ "concurrency": 8 })), 8);
        assert_eq!(parse_concurrency(&json!({ "concurrency": 0 })), 1);
        assert_eq!(
            parse_concurrency(&json!({ "concurrency": "lots" })),
            DEFAULT_CONCURRENCY
        );
    }
}
