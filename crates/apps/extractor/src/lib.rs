//! Generic extraction app: fetch a list of URLs (tiered) and run a declarative
//! rule set over all of them in parallel across every CPU core. Showcases the
//! no-GIL, SIMD extraction engine — the fetched documents are parsed and
//! extracted concurrently in one process, then deduped into a dataset.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use pumper_core::{
    extract_batch_with_report, signals_batch, AppContext, AppManifest, CompiledRuleSet, CostClass,
    DocReport, Error, FetchHealth, FetchRequest, FetchStrategy, FieldStatus, ManifestExample,
    ObservedDoc, Record, Result, RuleSet, ScrapeApp, UpsertSummary,
};
use pumper_core::config::ArchiveConfig;
use pumper_engine_archive::ArchiveEngine;
use serde_json::{json, Value};

pub struct Extractor;

/// Default per-run snapshot cap for the Wayback backfill mode
/// (`source.archive.max_snapshots`), and the hard ceiling it clamps to — one
/// run never fans over more than [`ARCHIVE_SNAPSHOT_CEILING`] captures; wider
/// ranges report `truncated: true` and are resumed with a narrower `from`/`to`.
const DEFAULT_MAX_SNAPSHOTS: usize = 100;
const ARCHIVE_SNAPSHOT_CEILING: usize = 1000;

/// Parsed `source.archive` params (Wayback historical backfill).
struct ArchiveParams {
    /// The CDX target: an exact URL (`url`) or a Wayback wildcard/prefix
    /// pattern (`url_pattern`, e.g. `example.com/products/*`).
    target: String,
    from: Option<String>,
    to: Option<String>,
    max_snapshots: usize,
    base_url: String,
}

/// Pure parse/validation of the `source.archive` object: exactly one of
/// `url`/`url_pattern`; `from`/`to` must be 4-14 digit CDX bounds when
/// present; `max_snapshots` defaults to [`DEFAULT_MAX_SNAPSHOTS`] and clamps
/// into `1..=`[`ARCHIVE_SNAPSHOT_CEILING`].
fn parse_archive_params(
    archive: &serde_json::Map<String, Value>,
) -> std::result::Result<ArchiveParams, String> {
    let url = archive.get("url").and_then(Value::as_str);
    let pattern = archive.get("url_pattern").and_then(Value::as_str);
    let target = match (url, pattern) {
        (Some(u), None) => u.to_string(),
        (None, Some(p)) => p.to_string(),
        (Some(_), Some(_)) => {
            return Err("source.archive: url and url_pattern are mutually exclusive".into())
        }
        (None, None) => {
            return Err("source.archive requires url or url_pattern".into());
        }
    };
    let bound = |k: &str| -> std::result::Result<Option<String>, String> {
        match archive.get(k) {
            None => Ok(None),
            Some(v) => {
                let s = v
                    .as_str()
                    .ok_or_else(|| format!("source.archive.{k} must be a string"))?;
                if !pumper_engine_archive::valid_cdx_bound(s) {
                    return Err(format!(
                        "source.archive.{k} '{s}' is not a CDX time bound (4-14 digits, e.g. \
                         \"2019\" or \"20190601123045\")"
                    ));
                }
                Ok(Some(s.to_string()))
            }
        }
    };
    let max_snapshots = archive
        .get("max_snapshots")
        .and_then(Value::as_u64)
        .map(|n| (n.max(1) as usize).min(ARCHIVE_SNAPSHOT_CEILING))
        .unwrap_or(DEFAULT_MAX_SNAPSHOTS);
    let base_url = archive
        .get("base_url")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| ArchiveConfig::default().base_url);
    Ok(ArchiveParams {
        target,
        from: bound("from")?,
        to: bound("to")?,
        max_snapshots,
        base_url,
    })
}

/// Max live records pulled from a source dataset when no explicit `keys` (and no
/// `_trigger.keys`) narrow the set — bounds the dataset read and the fan-out.
/// Backfill mode also pages through `page_versions` in batches of this size.
const SOURCE_LIST_LIMIT: i64 = 10_000;

/// The crawl app's versioned archive dataset: one record per CHANGED revision of
/// a page, keyed `{url}#{revision}`, carrying `{url, revision, artifact_path,
/// job_id, simhash, fetched_at}` — the same artifact contract as `pages`, so
/// [`AppContext::read_source_artifact`] resolves historical bodies unchanged.
const VERSIONS_DATASET: &str = "page_versions";

/// One resolved input document: the output-record key, the natural source URL,
/// an optional observation timestamp (set only for archived versions / the
/// version-resolved live body), and the body itself.
///
/// Records extracted from a historical version are keyed
/// `{natural_key}@{observed_at_date}` — a DISTINCT key per observation — so
/// change detection treats backfill rows as separate time-series points, not as
/// churn on the present-day record.
struct SourceDoc {
    key: String,
    url: String,
    observed_at: Option<String>,
    /// Provenance tag for records whose body came from an external archive
    /// rather than this system's own fetch/crawl history (`"wayback"` for the
    /// `source.archive` backfill mode); rendered as `_fetched_via` so the two
    /// histories compose under one convention.
    fetched_via: Option<&'static str>,
    body: String,
}

impl SourceDoc {
    /// A present-day document: key IS the natural key, no observation tag.
    fn live(key: String, body: String) -> Self {
        Self {
            url: key.clone(),
            key,
            observed_at: None,
            fetched_via: None,
            body,
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

/// Default in-flight fetch cap, matching `CrawlConfig.concurrency`.
const DEFAULT_FETCH_CONCURRENCY: usize = 16;

/// Read the `concurrency` param (max in-flight fetches), clamped to `>= 1` and
/// defaulting to [`DEFAULT_FETCH_CONCURRENCY`]. Bounds the URL-list fan-out so a
/// large `urls` list can't open one socket per URL at once.
fn fetch_concurrency(ctx: &AppContext) -> usize {
    parse_concurrency(&ctx.params)
}

/// Pure param parse for [`fetch_concurrency`] — clamps `concurrency` to `>= 1`,
/// defaulting to [`DEFAULT_FETCH_CONCURRENCY`].
fn parse_concurrency(params: &Value) -> usize {
    params
        .get("concurrency")
        .and_then(Value::as_u64)
        .map(|n| n.max(1) as usize)
        .unwrap_or(DEFAULT_FETCH_CONCURRENCY)
}

/// Aggregate the per-document reports into a quality signal for the job result:
/// how many field extractions matched out of the total attempted, plus the
/// fields with the highest miss rate (an empty or errored extraction is a miss).
/// Returns `(matched, total, worst_fields)`; `worst_fields` lists only fields
/// that missed at least once, worst first.
fn summarize_reports<'a>(
    reports: impl IntoIterator<Item = &'a DocReport>,
) -> (u64, u64, Vec<Value>) {
    let mut matched: u64 = 0;
    let mut total: u64 = 0;
    let mut doc_count: u64 = 0;
    // field -> (misses, errors)
    let mut misses: std::collections::BTreeMap<&str, (u64, u64)> =
        std::collections::BTreeMap::new();
    for report in reports {
        doc_count += 1;
        for (field, status) in &report.fields {
            total += 1;
            let entry = misses.entry(field.as_str()).or_default();
            match status {
                // A container that matched but held no items is not a miss — the
                // listing selector still binds, the listing is just quiet.
                FieldStatus::Matched | FieldStatus::ContainerEmpty => matched += 1,
                FieldStatus::Empty => entry.0 += 1,
                FieldStatus::Error { .. } => {
                    entry.0 += 1;
                    entry.1 += 1;
                }
            }
        }
    }
    let docs = doc_count.max(1) as f64;
    let mut worst: Vec<Value> = misses
        .into_iter()
        .filter(|(_, (m, _))| *m > 0)
        .map(|(field, (m, errors))| {
            json!({
                "field": field,
                "misses": m,
                "errors": errors,
                "miss_rate": ((m as f64 / docs) * 1000.0).round() / 1000.0,
            })
        })
        .collect();
    // Highest miss count first; ties broken by field name for stable output.
    worst.sort_by(|a, b| {
        b["misses"]
            .as_u64()
            .cmp(&a["misses"].as_u64())
            .then_with(|| a["field"].as_str().cmp(&b["field"].as_str()))
    });
    (matched, total, worst)
}

/// Runs the compiled rules over `docs` off the async runtime (rayon fan-out),
/// returning each record paired with its per-field [`DocReport`].
async fn run_extraction(
    compiled: Arc<CompiledRuleSet>,
    docs: Vec<String>,
) -> Result<Vec<(Value, DocReport)>> {
    tokio::task::spawn_blocking(move || extract_batch_with_report(&compiled, &docs))
        .await
        .map_err(|e| Error::App(format!("extract task failed: {e}")))
}

/// Shared tail for both input modes: extract the `(key, doc)` pairs in parallel,
/// tag each record with its source key as `_url`, upsert into `dataset`, and
/// return the records plus the aggregate quality signal. `key` is a source URL
/// (urls mode) or a dataset record key (source mode) — for the crawl `pages`
/// dataset the key IS the canonical URL, so `_url` stays meaningful.
async fn extract_and_upsert(
    ctx: &AppContext,
    compiled: Arc<CompiledRuleSet>,
    dataset: &str,
    keyed: Vec<SourceDoc>,
    fetch: FetchHealth,
) -> Result<ExtractOutcome> {
    // Split keys/meta from bodies without copying the bodies — `keyed` is owned
    // and dropped here anyway (was: `.iter().map(|(_,d)| d.clone())`, deep-cloning
    // every HTML body and roughly doubling peak RSS over the whole batch).
    let mut keys: Vec<String> = Vec::with_capacity(keyed.len());
    let mut metas: Vec<(String, Option<String>, Option<&'static str>)> =
        Vec::with_capacity(keyed.len());
    let mut docs: Vec<String> = Vec::with_capacity(keyed.len());
    for d in keyed {
        keys.push(d.key);
        metas.push((d.url, d.observed_at, d.fetched_via));
        docs.push(d.body);
    }
    let reported = run_extraction(compiled, docs.clone()).await?;
    // Borrow the reports rather than deep-cloning each into a throwaway Vec.
    let (matched, total, worst) = summarize_reports(reported.iter().map(|(_, r)| r));

    // Health verdict FIRST, then the write: the state settled here is what the
    // upsert below gates on (trust stamp, quarantine dataset, removal
    // suppression). Judging afterwards would stamp a verdict that did not exist.
    let verdict = observe(ctx, dataset, &keys, docs, &reported, fetch).await;

    let mut records: Vec<Value> = Vec::with_capacity(reported.len());
    let items: Vec<(String, Value)> = keys
        .into_iter()
        .zip(metas)
        .zip(reported)
        .map(|((key, (url, observed_at, fetched_via)), (mut rec, _))| {
            tag_record(&mut rec, url, observed_at, fetched_via);
            records.push(rec.clone());
            (key, rec)
        })
        .collect();
    let summary = ctx.upsert_many(dataset, &items).await?;
    Ok(ExtractOutcome {
        records,
        matched,
        total,
        worst,
        summary,
        health: verdict,
    })
}

/// Stamps the shared provenance convention onto an extracted record: `_url`
/// (natural source URL), `_observed_at` (when the body was observed; absent for
/// present-day bodies), and `_fetched_via` (external-archive provenance, e.g.
/// `"wayback"`; absent for this system's own fetches).
fn tag_record(
    rec: &mut Value,
    url: String,
    observed_at: Option<String>,
    fetched_via: Option<&'static str>,
) {
    if let Value::Object(map) = rec {
        map.insert("_url".into(), Value::String(url));
        if let Some(ts) = observed_at {
            map.insert("_observed_at".into(), Value::String(ts));
        }
        if let Some(via) = fetched_via {
            map.insert("_fetched_via".into(), Value::String(via.into()));
        }
    }
}

/// What one extraction pass produced: the records, the aggregate quality signal,
/// the write summary, and the source-health verdict.
struct ExtractOutcome {
    records: Vec<Value>,
    matched: u64,
    total: u64,
    worst: Vec<Value>,
    summary: UpsertSummary,
    health: Option<Value>,
}

/// Reports this run to the health detector and renders its verdict for the job
/// result. Best-effort: a detection failure is logged and the run still succeeds,
/// because health is a derived judgement and must never fail a working scrape.
async fn observe(
    ctx: &AppContext,
    dataset: &str,
    keys: &[String],
    docs: Vec<String>,
    reported: &[(Value, DocReport)],
    fetch: FetchHealth,
) -> Option<Value> {
    if !ctx.health.enabled() {
        return None;
    }
    let values: Vec<Value> = reported.iter().map(|(v, _)| v.clone()).collect();
    // Fingerprinting parses each body once more, on the same rayon path the
    // extraction ran on — off the async runtime so it can't stall the reactor.
    let signals = tokio::task::spawn_blocking(move || signals_batch(&docs, &values))
        .await
        .map_err(|e| Error::App(format!("fingerprint task failed: {e}")))
        .ok()?;
    let observed: Vec<ObservedDoc> = keys
        .iter()
        .zip(reported)
        .zip(signals)
        .map(|((key, (values, report)), signals)| ObservedDoc {
            key: key.clone(),
            values: values.clone(),
            report: report.clone(),
            signals,
        })
        .collect();
    match ctx.observe_extraction(dataset, &observed, fetch).await {
        Ok(verdict) => verdict.map(|v| {
            if v.state != v.previous_state {
                tracing::warn!(
                    source = %v.source_id,
                    from = v.previous_state.as_str(),
                    to = v.state.as_str(),
                    score = v.score,
                    diagnosis = v.diagnosis.map(|d| d.as_str()).unwrap_or("-"),
                    "extraction health state changed"
                );
            }
            json!(v)
        }),
        Err(e) => {
            tracing::warn!("extraction health evaluation failed: {e}");
            None
        }
    }
}

#[async_trait]
impl ScrapeApp for Extractor {
    fn name(&self) -> &'static str {
        "extractor"
    }

    fn description(&self) -> &'static str {
        "Fetch many URLs (or read stored crawl bodies) and extract fields in parallel via a \
         declarative rule set. Params: {\"urls\": [..] OR \"source\": {\"app\": .., \
         \"dataset\": .., \"keys\": [..]?}, \"rules\": {\"field\": {\"type\": \
         \"css|regex|json|xpath|const\", ..}}, \"strategy\": \"http|browser|auto\", \
         \"concurrency\": 16 (max in-flight fetches), \"dataset\": \"extracted\"}. \
         Source mode reads each record's stored body \
         (artifact_path under the origin job's dir) instead of re-fetching; keys default to \
         the firing trigger's _trigger.keys, else all live records. The crawl's versioned \
         archive is reachable via source.as_of (RFC3339 snapshot), source.versions: \"all\" \
         (every archived revision + current), or source.backfill: true + url_pattern \
         (batched fan over the whole page_versions archive); historical records are keyed \
         {url}@{date} and tagged _url + _observed_at. source.archive: {url|url_pattern, \
         from, to, max_snapshots} backfills from the Wayback Machine instead — snapshots \
         are digest-deduped, fetched via the governed engine, and upserted with the same \
         {url}@{date} keys plus _fetched_via: \"wayback\"."
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["rules"],
                "properties": {
                    "rules": {
                        "type": "object",
                        "minProperties": 1,
                        "description": "field -> rule; each rule is {\"type\": \"css|regex|json|xpath|const\", ...type-specific keys}"
                    },
                    "urls": {
                        "type": "array",
                        "items": { "type": "string", "pattern": "^https?://" },
                        "minItems": 1,
                        "description": "URL mode: fetch these and extract. Mutually exclusive with `source`."
                    },
                    "source": {
                        "type": "object",
                        "anyOf": [
                            { "required": ["app", "dataset"] },
                            { "required": ["archive"] }
                        ],
                        "properties": {
                            "archive": {
                                "type": "object",
                                "properties": {
                                    "url": {
                                        "type": "string",
                                        "description": "Exact URL to backfill from the Wayback Machine. Mutually exclusive with url_pattern."
                                    },
                                    "url_pattern": {
                                        "type": "string",
                                        "description": "Wayback CDX wildcard/prefix target (e.g. \"example.com/products/*\"). Mutually exclusive with url."
                                    },
                                    "from": {
                                        "type": "string",
                                        "pattern": "^[0-9]{4,14}$",
                                        "description": "Lower capture-time bound: YYYYMMDDhhmmss or any digit prefix (e.g. \"2019\")."
                                    },
                                    "to": {
                                        "type": "string",
                                        "pattern": "^[0-9]{4,14}$",
                                        "description": "Upper capture-time bound, same format as `from`."
                                    },
                                    "max_snapshots": {
                                        "type": "integer",
                                        "minimum": 1,
                                        "maximum": 1000,
                                        "description": "Per-run snapshot cap (default 100, ceiling 1000); the result reports truncated: true when the range held more."
                                    },
                                    "base_url": {
                                        "type": "string",
                                        "description": "Wayback deployment base URL (default https://web.archive.org)."
                                    }
                                },
                                "description": "Wayback historical backfill: enumerate archived captures (digest-deduped, oldest first), extract each, and upsert time-series records keyed {url}@{date} tagged _fetched_via: \"wayback\". No app/dataset needed."
                            },
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
                                "description": "Backfill only: regex a version's URL must match to be extracted."
                            }
                        },
                        "description": "Source mode: read stored bodies of these records (no re-fetch)."
                    },
                    "strategy": { "type": "string", "enum": ["http", "browser", "auto"] },
                    "concurrency": { "type": "integer", "minimum": 1, "maximum": 64 },
                    "dataset": { "type": "string", "description": "Output dataset name (default \"extracted\")." }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Extract title + first heading from two live pages via CSS rules",
                    params: json!({
                        "urls": ["https://example.com/a", "https://example.com/b"],
                        "rules": {
                            "title": { "type": "css", "selector": "title" },
                            "heading": { "type": "css", "selector": "h1" }
                        },
                        "dataset": "extracted"
                    }),
                },
                ManifestExample {
                    description: "Re-extract stored crawl bodies (crawl/pages) without re-fetching",
                    params: json!({
                        "source": { "app": "crawl", "dataset": "pages" },
                        "rules": { "title": { "type": "css", "selector": "title" } },
                        "concurrency": 8
                    }),
                },
                ManifestExample {
                    description: "Wayback historical backfill: extract a price time series \
                                  from the web archive's captures of a page, before this \
                                  system ever crawled it",
                    params: json!({
                        "source": {
                            "archive": {
                                "url": "https://example.com/products/widget",
                                "from": "2019",
                                "to": "20211231",
                                "max_snapshots": 200
                            }
                        },
                        "rules": { "price": { "type": "css", "selector": ".price" } },
                        "dataset": "price_history"
                    }),
                },
                ManifestExample {
                    description: "Retroactive backfill: run new rules over every archived \
                                  version of matching pages, producing a time-series dataset",
                    params: json!({
                        "source": {
                            "app": "crawl",
                            "dataset": "pages",
                            "backfill": true,
                            "url_pattern": "^https://example\\.com/products/"
                        },
                        "rules": { "price": { "type": "css", "selector": ".price" } },
                        "dataset": "price_history"
                    }),
                },
            ],
            output_shape: Some(
                "{extracted, errors, dataset, new, changed, unchanged, removed?} — an upsert \
                 summary plus per-document extraction error counts",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let rules: RuleSet = ctx
            .params
            .get("rules")
            .cloned()
            .ok_or_else(|| Error::App("param 'rules' is required".into()))
            .and_then(|v| {
                serde_json::from_value(v).map_err(|e| Error::App(format!("bad rules: {e}")))
            })?;
        // Compile (and validate selectors/regex) once, before the fan-out.
        let compiled = Arc::new(rules.compile()?);
        let dataset = ctx
            .params
            .get("dataset")
            .and_then(Value::as_str)
            .unwrap_or("extracted")
            .to_string();

        // Two input modes: fetch live `urls`, or read stored bodies from a
        // crawl→dataset `source`. Exactly one is required.
        if ctx.params.get("source").is_some() {
            self.run_source_mode(&ctx, compiled, &dataset).await
        } else {
            self.run_urls_mode(&ctx, compiled, &dataset).await
        }
    }
}

impl Extractor {
    /// URLs mode: fetch each URL (tiered) and extract. Failed/empty fetches are
    /// attributed in `failed` and skipped — never upserted as all-null records.
    async fn run_urls_mode(
        &self,
        ctx: &AppContext,
        compiled: Arc<CompiledRuleSet>,
        dataset: &str,
    ) -> Result<Value> {
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
        let concurrency = fetch_concurrency(ctx);

        // Fetch URLs with a bounded fan-out: the governor serializes same-host
        // requests but places no global cap, so a 5000-URL/800-host list would
        // otherwise open thousands of sockets at once (fd exhaustion). Cap the
        // in-flight fetches like the sibling `crawl` app does (default 16).
        let fetcher = ctx.engines.fetch.clone();
        let fetches = urls.iter().cloned().map(|url| {
            let f = fetcher.clone();
            let mut req = FetchRequest::new(&url);
            req.strategy = strategy;
            async move {
                match f.fetch(req).await {
                    // The health gate needs to know whether the *fetch layer* was
                    // healthy, which is the winning tier's structured verdict — not
                    // whether a body came back non-empty. A bot wall returns plenty
                    // of bytes.
                    Ok(out) => {
                        let healthy = tier_won(&out);
                        (
                            url,
                            out.html.or(out.text).filter(|d| !d.is_empty()),
                            healthy,
                        )
                    }
                    Err(_) => (url, None, false),
                }
            }
        });
        let fetched_pairs: Vec<(String, Option<String>, bool)> = futures::stream::iter(fetches)
            .buffer_unordered(concurrency)
            .collect()
            .await;

        let mut keyed: Vec<SourceDoc> = Vec::new();
        let mut failed: Vec<String> = Vec::new();
        let mut fetch = FetchHealth {
            attempted: urls.len() as u32,
            ok: 0,
        };
        for (url, doc, healthy) in fetched_pairs {
            if healthy {
                fetch.ok += 1;
            }
            match doc {
                Some(d) => keyed.push(SourceDoc::live(url, d)),
                None => failed.push(url),
            }
        }

        let requested = urls.len();
        let fetched = keyed.len();
        let out = extract_and_upsert(ctx, compiled, dataset, keyed, fetch).await?;

        Ok(json!({
            "mode": "urls",
            "requested": requested,
            "fetched": fetched,
            "skipped": failed.len(),
            "failed": failed,
            "fetch_ok_rate": fetch.rate(),
            "new": out.summary.new.len(),
            "changed": out.summary.changed.len(),
            "unchanged": out.summary.unchanged,
            "fields_matched": out.matched,
            "fields_total": out.total,
            "worst_fields": out.worst,
            "health": out.health,
            "records": out.records,
        }))
    }

    /// Source mode: read stored crawl bodies from `{app, dataset, keys?}` instead
    /// of re-fetching. Keys default to the firing trigger's `_trigger.keys`, else
    /// every live record. Missing/unreadable artifacts are counted and listed
    /// per key in `missing` rather than silently producing null records.
    async fn run_source_mode(
        &self,
        ctx: &AppContext,
        compiled: Arc<CompiledRuleSet>,
        dataset: &str,
    ) -> Result<Value> {
        let source = ctx
            .params
            .get("source")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                Error::App("param 'source' must be an object {app, dataset, keys?}".into())
            })?;
        // Wayback historical backfill: `source.archive` reads bodies from the
        // web archive's CDX index instead of a local app dataset — no
        // app/dataset needed.
        if let Some(archive) = source.get("archive").and_then(Value::as_object) {
            return self
                .run_archive_backfill(ctx, compiled, dataset, archive)
                .await;
        }
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

        // Key precedence: explicit source.keys > _trigger.keys (crawl→extract via
        // a dataset trigger) > all live records in the source dataset.
        let str_array = |v: Option<&Value>| -> Option<Vec<String>> {
            v.and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
        };
        let explicit_keys = str_array(source.get("keys"))
            .or_else(|| str_array(ctx.params.pointer("/_trigger/keys")));

        // Versioned-archive resolution (crawl `page_versions`): `backfill` fans a
        // rule set over ALL archived versions matching a URL pattern (its own
        // batched runner); `as_of` / `versions:"all"` resolve the chosen keys
        // through the archive instead of the live record.
        if source
            .get("backfill")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return self
                .run_backfill(ctx, compiled, dataset, &src_app, source)
                .await;
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

        let mut keyed: Vec<SourceDoc> = Vec::new();
        let mut missing: Vec<Value> = Vec::new();
        let requested: usize;

        // Resolve the natural keys first (explicit / trigger keys, else the live
        // sweep) — every mode selects the same key set; the modes differ only in
        // WHICH stored body each key resolves to. The sweep already holds the
        // records, so it carries them along instead of re-fetching per key.
        let selected: Vec<(String, Option<Record>)> = if let Some(keys) = explicit_keys {
            requested = keys.len();
            keys.into_iter().map(|k| (k, None)).collect()
        } else {
            // No keys: every live (not removed, not gone) record.
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
                        Ok(body) => keyed.push(SourceDoc::live(key, body)),
                        Err(reason) => missing.push(json!({"key": key, "reason": reason})),
                    },
                    None => {
                        missing.push(json!({"key": key, "reason": "no record in source dataset"}))
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
                missing.push(json!({"key": key, "reason": "no record or archived version"}));
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
                    Ok(body) => keyed.push(SourceDoc {
                        key: versioned_key(&key, ts),
                        url: key.clone(),
                        observed_at: Some(ts.clone()),
                        fetched_via: None,
                        body,
                    }),
                    Err(reason) => {
                        missing.push(json!({"key": record.key, "reason": reason}));
                    }
                }
            }
        }

        let loaded = keyed.len();
        // Nothing was fetched, so the fetch layer cannot explain a bad extraction
        // and must not gate the verdict. An unreadable stored body is a corpus
        // problem, not a fetch problem, and is reported in `missing` instead.
        let out = extract_and_upsert(ctx, compiled, dataset, keyed, FetchHealth::default()).await?;

        Ok(json!({
            "mode": "source",
            "source": {"app": src_app, "dataset": src_dataset},
            "requested": requested,
            "loaded": loaded,
            "missing": missing.len(),
            "missing_keys": missing,
            "new": out.summary.new.len(),
            "changed": out.summary.changed.len(),
            "unchanged": out.summary.unchanged,
            "fields_matched": out.matched,
            "fields_total": out.total,
            "worst_fields": out.worst,
            "health": out.health,
            "records": out.records,
        }))
    }

    /// Backfill mode: fan the compiled rule set over ALL archived versions in the
    /// source app's `page_versions` dataset (optionally narrowed by a
    /// `url_pattern` regex), paging in [`SOURCE_LIST_LIMIT`] batches and
    /// extracting+upserting per batch so a large archive never accumulates in
    /// memory. Records are keyed `{url}@{observed_at_date}` and tagged
    /// `_url`/`_observed_at`, producing naturally time-series datasets. Only the
    /// archive is fanned — a plain `source` run covers the present-day bodies.
    async fn run_backfill(
        &self,
        ctx: &AppContext,
        compiled: Arc<CompiledRuleSet>,
        dataset: &str,
        src_app: &str,
        source: &serde_json::Map<String, Value>,
    ) -> Result<Value> {
        let pattern = source
            .get("url_pattern")
            .and_then(Value::as_str)
            .map(|p| {
                regex::Regex::new(p).map_err(|e| Error::App(format!("bad url_pattern '{p}': {e}")))
            })
            .transpose()?;

        let mut after: Option<(String, String)> = None;
        let mut scanned = 0usize;
        let mut skipped_pattern = 0usize;
        let mut loaded = 0usize;
        let mut batches = 0usize;
        let mut missing: Vec<Value> = Vec::new();
        let (mut new, mut changed, mut unchanged) = (0usize, 0usize, 0usize);
        let (mut fields_matched, mut fields_total) = (0u64, 0u64);
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

            let mut keyed: Vec<SourceDoc> = Vec::new();
            for v in &batch {
                if v.removed_at.is_some() {
                    continue;
                }
                scanned += 1;
                let Some(url) = v.data.get("url").and_then(Value::as_str) else {
                    missing.push(json!({"key": v.key, "reason": "version record has no url"}));
                    continue;
                };
                if pattern.as_ref().is_some_and(|re| !re.is_match(url)) {
                    skipped_pattern += 1;
                    continue;
                }
                let Some(ts) = v.data.get("fetched_at").and_then(Value::as_str) else {
                    missing
                        .push(json!({"key": v.key, "reason": "version record has no fetched_at"}));
                    continue;
                };
                match ctx.read_source_artifact(src_app, v).await {
                    Ok(body) => keyed.push(SourceDoc {
                        key: versioned_key(url, ts),
                        url: url.to_string(),
                        observed_at: Some(ts.to_string()),
                        fetched_via: None,
                        body,
                    }),
                    Err(reason) => missing.push(json!({"key": v.key, "reason": reason})),
                }
            }
            if !keyed.is_empty() {
                loaded += keyed.len();
                batches += 1;
                let out = extract_and_upsert(
                    ctx,
                    compiled.clone(),
                    dataset,
                    keyed,
                    FetchHealth::default(),
                )
                .await?;
                new += out.summary.new.len();
                changed += out.summary.changed.len();
                unchanged += out.summary.unchanged;
                fields_matched += out.matched;
                fields_total += out.total;
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
            "source": {"app": src_app, "dataset": VERSIONS_DATASET},
            "scanned": scanned,
            "skipped_pattern": skipped_pattern,
            "loaded": loaded,
            "batches": batches,
            "missing": missing_count,
            "missing_keys": missing,
            "new": new,
            "changed": changed,
            "unchanged": unchanged,
            "fields_matched": fields_matched,
            "fields_total": fields_total,
        }))
    }

    /// Wayback historical backfill (`source.archive`): enumerate the web
    /// archive's CDX captures of a URL (or Wayback wildcard pattern) across a
    /// date range, fetch each snapshot's raw body through the governed HTTP
    /// engine, run the ruleset, and upsert records keyed
    /// `{natural_key}@{snapshot_date}` tagged `_url` + `_observed_at` +
    /// `_fetched_via: "wayback"` — the same convention as the crawl-archive
    /// backfill, so a Wayback pre-history and this system's own crawl history
    /// compose into one time series. Bounded by `max_snapshots` per run, with
    /// the enumeration's honest `truncated` flag echoed in the result.
    async fn run_archive_backfill(
        &self,
        ctx: &AppContext,
        compiled: Arc<CompiledRuleSet>,
        dataset: &str,
        archive: &serde_json::Map<String, Value>,
    ) -> Result<Value> {
        let p = parse_archive_params(archive).map_err(Error::App)?;
        let engine = ArchiveEngine::new(
            &ArchiveConfig {
                enabled: true,
                base_url: p.base_url.clone(),
            },
            ctx.engines.http.clone(),
        );
        let list = engine
            .list_snapshots(&p.target, p.from.as_deref(), p.to.as_deref(), p.max_snapshots)
            .await?;
        let found = list.snapshots.len();
        let truncated = list.truncated;

        // Fetch each snapshot's raw body through the governed HTTP engine —
        // archive.org keeps the same per-host politeness as any other host —
        // with the same bounded fan-out as the urls mode.
        let concurrency = fetch_concurrency(ctx);
        let http = ctx.engines.http.clone();
        let base = engine.base_url().to_string();
        let fetches = list.snapshots.into_iter().map(|snap| {
            let http = http.clone();
            let base = base.clone();
            async move {
                let Some(dt) = pumper_engine_archive::snapshot_datetime(&snap.timestamp) else {
                    return (snap, Err("unparseable capture timestamp".to_string()));
                };
                let observed = dt.to_rfc3339();
                let url = pumper_engine_archive::snapshot_url(&base, &snap.timestamp, &snap.original);
                match http.fetch(pumper_core::HttpRequest::get(url)).await {
                    Ok(resp) if resp.is_success() && !resp.body.is_empty() => {
                        (snap, Ok((observed, resp.body)))
                    }
                    Ok(resp) => (snap, Err(format!("snapshot fetch: status {}", resp.status))),
                    Err(e) => (snap, Err(format!("snapshot fetch failed: {e}"))),
                }
            }
        });
        let fetched_pairs: Vec<_> = futures::stream::iter(fetches)
            .buffer_unordered(concurrency)
            .collect()
            .await;

        let mut keyed: Vec<SourceDoc> = Vec::new();
        let mut failed: Vec<Value> = Vec::new();
        let mut fetch = FetchHealth {
            attempted: found as u32,
            ok: 0,
        };
        for (snap, outcome) in fetched_pairs {
            match outcome {
                Ok((observed, body)) => {
                    fetch.ok += 1;
                    keyed.push(SourceDoc {
                        key: versioned_key(&snap.original, &observed),
                        url: snap.original,
                        observed_at: Some(observed),
                        fetched_via: Some("wayback"),
                        body,
                    });
                }
                Err(reason) => failed.push(json!({
                    "timestamp": snap.timestamp,
                    "url": snap.original,
                    "reason": reason,
                })),
            }
        }
        // Snapshots enumerate oldest-first but the fan-out completes out of
        // order; restore chronology so same-day re-captures upsert newest-last.
        keyed.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.observed_at.cmp(&b.observed_at)));

        let fetched = keyed.len();
        let out = extract_and_upsert(ctx, compiled, dataset, keyed, fetch).await?;

        let failed_count = failed.len();
        failed.truncate(MISSING_ECHO_LIMIT);
        Ok(json!({
            "mode": "archive",
            "target": p.target,
            "from": p.from,
            "to": p.to,
            "max_snapshots": p.max_snapshots,
            "snapshots_found": found,
            "truncated": truncated,
            "fetched": fetched,
            "failed": failed_count,
            "failed_snapshots": failed,
            "fetch_ok_rate": fetch.rate(),
            "new": out.summary.new.len(),
            "changed": out.summary.changed.len(),
            "unchanged": out.summary.unchanged,
            "fields_matched": out.matched,
            "fields_total": out.total,
            "worst_fields": out.worst,
            "health": out.health,
            "records": out.records,
        }))
    }
}

/// Cap on the per-key `missing_keys` echo in a backfill result — a large archive
/// could otherwise blow up the stored job result; `missing` keeps the full count.
const MISSING_ECHO_LIMIT: usize = 100;

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

/// Whether the fetch layer actually delivered: some tier won with a verdict of
/// `ok` and, where it has an HTTP status, a 2xx.
///
/// Keyed on the structured `TierVerdict`, never the prose escalation trail — the
/// trail is a rendered view, and matching on its text is how a router silently
/// stops working after a wording change.
fn tier_won(out: &pumper_core::FetchOutcome) -> bool {
    out.trace.iter().any(|t| {
        t.verdict == pumper_core::TierVerdict::Ok
            && t.http_status.is_none_or(|s| (200..300).contains(&s))
    })
}

#[cfg(test)]
mod tests {
    use super::summarize_reports;
    use pumper_core::{DocReport, FieldStatus};

    fn report(pairs: &[(&str, FieldStatus)]) -> DocReport {
        DocReport {
            fields: pairs
                .iter()
                .cloned()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            ..DocReport::default()
        }
    }

    #[test]
    fn aggregate_matched_total_and_worst_fields() {
        let err = FieldStatus::Error { detail: "x".into() };
        let reports = [
            report(&[
                ("title", FieldStatus::Matched),
                ("price", FieldStatus::Empty),
                ("sku", err.clone()),
            ]),
            report(&[
                ("title", FieldStatus::Matched),
                ("price", FieldStatus::Empty),
                ("sku", FieldStatus::Matched),
            ]),
        ];
        let (matched, total, worst) = summarize_reports(reports.iter());
        assert_eq!(total, 6);
        assert_eq!(matched, 3); // 2 titles + 1 sku
                                // price misses twice (worst), sku misses once with one error; title never misses.
        assert_eq!(worst.len(), 2);
        assert_eq!(worst[0]["field"], "price");
        assert_eq!(worst[0]["misses"], 2);
        assert_eq!(worst[0]["errors"], 0);
        assert_eq!(worst[0]["miss_rate"], 1.0);
        assert_eq!(worst[1]["field"], "sku");
        assert_eq!(worst[1]["misses"], 1);
        assert_eq!(worst[1]["errors"], 1);
        assert_eq!(worst[1]["miss_rate"], 0.5);
    }

    #[test]
    fn all_matched_has_no_worst_fields() {
        let reports = [report(&[
            ("a", FieldStatus::Matched),
            ("b", FieldStatus::Matched),
        ])];
        let (matched, total, worst) = summarize_reports(reports.iter());
        assert_eq!((matched, total), (2, 2));
        assert!(worst.is_empty());
    }

    #[test]
    fn versioned_key_uses_date_part() {
        use super::versioned_key;
        assert_eq!(
            versioned_key("https://a/x", "2026-07-30T10:11:12+00:00"),
            "https://a/x@2026-07-30"
        );
        // Degenerate timestamp shorter than a date: used as-is, never panics.
        assert_eq!(versioned_key("https://a/x", "2026"), "https://a/x@2026");
    }

    #[test]
    fn pick_as_of_selects_newest_at_or_before_cutoff() {
        use super::pick_as_of;
        let observed = vec![
            "2026-01-01T00:00:00+00:00".to_string(),
            "2026-03-01T00:00:00+00:00".to_string(),
            "2026-06-01T00:00:00+00:00".to_string(),
            "not-a-timestamp".to_string(), // skipped, never picked
        ];
        // Between the 2nd and 3rd observations → the 2nd wins.
        assert_eq!(
            pick_as_of(&observed, "2026-04-15T00:00:00Z").unwrap(),
            Some(1)
        );
        // Exactly at an observation → inclusive.
        assert_eq!(
            pick_as_of(&observed, "2026-06-01T00:00:00Z").unwrap(),
            Some(2)
        );
        // Before everything → None (honest miss, not a fallback to the present).
        assert_eq!(pick_as_of(&observed, "2025-12-31T23:59:59Z").unwrap(), None);
        // Bad as_of is an error, not a silent empty pick.
        assert!(pick_as_of(&observed, "yesterday").is_err());
    }

    #[test]
    fn archive_params_require_exactly_one_target() {
        use super::parse_archive_params;
        use serde_json::json;
        let obj = |v: serde_json::Value| v.as_object().unwrap().clone();
        // url alone and url_pattern alone both work.
        let p = parse_archive_params(&obj(json!({"url": "https://a/x"}))).unwrap();
        assert_eq!(p.target, "https://a/x");
        let p = parse_archive_params(&obj(json!({"url_pattern": "a.com/products/*"}))).unwrap();
        assert_eq!(p.target, "a.com/products/*");
        // Neither or both are errors, not guesses.
        assert!(parse_archive_params(&obj(json!({}))).is_err());
        assert!(
            parse_archive_params(&obj(json!({"url": "https://a/", "url_pattern": "a/*"}))).is_err()
        );
    }

    #[test]
    fn archive_params_validate_bounds_and_clamp_the_cap() {
        use super::{parse_archive_params, ARCHIVE_SNAPSHOT_CEILING, DEFAULT_MAX_SNAPSHOTS};
        use serde_json::json;
        let obj = |v: serde_json::Value| v.as_object().unwrap().clone();
        let p = parse_archive_params(&obj(
            json!({"url": "https://a/x", "from": "2019", "to": "20211231"}),
        ))
        .unwrap();
        assert_eq!(p.from.as_deref(), Some("2019"));
        assert_eq!(p.to.as_deref(), Some("20211231"));
        assert_eq!(p.max_snapshots, DEFAULT_MAX_SNAPSHOTS);
        assert_eq!(p.base_url, "https://web.archive.org");
        // Non-digit bounds are rejected before any network call.
        assert!(parse_archive_params(&obj(json!({"url": "https://a/x", "from": "2019-06"}))).is_err());
        assert!(parse_archive_params(&obj(json!({"url": "https://a/x", "to": "yesterday"}))).is_err());
        // The per-run cap clamps into 1..=ceiling — never unbounded.
        let p = parse_archive_params(&obj(json!({"url": "https://a/x", "max_snapshots": 0}))).unwrap();
        assert_eq!(p.max_snapshots, 1);
        let p = parse_archive_params(&obj(json!({"url": "https://a/x", "max_snapshots": 999999})))
            .unwrap();
        assert_eq!(p.max_snapshots, ARCHIVE_SNAPSHOT_CEILING);
    }

    #[test]
    fn wayback_records_carry_the_m42_key_and_tag_convention() {
        use super::{tag_record, versioned_key};
        use serde_json::json;
        // Key: {natural_key}@{snapshot_date} — the crawl-archive convention.
        let observed = "2020-05-01T12:30:00+00:00";
        assert_eq!(
            versioned_key("https://a/x", observed),
            "https://a/x@2020-05-01"
        );
        // Tags: _url + _observed_at + _fetched_via: "wayback".
        let mut rec = json!({"price": "9.99"});
        tag_record(
            &mut rec,
            "https://a/x".into(),
            Some(observed.into()),
            Some("wayback"),
        );
        assert_eq!(rec["_url"], "https://a/x");
        assert_eq!(rec["_observed_at"], observed);
        assert_eq!(rec["_fetched_via"], "wayback");
        // A present-day record gets neither historical tag — the convention
        // stays composable with the crawl history (which sets no _fetched_via).
        let mut rec = json!({"price": "9.99"});
        tag_record(&mut rec, "https://a/x".into(), None, None);
        assert_eq!(rec["_url"], "https://a/x");
        assert!(rec.get("_observed_at").is_none());
        assert!(rec.get("_fetched_via").is_none());
    }

    #[test]
    fn concurrency_defaults_clamps_and_overrides() {
        use super::{parse_concurrency, DEFAULT_FETCH_CONCURRENCY};
        use serde_json::json;
        // Absent → default.
        assert_eq!(parse_concurrency(&json!({})), DEFAULT_FETCH_CONCURRENCY);
        // Explicit override honored.
        assert_eq!(parse_concurrency(&json!({ "concurrency": 4 })), 4);
        // Zero clamps up to 1 (never an unbounded/idle stream).
        assert_eq!(parse_concurrency(&json!({ "concurrency": 0 })), 1);
        // Non-numeric → default.
        assert_eq!(
            parse_concurrency(&json!({ "concurrency": "lots" })),
            DEFAULT_FETCH_CONCURRENCY
        );
    }
}
