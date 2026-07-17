//! Generic extraction app: fetch a list of URLs (tiered) and run a declarative
//! rule set over all of them in parallel across every CPU core. Showcases the
//! no-GIL, SIMD extraction engine — the fetched documents are parsed and
//! extracted concurrently in one process, then deduped into a dataset.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use pumper_core::{
    extract_batch_with_report, AppContext, CompiledRuleSet, DocReport, Error, FetchRequest,
    FetchStrategy, FieldStatus, Record, Result, RuleSet, ScrapeApp, UpsertSummary,
};
use serde_json::{json, Value};

pub struct Extractor;

/// Max live records pulled from a source dataset when no explicit `keys` (and no
/// `_trigger.keys`) narrow the set — bounds the dataset read and the fan-out.
const SOURCE_LIST_LIMIT: i64 = 10_000;

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
fn summarize_reports<'a>(reports: impl IntoIterator<Item = &'a DocReport>) -> (u64, u64, Vec<Value>) {
    let mut matched: u64 = 0;
    let mut total: u64 = 0;
    let mut doc_count: u64 = 0;
    // field -> (misses, errors)
    let mut misses: std::collections::BTreeMap<&str, (u64, u64)> = std::collections::BTreeMap::new();
    for report in reports {
        doc_count += 1;
        for (field, status) in &report.fields {
            total += 1;
            let entry = misses.entry(field.as_str()).or_default();
            match status {
                FieldStatus::Matched => matched += 1,
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
        b["misses"].as_u64().cmp(&a["misses"].as_u64()).then_with(|| {
            a["field"].as_str().cmp(&b["field"].as_str())
        })
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
    keyed: Vec<(String, String)>,
) -> Result<(Vec<Value>, u64, u64, Vec<Value>, UpsertSummary)> {
    // Split keys from bodies without copying either — `keyed` is owned and dropped
    // here anyway (was: `.iter().map(|(_,d)| d.clone())`, deep-cloning every HTML
    // body and roughly doubling peak RSS over the whole batch).
    let (keys, docs): (Vec<String>, Vec<String>) = keyed.into_iter().unzip();
    let reported = run_extraction(compiled, docs).await?;
    // Borrow the reports rather than deep-cloning each into a throwaway Vec.
    let (matched, total, worst) = summarize_reports(reported.iter().map(|(_, r)| r));

    let mut records: Vec<Value> = Vec::with_capacity(reported.len());
    let items: Vec<(String, Value)> = keys
        .into_iter()
        .zip(reported)
        .map(|(key, (mut rec, _))| {
            if let Value::Object(map) = &mut rec {
                map.insert("_url".into(), Value::String(key.clone()));
            }
            records.push(rec.clone());
            (key, rec)
        })
        .collect();
    let summary = ctx.upsert_many(dataset, &items).await?;
    Ok((records, matched, total, worst, summary))
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
         the firing trigger's _trigger.keys, else all live records."
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let rules: RuleSet = ctx
            .params
            .get("rules")
            .cloned()
            .ok_or_else(|| Error::App("param 'rules' is required".into()))
            .and_then(|v| serde_json::from_value(v).map_err(|e| Error::App(format!("bad rules: {e}"))))?;
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
                    Ok(out) => (url, out.html.or(out.text).filter(|d| !d.is_empty())),
                    Err(_) => (url, None),
                }
            }
        });
        let fetched_pairs: Vec<(String, Option<String>)> = futures::stream::iter(fetches)
            .buffer_unordered(concurrency)
            .collect()
            .await;

        let mut keyed: Vec<(String, String)> = Vec::new();
        let mut failed: Vec<String> = Vec::new();
        for (url, doc) in fetched_pairs {
            match doc {
                Some(d) => keyed.push((url, d)),
                None => failed.push(url),
            }
        }

        let requested = urls.len();
        let fetched = keyed.len();
        let (records, matched, total, worst, summary) =
            extract_and_upsert(ctx, compiled, dataset, keyed).await?;

        Ok(json!({
            "mode": "urls",
            "requested": requested,
            "fetched": fetched,
            "skipped": failed.len(),
            "failed": failed,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "fields_matched": matched,
            "fields_total": total,
            "worst_fields": worst,
            "records": records,
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

        // Key precedence: explicit source.keys > _trigger.keys (crawl→extract via
        // a dataset trigger) > all live records in the source dataset.
        let str_array = |v: Option<&Value>| -> Option<Vec<String>> {
            v.and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        };
        let explicit_keys = str_array(source.get("keys"))
            .or_else(|| str_array(ctx.params.pointer("/_trigger/keys")));

        let mut keyed: Vec<(String, String)> = Vec::new();
        let mut missing: Vec<Value> = Vec::new();
        let requested: usize;

        if let Some(keys) = explicit_keys {
            // Named keys: fetch each record, then read its stored body. Both a
            // missing record and an unreadable artifact are reported per key.
            requested = keys.len();
            for key in keys {
                match ctx.datasets.get(&src_app, &src_dataset, &key).await? {
                    Some(r) => match ctx.read_source_artifact(&src_app, &r).await {
                        Ok(body) => keyed.push((key, body)),
                        Err(reason) => missing.push(json!({"key": key, "reason": reason})),
                    },
                    None => missing
                        .push(json!({"key": key, "reason": "no record in source dataset"})),
                }
            }
        } else {
            // No keys: process every live (not removed, not gone) record.
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
                    Err(reason) => missing.push(json!({"key": r.key, "reason": reason})),
                }
            }
        }

        let loaded = keyed.len();
        let (out_records, matched, total, worst, summary) =
            extract_and_upsert(ctx, compiled, dataset, keyed).await?;

        Ok(json!({
            "mode": "source",
            "source": {"app": src_app, "dataset": src_dataset},
            "requested": requested,
            "loaded": loaded,
            "missing": missing.len(),
            "missing_keys": missing,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "fields_matched": matched,
            "fields_total": total,
            "worst_fields": worst,
            "records": out_records,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::summarize_reports;
    use pumper_core::{DocReport, FieldStatus};

    fn report(pairs: &[(&str, FieldStatus)]) -> DocReport {
        DocReport { fields: pairs.iter().cloned().map(|(k, v)| (k.to_string(), v)).collect() }
    }

    #[test]
    fn aggregate_matched_total_and_worst_fields() {
        let err = FieldStatus::Error { detail: "x".into() };
        let reports = vec![
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
        let reports = vec![report(&[("a", FieldStatus::Matched), ("b", FieldStatus::Matched)])];
        let (matched, total, worst) = summarize_reports(reports.iter());
        assert_eq!((matched, total), (2, 2));
        assert!(worst.is_empty());
    }

    #[test]
    fn concurrency_defaults_clamps_and_overrides() {
        use serde_json::json;
        use super::{parse_concurrency, DEFAULT_FETCH_CONCURRENCY};
        // Absent → default.
        assert_eq!(parse_concurrency(&json!({})), DEFAULT_FETCH_CONCURRENCY);
        // Explicit override honored.
        assert_eq!(parse_concurrency(&json!({ "concurrency": 4 })), 4);
        // Zero clamps up to 1 (never an unbounded/idle stream).
        assert_eq!(parse_concurrency(&json!({ "concurrency": 0 })), 1);
        // Non-numeric → default.
        assert_eq!(parse_concurrency(&json!({ "concurrency": "lots" })), DEFAULT_FETCH_CONCURRENCY);
    }
}
