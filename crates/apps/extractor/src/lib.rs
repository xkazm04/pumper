//! Generic extraction app: fetch a list of URLs (tiered) and run a declarative
//! rule set over all of them in parallel across every CPU core. Showcases the
//! no-GIL, SIMD extraction engine — the fetched documents are parsed and
//! extracted concurrently in one process, then deduped into a dataset.

use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::{
    extract_batch_with_report, AppContext, CompiledRuleSet, DocReport, Error, FetchRequest,
    FetchStrategy, FieldStatus, Result, RuleSet, ScrapeApp,
};
use serde_json::{json, Value};

pub struct Extractor;

/// Aggregate the per-document reports into a quality signal for the job result:
/// how many field extractions matched out of the total attempted, plus the
/// fields with the highest miss rate (an empty or errored extraction is a miss).
/// Returns `(matched, total, worst_fields)`; `worst_fields` lists only fields
/// that missed at least once, worst first.
fn summarize_reports(reports: &[DocReport]) -> (u64, u64, Vec<Value>) {
    let mut matched: u64 = 0;
    let mut total: u64 = 0;
    // field -> (misses, errors)
    let mut misses: std::collections::BTreeMap<&str, (u64, u64)> = std::collections::BTreeMap::new();
    for report in reports {
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
    let docs = reports.len().max(1) as f64;
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
            let url = url.clone();
            let mut req = FetchRequest::new(&url);
            req.strategy = strategy;
            async move {
                // A failed (or empty) fetch is attributed to its URL and skipped
                // — never upserted as an all-null record that pollutes the dataset.
                match f.fetch(req).await {
                    Ok(out) => (url, out.html.or(out.text).filter(|d| !d.is_empty())),
                    Err(_) => (url, None),
                }
            }
        });
        let fetched_pairs: Vec<(String, Option<String>)> =
            futures::future::join_all(fetches).await;

        let mut keys: Vec<String> = Vec::new();
        let mut docs: Vec<String> = Vec::new();
        let mut failed: Vec<String> = Vec::new();
        for (url, doc) in fetched_pairs {
            match doc {
                Some(d) => {
                    keys.push(url);
                    docs.push(d);
                }
                None => failed.push(url),
            }
        }

        // Extract the surviving docs in parallel across all cores, with a
        // per-field quality report per document.
        let reported = run_extraction(compiled.clone(), docs).await?;
        let (matched, total, worst) =
            summarize_reports(&reported.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>());

        // Tag each record with its source URL and upsert for dedup.
        let mut records: Vec<Value> = Vec::with_capacity(reported.len());
        let items: Vec<(String, Value)> = keys
            .iter()
            .zip(reported.into_iter())
            .map(|(url, (mut rec, _))| {
                if let Value::Object(map) = &mut rec {
                    map.insert("_url".into(), Value::String(url.clone()));
                }
                records.push(rec.clone());
                (url.clone(), rec)
            })
            .collect();
        let summary = ctx.upsert_many(&dataset, &items).await?;

        Ok(json!({
            "mode": "urls",
            "requested": urls.len(),
            "fetched": keys.len(),
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
        let (matched, total, worst) = summarize_reports(&reports);
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
        let (matched, total, worst) = summarize_reports(&reports);
        assert_eq!((matched, total), (2, 2));
        assert!(worst.is_empty());
    }
}
