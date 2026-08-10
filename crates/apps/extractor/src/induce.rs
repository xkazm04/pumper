//! Induce mode (`induce` param, M09): zero-shot wrapper induction over STORED
//! bodies — statistically mine a CANDIDATE rule set (a top-level `each`) from a
//! set of same-template pages the crawl already paid for. No LLM, no
//! demonstrations; pure-Rust heuristics in `pumper_core::induce`.
//!
//! STRICTLY READ-ONLY, like replay: this mode never writes a dataset record.
//! Its output is the job result JSON plus an `induced-ruleset.json` job
//! artifact carrying the candidate rules and their per-field support evidence
//! for human review. The intended workflow is induce → review → validate the
//! candidate against the corpus with the `replay` mode → only then deploy.

use pumper_core::{AppContext, Error, InduceOptions, Record, Result};
use serde_json::{json, Map, Value};

use crate::{MISSING_ECHO_LIMIT, SOURCE_LIST_LIMIT};

/// Default / ceiling on pages one induction run reads. Induction wants a
/// same-template sample, not the whole corpus — 50 pages is already plenty of
/// statistical support.
pub(crate) const DEFAULT_MAX_PAGES: usize = 50;
pub(crate) const MAX_PAGES_CEILING: usize = 500;

/// Parsed and validated `induce` params.
pub(crate) struct InduceParams {
    /// Explicit stored-page keys (crawl keys ARE canonical URLs)…
    pub urls: Option<Vec<String>>,
    /// …or a regex over record keys selecting the page set.
    pub url_pattern: Option<String>,
    pub app: String,
    pub dataset: String,
    pub min_support: f64,
    pub min_instances: usize,
    pub max_fields: usize,
    pub max_pages: usize,
}

/// Pure parse/validation of the `induce` object: exactly `urls` or
/// `url_pattern` (the page set must be intentional — same-template pages, not
/// "whatever is in the store"); thresholds clamp to sane ranges.
pub(crate) fn parse_induce_params(
    induce: &Map<String, Value>,
) -> std::result::Result<InduceParams, String> {
    let urls: Option<Vec<String>> = induce.get("urls").and_then(Value::as_array).map(|a| {
        a.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    });
    let url_pattern = induce
        .get("url_pattern")
        .and_then(Value::as_str)
        .map(str::to_string);
    match (&urls, &url_pattern) {
        (Some(u), None) if u.is_empty() => {
            return Err("induce.urls must be a non-empty array of strings".into())
        }
        (Some(_), Some(_)) => {
            return Err("induce.urls and induce.url_pattern are mutually exclusive".into())
        }
        (None, None) => {
            return Err(
                "induce requires urls or url_pattern — induction needs an intentional \
                 set of same-template pages"
                    .into(),
            )
        }
        _ => {}
    }
    let defaults = InduceOptions::default();
    let min_support = induce
        .get("min_support")
        .and_then(Value::as_f64)
        .unwrap_or(defaults.min_support);
    if !(0.05..=1.0).contains(&min_support) {
        return Err(format!(
            "induce.min_support must be in 0.05..=1.0, got {min_support}"
        ));
    }
    let min_instances = induce
        .get("min_instances")
        .and_then(Value::as_u64)
        .map(|n| (n as usize).max(2))
        .unwrap_or(defaults.min_instances);
    let max_fields = induce
        .get("max_fields")
        .and_then(Value::as_u64)
        .map(|n| (n as usize).clamp(1, 32))
        .unwrap_or(defaults.max_fields);
    let max_pages = induce
        .get("max_pages")
        .and_then(Value::as_u64)
        .map(|n| (n.max(1) as usize).min(MAX_PAGES_CEILING))
        .unwrap_or(DEFAULT_MAX_PAGES);
    Ok(InduceParams {
        urls,
        url_pattern,
        app: induce
            .get("app")
            .and_then(Value::as_str)
            .unwrap_or("crawl")
            .to_string(),
        dataset: induce
            .get("dataset")
            .and_then(Value::as_str)
            .unwrap_or("pages")
            .to_string(),
        min_support,
        min_instances,
        max_fields,
        max_pages,
    })
}

/// The induce runner: load the selected stored bodies (latest artifacts, no
/// re-fetch), run the induction heuristics off the async runtime, and return
/// the candidate rule set + evidence. Never writes a dataset — see module doc.
pub(crate) async fn run_induce(ctx: &AppContext, induce: &Map<String, Value>) -> Result<Value> {
    let p = parse_induce_params(induce).map_err(Error::App)?;

    // Resolve the page set to live records.
    let mut missing: Vec<Value> = Vec::new();
    let live = |r: &Record| {
        r.removed_at.is_none() && !r.data.get("gone").and_then(Value::as_bool).unwrap_or(false)
    };
    let (records, matching): (Vec<Record>, usize) = if let Some(urls) = &p.urls {
        let mut out = Vec::with_capacity(urls.len().min(p.max_pages));
        for url in urls.iter().take(p.max_pages) {
            match ctx.datasets.get(&p.app, &p.dataset, url).await? {
                Some(r) if live(&r) => out.push(r),
                Some(_) => missing.push(json!({"key": url, "reason": "record removed/gone"})),
                None => missing.push(json!({"key": url, "reason": "no record in source dataset"})),
            }
        }
        let n = urls.len();
        (out, n)
    } else {
        let pattern = p.url_pattern.as_deref().unwrap_or_default();
        let re = regex::Regex::new(pattern)
            .map_err(|e| Error::App(format!("bad induce.url_pattern '{pattern}': {e}")))?;
        let mut records: Vec<Record> = ctx
            .datasets
            .list(&p.app, &p.dataset, SOURCE_LIST_LIMIT)
            .await?
            .into_iter()
            .filter(|r| live(r) && re.is_match(&r.key))
            .collect();
        let matching = records.len();
        records.truncate(p.max_pages);
        (records, matching)
    };
    let truncated = matching > p.max_pages;

    // Load the stored bodies (latest artifacts only — single-page-set v1).
    let mut keys: Vec<String> = Vec::new();
    let mut docs: Vec<String> = Vec::new();
    for r in records {
        match ctx.read_source_artifact(&p.app, &r).await {
            Ok(body) => {
                keys.push(r.key);
                docs.push(body);
            }
            Err(reason) => missing.push(json!({"key": r.key, "reason": reason})),
        }
    }

    let opts = InduceOptions {
        min_support: p.min_support,
        min_instances: p.min_instances,
        max_fields: p.max_fields,
    };
    let loaded = docs.len();
    let induction = tokio::task::spawn_blocking(move || pumper_core::induce(&docs, &opts))
        .await
        .map_err(|e| Error::App(format!("induce task failed: {e}")))??;

    let missing_count = missing.len();
    missing.truncate(MISSING_ECHO_LIMIT);
    let base = json!({
        "mode": "induce",
        "source": {"app": p.app, "dataset": p.dataset},
        "url_pattern": p.url_pattern,
        "min_support": p.min_support,
        "min_instances": p.min_instances,
        "pages_matching": matching,
        "truncated": truncated,
        "docs": loaded,
        "missing": missing_count,
        "missing_keys": missing,
    });
    let Some(ind) = induction else {
        let mut result = base;
        result["induced"] = json!(false);
        result["reason"] = json!(format!(
            "no repeating container cleared the thresholds (>= {} instances per page on >= {} \
             of the pages, with at least one varying field slot) — the pages may not share a \
             template, or the listing markup is class-less",
            p.min_instances, p.min_support
        ));
        return Ok(result);
    };

    // The one write this mode performs: a per-job FILE artifact (not a dataset
    // record) carrying the candidate rules + evidence, durable for review.
    let evidence =
        serde_json::to_value(&ind).map_err(|e| Error::App(format!("serialize induction: {e}")))?;
    let bytes = serde_json::to_vec_pretty(&evidence)
        .map_err(|e| Error::App(format!("serialize induction: {e}")))?;
    ctx.save_artifact("induced-ruleset.json", &bytes).await?;

    let mut result = base;
    result["induced"] = json!(true);
    result["rules"] = evidence["rules"].clone();
    result["container"] = evidence["container"].clone();
    result["fields"] = evidence["fields"].clone();
    result["candidates_considered"] = evidence["candidates_considered"].clone();
    result["artifact"] = json!("induced-ruleset.json");
    // Chaining guidance: these rules are CANDIDATES for human review — the
    // replay mode validates them against the stored corpus before deployment.
    result["next"] = json!(format!(
        "Review the candidate rules, then validate them read-only against the corpus with the \
         replay mode: {{\"replay\": {{\"rules\": <rules above>, \"against\": {{\"app\": \"{}\", \
         \"dataset\": \"{}\"{}}}}}}}. Induced rules are suggestions and are never auto-deployed.",
        p.app,
        p.dataset,
        p.url_pattern
            .as_deref()
            .map(|re| format!(", \"url_pattern\": \"{re}\""))
            .unwrap_or_default(),
    ));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{parse_induce_params, DEFAULT_MAX_PAGES, MAX_PAGES_CEILING};
    use serde_json::{json, Map, Value};

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn requires_exactly_one_page_selector() {
        // Neither, both, and empty urls are all errors — never a silent
        // "induce over everything".
        assert!(parse_induce_params(&obj(json!({}))).is_err());
        assert!(parse_induce_params(&obj(json!({
            "urls": ["https://a/1"], "url_pattern": "^https://a/"
        })))
        .is_err());
        assert!(parse_induce_params(&obj(json!({"urls": []}))).is_err());
        let p = parse_induce_params(&obj(json!({"urls": ["https://a/1", "https://a/2"]}))).unwrap();
        assert_eq!(p.urls.as_ref().unwrap().len(), 2);
        let p = parse_induce_params(&obj(json!({"url_pattern": "^https://a/p/"}))).unwrap();
        assert_eq!(p.url_pattern.as_deref(), Some("^https://a/p/"));
    }

    #[test]
    fn defaults_match_core_and_source_defaults() {
        let p = parse_induce_params(&obj(json!({"url_pattern": "x"}))).unwrap();
        assert_eq!((p.app.as_str(), p.dataset.as_str()), ("crawl", "pages"));
        assert_eq!(p.min_support, 0.6);
        assert_eq!(p.min_instances, 3);
        assert_eq!(p.max_fields, 12);
        assert_eq!(p.max_pages, DEFAULT_MAX_PAGES);
    }

    #[test]
    fn thresholds_validate_and_clamp() {
        // min_support outside (0.05..=1.0) is an error, not a guess.
        assert!(
            parse_induce_params(&obj(json!({"url_pattern": "x", "min_support": 0.0}))).is_err()
        );
        assert!(
            parse_induce_params(&obj(json!({"url_pattern": "x", "min_support": 1.5}))).is_err()
        );
        // min_instances floors at 2; max_pages clamps into 1..=ceiling.
        let p = parse_induce_params(&obj(json!({
            "url_pattern": "x", "min_instances": 0, "max_pages": 999999, "max_fields": 0
        })))
        .unwrap();
        assert_eq!(p.min_instances, 2);
        assert_eq!(p.max_pages, MAX_PAGES_CEILING);
        assert_eq!(p.max_fields, 1);
    }
}
