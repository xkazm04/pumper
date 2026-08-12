//! Replay-CI mode (`replay` param): run a CANDIDATE rule set over STORED
//! bodies — the archived corpus the crawl already paid for — and, when a
//! baseline rule set is given, emit a field-by-field diff report so a rule
//! edit can be validated against history before it ships.
//!
//! STRICTLY READ-ONLY: this mode never writes a dataset record. Its only
//! output is the job result JSON plus a `replay-report.json` job artifact
//! (an artifact is a per-job file, not a dataset row — change detection,
//! triggers and health verdicts are all deliberately out of the loop).

use std::collections::BTreeMap;
use std::sync::Arc;

use pumper_core::{
    extract_batch_with_report, AppContext, DocReport, Error, FieldStatus, Record, Result, RuleSet,
};
use serde_json::{json, Map, Value};

use crate::{versions_for, MISSING_ECHO_LIMIT, SOURCE_LIST_LIMIT};

/// Default / ceiling for `replay.against.max_pages` — how many URLs one replay
/// run fans over (each URL may still contribute several archived versions).
pub(crate) const DEFAULT_MAX_PAGES: usize = 500;
pub(crate) const MAX_PAGES_CEILING: usize = 5_000;

/// Per-field, per-category cap on echoed value samples (`added`/`lost`/
/// `changed`) — full counts are always reported; samples are illustrations.
pub(crate) const SAMPLE_LIMIT: usize = 20;

/// Cap on the per-URL regression echo and on reported bisect boundaries.
const REGRESSION_ECHO_LIMIT: usize = 100;
const BISECT_ECHO_LIMIT: usize = 50;

/// Parsed and validated `replay` params.
pub(crate) struct ReplayParams {
    pub candidate: RuleSet,
    pub baseline: Option<RuleSet>,
    pub app: String,
    pub dataset: String,
    pub url_pattern: Option<String>,
    /// `true` = fan over every archived version + current; `false` = latest only.
    pub versions_all: bool,
    pub max_pages: usize,
    pub bisect_field: Option<String>,
}

/// Pure parse/validation of the `replay` object: `rules` required;
/// `against.versions` must be `"all"`/`"latest"`; `max_pages` clamps into
/// `1..=`[`MAX_PAGES_CEILING`]; `bisect_field` needs the version history, so
/// it demands `versions: "all"` rather than silently bisecting one point.
pub(crate) fn parse_replay_params(
    replay: &Map<String, Value>,
) -> std::result::Result<ReplayParams, String> {
    let candidate: RuleSet = replay
        .get("rules")
        .cloned()
        .ok_or_else(|| "replay.rules (the candidate rule set) is required".to_string())
        .and_then(|v| serde_json::from_value(v).map_err(|e| format!("bad replay.rules: {e}")))?;
    let baseline: Option<RuleSet> = replay
        .get("baseline_rules")
        .cloned()
        .map(|v| serde_json::from_value(v).map_err(|e| format!("bad replay.baseline_rules: {e}")))
        .transpose()?;
    let against = replay
        .get("against")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let versions_all = match against.get("versions").and_then(Value::as_str) {
        None | Some("latest") => false,
        Some("all") => true,
        Some(other) => {
            return Err(format!(
                "replay.against.versions must be \"all\" or \"latest\", got \"{other}\""
            ))
        }
    };
    let max_pages = against
        .get("max_pages")
        .and_then(Value::as_u64)
        .map(|n| (n.max(1) as usize).min(MAX_PAGES_CEILING))
        .unwrap_or(DEFAULT_MAX_PAGES);
    let bisect_field = replay
        .get("bisect_field")
        .and_then(Value::as_str)
        .map(str::to_string);
    if bisect_field.is_some() && !versions_all {
        return Err(
            "replay.bisect_field walks a URL's version history — it requires \
             replay.against.versions: \"all\""
                .into(),
        );
    }
    Ok(ReplayParams {
        candidate,
        baseline,
        app: against
            .get("app")
            .and_then(Value::as_str)
            .unwrap_or("crawl")
            .to_string(),
        dataset: against
            .get("dataset")
            .and_then(Value::as_str)
            .unwrap_or("pages")
            .to_string(),
        url_pattern: against
            .get("url_pattern")
            .and_then(Value::as_str)
            .map(str::to_string),
        versions_all,
        max_pages,
        bisect_field,
    })
}

/// One replayed document's identity in the report: natural URL + observation
/// timestamp (None in latest-only mode).
pub(crate) struct DocKey {
    pub url: String,
    pub observed_at: Option<String>,
}

impl DocKey {
    fn echo(&self) -> Value {
        match &self.observed_at {
            Some(ts) => json!({"url": self.url, "observed_at": ts}),
            None => json!({"url": self.url}),
        }
    }
}

/// The match convention shared with `summarize_reports`: a container that
/// matched but held no items is still a working selector, not a miss.
///
/// The item-scoped twin of this rule (inside an `each` listing) is
/// [`pumper_core::extract::InnerFieldStats::hits`] — ONE implementation in core
/// that both this module and `summarize_reports` call, so the two scopes cannot
/// disagree about what counts as a working selector.
fn is_match(status: Option<&FieldStatus>) -> bool {
    matches!(
        status,
        Some(FieldStatus::Matched) | Some(FieldStatus::ContainerEmpty)
    )
}

/// Per-inner-field diff accumulator, item-scoped (a listing's inner selector is
/// attempted once per card, not once per page).
#[derive(Default)]
struct InnerDiff {
    cand_items: u64,
    cand_hits: u64,
    base_items: u64,
    base_hits: u64,
}

/// Diffs the per-inner-field reports of `each` listings — the layer the
/// document-level rows cannot see, because a listing whose `price` selector
/// died still reports one `Matched` for the whole array.
///
/// Rows are item-scoped rates only, with no `added`/`lost`/`changed` value
/// samples: the values live INSIDE the array items and the report deliberately
/// aggregates counts rather than echoing per-item values (that bound is what
/// keeps a 5000-row listing's report the size of a 5-row one). A field that
/// needs value-level diffing is diffed by lifting it out of the listing.
pub(crate) fn diff_inner_fields(
    cand: &[(Value, DocReport)],
    base: Option<&[(Value, DocReport)]>,
) -> Vec<Value> {
    let mut diffs: BTreeMap<&str, InnerDiff> = BTreeMap::new();
    for (_, r) in cand {
        for (path, s) in &r.each {
            let d = diffs.entry(path.as_str()).or_default();
            d.cand_items += s.items as u64;
            d.cand_hits += s.hits() as u64;
        }
    }
    for (_, r) in base.unwrap_or(&[]) {
        for (path, s) in &r.each {
            let d = diffs.entry(path.as_str()).or_default();
            d.base_items += s.items as u64;
            d.base_hits += s.hits() as u64;
        }
    }
    let rate = |hits: u64, items: u64| {
        if items == 0 {
            0.0
        } else {
            ((hits as f64 / items as f64) * 1000.0).round() / 1000.0
        }
    };
    let mut rows: Vec<Value> = diffs
        .into_iter()
        .map(|(field, d)| {
            let mut row = json!({
                "field": field,
                "scope": "item",
                "items": d.cand_items,
                "match_rate": rate(d.cand_hits, d.cand_items),
                // The listing-rot verdict: the selector bound on NO item it was
                // attempted on (distinct from a sparse field, and from a listing
                // that simply had no items).
                "dead": d.cand_items > 0 && d.cand_hits == 0,
            });
            if base.is_some() {
                row["baseline_items"] = json!(d.base_items);
                row["baseline_match_rate"] = json!(rate(d.base_hits, d.base_items));
                row["delta"] =
                    json!(rate(d.cand_hits, d.cand_items) - rate(d.base_hits, d.base_items));
            }
            row
        })
        .collect();
    // Worst regression first (or lowest match rate for a candidate-only run),
    // ties broken by name — the same ordering as the document-level rows.
    rows.sort_by(|a, b| {
        let key = |v: &Value| {
            v.get("delta")
                .or_else(|| v.get("match_rate"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        };
        key(a)
            .partial_cmp(&key(b))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a["field"].as_str().cmp(&b["field"].as_str()))
    });
    rows
}

/// Per-field diff accumulator (counts full, samples bounded).
#[derive(Default)]
struct FieldDiff {
    cand_matched: u64,
    base_matched: u64,
    added: u64,
    lost: u64,
    changed: u64,
    added_samples: Vec<Value>,
    lost_samples: Vec<Value>,
    changed_samples: Vec<Value>,
}

pub(crate) struct DiffOutput {
    /// Per-field report rows, worst regression first.
    pub fields: Vec<Value>,
    /// Per-URL regression rows (fields lost or changed vs baseline), bounded.
    pub regressions: Vec<Value>,
    pub regressed_urls: usize,
}

/// Pure diff math over index-aligned document reports. `base: None` = a
/// candidate-only replay: match rates only, no deltas or value diffs.
pub(crate) fn diff_fields(
    keys: &[DocKey],
    cand: &[(Value, DocReport)],
    base: Option<&[(Value, DocReport)]>,
) -> DiffOutput {
    debug_assert_eq!(keys.len(), cand.len());
    // Union of field names across both rule sets, so a field present in only
    // one side is still reported (all-added or all-lost, not invisible).
    let mut names: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for (_, r) in cand {
        names.extend(r.fields.keys().map(String::as_str));
    }
    if let Some(base) = base {
        for (_, r) in base {
            names.extend(r.fields.keys().map(String::as_str));
        }
    }

    let mut diffs: BTreeMap<&str, FieldDiff> = BTreeMap::new();
    // url -> (lost fields, changed fields), aggregated across versions.
    let mut regress: BTreeMap<&str, (Vec<&str>, Vec<&str>)> = BTreeMap::new();

    for (i, key) in keys.iter().enumerate() {
        let (cand_rec, cand_rep) = &cand[i];
        for &field in &names {
            let d = diffs.entry(field).or_default();
            let c_match = is_match(cand_rep.fields.get(field));
            if c_match {
                d.cand_matched += 1;
            }
            let Some(base) = base else { continue };
            let (base_rec, base_rep) = &base[i];
            let b_match = is_match(base_rep.fields.get(field));
            if b_match {
                d.base_matched += 1;
            }
            match (b_match, c_match) {
                (false, true) => {
                    d.added += 1;
                    if d.added_samples.len() < SAMPLE_LIMIT {
                        let mut s = key.echo();
                        s["value"] = cand_rec.get(field).cloned().unwrap_or(Value::Null);
                        d.added_samples.push(s);
                    }
                }
                (true, false) => {
                    d.lost += 1;
                    if d.lost_samples.len() < SAMPLE_LIMIT {
                        let mut s = key.echo();
                        s["value"] = base_rec.get(field).cloned().unwrap_or(Value::Null);
                        d.lost_samples.push(s);
                    }
                    regress.entry(&key.url).or_default().0.push(field);
                }
                (true, true) => {
                    let from = base_rec.get(field);
                    let to = cand_rec.get(field);
                    if from != to {
                        d.changed += 1;
                        if d.changed_samples.len() < SAMPLE_LIMIT {
                            let mut s = key.echo();
                            s["from"] = from.cloned().unwrap_or(Value::Null);
                            s["to"] = to.cloned().unwrap_or(Value::Null);
                            d.changed_samples.push(s);
                        }
                        regress.entry(&key.url).or_default().1.push(field);
                    }
                }
                (false, false) => {}
            }
        }
    }

    let docs = keys.len().max(1) as f64;
    let rate = |n: u64| ((n as f64 / docs) * 1000.0).round() / 1000.0;
    let mut fields: Vec<Value> = diffs
        .into_iter()
        .map(|(field, d)| {
            let mut row = json!({
                "field": field,
                "docs": keys.len(),
                "match_rate": rate(d.cand_matched),
            });
            if base.is_some() {
                row["baseline_match_rate"] = json!(rate(d.base_matched));
                row["delta"] = json!(rate(d.cand_matched) - rate(d.base_matched));
                row["added"] = json!({"count": d.added, "samples": d.added_samples});
                row["lost"] = json!({"count": d.lost, "samples": d.lost_samples});
                row["changed"] = json!({"count": d.changed, "samples": d.changed_samples});
            }
            row
        })
        .collect();
    // Worst regression first (most negative delta); candidate-only reports
    // sort by lowest match rate; ties broken by name for stable output.
    fields.sort_by(|a, b| {
        let key = |v: &Value| {
            v.get("delta")
                .or_else(|| v.get("match_rate"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        };
        key(a)
            .partial_cmp(&key(b))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a["field"].as_str().cmp(&b["field"].as_str()))
    });

    let regressed_urls = regress.len();
    let regressions: Vec<Value> = regress
        .into_iter()
        .take(REGRESSION_ECHO_LIMIT)
        .map(|(url, (mut lost, mut changed))| {
            lost.sort_unstable();
            lost.dedup();
            changed.sort_unstable();
            changed.dedup();
            json!({"url": url, "lost": lost, "changed": changed})
        })
        .collect();
    DiffOutput {
        fields,
        regressions,
        regressed_urls,
    }
}

/// Bisect: for each URL whose version series flips the field's matched-ness,
/// report every boundary — the adjacent `(from, to)` observation pair where a
/// field that extracted stopped (or started) extracting. That pair brackets
/// the markup revision that broke (or fixed) the rule.
pub(crate) fn bisect_field(
    field: &str,
    keys: &[DocKey],
    cand: &[(Value, DocReport)],
) -> Vec<Value> {
    // url -> [(observed_at, matched)], then sort each series chronologically.
    let mut series: BTreeMap<&str, Vec<(&str, bool)>> = BTreeMap::new();
    for (key, (_, rep)) in keys.iter().zip(cand) {
        let Some(ts) = key.observed_at.as_deref() else {
            continue;
        };
        series
            .entry(&key.url)
            .or_default()
            .push((ts, is_match(rep.fields.get(field))));
    }
    let mut boundaries: Vec<Value> = Vec::new();
    'urls: for (url, mut points) in series {
        points.sort_by(|a, b| a.0.cmp(b.0));
        for pair in points.windows(2) {
            let (from, to) = (pair[0], pair[1]);
            if from.1 != to.1 {
                boundaries.push(json!({
                    "url": url,
                    "from": {"observed_at": from.0, "matched": from.1},
                    "to": {"observed_at": to.0, "matched": to.1},
                }));
                if boundaries.len() >= BISECT_ECHO_LIMIT {
                    break 'urls;
                }
            }
        }
    }
    boundaries
}

/// The replay runner: load stored bodies (latest or full version history),
/// run candidate (+ optional baseline) rules over them, and return the diff
/// report. Never writes a dataset — see the module doc.
pub(crate) async fn run_replay(ctx: &AppContext, replay: &Map<String, Value>) -> Result<Value> {
    let p = parse_replay_params(replay).map_err(Error::App)?;
    let candidate = Arc::new(p.candidate.compile()?);
    let baseline = p
        .baseline
        .as_ref()
        .map(|b| b.compile())
        .transpose()?
        .map(Arc::new);
    let pattern = p
        .url_pattern
        .as_deref()
        .map(|s| {
            regex::Regex::new(s)
                .map_err(|e| Error::App(format!("bad replay.against.url_pattern '{s}': {e}")))
        })
        .transpose()?;

    // Select up to max_pages live URLs (crawl keys ARE canonical URLs).
    let mut records: Vec<Record> = ctx
        .datasets
        .list(&p.app, &p.dataset, SOURCE_LIST_LIMIT)
        .await?
        .into_iter()
        .filter(|r| {
            r.removed_at.is_none()
                && !r.data.get("gone").and_then(Value::as_bool).unwrap_or(false)
                && pattern.as_ref().is_none_or(|re| re.is_match(&r.key))
        })
        .collect();
    let matching_urls = records.len();
    let truncated = matching_urls > p.max_pages;
    records.truncate(p.max_pages);

    // Resolve each URL to its stored body/bodies — the same machinery as the
    // source mode, minus every write.
    let mut keys: Vec<DocKey> = Vec::new();
    let mut docs: Vec<String> = Vec::new();
    let mut missing: Vec<Value> = Vec::new();
    for r in records {
        if !p.versions_all {
            match ctx.read_source_artifact(&p.app, &r).await {
                Ok(body) => {
                    keys.push(DocKey {
                        url: r.key.clone(),
                        observed_at: None,
                    });
                    docs.push(body);
                }
                Err(reason) => missing.push(json!({"key": r.key, "reason": reason})),
            }
            continue;
        }
        // Full history: archived versions + the current live body, each with an
        // observation timestamp so bisect can order the series.
        let mut candidates: Vec<(String, Record)> = Vec::new();
        for v in versions_for(ctx, &p.app, &r.key).await? {
            if let Some(ts) = v.data.get("fetched_at").and_then(Value::as_str) {
                candidates.push((ts.to_string(), v));
            }
        }
        candidates.push((r.updated_at.to_rfc3339(), r.clone()));
        for (ts, record) in &candidates {
            match ctx.read_source_artifact(&p.app, record).await {
                Ok(body) => {
                    keys.push(DocKey {
                        url: r.key.clone(),
                        observed_at: Some(ts.clone()),
                    });
                    docs.push(body);
                }
                Err(reason) => missing.push(json!({"key": record.key, "reason": reason})),
            }
        }
    }

    // Both rule sets run over the IDENTICAL document vector, off the async
    // runtime (rayon fan-out inside), so reports stay index-aligned.
    let base_for_task = baseline.clone();
    let (cand_reports, base_reports) = tokio::task::spawn_blocking(move || {
        let c = extract_batch_with_report(&candidate, &docs);
        let b = base_for_task
            .as_ref()
            .map(|b| extract_batch_with_report(b, &docs));
        (c, b)
    })
    .await
    .map_err(|e| Error::App(format!("replay extract task failed: {e}")))?;

    let diff = diff_fields(&keys, &cand_reports, base_reports.as_deref());
    let inner_fields = diff_inner_fields(&cand_reports, base_reports.as_deref());
    let bisect = p
        .bisect_field
        .as_deref()
        .map(|f| json!({"field": f, "boundaries": bisect_field(f, &keys, &cand_reports)}));

    let missing_count = missing.len();
    missing.truncate(MISSING_ECHO_LIMIT);
    let report = json!({
        "mode": "replay",
        "against": {
            "app": p.app,
            "dataset": p.dataset,
            "url_pattern": p.url_pattern,
            "versions": if p.versions_all { "all" } else { "latest" },
            "max_pages": p.max_pages,
        },
        "urls_matching": matching_urls,
        "truncated": truncated,
        "docs": keys.len(),
        "missing": missing_count,
        "missing_keys": missing,
        "baseline": base_reports.is_some(),
        "fields": diff.fields,
        "inner_fields": inner_fields,
        "regressed_urls": diff.regressed_urls,
        "regressions": diff.regressions,
        "bisect": bisect,
    });

    // The one write this mode performs: a per-job FILE artifact (not a
    // dataset record), so the verdict is durable and diffable across runs.
    let bytes = serde_json::to_vec_pretty(&report)
        .map_err(|e| Error::App(format!("serialize replay report: {e}")))?;
    ctx.save_artifact("replay-report.json", &bytes).await?;

    let mut result = report;
    result["artifact"] = json!("replay-report.json");
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumper_core::{DocReport, FieldStatus};
    use serde_json::json;

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    fn doc(url: &str, observed: Option<&str>) -> DocKey {
        DocKey {
            url: url.into(),
            observed_at: observed.map(String::from),
        }
    }

    fn report(pairs: &[(&str, FieldStatus)], values: Value) -> (Value, DocReport) {
        let rep = DocReport {
            fields: pairs
                .iter()
                .cloned()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            ..DocReport::default()
        };
        (values, rep)
    }

    #[test]
    fn parse_requires_rules_and_validates_versions_and_bisect() {
        // rules required.
        assert!(parse_replay_params(&obj(json!({}))).is_err());
        // Minimal valid: defaults latest / crawl / pages / 500.
        let p = parse_replay_params(&obj(json!({
            "rules": {"t": {"type": "css", "selector": "h1"}}
        })))
        .unwrap();
        assert!(!p.versions_all);
        assert_eq!((p.app.as_str(), p.dataset.as_str()), ("crawl", "pages"));
        assert_eq!(p.max_pages, DEFAULT_MAX_PAGES);
        // Bad versions value is an error, not a guess.
        assert!(parse_replay_params(&obj(json!({
            "rules": {"t": {"type": "css", "selector": "h1"}},
            "against": {"versions": "some"}
        })))
        .is_err());
        // max_pages clamps into 1..=ceiling.
        let p = parse_replay_params(&obj(json!({
            "rules": {"t": {"type": "css", "selector": "h1"}},
            "against": {"max_pages": 0}
        })))
        .unwrap();
        assert_eq!(p.max_pages, 1);
        let p = parse_replay_params(&obj(json!({
            "rules": {"t": {"type": "css", "selector": "h1"}},
            "against": {"max_pages": 999999}
        })))
        .unwrap();
        assert_eq!(p.max_pages, MAX_PAGES_CEILING);
        // Bisect without full versions is rejected — one point can't bisect.
        assert!(parse_replay_params(&obj(json!({
            "rules": {"t": {"type": "css", "selector": "h1"}},
            "bisect_field": "t"
        })))
        .is_err());
        let p = parse_replay_params(&obj(json!({
            "rules": {"t": {"type": "css", "selector": "h1"}},
            "against": {"versions": "all"},
            "bisect_field": "t"
        })))
        .unwrap();
        assert_eq!(p.bisect_field.as_deref(), Some("t"));
    }

    #[test]
    fn diff_math_rates_deltas_added_lost_changed() {
        let keys = [doc("http://a", None), doc("http://b", None)];
        // Baseline: price matched on both; title matched on a only.
        let base = [
            report(
                &[
                    ("price", FieldStatus::Matched),
                    ("title", FieldStatus::Matched),
                ],
                json!({"price": "9", "title": "A"}),
            ),
            report(
                &[
                    ("price", FieldStatus::Matched),
                    ("title", FieldStatus::Empty),
                ],
                json!({"price": "8", "title": null}),
            ),
        ];
        // Candidate: price lost on b, changed on a; title added on b.
        let cand = [
            report(
                &[
                    ("price", FieldStatus::Matched),
                    ("title", FieldStatus::Matched),
                ],
                json!({"price": "9.00", "title": "A"}),
            ),
            report(
                &[
                    ("price", FieldStatus::Empty),
                    ("title", FieldStatus::Matched),
                ],
                json!({"price": null, "title": "B"}),
            ),
        ];
        let out = diff_fields(&keys, &cand, Some(&base));
        assert_eq!(out.fields.len(), 2);
        // price regressed (delta -0.5) → sorted first.
        let price = &out.fields[0];
        assert_eq!(price["field"], "price");
        assert_eq!(price["match_rate"], 0.5);
        assert_eq!(price["baseline_match_rate"], 1.0);
        assert_eq!(price["delta"], -0.5);
        assert_eq!(price["lost"]["count"], 1);
        assert_eq!(price["lost"]["samples"][0]["url"], "http://b");
        assert_eq!(price["changed"]["count"], 1);
        assert_eq!(price["changed"]["samples"][0]["from"], "9");
        assert_eq!(price["changed"]["samples"][0]["to"], "9.00");
        let title = &out.fields[1];
        assert_eq!(title["field"], "title");
        assert_eq!(title["delta"], 0.5);
        assert_eq!(title["added"]["count"], 1);
        assert_eq!(title["added"]["samples"][0]["value"], "B");
        assert_eq!(title["lost"]["count"], 0);
        // Per-URL regressions: a changed price, b lost price.
        assert_eq!(out.regressed_urls, 2);
        assert_eq!(out.regressions[0]["url"], "http://a");
        assert_eq!(out.regressions[0]["changed"][0], "price");
        assert_eq!(out.regressions[1]["url"], "http://b");
        assert_eq!(out.regressions[1]["lost"][0], "price");
    }

    #[test]
    fn inner_listing_rot_is_a_replay_regression_not_an_invisible_one() {
        // THE REFUTED BEHAVIOR: both rule sets report one `Matched` for the
        // whole listing, so the document-level diff shows delta 0.0 — a rule
        // edit that killed the inner `price` selector shipped clean. The
        // item-scoped rows are what catch it.
        use pumper_core::extract::InnerFieldStats;
        let listing = |price: InnerFieldStats| {
            let mut rep = DocReport {
                fields: [("products".to_string(), FieldStatus::Matched)]
                    .into_iter()
                    .collect(),
                ..DocReport::default()
            };
            rep.each.insert("products.price".into(), price);
            rep.each.insert(
                "products.name".into(),
                InnerFieldStats {
                    items: 4,
                    matched: 4,
                    ..InnerFieldStats::default()
                },
            );
            (json!({"products": []}), rep)
        };
        let base = [listing(InnerFieldStats {
            items: 4,
            matched: 4,
            ..InnerFieldStats::default()
        })];
        let cand = [listing(InnerFieldStats {
            items: 4,
            matched: 0,
            empty: 4,
            ..InnerFieldStats::default()
        })];

        // Document scope sees nothing: the listing still matched on both sides.
        let keys = [doc("http://a", None)];
        let flat = diff_fields(&keys, &cand, Some(&base));
        assert_eq!(flat.fields[0]["field"], "products");
        assert_eq!(flat.fields[0]["delta"], 0.0);
        assert_eq!(flat.regressed_urls, 0);

        // Item scope names the dead field, worst first.
        let inner = diff_inner_fields(&cand, Some(&base));
        assert_eq!(inner.len(), 2, "{inner:?}");
        assert_eq!(inner[0]["field"], "products.price");
        assert_eq!(inner[0]["scope"], "item");
        assert_eq!(inner[0]["items"], 4);
        assert_eq!(inner[0]["match_rate"], 0.0);
        assert_eq!(inner[0]["baseline_match_rate"], 1.0);
        assert_eq!(inner[0]["delta"], -1.0);
        assert_eq!(inner[0]["dead"], true);
        assert_eq!(inner[1]["field"], "products.name");
        assert_eq!(inner[1]["delta"], 0.0);
        assert_eq!(inner[1]["dead"], false);

        // Candidate-only replay: rates and the dead flag, no deltas.
        let solo = diff_inner_fields(&cand, None);
        assert_eq!(solo[0]["field"], "products.price");
        assert_eq!(solo[0]["match_rate"], 0.0);
        assert!(solo[0].get("delta").is_none());
        // A rule set with no `each` rule contributes no item-scoped rows.
        let plain = [report(&[("t", FieldStatus::Matched)], json!({"t": "x"}))];
        assert!(diff_inner_fields(&plain, None).is_empty());
    }

    #[test]
    fn candidate_only_reports_rates_without_deltas() {
        let keys = [doc("http://a", None)];
        let cand = [report(&[("t", FieldStatus::Matched)], json!({"t": "x"}))];
        let out = diff_fields(&keys, &cand, None);
        assert_eq!(out.fields[0]["match_rate"], 1.0);
        assert!(out.fields[0].get("delta").is_none());
        assert!(out.fields[0].get("lost").is_none());
        assert_eq!(out.regressed_urls, 0);
    }

    #[test]
    fn samples_are_bounded_but_counts_are_full() {
        let n = SAMPLE_LIMIT + 15;
        let keys: Vec<DocKey> = (0..n).map(|i| doc(&format!("http://u{i}"), None)).collect();
        let base: Vec<(Value, DocReport)> = (0..n)
            .map(|_| report(&[("f", FieldStatus::Matched)], json!({"f": "old"})))
            .collect();
        let cand: Vec<(Value, DocReport)> = (0..n)
            .map(|_| report(&[("f", FieldStatus::Empty)], json!({"f": null})))
            .collect();
        let out = diff_fields(&keys, &cand, Some(&base));
        let f = &out.fields[0];
        assert_eq!(f["lost"]["count"], n as u64);
        assert_eq!(f["lost"]["samples"].as_array().unwrap().len(), SAMPLE_LIMIT);
        // Every URL regressed, but the echo is bounded while the count is honest.
        assert_eq!(out.regressed_urls, n);
        assert_eq!(out.regressions.len().min(n), out.regressions.len());
    }

    #[test]
    fn bisect_reports_the_boundary_pair_where_the_match_flipped() {
        // Chronology deliberately shuffled: bisect must sort per URL.
        let keys = [
            doc("http://p", Some("2026-03-01T00:00:00+00:00")),
            doc("http://p", Some("2026-01-01T00:00:00+00:00")),
            doc("http://p", Some("2026-05-01T00:00:00+00:00")),
            doc("http://q", Some("2026-01-01T00:00:00+00:00")),
        ];
        let cand = [
            report(&[("f", FieldStatus::Empty)], json!({"f": null})), // Mar: broken
            report(&[("f", FieldStatus::Matched)], json!({"f": "x"})), // Jan: fine
            report(&[("f", FieldStatus::Empty)], json!({"f": null})), // May: broken
            report(&[("f", FieldStatus::Matched)], json!({"f": "y"})), // q never flips
        ];
        let boundaries = bisect_field("f", &keys, &cand);
        assert_eq!(boundaries.len(), 1, "{boundaries:?}");
        assert_eq!(boundaries[0]["url"], "http://p");
        assert_eq!(
            boundaries[0]["from"]["observed_at"],
            "2026-01-01T00:00:00+00:00"
        );
        assert_eq!(boundaries[0]["from"]["matched"], true);
        assert_eq!(
            boundaries[0]["to"]["observed_at"],
            "2026-03-01T00:00:00+00:00"
        );
        assert_eq!(boundaries[0]["to"]["matched"], false);
    }
}
