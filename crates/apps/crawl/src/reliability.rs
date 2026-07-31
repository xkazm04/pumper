//! Web Reliability Index — persistence for the per-host telemetry the platform
//! already computes and previously discarded after one use (M41).
//!
//! Two datasets under the data-only app namespace [`APP`] (`web-reliability`;
//! no `ScrapeApp` registers under it — records are reachable through the
//! existing generic surfaces: `?filter=` on /datasets, export, changes,
//! triggers):
//!
//! - **`host_observations`** — one record per host per calendar day, keyed
//!   `{host}@{YYYY-MM-DD}`. Each run that touched the host folds its own
//!   tallies into the day record (counters sum; verdict fields keep the
//!   latest). Intra-day history is retained by the dataset store's normal
//!   revision mechanism — every fold is a revision.
//! - **`host_index`** — one record per host, keyed by host: cumulative rolling
//!   totals across every observation ever folded, plus the derived
//!   *scrapeability* score.
//!
//! ## Scrapeability score (formula v1, documented here and stamped on records)
//!
//! `score = 100 × Σ(wᵢ·cᵢ) / Σ(wᵢ)` over the components that have evidence
//! (weights renormalize over the observed subset — a host never fetched via
//! HTTP is scored on extraction health alone, and vice versa):
//!
//! | component           | weight | value (all clamped to 0..1)                          |
//! |---------------------|--------|------------------------------------------------------|
//! | `fetch_ok`          | 0.45   | 2xx responses / fetches                              |
//! | `bot_pressure`      | 0.25   | 1 − (403/429/503 responses / fetches)                |
//! | `extraction_health` | 0.15   | latest health-detector score for sources on the host |
//! | `conditional_get`   | 0.10   | 1.0 if any `304` observed, else validator-bearing responses / fetches |
//! | `availability`      | 0.05   | 1 − (404/410 responses / fetches)                    |
//!
//! Hosts with fewer than [`MIN_CONFIDENT_OBSERVATIONS`] folded observations
//! carry `low_confidence: true` — the score is published but flagged as thin
//! evidence, never hidden and never fabricated.
//!
//! Everything here consumes telemetry the runs already produced (crawl fetch
//! tallies, extraction health verdicts). **No new fetches, no probing.**
//! Persistence is best-effort: failures warn and never fail the job.

use pumper_core::datasets::Provenance;
use pumper_core::Datasets;
use serde_json::{json, Map, Value};

/// Data-only app namespace holding both datasets.
pub const APP: &str = "web-reliability";
/// Per-host per-day observation dataset (key `{host}@{date}`).
pub const OBS_DATASET: &str = "host_observations";
/// Per-host rolling aggregate dataset (key = host).
pub const INDEX_DATASET: &str = "host_index";
/// Below this many folded observations the index record is `low_confidence`.
pub const MIN_CONFIDENT_OBSERVATIONS: u64 = 3;
/// Version tag stamped on index records so a future formula change is visible.
pub const FORMULA: &str = "v1";
/// Worst-fields entries kept on an observation (evidence, not a full dump).
const WORST_FIELDS_KEPT: usize = 5;

/// Per-host fetch-layer tallies from one crawl run — counted at the same
/// observation point that already feeds `learn_tier` (the metering client),
/// just kept as counters instead of a single boolean.
#[derive(Debug, Default, Clone)]
pub struct CrawlHostObs {
    pub fetches: u64,
    /// 2xx responses.
    pub ok: u64,
    /// Bot-wall statuses (403/429/503) — same set `fetcher::http_bot_wall` uses.
    pub botwall: u64,
    /// Transport-layer failures (DNS/TLS/connect/timeout).
    pub transport_errors: u64,
    /// `304 Not Modified` answers to conditional GETs.
    pub not_modified: u64,
    /// `404`/`410` answers (gone lifecycle).
    pub gone: u64,
    /// Responses carrying an `ETag` or `Last-Modified` validator header.
    pub validators_seen: u64,
}

/// One extraction-health verdict attributed to a host — a rendering of the
/// health detector's `SourceVerdict` (which is per source, i.e. per
/// `{app}/{dataset}`, not per host; `verdict_scope: "source"` on the stored
/// record keeps that honest) plus the host's share of the run's documents.
#[derive(Debug, Clone)]
pub struct ExtractionObs {
    pub source_id: String,
    pub state: String,
    pub previous_state: String,
    /// Health-detector score, 0..1.
    pub score: f64,
    pub diagnosis: Option<String>,
    /// Documents from this host in the observed run.
    pub docs: u64,
    /// Run-level fetch ok-rate; `None` when nothing was fetched (source mode
    /// over stored bodies) — honest-Null, never a fabricated 1.0.
    pub fetch_ok_rate: Option<f64>,
    /// Top worst-miss fields for the run (already truncated by the caller or
    /// here to [`WORST_FIELDS_KEPT`]).
    pub worst_fields: Vec<Value>,
}

/// One host-attributed telemetry delta from a finished run.
#[derive(Debug, Clone)]
pub enum HostDelta {
    Crawl(CrawlHostObs),
    Extraction(ExtractionObs),
}

/// Observation key: `{host}@{date}`.
pub fn obs_key(host: &str, date: &str) -> String {
    format!("{host}@{date}")
}

fn as_map(v: Value) -> Map<String, Value> {
    match v {
        Value::Object(m) => m,
        _ => Map::new(),
    }
}

fn num(m: &Map<String, Value>, key: &str) -> u64 {
    m.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn add(m: &mut Map<String, Value>, key: &str, delta: u64) {
    let cur = num(m, key);
    m.insert(key.into(), json!(cur + delta));
}

fn crawl_counters(m: &mut Map<String, Value>, obs: &CrawlHostObs) {
    add(m, "runs", 1);
    add(m, "fetches", obs.fetches);
    add(m, "ok", obs.ok);
    add(m, "botwall", obs.botwall);
    add(m, "transport_errors", obs.transport_errors);
    add(m, "not_modified_304", obs.not_modified);
    add(m, "gone", obs.gone);
    add(m, "validators_seen", obs.validators_seen);
}

fn extraction_counters(m: &mut Map<String, Value>, obs: &ExtractionObs) {
    add(m, "runs", 1);
    add(m, "docs", obs.docs);
    // Latest-verdict fields (a day/host can see several runs; last wins, the
    // dataset revision history retains the sequence).
    m.insert("source_id".into(), json!(obs.source_id));
    m.insert("state".into(), json!(obs.state));
    m.insert("previous_state".into(), json!(obs.previous_state));
    m.insert("score".into(), json!(obs.score));
    m.insert("diagnosis".into(), json!(obs.diagnosis));
    m.insert("fetch_ok_rate".into(), json!(obs.fetch_ok_rate));
    m.insert(
        "worst_fields".into(),
        Value::Array(obs.worst_fields.iter().take(WORST_FIELDS_KEPT).cloned().collect()),
    );
    m.insert("verdict_scope".into(), json!("source"));
    let worst = m
        .get("worst_score")
        .and_then(Value::as_f64)
        .map_or(obs.score, |w| w.min(obs.score));
    m.insert("worst_score".into(), json!(worst));
}

/// Folds one run's delta into the (possibly existing) `{host}@{date}` day
/// record. Pure — callers do the read/write.
pub fn merge_observation(
    existing: Option<&Value>,
    host: &str,
    date: &str,
    job_id: &str,
    delta: &HostDelta,
) -> Value {
    let mut rec = existing.cloned().map(as_map).unwrap_or_default();
    rec.insert("host".into(), json!(host));
    rec.insert("date".into(), json!(date));
    rec.insert("last_job_id".into(), json!(job_id));
    let (section, fold): (&str, fn(&mut Map<String, Value>, &HostDelta)) = match delta {
        HostDelta::Crawl(_) => ("crawl", |m, d| {
            if let HostDelta::Crawl(o) = d {
                crawl_counters(m, o)
            }
        }),
        HostDelta::Extraction(_) => ("extraction", |m, d| {
            if let HostDelta::Extraction(o) = d {
                extraction_counters(m, o)
            }
        }),
    };
    let mut sec = rec.remove(section).map(as_map).unwrap_or_default();
    fold(&mut sec, delta);
    rec.insert(section.into(), Value::Object(sec));
    Value::Object(rec)
}

/// Folds one run's delta into the host's rolling `host_index` record and
/// recomputes the scrapeability score. Pure — callers do the read/write.
pub fn fold_index(existing: Option<&Value>, host: &str, date: &str, delta: &HostDelta) -> Value {
    let mut rec = existing.cloned().map(as_map).unwrap_or_default();
    rec.insert("host".into(), json!(host));
    if !rec.contains_key("first_date") {
        rec.insert("first_date".into(), json!(date));
    }
    rec.insert("last_date".into(), json!(date));
    add(&mut rec, "observations", 1);
    match delta {
        HostDelta::Crawl(o) => {
            let mut sec = rec.remove("crawl").map(as_map).unwrap_or_default();
            crawl_counters(&mut sec, o);
            rec.insert("crawl".into(), Value::Object(sec));
        }
        HostDelta::Extraction(o) => {
            let mut sec = rec.remove("extraction").map(as_map).unwrap_or_default();
            extraction_counters(&mut sec, o);
            rec.insert("extraction".into(), Value::Object(sec));
        }
    }
    let score = scrapeability(&rec);
    rec.insert("scrapeability".into(), score);
    Value::Object(rec)
}

/// Computes the scrapeability block from an index record's rolling totals.
/// See the module docs for formula v1. Components without evidence are
/// omitted and the weights renormalize; a record with no evidence at all
/// (shouldn't happen — folds always add some) scores `null`.
fn scrapeability(rec: &Map<String, Value>) -> Value {
    let observations = num(rec, "observations");
    let crawl = rec.get("crawl").and_then(Value::as_object);
    let extraction = rec.get("extraction").and_then(Value::as_object);

    let mut components = Map::new();
    let mut weighted = 0.0_f64;
    let mut weight_sum = 0.0_f64;
    let mut push = |name: &str, weight: f64, value: f64, components: &mut Map<String, Value>| {
        let v = value.clamp(0.0, 1.0);
        components.insert(name.into(), json!((v * 1000.0).round() / 1000.0));
        weighted += weight * v;
        weight_sum += weight;
    };

    if let Some(c) = crawl {
        let fetches = num(c, "fetches");
        if fetches > 0 {
            let f = fetches as f64;
            push("fetch_ok", 0.45, num(c, "ok") as f64 / f, &mut components);
            push(
                "bot_pressure",
                0.25,
                1.0 - num(c, "botwall") as f64 / f,
                &mut components,
            );
            let conditional = if num(c, "not_modified_304") > 0 {
                1.0
            } else {
                num(c, "validators_seen") as f64 / f
            };
            push("conditional_get", 0.10, conditional, &mut components);
            push(
                "availability",
                0.05,
                1.0 - num(c, "gone") as f64 / f,
                &mut components,
            );
        }
    }
    if let Some(e) = extraction {
        if num(e, "runs") > 0 {
            let health = e.get("score").and_then(Value::as_f64).unwrap_or(0.0);
            push("extraction_health", 0.15, health, &mut components);
        }
    }

    let score = if weight_sum > 0.0 {
        json!(((weighted / weight_sum) * 1000.0).round() / 10.0)
    } else {
        Value::Null
    };
    json!({
        "score": score,
        "formula": FORMULA,
        "components": components,
        "low_confidence": observations < MIN_CONFIDENT_OBSERVATIONS,
        "observations": observations,
    })
}

/// Persists a batch of per-host deltas from one finished run: folds each into
/// its `{host}@{date}` observation record and the host's rolling index record,
/// then writes both datasets. Read-modify-write per host (O(hosts), matching
/// the crawl's existing tally flush); a concurrent run folding the same host
/// can lose one fold — acceptable for telemetry, never for money. Best-effort
/// throughout: failures warn and are skipped, the job never fails on this.
/// Returns how many hosts were recorded.
pub async fn record_observations(
    datasets: &Datasets,
    job_id: &str,
    deltas: Vec<(String, HostDelta)>,
) -> usize {
    if deltas.is_empty() {
        return 0;
    }
    let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let mut obs_items: Vec<(String, Value)> = Vec::with_capacity(deltas.len());
    let mut idx_items: Vec<(String, Value)> = Vec::with_capacity(deltas.len());
    for (host, delta) in &deltas {
        let key = obs_key(host, &date);
        let existing = match datasets.get(APP, OBS_DATASET, &key).await {
            Ok(r) => r.map(|r| r.data),
            Err(e) => {
                tracing::warn!(host = %host, "web-reliability observation read failed: {e}");
                continue;
            }
        };
        obs_items.push((
            key,
            merge_observation(existing.as_ref(), host, &date, job_id, delta),
        ));
        let idx_existing = match datasets.get(APP, INDEX_DATASET, host).await {
            Ok(r) => r.map(|r| r.data),
            Err(e) => {
                tracing::warn!(host = %host, "web-reliability index read failed: {e}");
                continue;
            }
        };
        idx_items.push((
            host.clone(),
            fold_index(idx_existing.as_ref(), host, &date, delta),
        ));
    }
    let recorded = obs_items.len();
    // Derivation stamp (M12): these records are folds over EVERY fetch a run
    // made against a host, so the producing job is the only honest fact — there
    // is no single source URL and no RuleSet behind an aggregate counter.
    let prov = Provenance {
        job_id: Some(job_id.to_string()),
        ..Provenance::default()
    };
    if let Err(e) = datasets
        .upsert_many_stamped(APP, OBS_DATASET, &obs_items, None, Some(&prov))
        .await
    {
        tracing::warn!(job = %job_id, "web-reliability observations upsert failed: {e}");
        return 0;
    }
    if !idx_items.is_empty() {
        if let Err(e) = datasets
            .upsert_many_stamped(APP, INDEX_DATASET, &idx_items, None, Some(&prov))
            .await
        {
            tracing::warn!(job = %job_id, "web-reliability index upsert failed: {e}");
        }
    }
    recorded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crawl_obs() -> CrawlHostObs {
        CrawlHostObs {
            fetches: 10,
            ok: 8,
            botwall: 1,
            transport_errors: 1,
            not_modified: 0,
            gone: 0,
            validators_seen: 4,
        }
    }

    fn extraction_obs() -> ExtractionObs {
        ExtractionObs {
            source_id: "extractor/extracted".into(),
            state: "degraded".into(),
            previous_state: "healthy".into(),
            score: 0.4,
            diagnosis: Some("markup_drift".into()),
            docs: 12,
            fetch_ok_rate: Some(0.9),
            worst_fields: vec![json!({"field": "price", "misses": 6})],
        }
    }

    #[test]
    fn obs_key_is_host_at_date() {
        assert_eq!(obs_key("example.com", "2026-07-30"), "example.com@2026-07-30");
    }

    #[test]
    fn merge_crawl_into_empty_creates_day_record() {
        let rec = merge_observation(
            None,
            "example.com",
            "2026-07-30",
            "job-1",
            &HostDelta::Crawl(crawl_obs()),
        );
        assert_eq!(rec["host"], "example.com");
        assert_eq!(rec["date"], "2026-07-30");
        assert_eq!(rec["last_job_id"], "job-1");
        assert_eq!(rec["crawl"]["runs"], 1);
        assert_eq!(rec["crawl"]["fetches"], 10);
        assert_eq!(rec["crawl"]["botwall"], 1);
        assert_eq!(rec["crawl"]["validators_seen"], 4);
    }

    #[test]
    fn merge_crawl_twice_sums_counters() {
        let first = merge_observation(
            None,
            "example.com",
            "2026-07-30",
            "job-1",
            &HostDelta::Crawl(crawl_obs()),
        );
        let second = merge_observation(
            Some(&first),
            "example.com",
            "2026-07-30",
            "job-2",
            &HostDelta::Crawl(crawl_obs()),
        );
        assert_eq!(second["crawl"]["runs"], 2);
        assert_eq!(second["crawl"]["fetches"], 20);
        assert_eq!(second["crawl"]["ok"], 16);
        assert_eq!(second["last_job_id"], "job-2");
    }

    #[test]
    fn merge_extraction_onto_crawl_keeps_both_sections() {
        let first = merge_observation(
            None,
            "example.com",
            "2026-07-30",
            "job-1",
            &HostDelta::Crawl(crawl_obs()),
        );
        let rec = merge_observation(
            Some(&first),
            "example.com",
            "2026-07-30",
            "job-2",
            &HostDelta::Extraction(extraction_obs()),
        );
        assert_eq!(rec["crawl"]["fetches"], 10);
        assert_eq!(rec["extraction"]["docs"], 12);
        assert_eq!(rec["extraction"]["state"], "degraded");
        assert_eq!(rec["extraction"]["verdict_scope"], "source");
        assert_eq!(rec["extraction"]["worst_score"], 0.4);
    }

    #[test]
    fn extraction_worst_score_keeps_minimum() {
        let first = merge_observation(
            None,
            "h",
            "d",
            "j1",
            &HostDelta::Extraction(extraction_obs()),
        );
        let mut better = extraction_obs();
        better.score = 0.9;
        better.state = "healthy".into();
        let rec = merge_observation(Some(&first), "h", "d", "j2", &HostDelta::Extraction(better));
        assert_eq!(rec["extraction"]["score"], 0.9); // latest
        assert_eq!(rec["extraction"]["worst_score"], 0.4); // floor retained
    }

    #[test]
    fn fold_index_accumulates_and_scores() {
        let idx = fold_index(None, "example.com", "2026-07-30", &HostDelta::Crawl(crawl_obs()));
        assert_eq!(idx["observations"], 1);
        assert_eq!(idx["first_date"], "2026-07-30");
        assert_eq!(idx["crawl"]["fetches"], 10);
        let s = &idx["scrapeability"];
        assert_eq!(s["formula"], FORMULA);
        assert_eq!(s["low_confidence"], true); // 1 < 3 observations
        // Components without extraction evidence: no extraction_health key.
        assert!(s["components"].get("extraction_health").is_none());
        // fetch_ok 0.8, bot 0.9, cond 0.4, avail 1.0 over weights .45/.25/.10/.05
        // = (.36+.225+.04+.05)/.85 = 0.7941 → 79.4
        assert_eq!(s["score"], 79.4);
    }

    #[test]
    fn confidence_clears_at_three_observations() {
        let mut idx = fold_index(None, "h", "d1", &HostDelta::Crawl(crawl_obs()));
        idx = fold_index(Some(&idx), "h", "d2", &HostDelta::Crawl(crawl_obs()));
        assert_eq!(idx["scrapeability"]["low_confidence"], true);
        idx = fold_index(Some(&idx), "h", "d3", &HostDelta::Crawl(crawl_obs()));
        assert_eq!(idx["observations"], 3);
        assert_eq!(idx["scrapeability"]["low_confidence"], false);
        assert_eq!(idx["last_date"], "d3");
        assert_eq!(idx["first_date"], "d1");
    }

    #[test]
    fn extraction_only_host_scores_on_health_alone() {
        let idx = fold_index(None, "h", "d", &HostDelta::Extraction(extraction_obs()));
        let s = &idx["scrapeability"];
        // Only extraction_health present → weight renormalizes to 1.0.
        assert_eq!(s["score"], 40.0);
        assert!(s["components"].get("fetch_ok").is_none());
    }

    #[test]
    fn any_304_maxes_conditional_get_component() {
        let mut o = crawl_obs();
        o.not_modified = 2;
        o.validators_seen = 0;
        let idx = fold_index(None, "h", "d", &HostDelta::Crawl(o));
        assert_eq!(idx["scrapeability"]["components"]["conditional_get"], 1.0);
    }

    #[test]
    fn botwalled_host_scores_below_clean_host() {
        let clean = fold_index(None, "a", "d", &HostDelta::Crawl(crawl_obs()));
        let mut walled = crawl_obs();
        walled.ok = 1;
        walled.botwall = 9;
        let bad = fold_index(None, "b", "d", &HostDelta::Crawl(walled));
        let (c, b) = (
            clean["scrapeability"]["score"].as_f64().unwrap(),
            bad["scrapeability"]["score"].as_f64().unwrap(),
        );
        assert!(b < c, "botwalled {b} should score below clean {c}");
    }
}
