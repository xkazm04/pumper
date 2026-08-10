//! Observatory mode (M16): corpus-scale differential testing of WASM plugins
//! against the already-stored web. Replays each requested plugin over N sampled
//! stored pages per SITE (host), classifies per-page outcomes
//! (ok / trap / empty / schema_invalid), records timing + output-shape stats,
//! and upserts one row per (plugin, site) into the `observatory` dataset with a
//! drift score vs the previous run's row — so change detection + triggers on
//! that dataset surface extraction rot for free, with zero new fetches.
//!
//! Honest sampling: every row reports `sampled`/`total_pages`; a site with
//! fewer than [`LOW_CONFIDENCE_FLOOR`] stored pages is marked
//! `low_confidence: true`. Sample = newest half + deterministic-random rest
//! (seeded by site name, so reruns over an unchanged corpus pick the same
//! pages and drift reflects the plugin/corpus, not sampler noise).
//!
//! Fuel note: the `Plugins` trait does not surface per-call fuel consumed (the
//! wasmtime store is internal to engine-wasm), so rows carry `avg_elapsed_ms`
//! as the cost signal; fuel exhaustion still shows up as a `trap` outcome.

use pumper_core::{AppContext, Error, Record, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;

use crate::{SOURCE_LIST_LIMIT, VERSIONS_DATASET};

/// Default number of stored pages sampled per site.
pub(crate) const DEFAULT_SAMPLE_PER_SITE: usize = 25;

/// Sites with fewer stored pages than this are flagged `low_confidence`.
pub(crate) const LOW_CONFIDENCE_FLOOR: usize = 5;

/// An empty-rate increase of at least this much vs the previous run flags
/// `empty_rate_rising` — the canary for a site that quietly changed markup.
pub(crate) const EMPTY_RISE_THRESHOLD: f64 = 0.10;

/// Default output dataset for observatory rows (`plugin/observatory`).
const OBSERVATORY_DATASET: &str = "observatory";

/// Outcome taxonomy for one plugin-over-one-page replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Ran and produced a non-empty JSON value.
    Ok,
    /// The sandbox stopped it: fuel exhaustion, memory cap, or a panic.
    Trap,
    /// Ran fine but produced nothing usable (null/empty/all-null object, or a
    /// self-reported `{"error": ..}` object).
    Empty,
    /// Output violated the contract: invalid JSON, out-of-bounds return, or any
    /// other host-side ABI failure.
    SchemaInvalid,
}

/// Classify one replay result. Error-message matching is pinned to the host's
/// own wording (engine-wasm wraps traps as "plugin trapped (fuel/memory/panic)"
/// and task aborts as "panicked"); anything else a run can fail with is a
/// contract violation, not a sandbox stop.
pub(crate) fn classify_outcome(res: &std::result::Result<Value, String>) -> Outcome {
    match res {
        Err(msg) => {
            if msg.contains("trapped") || msg.contains("panicked") {
                Outcome::Trap
            } else {
                Outcome::SchemaInvalid
            }
        }
        Ok(v) => {
            if is_empty_output(v) {
                Outcome::Empty
            } else {
                Outcome::Ok
            }
        }
    }
}

/// "Produced nothing usable": JSON null, empty string, empty array/object, an
/// object whose every value is itself empty, or an object carrying an `error`
/// key (the plugin ran but reported it could not extract).
pub(crate) fn is_empty_output(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(m) => {
            m.is_empty() || m.contains_key("error") || m.values().all(is_empty_output)
        }
        _ => false,
    }
}

/// Outcome-rate vector in the fixed order [ok, trap, empty, schema_invalid].
pub(crate) type Rates = [f64; 4];

pub(crate) fn rates(counts: &[usize; 4]) -> Rates {
    let n: usize = counts.iter().sum();
    if n == 0 {
        return [0.0; 4];
    }
    let n = n as f64;
    [
        counts[0] as f64 / n,
        counts[1] as f64 / n,
        counts[2] as f64 / n,
        counts[3] as f64 / n,
    ]
}

/// Drift score between two runs' outcome distributions: the total-variation
/// distance `0.5 * Σ|Δ|`, in [0, 1]. 0 = identical mix, 1 = complete flip
/// (e.g. all-ok → all-empty).
pub(crate) fn drift_score(prev: &Rates, cur: &Rates) -> f64 {
    0.5 * prev
        .iter()
        .zip(cur.iter())
        .map(|(p, c)| (p - c).abs())
        .sum::<f64>()
}

/// Rising-empty-rate flag: the canary fires only on a genuine INCREASE of at
/// least [`EMPTY_RISE_THRESHOLD`] (falling or flat empty rates never flag).
pub(crate) fn empty_rate_rising(prev_empty: f64, cur_empty: f64) -> bool {
    cur_empty - prev_empty >= EMPTY_RISE_THRESHOLD
}

/// Output-shape stats over the OK outputs: (distinct top-level field names
/// across all ok pages, mean top-level field count per ok page). Non-object ok
/// outputs count as zero fields but still enter the mean.
pub(crate) fn shape_stats(oks: &[&Value]) -> (usize, f64) {
    if oks.is_empty() {
        return (0, 0.0);
    }
    let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut total_fields = 0usize;
    for v in oks {
        if let Value::Object(m) = v {
            total_fields += m.len();
            for k in m.keys() {
                names.insert(k.clone());
            }
        }
    }
    (names.len(), total_fields as f64 / oks.len() as f64)
}

/// Host (site) of a URL: scheme and userinfo stripped, lowercased, cut at the
/// first path/query/fragment separator. Unparseable → "unknown" so odd records
/// still land in a bucket instead of vanishing from the report.
pub(crate) fn site_of(url: &str) -> String {
    let rest = match url.find("://") {
        Some(i) => &url[i + 3..],
        None => url,
    };
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .rsplit('@')
        .next()
        .unwrap_or("");
    if host.is_empty() {
        "unknown".to_string()
    } else {
        host.to_ascii_lowercase()
    }
}

/// FNV-1a hash of the site name — the deterministic seed for its sampler.
pub(crate) fn site_seed(site: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in site.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// xorshift64* step — tiny deterministic PRNG, no dependency needed.
fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// Pick `k` sample indices from a NEWEST-FIRST candidate list of length
/// `total`: the newest `ceil(k/2)` pages plus a deterministic-random draw
/// (seeded) from the remainder — recency-weighted so drift reflects the current
/// site, with enough history to catch partial rot. Returns sorted, duplicate-
/// free indices; all of them when `total <= k`.
pub(crate) fn sample_indices(total: usize, k: usize, seed: u64) -> Vec<usize> {
    if total <= k {
        return (0..total).collect();
    }
    let newest = k.div_ceil(2).min(total);
    let mut picked: Vec<usize> = (0..newest).collect();
    let mut pool: Vec<usize> = (newest..total).collect();
    let mut state = seed | 1; // xorshift must not start at 0
    while picked.len() < k && !pool.is_empty() {
        let i = (next_rand(&mut state) % pool.len() as u64) as usize;
        picked.push(pool.swap_remove(i));
    }
    picked.sort_unstable();
    picked
}

/// One stored-page candidate: its RFC3339 observation timestamp (for
/// newest-first ordering) and the record whose `artifact_path` resolves the
/// body. Site bucketing happens at insertion, so the URL itself isn't kept.
struct Candidate {
    observed_at: String,
    record: Record,
}

/// Parsed observatory config from the job params.
struct ObsConfig {
    plugins: Vec<String>,
    sample_per_site: usize,
    src_app: String,
    src_dataset: String,
    out_dataset: String,
}

fn parse_config(ctx: &AppContext) -> Result<ObsConfig> {
    let obs = ctx
        .params
        .get("observatory")
        .cloned()
        .unwrap_or(Value::Null);
    let obs_obj = obs.as_object();
    let plugins: Vec<String> = obs_obj
        .and_then(|m| m.get("plugins"))
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_else(|| ctx.plugins.list());
    if plugins.is_empty() {
        return Err(Error::App(
            "observatory: no plugins requested and none loaded".into(),
        ));
    }
    let loaded = ctx.plugins.list();
    if let Some(unknown) = plugins.iter().find(|p| !loaded.contains(p)) {
        return Err(Error::App(format!(
            "observatory: plugin '{unknown}' is not loaded (see GET /plugins)"
        )));
    }
    let sample_per_site = obs_obj
        .and_then(|m| m.get("sample_per_site"))
        .and_then(Value::as_u64)
        .map(|n| n.max(1) as usize)
        .unwrap_or(DEFAULT_SAMPLE_PER_SITE);
    let src_app = ctx
        .params
        .pointer("/source/app")
        .and_then(Value::as_str)
        .unwrap_or("crawl")
        .to_string();
    let src_dataset = ctx
        .params
        .pointer("/source/dataset")
        .and_then(Value::as_str)
        .unwrap_or("pages")
        .to_string();
    let out_dataset = ctx
        .params
        .get("dataset")
        .and_then(Value::as_str)
        .unwrap_or(OBSERVATORY_DATASET)
        .to_string();
    Ok(ObsConfig {
        plugins,
        sample_per_site,
        src_app,
        src_dataset,
        out_dataset,
    })
}

/// Gather every stored-page candidate — the source dataset's live records
/// ("latest" artifacts) plus the crawl's `page_versions` archive — grouped by
/// site, newest first. Both reads are bounded by [`SOURCE_LIST_LIMIT`].
async fn gather_candidates(
    ctx: &AppContext,
    cfg: &ObsConfig,
) -> Result<BTreeMap<String, Vec<Candidate>>> {
    let mut by_site: BTreeMap<String, Vec<Candidate>> = BTreeMap::new();
    let live = ctx
        .datasets
        .list(&cfg.src_app, &cfg.src_dataset, SOURCE_LIST_LIMIT)
        .await?;
    for r in live {
        if r.removed_at.is_some() || r.data.get("gone").and_then(Value::as_bool).unwrap_or(false) {
            continue;
        }
        let url = r
            .data
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or(&r.key)
            .to_string();
        let observed_at = r.updated_at.to_rfc3339();
        by_site.entry(site_of(&url)).or_default().push(Candidate {
            observed_at,
            record: r,
        });
    }
    let versions = ctx
        .datasets
        .list(&cfg.src_app, VERSIONS_DATASET, SOURCE_LIST_LIMIT)
        .await?;
    for v in versions {
        if v.removed_at.is_some() {
            continue;
        }
        let (Some(url), Some(ts)) = (
            v.data.get("url").and_then(Value::as_str).map(String::from),
            v.data
                .get("fetched_at")
                .and_then(Value::as_str)
                .map(String::from),
        ) else {
            continue;
        };
        by_site.entry(site_of(&url)).or_default().push(Candidate {
            observed_at: ts,
            record: v,
        });
    }
    // Newest-first per site (RFC3339 strings compare chronologically enough for
    // same-corpus ordering; unparseable stamps just sort low).
    for cands in by_site.values_mut() {
        cands.sort_by(|a, b| b.observed_at.cmp(&a.observed_at));
    }
    Ok(by_site)
}

/// The observatory run: sample per site, replay each plugin, classify, diff
/// against the previous run's row, upsert per (plugin, site).
pub(crate) async fn run_observatory(ctx: &AppContext) -> Result<Value> {
    let cfg = parse_config(ctx)?;
    let by_site = gather_candidates(ctx, &cfg).await?;

    let run_at = chrono::Utc::now().to_rfc3339();
    let mut rows: Vec<(String, Value)> = Vec::new();
    let mut flagged: Vec<String> = Vec::new();
    let mut low_confidence_sites = 0usize;
    let mut pages_replayed = 0usize;

    for (site, candidates) in &by_site {
        let total = candidates.len();
        let idx = sample_indices(total, cfg.sample_per_site, site_seed(site));
        if total < LOW_CONFIDENCE_FLOOR {
            low_confidence_sites += 1;
        }
        // Read each sampled body ONCE and share it across all plugins.
        let mut bodies: Vec<String> = Vec::new();
        let mut unreadable = 0usize;
        for &i in &idx {
            match ctx
                .read_source_artifact(&cfg.src_app, &candidates[i].record)
                .await
            {
                Ok(body) if !body.is_empty() => bodies.push(body),
                Ok(_) => bodies.push(String::new()), // classified Empty below
                Err(_) => unreadable += 1,
            }
        }
        for plugin in &cfg.plugins {
            let mut counts = [0usize; 4];
            let mut oks: Vec<Value> = Vec::new();
            let mut elapsed_total_ms = 0.0f64;
            for body in &bodies {
                let start = std::time::Instant::now();
                let res: std::result::Result<Value, String> = if body.is_empty() {
                    Ok(Value::Null)
                } else {
                    ctx.plugins
                        .run(plugin, body, &Value::Null)
                        .await
                        .map_err(|e| e.to_string())
                };
                elapsed_total_ms += start.elapsed().as_secs_f64() * 1000.0;
                let outcome = classify_outcome(&res);
                match outcome {
                    Outcome::Ok => counts[0] += 1,
                    Outcome::Trap => counts[1] += 1,
                    Outcome::Empty => counts[2] += 1,
                    Outcome::SchemaInvalid => counts[3] += 1,
                }
                if outcome == Outcome::Ok {
                    if let Ok(v) = res {
                        oks.push(v);
                    }
                }
            }
            pages_replayed += bodies.len();
            let cur = rates(&counts);
            let ok_refs: Vec<&Value> = oks.iter().collect();
            let (distinct_fields, avg_fields) = shape_stats(&ok_refs);
            let classified: usize = counts.iter().sum();
            let avg_elapsed_ms = if classified > 0 {
                elapsed_total_ms / classified as f64
            } else {
                0.0
            };

            // Diff against the previous run's row BEFORE upserting the new one.
            let key = format!("{plugin}|{site}");
            let prev = ctx.datasets.get(&ctx.app, &cfg.out_dataset, &key).await?;
            let (drift, rising, prev_run_at) = match prev {
                Some(p) => {
                    let prev_rates: Rates = [
                        p.data.pointer("/rates/ok").and_then(Value::as_f64),
                        p.data.pointer("/rates/trap").and_then(Value::as_f64),
                        p.data.pointer("/rates/empty").and_then(Value::as_f64),
                        p.data
                            .pointer("/rates/schema_invalid")
                            .and_then(Value::as_f64),
                    ]
                    .map(|v| v.unwrap_or(0.0));
                    (
                        Some(drift_score(&prev_rates, &cur)),
                        empty_rate_rising(prev_rates[2], cur[2]),
                        p.data
                            .get("run_at")
                            .and_then(Value::as_str)
                            .map(String::from),
                    )
                }
                None => (None, false, None),
            };
            if rising {
                flagged.push(key.clone());
            }
            rows.push((
                key,
                json!({
                    "plugin": plugin,
                    "site": site,
                    "source": { "app": cfg.src_app, "dataset": cfg.src_dataset },
                    "run_at": run_at,
                    "total_pages": total,
                    "sampled": idx.len(),
                    "classified": classified,
                    "unreadable": unreadable,
                    "low_confidence": total < LOW_CONFIDENCE_FLOOR,
                    "outcomes": {
                        "ok": counts[0],
                        "trap": counts[1],
                        "empty": counts[2],
                        "schema_invalid": counts[3],
                    },
                    "rates": {
                        "ok": cur[0],
                        "trap": cur[1],
                        "empty": cur[2],
                        "schema_invalid": cur[3],
                    },
                    "shape": {
                        "distinct_fields": distinct_fields,
                        "avg_fields": avg_fields,
                    },
                    "avg_elapsed_ms": avg_elapsed_ms,
                    "drift_score": drift,
                    "empty_rate_rising": rising,
                    "prev_run_at": prev_run_at,
                }),
            ));
        }
    }

    let summary = ctx.upsert_many(&cfg.out_dataset, &rows).await?;
    Ok(json!({
        "mode": "observatory",
        "plugins": cfg.plugins,
        "source": { "app": cfg.src_app, "dataset": cfg.src_dataset },
        "dataset": cfg.out_dataset,
        "sites": by_site.len(),
        "rows": rows.len(),
        "pages_replayed": pages_replayed,
        "sample_per_site": cfg.sample_per_site,
        "low_confidence_sites": low_confidence_sites,
        "flagged_empty_rising": flagged,
        "new": summary.new.len(),
        "changed": summary.changed.len(),
        "unchanged": summary.unchanged,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- outcome classification -------------------------------------------

    #[test]
    fn classify_traps_on_sandbox_stops() {
        let trap: std::result::Result<Value, String> =
            Err("plugin trapped (fuel/memory/panic): all fuel consumed".into());
        assert_eq!(classify_outcome(&trap), Outcome::Trap);
        let panic: std::result::Result<Value, String> =
            Err("plugin task panicked: JoinError".into());
        assert_eq!(classify_outcome(&panic), Outcome::Trap);
    }

    #[test]
    fn classify_schema_invalid_on_contract_violations() {
        let bad_json: std::result::Result<Value, String> =
            Err("plugin returned invalid JSON: expected value".into());
        assert_eq!(classify_outcome(&bad_json), Outcome::SchemaInvalid);
        let oob: std::result::Result<Value, String> =
            Err("plugin output range out of bounds: ptr=9 len=9 mem=1".into());
        assert_eq!(classify_outcome(&oob), Outcome::SchemaInvalid);
    }

    #[test]
    fn classify_empty_vs_ok() {
        for v in [
            json!(null),
            json!(""),
            json!([]),
            json!({}),
            json!({ "title": null, "tags": [] }),
            json!({ "error": "no <title> found" }),
        ] {
            assert_eq!(classify_outcome(&Ok(v.clone())), Outcome::Empty, "{v}");
        }
        for v in [json!({ "title": "x" }), json!([1]), json!(42)] {
            assert_eq!(classify_outcome(&Ok(v.clone())), Outcome::Ok, "{v}");
        }
    }

    // --- drift-score math --------------------------------------------------

    #[test]
    fn drift_zero_when_identical_one_when_flipped() {
        let a: Rates = [1.0, 0.0, 0.0, 0.0];
        assert_eq!(drift_score(&a, &a), 0.0);
        let flipped: Rates = [0.0, 0.0, 1.0, 0.0];
        assert!((drift_score(&a, &flipped) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn drift_partial_shift_is_total_variation() {
        // 20% of pages moved from ok to empty → TV distance 0.2.
        let prev: Rates = [0.8, 0.0, 0.2, 0.0];
        let cur: Rates = [0.6, 0.0, 0.4, 0.0];
        assert!((drift_score(&prev, &cur) - 0.2).abs() < 1e-9);
    }

    #[test]
    fn empty_rising_only_on_threshold_increase() {
        assert!(empty_rate_rising(0.1, 0.3)); // +0.2 → flag
        assert!(empty_rate_rising(0.0, EMPTY_RISE_THRESHOLD)); // boundary flags
        assert!(!empty_rate_rising(0.1, 0.15)); // +0.05 → below threshold
        assert!(!empty_rate_rising(0.5, 0.3)); // falling never flags
        assert!(!empty_rate_rising(0.4, 0.4)); // flat never flags
    }

    #[test]
    fn rates_sum_to_one_and_empty_input_is_zero() {
        let r = rates(&[2, 1, 1, 0]);
        assert!((r.iter().sum::<f64>() - 1.0).abs() < 1e-9);
        assert_eq!(r[0], 0.5);
        assert_eq!(rates(&[0, 0, 0, 0]), [0.0; 4]);
    }

    // --- sampling honesty --------------------------------------------------

    #[test]
    fn sample_takes_everything_when_corpus_is_small() {
        assert_eq!(sample_indices(3, 25, 42), vec![0, 1, 2]);
        assert_eq!(sample_indices(0, 25, 42), Vec::<usize>::new());
    }

    #[test]
    fn sample_is_newest_plus_random_no_duplicates() {
        let idx = sample_indices(100, 25, site_seed("example.com"));
        assert_eq!(idx.len(), 25);
        // Newest half guaranteed: indices 0..13 (ceil(25/2)) all present.
        for i in 0..13 {
            assert!(idx.contains(&i), "newest index {i} missing");
        }
        // Some of history sampled too, all in bounds, no duplicates.
        assert!(idx.iter().any(|&i| i >= 13));
        assert!(idx.iter().all(|&i| i < 100));
        let mut dedup = idx.clone();
        dedup.dedup();
        assert_eq!(dedup, idx);
    }

    #[test]
    fn sample_is_deterministic_per_seed() {
        let a = sample_indices(200, 25, site_seed("example.com"));
        let b = sample_indices(200, 25, site_seed("example.com"));
        assert_eq!(a, b);
        // A different site (seed) draws a different history sample.
        let c = sample_indices(200, 25, site_seed("other.org"));
        assert_ne!(a, c);
    }

    #[test]
    fn low_confidence_floor_is_five() {
        assert!(4 < LOW_CONFIDENCE_FLOOR);
        assert!(!(5 < LOW_CONFIDENCE_FLOOR));
    }

    // --- shape stats + site bucketing --------------------------------------

    #[test]
    fn shape_stats_counts_distinct_and_average_fields() {
        let a = json!({ "title": "x", "lang": "en" });
        let b = json!({ "title": "y" });
        let c = json!("scalar"); // non-object ok output: 0 fields, still in mean
        let (distinct, avg) = shape_stats(&[&a, &b, &c]);
        assert_eq!(distinct, 2); // title, lang
        assert!((avg - 1.0).abs() < 1e-9); // (2 + 1 + 0) / 3
        assert_eq!(shape_stats(&[]), (0, 0.0));
    }

    #[test]
    fn site_of_strips_scheme_userinfo_port_kept() {
        assert_eq!(site_of("https://Example.COM/a/b?q=1"), "example.com");
        assert_eq!(site_of("http://user@host.io:8080/x"), "host.io:8080");
        assert_eq!(site_of("example.com/path"), "example.com");
        assert_eq!(site_of("https://"), "unknown");
        assert_eq!(site_of(""), "unknown");
    }
}
