//! Observatory mode (M16): corpus-scale differential testing of WASM plugins
//! against the already-stored web. Replays each requested plugin over N sampled
//! stored pages per SITE (host), classifies per-page outcomes
//! (ok / trap / empty / schema_invalid), records timing + output-shape stats,
//! and upserts one row per (plugin, config, site) into the `observatory` dataset
//! with a drift score vs the previous run's row — so change detection + triggers
//! on that dataset surface extraction rot for free, with zero new fetches.
//!
//! **What makes that true rather than merely claimed**: every measurement a row
//! carries is *volatile* — `run_at` moves every run by construction, and so do
//! `avg_elapsed_ms`, the fuel/memory figures, `drift_score` and `prev_run_at`.
//! Change detection hashes the whole canonical value, so writing them through a
//! plain `upsert_many` marked **every row `changed` on every run**:
//! `summary.unchanged` was structurally always 0, a watch on this dataset fired
//! on 100% of its rows every run, and the drift signal the feature exists to
//! raise was buried in universal noise. (`lib.rs` documents that exact
//! anti-pattern as the reason cost lives on the job result — and this file
//! committed it anyway.) Those fields are now declared [`derived_paths`], so
//! they are excluded from the change-detection hash and nothing else: the stored
//! row and every revision still carry them in full.
//!
//! Honest sampling: every row reports `sampled`/`total_pages`; a site with
//! fewer than [`LOW_CONFIDENCE_FLOOR`] stored pages is marked
//! `low_confidence: true`. Sample = newest half + deterministic-random rest
//! (seeded by site name, so reruns over an unchanged corpus pick the same
//! pages and drift reflects the plugin/corpus, not sampler noise).
//!
//! Honest attribution: a sampled page whose stored artifact is **unreadable**
//! or **empty** never reached the plugin, so it is reported as a corpus fact
//! (`unreadable`, `empty_artifacts`) and kept out of `classified`, `rates` and
//! `pages_replayed` — blaming a rotting corpus on the plugin is a false positive
//! on the very canary this feature exists to raise.
//!
//! Cost signal: rows report `cost_signal` — `"fuel"` when the host meters (the
//! wasmtime sandbox does, via `Plugins::run_metered`), `"elapsed_ms"` otherwise.
//! Fuel is deterministic, so a rising `avg_fuel_used` between runs is a real
//! statement about the plugin; `avg_elapsed_ms` also measures whatever else the
//! machine was doing, and is kept as the labelled fallback rather than as the
//! headline. Fuel exhaustion still shows up as a `trap` outcome.

use pumper_core::{AppContext, DerivedPaths, Error, Provenance, Record, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;

use crate::{SOURCE_LIST_LIMIT, VERSIONS_DATASET};

/// Default number of stored pages sampled per site.
pub(crate) const DEFAULT_SAMPLE_PER_SITE: usize = 25;

/// Ceiling on `sample_per_site`.
///
/// One no-argument `{"observatory": true}` audits every loaded plugin over every
/// site, so the replay count is `sites × plugins × sample_per_site` and the
/// host's semaphore caps *parallelism*, not *count*. The same number is the
/// schema's `maximum`, so the enqueue door refuses what this clamp would
/// otherwise silently rewrite.
pub(crate) const MAX_SAMPLE_PER_SITE: usize = 500;

/// Sites with fewer stored pages than this are flagged `low_confidence`.
pub(crate) const LOW_CONFIDENCE_FLOOR: usize = 5;

/// An empty-rate increase of at least this much vs the previous run flags
/// `empty_rate_rising` — the canary for a site that quietly changed markup.
pub(crate) const EMPTY_RISE_THRESHOLD: f64 = 0.10;

/// Default output dataset for observatory rows (`plugin/observatory`).
const OBSERVATORY_DATASET: &str = "observatory";

/// The row fields this audit **measures about the run** rather than observes
/// about the plugin's behaviour — excluded from the change-detection hash.
///
/// Named once, because two things have to agree about the list: the row builder
/// and the declaration at the upsert. Each entry is volatile by construction:
///
/// - `run_at` / `prev_run_at` — the clock. `run_at` alone guaranteed every row
///   was `changed` on every run.
/// - `avg_elapsed_ms` — wall clock; measures the machine's load as much as the
///   plugin's appetite. Never equal twice.
/// - `avg_fuel_used` / `max_fuel_used` / `max_memory_bytes` — the metered
///   figures. Fuel is deterministic *per input*, but the sample is not
///   guaranteed identical when the corpus grows, and a fractional average
///   almost never repeats.
/// - `drift_score` — a statement about the PAIR of runs, not about this
///   replay. It is computed from `rates`, which stays in the identity, so a
///   real behaviour change still fires; but adopting it into the hash would
///   mark a row `changed` the first time it settles (`null` → `0.0` on the
///   second run ever, `d` → `0.0` on the run after any real change) — movement
///   that is bookkeeping, not news.
///
/// Deliberately NOT derived: `total_pages`, `sampled`, `classified`,
/// `unreadable`, `empty_artifacts`, `outcomes`, `rates`, `shape`,
/// `low_confidence`, `empty_rate_rising`, `params`. Every one of those is a
/// fact about the corpus or the plugin, and movement in it IS the news this
/// dataset exists to carry.
const DERIVED_ROW_FIELDS: [&str; 7] = [
    "run_at",
    "prev_run_at",
    "avg_elapsed_ms",
    "avg_fuel_used",
    "max_fuel_used",
    "max_memory_bytes",
    "drift_score",
];

/// The record paths this app **derives** rather than observes — see
/// [`DERIVED_ROW_FIELDS`].
///
/// One-time cost of the declaration: the first run after deploy re-hashes every
/// stored observatory row, so they report `changed` once and then settle. Given
/// they reported `changed` on *every* run before, that is a strict improvement
/// from run two onwards.
pub(crate) fn derived_paths() -> DerivedPaths {
    DerivedPaths::new(DERIVED_ROW_FIELDS)
}

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

/// Classify one replay result.
///
/// The anti-pattern this replaces: classification used to match SUBSTRINGS of
/// the host's own error prose (`"trapped"`, `"panicked"`). Rewording one
/// `format!` in engine-wasm silently reclassified every row this app writes —
/// and drift scores are computed against those rows — with no test anywhere
/// failing. The host now carries a typed [`PluginFailure`], so the class is
/// read from the type and the message is free to say whatever is useful.
///
/// [`PluginFailure`]: pumper_core::error::PluginFailure
pub(crate) fn classify_outcome(res: &std::result::Result<Value, Error>) -> Outcome {
    use pumper_core::error::PluginFailure;
    match res {
        // `Trap` is the only class that means "the sandbox stopped it" — fuel
        // exhaustion, the memory cap and an explicit trap all arrive that way.
        // Every other class (a module that never exported the ABI, output that
        // is not the contract, an unknown name, a host fault) is a contract
        // violation from this report's point of view: the plugin produced no
        // usable answer for a reason that is not resource pressure.
        Err(e) => match e.plugin_failure() {
            Some(PluginFailure::Trap) => Outcome::Trap,
            _ => Outcome::SchemaInvalid,
        },
        Ok(v) => {
            if is_empty_output(v) {
                Outcome::Empty
            } else {
                Outcome::Ok
            }
        }
    }
}

/// How one sampled page resolved **before** any plugin ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageSource {
    /// A stored body the plugin can be replayed over.
    Replayable,
    /// The artifact read fine and held zero bytes.
    Empty,
    /// The artifact could not be read at all (reclaimed, deleted, bad path).
    Unreadable,
}

/// Classify a stored-artifact read, before the plugin is called.
///
/// THE ANTI-PATTERN THIS CLOSES: an artifact that read fine but held **zero
/// bytes** was pushed into the body list as an empty string, short-circuited to
/// `Ok(Value::Null)` *without calling the plugin*, and then bucketed `Empty` by
/// [`classify_outcome`] — which is the plugin's bucket. So a crawl that stored
/// zero-byte bodies inflated the site's empty rate, could trip
/// [`empty_rate_rising`], and inflated `drift_score`: a false positive on the
/// exact canary this feature exists to raise, attributed to the plugin instead
/// of to the corpus. Those pages also counted in `pages_replayed` though nothing
/// was replayed. `unreadable` was already tracked separately at the same seam —
/// the author's intent was clear, and empty just fell through the wrong side.
pub(crate) fn classify_page(read: &std::result::Result<String, String>) -> PageSource {
    match read {
        Ok(body) if body.is_empty() => PageSource::Empty,
        Ok(_) => PageSource::Replayable,
        Err(_) => PageSource::Unreadable,
    }
}

/// FNV-1a over bytes — the deterministic, dependency-free hash this file uses
/// for both the per-site sampler seed and the params fingerprint.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Row key for one (plugin, config, site).
///
/// A replay with no params keeps the historic `{plugin}|{site}` key, so existing
/// rows, watches and drift history survive the change. A **configured** replay
/// adds a short fingerprint of the params envelope: `plugin_params` is this
/// app's flagship feature ("one plugin configured per job instead of
/// recompiling a module per variation"), and two configurations of one plugin
/// sharing a key would overwrite each other's drift history run after run.
///
/// The fingerprint is over `serde_json`'s canonical serialization — its `Map` is
/// a `BTreeMap`, so key order is stable and the same config always fingerprints
/// the same way.
pub(crate) fn row_key(plugin: &str, params: &Value, site: &str) -> String {
    match params_fingerprint(params) {
        Some(fp) => format!("{plugin}@{fp}|{site}"),
        None => format!("{plugin}|{site}"),
    }
}

/// A short stable fingerprint of a params envelope, or `None` for "no
/// configuration" (`null`, or an empty object — both mean the plugin ran with
/// its own defaults, which is what the un-suffixed key already means).
pub(crate) fn params_fingerprint(params: &Value) -> Option<String> {
    let empty = match params {
        Value::Null => true,
        Value::Object(m) => m.is_empty(),
        _ => false,
    };
    if empty {
        return None;
    }
    Some(format!(
        "{:08x}",
        fnv1a(params.to_string().as_bytes()) >> 32
    ))
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
    fnv1a(site.as_bytes())
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

/// One audited plugin **and the configuration it is replayed with**.
///
/// THE ANTI-PATTERN THIS CLOSES: every plugin was replayed with
/// `&Value::Null`, though `plugin_params` is this app's flagship feature — "one
/// plugin configured per job instead of recompiling a module per variation" —
/// and the reference plugin `title-extractor` reads `params.tag`. A plugin that
/// only produces output *under a configuration* was therefore classified
/// `Empty` (or `SchemaInvalid`) at every site, forever. Because the rate never
/// *rose*, `empty_rate_rising` never flagged it, `drift_score` compared two
/// meaningless distributions, and the row read `low_confidence: false` and
/// looked authoritative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuditedPlugin {
    pub(crate) name: String,
    pub(crate) params: Value,
}

/// Parsed observatory config from the job params.
struct ObsConfig {
    plugins: Vec<AuditedPlugin>,
    sample_per_site: usize,
    src_app: String,
    src_dataset: String,
    out_dataset: String,
}

/// The plugin list to audit, with each entry's params resolved.
///
/// An entry may be a bare `"name"` — which inherits the job-level
/// `plugin_params` envelope, the same one every other mode of this app forwards
/// — or an object `{"name": .., "params": {..}}`, which wins over it. Omitting
/// `plugins` audits every loaded plugin, each with the job-level envelope.
/// Entries that are neither a string nor a named object are skipped rather than
/// silently audited under an empty name.
pub(crate) fn parse_audited_plugins(
    requested: Option<&Value>,
    loaded: &[String],
    job_params: &Value,
) -> Vec<AuditedPlugin> {
    let Some(list) = requested.and_then(Value::as_array) else {
        return loaded
            .iter()
            .map(|name| AuditedPlugin {
                name: name.clone(),
                params: job_params.clone(),
            })
            .collect();
    };
    list.iter()
        .filter_map(|entry| match entry {
            Value::String(name) => Some(AuditedPlugin {
                name: name.clone(),
                params: job_params.clone(),
            }),
            Value::Object(m) => m
                .get("name")
                .and_then(Value::as_str)
                .map(|name| AuditedPlugin {
                    name: name.to_string(),
                    params: m
                        .get("params")
                        .cloned()
                        .unwrap_or_else(|| job_params.clone()),
                }),
            _ => None,
        })
        .collect()
}

fn parse_config(ctx: &AppContext) -> Result<ObsConfig> {
    let obs = ctx
        .params
        .get("observatory")
        .cloned()
        .unwrap_or(Value::Null);
    let obs_obj = obs.as_object();
    let loaded = ctx.plugins.list();
    let plugins = parse_audited_plugins(
        obs_obj.and_then(|m| m.get("plugins")),
        &loaded,
        &crate::plugin_params(ctx),
    );
    if plugins.is_empty() {
        return Err(Error::App(
            "observatory: no plugins requested and none loaded".into(),
        ));
    }
    if let Some(unknown) = plugins.iter().find(|p| !loaded.contains(&p.name)) {
        return Err(Error::App(format!(
            "observatory: plugin '{}' is not loaded (see GET /plugins)",
            unknown.name
        )));
    }
    let sample_per_site = obs_obj
        .and_then(|m| m.get("sample_per_site"))
        .and_then(Value::as_u64)
        .map(|n| (n.max(1) as usize).min(MAX_SAMPLE_PER_SITE))
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
    // Corpus-level, counted ONCE per site (the bodies are read once and shared
    // across every audited plugin) — unlike `pages_replayed`, which is
    // plugin × page by construction.
    let mut pages_unreadable = 0usize;
    let mut pages_empty = 0usize;

    for (site, candidates) in &by_site {
        let total = candidates.len();
        let idx = sample_indices(total, cfg.sample_per_site, site_seed(site));
        if total < LOW_CONFIDENCE_FLOOR {
            low_confidence_sites += 1;
        }
        // Read each sampled body ONCE and share it across all plugins. A page
        // that never reaches the plugin is a CORPUS fact and is counted as one
        // — see [`classify_page`].
        let mut bodies: Vec<String> = Vec::new();
        let mut unreadable = 0usize;
        let mut empty_artifacts = 0usize;
        for &i in &idx {
            let read = ctx
                .read_source_artifact(&cfg.src_app, &candidates[i].record)
                .await;
            match classify_page(&read) {
                PageSource::Replayable => bodies.push(read.unwrap_or_default()),
                PageSource::Empty => empty_artifacts += 1,
                PageSource::Unreadable => unreadable += 1,
            }
        }
        pages_unreadable += unreadable;
        pages_empty += empty_artifacts;
        for audited in &cfg.plugins {
            let plugin = &audited.name;
            let mut counts = [0usize; 4];
            let mut oks: Vec<Value> = Vec::new();
            let mut elapsed_total_ms = 0.0f64;
            let mut cost = crate::CostRollup::default();
            for body in &bodies {
                let start = std::time::Instant::now();
                // Replayed with the params the plugin is CONFIGURED with — a
                // `Null` here made every params-aware module look permanently
                // broken (see [`AuditedPlugin`]).
                let res: std::result::Result<Value, Error> =
                    match ctx.plugins.run_metered(plugin, body, &audited.params).await {
                        Ok((value, stats)) => {
                            cost.record(&stats);
                            Ok(value)
                        }
                        Err(e) => Err(e),
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
            let key = row_key(plugin, &audited.params, site);
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
                    // The configuration this row was measured under. Two configs
                    // of one plugin are two rows, so neither overwrites the
                    // other's drift history.
                    "params": audited.params,
                    "site": site,
                    "source": { "app": cfg.src_app, "dataset": cfg.src_dataset },
                    "run_at": run_at,
                    "total_pages": total,
                    "sampled": idx.len(),
                    "classified": classified,
                    "unreadable": unreadable,
                    // Sampled pages whose stored body was zero bytes: the plugin
                    // was never called, so this is a corpus problem, reported as
                    // one instead of inflating the plugin's empty rate.
                    "empty_artifacts": empty_artifacts,
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
                    // Which number to READ as this row's cost. `avg_elapsed_ms`
                    // stays for continuity (and is the honest answer against an
                    // unmetered host), but where the sandbox meters, fuel is the
                    // deterministic signal a drift comparison actually wants.
                    "cost_signal": cost.signal(),
                    "avg_fuel_used": cost.avg_fuel(),
                    "max_fuel_used": cost.max_fuel(),
                    "fuel_budget": cost.fuel_budget(),
                    "max_memory_bytes": cost.max_memory(),
                    "drift_score": drift,
                    "empty_rate_rising": rising,
                    "prev_run_at": prev_run_at,
                }),
            ));
        }
    }

    // The measurement fields are declared DERIVED, so a re-run over an unchanged
    // corpus with unchanged plugin behaviour reports its rows `unchanged` — which
    // is what makes "change detection + triggers on that dataset surface
    // extraction rot for free" a true sentence rather than an aspiration.
    let summary = ctx
        .upsert_many_with_derived(
            &cfg.out_dataset,
            &rows,
            Provenance::default(),
            &derived_paths(),
        )
        .await?;
    // DELIBERATELY no `index_datasets`, unlike the three write modes.
    //
    // The pairing that forces it there does not exist here: those modes echo a
    // BOUNDED `records` sample, so without delegation their search coverage
    // would silently shrink to the first N outputs of a run. Observatory echoes
    // no records at all — there is nothing to shrink, so declaring the spec
    // would be a new capability, not the other half of a fix.
    //
    // And it is the wrong capability. These rows are operational diagnostics
    // about the fleet, not scraped content: `/search` is a corpus of records,
    // and adding per-(plugin, site) telemetry would dilute its facets and its
    // `?dataset=` filter with fleet state — the same reason `mpsv-vpm` keeps
    // `freshness` and `vacancy_ledger` out of its own declaration. A hit would
    // render untitled too, since the indexer builds a title from a record's
    // `title`/`name`/`headline`/`full_name` and these rows carry none.
    //
    // Crucially, this costs the INTENDED consumer nothing. A watch or dataset
    // trigger on `plugin/observatory` reads the change feed, not the index, and
    // the worker's `run_indexed_apps` always includes the job's own app — which
    // IS `plugin` here — so the hook batch loads these revisions either way.
    // Making that feed honest was this direction's whole job.
    Ok(json!({
        "mode": "observatory",
        "plugins": cfg.plugins.iter().map(|p| p.name.clone()).collect::<Vec<String>>(),
        "source": { "app": cfg.src_app, "dataset": cfg.src_dataset },
        "dataset": cfg.out_dataset,
        "sites": by_site.len(),
        "rows": rows.len(),
        "pages_replayed": pages_replayed,
        // Corpus problems, kept out of every plugin verdict above.
        "pages_unreadable": pages_unreadable,
        "pages_empty": pages_empty,
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

    use pumper_core::error::PluginFailure;

    fn failed(kind: PluginFailure, message: &str) -> std::result::Result<Value, Error> {
        Err(Error::plugin(kind, "title", message))
    }

    #[test]
    fn classify_traps_on_sandbox_stops() {
        assert_eq!(
            classify_outcome(&failed(PluginFailure::Trap, "all fuel consumed")),
            Outcome::Trap
        );
    }

    #[test]
    fn classify_schema_invalid_on_contract_violations() {
        assert_eq!(
            classify_outcome(&failed(
                PluginFailure::MalformedOutput,
                "returned invalid JSON: expected value"
            )),
            Outcome::SchemaInvalid
        );
        assert_eq!(
            classify_outcome(&failed(
                PluginFailure::MalformedOutput,
                "output range out of bounds: ptr=9 len=9 mem=1"
            )),
            Outcome::SchemaInvalid
        );
        // A module that never exported the ABI produced no answer either, and
        // it is not resource pressure — so it is not a trap.
        assert_eq!(
            classify_outcome(&failed(PluginFailure::MissingExport, "exports no 'memory'")),
            Outcome::SchemaInvalid
        );
    }

    /// THE anti-pattern: classification used to read substrings of the host's
    /// prose, so rewording one `format!` in engine-wasm reclassified stored
    /// rows — and drift is computed against those rows — with nothing failing.
    /// The class must be immune to the message, in both directions.
    #[test]
    fn classification_survives_rewording_and_is_not_fooled_by_lookalike_prose() {
        // A trap stays a trap however it is phrased — including phrasings that
        // contain none of the old marker words.
        for message in ["fuel budget exhausted", "the sandbox stopped it", ""] {
            assert_eq!(
                classify_outcome(&failed(PluginFailure::Trap, message)),
                Outcome::Trap,
                "reworded trap misclassified: {message:?}"
            );
        }
        // …and prose that merely LOOKS like a trap is not one. Under the old
        // substring rule both of these counted as sandbox stops.
        assert_eq!(
            classify_outcome(&failed(
                PluginFailure::MalformedOutput,
                "the plugin trapped the value in a string it then panicked on"
            )),
            Outcome::SchemaInvalid
        );
        // An error from outside the plugin host carries no class at all and
        // must not be promoted into the trap bucket.
        assert_eq!(
            classify_outcome(&Err(Error::App("plugin trapped, allegedly".into()))),
            Outcome::SchemaInvalid
        );
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
            assert_eq!(
                classify_outcome(&Ok::<Value, Error>(v.clone())),
                Outcome::Empty,
                "{v}"
            );
        }
        for v in [json!({ "title": "x" }), json!([1]), json!(42)] {
            assert_eq!(
                classify_outcome(&Ok::<Value, Error>(v.clone())),
                Outcome::Ok,
                "{v}"
            );
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
        assert_eq!(LOW_CONFIDENCE_FLOOR, 5);
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

    // --- the change identity ------------------------------------------------

    /// The declaration names exactly the volatile fields the row builder writes
    /// — if those ever drift apart the seam silently stops working, which is
    /// indistinguishable from it never having been added.
    #[test]
    fn the_derived_declaration_names_every_volatile_row_field() {
        assert_eq!(derived_paths(), DerivedPaths::new(DERIVED_ROW_FIELDS));
        assert!(!derived_paths().is_empty());
        // Nothing that carries the audit's actual FINDINGS may be derived —
        // deriving `rates` or `outcomes` would silence the signal instead of
        // the noise, which is the opposite failure and a much worse one.
        for finding in [
            "rates",
            "outcomes",
            "shape",
            "total_pages",
            "sampled",
            "classified",
            "unreadable",
            "empty_artifacts",
            "low_confidence",
            "empty_rate_rising",
            "params",
            "plugin",
            "site",
        ] {
            assert!(
                !DERIVED_ROW_FIELDS.contains(&finding),
                "{finding} is a finding, not telemetry — it must stay in the \
                 change-detection hash"
            );
        }
    }

    // --- corpus vs plugin attribution ---------------------------------------

    /// THE anti-pattern: a zero-byte stored artifact short-circuited to
    /// `Ok(Value::Null)` WITHOUT calling the plugin and was then bucketed
    /// `Empty` — the plugin's bucket. A crawl that stored empty bodies could
    /// therefore trip `empty_rate_rising` and inflate `drift_score`: a false
    /// positive on the exact canary this feature exists to raise, blamed on the
    /// plugin instead of the corpus.
    #[test]
    fn an_empty_stored_artifact_is_a_corpus_fact_not_a_plugin_empty() {
        assert_eq!(
            classify_page(&Ok("<h1>hi</h1>".to_string())),
            PageSource::Replayable
        );
        assert_eq!(classify_page(&Ok(String::new())), PageSource::Empty);
        assert_eq!(
            classify_page(&Err("unreadable artifact".to_string())),
            PageSource::Unreadable
        );
        // …and the plugin's own empty output is still the plugin's, so the two
        // are not collapsed in the other direction either.
        assert_eq!(
            classify_outcome(&Ok::<Value, Error>(json!({}))),
            Outcome::Empty
        );
    }

    // --- configured replays --------------------------------------------------

    /// THE anti-pattern: every plugin was replayed with `params: null`, so a
    /// module that only produces output under a configuration was `Empty` at
    /// every site forever — never *rising*, so never flagged, while the row read
    /// `low_confidence: false`.
    #[test]
    fn a_bare_name_inherits_the_job_params_and_an_object_overrides_them() {
        let loaded = vec!["title".to_string(), "delta-slim".to_string()];
        let job = json!({ "tag": "h2" });

        // Omitting `plugins` audits everything loaded, each with the job envelope.
        let all = parse_audited_plugins(None, &loaded, &job);
        assert_eq!(all.len(), 2);
        assert!(all.iter().all(|p| p.params == job));

        // A bare name inherits it; an object wins over it.
        let mixed = parse_audited_plugins(
            Some(&json!(["title", { "name": "delta-slim", "params": { "tag": "h3" } }])),
            &loaded,
            &job,
        );
        assert_eq!(
            mixed,
            vec![
                AuditedPlugin {
                    name: "title".into(),
                    params: json!({ "tag": "h2" })
                },
                AuditedPlugin {
                    name: "delta-slim".into(),
                    params: json!({ "tag": "h3" })
                },
            ]
        );
        // An object with no `params` still inherits.
        let inherit = parse_audited_plugins(Some(&json!([{ "name": "title" }])), &loaded, &job);
        assert_eq!(inherit[0].params, job);
        // Junk entries are skipped rather than audited under an empty name.
        assert!(parse_audited_plugins(Some(&json!([7, null, {}])), &loaded, &job).is_empty());
    }

    /// Two configurations of one plugin must not overwrite each other's drift
    /// history — and an unconfigured replay must keep the historic key, or every
    /// stored row and every watch on this dataset is orphaned on deploy.
    #[test]
    fn a_configured_replay_gets_its_own_row_while_the_default_keeps_its_key() {
        assert_eq!(
            row_key("title", &Value::Null, "example.com"),
            "title|example.com"
        );
        // An empty object means "no configuration" just as `null` does.
        assert_eq!(
            row_key("title", &json!({}), "example.com"),
            "title|example.com"
        );

        let h2 = row_key("title", &json!({ "tag": "h2" }), "example.com");
        let h3 = row_key("title", &json!({ "tag": "h3" }), "example.com");
        assert_ne!(h2, h3, "two configs, two rows");
        assert!(
            h2.starts_with("title@") && h2.ends_with("|example.com"),
            "{h2}"
        );
        // Stable across calls, and insensitive to key ORDER (serde_json's Map is
        // a BTreeMap, so the canonical form is what gets hashed).
        assert_eq!(h2, row_key("title", &json!({ "tag": "h2" }), "example.com"));
        assert_eq!(
            row_key("title", &json!({ "a": 1, "b": 2 }), "s"),
            row_key("title", &json!({ "b": 2, "a": 1 }), "s")
        );
        // …and the site still separates rows of one config.
        assert_ne!(h2, row_key("title", &json!({ "tag": "h2" }), "other.org"));
    }

    #[test]
    fn sample_per_site_has_a_ceiling_matching_the_schema() {
        use pumper_core::ScrapeApp;
        // `{"observatory": true}` alone is sites x plugins x sample_per_site
        // wasm executions, and the host's semaphore caps parallelism, not count.
        assert_eq!(MAX_SAMPLE_PER_SITE, 500);
        assert_eq!(
            crate::Plugin
                .manifest()
                .params_schema
                .unwrap()
                .pointer("/properties/observatory/oneOf/1/properties/sample_per_site/maximum")
                .and_then(Value::as_u64),
            Some(MAX_SAMPLE_PER_SITE as u64),
            "the clamp and the schema's maximum must be the same number"
        );
    }
}
