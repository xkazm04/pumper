//! Generic extraction app: fetch a list of URLs (tiered) and run a declarative
//! rule set over all of them in parallel across every CPU core. Showcases the
//! no-GIL, SIMD extraction engine — the fetched documents are parsed and
//! extracted concurrently in one process, then deduped into a dataset.

use std::sync::Arc;

use app_crawl::reliability;
use async_trait::async_trait;
use futures::StreamExt;
use pumper_core::config::ArchiveConfig;
use pumper_core::extract::extract_batch_with_report_at;
use pumper_core::{
    extract_and_fingerprint_batch, signals_batch, AppContext, AppManifest, CompiledRuleSet,
    CostClass, DocReport, DocSignals, Error, FetchHealth, FetchRequest, FetchStrategy, FieldStatus,
    ManifestExample, ObservedDoc, Provenance, Record, Result, RuleSet, ScrapeApp, UpsertSummary,
};
use pumper_engine_archive::ArchiveEngine;
use serde_json::{json, Value};

pub struct Extractor;

mod induce;
mod replay;

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

/// The no-keys sweep cap actually in force: `source.limit`, clamped into
/// `1..=`[`SOURCE_LIST_LIMIT`], defaulting to the ceiling.
///
/// Lowering it is the only way to exercise the truncation signal without a
/// 10,000-record fixture, and it doubles as a bounded smoke run over a large
/// corpus. It can only narrow the sweep — the ceiling is what keeps one job
/// from reading an unbounded dataset into memory.
fn parse_source_limit(source: &serde_json::Map<String, Value>) -> i64 {
    source
        .get("limit")
        .and_then(Value::as_u64)
        .map(|n| (n.max(1) as i64).min(SOURCE_LIST_LIMIT))
        .unwrap_or(SOURCE_LIST_LIMIT)
}

/// How many extracted records a write mode echoes into its persisted job
/// result by default, and the ceiling `records_echo` clamps to.
///
/// The echo used to be EVERY record. A 10,000-record run wrote a multi-MB JSON
/// blob into the `jobs` row, and that blob then rode the `job.succeeded`
/// webhook, the SSE event and the receipt — forever, for data that is already
/// durably in the dataset. A bounded prefix keeps the result a *sample* (which
/// is what a human or agent reading a job result actually wants) while the
/// dataset stays the record of truth; `index_datasets` is what keeps the full
/// set searchable (see [`ExtractOutcome::merge_into`]).
const DEFAULT_RECORDS_ECHO: usize = 100;
const MAX_RECORDS_ECHO: usize = 1000;

/// Pure param parse for the records echo: `records_echo`, clamped into
/// `0..=`[`MAX_RECORDS_ECHO`], defaulting to [`DEFAULT_RECORDS_ECHO`].
///
/// `0` is legal and means "counts only" — a caller streaming from the dataset
/// has no use for the echo at all. There is deliberately no "unbounded" option:
/// the whole point is that the persisted result has a size bound.
fn parse_records_echo(params: &Value) -> usize {
    params
        .get("records_echo")
        .and_then(Value::as_u64)
        .map(|n| (n as usize).min(MAX_RECORDS_ECHO))
        .unwrap_or(DEFAULT_RECORDS_ECHO)
}

/// Whether a capped dataset sweep may have left records behind.
///
/// THE ANTI-PATTERN THIS CLOSES: a full page means the CAP decided where the
/// sweep stopped, not the dataset. A 12,000-record source extracted 10,000 and
/// reported `requested: 10000` — a number indistinguishable from a dataset that
/// really does hold 10,000 rows, so nothing downstream could tell a complete
/// run from a silently partial one.
///
/// Judged on the page the store returned, BEFORE the removed/gone filter: how
/// many rows survived that filter says nothing about whether more rows exist
/// past the cap.
fn sweep_truncated(returned: usize, limit: i64) -> bool {
    limit > 0 && returned as i64 >= limit
}

/// Which key list a source-mode run is working from, and whether that list is
/// the WHOLE work scope.
///
/// THE ANTI-PATTERN THIS CLOSES: `source.keys` and the firing trigger's
/// `_trigger.keys` used to be collapsed into one `Option<Vec<String>>` by an
/// `.or_else()`, which destroyed exactly the provenance the result has to
/// report. A caller-supplied `keys` list is genuinely uncapped; a trigger hop's
/// list is capped at `[triggers] key_cap` (default 200) and the host declares
/// the cap in the unforgeable `_trigger.keys_truncated`. With the two collapsed,
/// a hop that extracted 200 of 5,000 changed records emitted `truncated: false`
/// — a clean, complete run — because `truncated` was only ever written by the
/// no-keys sweep branch.
#[derive(Debug, Default, PartialEq)]
struct KeySelection {
    /// The keys to process, or `None` for "sweep every live record".
    keys: Option<Vec<String>>,
    /// This key list is a PARTIAL work scope: the host capped the firing
    /// trigger's key list. Never set for a caller-supplied `source.keys`.
    truncated: bool,
}

/// Key precedence: explicit `source.keys` > `_trigger.keys` (crawl→extract via a
/// dataset trigger) > all live records in the source dataset.
fn select_keys(source: &serde_json::Map<String, Value>, params: &Value) -> KeySelection {
    fn str_array(v: Option<&Value>) -> Option<Vec<String>> {
        v.and_then(Value::as_array).map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
    }
    if let Some(keys) = str_array(source.get("keys")) {
        // The caller named the set: no cap applied to it, whatever the hop that
        // carried the job said about ITS key list.
        return KeySelection {
            keys: Some(keys),
            truncated: false,
        };
    }
    match str_array(params.pointer("/_trigger/keys")) {
        // Host-owned and unforgeable (`crates/server/src/triggers.rs`
        // HOST_OWNED_KEYS), so it is read rather than inferred by comparing
        // `keys.len()` against `_trigger.count`.
        Some(keys) => KeySelection {
            truncated: params
                .pointer("/_trigger/keys_truncated")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            keys: Some(keys),
        },
        None => KeySelection::default(),
    }
}

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

/// Hard ceiling on the in-flight fetch fan-out, whatever the caller asks for.
///
/// Every in-flight fetch holds a socket (and, on the browser tier, a tab), and
/// the per-host governor serializes hosts but caps nothing globally — so a
/// `concurrency: 100000` on a wide URL list is an fd-exhaustion request. The
/// same number is declared as the schema's `maximum`, so the enqueue door
/// refuses what this clamp would otherwise silently rewrite: **one bound, two
/// layers, never two different answers**.
const MAX_FETCH_CONCURRENCY: usize = 64;

/// Read the `concurrency` param (max in-flight fetches), clamped into
/// `1..=`[`MAX_FETCH_CONCURRENCY`] and defaulting to
/// [`DEFAULT_FETCH_CONCURRENCY`]. Bounds the URL-list fan-out so a large `urls`
/// list can't open one socket per URL at once.
fn fetch_concurrency(ctx: &AppContext) -> usize {
    parse_concurrency(&ctx.params)
}

/// Pure param parse for [`fetch_concurrency`] — clamps `concurrency` into
/// `1..=`[`MAX_FETCH_CONCURRENCY`], defaulting to
/// [`DEFAULT_FETCH_CONCURRENCY`].
fn parse_concurrency(params: &Value) -> usize {
    params
        .get("concurrency")
        .and_then(Value::as_u64)
        .map(|n| (n.max(1) as usize).min(MAX_FETCH_CONCURRENCY))
        .unwrap_or(DEFAULT_FETCH_CONCURRENCY)
}

/// The one mode a run executes. A params object may declare exactly one; see
/// [`resolve_run_mode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    /// `replay` — read-only rule-set CI over stored bodies.
    Replay,
    /// `induce` — read-only wrapper induction over stored bodies.
    Induce,
    /// `rules` + `urls` — fetch live and extract.
    Urls,
    /// `rules` + `source` — extract stored bodies (incl. backfill / archive).
    Source,
}

/// Every param root that declares a mode, in the order a conflict names them.
///
/// `rules` is in the set even though it is not a mode by itself: it is the
/// marker of "this job intends to WRITE records", and a `rules` sitting next to
/// a `replay` (which carries its own `replay.rules`) is exactly the confusion
/// this check exists to refuse.
const MODE_ROOTS: [&str; 5] = ["replay", "induce", "rules", "source", "urls"];

/// The read-only modes — each one owns the whole params object.
const READ_ONLY_ROOTS: [&str; 2] = ["replay", "induce"];

/// Resolves the ONE mode a params object requests, or names every conflicting
/// root it carries.
///
/// THE ANTI-PATTERN THIS CLOSES: `run()` used to test the roots in a fixed order
/// — `replay` > `induce` > `rules`, and inside rules-mode `source` > `urls` —
/// and execute the first that matched, returning `200` for the rest. A caller
/// who submitted `{rules, urls, replay}` believed an extraction had written
/// records; only a read-only replay ran, and nothing in the result said so. The
/// manifest's own prose already called these "mutually exclusive"; first-match
/// precedence is not exclusivity, it is a silent-wrong-result.
///
/// A JSON `null` counts as absent: `{"replay": null}` is how a params template
/// spells "not this run", and treating it as a declaration would refuse jobs
/// that ask for nothing at all.
fn resolve_run_mode(params: &Value) -> std::result::Result<RunMode, String> {
    let declared: Vec<&'static str> = MODE_ROOTS
        .iter()
        .copied()
        .filter(|k| params.get(*k).is_some_and(|v| !v.is_null()))
        .collect();
    let conflict = || {
        Err(format!(
            "conflicting extractor modes: {} — a job runs exactly ONE mode \
             (replay | induce | rules+urls | rules+source)",
            declared.join(" + ")
        ))
    };
    let has = |k: &str| declared.contains(&k);
    // A read-only root tolerates no company at all — not another read-only
    // root, and not the write roots whose records it would never produce.
    if READ_ONLY_ROOTS.iter().any(|k| has(k)) && declared.len() > 1 {
        return conflict();
    }
    // Inside write mode the two input roots are the exclusive pair: `source`
    // used to win and the `urls` list was never fetched.
    if has("source") && has("urls") {
        return conflict();
    }
    if has("replay") {
        return Ok(RunMode::Replay);
    }
    if has("induce") {
        return Ok(RunMode::Induce);
    }
    if has("source") {
        return Ok(RunMode::Source);
    }
    // `rules` alone (or nothing at all) resolves to urls mode, which reports the
    // missing input list itself — the enqueue door already refuses the shape.
    Ok(RunMode::Urls)
}

/// Running totals for one INNER field of an `each` listing, pooled across every
/// document of the run. The denominator is listing ITEMS, not documents — a
/// listing's inner selector is attempted once per card, not once per page.
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct InnerRollup {
    #[serde(default)]
    items: u64,
    #[serde(default)]
    hits: u64,
    #[serde(default)]
    misses: u64,
    #[serde(default)]
    errors: u64,
}

impl InnerRollup {
    fn add(&mut self, s: &pumper_core::extract::InnerFieldStats) {
        self.items += s.items as u64;
        self.hits += s.hits() as u64;
        self.misses += s.misses() as u64;
        self.errors += s.error as u64;
    }

    fn merge(&mut self, o: &InnerRollup) {
        self.items += o.items;
        self.hits += o.hits;
        self.misses += o.misses;
        self.errors += o.errors;
    }
}

/// One top-level field's miss tally. A named struct rather than a tuple because
/// it is serialized into the backfill checkpoint, where `(u64, u64)` would be a
/// positional blob nobody can read six months later.
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct FieldMisses {
    #[serde(default)]
    misses: u64,
    #[serde(default)]
    errors: u64,
}

/// The run's extraction-quality counters, **poolable**.
///
/// Rendering `worst_fields` used to happen once per `extract_and_upsert` call
/// and be thrown away, which is why the backfill mode — the one mode that calls
/// it repeatedly — reported `fields_matched`/`fields_total` pooled across every
/// batch but no `worst_fields` at all: there was nothing to pool them WITH. The
/// counters live here so a multi-batch run's breakdown is the same shape as a
/// single-batch run's, with cumulative denominators.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct QualityRollup {
    #[serde(default)]
    matched: u64,
    #[serde(default)]
    total: u64,
    /// Documents seen — the denominator of a document-scoped `miss_rate`.
    #[serde(default)]
    docs: u64,
    #[serde(default)]
    fields: std::collections::BTreeMap<String, FieldMisses>,
    /// Dotted inner path (`products.price`) -> pooled item counts.
    #[serde(default)]
    inner: std::collections::BTreeMap<String, InnerRollup>,
}

impl QualityRollup {
    /// Folds one batch's per-document reports in.
    fn add<'a>(&mut self, reports: impl IntoIterator<Item = &'a DocReport>) {
        for report in reports {
            self.docs += 1;
            for (field, status) in &report.fields {
                self.total += 1;
                let entry = self.fields.entry(field.clone()).or_default();
                match status {
                    // A container that matched but held no items is not a miss —
                    // the listing selector still binds, the listing is just quiet.
                    FieldStatus::Matched | FieldStatus::ContainerEmpty => self.matched += 1,
                    FieldStatus::Empty => entry.misses += 1,
                    FieldStatus::Error { .. } => {
                        entry.misses += 1;
                        entry.errors += 1;
                    }
                }
            }
            for (path, stats) in &report.each {
                self.inner.entry(path.clone()).or_default().add(stats);
            }
        }
    }

    /// Pools another batch's rollup into this one — the operation the backfill
    /// mode needs and the reason this is a struct rather than a local tuple.
    fn merge(&mut self, other: &QualityRollup) {
        self.matched += other.matched;
        self.total += other.total;
        self.docs += other.docs;
        for (field, m) in &other.fields {
            let e = self.fields.entry(field.clone()).or_default();
            e.misses += m.misses;
            e.errors += m.errors;
        }
        for (path, r) in &other.inner {
            self.inner.entry(path.clone()).or_default().merge(r);
        }
    }

    /// The fields with the highest miss rate, worst first; only fields that
    /// missed at least once appear.
    ///
    /// Rows come in two scopes. Top-level fields keep their original shape
    /// exactly (`{field, misses, errors, miss_rate}`, `miss_rate` per DOCUMENT).
    /// Inner fields of an `each` listing — keyed by their dotted path, e.g.
    /// `products.price` — add `{scope: "item", items, hits, dead}` and their
    /// `miss_rate` is per listing ITEM, which is why the scope tag is not
    /// optional. `matched`/`total` deliberately stay document-scoped: they are
    /// the run's rule-level match rate and folding item counts into them would
    /// make one wide listing outvote every other field in the rule set.
    fn worst_fields(&self) -> Vec<Value> {
        let docs = self.docs.max(1) as f64;
        let mut worst: Vec<Value> = self
            .fields
            .iter()
            .filter(|(_, m)| m.misses > 0)
            .map(|(field, m)| {
                json!({
                    "field": field,
                    "misses": m.misses,
                    "errors": m.errors,
                    "miss_rate": ((m.misses as f64 / docs) * 1000.0).round() / 1000.0,
                })
            })
            .collect();
        worst.extend(self.inner.iter().filter(|(_, r)| r.misses > 0).map(|(field, r)| {
            json!({
                "field": field,
                "misses": r.misses,
                "errors": r.errors,
                "miss_rate": ((r.misses as f64 / r.items.max(1) as f64) * 1000.0).round() / 1000.0,
                "scope": "item",
                "items": r.items,
                "hits": r.hits,
                "dead": inner_field_dead(r.items, r.hits),
            })
        }));
        // Highest miss count first; ties broken by field name for stable output.
        worst.sort_by(|a, b| {
            b["misses"]
                .as_u64()
                .cmp(&a["misses"].as_u64())
                .then_with(|| a["field"].as_str().cmp(&b["field"].as_str()))
        });
        worst
    }
}

/// Whether an inner listing field is WHOLLY dead across the run — the selector
/// bound on none of the items it was attempted on — as opposed to sparse (some
/// items carry it, some legitimately do not). This is the distinction the
/// listing's single array-level `Matched` erases, and the one an operator needs
/// to tell "the site dropped a class" from "not every card has a badge".
fn inner_field_dead(items: u64, hits: u64) -> bool {
    items > 0 && hits == 0
}

/// Runs the compiled rules over `docs` off the async runtime (rayon fan-out),
/// returning each record paired with its per-field [`DocReport`] — and, when
/// `fingerprint` is set, the resilience [`DocSignals`] taken from the **same
/// parse**.
///
/// `fingerprint` has to be decided here, before the fan-out: the DOM is dropped
/// at the end of each rayon closure, so a consumer that asks afterwards can only
/// be served by parsing the whole batch a second time — which is exactly what
/// this replaced. `None` back means nobody asked, never "fingerprinting failed".
/// `bases` is positional alongside `docs`: the URL each document was fetched
/// from, which `url_absolute` transforms resolve against.
async fn run_extraction(
    compiled: Arc<CompiledRuleSet>,
    docs: Vec<String>,
    bases: Vec<Option<String>>,
    fingerprint: bool,
) -> Result<(Vec<(Value, DocReport)>, Option<Vec<DocSignals>>)> {
    tokio::task::spawn_blocking(move || {
        if !fingerprint {
            return (extract_batch_with_report_at(&compiled, &docs, &bases), None);
        }
        // The fused extract+fingerprint path shares ONE DOM per document, but
        // its seam (`extract_and_fingerprint_batch`) carries no per-document
        // URL. A rule set that resolves URLs therefore takes the base-carrying
        // extraction and fingerprints in a second pass: one extra parse per
        // document, paid ONLY by rule sets that opted into `url_absolute` and
        // have the health detector on. The alternative — reusing the fused path
        // — would emit relative URLs whenever health happened to be enabled,
        // which is a config flag silently changing what a dataset contains.
        if compiled.needs_doc_url() {
            let reported = extract_batch_with_report_at(&compiled, &docs, &bases);
            let values: Vec<Value> = reported.iter().map(|(v, _)| v.clone()).collect();
            return (reported, Some(signals_batch(&docs, &values)));
        }
        let fused = extract_and_fingerprint_batch(&compiled, &docs);
        let mut reported = Vec::with_capacity(fused.len());
        let mut signals = Vec::with_capacity(fused.len());
        for (values, report, sig) in fused {
            reported.push((values, report));
            signals.push(sig);
        }
        (reported, Some(signals))
    })
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
    rules_hash: Option<&str>,
    echo: usize,
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
    // Each document's own URL travels with it into extraction, so `url_absolute`
    // resolves an item's `/item/123` href against the page it was scraped from.
    // A key that is not an absolute URL (a source dataset keyed by id rather
    // than link) parses to no base in core, and the report says so rather than
    // resolving against something invented here.
    let bases: Vec<Option<String>> = metas
        .iter()
        .map(|(url, _, _)| (!url.trim().is_empty()).then(|| url.clone()))
        .collect();
    // `docs` is MOVED, not cloned: fingerprinting now rides the extraction's own
    // parse, so nothing downstream needs the bodies again (was: `docs.clone()`,
    // a second full copy of every HTML body kept alive only so `observe` could
    // re-parse them).
    let (reported, signals) = run_extraction(compiled, docs, bases, ctx.health.enabled()).await?;
    // Borrow the reports rather than deep-cloning each into a throwaway Vec.
    let mut quality = QualityRollup::default();
    quality.add(reported.iter().map(|(_, r)| r));
    let worst = quality.worst_fields();
    let base_url_missing = docs_missing_base(reported.iter().map(|(_, r)| r));
    if base_url_missing > 0 {
        tracing::warn!(
            docs = base_url_missing,
            dataset,
            "url_absolute had no document URL to resolve against; those links stayed relative"
        );
    }

    // Health verdict FIRST, then the write: the state settled here is what the
    // upsert below gates on (trust stamp, quarantine dataset, removal
    // suppression). Judging afterwards would stamp a verdict that did not exist.
    let verdict = observe(ctx, dataset, &keys, signals, &reported, fetch, &worst).await;

    // Provenance (M12). `rules_hash` is the batch's honest shared fact: ONE
    // registered RuleSet produced every record here, so stamping it batch-wide
    // is exact — and it is the pin that makes these revisions re-derivable after
    // the caller's rules move on. `source_url` is per-record and the batch write
    // path carries only one stamp, so it is claimed ONLY when every document in
    // the batch came from the same URL (a single-URL run, or a Wayback backfill
    // of one page); a mixed batch leaves it Null rather than naming one of many.
    let prov = Provenance {
        rules_hash: rules_hash.map(str::to_string),
        source_url: single_source_url(&metas),
        ..Provenance::default()
    };

    let items: Vec<(String, Value)> = keys
        .into_iter()
        .zip(metas)
        .zip(reported)
        .map(|((key, (url, observed_at, fetched_via)), (mut rec, _))| {
            tag_record(&mut rec, url, observed_at, fetched_via);
            (key, rec)
        })
        .collect();
    // The echo is a bounded PREFIX, and the clone is paid only for the records
    // that will actually be echoed. It used to be `records.push(rec.clone())`
    // inside the map above: a full deep copy of every record on the write path
    // of every mode, kept alive purely so the job result could restate data the
    // dataset already holds.
    let records_total = items.len();
    let records: Vec<Value> = items.iter().take(echo).map(|(_, v)| v.clone()).collect();
    // Where this batch is ABOUT to land, read at the same point the write path
    // reads it (after `observe` settled the state, before the upsert). A
    // quarantined source is diverted to the shadow `<dataset>@q` inside
    // `upsert_many_with_provenance`, and until now no field of the result said
    // which of the two the caller should go looking in.
    let state = ctx.health.enforced_state(&ctx.app, dataset).await;
    let written = pumper_core::resilience::write_dataset(dataset, state);
    let summary = ctx
        .upsert_many_with_provenance(dataset, &items, prov)
        .await?;
    Ok(ExtractOutcome {
        records,
        records_total,
        quality,
        base_url_missing,
        summary,
        health: verdict,
        dataset: written,
        indexable: !state.skips_search_index(),
    })
}

/// The outcome of registering this run's rule set in the content-addressed
/// registry: the hash stamped on every revision the run writes, or the reason
/// there is none.
///
/// THE ANTI-PATTERN THIS CLOSES: registration failure was a `warn!` and nothing
/// else. The run succeeded, the records landed with a null `rules_hash`, and
/// those revisions are permanently NON-REPLAYABLE (the artifact-pinning
/// janitor, `replayable_revisions` and every re-derivation path key off that
/// stamp) — with no trace in the job result, the receipt, or the webhook. The
/// only evidence was a log line on a box nobody reads.
#[derive(Debug, Default, PartialEq)]
struct RulesRegistration {
    hash: Option<String>,
    error: Option<String>,
}

impl RulesRegistration {
    /// Pure classification of the registry call's outcome. Best-effort stays
    /// best-effort — provenance is additive metadata and a registry write
    /// failure must never fail a working extraction — but the degradation is
    /// now a FACT IN THE RESULT rather than only in a log.
    fn from_outcome(outcome: Result<String>) -> Self {
        match outcome {
            Ok(hash) => Self {
                hash: Some(hash),
                error: None,
            },
            Err(e) => Self {
                hash: None,
                error: Some(e.to_string()),
            },
        }
    }

    fn hash(&self) -> Option<&str> {
        self.hash.as_deref()
    }

    /// Stamps `rules_hash` (honest-null on failure) and, only when there is
    /// one, `rules_registration_error` onto a write mode's result.
    fn merge_into(&self, result: &mut Value) {
        let Value::Object(map) = result else { return };
        map.insert("rules_hash".into(), json!(self.hash));
        if let Some(e) = &self.error {
            map.insert("rules_registration_error".into(), json!(e));
        }
    }
}

/// Documents whose rule set asked to resolve URLs but had no document URL to
/// resolve against — every `url_absolute` field in them kept its raw, possibly
/// relative, value.
///
/// Named and counted rather than left implicit because the alternative is the
/// worst kind of quiet: a `url` column that holds `/item/1` for one run and
/// `https://shop/item/1` for the next, with the job result claiming a clean
/// pass either way.
fn docs_missing_base<'a>(reports: impl IntoIterator<Item = &'a DocReport>) -> u64 {
    reports.into_iter().filter(|r| r.base_url_missing).count() as u64
}

/// The one source URL every document in this batch came from, or `None` when
/// the batch spans several (or none). Honest-Null by construction: a batch-level
/// `source_url` may only be claimed when it is true of every record in it.
fn single_source_url(metas: &[(String, Option<String>, Option<&'static str>)]) -> Option<String> {
    let first = metas.first()?.0.as_str();
    metas
        .iter()
        .all(|(url, _, _)| url == first)
        .then(|| first.to_string())
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
/// the write summary, the source-health verdict, and where the batch landed.
struct ExtractOutcome {
    /// A BOUNDED prefix of the records written (see [`parse_records_echo`]) —
    /// a sample for the reader, never the corpus.
    records: Vec<Value>,
    /// How many records the batch actually wrote, echoed or not.
    records_total: usize,
    /// This batch's quality counters — poolable, so a multi-batch mode reports
    /// the same breakdown a single-batch mode does.
    quality: QualityRollup,
    /// Documents extracted without a base URL by a rule set that needed one
    /// ([`docs_missing_base`]).
    base_url_missing: u64,
    summary: UpsertSummary,
    health: Option<Value>,
    /// The dataset this batch ACTUALLY landed in: the requested name, or the
    /// shadow `<dataset>@q` when enforcement diverted a quarantined source.
    dataset: String,
    /// Whether this batch's rows may be offered to the full-text index.
    ///
    /// Gated HERE, in the producer, against the SOURCE's own verdict — the
    /// worker's gate reads the health of the spec's own pair, and once a
    /// quarantined source is diverted the spec names `<dataset>@q`, a pair no
    /// `observe_extraction` ever judges (so it always reads `Healthy`). Same
    /// reasoning, same vocabulary, as `grants_common::indexable`.
    indexable: bool,
}

impl ExtractOutcome {
    /// The result keys every single-batch write mode shares.
    ///
    /// `index_datasets` is the load-bearing one: it routes this run's records
    /// to the worker's **delta-driven** dataset indexer
    /// (`dataset_search_docs`), which reads the change feed for the named
    /// `(app, dataset)` and mints stable `<app>:<dataset>:<key>` doc ids that
    /// re-index in place and honour removals. Without it the only search
    /// coverage came from the result's `records` array — so bounding that echo
    /// without declaring this would silently shrink the index to the first N
    /// records of each run.
    fn merge_into(mut self, app: &str, result: &mut Value) {
        let Value::Object(map) = result else { return };
        let echoed = self.records.len();
        map.insert(
            "records".into(),
            Value::Array(std::mem::take(&mut self.records)),
        );
        map.insert("records_total".into(), json!(self.records_total));
        map.insert(
            "records_truncated".into(),
            json!(self.records_total > echoed),
        );
        map.insert("dataset".into(), json!(self.dataset));
        if self.indexable {
            map.insert(
                "index_datasets".into(),
                json!([{ "app": app, "dataset": self.dataset }]),
            );
        }
    }
}

/// Reports this run to the health detector and renders its verdict for the job
/// result. Best-effort: a detection failure is logged and the run still succeeds,
/// because health is a derived judgement and must never fail a working scrape.
async fn observe(
    ctx: &AppContext,
    dataset: &str,
    keys: &[String],
    signals: Option<Vec<DocSignals>>,
    reported: &[(Value, DocReport)],
    fetch: FetchHealth,
    worst: &[Value],
) -> Option<Value> {
    if !ctx.health.enabled() {
        return None;
    }
    // Captured before `fetch` moves into the detector. Honest-Null when nothing
    // was fetched (source mode over stored bodies) — never a fabricated 1.0.
    let fetch_ok_rate = (fetch.attempted > 0).then(|| fetch.rate());
    // Fingerprinted during extraction, from the extraction's own DOM (one parse
    // per document per run). `None` can only mean the extraction pass was told
    // not to fingerprint, which is decided by the same `health.enabled()` the
    // guard above reads.
    let signals = signals?;
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
        Ok(Some(v)) => {
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
            // Web Reliability Index (M41): persist the verdict the detector just
            // rendered — previously consumed once (the log line above + one job
            // result) and discarded. Attributed to each host the run's keys
            // resolve to; the verdict itself is per source (`{app}/{dataset}`),
            // which the stored record flags via `verdict_scope: "source"`.
            // Best-effort inside `record_observations`, never fails the run.
            let mut hosts: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            for key in keys {
                if let Some(host) = observation_host(key) {
                    *hosts.entry(host).or_default() += 1;
                }
            }
            let worst_kept: Vec<Value> = worst.iter().take(5).cloned().collect();
            let deltas: Vec<(String, reliability::HostDelta)> = hosts
                .into_iter()
                .map(|(host, host_docs)| {
                    (
                        host,
                        reliability::HostDelta::Extraction(reliability::ExtractionObs {
                            source_id: v.source_id.clone(),
                            state: v.state.as_str().to_string(),
                            previous_state: v.previous_state.as_str().to_string(),
                            score: v.score,
                            diagnosis: v.diagnosis.map(|d| d.as_str().to_string()),
                            docs: host_docs,
                            fetch_ok_rate,
                            worst_fields: worst_kept.clone(),
                        }),
                    )
                })
                .collect();
            reliability::record_observations(&ctx.datasets, &ctx.job_id.to_string(), deltas).await;
            Some(json!(v))
        }
        Ok(None) => None,
        Err(e) => {
            tracing::warn!("extraction health evaluation failed: {e}");
            None
        }
    }
}

/// Host a document key attributes its reliability observation to. Keys are
/// source URLs (urls mode) or dataset keys (source mode — the crawl `pages`
/// key IS the canonical URL; historical archive keys are `{url}@{date}`).
/// Non-URL keys yield `None` — an observation is only recorded when the host
/// is actually known, never guessed from an opaque key.
fn observation_host(key: &str) -> Option<String> {
    if !(key.starts_with("http://") || key.starts_with("https://")) {
        return None;
    }
    // Strip a historical `@YYYY-MM-DD` suffix so a bare-domain archive key
    // (`https://x.com@2026-01-01`) can't misparse the date as the host.
    let key = match key.rsplit_once('@') {
        Some((prefix, suffix))
            if suffix.len() == 10 && suffix.chars().all(|c| c.is_ascii_digit() || c == '-') =>
        {
            prefix
        }
        _ => key,
    };
    app_crawl::host_of(key)
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
         {url}@{date} keys plus _fetched_via: \"wayback\". replay: {rules, baseline_rules?, \
         against: {app, dataset, url_pattern?, versions: \"all\"|\"latest\", max_pages}, \
         bisect_field?} is the read-only CI mode: run candidate rules over stored bodies, \
         diff against a baseline rule set field by field (match-rate deltas, \
         added/lost/changed samples, per-URL regressions), write replay-report.json — \
         never a dataset record. induce: {urls|url_pattern, app?, dataset?, min_support \
         0.6, min_instances 3} is the read-only zero-shot wrapper-induction mode: \
         statistically mine a CANDIDATE each-shaped rule set (no LLM) from stored \
         same-template pages, emitting the rules + per-field support stats as result and \
         induced-ruleset.json artifact for human review; validate via replay before use."
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                // EXACTLY ONE mode per job, enforced at the enqueue door rather
                // than left to the app's dispatch order. `anyOf` used to admit
                // the very combinations the prose below calls mutually
                // exclusive — `{rules, urls, replay}` validated, enqueued, and
                // ran a read-only replay while the caller believed records were
                // written. Each branch names its own roots as `required` and
                // forbids every other root, so a conflicting object matches ZERO
                // branches and `oneOf` reports it. Mirrored by
                // `resolve_run_mode`, which is the guard for every door that
                // does not validate (and the one that names all the conflicts).
                "oneOf": [
                    {
                        "title": "replay mode (read-only)",
                        "required": ["replay"],
                        "not": { "anyOf": [
                            { "required": ["induce"] }, { "required": ["rules"] },
                            { "required": ["urls"] },   { "required": ["source"] }
                        ]}
                    },
                    {
                        "title": "induce mode (read-only)",
                        "required": ["induce"],
                        "not": { "anyOf": [
                            { "required": ["replay"] }, { "required": ["rules"] },
                            { "required": ["urls"] },   { "required": ["source"] }
                        ]}
                    },
                    {
                        "title": "urls mode (fetch live, write records)",
                        "required": ["rules", "urls"],
                        "not": { "anyOf": [
                            { "required": ["replay"] }, { "required": ["induce"] },
                            { "required": ["source"] }
                        ]}
                    },
                    {
                        "title": "source mode (stored bodies, write records)",
                        "required": ["rules", "source"],
                        "not": { "anyOf": [
                            { "required": ["replay"] }, { "required": ["induce"] },
                            { "required": ["urls"] }
                        ]}
                    }
                ],
                "properties": {
                    "rules": {
                        "type": "object",
                        "minProperties": 1,
                        "description": "Write mode: field -> rule; each rule is {\"type\": \"css|regex|json|xpath|const\", ...type-specific keys}. Pair with exactly one of `urls`/`source`. REFUSED alongside `replay`/`induce`, which carry their own rules."
                    },
                    "replay": {
                        "type": "object",
                        "required": ["rules"],
                        "properties": {
                            "rules": {
                                "type": "object",
                                "minProperties": 1,
                                "description": "The CANDIDATE rule set to validate against the stored corpus."
                            },
                            "baseline_rules": {
                                "type": "object",
                                "minProperties": 1,
                                "description": "Optional baseline (e.g. the currently deployed rules); when given the report carries per-field deltas + added/lost/changed value diffs."
                            },
                            "against": {
                                "type": "object",
                                "properties": {
                                    "app": { "type": "string", "description": "Source app of the stored bodies (default \"crawl\")." },
                                    "dataset": { "type": "string", "description": "Source dataset (default \"pages\")." },
                                    "url_pattern": { "type": "string", "description": "Regex a record key (URL) must match to be replayed." },
                                    "versions": {
                                        "type": "string",
                                        "enum": ["all", "latest"],
                                        "description": "\"latest\" (default): current body per URL. \"all\": every archived page_versions revision + current."
                                    },
                                    "max_pages": {
                                        "type": "integer",
                                        "minimum": 1,
                                        "maximum": 5000,
                                        "description": "URL cap per replay run (default 500); the report sets truncated: true when more matched."
                                    }
                                }
                            },
                            "bisect_field": {
                                "type": "string",
                                "description": "Walk each URL's version series and report every boundary observation pair where this field's match flipped. Requires against.versions: \"all\"."
                            }
                        },
                        "description": "Replay-CI: STRICTLY read-only — runs rules over stored bodies, emits result JSON + a replay-report.json artifact, never writes a dataset record. REFUSED alongside rules/urls/source/induce — the exclusivity is enforced at the door and in the app, not resolved by precedence."
                    },
                    "induce": {
                        "type": "object",
                        "oneOf": [
                            { "required": ["urls"] },
                            { "required": ["url_pattern"] }
                        ],
                        "properties": {
                            "urls": {
                                "type": "array",
                                "items": { "type": "string" },
                                "minItems": 1,
                                "description": "Explicit stored-page keys (crawl keys ARE canonical URLs) forming the same-template page set. Mutually exclusive with url_pattern."
                            },
                            "url_pattern": {
                                "type": "string",
                                "description": "Regex a stored record key (URL) must match to join the page set. Mutually exclusive with urls."
                            },
                            "app": { "type": "string", "description": "Source app of the stored bodies (default \"crawl\")." },
                            "dataset": { "type": "string", "description": "Source dataset (default \"pages\")." },
                            "min_support": {
                                "type": "number",
                                "minimum": 0.05,
                                "maximum": 1.0,
                                "description": "Support threshold (default 0.6): the container must repeat on this fraction of pages; a field slot must be present on this fraction of instances."
                            },
                            "min_instances": {
                                "type": "integer",
                                "minimum": 2,
                                "description": "Minimum repeats of the container signature per page (default 3)."
                            },
                            "max_fields": { "type": "integer", "minimum": 1, "maximum": 32, "description": "Cap on emitted field slots (default 12)." },
                            "max_pages": { "type": "integer", "minimum": 1, "maximum": 500, "description": "Page cap per induction run (default 50)." }
                        },
                        "description": "Zero-shot wrapper induction: STRICTLY read-only — statistically mines a CANDIDATE each-shaped rule set (repeating tag+class container, field slots whose text varies while structure stays fixed) from stored same-template pages. No LLM. Emits result JSON + an induced-ruleset.json artifact for human review; chain to `replay` for validation. Never writes a dataset record. REFUSED alongside rules/urls/source/replay — the exclusivity is enforced at the door and in the app, not resolved by precedence."
                    },
                    "urls": {
                        "type": "array",
                        "items": { "type": "string", "pattern": "^https?://" },
                        "minItems": 1,
                        "description": "URL mode: fetch these and extract. REFUSED alongside `source`, `replay` or `induce` — not silently outranked by them."
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
                            "limit": {
                                "type": "integer",
                                "minimum": 1,
                                "maximum": 10000,
                                "description": "Cap on the no-keys sweep of live records (default and ceiling 10000). Ignored when `keys`/`_trigger.keys` name the set. The result's `truncated` says whether the cap — rather than the dataset — decided where the sweep stopped, and is also true when the firing trigger's key list was capped (`_trigger.keys_truncated`)."
                            },
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
                        "description": "Source mode: read stored bodies of these records (no re-fetch). REFUSED alongside `urls`, `replay` or `induce`."
                    },
                    "strategy": { "type": "string", "enum": ["http", "browser", "auto"] },
                    "concurrency": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 64,
                        "description": "Max in-flight fetches (default 16, ceiling 64). The ceiling is enforced twice — refused here at the door, clamped in code for callers that reach the app another way — so the two layers can never disagree."
                    },
                    "records_echo": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 1000,
                        "description": "How many extracted records the persisted job result echoes (default 100, ceiling 1000, 0 = counts only). The echo is a SAMPLE — the records themselves are in the dataset, and the result reports records_total + records_truncated. Not applicable to backfill mode, which never echoes."
                    },
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
                ManifestExample {
                    description: "Replay-CI: validate a candidate rule edit against the \
                                  stored corpus, diffed field-by-field against the deployed \
                                  baseline — read-only, no dataset writes",
                    params: json!({
                        "replay": {
                            "rules": { "price": { "type": "css", "selector": ".price-v2" } },
                            "baseline_rules": { "price": { "type": "css", "selector": ".price" } },
                            "against": {
                                "url_pattern": "^https://example\\.com/products/",
                                "versions": "all",
                                "max_pages": 200
                            },
                            "bisect_field": "price"
                        }
                    }),
                },
                ManifestExample {
                    description: "Zero-shot wrapper induction: mine a candidate each-shaped \
                                  rule set from stored same-template listing pages — \
                                  read-only, review then validate with replay",
                    params: json!({
                        "induce": {
                            "url_pattern": "^https://example\\.com/search\\?page=",
                            "min_support": 0.6,
                            "min_instances": 3
                        }
                    }),
                },
            ],
            // Written from the four `Ok(json!(...))` sites, key for key. The
            // previous text promised {extracted, errors, removed?} — three keys
            // NO mode has ever emitted — while omitting most of what every mode
            // does emit. A manifest that describes a result nobody returns is
            // worse than no manifest: it is a contract a consumer can code
            // against and get null from forever.
            output_shape: Some(
                "Every mode returns `mode`. The three WRITE modes (mode: \"urls\" | \"source\" \
                 | \"archive\") return {dataset (the dataset actually written — \
                 `<name>@q` when a quarantined source was diverted), new, changed, unchanged, \
                 fields_matched, fields_total, worst_fields[], base_url_missing, health|null, \
                 rules_hash|null, rules_registration_error?, records[] (a BOUNDED echo — see \
                 `records_echo`), records_total, records_truncated, index_datasets?}. Plus, per \
                 mode: urls {requested, fetched, skipped, failed[], fetch_ok_rate}; source \
                 {source{app,dataset}, requested, limit, truncated (this run is PARTIAL: the \
                 no-keys sweep hit its cap, or the firing trigger's key list did — \
                 `_trigger.keys_truncated`), loaded, missing, missing_keys[]}; archive \
                 {target, from, to, \
                 max_snapshots, snapshots_found, truncated, fetched, failed, \
                 failed_snapshots[], fetch_ok_rate}. Backfill (mode: \"backfill\", via \
                 source.backfill) writes per batch and returns the same pooled quality keys \
                 minus the records echo: {resumed_from_checkpoint, source{app,dataset}, \
                 scanned, skipped_pattern, loaded, batches, missing, missing_keys[], dataset|\
                 null, new, changed, unchanged, fields_matched, fields_total, worst_fields[], \
                 base_url_missing, health|null (the final batch's verdict), rules_hash|null, \
                 index_datasets?}. Replay mode writes NOTHING and returns {mode: \"replay\", \
                 against{}, urls_matching, truncated, docs, missing, missing_keys[], baseline, \
                 fields[] (per-field match-rate deltas + added/lost/changed samples), \
                 inner_fields[], regressed_urls[], regressions, bisect?, artifact: \
                 \"replay-report.json\"}. Induce mode writes NOTHING and returns {mode: \
                 \"induce\", source{}, url_pattern, min_support, min_instances, \
                 pages_matching, truncated, docs, missing, missing_keys[], induced} plus, when \
                 induced is true, {rules (candidate RuleSet), container, fields[] (per-field \
                 support stats), candidates_considered, next, artifact: \
                 \"induced-ruleset.json\"} — or {reason} when it is false.",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        // ONE mode per job, decided before any work — a params object carrying
        // several mode roots is refused here rather than silently executing the
        // first one that matches (see `resolve_run_mode`).
        let mode = resolve_run_mode(&ctx.params).map_err(Error::App)?;
        let mode_object = |root: &str| -> Result<serde_json::Map<String, Value>> {
            ctx.params
                .get(root)
                .and_then(Value::as_object)
                .cloned()
                .ok_or_else(|| Error::App(format!("param '{root}' must be an object")))
        };
        match mode {
            // Replay-CI mode: candidate rules over stored bodies, read-only diff
            // report (no dataset writes) — its own param root, its own runner.
            RunMode::Replay => return replay::run_replay(&ctx, &mode_object("replay")?).await,
            // Induce mode: statistically mine a CANDIDATE rule set from stored
            // same-template pages — read-only (result + artifact, no dataset
            // writes) — its own param root, its own runner.
            RunMode::Induce => return induce::run_induce(&ctx, &mode_object("induce")?).await,
            RunMode::Urls | RunMode::Source => {}
        }
        let rules_json = ctx
            .params
            .get("rules")
            .cloned()
            .ok_or_else(|| Error::App("param 'rules' is required".into()))?;
        let rules: RuleSet = serde_json::from_value(rules_json.clone())
            .map_err(|e| Error::App(format!("bad rules: {e}")))?;
        // Compile (and validate selectors/regex) once, before the fan-out.
        let compiled = Arc::new(rules.compile()?);
        // M12: register THIS run's rule set in the content-addressed registry and
        // carry its hash onto every revision the run writes — extractor is the
        // one place in the fleet where a real `rules_hash` exists, and it is what
        // makes a record re-derivable once the caller's live rules have moved on.
        // Best-effort: provenance is additive metadata and a registry write
        // failure must never fail a working extraction — but it IS reported
        // (`rules_hash: null` + `rules_registration_error`), because unstamped
        // revisions are permanently non-replayable.
        let registration = RulesRegistration::from_outcome(ctx.register_rules(&rules_json).await);
        if let Some(e) = &registration.error {
            tracing::warn!("ruleset registration failed, revisions unstamped: {e}");
        }
        let dataset = ctx
            .params
            .get("dataset")
            .and_then(Value::as_str)
            .unwrap_or("extracted")
            .to_string();

        // Two input modes: fetch live `urls`, or read stored bodies from a
        // crawl→dataset `source`. Exactly one, already resolved above.
        if mode == RunMode::Source {
            self.run_source_mode(&ctx, compiled, &dataset, &registration)
                .await
        } else {
            self.run_urls_mode(&ctx, compiled, &dataset, &registration)
                .await
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
        registration: &RulesRegistration,
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
        //
        // Every fetch goes through the METERED chokepoint `ctx.fetch`, never the
        // raw `ctx.engines.fetch`: the raw fetcher skips the cost ledger, the
        // per-job budget clamp (so `strategy: "auto_with_research"` under a $1
        // budget — or the $0 a DataHub `cost:pause` forces — could spend
        // unbounded Claude money invisibly), the learned tier router, and the
        // VCR cassette (so a recorded run of this app silently hit the live
        // network on replay). Guarded by `crates/core/tests/fetch_chokepoint.rs`.
        //
        // The futures borrow `&ctx` — nothing is spawned, so there is no
        // `'static` bound to satisfy; `.cloned()` on the URLs stays load-bearing
        // for closure inference (see the sibling note in `app-plugin`).
        let fetches = urls.iter().cloned().map(|url| {
            let mut req = FetchRequest::new(&url);
            req.strategy = strategy;
            async move {
                match ctx.fetch(req).await {
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
        let out = extract_and_upsert(
            ctx,
            compiled,
            dataset,
            keyed,
            fetch,
            registration.hash(),
            parse_records_echo(&ctx.params),
        )
        .await?;

        // `dataset`, `records`, `records_total`, `records_truncated` and
        // `index_datasets` are added by `ExtractOutcome::merge_into` below —
        // one definition of the write-mode contract, not three.
        let mut result = json!({
            "mode": "urls",
            "requested": requested,
            "fetched": fetched,
            "skipped": failed.len(),
            "failed": failed,
            "fetch_ok_rate": fetch.rate(),
            "new": out.summary.new.len(),
            "changed": out.summary.changed.len(),
            "unchanged": out.summary.unchanged,
            "fields_matched": out.quality.matched,
            "fields_total": out.quality.total,
            "worst_fields": out.quality.worst_fields(),
            "base_url_missing": out.base_url_missing,
            "health": out.health.clone(),
        });
        registration.merge_into(&mut result);
        out.merge_into(&ctx.app, &mut result);
        Ok(result)
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
        registration: &RulesRegistration,
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
                .run_archive_backfill(ctx, compiled, dataset, archive, registration)
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
        // a dataset trigger) > all live records in the source dataset. The
        // selection carries WHICH of the two it got, because only the trigger's
        // list can have been capped.
        let selection = select_keys(source, &ctx.params);

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
                .run_backfill(ctx, compiled, dataset, &src_app, source, registration)
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
        let limit = parse_source_limit(source);
        // Two independent ways this run can be partial, reported through the one
        // `truncated` field: the host capped the trigger hop's key list, or the
        // no-keys sweep hit `limit`. They cannot both apply (a key list means no
        // sweep), so one flag carries both without ambiguity.
        let trigger_capped = selection.truncated;
        let mut truncated = trigger_capped;

        // Resolve the natural keys first (explicit / trigger keys, else the live
        // sweep) — every mode selects the same key set; the modes differ only in
        // WHICH stored body each key resolves to. The sweep already holds the
        // records, so it carries them along instead of re-fetching per key.
        let selected: Vec<(String, Option<Record>)> = if let Some(keys) = selection.keys {
            requested = keys.len();
            keys.into_iter().map(|k| (k, None)).collect()
        } else {
            // No keys: every live (not removed, not gone) record — up to `limit`.
            let page = ctx.datasets.list(&src_app, &src_dataset, limit).await?;
            // Judged on the PAGE, before the live filter: a full page means the
            // cap decided where the sweep stopped, whatever share of it survived
            // the filter. A 12,000-record dataset used to extract 10,000 and
            // report `requested: 10000` as if that were the whole of it.
            truncated = sweep_truncated(page.len(), limit);
            let records: Vec<Record> = page
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
        if trigger_capped {
            tracing::warn!(
                src_app,
                src_dataset,
                keys = requested,
                "trigger hop key list was TRUNCATED by the host key cap: this run covers only \
                 the first `[triggers] key_cap` changed records, not the whole delta"
            );
        } else if truncated {
            tracing::warn!(
                src_app,
                src_dataset,
                limit,
                "source sweep hit its record cap: this run covers only the most recently \
                 updated page of the dataset"
            );
        }

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
        let out = extract_and_upsert(
            ctx,
            compiled,
            dataset,
            keyed,
            FetchHealth::default(),
            registration.hash(),
            parse_records_echo(&ctx.params),
        )
        .await?;

        let missing_count = missing.len();
        missing.truncate(MISSING_ECHO_LIMIT);
        // See `run_urls_mode`: the shared write-mode keys come from `merge_into`.
        let mut result = json!({
            "mode": "source",
            "source": {"app": src_app, "dataset": src_dataset},
            "requested": requested,
            "limit": limit,
            "truncated": truncated,
            "loaded": loaded,
            "missing": missing_count,
            "missing_keys": missing,
            "new": out.summary.new.len(),
            "changed": out.summary.changed.len(),
            "unchanged": out.summary.unchanged,
            "fields_matched": out.quality.matched,
            "fields_total": out.quality.total,
            "worst_fields": out.quality.worst_fields(),
            "base_url_missing": out.base_url_missing,
            "health": out.health.clone(),
        });
        registration.merge_into(&mut result);
        out.merge_into(&ctx.app, &mut result);
        Ok(result)
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
        registration: &RulesRegistration,
    ) -> Result<Value> {
        let pattern = source
            .get("url_pattern")
            .and_then(Value::as_str)
            .map(|p| {
                regex::Regex::new(p).map_err(|e| Error::App(format!("bad url_pattern '{p}': {e}")))
            })
            .transpose()?;

        // Durable execution (M23): backfill is the one genuinely long extractor
        // mode — it pages the WHOLE `page_versions` archive, extracting and
        // upserting per batch. The resumable unit is the keyset cursor plus the
        // running tallies, so a reap resumes at the next page instead of
        // re-reading and re-extracting every archived revision from the start.
        let mut st = BackfillState::restore(ctx.restore());
        let resumed = st.after.is_some();
        let mut after: Option<(String, String)> = st.after.clone();
        let mut scanned = st.scanned;
        let mut skipped_pattern = st.skipped_pattern;
        let mut loaded = st.loaded;
        let mut batches = st.batches;
        let mut missing: Vec<Value> = Vec::new();
        let (mut new, mut changed, mut unchanged) = (st.new, st.changed, st.unchanged);
        let (mut fields_matched, mut fields_total) = (st.fields_matched, st.fields_total);
        let mut base_url_missing = st.base_url_missing;
        // Pooled across every batch of the (possibly resumed) scan, exactly like
        // the two counters above — it is their miss breakdown, and reporting one
        // cumulatively while the other covered a single batch would be worse
        // than reporting neither.
        let mut quality = st.quality.clone();
        // The verdict from the LAST batch that produced one. Verdicts are not
        // poolable — a verdict is the source's STATE, and averaging four states
        // is not a state — so the honest single answer is where the source
        // ended up. Every batch's verdict is recorded in the health store; only
        // the final one is echoed here.
        let mut health: Option<Value> = None;
        // Every dataset this run's batches actually landed in. Normally one; a
        // health flip mid-scan can split a backfill across `<dataset>` and the
        // shadow `<dataset>@q`, and a result naming only one of them would send
        // the reader to the wrong table. `indexed` is the subset the source's
        // own verdict allows into the search index (see `ExtractOutcome`).
        let mut written: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut indexed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
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
                    registration.hash(),
                    // Backfill has never echoed records and must not start: it
                    // is the mode that fans over a WHOLE archive. Zero here is
                    // also what makes the write path clone-free for it.
                    0,
                )
                .await?;
                new += out.summary.new.len();
                changed += out.summary.changed.len();
                unchanged += out.summary.unchanged;
                fields_matched += out.quality.matched;
                fields_total += out.quality.total;
                base_url_missing += out.base_url_missing;
                quality.merge(&out.quality);
                if out.health.is_some() {
                    health = out.health;
                }
                if out.indexable {
                    indexed.insert(out.dataset.clone());
                }
                written.insert(out.dataset);
            }
            // Cursor + tallies AFTER the batch's writes committed, so a resume
            // never re-does a page and never double-counts one.
            st = BackfillState {
                v: BACKFILL_STATE_VERSION,
                after: after.clone(),
                scanned,
                skipped_pattern,
                loaded,
                batches,
                new,
                changed,
                unchanged,
                fields_matched,
                fields_total,
                base_url_missing,
                quality: quality.clone(),
            };
            ctx.checkpoint(st.to_value()).await;
            if short {
                break;
            }
        }
        // Bound the per-key echo; the full count is still reported.
        let missing_count = missing.len();
        missing.truncate(MISSING_ECHO_LIMIT);
        let mut result = json!({
            "mode": "backfill",
            "resumed_from_checkpoint": resumed,
            "source": {"app": src_app, "dataset": VERSIONS_DATASET},
            "scanned": scanned,
            "skipped_pattern": skipped_pattern,
            "loaded": loaded,
            "batches": batches,
            "missing": missing_count,
            "missing_keys": missing,
            // Honest-null when no batch wrote: naming a target for a write that
            // never happened is exactly the kind of claim this direction removes.
            "dataset": written.iter().next_back(),
            "new": new,
            "changed": changed,
            "unchanged": unchanged,
            "fields_matched": fields_matched,
            "fields_total": fields_total,
            "worst_fields": quality.worst_fields(),
            "base_url_missing": base_url_missing,
            "health": health,
        });
        registration.merge_into(&mut result);
        // Search coverage for a mode that never echoed a record: the worker's
        // delta indexer reads the change feed for these pairs. Withheld when the
        // source's verdict says its rows do not belong in the index.
        if !indexed.is_empty() {
            result["index_datasets"] = Value::Array(
                indexed
                    .iter()
                    .map(|ds| json!({ "app": ctx.app, "dataset": ds }))
                    .collect(),
            );
        }
        Ok(result)
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
        registration: &RulesRegistration,
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
            .list_snapshots(
                &p.target,
                p.from.as_deref(),
                p.to.as_deref(),
                p.max_snapshots,
            )
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
                let url =
                    pumper_engine_archive::snapshot_url(&base, &snap.timestamp, &snap.original);
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
        keyed.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| a.observed_at.cmp(&b.observed_at))
        });

        let fetched = keyed.len();
        let out = extract_and_upsert(
            ctx,
            compiled,
            dataset,
            keyed,
            fetch,
            registration.hash(),
            parse_records_echo(&ctx.params),
        )
        .await?;

        let failed_count = failed.len();
        failed.truncate(MISSING_ECHO_LIMIT);
        // See `run_urls_mode`: the shared write-mode keys come from `merge_into`.
        let mut result = json!({
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
            "fields_matched": out.quality.matched,
            "fields_total": out.quality.total,
            "worst_fields": out.quality.worst_fields(),
            "base_url_missing": out.base_url_missing,
            "health": out.health.clone(),
        });
        registration.merge_into(&mut result);
        out.merge_into(&ctx.app, &mut result);
        Ok(result)
    }
}

/// Cap on the per-key `missing_keys` echo in a backfill result — a large archive
/// could otherwise blow up the stored job result; `missing` keeps the full count.
const MISSING_ECHO_LIMIT: usize = 100;

/// Backfill checkpoint blob version — bump on shape change; a mismatch restores
/// fresh (a full re-scan is correct; a mis-resumed cursor silently skips rows).
const BACKFILL_STATE_VERSION: u32 = 1;

/// The resumable state of a backfill run: the `page_versions` keyset cursor plus
/// the running tallies. `missing_keys` is deliberately NOT carried — it is a
/// per-attempt diagnostic, and a resumed run reporting a prior attempt's
/// unreadable artifacts as its own would be a fabricated observation.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
struct BackfillState {
    v: u32,
    /// Keyset cursor `(updated_at-as-stored, key)` of the last committed page.
    #[serde(default)]
    after: Option<(String, String)>,
    #[serde(default)]
    scanned: usize,
    #[serde(default)]
    skipped_pattern: usize,
    #[serde(default)]
    loaded: usize,
    #[serde(default)]
    batches: usize,
    #[serde(default)]
    new: usize,
    #[serde(default)]
    changed: usize,
    #[serde(default)]
    unchanged: usize,
    #[serde(default)]
    fields_matched: u64,
    #[serde(default)]
    fields_total: u64,
    /// Cumulative [`docs_missing_base`] across the resumed scan. `#[serde(default)]`
    /// like every sibling, so a checkpoint written before this field existed
    /// resumes at 0 rather than restarting the whole backfill.
    #[serde(default)]
    base_url_missing: u64,
    /// The pooled miss breakdown behind `fields_matched`/`fields_total`, carried
    /// so a resumed scan's `worst_fields` covers the same span its counters do.
    ///
    /// Those two counters are kept as their own fields rather than read off this
    /// rollup: they predate it, and a checkpoint written before `quality`
    /// existed must resume with correct totals (this field defaults to empty)
    /// instead of silently restarting them at zero.
    #[serde(default)]
    quality: QualityRollup,
}

impl BackfillState {
    /// Advisory restore: anything that isn't a current-version state restarts
    /// the scan from the top rather than erroring.
    fn restore(restored: Option<&Value>) -> Self {
        restored
            .and_then(|v| serde_json::from_value::<BackfillState>(v.clone()).ok())
            .filter(|s| s.v == BACKFILL_STATE_VERSION)
            .unwrap_or(BackfillState {
                v: BACKFILL_STATE_VERSION,
                ..BackfillState::default()
            })
    }

    fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
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
mod observation_host_tests {
    use super::observation_host;

    #[test]
    fn resolves_url_keys_to_hosts() {
        assert_eq!(
            observation_host("https://Example.COM/jobs?page=2"),
            Some("example.com".into())
        );
        assert_eq!(
            observation_host("http://example.com"),
            Some("example.com".into())
        );
    }

    #[test]
    fn strips_historical_archive_date_suffix() {
        assert_eq!(
            observation_host("https://example.com/page@2026-01-01"),
            Some("example.com".into())
        );
        // Bare-domain archive key: without the strip, `@` would misparse the
        // date as the host (userinfo rule).
        assert_eq!(
            observation_host("https://example.com@2026-01-01"),
            Some("example.com".into())
        );
    }

    #[test]
    fn non_url_keys_yield_no_host() {
        assert_eq!(observation_host("CA:electrician"), None);
        assert_eq!(observation_host("opportunity-12345"), None);
    }
}

#[cfg(test)]
mod tests {
    use super::QualityRollup;
    use pumper_core::{DocReport, FieldStatus};
    use serde_json::Value;

    /// The single-batch view of a [`QualityRollup`]: fold these reports in and
    /// render. Production code keeps the rollup itself (backfill pools several),
    /// but every assertion below is about one batch's summary.
    fn summarize_reports<'a>(
        reports: impl IntoIterator<Item = &'a DocReport>,
    ) -> (u64, u64, Vec<Value>) {
        let mut r = QualityRollup::default();
        r.add(reports);
        (r.matched, r.total, r.worst_fields())
    }

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
    fn worst_fields_surfaces_a_dead_inner_field_not_just_a_matched_listing() {
        // THE REFUTED BEHAVIOR: the listing rule reports one status for the
        // whole array, so a `price` selector that died on every card left
        // `worst_fields` EMPTY — the run looked perfect. It is not.
        use pumper_core::extract::InnerFieldStats;
        let each = |pairs: &[(&str, InnerFieldStats)]| DocReport {
            fields: [("products".to_string(), FieldStatus::Matched)]
                .into_iter()
                .collect(),
            each: pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            ..DocReport::default()
        };
        let dead = InnerFieldStats {
            items: 3,
            matched: 0,
            empty: 3,
            ..InnerFieldStats::default()
        };
        let sparse = InnerFieldStats {
            items: 3,
            matched: 1,
            empty: 2,
            ..InnerFieldStats::default()
        };
        let fine = InnerFieldStats {
            items: 3,
            matched: 3,
            ..InnerFieldStats::default()
        };
        let reports = [each(&[
            ("products.price", dead),
            ("products.badge", sparse),
            ("products.name", fine),
        ])];
        let (matched, total, worst) = summarize_reports(reports.iter());
        // Document-scoped totals are untouched: one field, one document, matched.
        assert_eq!((matched, total), (1, 1));
        // ...but the dead inner field is now the run's worst row.
        assert_eq!(worst.len(), 2, "{worst:?}");
        assert_eq!(worst[0]["field"], "products.price");
        assert_eq!(worst[0]["scope"], "item");
        assert_eq!(worst[0]["items"], 3);
        assert_eq!(worst[0]["misses"], 3);
        assert_eq!(worst[0]["miss_rate"], 1.0);
        assert_eq!(worst[0]["dead"], true);
        // A sparse field is reported but NOT flagged dead — the whole point.
        assert_eq!(worst[1]["field"], "products.badge");
        assert_eq!(worst[1]["dead"], false);
        assert_eq!(worst[1]["misses"], 2);
        // A healthy inner field never enters worst_fields at all.
        assert!(worst.iter().all(|w| w["field"] != "products.name"));
    }

    #[test]
    fn inner_container_empty_is_a_hit_not_a_miss_like_the_document_scope() {
        // The `ContainerEmpty` convention is implemented once per scope:
        // `summarize_reports` for documents, `InnerFieldStats::hits` for items.
        // Pin them together so neither can silently start counting a quiet
        // sub-listing as a broken selector.
        use pumper_core::extract::InnerFieldStats;
        let doc_scope = [report(&[("jobs", FieldStatus::ContainerEmpty)])];
        let (matched, total, worst) = summarize_reports(doc_scope.iter());
        assert_eq!((matched, total), (1, 1));
        assert!(
            worst.is_empty(),
            "document scope: quiet listing is not a miss"
        );

        let quiet_nested = InnerFieldStats {
            items: 2,
            container_empty: 2,
            ..InnerFieldStats::default()
        };
        assert_eq!(quiet_nested.hits(), 2);
        assert_eq!(quiet_nested.misses(), 0);
        assert!(!quiet_nested.is_dead());
        let item_scope = [DocReport {
            fields: [("products".to_string(), FieldStatus::Matched)]
                .into_iter()
                .collect(),
            each: [("products.variants".to_string(), quiet_nested)]
                .into_iter()
                .collect(),
            ..DocReport::default()
        }];
        let (_, _, worst) = summarize_reports(item_scope.iter());
        assert!(
            worst.is_empty(),
            "item scope: quiet sub-listing is not a miss"
        );
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
    fn docs_missing_base_counts_documents_not_fields() {
        // One flag per DOCUMENT, whatever the rule set's width: the question is
        // "did this page have a URL to resolve against", asked once per page.
        use super::docs_missing_base;
        let mut with = report(&[("a", FieldStatus::Matched), ("b", FieldStatus::Matched)]);
        with.base_url_missing = true;
        let without = report(&[("a", FieldStatus::Matched)]);
        assert_eq!(docs_missing_base([&with, &without, &with]), 2);
        assert_eq!(docs_missing_base([&without]), 0);
        assert_eq!(docs_missing_base(std::iter::empty()), 0);
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
        assert!(
            parse_archive_params(&obj(json!({"url": "https://a/x", "from": "2019-06"}))).is_err()
        );
        assert!(
            parse_archive_params(&obj(json!({"url": "https://a/x", "to": "yesterday"}))).is_err()
        );
        // The per-run cap clamps into 1..=ceiling — never unbounded.
        let p =
            parse_archive_params(&obj(json!({"url": "https://a/x", "max_snapshots": 0}))).unwrap();
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
    fn pooled_worst_fields_match_a_single_batch_over_the_same_documents() {
        // THE REFUTED BEHAVIOR: `worst_fields` was rendered per
        // `extract_and_upsert` call and thrown away, so the backfill mode — the
        // only multi-batch mode — reported pooled `fields_matched`/`fields_total`
        // and NO breakdown at all. Pooling has to give the same answer as one
        // big batch, or the two modes' quality reports mean different things.
        let err = FieldStatus::Error { detail: "x".into() };
        let a = [
            report(&[("title", FieldStatus::Matched), ("price", err.clone())]),
            report(&[("title", FieldStatus::Empty), ("price", FieldStatus::Empty)]),
        ];
        let b = [report(&[
            ("title", FieldStatus::Matched),
            ("price", FieldStatus::Empty),
        ])];

        let one_batch = {
            let mut r = QualityRollup::default();
            r.add(a.iter().chain(b.iter()));
            r
        };
        let pooled = {
            let mut first = QualityRollup::default();
            first.add(a.iter());
            let mut second = QualityRollup::default();
            second.add(b.iter());
            first.merge(&second);
            first
        };
        assert_eq!(pooled, one_batch, "merge must be exactly concatenation");
        // ...and the pooled denominators are cumulative, not per-batch: price
        // missed on all 3 documents.
        let worst = pooled.worst_fields();
        assert_eq!(worst[0]["field"], "price");
        assert_eq!(worst[0]["misses"], 3);
        assert_eq!(worst[0]["errors"], 1);
        assert_eq!(worst[0]["miss_rate"], 1.0);
        assert_eq!(worst[1]["field"], "title");
        assert_eq!(worst[1]["miss_rate"], 0.333);
    }

    #[test]
    fn a_pooled_rollup_survives_a_checkpoint_round_trip() {
        // The backfill carries the rollup in its resumable state; a shape that
        // does not round-trip would silently reset the breakdown on every reap
        // while the counters beside it kept climbing.
        use pumper_core::extract::InnerFieldStats;
        let mut r = QualityRollup::default();
        r.add([&DocReport {
            fields: [
                ("title".to_string(), FieldStatus::Matched),
                ("price".to_string(), FieldStatus::Empty),
            ]
            .into_iter()
            .collect(),
            each: [(
                "products.sku".to_string(),
                InnerFieldStats {
                    items: 4,
                    matched: 1,
                    empty: 3,
                    ..InnerFieldStats::default()
                },
            )]
            .into_iter()
            .collect(),
            ..DocReport::default()
        }]);
        let round_tripped: QualityRollup =
            serde_json::from_value(serde_json::to_value(&r).unwrap()).unwrap();
        assert_eq!(round_tripped, r);
        assert_eq!(round_tripped.worst_fields(), r.worst_fields());
        // An older checkpoint that predates the field restores empty, not broken.
        let empty: QualityRollup = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(empty, QualityRollup::default());
    }

    #[test]
    fn a_full_page_is_truncated_not_a_complete_sweep() {
        // THE REFUTED BEHAVIOR: source mode listed at most 10,000 live records
        // and reported that count as `requested` with no signal — a 12,000-record
        // dataset silently extracted 10,000 and looked like a clean full run.
        use super::sweep_truncated;
        assert!(sweep_truncated(10, 10), "a full page may hide more");
        assert!(sweep_truncated(11, 10), "over-full is still truncated");
        assert!(!sweep_truncated(9, 10), "a short page IS the whole dataset");
        assert!(!sweep_truncated(0, 10), "an empty dataset is not truncated");
    }

    #[test]
    fn sweep_truncation_is_judged_on_the_page_not_the_live_count() {
        // The distinction that makes the flag correct: the store returns a full
        // page, most of it removed/gone, so the LIVE count is small — but the
        // cap still decided where the read stopped and rows past it were never
        // seen. Judging on the filtered count would call that run complete.
        use super::sweep_truncated;
        let page_returned = 10; // the cap
        let live_after_filter = 3;
        assert!(sweep_truncated(page_returned, 10));
        assert!(!sweep_truncated(live_after_filter, 10));
    }

    #[test]
    fn a_capped_trigger_hop_is_not_a_complete_key_list() {
        // THE REFUTED BEHAVIOR: `source.keys` and `_trigger.keys` were collapsed
        // with `.or_else()`, and `truncated` was only ever written by the no-keys
        // sweep branch. So a hop the host capped at `[triggers] key_cap` — 200 of
        // 5,000 changed records — reported `truncated: false`: a clean run.
        use super::select_keys;
        use serde_json::json;
        let obj = |v: serde_json::Value| v.as_object().unwrap().clone();

        let capped = select_keys(
            &obj(json!({"app": "crawl", "dataset": "pages"})),
            &json!({"_trigger": {"keys": ["a", "b"], "keys_truncated": true, "count": 5000}}),
        );
        assert_eq!(capped.keys.as_deref(), Some(&["a".into(), "b".into()][..]));
        assert!(
            capped.truncated,
            "a capped hop must be distinguishable from a complete one"
        );

        // The same hop, uncapped: unchanged, and NOT truncated.
        let whole = select_keys(
            &obj(json!({"app": "crawl", "dataset": "pages"})),
            &json!({"_trigger": {"keys": ["a", "b"], "keys_truncated": false, "count": 2}}),
        );
        assert_eq!(whole.keys.as_deref(), Some(&["a".into(), "b".into()][..]));
        assert!(!whole.truncated);

        // A hop from a host that predates the flag reads as complete, not as a
        // false alarm on every trigger-fired run.
        let legacy = select_keys(
            &obj(json!({"app": "crawl", "dataset": "pages"})),
            &json!({"_trigger": {"keys": ["a"]}}),
        );
        assert!(!legacy.truncated);
    }

    #[test]
    fn a_caller_key_list_is_uncapped_not_truncated() {
        // The other half of the seam: `source.keys` is the CALLER's list, which
        // no cap applied to. It must not inherit the `keys_truncated` of the hop
        // that happened to carry the job — that would report a partial run on a
        // job that named its own complete work set.
        use super::select_keys;
        use serde_json::json;
        let obj = |v: serde_json::Value| v.as_object().unwrap().clone();

        let sel = select_keys(
            &obj(json!({"app": "crawl", "dataset": "pages", "keys": ["x"]})),
            &json!({"_trigger": {"keys": ["a", "b"], "keys_truncated": true}}),
        );
        assert_eq!(sel.keys.as_deref(), Some(&["x".into()][..]));
        assert!(!sel.truncated, "a caller-named key set is never capped");

        // And no keys anywhere is "sweep everything" — the sweep decides.
        let sweep = select_keys(
            &obj(json!({"app": "crawl", "dataset": "pages"})),
            &json!({}),
        );
        assert_eq!(sweep, super::KeySelection::default());
        assert!(sweep.keys.is_none());
    }

    #[test]
    fn source_limit_clamps_into_the_ceiling_and_defaults_to_it() {
        use super::{parse_source_limit, SOURCE_LIST_LIMIT};
        use serde_json::json;
        let obj = |v: serde_json::Value| v.as_object().unwrap().clone();
        assert_eq!(parse_source_limit(&obj(json!({}))), SOURCE_LIST_LIMIT);
        assert_eq!(parse_source_limit(&obj(json!({"limit": 5}))), 5);
        // Never zero (an idle sweep) and never above the ceiling (an unbounded read).
        assert_eq!(parse_source_limit(&obj(json!({"limit": 0}))), 1);
        assert_eq!(
            parse_source_limit(&obj(json!({"limit": 999_999}))),
            SOURCE_LIST_LIMIT
        );
        assert_eq!(
            parse_source_limit(&obj(json!({"limit": "many"}))),
            SOURCE_LIST_LIMIT
        );
    }

    #[test]
    fn registration_failure_surfaces_in_the_result_not_only_in_a_log() {
        // THE REFUTED BEHAVIOR: a failed `register_rules` was a `warn!` and
        // nothing more. The run returned 200, the records landed with a null
        // `rules_hash`, and those revisions are permanently non-replayable —
        // with no trace anywhere a consumer, receipt or webhook can see.
        use super::RulesRegistration;
        use serde_json::json;

        let failed = RulesRegistration::from_outcome(Err(pumper_core::Error::App(
            "registry write failed: database is locked".into(),
        )));
        assert_eq!(failed.hash(), None);
        let mut result = json!({ "mode": "urls" });
        failed.merge_into(&mut result);
        assert_eq!(result["rules_hash"], Value::Null, "honest null, not absent");
        assert!(
            result["rules_registration_error"]
                .as_str()
                .unwrap_or_default()
                .contains("database is locked"),
            "the reason travels with the null: {result}"
        );

        // The success path stamps the hash and invents no error key.
        let ok = RulesRegistration::from_outcome(Ok("sha256:abc".into()));
        assert_eq!(ok.hash(), Some("sha256:abc"));
        let mut result = json!({ "mode": "urls" });
        ok.merge_into(&mut result);
        assert_eq!(result["rules_hash"], "sha256:abc");
        assert!(result.get("rules_registration_error").is_none());
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

    #[test]
    fn concurrency_clamps_down_not_only_up() {
        // THE REFUTED BEHAVIOR: the clamp was `n.max(1)` only, so
        // `concurrency: 100000` over a wide URL list asked for 100000 in-flight
        // sockets — the exact fd exhaustion the bound exists to prevent.
        use super::{parse_concurrency, MAX_FETCH_CONCURRENCY};
        use serde_json::json;
        assert_eq!(
            parse_concurrency(&json!({ "concurrency": 100_000 })),
            MAX_FETCH_CONCURRENCY
        );
        assert_eq!(
            parse_concurrency(&json!({ "concurrency": u64::MAX })),
            MAX_FETCH_CONCURRENCY
        );
        // At the ceiling is honored exactly — the clamp is not off by one.
        assert_eq!(
            parse_concurrency(&json!({ "concurrency": MAX_FETCH_CONCURRENCY })),
            MAX_FETCH_CONCURRENCY
        );
    }

    /// The code clamp and the schema `maximum` are ONE bound. If they drift, a
    /// job refused at the door and a job clamped in code stop agreeing about
    /// what `concurrency` means.
    #[test]
    fn the_concurrency_ceiling_is_declared_once_not_twice() {
        use super::{Extractor, MAX_FETCH_CONCURRENCY};
        use pumper_core::ScrapeApp;
        let schema = Extractor.manifest().params_schema.expect("params schema");
        let c = &schema["properties"]["concurrency"];
        assert_eq!(c["maximum"], serde_json::json!(MAX_FETCH_CONCURRENCY));
        assert_eq!(c["minimum"], serde_json::json!(1));
    }
}

#[cfg(test)]
mod run_mode_tests {
    use super::{resolve_run_mode, RunMode};
    use serde_json::json;

    fn rules() -> serde_json::Value {
        json!({ "title": { "type": "css", "selector": "h1" } })
    }

    #[test]
    fn each_mode_resolves_to_itself() {
        assert_eq!(
            resolve_run_mode(&json!({ "replay": { "rules": rules() } })).unwrap(),
            RunMode::Replay
        );
        assert_eq!(
            resolve_run_mode(&json!({ "induce": { "url_pattern": "^https://a/" } })).unwrap(),
            RunMode::Induce
        );
        assert_eq!(
            resolve_run_mode(&json!({ "rules": rules(), "urls": ["https://a/"] })).unwrap(),
            RunMode::Urls
        );
        assert_eq!(
            resolve_run_mode(
                &json!({ "rules": rules(), "source": {"app":"crawl","dataset":"pages"} })
            )
            .unwrap(),
            RunMode::Source
        );
        // `rules` with neither input still resolves — urls mode reports the
        // missing list itself, with the message it always had.
        assert_eq!(
            resolve_run_mode(&json!({ "rules": rules() })).unwrap(),
            RunMode::Urls
        );
    }

    #[test]
    fn mode_conflict_rejected_not_first_match_win() {
        // THE REFUTED BEHAVIOR: replay outranked everything, so this params
        // object ran a READ-ONLY replay and returned 200 while the caller
        // believed an extraction had written records into `extracted`.
        let both = json!({
            "rules": rules(),
            "urls": ["https://a/"],
            "replay": { "rules": rules() },
        });
        let err = resolve_run_mode(&both).expect_err("first-match precedence is not exclusivity");
        // The error names EVERY conflicting root, not just the pair that lost.
        let named = err.split(" — ").next().unwrap_or_default();
        for root in ["replay", "rules", "urls"] {
            assert!(named.contains(root), "conflict must name `{root}`: {err}");
        }
        assert!(
            !named.contains("source"),
            "no root invented (the tail may still list the legal shapes): {err}"
        );
    }

    #[test]
    fn every_conflicting_pair_is_refused() {
        let value = |root: &str| match root {
            "rules" => rules(),
            "urls" => json!(["https://a/"]),
            "source" => json!({ "app": "crawl", "dataset": "pages" }),
            _ => json!({ "rules": rules() }),
        };
        // Every pair of mode roots that cannot coexist. `rules`+`urls` and
        // `rules`+`source` are the two legal pairs and are deliberately absent.
        let pairs = [
            ("replay", "induce"),
            ("replay", "rules"),
            ("replay", "urls"),
            ("replay", "source"),
            ("induce", "rules"),
            ("induce", "urls"),
            ("induce", "source"),
            ("urls", "source"),
        ];
        for (a, b) in pairs {
            let params = json!({ a: value(a), b: value(b) });
            let err = resolve_run_mode(&params)
                .expect_err(&format!("`{a}` + `{b}` must be refused, not ranked"));
            assert!(err.contains(a) && err.contains(b), "{a}+{b}: {err}");
        }
    }

    #[test]
    fn an_explicit_null_root_is_absent_not_a_declaration() {
        // A params template that spells "not this run" as `null` must not be
        // read as requesting two modes.
        let params = json!({ "rules": rules(), "urls": ["https://a/"], "replay": null });
        assert_eq!(resolve_run_mode(&params).unwrap(), RunMode::Urls);
        // ...and an all-null object still resolves rather than erroring oddly.
        assert_eq!(
            resolve_run_mode(&json!({ "replay": null, "induce": null })).unwrap(),
            RunMode::Urls
        );
    }
}
