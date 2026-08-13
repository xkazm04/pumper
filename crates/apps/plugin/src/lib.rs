//! Run a sandboxed WASM plugin over documents (fuel + memory limited), deduping
//! the JSON results into a dataset. The extraction logic lives in the .wasm
//! module — swappable at runtime without recompiling the service, and safe to run
//! even if untrusted. Two input modes, mirroring `extractor`: fetch live `urls`,
//! or read stored bodies from a crawl→dataset `source` (no re-fetch).

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::StreamExt;
use pumper_core::error::PluginFailure;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, FetchRequest, FetchStrategy, ManifestExample,
    Provenance, Record, Result, ScrapeApp,
};
use serde_json::{json, Value};

mod observatory;

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

/// The `rules_hash` pin (M12) for a plugin run, or `None` when this deployment
/// cannot state one honestly.
///
/// The "rules" that produced a plugin record are the WASM module plus the
/// per-job `plugin_params` envelope, so the registered pin is
/// `{plugin, version, params}` — and it is registered ONLY when the module
/// self-describes a `version` (`describe` export). Without a version, a pin
/// keyed on the plugin *name* would claim re-derivability across silently
/// swapped .wasm builds — exactly the fabrication `Provenance`'s honest-Null
/// contract forbids — so an undescribed plugin leaves `rules_hash` Null.
async fn plugin_rules_hash(ctx: &AppContext, plugin: &str) -> Option<String> {
    let version = ctx
        .plugins
        .manifests()
        .into_iter()
        .find(|m| m.get("name").and_then(Value::as_str) == Some(plugin))
        .and_then(|m| m.get("version").cloned())
        .filter(|v| !v.is_null())?;
    let pin = json!({
        "plugin": plugin,
        "version": version,
        "params": plugin_params(ctx),
    });
    // Best-effort: provenance is additive, and a registry write failure must
    // never fail a working plugin run.
    ctx.register_rules(&pin).await.ok()
}

/// The one source URL every document in this batch came from, or `None` when the
/// batch spans several. A batch-level `source_url` may only be claimed when it
/// is true of every record in it.
fn single_source_url(metas: &[DocMeta]) -> Option<String> {
    let first = metas.first()?.url.as_str();
    metas
        .iter()
        .all(|m| m.url == first)
        .then(|| first.to_string())
}

/// The batch stamp for a plugin upsert: the run's rules pin plus a source URL
/// only when the whole batch shares one.
fn batch_provenance(metas: &[DocMeta], rules_hash: Option<&str>) -> Provenance {
    Provenance {
        rules_hash: rules_hash.map(str::to_string),
        source_url: single_source_url(metas),
        ..Provenance::default()
    }
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

/// The refusal for a `plugin` param this host cannot execute.
///
/// [`Error::BadRequest`] deliberately, and not by widening anything: it is the
/// variant the runtime already treats as **terminal for the job**
/// (`Error::is_terminal_for_job`), and every input a retry would re-read — the
/// job's `plugin` param, the installed module set, `[plugins] enabled` — is
/// fixed for the life of the job, so the backoff ladder can only produce three
/// identical refusals and ~30s of waiting. `Error::Plugin` would carry a richer
/// class but is retryable, and classifying that whole variant terminal would
/// silently make a `trap` terminal too — the audit r18's profile fix asks for.
fn unloadable_plugin_error(plugin: &str, loaded: &[String]) -> Error {
    let available = if loaded.is_empty() {
        "no plugins are loaded at all — check `[plugins] enabled` and run \
         `just plugins-install`"
            .to_string()
    } else {
        format!("loaded and runnable: {}", loaded.join(", "))
    };
    Error::BadRequest(format!(
        "plugin '{plugin}' is not loaded (see GET /plugins); {available}"
    ))
}

/// Refuses a run whose named plugin the host cannot execute — **before** any
/// fetch, any dataset read and any rules-registry write.
///
/// THE ANTI-PATTERN THIS CLOSES: the door was `ctx.require_str("plugin")?`, a
/// type check and nothing more. A typo, an uninstalled build, or
/// `[plugins] enabled = false` therefore produced one `{"error": ..}` record per
/// URL, `ran: 0`, zero dataset writes and `Ok(..)` — i.e. a green job on
/// `GET /jobs`, a `succeeded` SSE event, a fired result webhook and an empty
/// dataset. Observatory mode in this same app has always validated
/// (`observatory::parse_config`), as does the trigger pipeline; the asymmetry
/// inside one app was the whole defect.
///
/// [`Plugins::has`] answers **executability**, not mere presence — the wasm host
/// resolves it through the same `executable` flag `list()` filters on — so a
/// describe-only module (no `extract`/`extract_v2` export) is refused here
/// instead of failing once per document.
///
/// [`Plugins::has`]: pumper_core::plugin::Plugins::has
fn require_runnable_plugin(ctx: &AppContext, plugin: &str) -> Result<()> {
    if ctx.plugins.has(plugin) {
        return Ok(());
    }
    Err(unloadable_plugin_error(plugin, &ctx.plugins.list()))
}

/// Why one document produced no plugin output.
///
/// Round 14 gave the host a typed [`PluginFailure`]; this app threw it away at
/// the fan-out (`json!({"error": e.to_string()})`), flattening
/// `Unknown | Disabled | Trap | MalformedOutput` into one opaque string — so the
/// result could not report failures by class, and `engine-wasm`'s "extraction
/// propagates the error" was false for the app that *is* extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DocFailure {
    /// The tiered fetch never delivered a document (urls mode only).
    Fetch,
    /// The body was empty, so the plugin was never called. A corpus problem,
    /// not the plugin's — kept distinct for exactly that reason.
    EmptyDocument,
    /// The sandbox call failed, carrying the host's own class.
    Plugin(PluginFailure),
}

impl DocFailure {
    /// Stable snake_case token for the result's `errors_by_class` breakdown.
    /// The plugin classes reuse [`PluginFailure::as_str`], which is already a
    /// contract (the trigger ledger's outcome vocabulary is built from it), so
    /// the two surfaces cannot drift apart.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            DocFailure::Fetch => "fetch",
            DocFailure::EmptyDocument => "empty_document",
            DocFailure::Plugin(kind) => kind.as_str(),
        }
    }

    /// Classify the error a plugin call returned. An error carrying no plugin
    /// class did not come out of the sandbox at all, so it is reported as a
    /// host fault rather than promoted into a class it never claimed — the same
    /// rule `observatory::classify_outcome` applies.
    pub(crate) fn from_plugin_error(e: &Error) -> Self {
        DocFailure::Plugin(e.plugin_failure().unwrap_or(PluginFailure::Host))
    }
}

/// A classified per-document failure: the class plus the message that produced
/// it (free to say whatever is useful — nothing classifies on the prose).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocError {
    class: DocFailure,
    message: String,
}

impl DocError {
    fn new(class: DocFailure, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }
}

/// One document's outcome: the plugin's own JSON output, or a classified
/// failure. Carried through the fan-out instead of being stringified at the
/// call site, so the result can count failures BY CLASS.
pub(crate) type DocOutcome = std::result::Result<Value, DocError>;

/// The result-echo record for one document outcome.
///
/// A failure keeps the long-standing `{"error": <message>}` shape — readers and
/// [`upsert_items`] both key on that field's presence — and now names its
/// `error_class` alongside, so a `trap` is distinguishable from a
/// `malformed_output` without parsing prose.
fn echo_record(outcome: &DocOutcome) -> Value {
    match outcome {
        Ok(v) => v.clone(),
        Err(e) => json!({ "error": e.message, "error_class": e.class.as_str() }),
    }
}

/// Per-document outcome counts for one run: what ran, what failed and why.
///
/// `ran` counts documents whose plugin call **returned**, whatever it returned.
/// That is deliberately not the same predicate as "was written": a plugin's own
/// `{"error": "no <title> found"}` output is DATA (the module ran and reported
/// it could not extract) and is counted separately in `plugin_reported` rather
/// than being folded into the host-failure classes. Before the typed seam the
/// two were the same untyped string test, so a run could not say which had
/// happened.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OutcomeTally {
    /// Documents whose plugin call returned.
    ran: usize,
    /// Documents that produced no plugin answer at all, counted per class.
    failures: BTreeMap<&'static str, usize>,
    /// Returned outputs carrying an `error` key. Not written to the dataset
    /// (see [`upsert_items`]), so this count is the only place they are visible.
    plugin_reported: usize,
}

impl OutcomeTally {
    fn record(&mut self, outcome: &DocOutcome) {
        match outcome {
            Ok(v) => {
                self.ran += 1;
                if v.get("error").is_some() {
                    self.plugin_reported += 1;
                }
            }
            Err(e) => *self.failures.entry(e.class.as_str()).or_default() += 1,
        }
    }

    /// Folds another batch's tally in — backfill runs batch by batch and the
    /// result reports one set of counts for the attempt.
    fn merge(&mut self, other: &OutcomeTally) {
        self.ran += other.ran;
        self.plugin_reported += other.plugin_reported;
        for (class, n) in &other.failures {
            *self.failures.entry(class).or_default() += n;
        }
    }

    /// Documents that produced no plugin answer.
    pub(crate) fn errors(&self) -> usize {
        self.failures.values().sum()
    }

    /// Documents the run actually attempted to run the plugin over.
    pub(crate) fn attempted(&self) -> usize {
        self.ran + self.errors()
    }

    /// `{class: count}` for the classes that actually occurred — an object of
    /// zeros would read as "we checked for a trap and found none", which a run
    /// that never traps has no way to distinguish from one that never looked.
    fn by_class(&self) -> Value {
        Value::Object(
            self.failures
                .iter()
                .map(|(class, n)| ((*class).to_string(), json!(n)))
                .collect(),
        )
    }
}

/// **Partial failure is data; total failure is a failed run.**
///
/// The policy, stated deliberately rather than inherited: a run where SOME
/// documents failed still produced records, and failing it would throw away a
/// 499-of-500 success — so those runs succeed and report the failures by class.
/// A run where EVERY attempted document failed produced nothing, and reporting
/// it `succeeded` is exactly what let a 100%-failed plugin job show green on
/// `GET /jobs`, emit a `succeeded` SSE event and fire a result webhook over an
/// empty dataset. A run that attempted **nothing** (an empty key set, a resumed
/// backfill with no rows left) is not a failure — there was nothing to fail at,
/// and an empty source is a legitimate quiet run.
fn every_document_failed(tally: &OutcomeTally) -> bool {
    tally.attempted() > 0 && tally.ran == 0
}

/// The failure for a run whose every document failed.
///
/// [`Error::App`], so it stays **retryable**: total failure here is usually
/// transient (a site down, every fetch timing out), unlike the deterministic
/// door refusal in [`unloadable_plugin_error`], which is terminal.
fn total_failure_error(plugin: &str, tally: &OutcomeTally) -> Error {
    let classes: Vec<String> = tally
        .failures
        .iter()
        .map(|(class, n)| format!("{class}={n}"))
        .collect();
    Error::App(format!(
        "plugin '{plugin}': all {} documents failed ({}) — nothing was written",
        tally.attempted(),
        classes.join(", ")
    ))
}

/// What a run's plugin calls cost, rolled up across the fan-out.
///
/// Deliberately reported on the JOB RESULT, never merged into the dataset
/// records: fuel varies run to run, so a per-record `fuel_used` would make
/// change detection mark every single record `changed` on every re-run — the
/// telemetry would destroy the very signal the datasets exist to carry.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct CostRollup {
    /// Calls whose cost the host actually measured.
    metered: u64,
    /// Calls that ran against a host that does not meter.
    unmetered: u64,
    fuel_total: u64,
    fuel_max: u64,
    fuel_budget: Option<u64>,
    memory_max: usize,
}

impl CostRollup {
    pub(crate) fn record(&mut self, stats: &pumper_core::plugin::PluginRunStats) {
        match stats.fuel_used {
            Some(fuel) => {
                self.metered += 1;
                self.fuel_total = self.fuel_total.saturating_add(fuel);
                self.fuel_max = self.fuel_max.max(fuel);
                self.fuel_budget = self.fuel_budget.or(stats.fuel_budget);
            }
            None => self.unmetered += 1,
        }
        if let Some(bytes) = stats.memory_bytes {
            self.memory_max = self.memory_max.max(bytes);
        }
    }

    /// Folds another batch's rollup in — backfill runs the plugin batch by
    /// batch, and the result reports one cost for the attempt.
    pub(crate) fn merge(&mut self, other: &CostRollup) {
        self.metered += other.metered;
        self.unmetered += other.unmetered;
        self.fuel_total = self.fuel_total.saturating_add(other.fuel_total);
        self.fuel_max = self.fuel_max.max(other.fuel_max);
        self.fuel_budget = self.fuel_budget.or(other.fuel_budget);
        self.memory_max = self.memory_max.max(other.memory_max);
    }

    /// Which number a reader should treat as this run's cost. `"fuel"` only when
    /// something was genuinely measured — a run with nothing metered says
    /// `"elapsed_ms"` rather than reporting a fuel figure of zero.
    pub(crate) fn signal(&self) -> &'static str {
        if self.metered > 0 {
            "fuel"
        } else {
            "elapsed_ms"
        }
    }

    pub(crate) fn avg_fuel(&self) -> Option<f64> {
        (self.metered > 0).then(|| self.fuel_total as f64 / self.metered as f64)
    }

    pub(crate) fn max_fuel(&self) -> Option<u64> {
        (self.metered > 0).then_some(self.fuel_max)
    }

    pub(crate) fn fuel_budget(&self) -> Option<u64> {
        self.fuel_budget
    }

    pub(crate) fn max_memory(&self) -> Option<usize> {
        (self.metered > 0).then_some(self.memory_max)
    }

    /// The `cost` object for a job result, or `None` when nothing was measured —
    /// a zeroed object would read as "this run was free".
    pub(crate) fn to_json(self) -> Option<Value> {
        (self.metered > 0).then(|| {
            json!({
                "signal": "fuel",
                "calls_metered": self.metered,
                "calls_unmetered": self.unmetered,
                "fuel_total": self.fuel_total,
                "fuel_max": self.fuel_max,
                "fuel_avg": self.avg_fuel(),
                "fuel_budget": self.fuel_budget,
                "memory_bytes_max": self.memory_max,
            })
        })
    }
}

pub struct Plugin;

/// Max live records pulled from a source dataset when no explicit `keys` (and no
/// `_trigger.keys`) narrow the set — bounds the dataset read and the fan-out.
/// Backfill mode also pages through `page_versions` in batches of this size.
pub(crate) const SOURCE_LIST_LIMIT: i64 = 10_000;

/// The crawl app's versioned archive dataset (see the crawl app): one record per
/// CHANGED revision of a page, keyed `{url}#{revision}`, carrying
/// `{url, revision, artifact_path, job_id, simhash, fetched_at}` — the same
/// artifact contract as `pages`, so `read_source_artifact` resolves historical
/// bodies unchanged.
pub(crate) const VERSIONS_DATASET: &str = "page_versions";

/// Cap on the per-key `missing_keys` echo in a backfill result — a large archive
/// could otherwise blow up the stored job result; `missing` keeps the full count.
const MISSING_ECHO_LIMIT: usize = 100;

/// Backfill checkpoint blob version — bump on shape change; a mismatch restores
/// fresh (a full re-scan is correct; a mis-resumed cursor silently skips rows).
const BACKFILL_STATE_VERSION: u32 = 1;

/// The resumable state of a backfill run (M23): the `page_versions` keyset
/// cursor plus the running tallies. `missing_keys` is deliberately NOT carried —
/// it is a per-attempt diagnostic, and reporting a prior attempt's unreadable
/// artifacts as this one's observations would be a fabrication.
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
    ran: usize,
    #[serde(default)]
    batches: usize,
    #[serde(default)]
    new: usize,
    #[serde(default)]
    changed: usize,
    #[serde(default)]
    unchanged: usize,
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
         _url + _observed_at. Observatory mode: {\"observatory\": true | {\"plugins\": \
         [..]?, \"sample_per_site\": 25}} replays each plugin (default all loaded) over \
         sampled stored pages per site (newest + seeded-random across the live dataset + \
         page_versions), classifies outcomes (ok/trap/empty/schema_invalid) and upserts \
         per (plugin, site) drift rows into the `observatory` dataset (sampled/total \
         reported; <5 stored pages => low_confidence; rising empty-rate flagged)."
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "anyOf": [
                    { "required": ["plugin"] },
                    { "required": ["observatory"] }
                ],
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
                    "dataset": { "type": "string", "description": "Output dataset name (default \"plugin_out\"; observatory mode defaults to \"observatory\")." },
                    "observatory": {
                        "description": "Observatory mode: replay plugins against sampled stored pages per site and upsert per (plugin, site) drift rows into the `observatory` dataset. `true` audits all loaded plugins with defaults; an object narrows/tunes it. `plugin` is not required in this mode; `source` defaults to {app: \"crawl\", dataset: \"pages\"}.",
                        "oneOf": [
                            { "type": "boolean" },
                            {
                                "type": "object",
                                "properties": {
                                    "plugins": {
                                        "type": "array",
                                        "items": { "type": "string", "minLength": 1 },
                                        "minItems": 1,
                                        "description": "Plugins to audit (default: all loaded)."
                                    },
                                    "sample_per_site": {
                                        "type": "integer",
                                        "minimum": 1,
                                        "description": "Stored pages sampled per site: newest half + seeded-random rest (default 25). Rows report sampled/total; sites with <5 stored pages are marked low_confidence."
                                    }
                                },
                                "additionalProperties": false
                            }
                        ]
                    }
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
                ManifestExample {
                    description: "Observatory: replay every loaded plugin against sampled \
                                  stored pages per site and record per-(plugin, site) drift rows",
                    params: json!({ "observatory": true }),
                },
                ManifestExample {
                    description: "Observatory over two plugins with a larger per-site sample",
                    params: json!({
                        "observatory": { "plugins": ["title"], "sample_per_site": 50 },
                        "source": { "app": "crawl", "dataset": "pages" }
                    }),
                },
            ],
            output_shape: Some(
                "{mode, plugin, ran, errors, errors_by_class, plugin_reported_errors, new, \
                 changed, unchanged, cost|null} — per-document plugin results deduped into the \
                 output dataset. `ran` counts calls that RETURNED; `errors` counts documents \
                 the plugin never answered for, broken down by class in `errors_by_class` \
                 (fetch / empty_document / unknown_plugin / plugins_disabled / missing_export / \
                 trap / malformed_output / host_error); `plugin_reported_errors` counts outputs \
                 the plugin returned carrying its own `error` key (data, not written). A run \
                 whose every attempted document failed FAILS the job. Plus, per mode: urls \
                 {requested, records[]}; source {source{app,dataset}, requested, loaded, \
                 missing, missing_keys[], records[]}; backfill {resumed_from_checkpoint, \
                 scanned, skipped_pattern, loaded, batches, missing, missing_keys[]} (no \
                 records echo). Observatory mode: {sites, rows, pages_replayed, \
                 low_confidence_sites, flagged_empty_rising, new, changed, unchanged} with \
                 per-(plugin, site) drift rows in the observatory dataset",
            ),
            cost_class: CostClass::Metered,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        // Observatory mode (M16): corpus-scale replay of plugins against stored
        // pages with per-(plugin, site) drift scoring — no `plugin` param needed
        // (it audits a plugin LIST, default all loaded).
        if ctx
            .params
            .get("observatory")
            .is_some_and(|v| v.as_bool().unwrap_or(true))
        {
            return observatory::run_observatory(&ctx).await;
        }
        let plugin = ctx.require_str("plugin")?.to_string();
        // The door: refuse a plugin this host cannot execute BEFORE the run
        // spends a fetch, a dataset read or a registry write on it.
        require_runnable_plugin(&ctx, &plugin)?;
        let dataset = ctx
            .params
            .get("dataset")
            .and_then(Value::as_str)
            .unwrap_or("plugin_out")
            .to_string();

        // Two input modes: fetch live `urls`, or read stored bodies from a
        // crawl→dataset `source`. Exactly one is required.
        let rules_hash = plugin_rules_hash(&ctx, &plugin).await;
        if ctx.params.get("source").is_some() {
            self.run_source_mode(&ctx, &plugin, &dataset, rules_hash.as_deref())
                .await
        } else {
            self.run_urls_mode(&ctx, &plugin, &dataset, rules_hash.as_deref())
                .await
        }
    }
}

impl Plugin {
    /// URLs mode: fetch each URL (tiered) and run the plugin over it — fetch and
    /// plugin execution pipelined per URL.
    async fn run_urls_mode(
        &self,
        ctx: &AppContext,
        plugin: &str,
        dataset: &str,
        rules_hash: Option<&str>,
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

        // Bounded fetch+run fan-out: the governor serializes same-host fetches but
        // caps nothing globally, so a large `urls` list would open one socket per
        // URL at once. `buffered` preserves order for the positional zip below.
        let concurrency = concurrency(ctx);
        let plugin_params = plugin_params(ctx);
        let plugins = ctx.plugins.clone();
        // Every fetch goes through the METERED chokepoint `ctx.fetch`, never the
        // raw `ctx.engines.fetch`: the raw fetcher skips the cost ledger, the
        // per-job budget clamp (so `strategy: "auto_with_research"` under a $1
        // budget — or the $0 a DataHub `cost:pause` forces — could spend
        // unbounded Claude money invisibly), the learned tier router, and the
        // VCR cassette (so a recorded run of this app silently hit the live
        // network on replay). The futures borrow `&ctx`; nothing is spawned, so
        // there is no `'static` bound to satisfy. Guarded by
        // `crates/core/tests/fetch_chokepoint.rs`.
        //
        // clippy::redundant_iter_cloned — the `cloned()` looks redundant (the body
        // only ever takes `&url`), but it is load-bearing for inference: with
        // `Item = &String`/`&str` the closure must implement `FnOnce` for ANY
        // lifetime to satisfy the `buffered()` Send bound, and rustc rejects it
        // with "implementation of FnOnce is not general enough". Owning the item
        // removes the lifetime from the closure signature. Verified: both
        // `.iter()` and `.iter().map(String::as_str)` fail to compile.
        #[allow(clippy::redundant_iter_cloned)]
        let tasks = urls.iter().cloned().map(|url| {
            let p = plugins.clone();
            let name = plugin.to_string();
            let pp = plugin_params.clone();
            let mut req = FetchRequest::new(&url);
            req.strategy = strategy;
            async move {
                let doc = match ctx.fetch(req).await {
                    Ok(out) => out.html.or(out.text).unwrap_or_default(),
                    Err(e) => {
                        return (
                            Err(DocError::new(DocFailure::Fetch, format!("fetch: {e}"))),
                            None,
                        )
                    }
                };
                if doc.is_empty() {
                    return (
                        Err(DocError::new(DocFailure::EmptyDocument, "empty document")),
                        None,
                    );
                }
                match p.run_metered(&name, &doc, &pp).await {
                    Ok((value, stats)) => (Ok(value), Some(stats)),
                    Err(e) => (
                        Err(DocError::new(
                            DocFailure::from_plugin_error(&e),
                            e.to_string(),
                        )),
                        None,
                    ),
                }
            }
        });
        let paired: Vec<(DocOutcome, Option<pumper_core::plugin::PluginRunStats>)> =
            futures::stream::iter(tasks)
                .buffered(concurrency)
                .collect()
                .await;
        let mut cost = CostRollup::default();
        let mut tally = OutcomeTally::default();
        let mut outcomes: Vec<DocOutcome> = Vec::with_capacity(paired.len());
        for (outcome, stats) in paired {
            if let Some(stats) = &stats {
                cost.record(stats);
            }
            tally.record(&outcome);
            outcomes.push(outcome);
        }
        if every_document_failed(&tally) {
            return Err(total_failure_error(plugin, &tally));
        }

        let metas: Vec<DocMeta> = urls.iter().map(|u| DocMeta::live(u.clone())).collect();
        let items = upsert_items(&metas, &mut outcomes);
        let summary = ctx
            .upsert_many_with_provenance(dataset, &items, batch_provenance(&metas, rules_hash))
            .await?;

        Ok(with_outcome_fields(
            json!({
                "mode": "urls",
                "plugin": plugin,
                "requested": urls.len(),
                "ran": tally.ran,
                "new": summary.new.len(),
                "changed": summary.changed.len(),
                "unchanged": summary.unchanged,
                "cost": cost.to_json(),
                "records": outcomes.iter().map(echo_record).collect::<Vec<Value>>(),
            }),
            &tally,
        ))
    }

    /// Source mode: run the plugin over already-crawled bodies (no re-fetch).
    /// Key precedence mirrors `extractor`: explicit `source.keys` → the firing
    /// trigger's `_trigger.keys` → all live records in the source dataset.
    async fn run_source_mode(
        &self,
        ctx: &AppContext,
        plugin: &str,
        dataset: &str,
        rules_hash: Option<&str>,
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
            return self
                .run_backfill(ctx, plugin, dataset, &src_app, rules_hash)
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

        let (metas, mut outcomes, cost, tally) = self.run_plugin_batch(ctx, plugin, keyed).await;
        let loaded = metas.len();
        if every_document_failed(&tally) {
            return Err(total_failure_error(plugin, &tally));
        }
        let items = upsert_items(&metas, &mut outcomes);
        let summary = ctx
            .upsert_many_with_provenance(dataset, &items, batch_provenance(&metas, rules_hash))
            .await?;

        Ok(with_outcome_fields(
            json!({
                "mode": "source",
                "plugin": plugin,
                "source": { "app": src_app, "dataset": src_dataset },
                "requested": requested,
                "loaded": loaded,
                "ran": tally.ran,
                "missing": missing.len(),
                "missing_keys": missing,
                "new": summary.new.len(),
                "changed": summary.changed.len(),
                "unchanged": summary.unchanged,
                "cost": cost.to_json(),
                "records": outcomes.iter().map(echo_record).collect::<Vec<Value>>(),
            }),
            &tally,
        ))
    }

    /// Runs the plugin over one batch of `(meta, body)` pairs with the bounded,
    /// order-preserving fan-out (bodies are moved into the tasks, never cloned);
    /// returns the metas re-paired positionally with the typed outcomes, the
    /// cost rollup and the per-class outcome tally.
    async fn run_plugin_batch(
        &self,
        ctx: &AppContext,
        plugin: &str,
        keyed: Vec<(DocMeta, String)>,
    ) -> (Vec<DocMeta>, Vec<DocOutcome>, CostRollup, OutcomeTally) {
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
                    return (
                        Err(DocError::new(DocFailure::EmptyDocument, "empty document")),
                        None,
                    );
                }
                match p.run_metered(&name, &doc, &pp).await {
                    Ok((value, stats)) => (Ok(value), Some(stats)),
                    Err(e) => (
                        Err(DocError::new(
                            DocFailure::from_plugin_error(&e),
                            e.to_string(),
                        )),
                        None,
                    ),
                }
            }
        });
        // Bounded run fan-out; `buffered` keeps order for the positional zip.
        let paired: Vec<(DocOutcome, Option<pumper_core::plugin::PluginRunStats>)> =
            futures::stream::iter(tasks)
                .buffered(concurrency)
                .collect()
                .await;
        let mut cost = CostRollup::default();
        let mut tally = OutcomeTally::default();
        let mut outcomes: Vec<DocOutcome> = Vec::with_capacity(paired.len());
        for (outcome, stats) in paired {
            if let Some(stats) = &stats {
                cost.record(stats);
            }
            tally.record(&outcome);
            outcomes.push(outcome);
        }
        (metas, outcomes, cost, tally)
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
        rules_hash: Option<&str>,
    ) -> Result<Value> {
        let pattern = ctx
            .params
            .pointer("/source/url_pattern")
            .and_then(Value::as_str)
            .map(|p| {
                regex::Regex::new(p).map_err(|e| Error::App(format!("bad url_pattern '{p}': {e}")))
            })
            .transpose()?;

        // Durable execution (M23): backfill is the one genuinely long plugin
        // mode — it pages the WHOLE `page_versions` archive, running the module
        // and upserting per batch. The resumable unit is the keyset cursor plus
        // the running tallies, so a reap resumes at the next page instead of
        // re-running the plugin over every archived revision from the start.
        let mut st = BackfillState::restore(ctx.restore());
        let resumed = st.after.is_some();
        let mut after: Option<(String, String)> = st.after.clone();
        let mut scanned = st.scanned;
        let mut skipped_pattern = st.skipped_pattern;
        let mut loaded = st.loaded;
        let mut ran = st.ran;
        let mut batches = st.batches;
        let mut missing: Vec<Value> = Vec::new();
        // Cost is per ATTEMPT, not per logical run: it is deliberately not in
        // `BackfillState`, because a resumed attempt did not pay for the batches
        // a previous one ran, and claiming it did would misprice the plugin.
        let mut cost = CostRollup::default();
        // Likewise per ATTEMPT: `ran` is checkpointed (it is a fact about the
        // logical run), but the failure breakdown describes the documents THIS
        // attempt saw, and restating a prior attempt's traps as this one's
        // observations would be a fabrication — the same rule `missing_keys`
        // already follows.
        let mut tally = OutcomeTally::default();
        let (mut new, mut changed, mut unchanged) = (st.new, st.changed, st.unchanged);
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
                let (metas, mut outcomes, batch_cost, batch_tally) =
                    self.run_plugin_batch(ctx, plugin, keyed).await;
                cost.merge(&batch_cost);
                tally.merge(&batch_tally);
                ran += batch_tally.ran;
                let items = upsert_items(&metas, &mut outcomes);
                let summary = ctx
                    .upsert_many_with_provenance(
                        dataset,
                        &items,
                        batch_provenance(&metas, rules_hash),
                    )
                    .await?;
                new += summary.new.len();
                changed += summary.changed.len();
                unchanged += summary.unchanged;
            }
            // Cursor + tallies AFTER the batch's writes committed, so a resume
            // never re-runs a page and never double-counts one.
            st = BackfillState {
                v: BACKFILL_STATE_VERSION,
                after: after.clone(),
                scanned,
                skipped_pattern,
                loaded,
                ran,
                batches,
                new,
                changed,
                unchanged,
            };
            ctx.checkpoint(st.to_value()).await;
            if short {
                break;
            }
        }
        if every_document_failed(&tally) {
            return Err(total_failure_error(plugin, &tally));
        }
        // Bound the per-key echo; the full count is still reported.
        let missing_count = missing.len();
        missing.truncate(MISSING_ECHO_LIMIT);
        Ok(with_outcome_fields(
            json!({
                "mode": "backfill",
                "resumed_from_checkpoint": resumed,
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
                // This attempt's plugin cost only — see `cost` above.
                "cost": cost.to_json(),
            }),
            &tally,
        ))
    }
}

/// Merges the outcome keys **every** write mode must report into that mode's
/// result object.
///
/// One definition of the manifest contract rather than three: `output_shape`
/// promised `errors` and no mode emitted it, because each of the three result
/// builders was written by hand and drifted independently. Anything a reader of
/// `GET /apps` is told to expect from a write mode belongs here.
fn with_outcome_fields(mut result: Value, tally: &OutcomeTally) -> Value {
    if let Value::Object(map) = &mut result {
        map.insert("errors".into(), json!(tally.errors()));
        map.insert("errors_by_class".into(), tally.by_class());
        map.insert(
            "plugin_reported_errors".into(),
            json!(tally.plugin_reported),
        );
    }
    result
}

/// Builds the upsert items from `(meta, outcome)` pairs: skip anything that
/// produced no record, tag each record with its natural source URL as `_url`
/// and, for archived versions, its `_observed_at`.
///
/// Two different things are skipped and the distinction is deliberate:
/// a typed [`DocError`] (the plugin never answered), and a returned output that
/// carries its own `error` key (the plugin ran and reported it could not
/// extract). The second is the plugin's DATA, but a record that is nothing but
/// an error message is not a fact about the page either, so it stays out of the
/// dataset — and [`OutcomeTally::plugin_reported`] counts it, which is the part
/// that used to be invisible.
fn upsert_items(metas: &[DocMeta], outcomes: &mut [DocOutcome]) -> Vec<(String, Value)> {
    metas
        .iter()
        .zip(outcomes.iter_mut())
        .filter_map(|(meta, outcome)| {
            let rec = outcome.as_mut().ok()?;
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
    use super::{
        batch_provenance, echo_record, every_document_failed, parse_concurrency, pick_as_of,
        total_failure_error, unloadable_plugin_error, upsert_items, versioned_key, CostRollup,
        DocError, DocFailure, DocMeta, DocOutcome, OutcomeTally, DEFAULT_CONCURRENCY,
    };
    use pumper_core::error::PluginFailure;
    use pumper_core::plugin::PluginRunStats;
    use pumper_core::Error;
    use serde_json::{json, Value};

    fn failed(class: DocFailure, message: &str) -> DocOutcome {
        Err(DocError::new(class, message))
    }

    fn tally_of(outcomes: &[DocOutcome]) -> OutcomeTally {
        let mut t = OutcomeTally::default();
        for o in outcomes {
            t.record(o);
        }
        t
    }

    fn metered(fuel: u64, memory: usize) -> PluginRunStats {
        PluginRunStats {
            fuel_used: Some(fuel),
            fuel_budget: Some(1_000_000),
            memory_bytes: Some(memory),
            memory_cap_bytes: Some(64 * 1024 * 1024),
        }
    }

    /// The cost object must be ABSENT rather than zeroed when nothing was
    /// measured: a `{"fuel_total": 0}` on an unmetered host reads as "this run
    /// was free", which is the one thing it definitely does not mean.
    #[test]
    fn an_unmetered_run_reports_no_cost_rather_than_a_free_one() {
        let mut roll = CostRollup::default();
        assert_eq!(roll.to_json(), None, "nothing ran at all");
        assert_eq!(roll.signal(), "elapsed_ms");

        roll.record(&PluginRunStats::unmetered());
        roll.record(&PluginRunStats::unmetered());
        assert_eq!(
            roll.to_json(),
            None,
            "two calls against a host that cannot meter is still no cost signal"
        );
        assert_eq!(roll.signal(), "elapsed_ms", "fall back, and say so");
        assert_eq!(roll.avg_fuel(), None);
    }

    #[test]
    fn a_metered_run_rolls_up_total_max_and_average() {
        let mut roll = CostRollup::default();
        roll.record(&metered(100, 65_536));
        roll.record(&metered(300, 131_072));
        roll.record(&PluginRunStats::unmetered()); // must not drag the average
        assert_eq!(roll.signal(), "fuel");
        assert_eq!(roll.avg_fuel(), Some(200.0), "averaged over METERED calls");
        assert_eq!(roll.max_fuel(), Some(300));
        assert_eq!(roll.max_memory(), Some(131_072));
        assert_eq!(roll.fuel_budget(), Some(1_000_000));
        let cost = roll.to_json().expect("something was measured");
        assert_eq!(cost["signal"], "fuel");
        assert_eq!(cost["calls_metered"], 2);
        assert_eq!(cost["calls_unmetered"], 1);
        assert_eq!(cost["fuel_total"], 400);
    }

    /// Backfill runs the plugin batch by batch and reports one cost per attempt.
    #[test]
    fn merging_batches_keeps_totals_and_maxima() {
        let mut a = CostRollup::default();
        a.record(&metered(100, 65_536));
        let mut b = CostRollup::default();
        b.record(&metered(500, 262_144));
        a.merge(&b);
        assert_eq!(a.max_fuel(), Some(500));
        assert_eq!(a.max_memory(), Some(262_144));
        assert_eq!(a.avg_fuel(), Some(300.0));
    }

    #[test]
    fn batch_source_url_is_claimed_only_when_every_doc_shares_one() {
        let one = [DocMeta::live("https://a/x".into())];
        let mixed = [
            DocMeta::live("https://a/x".into()),
            DocMeta::live("https://a/y".into()),
        ];
        assert_eq!(
            batch_provenance(&one, Some("deadbeef"))
                .source_url
                .as_deref(),
            Some("https://a/x")
        );
        // Naming one URL of many would be a fabrication on the other records.
        assert_eq!(batch_provenance(&mixed, Some("deadbeef")).source_url, None);
        // An empty batch knows nothing.
        assert_eq!(batch_provenance(&[], Some("deadbeef")).source_url, None);
    }

    #[test]
    fn an_undescribed_plugin_leaves_the_rules_pin_null() {
        // `plugin_rules_hash` returns None when the module doesn't self-describe
        // a version; the stamp must then carry no pin rather than one keyed on a
        // plugin NAME, which would claim re-derivability across swapped builds.
        let metas = [DocMeta::live("https://a/x".into())];
        let prov = batch_provenance(&metas, None);
        assert!(prov.rules_hash.is_none());
        assert!(!prov.replayable());
        // …but the URL it does know is still stated.
        assert_eq!(prov.source_url.as_deref(), Some("https://a/x"));
    }

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

    // --- the run door -------------------------------------------------------

    /// THE anti-pattern: the door was a type check, so a job naming a plugin the
    /// host cannot execute ran the whole fan-out and then reported SUCCESS. The
    /// refusal has to be terminal too — the plugin set and `[plugins] enabled`
    /// are fixed for the life of the job, so the retry ladder can only re-read
    /// them and re-refuse three times.
    #[test]
    fn an_unloadable_plugin_is_a_terminal_refusal_not_a_retried_one() {
        let err = unloadable_plugin_error("titel", &["title".into(), "delta-slim".into()]);
        assert!(
            err.is_terminal_for_job(),
            "a deterministic configuration error must not burn the retry ladder: {err}"
        );
        let msg = err.to_string();
        assert!(msg.contains("titel"), "name the plugin that was asked for");
        assert!(
            msg.contains("GET /plugins"),
            "point at the discovery surface"
        );
        assert!(msg.contains("title"), "and list what IS runnable: {msg}");
    }

    /// A host with nothing loaded is a different operator action from a typo —
    /// `[plugins] enabled = false` is a config change, not a build step — so the
    /// refusal must not tell an operator to check a list that is empty.
    #[test]
    fn an_empty_plugin_host_says_install_or_enable_not_check_the_list() {
        let msg = unloadable_plugin_error("title", &[]).to_string();
        assert!(msg.contains("no plugins are loaded"), "{msg}");
        assert!(msg.contains("plugins-install"), "{msg}");
        assert!(msg.contains("[plugins] enabled"), "{msg}");
    }

    // --- typed per-document failures ---------------------------------------

    /// The class survives out of the fan-out instead of being flattened into
    /// one opaque `{"error": <prose>}`. Rewording a host message must not move
    /// a document between classes — the same guarantee `PluginFailure` gives
    /// the observatory.
    #[test]
    fn a_document_failure_keeps_its_class_not_just_its_prose() {
        for (kind, token) in [
            (PluginFailure::Unknown, "unknown_plugin"),
            (PluginFailure::Disabled, "plugins_disabled"),
            (PluginFailure::MissingExport, "missing_export"),
            (PluginFailure::Trap, "trap"),
            (PluginFailure::MalformedOutput, "malformed_output"),
            (PluginFailure::Host, "host_error"),
        ] {
            let err = Error::plugin(kind, "title", "wholly reworded prose");
            assert_eq!(DocFailure::from_plugin_error(&err).as_str(), token);
        }
        // An error from outside the sandbox carries no class and must not be
        // promoted into one it never claimed.
        let outside = Error::App("the database was busy".into());
        assert_eq!(
            DocFailure::from_plugin_error(&outside),
            DocFailure::Plugin(PluginFailure::Host)
        );
        // A fetch that never delivered a document is not a plugin failure at
        // all, and neither is an empty stored body.
        assert_eq!(DocFailure::Fetch.as_str(), "fetch");
        assert_eq!(DocFailure::EmptyDocument.as_str(), "empty_document");
    }

    /// The echo keeps the `{"error": ..}` shape every existing reader keys on,
    /// and gains the class beside it.
    #[test]
    fn the_echo_names_the_class_without_dropping_the_error_key() {
        let echoed = echo_record(&failed(
            DocFailure::Plugin(PluginFailure::Trap),
            "all fuel consumed",
        ));
        assert_eq!(echoed["error"], "all fuel consumed");
        assert_eq!(echoed["error_class"], "trap");
        // A successful output passes through untouched.
        assert_eq!(
            echo_record(&Ok(json!({ "title": "x" }))),
            json!({ "title": "x" })
        );
    }

    // --- the failure policy -------------------------------------------------

    /// THE anti-pattern this closes: a run where every single document failed
    /// reported `ran: 0`, wrote nothing, and returned `Ok` — a green job, a
    /// `succeeded` SSE event, a fired result webhook and an empty dataset.
    /// Partial failure is a different case and must stay a success.
    #[test]
    fn a_total_failure_fails_the_run_while_a_partial_one_still_succeeds() {
        let all_failed = tally_of(&[
            failed(DocFailure::Plugin(PluginFailure::Disabled), "off"),
            failed(DocFailure::Plugin(PluginFailure::Disabled), "off"),
        ]);
        assert!(every_document_failed(&all_failed));

        let partial = tally_of(&[
            Ok(json!({ "title": "x" })),
            failed(DocFailure::Fetch, "fetch: 503"),
        ]);
        assert!(
            !every_document_failed(&partial),
            "one good record out of two is not a failed run"
        );

        // Nothing attempted is not a failure: an empty source, or a resumed
        // backfill with no rows left, is a legitimate quiet run.
        assert!(!every_document_failed(&OutcomeTally::default()));
    }

    /// The failure has to say WHICH classes killed the run — "all 3 documents
    /// failed" with no breakdown sends an operator to read logs.
    #[test]
    fn the_total_failure_error_names_the_classes_and_stays_retryable() {
        let tally = tally_of(&[
            failed(DocFailure::Plugin(PluginFailure::Trap), "fuel"),
            failed(DocFailure::Plugin(PluginFailure::Trap), "fuel"),
            failed(DocFailure::Fetch, "fetch: timeout"),
        ]);
        let err = total_failure_error("title", &tally);
        let msg = err.to_string();
        assert!(msg.contains("all 3 documents failed"), "{msg}");
        assert!(msg.contains("trap=2"), "{msg}");
        assert!(msg.contains("fetch=1"), "{msg}");
        assert!(
            !err.is_terminal_for_job(),
            "a site being down is transient — this one keeps its retries"
        );
    }

    // --- what becomes data --------------------------------------------------

    /// A plugin's own `{"error": "no <title> found"}` output is the module
    /// saying it could not extract — DATA about the page, not a host failure.
    /// It must not be counted as an error class, must not fail a total-failure
    /// check on its own, and must still stay out of the dataset (a record that
    /// is nothing but an error message is not a fact about the page).
    #[test]
    fn a_plugins_own_error_output_is_counted_as_data_not_as_a_host_failure() {
        let tally = tally_of(&[
            Ok(json!({ "error": "no <title> found" })),
            Ok(json!({ "title": "x" })),
        ]);
        assert_eq!(tally.ran, 2, "the module ran on both pages");
        assert_eq!(tally.errors(), 0, "neither is a host failure");
        assert_eq!(tally.plugin_reported, 1);
        assert!(!every_document_failed(&tally));

        let metas = [
            DocMeta::live("https://a/x".into()),
            DocMeta::live("https://a/y".into()),
        ];
        let mut outcomes: Vec<DocOutcome> = vec![
            Ok(json!({ "error": "no <title> found" })),
            Ok(json!({ "title": "x" })),
        ];
        let items = upsert_items(&metas, &mut outcomes);
        assert_eq!(items.len(), 1, "only the real extraction is written");
        assert_eq!(items[0].0, "https://a/y");
        assert_eq!(items[0].1["_url"], "https://a/y");
    }

    /// The tally's classes are what the result publishes, so an empty class
    /// must be ABSENT rather than zero — a `{"trap": 0}` reads as "we looked
    /// and found none", which a run that never traps cannot distinguish from
    /// one that never looked.
    #[test]
    fn the_class_breakdown_omits_classes_that_did_not_occur() {
        let tally = tally_of(&[
            failed(DocFailure::Plugin(PluginFailure::Trap), "fuel"),
            Ok(json!({ "title": "x" })),
        ]);
        assert_eq!(tally.by_class(), json!({ "trap": 1 }));
        assert_eq!(tally.errors(), 1);
        assert_eq!(tally.attempted(), 2);
        assert_eq!(OutcomeTally::default().by_class(), json!({}));
    }

    /// Backfill runs batch by batch and reports one breakdown for the attempt.
    #[test]
    fn merging_batch_tallies_pools_every_class() {
        let mut a = tally_of(&[failed(DocFailure::Plugin(PluginFailure::Trap), "fuel")]);
        let b = tally_of(&[
            failed(DocFailure::Plugin(PluginFailure::Trap), "fuel"),
            failed(DocFailure::Fetch, "503"),
            Ok(json!({ "error": "no title" })),
        ]);
        a.merge(&b);
        assert_eq!(a.by_class(), json!({ "trap": 2, "fetch": 1 }));
        assert_eq!(a.ran, 1);
        assert_eq!(a.plugin_reported, 1);
        assert_eq!(a.attempted(), 4);
    }

    /// Archived-version records carry both tags; a non-object output is written
    /// as-is rather than being silently dropped or wrapped.
    #[test]
    fn upsert_items_tags_url_and_observed_at_without_touching_scalars() {
        let metas = [
            DocMeta {
                key: "https://a/x@2026-01-05".into(),
                url: "https://a/x".into(),
                observed_at: Some("2026-01-05T00:00:00+00:00".into()),
            },
            DocMeta::live("https://a/y".into()),
        ];
        let mut outcomes: Vec<DocOutcome> = vec![Ok(json!({ "title": "v1" })), Ok(json!(42))];
        let items = upsert_items(&metas, &mut outcomes);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, "https://a/x@2026-01-05");
        assert_eq!(items[0].1["_url"], "https://a/x");
        assert_eq!(items[0].1["_observed_at"], "2026-01-05T00:00:00+00:00");
        assert_eq!(items[1].1, json!(42), "a scalar output is data too");
        // A typed failure never reaches the dataset.
        let mut only_failures: Vec<DocOutcome> =
            vec![failed(DocFailure::Fetch, "fetch: 503"), Ok(Value::Null)];
        let items = upsert_items(&metas, &mut only_failures);
        assert_eq!(items.len(), 1, "the fetch failure is not a record");
        assert_eq!(items[0].0, "https://a/y");
    }
}
