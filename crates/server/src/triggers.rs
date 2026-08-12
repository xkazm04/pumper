//! Reactive-pipeline trigger evaluation: the pure decision/shaping half.
//!
//! A trigger is an edge (source event → enqueue target app). This module owns
//! everything that can be unit-tested without a database: does the event match
//! the trigger's filters, may this hop fire (cycle/depth guards), what
//! `_trigger` object gets injected into the target's params, and the
//! idempotency key that makes a trigger fire at most once per source job run.
//! The worker hooks (`fire_dataset_triggers` / `fire_terminal_triggers`) do
//! the IO around these.

use pumper_core::config::TriggersConfig;
use pumper_core::{Job, Revision, Trigger};
use serde_json::{json, Value};

/// Whether a hop may fire, per the provenance riding in the source job's
/// `params._trigger` (chain of trigger ids + depth).
#[derive(Debug, PartialEq)]
pub enum FireDecision {
    /// Fire, carrying the next hop's provenance.
    Fire { depth: u32, chain: Vec<String> },
    /// The trigger already appears in the chain — a cycle; skip.
    SkipCycle,
    /// The chain is at max depth; skip.
    SkipDepth,
}

/// Reads provenance from a source job's params: (depth, chain). Jobs that were
/// not trigger-fired have neither — depth 0, empty chain.
pub fn provenance(source_params: &Value) -> (u32, Vec<String>) {
    let t = source_params.get("_trigger");
    let depth = t
        .and_then(|t| t.get("depth"))
        .and_then(Value::as_u64)
        .unwrap_or(0) as u32;
    let chain = t
        .and_then(|t| t.get("chain"))
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    (depth, chain)
}

/// Cycle + depth guard for one candidate hop.
pub fn decide(trigger_id: &str, source_params: &Value, cfg: &TriggersConfig) -> FireDecision {
    let (depth, mut chain) = provenance(source_params);
    if chain.iter().any(|id| id == trigger_id) {
        return FireDecision::SkipCycle;
    }
    if depth + 1 > cfg.max_depth {
        return FireDecision::SkipDepth;
    }
    chain.push(trigger_id.to_string());
    FireDecision::Fire {
        depth: depth + 1,
        chain,
    }
}

/// True when a revision's change kind passes the trigger's `on_change` filter.
/// `fresh` = new|changed; `any`/absent = everything.
pub fn change_matches(on_change: Option<&str>, change: &str) -> bool {
    match on_change.unwrap_or("any") {
        "any" => true,
        "fresh" => matches!(change, "new" | "changed"),
        filter => filter == change,
    }
}

/// True when a terminal status passes the trigger's `on_status` filter.
pub fn status_matches(on_status: Option<&str>, status: &str) -> bool {
    match on_status.unwrap_or("succeeded") {
        "any" => matches!(status, "succeeded" | "failed" | "cancelled"),
        filter => filter == status,
    }
}

/// Target params: the trigger's static template with `_trigger` merged over it
/// (injected key wins; a non-object template is replaced by a fresh object).
pub fn merged_params(template: &Value, trigger_obj: Value) -> Value {
    let mut obj = match template {
        Value::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };
    obj.insert("_trigger".to_string(), trigger_obj);
    Value::Object(obj)
}

/// The `_trigger` object for a dataset-change hop. Keys are capped at
/// `cfg.key_cap`; `count` stays exact — targets fetch full data by key.
///
/// `app` is the namespace the CHANGED RECORDS live under, which is not always
/// the source job's app: a `ca-grants` run's hop off `grants/unified` must tell
/// its target `grants`, because that is the only app the target can read those
/// keys back from. (`source_job_id` still carries the real provenance.)
#[allow(clippy::too_many_arguments)]
pub fn dataset_trigger_obj(
    trigger: &Trigger,
    source_job: &Job,
    app: &str,
    dataset: &str,
    revs: &[&Revision],
    depth: u32,
    chain: &[String],
    cfg: &TriggersConfig,
) -> Value {
    let keys: Vec<&str> = revs
        .iter()
        .take(cfg.key_cap)
        .map(|r| r.key.as_str())
        .collect();
    json!({
        "trigger_id": trigger.id,
        "source_kind": "dataset",
        "app": app,
        "dataset": dataset,
        "kind": trigger.on_change.as_deref().unwrap_or("any"),
        "count": revs.len(),
        "keys": keys,
        "source_job_id": source_job.id,
        "depth": depth,
        "chain": chain,
    })
}

/// The `_trigger` object for a terminal-job hop. Carries a compact result
/// summary (new/changed counts when the result exposes them), never the full
/// result — targets fetch it via GET /jobs/{id}.
pub fn terminal_trigger_obj(
    trigger: &Trigger,
    source_job: &Job,
    depth: u32,
    chain: &[String],
) -> Value {
    let summary = source_job.result.as_ref().map(|r| {
        json!({
            "new": r.get("new").cloned().unwrap_or(Value::Null),
            "changed": r.get("changed").cloned().unwrap_or(Value::Null),
        })
    });
    json!({
        "trigger_id": trigger.id,
        "source_kind": "job",
        "app": source_job.app,
        "status": source_job.status.as_str(),
        "source_job_id": source_job.id,
        "result_summary": summary,
        "depth": depth,
        "chain": chain,
    })
}

// ── plugin hooks (M15 "WASM everywhere" v1) ──────────────────────────────────

/// Interprets a predicate plugin's output: the contract is `{"pass": bool}`,
/// with a bare `true`/`false` accepted as shorthand. Anything else is a
/// malformed verdict (`None`) and the hook's fail-open default applies.
pub fn predicate_verdict(out: &Value) -> Option<bool> {
    match out {
        Value::Bool(b) => Some(*b),
        Value::Object(m) => m.get("pass").and_then(Value::as_bool),
        _ => None,
    }
}

/// What a failed/malformed predicate falls back to: `"skip"` → don't fire,
/// anything else (including absent) → fire. Fail-OPEN is the default — a
/// broken plugin must never silently wedge a pipeline edge.
pub fn predicate_fail_default(on_error: Option<&str>) -> bool {
    on_error != Some("skip")
}

/// Provenance/identity keys the host owns on a `_trigger` object. A transform
/// plugin may shape everything else, but these are re-stamped from the
/// original after it runs — lineage (`depth`/`chain` cycle guards, delivery
/// idempotency, the fired-runs view) must not be forgeable or losable from
/// inside the sandbox.
const PROVENANCE_KEYS: &[&str] = &[
    "trigger_id",
    "source_kind",
    "source_job_id",
    "event_id",
    "source_id",
    "depth",
    "chain",
];

/// Merges a transform plugin's output over the original `_trigger` object,
/// re-stamping the host-owned provenance keys. Non-object output violates the
/// contract → the original object is kept unchanged.
pub fn restamp_provenance(original: &Value, transformed: Value) -> Value {
    let Value::Object(mut out) = transformed else {
        return original.clone();
    };
    if let Value::Object(orig) = original {
        for key in PROVENANCE_KEYS {
            match orig.get(*key) {
                Some(v) => {
                    out.insert((*key).to_string(), v.clone());
                }
                None => {
                    out.remove(*key);
                }
            }
        }
    }
    Value::Object(out)
}

/// Names a trigger's CONFIGURED hooks point at that the plugin host has not
/// loaded, in hook order (predicate, then transform). Empty when the trigger
/// has no hooks, or when every named module is present.
///
/// The anti-pattern this exists to expose: a configured predicate whose module
/// was never built into `data/plugins/` takes the same fail-open path as a
/// predicate that passed, so a gate nobody deployed is indistinguishable from
/// a gate that said yes. The hop still fires — fail-open is the contract — but
/// the caller can now say so at error level and in the decision ledger.
pub fn missing_hook_plugins(plugins: &dyn pumper_core::Plugins, trigger: &Trigger) -> Vec<String> {
    let Some(hooks) = &trigger.plugin_hooks else {
        return Vec::new();
    };
    [hooks.predicate.as_ref(), hooks.transform.as_ref()]
        .into_iter()
        .flatten()
        .filter(|h| !plugins.has(&h.plugin))
        .map(|h| h.plugin.clone())
        .collect()
}

/// Runs a trigger's plugin hooks over the built `_trigger` object.
/// `None` = the predicate said skip; `Some(obj)` = the (possibly transformed)
/// object to merge into target params. Every failure path is fail-open with a
/// loud log: predicate errors fall back to the hook's `on_error` default
/// (fire), transform errors keep the untransformed object.
pub async fn apply_plugin_hooks(
    plugins: &dyn pumper_core::Plugins,
    trigger: &Trigger,
    obj: Value,
) -> Option<Value> {
    let Some(hooks) = &trigger.plugin_hooks else {
        return Some(obj);
    };
    if let Some(hook) = &hooks.predicate {
        let input = obj.to_string();
        match plugins.run(&hook.plugin, &input, &hook.params).await {
            Ok(out) => match predicate_verdict(&out) {
                Some(true) => {}
                Some(false) => {
                    info!(trigger = %trigger.id, plugin = %hook.plugin,
                          "trigger skipped: predicate plugin returned pass=false");
                    return None;
                }
                None => {
                    let fire = predicate_fail_default(hook.on_error.as_deref());
                    warn!(trigger = %trigger.id, plugin = %hook.plugin,
                          fallback = if fire { "fire" } else { "skip" },
                          "predicate plugin returned a malformed verdict (want {{\"pass\": bool}}): {out}");
                    if !fire {
                        return None;
                    }
                }
            },
            Err(e) => {
                let fire = predicate_fail_default(hook.on_error.as_deref());
                warn!(trigger = %trigger.id, plugin = %hook.plugin,
                      fallback = if fire { "fire" } else { "skip" },
                      "predicate plugin failed (trap/fuel/missing): {e}");
                if !fire {
                    return None;
                }
            }
        }
    }
    let obj = if let Some(hook) = &hooks.transform {
        let input = obj.to_string();
        match plugins.run(&hook.plugin, &input, &hook.params).await {
            Ok(out @ Value::Object(_)) => restamp_provenance(&obj, out),
            Ok(other) => {
                warn!(trigger = %trigger.id, plugin = %hook.plugin,
                      "transform plugin returned non-object output; keeping the original envelope: {other}");
                obj
            }
            Err(e) => {
                warn!(trigger = %trigger.id, plugin = %hook.plugin,
                      "transform plugin failed (trap/fuel/missing); keeping the original envelope: {e}");
                obj
            }
        }
    } else {
        obj
    };
    Some(obj)
}

/// At-most-once-per-source-run dedup key (existing partial unique index).
/// External triggers reuse it with the inbound event id as the source, so a
/// redelivered webhook (same `x-pumper-delivery-id`) fires each trigger once.
/// Terminal-job hops use it verbatim — a job has exactly one final status.
pub fn idempotency_key(trigger_id: &str, source_job_id: &str) -> String {
    format!("trig:{trigger_id}:{source_job_id}")
}

/// Which dataset batch of a source run a hop is firing from. One job run can
/// produce several batches — its own fan-out, plus one per saved-search view it
/// materializes — and they are evaluated against the same source job id, so the
/// scope is what keeps their idempotency keys apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetBatch<'a> {
    /// The source job's own run fan-out (`worker::finalize_fanout`).
    Run,
    /// A saved-search view materialized during the run, by saved-search id.
    View(&'a str),
}

impl DatasetBatch<'_> {
    /// The key segment that separates one batch of a run from another.
    fn key_scope(&self) -> String {
        match self {
            DatasetBatch::Run => String::new(),
            DatasetBatch::View(id) => format!(":view:{id}"),
        }
    }
}

/// Dedup key for ONE dataset hop.
///
/// The anti-pattern this defends: a run that writes several datasets evaluates
/// the same trigger once per dataset, and a key that omits the dataset makes
/// every hop after the first look like a redelivery of the first — so exactly
/// one arbitrary (HashMap-ordered) dataset ever fired. The dataset, and the
/// batch the dataset came from, are both part of the identity of a hop.
///
/// `app` joins them for the same reason, one namespace up: a run's batch now
/// spans every namespace it wrote under, so two apps owning identically-named
/// datasets in one run (`grants/unified` alongside a source app's own
/// `unified`) would otherwise produce the same key and silently drop the second
/// hop as a redelivery of the first.
pub fn dataset_idempotency_key(
    trigger_id: &str,
    source_job_id: &str,
    batch: DatasetBatch<'_>,
    app: &str,
    dataset: &str,
) -> String {
    format!(
        "{}{}:ds:{app}/{dataset}",
        idempotency_key(trigger_id, source_job_id),
        batch.key_scope()
    )
}

// ── external (ingress) matching ──────────────────────────────────────────────

/// Resolves a `$.a.b` JSON path against `payload` (objects only — the `?filter=`
/// grammar has no array indexing, matching the store-level SQL semantics).
fn lookup_path<'a>(payload: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = payload;
    for seg in path.trim_start_matches("$.").split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Scalar-to-text projection mirroring SQLite's `->>` used by the store-level
/// filters: strings stay bare, numbers/bools render, null/objects/arrays don't
/// participate in text comparisons.
fn value_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// True when ONE filter holds against the payload. Semantics mirror the SQL
/// `push_json_filters` mapping: `Eq` exact text, `Contains` case-insensitive
/// substring, `Gte`/`Lte` lexicographic text, `NumGteAny` numeric `>=` on any
/// of its paths (non-numbers never match, like the SQL `json_type` guard).
fn filter_matches(filter: &pumper_core::datasets::JsonFilter, payload: &Value) -> bool {
    use pumper_core::datasets::JsonFilter;
    match filter {
        JsonFilter::Eq { path, value } => {
            lookup_path(payload, path).and_then(value_text).as_deref() == Some(value.as_str())
        }
        JsonFilter::Contains { path, value } => lookup_path(payload, path)
            .and_then(value_text)
            .is_some_and(|t| t.to_lowercase().contains(&value.to_lowercase())),
        JsonFilter::Gte { path, value } => lookup_path(payload, path)
            .and_then(value_text)
            .is_some_and(|t| t.as_str() >= value.as_str()),
        JsonFilter::Lte { path, value } => lookup_path(payload, path)
            .and_then(value_text)
            .is_some_and(|t| t.as_str() <= value.as_str()),
        JsonFilter::NumGteAny { paths, value } => paths.iter().any(|p| {
            lookup_path(payload, p)
                .and_then(Value::as_f64)
                .is_some_and(|n| n >= *value)
        }),
    }
}

/// True when EVERY filter holds (AND, like the `?filter=` surface). An empty
/// set matches everything — an external trigger without predicates is a
/// source-level subscription.
pub fn payload_matches(filters: &[pumper_core::datasets::JsonFilter], payload: &Value) -> bool {
    filters.iter().all(|f| filter_matches(f, payload))
}

/// The `_trigger` object for an inbound-ingress hop. Carries the verified
/// payload itself — inbound events are size-capped at the door
/// (`[ingress] max_body_bytes`), so unlike job results it is safe to inline.
pub fn external_trigger_obj(
    trigger: &Trigger,
    source_id: &str,
    source_name: &str,
    event_id: &str,
    payload: &Value,
    depth: u32,
    chain: &[String],
) -> Value {
    json!({
        "trigger_id": trigger.id,
        "source_kind": "external",
        "source_id": source_id,
        "source_name": source_name,
        "event_id": event_id,
        "payload": payload,
        "depth": depth,
        "chain": chain,
    })
}

// ── the evaluation-set cache ─────────────────────────────────────────────────

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pumper_core::datasets::JsonFilter;
use pumper_core::storage::{NewTriggerRun, TRIGGER_SET_ID};
use pumper_core::{EnqueueOptions, Storage};
use tracing::{debug, error, info, warn};

use crate::state::AppState;

/// Which evaluation set a source event needs. `Dataset`/`Job` are keyed by the
/// source app; `External` by the ingress source id (its set also folds in the
/// `'*'` wildcard triggers, exactly as the SQL does).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalScope<'a> {
    Dataset(&'a str),
    Job(&'a str),
    External(&'a str),
}

impl EvalScope<'_> {
    /// Cache key. The kind is part of it — a dataset trigger and a job trigger
    /// on the same app are different sets, and collapsing them would fire both
    /// kinds on either event.
    fn key(&self) -> (&'static str, String) {
        match self {
            EvalScope::Dataset(app) => ("dataset", (*app).to_string()),
            EvalScope::Job(app) => ("job", (*app).to_string()),
            EvalScope::External(src) => ("external", (*src).to_string()),
        }
    }

    async fn load(&self, storage: &Storage) -> pumper_core::Result<Vec<Trigger>> {
        match self {
            EvalScope::Dataset(app) => storage.enabled_triggers("dataset", app).await,
            EvalScope::Job(app) => storage.enabled_triggers("job", app).await,
            EvalScope::External(src) => storage.enabled_external_triggers(src).await,
        }
    }
}

/// One trigger with everything derivable from it that is FIXED for its
/// lifetime, so the per-event path never re-derives it. Today that is the
/// parsed filter specs: they are validated at create time, immutable
/// afterwards (there is no update endpoint — a change is a delete + create),
/// and re-parsing them per inbound event is pure waste.
pub struct EvalTrigger {
    pub trigger: Trigger,
    /// `Ok` = the parsed predicate set (empty when the trigger has none).
    /// `Err` = the stored specs no longer parse; the fire path skips loudly
    /// rather than firing wide, exactly as the per-event parse used to.
    pub filters: std::result::Result<Vec<JsonFilter>, ()>,
}

impl EvalTrigger {
    fn new(trigger: Trigger) -> Self {
        let filters = match trigger.filters.as_deref() {
            None => Ok(Vec::new()),
            Some(specs) => crate::routes::parse_filters(specs).map_err(|_| ()),
        };
        Self { trigger, filters }
    }
}

/// An evaluation set: the triggers of one scope, prepared once.
pub type EvalSet = Arc<Vec<Arc<EvalTrigger>>>;

struct CacheEntry {
    generation: u64,
    set: EvalSet,
}

/// Generation-stamped cache of prepared evaluation sets.
///
/// The point is the ZERO-trigger case: a fleet with no triggers configured for
/// an app used to pay a `SELECT … FROM triggers` on every single job
/// completion, twice (dataset + terminal), to learn "still nothing". A cached
/// EMPTY set answers that without touching SQLite.
///
/// Coherence rests on [`Storage::trigger_generation`]: a reader samples the
/// generation BEFORE its SELECT and stamps the loaded set with that sample, so
/// a set can never be stamped newer than the data it actually contains. A
/// mutation bumps the generation after committing, so the very next lookup
/// (which samples the bumped value) misses and reloads. A slow loader that
/// finishes after a mutation stamps its result with the OLD generation, so it
/// is never served either — the worst case is a redundant reload, never a
/// stale decision.
#[derive(Default)]
pub struct TriggerEvalCache {
    entries: Mutex<HashMap<(&'static str, String), CacheEntry>>,
    /// How many times the cache actually went to the database. The observable
    /// the "no queries when nothing is configured" guarantee is tested against.
    db_loads: AtomicU64,
}

impl TriggerEvalCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cached set for `generation`, or `None` when the entry is absent or was
    /// stamped by an older generation.
    fn get(&self, key: &(&'static str, String), generation: u64) -> Option<EvalSet> {
        let entries = self.entries.lock().unwrap();
        let entry = entries.get(key)?;
        (entry.generation == generation).then(|| entry.set.clone())
    }

    /// Stores a freshly loaded set. Never lets an older generation overwrite a
    /// newer one (two concurrent loaders straddling a mutation), which would
    /// throw away the good entry without ever serving a stale one.
    fn put(&self, key: (&'static str, String), generation: u64, set: &EvalSet) {
        let mut entries = self.entries.lock().unwrap();
        match entries.get(&key) {
            Some(existing) if existing.generation > generation => {}
            _ => {
                entries.insert(
                    key,
                    CacheEntry {
                        generation,
                        set: set.clone(),
                    },
                );
            }
        }
    }

    /// Number of database loads performed so far — the observable the
    /// "a completion with nothing configured queries nothing" guarantee is
    /// asserted against. Read only by tests today, hence the allow.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn db_loads(&self) -> u64 {
        self.db_loads.load(Ordering::Relaxed)
    }
}

/// The prepared evaluation set for `scope`, from cache when the `triggers`
/// table has not changed since it was cached.
///
/// Sampling the generation before the load — and only before — is the whole
/// correctness argument; see [`TriggerEvalCache`].
pub async fn eval_set(state: &AppState, scope: EvalScope<'_>) -> pumper_core::Result<EvalSet> {
    let generation = state.storage.trigger_generation();
    let key = scope.key();
    if let Some(hit) = state.trigger_cache.get(&key, generation) {
        return Ok(hit);
    }
    state.trigger_cache.db_loads.fetch_add(1, Ordering::Relaxed);
    let set: EvalSet = Arc::new(
        scope
            .load(&state.storage)
            .await?
            .into_iter()
            .map(|t| Arc::new(EvalTrigger::new(t)))
            .collect(),
    );
    state.trigger_cache.put(key, generation, &set);
    Ok(set)
}

// ── worker hooks (IO around the pure helpers) ────────────────────────────────

/// Records ONE decision in the ledger (`trigger_runs`, migration 0036).
///
/// Fail-open by construction: a ledger write that fails is logged loudly and
/// swallowed. The hop it describes has already happened (or already been
/// skipped) — refusing to fire because we could not write the note about firing
/// would turn an observability table into a new failure mode for the pipeline.
async fn record(state: &AppState, run: NewTriggerRun<'_>) {
    if let Err(e) = state.storage.record_trigger_run(&run).await {
        warn!(trigger = %run.trigger_id, outcome = %run.outcome,
              "trigger decision ledger write failed (the decision itself stands): {e}");
    }
}

/// The part of a decision that is fixed for one source event, so each call site
/// spells out only the outcome and its detail.
struct Ctx<'a> {
    source_kind: &'a str,
    source_job_id: Option<String>,
    dataset: Option<&'a str>,
    event_id: Option<&'a str>,
}

impl Ctx<'_> {
    /// A ledger row for `trigger` with this context.
    fn row<'r>(&'r self, trigger_id: &'r str, outcome: &'r str) -> NewTriggerRun<'r> {
        NewTriggerRun {
            trigger_id,
            outcome,
            source_kind: self.source_kind,
            source_job_id: self.source_job_id.as_deref(),
            dataset: self.dataset,
            event_id: self.event_id,
            ..Default::default()
        }
    }
}

/// Reports every configured hook of `trigger` whose plugin is not loaded: one
/// error-level log and one `plugin_missing` ledger row per missing module.
///
/// Deliberately NOT a gate. The hop proceeds into the fail-open path exactly
/// as before — a mis-deployed plugin must not wedge a pipeline edge — but the
/// silence is what made this bug survivable, and the silence is what ends.
async fn report_missing_plugins(state: &AppState, trigger: &Trigger, ctx: &Ctx<'_>) {
    for plugin in missing_hook_plugins(state.plugins.as_ref(), trigger) {
        error!(trigger = %trigger.id, plugin = %plugin,
               "trigger hook names a plugin that is not loaded: the hook did NOTHING \
                (predicate did not gate / transform did not shape) and the hop takes the \
                fail-open path. Build and install it with `just plugins-install`, then \
                POST /plugins/reload");
        record(
            state,
            NewTriggerRun {
                detail: Some(&plugin),
                ..ctx.row(&trigger.id, "plugin_missing")
            },
        )
        .await;
    }
}

/// Fires enabled dataset triggers matching this run's revision batch. One
/// target job per trigger **per dataset** of the batch (idempotency-keyed),
/// carrying that dataset's capped key list in `params._trigger`. Fail-open:
/// evaluation errors are logged and never affect the source job.
///
/// Datasets are walked in sorted order so a multi-dataset run enqueues its hops
/// in a stable sequence rather than in `HashMap` (RandomState) order.
pub async fn fire_dataset_triggers(
    state: &AppState,
    job: &Job,
    batch: DatasetBatch<'_>,
    by_dataset: &HashMap<(&str, &str), Vec<&Revision>>,
) {
    let source_job_id = job.id.to_string();
    let mut fired = 0;
    // Sorted by (app, dataset) so a multi-namespace run enqueues its hops in a
    // stable sequence rather than in `HashMap` (RandomState) order.
    let mut pairs: Vec<(&str, &str)> = by_dataset.keys().copied().collect();
    pairs.sort_unstable();
    // The trigger set is loaded per APP of the batch. A run can write under
    // several namespaces (`ca-grants` -> `grants/unified`, `peer` ->
    // `peer_<origin>/<ds>`); scoping every hop to `job.app` gave those writes
    // zero trigger coverage. `eval_set` is cached per (kind, app), so the extra
    // lookups are bounded by the run's declared namespace count, not per dataset.
    let mut current: Option<(&str, EvalSet)> = None;
    for (app, dataset) in pairs {
        let trigs = match &current {
            Some((cached, set)) if *cached == app => set.clone(),
            _ => match eval_set(state, EvalScope::Dataset(app)).await {
                Ok(t) => {
                    current = Some((app, t.clone()));
                    t
                }
                Err(e) => {
                    // A transient read error here silently drops EVERY edge of
                    // this app, which is exactly what the ledger exists to
                    // surface. Other apps in the batch still get their chance.
                    warn!(job = %job.id, %app, "failed to load dataset triggers: {e}");
                    let detail = e.to_string();
                    record(
                        state,
                        NewTriggerRun {
                            trigger_id: TRIGGER_SET_ID,
                            outcome: "eval_set_error",
                            source_kind: "dataset",
                            source_job_id: Some(&source_job_id),
                            detail: Some(&detail),
                            ..Default::default()
                        },
                    )
                    .await;
                    continue;
                }
            },
        };
        if trigs.is_empty() {
            continue;
        }
        let revs = &by_dataset[&(app, dataset)];
        let ctx = Ctx {
            source_kind: "dataset",
            source_job_id: Some(source_job_id.clone()),
            dataset: Some(dataset),
            event_id: None,
        };
        for trigger in trigs
            .iter()
            .map(|t| &t.trigger)
            .filter(|t| t.covers_dataset(dataset))
        {
            let matching: Vec<&Revision> = revs
                .iter()
                .copied()
                .filter(|r| change_matches(trigger.on_change.as_deref(), &r.change))
                .collect();
            if matching.is_empty() {
                record(state, ctx.row(&trigger.id, "no_change_match")).await;
                continue;
            }
            let (depth, chain) = match decide(&trigger.id, &job.params, &state.config.triggers) {
                FireDecision::Fire { depth, chain } => (depth, chain),
                FireDecision::SkipCycle => {
                    warn!(trigger = %trigger.id, job = %job.id,
                          "trigger skipped: cycle detected in provenance chain");
                    record(state, ctx.row(&trigger.id, "cycle")).await;
                    continue;
                }
                FireDecision::SkipDepth => {
                    warn!(trigger = %trigger.id, job = %job.id,
                          max_depth = state.config.triggers.max_depth,
                          "trigger skipped: max chain depth reached");
                    record(state, ctx.row(&trigger.id, "depth")).await;
                    continue;
                }
            };
            let obj = dataset_trigger_obj(
                trigger,
                job,
                app,
                dataset,
                &matching,
                depth,
                &chain,
                &state.config.triggers,
            );
            let key = dataset_idempotency_key(&trigger.id, &source_job_id, batch, app, dataset);
            fired += enqueue_hop(state, trigger, job, obj, key, &ctx).await;
        }
    }
    if fired > 0 {
        state.notify.notify_one();
    }
}

/// Fires enabled terminal-job triggers matching this job's final status.
pub async fn fire_terminal_triggers(state: &AppState, job: &Job) {
    let source_job_id = job.id.to_string();
    let trigs = match eval_set(state, EvalScope::Job(&job.app)).await {
        Ok(t) if !t.is_empty() => t,
        Ok(_) => return,
        Err(e) => {
            warn!(job = %job.id, "failed to load job triggers: {e}");
            let detail = e.to_string();
            record(
                state,
                NewTriggerRun {
                    trigger_id: TRIGGER_SET_ID,
                    outcome: "eval_set_error",
                    source_kind: "job",
                    source_job_id: Some(&source_job_id),
                    detail: Some(&detail),
                    ..Default::default()
                },
            )
            .await;
            return;
        }
    };
    let ctx = Ctx {
        source_kind: "job",
        source_job_id: Some(source_job_id.clone()),
        dataset: None,
        event_id: None,
    };
    let mut fired = 0;
    for trigger in trigs.iter().map(|t| &t.trigger) {
        if !status_matches(trigger.on_status.as_deref(), job.status.as_str()) {
            record(state, ctx.row(&trigger.id, "status_mismatch")).await;
            continue;
        }
        let (depth, chain) = match decide(&trigger.id, &job.params, &state.config.triggers) {
            FireDecision::Fire { depth, chain } => (depth, chain),
            FireDecision::SkipCycle => {
                warn!(trigger = %trigger.id, job = %job.id,
                      "trigger skipped: cycle detected in provenance chain");
                record(state, ctx.row(&trigger.id, "cycle")).await;
                continue;
            }
            FireDecision::SkipDepth => {
                warn!(trigger = %trigger.id, job = %job.id,
                      max_depth = state.config.triggers.max_depth,
                      "trigger skipped: max chain depth reached");
                record(state, ctx.row(&trigger.id, "depth")).await;
                continue;
            }
        };
        let obj = terminal_trigger_obj(trigger, job, depth, &chain);
        let key = idempotency_key(&trigger.id, &source_job_id);
        fired += enqueue_hop(state, trigger, job, obj, key, &ctx).await;
    }
    if fired > 0 {
        state.notify.notify_one();
    }
}

/// Fires enabled external-kind triggers for one verified inbound event
/// (`POST /ingest/{id}`). Source filter is the trigger's `source_app` (an
/// ingress source id or `'*'`); JSON-path predicate filters are ANDed against
/// the payload. One target job per trigger per event id (idempotency-keyed, so
/// a redelivered webhook can't double-fire). Fail-open like the other hooks:
/// evaluation errors are logged, never surfaced to the sender.
pub async fn fire_external_triggers(
    state: &AppState,
    source_id: &str,
    source_name: &str,
    event_id: &str,
    payload: &Value,
) -> usize {
    let trigs = match eval_set(state, EvalScope::External(source_id)).await {
        Ok(t) if !t.is_empty() => t,
        Ok(_) => return 0,
        Err(e) => {
            warn!(source = %source_id, "failed to load external triggers: {e}");
            let detail = e.to_string();
            record(
                state,
                NewTriggerRun {
                    trigger_id: TRIGGER_SET_ID,
                    outcome: "eval_set_error",
                    source_kind: "external",
                    event_id: Some(event_id),
                    detail: Some(&detail),
                    ..Default::default()
                },
            )
            .await;
            return 0;
        }
    };
    let ctx = Ctx {
        source_kind: "external",
        source_job_id: None,
        dataset: None,
        event_id: Some(event_id),
    };
    let mut fired = 0;
    for entry in trigs.iter() {
        let trigger = &entry.trigger;
        // Predicates were validated at create time and parsed ONCE when this
        // evaluation set was prepared; a spec that no longer parses
        // (defensive) skips the trigger loudly rather than firing wide.
        let Ok(filters) = &entry.filters else {
            warn!(trigger = %trigger.id, "external trigger has unparseable filters; skipped");
            record(state, ctx.row(&trigger.id, "bad_filters")).await;
            continue;
        };
        if !payload_matches(filters, payload) {
            record(state, ctx.row(&trigger.id, "filter_miss")).await;
            continue;
        }
        // Inbound events carry no provenance — every chain starts here.
        let (depth, chain) = match decide(&trigger.id, &Value::Null, &state.config.triggers) {
            FireDecision::Fire { depth, chain } => (depth, chain),
            // Unreachable from a fresh chain, but keep the guards uniform.
            FireDecision::SkipCycle => {
                record(state, ctx.row(&trigger.id, "cycle")).await;
                continue;
            }
            FireDecision::SkipDepth => {
                record(state, ctx.row(&trigger.id, "depth")).await;
                continue;
            }
        };
        let obj = external_trigger_obj(
            trigger,
            source_id,
            source_name,
            event_id,
            payload,
            depth,
            &chain,
        );
        // Plugin predicate/transform hooks (fail-open, see `apply_plugin_hooks`).
        report_missing_plugins(state, trigger, &ctx).await;
        let Some(obj) = apply_plugin_hooks(state.plugins.as_ref(), trigger, obj).await else {
            record(state, ctx.row(&trigger.id, "predicate_veto")).await;
            continue;
        };
        if !state.registry.contains_key(&trigger.target_app) {
            warn!(trigger = %trigger.id, app = %trigger.target_app,
                  "external trigger skipped: target app not registered");
            record(
                state,
                NewTriggerRun {
                    detail: Some(&trigger.target_app),
                    ..ctx.row(&trigger.id, "target_unregistered")
                },
            )
            .await;
            continue;
        }
        let params = merged_params(&trigger.params, obj);
        if !hop_params_pass_target_schema(state, trigger, &params, &ctx).await {
            continue;
        }
        let key = idempotency_key(&trigger.id, event_id);
        let opts = EnqueueOptions {
            params,
            max_attempts: trigger.max_attempts,
            priority: trigger.priority,
            budget_usd: trigger.budget_usd,
            idempotency_key: Some(key.clone()),
            trigger_id: Some(trigger.id.clone()),
            ..Default::default()
        };
        match state.storage.enqueue_dedup(&trigger.target_app, opts).await {
            Ok((hop, true)) => {
                info!(trigger = %trigger.id, event = %event_id, target = %hop.id,
                      app = %trigger.target_app, "external trigger fired");
                let hop_id = hop.id.to_string();
                record(
                    state,
                    NewTriggerRun {
                        job_id: Some(&hop_id),
                        ..ctx.row(&trigger.id, "fired")
                    },
                )
                .await;
                fired += 1;
            }
            Ok((_, false)) => {
                // Already fired for this event id — the redelivery case, which
                // is the point of the key, but it must still be observable.
                debug!(trigger = %trigger.id, event = %event_id, key = %key,
                       "external trigger hop suppressed: a job already exists for this event");
                record(
                    state,
                    NewTriggerRun {
                        detail: Some(&key),
                        ..ctx.row(&trigger.id, "dedup")
                    },
                )
                .await;
            }
            Err(e) => {
                warn!(trigger = %trigger.id, event = %event_id, "external trigger enqueue failed: {e}");
                let detail = e.to_string();
                record(
                    state,
                    NewTriggerRun {
                        detail: Some(&detail),
                        ..ctx.row(&trigger.id, "enqueue_failed")
                    },
                )
                .await;
            }
        }
    }
    if fired > 0 {
        state.notify.notify_one();
    }
    fired
}

/// The hop's params door: the SAME schema check `POST /apps/{name}/jobs`
/// applies, run on the resolved params (the trigger's template with the
/// `_trigger` envelope merged over it) before anything is enqueued.
///
/// A trigger template that cannot satisfy its target's declared schema used to
/// enqueue anyway, so the only trace was a failed job on the target app — the
/// trigger's own `/runs` ledger said `fired`, which is the one thing that was
/// not true. `bad_params` is now a first-class outcome carrying the door's
/// pointer-path message in `detail`, so a broken template is visible where it
/// was authored.
///
/// Returns false (and records the decision) when the hop must not be enqueued.
async fn hop_params_pass_target_schema(
    state: &AppState,
    trigger: &Trigger,
    params: &Value,
    ctx: &Ctx<'_>,
) -> bool {
    match crate::mcp::validate_app_params(&state.registry, &trigger.target_app, params) {
        Ok(()) => true,
        Err(msg) => {
            warn!(trigger = %trigger.id, app = %trigger.target_app,
                  "trigger hop not enqueued: resolved params fail the target app's schema: {msg}");
            record(
                state,
                NewTriggerRun {
                    detail: Some(&msg),
                    ..ctx.row(&trigger.id, "bad_params")
                },
            )
            .await;
            false
        }
    }
}

/// Enqueues one triggered hop under `key` (dedup-guarded). Returns 1 when a job
/// was actually created, 0 when skipped/deduped/failed.
async fn enqueue_hop(
    state: &AppState,
    trigger: &Trigger,
    source: &Job,
    obj: Value,
    key: String,
    ctx: &Ctx<'_>,
) -> usize {
    // Plugin hooks first: a predicate may veto the hop, a transform may shape
    // the `_trigger` envelope. Both fail open (see `apply_plugin_hooks`), and a
    // hook whose plugin was never deployed says so loudly first.
    report_missing_plugins(state, trigger, ctx).await;
    let Some(obj) = apply_plugin_hooks(state.plugins.as_ref(), trigger, obj).await else {
        record(state, ctx.row(&trigger.id, "predicate_veto")).await;
        return 0;
    };
    if !state.registry.contains_key(&trigger.target_app) {
        warn!(trigger = %trigger.id, app = %trigger.target_app,
              "trigger skipped: target app not registered");
        record(
            state,
            NewTriggerRun {
                detail: Some(&trigger.target_app),
                ..ctx.row(&trigger.id, "target_unregistered")
            },
        )
        .await;
        return 0;
    }
    let params = merged_params(&trigger.params, obj);
    if !hop_params_pass_target_schema(state, trigger, &params, ctx).await {
        return 0;
    }
    let opts = EnqueueOptions {
        params,
        max_attempts: trigger.max_attempts,
        priority: trigger.priority,
        budget_usd: trigger.budget_usd,
        idempotency_key: Some(key.clone()),
        trigger_id: Some(trigger.id.clone()),
        // Reverse lineage: `trigger_id` says which trigger fired the hop, this
        // says which run's outcome did. `GET /jobs/{id}/receipt` reads it to
        // list the hops one job caused without scanning the jobs table.
        source_job_id: Some(source.id.to_string()),
        ..Default::default()
    };
    match state.storage.enqueue_dedup(&trigger.target_app, opts).await {
        Ok((hop, true)) => {
            info!(trigger = %trigger.id, source = %source.id, target = %hop.id,
                  app = %trigger.target_app, "trigger fired");
            let hop_id = hop.id.to_string();
            record(
                state,
                NewTriggerRun {
                    job_id: Some(&hop_id),
                    detail: Some(&key),
                    ..ctx.row(&trigger.id, "fired")
                },
            )
            .await;
            1
        }
        Ok((_, false)) => {
            // Not silent: a suppression that is CORRECT (a re-run of the same
            // batch) and one that is a key collision look identical from the
            // outside, so the key that suppressed it is part of the record.
            debug!(trigger = %trigger.id, source = %source.id, key = %key,
                   "trigger hop suppressed: a job already exists for this idempotency key");
            record(
                state,
                NewTriggerRun {
                    detail: Some(&key),
                    ..ctx.row(&trigger.id, "dedup")
                },
            )
            .await;
            0
        }
        Err(e) => {
            warn!(trigger = %trigger.id, source = %source.id, "trigger enqueue failed: {e}");
            let detail = e.to_string();
            record(
                state,
                NewTriggerRun {
                    detail: Some(&detail),
                    ..ctx.row(&trigger.id, "enqueue_failed")
                },
            )
            .await;
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> TriggersConfig {
        TriggersConfig {
            max_depth: 3,
            key_cap: 2,
        }
    }

    #[test]
    fn decide_fires_extends_chain_and_guards_cycles_and_depth() {
        // Fresh source (no provenance): fires at depth 1.
        assert_eq!(
            decide("T1", &json!({}), &cfg()),
            FireDecision::Fire {
                depth: 1,
                chain: vec!["T1".into()]
            }
        );
        // Same trigger already in the chain: cycle skip.
        let looped = json!({ "_trigger": { "depth": 1, "chain": ["T1"] } });
        assert_eq!(decide("T1", &looped, &cfg()), FireDecision::SkipCycle);
        // Different trigger continues the chain.
        assert_eq!(
            decide("T2", &looped, &cfg()),
            FireDecision::Fire {
                depth: 2,
                chain: vec!["T1".into(), "T2".into()]
            }
        );
        // Depth backstop.
        let deep = json!({ "_trigger": { "depth": 3, "chain": ["A", "B", "C"] } });
        assert_eq!(decide("T9", &deep, &cfg()), FireDecision::SkipDepth);
    }

    #[test]
    fn merged_params_injects_trigger_over_template() {
        let template = json!({ "mode": "batch", "_trigger": "stale" });
        let merged = merged_params(&template, json!({ "count": 5 }));
        assert_eq!(merged["mode"], "batch");
        assert_eq!(merged["_trigger"]["count"], 5); // injected wins
                                                    // Non-object template is replaced, not merged into.
        let merged = merged_params(&Value::Null, json!({ "count": 1 }));
        assert_eq!(merged["_trigger"]["count"], 1);
    }

    #[test]
    fn change_and_status_filters() {
        assert!(change_matches(Some("fresh"), "new"));
        assert!(change_matches(Some("fresh"), "changed"));
        assert!(!change_matches(Some("fresh"), "removed"));
        assert!(change_matches(Some("any"), "removed"));
        assert!(change_matches(None, "removed"));
        assert!(!change_matches(Some("new"), "changed"));

        assert!(status_matches(None, "succeeded"));
        assert!(!status_matches(None, "failed"));
        assert!(status_matches(Some("failed"), "failed"));
        assert!(status_matches(Some("any"), "cancelled"));
    }

    #[test]
    fn idempotency_key_is_per_trigger_per_source_run() {
        assert_eq!(idempotency_key("T1", "J1"), "trig:T1:J1");
        assert_ne!(idempotency_key("T1", "J1"), idempotency_key("T1", "J2"));
        assert_ne!(idempotency_key("T1", "J1"), idempotency_key("T2", "J1"));
    }

    #[test]
    fn dataset_hop_key_is_per_dataset_not_per_run() {
        // The bug: two datasets of ONE run collapsing onto one key, so the
        // second dataset's hop is dedup-suppressed as if it were a redelivery.
        let a = dataset_idempotency_key("T1", "J1", DatasetBatch::Run, "src", "grants");
        let b = dataset_idempotency_key("T1", "J1", DatasetBatch::Run, "src", "orgs");
        assert_ne!(a, b, "one key per dataset, not one per run");
        assert_eq!(a, "trig:T1:J1:ds:src/grants");
        // Still per trigger and per source run.
        assert_ne!(
            a,
            dataset_idempotency_key("T2", "J1", DatasetBatch::Run, "src", "grants")
        );
        assert_ne!(
            a,
            dataset_idempotency_key("T1", "J2", DatasetBatch::Run, "src", "grants")
        );
        // …and stable: the same (trigger, run, batch, app, dataset) re-derives.
        assert_eq!(
            a,
            dataset_idempotency_key("T1", "J1", DatasetBatch::Run, "src", "grants")
        );
    }

    /// One run's batch now spans every namespace it wrote under, so two apps
    /// owning an identically-named dataset in the SAME run would collapse onto
    /// one key and the second hop would be dropped as a redelivery of the first.
    #[test]
    fn dataset_hop_key_is_per_app_not_just_per_dataset_name() {
        let own = dataset_idempotency_key("T1", "J1", DatasetBatch::Run, "ca-grants", "unified");
        let virt = dataset_idempotency_key("T1", "J1", DatasetBatch::Run, "grants", "unified");
        assert_ne!(
            own, virt,
            "same dataset name in two namespaces of one run must be two hops"
        );
    }

    #[test]
    fn view_hop_key_does_not_collide_with_the_run_fanout_hop() {
        // A saved-search materialization rides the SOURCE job, so its hops carry
        // the same job id as the run's own fan-out. Same trigger, same app, same
        // dataset, same job — only the batch differs, and that must be enough.
        let run = dataset_idempotency_key("T1", "J1", DatasetBatch::Run, "src", "d");
        let view = dataset_idempotency_key("T1", "J1", DatasetBatch::View("S1"), "src", "d");
        assert_ne!(run, view);
        // Two views materialized by the same run are distinct too.
        let other = dataset_idempotency_key("T1", "J1", DatasetBatch::View("S2"), "src", "d");
        assert_ne!(view, other);
        // And a dataset hop never collides with the terminal-job hop of the
        // same run (they belong to different trigger kinds, but the keyspace
        // must not depend on that).
        assert_ne!(run, idempotency_key("T1", "J1"));
    }

    // ── the evaluation-set cache ─────────────────────────────────────────────

    fn eval_trigger(id: &str, filters: Option<Vec<String>>) -> Arc<EvalTrigger> {
        let mut t = trigger_with_hooks(TriggerPluginHooks {
            predicate: None,
            transform: None,
        });
        t.id = id.into();
        t.plugin_hooks = None;
        t.filters = filters;
        Arc::new(EvalTrigger::new(t))
    }

    fn ids(set: &EvalSet) -> Vec<String> {
        set.iter().map(|t| t.trigger.id.clone()).collect()
    }

    #[test]
    fn cache_serves_within_a_generation_not_across_one() {
        // The anti-pattern: a cache keyed only by scope, so a trigger created
        // after the first lookup never reaches the next firing decision.
        let cache = TriggerEvalCache::new();
        let key = EvalScope::Dataset("grants").key();
        let empty: EvalSet = Arc::new(Vec::new());
        cache.put(key.clone(), 0, &empty);
        assert!(cache.get(&key, 0).is_some(), "same generation hits");
        assert!(
            cache.get(&key, 1).is_none(),
            "a mutation's generation must miss, not serve the pre-mutation set"
        );
        // …and the reload under the new generation is what gets served next.
        let one: EvalSet = Arc::new(vec![eval_trigger("T1", None)]);
        cache.put(key.clone(), 1, &one);
        assert_eq!(ids(&cache.get(&key, 1).expect("hit")), vec!["T1"]);
        assert!(cache.get(&key, 0).is_none(), "the old stamp is gone too");
    }

    #[test]
    fn cache_keys_do_not_collide_across_kinds_or_apps() {
        // A dataset trigger on `grants` and a job trigger on `grants` are
        // different sets; one key for both would fire either kind on either
        // event.
        let cache = TriggerEvalCache::new();
        let ds = EvalScope::Dataset("grants").key();
        let job = EvalScope::Job("grants").key();
        let ext = EvalScope::External("grants").key();
        let other = EvalScope::Dataset("other").key();
        cache.put(ds.clone(), 0, &Arc::new(vec![eval_trigger("DS", None)]));
        assert!(cache.get(&job, 0).is_none());
        assert!(cache.get(&ext, 0).is_none());
        assert!(cache.get(&other, 0).is_none());
        assert_eq!(ids(&cache.get(&ds, 0).expect("hit")), vec!["DS"]);
    }

    #[test]
    fn a_slow_loader_never_overwrites_a_newer_entry() {
        // Loader A samples generation 0, a mutation bumps to 1, loader B stores
        // the fresh set, and only THEN does A finish. A's set is pre-mutation:
        // it must neither be served nor replace B's.
        let cache = TriggerEvalCache::new();
        let key = EvalScope::External("src").key();
        cache.put(key.clone(), 1, &Arc::new(vec![eval_trigger("FRESH", None)]));
        cache.put(key.clone(), 0, &Arc::new(vec![eval_trigger("STALE", None)]));
        assert_eq!(ids(&cache.get(&key, 1).expect("hit")), vec!["FRESH"]);
    }

    #[test]
    fn filters_are_parsed_once_into_the_eval_set_not_per_event() {
        // Parsed at prepare time, so the per-event path only matches.
        let ok = eval_trigger("T1", Some(vec!["$.ref:eq:refs/heads/main".into()]));
        let parsed = ok.filters.as_ref().expect("specs parse");
        assert_eq!(parsed.len(), 1);
        assert!(payload_matches(parsed, &payload()));
        // No specs is an empty predicate set (matches everything), NOT an error.
        let none = eval_trigger("T2", None);
        assert!(none.filters.as_ref().expect("no specs is Ok").is_empty());
        // A spec that no longer parses is remembered as the error it is, so the
        // fire path can skip loudly instead of firing wide.
        let bad = eval_trigger("T3", Some(vec!["not-a-filter-spec".into()]));
        assert!(bad.filters.is_err());
    }

    // ── external (ingress) matching ──────────────────────────────────────────

    use pumper_core::datasets::JsonFilter;

    fn payload() -> Value {
        json!({
            "ref": "refs/heads/main",
            "repository": { "full_name": "acme/docs", "stargazers_count": 42 },
            "forced": false,
        })
    }

    #[test]
    fn payload_eq_matches_nested_paths_and_exact_text() {
        let f = vec![JsonFilter::Eq {
            path: "$.ref".into(),
            value: "refs/heads/main".into(),
        }];
        assert!(payload_matches(&f, &payload()));
        let f = vec![JsonFilter::Eq {
            path: "$.repository.full_name".into(),
            value: "acme/docs".into(),
        }];
        assert!(payload_matches(&f, &payload()));
        // Exact text: a different branch does not match; a missing path never does.
        let f = vec![JsonFilter::Eq {
            path: "$.ref".into(),
            value: "refs/heads/dev".into(),
        }];
        assert!(!payload_matches(&f, &payload()));
        let f = vec![JsonFilter::Eq {
            path: "$.nope.deep".into(),
            value: "x".into(),
        }];
        assert!(!payload_matches(&f, &payload()));
        // Bools and numbers compare via their text projection (SQL ->> parity).
        let f = vec![JsonFilter::Eq {
            path: "$.forced".into(),
            value: "false".into(),
        }];
        assert!(payload_matches(&f, &payload()));
    }

    #[test]
    fn payload_contains_is_case_insensitive_substring() {
        let f = vec![JsonFilter::Contains {
            path: "$.repository.full_name".into(),
            value: "ACME".into(),
        }];
        assert!(payload_matches(&f, &payload()));
        let f = vec![JsonFilter::Contains {
            path: "$.ref".into(),
            value: "release".into(),
        }];
        assert!(!payload_matches(&f, &payload()));
    }

    #[test]
    fn payload_numgte_is_numeric_and_ignores_non_numbers() {
        let f = vec![JsonFilter::NumGteAny {
            paths: vec!["$.repository.stargazers_count".into()],
            value: 40.0,
        }];
        assert!(payload_matches(&f, &payload()));
        let f = vec![JsonFilter::NumGteAny {
            paths: vec!["$.repository.stargazers_count".into()],
            value: 100.0,
        }];
        assert!(!payload_matches(&f, &payload()));
        // A string field never satisfies a numeric filter (json_type guard parity).
        let f = vec![JsonFilter::NumGteAny {
            paths: vec!["$.ref".into()],
            value: 0.0,
        }];
        assert!(!payload_matches(&f, &payload()));
    }

    #[test]
    fn payload_filters_are_anded_and_empty_matches_all() {
        assert!(payload_matches(&[], &payload()));
        let f = vec![
            JsonFilter::Eq {
                path: "$.ref".into(),
                value: "refs/heads/main".into(),
            },
            JsonFilter::NumGteAny {
                paths: vec!["$.repository.stargazers_count".into()],
                value: 100.0, // fails -> whole set fails
            },
        ];
        assert!(!payload_matches(&f, &payload()));
    }

    #[test]
    fn external_trigger_obj_carries_payload_and_provenance() {
        let trigger = pumper_core::Trigger {
            id: "T1".into(),
            name: None,
            source_kind: "external".into(),
            source_app: "src-1".into(),
            source_dataset: None,
            on_change: None,
            on_status: None,
            target_app: "crawl".into(),
            params: json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            enabled: true,
            created_at: chrono::Utc::now(),
            filters: None,
            plugin_hooks: None,
        };
        let obj = external_trigger_obj(
            &trigger,
            "src-1",
            "github",
            "ev-9",
            &payload(),
            1,
            &["T1".into()],
        );
        assert_eq!(obj["source_kind"], "external");
        assert_eq!(obj["source_id"], "src-1");
        assert_eq!(obj["event_id"], "ev-9");
        assert_eq!(obj["payload"]["ref"], "refs/heads/main");
        assert_eq!(obj["depth"], 1);
    }

    // ── plugin hooks (M15) ───────────────────────────────────────────────────

    use pumper_core::{PluginHook, TriggerPluginHooks};

    /// Canned in-memory host standing in for the WASM runtime — the same
    /// stubbing move the plugin app tests use when the .wasm artifact is
    /// absent. Records the envelope each plugin received.
    struct StubPlugins {
        outputs: std::collections::HashMap<String, std::result::Result<Value, String>>,
        calls: std::sync::Mutex<Vec<(String, String, Value)>>,
    }

    impl StubPlugins {
        fn new(outputs: Vec<(&str, std::result::Result<Value, String>)>) -> Self {
            Self {
                outputs: outputs
                    .into_iter()
                    .map(|(n, o)| (n.to_string(), o))
                    .collect(),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl pumper_core::Plugins for StubPlugins {
        async fn run(&self, name: &str, input: &str, params: &Value) -> pumper_core::Result<Value> {
            self.calls
                .lock()
                .unwrap()
                .push((name.to_string(), input.to_string(), params.clone()));
            match self.outputs.get(name) {
                Some(Ok(v)) => Ok(v.clone()),
                Some(Err(e)) => Err(pumper_core::Error::App(e.clone())),
                None => Err(pumper_core::Error::App(format!("unknown plugin '{name}'"))),
            }
        }
        fn list(&self) -> Vec<String> {
            self.outputs.keys().cloned().collect()
        }
        async fn reload(&self) -> pumper_core::Result<usize> {
            Ok(0)
        }
    }

    fn hook(plugin: &str, params: Value, on_error: Option<&str>) -> PluginHook {
        PluginHook {
            plugin: plugin.into(),
            params,
            on_error: on_error.map(String::from),
        }
    }

    fn trigger_with_hooks(hooks: TriggerPluginHooks) -> pumper_core::Trigger {
        pumper_core::Trigger {
            id: "T1".into(),
            name: None,
            source_kind: "dataset".into(),
            source_app: "src".into(),
            source_dataset: Some("*".into()),
            on_change: None,
            on_status: None,
            target_app: "crawl".into(),
            params: json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            enabled: true,
            created_at: chrono::Utc::now(),
            filters: None,
            plugin_hooks: Some(hooks),
        }
    }

    fn delta() -> Value {
        json!({
            "trigger_id": "T1",
            "source_kind": "dataset",
            "source_job_id": "J1",
            "dataset": "d",
            "count": 3,
            "keys": ["k1", "k2"],
            "depth": 1,
            "chain": ["T1"],
        })
    }

    #[test]
    fn predicate_verdict_accepts_pass_object_and_bare_bool_only() {
        assert_eq!(predicate_verdict(&json!({ "pass": true })), Some(true));
        assert_eq!(predicate_verdict(&json!({ "pass": false })), Some(false));
        assert_eq!(predicate_verdict(&json!(true)), Some(true));
        assert_eq!(predicate_verdict(&json!(false)), Some(false));
        // Malformed: wrong type, missing key, non-bool pass.
        assert_eq!(predicate_verdict(&json!({ "pass": "yes" })), None);
        assert_eq!(predicate_verdict(&json!({ "ok": true })), None);
        assert_eq!(predicate_verdict(&json!("fire")), None);
        assert_eq!(predicate_verdict(&json!(1)), None);
    }

    #[test]
    fn predicate_fail_default_is_fail_open_unless_skip() {
        assert!(predicate_fail_default(None)); // absent → fire
        assert!(predicate_fail_default(Some("fire")));
        assert!(predicate_fail_default(Some("bogus"))); // unknown → still open
        assert!(!predicate_fail_default(Some("skip")));
    }

    #[test]
    fn restamp_provenance_pins_host_keys_and_rejects_non_objects() {
        let original = delta();
        // A transform that reshapes payload AND tries to forge lineage.
        let shaped = restamp_provenance(
            &original,
            json!({ "summary": "3 fresh", "depth": 99, "chain": [], "trigger_id": "EVIL", "event_id": "forged" }),
        );
        assert_eq!(shaped["summary"], "3 fresh"); // plugin's shaping kept
        assert_eq!(shaped["depth"], 1); // host keys re-stamped…
        assert_eq!(shaped["chain"], json!(["T1"]));
        assert_eq!(shaped["trigger_id"], "T1");
        assert!(shaped.get("event_id").is_none()); // …and unforgeable when absent
        assert!(
            shaped.get("count").is_none(),
            "non-provenance keys are the plugin's to drop"
        );
        // Contract violation: non-object output keeps the original untouched.
        assert_eq!(restamp_provenance(&original, json!("nope")), original);
        assert_eq!(restamp_provenance(&original, json!([1, 2])), original);
    }

    #[tokio::test]
    async fn hooks_absent_is_a_passthrough() {
        let plugins = StubPlugins::new(vec![]);
        let mut trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: None,
            transform: None,
        });
        trigger.plugin_hooks = None;
        let out = apply_plugin_hooks(&plugins, &trigger, delta()).await;
        assert_eq!(out, Some(delta()));
        assert!(plugins.calls.lock().unwrap().is_empty(), "no plugin runs");
    }

    #[tokio::test]
    async fn predicate_pass_false_skips_and_pass_true_fires() {
        let plugins = StubPlugins::new(vec![("gate", Ok(json!({ "pass": false })))]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({ "min_count": 5 }), None)),
            transform: None,
        });
        assert_eq!(apply_plugin_hooks(&plugins, &trigger, delta()).await, None);
        // The plugin saw the delta envelope as input and its own params.
        let calls = plugins.calls.lock().unwrap();
        assert_eq!(calls[0].0, "gate");
        assert_eq!(
            serde_json::from_str::<Value>(&calls[0].1).unwrap()["count"],
            3
        );
        assert_eq!(calls[0].2, json!({ "min_count": 5 }));
        drop(calls);

        let plugins = StubPlugins::new(vec![("gate", Ok(json!({ "pass": true })))]);
        assert_eq!(
            apply_plugin_hooks(&plugins, &trigger, delta()).await,
            Some(delta())
        );
    }

    #[tokio::test]
    async fn predicate_failure_is_fail_open_by_default_and_skip_when_configured() {
        // Trap/error → default fail-open: the hop still fires, envelope intact.
        let plugins = StubPlugins::new(vec![("gate", Err("fuel exhausted".into()))]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({}), None)),
            transform: None,
        });
        assert_eq!(
            apply_plugin_hooks(&plugins, &trigger, delta()).await,
            Some(delta())
        );
        // Unknown plugin (not loaded) is the same failure class.
        let plugins = StubPlugins::new(vec![]);
        assert_eq!(
            apply_plugin_hooks(&plugins, &trigger, delta()).await,
            Some(delta())
        );
        // Malformed verdict → same fail-open path.
        let plugins = StubPlugins::new(vec![("gate", Ok(json!({ "verdict": "yes" })))]);
        assert_eq!(
            apply_plugin_hooks(&plugins, &trigger, delta()).await,
            Some(delta())
        );
        // on_error: "skip" flips the default.
        let plugins = StubPlugins::new(vec![("gate", Err("trap".into()))]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({}), Some("skip"))),
            transform: None,
        });
        assert_eq!(apply_plugin_hooks(&plugins, &trigger, delta()).await, None);
    }

    #[tokio::test]
    async fn transform_shapes_the_envelope_and_fails_open_to_the_original() {
        let plugins = StubPlugins::new(vec![(
            "slim",
            Ok(json!({ "summary": "3 fresh in d", "depth": 42 })),
        )]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: None,
            transform: Some(hook("slim", json!({}), None)),
        });
        let out = apply_plugin_hooks(&plugins, &trigger, delta())
            .await
            .expect("transform never skips");
        assert_eq!(out["summary"], "3 fresh in d");
        assert_eq!(out["depth"], 1, "provenance re-stamped over the transform");
        assert_eq!(out["trigger_id"], "T1");

        // Error / non-object output → the untransformed envelope, loudly.
        let plugins = StubPlugins::new(vec![("slim", Err("trap".into()))]);
        assert_eq!(
            apply_plugin_hooks(&plugins, &trigger, delta()).await,
            Some(delta())
        );
        let plugins = StubPlugins::new(vec![("slim", Ok(json!([1, 2, 3])))]);
        assert_eq!(
            apply_plugin_hooks(&plugins, &trigger, delta()).await,
            Some(delta())
        );
    }

    #[tokio::test]
    async fn predicate_runs_before_transform_and_veto_short_circuits() {
        let plugins = StubPlugins::new(vec![
            ("gate", Ok(json!({ "pass": false }))),
            ("slim", Ok(json!({ "summary": "never" }))),
        ]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({}), None)),
            transform: Some(hook("slim", json!({}), None)),
        });
        assert_eq!(apply_plugin_hooks(&plugins, &trigger, delta()).await, None);
        let calls = plugins.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "veto must short-circuit the transform");
        assert_eq!(calls[0].0, "gate");
    }
}
