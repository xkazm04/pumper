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

/// The key list a dataset hop carries, and whether `cap` cut it short.
///
/// The anti-pattern this exists to name: `revs.iter().take(cap)` dropped record
/// #(cap+1) and everything after it, and the resulting hop was
/// indistinguishable from a complete one — `count` was exact, `keys` was short,
/// and nothing said which of the two the target should believe. `keys` is a
/// **work list** for `extractor`/`plugin` targets, so a truncation nobody
/// declares is a silent partial run, not a smaller sample.
pub fn capped_keys<'a>(revs: &[&'a Revision], cap: usize) -> (Vec<&'a str>, bool) {
    let keys: Vec<&str> = revs.iter().take(cap).map(|r| r.key.as_str()).collect();
    let truncated = revs.len() > keys.len();
    (keys, truncated)
}

/// The `_trigger` object for a dataset-change hop. `count` stays exact; the key
/// list is capped at `cfg.key_cap`, with `keys_truncated` declaring whether the
/// cap bit — targets fetch full data by key.
///
/// `keys` is the target's **work scope**, not a sample of one: `crates/apps/
/// extractor` and `crates/apps/plugin` both read `_trigger.keys` as the record
/// list to process. That is why the truncation is stated rather than left to be
/// inferred by comparing `keys.len()` against `count`, and why `keys` /
/// `keys_truncated` are host-owned (see [`HOST_OWNED_KEYS`]).
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
    let (keys, keys_truncated) = capped_keys(revs, cfg.key_cap);
    json!({
        "trigger_id": trigger.id,
        "source_kind": "dataset",
        "app": app,
        "dataset": dataset,
        "kind": trigger.on_change.as_deref().unwrap_or("any"),
        "count": revs.len(),
        "keys": keys,
        // Always present, so "did I get the whole delta?" is a field the target
        // reads rather than an arithmetic guess it has to make.
        "keys_truncated": keys_truncated,
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

/// Keys the HOST owns on a `_trigger` object. A transform plugin may shape
/// everything else, but these are re-stamped from the original after it runs.
///
/// Two classes, and the second one was the gap:
///
/// - **Lineage** — `trigger_id`, `source_kind`, `source_job_id`, `event_id`,
///   `source_id`, `depth`, `chain`. The `depth`/`chain` cycle guards, delivery
///   idempotency and the fired-runs view must not be forgeable or losable from
///   inside the sandbox.
/// - **Work scope** — `keys`, `keys_truncated`. The transform's output *is* the
///   target job's `params._trigger`, and `crates/apps/extractor` and
///   `crates/apps/plugin` both read `_trigger.keys` as the list of records to
///   process. A transform could therefore rescope the target's WORK, not just
///   its payload, in both directions: `delta-slim`'s `max_keys` (default 10,
///   against a host `key_cap` of 200) turned a 200-key hop into a 10-record
///   extract, and its `keep` mode — the configuration
///   docs/features/trigger-plugins.md pairs with `"target_app": "extractor"` —
///   dropped `keys` entirely, which makes the extractor's `.or_else(…)` yield
///   `None` and fall through to "every live record, up to `SOURCE_LIST_LIMIT`
///   (10,000)". A 3-record incremental extract became a full sweep.
///
/// Shrinking what a WEBHOOK carries is legitimate; shrinking what a JOB does is
/// not. The throttle for the latter is `[triggers] key_cap`, which is the
/// host's knob and stays the host's.
const HOST_OWNED_KEYS: &[&str] = &[
    // lineage
    "trigger_id",
    "source_kind",
    "source_job_id",
    "event_id",
    "source_id",
    "depth",
    "chain",
    // work scope
    "keys",
    "keys_truncated",
];

/// The work-scope subset of [`HOST_OWNED_KEYS`] — the keys whose DISAPPEARANCE
/// is itself a rescoping, because absent `keys` is not "no opinion", it is the
/// extractor's "sweep every live record" instruction.
const WORK_SCOPE_KEYS: &[&str] = &["keys", "keys_truncated"];

/// Which host-owned keys a transform's output would have changed, had the host
/// not re-stamped them.
///
/// Extracted so the re-stamp is not silent: a plugin author who believes
/// `max_keys` throttles a hop, and an operator reading logs, both deserve to
/// find out the sandbox's proposal was overruled rather than diffing two JSON
/// blobs to notice.
///
/// Dropping a LINEAGE key is not an override — shedding provenance is the
/// normal shape of a keep-list transform, and the host simply puts it back
/// (that is the documented contract). Dropping a WORK-SCOPE key is, because
/// absence changes what the target does.
pub fn host_owned_overrides(original: &Value, transformed: &Value) -> Vec<&'static str> {
    let (Value::Object(orig), Value::Object(out)) = (original, transformed) else {
        return Vec::new();
    };
    HOST_OWNED_KEYS
        .iter()
        .copied()
        .filter(|k| match (orig.get(*k), out.get(*k)) {
            // Proposed a different value for a key it does not own.
            (Some(a), Some(b)) => a != b,
            // Dropped it: only a rescoping when the key IS the scope.
            (Some(_), None) => WORK_SCOPE_KEYS.contains(k),
            // Conjured one the original never had.
            (None, Some(_)) => true,
            (None, None) => false,
        })
        .collect()
}

/// Merges a transform plugin's output over the original `_trigger` object,
/// re-stamping every [`HOST_OWNED_KEYS`] entry. Non-object output violates the
/// contract → the original object is kept unchanged.
pub fn restamp_host_owned(original: &Value, transformed: Value) -> Value {
    let Value::Object(mut out) = transformed else {
        return original.clone();
    };
    if let Value::Object(orig) = original {
        for key in HOST_OWNED_KEYS {
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

/// Names the plugins a trigger's CONFIGURED hooks point at that the host cannot
/// **execute**, in hook order (predicate, then transform). Empty when the
/// trigger has no hooks, or when every named module is usable.
///
/// "Cannot execute" is [`Plugins::has`], which answers for executability rather
/// than mere presence: a module that loaded but exports no `extract`/
/// `extract_v2` ABI (a describe-only dynamic-app module, say) is as useless to a
/// hook as one that was never installed, and used to answer `has() == true`.
///
/// The anti-pattern this exists to expose: a configured predicate whose module
/// was never built into `data/plugins/` takes the same fail-open path as a
/// predicate that passed, so a gate nobody deployed is indistinguishable from
/// a gate that said yes. The hop still fires — fail-open is the contract — but
/// the caller can now say so at error level and in the decision ledger.
///
/// [`Plugins::has`]: pumper_core::Plugins::has
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

/// Which hook slot something happened in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookSlot {
    Predicate,
    Transform,
}

impl HookSlot {
    pub fn as_str(self) -> &'static str {
        match self {
            HookSlot::Predicate => "predicate",
            HookSlot::Transform => "transform",
        }
    }
}

/// One ledger-worthy thing that happened while running a trigger's hooks.
///
/// [`apply_plugin_hooks`] is deliberately **pure**: it has no storage handle,
/// takes no `AppState`, and stays unit-testable against a stub host. It
/// therefore cannot write rows — it returns them, and the caller (which owns the
/// ledger context) records them. That is the same extracted-function shape the
/// rest of this module uses, and it is why every hook failure class could be
/// made visible without threading a database through a decision function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookIncident {
    pub slot: HookSlot,
    pub plugin: String,
    /// An allowlisted `trigger_runs.outcome` (see
    /// [`pumper_core::storage::TRIGGER_OUTCOMES`]).
    pub outcome: &'static str,
    /// The row's `detail`: names the slot, the plugin and what happened.
    pub detail: String,
}

/// What a trigger's plugin hooks decided, plus the rows the caller must record.
#[derive(Debug, Clone, PartialEq)]
pub struct HookVerdict {
    /// The (possibly transformed) `_trigger` object, or `None` when the hop was
    /// stopped (a predicate veto, or a predicate failure under `on_error: skip`).
    pub obj: Option<Value>,
    /// Every ledger-worthy incident, in hook order.
    pub incidents: Vec<HookIncident>,
}

impl HookVerdict {
    /// No hooks, or hooks that all did their job silently.
    fn clean(obj: Value) -> Self {
        Self {
            obj: Some(obj),
            incidents: Vec::new(),
        }
    }

    /// The reason the hop was stopped, for a dry-run to echo — the detail of the
    /// last incident, which is always the stopping one on a `None` verdict.
    pub fn stop_reason(&self) -> Option<&str> {
        self.obj
            .is_none()
            .then(|| self.incidents.last().map(|i| i.detail.as_str()))
            .flatten()
    }
}

/// The ledger outcome a failed plugin call deserves, read from the host's TYPED
/// failure class rather than from its message.
///
/// The anti-pattern this closes: all four of these classes used to end in the
/// same place — a `warn!` and nothing else — so "my trigger fired without being
/// gated" was unanswerable from the ledger that exists to answer exactly that.
/// Worse, under `on_error: skip` a crashed predicate was recorded as
/// `predicate_veto`, i.e. as a healthy gate decision for a sandbox that
/// crashed.
pub fn hook_failure_outcome(e: &pumper_core::Error) -> &'static str {
    use pumper_core::error::PluginFailure as F;
    match e.plugin_failure() {
        // The sandbox stopped it: explicit trap, fuel exhaustion, memory cap.
        Some(F::Trap) => "hook_trap",
        // It returned, but not the contract.
        Some(F::MalformedOutput) => "hook_malformed",
        // Loaded, but it is not an executable plugin (no extract ABI).
        Some(F::MissingExport) => "hook_not_executable",
        // Not loaded at all, or the whole subsystem is off — both mean the hook
        // did nothing and the hop was never gated.
        Some(F::Unknown) | Some(F::Disabled) => "plugin_missing",
        // The host broke around the call, or an error arrived from somewhere
        // that is not the plugin host at all. Either way it is our bug, not the
        // plugin's, and it must not read as one of the classes above.
        Some(F::Host) | None => "hook_host_error",
    }
}

/// Runs a trigger's plugin hooks over the built `_trigger` object.
///
/// Fail-open is unchanged: a predicate that traps, burns its fuel, is missing,
/// or answers nonsense still lets the hop fire unless `on_error: "skip"` says
/// otherwise, and a failing transform keeps the original envelope. What changed
/// is that every one of those paths now leaves a truthful [`HookIncident`] for
/// the caller to record, instead of only a `warn!` line.
pub async fn apply_plugin_hooks(
    plugins: &dyn pumper_core::Plugins,
    trigger: &Trigger,
    obj: Value,
) -> HookVerdict {
    let Some(hooks) = &trigger.plugin_hooks else {
        return HookVerdict::clean(obj);
    };
    let mut incidents: Vec<HookIncident> = Vec::new();

    if let Some(hook) = &hooks.predicate {
        let input = obj.to_string();
        // How a FAILED predicate resolves. Errors and malformed verdicts share
        // this: both mean "the gate did not answer", and `on_error` is the one
        // knob that decides what an unanswered gate means.
        let fire = predicate_fail_default(hook.on_error.as_deref());
        let outcome_and_detail = match plugins.run(&hook.plugin, &input, &hook.params).await {
            Ok(out) => match predicate_verdict(&out) {
                Some(true) => None,
                Some(false) => {
                    info!(trigger = %trigger.id, plugin = %hook.plugin,
                          "trigger skipped: predicate plugin returned pass=false");
                    incidents.push(HookIncident {
                        slot: HookSlot::Predicate,
                        plugin: hook.plugin.clone(),
                        outcome: "predicate_veto",
                        detail: format!("predicate plugin '{}' returned pass=false", hook.plugin),
                    });
                    return HookVerdict {
                        obj: None,
                        incidents,
                    };
                }
                None => Some((
                    "hook_malformed",
                    format!(
                        "predicate plugin '{}' returned a malformed verdict \
                         (want {{\"pass\": bool}}): {out}",
                        hook.plugin
                    ),
                )),
            },
            Err(e) => Some((hook_failure_outcome(&e), format!("{e}"))),
        };
        if let Some((outcome, why)) = outcome_and_detail {
            warn!(trigger = %trigger.id, plugin = %hook.plugin, %outcome,
                  fallback = if fire { "fire" } else { "skip" },
                  "predicate hook did not answer: {why}");
            // The stopped case keeps the FAILURE's own outcome rather than
            // borrowing `predicate_veto`. A sandbox that crashed and a gate that
            // said no are different facts, and an operator counting vetoes must
            // not be shown a crash as a healthy decision. The consequence lives
            // in `detail`, so one row carries both halves.
            let detail = if fire {
                format!("{why} — on_error=fire, hop NOT gated")
            } else {
                format!("{why} — on_error=skip, hop stopped")
            };
            incidents.push(HookIncident {
                slot: HookSlot::Predicate,
                plugin: hook.plugin.clone(),
                outcome,
                detail,
            });
            if !fire {
                return HookVerdict {
                    obj: None,
                    incidents,
                };
            }
        }
    }

    let obj = if let Some(hook) = &hooks.transform {
        let input = obj.to_string();
        match plugins.run(&hook.plugin, &input, &hook.params).await {
            Ok(out @ Value::Object(_)) => {
                let overruled = host_owned_overrides(&obj, &out);
                if !overruled.is_empty() {
                    warn!(trigger = %trigger.id, plugin = %hook.plugin, keys = ?overruled,
                          "transform plugin proposed different values for host-owned keys; \
                           re-stamped from the original — lineage and the target's work \
                           scope (`keys`) are not the sandbox's to change (the key throttle \
                           is [triggers] key_cap)");
                }
                restamp_host_owned(&obj, out)
            }
            Ok(other) => {
                warn!(trigger = %trigger.id, plugin = %hook.plugin,
                      "transform plugin returned non-object output; keeping the original envelope: {other}");
                incidents.push(HookIncident {
                    slot: HookSlot::Transform,
                    plugin: hook.plugin.clone(),
                    outcome: "hook_malformed",
                    // Same consequence phrasing as the transform error path
                    // below: both keep the original envelope, and a ledger that
                    // words one failure differently from another is a ledger an
                    // operator has to read twice.
                    detail: format!(
                        "transform plugin '{}' returned non-object output: {other} \
                         — original envelope kept, hop NOT shaped",
                        hook.plugin
                    ),
                });
                obj
            }
            Err(e) => {
                let outcome = hook_failure_outcome(&e);
                warn!(trigger = %trigger.id, plugin = %hook.plugin, %outcome,
                      "transform hook failed; keeping the original envelope: {e}");
                incidents.push(HookIncident {
                    slot: HookSlot::Transform,
                    plugin: hook.plugin.clone(),
                    outcome,
                    detail: format!("{e} — original envelope kept, hop NOT shaped"),
                });
                obj
            }
        }
    } else {
        obj
    };
    HookVerdict {
        obj: Some(obj),
        incidents,
    }
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

/// Logs, at error level, every configured hook of `trigger` whose plugin the
/// host cannot execute.
///
/// Deliberately NOT a gate. The hop proceeds into the fail-open path exactly
/// as before — a mis-deployed plugin must not wedge a pipeline edge — but the
/// silence is what made this bug survivable, and the silence is what ends.
///
/// The matching LEDGER row is not written here: it comes from the hook's own
/// [`HookIncident`] (the call fails with `unknown_plugin` /
/// `missing_export`, which [`hook_failure_outcome`] classifies), so a missing
/// plugin produces exactly one row, from the same place every other hook
/// failure does. This function is the loud log that names the fix.
fn report_missing_plugins(state: &AppState, trigger: &Trigger) {
    for plugin in missing_hook_plugins(state.plugins.as_ref(), trigger) {
        error!(trigger = %trigger.id, plugin = %plugin,
               "trigger hook names a plugin this host cannot execute (not installed, or \
                installed without the extract ABI): the hook does NOTHING — the predicate \
                does not gate, the transform does not shape — and the hop takes the \
                fail-open path. Build and install it with `just plugins-install`, then \
                POST /plugins/reload");
    }
}

/// Outcomes that describe the DEPLOYMENT rather than the event being evaluated.
///
/// A typo'd plugin name is not news on the ten-thousandth event; it is the same
/// fact it was on the first. Recording it per event buried the per-event
/// decisions the ledger exists for under identical rows — the known gap
/// `docs/features/trigger-plugins.md` used to carry.
fn is_static_hook_fact(outcome: &str) -> bool {
    matches!(outcome, "plugin_missing" | "hook_not_executable")
}

/// Records the ledger rows one hook evaluation produced.
///
/// Everything is written, EXCEPT [`is_static_hook_fact`] outcomes: those are
/// written once per `(trigger, plugin, outcome)` and then suppressed until
/// `POST /plugins/reload` clears the set — reloading being the only thing that
/// can change the answer, and therefore exactly the state change that re-arms
/// the report.
async fn record_hook_incidents(
    state: &AppState,
    trigger: &Trigger,
    ctx: &Ctx<'_>,
    incidents: &[HookIncident],
) {
    for inc in incidents {
        if is_static_hook_fact(inc.outcome) {
            let key = format!("{}|{}|{}", trigger.id, inc.plugin, inc.outcome);
            // Guard dropped before the await: the set is a fast membership
            // check, never held across IO.
            let first_sighting = {
                let mut seen = state.plugin_missing_reported.lock().await;
                seen.insert(key)
            };
            if !first_sighting {
                debug!(trigger = %trigger.id, plugin = %inc.plugin, outcome = %inc.outcome,
                       "hook deployment fault already in the ledger for this trigger; \
                        not writing an identical row per event (POST /plugins/reload re-arms it)");
                continue;
            }
        }
        record(
            state,
            NewTriggerRun {
                detail: Some(&inc.detail),
                ..ctx.row(&trigger.id, inc.outcome)
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
            // A truncated work list is a partial run, so it is said out loud as
            // well as declared in the envelope. `keys` targets (extractor,
            // plugin) process exactly what they are handed: the records past
            // the cap are simply not in this hop.
            if obj["keys_truncated"] == Value::Bool(true) {
                warn!(trigger = %trigger.id, job = %job.id, %app, %dataset,
                      count = matching.len(), key_cap = state.config.triggers.key_cap,
                      "dataset hop key list TRUNCATED: the target gets the first \
                       key_cap keys and `_trigger.keys_truncated: true`; records \
                       beyond the cap are not in this hop's work list");
            }
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
        report_missing_plugins(state, trigger);
        let verdict = apply_plugin_hooks(state.plugins.as_ref(), trigger, obj).await;
        record_hook_incidents(state, trigger, &ctx, &verdict.incidents).await;
        let Some(obj) = verdict.obj else {
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
    // hook whose plugin was never deployed says so loudly first. Every hook
    // incident — veto, trap, malformed output, missing module — lands in the
    // ledger before the hop's own decision does.
    report_missing_plugins(state, trigger);
    let verdict = apply_plugin_hooks(state.plugins.as_ref(), trigger, obj).await;
    record_hook_incidents(state, trigger, ctx, &verdict.incidents).await;
    let Some(obj) = verdict.obj else {
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

    use pumper_core::error::PluginFailure;
    use pumper_core::{PluginHook, TriggerPluginHooks};

    /// Canned in-memory host standing in for the WASM runtime — the same
    /// stubbing move the plugin app tests use when the .wasm artifact is
    /// absent. Records the envelope each plugin received.
    ///
    /// Failures are declared as a [`PluginFailure`] rather than as a message,
    /// because the class is what the ledger now reads: a stub that could only
    /// produce prose could not exercise the classification at all.
    struct StubPlugins {
        outputs: std::collections::HashMap<String, std::result::Result<Value, PluginFailure>>,
        calls: std::sync::Mutex<Vec<(String, String, Value)>>,
    }

    impl StubPlugins {
        fn new(outputs: Vec<(&str, std::result::Result<Value, PluginFailure>)>) -> Self {
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
                Some(Err(kind)) => Err(pumper_core::Error::plugin(*kind, name, "stub failure")),
                None => Err(pumper_core::Error::plugin(
                    PluginFailure::Unknown,
                    name,
                    "not loaded",
                )),
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
    fn restamp_host_owned_pins_host_keys_and_rejects_non_objects() {
        let original = delta();
        // A transform that reshapes payload AND tries to forge lineage.
        let shaped = restamp_host_owned(
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
            "keys the host does not own are the plugin's to drop"
        );
        // Contract violation: non-object output keeps the original untouched.
        assert_eq!(restamp_host_owned(&original, json!("nope")), original);
        assert_eq!(restamp_host_owned(&original, json!([1, 2])), original);
    }

    /// The anti-pattern: `_trigger` IS the target job's params, and `extractor`
    /// / `plugin` read `_trigger.keys` as their WORK LIST. A transform that
    /// shrank it rescoped the target's work; one that dropped it sent the
    /// extractor down its "no keys → every live record, up to 10,000" path.
    /// Both were reachable from the shipped `delta-slim` with the configuration
    /// the docs demonstrate.
    #[test]
    fn a_transform_cannot_narrow_or_delete_the_targets_work_scope() {
        let original = delta(); // keys: ["k1", "k2"]

        // 1. Narrowing (delta-slim `max_keys`).
        let shaped = restamp_host_owned(&original, json!({ "keys": ["k1"], "slimmed": true }));
        assert_eq!(
            shaped["keys"],
            json!(["k1", "k2"]),
            "a sandbox-proposed shorter work list is overruled by the host's"
        );
        assert_eq!(shaped["slimmed"], true, "payload shaping still lands");

        // 2. Deleting (delta-slim `keep`, the documented extractor pairing).
        let shaped = restamp_host_owned(&original, json!({ "dataset": "d", "count": 3 }));
        assert_eq!(
            shaped["keys"],
            json!(["k1", "k2"]),
            "a dropped work list comes back: its absence means `sweep everything`"
        );

        // 3. And the partial-delta flag cannot be cleared from inside either.
        let capped = json!({ "keys": ["k1"], "keys_truncated": true, "count": 9 });
        let shaped = restamp_host_owned(&capped, json!({ "keys_truncated": false }));
        assert_eq!(shaped["keys_truncated"], true);
    }

    /// The re-stamp must not be silent — a plugin author and an operator both
    /// need to learn the sandbox's proposal was overruled.
    #[test]
    fn host_owned_overrides_names_what_was_overruled_and_stays_quiet_otherwise() {
        assert!(
            WORK_SCOPE_KEYS.iter().all(|k| HOST_OWNED_KEYS.contains(k)),
            "the work-scope keys must be a subset of the host-owned ones, or the \
             host restamps something it never claimed"
        );
        let original = delta();
        assert_eq!(
            host_owned_overrides(&original, &json!({ "keys": ["k1"], "depth": 99 })),
            vec!["depth", "keys"],
            "in HOST_OWNED_KEYS order, and shedding the other lineage keys is not \
             an override — that is what a keep-list transform normally does"
        );
        // Dropping the work list IS one, though: absence is an instruction.
        assert_eq!(
            host_owned_overrides(&original, &json!({ "dataset": "d", "count": 3 })),
            vec!["keys"]
        );
        // A shaping that leaves every host key exactly as it found it is silent.
        let faithful = json!({
            "trigger_id": "T1", "source_kind": "dataset", "source_job_id": "J1",
            "depth": 1, "chain": ["T1"], "keys": ["k1", "k2"], "summary": "3 fresh",
        });
        assert!(host_owned_overrides(&original, &faithful).is_empty());
        // …and so is a plugin that adds its own field while leaving the work
        // list alone — the shape a transform SHOULD have.
        assert!(host_owned_overrides(
            &original,
            &json!({ "summary": "3 fresh", "keys": ["k1", "k2"] })
        )
        .is_empty());
        // Conjuring a host key the original never had is not silent either
        // (`keys` rides along here because this output also drops it).
        assert_eq!(
            host_owned_overrides(&original, &json!({ "event_id": "forged" })),
            vec!["event_id", "keys"]
        );
        // Non-objects have nothing to compare.
        assert!(host_owned_overrides(&original, &json!("nope")).is_empty());
    }

    /// The pre-existing, plugin-free half of the same bug: `take(key_cap)`
    /// dropped record #(cap+1) onward and the hop still looked complete.
    #[test]
    fn a_capped_key_list_is_flagged_not_passed_off_as_the_whole_delta() {
        let revs = [rev("a"), rev("b"), rev("c")];
        let borrowed: Vec<&Revision> = revs.iter().collect();

        let (keys, truncated) = capped_keys(&borrowed, 2);
        assert_eq!(keys, vec!["a", "b"]);
        assert!(truncated, "3 revisions do not fit in a cap of 2");

        // Exactly at the cap is NOT truncated — an off-by-one here would cry
        // wolf on every full-cap hop.
        let (keys, truncated) = capped_keys(&borrowed, 3);
        assert_eq!(keys, vec!["a", "b", "c"]);
        assert!(!truncated);
        let (_, truncated) = capped_keys(&borrowed, 99);
        assert!(!truncated);

        // A cap of 0 hands over no work list at all, and says so — it must not
        // read as "no keys", which is the extractor's sweep-everything path.
        let (keys, truncated) = capped_keys(&borrowed, 0);
        assert!(keys.is_empty());
        assert!(truncated);
    }

    fn rev(key: &str) -> Revision {
        Revision {
            app: "src".into(),
            dataset: "d".into(),
            key: key.into(),
            revision: 1,
            change: "new".into(),
            data: None,
            diff: None,
            created_at: chrono::Utc::now(),
            trust: "stable".into(),
            provenance: pumper_core::datasets::Provenance::default(),
        }
    }

    /// The object half of a verdict — what `apply_plugin_hooks` used to return
    /// before it also had to report WHY. The incident half has its own tests.
    async fn hook_obj(
        plugins: &dyn pumper_core::Plugins,
        trigger: &pumper_core::Trigger,
        obj: Value,
    ) -> Option<Value> {
        apply_plugin_hooks(plugins, trigger, obj).await.obj
    }

    #[tokio::test]
    async fn hooks_absent_is_a_passthrough() {
        let plugins = StubPlugins::new(vec![]);
        let mut trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: None,
            transform: None,
        });
        trigger.plugin_hooks = None;
        let verdict = apply_plugin_hooks(&plugins, &trigger, delta()).await;
        assert_eq!(verdict.obj, Some(delta()));
        assert!(
            verdict.incidents.is_empty(),
            "a trigger with no hooks has nothing to report"
        );
        assert!(plugins.calls.lock().unwrap().is_empty(), "no plugin runs");
    }

    #[tokio::test]
    async fn predicate_pass_false_skips_and_pass_true_fires() {
        let plugins = StubPlugins::new(vec![("gate", Ok(json!({ "pass": false })))]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({ "min_count": 5 }), None)),
            transform: None,
        });
        assert_eq!(hook_obj(&plugins, &trigger, delta()).await, None);
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
        assert_eq!(hook_obj(&plugins, &trigger, delta()).await, Some(delta()));
    }

    #[tokio::test]
    async fn predicate_failure_is_fail_open_by_default_and_skip_when_configured() {
        // Trap/error → default fail-open: the hop still fires, envelope intact.
        let plugins = StubPlugins::new(vec![("gate", Err(PluginFailure::Trap))]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({}), None)),
            transform: None,
        });
        assert_eq!(hook_obj(&plugins, &trigger, delta()).await, Some(delta()));
        // Unknown plugin (not loaded) is the same failure class.
        let plugins = StubPlugins::new(vec![]);
        assert_eq!(hook_obj(&plugins, &trigger, delta()).await, Some(delta()));
        // Malformed verdict → same fail-open path.
        let plugins = StubPlugins::new(vec![("gate", Ok(json!({ "verdict": "yes" })))]);
        assert_eq!(hook_obj(&plugins, &trigger, delta()).await, Some(delta()));
        // on_error: "skip" flips the default.
        let plugins = StubPlugins::new(vec![("gate", Err(PluginFailure::Trap))]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({}), Some("skip"))),
            transform: None,
        });
        assert_eq!(hook_obj(&plugins, &trigger, delta()).await, None);
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
        let out = hook_obj(&plugins, &trigger, delta())
            .await
            .expect("transform never skips");
        assert_eq!(out["summary"], "3 fresh in d");
        assert_eq!(out["depth"], 1, "provenance re-stamped over the transform");
        assert_eq!(out["trigger_id"], "T1");

        // Error / non-object output → the untransformed envelope, loudly.
        let plugins = StubPlugins::new(vec![("slim", Err(PluginFailure::Trap))]);
        assert_eq!(hook_obj(&plugins, &trigger, delta()).await, Some(delta()));
        let plugins = StubPlugins::new(vec![("slim", Ok(json!([1, 2, 3])))]);
        assert_eq!(hook_obj(&plugins, &trigger, delta()).await, Some(delta()));
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
        assert_eq!(hook_obj(&plugins, &trigger, delta()).await, None);
        let calls = plugins.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "veto must short-circuit the transform");
        assert_eq!(calls[0].0, "gate");
    }

    // ── hook incidents: what the ledger is told ─────────────────────────────

    /// THE conflation this closes: a predicate that CRASHED under
    /// `on_error: skip` was recorded as `predicate_veto` — the same word a
    /// healthy gate saying "no" uses. An operator reading the ledger saw a
    /// clean gate decision for a sandbox that had blown up.
    #[tokio::test]
    async fn a_crashed_predicate_is_not_recorded_as_a_veto() {
        let plugins = StubPlugins::new(vec![("gate", Err(PluginFailure::Trap))]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({}), Some("skip"))),
            transform: None,
        });
        let verdict = apply_plugin_hooks(&plugins, &trigger, delta()).await;
        assert_eq!(verdict.obj, None, "on_error=skip still stops the hop");
        assert_eq!(verdict.incidents.len(), 1);
        let inc = &verdict.incidents[0];
        assert_eq!(inc.outcome, "hook_trap");
        assert_ne!(
            inc.outcome, "predicate_veto",
            "a crashed sandbox must never read as a gate decision"
        );
        assert_eq!(inc.slot, HookSlot::Predicate);
        assert_eq!(inc.plugin, "gate");
        assert!(
            inc.detail.contains("on_error=skip") && inc.detail.contains("stopped"),
            "the row must say the hop was stopped: {}",
            inc.detail
        );
        assert_eq!(
            verdict.stop_reason(),
            Some(inc.detail.as_str()),
            "a dry-run echoes the real reason, not a fabricated one"
        );

        // …and the genuine article still uses the word it earned.
        let plugins = StubPlugins::new(vec![("gate", Ok(json!({ "pass": false })))]);
        let verdict = apply_plugin_hooks(&plugins, &trigger, delta()).await;
        assert_eq!(verdict.obj, None);
        assert_eq!(verdict.incidents[0].outcome, "predicate_veto");
        assert!(verdict.incidents[0].detail.contains("pass=false"));
    }

    /// The four sandbox failure classes the ledger could not represent at all:
    /// a hop fired ungated and the only trace was a `warn!`. Each now leaves a
    /// distinct, truthful row — and the hop still fires, because fail-open is
    /// the contract and this is honesty, not a behaviour change.
    #[tokio::test]
    async fn every_hook_failure_class_leaves_its_own_row_and_still_fires() {
        for (kind, expected) in [
            (PluginFailure::Trap, "hook_trap"),
            (PluginFailure::MalformedOutput, "hook_malformed"),
            (PluginFailure::MissingExport, "hook_not_executable"),
            (PluginFailure::Unknown, "plugin_missing"),
            (PluginFailure::Disabled, "plugin_missing"),
            (PluginFailure::Host, "hook_host_error"),
        ] {
            let plugins = StubPlugins::new(vec![("gate", Err(kind))]);
            let trigger = trigger_with_hooks(TriggerPluginHooks {
                predicate: Some(hook("gate", json!({}), None)),
                transform: None,
            });
            let verdict = apply_plugin_hooks(&plugins, &trigger, delta()).await;
            assert_eq!(
                verdict.obj,
                Some(delta()),
                "{kind:?} must still fail OPEN — this change is honesty, not a gate"
            );
            assert_eq!(verdict.incidents.len(), 1, "{kind:?}");
            assert_eq!(verdict.incidents[0].outcome, expected, "{kind:?}");
            assert!(
                verdict.incidents[0].detail.contains("NOT gated"),
                "{kind:?}: the row must say the hop was not gated: {}",
                verdict.incidents[0].detail
            );
        }
    }

    /// A predicate that answers something other than `{"pass": bool}` is a
    /// contract violation, not a trap — and a transform that answers a
    /// non-object is the same violation in the other slot.
    #[tokio::test]
    async fn malformed_hook_output_is_recorded_as_malformed_in_either_slot() {
        let plugins = StubPlugins::new(vec![("gate", Ok(json!({ "verdict": "yes" })))]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({}), None)),
            transform: None,
        });
        let verdict = apply_plugin_hooks(&plugins, &trigger, delta()).await;
        assert_eq!(verdict.obj, Some(delta()));
        assert_eq!(verdict.incidents[0].outcome, "hook_malformed");
        assert_eq!(verdict.incidents[0].slot, HookSlot::Predicate);

        let plugins = StubPlugins::new(vec![("slim", Ok(json!([1, 2, 3])))]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: None,
            transform: Some(hook("slim", json!({}), None)),
        });
        let verdict = apply_plugin_hooks(&plugins, &trigger, delta()).await;
        assert_eq!(verdict.obj, Some(delta()), "the original envelope survives");
        assert_eq!(verdict.incidents[0].outcome, "hook_malformed");
        assert_eq!(verdict.incidents[0].slot, HookSlot::Transform);
        assert!(verdict.incidents[0].detail.contains("NOT shaped"));
    }

    /// A hop that sails through both hooks says nothing — the rows have to mean
    /// something, and a per-event row for a healthy edge is the amplification
    /// this whole direction is about avoiding.
    #[tokio::test]
    async fn healthy_hooks_report_no_incidents() {
        let plugins = StubPlugins::new(vec![
            ("gate", Ok(json!({ "pass": true }))),
            ("slim", Ok(json!({ "summary": "ok" }))),
        ]);
        let trigger = trigger_with_hooks(TriggerPluginHooks {
            predicate: Some(hook("gate", json!({}), None)),
            transform: Some(hook("slim", json!({}), None)),
        });
        let verdict = apply_plugin_hooks(&plugins, &trigger, delta()).await;
        assert!(verdict.obj.is_some());
        assert!(verdict.incidents.is_empty());
        assert_eq!(verdict.stop_reason(), None);
    }

    /// The convention, enforced as an inventory rather than as a sentence: every
    /// outcome this module can hand the ledger is in
    /// [`pumper_core::storage::TRIGGER_OUTCOMES`], and every `hook_*` word in
    /// that vocabulary is one something here actually produces. A row whose
    /// outcome is not in the allowlist is a value `GET /triggers/{id}/runs`
    /// documents as impossible; a listed word nothing produces is dead API.
    #[test]
    fn hook_outcomes_and_the_storage_allowlist_agree_in_both_directions() {
        use pumper_core::storage::TRIGGER_OUTCOMES;
        use std::collections::BTreeSet;

        let mut produced: BTreeSet<&str> = BTreeSet::new();
        for kind in [
            PluginFailure::Unknown,
            PluginFailure::Disabled,
            PluginFailure::MissingExport,
            PluginFailure::Trap,
            PluginFailure::MalformedOutput,
            PluginFailure::Host,
        ] {
            produced.insert(hook_failure_outcome(&pumper_core::Error::plugin(
                kind, "p", "x",
            )));
        }
        // The two this module names directly rather than deriving from a class.
        produced.insert("predicate_veto");
        produced.insert("hook_malformed");
        // An error that never came from the plugin host at all still lands
        // somewhere allowlisted rather than inventing a word.
        produced.insert(hook_failure_outcome(&pumper_core::Error::App("x".into())));

        for outcome in &produced {
            assert!(
                TRIGGER_OUTCOMES.contains(outcome),
                "'{outcome}' is recorded but not in TRIGGER_OUTCOMES — add it there \
                 (with a comment saying what it means) or stop producing it"
            );
        }
        let listed: BTreeSet<&str> = TRIGGER_OUTCOMES
            .iter()
            .copied()
            .filter(|o| o.starts_with("hook_"))
            .collect();
        let produced_hook: BTreeSet<&str> = produced
            .iter()
            .copied()
            .filter(|o| o.starts_with("hook_"))
            .collect();
        assert_eq!(
            listed, produced_hook,
            "the hook_* vocabulary drifted: TRIGGER_OUTCOMES and this module disagree"
        );
    }

    /// Which outcomes are bounded, pinned: exactly the ones that describe the
    /// DEPLOYMENT. A per-event failure (a trap on this particular delta) is news
    /// every time and must never be suppressed.
    #[test]
    fn only_deployment_facts_are_deduped_not_per_event_failures() {
        assert!(is_static_hook_fact("plugin_missing"));
        assert!(is_static_hook_fact("hook_not_executable"));
        for per_event in [
            "hook_trap",
            "hook_malformed",
            "hook_host_error",
            "predicate_veto",
            "fired",
        ] {
            assert!(
                !is_static_hook_fact(per_event),
                "'{per_event}' varies per event — suppressing it would hide real failures"
            );
        }
    }
}
