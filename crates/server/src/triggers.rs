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
pub fn dataset_trigger_obj(
    trigger: &Trigger,
    source_job: &Job,
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
        "app": source_job.app,
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
pub fn idempotency_key(trigger_id: &str, source_job_id: &str) -> String {
    format!("trig:{trigger_id}:{source_job_id}")
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

// ── worker hooks (IO around the pure helpers) ────────────────────────────────

use std::collections::HashMap;

use pumper_core::EnqueueOptions;
use tracing::{info, warn};

use crate::state::AppState;

/// Fires enabled dataset triggers matching this run's revision batch. One
/// target job per trigger per source run (idempotency-keyed), carrying the
/// capped key batch in `params._trigger`. Fail-open: evaluation errors are
/// logged and never affect the source job.
pub async fn fire_dataset_triggers(
    state: &AppState,
    job: &Job,
    by_dataset: &HashMap<&str, Vec<&Revision>>,
) {
    let trigs = match state.storage.enabled_triggers("dataset", &job.app).await {
        Ok(t) if !t.is_empty() => t,
        Ok(_) => return,
        Err(e) => {
            warn!(job = %job.id, "failed to load dataset triggers: {e}");
            return;
        }
    };
    let mut fired = 0;
    for (dataset, revs) in by_dataset {
        for trigger in trigs.iter().filter(|t| t.covers_dataset(dataset)) {
            let matching: Vec<&Revision> = revs
                .iter()
                .copied()
                .filter(|r| change_matches(trigger.on_change.as_deref(), &r.change))
                .collect();
            if matching.is_empty() {
                continue;
            }
            let (depth, chain) = match decide(&trigger.id, &job.params, &state.config.triggers) {
                FireDecision::Fire { depth, chain } => (depth, chain),
                FireDecision::SkipCycle => {
                    warn!(trigger = %trigger.id, job = %job.id,
                          "trigger skipped: cycle detected in provenance chain");
                    continue;
                }
                FireDecision::SkipDepth => {
                    warn!(trigger = %trigger.id, job = %job.id,
                          max_depth = state.config.triggers.max_depth,
                          "trigger skipped: max chain depth reached");
                    continue;
                }
            };
            let obj = dataset_trigger_obj(
                trigger,
                job,
                dataset,
                &matching,
                depth,
                &chain,
                &state.config.triggers,
            );
            fired += enqueue_hop(state, trigger, job, obj).await;
        }
    }
    if fired > 0 {
        state.notify.notify_one();
    }
}

/// Fires enabled terminal-job triggers matching this job's final status.
pub async fn fire_terminal_triggers(state: &AppState, job: &Job) {
    let trigs = match state.storage.enabled_triggers("job", &job.app).await {
        Ok(t) if !t.is_empty() => t,
        Ok(_) => return,
        Err(e) => {
            warn!(job = %job.id, "failed to load job triggers: {e}");
            return;
        }
    };
    let mut fired = 0;
    for trigger in &trigs {
        if !status_matches(trigger.on_status.as_deref(), job.status.as_str()) {
            continue;
        }
        let (depth, chain) = match decide(&trigger.id, &job.params, &state.config.triggers) {
            FireDecision::Fire { depth, chain } => (depth, chain),
            FireDecision::SkipCycle => {
                warn!(trigger = %trigger.id, job = %job.id,
                      "trigger skipped: cycle detected in provenance chain");
                continue;
            }
            FireDecision::SkipDepth => {
                warn!(trigger = %trigger.id, job = %job.id,
                      max_depth = state.config.triggers.max_depth,
                      "trigger skipped: max chain depth reached");
                continue;
            }
        };
        let obj = terminal_trigger_obj(trigger, job, depth, &chain);
        fired += enqueue_hop(state, trigger, job, obj).await;
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
    let trigs = match state.storage.enabled_external_triggers(source_id).await {
        Ok(t) if !t.is_empty() => t,
        Ok(_) => return 0,
        Err(e) => {
            warn!(source = %source_id, "failed to load external triggers: {e}");
            return 0;
        }
    };
    let mut fired = 0;
    for trigger in &trigs {
        // Predicates were validated at create time; a spec that no longer
        // parses (defensive) skips the trigger loudly rather than firing wide.
        let filters = match trigger.filters.as_deref() {
            Some(specs) => match crate::routes::parse_filters(specs) {
                Ok(f) => f,
                Err(_) => {
                    warn!(trigger = %trigger.id, "external trigger has unparseable filters; skipped");
                    continue;
                }
            },
            None => Vec::new(),
        };
        if !payload_matches(&filters, payload) {
            continue;
        }
        // Inbound events carry no provenance — every chain starts here.
        let (depth, chain) = match decide(&trigger.id, &Value::Null, &state.config.triggers) {
            FireDecision::Fire { depth, chain } => (depth, chain),
            // Unreachable from a fresh chain, but keep the guards uniform.
            FireDecision::SkipCycle | FireDecision::SkipDepth => continue,
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
        let Some(obj) = apply_plugin_hooks(state.plugins.as_ref(), trigger, obj).await else {
            continue;
        };
        if !state.registry.contains_key(&trigger.target_app) {
            warn!(trigger = %trigger.id, app = %trigger.target_app,
                  "external trigger skipped: target app not registered");
            continue;
        }
        let opts = EnqueueOptions {
            params: merged_params(&trigger.params, obj),
            max_attempts: trigger.max_attempts,
            priority: trigger.priority,
            budget_usd: trigger.budget_usd,
            idempotency_key: Some(idempotency_key(&trigger.id, event_id)),
            trigger_id: Some(trigger.id.clone()),
            ..Default::default()
        };
        match state.storage.enqueue_dedup(&trigger.target_app, opts).await {
            Ok((hop, true)) => {
                info!(trigger = %trigger.id, event = %event_id, target = %hop.id,
                      app = %trigger.target_app, "external trigger fired");
                fired += 1;
            }
            Ok((_, false)) => {} // already fired for this event id (redelivery)
            Err(e) => {
                warn!(trigger = %trigger.id, event = %event_id, "external trigger enqueue failed: {e}");
            }
        }
    }
    if fired > 0 {
        state.notify.notify_one();
    }
    fired
}

/// Enqueues one triggered hop (dedup-guarded). Returns 1 when a job was
/// actually created, 0 when skipped/deduped/failed.
async fn enqueue_hop(state: &AppState, trigger: &Trigger, source: &Job, obj: Value) -> usize {
    // Plugin hooks first: a predicate may veto the hop, a transform may shape
    // the `_trigger` envelope. Both fail open (see `apply_plugin_hooks`).
    let Some(obj) = apply_plugin_hooks(state.plugins.as_ref(), trigger, obj).await else {
        return 0;
    };
    if !state.registry.contains_key(&trigger.target_app) {
        warn!(trigger = %trigger.id, app = %trigger.target_app,
              "trigger skipped: target app not registered");
        return 0;
    }
    let opts = EnqueueOptions {
        params: merged_params(&trigger.params, obj),
        max_attempts: trigger.max_attempts,
        priority: trigger.priority,
        budget_usd: trigger.budget_usd,
        idempotency_key: Some(idempotency_key(&trigger.id, &source.id.to_string())),
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
            1
        }
        Ok((_, false)) => 0, // already fired for this source run
        Err(e) => {
            warn!(trigger = %trigger.id, source = %source.id, "trigger enqueue failed: {e}");
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
