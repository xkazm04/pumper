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
    FireDecision::Fire { depth: depth + 1, chain }
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

/// At-most-once-per-source-run dedup key (existing partial unique index).
pub fn idempotency_key(trigger_id: &str, source_job_id: &str) -> String {
    format!("trig:{trigger_id}:{source_job_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> TriggersConfig {
        TriggersConfig { max_depth: 3, key_cap: 2 }
    }

    #[test]
    fn decide_fires_extends_chain_and_guards_cycles_and_depth() {
        // Fresh source (no provenance): fires at depth 1.
        assert_eq!(
            decide("T1", &json!({}), &cfg()),
            FireDecision::Fire { depth: 1, chain: vec!["T1".into()] }
        );
        // Same trigger already in the chain: cycle skip.
        let looped = json!({ "_trigger": { "depth": 1, "chain": ["T1"] } });
        assert_eq!(decide("T1", &looped, &cfg()), FireDecision::SkipCycle);
        // Different trigger continues the chain.
        assert_eq!(
            decide("T2", &looped, &cfg()),
            FireDecision::Fire { depth: 2, chain: vec!["T1".into(), "T2".into()] }
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
}
