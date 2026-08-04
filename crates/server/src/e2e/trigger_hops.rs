//! Dataset-trigger hop identity: one hop PER dataset of a run, and a saved
//! search's view hops kept apart from the run's own fan-out hops even though
//! both ride the same source job id.

use std::collections::HashMap;
use std::sync::Arc;

use pumper_core::datasets::{Provenance, Revision};
use pumper_core::{EnqueueOptions, NewTrigger};
use serde_json::json;

use super::harness::{test_state, FakeApp};
use crate::triggers::{fire_dataset_triggers, DatasetBatch};

fn rev(dataset: &str, key: &str) -> Revision {
    Revision {
        app: "src".into(),
        dataset: dataset.into(),
        key: key.into(),
        revision: 1,
        change: "new".into(),
        data: Some(json!({ "k": key })),
        diff: None,
        created_at: chrono::Utc::now(),
        trust: "stable".into(),
        provenance: Provenance::default(),
    }
}

/// Hops the trigger fired, as `(dataset, idempotency_key)` pairs. The key is
/// read straight from the row — `Job` does not expose it, and it is exactly the
/// identity under test.
async fn hops(state: &crate::state::AppState, trigger_id: &str) -> Vec<(String, String)> {
    let rows: Vec<(Option<String>, String)> =
        sqlx::query_as("SELECT idempotency_key, params FROM jobs WHERE trigger_id = ?1")
            .bind(trigger_id)
            .fetch_all(&state.storage.pool())
            .await
            .unwrap();
    let mut out: Vec<(String, String)> = rows
        .into_iter()
        .map(|(key, params)| {
            let params: serde_json::Value = serde_json::from_str(&params).unwrap();
            (
                params["_trigger"]["dataset"]
                    .as_str()
                    .expect("dataset hop carries its dataset")
                    .to_string(),
                key.expect("hops are dedup-keyed"),
            )
        })
        .collect();
    out.sort();
    out
}

/// The anti-pattern: a run writing three datasets under a `'*'` trigger fired
/// ONE hop for a RandomState-arbitrary dataset and silently dedup-suppressed
/// the rest, because the idempotency key omitted the dataset.
#[tokio::test]
async fn multi_dataset_run_fires_one_hop_per_dataset_not_one_per_run() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let trigger = state
        .storage
        .create_trigger(&NewTrigger {
            name: Some("all-datasets"),
            source_kind: "dataset",
            source_app: "src",
            source_dataset: Some("*"),
            on_change: Some("fresh"),
            on_status: None,
            target_app: "fake",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: None,
        })
        .await
        .unwrap();
    let source = state
        .storage
        .enqueue("src", EnqueueOptions::default())
        .await
        .unwrap();

    let revs = vec![rev("alpha", "a1"), rev("beta", "b1"), rev("gamma", "g1")];
    let mut by_dataset: HashMap<&str, Vec<&Revision>> = HashMap::new();
    for r in &revs {
        by_dataset.entry(r.dataset.as_str()).or_default().push(r);
    }

    fire_dataset_triggers(&state, &source, DatasetBatch::Run, &by_dataset).await;
    let fired = hops(&state, &trigger.id).await;
    assert_eq!(
        fired.iter().map(|(d, _)| d.as_str()).collect::<Vec<_>>(),
        vec!["alpha", "beta", "gamma"],
        "every dataset of the batch gets its own hop"
    );
    let job = source.id;
    assert_eq!(
        fired.iter().map(|(_, k)| k.as_str()).collect::<Vec<_>>(),
        vec![
            format!("trig:{}:{job}:ds:alpha", trigger.id),
            format!("trig:{}:{job}:ds:beta", trigger.id),
            format!("trig:{}:{job}:ds:gamma", trigger.id),
        ],
        "and its own dedup key"
    );

    // Deterministic: re-evaluating the identical batch adds nothing — the
    // per-dataset keys still dedupe within one source run.
    fire_dataset_triggers(&state, &source, DatasetBatch::Run, &by_dataset).await;
    assert_eq!(
        hops(&state, &trigger.id).await.len(),
        3,
        "re-evaluation is a no-op"
    );
}

/// The second half of the same collision: a saved-search view materialized by a
/// run re-badges the SOURCE job (same id) before firing, so a view targeting the
/// job's own app used to dedup against the run's own fan-out hop and vanish.
#[tokio::test]
async fn view_materialization_hop_does_not_dedup_against_the_fanout_hop() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let trigger = state
        .storage
        .create_trigger(&NewTrigger {
            name: Some("same-app"),
            source_kind: "dataset",
            source_app: "src",
            source_dataset: Some("*"),
            on_change: Some("fresh"),
            on_status: None,
            target_app: "fake",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: None,
        })
        .await
        .unwrap();
    let source = state
        .storage
        .enqueue("src", EnqueueOptions::default())
        .await
        .unwrap();

    let revs = vec![rev("d", "k1")];
    let mut by_dataset: HashMap<&str, Vec<&Revision>> = HashMap::new();
    by_dataset.insert("d", revs.iter().collect());

    // Same trigger, same source job, same dataset — only the batch differs.
    fire_dataset_triggers(&state, &source, DatasetBatch::Run, &by_dataset).await;
    fire_dataset_triggers(&state, &source, DatasetBatch::View("S1"), &by_dataset).await;
    fire_dataset_triggers(&state, &source, DatasetBatch::View("S2"), &by_dataset).await;

    let job = source.id;
    let keys: Vec<String> = hops(&state, &trigger.id)
        .await
        .into_iter()
        .map(|(_, k)| k)
        .collect();
    assert_eq!(
        keys,
        vec![
            format!("trig:{}:{job}:ds:d", trigger.id),
            format!("trig:{}:{job}:view:S1:ds:d", trigger.id),
            format!("trig:{}:{job}:view:S2:ds:d", trigger.id),
        ],
        "the run fan-out and each view materialization are distinct hops"
    );
}
