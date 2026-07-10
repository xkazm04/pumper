//! Integration test for the reactive-trigger storage layer: CRUD round-trip,
//! the evaluation set (enabled, per source kind), idempotent hop enqueue
//! (at most once per source run), and the jobs.trigger_id lineage view.
//! Runs against a real temp-dir SQLite with the full migration chain.

use pumper_core::config::StorageConfig;
use pumper_core::{EnqueueOptions, NewTrigger, Storage};
use serde_json::json;

#[tokio::test]
async fn trigger_crud_idempotent_fire_and_lineage() {
    let dir = std::env::temp_dir().join(format!("pumper-trigger-test-{}", uuid::Uuid::new_v4()));
    let cfg = StorageConfig {
        database_path: dir.join("pumper.db"),
        artifacts_dir: dir.join("artifacts"),
    };
    let storage = Storage::connect(&cfg).await.expect("connect + migrate");

    // Create a dataset-kind edge: grants/unified fresh changes -> research.
    let trigger = storage
        .create_trigger(&NewTrigger {
            name: Some("grants-to-research"),
            source_kind: "dataset",
            source_app: "grants",
            source_dataset: Some("*"),
            on_change: Some("fresh"),
            on_status: None,
            target_app: "research",
            params: &json!({ "mode": "batch" }),
            budget_usd: Some(2.0),
            priority: 5,
            max_attempts: 1,
        })
        .await
        .expect("create trigger");
    assert!(trigger.enabled);
    assert!(trigger.covers_dataset("unified"), "'*' covers any dataset");
    assert_eq!(trigger.params["mode"], "batch");

    // Evaluation set is scoped by (kind, app) and enabled.
    assert_eq!(storage.enabled_triggers("dataset", "grants").await.unwrap().len(), 1);
    assert!(storage.enabled_triggers("job", "grants").await.unwrap().is_empty());
    assert!(storage.enabled_triggers("dataset", "other").await.unwrap().is_empty());

    // A hop fires at most once per source run: same idempotency key dedupes.
    let opts = || EnqueueOptions {
        params: json!({ "_trigger": { "count": 3 } }),
        max_attempts: 1,
        idempotency_key: Some("trig:T1:SRC1".to_string()),
        trigger_id: Some(trigger.id.clone()),
        ..Default::default()
    };
    let (first, created_first) = storage.enqueue_dedup("research", opts()).await.unwrap();
    let (second, created_second) = storage.enqueue_dedup("research", opts()).await.unwrap();
    assert!(created_first);
    assert!(!created_second, "re-evaluation must not double-fire");
    assert_eq!(first.id, second.id);
    assert_eq!(first.trigger_id.as_deref(), Some(trigger.id.as_str()));

    // Lineage: the trigger's runs view finds the hop.
    let runs = storage.jobs_by_trigger(&trigger.id, 10).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].id, first.id);

    // Disable removes it from the evaluation set; delete removes the row.
    assert!(storage.set_trigger_enabled(&trigger.id, false).await.unwrap());
    assert!(storage.enabled_triggers("dataset", "grants").await.unwrap().is_empty());
    assert!(storage.delete_trigger(&trigger.id).await.unwrap());
    assert!(storage.get_trigger(&trigger.id).await.unwrap().is_none());

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}
