//! Integration test for the reactive-trigger storage layer: CRUD round-trip,
//! the evaluation set (enabled, per source kind), idempotent hop enqueue
//! (at most once per source run), and the jobs.trigger_id lineage view.
//! Runs against a real temp-dir SQLite with the full migration chain.

use pumper_core::{EnqueueOptions, NewTrigger};
use serde_json::json;

#[tokio::test]
async fn trigger_crud_idempotent_fire_and_lineage() {
    let store = pumper_core::testing::TempStore::new("trigger-test").await;
    let storage = &store.storage;

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
            filters: None,
        })
        .await
        .expect("create trigger");
    assert!(trigger.enabled);
    assert!(trigger.covers_dataset("unified"), "'*' covers any dataset");
    assert_eq!(trigger.params["mode"], "batch");

    // Evaluation set is scoped by (kind, app) and enabled.
    assert_eq!(
        storage
            .enabled_triggers("dataset", "grants")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(storage
        .enabled_triggers("job", "grants")
        .await
        .unwrap()
        .is_empty());
    assert!(storage
        .enabled_triggers("dataset", "other")
        .await
        .unwrap()
        .is_empty());

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
    assert!(storage
        .set_trigger_enabled(&trigger.id, false)
        .await
        .unwrap());
    assert!(storage
        .enabled_triggers("dataset", "grants")
        .await
        .unwrap()
        .is_empty());
    assert!(storage.delete_trigger(&trigger.id).await.unwrap());
    assert!(storage.get_trigger(&trigger.id).await.unwrap().is_none());
}

#[tokio::test]
async fn ingress_sources_and_external_triggers_roundtrip() {
    let store = pumper_core::testing::TempStore::new("ingress-test").await;
    let storage = &store.storage;

    // Ingress source CRUD.
    let src = storage
        .create_ingress_source("github", "hush")
        .await
        .expect("create ingress source");
    assert!(src.enabled);
    assert_eq!(src.name, "github");
    assert_eq!(src.secret, "hush");
    // The secret never serializes (list/read responses must not leak it).
    let json = serde_json::to_value(&src).unwrap();
    assert!(json.get("secret").is_none(), "secret must not serialize");
    assert_eq!(storage.list_ingress_sources().await.unwrap().len(), 1);

    // External trigger with payload predicates persists filters verbatim.
    let trig = storage
        .create_trigger(&NewTrigger {
            name: Some("push-to-crawl"),
            source_kind: "external",
            source_app: &src.id,
            source_dataset: None,
            on_change: None,
            on_status: None,
            target_app: "crawl",
            params: &json!({ "url": "https://acme.dev/docs" }),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: Some(&["$.ref:eq:refs/heads/main".to_string()]),
        })
        .await
        .expect("create external trigger");
    assert_eq!(
        trig.filters.as_deref(),
        Some(&["$.ref:eq:refs/heads/main".to_string()][..])
    );

    // Wildcard trigger matches every source; the evaluation set carries both.
    storage
        .create_trigger(&NewTrigger {
            name: Some("any-source"),
            source_kind: "external",
            source_app: "*",
            source_dataset: None,
            on_change: None,
            on_status: None,
            target_app: "crawl",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
        })
        .await
        .expect("create wildcard trigger");
    let set = storage.enabled_external_triggers(&src.id).await.unwrap();
    assert_eq!(set.len(), 2, "exact + wildcard both evaluate");
    // A different source only sees the wildcard.
    assert_eq!(
        storage.enabled_external_triggers("other").await.unwrap().len(),
        1
    );

    // Disable/delete round-trip.
    assert!(storage
        .set_ingress_source_enabled(&src.id, false)
        .await
        .unwrap());
    assert!(!storage
        .get_ingress_source(&src.id)
        .await
        .unwrap()
        .unwrap()
        .enabled);
    assert!(storage.delete_ingress_source(&src.id).await.unwrap());
    assert!(storage.get_ingress_source(&src.id).await.unwrap().is_none());
}
