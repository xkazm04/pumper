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
            plugin_hooks: None,
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
            plugin_hooks: None,
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
            plugin_hooks: None,
        })
        .await
        .expect("create wildcard trigger");
    let set = storage.enabled_external_triggers(&src.id).await.unwrap();
    assert_eq!(set.len(), 2, "exact + wildcard both evaluate");
    // A different source only sees the wildcard.
    assert_eq!(
        storage
            .enabled_external_triggers("other")
            .await
            .unwrap()
            .len(),
        1
    );

    // Disable/delete round-trip.
    assert!(storage
        .set_ingress_source_enabled(&src.id, false)
        .await
        .unwrap());
    assert!(
        !storage
            .get_ingress_source(&src.id)
            .await
            .unwrap()
            .unwrap()
            .enabled
    );
    assert!(storage.delete_ingress_source(&src.id).await.unwrap());
    assert!(storage.get_ingress_source(&src.id).await.unwrap().is_none());
}

/// The decision ledger (migration 0036): fires and skips land in the same
/// table, page newest-first, and are bounded by age — the anti-pattern being a
/// ledger that only records successes (which is what `jobs.trigger_id` already
/// was) or one that grows forever.
#[tokio::test]
async fn trigger_decision_ledger_records_skips_not_only_fires() {
    use pumper_core::storage::{NewTriggerRun, TRIGGER_SET_ID};
    let store = pumper_core::testing::TempStore::new("trigger-ledger-test").await;
    let storage = &store.storage;

    for (outcome, dataset) in [
        ("fired", Some("unified")),
        ("dedup", Some("unified")),
        ("no_change_match", Some("orgs")),
    ] {
        storage
            .record_trigger_run(&NewTriggerRun {
                trigger_id: "T1",
                outcome,
                source_kind: "dataset",
                source_job_id: Some("J1"),
                dataset,
                job_id: (outcome == "fired").then_some("HOP1"),
                ..Default::default()
            })
            .await
            .expect("record decision");
    }
    // A decision about the whole edge set (the set failed to load) is recorded
    // against the sentinel, not against a trigger that was never reached.
    storage
        .record_trigger_run(&NewTriggerRun {
            trigger_id: TRIGGER_SET_ID,
            outcome: "eval_set_error",
            source_kind: "dataset",
            source_job_id: Some("J1"),
            detail: Some("database is locked"),
            ..Default::default()
        })
        .await
        .unwrap();

    let page = storage
        .list_trigger_runs_page("T1", None, 10)
        .await
        .unwrap();
    assert_eq!(page.len(), 3, "the sentinel row belongs to no trigger");
    let mut outcomes: Vec<&str> = page.iter().map(|r| r.outcome.as_str()).collect();
    outcomes.sort();
    assert_eq!(outcomes, ["dedup", "fired", "no_change_match"]);
    let fired = page.iter().find(|r| r.outcome == "fired").unwrap();
    assert_eq!(fired.job_id.as_deref(), Some("HOP1"));
    assert_eq!(fired.dataset.as_deref(), Some("unified"));

    let set = storage
        .list_trigger_runs_page(TRIGGER_SET_ID, None, 10)
        .await
        .unwrap();
    assert_eq!(set.len(), 1);
    assert_eq!(set[0].detail.as_deref(), Some("database is locked"));

    // Keyset paging walks the whole ledger exactly once.
    let first = storage.list_trigger_runs_page("T1", None, 2).await.unwrap();
    assert_eq!(first.len(), 2);
    let after = (
        pumper_core::datasets::ts(first[1].created_at),
        first[1].id.clone(),
    );
    let second = storage
        .list_trigger_runs_page("T1", Some(after), 2)
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert!(!first.iter().any(|r| r.id == second[0].id));

    // Bounded: `0` days disables the prune, and a fresh row is never old.
    assert_eq!(storage.prune_trigger_runs(0).await.unwrap(), 0);
    assert_eq!(storage.prune_trigger_runs(1).await.unwrap(), 0);
}

/// The sandbox failure classes the ledger could not previously REPRESENT: a
/// hook that trapped, burned its fuel, answered garbage, or named a module with
/// no runnable ABI all fired the hop with nothing but a `warn!` behind them.
/// Each is now its own word, and each round-trips with its detail intact — a
/// vocabulary the API documents is only real if the store actually holds it.
#[tokio::test]
async fn the_ledger_holds_every_hook_failure_class_distinctly() {
    use pumper_core::storage::{NewTriggerRun, TRIGGER_OUTCOMES};
    let store = pumper_core::testing::TempStore::new("trigger-hook-outcomes").await;
    let storage = &store.storage;

    // The four hook failure classes, plus the veto they must never be confused
    // with. `plugin_missing` predates this set and is covered above.
    let classes = [
        (
            "hook_trap",
            "predicate plugin 'gate' trapped — on_error=skip, hop stopped",
        ),
        (
            "hook_malformed",
            "transform plugin 'slim' returned non-object output",
        ),
        (
            "hook_not_executable",
            "predicate plugin 'stub' exports no extract ABI",
        ),
        (
            "hook_host_error",
            "the plugin host's blocking task panicked",
        ),
        (
            "predicate_veto",
            "predicate plugin 'gate' returned pass=false",
        ),
    ];
    for (outcome, detail) in classes {
        assert!(
            TRIGGER_OUTCOMES.contains(&outcome),
            "'{outcome}' must be in the documented vocabulary before anything writes it"
        );
        storage
            .record_trigger_run(&NewTriggerRun {
                trigger_id: "T9",
                outcome,
                source_kind: "job",
                source_job_id: Some("J9"),
                detail: Some(detail),
                ..Default::default()
            })
            .await
            .expect("record hook decision");
    }

    let page = storage
        .list_trigger_runs_page("T9", None, 50)
        .await
        .unwrap();
    assert_eq!(page.len(), classes.len());
    for (outcome, detail) in classes {
        let row = page
            .iter()
            .find(|r| r.outcome == outcome)
            .unwrap_or_else(|| panic!("no '{outcome}' row"));
        assert_eq!(
            row.detail.as_deref(),
            Some(detail),
            "'{outcome}' lost the detail that names the plugin and the consequence"
        );
    }
    // The distinctness that matters: a crashed sandbox and a gate that said no
    // are separate rows, not one word doing both jobs.
    assert_eq!(
        page.iter()
            .filter(|r| r.outcome == "predicate_veto")
            .count(),
        1,
        "only the genuine pass=false answer may claim the veto word"
    );
}

/// M15: plugin_hooks JSON column round-trips through create/get, and an
/// all-empty hooks object stores as NULL (no hooks).
#[tokio::test]
async fn trigger_plugin_hooks_roundtrip() {
    use pumper_core::{PluginHook, TriggerPluginHooks};
    let store = pumper_core::testing::TempStore::new("trigger-hooks-test").await;
    let storage = &store.storage;

    let hooks = TriggerPluginHooks {
        predicate: Some(PluginHook {
            plugin: "trigger-gate".into(),
            params: json!({ "min_count": 5 }),
            on_error: Some("skip".into()),
        }),
        transform: Some(PluginHook {
            plugin: "delta-slim".into(),
            params: json!({ "keep": ["dataset", "count"] }),
            on_error: None,
        }),
    };
    let trigger = storage
        .create_trigger(&NewTrigger {
            name: Some("hooked"),
            source_kind: "dataset",
            source_app: "grants",
            source_dataset: Some("*"),
            on_change: Some("fresh"),
            on_status: None,
            target_app: "research",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: Some(&hooks),
        })
        .await
        .expect("create trigger with hooks");
    let read = storage
        .get_trigger(&trigger.id)
        .await
        .unwrap()
        .expect("trigger row");
    let h = read.plugin_hooks.expect("hooks persisted");
    let p = h.predicate.expect("predicate hook");
    assert_eq!(p.plugin, "trigger-gate");
    assert_eq!(p.params, json!({ "min_count": 5 }));
    assert_eq!(p.on_error.as_deref(), Some("skip"));
    let t = h.transform.expect("transform hook");
    assert_eq!(t.plugin, "delta-slim");
    assert!(t.on_error.is_none());

    // All-empty hooks object → stored as NULL, read back as None.
    let empty = TriggerPluginHooks {
        predicate: None,
        transform: None,
    };
    let bare = storage
        .create_trigger(&NewTrigger {
            name: Some("bare"),
            source_kind: "dataset",
            source_app: "grants",
            source_dataset: Some("*"),
            on_change: Some("fresh"),
            on_status: None,
            target_app: "research",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: Some(&empty),
        })
        .await
        .expect("create trigger with empty hooks");
    assert!(storage
        .get_trigger(&bare.id)
        .await
        .unwrap()
        .unwrap()
        .plugin_hooks
        .is_none());
}
