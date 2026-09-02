//! The extraction profile registry (N12 step 1, `resilient-extraction.md` §4)
//! against a real temp-dir SQLite with the full migration chain.
//!
//! The unit tests in `resilience::profiles` cover the pure origin/repairability
//! question. This covers what only a database can answer: that a profile is
//! append-only, that promotion is a pointer move and nothing else, that a
//! rollback is expressible as the same pointer move backwards, and that a run
//! nobody stamped keeps `profile_version = NULL` rather than a version zero.

use std::sync::Arc;

use pumper_core::config::ResilienceConfig;
use pumper_core::resilience::store::Resilience;
use pumper_core::testing::TempStore;
use pumper_core::{doc_signals, Datasets, FetchHealth, ObservedDoc, RunReport};
use serde_json::json;
use uuid::Uuid;

fn v1() -> serde_json::Value {
    json!({"price": {"type": "css", "selector": ".price"}})
}

fn v2() -> serde_json::Value {
    json!({"price": {"type": "css", "selector": "[data-price]"}})
}

#[tokio::test]
async fn a_profile_is_append_only_and_promotion_is_only_a_pointer_move() {
    let store = TempStore::new("profiles-append").await;
    let ds = Datasets::new(store.storage.pool().clone());

    let p = ds
        .ensure_profile("acme-products", "extractor", "products", &v1())
        .await
        .unwrap();
    assert_eq!(p.active_version, 1);
    assert_eq!(p.app, "extractor");

    // Idempotent by name: provisioning twice must not fork a second lineage.
    let again = ds
        .ensure_profile("acme-products", "extractor", "products", &v2())
        .await
        .unwrap();
    assert_eq!(again.active_version, 1);
    assert_eq!(ds.profile_versions("acme-products").await.unwrap().len(), 1);

    // A candidate exists WITHOUT being what anything runs with — the property
    // that makes shadow mode expressible at all.
    let ver = ds
        .add_profile_version(
            "acme-products",
            &v2(),
            "inversion",
            Some(1),
            Some(&json!({"holdout_match_rate": 0.98})),
        )
        .await
        .unwrap();
    assert_eq!(ver, 2);
    assert_eq!(
        ds.profile("acme-products")
            .await
            .unwrap()
            .unwrap()
            .active_version,
        1,
        "adding a version must not promote it"
    );
    let (active_v, active_rules) = ds
        .profile_rules("acme-products", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active_v, 1);
    assert_eq!(active_rules, v1());

    // Promotion is the pointer move, and nothing else: v1 is still readable.
    ds.set_active_profile_version("acme-products", 2)
        .await
        .unwrap();
    assert_eq!(
        ds.profile_rules("acme-products", None)
            .await
            .unwrap()
            .unwrap(),
        (2, v2())
    );
    assert_eq!(
        ds.profile_rules("acme-products", Some(1))
            .await
            .unwrap()
            .unwrap(),
        (1, v1()),
        "history is never rewritten, so rollback is the same move backwards"
    );

    let versions = ds.profile_versions("acme-products").await.unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version, 2);
    assert_eq!(versions[0].origin, "inversion");
    assert_eq!(versions[0].parent_version, Some(1));
    assert_eq!(
        versions[0].evidence.as_ref().unwrap()["holdout_match_rate"],
        0.98
    );
    // Every version is also in the content-addressed rules registry, so a
    // revision stamped with its hash stays replayable.
    assert!(ds
        .rules_by_hash(&versions[0].rules_hash)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn promoting_a_version_nobody_wrote_is_refused_not_silently_applied() {
    // A promotion to a nonexistent version would leave the source with no rules
    // at all — worse than the broken rules it replaced.
    let store = TempStore::new("profiles-refuse").await;
    let ds = Datasets::new(store.storage.pool().clone());
    ds.ensure_profile("p", "extractor", "products", &v1())
        .await
        .unwrap();
    let err = ds.set_active_profile_version("p", 7).await.unwrap_err();
    assert!(err.to_string().contains("no version 7"), "{err}");
    assert_eq!(ds.profile("p").await.unwrap().unwrap().active_version, 1);
}

#[tokio::test]
async fn an_unstamped_run_keeps_a_null_profile_version_not_a_zero() {
    // The derived-column lesson: NULL *means* "not profile-backed", so a run
    // recorded before the registry existed is correct by construction.
    let store = TempStore::new("profiles-stamp").await;
    let pool = store.storage.pool().clone();
    let ds = Arc::new(Datasets::new(pool.clone()));
    let cfg = ResilienceConfig {
        min_cohort_docs: 2,
        invariant_min_support: 2,
        ..ResilienceConfig::default()
    };
    let health = Resilience::new(pool.clone(), &cfg);
    let hs = health.store().expect("detection on");
    hs.ensure_source("extractor", "products").await.unwrap();

    let job = Uuid::new_v4();
    let docs: Vec<ObservedDoc> = (0..4)
        .map(|i| {
            let values = json!({ "price": format!("${i}") });
            let body = format!("<div class=\"price\">${i}</div>");
            ObservedDoc {
                key: format!("k{i}"),
                signals: doc_signals(&body, &values),
                values,
                report: Default::default(),
            }
        })
        .collect();
    health
        .observe(
            "extractor",
            &RunReport {
                job_id: job,
                dataset: "products",
                docs: &docs,
                fetch: FetchHealth {
                    attempted: 4,
                    ok: 4,
                },
                build_id: None,
            },
        )
        .await
        .unwrap();
    let _ = &ds;

    let sid = "extractor/products";
    assert_eq!(
        hs.run_profile_version(sid, &job.to_string()).await.unwrap(),
        None,
        "an unstamped run is honestly unknown, never version 0"
    );
    assert!(hs
        .stamp_profile_version(sid, &job.to_string(), 3)
        .await
        .unwrap());
    assert_eq!(
        hs.run_profile_version(sid, &job.to_string()).await.unwrap(),
        Some(3)
    );
    // A run that was never recorded (or has been pruned) stamps nothing and
    // says so, rather than reporting a write that did not happen.
    assert!(!hs
        .stamp_profile_version(sid, &Uuid::new_v4().to_string(), 3)
        .await
        .unwrap());
}
