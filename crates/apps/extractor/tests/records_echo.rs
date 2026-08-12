//! The job result echoes a SAMPLE of the records, not the corpus — and the
//! corpus stays searchable anyway.
//!
//! Every write mode used to serialize every extracted record into the persisted
//! job result. A 10,000-record run wrote a multi-MB JSON blob into the `jobs`
//! row, and that blob then rode the `job.succeeded` webhook, the SSE event and
//! the receipt — forever, restating data already durably in the dataset. The
//! write path also deep-cloned every record purely to build that echo.
//!
//! Bounding the echo is only safe because search coverage moves to the mature
//! path: the result declares `index_datasets`, and the worker's
//! `dataset_search_docs` indexes the run's records from the dataset CHANGE FEED
//! (stable `<app>:<dataset>:<key>` ids, removals honoured) rather than from the
//! result array. The feed is what these tests assert against — it is the exact
//! input the worker consumes.

use std::path::Path;
use std::sync::Arc;

use app_extractor::Extractor;
use pumper_core::config::ResilienceConfig;
use pumper_core::resilience::{Resilience, SourceState};
use pumper_core::testing::{TempStore, TestContext};
use pumper_core::{AppContext, ScrapeApp};
use serde_json::{json, Value};
use uuid::Uuid;

fn ctx_with(root: &Path, store: &TempStore, params: Value) -> AppContext {
    TestContext::new(&store.storage, "extractor")
        .params(params)
        .artifacts_dir(root.join("extractor").join("job"))
        .build()
}

/// Seeds `n` crawl pages sharing one body, keyed `http://p{i}`.
async fn seed_pages(store: &TempStore, n: usize) -> std::path::PathBuf {
    let root = store.path().to_path_buf();
    let crawl_job = Uuid::new_v4().to_string();
    let dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("p.html"), b"<h1>Hi</h1>")
        .await
        .unwrap();
    let items: Vec<(String, Value)> = (0..n)
        .map(|i| {
            let key = format!("http://p{i}");
            (
                key.clone(),
                json!({"url": key, "artifact_path": "p.html", "job_id": crawl_job}),
            )
        })
        .collect();
    store
        .datasets()
        .upsert_many("crawl", "pages", &items)
        .await
        .unwrap();
    root
}

fn source_params(extra: Value) -> Value {
    let mut params = json!({
        "source": {"app": "crawl", "dataset": "pages"},
        "rules": {"h": {"type": "css", "selector": "h1"}},
        "dataset": "prices"
    });
    if let (Value::Object(map), Value::Object(extra)) = (&mut params, extra) {
        map.extend(extra);
    }
    params
}

#[tokio::test]
async fn the_records_echo_is_a_bounded_prefix_not_the_whole_corpus() {
    let store = TempStore::new("extract-echo-bound").await;
    let root = seed_pages(&store, 5).await;

    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            source_params(json!({"records_echo": 2})),
        ))
        .await
        .unwrap();
    assert_eq!(out["loaded"], 5, "{out}");
    assert_eq!(out["records"].as_array().unwrap().len(), 2, "{out}");
    assert_eq!(out["records_total"], 5, "the honest total travels with it");
    assert_eq!(out["records_truncated"], true);
    // The echoed rows are real extracted records, not placeholders.
    assert_eq!(out["records"][0]["h"], "Hi");

    // Under the bound, nothing is claimed to be missing.
    let out = Extractor
        .run(ctx_with(&root, &store, source_params(json!({}))))
        .await
        .unwrap();
    assert_eq!(out["records"].as_array().unwrap().len(), 5, "{out}");
    assert_eq!(out["records_total"], 5);
    assert_eq!(out["records_truncated"], false);
}

#[tokio::test]
async fn records_echo_zero_reports_counts_without_the_corpus() {
    // A caller that reads from the dataset has no use for the echo at all —
    // and asking for none must still leave the counts honest.
    let store = TempStore::new("extract-echo-zero").await;
    let root = seed_pages(&store, 3).await;
    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            source_params(json!({"records_echo": 0})),
        ))
        .await
        .unwrap();
    assert_eq!(out["records"].as_array().unwrap().len(), 0, "{out}");
    assert_eq!(out["records_total"], 3);
    assert_eq!(out["records_truncated"], true);
    assert_eq!(out["new"], 3, "the records were still written: {out}");
}

#[tokio::test]
async fn a_capped_echo_does_not_shrink_the_indexable_change_feed() {
    // THE REGRESSION THIS FORBIDS: search coverage used to come from the
    // result's `records` array (worker `search_docs`). Capping that array
    // without declaring `index_datasets` would silently index only the first N
    // records of every run. The declaration routes indexing to the delta path,
    // whose input is the change feed asserted on here — every record written,
    // not every record echoed.
    let store = TempStore::new("extract-echo-coverage").await;
    let root = seed_pages(&store, 5).await;

    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            source_params(json!({"records_echo": 1})),
        ))
        .await
        .unwrap();
    assert_eq!(out["records"].as_array().unwrap().len(), 1, "{out}");

    // The declaration the worker reads: the app namespace it wrote under and
    // the dataset it actually landed in.
    assert_eq!(
        out["index_datasets"],
        json!([{ "app": "extractor", "dataset": "prices" }]),
        "{out}"
    );

    // What `dataset_search_docs` will find there: one indexable revision per
    // record written — five, not the one that was echoed.
    let revs = store
        .datasets()
        .changes_since("extractor", Some("prices"), None, 1000, None)
        .await
        .unwrap();
    assert_eq!(
        revs.len(),
        5,
        "the change feed carries every written record"
    );
    assert!(
        revs.iter()
            .all(|r| r.data.is_some() && r.change != "removed"),
        "every revision carries the snapshot the indexer needs"
    );
    let keys: Vec<&str> = revs.iter().map(|r| r.key.as_str()).collect();
    for i in 0..5 {
        assert!(keys.contains(&format!("http://p{i}").as_str()), "{keys:?}");
    }
}

#[tokio::test]
async fn backfill_declares_its_dataset_though_it_echoes_no_records() {
    // Backfill is the widest mode and has never echoed a record, so before the
    // declaration its output had NO search coverage at all beyond one
    // whole-result document.
    let store = TempStore::new("extract-echo-backfill").await;
    let root = store.path().to_path_buf();
    let crawl_job = Uuid::new_v4().to_string();
    let dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("v1.html"), b"<h1>v1</h1>")
        .await
        .unwrap();
    store
        .datasets()
        .upsert_many(
            "crawl",
            "page_versions",
            &[(
                "http://p#1".into(),
                json!({"url": "http://p", "revision": 1, "artifact_path": "v1.html",
                       "job_id": crawl_job, "simhash": 1,
                       "fetched_at": "2026-01-05T00:00:00+00:00"}),
            )],
        )
        .await
        .unwrap();

    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            json!({
                "source": {"app": "crawl", "dataset": "pages", "backfill": true},
                "rules": {"h": {"type": "css", "selector": "h1"}},
                "dataset": "h_history"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(out["loaded"], 1, "{out}");
    assert!(out.get("records").is_none(), "backfill never echoes: {out}");
    assert_eq!(
        out["index_datasets"],
        json!([{ "app": "extractor", "dataset": "h_history" }]),
        "{out}"
    );
}

#[tokio::test]
async fn a_quarantined_run_names_its_shadow_dataset_and_keeps_it_out_of_the_index() {
    // Two claims at once. (a) The result names `prices@q`, where the rows
    // really went — before this the caller had no way to know their extraction
    // had been diverted. (b) The spec is WITHHELD: the worker's own gate reads
    // the health of the spec's pair, and `("extractor", "prices@q")` is a pair
    // no `observe_extraction` ever judges, so it would always read Healthy and
    // wave quarantined rows into the index that saved-search alerts fire from.
    let store = TempStore::new("extract-echo-quarantine").await;
    let root = seed_pages(&store, 2).await;
    let health = Arc::new(Resilience::new(
        store.storage.pool(),
        &ResilienceConfig {
            enforce: true,
            ..ResilienceConfig::default()
        },
    ));
    let store_h = health.store().expect("resilience store");
    store_h.ensure_source("extractor", "prices").await.unwrap();
    store_h
        .set_state_manual("extractor/prices", SourceState::Quarantined, "test")
        .await
        .unwrap();

    let ctx = TestContext::new(&store.storage, "extractor")
        .params(source_params(json!({})))
        .health(Arc::clone(&health))
        .artifacts_dir(root.join("extractor").join("job"))
        .build();
    let out = Extractor.run(ctx).await.unwrap();

    assert_eq!(out["dataset"], "prices@q", "{out}");
    assert!(
        out.get("index_datasets").is_none(),
        "a quarantined source must not offer its rows to the index: {out}"
    );
    // ...and the rows really are in the shadow dataset, not the live one.
    assert!(store
        .datasets()
        .get("extractor", "prices@q", "http://p0")
        .await
        .unwrap()
        .is_some());
    assert!(store
        .datasets()
        .get("extractor", "prices", "http://p0")
        .await
        .unwrap()
        .is_none());
}
