//! What the persisted job result CLAIMS must be what the run did.
//!
//! Five things it used to get wrong, each proved here against the real app over
//! a real store:
//!
//! 1. the manifest's `output_shape` promised `{extracted, errors, removed?}` —
//!    keys no mode has ever emitted;
//! 2. the no-keys source sweep stopped at a record cap and reported the capped
//!    count as `requested`, with no signal that more rows existed;
//! 3. the backfill mode dropped `worst_fields` and the health verdict entirely,
//!    while reporting the pooled counters those numbers break down;
//! 4. a failed rule-set registration was a log line — the run returned 200 with
//!    permanently non-replayable revisions and no trace in the result;
//! 5. no mode named the dataset it actually wrote to, even though enforcement
//!    can divert a quarantined source to `<dataset>@q`.

use std::path::Path;

use app_extractor::Extractor;
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
async fn seed_pages(store: &TempStore, n: usize, body: &str) -> std::path::PathBuf {
    let root = store.path().to_path_buf();
    let crawl_job = Uuid::new_v4().to_string();
    let dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("p.html"), body.as_bytes())
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

#[tokio::test]
async fn a_capped_source_sweep_says_so_instead_of_looking_complete() {
    let store = TempStore::new("extract-sweep-cap").await;
    let root = seed_pages(&store, 3, "<h1>Hi</h1>").await;

    // The cap bites: 3 live records, sweep 2. `requested: 2` used to be the
    // ONLY number reported, indistinguishable from a 2-record dataset.
    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            json!({
                "source": {"app": "crawl", "dataset": "pages", "limit": 2},
                "rules": {"h": {"type": "css", "selector": "h1"}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(out["mode"], "source");
    assert_eq!(out["requested"], 2, "{out}");
    assert_eq!(out["limit"], 2);
    assert_eq!(out["truncated"], true, "the cap decided where it stopped");

    // The control arm: a cap the dataset does not reach is NOT truncation, or
    // the flag would be noise on every run.
    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            json!({
                "source": {"app": "crawl", "dataset": "pages", "limit": 50},
                "rules": {"h": {"type": "css", "selector": "h1"}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(out["requested"], 3, "{out}");
    assert_eq!(out["truncated"], false);
}

#[tokio::test]
async fn explicit_keys_are_never_reported_as_a_truncated_sweep() {
    // The caller named the set, so no cap applied to it — claiming truncation
    // would be as wrong as hiding it.
    let store = TempStore::new("extract-sweep-keys").await;
    let root = seed_pages(&store, 3, "<h1>Hi</h1>").await;
    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            json!({
                "source": {"app": "crawl", "dataset": "pages",
                           "keys": ["http://p0"], "limit": 1},
                "rules": {"h": {"type": "css", "selector": "h1"}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(out["requested"], 1);
    assert_eq!(out["truncated"], false, "{out}");
}

#[tokio::test]
async fn a_write_mode_names_the_dataset_it_wrote_and_the_rules_it_pinned() {
    let store = TempStore::new("extract-names-dataset").await;
    let root = seed_pages(&store, 1, "<h1>Hi</h1>").await;
    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            json!({
                "source": {"app": "crawl", "dataset": "pages"},
                "rules": {"h": {"type": "css", "selector": "h1"}},
                "dataset": "prices"
            }),
        ))
        .await
        .unwrap();
    // Healthy source → the requested name; a quarantined one would read
    // `prices@q`, which is the case that used to be invisible.
    assert_eq!(out["dataset"], "prices", "{out}");
    // The provenance pin is reported, not just stamped — and honest-null would
    // have carried a reason alongside it.
    assert!(
        out["rules_hash"].as_str().is_some_and(|h| !h.is_empty()),
        "the registered rules hash belongs in the result: {out}"
    );
    assert!(out.get("rules_registration_error").is_none());
}

#[tokio::test]
async fn backfill_reports_the_quality_breakdown_behind_its_own_counters() {
    // THE REFUTED BEHAVIOR: backfill reported pooled `fields_matched` /
    // `fields_total` but no `worst_fields` and no `health` — the two signals
    // that say WHICH selector rotted and whether the source is degrading. A
    // multi-thousand-revision backfill could report a 50% match rate with
    // nothing naming the field that produced it.
    let store = TempStore::new("extract-backfill-quality").await;
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
                "rules": {
                    "h": {"type": "css", "selector": "h1"},
                    "price": {"type": "css", "selector": ".price-that-is-gone"}
                },
                "dataset": "h_history"
            }),
        ))
        .await
        .unwrap();

    assert_eq!(out["mode"], "backfill");
    assert_eq!(out["loaded"], 1, "{out}");
    assert_eq!(out["fields_matched"], 1);
    assert_eq!(out["fields_total"], 2);
    // The breakdown those two numbers summarize is now present, and names the
    // dead selector rather than leaving the reader to guess.
    let worst = out["worst_fields"].as_array().expect("worst_fields");
    assert_eq!(worst.len(), 1, "{out}");
    assert_eq!(worst[0]["field"], "price");
    assert_eq!(worst[0]["misses"], 1);
    // The health verdict of the run's final batch, like every other write mode.
    assert!(!out["health"].is_null(), "backfill lost its verdict: {out}");
    // ...and it names where the batches landed + what pinned them.
    assert_eq!(out["dataset"], "h_history");
    assert!(out["rules_hash"].as_str().is_some_and(|h| !h.is_empty()));
}

#[tokio::test]
async fn a_backfill_that_wrote_nothing_names_no_dataset() {
    // Honest-null: a target for a write that never happened is a claim, and the
    // whole point of naming the dataset is that the name can be trusted.
    let store = TempStore::new("extract-backfill-empty").await;
    let root = seed_pages(&store, 1, "<h1>Hi</h1>").await;
    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            json!({
                "source": {"app": "crawl", "dataset": "pages", "backfill": true},
                "rules": {"h": {"type": "css", "selector": "h1"}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(out["loaded"], 0, "no page_versions rows exist: {out}");
    assert_eq!(out["dataset"], Value::Null, "{out}");
}

/// The manifest's `output_shape` is the contract a consumer codes against. An
/// EXPECTED-diff over the write-mode keys: every key it promises must actually
/// appear in a real write run, and the three keys it used to invent must be
/// gone.
#[tokio::test]
async fn output_shape_promises_only_keys_a_real_run_returns() {
    let shape = Extractor
        .manifest()
        .output_shape
        .expect("extractor declares an output shape");
    for invented in ["{extracted", "errors,", "removed?"] {
        assert!(
            !shape.contains(invented),
            "output_shape still promises `{invented}`, which no mode emits: {shape}"
        );
    }

    let store = TempStore::new("extract-output-shape").await;
    let root = seed_pages(&store, 1, "<h1>Hi</h1>").await;
    let out = Extractor
        .run(ctx_with(
            &root,
            &store,
            json!({
                "source": {"app": "crawl", "dataset": "pages"},
                "rules": {"h": {"type": "css", "selector": "h1"}}
            }),
        ))
        .await
        .unwrap();

    // Every key the shape names for a write mode, present in a real result.
    const WRITE_MODE_KEYS: &[&str] = &[
        "mode",
        "dataset",
        "new",
        "changed",
        "unchanged",
        "fields_matched",
        "fields_total",
        "worst_fields",
        "base_url_missing",
        "health",
        "rules_hash",
        "records",
    ];
    const SOURCE_MODE_KEYS: &[&str] = &[
        "source",
        "requested",
        "limit",
        "truncated",
        "loaded",
        "missing",
        "missing_keys",
    ];
    for key in WRITE_MODE_KEYS.iter().chain(SOURCE_MODE_KEYS) {
        assert!(out.get(*key).is_some(), "result is missing `{key}`: {out}");
        assert!(
            shape.contains(key),
            "output_shape does not name `{key}`, which the result carries"
        );
    }
}
