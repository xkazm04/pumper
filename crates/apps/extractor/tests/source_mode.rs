//! End-to-end test for the extractor's `source` mode (the crawl → extract seam).
//! Builds a real temp-dir SQLite + `Datasets`, seeds `pages`-style records with
//! `artifact_path` + `job_id`, writes a body to the origin job's artifacts dir,
//! then runs the app and asserts it extracts from the stored body (never
//! fetching — the harness engines panic on any fetch) and reports
//! missing/unreadable artifacts per key.

use std::path::Path;

use app_extractor::Extractor;
use pumper_core::testing::{TempStore, TestContext};
use pumper_core::{AppContext, ScrapeApp};
use serde_json::{json, Value};
use uuid::Uuid;

/// Health detection on, enforcement off — the shipping default, so these tests
/// also cover that observing a run never changes what the app returns. The
/// artifacts dir must be `<root>/extractor/<job>` so the app resolves the
/// shared artifacts root two levels up.
fn ctx_with(root: &Path, store: &TempStore, params: Value) -> AppContext {
    TestContext::new(&store.storage, "extractor")
        .params(params)
        .artifacts_dir(root.join("extractor").join("job"))
        .build()
}

#[tokio::test]
async fn source_mode_extracts_stored_bodies_and_reports_missing() {
    let store = TempStore::new("extract-source").await;
    let root = store.path().to_path_buf();
    let datasets = store.datasets();

    // The origin crawl job wrote one body to disk under its per-job dir.
    let crawl_job = Uuid::new_v4().to_string();
    let crawl_dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&crawl_dir).await.unwrap();
    tokio::fs::write(
        crawl_dir.join("page-0001.html"),
        b"<html><h1>Hello World</h1></html>",
    )
    .await
    .unwrap();

    // Seed pages: (a) present body, (b) body path points at a missing file,
    // (c) record has no artifact_path. Key = canonical URL, as the crawl writes.
    datasets
        .upsert_many(
            "crawl",
            "pages",
            &[
                (
                    "http://a".into(),
                    json!({"url":"http://a","artifact_path":"page-0001.html","job_id":crawl_job}),
                ),
                (
                    "http://b".into(),
                    json!({"url":"http://b","artifact_path":"page-9999.html","job_id":crawl_job}),
                ),
                (
                    "http://c".into(),
                    json!({"url":"http://c","job_id":crawl_job}),
                ),
            ],
        )
        .await
        .unwrap();

    // Explicit keys, including one (`http://d`) with no record at all.
    let params = json!({
        "source": {"app":"crawl","dataset":"pages",
                   "keys":["http://a","http://b","http://c","http://d"]},
        "rules": {"headline": {"type":"css","selector":"h1"}}
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();

    assert_eq!(out["mode"], "source");
    assert_eq!(out["requested"], 4);
    assert_eq!(out["loaded"], 1, "only http://a has a readable body: {out}");
    assert_eq!(
        out["missing"], 3,
        "b unreadable, c no artifact_path, d no record: {out}"
    );
    // The one loaded doc extracted from the STORED body (engines would panic).
    assert_eq!(out["records"][0]["headline"], "Hello World");
    assert_eq!(out["records"][0]["_url"], "http://a");
    assert_eq!(out["new"], 1);
    assert_eq!(out["fields_matched"], 1);
    assert_eq!(out["fields_total"], 1);
    // Missing reasons are attributed per key.
    let missing: Vec<String> = out["missing_keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["key"].as_str().unwrap().into())
        .collect();
    assert!(missing.contains(&"http://b".to_string()));
    assert!(missing.contains(&"http://c".to_string()));
    assert!(missing.contains(&"http://d".to_string()));

    // The extracted fields landed in the `extracted` dataset under the extractor app.
    let stored = datasets
        .get("extractor", "extracted", "http://a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.data["headline"], "Hello World");
}

#[tokio::test]
async fn source_mode_without_keys_sweeps_live_records() {
    let store = TempStore::new("extract-sweep").await;
    let root = store.path().to_path_buf();
    let datasets = store.datasets();

    let crawl_job = Uuid::new_v4().to_string();
    let crawl_dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&crawl_dir).await.unwrap();
    tokio::fs::write(crawl_dir.join("p.html"), b"<h1>Only</h1>")
        .await
        .unwrap();

    datasets
        .upsert_many(
            "crawl",
            "pages",
            &[(
                "http://only".into(),
                json!({"url":"http://only","artifact_path":"p.html","job_id":crawl_job}),
            )],
        )
        .await
        .unwrap();

    // No keys, no trigger → sweep all live records.
    let params = json!({
        "source": {"app":"crawl","dataset":"pages"},
        "rules": {"h": {"type":"css","selector":"h1"}}
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    assert_eq!(out["requested"], 1);
    assert_eq!(out["loaded"], 1);
    assert_eq!(out["records"][0]["h"], "Only");
}
