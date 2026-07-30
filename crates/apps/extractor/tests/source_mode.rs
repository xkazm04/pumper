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

/// Seeds one URL's versioned archive: a live `pages` record (current body) plus
/// two `page_versions` records with revision-suffixed artifact copies, exactly
/// the shape the crawl app's DatasetPageSink writes on `changed`.
async fn seed_versioned_archive(store: &pumper_core::testing::TempStore) -> std::path::PathBuf {
    let root = store.path().to_path_buf();
    let datasets = store.datasets();
    let crawl_job = Uuid::new_v4().to_string();
    let dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("page-aa.r1.html"), b"<h1>v1</h1>")
        .await
        .unwrap();
    tokio::fs::write(dir.join("page-aa.r2.html"), b"<h1>v2</h1>")
        .await
        .unwrap();
    tokio::fs::write(dir.join("page-aa.html"), b"<h1>v3</h1>")
        .await
        .unwrap();
    datasets
        .upsert_many(
            "crawl",
            "pages",
            &[(
                "http://p".into(),
                json!({"url":"http://p","artifact_path":"page-aa.html","job_id":crawl_job}),
            )],
        )
        .await
        .unwrap();
    datasets
        .upsert_many(
            "crawl",
            "page_versions",
            &[
                (
                    "http://p#1".into(),
                    json!({"url":"http://p","revision":1,"artifact_path":"page-aa.r1.html",
                           "job_id":crawl_job,"simhash":1,
                           "fetched_at":"2026-01-05T00:00:00+00:00"}),
                ),
                (
                    "http://p#2".into(),
                    json!({"url":"http://p","revision":2,"artifact_path":"page-aa.r2.html",
                           "job_id":crawl_job,"simhash":2,
                           "fetched_at":"2026-03-05T00:00:00+00:00"}),
                ),
            ],
        )
        .await
        .unwrap();
    root
}

#[tokio::test]
async fn source_mode_as_of_resolves_the_archived_version() {
    let store = TempStore::new("extract-asof").await;
    let root = seed_versioned_archive(&store).await;

    // Between v1 (Jan) and v2 (Mar) → v1's body; keyed {url}@{date}, tagged.
    let params = json!({
        "source": {"app":"crawl","dataset":"pages","keys":["http://p"],
                   "as_of":"2026-02-01T00:00:00Z"},
        "rules": {"h": {"type":"css","selector":"h1"}}
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    assert_eq!(out["loaded"], 1, "{out}");
    assert_eq!(out["records"][0]["h"], "v1");
    assert_eq!(out["records"][0]["_url"], "http://p");
    assert_eq!(
        out["records"][0]["_observed_at"],
        "2026-01-05T00:00:00+00:00"
    );
    let stored = store
        .datasets()
        .get("extractor", "extracted", "http://p@2026-01-05")
        .await
        .unwrap()
        .expect("record keyed {url}@{date}");
    assert_eq!(stored.data["h"], "v1");

    // An as_of before the first observation is an honest miss, never the present.
    let params = json!({
        "source": {"app":"crawl","dataset":"pages","keys":["http://p"],
                   "as_of":"2025-01-01T00:00:00Z"},
        "rules": {"h": {"type":"css","selector":"h1"}}
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    assert_eq!(out["loaded"], 0, "{out}");
    assert_eq!(out["missing"], 1);
}

#[tokio::test]
async fn source_mode_versions_all_fans_over_archive_plus_live() {
    let store = TempStore::new("extract-versions").await;
    let root = seed_versioned_archive(&store).await;

    let params = json!({
        "source": {"app":"crawl","dataset":"pages","keys":["http://p"],"versions":"all"},
        "rules": {"h": {"type":"css","selector":"h1"}}
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    // Two archived versions + the current live body.
    assert_eq!(out["loaded"], 3, "{out}");
    let hs: Vec<&str> = out["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["h"].as_str().unwrap())
        .collect();
    assert!(
        hs.contains(&"v1") && hs.contains(&"v2") && hs.contains(&"v3"),
        "{hs:?}"
    );
}

#[tokio::test]
async fn backfill_fans_over_matching_versions_only() {
    let store = TempStore::new("extract-backfill").await;
    let root = seed_versioned_archive(&store).await;

    // Add an archive row for a URL the pattern must exclude.
    let crawl_job = Uuid::new_v4().to_string();
    let dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("other.r1.html"), b"<h1>other</h1>")
        .await
        .unwrap();
    store
        .datasets()
        .upsert_many(
            "crawl",
            "page_versions",
            &[(
                "http://other#1".into(),
                json!({"url":"http://other","revision":1,"artifact_path":"other.r1.html",
                       "job_id":crawl_job,"simhash":9,
                       "fetched_at":"2026-02-01T00:00:00+00:00"}),
            )],
        )
        .await
        .unwrap();

    let params = json!({
        "source": {"app":"crawl","dataset":"pages","backfill":true,"url_pattern":"^http://p$"},
        "rules": {"h": {"type":"css","selector":"h1"}},
        "dataset": "h_history"
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    assert_eq!(out["mode"], "backfill");
    assert_eq!(out["scanned"], 3, "{out}");
    assert_eq!(out["skipped_pattern"], 1);
    assert_eq!(out["loaded"], 2);
    assert_eq!(out["new"], 2);
    // Time-series keying: one record per observation date.
    let v1 = store
        .datasets()
        .get("extractor", "h_history", "http://p@2026-01-05")
        .await
        .unwrap()
        .expect("v1 backfill record");
    assert_eq!(v1.data["h"], "v1");
    assert_eq!(v1.data["_observed_at"], "2026-01-05T00:00:00+00:00");
    let v2 = store
        .datasets()
        .get("extractor", "h_history", "http://p@2026-03-05")
        .await
        .unwrap()
        .expect("v2 backfill record");
    assert_eq!(v2.data["h"], "v2");
}
