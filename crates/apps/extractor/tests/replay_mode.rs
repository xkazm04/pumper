//! End-to-end tests for the extractor's replay-CI mode: candidate rules run
//! over STORED bodies, diffed field-by-field against a baseline, with the
//! read-only contract (no dataset writes, ever) asserted against the real
//! store. Engines are the harness's panicking stubs — replay must never fetch.

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

/// Seeds two live `pages` records with stored bodies: `http://a` has both an
/// old-style and a new-style price node; `http://b` has only the old style.
async fn seed_pages(store: &TempStore) -> std::path::PathBuf {
    let root = store.path().to_path_buf();
    let job = Uuid::new_v4().to_string();
    let dir = root.join("crawl").join(&job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(
        dir.join("a.html"),
        b"<h1>A</h1><span class=\"price\">9</span><span class=\"price-v2\">9.00</span>",
    )
    .await
    .unwrap();
    tokio::fs::write(
        dir.join("b.html"),
        b"<h1>B</h1><span class=\"price\">8</span>",
    )
    .await
    .unwrap();
    store
        .datasets()
        .upsert_many(
            "crawl",
            "pages",
            &[
                (
                    "http://a".into(),
                    json!({"url":"http://a","artifact_path":"a.html","job_id":job}),
                ),
                (
                    "http://b".into(),
                    json!({"url":"http://b","artifact_path":"b.html","job_id":job}),
                ),
            ],
        )
        .await
        .unwrap();
    root
}

#[tokio::test]
async fn replay_diffs_candidate_against_baseline_and_writes_no_datasets() {
    let store = TempStore::new("replay-diff").await;
    let root = seed_pages(&store).await;

    // Candidate migrates price to .price-v2 (only http://a has it) and adds a
    // heading field the baseline never extracted.
    let params = json!({
        "replay": {
            "rules": {
                "price": {"type":"css","selector":".price-v2"},
                "heading": {"type":"css","selector":"h1"}
            },
            "baseline_rules": {
                "price": {"type":"css","selector":".price"}
            }
        }
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();

    assert_eq!(out["mode"], "replay");
    assert_eq!(out["baseline"], true);
    assert_eq!(out["docs"], 2, "{out}");
    assert_eq!(out["urls_matching"], 2);
    assert_eq!(out["truncated"], false);

    // price regressed on http://b (baseline matched, candidate lost) and
    // changed value on http://a — sorted worst-first, so price leads.
    let fields = out["fields"].as_array().unwrap();
    let price = &fields[0];
    assert_eq!(price["field"], "price", "{out}");
    assert_eq!(price["baseline_match_rate"], 1.0);
    assert_eq!(price["match_rate"], 0.5);
    assert_eq!(price["delta"], -0.5);
    assert_eq!(price["lost"]["count"], 1);
    assert_eq!(price["lost"]["samples"][0]["url"], "http://b");
    assert_eq!(price["lost"]["samples"][0]["value"], "8");
    assert_eq!(price["changed"]["count"], 1);
    assert_eq!(price["changed"]["samples"][0]["from"], "9");
    assert_eq!(price["changed"]["samples"][0]["to"], "9.00");
    // heading is all-added (baseline had no such field).
    let heading = &fields[1];
    assert_eq!(heading["field"], "heading");
    assert_eq!(heading["added"]["count"], 2);
    // Per-URL regressions attribute the damage.
    assert_eq!(out["regressed_urls"], 2);
    let regs = out["regressions"].as_array().unwrap();
    assert_eq!(regs[0]["url"], "http://a");
    assert_eq!(regs[0]["changed"][0], "price");
    assert_eq!(regs[1]["url"], "http://b");
    assert_eq!(regs[1]["lost"][0], "price");

    // READ-ONLY invariant: the only datasets in the store are the crawl seeds
    // — replay wrote no record anywhere (not even an empty summary row).
    let all = store.datasets().list_all_datasets().await.unwrap();
    assert_eq!(
        all,
        vec![("crawl".to_string(), "pages".to_string())],
        "replay must not write datasets"
    );
    assert!(
        out.get("new").is_none(),
        "no upsert summary in replay output"
    );

    // The report landed as a job artifact and round-trips.
    let artifact = root
        .join("extractor")
        .join("job")
        .join("replay-report.json");
    let bytes = tokio::fs::read(&artifact)
        .await
        .expect("replay-report.json");
    let report: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(report["mode"], "replay");
    assert_eq!(report["fields"][0]["field"], "price");
    assert_eq!(out["artifact"], "replay-report.json");
}

#[tokio::test]
async fn replay_versions_all_bisects_the_boundary_where_a_field_broke() {
    let store = TempStore::new("replay-bisect").await;
    let root = store.path().to_path_buf();
    let job = Uuid::new_v4().to_string();
    let dir = root.join("crawl").join(&job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    // Jan: h1 present. Mar: markup changed, h1 gone. Live: still gone.
    tokio::fs::write(dir.join("p.r1.html"), b"<h1>old</h1>")
        .await
        .unwrap();
    tokio::fs::write(dir.join("p.r2.html"), b"<div class=\"t\">new</div>")
        .await
        .unwrap();
    tokio::fs::write(dir.join("p.html"), b"<div class=\"t\">newer</div>")
        .await
        .unwrap();
    let datasets = store.datasets();
    datasets
        .upsert_many(
            "crawl",
            "pages",
            &[(
                "http://p".into(),
                json!({"url":"http://p","artifact_path":"p.html","job_id":job}),
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
                    json!({"url":"http://p","revision":1,"artifact_path":"p.r1.html",
                           "job_id":job,"simhash":1,
                           "fetched_at":"2026-01-05T00:00:00+00:00"}),
                ),
                (
                    "http://p#2".into(),
                    json!({"url":"http://p","revision":2,"artifact_path":"p.r2.html",
                           "job_id":job,"simhash":2,
                           "fetched_at":"2026-03-05T00:00:00+00:00"}),
                ),
            ],
        )
        .await
        .unwrap();

    let params = json!({
        "replay": {
            "rules": {"h": {"type":"css","selector":"h1"}},
            "against": {"versions": "all"},
            "bisect_field": "h"
        }
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    // Two archived versions + the live body.
    assert_eq!(out["docs"], 3, "{out}");
    let boundaries = out["bisect"]["boundaries"].as_array().unwrap();
    assert_eq!(boundaries.len(), 1, "exactly one flip: {out}");
    assert_eq!(boundaries[0]["url"], "http://p");
    assert_eq!(
        boundaries[0]["from"]["observed_at"],
        "2026-01-05T00:00:00+00:00"
    );
    assert_eq!(boundaries[0]["from"]["matched"], true);
    assert_eq!(
        boundaries[0]["to"]["observed_at"],
        "2026-03-05T00:00:00+00:00"
    );
    assert_eq!(boundaries[0]["to"]["matched"], false);
    // Still read-only in versions mode.
    let all = store.datasets().list_all_datasets().await.unwrap();
    assert!(
        all.iter().all(|(app, _)| app == "crawl"),
        "replay wrote a dataset: {all:?}"
    );
}

#[tokio::test]
async fn replay_url_pattern_and_max_pages_bound_the_corpus() {
    let store = TempStore::new("replay-bounds").await;
    let root = seed_pages(&store).await;

    // Pattern narrows to one URL.
    let params = json!({
        "replay": {
            "rules": {"h": {"type":"css","selector":"h1"}},
            "against": {"url_pattern": "^http://a$"}
        }
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    assert_eq!(out["urls_matching"], 1, "{out}");
    assert_eq!(out["docs"], 1);
    assert_eq!(out["truncated"], false);

    // max_pages truncates honestly: full match count, capped docs, flag set.
    let params = json!({
        "replay": {
            "rules": {"h": {"type":"css","selector":"h1"}},
            "against": {"max_pages": 1}
        }
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    assert_eq!(out["urls_matching"], 2, "{out}");
    assert_eq!(out["docs"], 1);
    assert_eq!(out["truncated"], true);
}
