//! M12 provenance adoption: the extractor is the one app in the fleet that can
//! state a REAL `rules_hash` — the content-addressed pin of the rule set that
//! produced each record — so these tests hold that stamp (and the deliberately
//! honest-Null `source_url`) to the contract. Source mode is used throughout so
//! nothing is fetched (the harness engines panic on any fetch).

use std::path::Path;

use app_extractor::Extractor;
use pumper_core::datasets::rules_hash;
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

/// Seeds `n` crawl `pages` records whose stored bodies all exist on disk.
async fn seed_pages(store: &TempStore, urls: &[&str]) -> std::path::PathBuf {
    let root = store.path().to_path_buf();
    let crawl_job = Uuid::new_v4().to_string();
    let dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let mut items = Vec::new();
    for (i, url) in urls.iter().enumerate() {
        let file = format!("page-{i}.html");
        tokio::fs::write(dir.join(&file), format!("<h1>{url}</h1>").as_bytes())
            .await
            .unwrap();
        items.push((
            (*url).to_string(),
            json!({"url": url, "artifact_path": file, "job_id": crawl_job}),
        ));
    }
    store
        .datasets()
        .upsert_many("crawl", "pages", &items)
        .await
        .unwrap();
    root
}

const RULES: fn() -> Value = || json!({"h": {"type": "css", "selector": "h1"}});

#[tokio::test]
async fn every_record_is_stamped_with_the_registered_rules_hash() {
    let store = TempStore::new("extract-prov-rules").await;
    let root = seed_pages(&store, &["http://a", "http://b"]).await;

    let params = json!({
        "source": {"app": "crawl", "dataset": "pages"},
        "rules": RULES(),
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    assert_eq!(out["loaded"], 2, "{out}");

    // The hash stamped on the revision IS the canonical hash of the rules the
    // job was given — not a per-run id — so two runs of the same rules pin the
    // same version and a rule edit is visible as a different pin.
    let expected = rules_hash(&RULES());
    let datasets = store.datasets();
    for key in ["http://a", "http://b"] {
        let rev = datasets
            .history("extractor", "extracted", key, 1)
            .await
            .unwrap()
            .into_iter()
            .next()
            .expect("one revision per new record");
        assert_eq!(
            rev.provenance.rules_hash.as_deref(),
            Some(expected.as_str()),
            "record {key} must pin the ruleset that produced it"
        );
        // The producing job is always known and always stamped by the runtime.
        assert!(rev.provenance.job_id.is_some());
    }

    // …and the rules themselves are retrievable by that hash, which is what
    // makes the stamp a re-derivation pin rather than an opaque fingerprint.
    let registered = datasets.rules_by_hash(&expected).await.unwrap();
    assert_eq!(registered.as_ref(), Some(&RULES()));
}

#[tokio::test]
async fn source_url_is_claimed_only_when_the_whole_batch_shares_one() {
    let store = TempStore::new("extract-prov-url").await;
    let root = seed_pages(&store, &["http://a", "http://b"]).await;
    let datasets = store.datasets();

    // Mixed batch: no single URL is true of every record, so naming one would
    // be a fabrication — the stamp stays Null.
    let params = json!({
        "source": {"app": "crawl", "dataset": "pages"},
        "rules": RULES(),
    });
    Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    let rev = datasets
        .history("extractor", "extracted", "http://a", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        rev.provenance.source_url, None,
        "a multi-URL batch must not name one of its URLs"
    );

    // Single-document batch: the URL is true of every record in it, so it is
    // stamped.
    let params = json!({
        "source": {"app": "crawl", "dataset": "pages", "keys": ["http://b"]},
        "rules": RULES(),
        "dataset": "single",
    });
    Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    let rev = datasets
        .history("extractor", "single", "http://b", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(rev.provenance.source_url.as_deref(), Some("http://b"));
}

#[tokio::test]
async fn backfill_resumes_from_its_checkpoint_instead_of_re_scanning() {
    let store = TempStore::new("extract-prov-resume").await;
    let root = seed_pages(&store, &["http://a"]).await;

    // A prior attempt's checkpoint: cursor already past the (single) archive
    // page, with its tallies. The resumed attempt must carry those forward and
    // scan nothing more, rather than restarting the whole archive walk.
    let restored = json!({
        "v": 1,
        "after": ["9999-01-01T00:00:00.000Z", "zzz"],
        "scanned": 7,
        "skipped_pattern": 2,
        "loaded": 5,
        "batches": 1,
        "new": 5,
        "changed": 0,
        "unchanged": 0,
        "fields_matched": 5,
        "fields_total": 5,
    });
    let ctx = TestContext::new(&store.storage, "extractor")
        .params(json!({
            "source": {"app": "crawl", "dataset": "pages", "backfill": true},
            "rules": RULES(),
        }))
        .artifacts_dir(root.join("extractor").join("job"))
        .restored(restored)
        .build();
    let out = Extractor.run(ctx).await.unwrap();

    assert_eq!(out["mode"], "backfill");
    assert_eq!(out["resumed_from_checkpoint"], true);
    assert_eq!(out["scanned"], 7, "prior progress must not be recounted");
    assert_eq!(out["new"], 5);
    assert_eq!(out["fields_matched"], 5);
}

#[tokio::test]
async fn an_unusable_checkpoint_restarts_the_scan_rather_than_erroring() {
    let store = TempStore::new("extract-prov-poison").await;
    let root = seed_pages(&store, &["http://a"]).await;

    let ctx = TestContext::new(&store.storage, "extractor")
        .params(json!({
            "source": {"app": "crawl", "dataset": "pages", "backfill": true},
            "rules": RULES(),
        }))
        .artifacts_dir(root.join("extractor").join("job"))
        // Foreign shape / future version: advisory, so it means "start fresh".
        .restored(json!({"v": 99, "after": ["x", "y"], "scanned": 4242}))
        .build();
    let out = Extractor.run(ctx).await.unwrap();
    assert_eq!(out["resumed_from_checkpoint"], false);
    assert_eq!(out["scanned"], 0, "no page_versions rows exist: {out}");
}
