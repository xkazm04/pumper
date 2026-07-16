//! Integration test for dataset upsert atomicity under concurrency, against a
//! real temp-dir SQLite (WAL + busy_timeout) with the full migration chain.
//! Proves that concurrent same-key writers do not corrupt the per-key revision
//! chain — the bug that motivated wrapping upsert in a BEGIN IMMEDIATE
//! transaction (SELECT + record write + revision append as one atomic unit).

use std::sync::Arc;

use pumper_core::config::StorageConfig;
use pumper_core::{Datasets, Storage};
use serde_json::json;
use uuid::Uuid;

async fn fresh_db(tag: &str) -> (Storage, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("pumper-{tag}-{}", Uuid::new_v4()));
    let cfg = StorageConfig {
        database_path: dir.join("pumper.db"),
        artifacts_dir: dir.join("artifacts"),
    };
    let storage = Storage::connect(&cfg).await.expect("connect + migrate");
    (storage, dir)
}

#[tokio::test]
async fn reindex_rewrites_stale_simhashes_without_touching_content() {
    let (storage, dir) = fresh_db("datasets-reindex").await;
    let pool = storage.pool();
    let ds = Datasets::new(storage.pool());

    ds.upsert("app", "d", "k", &json!({ "title": "hello world simhash reindex" }))
        .await
        .unwrap();

    // What the current hash should produce, plus the content fields that must NOT move.
    let (correct_sim, hash_before, updated_before): (i64, String, String) =
        sqlx::query_as("SELECT simhash, hash, updated_at FROM records WHERE key = 'k'")
            .fetch_one(&pool)
            .await
            .unwrap();

    // Simulate a fingerprint left behind by an older token hash.
    sqlx::query("UPDATE records SET simhash = 12345 WHERE key = 'k'")
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(ds.reindex_simhashes().await.unwrap(), 1, "stale row must be rewritten");

    let (sim_after, hash_after, updated_after): (i64, String, String) =
        sqlx::query_as("SELECT simhash, hash, updated_at FROM records WHERE key = 'k'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sim_after, correct_sim, "simhash recomputed from the stored data");
    // Content hash + timestamps untouched → the change-feed sees no fake revision.
    assert_eq!(hash_after, hash_before, "content hash must not move");
    assert_eq!(updated_after, updated_before, "updated_at must not move");

    // Idempotent: a second run finds nothing to rewrite.
    assert_eq!(ds.reindex_simhashes().await.unwrap(), 0, "reindex must be idempotent");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_key_upserts_keep_revision_chain_intact() {
    let (storage, dir) = fresh_db("datasets-concurrency").await;
    let pool = storage.pool();
    let ds = Arc::new(Datasets::new(storage.pool()));

    // 20 concurrent writers, each upserting the SAME key with a DISTINCT value.
    // Serialized correctly, each observes a different prior and appends exactly
    // one revision → a contiguous 1..=20 chain. The pre-fix non-atomic path let
    // two writers compute the same MAX(revision)+1 (duplicate/aborted revisions).
    const N: i64 = 20;
    let mut handles = Vec::new();
    for i in 0..N {
        let ds = ds.clone();
        handles.push(tokio::spawn(async move {
            ds.upsert("app", "d", "k", &json!({ "v": i })).await
        }));
    }
    for h in handles {
        h.await.expect("task joined").expect("upsert ok");
    }

    // Exactly one record for the key.
    let record_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM records WHERE app = 'app' AND dataset = 'd' AND key = 'k'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(record_count, 1, "the key must resolve to a single record");

    // Revision numbers are exactly 1..=N — contiguous, unique, none lost.
    let revisions: Vec<i64> = sqlx::query_scalar(
        "SELECT revision FROM record_revisions \
         WHERE app = 'app' AND dataset = 'd' AND key = 'k' ORDER BY revision",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let expected: Vec<i64> = (1..=N).collect();
    assert_eq!(
        revisions, expected,
        "revision chain must be contiguous 1..={N} with no duplicates or gaps"
    );

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn list_filtered_ordered_returns_soonest_rows_past_the_cap() {
    // The closing-soon correctness bug: ordering by close_date must happen in SQL
    // *before* the LIMIT, or a small cap returns an arbitrary (updated_at) slice
    // that an in-memory sort only reorders — silently dropping a grant closing
    // tomorrow. Seed more matches than the cap, with close dates in shuffled
    // insert order, and assert the cap returns the genuinely soonest ones.
    use pumper_core::datasets::JsonFilter;

    let (storage, dir) = fresh_db("datasets-ordered").await;
    let ds = Datasets::new(storage.pool());

    // Insert 10 open grants with close dates 2026-03-10 .. 2026-03-01 in an order
    // that is NOT close-date order (so updated_at order != close_date order).
    let order = [5, 9, 1, 7, 3, 10, 2, 8, 4, 6];
    for day in order {
        let key = format!("g{day:02}");
        let close = format!("2026-03-{day:02}");
        ds.upsert("grants", "unified", &key, &json!({ "status": "open", "close_date": close }))
            .await
            .unwrap();
    }

    let filters = vec![
        JsonFilter::Eq { path: "$.status".into(), value: "open".into() },
        JsonFilter::Gte { path: "$.close_date".into(), value: "2026-01-01".into() },
    ];

    // count_filtered reports the true total, independent of any cap.
    let count = ds.count_filtered("grants", "unified", &filters).await.unwrap();
    assert_eq!(count, 10, "count is the full window, not the return cap");

    // A cap of 3 must return the three SOONEST (01, 02, 03), not an arbitrary slice.
    let top = ds
        .list_filtered_ordered("grants", "unified", &filters, "$.close_date", 3)
        .await
        .unwrap();
    let closes: Vec<String> = top
        .iter()
        .map(|r| r.data.get("close_date").and_then(|v| v.as_str()).unwrap().to_string())
        .collect();
    assert_eq!(closes, vec!["2026-03-01", "2026-03-02", "2026-03-03"]);

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn upsert_many_is_correct_across_chunk_boundaries() {
    // 600 records exceeds the 500-record commit chunk, so this exercises the
    // multi-transaction batch path. Correctness must be identical to per-record.
    let (storage, dir) = fresh_db("datasets-upsert-many").await;
    let ds = Datasets::new(storage.pool());

    let items: Vec<(String, serde_json::Value)> = (0..600)
        .map(|i| (format!("k{i:04}"), json!({ "n": i })))
        .collect();

    // First run: all new.
    let s1 = ds.upsert_many("app", "d", &items).await.unwrap();
    assert_eq!(s1.new.len(), 600);
    assert_eq!(s1.changed.len(), 0);
    assert_eq!(s1.unchanged, 0);

    // Re-run identical: all unchanged (no new revisions).
    let s2 = ds.upsert_many("app", "d", &items).await.unwrap();
    assert_eq!(s2.unchanged, 600, "identical re-upsert is all unchanged");
    assert_eq!(s2.new.len(), 0);

    // Change one record on each side of the chunk boundary.
    let changed = vec![
        ("k0007".to_string(), json!({ "n": 7, "extra": true })),
        ("k0512".to_string(), json!({ "n": 512, "extra": true })),
    ];
    let s3 = ds.upsert_many("app", "d", &changed).await.unwrap();
    assert_eq!(s3.changed.len(), 2);

    // Every record resolves to exactly one row; the two changed keys have 2
    // revisions (new + changed), the rest have 1 — the chain stayed intact.
    let pool = storage.pool();
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM records WHERE app='app' AND dataset='d'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(total, 600);
    let revs_changed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM record_revisions WHERE app='app' AND dataset='d' AND key='k0512'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(revs_changed, 2, "new + changed revisions");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn detect_removed_tombstones_with_matching_removed_revisions() {
    // Every tombstone must have its `removed` revision — the atomicity guarantee.
    // A tombstone without a revision is a permanently-lost removal signal.
    let (storage, dir) = fresh_db("datasets-detect-removed").await;
    let ds = Datasets::new(storage.pool());
    let pool = storage.pool();

    // Seed 5 live records.
    let items: Vec<(String, serde_json::Value)> = (0..5)
        .map(|i| (format!("k{i}"), json!({ "n": i })))
        .collect();
    ds.upsert_many("app", "d", &items).await.unwrap();

    // Next full snapshot drops k1 and k3.
    let present: Vec<String> = vec!["k0".into(), "k2".into(), "k4".into()];
    let mut removed = ds.detect_removed("app", "d", &present).await.unwrap();
    removed.sort();
    assert_eq!(removed, vec!["k1".to_string(), "k3".to_string()]);

    // Each removed key is tombstoned AND has a `removed` revision (they agree).
    for key in ["k1", "k3"] {
        let removed_at: Option<String> = sqlx::query_scalar(
            "SELECT removed_at FROM records WHERE app='app' AND dataset='d' AND key=?1",
        )
        .bind(key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(removed_at.is_some(), "{key} must be tombstoned");
        let rev_changes: Vec<String> = sqlx::query_scalar(
            "SELECT change FROM record_revisions WHERE app='app' AND dataset='d' AND key=?1 ORDER BY revision",
        )
        .bind(key)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rev_changes, vec!["new", "removed"], "{key} revision chain: new then removed");
    }

    // Idempotent: a second identical snapshot re-removes nothing (already tombstoned).
    let removed2 = ds.detect_removed("app", "d", &present).await.unwrap();
    assert!(removed2.is_empty(), "already-removed keys are not re-removed");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}
