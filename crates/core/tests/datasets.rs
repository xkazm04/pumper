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
