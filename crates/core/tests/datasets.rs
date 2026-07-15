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
