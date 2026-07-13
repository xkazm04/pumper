//! Integration tests for the job queue's claim ordering — in particular the
//! priority-aging starvation guard — against a real temp-dir SQLite with the
//! full migration chain. Timestamps are manipulated directly so the tests are
//! deterministic (no sleeping).

use chrono::{Duration, SecondsFormat, Utc};
use pumper_core::config::StorageConfig;
use pumper_core::Storage;
use sqlx::SqlitePool;
use uuid::Uuid;

/// Fresh temp-dir SQLite with the full migration chain.
async fn fresh_db(tag: &str) -> (Storage, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("pumper-{tag}-{}", Uuid::new_v4()));
    let cfg = StorageConfig {
        database_path: dir.join("pumper.db"),
        artifacts_dir: dir.join("artifacts"),
    };
    let storage = Storage::connect(&cfg).await.expect("connect + migrate");
    (storage, dir)
}

/// Inserts a queued job with an explicit priority and queue-wait: `waited_secs`
/// ago is when it was created (and became available). Returns its id.
async fn insert_queued(pool: &SqlitePool, app: &str, priority: i64, waited_secs: i64) -> Uuid {
    let id = Uuid::new_v4();
    let created = Utc::now() - Duration::seconds(waited_secs);
    let ts = created.to_rfc3339_opts(SecondsFormat::Micros, true);
    sqlx::query(
        "INSERT INTO jobs (id, app, params, status, attempts, max_attempts, priority, \
         created_at, available_at) \
         VALUES (?1, ?2, '{}', 'queued', 0, 1, ?3, ?4, ?4)",
    )
    .bind(id.to_string())
    .bind(app)
    .bind(priority)
    .bind(&ts)
    .execute(pool)
    .await
    .expect("insert job");
    id
}

#[tokio::test]
async fn priority_aging_lets_a_starved_low_priority_job_claim() {
    let (storage, dir) = fresh_db("aging").await;
    let pool = storage.pool();

    // A low-priority job that has already waited an hour...
    let starved = insert_queued(&pool, "a", 0, 3600).await;
    // ...behind a continuous stream of fresh high-priority work.
    for _ in 0..5 {
        insert_queued(&pool, "a", 10, 0).await;
    }

    // Coefficient 300s: the starved job's effective priority is
    // 0 + 3600/300 = 12, which beats the fresh priority-10 jobs. It claims first.
    let claimed = storage
        .claim_next(&[], 300.0)
        .await
        .expect("claim")
        .expect("a job");
    assert_eq!(claimed.id, starved, "aged low-priority job should overtake fresh high-priority work");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn aging_disabled_keeps_strict_priority_order() {
    let (storage, dir) = fresh_db("aging-off").await;
    let pool = storage.pool();

    // Same setup as above, but with aging disabled the starved job stays behind.
    let _starved = insert_queued(&pool, "a", 0, 3600).await;
    let mut high = Vec::new();
    for _ in 0..3 {
        high.push(insert_queued(&pool, "a", 10, 0).await);
    }

    let claimed = storage
        .claim_next(&[], 0.0)
        .await
        .expect("claim")
        .expect("a job");
    assert!(
        high.contains(&claimed.id),
        "with aging off a high-priority job must claim before the aged low-priority one"
    );

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn equal_priority_claims_fifo() {
    let (storage, dir) = fresh_db("fifo").await;
    let pool = storage.pool();

    // Three equal-priority jobs at different ages; FIFO = oldest first, whether
    // or not aging is on (aging only strengthens the older-first ordering).
    let oldest = insert_queued(&pool, "a", 5, 300).await;
    let middle = insert_queued(&pool, "a", 5, 200).await;
    let newest = insert_queued(&pool, "a", 5, 100).await;

    let first = storage.claim_next(&[], 900.0).await.unwrap().unwrap();
    assert_eq!(first.id, oldest, "oldest equal-priority job claims first");
    let second = storage.claim_next(&[], 900.0).await.unwrap().unwrap();
    assert_eq!(second.id, middle);
    let third = storage.claim_next(&[], 900.0).await.unwrap().unwrap();
    assert_eq!(third.id, newest);

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}
