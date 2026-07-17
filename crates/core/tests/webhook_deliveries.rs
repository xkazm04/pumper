//! Integration tests for the webhook dead-letter auto-drain lifecycle against a
//! real temp-dir SQLite with the full migration chain. `next_retry_at` is
//! backdated directly so the tests are deterministic (no sleeping).

use pumper_core::config::StorageConfig;
use pumper_core::Storage;
use sqlx::SqlitePool;
use uuid::Uuid;

async fn fresh_db(tag: &str) -> (Storage, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("pumper-{tag}-{}", Uuid::new_v4()));
    let cfg = StorageConfig {
        database_path: dir.join("pumper.db"),
        artifacts_dir: dir.join("artifacts"),
        ..StorageConfig::default()
    };
    let storage = Storage::connect(&cfg).await.expect("connect + migrate");
    (storage, dir)
}

/// Forces a failed delivery's `next_retry_at` into the past so `due_deliveries`
/// returns it without waiting out the real backoff.
async fn make_due(pool: &SqlitePool, id: &str) {
    sqlx::query("UPDATE webhook_deliveries SET next_retry_at = '2000-01-01T00:00:00.000000Z' WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

async fn status_and_retry(pool: &SqlitePool, id: &str) -> (String, i64) {
    sqlx::query_as("SELECT status, retry_count FROM webhook_deliveries WHERE id = ?1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

const BACKOFF: &[i64] = &[30, 60, 300, 1800, 7200];
const MAX_RETRIES: i64 = 5;

#[tokio::test]
async fn fail_schedules_retry_then_drain_claims_it() {
    let (storage, dir) = fresh_db("dlq-drain").await;
    let pool = storage.pool();

    let id = storage
        .create_delivery("job", &Uuid::new_v4().to_string(), "https://x/hook", "job.terminal", "{}")
        .await
        .unwrap();

    // First failure schedules a retry (status stays 'failed', next_retry_at set).
    storage.fail_delivery(&id, 3, Some("boom"), MAX_RETRIES, BACKOFF).await.unwrap();
    let (status, rc) = status_and_retry(&pool, &id).await;
    assert_eq!(status, "failed");
    assert_eq!(rc, 0, "initial failure hasn't consumed a drain retry yet");

    // Not due yet (backoff is ~30s in the future) → drain scan skips it.
    assert!(storage.due_deliveries(10).await.unwrap().is_empty());

    // Backdate → now due → appears in the work list.
    make_due(&pool, &id).await;
    let due = storage.due_deliveries(10).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, id);

    // Claim bumps retry_count and flips to 'pending' so a second tick can't grab it.
    assert!(storage.begin_delivery_retry(&id).await.unwrap());
    let (status, rc) = status_and_retry(&pool, &id).await;
    assert_eq!(status, "pending");
    assert_eq!(rc, 1);
    // A racing second claim finds it no longer 'failed'.
    assert!(!storage.begin_delivery_retry(&id).await.unwrap());

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn repeated_failures_eventually_go_dead() {
    let (storage, dir) = fresh_db("dlq-dead").await;
    let pool = storage.pool();

    let id = storage
        .create_delivery("job", &Uuid::new_v4().to_string(), "https://x/hook", "e", "{}")
        .await
        .unwrap();

    // Walk the full retry ladder: fail → claim → fail → … until 'dead'.
    storage.fail_delivery(&id, 3, Some("e"), MAX_RETRIES, BACKOFF).await.unwrap();
    for _ in 0..MAX_RETRIES {
        make_due(&pool, &id).await;
        assert!(storage.begin_delivery_retry(&id).await.unwrap());
        storage.fail_delivery(&id, 1, Some("e"), MAX_RETRIES, BACKOFF).await.unwrap();
    }
    let (status, rc) = status_and_retry(&pool, &id).await;
    assert_eq!(status, "dead", "past the retry cap the row is dead, not endlessly retried");
    assert_eq!(rc, MAX_RETRIES);
    // A dead row is never due again.
    make_due(&pool, &id).await;
    assert!(storage.due_deliveries(10).await.unwrap().is_empty());

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn delivered_clears_the_retry_schedule() {
    let (storage, dir) = fresh_db("dlq-ok").await;
    let pool = storage.pool();

    let id = storage
        .create_delivery("change", "watch-1", "https://x/hook", "dataset.changed", "{}")
        .await
        .unwrap();
    storage.fail_delivery(&id, 3, Some("e"), MAX_RETRIES, BACKOFF).await.unwrap();
    // A later successful (re)delivery clears next_retry_at so the drain won't re-send.
    storage.finish_delivery(&id, true, 1, None).await.unwrap();
    let (status, _) = status_and_retry(&pool, &id).await;
    assert_eq!(status, "delivered");
    make_due(&pool, &id).await; // even if forced due, status='delivered' is not scanned
    assert!(storage.due_deliveries(10).await.unwrap().is_empty());

    let _ = std::fs::remove_dir_all(dir);
}
