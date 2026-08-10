//! Integration tests for the webhook dead-letter auto-drain lifecycle against a
//! real temp-dir SQLite with the full migration chain. `next_retry_at` is
//! backdated directly so the tests are deterministic (no sleeping).

use sqlx::SqlitePool;
use uuid::Uuid;

use pumper_core::testing::TempStore;

async fn fresh_db(tag: &str) -> TempStore {
    TempStore::new(tag).await
}

/// Forces a failed delivery's `next_retry_at` into the past so `due_deliveries`
/// returns it without waiting out the real backoff.
async fn make_due(pool: &SqlitePool, id: &str) {
    sqlx::query(
        "UPDATE webhook_deliveries SET next_retry_at = '2000-01-01T00:00:00.000000Z' WHERE id = ?1",
    )
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

/// Backdates a row's `updated_at` — the shape a delivery has after the process
/// died mid-send: `status='pending'`, no `next_retry_at`, and an `updated_at`
/// from before the crash.
async fn backdate(pool: &SqlitePool, id: &str) {
    sqlx::query("UPDATE webhook_deliveries SET updated_at = '2000-01-01T00:00:00.000000Z' WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

async fn next_retry_at(pool: &SqlitePool, id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT next_retry_at FROM webhook_deliveries WHERE id = ?1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// How long a delivery may sit `pending` before it is presumed abandoned. Mirrors
/// `pumper_server::webhook::STALE_PENDING_SECS` (10 minutes ≈ 11.8x the 51s
/// worst-case in-process delivery).
const STALE_SECS: i64 = 600;

const BACKOFF: &[i64] = &[30, 60, 300, 1800, 7200];
const MAX_RETRIES: i64 = 5;

#[tokio::test]
async fn fail_schedules_retry_then_drain_claims_it() {
    let store = fresh_db("dlq-drain").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let id = storage
        .create_delivery(
            "job",
            &Uuid::new_v4().to_string(),
            "https://x/hook",
            "job.terminal",
            "{}",
        )
        .await
        .unwrap();

    // First failure schedules a retry (status stays 'failed', next_retry_at set).
    storage
        .fail_delivery(&id, 3, Some("boom"), MAX_RETRIES, BACKOFF)
        .await
        .unwrap();
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
}

#[tokio::test]
async fn repeated_failures_eventually_go_dead() {
    let store = fresh_db("dlq-dead").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let id = storage
        .create_delivery(
            "job",
            &Uuid::new_v4().to_string(),
            "https://x/hook",
            "e",
            "{}",
        )
        .await
        .unwrap();

    // Walk the full retry ladder: fail → claim → fail → … until 'dead'.
    storage
        .fail_delivery(&id, 3, Some("e"), MAX_RETRIES, BACKOFF)
        .await
        .unwrap();
    for _ in 0..MAX_RETRIES {
        make_due(&pool, &id).await;
        assert!(storage.begin_delivery_retry(&id).await.unwrap());
        storage
            .fail_delivery(&id, 1, Some("e"), MAX_RETRIES, BACKOFF)
            .await
            .unwrap();
    }
    let (status, rc) = status_and_retry(&pool, &id).await;
    assert_eq!(
        status, "dead",
        "past the retry cap the row is dead, not endlessly retried"
    );
    assert_eq!(rc, MAX_RETRIES);
    // A dead row is never due again.
    make_due(&pool, &id).await;
    assert!(storage.due_deliveries(10).await.unwrap().is_empty());
}

#[tokio::test]
async fn delivered_clears_the_retry_schedule() {
    let store = fresh_db("dlq-ok").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let id = storage
        .create_delivery(
            "change",
            "watch-1",
            "https://x/hook",
            "dataset.changed",
            "{}",
        )
        .await
        .unwrap();
    storage
        .fail_delivery(&id, 3, Some("e"), MAX_RETRIES, BACKOFF)
        .await
        .unwrap();
    // A later successful (re)delivery clears next_retry_at so the drain won't re-send.
    storage.finish_delivery(&id, true, 1, None).await.unwrap();
    let (status, _) = status_and_retry(&pool, &id).await;
    assert_eq!(status, "delivered");
    make_due(&pool, &id).await; // even if forced due, status='delivered' is not scanned
    assert!(storage.due_deliveries(10).await.unwrap().is_empty());
}

/// The anti-pattern: a delivery whose sender died between `create_delivery`
/// (row = `pending`) and the outcome write stayed `pending` FOREVER —
/// `due_deliveries` scans `failed` only, so nothing re-sent it, and
/// `prune_ledgers` touches only `delivered`/`dead`, so nothing reclaimed it.
/// An unbounded leak of undelivered payloads, invisible in the DLQ view.
#[tokio::test]
async fn stale_pending_reclaimed_not_stuck_forever() {
    let store = fresh_db("dlq-reclaim").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let id = storage
        .create_delivery(
            "change",
            "watch-crash",
            "https://x/hook",
            "dataset.changed",
            "{}",
        )
        .await
        .unwrap();
    // Crash shape: still 'pending', never scheduled, last touched long ago.
    backdate(&pool, &id).await;
    assert_eq!(status_and_retry(&pool, &id).await.0, "pending");
    assert!(next_retry_at(&pool, &id).await.is_none());
    assert!(
        storage.due_deliveries(10).await.unwrap().is_empty(),
        "a pending row is invisible to the drain — that is the bug"
    );

    assert_eq!(storage.reclaim_stale_deliveries(STALE_SECS).await.unwrap(), 1);
    let (status, rc) = status_and_retry(&pool, &id).await;
    assert_eq!(status, "failed", "reclaimed back into the retry ladder");
    assert_eq!(rc, 0, "reclaim must not consume a retry — the drain claim does");
    assert!(next_retry_at(&pool, &id).await.is_some(), "and marked due");

    // …and it now walks the normal ladder end to end, ending prunable ('dead').
    let due = storage.due_deliveries(10).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, id);
    assert!(storage.begin_delivery_retry(&id).await.unwrap());
    storage
        .fail_delivery(&id, 1, Some("e"), MAX_RETRIES, BACKOFF)
        .await
        .unwrap();
    for _ in 1..MAX_RETRIES {
        make_due(&pool, &id).await;
        assert!(storage.begin_delivery_retry(&id).await.unwrap());
        storage
            .fail_delivery(&id, 1, Some("e"), MAX_RETRIES, BACKOFF)
            .await
            .unwrap();
    }
    assert_eq!(status_and_retry(&pool, &id).await.0, "dead");
}

/// The other half of the contract: a delivery that is merely *slow* must not be
/// handed a second sender. Only rows older than the threshold are reclaimed.
#[tokio::test]
async fn fresh_pending_not_reclaimed() {
    let store = fresh_db("dlq-reclaim-fresh").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let id = storage
        .create_delivery("job", &Uuid::new_v4().to_string(), "https://x/h", "e", "{}")
        .await
        .unwrap();
    assert_eq!(storage.reclaim_stale_deliveries(STALE_SECS).await.unwrap(), 0);
    assert_eq!(status_and_retry(&pool, &id).await.0, "pending");

    // A row claimed by a drain retry moments ago is equally off-limits.
    storage
        .fail_delivery(&id, 3, Some("e"), MAX_RETRIES, BACKOFF)
        .await
        .unwrap();
    make_due(&pool, &id).await;
    assert!(storage.begin_delivery_retry(&id).await.unwrap());
    assert_eq!(storage.reclaim_stale_deliveries(STALE_SECS).await.unwrap(), 0);
    assert_eq!(status_and_retry(&pool, &id).await.0, "pending");
}

/// The two claims are deliberately different: an operator may replay a `dead`
/// row by id, but the auto-drain must never resurrect one — otherwise `dead`
/// means nothing and the ladder never terminates.
#[tokio::test]
async fn manual_replay_claims_dead_but_the_drain_does_not_resurrect_it() {
    let store = fresh_db("dlq-replay-claim").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let id = storage
        .create_delivery("job", &Uuid::new_v4().to_string(), "https://x/h", "e", "{}")
        .await
        .unwrap();
    // Walk it to dead.
    storage
        .fail_delivery(&id, 3, Some("e"), MAX_RETRIES, BACKOFF)
        .await
        .unwrap();
    for _ in 0..MAX_RETRIES {
        make_due(&pool, &id).await;
        assert!(storage.begin_delivery_retry(&id).await.unwrap());
        storage
            .fail_delivery(&id, 1, Some("e"), MAX_RETRIES, BACKOFF)
            .await
            .unwrap();
    }
    assert_eq!(status_and_retry(&pool, &id).await.0, "dead");

    // The drain's claim refuses it…
    assert!(!storage.begin_delivery_retry(&id).await.unwrap());
    // …the manual one takes it, in flight.
    assert!(storage.begin_delivery_replay(&id, false).await.unwrap());
    assert_eq!(status_and_retry(&pool, &id).await.0, "pending");
    // A second manual replay while it is in flight loses the race → 409.
    assert!(!storage.begin_delivery_replay(&id, false).await.unwrap());
}

/// A delivered row is only re-sent when the caller says so out loud.
#[tokio::test]
async fn delivered_replay_needs_force_not_a_bare_post() {
    let store = fresh_db("dlq-replay-force").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let id = storage
        .create_delivery("job", &Uuid::new_v4().to_string(), "https://x/h", "e", "{}")
        .await
        .unwrap();
    storage.finish_delivery(&id, true, 1, None).await.unwrap();
    assert_eq!(status_and_retry(&pool, &id).await.0, "delivered");

    assert!(!storage.begin_delivery_replay(&id, false).await.unwrap());
    assert_eq!(status_and_retry(&pool, &id).await.0, "delivered");
    assert!(storage.begin_delivery_replay(&id, true).await.unwrap());
    assert_eq!(status_and_retry(&pool, &id).await.0, "pending");
}
