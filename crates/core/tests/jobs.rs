//! Integration tests for the job queue's claim ordering — in particular the
//! priority-aging starvation guard — against a real temp-dir SQLite with the
//! full migration chain. Timestamps are manipulated directly so the tests are
//! deterministic (no sleeping).

use chrono::{Duration, SecondsFormat, Utc};
use pumper_core::{EnqueueOptions, JobStatus};
use serde_json::json;
use sqlx::SqlitePool;
use uuid::Uuid;

/// Fresh temp-dir SQLite with the full migration chain.
use pumper_core::testing::TempStore;

async fn fresh_db(tag: &str) -> TempStore {
    TempStore::new(tag).await
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
    let store = fresh_db("aging").await;
    let storage = &store.storage;
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
    assert_eq!(
        claimed.id, starved,
        "aged low-priority job should overtake fresh high-priority work"
    );
}

#[tokio::test]
async fn aging_disabled_keeps_strict_priority_order() {
    let store = fresh_db("aging-off").await;
    let storage = &store.storage;
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
}

#[tokio::test]
async fn equal_priority_claims_fifo() {
    let store = fresh_db("fifo").await;
    let storage = &store.storage;
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
}

/// Inserts a job already in a terminal `failed` state.
async fn insert_failed(pool: &SqlitePool, app: &str) -> Uuid {
    let id = Uuid::new_v4();
    let ts = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
    sqlx::query(
        "INSERT INTO jobs (id, app, params, status, attempts, max_attempts, priority, \
         error, created_at, available_at, started_at, finished_at) \
         VALUES (?1, ?2, '{}', 'failed', 1, 1, 0, 'boom', ?3, ?3, ?3, ?3)",
    )
    .bind(id.to_string())
    .bind(app)
    .bind(&ts)
    .execute(pool)
    .await
    .expect("insert failed job");
    id
}

#[tokio::test]
async fn reset_requeues_running_and_fences_stale_completion() {
    let store = fresh_db("reset").await;
    let storage = &store.storage;

    let job = storage
        .enqueue(
            "a",
            EnqueueOptions {
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Reset only applies to running jobs.
    assert!(
        storage.reset(job.id).await.unwrap().is_none(),
        "queued job is not resettable"
    );

    // Claim -> running, attempts = 1.
    let claimed = storage.claim_next(&[], 0.0).await.unwrap().unwrap();
    assert_eq!(claimed.id, job.id);
    assert_eq!(claimed.attempts, 1);

    // Reset re-queues it (the orphaned task still holds attempt 1).
    let after = storage.reset(job.id).await.unwrap().unwrap();
    assert_eq!(after.status, JobStatus::Queued);
    assert_eq!(after.attempts, 1);

    // Stale attempt-1 completion is discarded: the row is queued, not running.
    assert!(!storage
        .complete(job.id, 1, json!({"stale": true}))
        .await
        .unwrap());

    // Re-claim -> attempts = 2 (fence advances past the orphan).
    let reclaimed = storage.claim_next(&[], 0.0).await.unwrap().unwrap();
    assert_eq!(reclaimed.attempts, 2);

    // Orphan's attempt-1 write still discarded; the live attempt-2 write lands.
    assert!(!storage
        .complete(job.id, 1, json!({"stale": true}))
        .await
        .unwrap());
    assert!(storage
        .complete(job.id, 2, json!({"ok": true}))
        .await
        .unwrap());

    let done = storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(done.status, JobStatus::Succeeded);
    assert_eq!(done.result, Some(json!({"ok": true})));
}

#[tokio::test]
async fn cancel_running_is_attempt_guarded() {
    let store = fresh_db("cancel-running").await;
    let storage = &store.storage;
    let job = storage
        .enqueue("a", EnqueueOptions::default())
        .await
        .unwrap();
    let claimed = storage.claim_next(&[], 0.0).await.unwrap().unwrap();
    assert_eq!(claimed.attempts, 1);

    // Wrong attempt / not-running -> no-op.
    assert!(!storage.cancel_running(job.id, 2).await.unwrap());
    // Correct attempt cancels the in-flight job.
    assert!(storage.cancel_running(job.id, 1).await.unwrap());
    assert_eq!(
        storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Cancelled
    );
    // Idempotent: already terminal.
    assert!(!storage.cancel_running(job.id, 1).await.unwrap());
}

#[tokio::test]
async fn retry_bulk_requeues_filtered_batch() {
    let store = fresh_db("bulk").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let a1 = insert_failed(&pool, "a").await;
    let a2 = insert_failed(&pool, "a").await;
    let b1 = insert_failed(&pool, "b").await;

    // Scoped to app "a": only its two failed jobs re-queue.
    let requeued = storage
        .retry_bulk(JobStatus::Failed, Some("a"), 500)
        .await
        .unwrap();
    assert_eq!(requeued.len(), 2);
    // The app rides back with each id — the caller announces these re-queues,
    // and an event with a blank app reaches no app-scoped watcher.
    assert!(requeued.iter().all(|(_, app)| app == "a"));
    let ids: Vec<_> = requeued.into_iter().map(|(id, _)| id).collect();
    assert!(ids.contains(&a1) && ids.contains(&a2));
    for id in [a1, a2] {
        let j = storage.get(id).await.unwrap().unwrap();
        assert_eq!(j.status, JobStatus::Queued);
        assert_eq!(j.max_attempts, 2, "one more attempt granted");
    }
    // app "b" untouched.
    assert_eq!(
        storage.get(b1).await.unwrap().unwrap().status,
        JobStatus::Failed
    );

    // Cap is respected.
    insert_failed(&pool, "c").await;
    insert_failed(&pool, "c").await;
    let capped = storage
        .retry_bulk(JobStatus::Failed, None, 1)
        .await
        .unwrap();
    assert_eq!(capped.len(), 1);
}

/// Inserts a `running` job with an explicit heartbeat age (seconds ago).
async fn insert_running(
    pool: &SqlitePool,
    app: &str,
    attempts: i64,
    max_attempts: i64,
    heartbeat_secs_ago: i64,
) -> Uuid {
    let id = Uuid::new_v4();
    let now = Utc::now();
    let started = (now - Duration::seconds(heartbeat_secs_ago + 1))
        .to_rfc3339_opts(SecondsFormat::Micros, true);
    let hb =
        (now - Duration::seconds(heartbeat_secs_ago)).to_rfc3339_opts(SecondsFormat::Micros, true);
    sqlx::query(
        "INSERT INTO jobs (id, app, params, status, attempts, max_attempts, priority, \
         created_at, available_at, started_at, heartbeat_at) \
         VALUES (?1, ?2, '{}', 'running', ?3, ?4, 0, ?5, ?5, ?5, ?6)",
    )
    .bind(id.to_string())
    .bind(app)
    .bind(attempts)
    .bind(max_attempts)
    .bind(&started)
    .bind(&hb)
    .execute(pool)
    .await
    .expect("insert running job");
    id
}

#[tokio::test]
async fn reaper_requeues_stale_but_leaves_fresh_running_jobs() {
    let store = fresh_db("reap").await;
    let storage = &store.storage;
    let pool = storage.pool();

    // Hung: last heartbeat 300s ago, attempts remain -> re-queued.
    let stale = insert_running(&pool, "a", 1, 3, 300).await;
    // Slow-but-alive: heartbeat 5s ago -> never reaped.
    let fresh = insert_running(&pool, "a", 1, 3, 5).await;

    let reaped = storage.reap_stale(120).await.unwrap();
    assert_eq!(reaped.len(), 1, "only the stale job is reaped");
    assert_eq!(reaped[0].0, stale);
    assert_eq!(reaped[0].2, JobStatus::Queued);

    assert_eq!(
        storage.get(stale).await.unwrap().unwrap().status,
        JobStatus::Queued
    );
    assert_eq!(
        storage.get(fresh).await.unwrap().unwrap().status,
        JobStatus::Running,
        "a slow-but-alive job must survive the reaper"
    );
}

#[tokio::test]
async fn reaper_fails_permanently_when_attempts_exhausted() {
    let store = fresh_db("reap-exhausted").await;
    let storage = &store.storage;
    let pool = storage.pool();

    // Stale AND out of attempts (attempts == max) -> permanent failure.
    let stale = insert_running(&pool, "a", 3, 3, 300).await;
    let reaped = storage.reap_stale(120).await.unwrap();
    assert_eq!(reaped.len(), 1);
    assert_eq!(reaped[0].2, JobStatus::Failed);

    let job = storage.get(stale).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Failed);
    assert!(job.error.unwrap().contains("lease expired"));
}

#[tokio::test]
async fn heartbeat_refresh_keeps_a_job_off_the_reaper() {
    let store = fresh_db("heartbeat").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let running = insert_running(&pool, "a", 1, 1, 300).await;
    // Before the beat it would be reaped; the beat refreshes the lease.
    assert!(storage.heartbeat(running, 1).await.unwrap());
    assert!(
        storage.reap_stale(120).await.unwrap().is_empty(),
        "a freshly-heartbeated job is not stale"
    );
    // Attempt-guarded: a stale task can't refresh a row it no longer owns.
    assert!(!storage.heartbeat(running, 2).await.unwrap());
}

/// The retry ladder must not be a pure function of the attempt number.
///
/// The anti-pattern: `10s · 2^attempts` exactly. One dependency outage fails
/// every job in flight within the same second, and a deterministic ladder then
/// re-queues all of them at the same instant — the synchronized retry that turns
/// a recovering dependency straight back into a failing one, and does it harder
/// on each rung because the herd only grows. `fail_delivery` already jittered its
/// webhook ladder for exactly this reason; the job ladder did not.
#[tokio::test]
async fn the_retry_ladder_is_jittered_so_a_fleet_wide_outage_does_not_re_queue_in_lockstep() {
    let store = fresh_db("retry-jitter").await;
    let storage = &store.storage;
    let pool = storage.pool();

    // Twenty jobs, all on the same rung (attempt 3 -> base 80s), all failed at
    // once — the shape of a dependency outage.
    let mut whens: Vec<chrono::DateTime<Utc>> = Vec::new();
    let before = Utc::now();
    for _ in 0..20 {
        let id = insert_running(&pool, "a", 3, 5, 0).await;
        assert_eq!(
            storage.fail(id, 3, "dependency down").await.unwrap(),
            Some(JobStatus::Queued)
        );
        let job = storage.get(id).await.unwrap().unwrap();
        whens.push(job.available_at);
    }

    // The band: never earlier than the base delay (jitter only ever adds, so a
    // retry can't come back sooner than the ladder promised), never more than
    // +25% past it.
    let base = 80i64;
    for when in &whens {
        let delay = (*when - before).num_seconds();
        assert!(
            (base..=base + base / 4 + 2).contains(&delay),
            "delay {delay}s is outside [{base}, {}]s",
            base + base / 4 + 2
        );
    }

    // …and they really are spread. A deterministic ladder would put all twenty
    // on one timestamp; this is the assertion that fails if the jitter is
    // removed.
    let distinct: std::collections::BTreeSet<i64> =
        whens.iter().map(|w| w.timestamp_millis()).collect();
    assert!(
        distinct.len() >= 10,
        "only {} distinct retry times across 20 jobs — the ladder is re-synchronizing the herd",
        distinct.len()
    );
}
