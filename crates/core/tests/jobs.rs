//! Integration tests for the job queue's claim ordering — in particular the
//! priority-aging starvation guard — against a real temp-dir SQLite with the
//! full migration chain. Timestamps are manipulated directly so the tests are
//! deterministic (no sleeping).

use chrono::{Duration, SecondsFormat, Utc};
use pumper_core::storage::{FailReason, JobQuery};
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

/// The boot sweep and the lease reaper act on the SAME class of row — a
/// `running` job whose executor is gone — and used to apply two different
/// policies to it. The sweep was one blanket `UPDATE … WHERE status='running'`:
/// instant re-queue, no backoff, no reason, and no respect for an exhausted
/// attempt budget, so a job that had burned its last attempt crashing the
/// process got a free one, forever, as long as it crashed the process.
#[tokio::test]
async fn the_boot_sweep_issues_the_same_verdict_the_reaper_does() {
    let store = fresh_db("boot-sweep").await;
    let storage = &store.storage;
    let pool = storage.pool();

    // Attempts remain -> re-queued, with the ladder's backoff, not instantly.
    let retryable = insert_running(&pool, "a", 1, 3, 1).await;
    // Attempts exhausted -> permanently failed, exactly as the reaper would.
    let exhausted = insert_running(&pool, "a", 3, 3, 1).await;

    let before = Utc::now();
    let sweep = storage.recover_stuck().await.unwrap();
    assert_eq!((sweep.requeued, sweep.failed, sweep.skipped), (1, 1, 0));
    assert!(!sweep.truncated);
    assert_eq!(sweep.verdicts.len(), 2);

    let job = storage.get(retryable).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Queued);
    assert!(
        job.available_at > before,
        "a boot re-queue takes the backoff ladder, like every other recovery"
    );
    // The reason is on the row: "the boot sweep did this" must stay
    // distinguishable from "the executor failed this", forever.
    assert!(
        job.error.as_deref().unwrap_or_default().contains("boot"),
        "error was {:?}",
        job.error
    );

    let job = storage.get(exhausted).await.unwrap().unwrap();
    assert_eq!(
        job.status,
        JobStatus::Failed,
        "an exhausted job must not earn an extra attempt by killing the process"
    );
    assert!(job.error.as_deref().unwrap_or_default().contains("boot"));
}

/// A sweep is only meaningful if it left the rows it did NOT verdict alone,
/// and if re-running it is safe.
#[tokio::test]
async fn the_boot_sweep_is_idempotent_and_touches_only_running_rows() {
    let store = fresh_db("boot-sweep-idempotent").await;
    let storage = &store.storage;
    let pool = storage.pool();

    let running = insert_running(&pool, "a", 1, 3, 1).await;
    let queued = storage
        .enqueue("a", EnqueueOptions::default())
        .await
        .unwrap();

    assert_eq!(storage.recover_stuck().await.unwrap().requeued, 1);
    // Second pass: nothing is `running` any more, so nothing is verdicted.
    let again = storage.recover_stuck().await.unwrap();
    assert_eq!((again.requeued, again.failed, again.skipped), (0, 0, 0));

    assert_eq!(
        storage.get(running).await.unwrap().unwrap().attempts,
        1,
        "recovery does not spend an attempt; the claim does"
    );
    let untouched = storage.get(queued.id).await.unwrap().unwrap();
    assert_eq!(untouched.status, JobStatus::Queued);
    assert!(
        untouched.error.is_none(),
        "a queued job was never running and must carry no recovery reason"
    );
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
            storage
                .fail(id, 3, FailReason::Text("dependency down"))
                .await
                .unwrap(),
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

// ───────── the lineage slice: the plan, not just the rows ─────────
//
// `GET /jobs`'s filter surface is assembled from only the predicates a caller
// actually supplied, which is the part no behavioural test can see. The obvious
// alternative — one static statement with a `(?n IS NULL OR col = ?n)` clause
// per column — returns byte-identical rows and **full-scans `jobs` every
// time**: SQLite cannot use an index for a term it cannot prove is an equality
// while planning. So the claim "this filter rides the index migration
// 0012/0014/0041 paid for" is asserted here against the real
// `EXPLAIN QUERY PLAN`, and nowhere else.

/// Inserts one job with the lineage columns under the test's control.
async fn insert_lineage_job(
    pool: &SqlitePool,
    app: &str,
    status: &str,
    created_at: &str,
    schedule_id: Option<&str>,
    trigger_id: Option<&str>,
    target_key: Option<&str>,
) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO jobs (id, app, params, status, attempts, max_attempts, priority, \
         created_at, available_at, schedule_id, trigger_id, target_key) \
         VALUES (?1, ?2, '{}', ?3, 0, 1, 0, ?4, ?4, ?5, ?6, ?7)",
    )
    .bind(id.to_string())
    .bind(app)
    .bind(status)
    .bind(created_at)
    .bind(schedule_id)
    .bind(trigger_id)
    .bind(target_key)
    .execute(pool)
    .await
    .expect("insert lineage job");
    id
}

fn at(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .expect("rfc3339")
        .with_timezone(&Utc)
}

/// The anti-pattern this guards: a `(?n IS NULL OR col = ?n)` WHERE clause.
/// It passes every row-level test and reaches NONE of these indexes, which
/// would leave the whole lineage filter a convenience over a table scan.
#[tokio::test]
async fn lineage_filters_ride_the_indexes_not_a_table_scan() {
    let store = fresh_db("lineage-plan").await;
    let storage = &store.storage;
    let pool = storage.pool();
    insert_lineage_job(
        &pool,
        "a",
        "failed",
        "2026-10-02T00:00:00.000000Z",
        Some("S"),
        Some("T"),
        Some("K"),
    )
    .await;

    let plan_for = |q: JobQuery| {
        let storage = storage.clone();
        async move {
            storage
                .explain_job_list(&q, 50)
                .await
                .expect("explain")
                .join(" | ")
        }
    };

    // Migration 0014: idx_jobs_trigger (trigger_id, created_at DESC). The
    // index tail IS the ORDER BY, so there is not even a sort to do.
    let joined = plan_for(JobQuery {
        trigger_id: Some("T".into()),
        status: Some(JobStatus::Failed),
        ..Default::default()
    })
    .await;
    assert!(
        joined.contains("idx_jobs_trigger"),
        "?trigger_id= must ride migration 0014's index; plan was: {joined}"
    );
    assert!(
        !joined.contains("SCAN jobs"),
        "a full scan here means the WHERE clause was assembled with nullable \
         parameters after all: {joined}"
    );
    assert!(
        !joined.contains("TEMP B-TREE"),
        "idx_jobs_trigger is (trigger_id, created_at DESC) — the ORDER BY is \
         the index's own tail and must cost no sort: {joined}"
    );

    // Migration 0012: idx_jobs_schedule (schedule_id, status). Until this
    // surface existed its only reader in the tree was the scheduler's internal
    // overlap probe.
    let joined = plan_for(JobQuery {
        schedule_id: Some("S".into()),
        status: Some(JobStatus::Failed),
        ..Default::default()
    })
    .await;
    assert!(
        joined.contains("idx_jobs_schedule") && joined.contains("status=?"),
        "?schedule_id=&status= must use BOTH columns of 0012's index: {joined}"
    );

    // Migration 0041: idx_jobs_status_target is (status, target_key) — `status`
    // LEADS.
    let joined = plan_for(JobQuery {
        target_key: Some("K".into()),
        status: Some(JobStatus::Failed),
        ..Default::default()
    })
    .await;
    assert!(
        joined.contains("idx_jobs_status_target"),
        "?status=&target_key= is the form 0041's (status, target_key) index \
         covers: {joined}"
    );

    // ...so a bare `?target_key=` CANNOT use it. Pinned as a known gap rather
    // than papered over: claiming otherwise in a doc would be a claim this
    // plan refutes, and re-ordering the index is a migration this change does
    // not own.
    let bare = plan_for(JobQuery {
        target_key: Some("K".into()),
        ..Default::default()
    })
    .await;
    assert!(
        bare.contains("SCAN jobs"),
        "KNOWN GAP, pinned so it cannot be quietly mis-documented: 0041's \
         index leads with `status`, so a bare ?target_key= scans. If this \
         stops scanning the index changed, and docs/features/http-api.md owes \
         an edit: {bare}"
    );

    // The window rides the `created_at` tail of whichever index the equality
    // picked, as a RANGE — not a post-filter over a sorted scan.
    let joined = plan_for(JobQuery {
        app: Some("a".into()),
        since: Some(at("2026-10-01T00:00:00Z")),
        ..Default::default()
    })
    .await;
    assert!(
        joined.contains("idx_jobs_app") && joined.contains("created_at>"),
        "?app=&since= must become a range on idx_jobs_app's (app, created_at \
         DESC): {joined}"
    );

    // And the unfiltered list is honestly a scan — the floor that proves the
    // assertions above read a real plan rather than a constant string.
    let joined = plan_for(JobQuery::default()).await;
    assert!(
        joined.contains("SCAN jobs"),
        "with no predicate there is no index to ride; a plan that claims one \
         means this probe is not explaining the real statement: {joined}"
    );
}

/// The `created_at` window is half-open, `[since, until)`, at the STORE — the
/// boundary row belongs to exactly one of two adjacent windows, which is what
/// makes a day-at-a-time walk's counts add up.
#[tokio::test]
async fn the_created_at_window_is_half_open() {
    let store = fresh_db("lineage-window").await;
    let storage = &store.storage;
    let pool = storage.pool();
    let first = insert_lineage_job(
        &pool,
        "a",
        "succeeded",
        "2026-10-01T00:00:00.000000Z",
        None,
        None,
        None,
    )
    .await;
    let boundary = insert_lineage_job(
        &pool,
        "a",
        "succeeded",
        "2026-10-02T00:00:00.000000Z",
        None,
        None,
        None,
    )
    .await;

    let ids = |q: JobQuery| {
        let storage = storage.clone();
        async move {
            storage
                .list(&q, 50)
                .await
                .expect("list")
                .into_iter()
                .map(|j| j.id)
                .collect::<Vec<Uuid>>()
        }
    };

    assert_eq!(
        ids(JobQuery {
            since: Some(at("2026-10-01T00:00:00Z")),
            until: Some(at("2026-10-02T00:00:00Z")),
            ..Default::default()
        })
        .await,
        vec![first],
        "`until` is EXCLUSIVE: the job stamped exactly at it is the NEXT \
         window's, or a per-day count double-counts every boundary"
    );
    assert_eq!(
        ids(JobQuery {
            since: Some(at("2026-10-02T00:00:00Z")),
            ..Default::default()
        })
        .await,
        vec![boundary],
        "...and `since` is INCLUSIVE"
    );
}

/// The rollup aggregates the SAME `WHERE` the list filters by, and reports only
/// the statuses the window actually holds.
#[tokio::test]
async fn the_status_rollup_is_scoped_by_the_same_predicates_as_the_list() {
    let store = fresh_db("lineage-rollup").await;
    let storage = &store.storage;
    let pool = storage.pool();
    for i in 0..12 {
        insert_lineage_job(
            &pool,
            "a",
            "succeeded",
            &format!("2026-10-02T00:00:{i:02}.000000Z"),
            Some("S"),
            None,
            None,
        )
        .await;
    }
    for i in 0..3 {
        insert_lineage_job(
            &pool,
            "a",
            "failed",
            &format!("2026-10-02T01:00:{i:02}.000000Z"),
            Some("S"),
            None,
            None,
        )
        .await;
    }
    // Another schedule's work, which the scoped rollup must not count.
    insert_lineage_job(
        &pool,
        "a",
        "failed",
        "2026-10-02T02:00:00.000000Z",
        Some("T"),
        None,
        None,
    )
    .await;

    let counts = storage
        .status_counts_filtered(&JobQuery {
            schedule_id: Some("S".into()),
            ..Default::default()
        })
        .await
        .expect("rollup");
    assert_eq!(
        counts,
        vec![("failed".to_string(), 3), ("succeeded".to_string(), 12)],
        "S's 12 succeeded and 3 failed, and T's failure is not S's"
    );
    assert!(
        !counts.iter().any(|(_, n)| *n == 0),
        "an absent status is ABSENT, not a zero: {counts:?}"
    );

    // Unscoped, it agrees with the /metrics aggregate over the same table.
    let mut unscoped = storage
        .status_counts_filtered(&JobQuery::default())
        .await
        .expect("rollup");
    let mut metrics = storage.status_counts().await.expect("status_counts");
    unscoped.sort();
    metrics.sort();
    assert_eq!(
        unscoped, metrics,
        "with no predicate the scoped rollup IS /metrics' aggregate; two \
         spellings of one count is how a dashboard and a triage page come to \
         disagree about the same queue"
    );
}
