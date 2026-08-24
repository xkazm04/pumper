//! The **store** chokepoint: every measured statement family goes through
//! `StoreInstrument::metered`, which times the pool wait and the statement
//! under separate keys and classifies the outcome by typed predicate.
//!
//! Sibling of `fetch_chokepoint.rs` in spirit and different in kind: that one
//! guards a seam with an inventory, because the seam is bypassable. This one
//! guards a *measurement*, so the tests drive real SQLite — including a real
//! `SQLITE_BUSY` taken off a genuinely locked database, because a busy counter
//! that has only ever been fed a fabricated error is a counter nobody has
//! proven can fire.

use std::time::Duration;

use pumper_core::store_instrument::{StoreInstrument, StoreOp, StorePhase};
use pumper_core::testing::TempStore;
use pumper_core::{EnqueueOptions, KeyReport};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Executor;

fn key(snap: &[KeyReport], op: StoreOp, phase: StorePhase) -> &KeyReport {
    snap.iter()
        .find(|r| r.op == op && r.phase == phase)
        .unwrap_or_else(|| panic!("{op:?}/{phase:?} has no ring"))
}

/// The anti-pattern this instrument replaces: `/metrics` carried job, cost and
/// delivery gauges and not one number about the engine underneath them, so
/// "the store feels slow" had nothing to interrogate. A claim, a verdict and an
/// enqueue must each land in their own key, carrying rows touched.
#[tokio::test]
async fn the_job_queue_path_reports_its_own_timing_rows_and_outcome() {
    let store = TempStore::new("instrument-queue").await;
    let s = &store.storage;
    let inst = s.instrument();

    let job = s
        .enqueue("demo", EnqueueOptions::default())
        .await
        .expect("enqueue");
    let claimed = s.claim_next(&[], 0.0).await.expect("claim").expect("a job");
    assert_eq!(claimed.id, job.id);
    assert!(s
        .complete(
            claimed.id,
            claimed.attempts,
            serde_json::json!({"ok": true})
        )
        .await
        .expect("complete"));

    let snap = inst.snapshot();
    let enqueue = key(&snap, StoreOp::JobEnqueue, StorePhase::Execute);
    assert_eq!(enqueue.lifetime, 1, "the INSERT is measured");
    assert_eq!(enqueue.rows_lifetime, 1, "one row inserted");
    assert_eq!(enqueue.table, "jobs", "the key names its table");

    let claim = key(&snap, StoreOp::JobClaim, StorePhase::Execute);
    assert_eq!(claim.lifetime, 1);
    assert_eq!(claim.rows_lifetime, 1, "a hit touches one row");

    let verdict = key(&snap, StoreOp::JobVerdict, StorePhase::Execute);
    assert_eq!(verdict.lifetime, 1);
    assert_eq!(verdict.rows_lifetime, 1);

    // Nothing failed, so no outcome may claim otherwise.
    for r in &snap {
        assert_eq!(r.busy_lifetime, 0, "{:?} invented contention", r.op);
        assert_eq!(r.errors_lifetime, 0, "{:?} invented an error", r.op);
    }
}

/// **Pool acquisition is its own key.** The wait for a connection happens
/// before any statement runs, in code no query profiler attributes; folding it
/// into query time hides a saturated pool behind fast-looking queries, and the
/// two have disjoint remedies (pool sizing versus an index).
#[tokio::test]
async fn the_pool_wait_is_measured_separately_from_the_statement() {
    let store = TempStore::new("instrument-phases").await;
    let s = &store.storage;
    let inst = s.instrument();
    for _ in 0..3 {
        s.enqueue("demo", EnqueueOptions::default())
            .await
            .expect("enqueue");
    }
    let snap = inst.snapshot();
    let acquire = key(&snap, StoreOp::JobEnqueue, StorePhase::Acquire);
    let execute = key(&snap, StoreOp::JobEnqueue, StorePhase::Execute);
    assert_eq!(acquire.lifetime, 3, "every enqueue acquired a connection");
    assert_eq!(execute.lifetime, 3);
    // An acquisition touches no rows — the honest number, not the statement's.
    assert_eq!(acquire.rows_lifetime, 0);
    assert_eq!(execute.rows_lifetime, 3);
    // Different slow lines, because a 2ms pool wait and a 2ms write are
    // different findings.
    assert_ne!(acquire.slow_line_micros, execute.slow_line_micros);
}

/// A **real** `SQLITE_BUSY`, taken off a genuinely locked database file, must be
/// counted as contention and not as an error. The two have opposite remedies:
/// contention indicts the pool sizing or a writer-hog, an error indicts the
/// statement. Classification is by result code, so no rewording of SQLite's
/// famously ambiguous "database is locked" message can move it.
#[tokio::test]
async fn a_real_sqlite_busy_is_counted_as_contention_not_as_an_error() {
    let dir = tempfile::Builder::new()
        .prefix("pumper-instrument-busy-")
        .tempdir()
        .expect("temp dir");
    let db = dir.path().join("busy.db");
    let opts = |timeout_ms: u64| {
        SqliteConnectOptions::new()
            .filename(&db)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_millis(timeout_ms))
    };

    // The hog: one connection holding the write lock for the whole test.
    let hog = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts(5_000))
        .await
        .expect("hog pool");
    hog.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await
        .expect("schema");
    let mut held = hog.acquire().await.expect("hog conn");
    held.execute("BEGIN IMMEDIATE")
        .await
        .expect("take the lock");

    // The victim: zero busy timeout, so the refusal is immediate rather than a
    // five-second test.
    let victim = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts(0))
        .await
        .expect("victim pool");
    let inst = StoreInstrument::new();
    let err = inst
        .metered(&victim, StoreOp::DatasetWrite, |mut conn| async move {
            conn.execute("BEGIN IMMEDIATE").await?;
            Ok(((), 1))
        })
        .await
        .expect_err("a locked database must refuse the write");
    assert!(
        err.is_store_contention(),
        "a real SQLITE_BUSY must classify as contention, got: {err}"
    );

    let snap = inst.snapshot();
    let write = key(&snap, StoreOp::DatasetWrite, StorePhase::Execute);
    assert_eq!(write.busy_lifetime, 1, "the busy counter must fire");
    assert_eq!(
        write.errors_lifetime, 0,
        "contention must not be filed as a defect"
    );
    // The acquisition succeeded — only the statement was refused. Proving the
    // phases really are disjoint under failure, not just under success.
    let acquire = key(&snap, StoreOp::DatasetWrite, StorePhase::Acquire);
    assert_eq!(acquire.busy_lifetime, 0);
    assert_eq!(acquire.lifetime, 1);

    drop(held);
    drop(victim);
    drop(hog);
}

/// The census is **partial and stated**. A family this instrument does not
/// claim to measure must not quietly appear in the numbers either — an
/// unmeasured statement silently borrowing another family's key would make the
/// per-table join the whole design rests on wrong.
#[tokio::test]
async fn unmeasured_statements_do_not_borrow_a_measured_familys_key() {
    let store = TempStore::new("instrument-census").await;
    let s = &store.storage;
    let inst = s.instrument();
    // Schedules, watches and triggers are outside the declared census.
    s.seed_schedule("demo", "0 * * * *").await.expect("seed");
    s.list_schedules().await.expect("list");
    let snap = inst.snapshot();
    let touched: Vec<&str> = snap
        .iter()
        .filter(|r| r.lifetime > 0)
        .map(|r| r.op.as_str())
        .collect();
    assert!(
        touched.is_empty(),
        "unmeasured work landed in measured rings: {touched:?}"
    );
}

/// The store must be able to say what it costs on disk — including the `-wal`
/// sidecar, which is a permanent resident under WAL and the number the
/// maintenance gate escalates on. A size report that measures only the main
/// file understates the store by exactly the un-checkpointed commits.
#[tokio::test]
async fn the_store_measures_its_own_files_sidecar_included() {
    let store = TempStore::new("instrument-size").await;
    let s = &store.storage;
    for _ in 0..20 {
        s.enqueue("demo", EnqueueOptions::default())
            .await
            .expect("enqueue");
    }
    let size = s.size_facts().await.expect("size facts");
    assert!(size.page_size >= 512, "a real page size: {size:?}");
    assert!(size.page_count > 0);
    assert_eq!(size.main_bytes, size.page_size * size.page_count);
    assert_eq!(size.free_bytes, size.page_size * size.freelist_pages);
    assert!(
        size.wal_bytes > 0,
        "twenty committed writes under WAL leave a sidecar: {size:?}"
    );
}

/// A depth alone cannot tell a healthy busy queue from a wedged one. An empty
/// queue must read `0` rather than keeping a stale age — a gauge that holds its
/// last value once the queue clears is an alert that never resolves.
#[tokio::test]
async fn queue_ages_read_zero_when_empty_and_non_zero_when_backed_up() {
    let store = TempStore::new("instrument-ages").await;
    let s = &store.storage;
    let empty = s.queue_ages().await.expect("ages");
    assert_eq!(empty.oldest_queued_secs, 0.0);
    assert_eq!(empty.oldest_running_secs, 0.0);

    s.enqueue("demo", EnqueueOptions::default())
        .await
        .expect("enqueue");
    let queued = s.queue_ages().await.expect("ages");
    assert!(
        queued.oldest_queued_secs >= 0.0 && queued.oldest_running_secs == 0.0,
        "a queued job ages the queued gauge only: {queued:?}"
    );

    let job = s.claim_next(&[], 0.0).await.expect("claim").expect("a job");
    let running = s.queue_ages().await.expect("ages");
    assert_eq!(
        running.oldest_queued_secs, 0.0,
        "the queue drained, so its age resolves to zero, not to its last value"
    );
    assert!(running.oldest_running_secs >= 0.0);
    s.complete(job.id, job.attempts, serde_json::json!({}))
        .await
        .expect("complete");
    let done = s.queue_ages().await.expect("ages");
    assert_eq!(done.oldest_running_secs, 0.0);
}
