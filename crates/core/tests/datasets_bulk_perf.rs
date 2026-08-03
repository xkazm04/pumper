//! Bulk-upsert cost harness — `#[ignore]`d, run with `just test-ignored`.
//!
//! Reports the two numbers the batch write path is judged on:
//!
//! 1. **Wall clock** of a ~50k-record `upsert_many` (all-new, then the
//!    all-unchanged re-sync every scheduled run actually does).
//! 2. **Write-lock hold time as another app experiences it** — a competing
//!    writer on a second connection upserts one tiny record in a loop while the
//!    bulk sync runs, and we report its worst stall. That is the metric behind
//!    "cross-app write stalls during a large sync": the DB-wide write lock is
//!    held for a whole chunk, so the other app's `BEGIN IMMEDIATE` waits.
//!
//! Timing-dependent by construction, so it asserts nothing tight — it prints.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pumper_core::testing::TempStore;
use pumper_core::Datasets;
use serde_json::json;

const N: usize = 50_000;

fn corpus(n: usize, salt: u64) -> Vec<(String, serde_json::Value)> {
    (0..n)
        .map(|i| {
            (
                format!("posting-{i:06}"),
                json!({
                    "id": i,
                    "title": format!("Software engineer {i} at company {}", i % 977),
                    "location": format!("City {}", i % 43),
                    "salary": 30_000 + (i as u64 % 70_000) + salt,
                    "description": format!(
                        "We are looking for a candidate with skills in area {} to join team {}.",
                        i % 31,
                        i % 17
                    ),
                }),
            )
        })
        .collect()
}

/// Runs `body` while a competing writer hammers a *different* dataset on a
/// different connection; returns (body duration, competing writer's worst stall,
/// number of competing writes).
async fn with_competing_writer<F, Fut, T>(ds: Arc<Datasets>, body: F) -> (Duration, Rival, T)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let stop = Arc::new(AtomicBool::new(false));
    let rival = {
        let ds = Arc::clone(&ds);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut r = Rival::default();
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                // A `database is locked` here is not a test bug — it is the
                // measurement: the bulk sync held the DB-wide write lock past
                // the 5s busy_timeout and starved another app's writer out.
                match ds
                    .upsert("rival", "d", "k", &json!({ "n": r.writes }))
                    .await
                {
                    Ok(_) => r.writes += 1,
                    Err(_) => r.starved += 1,
                }
                r.worst = r.worst.max(t.elapsed());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            r
        })
    };

    let t = Instant::now();
    let out = body().await;
    let elapsed = t.elapsed();
    stop.store(true, Ordering::Relaxed);
    let r = rival.await.expect("rival task");
    (elapsed, r, out)
}

/// What the competing writer experienced while the bulk sync ran.
#[derive(Default)]
struct Rival {
    /// Successful small writes.
    writes: u64,
    /// Writes that gave up after the 5s `busy_timeout` (SQLITE_BUSY).
    starved: u64,
    /// Worst single-write latency, i.e. the observed write-lock hold time.
    worst: Duration,
}

impl std::fmt::Display for Rival {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "worst stall {:>8.1}ms  ok {:>5}  starved(BUSY) {:>4}",
            self.worst.as_secs_f64() * 1000.0,
            self.writes,
            self.starved
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "perf harness: ~50k records, timing-dependent"]
async fn bulk_upsert_50k_cost_report() {
    let store = TempStore::new("datasets-bulk-perf").await;
    let ds = Arc::new(Datasets::new(store.storage.pool()));

    let fresh = corpus(N, 0);
    let (insert_wall, insert_rival, s1) = with_competing_writer(Arc::clone(&ds), || async {
        ds.upsert_many("bench", "postings", &fresh).await.unwrap()
    })
    .await;
    assert_eq!(s1.new.len(), N);

    let (unchanged_wall, unchanged_rival, s2) = with_competing_writer(Arc::clone(&ds), || async {
        ds.upsert_many("bench", "postings", &fresh).await.unwrap()
    })
    .await;
    assert_eq!(s2.unchanged, N);

    let touched = corpus(N, 1);
    let (changed_wall, changed_rival, s3) = with_competing_writer(Arc::clone(&ds), || async {
        ds.upsert_many("bench", "postings", &touched).await.unwrap()
    })
    .await;
    assert_eq!(s3.changed.len(), N);

    println!("\n=== bulk upsert cost, N={N} ===");
    println!(
        "all-new       wall {:>8.2}s   {insert_wall_rival}",
        insert_wall.as_secs_f64(),
        insert_wall_rival = insert_rival
    );
    println!(
        "all-unchanged wall {:>8.2}s   {unchanged_rival}",
        unchanged_wall.as_secs_f64()
    );
    println!(
        "all-changed   wall {:>8.2}s   {changed_rival}",
        changed_wall.as_secs_f64()
    );

    // Write-lock HOLD time, measured directly: a batch of exactly one commit
    // chunk is one `BEGIN IMMEDIATE` … `COMMIT` window, so its wall clock IS how
    // long the DB-wide write lock is denied to every other app.
    let store = TempStore::new("datasets-bulk-perf-chunk").await;
    let ds = Datasets::new(store.storage.pool());
    let one_chunk = corpus(500, 0);
    let t = Instant::now();
    ds.upsert_many("bench", "one", &one_chunk).await.unwrap();
    let hold_new = t.elapsed();
    let t = Instant::now();
    ds.upsert_many("bench", "one", &one_chunk).await.unwrap();
    let hold_unchanged = t.elapsed();
    let touched_chunk = corpus(500, 1);
    let t = Instant::now();
    ds.upsert_many("bench", "one", &touched_chunk)
        .await
        .unwrap();
    let hold_changed = t.elapsed();
    println!("\n=== write-lock hold time, one 500-record chunk (1 transaction) ===");
    println!("new       {:>7.1}ms", hold_new.as_secs_f64() * 1000.0);
    println!("unchanged {:>7.1}ms", hold_unchanged.as_secs_f64() * 1000.0);
    println!("changed   {:>7.1}ms", hold_changed.as_secs_f64() * 1000.0);
}
