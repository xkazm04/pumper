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

#[tokio::test]
#[ignore = "perf harness: ~50k records, timing-dependent"]
async fn duplicate_scan_50k_cost_report() {
    // Banded candidate lookup vs the all-pairs scan it replaced, on the same
    // 50k rows. The all-pairs reference runs here (not in the store) so the
    // comparison is like-for-like on the same fingerprints.
    let store = TempStore::new("datasets-dup-perf").await;
    let ds = Datasets::new(store.storage.pool());
    ds.upsert_many("bench", "postings", &distinct_corpus(N))
        .await
        .unwrap();

    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT key, simhash FROM records \
         WHERE app = 'bench' AND dataset = 'postings' AND removed_at IS NULL",
    )
    .fetch_all(&store.storage.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), N);

    println!("\n=== duplicate scan, N={N} ===");
    for distance in [3u32, 8, 20] {
        let t = Instant::now();
        let banded = ds
            .duplicate_pairs("bench", "postings", distance)
            .await
            .unwrap();
        let banded_ms = t.elapsed().as_secs_f64() * 1000.0;

        let t = Instant::now();
        let brute = all_pairs_scan(&rows, distance);
        let brute_ms = t.elapsed().as_secs_f64() * 1000.0;

        assert_eq!(banded.len(), brute.len(), "pair counts must agree");
        println!(
            "distance {distance:>2}: banded {banded_ms:>9.1}ms   all-pairs {brute_ms:>9.1}ms   \
             ({} pairs)",
            banded.len()
        );
    }
}

/// A corpus of genuinely DISTINCT records — the shape a duplicate scan is
/// actually run over (a grants/postings corpus where near-dups are the rare
/// finding, not the norm), plus a small planted set of true near-duplicates so
/// the scan has something to find. `corpus()` above is the opposite extreme:
/// every record differs only in a number, so nearly all 1.25e9 pairs are
/// near-dups and the MAX_DUP_PAIRS cap binds on the very first row.
fn distinct_corpus(n: usize) -> Vec<(String, serde_json::Value)> {
    // A wide vocabulary: 24 tokens drawn from 200k make each record's token set
    // essentially unique, which is what makes the fingerprints spread across the
    // band buckets the way a real corpus of distinct documents does.
    let mut seed = 0x243f_6a88_85a3_08d3u64;
    let mut next = move || {
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        seed.wrapping_mul(0x2545_f491_4f6c_dd1d)
    };
    let mut out: Vec<(String, serde_json::Value)> = Vec::with_capacity(n);
    for i in 0..n {
        // Every 500th record is a near-copy of its predecessor: a real
        // duplicate the scan must still find.
        if i % 500 == 499 && !out.is_empty() {
            let mut near = out[i - 1].1.clone();
            near["id"] = json!(i);
            out.push((format!("doc-{i:06}"), near));
            continue;
        }
        let body: String = (0..24)
            .map(|_| format!("tok{}", next() % 200_000))
            .collect::<Vec<_>>()
            .join(" ");
        out.push((format!("doc-{i:06}"), json!({ "id": i, "body": body })));
    }
    out
}

/// The O(n²) scan `duplicate_pairs` used to do, kept here as the perf reference.
fn all_pairs_scan(rows: &[(String, i64)], max_distance: u32) -> Vec<(usize, usize, u32)> {
    const MAX_DUP_PAIRS: usize = 10_000;
    let mut pairs = Vec::new();
    'scan: for i in 0..rows.len() {
        if rows[i].1 == 0 {
            continue;
        }
        for j in (i + 1)..rows.len() {
            if rows[j].1 == 0 {
                continue;
            }
            let distance = pumper_core::simhash::hamming(rows[i].1 as u64, rows[j].1 as u64);
            if distance <= max_distance {
                pairs.push((i, j, distance));
                if pairs.len() >= MAX_DUP_PAIRS {
                    break 'scan;
                }
            }
        }
    }
    pairs
}
