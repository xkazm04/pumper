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
//! It used to print those numbers and assert nothing about them — no percentile,
//! no ceiling, no schedule, no artifact, no trend, which is a harness that can
//! only ever report that it ran. It now **measures and emits**; the pass criteria
//! are pre-declared in `.lanes/criteria.json` and judged by
//! `scripts/ci/lane-certify.mjs` (`just lanes`, and the nightly `long-lanes` CI
//! leg). Keeping the judgement out of here is what stops a bound being quietly
//! relaxed in the same commit that broke it.

mod lane_artifact;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lane_artifact::Lane;
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
                let waited = t.elapsed();
                // Every sample, not just the worst: a single maximum cannot be
                // judged at a percentile, and the whole point of the lane is
                // that an average hides exactly the tail it exists to see.
                r.samples.push(waited);
                r.worst = r.worst.max(waited);
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
    /// Every single-write latency, for the percentile bounds the lane judges.
    samples: Vec<Duration>,
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
#[ignore = "long lane `datasets-bulk-upsert` — needs a ~50k-record corpus, minutes not seconds; criteria in .lanes/criteria.json, run by `just lanes` and the nightly CI leg"]
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

    // Write-lock HOLD time, measured directly and AS A SEQUENCE: a batch of
    // exactly one commit chunk is one `BEGIN IMMEDIATE` … `COMMIT` window, so
    // its wall clock IS how long the DB-wide write lock is denied to every
    // other app. Taking a hundred of them as the table grows from 0 to 50k rows
    // is what separates warm-up from growth: a single endpoint number is
    // compatible with a per-chunk cost that has been climbing all along, and
    // the criterion the lane judges is the SLOPE over the run's second half.
    const CHUNK: usize = 500;
    const CHUNKS: usize = N / CHUNK;
    let store = TempStore::new("datasets-bulk-perf-chunk").await;
    let ds = Datasets::new(store.storage.pool());
    let mut hold_new: Vec<Duration> = Vec::with_capacity(CHUNKS);
    for c in 0..CHUNKS {
        let slice = fresh[c * CHUNK..(c + 1) * CHUNK].to_vec();
        let t = Instant::now();
        ds.upsert_many("bench", "chunked", &slice).await.unwrap();
        hold_new.push(t.elapsed());
    }
    let mut hold_unchanged: Vec<Duration> = Vec::with_capacity(CHUNKS);
    for c in 0..CHUNKS {
        let slice = fresh[c * CHUNK..(c + 1) * CHUNK].to_vec();
        let t = Instant::now();
        ds.upsert_many("bench", "chunked", &slice).await.unwrap();
        hold_unchanged.push(t.elapsed());
    }
    let ms = |d: &Duration| d.as_secs_f64() * 1000.0;
    println!(
        "\n=== write-lock hold time, {CHUNKS} x {CHUNK}-record chunks (1 transaction each) ==="
    );
    println!(
        "new       first {:>7.1}ms  last {:>7.1}ms",
        ms(&hold_new[0]),
        ms(hold_new.last().unwrap())
    );
    println!(
        "unchanged first {:>7.1}ms  last {:>7.1}ms",
        ms(&hold_unchanged[0]),
        ms(hold_unchanged.last().unwrap())
    );

    let mut lane = Lane::new(
        "datasets-bulk-upsert",
        json!({
            "records": N,
            "chunk_records": CHUNK,
            "chunks": CHUNKS,
            "record_shape": "synthetic job posting: id, title, location, salary, description (~250 bytes of JSON)",
            "competing_writer": "one rival app upserting a single-key record on a SECOND connection every 1ms, sqlite busy_timeout 5s",
            "shape_fidelity": "DECLARED-APPROXIMATE. The record shape is modelled on the grants/postings corpora this store actually holds, but the real arrival mix — burstiness, per-source size skew, concurrent readers — is NOT reproduced. Every bound in .lanes/criteria.json certifies THIS traffic and no other.",
        }),
    );
    lane.durations_ms("chunk_hold_ms_new", &hold_new)
        .durations_ms("chunk_hold_ms_unchanged", &hold_unchanged)
        .durations_ms("rival_stall_ms", &insert_rival.samples)
        .secs("all_new_wall_s", insert_wall)
        .secs("all_unchanged_wall_s", unchanged_wall)
        .secs("all_changed_wall_s", changed_wall)
        .scalar(
            "rival_starved",
            (insert_rival.starved + unchanged_rival.starved + changed_rival.starved) as f64,
        )
        .scalar("rival_writes", insert_rival.writes as f64)
        .ms("rival_worst_ms", insert_rival.worst);
    lane.emit();
}

#[tokio::test]
#[ignore = "long lane `datasets-duplicate-scan` — needs a ~50k-record corpus; criteria in .lanes/criteria.json, run by `just lanes` and the nightly CI leg"]
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
    let mut lane = Lane::new(
        "datasets-duplicate-scan",
        json!({
            "records": N,
            "record_shape": "24 tokens drawn from a 200k vocabulary, so fingerprints spread across band buckets the way a corpus of genuinely distinct documents does; every 500th record is a planted near-duplicate",
            "distances": [3, 8, 20],
            "reference": "the O(n^2) all-pairs scan `duplicate_pairs` replaced, run in-process on the SAME fingerprints",
            "shape_fidelity": "DECLARED-APPROXIMATE for absolute cost, EXACT for the comparison — both halves see identical input, which is why the lane's bounds are ratios against the reference rather than milliseconds that would only certify this runner's CPU.",
        }),
    );
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
        lane.scalar(&format!("banded_ms_d{distance}"), banded_ms)
            .scalar(&format!("all_pairs_ms_d{distance}"), brute_ms)
            .scalar(&format!("pairs_d{distance}"), banded.len() as f64);
    }
    lane.emit();
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
