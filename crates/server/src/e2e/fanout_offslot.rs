//! The post-completion fan-out running **off** the worker's concurrency permit
//! (`[worker] fanout_concurrency`), measured against a deliberately slow search
//! index — the cheapest realistic stand-in for the fan-out work that actually
//! hurts in production (a large index commit, a big materialization).
//!
//! The point of the change is throughput, so the headline test measures it,
//! with the pool disabled (`fanout_concurrency = 0`, the historical inline
//! behaviour) as the control arm. The other two tests are the guardrails that
//! make the speed-up honest: off-slot work must still *complete*, and shutdown
//! must not silently lose it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pumper_core::{
    EnqueueOptions, JobStatus, Result, Search, SearchDoc, SearchRequest, SearchResponse,
};
use serde_json::json;

use super::harness::{test_state_indexed, wait_status, FakeApp, WorkerLoop};

/// A search index that takes `delay` to accept a batch, and counts batches.
/// Everything else is `NoSearch`.
struct SlowSearch {
    delay: Duration,
    batches: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Search for SlowSearch {
    async fn index(&self, _docs: Vec<SearchDoc>) -> Result<()> {
        tokio::time::sleep(self.delay).await;
        self.batches.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn query(&self, _req: SearchRequest) -> Result<SearchResponse> {
        Ok(SearchResponse::default())
    }
    async fn delete_ids(&self, _ids: &[String]) -> Result<()> {
        Ok(())
    }
    async fn delete_dataset(&self, _app: &str, _dataset: &str) -> Result<()> {
        Ok(())
    }
    async fn doc_count(&self) -> Result<u64> {
        Ok(0)
    }
}

const JOBS: usize = 6;
const SCRAPE_MS: u64 = 40;
const INDEX_MS: u64 = 300;
const WORKER_CONCURRENCY: usize = 2;

/// Runs [`JOBS`] trivial jobs through the real loop at concurrency
/// [`WORKER_CONCURRENCY`] against an index that takes [`INDEX_MS`], and returns
/// how long the queue took to drain them all.
async fn drain_millis(fanout_concurrency: usize) -> (u128, usize) {
    let batches = Arc::new(AtomicUsize::new(0));
    let search = Arc::new(SlowSearch {
        delay: Duration::from_millis(INDEX_MS),
        batches: batches.clone(),
    });
    let (state, _store) = test_state_indexed(vec![Arc::new(FakeApp)], search, |c| {
        c.worker.concurrency = WORKER_CONCURRENCY;
        c.worker.fanout_concurrency = fanout_concurrency;
        c.worker.shutdown_drain_secs = 10;
    })
    .await;
    let mut ids = Vec::new();
    for _ in 0..JOBS {
        ids.push(
            state
                .storage
                .enqueue(
                    "fake",
                    EnqueueOptions {
                        params: json!({ "sleep_ms": SCRAPE_MS }),
                        ..Default::default()
                    },
                )
                .await
                .expect("enqueue")
                .id,
        );
    }

    let started = Instant::now();
    let worker_loop = WorkerLoop::start(&state);
    for id in ids {
        wait_status(&state, id, JobStatus::Succeeded, Duration::from_secs(60)).await;
    }
    let elapsed = started.elapsed().as_millis();
    // The shutdown drain is what proves the off-slot work still finished.
    worker_loop.shutdown(&state, Duration::from_secs(30)).await;
    (elapsed, batches.load(Ordering::SeqCst))
}

/// The regression this direction exists for: a slow index used to run inline in
/// the per-job task **while it still held a worker permit**, so the queue's
/// scrape throughput was gated on derived, outbound work.
///
/// With the pool disabled the queue can only retire `WORKER_CONCURRENCY` jobs
/// per (scrape + index) round; with it enabled the permit is released after the
/// completion write, so the rounds cost scrape time only.
#[tokio::test]
async fn a_slow_index_no_longer_holds_a_scrape_permit() {
    let (inline_ms, inline_batches) = drain_millis(0).await;
    let (offslot_ms, offslot_batches) = drain_millis(4).await;
    println!(
        "fan-out throughput: {JOBS} jobs @ concurrency {WORKER_CONCURRENCY}, \
         scrape {SCRAPE_MS}ms + index {INDEX_MS}ms — inline {inline_ms}ms \
         ({inline_batches} index batches) vs off-slot {offslot_ms}ms \
         ({offslot_batches} index batches)"
    );
    // Neither arm may lose work: off-slot is a scheduling change, not a
    // sampling one.
    assert_eq!(inline_batches, JOBS, "inline arm must index every job");
    assert_eq!(offslot_batches, JOBS, "off-slot arm must index every job");
    // Analytically: inline ≈ ceil(6/2) × 340ms ≈ 1020ms, off-slot ≈ ceil(6/2) ×
    // 40ms ≈ 120ms. Asserting only a 2× improvement leaves generous headroom
    // for a loaded CI box while still failing if the fan-out creeps back onto
    // the permit.
    assert!(
        offslot_ms * 2 < inline_ms,
        "moving the fan-out off the permit must at least halve the drain time; \
         inline {inline_ms}ms vs off-slot {offslot_ms}ms"
    );
}

/// Off-slot must not mean fire-and-forget: the work still happens, and the
/// stage timings that describe it are persisted for the receipt.
#[tokio::test]
async fn off_slot_fanout_still_completes_and_records_its_stage_timings() {
    let batches = Arc::new(AtomicUsize::new(0));
    let search = Arc::new(SlowSearch {
        delay: Duration::from_millis(50),
        batches: batches.clone(),
    });
    let (state, _store) = test_state_indexed(vec![Arc::new(FakeApp)], search, |c| {
        c.worker.fanout_concurrency = 4;
    })
    .await;
    let job = state
        .storage
        .enqueue("fake", EnqueueOptions::default())
        .await
        .unwrap();

    assert!(crate::worker::run_one(&state).await);

    assert_eq!(
        batches.load(Ordering::SeqCst),
        1,
        "the run_one seam must wait for the fan-out it moved off the task"
    );
    let stages = state
        .storage
        .job_stages(job.id)
        .await
        .unwrap()
        .expect("a succeeded job records where its wall-clock went");
    assert_eq!(stages.attempt, 1);
    assert!(
        stages.run_ms.is_some(),
        "the scrape stage is always measured"
    );
    let index_ms = stages.index_ms.expect("the index stage ran");
    assert!(
        index_ms >= 50,
        "the index stage must account for the index's own latency, got {index_ms}ms"
    );
    assert!(stages.alerts_ms.is_some(), "the alert stage ran");
    let total = stages.total_ms.expect("total is always closed");
    assert!(
        total >= index_ms,
        "total ({total}ms) must cover the stages it contains ({index_ms}ms)"
    );
}

/// The drain must account for fan-out that no longer holds a permit — otherwise
/// "all permits reacquired" would falsely mean "the queue is idle" and shutdown
/// would drop in-flight index/hook/alert work without a word.
#[tokio::test]
async fn shutdown_drains_in_flight_fanout_instead_of_dropping_it() {
    let batches = Arc::new(AtomicUsize::new(0));
    let search = Arc::new(SlowSearch {
        delay: Duration::from_millis(400),
        batches: batches.clone(),
    });
    let (state, _store) = test_state_indexed(vec![Arc::new(FakeApp)], search, |c| {
        c.worker.fanout_concurrency = 4;
        // 8s window → ~1.6s of suspend/fan-out grace, comfortably over the
        // 400ms index this job's fan-out is sitting in.
        c.worker.shutdown_drain_secs = 8;
    })
    .await;
    let job = state
        .storage
        .enqueue("fake", EnqueueOptions::default())
        .await
        .unwrap();

    let worker_loop = WorkerLoop::start(&state);
    // The row goes terminal the moment the completion lands — i.e. while the
    // fan-out is still running on the pool. Shutting down here is exactly the
    // race the drain has to cover.
    wait_status(
        &state,
        job.id,
        JobStatus::Succeeded,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        batches.load(Ordering::SeqCst),
        0,
        "the job must reach its terminal state BEFORE the index finishes, or this \
         test isn't exercising the race"
    );

    worker_loop.shutdown(&state, Duration::from_secs(30)).await;

    assert_eq!(
        batches.load(Ordering::SeqCst),
        1,
        "shutdown must drain in-flight fan-out, not exit over it"
    );
    assert_eq!(state.fanout.inflight(), 0);
}
