//! Live job-progress seam. Long-running apps (the crawler) report compact
//! snapshots through [`pumper_core::ProgressReporter`]; the runtime keeps only
//! the latest snapshot per job in memory (surfaced on `GET /jobs/{id}`) and
//! emits it as a `progress` job event through the [`EventBus`] so
//! `/jobs/{id}/stream` and `/events` subscribers see it live.
//!
//! Progress is in-flight telemetry only: it lives in an in-memory map, NOT the
//! jobs table. Chosen over an append-only column because (1) a 100k-page crawl
//! would otherwise write the jobs row on a hot path, (2) the terminal result
//! already persists the final counts, and (3) losing in-flight progress across a
//! restart is acceptable — the job is re-queued and re-reports. Each reporter
//! throttles its own persist+emit to ≥ every 2s or `MAX_UPDATES` calls so a
//! tight in-loop stride never floods the EventBus.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pumper_core::ProgressReporter;
use serde_json::Value;
use uuid::Uuid;

use crate::events::{EventBus, JobEvent};

/// Minimum wall-clock spacing between a reporter's persist+emit ticks.
const MIN_INTERVAL: Duration = Duration::from_secs(2);
/// Force a persist+emit after this many `report` calls even inside the interval,
/// so a fast crawl still advances the snapshot between time ticks.
const MAX_UPDATES: u32 = 50;

/// Latest-progress store: one JSON snapshot per in-flight job. Cleared when the
/// job finalizes.
#[derive(Default)]
pub struct ProgressStore {
    latest: Mutex<HashMap<Uuid, Value>>,
}

impl ProgressStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// The latest reported snapshot for a job, if any is still buffered.
    pub fn snapshot(&self, id: &Uuid) -> Option<Value> {
        self.latest.lock().unwrap().get(id).cloned()
    }

    fn set(&self, id: Uuid, snapshot: Value) {
        self.latest.lock().unwrap().insert(id, snapshot);
    }

    /// Drops a finished job's buffered progress (called from `finalize`).
    pub fn clear(&self, id: &Uuid) {
        self.latest.lock().unwrap().remove(id);
    }

    /// Builds a throttled reporter bound to one job. Handed to the app via
    /// `AppContext::progress`.
    pub fn reporter(
        self: &Arc<Self>,
        job_id: Uuid,
        app: String,
        events: Arc<EventBus>,
    ) -> Arc<JobProgressReporter> {
        Arc::new(JobProgressReporter {
            job_id,
            app,
            events,
            store: self.clone(),
            last: Mutex::new(None),
            since: AtomicU32::new(0),
        })
    }
}

/// A per-job [`ProgressReporter`] that persists the latest snapshot and emits a
/// `progress` job event, throttled to ≥ every [`MIN_INTERVAL`] or every
/// [`MAX_UPDATES`] calls.
pub struct JobProgressReporter {
    job_id: Uuid,
    app: String,
    events: Arc<EventBus>,
    store: Arc<ProgressStore>,
    last: Mutex<Option<Instant>>,
    since: AtomicU32,
}

/// Minimum wall-clock spacing between a job's durable checkpoint writes. Wider
/// than the progress interval — each save is a real SQLite write of the whole
/// state blob, and losing a few seconds of progress on resume is the documented
/// contract (the frontier/seen-set style state apps checkpoint is idempotent to
/// replay).
const CHECKPOINT_MIN_INTERVAL: Duration = Duration::from_secs(5);

/// The server's [`pumper_core::CheckpointSink`]: persists a job's checkpoint
/// blob through [`Storage::save_checkpoint`], throttled like
/// [`JobProgressReporter`] (first call writes, then ≥ every
/// [`CHECKPOINT_MIN_INTERVAL`]; `force` bypasses the throttle for final/suspend
/// snapshots). Writes carry the attempt number so the storage layer's
/// attempts-lineage fence discards saves from a task whose job was reset or
/// reaped mid-run — mirroring `complete(job.id, job.attempts, ..)`.
pub struct JobCheckpointer {
    job_id: Uuid,
    attempt: i64,
    storage: std::sync::Arc<pumper_core::Storage>,
    last: Mutex<Option<Instant>>,
}

impl JobCheckpointer {
    pub fn new(job_id: Uuid, attempt: i64, storage: std::sync::Arc<pumper_core::Storage>) -> Self {
        Self {
            job_id,
            attempt,
            storage,
            last: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl pumper_core::CheckpointSink for JobCheckpointer {
    async fn save(&self, state: Value, force: bool) -> bool {
        if !force {
            let now = Instant::now();
            let mut last = self.last.lock().unwrap();
            let due = last.is_none_or(|prev| now.duration_since(prev) >= CHECKPOINT_MIN_INTERVAL);
            if !due {
                // Throttle-skipped, not failed: the caller keeps its loop cheap.
                return true;
            }
            *last = Some(now);
        } else {
            *self.last.lock().unwrap() = Some(Instant::now());
        }
        match self
            .storage
            .save_checkpoint(self.job_id, self.attempt, &state)
            .await
        {
            Ok(true) => true,
            Ok(false) => {
                // The job was reset/reaped and re-claimed; this task's state is
                // stale and must not overwrite the live attempt's checkpoint.
                tracing::warn!(job = %self.job_id, "checkpoint discarded: stale attempt lineage");
                false
            }
            Err(e) => {
                tracing::warn!(job = %self.job_id, "checkpoint write failed: {e}");
                false
            }
        }
    }
}

impl ProgressReporter for JobProgressReporter {
    fn report(&self, snapshot: Value) {
        // Throttle: emit on the first call, then only once the interval elapses
        // or MAX_UPDATES reports accumulate. The counter is reset on each emit.
        let count = self.since.fetch_add(1, Ordering::Relaxed) + 1;
        let now = Instant::now();
        let mut last = self.last.lock().unwrap();
        let due = match *last {
            None => true,
            Some(prev) => now.duration_since(prev) >= MIN_INTERVAL || count >= MAX_UPDATES,
        };
        if !due {
            return;
        }
        *last = Some(now);
        self.since.store(0, Ordering::Relaxed);
        drop(last);

        self.store.set(self.job_id, snapshot.clone());
        let mut event = JobEvent::new(self.job_id, self.app.clone(), "progress");
        event.result = Some(snapshot);
        self.events.emit(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn throttle_emits_first_then_coalesces_within_interval() {
        let store = Arc::new(ProgressStore::new());
        let events = Arc::new(EventBus::new(16, 16));
        let id = Uuid::new_v4();
        let reporter = store.reporter(id, "crawl".into(), events.clone());

        // First report always emits: snapshot buffered, one event on the bus.
        reporter.report(json!({ "crawled": 1 }));
        assert_eq!(store.snapshot(&id), Some(json!({ "crawled": 1 })));
        assert_eq!(events.latest_seq(), 1);

        // A burst within the 2s interval (and under MAX_UPDATES) is suppressed:
        // the buffered snapshot and the event count stay put.
        for n in 2..10 {
            reporter.report(json!({ "crawled": n }));
        }
        assert_eq!(
            store.snapshot(&id),
            Some(json!({ "crawled": 1 })),
            "coalesced"
        );
        assert_eq!(
            events.latest_seq(),
            1,
            "no extra events emitted mid-interval"
        );

        // Clearing drops the buffered snapshot (finalize path).
        store.clear(&id);
        assert_eq!(store.snapshot(&id), None);
    }

    #[tokio::test]
    async fn checkpointer_throttles_persists_and_respects_lineage() {
        use pumper_core::CheckpointSink;
        let store = pumper_core::testing::TempStore::new("cp-reporter").await;
        let storage = std::sync::Arc::new(store.storage.clone());
        let job = storage
            .enqueue("crawl", pumper_core::EnqueueOptions::default())
            .await
            .unwrap();
        let claimed = storage.claim_next(&[], 0.0).await.unwrap().unwrap();

        let cp = JobCheckpointer::new(job.id, claimed.attempts, storage.clone());
        // First save writes; an immediate second is throttle-skipped (reported
        // ok, blob unchanged); force bypasses the throttle.
        assert!(cp.save(json!({"n": 1}), false).await);
        assert!(cp.save(json!({"n": 2}), false).await);
        let (state, _) = storage.load_checkpoint(job.id).await.unwrap().unwrap();
        assert_eq!(state, json!({"n": 1}), "second save coalesced");
        assert!(cp.save(json!({"n": 3}), true).await);
        let (state, _) = storage.load_checkpoint(job.id).await.unwrap().unwrap();
        assert_eq!(state, json!({"n": 3}), "forced save bypasses the throttle");

        // A checkpointer holding a stale attempt lineage reports failure and
        // leaves the live blob alone.
        let stale = JobCheckpointer::new(job.id, claimed.attempts - 1, storage.clone());
        assert!(!stale.save(json!({"n": 99}), true).await);
        let (state, _) = storage.load_checkpoint(job.id).await.unwrap().unwrap();
        assert_eq!(state, json!({"n": 3}), "stale lineage never overwrites");
    }

    #[test]
    fn max_updates_forces_emit_within_interval() {
        let store = Arc::new(ProgressStore::new());
        let events = Arc::new(EventBus::new(128, 128));
        let id = Uuid::new_v4();
        let reporter = store.reporter(id, "crawl".into(), events.clone());
        // MAX_UPDATES reports advance the snapshot even without the 2s tick.
        for n in 0..=MAX_UPDATES {
            reporter.report(json!({ "n": n }));
        }
        // First call emitted (seq 1); the MAX_UPDATES-th since then forces a 2nd.
        assert_eq!(events.latest_seq(), 2);
        assert_eq!(store.snapshot(&id), Some(json!({ "n": MAX_UPDATES })));
    }
}
