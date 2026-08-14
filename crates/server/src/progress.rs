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
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pumper_core::ProgressReporter;
use serde_json::{json, Value};
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
    /// Saves that did not land, by kind. Atomics, not a second mutex: this is on
    /// the checkpoint path, which already pays for the throttle lock. `Relaxed`
    /// is enough — the tally is read after the app future has completed, and no
    /// other state is published through it.
    stale_lineage: AtomicU64,
    storage_error: AtomicU64,
    /// Set by [`announcing`](Self::announcing): the bus a first-of-its-kind
    /// failure is announced on. `None` in tests and embedders — the counting
    /// works either way.
    events: Option<(String, Arc<EventBus>)>,
}

/// Job-event status for a checkpoint that did not land. Non-terminal (like
/// `progress`), so a `/jobs/{id}/stream` subscriber sees it and the stream
/// stays open.
pub const CHECKPOINT_FAILED_STATUS: &str = "checkpoint_failed";

/// The key a run's failed-checkpoint tally rides on its stored result.
pub const CHECKPOINT_FAILURES_KEY: &str = "checkpoint_failures";

/// Why a checkpoint save did not land. The two mean different things to an
/// operator: `StaleLineage` says **another attempt owns this job** (this task's
/// state is supposed to lose), while `StorageError` says the disk or the blob is
/// the problem and **this run has no durable state at all** — the next reap or
/// restart will resume it from nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointFailure {
    StaleLineage,
    StorageError,
}

impl CheckpointFailure {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StaleLineage => "stale_lineage",
            Self::StorageError => "storage_error",
        }
    }
}

/// A run's tally of checkpoint saves that did not land, by kind. A
/// **throttle-skipped** save is not a failure and is never counted here —
/// conflating the two would manufacture the alarm this exists to report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckpointFailures {
    pub stale_lineage: u64,
    pub storage_error: u64,
}

impl CheckpointFailures {
    pub fn total(self) -> u64 {
        self.stale_lineage + self.storage_error
    }

    pub fn is_empty(self) -> bool {
        self.total() == 0
    }
}

/// Whether a failure of this kind announces itself on the bus. Only the first
/// of each kind does: a run whose lineage went stale fails **every** later save,
/// and one event per save would turn a durability alarm into a flood. The
/// counter carries the rest.
fn announces_failure(prior_of_this_kind: u64) -> bool {
    prior_of_this_kind == 0
}

/// The `checkpoint_failures` block a run's stored result carries. `None` when
/// every save landed: an all-zero block on every job would bury the ones that
/// mean something, and this repo reports visible gaps, not fabricated zeros.
pub fn checkpoint_failure_stamp(failures: CheckpointFailures) -> Option<Value> {
    (!failures.is_empty()).then(|| {
        json!({
            "stale_lineage": failures.stale_lineage,
            "storage_error": failures.storage_error,
            "total": failures.total(),
        })
    })
}

impl JobCheckpointer {
    pub fn new(job_id: Uuid, attempt: i64, storage: std::sync::Arc<pumper_core::Storage>) -> Self {
        Self {
            job_id,
            attempt,
            storage,
            last: Mutex::new(None),
            stale_lineage: AtomicU64::new(0),
            storage_error: AtomicU64::new(0),
            events: None,
        }
    }

    /// Announces the first failure of each kind as a `checkpoint_failed` job
    /// event on `events`, so a dropped checkpoint is visible **while the job is
    /// still running** instead of only in a log line nobody is tailing.
    #[must_use]
    pub fn announcing(mut self, app: impl Into<String>, events: Arc<EventBus>) -> Self {
        self.events = Some((app.into(), events));
        self
    }

    /// This run's saves that did not land, by kind.
    pub fn failures(&self) -> CheckpointFailures {
        CheckpointFailures {
            stale_lineage: self.stale_lineage.load(Ordering::Relaxed),
            storage_error: self.storage_error.load(Ordering::Relaxed),
        }
    }

    /// Counts one failed save and, if it is the first of its kind, announces it.
    fn record_failure(&self, kind: CheckpointFailure) {
        let counter = match kind {
            CheckpointFailure::StaleLineage => &self.stale_lineage,
            CheckpointFailure::StorageError => &self.storage_error,
        };
        let prior = counter.fetch_add(1, Ordering::Relaxed);
        if !announces_failure(prior) {
            return;
        }
        if let Some((app, events)) = &self.events {
            let mut event = JobEvent::new(self.job_id, app.clone(), CHECKPOINT_FAILED_STATUS);
            event.result = Some(json!({
                "reason": kind.as_str(),
                "attempt": self.attempt,
            }));
            events.emit(event);
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
                // Deliberately NOT counted — the previous snapshot is still
                // durable, so counting it would report a loss that never was.
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
                self.record_failure(CheckpointFailure::StaleLineage);
                false
            }
            Err(e) => {
                tracing::warn!(job = %self.job_id, "checkpoint write failed: {e}");
                self.record_failure(CheckpointFailure::StorageError);
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

    /// The two `false` returns mean different things and must be counted
    /// apart — and a throttle-skip, which is a deliberate `true`, must never be
    /// counted as either.
    #[tokio::test]
    async fn a_throttle_skipped_save_is_not_counted_as_a_failed_one() {
        use pumper_core::CheckpointSink;
        let store = pumper_core::testing::TempStore::new("cp-failures").await;
        let storage = std::sync::Arc::new(store.storage.clone());
        let job = storage
            .enqueue("crawl", pumper_core::EnqueueOptions::default())
            .await
            .unwrap();
        let claimed = storage.claim_next(&[], 0.0).await.unwrap().unwrap();

        let cp = JobCheckpointer::new(job.id, claimed.attempts, storage.clone());
        assert!(cp.save(json!({ "n": 1 }), false).await, "first save lands");
        assert!(
            cp.save(json!({ "n": 2 }), false).await,
            "throttle-skipped, reported as landed"
        );
        assert_eq!(
            cp.failures(),
            CheckpointFailures::default(),
            "a coalesced save is not a lost one — counting it would manufacture \
             the very alarm this tally exists to raise"
        );
        assert!(checkpoint_failure_stamp(cp.failures()).is_none());

        // Stale lineage: another attempt owns the row.
        let stale = JobCheckpointer::new(job.id, claimed.attempts - 1, storage.clone());
        assert!(!stale.save(json!({ "n": 99 }), true).await);
        assert_eq!(
            stale.failures(),
            CheckpointFailures {
                stale_lineage: 1,
                storage_error: 0
            }
        );

        // Storage error: the blob is over the 8 MiB cap. A different kind, and
        // it must not land in the lineage bucket.
        let big = json!({ "big": "x".repeat(pumper_core::MAX_CHECKPOINT_BYTES) });
        assert!(!cp.save(big, true).await);
        assert_eq!(
            cp.failures(),
            CheckpointFailures {
                stale_lineage: 0,
                storage_error: 1
            }
        );
        assert_eq!(
            checkpoint_failure_stamp(cp.failures()),
            Some(json!({ "stale_lineage": 0, "storage_error": 1, "total": 1 })),
            "the surviving surface names WHICH failure, not just that one happened"
        );
    }

    /// A run whose lineage went stale fails every subsequent save. One event per
    /// save would flood the bus with the same fact; the counter carries the rest.
    #[tokio::test]
    async fn a_repeated_failure_announces_once_and_then_only_counts() {
        use pumper_core::CheckpointSink;
        assert!(announces_failure(0));
        assert!(!announces_failure(1));
        assert!(!announces_failure(97));

        let store = pumper_core::testing::TempStore::new("cp-announce").await;
        let storage = std::sync::Arc::new(store.storage.clone());
        let job = storage
            .enqueue("crawl", pumper_core::EnqueueOptions::default())
            .await
            .unwrap();
        let claimed = storage.claim_next(&[], 0.0).await.unwrap().unwrap();
        let events = Arc::new(EventBus::new(64, 1 << 20));

        let stale = JobCheckpointer::new(job.id, claimed.attempts - 1, storage.clone())
            .announcing("crawl", events.clone());
        for _ in 0..4 {
            assert!(!stale.save(json!({ "n": 1 }), true).await);
        }
        assert_eq!(events.latest_seq(), 1, "announced once, not four times");
        assert_eq!(stale.failures().stale_lineage, 4, "counted every time");

        // A second KIND is its own news, so it announces on its own.
        let big = json!({ "big": "x".repeat(pumper_core::MAX_CHECKPOINT_BYTES) });
        assert!(!stale.save(big, true).await);
        assert_eq!(events.latest_seq(), 2);
        assert_eq!(stale.failures().storage_error, 1);
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
