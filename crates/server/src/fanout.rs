//! Bounded, off-slot fan-out for finished jobs.
//!
//! Everything a succeeded job does *after* its result is persisted — search
//! indexing, watch webhooks, dataset triggers, saved-search alerts and
//! materialization, the terminal event and the result webhook — used to run
//! inline in the per-job task, which still held one of the worker's
//! `[worker] concurrency` permits (default 4). A slow index or a large
//! materialization therefore burned a *scrape* slot for its whole duration:
//! the queue's throughput was gated on derived, outbound work that has nothing
//! to do with fetching.
//!
//! This pool moves that work off the permit **without making it
//! fire-and-forget**, which is the whole design constraint:
//!
//! - **Bounded concurrency** (`[worker] fanout_concurrency`), so moving the
//!   work off the slot doesn't replace one queue with an unbounded one.
//! - **Bounded backlog** (`[worker] fanout_max_queued`). At the ceiling a unit
//!   runs *inline* on the caller — see [`placement`]. Inline is slow; dropped
//!   is a silently-unsent webhook, so the backpressure is never a drop.
//! - **Drainable.** [`FanoutPool::drain`] is awaited by the worker's shutdown
//!   drain, so in-flight fan-out is finished (or *counted and logged*) instead
//!   of vanishing with the process.
//! - **Loud.** A panicking unit is caught and logged with its job id rather
//!   than dying inside an unobserved `JoinHandle`.
//!
//! The type is deliberately reusable: the server runs **two** instances. The
//! worker's (`AppState::fanout`) carries a finished job's derived work; a
//! second one (`AppState::deliveries`, sized by `crate::webhook`) carries
//! outbound webhook deliveries, which used to be bare `tokio::spawn`s outside
//! any lifecycle. They are separate instances rather than one shared pool
//! because their backpressure means different things — see the sizing rationale
//! on `webhook::DELIVERY_CONCURRENCY`.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tracing::{error, warn};
use uuid::Uuid;

/// Where one fan-out unit should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// On the pool, off the caller's worker permit.
    OffSlot,
    /// On the caller (holding its permit) — the backlog is full, or the pool is
    /// disabled. Slower, never lossy.
    Inline,
}

/// The backpressure decision, extracted so it is testable without a runtime.
///
/// `concurrency == 0` disables the pool entirely (everything inline — the
/// pre-M-fan-out behaviour, kept as an escape hatch and as the control arm of
/// the throughput benchmark). Otherwise a unit goes off-slot while the pool's
/// in-flight+queued count is below `max_queued`, and inline at or above it.
///
/// The anti-pattern this encodes: an unbounded spawn queue that looks fast
/// until memory is the thing that fails, and a "drop it when busy" policy that
/// turns a full backlog into missing webhooks.
pub fn placement(concurrency: usize, inflight: usize, max_queued: usize) -> Placement {
    if concurrency == 0 || max_queued == 0 || inflight >= max_queued {
        Placement::Inline
    } else {
        Placement::OffSlot
    }
}

/// Decrements the in-flight counter on drop, so a panicking or cancelled unit
/// can never leak backlog capacity (which would silently degrade the pool to
/// "always inline").
struct Ticket(Arc<AtomicUsize>);

impl Drop for Ticket {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct FanoutPool {
    permits: Arc<Semaphore>,
    inflight: Arc<AtomicUsize>,
    concurrency: usize,
    max_queued: usize,
}

impl FanoutPool {
    pub fn new(concurrency: usize, max_queued: usize) -> Self {
        Self {
            // `Semaphore::new(0)` is never acquired from: with concurrency 0
            // `placement` always returns Inline, so no task is ever spawned.
            permits: Arc::new(Semaphore::new(concurrency)),
            inflight: Arc::new(AtomicUsize::new(0)),
            concurrency,
            max_queued,
        }
    }

    /// Units currently spawned and not yet finished (running **or** waiting for
    /// a permit).
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    /// Runs `unit` off the caller's task when the pool has room, else inline.
    ///
    /// `what` and `job` are only used for logging, and they are what makes a
    /// failure in here visible: a panicking unit is caught and reported against
    /// its job instead of disappearing into a detached task.
    pub async fn run<F>(&self, what: &'static str, job: Uuid, unit: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.run_tagged(what, job.to_string(), unit).await
    }

    /// [`FanoutPool::run`] for units that are not identified by a job id — a
    /// webhook delivery is tagged `<kind>:<ref_id>`, a replay by its delivery
    /// id. Same guarantees; only the log label differs.
    pub async fn run_tagged<F>(&self, what: &'static str, tag: String, unit: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if placement(self.concurrency, self.inflight(), self.max_queued) == Placement::Inline {
            if self.concurrency > 0 {
                warn!(
                    unit = %tag, stage = what, inflight = self.inflight(), max = self.max_queued,
                    "fan-out backlog full; running this unit inline on its caller (a job's \
                     fan-out then holds a worker permit until it finishes) rather than dropping it"
                );
            }
            unit.await;
            return;
        }
        self.inflight.fetch_add(1, Ordering::SeqCst);
        let ticket = Ticket(self.inflight.clone());
        let permits = self.permits.clone();
        tokio::spawn(async move {
            // Held for the body, released with the task.
            let _permit = permits.acquire_owned().await;
            let caught = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(unit)).await;
            if caught.is_err() {
                error!(
                    unit = %tag, stage = what,
                    "fan-out unit panicked; whatever produced it is already persisted, but this \
                     derived/outbound work (index / hooks / alerts / delivery) did not complete"
                );
            }
            drop(ticket);
        });
    }

    /// Waits until nothing is in flight, or `timeout` elapses. Returns how many
    /// units were still running at the deadline — non-zero means shutdown did
    /// lose work, and the caller says so out loud.
    pub async fn drain(&self, timeout: Duration) -> usize {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let left = self.inflight();
            if left == 0 {
                return 0;
            }
            if tokio::time::Instant::now() >= deadline {
                return left;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The anti-pattern: a full backlog that silently discards fan-out, turning
    /// "the webhook didn't arrive" into an unexplained absence.
    #[test]
    fn overflowing_backlog_runs_inline_not_dropped() {
        assert_eq!(placement(4, 0, 64), Placement::OffSlot);
        assert_eq!(placement(4, 63, 64), Placement::OffSlot);
        // At and beyond the ceiling: inline, i.e. slow — but still executed.
        assert_eq!(placement(4, 64, 64), Placement::Inline);
        assert_eq!(placement(4, 999, 64), Placement::Inline);
    }

    /// `fanout_concurrency = 0` is the documented escape hatch back to the
    /// original inline behaviour (and the control arm of the benchmark).
    #[test]
    fn zero_concurrency_means_everything_inline() {
        assert_eq!(placement(0, 0, 64), Placement::Inline);
        assert_eq!(placement(0, 0, 0), Placement::Inline);
        assert_eq!(placement(4, 0, 0), Placement::Inline);
    }

    #[tokio::test]
    async fn drain_waits_for_inflight_units_and_reports_stragglers() {
        let pool = FanoutPool::new(2, 8);
        let done = Arc::new(AtomicUsize::new(0));
        for _ in 0..4 {
            let done = done.clone();
            pool.run("test", Uuid::new_v4(), async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                done.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        }
        assert_eq!(pool.drain(Duration::from_secs(5)).await, 0);
        assert_eq!(
            done.load(Ordering::SeqCst),
            4,
            "drain must not return early"
        );

        // A unit that outlives the deadline is counted, not silently ignored.
        pool.run("test", Uuid::new_v4(), async {
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;
        assert_eq!(pool.drain(Duration::from_millis(50)).await, 1);
    }

    /// A panicking unit must not leak backlog capacity — otherwise the pool
    /// silently degrades to permanently-inline.
    #[tokio::test]
    async fn a_panicking_unit_is_contained_and_releases_its_slot() {
        let pool = FanoutPool::new(1, 4);
        pool.run("test", Uuid::new_v4(), async { panic!("fan-out boom") })
            .await;
        assert_eq!(pool.drain(Duration::from_secs(5)).await, 0);
        assert_eq!(pool.inflight(), 0, "the ticket must be released on unwind");
    }
}
