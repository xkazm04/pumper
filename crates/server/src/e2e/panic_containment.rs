//! Panic containment: an app that unwinds fails its job on the same tick,
//! through the normal attempt-fenced `fail()` path, with the panic payload as
//! the error — instead of leaving the row `running` until the reaper mislabels
//! it "lease expired (heartbeat stale)" `stale_after_secs` later.

use std::sync::Arc;

use pumper_core::{AppContext, EnqueueOptions, JobStatus, Result, ScrapeApp};
use serde_json::Value;

use super::harness::test_state;
use crate::worker;

/// An app whose `run` panics. `panic!` with a formatted message so the payload
/// is a `String` (the `&'static str` case is covered by the unit tests).
struct PanickingApp;

#[async_trait::async_trait]
impl ScrapeApp for PanickingApp {
    fn name(&self) -> &'static str {
        "boom"
    }
    async fn run(&self, _ctx: AppContext) -> Result<Value> {
        // Yield first, so the panic happens from inside a poll that the
        // worker's select loop is actually driving.
        tokio::task::yield_now().await;
        panic!("scraper hit an unwrap: {}", "index out of bounds");
    }
}

#[tokio::test]
async fn a_panicking_app_fails_its_job_now_not_after_the_stale_window() {
    let (state, _store) = test_state(vec![Arc::new(PanickingApp)]).await;
    assert!(
        state.config.worker.stale_after_secs > 0,
        "the reaper backstop is on; this test must beat it, not rely on it"
    );
    let job = state
        .storage
        .enqueue(
            "boom",
            EnqueueOptions {
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(worker::run_one(&state).await, "the job must be claimed");

    let row = state.storage.get(job.id).await.unwrap().expect("job row");
    assert_eq!(
        row.status,
        JobStatus::Failed,
        "a panicking app must not leave the row running for the reaper"
    );
    let error = row.error.expect("a panicked job records an error");
    assert!(
        error.starts_with("panicked: "),
        "panics are distinguishable from app errors/timeouts/reaped leases: {error}"
    );
    assert!(
        error.contains("scraper hit an unwrap: index out of bounds"),
        "the panic payload is the only explanation of the failure: {error}"
    );
    assert!(
        error.contains("panic_containment.rs"),
        "panic location is carried when available: {error}"
    );
    assert_ne!(
        error, "lease expired (heartbeat stale)",
        "the reaper's message would misdescribe what happened"
    );
}

#[tokio::test]
async fn a_panic_retries_like_any_other_failure() {
    let (state, _store) = test_state(vec![Arc::new(PanickingApp)]).await;
    let job = state
        .storage
        .enqueue(
            "boom",
            EnqueueOptions {
                max_attempts: 2,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(worker::run_one(&state).await);

    // Attempt-fenced `fail()` with attempts remaining → re-queued with backoff,
    // exactly as an app-returned error would be. Containment changed the error
    // text, not the retry semantics.
    let row = state.storage.get(job.id).await.unwrap().expect("job row");
    assert_eq!(row.status, JobStatus::Queued, "retry pending");
    assert_eq!(row.attempts, 1);
    assert!(row.available_at > row.created_at, "backoff applied");
}
