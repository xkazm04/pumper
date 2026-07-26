//! Graceful shutdown: the worker loop stops claiming the moment the token
//! fires, waits out the drain deadline, and re-queues the straggler so it
//! resumes cleanly on the next boot — nothing stranded in `running`.

use std::sync::Arc;
use std::time::Duration;

use pumper_core::{EnqueueOptions, JobStatus};
use serde_json::json;

use super::harness::{test_state, FakeApp};
use crate::worker;

#[tokio::test]
async fn shutdown_drains_then_requeues_the_straggler() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    // Drain deadline is 1s (harness config); the app sleeps far past it.
    let job = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions { params: json!({ "sleep_ms": 60_000 }), ..Default::default() },
        )
        .await
        .unwrap();

    let handle = tokio::spawn(worker::run(state.clone()));

    // Wait until the worker actually claimed the job.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = state.storage.get(job.id).await.unwrap().unwrap();
        if row.status == JobStatus::Running {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "worker never claimed the job");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    state.shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("worker loop must exit within the drain deadline")
        .expect("worker task must not panic");

    // The straggler went back to the queue with its attempt intact — the
    // recover_stuck path, not a failure and not a stranded `running` row.
    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued, "straggler re-queued for the next boot");
}
