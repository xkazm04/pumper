//! Graceful shutdown: the worker loop stops claiming the moment the token
//! fires, waits out the drain deadline, and re-queues the straggler so it
//! resumes cleanly on the next boot — nothing stranded in `running`.
//!
//! The same promise for OUTBOUND work: a webhook delivery in flight when the
//! token fires is drained, not abandoned mid-POST.

use std::sync::Arc;
use std::time::Duration;

use pumper_core::{EnqueueOptions, JobStatus};
use serde_json::json;

use super::harness::{test_state, test_state_with, FakeApp, TestReceiver};
use crate::{webhook, worker};

#[tokio::test]
async fn shutdown_drains_then_requeues_the_straggler() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    // Drain deadline is 1s (harness config); the app sleeps far past it.
    let job = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params: json!({ "sleep_ms": 60_000 }),
                ..Default::default()
            },
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker never claimed the job"
        );
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
    assert_eq!(
        row.status,
        JobStatus::Queued,
        "straggler re-queued for the next boot"
    );
}

/// A delivery's scheduled retry time. Not on the `Delivery` model, and the point
/// of the assertion is precisely that the column is set — so read it directly.
async fn next_retry_at(state: &crate::state::AppState, id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT next_retry_at FROM webhook_deliveries WHERE id = ?1")
        .bind(id)
        .fetch_one(&state.storage.pool())
        .await
        .expect("read next_retry_at")
}

/// The anti-pattern: every delivery escaped through a bare `tokio::spawn`, so a
/// clean shutdown returned "drained" while N POSTs were still open — the row
/// left `pending` with no schedule, the payload neither delivered nor in the DLQ.
///
/// Both terminal shapes are covered here, because "drained" has to mean both:
/// a receiver that answers (→ `delivered`) and one that refuses (→ `failed`
/// **with a next_retry_at**, i.e. back on the ladder rather than stranded).
#[tokio::test]
async fn shutdown_drains_deliveries_leaving_no_unscheduled_pending_row() {
    // 60s budget → a 10s fan-out/delivery grace slice (see `worker::drain`),
    // comfortably past the ~6s a refused delivery's in-process ladder takes
    // (3 attempts with 0s/2s/4s backoff).
    let (state, _store) = test_state_with(vec![], |c| c.worker.shutdown_drain_secs = 60).await;
    // Slow enough that these are unambiguously mid-flight when shutdown fires.
    let good = TestReceiver::spawn_slow(vec![], Duration::from_millis(300)).await;
    let refusing = TestReceiver::spawn_slow(vec![500, 500, 500], Duration::from_millis(50)).await;

    for i in 0..3 {
        webhook::dispatch_event(
            &state,
            "test",
            &format!("ok-{i}"),
            &good.url(),
            "test.event",
            &json!({ "i": i }),
            None,
        )
        .await;
    }
    webhook::dispatch_event(
        &state,
        "test",
        "refused-0",
        &refusing.url(),
        "test.event",
        &json!({ "i": "x" }),
        None,
    )
    .await;
    assert!(
        state.deliveries.inflight() > 0,
        "deliveries are TRACKED by a drainable pool, not detached tasks — this is the \
         structural claim the rest of the test depends on"
    );

    // Cancel first, then start the loop: it breaks out on the first select and
    // goes straight to the drain, which is the path under test.
    state.shutdown.cancel();
    let handle = tokio::spawn(worker::run(state.clone()));
    tokio::time::timeout(Duration::from_secs(90), handle)
        .await
        .expect("worker loop must exit within the drain deadline")
        .expect("worker task must not panic");

    assert_eq!(
        state.deliveries.inflight(),
        0,
        "no delivery task outlives the drain"
    );
    assert_eq!(good.hits_so_far().len(), 3, "every accepted POST completed");
    assert_eq!(
        refusing.hits_so_far().len(),
        3,
        "the refused delivery walked its whole in-process ladder before the drain returned"
    );

    let rows = state.storage.list_deliveries(None, 100).await.unwrap();
    assert_eq!(rows.len(), 4);
    for d in &rows {
        assert_ne!(
            d.status, "pending",
            "a clean drain leaves no delivery in flight: {d:?}"
        );
    }
    assert_eq!(
        rows.iter().filter(|d| d.status == "delivered").count(),
        3,
        "the three accepted deliveries are terminal"
    );
    let refused: Vec<_> = rows.iter().filter(|d| d.status == "failed").collect();
    assert_eq!(refused.len(), 1);
    assert!(
        next_retry_at(&state, &refused[0].id).await.is_some(),
        "a failed delivery is SCHEDULED, not merely not-pending — it is back on the ladder"
    );
}
