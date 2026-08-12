//! What the job-control doors *tell* the outside world.
//!
//! Two lies lived here. (1) Bulk retry and queued-cancel emitted their events
//! with a **blank app**, and every app-scoped watcher (`GET /mcp?app=…`,
//! `LiveFilter::keep`) filters on an exact app match — so those transitions were
//! invisible to precisely the clients subscribed to them. (2) The graceful-
//! shutdown drain fires each running job's cancel token to mean *suspend*, and
//! `DELETE /jobs/{id}` fires the same token to mean *stop* — the worker read
//! both as suspend, so an operator's cancel inside the drain window silently
//! became "run it again after the restart", while the door answered
//! `{cancelled: true}`.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::{EnqueueOptions, JobStatus};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, test_state_with, wait_status, FakeApp, WorkerLoop};
use crate::events::JobEvent;
use crate::state::AppState;
use crate::{routes, worker};

async fn send(router: &axum::Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Everything on the bus right now, drained without blocking.
fn drain_events(
    rx: &mut tokio::sync::broadcast::Receiver<(u64, Arc<JobEvent>)>,
) -> Vec<Arc<JobEvent>> {
    let mut out = Vec::new();
    while let Ok((_, ev)) = rx.try_recv() {
        out.push(ev);
    }
    out
}

async fn enqueue(state: &AppState, params: Value) -> pumper_core::Job {
    state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params,
                ..Default::default()
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn control_events_carry_app_not_blank() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;

    // Two jobs driven to `failed`, so bulk retry has something to resurrect.
    let failed = enqueue(&state, json!({ "fail": "boom" })).await;
    assert!(worker::run_one(&state).await);
    wait_status(&state, failed.id, JobStatus::Failed, Duration::from_secs(5)).await;
    // And one still queued, for the synchronous cancel door.
    let queued = enqueue(&state, json!({ "sleep_ms": 0 })).await;

    let router = routes::router(state.clone());
    let mut rx = state.events.subscribe();

    let (status, body) = send(&router, "POST", "/jobs/retry", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["retried"], 1, "{body}");
    assert!(
        body["ids"][0].is_string(),
        "the wire shape is unchanged — `ids` is still a bare uuid array: {body}"
    );

    let (status, body) = send(
        &router,
        "DELETE",
        &format!("/jobs/{}", queued.id),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["cancelled"], true);

    let events = drain_events(&mut rx);
    assert_eq!(events.len(), 2, "one per control action: {events:?}");
    for ev in &events {
        assert_eq!(
            ev.app, "fake",
            "a blank app is filtered out of every app-scoped watcher: {ev:?}"
        );
    }
    let kinds: Vec<&str> = events.iter().map(|e| e.status.as_str()).collect();
    assert!(
        kinds.contains(&"queued") && kinds.contains(&"cancelled"),
        "{kinds:?}"
    );
}

/// The drain window: an operator's cancel must END the job, not schedule it for
/// the next boot.
///
/// Deterministic by construction. A 6s drain budget gives `worker::drain` a ~5s
/// phase-1 "clean finish" window (the tail is reserved for the suspend
/// round-trip), so the `DELETE` below lands *inside* the drain with the run not
/// yet resolved — the exact state in which every cancel used to be reinterpreted
/// as a suspend, because `execute` asked only "is the process shutting down?".
/// The narrower phase-2 race (the drain resolving the token microseconds first)
/// is covered at the unit level by `worker::cancel_kind_tests`, which also pins
/// the honest `{cancelled: false, suspended: true}` answer for it.
#[tokio::test]
async fn cancel_during_drain_cancels_not_resurrects() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.worker.shutdown_drain_secs = 6;
    })
    .await;
    let job = enqueue(&state, json!({ "sleep_ms": 600_000 })).await;

    let worker_loop = WorkerLoop::start(&state);
    wait_status(&state, job.id, JobStatus::Running, Duration::from_secs(10)).await;

    let router = routes::router(state.clone());
    // Shutdown first, then the cancel: the worker has stopped claiming and is
    // draining, and this job's token now has two possible meanings.
    state.shutdown.cancel();
    let (status, body) = send(&router, "DELETE", &format!("/jobs/{}", job.id), json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["running"], true, "the job was in flight: {body}");
    assert_eq!(
        body["cancelled"], true,
        "user intent outranks the drain — this run had not resolved yet: {body}"
    );

    worker_loop.shutdown(&state, Duration::from_secs(30)).await;

    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Cancelled,
        "the door promised a cancel; the queue must agree. A suspend would re-queue the job to \
         resurrect on the next boot — precisely what the operator asked us not to do."
    );
    assert!(
        state
            .storage
            .load_checkpoint(job.id)
            .await
            .unwrap()
            .is_none(),
        "a cancelled job's checkpoint is dropped; nothing will resume it"
    );
}

/// Drain semantics are unchanged for a job nobody cancelled: the token storm
/// still means suspend, and the straggler goes back to the queue.
#[tokio::test]
async fn an_uncancelled_job_still_suspends_on_drain() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let job = enqueue(&state, json!({ "sleep_ms": 600_000 })).await;

    let worker_loop = WorkerLoop::start(&state);
    wait_status(&state, job.id, JobStatus::Running, Duration::from_secs(10)).await;
    worker_loop.shutdown(&state, Duration::from_secs(20)).await;

    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "nobody cancelled it, so the drain re-queues it to resume next boot"
    );
}
