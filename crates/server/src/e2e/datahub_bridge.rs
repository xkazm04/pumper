//! The DataHub bridge against a mock GMS — the emitter's first tests that
//! exercise the wire instead of a pure aspect builder.
//!
//! No mock-HTTP crate is added: the workspace has none, and the repo already
//! owns this shape (`harness::TestReceiver`, `e2e/fetch_proxy.rs`) — a loopback
//! axum server on an ephemeral port. [`MockGms`] is the DataHub-flavoured
//! version: it records every ingestion POST's parsed entity batch, and answers
//! from a status script so a mid-batch failure can be scripted exactly.
//!
//! What is pinned here:
//! - ingestion is **batched at 25 entities**, so one oversized payload can't
//!   take down a whole emission;
//! - a failing batch **aborts the rest** and the status entry says how many
//!   entities already landed (there is no rollback, and deliberately no retry);
//! - `POST /datahub/sync` is **not re-entrant** — a second concurrent call is
//!   rejected rather than doubling the GMS load and racing the lineage
//!   read-merge.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use super::harness::{test_state_with, wait_for, FakeApp};
use crate::datahub::SyncOutcome;
use crate::state::AppState;

/// A loopback stand-in for a DataHub GMS. Records the entity batch of every
/// ingestion POST and replies with the next scripted status (200 once the
/// script runs out).
struct MockGms {
    addr: SocketAddr,
    batches: Arc<Mutex<Vec<Vec<Value>>>>,
    graphql: Arc<Mutex<usize>>,
}

impl MockGms {
    async fn spawn(statuses: Vec<u16>, delay: Duration) -> Self {
        Self::spawn_with(statuses, delay, Duration::ZERO).await
    }

    /// `delay` slows the ingestion path; `graphql_delay` slows the governance
    /// read path (`/api/graphql`) independently, so a *hanging poll* can be
    /// staged without also stalling emissions.
    async fn spawn_with(statuses: Vec<u16>, delay: Duration, graphql_delay: Duration) -> Self {
        let batches: Arc<Mutex<Vec<Vec<Value>>>> = Arc::new(Mutex::new(Vec::new()));
        let graphql: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let script = Arc::new(Mutex::new(VecDeque::from(statuses)));

        let batches_h = batches.clone();
        let graphql_h = graphql.clone();
        let handler = move |req: axum::extract::Request| {
            let batches = batches_h.clone();
            let graphql = graphql_h.clone();
            let script = script.clone();
            async move {
                let is_graphql = req.uri().path().contains("/api/graphql");
                let is_ingest = req.method() == axum::http::Method::POST && !is_graphql;
                let body = axum::body::to_bytes(req.into_body(), 1 << 22)
                    .await
                    .unwrap_or_default();
                if is_ingest {
                    let parsed: Vec<Value> = serde_json::from_slice(&body).unwrap_or_default();
                    batches.lock().unwrap().push(parsed);
                }
                if is_graphql {
                    *graphql.lock().unwrap() += 1;
                    tokio::time::sleep(graphql_delay).await;
                    return (
                        axum::http::StatusCode::OK,
                        json!({"data": {"dataset": null}}).to_string(),
                    );
                }
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                let status = script.lock().unwrap().pop_front().unwrap_or(200);
                (
                    axum::http::StatusCode::from_u16(status).unwrap(),
                    "{}".to_string(),
                )
            }
        };
        let app = axum::Router::new().fallback(axum::routing::any(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback GMS");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            addr,
            batches,
            graphql,
        }
    }

    /// Governance GraphQL reads received so far.
    fn graphql_reads(&self) -> usize {
        *self.graphql.lock().unwrap()
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn batch_sizes(&self) -> Vec<usize> {
        self.batches.lock().unwrap().iter().map(Vec::len).collect()
    }

    fn requests(&self) -> usize {
        self.batches.lock().unwrap().len()
    }
}

/// A state wired to `gms`, with the emit toggles pinned so entity counts are
/// arithmetic rather than config-dependent.
async fn state_for(gms: &MockGms) -> (AppState, pumper_core::testing::TempStore) {
    let url = gms.url();
    test_state_with(vec![Arc::new(FakeApp)], move |c| {
        c.datahub.enabled = true;
        c.datahub.gms_url = url;
        c.datahub.emit_schema = true;
        c.datahub.emit_profile = true;
        // Topology needs schedules/triggers, of which a fresh temp store has
        // none — off here so the entity count is exactly datasets × aspects.
        c.datahub.emit_flows = false;
    })
    .await
}

/// `n` datasets, one record each ⇒ 4 aspects per dataset (properties,
/// operation, profile, schema).
const ASPECTS_PER_DATASET: usize = 4;

async fn seed_datasets(state: &AppState, n: usize) {
    for i in 0..n {
        state
            .datasets
            .upsert_trusted("fake", &format!("d{i}"), "k1", &json!({"v": i}), None)
            .await
            .expect("seed record");
    }
}

/// The anti-pattern: one giant ingestion POST, where a single oversized payload
/// fails the whole emission.
#[tokio::test]
async fn ingestion_is_batched_at_25_not_one_giant_post() {
    let gms = MockGms::spawn(vec![], Duration::ZERO).await;
    let (state, _store) = state_for(&gms).await;
    // 7 datasets × 4 aspects = 28 entities ⇒ 25 + 3.
    seed_datasets(&state, 7).await;

    let summary = match crate::datahub::full_sync(&state).await {
        SyncOutcome::Ran(v) => v,
        SyncOutcome::Busy => panic!("nothing else is syncing"),
    };
    assert_eq!(summary["ok"], true, "summary: {summary}");
    assert_eq!(summary["entities"], 7 * ASPECTS_PER_DATASET);
    assert_eq!(gms.batch_sizes(), vec![25, 3]);
}

/// A batch failure aborts the remainder — and the recorded error must say what
/// already landed, because the earlier batches are at GMS with no rollback and
/// (by design) no retry.
#[tokio::test]
async fn a_failed_batch_aborts_the_rest_and_reports_what_already_landed() {
    // First batch OK, second rejected. A third would mean "kept going".
    let gms = MockGms::spawn(vec![200, 500], Duration::ZERO).await;
    let (state, _store) = state_for(&gms).await;
    seed_datasets(&state, 15).await; // 60 entities ⇒ 25 / 25 / 10

    let summary = match crate::datahub::full_sync(&state).await {
        SyncOutcome::Ran(v) => v,
        SyncOutcome::Busy => panic!("nothing else is syncing"),
    };
    assert_eq!(summary["ok"], false, "summary: {summary}");
    assert_eq!(
        gms.requests(),
        2,
        "the failing batch must abort the emission, not continue through it"
    );
    let err = summary["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("partial: 25 of 60 entities already ingested"),
        "the error must name what already landed, got: {err}"
    );

    // The failure is filed as the last ERROR and counted, and the status view
    // exposes both slots independently.
    let status = crate::datahub::status(&state);
    assert_eq!(status["emissions"]["failed"], 1);
    assert_eq!(status["emissions"]["ok"], 0);
    assert!(status["emissions"]["last_error"]["error"]
        .as_str()
        .unwrap()
        .contains("partial: 25 of 60"));
    assert!(status["emissions"]["last_success"].is_null());
}

/// The anti-pattern this replaces: `on_job_success` bare-`tokio::spawn`ing its
/// emission, so the shutdown drain (which only knows about the fan-out pool)
/// exited over an in-flight emission without waiting for it or counting it.
/// Now the emission IS a fan-out unit: visible to `inflight()`, and the drain
/// either finishes it or reports it as abandoned.
#[tokio::test]
async fn a_job_emission_is_tracked_by_the_drain_not_silently_detached() {
    let gms = MockGms::spawn(vec![], Duration::from_millis(300)).await;
    let (state, _store) = state_for(&gms).await;
    seed_datasets(&state, 2).await;
    let job = state
        .storage
        .enqueue("fake", pumper_core::EnqueueOptions::default())
        .await
        .expect("enqueue");

    crate::datahub::on_job_success(&state, &job, Vec::new()).await;
    assert_eq!(
        state.fanout.inflight(),
        1,
        "the emission must be a tracked fan-out unit, not a detached spawn"
    );

    // What the shutdown drain does: wait, bounded — and here it completes, so
    // the metadata actually reached GMS instead of vanishing with the process.
    assert_eq!(state.fanout.drain(Duration::from_secs(10)).await, 0);
    assert!(gms.requests() >= 1, "the drained emission must have posted");
    assert_eq!(crate::datahub::status(&state)["emissions"]["ok"], 1);
}

/// The anti-pattern: two `/datahub/sync` calls running at once — double GMS
/// load, and two lineage read-merges that can interleave into lost edges.
#[tokio::test]
async fn a_second_sync_during_one_in_flight_is_rejected_not_run() {
    // Slow GMS so the first sync is provably still in flight.
    let gms = MockGms::spawn(vec![], Duration::from_millis(400)).await;
    let (state, _store) = state_for(&gms).await;
    seed_datasets(&state, 7).await;

    let first = tokio::spawn({
        let state = state.clone();
        async move { matches!(crate::datahub::full_sync(&state).await, SyncOutcome::Ran(_)) }
    });
    // Wait until the first sync is actually talking to GMS.
    wait_for(
        "the first sync to reach GMS",
        Duration::from_secs(5),
        || {
            let gms_requests = gms.requests();
            async move { gms_requests > 0 }
        },
    )
    .await;

    assert!(
        matches!(crate::datahub::full_sync(&state).await, SyncOutcome::Busy),
        "a concurrent full sync must be rejected (409), not doubled"
    );
    assert_eq!(
        crate::datahub::status(&state)["emissions"]["sync_running"],
        true
    );

    assert!(first.await.expect("first sync task"), "first sync must run");
    // Rejection is not a wedge: the slot is free once the first one finishes.
    assert!(matches!(
        crate::datahub::full_sync(&state).await,
        SyncOutcome::Ran(_)
    ));
}

/// The anti-pattern: `govern_tick` stamping `last_poll` when the poll STARTED,
/// so a poll slower than the interval overlapped the next one and two tasks
/// raced to write `paused_apps`. Completion now gates the next poll, and an
/// in-flight poll blocks a tick outright — proven here with a GMS that hangs
/// its GraphQL reads while the interval is artificially aged past due.
#[tokio::test]
async fn a_tick_during_a_hanging_poll_does_not_start_a_second_poll() {
    let gms = MockGms::spawn_with(vec![], Duration::ZERO, Duration::from_secs(3)).await;
    let url = gms.url();
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], move |c| {
        c.datahub.enabled = true;
        c.datahub.gms_url = url;
        c.datahub.govern = true;
    })
    .await;
    seed_datasets(&state, 1).await;

    crate::datahub::govern_tick(&state);
    wait_for("the poll to reach GMS", Duration::from_secs(5), || {
        let reads = gms.graphql_reads();
        async move { reads > 0 }
    })
    .await;

    // Age the completion stamp well past the interval, so the ONLY thing that
    // can hold this tick back is the in-flight guard.
    {
        let mut g = state.datahub_govern.lock().unwrap();
        assert!(g.in_flight, "the first poll must be marked in flight");
        g.last_poll = std::time::Instant::now().checked_sub(Duration::from_secs(3600));
    }
    crate::datahub::govern_tick(&state);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        gms.graphql_reads(),
        1,
        "a tick during an in-flight poll must not start a second poll"
    );

    // And the guard is not a wedge: completion clears it and re-stamps.
    wait_for("the poll to finish", Duration::from_secs(15), || {
        let done = !state.datahub_govern.lock().unwrap().in_flight;
        async move { done }
    })
    .await;
    let g = state.datahub_govern.lock().unwrap();
    assert!(
        g.last_poll
            .is_some_and(|t| t.elapsed() < Duration::from_secs(60)),
        "completion must re-stamp last_poll, so the interval restarts from the END"
    );
}
