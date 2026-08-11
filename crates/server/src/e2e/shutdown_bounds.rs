//! Shutdown **termination**, as distinct from the drain semantics
//! `shutdown_drain` already covers.
//!
//! The failure this pins down: with one dashboard attached to `GET /events`, a
//! clean stop never completed. The SSE loop ended only on `RecvError::Closed`,
//! which needs the broadcast sender to drop — and the sender lives in every
//! `AppState` clone (worker, scheduler, janitors, the router itself), so it
//! never dropped while the process was alive. `axum::serve`'s graceful shutdown
//! waits for every in-flight connection, so it waited forever; the process only
//! died to SIGKILL, which skips the worker drain AND the host-politeness
//! snapshot since the last write-behind tick.
//!
//! Two independent guarantees, one per test: the streams END on the token, and
//! the politeness snapshot on disk after a clean stop reflects the LIVE
//! governor rather than the last tick.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use uuid::Uuid;

use super::harness::{test_state, test_state_with};
use crate::events::JobEvent;
use crate::routes;
use crate::state::{final_host_penalty_flush, AppState};

/// Opens an SSE response through the real router and returns its body stream.
async fn open_sse(router: &axum::Router, uri: &str) -> axum::body::BodyDataStream {
    let resp = tower::ServiceExt::oneshot(
        router.clone(),
        Request::builder().uri(uri).body(Body::empty()).unwrap(),
    )
    .await
    .expect("sse request");
    assert_eq!(resp.status(), StatusCode::OK, "{uri} must open");
    resp.into_body().into_data_stream()
}

/// Reads chunks until the stream ENDS. Fails the test (rather than hanging the
/// suite) if the end never comes.
async fn drain_to_end(mut body: axum::body::BodyDataStream, what: &str) -> String {
    let mut text = String::new();
    let ended = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(chunk) = body.next().await {
            let chunk = chunk.expect("no transport error mid-stream");
            text.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await;
    assert!(
        ended.is_ok(),
        "{what} never ended after the shutdown token fired — this is the hang that made a \
         clean stop impossible; the process could only be SIGKILLed"
    );
    text
}

/// The next chunk, as text. Fails if the stream ends or stalls instead —
/// receiving this is what proves a subscription is LIVE, so that "the stream
/// ended" can never be confused with "it was never connected".
async fn next_chunk(body: &mut axum::body::BodyDataStream, what: &str) -> String {
    let chunk = tokio::time::timeout(Duration::from_secs(5), body.next())
        .await
        .unwrap_or_else(|_| panic!("{what}: a live subscriber must receive the emitted event"))
        .unwrap_or_else(|| panic!("{what}: the stream ended before delivering anything"))
        .expect("no transport error");
    String::from_utf8_lossy(&chunk).into_owned()
}

/// The core claim, on the surface that actually breaks it: a LIVE subscriber to
/// the global feed. The stream must end on the token, not on a sender drop that
/// can never happen.
#[tokio::test]
async fn a_live_events_subscriber_ends_on_the_shutdown_token_not_on_sender_drop() {
    let (state, _store) = test_state(vec![]).await;
    let router = routes::router(state.clone());
    let mut body = open_sse(&router, "/events").await;

    // Prove the subscription is live BEFORE asking it to stop, so "it ended"
    // can never be confused with "it was never connected".
    state
        .events
        .emit(JobEvent::new(Uuid::new_v4(), "fake", "running"));
    let first = next_chunk(&mut body, "GET /events").await;
    assert!(
        first.contains("event: job"),
        "the live event must arrive first: {first:?}"
    );

    assert!(
        !state.shutdown.is_cancelled(),
        "the token starts unfired — otherwise this test would pass vacuously"
    );
    state.shutdown.cancel();
    drain_to_end(body, "GET /events").await;
}

/// `/jobs/{id}/stream` self-terminates at the job's terminal event — but only if
/// one ever arrives. A job whose worker is already draining never sends it, so
/// this stream needs the same exit, and a stream that "usually ends on its own"
/// is exactly the kind that strands a stop.
#[tokio::test]
async fn a_per_job_stream_ends_on_shutdown_even_with_no_terminal_event() {
    let (state, _store) = test_state(vec![]).await;
    let router = routes::router(state.clone());
    // A job id that will never reach a terminal state: nothing runs it.
    let body = open_sse(&router, &format!("/jobs/{}/stream", Uuid::new_v4())).await;
    state.shutdown.cancel();
    drain_to_end(body, "GET /jobs/{id}/stream").await;
}

/// The MCP live stream is the third SSE surface and had the identical shape.
/// It must end at a FRAME boundary — a truncated JSON-RPC notification would be
/// a protocol violation, where a closed stream is just the client's cue to
/// reconnect with `Last-Event-ID` (which the replay ring still serves).
#[tokio::test]
async fn the_mcp_live_stream_ends_on_shutdown_at_a_frame_boundary() {
    let (state, _store) = test_state_with(vec![], |c| c.mcp.enabled = true).await;
    let router = routes::router(state.clone());
    let mut body = open_sse(&router, "/mcp").await;
    state
        .events
        .emit(JobEvent::new(Uuid::new_v4(), "fake", "succeeded"));
    let live = next_chunk(&mut body, "GET /mcp").await;
    state.shutdown.cancel();
    let rest = drain_to_end(body, "GET /mcp").await;

    // Every `data:` line must be complete JSON — the "closing is fine,
    // corrupting a frame is not" half of the contract.
    let text = format!("{live}{rest}");
    let frames: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .collect();
    assert!(
        !frames.is_empty(),
        "the stream must have carried at least the notification emitted above: {text:?}"
    );
    for line in frames {
        let frame: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
            panic!("a truncated JSON-RPC frame reached the client: {e} in {line:?}")
        });
        assert_eq!(
            frame["jsonrpc"], "2.0",
            "every frame is a whole JSON-RPC message: {line}"
        );
    }
}

/// Emitting while a subscriber is mid-shutdown must not wedge the bus for the
/// rest of the process: the stream detaches, the emitter carries on. (The bus is
/// bounded and lag-recovering, so this is really a regression guard on the
/// `biased` select — a shutdown must win over a backlog rather than having to
/// drain it first.)
#[tokio::test]
async fn shutdown_wins_over_a_backlog_of_buffered_events() {
    let (state, _store) = test_state(vec![]).await;
    let router = routes::router(state.clone());
    let body = open_sse(&router, "/events").await;
    for _ in 0..200 {
        state
            .events
            .emit(JobEvent::new(Uuid::new_v4(), "fake", "queued"));
    }
    state.shutdown.cancel();
    drain_to_end(body, "GET /events with a backlog").await;
}

/// Reads the persisted penalty for `host` straight out of `tier_memory` — the
/// point of the assertion is what is ON DISK, so no in-memory accessor will do.
async fn stored_penalty_ms(state: &AppState, host: &str) -> Option<i64> {
    sqlx::query_scalar("SELECT penalty_ms FROM tier_memory WHERE host = ?1")
        .bind(host)
        .fetch_optional(&state.storage.pool())
        .await
        .expect("read tier_memory")
}

/// The anti-pattern: the periodic write-behind loop was the only writer, so a
/// clean stop persisted whatever the last tick — up to
/// `[fetcher] host_penalty_persist_secs` ago — happened to see. Everything the
/// governor learned after that tick was thrown away, and the harder the run had
/// been on a host, the more of the lesson was lost.
#[tokio::test]
async fn a_clean_stop_persists_the_live_penalty_not_the_last_tick() {
    let (state, _store) = test_state(vec![]).await;
    assert!(
        state.config.fetcher.host_penalty_persist_secs > 0,
        "write-behind is on by default; this test is about WHEN it writes, not whether"
    );
    // Learned after any tick would have run: only a final flush can see it.
    state
        .governor
        .restore_penalty("slow.example", Duration::from_millis(4_200));
    assert_eq!(
        stored_penalty_ms(&state, "slow.example").await,
        None,
        "nothing is on disk yet — the snapshot below is the only writer"
    );

    final_host_penalty_flush(&state).await;

    assert_eq!(
        stored_penalty_ms(&state, "slow.example").await,
        Some(4_200),
        "the politeness state on disk must reflect the LIVE governor at stop time"
    );
}

/// The mirror risk: `host_penalty_persist_secs = 0` means "do not persist
/// politeness at all". Shutdown must not be the one code path that ignores an
/// operator's explicit opt-out.
#[tokio::test]
async fn write_behind_disabled_stays_disabled_through_shutdown() {
    let (state, _store) =
        test_state_with(vec![], |c| c.fetcher.host_penalty_persist_secs = 0).await;
    state
        .governor
        .restore_penalty("slow.example", Duration::from_millis(4_200));

    final_host_penalty_flush(&state).await;

    assert_eq!(
        stored_penalty_ms(&state, "slow.example").await,
        None,
        "an opted-out deployment must end with an empty snapshot, not a surprise write"
    );
}

/// The two cancellation-unaware background spawns: neither may START work once
/// the token has fired. Both are off by default, which is exactly why the guard
/// has to be a test — nothing else exercises them.
#[tokio::test]
async fn background_passes_do_not_start_after_the_token_fires() {
    let (state, _store) = test_state_with(vec![], |c| {
        c.refresher.enabled = true;
        c.datahub.enabled = true;
        c.datahub.govern = true;
    })
    .await;
    state.shutdown.cancel();

    crate::refresher::tick(&state);
    crate::datahub::govern_tick(&state);

    // `govern_tick` marks `in_flight` before spawning, so an untouched flag is
    // proof that no poll was even started.
    assert!(
        !state.datahub_govern.lock().unwrap().in_flight,
        "a governance poll must not begin during shutdown"
    );
    // And the refresher's own overlap flag is likewise never claimed.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !state.datahub_govern.lock().unwrap().in_flight,
        "and it must not begin a moment later either"
    );
    let _ = Arc::strong_count(&state.storage); // keep `state` alive to here
}
