//! Server-sent event streams and their replay/resume plumbing: the global
//! `/events` feed and the per-job `/jobs/{id}/stream` scope, plus the helpers
//! that build, resume, and recover the SSE sequence.

use std::convert::Infallible;

use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use pumper_core::JobStatus;
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

use crate::events::JobEvent;
use crate::state::AppState;

/// SSE stream of all job status transitions.
///
/// Every event carries a monotonic id. A client reconnecting with a
/// `Last-Event-ID` header is replayed the events it missed from the in-memory
/// ring; if the gap is older than the ring retains, a single `reset` event is
/// emitted first so the client knows to resync its view. Live subscribers that
/// fall behind the broadcast buffer recover the same way instead of dropping
/// events silently.
#[utoipa::path(
    get,
    path = "/events",
    tag = "events",
    responses((status = 200, description = "SSE stream of job status transitions. Each event carries a monotonic `id`; reconnect with a `Last-Event-ID` header to replay the missed gap (or receive a `reset` event when it is too old).", content_type = "text/event-stream"))
)]
pub(crate) async fn stream_events(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let after = last_event_id(&headers);
    let mut rx = state.events.subscribe();
    let shutdown = state.shutdown.clone();
    let (initial, mut last_seq) = resume(&state, after, |_| true);
    let stream = async_stream::stream! {
        for ev in initial {
            yield Ok(ev);
        }
        loop {
            let Some(received) = next_or_shutdown(&mut rx, &shutdown).await else {
                break;
            };
            match received {
                Ok((seq, event)) => {
                    if seq <= last_seq {
                        continue; // already replayed (overlap window)
                    }
                    last_seq = seq;
                    yield Ok(sse_event(seq, &event));
                }
                Err(RecvError::Lagged(_)) => {
                    for ev in recover(&state, &mut last_seq, |_| true) {
                        yield Ok(ev);
                    }
                }
                Err(RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// The next bus event, or `None` when the process is shutting down.
///
/// The anti-pattern this replaces: awaiting `rx.recv()` bare, so the stream
/// ended only on `RecvError::Closed` — which needs the broadcast **sender** to
/// drop, and the sender lives in every `AppState` clone (worker, scheduler,
/// janitors, the router itself). `Closed` therefore never arrives while the
/// process is alive, `KeepAlive` kept the socket healthy, and one attached
/// dashboard was enough to make `axum::serve`'s graceful shutdown wait forever.
/// Selecting on the shutdown token ends the stream cleanly instead: the
/// generator returns, axum finishes the response body, and the client sees an
/// ordinary end-of-stream rather than a connection reset.
///
/// `biased` so a pending shutdown wins over a backlog of buffered events — a
/// stopping process must not have to drain the bus first.
pub(crate) async fn next_or_shutdown<T: Clone>(
    rx: &mut tokio::sync::broadcast::Receiver<T>,
    shutdown: &tokio_util::sync::CancellationToken,
) -> Option<Result<T, RecvError>> {
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => None,
        received = rx.recv() => Some(received),
    }
}

/// SSE stream scoped to one job; closes once the job reaches a terminal state.
/// Supports the same `Last-Event-ID` resume as `/events`, filtered to this job.
#[utoipa::path(
    get,
    path = "/jobs/{id}/stream",
    tag = "events",
    params(("id" = Uuid, Path, description = "Job id")),
    responses((status = 200, description = "SSE stream scoped to one job; replays current state on connect, closes at terminal. Same `Last-Event-ID` resume as `/events`.", content_type = "text/event-stream"))
)]
pub(crate) async fn stream_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: axum::http::HeaderMap,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let after = last_event_id(&headers);
    // Subscribe before snapshotting so no transition slips through the gap.
    let mut rx = state.events.subscribe();
    // A fresh connect (no resume point) gets the current state up front; a
    // resuming client already has it and only wants the gap.
    let snapshot = if after.is_none() {
        state.storage.get(id).await.ok().flatten()
    } else {
        None
    };
    let shutdown = state.shutdown.clone();
    let (replayed, mut last_seq) = resume(&state, after, move |ev| ev.job_id == id);
    let stream = async_stream::stream! {
        for ev in replayed {
            yield Ok(ev);
        }
        if let Some(job) = snapshot {
            let mut event = JobEvent::new(job.id, job.app.clone(), job.status.as_str());
            event.result = job.result.clone();
            event.error = job.error.clone();
            yield Ok(snapshot_event(&event));
            if job.status.is_terminal() {
                return;
            }
        }
        loop {
            // Self-terminating at the job's terminal event, but only if one ever
            // arrives — a job whose worker is already draining never sends it,
            // so this stream needs the same shutdown exit as `/events`.
            let Some(received) = next_or_shutdown(&mut rx, &shutdown).await else {
                break;
            };
            match received {
                Ok((seq, event)) => {
                    if seq <= last_seq {
                        continue;
                    }
                    last_seq = seq;
                    if event.job_id != id {
                        continue;
                    }
                    let done = JobStatus::parse(event.status.as_str())
                        .is_some_and(|s| s.is_terminal());
                    yield Ok(sse_event(seq, &event));
                    if done {
                        break;
                    }
                }
                Err(RecvError::Lagged(_)) => {
                    for ev in recover(&state, &mut last_seq, |ev| ev.job_id == id) {
                        yield Ok(ev);
                    }
                }
                Err(RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Parses a `Last-Event-ID` header into the sequence id the client last saw.
fn last_event_id(headers: &axum::http::HeaderMap) -> Option<u64> {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse().ok())
}

/// Builds the connect-time replay for a resuming client: the buffered events it
/// missed (filtered by `keep`), preceded by a `reset` marker when the gap is too
/// old. Returns the events plus the highest sequence id now delivered, which the
/// live loop uses to dedup the broadcast overlap window.
fn resume(
    state: &AppState,
    after: Option<u64>,
    keep: impl Fn(&JobEvent) -> bool,
) -> (Vec<Event>, u64) {
    let Some(after) = after else {
        return (Vec::new(), 0);
    };
    match state.events.replay(after) {
        crate::events::Replay::Reset => {
            let latest = state.events.latest_seq();
            (vec![reset_event(latest)], latest)
        }
        crate::events::Replay::Events(events) => {
            let mut last = after;
            let mut out = Vec::new();
            for (seq, event) in events {
                last = seq;
                if keep(&event) {
                    out.push(sse_event(seq, &event));
                }
            }
            (out, last)
        }
    }
}

/// Recovers a live subscriber that lagged past the broadcast buffer: replays the
/// ring past `last_seq`, advancing it, or emits a single `reset` when the gap is
/// unrecoverable.
fn recover(state: &AppState, last_seq: &mut u64, keep: impl Fn(&JobEvent) -> bool) -> Vec<Event> {
    match state.events.replay(*last_seq) {
        crate::events::Replay::Reset => {
            let latest = state.events.latest_seq();
            *last_seq = latest;
            vec![reset_event(latest)]
        }
        crate::events::Replay::Events(events) => {
            let mut out = Vec::new();
            for (seq, event) in events {
                *last_seq = seq;
                if keep(&event) {
                    out.push(sse_event(seq, &event));
                }
            }
            out
        }
    }
}

fn sse_event(seq: u64, event: &JobEvent) -> Event {
    Event::default()
        .id(seq.to_string())
        .event("job")
        .json_data(event)
        .unwrap_or_else(|_| Event::default().comment("serialize error"))
}

/// Connect-time snapshot of a job's current state (no sequence id — it is a
/// synthesized view, not a buffered transition).
fn snapshot_event(event: &JobEvent) -> Event {
    Event::default()
        .event("job")
        .json_data(event)
        .unwrap_or_else(|_| Event::default().comment("serialize error"))
}

/// Signals a resuming client that its requested id fell out of the replay ring;
/// it should discard assumptions and resync. Carries the latest id so the client
/// can advance its `Last-Event-ID` pointer.
fn reset_event(latest: u64) -> Event {
    Event::default()
        .id(latest.to_string())
        .event("reset")
        .data("replay gap: reconnect point too old, resync state")
}

