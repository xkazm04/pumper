//! Server-sent event streams and their replay/resume plumbing: the global
//! `/events` feed and the per-job `/jobs/{id}/stream` scope, plus the helpers
//! that build, resume, and recover the SSE sequence.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Json;
use pumper_core::JobStatus;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use utoipa::IntoParams;
use uuid::Uuid;

use crate::events::{JobEvent, SeqEvent};
use crate::routes::error::ApiError;
use crate::state::AppState;

/// Events one `GET /events/log` page may return. Matches the other keyset pages
/// in this API (`GET /jobs`, `GET /webhooks/deliveries`).
const MAX_LOG_PAGE: i64 = 500;

/// Ceiling on a connect-time replay served from the DURABLE log. A client
/// further behind than this is told to `reset` rather than handed a multi-minute
/// backlog on one connection — the gap is recoverable through `GET /events/log`
/// at the client's own pace, which is what that route is for.
const MAX_LOG_RESUME: usize = 10_000;

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
    let (initial, mut last_seq) = resume(&state, after, |_| true).await;
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
                    for ev in recover(&state, &mut last_seq, |_| true).await {
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
    let (replayed, mut last_seq) = resume(&state, after, move |ev| ev.job_id == id).await;
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
                    for ev in recover(&state, &mut last_seq, |ev| ev.job_id == id).await {
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

/// The events after `after`, from the ring if it still has them and from the
/// **durable log** if it does not — the read-through that N05 exists for.
///
/// `Err(latest)` is the only remaining `reset` path, and it now means something
/// much narrower than it used to: not "the ring evicted your place" (which
/// happens after 1024 events, i.e. constantly) but "the log has been pruned past
/// your cursor, or is off, or is unreadable". A client that reconnects within
/// the retention window across a restart gets its gap, not a resync order.
pub(crate) async fn replay_or_log(state: &AppState, after: u64) -> Result<Vec<SeqEvent>, u64> {
    let latest = state.events.latest_seq();
    // Already current: nothing to replay, and nothing to look up.
    if after >= latest {
        return Ok(Vec::new());
    }
    // The ring answers only when it covers the gap COMPLETELY — its first
    // buffered event must be exactly `after + 1`.
    //
    // The anti-pattern this replaces, and the reason it is spelled out: a fresh
    // process's ring is *empty*, and an empty ring reports `Events([])` ("no
    // loss possible") rather than `Reset`. Trusting that answer is how a resume
    // across a restart silently returned nothing at all — no events, no `reset`,
    // no error — which is worse than the `reset` N05 exists to remove.
    if let crate::events::Replay::Events(events) = state.events.replay(after) {
        if events.first().is_some_and(|(seq, _)| *seq == after + 1) {
            return Ok(events);
        }
    }
    if !state.events.logs() {
        return Err(latest);
    }
    // The bus's queue may still hold rows the log has not seen; flush before
    // reading or a resume across a fresh restart can miss the tail.
    if let Err(e) = state.events.persist_pending(&state.storage).await {
        tracing::warn!("event log: flush before replay failed: {e}");
    }
    let mut out: Vec<SeqEvent> = Vec::new();
    let mut cursor = after as i64;
    while (out.len() as u64) < latest.saturating_sub(after) {
        let page = match state
            .storage
            .events_after(cursor, None, None, MAX_LOG_PAGE)
            .await
        {
            Ok(page) if page.is_empty() => break,
            Ok(page) => page,
            Err(e) => {
                tracing::warn!("event log: replay read failed: {e}");
                return Err(latest);
            }
        };
        for record in page {
            // The first row must be exactly `after + 1`, or the log has been
            // pruned past this cursor and the gap is genuinely unrecoverable.
            if out.is_empty() && record.seq != cursor + 1 {
                return Err(latest);
            }
            cursor = record.seq;
            match serde_json::from_value::<JobEvent>(record.payload) {
                Ok(event) => out.push((record.seq as u64, Arc::new(event))),
                // A row whose body no longer parses is skipped, not fatal: one
                // corrupt payload must not turn a recoverable gap into a reset.
                Err(e) => tracing::warn!(seq = record.seq, "event log: unreadable payload: {e}"),
            }
            if out.len() >= MAX_LOG_RESUME {
                return Err(latest);
            }
        }
    }
    Ok(out)
}

/// Builds the connect-time replay for a resuming client: the events it missed
/// (filtered by `keep`), preceded by a `reset` marker when the gap is gone from
/// both the ring and the log. Returns the events plus the highest sequence id
/// now delivered, which the live loop uses to dedup the broadcast overlap
/// window.
async fn resume(
    state: &AppState,
    after: Option<u64>,
    keep: impl Fn(&JobEvent) -> bool,
) -> (Vec<Event>, u64) {
    let Some(after) = after else {
        return (Vec::new(), 0);
    };
    match replay_or_log(state, after).await {
        Err(latest) => (vec![reset_event(latest)], latest),
        Ok(events) => {
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

/// Recovers a live subscriber that lagged past the broadcast buffer: replays
/// past `last_seq` (ring, then log), advancing it, or emits a single `reset`
/// when the gap is unrecoverable.
async fn recover(
    state: &AppState,
    last_seq: &mut u64,
    keep: impl Fn(&JobEvent) -> bool,
) -> Vec<Event> {
    match replay_or_log(state, *last_seq).await {
        Err(latest) => {
            *last_seq = latest;
            vec![reset_event(latest)]
        }
        Ok(events) => {
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

#[derive(Debug, Deserialize, IntoParams)]
pub(crate) struct EventLogQuery {
    /// Return events with `seq` strictly greater than this. `0` (the default)
    /// starts at the beginning of what is retained.
    after: Option<i64>,
    /// Exact event kind (`job.succeeded`, `dataset.changed`, `external`, …).
    kind: Option<String>,
    /// Exact namespace.
    app: Option<String>,
    limit: Option<i64>,
}

/// Keyset page over the durable event log — the pull half of N05, beside the
/// SSE stream at `GET /events`.
///
/// Two surfaces rather than one because they answer different questions and
/// SSE cannot answer the second: `GET /events` is "tell me what happens next",
/// this is "what happened after seq N", which is what a consumer holding a
/// cursor (`@pumper/sync`'s `subscribe`, a peer, an agent) actually asks. It
/// lives at `/events/log` and not at `GET /events?after=` because that path is
/// the SSE stream and one path cannot be two content types.
///
/// `next_after` is the cursor to send back; it is `null` when the page reached
/// the end of the log, which is how a poller knows to back off rather than spin.
#[utoipa::path(
    get,
    path = "/events/log",
    tag = "events",
    params(EventLogQuery),
    responses(
        (status = 200, description = "`{events, count, next_after, latest_seq, retained, retention_days}`. Ascending by `seq`."),
        (status = 409, description = "The durable event log is off (`[events] log_enabled = false`)", body = Object),
    )
)]
pub(crate) async fn event_log(
    State(state): State<AppState>,
    Query(query): Query<EventLogQuery>,
) -> Result<Json<Value>, ApiError> {
    if !state.events.logs() {
        return Err(ApiError(
            axum::http::StatusCode::CONFLICT,
            "the durable event log is disabled (`[events] log_enabled = false`); only the live              SSE stream at GET /events is available"
                .into(),
        ));
    }
    // Flush first: a poller that just saw an event on SSE must be able to read
    // it here, or the two surfaces disagree about what has happened.
    if let Err(e) = state.events.persist_pending(&state.storage).await {
        tracing::warn!("event log: flush before read failed: {e}");
    }
    let limit = query.limit.unwrap_or(100).clamp(1, MAX_LOG_PAGE);
    let after = query.after.unwrap_or(0).max(0);
    let events = state
        .storage
        .events_after(after, query.kind.as_deref(), query.app.as_deref(), limit)
        .await?;
    // `null` when this page did not fill: the caller is caught up, and a poller
    // that keeps sending the same cursor is spinning, not paging.
    let next_after = (events.len() as i64 == limit)
        .then(|| events.last().map(|e| e.seq))
        .flatten();
    Ok(Json(json!({
        "count": events.len(),
        "next_after": next_after,
        "latest_seq": state.events.latest_seq(),
        "retained": state.storage.count_events().await.unwrap_or_default(),
        "pending": state.events.pending_len(),
        "retention_days": state.config.events.log_retention_days,
        "events": events,
    })))
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
