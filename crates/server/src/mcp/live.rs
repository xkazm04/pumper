//! The MCP transport's server→client half: `GET /mcp` opens an SSE stream of
//! JSON-RPC `notifications/*` messages bridged from the [`EventBus`], plus the
//! `wait_job` tool that awaits one job's terminal status over the same bus.
//!
//! Mirrors the `/events` SSE route's replay/recover discipline (the bus is
//! consumed read-only — subscribe + replay, never refactored): every SSE event
//! carries the bus's monotonic sequence as its wire id, so a client that
//! reconnects with `Last-Event-ID` is replayed the gap it missed, or is sent a
//! `notifications/pumper/reset` when the gap has already fallen out of the
//! ring. Buffering is bounded twice over — the broadcast channel and the replay
//! ring both have fixed capacities — so a slow consumer can never block the
//! bus: it *lags*, gets a warning log, and is recovered from the ring (or told
//! to reset) instead of stalling emitters.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use pumper_core::JobStatus;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;

use crate::events::{JobEvent, Replay};
use crate::state::AppState;

/// JSON-RPC method carried by every bridged bus event.
const NOTIFY_EVENT: &str = "notifications/pumper/job";
/// JSON-RPC method telling a resuming client its replay gap was evicted.
const NOTIFY_RESET: &str = "notifications/pumper/reset";

/// Per-connection filters, from `GET /mcp?app=…&kind=…` query params.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct LiveFilter {
    /// Only events emitted by this app (exact match on the event's `app`).
    app: Option<String>,
    /// Comma-separated event kinds to keep — job statuses (`queued`,
    /// `running`, `succeeded`, `failed`, `cancelled`) and/or `external`.
    kind: Option<String>,
}

impl LiveFilter {
    fn keep(&self, ev: &JobEvent) -> bool {
        if let Some(app) = &self.app {
            if ev.app != *app {
                return false;
            }
        }
        if let Some(kinds) = &self.kind {
            let mut wanted = kinds.split(',').map(str::trim).filter(|k| !k.is_empty());
            if !wanted.any(|k| k == ev.status) {
                return false;
            }
        }
        true
    }
}

/// `GET /mcp`: the streamable-HTTP transport's SSE stream. Each SSE event's
/// `data` is one JSON-RPC notification (`notifications/pumper/job` carrying the
/// bus event, or `notifications/pumper/reset` on an unrecoverable replay gap)
/// and its `id` is the bus sequence, so `Last-Event-ID` resume works exactly
/// like the plain `/events` feed.
pub(crate) async fn handle_get(
    State(state): State<AppState>,
    Query(filter): Query<LiveFilter>,
    headers: HeaderMap,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let after = last_event_id(&headers);
    // Subscribe before replaying so no event slips through the gap between
    // "read the ring" and "listen live"; the overlap is deduped by `last_seq`.
    let mut rx = state.events.subscribe();
    let shutdown = state.shutdown.clone();
    let (initial, mut last_seq) = replay_backlog(&state, after, &filter);
    let stream = async_stream::stream! {
        for ev in initial {
            yield Ok(ev);
        }
        loop {
            // Ends on the shutdown token, at a frame boundary: the generator
            // returns between complete SSE events, so the client sees a clean
            // end-of-stream (its cue to reconnect with `Last-Event-ID`, which
            // the replay ring already serves) and never a truncated JSON-RPC
            // frame. Same helper as the plain `/events` feed — see
            // `routes::next_or_shutdown` for why a bare `recv()` never ends.
            let Some(received) = crate::routes::next_or_shutdown(&mut rx, &shutdown).await else {
                break;
            };
            match received {
                Ok((seq, event)) => {
                    if seq <= last_seq {
                        continue; // already replayed (overlap window)
                    }
                    last_seq = seq;
                    if filter.keep(&event) {
                        yield Ok(notification_event(seq, &event));
                    }
                }
                Err(RecvError::Lagged(missed)) => {
                    // Bounded buffering doing its job: this consumer fell
                    // behind the broadcast capacity and dropped events rather
                    // than blocking the bus. Warn, then recover what the ring
                    // still holds (or tell the client to reset).
                    tracing::warn!(
                        missed,
                        "slow MCP notification consumer lagged the event bus; \
                         recovering from the replay ring"
                    );
                    for ev in recover(&state, &mut last_seq, &filter) {
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
fn last_event_id(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse().ok())
}

/// Connect-time replay for a resuming client: buffered events it missed
/// (post-filter), preceded by a reset notification when the gap is too old.
/// Returns the events plus the highest sequence id now delivered.
fn replay_backlog(state: &AppState, after: Option<u64>, filter: &LiveFilter) -> (Vec<Event>, u64) {
    let Some(after) = after else {
        return (Vec::new(), 0);
    };
    match state.events.replay(after) {
        Replay::Reset => {
            let latest = state.events.latest_seq();
            (vec![reset_event(latest)], latest)
        }
        Replay::Events(events) => {
            let mut last = after;
            let mut out = Vec::new();
            for (seq, event) in events {
                last = seq;
                if filter.keep(&event) {
                    out.push(notification_event(seq, &event));
                }
            }
            (out, last)
        }
    }
}

/// Recovers a lagged live subscriber from the replay ring, advancing
/// `last_seq`, or emits a single reset notification when the gap is gone.
fn recover(state: &AppState, last_seq: &mut u64, filter: &LiveFilter) -> Vec<Event> {
    match state.events.replay(*last_seq) {
        Replay::Reset => {
            let latest = state.events.latest_seq();
            *last_seq = latest;
            vec![reset_event(latest)]
        }
        Replay::Events(events) => {
            let mut out = Vec::new();
            for (seq, event) in events {
                *last_seq = seq;
                if filter.keep(&event) {
                    out.push(notification_event(seq, &event));
                }
            }
            out
        }
    }
}

/// One bus event as an SSE-framed JSON-RPC notification.
fn notification_event(seq: u64, event: &JobEvent) -> Event {
    let msg = json!({
        "jsonrpc": "2.0",
        "method": NOTIFY_EVENT,
        "params": { "seq": seq, "event": event },
    });
    Event::default().id(seq.to_string()).data(msg.to_string())
}

/// Tells a resuming client its replay gap was evicted; carries the latest
/// sequence so the client can advance its `Last-Event-ID` pointer and resync.
fn reset_event(latest: u64) -> Event {
    let msg = json!({
        "jsonrpc": "2.0",
        "method": NOTIFY_RESET,
        "params": {
            "latest_seq": latest,
            "reason": "replay gap: reconnect point too old, resync state",
        },
    });
    Event::default()
        .id(latest.to_string())
        .data(msg.to_string())
}

// ---- wait_job ----------------------------------------------------------------

/// The `wait_job` tool: blocks (bounded) until the job reaches a terminal
/// status, watching the event bus rather than polling storage. `timeout_secs`
/// is clamped to `[mcp] wait_job_max_secs`; hitting the deadline is a normal
/// result with `timed_out: true` and the job's current snapshot, not an error —
/// the agent can decide to wait again or move on.
pub(crate) async fn wait_job(state: &AppState, args: &Value) -> Result<Value, String> {
    let id: uuid::Uuid = super::require_str(args, "job_id")?
        .parse()
        .map_err(|e| format!("invalid job_id: {e}"))?;
    let cap = state.config.mcp.wait_job_max_secs.max(1);
    let timeout = args
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .unwrap_or(cap)
        .clamp(1, cap);
    // Subscribe BEFORE the status read so a transition emitted between the read
    // and the listen loop cannot be missed.
    let mut rx = state.events.subscribe();
    let job = fetch(state, id).await?;
    if job.status.is_terminal() {
        let status = job.status.as_str().to_string();
        return Ok(finished(job, &status));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout);
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Err(_) => break, // deadline reached
            Ok(Err(RecvError::Closed)) => break,
            Ok(Err(RecvError::Lagged(_))) => {
                // The terminal transition may be among the dropped events —
                // storage is the truth, so re-check it instead of the ring.
                let job = fetch(state, id).await?;
                if job.status.is_terminal() {
                    let status = job.status.as_str().to_string();
                    return Ok(finished(job, &status));
                }
            }
            Ok(Ok((_seq, event))) => {
                if event.job_id == id
                    && JobStatus::parse(event.status.as_str()).is_some_and(|s| s.is_terminal())
                {
                    let job = fetch(state, id).await?;
                    return Ok(finished(job, &event.status));
                }
            }
        }
    }
    let job = fetch(state, id).await?;
    Ok(json!({
        "timed_out": true,
        "waited_secs": timeout,
        "status": job.status.as_str(),
        "job": job,
        "note": "not terminal yet — call wait_job again to keep waiting",
    }))
}

async fn fetch(state: &AppState, id: uuid::Uuid) -> Result<pumper_core::Job, String> {
    state
        .storage
        .get(id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("unknown job '{id}'"))
}

fn finished(job: pumper_core::Job, status: &str) -> Value {
    json!({ "timed_out": false, "status": status, "job": job })
}
