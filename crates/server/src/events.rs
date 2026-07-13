//! Live job events, broadcast to any SSE subscribers.
//!
//! Every emitted event is stamped with a process-global monotonic sequence id
//! and appended to a bounded in-memory replay ring. SSE handlers surface the
//! sequence as the wire-level event id, so a client that reconnects with
//! `Last-Event-ID` can be replayed the gap it missed (or told to `reset` when
//! the gap has already fallen out of the ring). The same ring lets a live
//! subscriber recover from broadcast lag without losing events.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::Serialize;
use serde_json::Value;
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct JobEvent {
    pub job_id: Uuid,
    pub app: String,
    /// queued | running | succeeded | failed | cancelled
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl JobEvent {
    pub fn new(job_id: Uuid, app: impl Into<String>, status: impl Into<String>) -> Self {
        Self {
            job_id,
            app: app.into(),
            status: status.into(),
            result: None,
            error: None,
        }
    }
}

/// A `JobEvent` paired with its monotonic sequence id.
pub type SeqEvent = (u64, JobEvent);

/// Outcome of a replay request against the ring.
pub enum Replay {
    /// The requested `after` id is older than anything still buffered — the
    /// caller lost events it can never recover, so it should reset its view.
    Reset,
    /// Buffered events with id strictly greater than `after` (may be empty when
    /// the caller is already current).
    Events(Vec<SeqEvent>),
}

/// Fan-out of job status transitions with a bounded replay ring.
///
/// `emit` assigns the next sequence id, appends to the ring (evicting the
/// oldest past capacity), and broadcasts `(seq, event)` to live subscribers.
pub struct EventBus {
    seq: AtomicU64,
    ring: Mutex<VecDeque<SeqEvent>>,
    capacity: usize,
    tx: broadcast::Sender<SeqEvent>,
}

impl EventBus {
    pub fn new(broadcast_capacity: usize, ring_capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(broadcast_capacity);
        Self {
            seq: AtomicU64::new(0),
            ring: Mutex::new(VecDeque::with_capacity(ring_capacity)),
            capacity: ring_capacity.max(1),
            tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SeqEvent> {
        self.tx.subscribe()
    }

    /// Highest sequence id assigned so far (0 before the first event).
    pub fn latest_seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    /// Stamps `event` with the next id, buffers it, and broadcasts it. Returns
    /// the assigned id. Broadcast errors (no subscribers) are ignored.
    pub fn emit(&self, event: JobEvent) -> u64 {
        let seq = self.seq.fetch_add(1, Ordering::AcqRel) + 1;
        {
            let mut ring = self.ring.lock().unwrap();
            if ring.len() >= self.capacity {
                ring.pop_front();
            }
            ring.push_back((seq, event.clone()));
        }
        let _ = self.tx.send((seq, event));
        seq
    }

    /// Events buffered after `after`, or `Reset` when the id immediately after
    /// `after` has already been evicted (an unrecoverable gap).
    pub fn replay(&self, after: u64) -> Replay {
        let ring = self.ring.lock().unwrap();
        let Some((oldest, _)) = ring.front() else {
            // Nothing buffered yet: no loss possible, just nothing to replay.
            return Replay::Events(Vec::new());
        };
        // The next id the caller wants is `after + 1`. If that id predates the
        // oldest buffered event, the gap was evicted and can't be replayed.
        if *oldest > after + 1 {
            return Replay::Reset;
        }
        let events = ring
            .iter()
            .filter(|(seq, _)| *seq > after)
            .cloned()
            .collect();
        Replay::Events(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(status: &str) -> JobEvent {
        JobEvent::new(Uuid::nil(), "test", status)
    }

    #[test]
    fn emit_assigns_monotonic_ids() {
        let bus = EventBus::new(16, 8);
        assert_eq!(bus.emit(ev("queued")), 1);
        assert_eq!(bus.emit(ev("running")), 2);
        assert_eq!(bus.emit(ev("succeeded")), 3);
        assert_eq!(bus.latest_seq(), 3);
    }

    #[test]
    fn replay_returns_events_after_cursor() {
        let bus = EventBus::new(16, 8);
        for _ in 0..3 {
            bus.emit(ev("running"));
        }
        match bus.replay(1) {
            Replay::Events(evs) => {
                let ids: Vec<u64> = evs.iter().map(|(s, _)| *s).collect();
                assert_eq!(ids, vec![2, 3]);
            }
            Replay::Reset => panic!("expected replay, got reset"),
        }
    }

    #[test]
    fn replay_current_cursor_is_empty_not_reset() {
        let bus = EventBus::new(16, 8);
        bus.emit(ev("running"));
        match bus.replay(1) {
            Replay::Events(evs) => assert!(evs.is_empty()),
            Replay::Reset => panic!("current cursor must not reset"),
        }
    }

    #[test]
    fn replay_resets_when_gap_evicted() {
        // Ring holds only the last 4 events; ids 1..=6 emitted, so 1 and 2 are
        // evicted (oldest retained id is 3).
        let bus = EventBus::new(16, 4);
        for _ in 0..6 {
            bus.emit(ev("running"));
        }
        // Cursor at 1 wants id 2, which was evicted -> unrecoverable.
        assert!(matches!(bus.replay(1), Replay::Reset));
        // Cursor at 2 wants id 3, still buffered -> replayable.
        assert!(matches!(bus.replay(2), Replay::Events(_)));
    }

    #[test]
    fn replay_empty_ring_is_noop() {
        let bus = EventBus::new(16, 8);
        assert!(matches!(bus.replay(0), Replay::Events(evs) if evs.is_empty()));
    }
}
