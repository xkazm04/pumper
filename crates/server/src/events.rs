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
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::Value;
use tokio::sync::broadcast;
use uuid::Uuid;

/// Default byte ceiling for the replay ring (32 MiB) — bounds RSS from buffered
/// large-result events regardless of the count capacity.
pub const DEFAULT_MAX_RING_BYTES: usize = 32 * 1024 * 1024;

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
///
/// The event is behind an `Arc` so the ring, the broadcast slot, and every
/// subscriber share **one** allocation instead of deep-cloning a possibly
/// multi-MB `result` tree per copy. `recv()` on the broadcast channel then costs
/// a refcount bump, not an O(size) clone × N receivers.
pub type SeqEvent = (u64, Arc<JobEvent>);

/// Outcome of a replay request against the ring.
pub enum Replay {
    /// The requested `after` id is older than anything still buffered — the
    /// caller lost events it can never recover, so it should reset its view.
    Reset,
    /// Buffered events with id strictly greater than `after` (may be empty when
    /// the caller is already current).
    Events(Vec<SeqEvent>),
}

/// One buffered event plus the approximate byte cost charged to the ring's byte
/// budget (computed once at emit), so eviction can refund it exactly.
struct Buffered {
    event: SeqEvent,
    bytes: usize,
}

/// Ring contents guarded by one mutex: the deque plus its running byte total.
struct Ring {
    deque: VecDeque<Buffered>,
    bytes: usize,
}

/// Fan-out of job status transitions with a bounded replay ring.
///
/// `emit` assigns the next sequence id, appends to the ring (evicting the oldest
/// past **either** the count capacity **or** the byte budget), and broadcasts
/// `(seq, Arc<event>)` to live subscribers.
pub struct EventBus {
    seq: AtomicU64,
    ring: Mutex<Ring>,
    capacity: usize,
    /// Soft ceiling on the ring's aggregate serialized-result bytes. The ring is
    /// otherwise bounded only by event *count*, so a burst of large-result jobs
    /// could pin `capacity × result_size` (~1 GB at 1 MB × 1024) of RSS for the
    /// process lifetime. Always keeps at least one event so replay stays useful.
    max_bytes: usize,
    tx: broadcast::Sender<SeqEvent>,
}

/// Approximate an event's memory cost by its serialized `result` length (the
/// only unbounded field); the fixed struct overhead is negligible next to a
/// multi-MB result and not worth serializing the whole event to measure.
fn approx_bytes(event: &JobEvent) -> usize {
    event
        .result
        .as_ref()
        .map(|v| serde_json::to_string(v).map(|s| s.len()).unwrap_or(0))
        .unwrap_or(0)
}

impl EventBus {
    pub fn new(broadcast_capacity: usize, ring_capacity: usize) -> Self {
        Self::with_byte_budget(broadcast_capacity, ring_capacity, DEFAULT_MAX_RING_BYTES)
    }

    pub fn with_byte_budget(
        broadcast_capacity: usize,
        ring_capacity: usize,
        max_bytes: usize,
    ) -> Self {
        let (tx, _) = broadcast::channel(broadcast_capacity);
        Self {
            seq: AtomicU64::new(0),
            ring: Mutex::new(Ring { deque: VecDeque::with_capacity(ring_capacity), bytes: 0 }),
            capacity: ring_capacity.max(1),
            max_bytes,
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
        // Assign the id, buffer, and broadcast all under the ring lock so
        // concurrent emitters (per-job worker tasks + HTTP handlers) can't
        // interleave: without this, seq assignment happened before the lock, so a
        // higher id could be buffered/sent ahead of a lower one — corrupting ring
        // and wire order and triggering false `reset` gaps for live subscribers.
        let bytes = approx_bytes(&event);
        let event = Arc::new(event);
        let mut ring = self.ring.lock().unwrap();
        let seq = self.seq.fetch_add(1, Ordering::AcqRel) + 1;
        ring.deque.push_back(Buffered { event: (seq, Arc::clone(&event)), bytes });
        ring.bytes += bytes;
        // Evict oldest past the count capacity OR the byte budget, always keeping
        // the event just pushed so the ring is never empty after an emit.
        while ring.deque.len() > self.capacity
            || (ring.bytes > self.max_bytes && ring.deque.len() > 1)
        {
            if let Some(old) = ring.deque.pop_front() {
                ring.bytes -= old.bytes;
            }
        }
        let _ = self.tx.send((seq, event));
        seq
    }

    /// Events buffered after `after`, or `Reset` when the id immediately after
    /// `after` has already been evicted (an unrecoverable gap).
    pub fn replay(&self, after: u64) -> Replay {
        let ring = self.ring.lock().unwrap();
        let Some(front) = ring.deque.front() else {
            // Nothing buffered yet: no loss possible, just nothing to replay.
            return Replay::Events(Vec::new());
        };
        let oldest = front.event.0;
        // The next id the caller wants is `after + 1`. If that id predates the
        // oldest buffered event, the gap was evicted and can't be replayed.
        // `saturating_add` guards an adversarial `Last-Event-ID: u64::MAX` (a
        // plain `+ 1` panics in debug / wraps to 0 in release).
        if oldest > after.saturating_add(1) {
            return Replay::Reset;
        }
        // Clone here is a per-event `Arc` refcount bump, not a result deep-copy.
        let events = ring
            .deque
            .iter()
            .filter(|b| b.event.0 > after)
            .map(|b| b.event.clone())
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

    fn ev_with_result(bytes: usize) -> JobEvent {
        let mut e = JobEvent::new(Uuid::nil(), "test", "succeeded");
        e.result = Some(Value::String("x".repeat(bytes)));
        e
    }

    #[test]
    fn byte_budget_evicts_before_count_capacity() {
        // Count capacity is large (100), but the byte budget is ~3 KB and each
        // event carries a ~1 KB result — so the ring is held to a handful of
        // events by bytes, not by count.
        let bus = EventBus::with_byte_budget(256, 100, 3_000);
        for _ in 0..50 {
            bus.emit(ev_with_result(1_000));
        }
        let ring = bus.ring.lock().unwrap();
        assert!(ring.deque.len() < 10, "byte budget should cap well under count capacity");
        assert!(ring.bytes <= 3_000 || ring.deque.len() == 1, "bytes within budget (or the mandatory last event)");
        // The running byte total stays consistent with the retained events.
        let summed: usize = ring.deque.iter().map(|b| b.bytes).sum();
        assert_eq!(summed, ring.bytes);
    }

    #[test]
    fn byte_budget_always_keeps_at_least_one() {
        // A single event larger than the whole budget must still be retained.
        let bus = EventBus::with_byte_budget(16, 8, 100);
        bus.emit(ev_with_result(10_000));
        let ring = bus.ring.lock().unwrap();
        assert_eq!(ring.deque.len(), 1);
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
