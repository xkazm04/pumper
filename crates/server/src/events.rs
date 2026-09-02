//! Live events, broadcast to SSE subscribers and appended to a durable log.
//!
//! Every emitted event is stamped with a monotonic sequence id and appended to a
//! bounded in-memory replay ring. SSE handlers surface the sequence as the
//! wire-level event id, so a client that reconnects with `Last-Event-ID` can be
//! replayed the gap it missed. The same ring lets a live subscriber recover from
//! broadcast lag without losing events.
//!
//! ## The durable log (N05)
//!
//! The ring alone made two promises it could not keep: the sequence restarted at
//! `0` on every boot, so a client's `Last-Event-ID` pointed at somebody else's
//! event; and a gap older than 1024 events (or 32 MiB) was answered with `reset`
//! — "rebuild your whole view, I lost your place". With `[events] log_enabled`
//! (default ON) the ring becomes a **read-through cache** over an `events`
//! table:
//!
//! - `emit` stays synchronous and infallible — it assigns the id, buffers, and
//!   broadcasts exactly as before, and additionally queues the row for the log.
//!   The queue is what keeps a SQLite write off ~15 call sites that are not
//!   async and must never block on a store.
//! - [`EventBus::persist_pending`] writes the queue in ONE transaction. It runs
//!   at the head of the subscription outbox — on the scheduler tick and in a
//!   finished job's fan-out — so the log is at most one tick behind the ring and
//!   the outbox never reads a cursor past what is durable.
//! - The queue is bounded (`[events] pending_capacity`). Past it the OLDEST
//!   entries are dropped and **counted**, and the count is reported by the next
//!   persist: a store that is down costs bounded memory and a stated loss, not
//!   unbounded RSS and a silent one.
//! - [`EventBus::seed_seq`] sets the counter from `MAX(seq)` at boot, which is
//!   what makes a cursor survive a restart.
//!
//! Everything above is inert when `log_enabled = false`: no queue, no rows, and
//! the pre-N05 `reset` semantics.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pumper_core::{NewEvent, Storage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;
use uuid::Uuid;

/// Default byte ceiling for the replay ring (32 MiB) — bounds RSS from buffered
/// large-result events regardless of the count capacity.
pub const DEFAULT_MAX_RING_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEvent {
    pub job_id: Uuid,
    pub app: String,
    /// A job transition (`queued` | `running` | `waiting` | `succeeded` |
    /// `failed` | `cancelled` | `progress`), `external` for an inbound ingress
    /// event, or a dotted **domain** kind (`dataset.changed`,
    /// `transaction.submitted`, `source.repair_promoted`, `workflow.completed`)
    /// emitted by [`JobEvent::domain`]. [`event_kind`] is the one place that
    /// turns this into the log's `kind` vocabulary.
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Where this run's wall-clock went (run / index / hooks / alerts / total),
    /// stamped on the terminal event of a succeeded job. Absent on every other
    /// transition and on jobs that failed before their fan-out — an unmeasured
    /// stage is omitted, never reported as zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stages: Option<pumper_core::JobStages>,
    /// What this event is ABOUT when that is not a job: a watch's dataset, a
    /// transaction id, a source name. Absent on job transitions, where the
    /// subject is `job_id` — an omitted field rather than a duplicate one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
}

impl JobEvent {
    pub fn new(job_id: Uuid, app: impl Into<String>, status: impl Into<String>) -> Self {
        Self {
            job_id,
            app: app.into(),
            status: status.into(),
            result: None,
            error: None,
            stages: None,
            subject: None,
        }
    }

    /// A **domain** event: something happened that is not a job transition —
    /// a dataset changed, a transaction was submitted, a source's extraction
    /// rules were repaired.
    ///
    /// It rides the same bus, the same ring and the same log as a job
    /// transition, which is the entire point of N05: before this, a kind that
    /// was not one of four hand-wired webhook paths could not be subscribed to
    /// at all. `kind` is dotted (`dataset.changed`) and is carried verbatim into
    /// the log — see [`event_kind`].
    pub fn domain(
        kind: impl Into<String>,
        app: impl Into<String>,
        subject_id: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            // Domain events have no job identity. Nil rather than a synthesized
            // uuid so `/jobs/{id}/stream`'s `job_id` filter can never match one.
            job_id: Uuid::nil(),
            app: app.into(),
            status: kind.into(),
            result: Some(payload),
            error: None,
            stages: None,
            subject: Some(subject_id.into()),
        }
    }

    /// An inbound-ingress event (`POST /ingest/{id}`), stamped onto the same
    /// bus as job transitions so it rides SSE + the replay ring for free.
    /// `event_id` is the per-delivery id (doubles as the trigger idempotency
    /// scope), `source` is the ingress source name, and the verified payload
    /// travels in `result`.
    pub fn external(event_id: Uuid, source: impl Into<String>, payload: Value) -> Self {
        Self {
            job_id: event_id,
            app: source.into(),
            status: "external".into(),
            result: Some(payload),
            error: None,
            stages: None,
            subject: None,
        }
    }
}

/// The log's `kind` for one bus event — the subscribable vocabulary.
///
/// Three shapes, and the rule is the dot:
/// - `external` stays `external` (the inbound-ingress kind, named before N05
///   and kept so existing `?kind=external` filters do not break);
/// - anything already dotted is a domain kind and passes through verbatim
///   (`dataset.changed`, `transaction.pending`, `workflow.completed`);
/// - everything else is a job status and becomes `job.<status>`.
///
/// Extracted and tested rather than inlined at the two persistence call sites
/// because a selector matches on this string: a drift between what the log
/// stores and what a subscription asks for is a subscription that silently
/// never fires.
pub fn event_kind(event: &JobEvent) -> String {
    if event.status == "external" {
        return "external".to_string();
    }
    if event.status.contains('.') {
        return event.status.clone();
    }
    format!("job.{}", event.status)
}

/// The log's `subject_id` for one bus event: the explicit subject when the event
/// has one, else the job id.
pub fn event_subject(event: &JobEvent) -> String {
    event
        .subject
        .clone()
        .unwrap_or_else(|| event.job_id.to_string())
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
    /// N05: rows queued for the durable log, oldest first. `None` when
    /// `[events] log_enabled = false` — not an empty queue, so `emit` does not
    /// even take the lock.
    pending: Option<Mutex<VecDeque<NewEvent>>>,
    /// Ceiling on `pending`. See the module docs: exceeding it is a bounded,
    /// counted loss.
    pending_capacity: usize,
    /// Events dropped before their log write because the queue was full.
    /// Reported by the next [`EventBus::persist_pending`] and never reset — a
    /// process that lost events should keep saying so.
    dropped_before_persist: AtomicU64,
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
            ring: Mutex::new(Ring {
                deque: VecDeque::with_capacity(ring_capacity),
                bytes: 0,
            }),
            capacity: ring_capacity.max(1),
            max_bytes,
            tx,
            pending: None,
            pending_capacity: 0,
            dropped_before_persist: AtomicU64::new(0),
        }
    }

    /// Turns on the durable log. Without this the bus is exactly the pre-N05
    /// in-memory ring, which is what `[events] log_enabled = false` restores and
    /// what every unit test that builds a bare bus gets.
    pub fn with_log(mut self, pending_capacity: usize) -> Self {
        self.pending = Some(Mutex::new(VecDeque::new()));
        self.pending_capacity = pending_capacity.max(1);
        self
    }

    /// Whether this bus writes to the durable log.
    pub fn logs(&self) -> bool {
        self.pending.is_some()
    }

    /// Seeds the sequence counter from the log's `MAX(seq)` at boot.
    ///
    /// This one line is what makes `Last-Event-ID` mean the same thing across a
    /// restart. Without it the counter restarts at 0, the first event of the new
    /// process collides with an id the last process already used, and every
    /// resuming client is either lied to or told to `reset`.
    ///
    /// Called once, before anything can emit (`AppState::init`, and any test
    /// that simulates a restart). Idempotent and monotonic: it never lowers a
    /// counter that has already moved.
    pub fn seed_seq(&self, seq: u64) {
        self.seq.fetch_max(seq, Ordering::AcqRel);
    }

    /// Events queued for the log right now (0 when the log is off).
    pub fn pending_len(&self) -> usize {
        match &self.pending {
            Some(q) => q.lock().unwrap().len(),
            None => 0,
        }
    }

    /// Events dropped before their log write because the queue was full.
    pub fn dropped_before_persist(&self) -> u64 {
        self.dropped_before_persist.load(Ordering::Relaxed)
    }

    /// Writes every queued event to the durable log in one transaction.
    ///
    /// Returns the number of rows that landed. On a store error the batch is
    /// pushed back onto the FRONT of the queue in its original order and the
    /// error is returned: an event that could not be written is still owed, and
    /// the next tick retries it. (If the queue has filled past its ceiling
    /// meanwhile, the re-queue drops the oldest — the same bounded loss, still
    /// counted.)
    pub async fn persist_pending(&self, storage: &Storage) -> Result<usize, pumper_core::Error> {
        let Some(queue) = &self.pending else {
            return Ok(0);
        };
        let batch: Vec<NewEvent> = {
            let mut q = queue.lock().unwrap();
            q.drain(..).collect()
        };
        if batch.is_empty() {
            return Ok(0);
        }
        match storage.append_events(&batch).await {
            Ok(landed) => Ok(landed),
            Err(e) => {
                let mut q = queue.lock().unwrap();
                for ev in batch.into_iter().rev() {
                    q.push_front(ev);
                }
                while q.len() > self.pending_capacity {
                    q.pop_front();
                    self.dropped_before_persist.fetch_add(1, Ordering::Relaxed);
                }
                Err(e)
            }
        }
    }

    /// Queues one stamped event for the log, evicting the oldest if the queue is
    /// at its ceiling. Called under the ring lock so the queue's order is the
    /// sequence order.
    fn queue_for_log(&self, seq: u64, event: &JobEvent) {
        let Some(queue) = &self.pending else {
            return;
        };
        let payload = match serde_json::to_value(event) {
            Ok(payload) => payload,
            // Unserializable is not silently droppable: the row still records
            // that the event happened and why its body is missing.
            Err(e) => serde_json::json!({ "unserializable": e.to_string() }),
        };
        let mut q = queue.lock().unwrap();
        while q.len() >= self.pending_capacity {
            q.pop_front();
            self.dropped_before_persist.fetch_add(1, Ordering::Relaxed);
        }
        q.push_back(NewEvent {
            seq: seq as i64,
            kind: event_kind(event),
            app: event.app.clone(),
            subject_id: event_subject(event),
            payload,
        });
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
        ring.deque.push_back(Buffered {
            event: (seq, Arc::clone(&event)),
            bytes,
        });
        ring.bytes += bytes;
        // Queue the durable row here, still under the ring lock, so the log's
        // row order is the sequence order even when two emitters interleave.
        self.queue_for_log(seq, &event);
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
        assert!(
            ring.deque.len() < 10,
            "byte budget should cap well under count capacity"
        );
        assert!(
            ring.bytes <= 3_000 || ring.deque.len() == 1,
            "bytes within budget (or the mandatory last event)"
        );
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

    // ---- N05: the durable log ------------------------------------------

    /// A job status becomes `job.<status>`, a dotted domain kind passes through,
    /// and `external` keeps the name it had before the log existed.
    ///
    /// The anti-pattern: computing the kind inline at each persistence site.
    /// A selector matches on this exact string, so any drift between what the
    /// log stores and what a subscription asks for is a subscription that never
    /// fires and says nothing.
    #[test]
    fn kind_is_namespaced_not_a_bare_status() {
        assert_eq!(event_kind(&ev("succeeded")), "job.succeeded");
        assert_eq!(event_kind(&ev("waiting")), "job.waiting");
        assert_eq!(event_kind(&ev("progress")), "job.progress");
        // Dotted kinds are already namespaced — `job.dataset.changed` would be
        // a kind no subscription could name.
        assert_eq!(
            event_kind(&JobEvent::domain(
                "dataset.changed",
                "grants",
                "unified",
                Value::Null
            )),
            "dataset.changed"
        );
        assert_eq!(
            event_kind(&JobEvent::domain(
                "transaction.submitted",
                "transact",
                "tx1",
                Value::Null
            )),
            "transaction.submitted"
        );
        // Pre-N05 name, kept: `?kind=external` filters already exist.
        assert_eq!(
            event_kind(&JobEvent::external(Uuid::nil(), "src", Value::Null)),
            "external"
        );
    }

    /// A domain event's subject is its own; a job event's is the job id.
    #[test]
    fn subject_is_the_domain_id_not_a_nil_job_id() {
        let domain = JobEvent::domain("dataset.changed", "grants", "unified", Value::Null);
        assert_eq!(event_subject(&domain), "unified");
        let id = Uuid::new_v4();
        assert_eq!(
            event_subject(&JobEvent::new(id, "fake", "queued")),
            id.to_string()
        );
    }

    /// The restart property, at the bus level: seeding lifts the counter so the
    /// next id cannot collide with one a previous process already put on the
    /// wire. Monotonic — a stale seed can never rewind a live counter.
    #[test]
    fn seeding_lifts_the_counter_and_never_lowers_it() {
        let bus = EventBus::new(16, 8).with_log(64);
        bus.seed_seq(41);
        assert_eq!(bus.emit(ev("queued")), 42);
        bus.seed_seq(7);
        assert_eq!(bus.emit(ev("running")), 43, "a stale seed must not rewind");
    }

    /// Without `with_log` nothing is queued: `[events] log_enabled = false` is
    /// the pre-N05 bus byte for byte.
    #[test]
    fn a_bus_without_a_log_queues_nothing() {
        let bus = EventBus::new(16, 8);
        bus.emit(ev("queued"));
        assert!(!bus.logs());
        assert_eq!(bus.pending_len(), 0);
    }

    /// A wedged store costs bounded memory and a COUNTED loss, not unbounded
    /// RSS and a silent one.
    #[test]
    fn a_full_pending_queue_drops_oldest_and_counts_it() {
        let bus = EventBus::new(64, 64).with_log(4);
        for _ in 0..10 {
            bus.emit(ev("running"));
        }
        assert_eq!(bus.pending_len(), 4, "the queue is bounded");
        assert_eq!(bus.dropped_before_persist(), 6, "and says how much it lost");
    }
}
