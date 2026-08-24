//! The store measuring itself.
//!
//! No monitoring agent watches an embedded database. Either the process
//! measures its own SQLite behaviour or "the store feels slow" has nothing to
//! interrogate — which is exactly where this repo was: `/metrics` carried job,
//! cost and delivery gauges and not one number about the engine underneath them
//! (registry: embedded-db/db-self-instrumentation).
//!
//! ## What is measured, and what is not
//!
//! **An honest partial census beats a fake total one.** The workspace issues
//! roughly two hundred distinct statements; this instrument wraps
//! [`StoreOp::ALL`] — the job queue's own path (enqueue, the atomic claim, the
//! finish/fail verdicts, the recovery sweep, the status aggregate) and the
//! dataset write path — and nothing else. Every number here is therefore a
//! claim about *those families only*. Reads of schedules, watches, triggers,
//! deliveries, the HTTP cache and the search index are **unmeasured**, and a
//! p95 here says nothing about them. That boundary is stated on the diagnostic
//! surface too, so nobody quotes a partial census as a total one.
//!
//! ## The keying rule
//!
//! Keys are `(operation family, phase)` drawn from two closed vocabularies, and
//! each family names the table it touches — never statement text, which embeds
//! values (unbounded cardinality) and shatters one logical hot path across many
//! near-duplicate keys. Naming the table is also what makes the join the
//! technique asks for *possible*: the accounting report
//! ([`crate::storage::Storage::ledger_stats`], and now [`StoreSize`]) says which
//! table is big, these rings say which is slow, and "big AND degrading" is the
//! strongest signal the pair produces.
//!
//! **Pool acquisition is its own phase.** The wait for a connection happens
//! before any statement runs, in code no query profiler attributes; folding it
//! into query time hides a saturated pool behind fast-looking queries, and the
//! two have disjoint remedies (pool sizing versus an index).
//!
//! ## The write-path budget
//!
//! This wraps the hottest chokepoint in the process, so [`StoreInstrument::record`]
//! is constant-time: one clock read, one packed `u64` stored into a fixed ring,
//! a handful of relaxed atomic adds. No allocation, no formatting, no lock, and
//! — the rule this layer adds on top of the general ring discipline — **no use
//! of the database**. Metrics that write to a metrics table turn every measured
//! operation into two, contend for the very locks being measured, and recurse
//! the instrument into its own signal.
//!
//! Everything expensive lives at read time ([`StoreInstrument::snapshot`]),
//! paid by the one operator who asked.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::error::{Error, Result};

/// Records retained per key. Sized from the question it answers: hundreds of
/// samples make a stable tail percentile, and the whole instrument costs
/// `KEY_COUNT × RING × 12 bytes` — under 3 KiB — which is a number someone can
/// approve rather than a growth curve to notice in a heap snapshot later.
const RING: usize = 256;

/// How often one key may emit a slow-operation diagnostic. The count is never
/// rate-limited (see [`KeyReport::slow_lifetime`]); only the log line is.
pub const SLOW_WARN_WINDOW: Duration = Duration::from_secs(60);

/// Maintenance pass records kept for the diagnostic surface. A pass happens on
/// the order of once per interval, so this is hours of history, not seconds.
const MAINTENANCE_LOG: usize = 64;

// ---- the closed vocabularies ----------------------------------------------

/// The operation families this store measures — a **closed** vocabulary.
///
/// Closed by construction: the key space is `StoreOp::ALL × StorePhase::ALL`,
/// so per-key rings can never multiply without bound the way a statement-keyed
/// map would. Adding a family is a deliberate edit here, and
/// `store_instrument_vocabulary.rs` refuses a family with no table decision, no
/// token, or a token that collides with another's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreOp {
    /// `Storage::enqueue_dedup`'s INSERT — the door every unit of work enters
    /// through.
    JobEnqueue,
    /// `Storage::claim_next` — the atomic claim. The single statement whose
    /// latency the whole worker pool waits on.
    JobClaim,
    /// The terminal verdicts: `complete`, `fail`'s UPDATE, `fail_permanently`.
    JobVerdict,
    /// The recovery sweep's re-queue path (`reap_stale`), which runs at boot
    /// and on the lease reaper's tick.
    JobRecovery,
    /// The `GROUP BY status` aggregate plus the queue-age read behind
    /// `/metrics` — a full scan of `jobs`, and the one measured read that gets
    /// slower purely because the table got bigger.
    JobStatusCounts,
    /// The dataset write path: `Datasets::upsert_stamped` and the batch
    /// upsert's chunk loop, both inside `BEGIN IMMEDIATE`. This is where the
    /// writer lock is actually held.
    DatasetWrite,
    /// Quiet-window maintenance passes (checkpoint / optimize / analyze). They
    /// write their records back into this same instrument, which is what makes
    /// "was that stall at 14:03 us?" answerable.
    Maintenance,
}

impl StoreOp {
    pub const ALL: &'static [StoreOp] = &[
        StoreOp::JobEnqueue,
        StoreOp::JobClaim,
        StoreOp::JobVerdict,
        StoreOp::JobRecovery,
        StoreOp::JobStatusCounts,
        StoreOp::DatasetWrite,
        StoreOp::Maintenance,
    ];

    /// Stable snake_case token. These strings ARE a contract — they are metric
    /// label values an operator's dashboard queries by.
    pub fn as_str(self) -> &'static str {
        match self {
            StoreOp::JobEnqueue => "job_enqueue",
            StoreOp::JobClaim => "job_claim",
            StoreOp::JobVerdict => "job_verdict",
            StoreOp::JobRecovery => "job_recovery",
            StoreOp::JobStatusCounts => "job_status_counts",
            StoreOp::DatasetWrite => "dataset_write",
            StoreOp::Maintenance => "maintenance",
        }
    }

    /// The table this family touches, or `"none"` for a family that touches no
    /// single table.
    ///
    /// This is the join key with the storage-accounting report: an operator who
    /// sees `records` at 1.7 GB and `dataset_write`'s p95 at 40× its baseline is
    /// holding both halves of one finding. `Maintenance` is whole-store work,
    /// so it honestly declares no table rather than picking one.
    pub fn table(self) -> &'static str {
        match self {
            StoreOp::JobEnqueue
            | StoreOp::JobClaim
            | StoreOp::JobVerdict
            | StoreOp::JobRecovery
            | StoreOp::JobStatusCounts => "jobs",
            StoreOp::DatasetWrite => "records",
            StoreOp::Maintenance => "none",
        }
    }

    /// The line above which one operation of this family is **slow**, in
    /// microseconds.
    ///
    /// Server-derived thresholds (100ms, 1s) are deaf to a local store: an
    /// embedded read is microseconds-to-low-milliseconds, so a query that takes
    /// 50ms here is pathological — a missing index, a lock convoy, a checkpoint
    /// storm — while sitting comfortably under any of them. Each line is set an
    /// order of magnitude above the family's healthy p95 on this machine, and
    /// each is *published* as `pumper_store_slow_line_seconds` beside its count,
    /// so "N slow operations" can never be quoted in a conversation where
    /// everyone assumes a different N.
    pub fn slow_line(self, phase: StorePhase) -> Duration {
        match phase {
            // Acquiring a connection from an in-process pool is a lock handoff.
            // Anything past 2ms means the pool is saturated, not that SQLite is
            // slow — a completely different remedy, which is why it is its own
            // key rather than folded into the query below.
            StorePhase::Acquire => Duration::from_millis(2),
            StorePhase::Execute => match self {
                // Indexed point writes and the claim's single UPDATE: single
                // digits, per the technique.
                StoreOp::JobEnqueue | StoreOp::JobClaim | StoreOp::JobVerdict => {
                    Duration::from_millis(5)
                }
                // A bounded sweep and a full `GROUP BY` over `jobs` — both scan,
                // so their honest line is an order of magnitude higher. A
                // uniform 5ms here would cry wolf on healthy work, and a line
                // nobody believes is a line nobody reads.
                StoreOp::JobRecovery | StoreOp::JobStatusCounts => Duration::from_millis(25),
                // A chunk of up to 500 records inside `BEGIN IMMEDIATE`. This
                // one holds the writer lock, so its line is the one whose
                // breaches most directly explain a stall elsewhere.
                StoreOp::DatasetWrite => Duration::from_millis(50),
                // Maintenance is *meant* to take real time; the point of
                // measuring it is the degradation trend (checkpoint durations
                // growing is the escalation evidence), not a per-pass alarm.
                StoreOp::Maintenance => Duration::from_millis(1_000),
            },
        }
    }
}

/// Which half of an operation a record belongs to.
///
/// The split is the technique's, not a convenience: pool acquisition and
/// statement execution have **disjoint remedies**, so a p95 that averaged them
/// would point at neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorePhase {
    /// Waiting for a pooled connection.
    Acquire,
    /// Running the statements on it.
    Execute,
}

impl StorePhase {
    pub const ALL: &'static [StorePhase] = &[StorePhase::Acquire, StorePhase::Execute];

    pub fn as_str(self) -> &'static str {
        match self {
            StorePhase::Acquire => "acquire",
            StorePhase::Execute => "execute",
        }
    }
}

/// How one measured operation ended.
///
/// `Busy` is separated from `Error` because it is the database-specific fact
/// the technique insists on: a p95 driven by lock waits indicts the pool sizing
/// or a writer-hog, not the query plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OpOutcome {
    Ok,
    /// SQLite answered `SQLITE_BUSY` / `SQLITE_LOCKED`, or the pool timed out.
    Busy,
    /// Any other failure.
    Error,
}

impl OpOutcome {
    /// Classifies a failure **by typed predicate**, never by message text — the
    /// discipline `Error::is_terminal_for_job` already holds this repo to.
    pub fn of_error(e: &Error) -> Self {
        if e.is_store_contention() {
            OpOutcome::Busy
        } else {
            OpOutcome::Error
        }
    }
}

/// Total keys: the vocabulary is closed, so this is a compile-time bound.
const KEY_COUNT: usize = 7 * 2;

fn key_index(op: StoreOp, phase: StorePhase) -> usize {
    let o = StoreOp::ALL
        .iter()
        .position(|c| *c == op)
        .expect("StoreOp::ALL enumerates every family");
    o * StorePhase::ALL.len() + phase as usize
}

// ---- the packed record ----------------------------------------------------

/// One measured operation, as it lives in the ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpRecord {
    /// Duration, saturating at `u32::MAX` µs (~71 minutes).
    pub micros: u32,
    /// Rows the statement touched, saturating at 2^24-1. Separates "the query
    /// got slower" from "the table got bigger" — an index fixes one, a
    /// retention policy the other.
    pub rows: u32,
    pub outcome: OpOutcome,
}

/// Largest row count a record can carry; higher values saturate here rather
/// than wrapping into a smaller lie.
pub const ROWS_MAX: u32 = (1 << 24) - 1;

/// Packs a record into one `u64` so a write is a single relaxed store — no
/// lock, no tearing, no allocation. Layout: `micros:32 | rows:24 | outcome:8`.
fn pack(r: OpRecord) -> u64 {
    let outcome = match r.outcome {
        OpOutcome::Ok => 0u64,
        OpOutcome::Busy => 1,
        OpOutcome::Error => 2,
    };
    ((r.micros as u64) << 32) | ((r.rows.min(ROWS_MAX) as u64) << 8) | outcome
}

fn unpack(v: u64) -> OpRecord {
    OpRecord {
        micros: (v >> 32) as u32,
        rows: ((v >> 8) & 0x00ff_ffff) as u32,
        outcome: match v & 0xff {
            0 => OpOutcome::Ok,
            1 => OpOutcome::Busy,
            _ => OpOutcome::Error,
        },
    }
}

// ---- the per-key ring -----------------------------------------------------

/// A fixed window of raw records for one key, plus the lifetime counters that
/// must survive eviction.
///
/// The two are deliberately separate. A wrapped ring answers for *the last N
/// records*, not for "since startup"; computing a lifetime total from it would
/// silently convert a lifetime claim into a window claim the day the ring first
/// wraps — which is exactly the day traffic became interesting.
struct KeyRing {
    slots: [AtomicU64; RING],
    /// Seconds since the instrument was created, per slot. Whole seconds
    /// because `u32` then covers 136 years: a window narrower than a second
    /// reads as `0`, which the surfaces render as "under a second" rather than
    /// as a wrong number.
    stamps: [AtomicU32; RING],
    cursor: AtomicU64,
    total: AtomicU64,
    slow: AtomicU64,
    busy: AtomicU64,
    errors: AtomicU64,
    rows: AtomicU64,
    worst_micros: AtomicU64,
}

impl KeyRing {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
            stamps: std::array::from_fn(|_| AtomicU32::new(0)),
            cursor: AtomicU64::new(0),
            total: AtomicU64::new(0),
            slow: AtomicU64::new(0),
            busy: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            worst_micros: AtomicU64::new(0),
        }
    }
}

// ---- the rate limiter in front of the warn channel ------------------------

/// What the slow-operation warn channel should do about one breach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarnDecision {
    /// Emit the diagnostic.
    Report,
    /// Emit the diagnostic **and** the roll-over summary for the window that
    /// just closed having swallowed `suppressed` breaches.
    ReportWithSummary { suppressed: u64, worst_micros: u32 },
    /// Stay quiet. The count still lives in the counters.
    Suppress,
}

/// One key's budget for the push-mode consumer.
///
/// A rate limiter is the easiest way to build a blindfold, so this one is built
/// the way `worker::ClaimOutage` is: the events are capped, the *count* is not,
/// and when a window rolls over having suppressed anything it says how many and
/// how bad the worst one was. Silent suppression converts "a burst happened"
/// into "nothing happened" — the instrument lying in exactly the moment it
/// exists for. A retry storm's hundredth slow query is noise; the fact that
/// there were a hundred is the finding.
#[derive(Debug, Default)]
pub struct SlowWarn {
    window_started: Option<Instant>,
    suppressed: u64,
    worst_micros: u32,
}

impl SlowWarn {
    /// Records one threshold breach and answers whether it earns a log line.
    /// `now` is passed in so the policy is a pure function under test.
    pub fn breach(&mut self, now: Instant, micros: u32, window: Duration) -> WarnDecision {
        match self.window_started {
            Some(at) if now.duration_since(at) < window => {
                self.suppressed += 1;
                self.worst_micros = self.worst_micros.max(micros);
                WarnDecision::Suppress
            }
            Some(_) => {
                let suppressed = std::mem::take(&mut self.suppressed);
                let worst = std::mem::take(&mut self.worst_micros);
                self.window_started = Some(now);
                if suppressed > 0 {
                    WarnDecision::ReportWithSummary {
                        suppressed,
                        worst_micros: worst,
                    }
                } else {
                    WarnDecision::Report
                }
            }
            None => {
                self.window_started = Some(now);
                WarnDecision::Report
            }
        }
    }
}

// ---- maintenance pass records ---------------------------------------------

/// The maintenance tasks whose passes are recorded — a closed vocabulary, for
/// the same reason [`StoreOp`] is one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceTask {
    /// `PRAGMA wal_checkpoint(PASSIVE)`, chunked.
    WalCheckpoint,
    /// `PRAGMA optimize`.
    Optimize,
    /// `ANALYZE` — the longer rung.
    Analyze,
    /// The always-on hourly janitor (derived state).
    StoreJanitor,
    /// The opt-in retention janitor (accrued value).
    RetentionJanitor,
}

impl MaintenanceTask {
    pub const ALL: &'static [MaintenanceTask] = &[
        MaintenanceTask::WalCheckpoint,
        MaintenanceTask::Optimize,
        MaintenanceTask::Analyze,
        MaintenanceTask::StoreJanitor,
        MaintenanceTask::RetentionJanitor,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            MaintenanceTask::WalCheckpoint => "wal_checkpoint",
            MaintenanceTask::Optimize => "optimize",
            MaintenanceTask::Analyze => "analyze",
            MaintenanceTask::StoreJanitor => "store_janitor",
            MaintenanceTask::RetentionJanitor => "retention_janitor",
        }
    }
}

/// Why a pass ran — which rung of the escalation ladder authorised it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PassTrigger {
    /// Rung 1, the preferred one: the gauge read zero and the minimum interval
    /// had elapsed.
    Quiet,
    /// Rung 2: the pass had been deferred past its staleness bound, so it ran
    /// during "quieter" rather than waiting for perfect quiet — at a reduced
    /// chunk size.
    Stale,
    /// Rung 3: a stated, measurable **harm** bound was breached, so the pass
    /// ran regardless of activity and said so.
    Harm,
}

impl PassTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            PassTrigger::Quiet => "quiet",
            PassTrigger::Stale => "stale",
            PassTrigger::Harm => "harm",
        }
    }
}

/// How a pass ended.
///
/// Three results, never two: a log that records only successes cannot tell a
/// healthy store from a scheduler that has been deferring for a month, and the
/// discovery arrives as a disk-full report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PassOutcome {
    /// It ran. `work == 0` means "ran and found nothing to do" — still a run.
    Ran,
    /// The activity gate said busy and no rung escalated.
    Deferred,
    /// It was attempted and failed.
    Failed,
}

impl PassOutcome {
    pub const ALL: &'static [PassOutcome] =
        &[PassOutcome::Ran, PassOutcome::Deferred, PassOutcome::Failed];

    pub fn as_str(self) -> &'static str {
        match self {
            PassOutcome::Ran => "ran",
            PassOutcome::Deferred => "deferred",
            PassOutcome::Failed => "failed",
        }
    }
}

/// One maintenance pass, as it lands in the flight recorder.
#[derive(Debug, Clone, Serialize)]
pub struct MaintenancePass {
    pub task: MaintenanceTask,
    /// `None` on a deferral — nothing authorised a run.
    pub trigger: Option<PassTrigger>,
    /// The activity gauge at the moment the gate was consulted.
    pub gauge: u64,
    pub duration_ms: u64,
    /// Pages checkpointed, rows pruned — whatever this task counts. Zero is a
    /// legitimate `Ran`.
    pub work: u64,
    pub outcome: PassOutcome,
    /// One short human clause: the harm figure that escalated it, the error
    /// that failed it. Never a place to hide a number a series should carry.
    pub detail: String,
    pub at: DateTime<Utc>,
}

// ---- the instrument -------------------------------------------------------

/// The in-memory instrument. One per process, shared by every store that writes
/// through the measured chokepoint.
pub struct StoreInstrument {
    rings: Vec<KeyRing>,
    warns: Vec<Mutex<SlowWarn>>,
    /// Bounded flight recorder for maintenance passes. A `Mutex` and a `String`
    /// are affordable *here* and nowhere else in this file: a pass happens
    /// roughly once per interval, not once per query, so the hot-path budget
    /// that governs [`StoreInstrument::record`] does not apply.
    maintenance: Mutex<VecDeque<MaintenancePass>>,
    maintenance_counts: Vec<AtomicU64>,
    started: Instant,
}

impl Default for StoreInstrument {
    fn default() -> Self {
        Self::new()
    }
}

impl StoreInstrument {
    pub fn new() -> Self {
        Self {
            rings: (0..KEY_COUNT).map(|_| KeyRing::new()).collect(),
            warns: (0..KEY_COUNT)
                .map(|_| Mutex::new(SlowWarn::default()))
                .collect(),
            maintenance: Mutex::new(VecDeque::with_capacity(MAINTENANCE_LOG)),
            maintenance_counts: (0..MaintenanceTask::ALL.len() * PassOutcome::ALL.len())
                .map(|_| AtomicU64::new(0))
                .collect(),
            started: Instant::now(),
        }
    }

    fn now_secs(&self) -> u32 {
        self.started.elapsed().as_secs().min(u32::MAX as u64) as u32
    }

    /// The write path. Constant-time by construction: one clock read, one
    /// packed store, five relaxed adds. Anything that allocates, formats or
    /// blocks belongs in [`Self::snapshot`] instead.
    pub fn record(
        &self,
        op: StoreOp,
        phase: StorePhase,
        elapsed: Duration,
        rows: u64,
        outcome: OpOutcome,
    ) {
        let k = &self.rings[key_index(op, phase)];
        let micros = elapsed.as_micros().min(u32::MAX as u128) as u32;
        let rec = OpRecord {
            micros,
            rows: rows.min(ROWS_MAX as u64) as u32,
            outcome,
        };
        // `fetch_add` on the cursor is what makes concurrent writers safe
        // without a lock: each gets its own slot index.
        let i = (k.cursor.fetch_add(1, Ordering::Relaxed) as usize) % RING;
        k.stamps[i].store(self.now_secs(), Ordering::Relaxed);
        k.slots[i].store(pack(rec), Ordering::Relaxed);
        k.total.fetch_add(1, Ordering::Relaxed);
        k.rows.fetch_add(rows, Ordering::Relaxed);
        match outcome {
            OpOutcome::Ok => {}
            OpOutcome::Busy => {
                k.busy.fetch_add(1, Ordering::Relaxed);
            }
            OpOutcome::Error => {
                k.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        if elapsed >= op.slow_line(phase) {
            k.slow.fetch_add(1, Ordering::Relaxed);
        }
        k.worst_micros.fetch_max(micros as u64, Ordering::Relaxed);
    }

    /// **The one measured chokepoint.** Acquires a pooled connection, hands it
    /// to `f`, and records both halves under their own keys.
    ///
    /// `f` returns `(value, rows_touched)` because only the call site knows
    /// what "rows" means for its statement — a claim touches one row, a batch
    /// upsert touches its chunk. Reporting `0` there is legal and honest; making
    /// the number up is not.
    ///
    /// Wrapping ~200 statements individually would be a fake total census. This
    /// wraps one function, and the families that pass through it are enumerated
    /// on [`StoreOp`]; everything else is stated as unmeasured.
    pub async fn metered<T, F, Fut>(&self, pool: &sqlx::SqlitePool, op: StoreOp, f: F) -> Result<T>
    where
        F: FnOnce(sqlx::pool::PoolConnection<sqlx::Sqlite>) -> Fut,
        Fut: std::future::Future<Output = MeasuredResult<T>>,
    {
        let started = Instant::now();
        let conn = pool.acquire().await;
        let waited = started.elapsed();
        let conn = match conn {
            Ok(conn) => {
                // Rows touched by an acquisition is not a number that exists;
                // `0` says so rather than inventing one.
                self.record(op, StorePhase::Acquire, waited, 0, OpOutcome::Ok);
                self.warn_if_slow(op, StorePhase::Acquire, waited);
                conn
            }
            Err(e) => {
                let e = Error::from(e);
                self.record(op, StorePhase::Acquire, waited, 0, OpOutcome::of_error(&e));
                self.warn_if_slow(op, StorePhase::Acquire, waited);
                return Err(e);
            }
        };
        let started = Instant::now();
        let result = f(conn).await;
        let took = started.elapsed();
        match result {
            Ok((value, rows)) => {
                self.record(op, StorePhase::Execute, took, rows, OpOutcome::Ok);
                self.warn_if_slow(op, StorePhase::Execute, took);
                Ok(value)
            }
            Err(e) => {
                self.record(op, StorePhase::Execute, took, 0, OpOutcome::of_error(&e));
                self.warn_if_slow(op, StorePhase::Execute, took);
                Err(e)
            }
        }
    }

    /// The push-mode consumer: a rate-limited diagnostic naming key, duration
    /// and threshold. Called only after a breach, so the `Mutex` and the
    /// formatting are paid by the pathological case, never by the healthy one.
    pub fn warn_if_slow(&self, op: StoreOp, phase: StorePhase, elapsed: Duration) {
        let line = op.slow_line(phase);
        if elapsed < line {
            return;
        }
        let micros = elapsed.as_micros().min(u32::MAX as u128) as u32;
        let decision = {
            let mut w = self.warns[key_index(op, phase)]
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            w.breach(Instant::now(), micros, SLOW_WARN_WINDOW)
        };
        match decision {
            WarnDecision::Suppress => {}
            WarnDecision::Report => tracing::warn!(
                op = op.as_str(),
                table = op.table(),
                phase = phase.as_str(),
                micros,
                threshold_micros = line.as_micros() as u64,
                "slow store operation"
            ),
            WarnDecision::ReportWithSummary {
                suppressed,
                worst_micros,
            } => tracing::warn!(
                op = op.as_str(),
                table = op.table(),
                phase = phase.as_str(),
                micros,
                threshold_micros = line.as_micros() as u64,
                suppressed,
                worst_suppressed_micros = worst_micros,
                "slow store operation ({suppressed} further breaches were suppressed in the \
                 last {}s, worst {worst_micros}µs)",
                SLOW_WARN_WINDOW.as_secs()
            ),
        }
    }

    /// True when the most recent connection acquisition on any measured family
    /// waited past its slow line.
    ///
    /// This is the pool-saturation half of the activity picture the maintenance
    /// gate reads: a saturated pool is the strongest possible "not a quiet
    /// window" signal, and it is invisible to a gauge that only counts requests
    /// and jobs.
    pub fn pool_saturated(&self) -> bool {
        StoreOp::ALL.iter().any(|op| {
            let k = &self.rings[key_index(*op, StorePhase::Acquire)];
            let n = k.cursor.load(Ordering::Relaxed);
            if n == 0 {
                return false;
            }
            let last = unpack(k.slots[((n - 1) as usize) % RING].load(Ordering::Relaxed));
            u128::from(last.micros) >= op.slow_line(StorePhase::Acquire).as_micros()
        })
    }

    /// Appends a maintenance pass record and bumps its `(task, outcome)`
    /// counter. Bounded: the oldest record falls out, so the reaper is the
    /// structure itself.
    pub fn record_pass(&self, pass: MaintenancePass) {
        let t = MaintenanceTask::ALL
            .iter()
            .position(|c| *c == pass.task)
            .expect("MaintenanceTask::ALL enumerates every task");
        let o = PassOutcome::ALL
            .iter()
            .position(|c| *c == pass.outcome)
            .expect("PassOutcome::ALL enumerates every outcome");
        self.maintenance_counts[t * PassOutcome::ALL.len() + o].fetch_add(1, Ordering::Relaxed);
        let mut log = self.maintenance.lock().unwrap_or_else(|e| e.into_inner());
        if log.len() == MAINTENANCE_LOG {
            log.pop_front();
        }
        log.push_back(pass);
    }

    /// Lifetime pass count for one `(task, outcome)` pair. Emitted even at
    /// zero — an absent series and a zero series are different answers, and
    /// "0 deferred" is the one an operator most wants to be able to read.
    pub fn pass_count(&self, task: MaintenanceTask, outcome: PassOutcome) -> u64 {
        let t = MaintenanceTask::ALL
            .iter()
            .position(|c| *c == task)
            .expect("enumerated");
        let o = PassOutcome::ALL
            .iter()
            .position(|c| *c == outcome)
            .expect("enumerated");
        self.maintenance_counts[t * PassOutcome::ALL.len() + o].load(Ordering::Relaxed)
    }

    /// The bounded flight recorder, newest last.
    pub fn recent_passes(&self) -> Vec<MaintenancePass> {
        self.maintenance
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    /// Read-time derivation over every key. Sorts a copy of each window; for
    /// rings of a few hundred elements that is microseconds, paid once by the
    /// operator who asked rather than by every operation measured.
    pub fn snapshot(&self) -> Vec<KeyReport> {
        let mut out = Vec::with_capacity(KEY_COUNT);
        for op in StoreOp::ALL {
            for phase in StorePhase::ALL {
                out.push(self.report(*op, *phase));
            }
        }
        out
    }

    fn report(&self, op: StoreOp, phase: StorePhase) -> KeyReport {
        let k = &self.rings[key_index(op, phase)];
        let written = k.cursor.load(Ordering::Relaxed);
        let n = (written as usize).min(RING);
        let mut micros: Vec<u32> = Vec::with_capacity(n);
        let mut oldest = u32::MAX;
        let mut newest = 0u32;
        let mut window_rows = 0u64;
        for i in 0..n {
            let rec = unpack(k.slots[i].load(Ordering::Relaxed));
            micros.push(rec.micros);
            window_rows += rec.rows as u64;
            let at = k.stamps[i].load(Ordering::Relaxed);
            oldest = oldest.min(at);
            newest = newest.max(at);
        }
        micros.sort_unstable();
        KeyReport {
            op,
            phase,
            table: op.table(),
            samples: n as u64,
            lifetime: k.total.load(Ordering::Relaxed),
            wrapped: written > RING as u64,
            window_secs: if n == 0 { 0 } else { (newest - oldest) as u64 },
            window_rows,
            p50_micros: nearest_rank(&micros, 50),
            p95_micros: nearest_rank(&micros, 95),
            slow_line_micros: op.slow_line(phase).as_micros() as u64,
            slow_lifetime: k.slow.load(Ordering::Relaxed),
            busy_lifetime: k.busy.load(Ordering::Relaxed),
            errors_lifetime: k.errors.load(Ordering::Relaxed),
            rows_lifetime: k.rows.load(Ordering::Relaxed),
            worst_micros: k.worst_micros.load(Ordering::Relaxed),
        }
    }
}

/// Nearest-rank percentile over an ascending window: the returned value is an
/// **observed** sample, never an interpolated fiction between two of them. One
/// method, stated once, used by every surface — two panels computing "p95" two
/// different ways is a vocabulary split wearing numbers.
///
/// An empty window has no percentile, so it answers `0` and every surface
/// renders `samples` beside the figure: a p95 over 7 samples is the 7th sample.
pub fn nearest_rank(sorted_micros: &[u32], percentile: u32) -> u64 {
    if sorted_micros.is_empty() {
        return 0;
    }
    let n = sorted_micros.len();
    // ceil(p/100 * n), 1-based, clamped into range.
    let rank = (percentile as usize * n).div_ceil(100);
    sorted_micros[rank.clamp(1, n) - 1] as u64
}

/// One key's derived statistics. **Every figure names its recomputation** in
/// its doc comment, so a number quoted out of this struct can be re-derived,
/// re-questioned or extended rather than believed.
#[derive(Debug, Clone, Serialize)]
pub struct KeyReport {
    pub op: StoreOp,
    pub phase: StorePhase,
    /// The table this family touches — the join key with the accounting report.
    pub table: &'static str,
    /// Records currently in the window: `min(lifetime, 256)`. Rendered beside
    /// every percentile, because a p95 over 7 samples is the 7th sample.
    pub samples: u64,
    /// Operations since process start. A **lifetime** claim: a separate
    /// monotonic counter, not something derived from the window.
    pub lifetime: u64,
    /// Whether the ring has wrapped, i.e. whether `samples < lifetime`.
    pub wrapped: bool,
    /// Seconds between the oldest and newest surviving record. `0` means the
    /// whole window happened inside one second, not that there is no window —
    /// read it with `samples`.
    pub window_secs: u64,
    /// Rows touched **within the window** — the companion to `window_secs`, and
    /// deliberately not the lifetime figure below.
    pub window_rows: u64,
    /// Nearest-rank median: sort the window ascending, take element
    /// `ceil(0.50·n)`.
    pub p50_micros: u64,
    /// Nearest-rank p95: sort the window ascending, take element
    /// `ceil(0.95·n)`.
    pub p95_micros: u64,
    /// The predicate that gives `slow_lifetime` its meaning.
    pub slow_line_micros: u64,
    /// Lifetime count of operations at or past `slow_line_micros`.
    pub slow_lifetime: u64,
    /// Lifetime count of `SQLITE_BUSY` / `SQLITE_LOCKED` / pool timeouts.
    pub busy_lifetime: u64,
    /// Lifetime count of every other failure.
    pub errors_lifetime: u64,
    /// Lifetime sum of rows touched.
    pub rows_lifetime: u64,
    /// Worst single duration since process start — a lifetime fact, so it does
    /// not evaporate when the ring wraps.
    pub worst_micros: u64,
}

// ---- what the store's own files cost --------------------------------------

/// The database's size on disk, measured rather than estimated.
///
/// Three numbers, because they answer three different questions and conflating
/// them makes the report unable to answer its own follow-up: `main_bytes` is
/// what the file occupies, `free_bytes` is how much of that is recycled-but-not
/// -returned space (so "will pruning shrink the file?" has an answer), and
/// `wal_bytes` is the write-ahead sidecar — a permanent resident under WAL, and
/// the one that grows without bound if checkpoints stop happening.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct StoreSize {
    pub page_size: u64,
    pub page_count: u64,
    pub freelist_pages: u64,
    /// `page_count × page_size` — pages allocated, not bytes in live rows.
    pub main_bytes: u64,
    /// `freelist_pages × page_size`.
    pub free_bytes: u64,
    /// The `-wal` sidecar's size on disk, or `0` when there is none.
    pub wal_bytes: u64,
}

/// The `Result` alias the measured chokepoint speaks in.
pub type MeasuredResult<T> = Result<(T, u64)>;

#[cfg(test)]
mod tests {
    use super::*;

    /// The keying rule, made structural: a family with no token, no table
    /// decision, or a token that collides with another's would key two logical
    /// hot paths into one ring — the exact failure per-statement keying causes
    /// from the other direction.
    #[test]
    fn the_operation_vocabulary_is_closed_and_every_family_names_its_table() {
        let mut tokens: Vec<&str> = StoreOp::ALL.iter().map(|o| o.as_str()).collect();
        tokens.sort_unstable();
        let mut unique = tokens.clone();
        unique.dedup();
        assert_eq!(tokens, unique, "two families share a token: {tokens:?}");
        for op in StoreOp::ALL {
            assert!(!op.as_str().is_empty() && op.as_str().is_ascii());
            assert!(!op.table().is_empty(), "{op:?} names no table");
        }
        assert_eq!(
            StoreOp::ALL.len() * StorePhase::ALL.len(),
            KEY_COUNT,
            "KEY_COUNT must equal the closed key space, or the ring vector is mis-sized"
        );
    }

    /// Every key must be reachable and distinct, or two families quietly share
    /// a ring.
    #[test]
    fn every_key_maps_to_its_own_ring() {
        let mut seen = std::collections::HashSet::new();
        for op in StoreOp::ALL {
            for phase in StorePhase::ALL {
                let i = key_index(*op, *phase);
                assert!(i < KEY_COUNT, "{op:?}/{phase:?} indexes out of range");
                assert!(seen.insert(i), "{op:?}/{phase:?} collides on ring {i}");
            }
        }
        assert_eq!(seen.len(), KEY_COUNT);
    }

    /// "Slow" for a local store is single-digit milliseconds. A server-derived
    /// threshold (100ms, 1s) would sit above every pathology this instrument
    /// exists to catch, so the point-operation lines are held to single digits
    /// and the scanning families are capped an order of magnitude above.
    #[test]
    fn slow_lines_are_local_store_lines_not_networked_ones() {
        for op in StoreOp::ALL {
            let acquire = op.slow_line(StorePhase::Acquire);
            assert!(
                acquire <= Duration::from_millis(2),
                "{op:?} acquire line {acquire:?} is a network threshold, not a pool one"
            );
        }
        for op in [StoreOp::JobEnqueue, StoreOp::JobClaim, StoreOp::JobVerdict] {
            let line = op.slow_line(StorePhase::Execute);
            assert!(
                line < Duration::from_millis(10),
                "{op:?}'s line {line:?} is not single-digit milliseconds"
            );
        }
        // And the scanning families are allowed more — but never a networked
        // 100ms for anything but the batch write and maintenance.
        assert_eq!(
            StoreOp::JobStatusCounts.slow_line(StorePhase::Execute),
            Duration::from_millis(25)
        );
    }

    /// The record survives its round trip through the packed `u64`, including
    /// the two facts the technique insists on beyond duration: rows touched and
    /// the busy outcome.
    #[test]
    fn a_packed_record_keeps_its_rows_and_its_busy_flag() {
        for outcome in [OpOutcome::Ok, OpOutcome::Busy, OpOutcome::Error] {
            let r = OpRecord {
                micros: 1_234_567,
                rows: 4_096,
                outcome,
            };
            assert_eq!(unpack(pack(r)), r);
        }
        // Saturation, not wraparound: a batch bigger than the field reports the
        // ceiling rather than a smaller lie.
        let huge = OpRecord {
            micros: u32::MAX,
            rows: u32::MAX,
            outcome: OpOutcome::Ok,
        };
        assert_eq!(unpack(pack(huge)).rows, ROWS_MAX);
        assert_eq!(unpack(pack(huge)).micros, u32::MAX);
    }

    /// The instrument must not fold pool waits into query time: the two have
    /// disjoint remedies (pool sizing versus an index), and an averaged p95
    /// points at neither.
    #[test]
    fn pool_wait_is_a_separate_key_from_query_time() {
        let inst = StoreInstrument::new();
        inst.record(
            StoreOp::JobClaim,
            StorePhase::Acquire,
            Duration::from_millis(80),
            0,
            OpOutcome::Ok,
        );
        inst.record(
            StoreOp::JobClaim,
            StorePhase::Execute,
            Duration::from_micros(300),
            1,
            OpOutcome::Ok,
        );
        let snap = inst.snapshot();
        let acquire = snap
            .iter()
            .find(|r| r.op == StoreOp::JobClaim && r.phase == StorePhase::Acquire)
            .expect("acquire key");
        let execute = snap
            .iter()
            .find(|r| r.op == StoreOp::JobClaim && r.phase == StorePhase::Execute)
            .expect("execute key");
        assert_eq!(acquire.p95_micros, 80_000);
        assert_eq!(execute.p95_micros, 300);
        assert_eq!(acquire.slow_lifetime, 1, "80ms is a saturated pool");
        assert_eq!(execute.slow_lifetime, 0, "300µs is a healthy claim");
    }

    /// A wrapped ring answers for the last N records, never "since startup".
    /// Deriving a lifetime total from it would convert a lifetime claim into a
    /// window claim the day the ring first wraps.
    #[test]
    fn a_wrapped_window_does_not_impersonate_a_lifetime_total() {
        let inst = StoreInstrument::new();
        for i in 0..(RING + 50) {
            inst.record(
                StoreOp::JobEnqueue,
                StorePhase::Execute,
                Duration::from_micros(i as u64),
                1,
                OpOutcome::Ok,
            );
        }
        let r = inst
            .snapshot()
            .into_iter()
            .find(|r| r.op == StoreOp::JobEnqueue && r.phase == StorePhase::Execute)
            .expect("key");
        assert_eq!(r.samples, RING as u64);
        assert_eq!(r.lifetime, (RING + 50) as u64);
        assert!(r.wrapped);
        assert_eq!(r.rows_lifetime, (RING + 50) as u64);
        assert_eq!(
            r.window_rows, RING as u64,
            "the window's rows, not lifetime"
        );
    }

    /// Before the ring fills, n < N — derive over what exists and disclose the
    /// n. A p95 over 7 samples is the 7th sample.
    #[test]
    fn a_short_window_discloses_its_n_and_returns_an_observed_sample() {
        let sorted: Vec<u32> = (1..=7).collect();
        assert_eq!(nearest_rank(&sorted, 95), 7);
        assert_eq!(nearest_rank(&sorted, 50), 4);
        assert_eq!(nearest_rank(&[], 95), 0);
        // Nearest-rank returns an observed value, never an interpolation.
        let two = [10u32, 20];
        assert!(matches!(nearest_rank(&two, 95), 10 | 20));
    }

    /// The rate limiter must not become a blindfold: when a window rolls over
    /// having swallowed breaches, the next line carries how many and how bad
    /// the worst was. Silent suppression turns "a burst happened" into "nothing
    /// happened".
    #[test]
    fn a_suppressed_burst_is_summarised_not_erased() {
        let mut w = SlowWarn::default();
        let t0 = Instant::now();
        let window = Duration::from_secs(60);
        assert_eq!(w.breach(t0, 9_000, window), WarnDecision::Report);
        for micros in [11_000, 40_000, 12_000] {
            assert_eq!(
                w.breach(t0 + Duration::from_secs(1), micros, window),
                WarnDecision::Suppress
            );
        }
        assert_eq!(
            w.breach(t0 + Duration::from_secs(61), 8_000, window),
            WarnDecision::ReportWithSummary {
                suppressed: 3,
                worst_micros: 40_000,
            }
        );
        // The next window starts clean: a summary that repeated itself would
        // double-count the same burst.
        assert_eq!(
            w.breach(t0 + Duration::from_secs(200), 8_000, window),
            WarnDecision::Report
        );
    }

    /// The count survives the limiter. Every breach is counted whether or not
    /// it was logged — the events are capped, the count is not.
    #[test]
    fn every_breach_is_counted_even_when_its_log_line_is_suppressed() {
        let inst = StoreInstrument::new();
        for _ in 0..50 {
            inst.record(
                StoreOp::JobClaim,
                StorePhase::Execute,
                Duration::from_millis(30),
                1,
                OpOutcome::Ok,
            );
            inst.warn_if_slow(
                StoreOp::JobClaim,
                StorePhase::Execute,
                Duration::from_millis(30),
            );
        }
        let r = inst
            .snapshot()
            .into_iter()
            .find(|r| r.op == StoreOp::JobClaim && r.phase == StorePhase::Execute)
            .expect("key");
        assert_eq!(r.slow_lifetime, 50);
        assert_eq!(r.slow_line_micros, 5_000, "the count carries its predicate");
    }

    /// A saturated pool is the strongest "not a quiet window" signal there is,
    /// and it is invisible to a gauge that only counts requests and jobs.
    #[test]
    fn a_saturated_pool_reads_as_busy_for_the_maintenance_gate() {
        let inst = StoreInstrument::new();
        assert!(
            !inst.pool_saturated(),
            "a fresh instrument is not saturated"
        );
        inst.record(
            StoreOp::DatasetWrite,
            StorePhase::Acquire,
            Duration::from_micros(40),
            0,
            OpOutcome::Ok,
        );
        assert!(!inst.pool_saturated(), "40µs is a healthy handoff");
        inst.record(
            StoreOp::DatasetWrite,
            StorePhase::Acquire,
            Duration::from_millis(60),
            0,
            OpOutcome::Ok,
        );
        assert!(inst.pool_saturated(), "60µs*1000 of waiting is contention");
    }

    /// Deferral is an outcome. All three must be counted separately, or a
    /// scheduler that has deferred for a month reads exactly like a healthy
    /// store with nothing to do.
    #[test]
    fn ran_deferred_and_failed_are_three_different_answers() {
        let inst = StoreInstrument::new();
        for (outcome, n) in [
            (PassOutcome::Ran, 2),
            (PassOutcome::Deferred, 5),
            (PassOutcome::Failed, 1),
        ] {
            for _ in 0..n {
                inst.record_pass(MaintenancePass {
                    task: MaintenanceTask::WalCheckpoint,
                    trigger: (outcome == PassOutcome::Ran).then_some(PassTrigger::Quiet),
                    gauge: 0,
                    duration_ms: 1,
                    work: 0,
                    outcome,
                    detail: String::new(),
                    at: Utc::now(),
                });
            }
        }
        assert_eq!(
            inst.pass_count(MaintenanceTask::WalCheckpoint, PassOutcome::Ran),
            2
        );
        assert_eq!(
            inst.pass_count(MaintenanceTask::WalCheckpoint, PassOutcome::Deferred),
            5
        );
        assert_eq!(
            inst.pass_count(MaintenanceTask::WalCheckpoint, PassOutcome::Failed),
            1
        );
        // A task that never ran reads zero, not absent.
        assert_eq!(
            inst.pass_count(MaintenanceTask::Analyze, PassOutcome::Ran),
            0
        );
    }

    /// The flight recorder is bounded by construction: the write pointer is the
    /// reaper, so nothing has to remember to trim it.
    #[test]
    fn the_maintenance_log_is_bounded_by_its_own_write_pointer() {
        let inst = StoreInstrument::new();
        for i in 0..(MAINTENANCE_LOG + 20) {
            inst.record_pass(MaintenancePass {
                task: MaintenanceTask::Optimize,
                trigger: Some(PassTrigger::Quiet),
                gauge: 0,
                duration_ms: i as u64,
                work: 0,
                outcome: PassOutcome::Ran,
                detail: String::new(),
                at: Utc::now(),
            });
        }
        let recent = inst.recent_passes();
        assert_eq!(recent.len(), MAINTENANCE_LOG);
        assert_eq!(
            recent.last().expect("newest").duration_ms,
            (MAINTENANCE_LOG + 19) as u64,
            "newest last"
        );
        // The lifetime counter is NOT the window: it outlives eviction.
        assert_eq!(
            inst.pass_count(MaintenanceTask::Optimize, PassOutcome::Ran),
            (MAINTENANCE_LOG + 20) as u64
        );
    }

    /// The maintenance vocabularies are closed too, and their tokens are metric
    /// label values — a collision would merge two tasks' histories.
    #[test]
    fn the_maintenance_vocabulary_is_closed() {
        let mut tokens: Vec<&str> = MaintenanceTask::ALL.iter().map(|t| t.as_str()).collect();
        tokens.sort_unstable();
        let mut unique = tokens.clone();
        unique.dedup();
        assert_eq!(tokens, unique);
        assert_eq!(PassOutcome::ALL.len(), 3, "three outcomes, never two");
        for t in MaintenanceTask::ALL {
            for o in PassOutcome::ALL {
                // Every pair must be addressable, or a series is unreachable.
                assert_eq!(StoreInstrument::new().pass_count(*t, *o), 0);
            }
        }
    }
}
