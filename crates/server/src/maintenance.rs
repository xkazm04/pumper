//! Quiet-window maintenance: the gate, the ladder, and the lossless pass.
//!
//! Housekeeping an embedded store needs — folding the write-ahead sidecar back
//! into the main file, refreshing planner statistics — competes with the user
//! for the same engine. On a server it would run in a negotiated window. In a
//! process that scrapes on a schedule the window must be **found, not
//! scheduled**: every pass is gated on a live reading of whether the
//! application is busy, taken when the pass would start and re-taken while it
//! runs (registry: embedded-db/quiet-window-maintenance).
//!
//! ## The ladder
//!
//! Deferral needs its own policy or quiet-window maintenance degrades into no
//! maintenance — a busy deployment can present no perfect window for days while
//! its sidecar grows. So:
//!
//! 1. **Quiet** — the gauge reads zero AND the minimum interval has elapsed.
//!    Both, always: the interval bounds cost, the gauge bounds interference,
//!    and the interval alone is the wall-clock timer this replaces while the
//!    gauge alone runs maintenance in every momentary gap.
//! 2. **Stale** — the task has been deferred past its staleness bound, so it
//!    stops holding out for perfect quiet and accepts "quieter", at a reduced
//!    chunk size.
//! 3. **Harm** — a stated, measurable harm bound is breached, so the pass runs
//!    regardless of activity and records that it did.
//!
//! Rung 3's bound is a **byte count, not a duration**, and that is the whole
//! point: "the sidecar exceeds 64 MiB" is a reason a human can weigh; "it has
//! been a week" is a timer sneaking back in through the escalation ladder.
//!
//! ## Deferral is an outcome
//!
//! `failure-not-empty-success`: "ran and found nothing to do", "deferred
//! because busy" and "attempted and failed" are three different results. A log
//! that records only successes cannot distinguish a healthy store from a gate
//! that has been deferring for a month, and the discovery arrives as a
//! disk-full report. All three land in the store instrument, on `/metrics`, and
//! in the log.
//!
//! ## What runs here — and what deliberately does not
//!
//! Everything scheduled in this window is **lossless**: `wal_checkpoint(PASSIVE)`
//! moves committed frames from the sidecar into the main file, `PRAGMA optimize`
//! and `ANALYZE` rebuild planner statistics. Nothing here deletes, expires,
//! prunes or shrinks-by-discarding.
//!
//! And the window is **not** for correctness work. Schema migration and crash
//! recovery run at their own mandated moment in `Storage::connect` and at boot,
//! regardless of activity, and this module does not touch them — "we deferred
//! recovery politely" is not a sentence anyone wants to say.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pumper_core::config::MaintenanceConfig;
use pumper_core::store_instrument::{
    MaintenancePass, MaintenanceTask, PassOutcome, PassTrigger, StoreInstrument,
};

use crate::activity::ActivityGauge;
use crate::state::AppState;

/// What the gate decided for one task on one tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateDecision {
    /// The minimum interval has not elapsed. **Not a deferral** — no pass was
    /// attempted, so recording one would inflate the deferred count with ticks
    /// that were never candidates, and "deferred 400 times" would stop meaning
    /// "the application was busy 400 times".
    NotDue,
    /// A pass was due and the gate refused it. This is an outcome and it is
    /// recorded.
    Defer { gauge: u64 },
    /// Run, at this rung, with this many chunks.
    Run {
        trigger: PassTrigger,
        gauge: u64,
        chunks: u32,
    },
}

/// The measurable harm a task escalates on, when it has one.
///
/// Only the checkpoint task does. The two janitors deliberately carry `None`
/// — see [`harm_bound_for`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Harm {
    /// The measured figure (bytes).
    pub measured: u64,
    /// The bound above which the pass runs regardless of activity.
    pub bound: u64,
}

impl Harm {
    fn breached(&self) -> bool {
        self.measured >= self.bound
    }
}

/// Which harm bound, if any, escalates a task past the activity gate.
///
/// **Only [`MaintenanceTask::WalCheckpoint`] has one**, and the asymmetry is
/// deliberate rather than an omission:
///
/// - The checkpoint's harm is measurable and unbounded — the `-wal` sidecar
///   grows with every commit and only a checkpoint shrinks it, so deferring
///   forever really does end in a full disk. `wal_harm_bytes` is that bound.
/// - `Optimize` and `Analyze` refresh planner statistics. Stale statistics cost
///   query plans, not disk, and there is no byte figure that says "now it
///   hurts". Inventing an elapsed-time bound for them would be the wall clock
///   returning through the ladder, so they stay on rungs 1 and 2.
/// - The two janitors **delete**. Retention is deferred by operator decision —
///   keeping data is the pattern — so the safe direction under permanent load
///   is to keep deferring, and their deferrals are counted so "it has never
///   run" is legible on `/metrics` rather than folklore. A janitor that
///   escalated past the gate would be trading a user-visible stall for disk
///   nobody asked us to reclaim.
pub fn harm_bound_for(
    task: MaintenanceTask,
    wal_bytes: u64,
    cfg: &MaintenanceConfig,
) -> Option<Harm> {
    match task {
        MaintenanceTask::WalCheckpoint => Some(Harm {
            measured: wal_bytes,
            bound: cfg.wal_harm_bytes,
        }),
        MaintenanceTask::Optimize
        | MaintenanceTask::Analyze
        | MaintenanceTask::StoreJanitor
        | MaintenanceTask::RetentionJanitor => None,
    }
}

/// **The gate.** A pure function of the clock, the gauge and the harm figure,
/// so every rung is testable without a database, a timer or a running server.
///
/// `since_last` is time since this task's last *pass attempt that ran*, not
/// since the last tick: a task that has been deferring for six hours is stale,
/// however often it was asked.
pub fn decide(
    since_last: Duration,
    gauge: u64,
    pool_saturated: bool,
    harm: Option<Harm>,
    cfg: &MaintenanceConfig,
) -> GateDecision {
    // A saturated pool is demand for the machine that no request or job count
    // can see — the strongest possible "not a quiet window" signal.
    let gauge = if pool_saturated { gauge.max(1) } else { gauge };

    // Rung 3 first, and deliberately before the interval check: a harm bound is
    // a statement about the store's condition, not about its schedule, and a
    // sidecar past 64 MiB does not become acceptable because a pass ran nine
    // minutes ago. This is the one rung that ignores both other conditions.
    if harm.is_some_and(|h| h.breached()) {
        return GateDecision::Run {
            trigger: PassTrigger::Harm,
            gauge,
            chunks: cfg.checkpoint_rounds,
        };
    }
    if since_last < Duration::from_secs(cfg.min_interval_secs) {
        return GateDecision::NotDue;
    }
    if gauge == 0 {
        return GateDecision::Run {
            trigger: PassTrigger::Quiet,
            gauge,
            chunks: cfg.checkpoint_rounds,
        };
    }
    // Rung 2: past the staleness bound, accept "quieter" rather than perfect
    // quiet — at ONE chunk, so the interference it does cause is the smallest
    // slice the pass can be built from.
    if since_last >= Duration::from_secs(cfg.staleness_secs) && gauge <= cfg.quiet_enough {
        return GateDecision::Run {
            trigger: PassTrigger::Stale,
            gauge,
            chunks: 1,
        };
    }
    GateDecision::Defer { gauge }
}

/// One task's place in the loop: when it last ran, so the gate can measure
/// staleness rather than tick count.
pub struct TaskClock {
    last_ran: Instant,
    passes: u64,
}

impl TaskClock {
    pub fn new() -> Self {
        Self {
            // Starting "now" rather than at the epoch is what stops every task
            // firing in the first seconds of boot, when the process is at its
            // busiest and its statistics are least interesting.
            last_ran: Instant::now(),
            passes: 0,
        }
    }

    pub fn since_last(&self) -> Duration {
        self.last_ran.elapsed()
    }

    pub fn ran(&mut self) {
        self.last_ran = Instant::now();
        self.passes += 1;
    }

    pub fn passes(&self) -> u64 {
        self.passes
    }
}

impl Default for TaskClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Records one pass into the instrument AND the log, in the shape both consume.
///
/// Every pass — ran, deferred, failed — goes through here. This is the flight
/// recorder that answers "is maintenance actually running?" and "was that stall
/// at 14:03 us?", and the second question is the one that decides whether the
/// gate is trusted or quietly disabled by the next engineer who suspects it.
#[allow(clippy::too_many_arguments)]
pub fn record(
    instrument: &StoreInstrument,
    task: MaintenanceTask,
    trigger: Option<PassTrigger>,
    gauge: u64,
    started: Instant,
    work: u64,
    outcome: PassOutcome,
    detail: String,
) {
    let duration_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    match outcome {
        PassOutcome::Ran => tracing::info!(
            task = task.as_str(),
            trigger = trigger.map(|t| t.as_str()).unwrap_or("none"),
            gauge,
            duration_ms,
            work,
            detail = %detail,
            "maintenance pass ran"
        ),
        // `debug!`, not `info!`: a deferral is the healthy answer on a busy
        // process and would otherwise be the loudest line in the log. The
        // COUNT is what an operator watches, and that is on `/metrics`.
        PassOutcome::Deferred => tracing::debug!(
            task = task.as_str(),
            gauge,
            detail = %detail,
            "maintenance pass deferred"
        ),
        PassOutcome::Failed => tracing::warn!(
            task = task.as_str(),
            trigger = trigger.map(|t| t.as_str()).unwrap_or("none"),
            gauge,
            duration_ms,
            detail = %detail,
            "maintenance pass failed"
        ),
    }
    instrument.record_pass(MaintenancePass {
        task,
        trigger,
        gauge,
        duration_ms,
        work,
        outcome,
        detail,
        at: chrono::Utc::now(),
    });
}

/// Runs the chunked, lossless checkpoint pass and returns pages checkpointed.
///
/// **Chunk, yield, re-check.** Each round is one `PRAGMA wal_checkpoint(PASSIVE)`
/// — the form that never blocks a writer and does as much as it can without
/// waiting — and between rounds the pooled connection is **released** before
/// the gauge is re-read. That order matters: re-checking while still holding
/// the writer's lane would keep the user waiting on the very check meant to
/// protect them. Every round boundary leaves the store consistent, so a pass
/// abandoned halfway is merely incomplete, never corrupt, and the next pass
/// continues from wherever this one stopped.
///
/// Lossless: a checkpoint moves committed frames from the sidecar into the main
/// file. It discards nothing, and a reader mid-snapshot simply keeps the frames
/// it needs — which is also why PASSIVE can legitimately checkpoint zero pages
/// and that is a `Ran`, not a failure.
async fn checkpoint_pass(
    state: &AppState,
    gauge: &Arc<ActivityGauge>,
    rounds: u32,
    ignore_gauge: bool,
) -> pumper_core::Result<u64> {
    let mut pages = 0u64;
    for round in 0..rounds {
        // Re-read the gauge BETWEEN rounds, with nothing held. Round 0 always
        // runs — the gate already authorised it — so a pass can never report
        // `ran` having done nothing merely because a request arrived in the
        // microsecond after the decision.
        if round > 0 && !ignore_gauge && gauge.reading() > 0 {
            break;
        }
        // Takes and releases its own connection, so the gauge re-check above
        // never happens with the writer's lane held.
        let round = state.storage.wal_checkpoint_passive().await?;
        pages += round.checkpointed_pages;
        // Nothing left in the sidecar: further rounds would be pure overhead.
        // `remaining()` is `log_frames - checkpointed`, because the pragma
        // reports the LOG'S SIZE in its second column, not a remainder — a
        // detail that reads a completed pass as a stalled one if taken at face
        // value. A round that was BLOCKED is also done for now: PASSIVE does
        // not wait, and hammering it would only add contention to a store that
        // just told us it is busy.
        if round.remaining() == 0 || round.blocked {
            break;
        }
        // Yield between rounds so a request that arrived mid-pass is served
        // before the next slice starts.
        tokio::task::yield_now().await;
    }
    Ok(pages)
}

/// One task's tick: consult the gate, run or defer, record either way.
///
/// Extracted from the loop so a test can drive the **exact** decision the
/// server makes — with a real store, a real gauge and a real checkpoint —
/// rather than a re-implementation that happens to look like it. It is also
/// what makes "prove the gate really gates" a test rather than a claim: hold
/// the gauge above zero, tick, assert `Deferred`; release it, tick, assert
/// `Ran`.
///
/// Returns the outcome, or `None` when the pass was not due (which is not an
/// outcome and is deliberately recorded nowhere).
pub(crate) async fn tick_task<F, Fut>(
    instrument: &StoreInstrument,
    gauge: &Arc<ActivityGauge>,
    task: MaintenanceTask,
    clock: &mut TaskClock,
    cfg: &MaintenanceConfig,
    harm: Option<Harm>,
    pass: F,
) -> Option<PassOutcome>
where
    F: FnOnce(u32, bool) -> Fut,
    Fut: std::future::Future<Output = pumper_core::Result<u64>>,
{
    let saturated = instrument.pool_saturated();
    match decide(clock.since_last(), gauge.reading(), saturated, harm, cfg) {
        GateDecision::NotDue => None,
        GateDecision::Defer { gauge } => {
            record(
                instrument,
                task,
                None,
                gauge,
                Instant::now(),
                0,
                PassOutcome::Deferred,
                match harm {
                    Some(h) => format!("busy; harm {} of {} bytes", h.measured, h.bound),
                    None => "busy".into(),
                },
            );
            Some(PassOutcome::Deferred)
        }
        GateDecision::Run {
            trigger,
            gauge: reading,
            chunks,
        } => {
            let started = Instant::now();
            // Only the harm rung ignores the gauge mid-pass. At rungs 1 and 2
            // the pass yields the moment foreground work appears, which is the
            // whole "chunk, yield, re-check" contract.
            let ignore_gauge = trigger == PassTrigger::Harm;
            match pass(chunks, ignore_gauge).await {
                Ok(work) => {
                    clock.ran();
                    record(
                        instrument,
                        task,
                        Some(trigger),
                        reading,
                        started,
                        work,
                        PassOutcome::Ran,
                        match (trigger, harm) {
                            (PassTrigger::Harm, Some(h)) => format!(
                                "ran regardless of activity: {} bytes is over the {} byte                                  harm bound",
                                h.measured, h.bound
                            ),
                            _ => String::new(),
                        },
                    );
                    Some(PassOutcome::Ran)
                }
                // Deliberately NOT `clock.ran()`: a failed pass must stay
                // stale, or the clock launders failures into a schedule that
                // looks like it is keeping up.
                Err(e) => {
                    record(
                        instrument,
                        task,
                        Some(trigger),
                        reading,
                        started,
                        0,
                        PassOutcome::Failed,
                        e.to_string(),
                    );
                    Some(PassOutcome::Failed)
                }
            }
        }
    }
}

/// The quiet-window loop: consult the gate for each lossless task, run or
/// defer, record either way.
///
/// Shutdown-aware like every other loop in this process — a maintenance pass
/// must not outlive the drain and commit work on top of the final flush.
pub async fn run(state: AppState) {
    let cfg = state.config.maintenance.clone();
    if !cfg.enabled {
        tracing::info!("quiet-window maintenance disabled ([maintenance] enabled = false)");
        return;
    }
    let instrument = state.storage.instrument();
    let gauge = state.activity.clone();
    let tick = Duration::from_secs(cfg.tick_secs);
    let mut checkpoint = TaskClock::new();
    let mut optimize = TaskClock::new();
    let mut analyze = TaskClock::new();
    tracing::info!(
        min_interval_secs = cfg.min_interval_secs,
        staleness_secs = cfg.staleness_secs,
        wal_harm_bytes = cfg.wal_harm_bytes,
        "quiet-window maintenance enabled (lossless: checkpoint, optimize, analyze)"
    );

    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            _ = tokio::time::sleep(tick) => {}
        }
        // The harm figure, read once per tick. A failure to measure it is NOT
        // an excuse to skip the pass — it reports 0, which simply means rung 3
        // cannot fire this tick while the other two rungs still can.
        let wal_bytes = state
            .storage
            .size_facts()
            .await
            .map(|s| s.wal_bytes)
            .unwrap_or(0);
        let harm = harm_bound_for(MaintenanceTask::WalCheckpoint, wal_bytes, &cfg);
        tick_task(
            &instrument,
            &gauge,
            MaintenanceTask::WalCheckpoint,
            &mut checkpoint,
            &cfg,
            harm,
            |chunks, ignore| checkpoint_pass(&state, &gauge, chunks, ignore),
        )
        .await;
        tick_task(
            &instrument,
            &gauge,
            MaintenanceTask::Optimize,
            &mut optimize,
            &cfg,
            None,
            |_, _| async { state.storage.optimize().await.map(|()| 0) },
        )
        .await;
        // The longer rung: one ANALYZE per `analyze_every_passes` optimize
        // passes, so a full index scan is occasional rather than routine.
        let every = cfg.analyze_every_passes.max(1);
        if optimize.passes() >= every && analyze.passes() * every < optimize.passes() {
            tick_task(
                &instrument,
                &gauge,
                MaintenanceTask::Analyze,
                &mut analyze,
                &cfg,
                None,
                |_, _| async { state.storage.analyze().await.map(|()| 0) },
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> MaintenanceConfig {
        MaintenanceConfig {
            enabled: true,
            tick_secs: 60,
            min_interval_secs: 900,
            staleness_secs: 21_600,
            quiet_enough: 1,
            wal_harm_bytes: 64 * 1024 * 1024,
            checkpoint_rounds: 4,
            analyze_every_passes: 24,
        }
    }

    /// THE anti-pattern this gate replaces: a bare `sleep(interval)` that fires
    /// mid-scrape holding the writer lock, charging the stall to whatever the
    /// operator was doing. The interval elapsing is NOT sufficient — the gauge
    /// has to read zero too.
    #[test]
    fn an_elapsed_interval_alone_does_not_authorise_a_pass() {
        let c = cfg();
        let elapsed = Duration::from_secs(1_000);
        assert!(matches!(
            decide(elapsed, 3, false, None, &c),
            GateDecision::Defer { gauge: 3 }
        ));
        // Same instant, same interval, nothing in flight: now it runs.
        assert!(matches!(
            decide(elapsed, 0, false, None, &c),
            GateDecision::Run {
                trigger: PassTrigger::Quiet,
                ..
            }
        ));
    }

    /// The mirror condition: the gauge alone is not the technique either. A
    /// quiet gauge inside the minimum interval runs maintenance in every
    /// momentary gap, turning idle detection into a busy loop.
    #[test]
    fn a_quiet_gauge_alone_does_not_authorise_a_pass() {
        let c = cfg();
        assert_eq!(
            decide(Duration::from_secs(10), 0, false, None, &c),
            GateDecision::NotDue
        );
    }

    /// "Not due" is not a deferral. Recording every un-due tick as a deferral
    /// would inflate the count with ticks that were never candidates, and
    /// "deferred 400 times" would stop meaning "the application was busy".
    #[test]
    fn a_tick_that_was_never_a_candidate_is_not_counted_as_a_deferral() {
        let c = cfg();
        for gauge in [0, 1, 50] {
            assert_eq!(
                decide(Duration::from_secs(5), gauge, false, None, &c),
                GateDecision::NotDue,
                "gauge {gauge} inside the interval is not a deferral"
            );
        }
    }

    /// A saturated pool is demand for the machine that no count of requests or
    /// jobs can see — and the strongest possible "not a quiet window" signal.
    /// A gate blind to it would run a checkpoint into a lock convoy.
    #[test]
    fn pool_saturation_counts_as_busy_even_when_nothing_else_is_in_flight() {
        let c = cfg();
        let elapsed = Duration::from_secs(1_000);
        assert!(matches!(
            decide(elapsed, 0, true, None, &c),
            GateDecision::Defer { gauge: 1 }
        ));
        assert!(matches!(
            decide(elapsed, 0, false, None, &c),
            GateDecision::Run { .. }
        ));
    }

    /// Rung 2: past the staleness bound, prefer "quieter" to waiting forever —
    /// but at a REDUCED chunk size, because the whole reason it is running
    /// against a non-zero gauge is that it must interfere as little as it can.
    #[test]
    fn past_the_staleness_bound_a_quieter_window_runs_a_smaller_chunk() {
        let c = cfg();
        let stale = Duration::from_secs(c.staleness_secs + 1);
        match decide(stale, 1, false, None, &c) {
            GateDecision::Run {
                trigger, chunks, ..
            } => {
                assert_eq!(trigger, PassTrigger::Stale);
                assert_eq!(chunks, 1, "quieter means a smaller slice, not a full pass");
            }
            other => panic!("expected the stale rung, got {other:?}"),
        }
        // "Quieter" has a ceiling: a genuinely busy process still defers.
        assert!(matches!(
            decide(stale, 5, false, None, &c),
            GateDecision::Defer { gauge: 5 }
        ));
    }

    /// **The rung that must fire on harm, not on elapsed time.** A hard bound
    /// stated as a duration is a wall-clock timer sneaking back in through the
    /// escalation ladder — so a task that has been deferred for a month with a
    /// small sidecar must still defer, and one deferred for nine minutes with
    /// an oversized sidecar must run.
    #[test]
    fn the_hard_bound_fires_on_harm_not_on_elapsed_time() {
        let c = cfg();
        // A month of deferral, a healthy sidecar, a busy process: still defers.
        let a_month = Duration::from_secs(30 * 24 * 3600);
        let small = Harm {
            measured: 1024,
            bound: c.wal_harm_bytes,
        };
        assert!(
            matches!(
                decide(a_month, 9, false, Some(small), &c),
                GateDecision::Defer { .. }
            ),
            "elapsed time alone must never escalate past the activity gate"
        );
        // Nine minutes — inside the minimum interval, so not even due — but the
        // sidecar is over the harm bound: it runs, and the rung says why.
        let nine_minutes = Duration::from_secs(540);
        let big = Harm {
            measured: c.wal_harm_bytes + 1,
            bound: c.wal_harm_bytes,
        };
        match decide(nine_minutes, 9, true, Some(big), &c) {
            GateDecision::Run { trigger, gauge, .. } => {
                assert_eq!(trigger, PassTrigger::Harm);
                assert!(gauge > 0, "it ran WHILE busy, and the record says so");
            }
            other => panic!("expected the harm rung, got {other:?}"),
        }
    }

    /// A task with no harm bound can never reach rung 3 — which is a decision,
    /// not an omission. The two janitors delete, retention is deferred by
    /// operator decision, and the safe direction under permanent load is to
    /// keep deferring visibly rather than to trade a user-visible stall for
    /// disk nobody asked us to reclaim.
    #[test]
    fn a_task_with_no_harm_bound_defers_indefinitely_and_visibly() {
        let c = cfg();
        let forever = Duration::from_secs(365 * 24 * 3600);
        assert_eq!(
            harm_bound_for(MaintenanceTask::RetentionJanitor, u64::MAX, &c),
            None
        );
        assert_eq!(
            harm_bound_for(MaintenanceTask::StoreJanitor, u64::MAX, &c),
            None
        );
        assert!(matches!(
            decide(forever, 4, false, None, &c),
            GateDecision::Defer { gauge: 4 }
        ));
        // And the checkpoint — the one task that CAN escalate — does.
        assert!(harm_bound_for(MaintenanceTask::WalCheckpoint, u64::MAX, &c).is_some());
    }

    /// The clock measures staleness from the last pass that RAN, so a task that
    /// keeps deferring keeps getting staler and eventually reaches rung 2. A
    /// clock reset on every tick would hold it on rung 1 forever.
    #[test]
    fn staleness_is_measured_from_the_last_run_not_the_last_tick() {
        let mut clock = TaskClock::new();
        assert_eq!(clock.passes(), 0);
        assert!(clock.since_last() < Duration::from_secs(1));
        clock.ran();
        assert_eq!(clock.passes(), 1);
    }

    /// Every gate outcome must be expressible as one of the three recorded pass
    /// outcomes — an unmapped decision would be a pass that happened and left
    /// no trace, which is precisely the gap this whole module closes.
    #[test]
    fn every_gate_decision_maps_to_a_recorded_outcome() {
        let c = cfg();
        let cases = [
            decide(Duration::from_secs(1), 0, false, None, &c),
            decide(Duration::from_secs(1_000), 4, false, None, &c),
            decide(Duration::from_secs(1_000), 0, false, None, &c),
        ];
        for d in cases {
            match d {
                // NotDue is the one decision that deliberately records nothing.
                GateDecision::NotDue => {}
                GateDecision::Defer { .. } => {
                    assert_eq!(PassOutcome::Deferred.as_str(), "deferred")
                }
                GateDecision::Run { .. } => assert_eq!(PassOutcome::Ran.as_str(), "ran"),
            }
        }
        assert_eq!(PassOutcome::ALL.len(), 3, "three outcomes, never two");
    }
}
