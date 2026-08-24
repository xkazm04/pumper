//! The quiet-window gate, driven end to end: a real store, a real activity
//! gauge, a real `wal_checkpoint(PASSIVE)`.
//!
//! `maintenance::decide`'s rungs are unit-tested as a pure function. What those
//! tests cannot prove is that the gate is *wired* — that the gauge the server
//! feeds at its front doors is the same gauge the pass reads, and that a pass
//! authorised by the gate actually moves pages. A gate that returns the right
//! answer to a caller nobody connected is the most convincing kind of nothing.

use std::sync::Arc;

use pumper_core::store_instrument::{MaintenanceTask, PassOutcome, PassTrigger};
use pumper_core::EnqueueOptions;

use super::harness::{test_state, FakeApp};
use crate::maintenance::{self, Harm, TaskClock};

/// A clock that has already run once, paired with [`fast_cfg`]'s zero
/// interval, so the tests below vary exactly one thing: the gauge.
///
/// `TaskClock` starts "now" on purpose (nothing fires during boot), and no test
/// may sleep fifteen minutes to get past that — so the CONFIG is what moves
/// here, never the clock, which keeps the clock under test rather than mocked.
fn due_clock() -> TaskClock {
    let mut clock = TaskClock::new();
    clock.ran();
    clock
}

fn fast_cfg() -> pumper_core::config::MaintenanceConfig {
    pumper_core::config::MaintenanceConfig {
        enabled: true,
        tick_secs: 1,
        // Zero: the interval is not what these tests are about, and leaving it
        // at fifteen minutes would make every case below read `NotDue`.
        min_interval_secs: 0,
        staleness_secs: 0,
        quiet_enough: 0,
        wal_harm_bytes: 64 * 1024 * 1024,
        checkpoint_rounds: 4,
        analyze_every_passes: 24,
    }
}

/// **The gate really gates.** With foreground work in flight the pass is
/// deferred and the deferral is recorded; the moment the gauge drops to zero
/// the identical pass runs. This is the whole difference between the wall-clock
/// janitors this replaces and a window that is found rather than scheduled.
#[tokio::test]
async fn a_pass_defers_while_work_is_in_flight_and_runs_when_it_stops() {
    let (state, _tmp) = test_state(vec![Arc::new(FakeApp)]).await;
    let instrument = state.storage.instrument();
    let cfg = fast_cfg();
    let mut clock = due_clock();

    // Something is in flight — exactly what an HTTP request or a running job
    // registers at the front door.
    let busy = state.activity.enter();
    assert_eq!(state.activity.reading(), 1);
    let outcome = maintenance::tick_task(
        &instrument,
        &state.activity,
        MaintenanceTask::WalCheckpoint,
        &mut clock,
        &cfg,
        None,
        |_, _| async { panic!("the pass must not run while the gauge is above zero") },
    )
    .await;
    assert_eq!(outcome, Some(PassOutcome::Deferred));
    assert_eq!(
        instrument.pass_count(MaintenanceTask::WalCheckpoint, PassOutcome::Deferred),
        1,
        "a deferral is an OUTCOME and is recorded"
    );
    assert_eq!(
        instrument.pass_count(MaintenanceTask::WalCheckpoint, PassOutcome::Ran),
        0
    );

    // The work finishes.
    drop(busy);
    assert_eq!(state.activity.reading(), 0);
    let outcome = maintenance::tick_task(
        &instrument,
        &state.activity,
        MaintenanceTask::WalCheckpoint,
        &mut clock,
        &cfg,
        None,
        |_, _| async { Ok(7) },
    )
    .await;
    assert_eq!(outcome, Some(PassOutcome::Ran));
    assert_eq!(
        instrument.pass_count(MaintenanceTask::WalCheckpoint, PassOutcome::Ran),
        1
    );
    let last = instrument.recent_passes().pop().expect("a pass record");
    assert_eq!(last.trigger, Some(PassTrigger::Quiet));
    assert_eq!(last.gauge, 0, "the record carries what the gate saw");
    assert_eq!(last.work, 7);
}

/// The worker's half of the gauge is wired to the same counter the gate reads.
/// A gauge fed only by HTTP would present a "quiet window" during the exact
/// minutes a scrape holds the writer lane — the failure this whole mechanism
/// exists to prevent.
#[tokio::test]
async fn a_running_job_makes_the_gauge_busy_and_releases_it_when_done() {
    let (state, _tmp) = test_state(vec![Arc::new(FakeApp)]).await;
    assert_eq!(state.activity.reading(), 0);
    state
        .storage
        .enqueue("fake", EnqueueOptions::default())
        .await
        .expect("enqueue");
    assert!(crate::worker::run_one(&state).await, "a job ran");
    assert_eq!(
        state.activity.reading(),
        0,
        "the guard is released when the run ends — a leaked count would disable \
         maintenance for the life of the process"
    );
    assert_eq!(
        state.activity.raw(),
        0,
        "and it is balanced, not merely clamped"
    );
}

/// **The escalation rung fires on harm, not on elapsed time.** A pass that has
/// been deferred forever with a healthy sidecar keeps deferring; a pass whose
/// sidecar is over the stated byte bound runs while the application is visibly
/// busy, and says in its record that it did.
#[tokio::test]
async fn the_escalation_rung_fires_on_the_harm_bound_not_on_a_timer() {
    let (state, _tmp) = test_state(vec![Arc::new(FakeApp)]).await;
    let instrument = state.storage.instrument();
    let cfg = fast_cfg();
    let mut clock = due_clock();
    let _busy = state.activity.enter();

    // A sidecar well under the bound: however long this has been waiting, it
    // does not get to interrupt foreground work.
    let healthy = Harm {
        measured: 4_096,
        bound: cfg.wal_harm_bytes,
    };
    let outcome = maintenance::tick_task(
        &instrument,
        &state.activity,
        MaintenanceTask::WalCheckpoint,
        &mut clock,
        &cfg,
        Some(healthy),
        |_, _| async { panic!("no harm, no escalation") },
    )
    .await;
    assert_eq!(outcome, Some(PassOutcome::Deferred));

    // The same instant, the same gauge, the same clock — only the harm figure
    // moves, and now the pass runs.
    let breached = Harm {
        measured: cfg.wal_harm_bytes + 1,
        bound: cfg.wal_harm_bytes,
    };
    let outcome = maintenance::tick_task(
        &instrument,
        &state.activity,
        MaintenanceTask::WalCheckpoint,
        &mut clock,
        &cfg,
        Some(breached),
        |_, _| async { Ok(42) },
    )
    .await;
    assert_eq!(outcome, Some(PassOutcome::Ran));
    let last = instrument.recent_passes().pop().expect("a pass record");
    assert_eq!(last.trigger, Some(PassTrigger::Harm));
    assert!(
        last.gauge > 0,
        "it ran WHILE busy — the record must admit it"
    );
    assert!(
        last.detail.contains("regardless of activity"),
        "an escalation has to say so: {}",
        last.detail
    );
}

/// The pass itself is real and lossless: it moves committed frames out of the
/// sidecar and into the main file, and every row that was there is still there.
/// A checkpoint that discarded anything would violate the deferred-retention
/// posture this whole lane runs under.
#[tokio::test]
async fn the_checkpoint_pass_shrinks_the_sidecar_and_loses_nothing() {
    let (state, _tmp) = test_state(vec![Arc::new(FakeApp)]).await;
    for _ in 0..50 {
        state
            .storage
            .enqueue("fake", EnqueueOptions::default())
            .await
            .expect("enqueue");
    }
    let before = state.storage.size_facts().await.expect("size");
    assert!(before.wal_bytes > 0, "fifty commits leave a sidecar");
    let queued_before = state.storage.status_counts().await.expect("counts");

    let round = state
        .storage
        .wal_checkpoint_passive()
        .await
        .expect("a passive checkpoint never fails on an idle store");
    assert!(
        round.checkpointed_pages > 0,
        "an idle store with a full sidecar must actually checkpoint: {round:?}"
    );
    assert_eq!(
        round.remaining(),
        0,
        "PASSIVE drained it — note `log_frames` is the log's SIZE, not a remainder: {round:?}"
    );

    let after = state.storage.size_facts().await.expect("size");
    assert!(
        after.main_bytes >= before.main_bytes,
        "frames moved INTO the main file: {before:?} -> {after:?}"
    );
    assert_eq!(
        state.storage.status_counts().await.expect("counts"),
        queued_before,
        "a checkpoint relocates committed data; it must never discard a row"
    );
    // And the pass records itself into the same instrument as everything else,
    // which is what makes "was that stall at 14:03 us?" answerable.
    let snap = state.storage.instrument().snapshot();
    let maint = snap
        .iter()
        .find(|r| {
            r.op == pumper_core::StoreOp::Maintenance && r.phase == pumper_core::StorePhase::Execute
        })
        .expect("the maintenance key");
    assert!(maint.lifetime > 0, "the checkpoint measured itself");
}

/// An attempted-and-failed pass is neither a run nor a deferral, and it must
/// NOT advance the clock — a clock the failure path touches launders errors
/// into a schedule that looks like it is keeping up.
#[tokio::test]
async fn a_failed_pass_is_its_own_outcome_and_stays_stale() {
    let (state, _tmp) = test_state(vec![Arc::new(FakeApp)]).await;
    let instrument = state.storage.instrument();
    let mut cfg = fast_cfg();
    cfg.min_interval_secs = 3_600; // so `NotDue` is what a reset clock would say
    let mut clock = TaskClock::new();
    let outcome = maintenance::tick_task(
        &instrument,
        &state.activity,
        MaintenanceTask::Optimize,
        &mut clock,
        &cfg,
        // The harm rung is the only way past a 1-hour interval on a fresh
        // clock, and it is exactly the rung an emergency would take.
        Some(Harm {
            measured: 1,
            bound: 1,
        }),
        |_, _| async { Err(pumper_core::Error::App("the disk went away".into())) },
    )
    .await;
    assert_eq!(outcome, Some(PassOutcome::Failed));
    assert_eq!(
        instrument.pass_count(MaintenanceTask::Optimize, PassOutcome::Failed),
        1
    );
    assert_eq!(
        instrument.pass_count(MaintenanceTask::Optimize, PassOutcome::Ran),
        0,
        "a failure is not a success with zero work"
    );
    assert_eq!(clock.passes(), 0, "a failed pass does not count as a pass");
    let last = instrument.recent_passes().pop().expect("a record");
    assert!(
        last.detail.contains("disk went away"),
        "the failure names itself: {}",
        last.detail
    );
}
