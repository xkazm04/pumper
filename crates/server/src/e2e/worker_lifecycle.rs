//! The worker's **lifecycle** surface, driven through the real loop wherever
//! the loop is what's under test: shutdown suspend → checkpoint resume (without
//! re-paying the job's budget), the stuck-job reaper, the wall-clock timeout,
//! cooperative cancel of a running job, and the per-app concurrency cap.
//!
//! Plus the ordering guarantee `execute`'s own comment calls "the enforcement":
//! the health and contract gates run BEFORE the watch/trigger hooks, because a
//! delivered webhook cannot be recalled. That one is guarded twice — behaviourally
//! (a degraded source's changes reach no watch and no trigger) and structurally
//! (an inventory test over the call order in `worker.rs`).
//!
//! Everything here is condition-polled, offline, and Chrome-free.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pumper_core::{
    AppContext, EnqueueOptions, JobStatus, NewTrigger, Result, ScrapeApp, SourceState,
};
use serde_json::{json, Value};

use super::harness::{test_state, test_state_with, wait_for, wait_status, FakeApp, TestReceiver};
use crate::worker;

// ---- suspend → resume ------------------------------------------------------

/// What one attempt inherited: `(restored checkpoint, remaining budget)`.
type Inherited = (Option<Value>, Option<f64>);

/// Attempt 1 spends against the job budget, checkpoints, then hangs so the
/// drain has to suspend it. Attempt 2 (restored) reports what it inherited.
struct BudgetedResumeApp {
    /// One entry per attempt, in order.
    seen: Arc<Mutex<Vec<Inherited>>>,
    /// Set once attempt 1 has metered its spend, so the test can suspend at a
    /// known point instead of after a sleep.
    paid: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ScrapeApp for BudgetedResumeApp {
    fn name(&self) -> &'static str {
        "resumer"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let restored = ctx.restore().cloned();
        let remaining = ctx.remaining_budget_usd().await?;
        self.seen
            .lock()
            .unwrap()
            .push((restored.clone(), remaining));
        if restored.is_some() {
            return Ok(json!({ "resumed": true, "remaining": remaining }));
        }
        ctx.meter("http", None, 0.40, Some("attempt 1 spend")).await;
        assert!(ctx.checkpoint_now(json!({ "page": 7 })).await);
        self.paid.notify_waiters();
        // Hang until the drain's cooperative suspend stops us.
        std::future::pending::<()>().await;
        unreachable!()
    }
}

#[tokio::test]
async fn suspend_resumes_from_the_checkpoint_without_re_paying_the_budget() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let paid = Arc::new(tokio::sync::Notify::new());
    let app = Arc::new(BudgetedResumeApp {
        seen: seen.clone(),
        paid: paid.clone(),
    });
    let (state, _store) = test_state(vec![app]).await;
    let job = state
        .storage
        .enqueue(
            "resumer",
            EnqueueOptions {
                budget_usd: Some(1.0),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    let notified = paid.notified();
    let worker_loop = super::harness::WorkerLoop::start(&state);
    tokio::pin!(notified);
    tokio::time::timeout(Duration::from_secs(10), &mut notified)
        .await
        .expect("attempt 1 must reach its metered checkpoint");

    // Shutdown: the drain's phase 2 fires the per-job cancel token, and
    // `execute` treats that as SUSPEND — reset, not cancel, checkpoint intact.
    worker_loop.shutdown(&state, Duration::from_secs(15)).await;

    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        JobStatus::Queued,
        "a suspended job is re-queued, not cancelled and not left running"
    );
    assert!(
        row.max_attempts > 1,
        "suspend grants attempts headroom (reset semantics), got {}",
        row.max_attempts
    );
    assert!(
        state
            .storage
            .load_checkpoint(job.id)
            .await
            .unwrap()
            .is_some(),
        "the checkpoint must survive the suspend — it IS the resume"
    );

    // Next boot: the same `execute` path re-claims and hands back the checkpoint.
    assert!(worker::run_one(&state).await, "the suspended job re-claims");
    let attempts = seen.lock().unwrap().clone();
    assert_eq!(attempts.len(), 2, "one suspend, one resume");
    assert_eq!(attempts[0].0, None, "attempt 1 started fresh");
    assert_eq!(attempts[0].1, Some(1.0), "attempt 1 saw the full budget");
    assert_eq!(
        attempts[1].0,
        Some(json!({ "page": 7 })),
        "the resume starts from the checkpoint, not from scratch"
    );
    let resumed_headroom = attempts[1].1.expect("budgeted job has headroom");
    assert!(
        (resumed_headroom - 0.60).abs() < 1e-9,
        "the resumed attempt must inherit attempt 1's $0.40 of spend, not re-pay \
         the whole budget; got {resumed_headroom}"
    );
    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Succeeded);
}

// ---- reaper ----------------------------------------------------------------

/// Backdates a running job's lease so the reaper sees it as stale, with no sleep.
async fn expire_lease(state: &crate::state::AppState, id: uuid::Uuid) {
    sqlx::query("UPDATE jobs SET heartbeat_at = '2000-01-01T00:00:00.000000Z' WHERE id = ?1")
        .bind(id.to_string())
        .execute(&state.storage.pool())
        .await
        .expect("backdate lease");
}

#[tokio::test]
async fn the_reaper_requeues_a_stale_lease_and_fails_it_once_attempts_run_out() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let retryable = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                max_attempts: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let last_chance = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // Claim both into `running` (no app runs — this is the lease, not the work).
    for _ in 0..2 {
        assert!(state.storage.claim_next(&[], 0.0).await.unwrap().is_some());
    }
    expire_lease(&state, retryable.id).await;
    expire_lease(&state, last_chance.id).await;

    worker::reap_once(&state).await;

    let a = state.storage.get(retryable.id).await.unwrap().unwrap();
    assert_eq!(
        a.status,
        JobStatus::Queued,
        "attempts remaining → re-queued with failure semantics"
    );
    let b = state.storage.get(last_chance.id).await.unwrap().unwrap();
    assert_eq!(
        b.status,
        JobStatus::Failed,
        "attempts exhausted → permanent"
    );
    assert_eq!(b.error.as_deref(), Some("lease expired (heartbeat stale)"));

    // The permanent failure went through `finalize`, so it is on the bus like
    // any other terminal outcome.
    let statuses: Vec<String> = match state.events.replay(0) {
        crate::events::Replay::Events(evs) => evs
            .iter()
            .filter(|(_, e)| e.job_id == b.id)
            .map(|(_, e)| e.status.clone())
            .collect(),
        _ => panic!("expected buffered events"),
    };
    assert!(
        statuses.contains(&"failed".to_string()),
        "reaped-permanent must emit its terminal event, got {statuses:?}"
    );
}

#[tokio::test]
async fn a_zero_stale_window_disables_the_reaper() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.worker.stale_after_secs = 0;
    })
    .await;
    let job = state
        .storage
        .enqueue("fake", EnqueueOptions::default())
        .await
        .unwrap();
    assert!(state.storage.claim_next(&[], 0.0).await.unwrap().is_some());
    expire_lease(&state, job.id).await;

    worker::reap_once(&state).await;

    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        JobStatus::Running,
        "stale_after_secs = 0 means the reaper touches nothing"
    );
}

// ---- timeout ---------------------------------------------------------------

#[tokio::test]
async fn an_overrunning_job_times_out_rather_than_running_forever() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.worker.job_timeout_secs = 1;
    })
    .await;
    let job = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params: json!({ "sleep_ms": 600_000 }),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(worker::run_one(&state).await);

    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(
        row.error.as_deref(),
        Some("timed out after 1s"),
        "the timeout names itself — distinct from an app error, a panic, and a \
         reaped lease"
    );
}

// ---- cooperative cancel ----------------------------------------------------

#[tokio::test]
async fn cancelling_a_running_job_marks_it_cancelled_not_failed() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let job = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params: json!({ "sleep_ms": 600_000 }),
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let worker_loop = super::harness::WorkerLoop::start(&state);
    wait_status(&state, job.id, JobStatus::Running, Duration::from_secs(10)).await;

    // Exactly what `DELETE /jobs/{id}` does to a running job: fire the token the
    // worker registered for this attempt.
    let token = state
        .job_cancels
        .lock()
        .unwrap()
        .get(&job.id)
        .map(|(_, t)| t.clone())
        .expect("a running job has a registered cancel token");
    token.cancel();

    let row = wait_status(
        &state,
        job.id,
        JobStatus::Cancelled,
        Duration::from_secs(10),
    )
    .await;
    assert!(
        row.error.is_none(),
        "a user cancel is not a failure: {:?}",
        row.error
    );
    assert!(
        row.attempts < row.max_attempts,
        "cancel is terminal — it must not have burned through the retries"
    );
    assert!(
        state
            .storage
            .load_checkpoint(job.id)
            .await
            .unwrap()
            .is_none(),
        "a cancelled job's checkpoint is dropped; it will never be resumed"
    );
    worker_loop.shutdown(&state, Duration::from_secs(15)).await;
}

// ---- per-app concurrency cap ----------------------------------------------

/// Records the peak number of its own concurrent runs, and (when `want` > 1)
/// holds each run until that many overlap, so the "cap allows overlap" case is
/// proven by a condition rather than by a hopeful sleep.
struct ConcurrencyProbe {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    want: usize,
}

#[async_trait::async_trait]
impl ScrapeApp for ConcurrencyProbe {
    fn name(&self) -> &'static str {
        "probe"
    }
    async fn run(&self, _ctx: AppContext) -> Result<Value> {
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(750);
        while self.live.load(Ordering::SeqCst) < self.want && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.live.fetch_sub(1, Ordering::SeqCst);
        Ok(json!({ "ok": true }))
    }
}

async fn run_probe(cap: usize, want: usize) -> usize {
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let app = Arc::new(ConcurrencyProbe {
        live: live.clone(),
        peak: peak.clone(),
        want,
    });
    let (state, _store) = test_state_with(vec![app], |c| {
        c.worker.concurrency = 8;
        c.worker.app_concurrency.insert("probe".into(), cap);
    })
    .await;
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(
            state
                .storage
                .enqueue("probe", EnqueueOptions::default())
                .await
                .unwrap()
                .id,
        );
    }

    let worker_loop = super::harness::WorkerLoop::start(&state);
    for id in ids {
        wait_status(&state, id, JobStatus::Succeeded, Duration::from_secs(30)).await;
    }
    worker_loop.shutdown(&state, Duration::from_secs(15)).await;
    peak.load(Ordering::SeqCst)
}

#[tokio::test]
async fn a_per_app_cap_of_one_never_runs_two_of_that_app_at_once() {
    // Global concurrency is 8 and three jobs are queued: only the per-app cap
    // can hold them to one at a time.
    assert_eq!(
        run_probe(1, 1).await,
        1,
        "per-app cap of 1 was not honoured"
    );
}

#[tokio::test]
async fn raising_the_per_app_cap_lets_the_same_jobs_overlap() {
    // The control for the test above: with the cap at 3 the very same app does
    // overlap, so a peak of 1 there is the cap, not the harness serialising.
    assert!(
        run_probe(3, 3).await > 1,
        "cap 3 must allow overlap, else the cap-1 assertion proves nothing"
    );
}

// ---- gate ordering ---------------------------------------------------------

#[tokio::test]
async fn the_health_gate_runs_before_the_watch_and_trigger_hooks() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.resilience.enabled = true;
        c.resilience.enforce = true;
    })
    .await;
    assert!(state.health.enforcing(), "gate must actually be armed");
    let store = state.health.store().expect("enabled health store");
    store.ensure_source("fake", "d").await.unwrap();
    assert!(store
        .set_state_manual(
            &pumper_core::resilience::source_id("fake", "d"),
            SourceState::Degraded,
            "test: a source we no longer stand behind",
        )
        .await
        .unwrap());

    let rx = TestReceiver::spawn(vec![]).await;
    state
        .storage
        .create_watch("fake", "d", &rx.url(), None, "webhook")
        .await
        .unwrap();
    state
        .storage
        .create_trigger(&NewTrigger {
            name: Some("must-not-fire"),
            source_kind: "dataset",
            source_app: "fake",
            source_dataset: Some("d"),
            on_change: None,
            on_status: None,
            target_app: "fake",
            params: &json!({ "hop": true }),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: None,
        })
        .await
        .unwrap();

    let job = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params: json!({
                    "dataset": "d",
                    "sync": [{ "key": "k1", "data": { "n": 1 } }],
                }),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(worker::run_one(&state).await);
    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Succeeded,
        "suppression gates PUSHES, never the job or the stored data"
    );

    // Control: the run really did produce revisions, so an empty batch is not
    // what silenced the hooks.
    let changes = state
        .datasets
        .changes_since("fake", Some("d"), job.started_at, 100, None)
        .await
        .unwrap();
    assert!(!changes.is_empty(), "the run wrote revisions");

    // Give any (wrongly) dispatched webhook / trigger hop time to land.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let hops: Vec<(String,)> = sqlx::query_as("SELECT id FROM jobs WHERE id != ?1")
        .bind(job.id.to_string())
        .fetch_all(&state.storage.pool())
        .await
        .unwrap();
    assert!(
        hops.is_empty(),
        "a degraded source must not fire dataset triggers"
    );
    assert!(
        rx.hits_so_far().is_empty(),
        "a degraded source must not fire watches — a delivered webhook cannot \
         be recalled, which is exactly why the gate sits above the hooks"
    );
}

/// Structural companion to the behavioural test above: the two gates must
/// appear BEFORE the two hooks. `suppress_unhealthy`'s own comment says "this
/// ordering IS the enforcement, and if it moves below them the guarantee is
/// gone" — this fails if anyone moves it.
///
/// The four call sites moved out of `execute` and into `finalize_fanout` when
/// the fan-out came off the worker's concurrency permit. That is a change of
/// *task*, not of order — so this test keeps the identical order assertion and
/// adds the property the move made necessary: all four must live in ONE
/// function body. Splitting them across the permit boundary (some inline, some
/// on the pool) would let a webhook fire concurrently with the gate that is
/// supposed to have already vetoed it, which the order check alone cannot see.
#[tokio::test]
async fn gate_calls_precede_hook_calls_in_the_success_fanout() {
    let src = include_str!("../worker.rs");
    let at = |needle: &str| {
        src.find(needle)
            .unwrap_or_else(|| panic!("call site not found in worker.rs: {needle}"))
    };
    // EXPECTED order — the pipeline run after a successful job.
    let expected = [
        "suppress_unhealthy(&state, &job.app, &mut by_dataset)",
        "enforce_contracts(&state, &job, &mut by_dataset)",
        "notify_watches(&state, &job, &by_dataset)",
        // The run fan-out's `fire_dataset_triggers` call, pinned by its batch
        // argument: the call itself now spans several lines (line breaks and
        // indentation are rustfmt's and the checkout's to decide), while
        // `DatasetBatch::Run` appears exactly once and only there.
        "crate::triggers::DatasetBatch::Run,",
    ];
    let positions: Vec<usize> = expected.iter().map(|n| at(n)).collect();
    assert!(
        positions.windows(2).all(|w| w[0] < w[1]),
        "gates must precede hooks; found order {:?} for {expected:?}",
        positions
    );
    // Each call site appears exactly once, so a second (ungated) copy of a hook
    // can't be added elsewhere without failing here.
    for needle in expected {
        assert_eq!(
            src.matches(needle).count(),
            1,
            "the gate/hook pipeline must have exactly one call site each: {needle}"
        );
    }
    // …and all four sit inside `finalize_fanout`, the single unit the pool runs.
    let fanout_start = at("async fn finalize_fanout(");
    let next_fn = src[fanout_start..]
        .find("\nfn ")
        .map(|i| fanout_start + i)
        .expect("finalize_fanout is followed by another item");
    assert!(
        positions
            .iter()
            .all(|p| (fanout_start..next_fn).contains(p)),
        "every gate and hook must run inside finalize_fanout — one task, one order"
    );
}

// ---- unregistered app ------------------------------------------------------

#[tokio::test]
async fn a_job_for_an_unregistered_app_fails_permanently_instead_of_hanging() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let job = state
        .storage
        .enqueue(
            "ghost",
            EnqueueOptions {
                max_attempts: 5,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(worker::run_one(&state).await);

    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        JobStatus::Failed,
        "an unregistered app can never succeed; retrying it 5× is pure waste"
    );
    assert_eq!(row.error.as_deref(), Some("app not registered"));
}

/// `heartbeat_at` is a queue column, not a `Job` field — read it raw.
async fn lease_stamp(state: &crate::state::AppState, id: uuid::Uuid) -> Option<String> {
    sqlx::query_scalar::<_, Option<String>>("SELECT heartbeat_at FROM jobs WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(&state.storage.pool())
        .await
        .expect("read lease")
}

/// A slow-but-alive job is never mistaken for a hung one: it keeps yielding, so
/// the heartbeat keeps landing and the reaper's cutoff never catches it. This
/// is the property `catch_unwind` explicitly cannot provide for a non-yielding
/// wedge — the reaper stays the backstop there.
#[tokio::test]
async fn a_yielding_job_keeps_its_lease_fresh() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.worker.heartbeat_secs = 1;
        c.worker.stale_after_secs = 3600;
    })
    .await;
    let job = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params: json!({ "sleep_ms": 600_000 }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let worker_loop = super::harness::WorkerLoop::start(&state);
    wait_status(&state, job.id, JobStatus::Running, Duration::from_secs(10)).await;
    let first = lease_stamp(&state, job.id)
        .await
        .expect("claim stamps a lease");
    wait_for(
        "the heartbeat to advance while the app is still awaiting",
        Duration::from_secs(15),
        || async {
            lease_stamp(&state, job.id)
                .await
                .is_some_and(|now| now > first)
        },
    )
    .await;
    worker_loop.shutdown(&state, Duration::from_secs(15)).await;
}
