use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pumper_core::{AppContext, Job, JobStatus, SearchDoc};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::events::JobEvent;
use crate::state::AppState;
use crate::webhook;

/// Claims due jobs and runs them on the shared engines, bounded by a global
/// concurrency cap and a per-app cap (so one busy app can't starve the others).
/// Wakes instantly on enqueue via Notify.
pub async fn run(state: AppState) {
    let concurrency = state.config.worker.concurrency.max(1);
    let poll = Duration::from_secs(state.config.worker.poll_interval_secs.max(1));
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let running: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    info!(concurrency, "job worker started");

    loop {
        // Stop claiming new work the moment shutdown is signalled — jobs already
        // running keep their permits and are drained below.
        let permit = tokio::select! {
            biased;
            _ = state.shutdown.cancelled() => break,
            permit = semaphore.clone().acquire_owned() => permit.expect("semaphore closed"),
        };
        if state.shutdown.is_cancelled() {
            drop(permit);
            break;
        }

        let blocked = blocked_apps(&state, &running).await;
        let aging = state.config.worker.priority_aging_coefficient_secs;
        match state.storage.claim_next(&blocked, aging).await {
            Ok(Some(job)) => {
                {
                    let mut counts = running.lock().await;
                    *counts.entry(job.app.clone()).or_insert(0) += 1;
                }
                let state = state.clone();
                let running = running.clone();
                tokio::spawn(async move {
                    // Register a cancellation token so `DELETE /jobs/{id}` can
                    // abort this in-flight run. Keyed by attempt so an
                    // overlapping re-claim (after a reset/reap) doesn't clobber
                    // or get clobbered by this task's registry entry.
                    let cancel = tokio_util::sync::CancellationToken::new();
                    state
                        .job_cancels
                        .lock()
                        .unwrap()
                        .insert(job.id, (job.attempts, cancel.clone()));
                    publish(&state, JobEvent::new(job.id, job.app.clone(), "running"));
                    execute(state.clone(), job.clone(), cancel).await;
                    {
                        let mut m = state.job_cancels.lock().unwrap();
                        if m.get(&job.id).map(|(a, _)| *a) == Some(job.attempts) {
                            m.remove(&job.id);
                        }
                    }
                    {
                        let mut counts = running.lock().await;
                        if let Some(n) = counts.get_mut(&job.app) {
                            *n = n.saturating_sub(1);
                        }
                    }
                    // A finished job may unblock a previously-capped app.
                    state.notify.notify_one();
                    drop(permit);
                });
            }
            Ok(None) => {
                drop(permit);
                tokio::select! {
                    _ = state.shutdown.cancelled() => break,
                    _ = state.notify.notified() => {}
                    _ = tokio::time::sleep(poll) => {}
                }
            }
            Err(e) => {
                drop(permit);
                error!("failed to claim job: {e}");
                tokio::select! {
                    _ = state.shutdown.cancelled() => break,
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        }
    }

    drain(&state, &semaphore, concurrency).await;
}

/// One claim→execute→finalize pass with no loop, semaphore, or per-app caps —
/// the deterministic seam the e2e tests drive. Mirrors the spawn body in
/// `run()`: cancel-token registration, the `running` event, execution with all
/// side effects, and attempt-matched token cleanup. Returns whether a job was
/// claimed.
#[cfg(test)]
pub(crate) async fn run_one(state: &AppState) -> bool {
    let aging = state.config.worker.priority_aging_coefficient_secs;
    match state.storage.claim_next(&[], aging).await {
        Ok(Some(job)) => {
            let cancel = tokio_util::sync::CancellationToken::new();
            state
                .job_cancels
                .lock()
                .unwrap()
                .insert(job.id, (job.attempts, cancel.clone()));
            publish(state, JobEvent::new(job.id, job.app.clone(), "running"));
            execute(state.clone(), job.clone(), cancel).await;
            {
                let mut m = state.job_cancels.lock().unwrap();
                if m.get(&job.id).map(|(a, _)| *a) == Some(job.attempts) {
                    m.remove(&job.id);
                }
            }
            // The fan-out is deliberately off this task now, so this seam waits
            // for it: `run_one` promises "one job, fully processed", and every
            // test that asserts on a webhook, a trigger hop or an index write
            // depends on that promise.
            //
            // In this order: the fan-out is what QUEUES webhook deliveries, so
            // draining deliveries first would just race the ones it is about to
            // enqueue. Draining second is what makes "the delivery arrived" a
            // synchronization point rather than a deadline poll.
            state.fanout.drain(Duration::from_secs(60)).await;
            state.deliveries.drain(Duration::from_secs(60)).await;
            true
        }
        _ => false,
    }
}

/// Graceful-shutdown drain: waits for in-flight jobs to finish (each holds a
/// semaphore permit, so reacquiring all of them means the queue is idle) — but
/// splits `shutdown_drain_secs` into two phases instead of waiting the whole
/// window and abandoning whatever is left:
///
/// 1. **Clean finish** (deadline minus a suspend-grace slice): jobs that can
///    complete, do.
/// 2. **Cooperative suspend**: every still-running job's cancel token is fired.
///    Under an active shutdown, `execute` treats that cancellation as *suspend*
///    — the job is re-queued (`reset` semantics, attempts headroom granted) and
///    its latest durable checkpoint resumes it on the next boot. This is the
///    "checkpoint instead of abandon" half of durable execution.
///
/// Anything still running at the true deadline is re-queued via
/// `recover_stuck`, exactly as before — its checkpoint also survives, since
/// checkpoints are only cleared on completion.
async fn drain(state: &AppState, semaphore: &Arc<Semaphore>, concurrency: usize) {
    let total_secs = state.config.worker.shutdown_drain_secs;
    let deadline = Duration::from_secs(total_secs);
    // Reserve the tail of the window for the suspend round-trip (token fire →
    // requeue → permit release); 1–10s scaled to the configured window.
    let grace = Duration::from_secs((total_secs / 5).clamp(1, 10).min(total_secs.max(1)));
    let clean_window = deadline.saturating_sub(grace);
    info!(
        deadline_secs = deadline.as_secs(),
        suspend_grace_secs = grace.as_secs(),
        "worker draining in-flight jobs"
    );
    let acquire = semaphore.clone().acquire_many_owned(concurrency as u32);
    tokio::pin!(acquire);
    if tokio::time::timeout(clean_window, &mut acquire)
        .await
        .is_ok()
    {
        info!("worker drained cleanly; no jobs left running");
        drain_fanout(state, grace).await;
        return;
    }
    // Phase 2: signal cooperative suspend through the existing per-job cancel
    // tokens. `execute` sees shutdown + cancellation and re-queues with the
    // checkpoint intact rather than marking the job cancelled.
    let tokens: Vec<_> = state
        .job_cancels
        .lock()
        .unwrap()
        .values()
        .map(|(_, token)| token.clone())
        .collect();
    warn!(
        jobs = tokens.len(),
        "drain window closing; suspending in-flight jobs to their checkpoints"
    );
    for token in tokens {
        token.cancel();
    }
    match tokio::time::timeout(grace, &mut acquire).await {
        Ok(_) => info!("in-flight jobs suspended; checkpoints will resume them on next boot"),
        Err(_) => match state.storage.recover_stuck().await {
            Ok(n) => warn!(
                requeued = n,
                "drain deadline reached; re-queued still-running jobs"
            ),
            Err(e) => error!("drain re-queue failed: {e}"),
        },
    }
    drain_fanout(state, grace).await;
}

/// Second half of the drain: a job's fan-out no longer holds a worker permit,
/// so re-acquiring every permit above no longer proves the queue is idle. Wait
/// for the pools too, and — this is the point — **say so** when they don't
/// finish, instead of letting the process exit over silently-unsent webhooks.
///
/// Two pools, in dependency order. The job fan-out is what *queues* webhook
/// deliveries, so it drains first; draining deliveries first would race the ones
/// the fan-out is about to enqueue and declare victory over an empty pool.
async fn drain_fanout(state: &AppState, grace: Duration) {
    drain_pool(
        &state.fanout,
        grace,
        "job fan-out",
        "those jobs' index/hook/alert work did not finish (their results are persisted)",
    )
    .await;
    drain_pool(
        &state.deliveries,
        grace,
        "webhook deliveries",
        "those rows stay 'pending' and are returned to the retry ladder by the stale-pending \
         reclaim on the next boot — nothing is lost, but it is late",
    )
    .await;
}

/// Drains one pool within `grace`, reporting stragglers with what their loss
/// actually costs. Shared by both pools so neither can quietly skip the "say
/// what was abandoned" half.
async fn drain_pool(
    pool: &crate::fanout::FanoutPool,
    grace: Duration,
    what: &'static str,
    consequence: &'static str,
) {
    let inflight = pool.inflight();
    if inflight == 0 {
        return;
    }
    info!(inflight, pool = what, "draining in-flight work");
    let left = pool.drain(grace).await;
    if left > 0 {
        warn!(
            abandoned = left,
            pool = what,
            "shutdown reached its deadline with work still running: {consequence}"
        );
    } else {
        info!(pool = what, "drained cleanly");
    }
}

/// The checkpoint state to hand a freshly-claimed attempt, applying the
/// poisoned-blob escape: once `max_resume_failures` restored attempts have all
/// failed to complete, the checkpoint is discarded (fresh start) instead of
/// being retried forever. Fail-open — an unreadable checkpoint store never
/// blocks the run, it just means no restore.
async fn load_restore(state: &AppState, job: &Job) -> Option<Value> {
    let max = state.config.worker.max_resume_failures;
    if max <= 0 {
        return None;
    }
    match state.storage.load_checkpoint(job.id).await {
        Ok(Some((checkpoint, failures))) => {
            if failures >= max {
                warn!(
                    job = %job.id,
                    failures,
                    "checkpoint looks poisoned ({failures} restored attempts failed); \
                     discarding for a fresh start"
                );
                if let Err(e) = state.storage.clear_checkpoint(job.id).await {
                    warn!(job = %job.id, "poisoned-checkpoint clear failed: {e}");
                }
                return None;
            }
            // Count this hand-out; a completing attempt clears the row, so the
            // counter only ever reaches `max` through repeated failures.
            if let Err(e) = state.storage.bump_checkpoint_resumes(job.id).await {
                warn!(job = %job.id, "checkpoint resume-count bump failed: {e}");
            }
            info!(job = %job.id, attempt = job.attempts, "resuming from durable checkpoint");
            Some(checkpoint)
        }
        Ok(None) => None,
        Err(e) => {
            warn!(job = %job.id, "checkpoint load failed, starting fresh: {e}");
            None
        }
    }
}

/// Apps currently at or above their concurrency limit (0 = unlimited).
async fn blocked_apps(
    state: &AppState,
    running: &Arc<Mutex<HashMap<String, usize>>>,
) -> Vec<String> {
    let counts = running.lock().await;
    counts
        .iter()
        .filter_map(|(app, &n)| {
            let limit = app_limit(state, app);
            (limit > 0 && n >= limit).then(|| app.clone())
        })
        .collect()
}

fn app_limit(state: &AppState, app: &str) -> usize {
    state
        .config
        .worker
        .app_concurrency
        .get(app)
        .copied()
        .unwrap_or(state.config.worker.default_app_concurrency)
}

/// How a run left the queue: the app finished (Ok/Err), it panicked, the
/// wall-clock timeout tripped, or a cancellation token fired mid-run.
enum Outcome {
    Finished(pumper_core::Result<Value>),
    /// The app's future unwound. Carries the rendered `panicked: …` error.
    Panicked(String),
    TimedOut,
    Cancelled,
}

/// Prefix on a panic-derived `job.error`, so a caller reading the row can tell
/// a panic apart from an app-returned error, a `timed out after Ns` timeout,
/// and a reaper's `lease expired (heartbeat stale)`.
const PANIC_ERROR_PREFIX: &str = "panicked: ";

/// Renders a caught panic payload as a job error string.
///
/// `std`'s payload is `&str` for a literal `panic!("…")` and `String` for a
/// formatted one; anything else (a `panic_any`) has no printable form, so we
/// say so rather than inventing a message. `location` is the `file:line:col`
/// captured by [`install_panic_location_hook`] when available.
fn panic_error(payload: &(dyn std::any::Any + Send), location: Option<&str>) -> String {
    let msg = payload
        .downcast_ref::<&'static str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    match location {
        Some(loc) => format!("{PANIC_ERROR_PREFIX}{msg} (at {loc})"),
        None => format!("{PANIC_ERROR_PREFIX}{msg}"),
    }
}

thread_local! {
    /// Set by the panic hook on the panicking thread; read back by the
    /// `catch_unwind` handler, which resumes on that same thread.
    static LAST_PANIC_LOCATION: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// Installs, once per process, a panic hook that stashes each panic's source
/// location before delegating to whatever hook was already installed.
///
/// `catch_unwind` hands back only the payload — the `file:line:col` lives in
/// `PanicHookInfo` and is otherwise lost. Chaining (rather than replacing) the
/// previous hook keeps the default backtrace logging and anything a host binary
/// installed (e.g. Sentry) intact.
fn install_panic_location_hook() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let location = info
                .location()
                .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()));
            LAST_PANIC_LOCATION.with(|slot| *slot.borrow_mut() = location);
            previous(info);
        }));
    });
}

/// Takes (and clears) the location recorded by the most recent panic on this
/// thread.
fn take_panic_location() -> Option<String> {
    LAST_PANIC_LOCATION.with(|slot| slot.borrow_mut().take())
}

async fn execute(state: AppState, job: Job, cancel: tokio_util::sync::CancellationToken) {
    // Stage accounting starts at the claim, so `total_ms` covers the whole
    // slot+fan-out span a caller sees as "this job took N seconds".
    let started = std::time::Instant::now();
    let Some(app) = state.registry.get(&job.app).cloned() else {
        warn!(app = %job.app, job = %job.id, "job references unregistered app");
        let _ = state
            .storage
            .fail_permanently(job.id, job.attempts, "app not registered")
            .await;
        finalize(&state, job.id).await;
        return;
    };

    // VCR (M24): `record: true` / `replay_of: <job_id>` params. Parsed and
    // resolved before anything runs — a replay whose cassette is missing (or a
    // contradictory record+replay combination) fails the job immediately with
    // the typed reason instead of half-running live.
    let (vcr_record, replay_of) = match vcr_params(&job.params) {
        Ok(parsed) => parsed,
        Err(msg) => {
            warn!(job = %job.id, "invalid vcr params: {msg}");
            let _ = state
                .storage
                .fail_permanently(job.id, job.attempts, &msg)
                .await;
            finalize(&state, job.id).await;
            return;
        }
    };
    let artifacts_dir = state
        .storage
        .artifacts_dir
        .join(&job.app)
        .join(job.id.to_string());
    let vcr = if let Some(replay_id) = replay_of {
        // Cassettes live beside the recorded job's other artifacts, under the
        // SAME app (a job can only replay a run of its own app — the fetches
        // it makes are the ones that app's code makes).
        let recorded_dir = state
            .storage
            .artifacts_dir
            .join(&job.app)
            .join(replay_id.to_string());
        match pumper_core::Cassette::load(&recorded_dir, replay_id).await {
            Ok(cassette) => {
                info!(
                    job = %job.id,
                    replay_of = %replay_id,
                    entries = cassette.len(),
                    "vcr replay: serving fetches from recorded cassette ($0, no network)"
                );
                pumper_core::Vcr::Replay(Arc::new(cassette))
            }
            Err(e) => {
                warn!(job = %job.id, replay_of = %replay_id, "vcr replay unavailable: {e}");
                let _ = state
                    .storage
                    .fail_permanently(job.id, job.attempts, &e.to_string())
                    .await;
                finalize(&state, job.id).await;
                return;
            }
        }
    } else if vcr_record {
        info!(job = %job.id, "vcr record: persisting fetches to this job's cassette");
        pumper_core::Vcr::Record(Arc::new(pumper_core::Recorder::new(artifacts_dir.clone())))
    } else {
        pumper_core::Vcr::Off
    };

    info!(job = %job.id, app = %job.app, attempt = job.attempts, "job started");
    // Seed the running spend total from the ledger: a retried job's prior
    // attempts already spent real money against this job's budget, and that
    // spend must still count toward the ceiling. Fail-open per the worker
    // convention — an unreadable ledger must not block the run, it only means
    // this attempt starts its accounting from zero.
    let spent_seed = match state.costs.job_total(job.id).await {
        Ok(total) => total,
        Err(e) => {
            warn!(job = %job.id, "cost ledger read failed, seeding spend at 0: {e}");
            0.0
        }
    };
    // Durable execution: hand this attempt the last persisted checkpoint (if
    // any), with the poisoned-blob escape — a checkpoint whose every restored
    // attempt has failed is discarded so the job can start fresh instead of
    // dying to the same state `max_resume_failures` more times.
    let restored = load_restore(&state, &job).await;
    let ctx = AppContext {
        job_id: job.id,
        app: job.app.clone(),
        params: job.params.clone(),
        engines: state.engines.clone(),
        datasets: state.datasets.clone(),
        costs: state.costs.clone(),
        // M26: a `cost:pause` DataHub tag on the app's datasets forces $0 —
        // the budget governor then serves free tiers only (reversible pause).
        budget_usd: crate::datahub::effective_budget(&state, &job.app, job.budget_usd),
        spent_usd: std::sync::Arc::new(pumper_core::SpentTotal::new(spent_seed)),
        research_cache: state.research_cache.clone(),
        tiers: state.tiers.clone(),
        health: state.health.clone(),
        recipes: Arc::new(state.storage.recipes()),
        plugins: state.plugins.clone(),
        progress: state
            .progress
            .reporter(job.id, job.app.clone(), state.events.clone()),
        checkpoints: Arc::new(crate::progress::JobCheckpointer::new(
            job.id,
            job.attempts,
            state.storage.clone(),
        )),
        restored,
        vcr,
        artifacts_dir,
    };

    let timeout = Duration::from_secs(state.config.worker.job_timeout_secs);
    // Panic containment: an app that unwinds must fail its job on THIS tick,
    // through the normal attempt-fenced `fail()` path, carrying the panic
    // payload as the error. Without this the spawned task just dies, the row
    // stays `running`, and `stale_after_secs` later the reaper mislabels it
    // "lease expired (heartbeat stale)".
    //
    // AssertUnwindSafe: the state a panicking app could leave torn is its own
    // (`ctx` is moved in and dropped with the future); everything the worker
    // touches afterwards — the storage handle, the job row, the event bus — is
    // read fresh from `state`, never from the app's half-finished values.
    //
    // This CANNOT catch a non-yielding wedge: an app spinning without an
    // `.await` never returns from `poll`, so neither this branch nor the
    // heartbeat branch above is ever reached and no unwind happens at all. The
    // heartbeat goes stale and the reaper (`reap_once`) remains the backstop for
    // that class — as it does for a hard abort (`process::exit`, OOM, SIGKILL).
    let run = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(app.run(ctx)));
    install_panic_location_hook();
    tokio::pin!(run);
    let sleep = tokio::time::sleep(timeout);
    tokio::pin!(sleep);
    // Heartbeat interval: fires only while the app future yields (awaits). If the
    // app wedges in a non-yielding loop this select can't reach the heartbeat
    // branch, so the heartbeat goes stale and the reaper recovers the job — while
    // a slow-but-alive job keeps beating and is never reaped.
    let hb_secs = state.config.worker.heartbeat_secs;
    let mut heartbeat = (hb_secs > 0).then(|| {
        let mut i = tokio::time::interval(Duration::from_secs(hb_secs));
        i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        i
    });
    // Race the app future against the wall-clock timeout, the cancel token, and
    // the heartbeat tick.
    let run_started = std::time::Instant::now();
    let outcome = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break Outcome::Cancelled,
            _ = &mut sleep => break Outcome::TimedOut,
            res = &mut run => break match res {
                Ok(res) => Outcome::Finished(res),
                Err(payload) => {
                    Outcome::Panicked(panic_error(&*payload, take_panic_location().as_deref()))
                }
            },
            _ = maybe_tick(&mut heartbeat) => {
                let _ = state.storage.heartbeat(job.id, job.attempts).await;
            }
        }
    };
    let run_ms = elapsed_ms(run_started);

    match outcome {
        Outcome::Cancelled if state.shutdown.is_cancelled() => {
            // Shutdown suspend, not a user cancel: `drain` fired the per-job
            // cancel tokens ahead of its deadline so in-flight jobs stop *now*,
            // while their latest durable checkpoint (written throttled during
            // the run) survives. Re-queue with attempts headroom — mirroring
            // `reset`, not `cancel` — so the job resumes from its checkpoint on
            // the next boot instead of being abandoned mid-work.
            match state.storage.reset(job.id).await {
                Ok(Some(_)) => {
                    warn!(job = %job.id, "job suspended for shutdown; re-queued to resume from checkpoint");
                    publish(&state, JobEvent::new(job.id, job.app.clone(), "queued"));
                }
                Ok(None) => {}
                Err(e) => error!(job = %job.id, "failed to persist shutdown suspend: {e}"),
            }
            return;
        }
        Outcome::Cancelled => {
            // Cooperative cancel of a running job: mark it cancelled (not failed)
            // and emit the terminal event, mirroring the queued-cancel path
            // (event only, no result webhook). Guarded, so a job that raced to a
            // terminal state or was reset first is left untouched.
            match state.storage.cancel_running(job.id, job.attempts).await {
                Ok(true) => {
                    warn!(job = %job.id, "running job cancelled");
                    if let Err(e) = state.storage.clear_checkpoint(job.id).await {
                        warn!(job = %job.id, "cancelled-job checkpoint clear failed: {e}");
                    }
                    publish(&state, JobEvent::new(job.id, job.app.clone(), "cancelled"));
                }
                Ok(false) => {}
                Err(e) => error!(job = %job.id, "failed to persist cancellation: {e}"),
            }
            return;
        }
        Outcome::Finished(Ok(mut result)) => {
            // Mark replay runs on the stored result: a replayed job's output is
            // derived from recorded bytes, not the live web, and anyone reading
            // it later must be able to tell.
            if let (Some(replay_id), Value::Object(map)) = (replay_of, &mut result) {
                map.insert("vcr_replay_of".into(), Value::String(replay_id.to_string()));
            }
            // Information economics (M04): parse the result's UpsertSummary-shaped
            // counts BEFORE `complete` consumes it. Recorded only if the
            // completion lands (below) — a stale attempt's numbers are not yield.
            let yields = pumper_core::extract_yields(&result);
            match state.storage.complete(job.id, job.attempts, result).await {
                Ok(true) => {
                    info!(job = %job.id, "job succeeded");
                    // A completed job's checkpoint is spent state; drop it so
                    // the table only holds resumable (in-progress) work.
                    if let Err(e) = state.storage.clear_checkpoint(job.id).await {
                        warn!(job = %job.id, "checkpoint clear failed: {e}");
                    }
                    // Persist this run's yield next to its cost, so /economics
                    // can price the records. Best-effort telemetry, fail-open —
                    // accounting never touches a job's outcome.
                    if !yields.is_empty() {
                        if let Err(e) = state
                            .storage
                            .record_job_yield(job.id, &job.app, &yields)
                            .await
                        {
                            warn!(job = %job.id, "job-yield record failed: {e}");
                        }
                    }
                }
                Ok(false) => {
                    // The job was reset/reaped mid-run and re-claimed elsewhere;
                    // this run's result is stale. Drop it (no side effects, no
                    // finalize) so the live attempt owns the outcome.
                    warn!(job = %job.id, "completion discarded: job was reset or reaped mid-run");
                    return;
                }
                Err(e) => {
                    // The row is still `running`: this task never took ownership
                    // of an outcome, so it must not announce one. The reaper is
                    // the recovery path (as it is for a hard abort).
                    error!(job = %job.id, "failed to persist result: {e}; skipping fan-out");
                    return;
                }
            }
            // Everything past this point is DERIVED and OUTBOUND work — search
            // indexing, hooks, alerts, the terminal event. It used to run right
            // here, still holding this job's worker permit, so a slow index or a
            // large materialization burned a scrape slot. It now runs on the
            // bounded fan-out pool instead (inline when the pool is full or
            // disabled — never dropped). See `crate::fanout`.
            let stages = StageWatch::new(job.attempts, run_ms, started);
            let (st, jb) = (state.clone(), job.clone());
            state
                .fanout
                .run("finalize", job.id, async move {
                    finalize_fanout(st, jb, stages).await;
                })
                .await;
            return;
        }
        Outcome::Finished(Err(e)) => {
            warn!(job = %job.id, error = %e, "job failed");
            match state
                .storage
                .fail(job.id, job.attempts, &e.to_string())
                .await
            {
                Ok(Some(JobStatus::Queued)) => {
                    // Not terminal — retry pending; wake the worker and return.
                    state.notify.notify_one();
                    return;
                }
                // Stale (job reset/reaped mid-run): the live attempt owns it.
                Ok(None) => return,
                Ok(Some(_)) => {}
                Err(pe) => error!(job = %job.id, "failed to persist failure: {pe}"),
            }
        }
        Outcome::Panicked(error) => {
            // Same attempt-fenced path as an app-returned error — fencing,
            // backoff and retry semantics are identical; only the error text
            // (and its `panicked: ` marker) differs.
            error!(job = %job.id, %error, "job panicked");
            match state.storage.fail(job.id, job.attempts, &error).await {
                Ok(Some(JobStatus::Queued)) => {
                    state.notify.notify_one();
                    return;
                }
                // Stale (job reset/reaped mid-run): the live attempt owns it.
                Ok(None) => return,
                Ok(Some(_)) => {}
                Err(pe) => error!(job = %job.id, "failed to persist panic: {pe}"),
            }
        }
        Outcome::TimedOut => {
            warn!(job = %job.id, timeout_secs = timeout.as_secs(), "job timed out");
            match state
                .storage
                .fail(
                    job.id,
                    job.attempts,
                    &format!("timed out after {}s", timeout.as_secs()),
                )
                .await
            {
                Ok(Some(JobStatus::Queued)) => {
                    state.notify.notify_one();
                    return;
                }
                Ok(None) => return,
                _ => {}
            }
        }
    }
    finalize(&state, job.id).await;
}

/// Milliseconds since `t`, saturating — a duration can't be negative and an
/// `i64` of milliseconds covers ~292 million years, so this never wraps.
fn elapsed_ms(t: std::time::Instant) -> i64 {
    t.elapsed().as_millis().min(i64::MAX as u128) as i64
}

/// The stages a completed run's wall-clock is attributed to.
#[derive(Debug, Clone, Copy)]
enum Stage {
    Index,
    Hooks,
    Alerts,
}

/// Accumulates one run's per-stage wall clock into a [`pumper_core::JobStages`].
///
/// A stage that never runs stays `None` — "this run didn't get there" — and is
/// never filled in with a 0 that would read as "it was free". Only stages that
/// actually executed report a number.
struct StageWatch {
    started: std::time::Instant,
    stages: pumper_core::JobStages,
}

impl StageWatch {
    fn new(attempt: i64, run_ms: i64, started: std::time::Instant) -> Self {
        Self {
            started,
            stages: pumper_core::JobStages {
                attempt,
                run_ms: Some(run_ms),
                ..Default::default()
            },
        }
    }

    /// Runs `fut`, recording how long it took against `which`.
    async fn time<F: std::future::Future>(&mut self, which: Stage, fut: F) -> F::Output {
        let at = std::time::Instant::now();
        let out = fut.await;
        let ms = elapsed_ms(at);
        match which {
            Stage::Index => self.stages.index_ms = Some(ms),
            Stage::Hooks => self.stages.hooks_ms = Some(ms),
            Stage::Alerts => self.stages.alerts_ms = Some(ms),
        }
        out
    }

    /// Closes the total span and hands back the record to persist.
    fn finish(mut self) -> pumper_core::JobStages {
        self.stages.total_ms = Some(elapsed_ms(self.started));
        self.stages
    }
}

/// Whether the outcome this task wrote is still the job's outcome — the fence
/// that keeps a stale run's fan-out from firing.
///
/// The anti-pattern: a job reset or reaped mid-run is re-claimed elsewhere, and
/// the abandoned task then indexes, webhooks and triggers on behalf of a run
/// that no longer owns the job. `complete()` already fences the *write* on
/// `(status, attempts)`; because the fan-out now runs on its own task, the same
/// fence has to be re-checked when that task starts.
///
/// `None` (the row is gone) and any attempt/status mismatch both mean "not
/// ours" — fail closed, since a delivered webhook cannot be recalled.
fn fanout_owns_outcome(row: Option<&Job>, attempt: i64) -> bool {
    matches!(row, Some(j) if j.attempts == attempt && j.status == JobStatus::Succeeded)
}

/// Everything a succeeded job does after its result is persisted, run as ONE
/// unit off the worker's concurrency permit (see [`crate::fanout`]).
///
/// The ordering inside this function is load-bearing and guarded twice (a
/// behavioural e2e and a structural inventory test over these call sites):
/// `suppress_unhealthy` → `enforce_contracts` → `notify_watches` →
/// `fire_dataset_triggers`. The gates sit above the hooks because a delivered
/// webhook cannot be recalled. Moving this block onto another task does not
/// weaken that: it is the same sequential block, executed in the same order, on
/// one task — and it now begins with an explicit staleness fence
/// ([`fanout_owns_outcome`]) that the inline version got for free from having
/// just written the completion itself.
async fn finalize_fanout(state: AppState, job: Job, mut stages: StageWatch) {
    let row = match state.storage.get(job.id).await {
        Ok(row) => row,
        Err(e) => {
            // Fail closed: unable to prove this run still owns the job.
            warn!(job = %job.id, "fan-out skipped: job re-read failed: {e}");
            return;
        }
    };
    if !fanout_owns_outcome(row.as_ref(), job.attempts) {
        warn!(
            job = %job.id, attempt = job.attempts,
            "fan-out discarded: the job was reset or reaped and no longer carries this run's \
             outcome"
        );
        return;
    }
    let result = row.and_then(|j| j.result).unwrap_or(Value::Null);

    // Index the result into full-text search. Apps whose result stays compact
    // (counts, not arrays) can additionally name datasets to index per-record
    // via `index_datasets` — see `dataset_search_docs`.
    let index_specs = crate::datahub::index_dataset_specs(&result);
    stages
        .time(Stage::Index, async {
            let mut docs = search_docs(&job.app, job.id, &result);
            // Ghost-doc GC: identity-less job-result docs embed the job id, so
            // every run used to mint a permanent new set. Sweep the app's prior
            // snapshot BEFORE adding this run's (delete-then-add in opstamp
            // order, so the new docs survive their own sweep).
            if sweeps_prior_job_snapshot(&docs) {
                if let Err(e) = state
                    .search
                    .delete_dataset(&job.app, JOB_RESULT_DATASET)
                    .await
                {
                    warn!(job = %job.id, "search job-snapshot sweep failed: {e}");
                }
            }
            let (dataset_docs, dataset_deletes) = dataset_search_docs(&state, &job, &result).await;
            docs.extend(dataset_docs);
            if let Err(e) = state.search.index(docs).await {
                warn!(job = %job.id, "search index failed: {e}");
            }
            // Removed records (this run's `removed` revisions) are dropped from
            // the index rather than left as stale hits.
            if !dataset_deletes.is_empty() {
                if let Err(e) = state.search.delete_ids(&dataset_deletes).await {
                    warn!(job = %job.id, "search delete failed: {e}");
                }
            }
        })
        .await;

    stages
        .time(Stage::Hooks, async {
            // One revision batch for this run, shared by watches + triggers.
            let changes = load_run_changes(&state, &job).await;
            if changes.is_empty() {
                return;
            }
            let mut by_dataset = group_by_dataset(&changes);
            // A degrading source never pushes. A webhook is irreversible once
            // sent, so a source we no longer stand behind is dropped here,
            // before the hooks — this ordering IS the enforcement, and if it
            // moves below them the guarantee is gone.
            suppress_unhealthy(&state, &job.app, &mut by_dataset).await;
            // Declared data contracts (M20): the same choke point, the
            // declared complement to the inferred gate above. Verdicts are
            // always recorded; datasets are dropped only under
            // `[contracts] enforce = true`.
            enforce_contracts(&state, &job, &mut by_dataset);
            notify_watches(&state, &job, &by_dataset).await;
            crate::triggers::fire_dataset_triggers(
                &state,
                &job,
                crate::triggers::DatasetBatch::Run,
                &by_dataset,
            )
            .await;
        })
        .await;

    // Saved searches are scoped against every app this run put documents
    // under — the job's own app AND the virtual apps named by
    // `index_datasets` (e.g. `grants`), which is the app those docs
    // actually carry. See `run_indexed_apps`.
    let indexed_apps = run_indexed_apps(&job.app, &index_specs);
    stages
        .time(
            Stage::Alerts,
            notify_saved_searches(&state, &job, &indexed_apps),
        )
        .await;
    // Metadata shadow: push this run's dataset entities/lineage/freshness
    // to DataHub. Off-slot (on this pool) and fail-open — a down GMS never
    // touches jobs, and a shutdown drains or loudly counts the emission
    // rather than dropping it.
    crate::datahub::on_job_success(&state, &job, index_specs).await;

    // Stamp where the wall-clock went before the terminal event, so the event
    // (and `GET /jobs/{id}/receipt`) can carry it. Best-effort telemetry.
    let stages = stages.finish();
    if let Err(e) = state
        .storage
        .record_job_stages(job.id, &job.app, &stages)
        .await
    {
        warn!(job = %job.id, "stage-timing record failed: {e}");
    }
    finalize_with_stages(&state, job.id, Some(stages)).await;
}

/// Parses the VCR enqueue params out of a job's params object:
/// `record: true` and/or `replay_of: "<job uuid>"`. Pure so it's unit-testable.
/// Errors (a malformed uuid, or both modes at once) are permanent-fail
/// messages — a job with contradictory or unusable VCR intent must not run.
fn vcr_params(params: &Value) -> Result<(bool, Option<Uuid>), String> {
    let record = params
        .get("record")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let replay_of = match params.get("replay_of") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(
            Uuid::parse_str(s)
                .map_err(|e| format!("vcr: replay_of is not a job uuid ({s:?}): {e}"))?,
        ),
        Some(other) => {
            return Err(format!(
                "vcr: replay_of must be a job-id string, got {other}"
            ))
        }
    };
    if record && replay_of.is_some() {
        return Err(
            "vcr: a job cannot both record and replay — a replay's cassette IS the record"
                .to_string(),
        );
    }
    Ok((record, replay_of))
}

/// Ticks the heartbeat interval when enabled, or waits forever when it isn't —
/// so the heartbeat select branch simply never fires with heartbeating disabled.
async fn maybe_tick(hb: &mut Option<tokio::time::Interval>) {
    match hb {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// One reaper pass: re-queues (or permanently fails) running jobs whose lease
/// (heartbeat) has gone stale, then routes each outcome the same way the worker
/// would have. Re-queued jobs wake the worker; permanently-failed ones go
/// through `finalize` so their callback + terminal triggers fire like any other
/// failure. Piggybacks the scheduler tick (`stale_after_secs == 0` disables it).
pub async fn reap_once(state: &AppState) {
    // The trigger decision ledger rides this tick (see `prune_trigger_ledger`)
    // — before the `stale_after_secs == 0` early return, because a deployment
    // with the reaper disabled still writes decisions.
    prune_trigger_ledger(state).await;
    let stale = state.config.worker.stale_after_secs;
    if stale == 0 {
        return;
    }
    let reaped = match state.storage.reap_stale(stale as i64).await {
        Ok(reaped) => reaped,
        Err(e) => {
            error!("stuck-job reaper failed: {e}");
            return;
        }
    };
    for (id, app, status) in reaped {
        match status {
            JobStatus::Queued => {
                warn!(job = %id, %app, "reaped stale job: re-queued for another attempt");
                publish(state, JobEvent::new(id, app, "queued"));
                state.notify.notify_one();
            }
            _ => {
                warn!(job = %id, %app, "reaped stale job: attempts exhausted, failing permanently");
                finalize(state, id).await;
            }
        }
    }
}

/// How long a trigger decision row lives. The ledger is DIAGNOSTIC — one row
/// per candidate edge per source event, negatives included — which is a far
/// higher write rate than the evidence ledgers in `LEDGER_TABLES`, and losing an
/// old row loses no history anyone can act on. So unlike those it is bounded by
/// default (they are opt-in precisely because deleting them IS data loss).
const TRIGGER_RUN_RETENTION_DAYS: u64 = 14;

/// How often the prune actually runs. The reaper tick is per
/// `schedule_tick_secs` (seconds); a retention sweep at that rate would be pure
/// write amplification for a bound measured in days.
const TRIGGER_RUN_PRUNE_EVERY: Duration = Duration::from_secs(3600);

/// Last completed prune, process-local. A restart re-arms it, which is the same
/// posture the ingress rate-limit buckets take: for a janitorial bound, an extra
/// sweep after a restart is free and a missed one is not.
static LAST_TRIGGER_RUN_PRUNE: std::sync::Mutex<Option<std::time::Instant>> =
    std::sync::Mutex::new(None);

/// Whether the ledger prune is due. The anti-pattern: a sweep on every reaper
/// tick, i.e. a `DELETE` every few seconds against a table bounded in days.
fn prune_is_due(
    last: Option<std::time::Instant>,
    now: std::time::Instant,
    every: Duration,
) -> bool {
    match last {
        None => true, // first tick after boot
        Some(t) => now.duration_since(t) >= every,
    }
}

/// Bounds `trigger_runs` by age, at most once per [`TRIGGER_RUN_PRUNE_EVERY`].
/// Fail-open and quiet: a ledger that could not be pruned is a disk-space
/// concern, never a reason to disturb the queue.
async fn prune_trigger_ledger(state: &AppState) {
    let now = std::time::Instant::now();
    {
        // The guard is dropped before the await: a std Mutex must never be held
        // across one, and the claim is "this task owns the next sweep".
        let mut last = LAST_TRIGGER_RUN_PRUNE.lock().expect("prune clock poisoned");
        if !prune_is_due(*last, now, TRIGGER_RUN_PRUNE_EVERY) {
            return;
        }
        *last = Some(now);
    }
    match state
        .storage
        .prune_trigger_runs(TRIGGER_RUN_RETENTION_DAYS)
        .await
    {
        Ok(n) if n > 0 => info!(
            pruned = n,
            days = TRIGGER_RUN_RETENTION_DAYS,
            "pruned old trigger decisions"
        ),
        Ok(_) => {}
        Err(e) => warn!("trigger decision ledger prune failed: {e}"),
    }
}

/// Everything this run wrote: revisions after the attempt's start. Fail-open
/// (empty on error) — side effects never block the job outcome.
async fn load_run_changes(state: &AppState, job: &Job) -> Vec<pumper_core::Revision> {
    match state
        // Unfiltered by trust: push suppression is decided per source in
        // `suppress_unhealthy`, not by filtering rows, so a partially-degraded
        // app's healthy datasets still deliver.
        .datasets
        .changes_since(&job.app, None, job.started_at, 1000, None)
        .await
    {
        Ok(changes) => changes,
        Err(e) => {
            warn!(job = %job.id, "failed to load run changes: {e}");
            Vec::new()
        }
    }
}

fn group_by_dataset(
    changes: &[pumper_core::Revision],
) -> HashMap<&str, Vec<&pumper_core::Revision>> {
    let mut by_dataset: HashMap<&str, Vec<&pumper_core::Revision>> = HashMap::new();
    for rev in changes {
        by_dataset
            .entry(rev.dataset.as_str())
            .or_default()
            .push(rev);
    }
    by_dataset
}

/// Drops the datasets whose source health suppresses outbound pushes, so watches
/// and triggers never see them.
///
/// Per-dataset rather than per-job: one app can serve several datasets, and a
/// break in one must not silence the others. A no-op unless
/// `[resilience] enforce` is on.
async fn suppress_unhealthy(
    state: &AppState,
    app: &str,
    by_dataset: &mut HashMap<&str, Vec<&pumper_core::Revision>>,
) {
    if !state.health.enforcing() {
        return;
    }
    let datasets: Vec<&str> = by_dataset.keys().copied().collect();
    for dataset in datasets {
        let health = state.health.enforced_state(app, dataset).await;
        if health.suppresses_pushes() {
            warn!(
                %app,
                dataset,
                state = health.as_str(),
                "pushes suppressed: source health is degraded, and a delivered webhook \
                 cannot be recalled"
            );
            by_dataset.remove(dataset);
        }
    }
}

/// Evaluates declared data contracts (`[source.contract]` in the catalog) over
/// this run's revisions, at the same choke point where `suppress_unhealthy`
/// gates pushes — before any webhook or trigger fires.
///
/// Semantics: datasets without a declared contract are untouched. For each
/// contracted dataset the verdict is `pass` (no violations), `warn`
/// (violations, `[contracts] enforce = false` — recorded and surfaced, nothing
/// gated) or `block` (violations with enforcement on — the dataset is removed
/// from the batch so watches/triggers never see it; the data itself is already
/// stored and stays queryable). The latest verdict per `<app>/<dataset>` is
/// kept in memory and surfaced on `/catalog/health` and `/sources`. Fail-open:
/// an unreadable catalog skips evaluation, it never blocks delivery.
fn enforce_contracts(
    state: &AppState,
    job: &Job,
    by_dataset: &mut HashMap<&str, Vec<&pumper_core::Revision>>,
) {
    let catalog = match pumper_core::Catalog::load() {
        Ok(c) => c,
        Err(e) => {
            warn!(job = %job.id, "contract check skipped (catalog unreadable): {e}");
            return;
        }
    };
    let enforce = state.config.contracts.enforce;
    let datasets: Vec<&str> = by_dataset.keys().copied().collect();
    for dataset in datasets {
        let Some((_, contract)) = catalog.contract_for(&job.app, dataset) else {
            continue;
        };
        let revs = &by_dataset[dataset];
        let records: Vec<&serde_json::Value> = revs
            .iter()
            .filter(|r| r.change != "removed")
            .filter_map(|r| r.data.as_ref())
            .collect();
        let removed = revs.iter().filter(|r| r.change == "removed").count();
        let violations = contract.evaluate(&records, removed);
        let verdict = pumper_core::ContractVerdict::from_violations(&violations, enforce);
        let outcome = serde_json::json!({
            "verdict": verdict.as_str(),
            "violations": violations,
            "records": records.len(),
            "removed": removed,
            "enforced": enforce,
            "job_id": job.id,
            "checked_at": pumper_core::datasets::ts(chrono::Utc::now()),
        });
        state
            .contract_verdicts
            .lock()
            .expect("contract verdict lock")
            .insert(format!("{}/{}", job.app, dataset), outcome);
        match verdict {
            pumper_core::ContractVerdict::Pass => {}
            pumper_core::ContractVerdict::Warn => warn!(
                app = %job.app,
                dataset,
                ?violations,
                "data contract violated (warn-only: [contracts] enforce = false)"
            ),
            pumper_core::ContractVerdict::Block => {
                warn!(
                    app = %job.app,
                    dataset,
                    ?violations,
                    "pushes suppressed: declared data contract violated"
                );
                by_dataset.remove(dataset);
            }
        }
    }
}

/// Fires `dataset.changed` webhooks at every enabled watch whose dataset saw
/// new/changed/removed revisions during this job run. Best-effort: delivery
/// failures never affect the job outcome.
async fn notify_watches(
    state: &AppState,
    job: &Job,
    by_dataset: &HashMap<&str, Vec<&pumper_core::Revision>>,
) {
    let watches = match state.storage.enabled_watches(&job.app).await {
        Ok(w) if !w.is_empty() => w,
        Ok(_) => return,
        Err(e) => {
            warn!(job = %job.id, "failed to load watches: {e}");
            return;
        }
    };

    for (dataset, revs) in by_dataset {
        for watch in watches.iter().filter(|w| w.covers(dataset)) {
            let payload = serde_json::json!({
                "event": "dataset.changed",
                "watch_id": watch.id,
                "job_id": job.id,
                "app": job.app,
                "dataset": dataset,
                "count": revs.len(),
                "changes": revs,
            });
            webhook::dispatch_change(state, watch.clone(), payload).await;
        }
    }
}

/// Every app namespace this run's documents landed under: the job's own app,
/// plus each **virtual** app named by the result's `index_datasets` specs.
///
/// The two are genuinely different namespaces. `dataset_search_docs` indexes the
/// named datasets under the spec's `app` verbatim, so a `ca-grants` run publishes
/// its per-opportunity docs as app `grants` (`grants_common::UNIFIED_APP`). A
/// saved search scoped to `grants` — the only scope that matches how those docs
/// were indexed — must therefore be considered in scope for that run.
///
/// Order-preserving and de-duplicated (job app first); empty app names are
/// dropped, since an empty namespace matches nothing.
fn run_indexed_apps(job_app: &str, index_specs: &[(String, String)]) -> Vec<String> {
    let mut apps: Vec<String> = Vec::with_capacity(1 + index_specs.len());
    for app in std::iter::once(job_app).chain(index_specs.iter().map(|(a, _)| a.as_str())) {
        if !app.is_empty() && !apps.iter().any(|seen| seen == app) {
            apps.push(app.to_string());
        }
    }
    apps
}

/// Whether a saved search's `app` filter covers a run that indexed under
/// `indexed_apps` (see [`run_indexed_apps`]).
///
/// Unscoped (`None`) searches always run. A scoped search runs only when this
/// run actually wrote documents under that exact app — deliberately NOT widened
/// to "any app", so a search pinned to one namespace still ignores unrelated
/// runs.
fn search_covers_run(search_app: Option<&str>, indexed_apps: &[String]) -> bool {
    match search_app {
        None => true,
        Some(app) => indexed_apps.iter().any(|indexed| indexed == app),
    }
}

/// Runs enabled saved searches after a job's results were indexed, alerting
/// each NEW match exactly once (`saved_search_seen` dedup). Scoped by
/// [`search_covers_run`] over [`run_indexed_apps`] — the job's app plus the
/// virtual apps it indexed datasets under.
///
/// When several source apps feed one virtual app (`grants-gov` and `ca-grants`
/// both publishing into `grants`), each run re-evaluates the same search; the
/// `claim_unseen` dedupe is what keeps that from alerting twice on one document.
async fn notify_saved_searches(state: &AppState, job: &Job, indexed_apps: &[String]) {
    let searches = match state.storage.list_saved_searches(true).await {
        Ok(list) if !list.is_empty() => list,
        Ok(_) => return,
        Err(e) => {
            warn!(job = %job.id, "failed to load saved searches: {e}");
            return;
        }
    };
    // This job's just-indexed docs may still be uncommitted (index() defers its
    // commit). Force one now so standing alerts see them this run instead of
    // missing them until the next commit. Only jobs that actually have saved
    // searches pay this — the amortization holds for the rest.
    if let Err(e) = state.search.flush().await {
        warn!(job = %job.id, "search flush before saved-search scan failed: {e}");
    }
    for search in searches {
        if !search_covers_run(search.app.as_deref(), indexed_apps) {
            // Silence is this bug's signature: a mis-scoped standing alert used
            // to skip every run without a word. Say which filter missed what.
            debug!(
                search = %search.id, job = %job.id,
                filter_app = search.app.as_deref().unwrap_or(""),
                indexed_apps = ?indexed_apps,
                "saved search skipped: its app filter is not among the apps this run indexed under"
            );
            continue;
        }
        // Materialization (M13 "queries as datasets") runs regardless of whether
        // the alert path below finds anything NEW — an unchanged-but-reordered
        // or shrunken result set still has to refresh (and tombstone) the view.
        if let Some(mat) = search.materialize.clone() {
            materialize_saved_search(state, job, &search, &mat).await;
        }
        let req = pumper_core::SearchRequest {
            q: search.query.clone(),
            limit: 50,
            app: search.app.clone(),
            dataset: search.dataset.clone(),
            fuzzy: false,
            ..Default::default()
        };
        let results = match state.search.query(req).await {
            Ok(results) => results,
            Err(e) => {
                warn!(search = %search.id, "saved search query failed: {e}");
                continue;
            }
        };
        let ids: Vec<String> = results.hits.iter().map(|h| h.id.clone()).collect();
        if ids.is_empty() {
            debug!(
                search = %search.id, job = %job.id, query = %search.query,
                filter_app = search.app.as_deref().unwrap_or(""),
                filter_dataset = search.dataset.as_deref().unwrap_or(""),
                "saved search ran but matched no documents"
            );
            continue;
        }
        let unseen = match state.storage.claim_unseen(&search.id, &ids).await {
            Ok(unseen) if !unseen.is_empty() => unseen,
            Ok(_) => {
                debug!(
                    search = %search.id, job = %job.id, hits = ids.len(),
                    "saved search matched only already-alerted documents; no webhook"
                );
                continue;
            }
            Err(e) => {
                warn!(search = %search.id, "saved search dedup failed: {e}");
                continue;
            }
        };
        let matches: Vec<_> = results
            .hits
            .iter()
            .filter(|h| unseen.contains(&h.id))
            .collect();
        let payload = serde_json::json!({
            "event": "search.matched",
            "search_id": search.id,
            "query": search.query,
            "job_id": job.id,
            "app": job.app,
            "count": matches.len(),
            "matches": matches,
        });
        webhook::dispatch_event(
            state,
            "search",
            &search.id,
            &search.url,
            "search.matched",
            &payload,
            search.secret.clone(),
        )
        .await;
    }
}

/// Materializes one saved search's current result set into its target dataset
/// (M13 "queries as datasets"): runs the query (facets OFF — the runner reads
/// hits, not breakdowns; capped by `[search] max_materialize_results`), upserts
/// the hits as view records, and tombstones records that fell out of the result
/// set (`detect_removed` semantics — an EMPTY result set never wipes the view).
/// The view's deltas then feed the same push machinery as any dataset: watches
/// and dataset triggers fire here under the VIEW's app, because the run-scoped
/// pass in `execute` only covers `job.app`. Best-effort throughout — a broken
/// view never touches the job outcome or the alert path.
async fn materialize_saved_search(
    state: &AppState,
    job: &Job,
    search: &pumper_core::SavedSearch,
    mat: &pumper_core::SearchMaterialize,
) {
    let cap = state.config.search.max_materialize_results.max(1);
    let req = pumper_core::SearchRequest {
        q: search.query.clone(),
        limit: cap,
        app: search.app.clone(),
        dataset: search.dataset.clone(),
        ..Default::default()
    };
    // Everything the materialization writes is strictly after this instant;
    // `changes_since` below replays exactly this run's view deltas.
    let mark = chrono::Utc::now();
    let results = match state.search.query(req).await {
        Ok(results) => results,
        Err(e) => {
            warn!(search = %search.id, "materialize query failed: {e}");
            return;
        }
    };
    match state
        .datasets
        .materialize_search_hits(&mat.app, &mat.dataset, &results.hits, cap)
        .await
    {
        Ok((summary, removed)) => info!(
            search = %search.id, app = %mat.app, dataset = %mat.dataset,
            new = summary.new.len(), changed = summary.changed.len(),
            unchanged = summary.unchanged, removed = removed.len(),
            "saved search materialized"
        ),
        Err(e) => {
            warn!(search = %search.id, "saved search materialization failed: {e}");
            return;
        }
    }
    let changes = match state
        .datasets
        .changes_since(
            &mat.app,
            Some(&mat.dataset),
            Some(mark),
            cap as i64 * 2,
            None,
        )
        .await
    {
        Ok(changes) => changes,
        Err(e) => {
            warn!(search = %search.id, "failed to load view deltas: {e}");
            return;
        }
    };
    if changes.is_empty() {
        return;
    }
    // Watches and dataset triggers are looked up per app; re-badge the job so
    // the lookups target the view's app while provenance keeps the real job id.
    let mut view_job = job.clone();
    view_job.app = mat.app.clone();
    let by_dataset = group_by_dataset(&changes);
    notify_watches(state, &view_job, &by_dataset).await;
    // The view's hops ride the SOURCE job's id (provenance), so their dedup keys
    // are scoped by the saved search — otherwise a view whose target app is the
    // job's own app collides with the run's own fan-out hop.
    crate::triggers::fire_dataset_triggers(
        state,
        &view_job,
        crate::triggers::DatasetBatch::View(&search.id),
        &by_dataset,
    )
    .await;
}

/// Emits the terminal event and fires the result webhook, if configured.
async fn finalize(state: &AppState, id: uuid::Uuid) {
    finalize_with_stages(state, id, None).await;
}

/// [`finalize`] plus this run's stage timings, when the caller measured them.
/// Only the success path (the fan-out) does — a job that failed before its
/// fan-out has no index/hooks/alerts numbers, and `None` says so rather than
/// stamping zeros.
async fn finalize_with_stages(
    state: &AppState,
    id: uuid::Uuid,
    stages: Option<pumper_core::JobStages>,
) {
    // In-flight progress is done being useful once the job is terminal; drop the
    // buffered snapshot so the map doesn't grow with completed jobs.
    state.progress.clear(&id);
    let Ok(Some(job)) = state.storage.get(id).await else {
        return;
    };
    // A terminal job's checkpoint can never be resumed from again (retry grants
    // a fresh attempt lineage and complete/fail paths land here) — drop it so
    // the checkpoints table only holds live, resumable work.
    if matches!(job.status, JobStatus::Failed | JobStatus::Cancelled) {
        if let Err(e) = state.storage.clear_checkpoint(job.id).await {
            warn!(job = %job.id, "terminal checkpoint clear failed: {e}");
        }
    }
    let mut event = JobEvent::new(job.id, job.app.clone(), job.status.as_str());
    event.result = job.result.clone();
    event.error = job.error.clone();
    event.stages = stages;
    publish(state, event);
    webhook::dispatch(state, job.clone()).await;
    // Permanent-failure firehose: a job that exhausted its attempts (app error,
    // timeout, or a reaped stale lease — all land here via `finalize`) notifies
    // the global `[webhooks] failure_url` subscriber, if configured. Retryable
    // requeues never reach `finalize`, so this is permanent failures only.
    if job.status == JobStatus::Failed {
        if let Some(url) = &state.config.webhooks.failure_url {
            webhook::dispatch_failure(
                state,
                url,
                state.config.webhooks.failure_secret.clone(),
                &job,
            )
            .await;
        }
    }
    // Terminal-job triggers: the job's final status is an event other apps can
    // chain on (e.g. "when crawl succeeds, run extract").
    crate::triggers::fire_terminal_triggers(state, &job).await;
}

fn publish(state: &AppState, event: JobEvent) {
    // Stamps the event with its sequence id and buffers it for replay.
    state.events.emit(event);
}

/// Reserved dataset name stamped on job-result docs that have **no stable
/// identity** — the whole-result document and array elements with no url. Their
/// ids embed the job id, so every run mints fresh ones; they are therefore the
/// app's *latest run snapshot* and the previous run's set is swept before this
/// run's lands (see [`sweeps_prior_job_snapshot`]).
///
/// It is a reserved name, not a real dataset: nothing in `Datasets` stores under
/// it. Previously these docs stamped `dataset = app`, which put a dataset that
/// does not exist into `/search` facets and made `?dataset=<app>` a filter over
/// phantom rows.
pub(crate) const JOB_RESULT_DATASET: &str = "_job";

/// Reserved dataset name for job-result array elements that DO carry a url. Their
/// id is `<app>:<url>`, so re-runs upsert in place; they accumulate across runs as
/// a legitimate corpus and are never swept.
pub(crate) const JOB_RECORD_DATASET: &str = "_records";

/// Builds full-text search documents from a job's result: each element of a
/// `records`/`stories`/`items` array, or the whole result as one document.
fn search_docs(app: &str, job_id: Uuid, result: &Value) -> Vec<SearchDoc> {
    let mut docs = Vec::new();
    for key in ["records", "stories", "items"] {
        if let Some(arr) = result.get(key).and_then(Value::as_array) {
            for (i, rec) in arr.iter().enumerate() {
                docs.push(record_doc(app, job_id, i, rec));
            }
        }
    }
    if docs.is_empty() {
        docs.push(SearchDoc {
            id: format!("{app}:{job_id}"),
            app: app.to_string(),
            dataset: JOB_RESULT_DATASET.to_string(),
            url: String::new(),
            title: app.to_string(),
            body: result.to_string(),
            // Job-result docs carry no record timestamp — index at completion time.
            indexed_at: chrono::Utc::now().timestamp(),
        });
    }
    docs
}

/// True when this run mints at least one identity-less job-result doc — the only
/// case where the app's previous run left documents that nothing will ever
/// upsert or delete. The sweep is `delete_dataset(app, "_job")` issued *before*
/// this run's docs are indexed, so the index holds one run's snapshot per app
/// instead of one per run forever.
///
/// Deliberately NOT unconditional: `delete_dataset` commits, and a run whose
/// records are all url-keyed (they upsert, nothing accumulates) would pay that
/// fsync for nothing — undoing the deferred-commit amortization.
fn sweeps_prior_job_snapshot(docs: &[SearchDoc]) -> bool {
    docs.iter().any(|d| d.dataset == JOB_RESULT_DATASET)
}

/// Search docs + delete-ids for datasets the result names in `index_datasets`
/// (`[{ "app", "dataset" }]`), covering **only the records this run touched** —
/// not the whole dataset.
///
/// The old version re-read and re-indexed the entire named dataset on every job
/// completion (`list(.., 100_000)` → one doc per live record → delete+add each in
/// Tantivy). For a dataset like `grants/unified` (~5k rows, synced daily by two
/// apps) that is ~100–1000× write amplification for the handful of rows that
/// actually changed, and it grows with the corpus forever. Instead this reads the
/// dataset's revisions since the job started (the change feed already records
/// them), indexes the new/changed keys from their snapshots, and returns the
/// `removed` keys for deletion — cost O(changes), not O(corpus).
///
/// Note: because this no longer rebuilds the full index each run, a *wiped* index
/// (schema-drift rebuild) is refilled only as rows change; the standalone
/// backfill/reindex path (search finding #2) is the recovery for that case.
/// Doc ids are `<app>:<dataset>:<key>` (`SearchDoc::dataset_id`), so a re-index
/// replaces rather than duplicates. Failures are logged, not fatal — search is a
/// derived artifact and must never fail a completed job.
async fn dataset_search_docs(
    state: &AppState,
    job: &Job,
    result: &Value,
) -> (Vec<SearchDoc>, Vec<String>) {
    let Some(specs) = result.get("index_datasets").and_then(Value::as_array) else {
        return (Vec::new(), Vec::new());
    };
    let mut docs = Vec::new();
    let mut deletes = Vec::new();
    for spec in specs {
        let (Some(app), Some(dataset)) = (
            spec.get("app").and_then(Value::as_str),
            spec.get("dataset").and_then(Value::as_str),
        ) else {
            continue;
        };
        // A degrading source never poisons the search index. Because indexing is
        // delta-driven from the change feed, the skipped revisions are picked up
        // by the next healthy run's window — or by `search-backfill`.
        //
        // Scope note: this gate reads the health of the SPEC's own pair. For a
        // VIRTUAL pair — one no app ever calls `observe_extraction` for, such as
        // `("grants","unified")`, which three source apps write into — there is
        // no verdict to read and `enforced_state` always answers `Healthy`. Those
        // producers gate themselves by withholding the spec (see
        // `grants_common::indexable`); this check is the backstop for specs that
        // name a pair the health ladder actually judges.
        let health = state.health.enforced_state(app, dataset).await;
        if health.skips_search_index() {
            warn!(%app, %dataset, state = health.as_str(), "search indexing skipped: source health");
            continue;
        }
        // This dataset's revisions from this run. Scoped to `app`/`dataset`
        // explicitly because the indexed dataset (e.g. `grants/unified`) lives in a
        // different app namespace than the running app (e.g. `grants-gov`).
        let revs = match state
            .datasets
            .changes_since(app, Some(dataset), job.started_at, 100_000, None)
            .await
        {
            Ok(revs) => revs,
            Err(e) => {
                warn!("index_datasets: failed to load changes for {app}/{dataset}: {e}");
                continue;
            }
        };
        let (spec_docs, spec_deletes) = dataset_docs_from_revisions(app, dataset, &revs);
        docs.extend(spec_docs);
        deletes.extend(spec_deletes);
    }
    (docs, deletes)
}

/// The pure core of [`dataset_search_docs`]: one dataset's revision window →
/// `(docs to index, doc ids to delete)`.
///
/// `changes_since` returns revisions **newest-first**, so the first revision seen
/// for a key is its final state in this window; later ones are superseded and
/// skipped. Without that dedupe a key written twice in one run would be indexed
/// twice (harmless — same id, upsert) *or*, far worse, resurrected: a key that
/// was changed and then removed would emit both a delete and an add, and the add
/// would win.
fn dataset_docs_from_revisions(
    app: &str,
    dataset: &str,
    revs: &[pumper_core::Revision],
) -> (Vec<SearchDoc>, Vec<String>) {
    let mut docs = Vec::new();
    let mut deletes = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for rev in revs {
        if !seen.insert(rev.key.as_str()) {
            continue;
        }
        if rev.change == "removed" {
            deletes.push(SearchDoc::dataset_id(app, dataset, &rev.key));
        } else if let Some(data) = &rev.data {
            docs.push(SearchDoc::from_dataset_record(
                app,
                dataset,
                &rev.key,
                data,
                rev.created_at.timestamp(),
            ));
        }
    }
    (docs, deletes)
}

/// One search document for a stored dataset record (stable id, app+dataset
/// preserved for facets). Mirrors `record_doc`'s title/url field picking.
fn record_doc(app: &str, job_id: Uuid, i: usize, rec: &Value) -> SearchDoc {
    let url = ["_url", "url"]
        .iter()
        .find_map(|k| rec.get(*k).and_then(Value::as_str))
        .unwrap_or("")
        .to_string();
    let title = ["title", "name", "headline", "full_name"]
        .iter()
        .find_map(|k| rec.get(*k).and_then(Value::as_str))
        .unwrap_or("")
        .to_string();
    // A record with a url has a stable identity (upserts on re-run) and lives in
    // the durable `_records` namespace; one without is tied to this job id and
    // belongs to the sweepable latest-run snapshot.
    let (id, dataset) = if url.is_empty() {
        (format!("{app}:{job_id}:{i}"), JOB_RESULT_DATASET)
    } else {
        (format!("{app}:{url}"), JOB_RECORD_DATASET)
    };
    SearchDoc {
        id,
        app: app.to_string(),
        dataset: dataset.to_string(),
        url,
        title,
        body: rec.to_string(),
        // Job-result docs carry no record timestamp — index at completion time.
        indexed_at: chrono::Utc::now().timestamp(),
    }
}

#[cfg(test)]
mod job_result_doc_tests {
    use super::{
        record_doc, search_docs, sweeps_prior_job_snapshot, JOB_RECORD_DATASET, JOB_RESULT_DATASET,
    };
    use serde_json::json;
    use uuid::Uuid;

    /// The anti-pattern: stamping `dataset = <app>` on documents that came from a
    /// job result, which put a dataset that does not exist into `/search` facets
    /// and made `?dataset=<app>` a filter over phantom rows.
    #[test]
    fn job_docs_stamp_reserved_dataset_not_the_app_name() {
        let job = Uuid::new_v4();
        let whole = search_docs("hackernews", job, &json!({"count": 3}));
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0].app, "hackernews");
        assert_eq!(whole[0].dataset, JOB_RESULT_DATASET);
        assert_ne!(whole[0].dataset, "hackernews");

        let urlless = record_doc("hackernews", job, 0, &json!({"title": "no url"}));
        assert_eq!(urlless.dataset, JOB_RESULT_DATASET);
        let urlful = record_doc("hackernews", job, 0, &json!({"url": "https://x/1"}));
        assert_eq!(urlful.dataset, JOB_RECORD_DATASET);
        assert_ne!(urlful.dataset, "hackernews");
    }

    /// Identity: url-keyed docs upsert (stable id), identity-less docs carry the
    /// job id and are therefore per-run — which is exactly why they need sweeping.
    #[test]
    fn urlless_docs_are_per_run_not_upserting() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let rec = json!({"title": "no url"});
        assert_ne!(
            record_doc("app", a, 0, &rec).id,
            record_doc("app", b, 0, &rec).id,
            "identity-less docs differ per run — the ghost source"
        );
        let with_url = json!({"url": "https://x/1"});
        assert_eq!(
            record_doc("app", a, 0, &with_url).id,
            record_doc("app", b, 0, &with_url).id,
            "url-keyed docs upsert across runs"
        );
    }

    /// The anti-pattern: sweeping (and thus committing) on every run, including
    /// runs whose docs all upsert and leave nothing behind.
    #[test]
    fn sweeps_only_runs_that_mint_identityless_docs_not_url_keyed_ones() {
        let job = Uuid::new_v4();
        let url_keyed = search_docs(
            "hackernews",
            job,
            &json!({"stories": [{"url": "https://x/1"}, {"url": "https://x/2"}]}),
        );
        assert!(!sweeps_prior_job_snapshot(&url_keyed));

        let whole = search_docs("hackernews", job, &json!({"count": 3}));
        assert!(sweeps_prior_job_snapshot(&whole));

        let mixed = search_docs(
            "hackernews",
            job,
            &json!({"items": [{"url": "https://x/1"}, {"title": "no url"}]}),
        );
        assert!(
            sweeps_prior_job_snapshot(&mixed),
            "one identity-less doc in the batch is enough to have left ghosts"
        );
    }
}

#[cfg(test)]
mod dataset_search_doc_tests {
    use super::dataset_docs_from_revisions;
    use chrono::{TimeZone, Utc};
    use pumper_core::{Provenance, Revision};
    use serde_json::json;

    /// A revision as `changes_since` returns it (newest-first ordering is the
    /// caller's; `secs` only stamps `created_at`).
    fn rev(key: &str, change: &str, data: Option<serde_json::Value>, secs: i64) -> Revision {
        Revision {
            app: "grants".into(),
            dataset: "unified".into(),
            key: key.into(),
            revision: 1,
            change: change.into(),
            data,
            diff: None,
            created_at: Utc.timestamp_opt(secs, 0).unwrap(),
            trust: "stable".into(),
            provenance: Provenance::default(),
        }
    }

    #[test]
    fn new_and_changed_keys_are_indexed_from_their_revision_snapshot() {
        let revs = vec![
            rev(
                "a",
                "new",
                Some(json!({"title": "Alpha", "url": "https://x/a"})),
                100,
            ),
            rev("b", "changed", Some(json!({"name": "Beta"})), 200),
        ];
        let (docs, deletes) = dataset_docs_from_revisions("grants", "unified", &revs);
        assert!(deletes.is_empty());
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].id, "grants:unified:a");
        assert_eq!(docs[0].app, "grants");
        assert_eq!(docs[0].dataset, "unified");
        assert_eq!(docs[0].title, "Alpha");
        assert_eq!(docs[0].url, "https://x/a");
        assert_eq!(
            docs[0].indexed_at, 100,
            "recency comes from the revision, not wall clock"
        );
        assert_eq!(docs[1].title, "Beta", "title falls back through name");
    }

    /// The anti-pattern: a removed key left in the index as a stale hit forever.
    #[test]
    fn removed_keys_are_deleted_not_left_as_stale_hits() {
        let revs = vec![rev("gone", "removed", None, 300)];
        let (docs, deletes) = dataset_docs_from_revisions("grants", "unified", &revs);
        assert!(docs.is_empty(), "a removed key has no snapshot to index");
        assert_eq!(deletes, vec!["grants:unified:gone".to_string()]);
    }

    /// The anti-pattern: taking every revision in the window, so a key written
    /// twice in one run is processed from a superseded state — and a
    /// changed-then-removed key gets RESURRECTED by the older add.
    #[test]
    fn latest_revision_per_key_wins_not_every_revision() {
        // changes_since is newest-first.
        let revs = vec![
            rev("a", "changed", Some(json!({"title": "final"})), 300),
            rev("a", "new", Some(json!({"title": "first"})), 100),
        ];
        let (docs, deletes) = dataset_docs_from_revisions("grants", "unified", &revs);
        assert_eq!(docs.len(), 1, "one doc per key, not one per revision");
        assert_eq!(docs[0].title, "final");
        assert!(deletes.is_empty());

        let removed_last = vec![
            rev("a", "removed", None, 300),
            rev("a", "changed", Some(json!({"title": "first"})), 100),
        ];
        let (docs, deletes) = dataset_docs_from_revisions("grants", "unified", &removed_last);
        assert!(
            docs.is_empty(),
            "a key removed at the end of the run must not be re-added by its earlier revision"
        );
        assert_eq!(deletes, vec!["grants:unified:a".to_string()]);
    }

    /// Ids must match `SearchDoc::dataset_id` exactly — the live path, the delete
    /// path and the offline backfill all key off it.
    #[test]
    fn delete_ids_match_the_shared_doc_id_builder() {
        let revs = vec![rev("k/1", "removed", None, 1)];
        let (_, deletes) = dataset_docs_from_revisions("a", "d", &revs);
        assert_eq!(
            deletes,
            vec![pumper_core::SearchDoc::dataset_id("a", "d", "k/1")]
        );
    }
}

#[cfg(test)]
mod ledger_prune_tests {
    use super::{prune_is_due, TRIGGER_RUN_PRUNE_EVERY};
    use std::time::{Duration, Instant};

    /// The anti-pattern: a retention sweep on every reaper tick (seconds) for a
    /// bound measured in days.
    #[test]
    fn prune_runs_once_per_interval_not_every_tick() {
        let t0 = Instant::now();
        assert!(
            prune_is_due(None, t0, TRIGGER_RUN_PRUNE_EVERY),
            "first tick"
        );
        let every = Duration::from_secs(60);
        assert!(!prune_is_due(Some(t0), t0 + Duration::from_secs(1), every));
        assert!(!prune_is_due(Some(t0), t0 + Duration::from_secs(59), every));
        assert!(prune_is_due(Some(t0), t0 + every, every));
        assert!(prune_is_due(Some(t0), t0 + Duration::from_secs(600), every));
    }
}

#[cfg(test)]
mod fanout_fence_tests {
    use super::fanout_owns_outcome;
    use pumper_core::{Job, JobStatus};

    fn job(status: JobStatus, attempts: i64) -> Job {
        Job {
            id: uuid::Uuid::new_v4(),
            app: "fake".into(),
            params: serde_json::json!({}),
            status,
            attempts,
            max_attempts: 3,
            priority: 0,
            callback_url: None,
            callback_secret: None,
            budget_usd: None,
            schedule_id: None,
            trigger_id: None,
            result: None,
            error: None,
            created_at: chrono::Utc::now(),
            available_at: chrono::Utc::now(),
            started_at: None,
            finished_at: None,
        }
    }

    /// The anti-pattern the fence exists for: a job reset or reaped mid-run is
    /// re-claimed elsewhere (its attempt advances), and the abandoned task's
    /// fan-out then indexes and webhooks on behalf of a run that no longer owns
    /// the job. A delivered webhook cannot be recalled, so this fails closed.
    #[test]
    fn a_reclaimed_attempt_does_not_fan_out() {
        assert!(fanout_owns_outcome(Some(&job(JobStatus::Succeeded, 1)), 1));
        // Reset + re-claimed: attempts advanced past what this task holds.
        assert!(!fanout_owns_outcome(Some(&job(JobStatus::Succeeded, 2)), 1));
        // Re-queued and not yet re-claimed, or retried into a new lineage.
        assert!(!fanout_owns_outcome(Some(&job(JobStatus::Queued, 1)), 1));
        assert!(!fanout_owns_outcome(Some(&job(JobStatus::Running, 1)), 1));
        // Someone else's terminal state is not this run's outcome either.
        assert!(!fanout_owns_outcome(Some(&job(JobStatus::Failed, 1)), 1));
        assert!(!fanout_owns_outcome(Some(&job(JobStatus::Cancelled, 1)), 1));
    }

    /// A row that vanished (pruned, or another install's id) proves nothing —
    /// and "cannot prove it" must not mean "push anyway".
    #[test]
    fn a_missing_row_fails_closed() {
        assert!(!fanout_owns_outcome(None, 1));
    }
}

#[cfg(test)]
mod panic_error_tests {
    use super::{panic_error, PANIC_ERROR_PREFIX};

    /// The anti-pattern: a panic reported as an indistinguishable failure (or,
    /// worse, as the reaper's "lease expired" message two minutes later).
    #[test]
    fn panic_error_is_distinguishable_from_app_error_and_reaped_lease() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("boom".to_string());
        let rendered = panic_error(&*payload, Some("crates/apps/x/src/lib.rs:12:5"));
        assert!(rendered.starts_with(PANIC_ERROR_PREFIX));
        assert!(rendered.contains("boom"));
        assert!(rendered.contains("crates/apps/x/src/lib.rs:12:5"));
        // The three error strings a caller must be able to tell apart.
        assert!(!rendered.starts_with("timed out after"));
        assert_ne!(rendered, "lease expired (heartbeat stale)");
    }

    #[test]
    fn str_and_string_payloads_both_render_their_message() {
        let literal: Box<dyn std::any::Any + Send> = Box::new("unwrap on None");
        let owned: Box<dyn std::any::Any + Send> = Box::new("unwrap on None".to_string());
        assert_eq!(panic_error(&*literal, None), "panicked: unwrap on None");
        assert_eq!(panic_error(&*owned, None), "panicked: unwrap on None");
    }

    /// A `panic_any(non_string)` has no printable message — say so rather than
    /// producing an empty, meaningless error.
    #[test]
    fn opaque_payload_says_so_instead_of_rendering_empty() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(42u32);
        assert_eq!(
            panic_error(&*payload, None),
            "panicked: <non-string panic payload>"
        );
    }
}

#[cfg(test)]
mod saved_search_scope_tests {
    use super::{run_indexed_apps, search_covers_run};
    use serde_json::json;

    /// The regression: `grants-gov`/`ca-grants` index their per-opportunity docs
    /// under the VIRTUAL app `grants` (`grants_common::UNIFIED_APP`), so a saved
    /// search scoped to `grants` — the only scope matching how those docs were
    /// indexed — must not be skipped just because `job.app` is the source app.
    #[test]
    fn alert_scoped_to_virtual_app_is_not_skipped() {
        // The result shape `UnifiedOutcome::merge_into` writes, read back through
        // the same parser the worker uses.
        let result = json!({
            "unified": { "new": 2, "changed": 0 },
            "index_datasets": [{ "app": "grants", "dataset": "unified" }],
        });
        let specs = crate::datahub::index_dataset_specs(&result);
        let indexed = run_indexed_apps("ca-grants", &specs);
        assert_eq!(indexed, vec!["ca-grants".to_string(), "grants".to_string()]);
        assert!(
            search_covers_run(Some("grants"), &indexed),
            "a search scoped to the virtual app must run on the source app's job"
        );
        assert!(search_covers_run(Some("ca-grants"), &indexed));
        assert!(search_covers_run(None, &indexed), "unscoped always runs");
    }

    /// Scoping is widened to the run's real namespaces — never to "all apps".
    #[test]
    fn unrelated_app_scope_is_still_excluded() {
        let indexed = run_indexed_apps("ca-grants", &[("grants".into(), "unified".into())]);
        assert!(!search_covers_run(Some("hackernews"), &indexed));
        assert!(!search_covers_run(Some("eu-sedia"), &indexed));
        assert!(
            !search_covers_run(Some(""), &indexed),
            "an empty app filter names no namespace and matches nothing"
        );
    }

    /// A plain run with no `index_datasets` keeps exactly the old semantics.
    #[test]
    fn job_without_index_datasets_scopes_to_its_own_app_only() {
        let indexed = run_indexed_apps("hackernews", &[]);
        assert_eq!(indexed, vec!["hackernews".to_string()]);
        assert!(search_covers_run(Some("hackernews"), &indexed));
        assert!(!search_covers_run(Some("grants"), &indexed));
    }

    /// Several source apps feed one virtual app; the app list stays a set, so a
    /// search is evaluated once per run (dedupe across runs is `claim_unseen`).
    #[test]
    fn repeated_virtual_app_is_listed_once() {
        let specs = vec![
            ("grants".to_string(), "unified".to_string()),
            ("grants".to_string(), "events".to_string()),
        ];
        assert_eq!(
            run_indexed_apps("grants", &specs),
            vec!["grants".to_string()],
            "job app equal to the virtual app collapses to one entry"
        );
    }
}

#[cfg(test)]
mod vcr_param_tests {
    use super::vcr_params;
    use serde_json::json;

    #[test]
    fn absent_params_mean_vcr_off() {
        assert_eq!(vcr_params(&json!({})).unwrap(), (false, None));
        assert_eq!(
            vcr_params(&json!({"url": "https://x/", "record": false})).unwrap(),
            (false, None)
        );
    }

    #[test]
    fn record_flag_and_replay_uuid_parse() {
        assert_eq!(vcr_params(&json!({"record": true})).unwrap(), (true, None));
        let id = uuid::Uuid::new_v4();
        assert_eq!(
            vcr_params(&json!({"replay_of": id.to_string()})).unwrap(),
            (false, Some(id))
        );
    }

    #[test]
    fn malformed_replay_of_is_an_error_not_a_silent_live_run() {
        assert!(vcr_params(&json!({"replay_of": "not-a-uuid"})).is_err());
        assert!(vcr_params(&json!({"replay_of": 42})).is_err());
    }

    #[test]
    fn record_plus_replay_is_contradictory() {
        let id = uuid::Uuid::new_v4().to_string();
        assert!(vcr_params(&json!({"record": true, "replay_of": id})).is_err());
    }
}
