use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pumper_core::{AppContext, Job, JobStatus, SearchDoc};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use tracing::{error, info, warn};
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
            let mut m = state.job_cancels.lock().unwrap();
            if m.get(&job.id).map(|(a, _)| *a) == Some(job.attempts) {
                m.remove(&job.id);
            }
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

/// How a run left the queue: the app finished (Ok/Err), the wall-clock timeout
/// tripped, or a cancellation token fired mid-run.
enum Outcome {
    Finished(pumper_core::Result<Value>),
    TimedOut,
    Cancelled,
}

async fn execute(state: AppState, job: Job, cancel: tokio_util::sync::CancellationToken) {
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
        budget_usd: job.budget_usd,
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
    let run = app.run(ctx);
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
    let outcome = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break Outcome::Cancelled,
            _ = &mut sleep => break Outcome::TimedOut,
            res = &mut run => break Outcome::Finished(res),
            _ = maybe_tick(&mut heartbeat) => {
                let _ = state.storage.heartbeat(job.id, job.attempts).await;
            }
        }
    };

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
            // Index the result into full-text search before persisting it. Apps
            // whose result stays compact (counts, not arrays) can additionally
            // name datasets to index per-record via `index_datasets` — see
            // `dataset_search_docs`.
            let mut docs = search_docs(&job.app, job.id, &result);
            let index_specs = crate::datahub::index_dataset_specs(&result);
            let (dataset_docs, dataset_deletes) = dataset_search_docs(&state, &job, &result).await;
            docs.extend(dataset_docs);
            if let Err(e) = state.search.index(docs).await {
                warn!(job = %job.id, "search index failed: {e}");
            }
            // Removed records (this run's `removed` revisions) are dropped from the
            // index rather than left as stale hits.
            if !dataset_deletes.is_empty() {
                if let Err(e) = state.search.delete_ids(&dataset_deletes).await {
                    warn!(job = %job.id, "search delete failed: {e}");
                }
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
                Err(e) => error!(job = %job.id, "failed to persist result: {e}"),
            }
            // One revision batch for this run, shared by watches + triggers.
            let changes = load_run_changes(&state, &job).await;
            if !changes.is_empty() {
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
                crate::triggers::fire_dataset_triggers(&state, &job, &by_dataset).await;
            }
            notify_saved_searches(&state, &job).await;
            // Metadata shadow: push this run's dataset entities/lineage/freshness
            // to DataHub. Detached and fail-open — a down GMS never touches jobs.
            crate::datahub::on_job_success(state.clone(), job.clone(), index_specs);
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
            webhook::dispatch_change(
                state.webhook_client.clone(),
                state.storage.clone(),
                watch.clone(),
                payload,
            );
        }
    }
}

/// Runs enabled saved searches after a job's results were indexed, alerting
/// each NEW match exactly once (`saved_search_seen` dedup). Scoped to searches
/// whose app filter is empty or matches the finished job's app.
async fn notify_saved_searches(state: &AppState, job: &Job) {
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
        if search.app.as_deref().is_some_and(|app| app != job.app) {
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
        let unseen = match state.storage.claim_unseen(&search.id, &ids).await {
            Ok(unseen) if !unseen.is_empty() => unseen,
            Ok(_) => continue,
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
            state.webhook_client.clone(),
            state.storage.clone(),
            "search",
            &search.id,
            &search.url,
            "search.matched",
            &payload,
            search.secret.clone(),
        );
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
    crate::triggers::fire_dataset_triggers(state, &view_job, &by_dataset).await;
}

/// Emits the terminal event and fires the result webhook, if configured.
async fn finalize(state: &AppState, id: uuid::Uuid) {
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
    publish(state, event);
    webhook::dispatch(
        state.webhook_client.clone(),
        state.storage.clone(),
        job.clone(),
    );
    // Permanent-failure firehose: a job that exhausted its attempts (app error,
    // timeout, or a reaped stale lease — all land here via `finalize`) notifies
    // the global `[webhooks] failure_url` subscriber, if configured. Retryable
    // requeues never reach `finalize`, so this is permanent failures only.
    if job.status == JobStatus::Failed {
        if let Some(url) = &state.config.webhooks.failure_url {
            webhook::dispatch_failure(
                state.webhook_client.clone(),
                state.storage.clone(),
                url,
                state.config.webhooks.failure_secret.clone(),
                &job,
            );
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
            dataset: app.to_string(),
            url: String::new(),
            title: app.to_string(),
            body: result.to_string(),
            // Job-result docs carry no record timestamp — index at completion time.
            indexed_at: chrono::Utc::now().timestamp(),
        });
    }
    docs
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
        // changes_since is newest-first; keep only the latest revision per key so
        // a key changed twice in one run is indexed/deleted once, from its final state.
        let mut seen = std::collections::HashSet::new();
        for rev in revs {
            if !seen.insert(rev.key.clone()) {
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
    let id = if url.is_empty() {
        format!("{app}:{job_id}:{i}")
    } else {
        format!("{app}:{url}")
    };
    SearchDoc {
        id,
        app: app.to_string(),
        dataset: app.to_string(),
        url,
        title,
        body: rec.to_string(),
        // Job-result docs carry no record timestamp — index at completion time.
        indexed_at: chrono::Utc::now().timestamp(),
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
