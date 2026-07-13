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

/// Graceful-shutdown drain: waits up to `shutdown_drain_secs` for in-flight jobs
/// to finish (each holds a semaphore permit, so reacquiring all of them means the
/// queue is idle). Jobs still running when the deadline passes are re-queued —
/// mirroring `recover_stuck` — so a slow job resumes cleanly on the next boot
/// instead of being stranded in `running`.
async fn drain(state: &AppState, semaphore: &Arc<Semaphore>, concurrency: usize) {
    let deadline = Duration::from_secs(state.config.worker.shutdown_drain_secs);
    info!(deadline_secs = deadline.as_secs(), "worker draining in-flight jobs");
    let acquire = semaphore.clone().acquire_many_owned(concurrency as u32);
    match tokio::time::timeout(deadline, acquire).await {
        Ok(_) => info!("worker drained cleanly; no jobs left running"),
        Err(_) => match state.storage.recover_stuck().await {
            Ok(n) => warn!(requeued = n, "drain deadline reached; re-queued still-running jobs"),
            Err(e) => error!("drain re-queue failed: {e}"),
        },
    }
}

/// Apps currently at or above their concurrency limit (0 = unlimited).
async fn blocked_apps(state: &AppState, running: &Arc<Mutex<HashMap<String, usize>>>) -> Vec<String> {
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

    info!(job = %job.id, app = %job.app, attempt = job.attempts, "job started");
    let ctx = AppContext {
        job_id: job.id,
        app: job.app.clone(),
        params: job.params.clone(),
        engines: state.engines.clone(),
        datasets: state.datasets.clone(),
        costs: state.costs.clone(),
        budget_usd: job.budget_usd,
        research_cache: state.research_cache.clone(),
        tiers: state.tiers.clone(),
        plugins: state.plugins.clone(),
        artifacts_dir: state
            .storage
            .artifacts_dir
            .join(&job.app)
            .join(job.id.to_string()),
    };

    let timeout = Duration::from_secs(state.config.worker.job_timeout_secs);
    let run = app.run(ctx);
    tokio::pin!(run);
    let sleep = tokio::time::sleep(timeout);
    tokio::pin!(sleep);
    // Race the app future against the wall-clock timeout and the cancel token.
    let outcome = tokio::select! {
        biased;
        _ = cancel.cancelled() => Outcome::Cancelled,
        _ = &mut sleep => Outcome::TimedOut,
        res = &mut run => Outcome::Finished(res),
    };

    match outcome {
        Outcome::Cancelled => {
            // Cooperative cancel of a running job: mark it cancelled (not failed)
            // and emit the terminal event, mirroring the queued-cancel path
            // (event only, no result webhook). Guarded, so a job that raced to a
            // terminal state or was reset first is left untouched.
            match state.storage.cancel_running(job.id, job.attempts).await {
                Ok(true) => {
                    warn!(job = %job.id, "running job cancelled");
                    publish(&state, JobEvent::new(job.id, job.app.clone(), "cancelled"));
                }
                Ok(false) => {}
                Err(e) => error!(job = %job.id, "failed to persist cancellation: {e}"),
            }
            return;
        }
        Outcome::Finished(Ok(result)) => {
            // Index the result into full-text search before persisting it.
            let docs = search_docs(&job.app, job.id, &result);
            if let Err(e) = state.search.index(docs).await {
                warn!(job = %job.id, "search index failed: {e}");
            }
            match state.storage.complete(job.id, job.attempts, result).await {
                Ok(true) => info!(job = %job.id, "job succeeded"),
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
                let by_dataset = group_by_dataset(&changes);
                notify_watches(&state, &job, &by_dataset).await;
                crate::triggers::fire_dataset_triggers(&state, &job, &by_dataset).await;
            }
            notify_saved_searches(&state, &job).await;
        }
        Outcome::Finished(Err(e)) => {
            warn!(job = %job.id, error = %e, "job failed");
            match state.storage.fail(job.id, job.attempts, &e.to_string()).await {
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
                .fail(job.id, job.attempts, &format!("timed out after {}s", timeout.as_secs()))
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

/// Everything this run wrote: revisions after the attempt's start. Fail-open
/// (empty on error) — side effects never block the job outcome.
async fn load_run_changes(state: &AppState, job: &Job) -> Vec<pumper_core::Revision> {
    match state
        .datasets
        .changes_since(&job.app, None, job.started_at, 1000)
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
        by_dataset.entry(rev.dataset.as_str()).or_default().push(rev);
    }
    by_dataset
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
    for search in searches {
        if search.app.as_deref().is_some_and(|app| app != job.app) {
            continue;
        }
        let req = pumper_core::SearchRequest {
            q: search.query.clone(),
            limit: 50,
            app: search.app.clone(),
            dataset: search.dataset.clone(),
            fuzzy: false,
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
            payload,
            search.secret.clone(),
        );
    }
}

/// Emits the terminal event and fires the result webhook, if configured.
async fn finalize(state: &AppState, id: uuid::Uuid) {
    let Ok(Some(job)) = state.storage.get(id).await else {
        return;
    };
    let mut event = JobEvent::new(job.id, job.app.clone(), job.status.as_str());
    event.result = job.result.clone();
    event.error = job.error.clone();
    publish(state, event);
    webhook::dispatch(state.webhook_client.clone(), state.storage.clone(), job.clone());
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
        });
    }
    docs
}

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
    }
}
