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
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");

        let blocked = blocked_apps(&state, &running).await;
        match state.storage.claim_next(&blocked).await {
            Ok(Some(job)) => {
                {
                    let mut counts = running.lock().await;
                    *counts.entry(job.app.clone()).or_insert(0) += 1;
                }
                let state = state.clone();
                let running = running.clone();
                tokio::spawn(async move {
                    publish(&state, JobEvent::new(job.id, job.app.clone(), "running"));
                    execute(state.clone(), job.clone()).await;
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
                    _ = state.notify.notified() => {}
                    _ = tokio::time::sleep(poll) => {}
                }
            }
            Err(e) => {
                drop(permit);
                error!("failed to claim job: {e}");
                tokio::time::sleep(poll).await;
            }
        }
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

async fn execute(state: AppState, job: Job) {
    let Some(app) = state.registry.get(&job.app).cloned() else {
        warn!(app = %job.app, job = %job.id, "job references unregistered app");
        let _ = state
            .storage
            .fail_permanently(job.id, "app not registered")
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
        plugins: state.plugins.clone(),
        artifacts_dir: state
            .storage
            .artifacts_dir
            .join(&job.app)
            .join(job.id.to_string()),
    };

    let timeout = Duration::from_secs(state.config.worker.job_timeout_secs);
    match tokio::time::timeout(timeout, app.run(ctx)).await {
        Ok(Ok(result)) => {
            // Index the result into full-text search before persisting it.
            let docs = search_docs(&job.app, job.id, &result);
            if let Err(e) = state.search.index(docs).await {
                warn!(job = %job.id, "search index failed: {e}");
            }
            if let Err(e) = state.storage.complete(job.id, result).await {
                error!(job = %job.id, "failed to persist result: {e}");
            } else {
                info!(job = %job.id, "job succeeded");
            }
            notify_watches(&state, &job).await;
        }
        Ok(Err(e)) => {
            warn!(job = %job.id, error = %e, "job failed");
            match state.storage.fail(job.id, &e.to_string()).await {
                Ok(JobStatus::Queued) => {
                    // Not terminal — retry pending; wake the worker and return.
                    state.notify.notify_one();
                    return;
                }
                Ok(_) => {}
                Err(pe) => error!(job = %job.id, "failed to persist failure: {pe}"),
            }
        }
        Err(_) => {
            warn!(job = %job.id, timeout_secs = timeout.as_secs(), "job timed out");
            match state
                .storage
                .fail(job.id, &format!("timed out after {}s", timeout.as_secs()))
                .await
            {
                Ok(JobStatus::Queued) => {
                    state.notify.notify_one();
                    return;
                }
                _ => {}
            }
        }
    }
    finalize(&state, job.id).await;
}

/// Fires `dataset.changed` webhooks at every enabled watch whose dataset saw
/// new/changed/removed revisions during this job run. Best-effort: delivery
/// failures never affect the job outcome.
async fn notify_watches(state: &AppState, job: &Job) {
    let watches = match state.storage.enabled_watches(&job.app).await {
        Ok(w) if !w.is_empty() => w,
        Ok(_) => return,
        Err(e) => {
            warn!(job = %job.id, "failed to load watches: {e}");
            return;
        }
    };
    // Everything this run wrote: revisions after the attempt's start.
    let changes = match state
        .datasets
        .changes_since(&job.app, None, job.started_at, 1000)
        .await
    {
        Ok(c) if !c.is_empty() => c,
        Ok(_) => return,
        Err(e) => {
            warn!(job = %job.id, "failed to load changes for watches: {e}");
            return;
        }
    };

    let mut by_dataset: HashMap<&str, Vec<&pumper_core::Revision>> = HashMap::new();
    for rev in &changes {
        by_dataset.entry(rev.dataset.as_str()).or_default().push(rev);
    }

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

/// Emits the terminal event and fires the result webhook, if configured.
async fn finalize(state: &AppState, id: uuid::Uuid) {
    let Ok(Some(job)) = state.storage.get(id).await else {
        return;
    };
    let mut event = JobEvent::new(job.id, job.app.clone(), job.status.as_str());
    event.result = job.result.clone();
    event.error = job.error.clone();
    publish(state, event);
    webhook::dispatch(state.webhook_client.clone(), state.storage.clone(), job);
}

fn publish(state: &AppState, event: JobEvent) {
    // Ignore send errors: no subscribers is fine.
    let _ = state.events.send(event);
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
