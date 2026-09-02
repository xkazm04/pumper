//! N18 — executor mode: `pumper --executor --coordinator <url>`.
//!
//! The same binary, started with two flags, becomes an **outbound worker**: it
//! binds no port, serves no API, runs no scheduler and no janitors, and instead
//! loops
//!
//! ```text
//! claim  ->  execute  ->  report
//! ```
//!
//! against a coordinator it dials. Everything that makes a job *durable* stays
//! on the coordinator — the queue, the attempt ladder, the lease, the gates, the
//! fan-out. What moves is `execute`: the fetch ladder, the browser, the Claude
//! subprocess, the plugin host. That is the point: concurrency and geography
//! scale by starting a process, not by the coordinator's core count.
//!
//! ## The scratch store, and the one handle that refuses
//!
//! An executor still needs a *local* SQLite file, because half of an
//! `AppContext` is store-backed: the cost ledger, the HTTP cache, learned host
//! weather, the research cache, the recipe store. Those are all **process-local
//! derived state** — losing them costs a cache miss, never a record — so the
//! executor keeps its own under its own `[storage] database_path`.
//!
//! `datasets` is the exception, and it is handed a [`refusing_datasets`] handle:
//! a `Datasets` over an empty in-memory database with no schema at all, so any
//! write fails loudly instead of landing somewhere nobody will ever read. The
//! honest alternative would be an RPC dataset client, which is explicitly out of
//! the v1 slice; the *dishonest* alternative — a real local `Datasets` over the
//! scratch store — would make an upserting app look like it succeeded while its
//! records evaporated on a machine the operator does not even collect from.
//! Only apps that declare `ScrapeApp::executor` (result-only, pinned by an
//! inventory test) are ever handed out, so in practice this handle is a
//! backstop, not a workflow.
//!
//! ## Failure model
//!
//! The executor is allowed to die at any instant, and nothing special happens:
//! its job stops being heartbeated, the coordinator's reaper re-queues it on the
//! usual `stale_after_secs` lease, the next claim advances `attempts`, and the
//! resumed attempt picks up the last checkpoint this executor pushed. If the
//! dead process comes *back* and reports its result anyway, the coordinator's
//! `(status='running', attempts, executor_id)` fence refuses it with a `409`.
//! None of that is new code — it is the lease and fence the local worker has
//! always run under, reached over HTTP.

use std::sync::Arc;
use std::time::Duration;

use pumper_core::{AppContext, Config, Datasets};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::state::AppState;

/// Header the shared plane secret travels in, both directions.
use pumper_core::config::EXECUTOR_SECRET_HEADER;

/// Wall-clock ceiling on one HTTP call to the coordinator, other than the claim
/// long-poll (which gets `claim_wait_secs` plus this as slack).
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// What `POST /executors/claim` answers with, decoded.
#[derive(Debug, Deserialize)]
struct ClaimedJob {
    job_id: Uuid,
    app: String,
    params: Value,
    attempt: i64,
    budget_usd: Option<f64>,
    restored: Option<Value>,
    resumed_input: Option<Value>,
}

/// The executor's identity: configured, or derived from host + pid.
///
/// Derivation is deliberately *not* a UUID: a random id per boot makes
/// `GET /executors` accumulate a new ghost row on every restart, and makes "this
/// box keeps dying" indistinguishable from "the fleet is growing". Host+pid at
/// least names the machine; a fleet worth reading history for sets
/// `[executors] executor_id`.
pub(crate) fn resolve_executor_id(configured: &str) -> String {
    let configured = configured.trim();
    if !configured.is_empty() {
        return configured.to_string();
    }
    let host = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "executor".to_string());
    format!("{host}-{}", std::process::id())
}

/// The coordinator URL, with the CLI flag winning over the config key.
///
/// `Err` rather than a default: an executor with nowhere to dial would poll
/// nothing forever while looking perfectly healthy in its own logs, which is the
/// most expensive way to be misconfigured.
pub(crate) fn resolve_coordinator(flag: Option<&str>, configured: &str) -> anyhow::Result<String> {
    let url = flag
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .unwrap_or(configured.trim());
    if url.is_empty() {
        anyhow::bail!(
            "--executor needs a coordinator: pass --coordinator <url> or set \
             [executors] coordinator_url"
        );
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        anyhow::bail!("coordinator ('{url}') must be an absolute http(s) URL");
    }
    Ok(url.trim_end_matches('/').to_string())
}

/// A `Datasets` handle over an empty, schema-less in-memory database.
///
/// Every dataset write through it fails with SQLite's own `no such table:
/// records`, which is exactly the intent: on an executor there is nowhere
/// legitimate for a dataset write to go, and a **loud** failure is the only
/// honest answer. The alternative that "works" — pointing it at the executor's
/// scratch store — is the one that must never ship: the app would report
/// success and its records would exist only on a machine nobody collects from.
async fn refusing_datasets() -> anyhow::Result<Arc<Datasets>> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    Ok(Arc::new(Datasets::new(pool)))
}

/// Posts progress snapshots to the coordinator, throttled exactly like the
/// server-side reporter (first call writes, then ≥ every 2s).
///
/// The throttle lives HERE rather than on the coordinator because this is the
/// side that knows how chatty the app is, and because the alternative — one HTTP
/// round trip per `ctx.progress.report(..)` — turns a tight crawl loop into a
/// denial of service against the coordinator's own event bus.
struct RemoteProgress {
    client: reqwest::Client,
    url: String,
    secret: String,
    executor_id: String,
    attempt: i64,
    last: std::sync::Mutex<Option<std::time::Instant>>,
}

impl pumper_core::ProgressReporter for RemoteProgress {
    fn report(&self, snapshot: Value) {
        {
            let mut last = self.last.lock().unwrap();
            let now = std::time::Instant::now();
            let due = last.is_none_or(|prev| now.duration_since(prev) >= Duration::from_secs(2));
            if !due {
                return;
            }
            *last = Some(now);
        }
        let (client, url, secret, executor_id, attempt) = (
            self.client.clone(),
            self.url.clone(),
            self.secret.clone(),
            self.executor_id.clone(),
            self.attempt,
        );
        // Fire-and-forget, like the local reporter: progress is telemetry and
        // must never be able to fail (or slow) the run it describes.
        tokio::spawn(async move {
            let body = json!({
                "executor_id": executor_id,
                "attempt": attempt,
                "state": snapshot,
            });
            let _ = client
                .post(&url)
                .header(EXECUTOR_SECRET_HEADER, &secret)
                .timeout(CALL_TIMEOUT)
                .json(&body)
                .send()
                .await;
        });
    }
}

/// Pushes checkpoints to the coordinator, where the same attempts-lineage fence
/// the local sink writes under applies.
///
/// Unlike progress, this one is **awaited and its verdict is honest**: `false`
/// when the coordinator refused (the fence rejected it, or the call failed), so
/// an app counting failed checkpoints gets the same signal it does locally.
struct RemoteCheckpoints {
    client: reqwest::Client,
    url: String,
    secret: String,
    executor_id: String,
    attempt: i64,
    last: std::sync::Mutex<Option<std::time::Instant>>,
}

#[async_trait::async_trait]
impl pumper_core::CheckpointSink for RemoteCheckpoints {
    async fn save(&self, state: Value, force: bool) -> bool {
        if !force {
            let mut last = self.last.lock().unwrap();
            let now = std::time::Instant::now();
            let due = last.is_none_or(|prev| now.duration_since(prev) >= Duration::from_secs(5));
            if !due {
                // Throttle-skipped is not a failure: the previous snapshot is
                // still durable on the coordinator.
                return true;
            }
            *last = Some(now);
        }
        let body = json!({
            "executor_id": self.executor_id,
            "attempt": self.attempt,
            "state": state,
        });
        match self
            .client
            .post(&self.url)
            .header(EXECUTOR_SECRET_HEADER, &self.secret)
            .timeout(CALL_TIMEOUT)
            .json(&body)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => true,
            Ok(r) => {
                tracing::warn!(status = %r.status(), "checkpoint refused by coordinator");
                false
            }
            Err(e) => {
                tracing::warn!("checkpoint push failed: {e}");
                false
            }
        }
    }
}

/// One executor process: build the engines, then loop claim → execute → report
/// until the shutdown signal.
pub(crate) async fn run(mut config: Config, coordinator: Option<String>) -> anyhow::Result<()> {
    let url = resolve_coordinator(coordinator.as_deref(), &config.executors.coordinator_url)?;
    let secret = config.executors.secret.trim().to_string();
    if secret.is_empty() {
        anyhow::bail!(
            "[executors] secret must be set in executor mode — it is the credential this \
             process presents to the coordinator, and an executor without one can claim nothing"
        );
    }
    let executor_id = resolve_executor_id(&config.executors.executor_id);

    // An executor serves nothing, so the surfaces that only make sense on a
    // coordinator are forced off rather than left to a copied config file: a
    // second Tantivy index and a second MCP endpoint on the same box would be
    // two silent, confusing duplicates of the coordinator's.
    config.search.enabled = false;
    config.mcp.enabled = false;
    config.events.log_enabled = false;

    let state = AppState::init(config).await?;
    // The one handle that refuses. Everything else in this state is legitimate
    // process-local derived state (cost ledger, cache, host weather, recipes).
    let state = AppState {
        datasets: refusing_datasets().await?,
        ..state
    };

    let shutdown = state.shutdown.clone();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            crate::shutdown_signal().await;
            tracing::info!("shutdown signal received; finishing the current job");
            shutdown.cancel();
        }
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(
            state.config.executors.claim_wait_secs + CALL_TIMEOUT.as_secs(),
        ))
        .build()?;
    let capabilities = state.config.executors.capabilities.clone();
    let eligible = crate::executors::eligible_apps(&state.registry, &capabilities);
    tracing::info!(
        %executor_id, coordinator = %url, apps = ?eligible,
        "executor started; claiming from the coordinator"
    );
    if eligible.is_empty() {
        tracing::warn!(
            "no app in this build is both executor-eligible and named by [executors] \
             capabilities — this process will poll forever and claim nothing"
        );
    }

    let poll = Duration::from_secs(state.config.executors.poll_interval_secs.max(1));
    while !shutdown.is_cancelled() {
        let claimed = claim(&client, &url, &secret, &executor_id, &capabilities).await;
        match claimed {
            Ok(Some(job)) => {
                execute_and_report(&state, &client, &url, &secret, &executor_id, job).await
            }
            Ok(None) => {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(poll) => {}
                }
            }
            Err(e) => {
                tracing::warn!("claim failed: {e}");
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        }
    }
    tracing::info!("executor stopped");
    Ok(())
}

async fn claim(
    client: &reqwest::Client,
    url: &str,
    secret: &str,
    executor_id: &str,
    capabilities: &[String],
) -> anyhow::Result<Option<ClaimedJob>> {
    let response = client
        .post(format!("{url}/executors/claim"))
        .header(EXECUTOR_SECRET_HEADER, secret)
        .json(&json!({ "executor_id": executor_id, "capabilities": capabilities }))
        .send()
        .await?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(None);
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("coordinator answered {status}: {body}");
    }
    Ok(Some(response.json::<ClaimedJob>().await?))
}

/// Runs one claimed job locally and reports its outcome, heartbeating for as
/// long as it runs.
async fn execute_and_report(
    state: &AppState,
    client: &reqwest::Client,
    url: &str,
    secret: &str,
    executor_id: &str,
    job: ClaimedJob,
) {
    let Some(app) = state.registry.get(&job.app).cloned() else {
        // The coordinator handed out an app this build does not have. Report it
        // as a failure rather than dropping it: silence would leave the job
        // running until its lease expired, once per claim, forever.
        report(
            client,
            url,
            secret,
            executor_id,
            &job,
            Err(format!(
                "executor does not have app '{}' registered — its build is older or narrower \
                 than the coordinator's",
                job.app
            )),
            None,
        )
        .await;
        return;
    };

    let base = format!("{url}/jobs/{}", job.job_id);
    let progress = Arc::new(RemoteProgress {
        client: client.clone(),
        url: format!("{base}/progress"),
        secret: secret.to_string(),
        executor_id: executor_id.to_string(),
        attempt: job.attempt,
        last: std::sync::Mutex::new(None),
    });
    let checkpoints = Arc::new(RemoteCheckpoints {
        client: client.clone(),
        url: format!("{base}/checkpoint"),
        secret: secret.to_string(),
        executor_id: executor_id.to_string(),
        attempt: job.attempt,
        last: std::sync::Mutex::new(None),
    });
    let artifacts_dir = state
        .storage
        .artifacts_dir
        .join(&job.app)
        .join(job.job_id.to_string());
    let ctx = AppContext {
        job_id: job.job_id,
        app: job.app.clone(),
        params: job.params.clone(),
        engines: state.engines.clone(),
        // The refusing handle (see `refusing_datasets`).
        datasets: state.datasets.clone(),
        costs: state.costs.clone(),
        budget_usd: job.budget_usd,
        spent_usd: Arc::new(pumper_core::SpentTotal::new(0.0)),
        research_cache: state.research_cache.clone(),
        tiers: state.tiers.clone(),
        health: state.health.clone(),
        recipes: Arc::new(state.storage.recipes()),
        plugins: state.plugins.clone(),
        progress,
        checkpoints,
        restored: job.restored.clone(),
        resumed_input: job.resumed_input.clone(),
        // VCR is coordinator-side state (cassettes live beside the recorded
        // job's artifacts, on the coordinator's disk), so a remote run records
        // and replays nothing. Out of the v1 slice, deliberately and stated.
        vcr: pumper_core::Vcr::Off,
        artifacts_dir,
    };

    tracing::info!(job = %job.job_id, app = %job.app, attempt = job.attempt, "executing job");
    let started = std::time::Instant::now();
    let mut run = Box::pin(app.run(ctx));
    let mut beat = tokio::time::interval(Duration::from_secs(
        state.config.worker.heartbeat_secs.max(1),
    ));
    beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let outcome = loop {
        tokio::select! {
            res = &mut run => break res.map_err(|e| e.to_string()),
            _ = beat.tick() => {
                // The heartbeat's answer is also the stop signal: a coordinator
                // that no longer considers this executor the owner (reaped,
                // reset, cancelled) answers 409, and continuing to spend on a
                // result that will be refused is pure waste.
                if !heartbeat(client, &base, secret, executor_id, job.attempt).await {
                    tracing::warn!(
                        job = %job.job_id,
                        "coordinator no longer considers this executor the owner; abandoning \
                         the run (its checkpoint is already durable there)"
                    );
                    return;
                }
            }
        }
    };
    let run_ms = started.elapsed().as_millis() as i64;
    report(
        client,
        url,
        secret,
        executor_id,
        &job,
        outcome,
        Some(run_ms),
    )
    .await;
}

/// One heartbeat. `false` means "you no longer own this job" — a refusal or an
/// unreachable coordinator both count, because an executor that cannot prove it
/// holds the lease is in exactly the position the fence exists to handle.
async fn heartbeat(
    client: &reqwest::Client,
    base: &str,
    secret: &str,
    executor_id: &str,
    attempt: i64,
) -> bool {
    let body = json!({ "executor_id": executor_id, "attempt": attempt });
    match client
        .post(format!("{base}/heartbeat"))
        .header(EXECUTOR_SECRET_HEADER, secret)
        .timeout(CALL_TIMEOUT)
        .json(&body)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => true,
        Ok(r) => {
            tracing::warn!(status = %r.status(), "heartbeat refused");
            false
        }
        // A transport failure is NOT treated as loss of ownership: the lease is
        // still live on the coordinator for `stale_after_secs`, and abandoning
        // work over one dropped packet would throw away a run that is about to
        // finish. The reaper is the backstop if the outage really lasts.
        Err(e) => {
            tracing::warn!("heartbeat failed: {e}");
            true
        }
    }
}

/// Reports one outcome, retrying the POST a few times: the run is already paid
/// for, so losing its result to one dropped connection is the single most
/// expensive failure this process has.
async fn report(
    client: &reqwest::Client,
    url: &str,
    secret: &str,
    executor_id: &str,
    job: &ClaimedJob,
    outcome: std::result::Result<Value, String>,
    run_ms: Option<i64>,
) {
    let mut body = json!({
        "executor_id": executor_id,
        "attempt": job.attempt,
        "run_ms": run_ms,
    });
    match &outcome {
        Ok(result) => body["result"] = result.clone(),
        Err(error) => body["error"] = json!(error),
    }
    for attempt in 0..3u32 {
        match client
            .post(format!("{url}/jobs/{}/finish", job.job_id))
            .header(EXECUTOR_SECRET_HEADER, secret)
            .timeout(CALL_TIMEOUT)
            .json(&body)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                tracing::info!(job = %job.job_id, "reported outcome to coordinator");
                return;
            }
            // A 409 is final, not retryable: the coordinator has told us this
            // attempt no longer owns the job, and re-asking cannot change that.
            Ok(r) if r.status() == reqwest::StatusCode::CONFLICT => {
                tracing::warn!(job = %job.job_id, "outcome refused by the fence: another attempt owns this job");
                return;
            }
            Ok(r) => tracing::warn!(job = %job.job_id, status = %r.status(), "finish refused"),
            Err(e) => tracing::warn!(job = %job.job_id, "finish failed: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
    }
    tracing::error!(
        job = %job.job_id,
        "could not report this job's outcome to the coordinator after 3 attempts; its lease \
         will go stale and the coordinator's reaper will re-queue it (the work is lost, the \
         job is not)"
    );
}

#[cfg(test)]
mod tests {
    use super::{refusing_datasets, resolve_coordinator, resolve_executor_id};

    /// The anti-pattern: an executor that starts happily with nowhere to dial,
    /// polls nothing forever, and looks healthy in its own logs the whole time.
    #[test]
    fn an_executor_with_no_coordinator_refuses_to_start() {
        assert!(resolve_coordinator(None, "").is_err());
        assert!(resolve_coordinator(Some("   "), "").is_err());
        // Not a URL at all — caught here rather than once per poll forever.
        assert!(resolve_coordinator(Some("localhost:8088"), "").is_err());
        // The flag wins over the config key, and a trailing slash is normalised
        // away so `{url}/jobs/..` never doubles it.
        assert_eq!(
            resolve_coordinator(Some("http://a:1/"), "http://b:2").unwrap(),
            "http://a:1"
        );
        assert_eq!(
            resolve_coordinator(None, "https://b:2").unwrap(),
            "https://b:2"
        );
    }

    #[test]
    fn a_configured_id_wins_and_a_derived_one_names_the_box() {
        assert_eq!(resolve_executor_id("  vps-1 "), "vps-1");
        let derived = resolve_executor_id("");
        assert!(
            derived.contains(&std::process::id().to_string()),
            "{derived}"
        );
    }

    /// The whole safety argument of the v1 slice in one assertion: an executor's
    /// dataset handle does not quietly write somewhere useless — it fails.
    ///
    /// The anti-pattern this guards is the "working" alternative: pointing
    /// `datasets` at the executor's own scratch store. The app would return
    /// success, the coordinator would fan out, and the records would exist only
    /// on a machine nobody reads.
    #[tokio::test]
    async fn the_executors_dataset_handle_refuses_instead_of_writing_locally() {
        let datasets = refusing_datasets().await.expect("build refusing handle");
        let err = datasets
            .upsert("app", "dataset", "key", &serde_json::json!({"a": 1}))
            .await
            .expect_err("a dataset write on an executor must FAIL, not land somewhere private");
        let rendered = err.to_string();
        assert!(
            rendered.contains("records") || rendered.contains("no such table"),
            "the refusal should name the missing store rather than being opaque: {rendered}"
        );
    }
}
