mod datahub;
#[cfg(test)]
mod e2e;
mod events;
mod fanout;
mod mcp;
mod progress;
mod refresher;
mod registry;
mod routes;
mod scheduler;
mod state;
mod triggers;
mod webhook;
mod worker;

use std::time::Duration;

use pumper_core::catalog::ContractsStatus;
use pumper_core::Config;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::state::AppState;

/// Env var carrying the Sentry DSN. Absent or blank means "no error reporting",
/// which is the default and must stay a silent no-op — see [`sentry_plan`].
const SENTRY_DSN_ENV: &str = "SENTRY_DSN";
/// Env var naming the deployment this process reports as (`production`,
/// `staging`, a developer's machine…).
const SENTRY_ENVIRONMENT_ENV: &str = "SENTRY_ENVIRONMENT";
/// Fallback environment tag. Honest rather than flattering: an unlabelled
/// process is a local one until someone says otherwise.
const SENTRY_DEFAULT_ENVIRONMENT: &str = "local";
/// Build identity, reused verbatim from the extraction-health run rows
/// (`build_id()` in crates/core/src/app.rs) so a Sentry release and a health run
/// row name the same build. Commit sha in CI, crate version otherwise.
const BUILD_ID_ENV: &str = "PUMPER_BUILD_ID";

/// How long in-flight HTTP work gets to finish after the shutdown token fires,
/// before the process stops waiting for it and moves on to the worker drain.
///
/// **Why this bound has to exist at all.** `axum::serve`'s graceful shutdown
/// resolves only once *every* connection has closed. One attached `/events`
/// dashboard was therefore enough to make that await never return: the SSE loop
/// ended only on `RecvError::Closed`, which needs the broadcast sender to drop,
/// and the sender lives in every `AppState` clone (worker, scheduler, janitors,
/// router). The process could only be killed, which skipped the worker drain and
/// the host-politeness snapshot. The streams now end on the token
/// ([`routes::events`], [`mcp::live`]) *and* the wait is bounded, because
/// "terminates" must not depend on every client behaving.
///
/// **Why a constant rather than a config key.** The number an operator actually
/// tunes is `[worker] shutdown_drain_secs` — how long a *job* gets — and it is
/// unaffected by this: the two windows run concurrently (both start when the
/// token fires), so the total stop time is the max of them, not the sum. This
/// one only covers request handlers, which are milliseconds outside the
/// streaming export path. Ten seconds is generous for that and leaves the
/// default worst case (25s drain) far inside systemd's 90s `TimeoutStopSec`.
const HTTP_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// How long the scheduler tick gets to finish after the shutdown token fires.
///
/// **Why joining it at all.** The scheduler task was spawned and its handle
/// dropped, so shutdown tore it down mid-`await` — between `enqueue` and
/// `touch_schedule` a scheduled job existed with its schedule's `last_run`
/// unmoved, and the same firing ran again on the next boot. Joining lets the
/// tick reach its own exit point, where the reconcile loop has already stopped
/// enqueuing (it checks the token between schedules).
///
/// **Why bounded rather than a bare `.await` like the worker.** Two of the four
/// piggybacked jobs are network-bound (the webhook dead-letter drain runs
/// in-line, by design), so a tick's duration is not a property this process
/// controls. The worker's drain is bounded by `[worker] shutdown_drain_secs`
/// from the inside; this loop has no such internal bound, so the bound lives
/// here. Concurrent with the HTTP grace and the worker drain — all three start
/// when the token fires — so it adds nothing to the common stop time.
const SCHEDULER_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// What [`sentry_plan`] decided to do with the environment it was handed.
///
/// Modelled as three cases rather than an `Option` so the *malformed*-DSN case
/// stays distinguishable from the *absent*-DSN case: absent is the normal,
/// silent default; malformed is an operator mistake worth exactly one line of
/// warning. Neither may stop the process from booting.
enum SentryPlan {
    /// No DSN configured. Reporting off, nothing logged.
    Disabled,
    /// A DSN was configured but does not parse. Reporting off, one warning.
    Invalid(String),
    /// Reporting on, with these options.
    Enabled(Box<sentry::ClientOptions>),
}

/// Decides whether this process reports errors, from raw env values.
///
/// Pure and total: it never panics, never touches the network, and never returns
/// an error the caller could accidentally propagate with `?`. A missing, empty,
/// or whitespace-only DSN yields [`SentryPlan::Disabled`], so an unconfigured
/// deployment boots byte-for-byte the way it did before Sentry existed. A DSN
/// that fails to parse is *also* non-fatal — a typo in a deploy env must not
/// take the service down.
///
/// `traces_sample_rate` stays at 0.0: performance tracing is deliberately out of
/// scope, errors are the whole point, and a non-zero default would quietly bill
/// the operator for spans they never asked for.
fn sentry_plan(dsn: Option<&str>, environment: Option<&str>, release: Option<&str>) -> SentryPlan {
    let Some(dsn) = dsn.map(str::trim).filter(|d| !d.is_empty()) else {
        return SentryPlan::Disabled;
    };
    let dsn = match dsn.parse::<sentry::types::Dsn>() {
        Ok(dsn) => dsn,
        Err(e) => return SentryPlan::Invalid(e.to_string()),
    };
    let environment = environment
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .unwrap_or(SENTRY_DEFAULT_ENVIRONMENT)
        .to_string();
    let release = release
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .unwrap_or(env!("CARGO_PKG_VERSION"))
        .to_string();
    SentryPlan::Enabled(Box::new(sentry::ClientOptions {
        dsn: Some(dsn),
        environment: Some(environment.into()),
        release: Some(release.into()),
        traces_sample_rate: 0.0,
        send_default_pii: false,
        ..Default::default()
    }))
}

/// Reads the environment, applies [`sentry_plan`], and initializes Sentry if it
/// said so. The returned guard flushes queued events on drop and *stops
/// reporting the moment it is dropped*, so the caller must hold it for the whole
/// process lifetime. `None` means reporting is off, which is not an error.
fn init_sentry() -> Option<sentry::ClientInitGuard> {
    let dsn = std::env::var(SENTRY_DSN_ENV).ok();
    let environment = std::env::var(SENTRY_ENVIRONMENT_ENV).ok();
    let release = std::env::var(BUILD_ID_ENV).ok();
    match sentry_plan(dsn.as_deref(), environment.as_deref(), release.as_deref()) {
        SentryPlan::Disabled => None,
        SentryPlan::Invalid(err) => {
            // Once, at boot — not per-event. The DSN itself is never echoed.
            tracing::warn!("{SENTRY_DSN_ENV} is set but unparseable ({err}); error reporting off");
            None
        }
        SentryPlan::Enabled(options) => Some(sentry::init(*options)),
    }
}

/// Synchronous entry point.
///
/// Sentry is initialized *before* the tokio runtime exists: the default
/// transport owns a background thread, and creating it inside a runtime that is
/// about to be torn down makes the final flush unreliable. The guard is bound to
/// a named local so it lives until `main` returns — binding it to `_` would drop
/// it immediately and silently disable reporting.
fn main() -> anyhow::Result<()> {
    load_dotenv();

    let _sentry_guard = init_sentry();

    // stdout logging is unchanged; the Sentry layer is *added* beside it. The
    // default `sentry-tracing` event filter maps `error!` to a Sentry event and
    // `warn!`/`info!` to breadcrumbs, which is exactly the accepted scope.
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .with(sentry_tracing::layer())
        .init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

/// One boot line saying what declared-contract enforcement can be **observed**
/// to do, from [`ContractsStatus`].
///
/// The anti-pattern: a malformed `catalog/data-sources.toml` was discovered only
/// as a per-job `warn` from the worker's publish seam plus two 500s on the
/// `/catalog/*` routes, while `/sources` kept rendering `contracts_enforce:
/// true` beside the last-good verdicts. Nothing ever said, in one place and
/// before any job ran, that the fleet was checking zero contracts. `error!`
/// rather than `warn!` because it is a silent, fleet-wide loss of a configured
/// guarantee — but still only a log: fail-open is the deliberate design
/// (`enforce_contracts`), and delivery must not stop because a TOML file broke.
fn log_contract_observability(status: &ContractsStatus) {
    if !status.catalog_ok {
        tracing::error!(
            enforce_configured = status.enforce_configured,
            error = status.catalog_error.as_deref().unwrap_or("unknown"),
            "catalog will not parse: every job skips declared-contract evaluation (fail-open), \
             and /catalog/sources + /catalog/health will fail — GET /sources reports this as \
             contracts.enforce_observed = false"
        );
    } else if status.declared > 0 || status.enforce_configured {
        tracing::info!(
            declared = status.declared,
            enforce = status.enforce_configured,
            reason = status.reason.as_deref().unwrap_or(""),
            "declared data contracts loaded from the catalog"
        );
    }
}

async fn run() -> anyhow::Result<()> {
    let config = Config::load()?;
    let state = AppState::init(config).await?;

    let recovered = state.storage.recover_stuck().await?;
    if recovered > 0 {
        tracing::info!(recovered, "re-queued jobs interrupted by previous shutdown");
    }

    // Say what declared-contract enforcement can actually be observed to do,
    // once, before the first job runs. Never blocking: the publish seam is
    // deliberately fail-open on an unreadable catalog, so this is visibility.
    log_contract_observability(&ContractsStatus::load(state.config.contracts.enforce).1);

    // Seed code-declared schedules into the DB (idempotent) so they become
    // editable rows the scheduler and API share.
    for app in state.registry.values() {
        if let Some(cron) = app.schedule() {
            state.storage.seed_schedule(app.name(), cron).await?;
        }
    }

    // Signal handling drives graceful shutdown: cancel the shared token so the
    // worker stops claiming, in-flight jobs drain, the scheduler/janitor exit,
    // and `axum::serve` stops accepting.
    let shutdown = state.shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received; draining");
        shutdown.cancel();
    });

    let worker = tokio::spawn(worker::run(state.clone()));
    // Joined at shutdown (see `SCHEDULER_SHUTDOWN_GRACE`) — an unjoined handle
    // meant the tick was torn down mid-await instead of finishing cleanly.
    let scheduler = tokio::spawn(scheduler::run(state.clone()));
    tokio::spawn(store_janitor(state.clone()));
    tokio::spawn(retention_janitor(state.clone()));

    let addr = format!("{}:{}", state.config.server.host, state.config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("pumper listening on http://{addr}");
    let shutdown = state.shutdown.clone();
    let server = tokio::spawn({
        let shutdown = shutdown.clone();
        let router = routes::router(state.clone());
        async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await
        }
    });

    // Bounded: after the token fires, in-flight connections get
    // `HTTP_SHUTDOWN_GRACE` and then the process proceeds regardless. The worker
    // has been draining in parallel since the same instant — this wait does not
    // delay it, it only stops the process from hanging on a client.
    let http_error = match await_bounded(server, &shutdown, HTTP_SHUTDOWN_GRACE).await {
        Some(Ok(Ok(()))) => {
            tracing::info!("http server stopped; awaiting worker drain");
            None
        }
        Some(Ok(Err(e))) => Some(anyhow::Error::new(e).context("http server")),
        Some(Err(e)) => Some(anyhow::Error::new(e).context("http server task")),
        None => {
            tracing::warn!(
                grace_secs = HTTP_SHUTDOWN_GRACE.as_secs(),
                "http connections still open at the shutdown grace deadline; abandoning them \
                 so the worker drain and the politeness snapshot still run"
            );
            None
        }
    };

    // The HTTP server has stopped accepting; wait for the worker to finish
    // draining (or re-queuing) in-flight jobs before exiting.
    let _ = worker.await;
    // And let the scheduler's current tick finish rather than dying mid-await
    // between an `enqueue` and its `touch_schedule`.
    if await_bounded(scheduler, &shutdown, SCHEDULER_SHUTDOWN_GRACE)
        .await
        .is_none()
    {
        tracing::warn!(
            grace_secs = SCHEDULER_SHUTDOWN_GRACE.as_secs(),
            "scheduler tick still running at the shutdown grace deadline; abandoning it"
        );
    }
    // Last, because the drain is the last thing that can teach the governor a
    // penalty: a job finishing during the drain must be in the snapshot too.
    state::final_host_penalty_flush(&state).await;
    tracing::info!("shutdown complete");
    match http_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Awaits `task`, but never longer than `grace` past the moment `shutdown`
/// fires.
///
/// `Some(join result)` means the task finished on its own; `None` means the
/// grace window elapsed first, and the task was **aborted** so it cannot keep
/// running behind the rest of the shutdown sequence.
///
/// The anti-pattern this makes impossible: awaiting a future whose completion is
/// gated on remote peers hanging up. A graceful HTTP shutdown is exactly that
/// shape — it is done when the last connection closes, which a client is under
/// no obligation to ever do.
async fn await_bounded<T>(
    mut task: tokio::task::JoinHandle<T>,
    shutdown: &CancellationToken,
    grace: Duration,
) -> Option<Result<T, tokio::task::JoinError>> {
    tokio::select! {
        joined = &mut task => Some(joined),
        _ = async {
            shutdown.cancelled().await;
            tokio::time::sleep(grace).await;
        } => {
            task.abort();
            None
        }
    }
}

/// Completes on the first Ctrl-C or platform terminate signal (SIGTERM on Unix,
/// console-close/shutdown on Windows).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(windows)]
    let terminate = async {
        use tokio::signal::windows;
        // Ctrl-Break, console-close, and system-shutdown are the SIGTERM
        // analogues on Windows; any of them triggers a graceful drain. A signal
        // that fails to register simply never fires (pending forever).
        let brk = async {
            match windows::ctrl_break() {
                Ok(mut s) => {
                    s.recv().await;
                }
                Err(_) => std::future::pending().await,
            }
        };
        let close = async {
            match windows::ctrl_close() {
                Ok(mut s) => {
                    s.recv().await;
                }
                Err(_) => std::future::pending().await,
            }
        };
        let shutdown = async {
            match windows::ctrl_shutdown() {
                Ok(mut s) => {
                    s.recv().await;
                }
                Err(_) => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = brk => {}
            _ = close => {}
            _ = shutdown => {}
        }
    };

    #[cfg(not(any(unix, windows)))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// Best-effort load of `KEY=VALUE` lines from a local `.env` into the process
/// environment, so keyed apps (e.g. census-density's `CENSUS_API_KEY`) work from
/// a plain `cargo run` without exporting the key by hand. Existing env vars win;
/// a missing or blank `.env` is ignored. Runs once at startup, before anything
/// reads the environment.
fn load_dotenv() {
    let Ok(contents) = std::fs::read_to_string(".env") else {
        return;
    };
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, val)) = line.split_once('=') {
            let key = key.trim();
            let val = val.trim().trim_matches('"').trim_matches('\'');
            if !key.is_empty() && std::env::var_os(key).is_none() {
                std::env::set_var(key, val);
            }
        }
    }
}

/// The **always-on** hourly janitor: the stores whose growth is derived state
/// nobody would miss, so bounding them needs no opt-in (unlike
/// [`retention_janitor`], which deletes accrued *value* and is therefore
/// off by default).
///
/// - **Expired `http_cache` entries** — a TTL that has passed is, by definition,
///   not servable.
/// - **Stale `tier_memory` rows** — rows carrying no pin, no strikes and no
///   penalty, both clocks past `[fetcher] host_memory_ttl_secs`. An HTTP win
///   zeroes a host's strikes but left the row behind forever, so this table grew
///   by one permanent row per host ever fetched. A row that still says
///   *anything* is never touched (see `TierMemory::prune_stale`).
/// - **Expired `research_cache` entries** — the most expensive bytes in the
///   store, and previously the only cache with no purge path at all.
/// - **The `revalidations` log** past `[refresher] retention_days`. The demand
///   path appends to it on every conditional GET, but its only pruner used to
///   live inside the refresher pass — unreachable at the shipping
///   `[refresher] enabled = false`.
/// - **`http_cache` rows past `[cache] max_rows`**, oldest-confirmed first. A
///   continuously-revalidated entry keeps pushing its own `expires_at` out, so
///   expiry alone never bounded this table.
///
/// Every pass is bounded work: three indexed deletes plus one `LIMIT`ed
/// eviction. Nothing here is data an operator would miss, which is what lets it
/// run without an opt-in.
async fn store_janitor(state: AppState) {
    let interval = std::time::Duration::from_secs(3600);
    let revalidation_days = state.config.refresher.retention_days;
    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
        match state.cache.purge_expired().await {
            Ok(n) if n > 0 => tracing::info!(purged = n, "store janitor evicted expired entries"),
            Ok(_) => {}
            Err(e) => tracing::warn!("cache purge failed: {e}"),
        }
        match state.research_cache.purge_expired().await {
            Ok(n) if n > 0 => {
                tracing::info!(purged = n, "store janitor evicted expired research answers")
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("research cache purge failed: {e}"),
        }
        match state.cache.prune_revalidations(revalidation_days).await {
            Ok(n) if n > 0 => tracing::info!(
                pruned = n,
                days = revalidation_days,
                "store janitor pruned the revalidation log"
            ),
            Ok(_) => {}
            Err(e) => tracing::warn!("revalidation prune failed: {e}"),
        }
        match state.cache.evict_over_cap().await {
            Ok(n) if n > 0 => tracing::info!(
                evicted = n,
                max_rows = state.cache.max_rows(),
                "store janitor evicted over-cap cache entries"
            ),
            Ok(_) => {}
            Err(e) => tracing::warn!("cache cap eviction failed: {e}"),
        }
        match state.tiers.prune_stale().await {
            Ok(n) if n > 0 => {
                tracing::info!(pruned = n, "store janitor reclaimed stale host memory")
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("host memory prune failed: {e}"),
        }
    }
}

/// Bounds every store that would otherwise grow without end.
///
/// **Revision history** older than the configured retention window (keeping the
/// newest N per record) — off unless `[storage] revision_retention_days > 0`,
/// because deleting a dataset's accrued history is data loss and must be opt-in.
///
/// **Archived bodies** under `artifacts_dir` older than
/// `[storage] artifact_retention_days` — same opt-in posture, and additionally
/// *pinned*: a body a replayable revision still points at is never reclaimed,
/// however old (`pumper_core::retention`). This is the one loop that touches the
/// artifact tree, and it runs the identical plan `GET /retention/preview` shows.
///
/// **The four unbounded ledgers** (`cost_events`, `webhook_deliveries`,
/// `job_yield`, `saved_search_seen`) — each with its own day knob, all off by
/// default, each scoped so an in-flight row survives (see
/// `Storage::prune_ledgers`).
///
/// **Extraction-health sketches and run rows** beyond
/// `[resilience] sketch_retention_runs` — on whenever detection is, because these
/// are a derived index rather than accrued value, and the config knob is
/// meaningless if nothing enforces it. Config validation already guarantees the
/// retention window is at least the baseline window, so this can never prune what
/// the detector is about to read.
async fn retention_janitor(state: AppState) {
    let days = state.config.storage.revision_retention_days;
    let keep_min = state.config.storage.revision_retention_keep_min;
    let artifact_days = state.config.storage.artifact_retention_days;
    let ledgers = state.config.storage.ledger_retention();
    let sketch_runs = state
        .health
        .enabled()
        .then(|| state.config.resilience.sketch_retention_runs);
    if !state
        .config
        .storage
        .any_retention_enabled(sketch_runs.is_some())
    {
        return; // nothing to bound
    }
    let interval = std::time::Duration::from_secs(6 * 3600);
    tracing::info!(
        days,
        keep_min,
        artifact_days,
        ?ledgers,
        ?sketch_runs,
        "retention janitor enabled"
    );
    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
        if days > 0 {
            let cutoff = chrono::Utc::now() - chrono::Duration::days(days as i64);
            match state.datasets.prune_revisions(cutoff, keep_min).await {
                Ok(n) if n > 0 => {
                    tracing::info!(pruned = n, days, "retention janitor pruned old revisions")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("revision prune failed: {e}"),
            }
        }
        if ledgers.any_enabled() {
            match state.storage.prune_ledgers(&ledgers).await {
                Ok(p) if p.total() > 0 => {
                    tracing::info!(?p, "retention janitor pruned ledgers")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("ledger prune failed: {e}"),
            }
        }
        if artifact_days > 0 {
            // Pins are read BEFORE the plan is built, from the same snapshot the
            // preview endpoint reads — a body that gained a replayable revision
            // since the last tick is protected on this one.
            match crate::routes::artifact_retention_plan(&state, artifact_days).await {
                Ok((files, plan)) => {
                    let (n, bytes) = pumper_core::retention::delete_artifacts(
                        &state.storage.artifacts_dir,
                        &files,
                        &plan,
                    );
                    if n > 0 {
                        tracing::info!(
                            files = n,
                            bytes,
                            pinned = plan.pinned_files,
                            "retention janitor reclaimed archived bodies"
                        );
                    }
                }
                Err(e) => tracing::warn!("artifact retention plan failed: {e}"),
            }
        }
        if let (Some(keep), Some(store)) = (sketch_runs, state.health.store()) {
            match store.prune(keep).await {
                Ok(n) if n > 0 => {
                    tracing::info!(pruned = n, keep, "retention janitor pruned health sketches")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("health sketch prune failed: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod sentry_tests {
    use super::{sentry_plan, SentryPlan, SENTRY_DEFAULT_ENVIRONMENT};

    /// A syntactically valid DSN pointing at a host that is never contacted in
    /// these tests (`sentry_plan` does no I/O). Not a credential.
    const FAKE_DSN: &str = "https://0123456789abcdef0123456789abcdef@o0.ingest.example.com/1";

    /// The property the whole feature hangs on: an unconfigured deployment must
    /// boot exactly as it did before Sentry existed. Every "no DSN" spelling an
    /// operator or a container runtime can produce — unset, empty string,
    /// whitespace — resolves to plain `Disabled`, never a panic and never an
    /// error the caller would `?` out of `main`.
    #[test]
    fn missing_dsn_disables_reporting_not_boot() {
        for dsn in [None, Some(""), Some("   "), Some("\t\n")] {
            assert!(
                matches!(sentry_plan(dsn, None, None), SentryPlan::Disabled),
                "dsn {dsn:?} must disable reporting silently"
            );
        }
    }

    /// A typo in a deploy env is an operator mistake, not an outage: the plan
    /// degrades to off (with one warning at the call site) instead of aborting.
    #[test]
    fn malformed_dsn_degrades_not_panics() {
        assert!(matches!(
            sentry_plan(Some("not-a-dsn"), None, None),
            SentryPlan::Invalid(_)
        ));
    }

    /// An enabled client is always environment-tagged. A blank/absent
    /// `SENTRY_ENVIRONMENT` falls back to the honest `local` rather than
    /// shipping untagged events that pool with production.
    #[test]
    fn blank_environment_falls_back_to_local_not_untagged() {
        for env in [None, Some(""), Some("  ")] {
            let SentryPlan::Enabled(opts) = sentry_plan(Some(FAKE_DSN), env, None) else {
                panic!("a valid dsn must enable reporting");
            };
            assert_eq!(
                opts.environment.as_deref(),
                Some(SENTRY_DEFAULT_ENVIRONMENT)
            );
        }
    }

    /// `release` comes from `PUMPER_BUILD_ID` so a Sentry release and an
    /// extraction-health run row name the same build; absent, it falls back to
    /// the crate version — the same fallback `build_id()` uses — never to empty.
    #[test]
    fn release_tracks_build_id_not_empty() {
        let SentryPlan::Enabled(opts) =
            sentry_plan(Some(FAKE_DSN), Some("staging"), Some("abc123"))
        else {
            panic!("a valid dsn must enable reporting");
        };
        assert_eq!(opts.release.as_deref(), Some("abc123"));
        assert_eq!(opts.environment.as_deref(), Some("staging"));
        // Performance tracing is out of scope; errors are the accepted surface.
        assert_eq!(opts.traces_sample_rate, 0.0);
        assert!(!opts.send_default_pii);

        let SentryPlan::Enabled(opts) = sentry_plan(Some(FAKE_DSN), None, Some(" ")) else {
            panic!("a valid dsn must enable reporting");
        };
        assert_eq!(opts.release.as_deref(), Some(env!("CARGO_PKG_VERSION")));
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::{await_bounded, HTTP_SHUTDOWN_GRACE};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// The whole point of the bound: a task that will never finish on its own
    /// must not hold the shutdown sequence open. `axum::serve`'s graceful
    /// shutdown is exactly that task whenever a client keeps a connection —
    /// an SSE dashboard — open, and the process used to die only to SIGKILL,
    /// skipping the worker drain and the politeness snapshot.
    #[tokio::test]
    async fn an_endless_task_is_abandoned_at_the_grace_deadline_not_awaited_forever() {
        let shutdown = CancellationToken::new();
        let ran = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let ran = ran.clone();
            async move {
                ran.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
        });
        shutdown.cancel();
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            await_bounded(task, &shutdown, Duration::from_millis(50)),
        )
        .await
        .expect("await_bounded must itself return within the bound");
        assert!(out.is_none(), "an endless task is reported as abandoned");
        assert!(ran.load(Ordering::SeqCst), "the task really did start");
    }

    /// The mirror risk, and the more damaging one: cutting off work that was
    /// about to finish. Inside the window the task's own result is returned
    /// untouched, so a clean stop still means a clean stop.
    #[tokio::test]
    async fn a_task_that_finishes_inside_the_window_is_joined_not_aborted() {
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            "drained"
        });
        shutdown.cancel();
        let out = await_bounded(task, &shutdown, Duration::from_secs(5)).await;
        assert_eq!(
            out.expect("finished on its own").expect("no panic"),
            "drained"
        );
    }

    /// Before the token fires there is no deadline at all — the grace window is
    /// measured from cancellation, never from the call. Otherwise a long-lived
    /// server would be torn down `grace` after boot.
    #[tokio::test]
    async fn the_window_starts_at_cancellation_not_at_the_call() {
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(120)).await;
            7u8
        });
        // Grace is far shorter than the task, but the token stays unfired for
        // longer than both, so the task still completes normally.
        let out = await_bounded(task, &shutdown, Duration::from_millis(10)).await;
        assert_eq!(out.expect("joined").expect("no panic"), 7);
    }

    /// The shipped window is a real, non-zero bound. A zero would make every
    /// stop abandon in-flight requests; an unbounded one is the bug this const
    /// exists to prevent.
    #[test]
    fn the_shipped_grace_is_bounded_and_non_zero() {
        assert!(HTTP_SHUTDOWN_GRACE > Duration::ZERO);
        assert!(HTTP_SHUTDOWN_GRACE <= Duration::from_secs(30));
    }
}
