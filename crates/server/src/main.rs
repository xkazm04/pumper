mod datahub;
#[cfg(test)]
mod e2e;
mod events;
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

use pumper_core::Config;
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

async fn run() -> anyhow::Result<()> {
    let config = Config::load()?;
    let state = AppState::init(config).await?;

    let recovered = state.storage.recover_stuck().await?;
    if recovered > 0 {
        tracing::info!(recovered, "re-queued jobs interrupted by previous shutdown");
    }

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
    tokio::spawn(scheduler::run(state.clone()));
    tokio::spawn(cache_janitor(state.clone()));
    tokio::spawn(retention_janitor(state.clone()));

    let addr = format!("{}:{}", state.config.server.host, state.config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("pumper listening on http://{addr}");
    let shutdown = state.shutdown.clone();
    axum::serve(listener, routes::router(state))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;

    // The HTTP server has stopped accepting; wait for the worker to finish
    // draining (or re-queuing) in-flight jobs before exiting.
    tracing::info!("http server stopped; awaiting worker drain");
    let _ = worker.await;
    tracing::info!("shutdown complete");
    Ok(())
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

/// Evicts expired cache entries hourly so the on-disk cache doesn't grow forever.
async fn cache_janitor(state: AppState) {
    let interval = std::time::Duration::from_secs(3600);
    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
        match state.cache.purge_expired().await {
            Ok(n) if n > 0 => tracing::info!(purged = n, "cache janitor evicted expired entries"),
            Ok(_) => {}
            Err(e) => tracing::warn!("cache purge failed: {e}"),
        }
    }
}

/// Bounds the two append-only stores.
///
/// **Revision history** older than the configured retention window (keeping the
/// newest N per record) — off unless `[storage] revision_retention_days > 0`,
/// because deleting a dataset's accrued history is data loss and must be opt-in.
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
    let sketch_runs = state
        .health
        .enabled()
        .then(|| state.config.resilience.sketch_retention_runs);
    if days == 0 && sketch_runs.is_none() {
        return; // nothing to bound
    }
    let interval = std::time::Duration::from_secs(6 * 3600);
    tracing::info!(days, keep_min, ?sketch_runs, "retention janitor enabled");
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
