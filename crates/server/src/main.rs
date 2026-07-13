mod events;
mod registry;
mod routes;
mod scheduler;
mod state;
mod triggers;
mod webhook;
mod worker;

use pumper_core::Config;
use tracing_subscriber::EnvFilter;

use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

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
