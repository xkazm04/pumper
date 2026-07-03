mod events;
mod registry;
mod routes;
mod scheduler;
mod state;
mod webhook;
mod worker;

use pumper_core::Config;
use tracing_subscriber::EnvFilter;

use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    tokio::spawn(worker::run(state.clone()));
    tokio::spawn(scheduler::run(state.clone()));
    tokio::spawn(cache_janitor(state.clone()));

    let addr = format!("{}:{}", state.config.server.host, state.config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("pumper listening on http://{addr}");
    axum::serve(listener, routes::router(state)).await?;
    Ok(())
}

/// Evicts expired cache entries hourly so the on-disk cache doesn't grow forever.
async fn cache_janitor(state: AppState) {
    let interval = std::time::Duration::from_secs(3600);
    loop {
        tokio::time::sleep(interval).await;
        match state.cache.purge_expired().await {
            Ok(n) if n > 0 => tracing::info!(purged = n, "cache janitor evicted expired entries"),
            Ok(_) => {}
            Err(e) => tracing::warn!("cache purge failed: {e}"),
        }
    }
}
