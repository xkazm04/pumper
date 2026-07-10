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

    tokio::spawn(worker::run(state.clone()));
    tokio::spawn(scheduler::run(state.clone()));
    tokio::spawn(cache_janitor(state.clone()));

    let addr = format!("{}:{}", state.config.server.host, state.config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("pumper listening on http://{addr}");
    axum::serve(listener, routes::router(state)).await?;
    Ok(())
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
        tokio::time::sleep(interval).await;
        match state.cache.purge_expired().await {
            Ok(n) if n > 0 => tracing::info!(purged = n, "cache janitor evicted expired entries"),
            Ok(_) => {}
            Err(e) => tracing::warn!("cache purge failed: {e}"),
        }
    }
}
