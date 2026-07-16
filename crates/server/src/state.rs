use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pumper_core::{
    Config, CostLedger, Datasets, EngineSet, Fetcher, Governor, HttpCache, NoPlugins, NoSearch,
    Plugins, ResearchCache, ScrapeApp, Search, Storage, TierMemory,
};
use pumper_engine_browser::BrowserEngine;
use pumper_engine_claude::ClaudeEngine;
use pumper_engine_http::HttpEngine;
use pumper_engine_search::TantivyIndex;
use pumper_engine_wasm::WasmPluginHost;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::events::EventBus;
use crate::progress::ProgressStore;

/// Capacity of the broadcast channel fanning live events to SSE subscribers.
const EVENT_BROADCAST_CAPACITY: usize = 512;
/// How many recent events the replay ring retains for `Last-Event-ID` resume
/// and broadcast-lag recovery. Older events fall out and trigger a `reset`.
const EVENT_RING_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub storage: Arc<Storage>,
    pub datasets: Arc<Datasets>,
    pub costs: Arc<CostLedger>,
    pub cache: Arc<HttpCache>,
    pub research_cache: Arc<ResearchCache>,
    pub tiers: Arc<TierMemory>,
    /// Live politeness governor — exposed so the `/hosts` diagnostics can read
    /// the current learned penalty and `DELETE /hosts/{host}/memory` can clear it.
    pub governor: Arc<Governor>,
    pub engines: Arc<EngineSet>,
    /// Sandboxed WASM plugin host.
    pub plugins: Arc<dyn Plugins>,
    /// Embedded full-text search index.
    pub search: Arc<dyn Search>,
    pub registry: Arc<HashMap<String, Arc<dyn ScrapeApp>>>,
    /// Pinged on enqueue so the worker picks up work without waiting a poll tick.
    pub notify: Arc<Notify>,
    /// Dedicated client for firing result webhooks.
    pub webhook_client: reqwest::Client,
    /// Fan-out of job status transitions to SSE subscribers, with a bounded
    /// replay ring backing `Last-Event-ID` resume.
    pub events: Arc<EventBus>,
    /// Latest live-progress snapshot per in-flight job (in-memory; surfaced on
    /// `GET /jobs/{id}`). Dropped on restart — progress is ephemeral telemetry.
    pub progress: Arc<ProgressStore>,
    /// Cancelled on SIGTERM/Ctrl-C to drive graceful shutdown: the worker stops
    /// claiming, in-flight jobs drain, and `axum::serve` stops accepting.
    pub shutdown: CancellationToken,
    /// Per-job cancellation tokens for jobs the worker is currently running,
    /// keyed by job id with the attempt number that owns the entry. `DELETE
    /// /jobs/{id}` on a running job fires its token; the owning worker task
    /// removes its entry on finish (attempt-matched so an overlapping re-claim's
    /// token is never clobbered). std Mutex — only quick insert/get/remove, no
    /// await held.
    pub job_cancels: Arc<std::sync::Mutex<HashMap<uuid::Uuid, (i64, CancellationToken)>>>,
    /// Short-TTL cache of the fully-rendered `/metrics` body, so a burst of
    /// Prometheus scrapes doesn't re-run the aggregate queries every time.
    pub metrics_cache: Arc<tokio::sync::Mutex<Option<(std::time::Instant, String)>>>,
}

impl AppState {
    pub async fn init(config: Config) -> anyhow::Result<Self> {
        let storage = Arc::new(Storage::connect(&config.storage).await?);
        let datasets = Arc::new(Datasets::new(storage.pool()));
        let costs = Arc::new(CostLedger::new(storage.pool()));
        let cache = Arc::new(HttpCache::new(storage.pool(), &config.cache));
        let research_cache = Arc::new(ResearchCache::new(
            storage.pool(),
            config.claude.research_cache_ttl_secs,
        ));
        let tiers = Arc::new(TierMemory::new(
            storage.pool(),
            config.fetcher.host_memory_ttl_secs,
        ));
        let governor = Arc::new(Governor::new(&config.governor));

        // Restore the governor's learned per-host penalties from the last
        // write-behind snapshot so politeness survives a restart.
        match tiers.load_penalties().await {
            Ok(saved) => {
                for (host, penalty_ms) in saved {
                    governor.restore_penalty(&host, Duration::from_millis(penalty_ms));
                }
            }
            Err(e) => tracing::warn!("failed to restore host penalties: {e}"),
        }

        // Write-behind: periodically snapshot the governor's learned penalties
        // into the host-profile table so they persist across restarts.
        let persist_secs = config.fetcher.host_penalty_persist_secs;
        if persist_secs > 0 {
            let governor = governor.clone();
            let tiers = tiers.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(persist_secs));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    let snapshot: Vec<(String, u64)> = governor
                        .snapshot_penalties()
                        .into_iter()
                        .map(|(host, penalty)| (host, penalty.as_millis().min(u64::MAX as u128) as u64))
                        .collect();
                    if let Err(e) = tiers.save_penalties(&snapshot).await {
                        tracing::warn!("host penalty write-behind failed: {e}");
                    }
                }
            });
        }

        // Session vault: both tiers resolve named profiles under the same root
        // (`[fetcher] profiles_dir`) — cookies.json for HTTP, browser/ for Chrome.
        let profiles_dir = config.fetcher.profiles_dir.clone();
        let http = Arc::new(HttpEngine::new(
            &config.http,
            governor.clone(),
            cache.clone(),
            profiles_dir.clone(),
        )?);
        let browser = Arc::new(BrowserEngine::new(&config.browser, profiles_dir));
        let claude = Arc::new(ClaudeEngine::new(&config.claude));
        let fetch =
            Fetcher::new(http.clone(), browser.clone(), claude.clone(), governor.clone(), &config.fetcher);
        let engines = EngineSet { http, browser, claude, fetch };

        let plugins: Arc<dyn Plugins> = if config.plugins.enabled {
            Arc::new(WasmPluginHost::new(&config.plugins)?)
        } else {
            Arc::new(NoPlugins)
        };
        let search: Arc<dyn Search> = if config.search.enabled {
            Arc::new(TantivyIndex::new(&config.search)?)
        } else {
            Arc::new(NoSearch)
        };

        // Collect explicitly rather than `.collect()`: a colliding ScrapeApp::name()
        // would silently overwrite the earlier app — it vanishes with no route, no
        // schedule, and a startup log that still claims success. A duplicate id is
        // a registration mistake, so fail loudly at boot.
        let apps = crate::registry::apps();
        let mut registry: HashMap<String, Arc<dyn ScrapeApp>> = HashMap::with_capacity(apps.len());
        for app in apps {
            let name = app.name().to_string();
            if registry.insert(name.clone(), app).is_some() {
                anyhow::bail!(
                    "duplicate app id '{name}' in registry::apps() — every ScrapeApp::name() must be unique"
                );
            }
        }
        tracing::info!(
            apps = ?registry.keys().collect::<Vec<_>>(),
            "registered scraping apps"
        );

        let webhook_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let events = Arc::new(EventBus::new(EVENT_BROADCAST_CAPACITY, EVENT_RING_CAPACITY));

        Ok(Self {
            config: Arc::new(config),
            storage,
            datasets,
            costs,
            cache,
            research_cache,
            tiers,
            governor,
            engines: Arc::new(engines),
            plugins,
            search,
            registry: Arc::new(registry),
            notify: Arc::new(Notify::new()),
            webhook_client,
            events,
            progress: Arc::new(ProgressStore::new()),
            shutdown: CancellationToken::new(),
            job_cancels: Arc::new(std::sync::Mutex::new(HashMap::new())),
            metrics_cache: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }
}
