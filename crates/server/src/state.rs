use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pumper_core::{
    Config, CostLedger, Datasets, EngineSet, Fetcher, Governor, HttpCache, NoPlugins, NoSearch,
    Plugins, ScrapeApp, Search, Storage,
};
use pumper_engine_browser::BrowserEngine;
use pumper_engine_claude::ClaudeEngine;
use pumper_engine_http::HttpEngine;
use pumper_engine_search::TantivyIndex;
use pumper_engine_wasm::WasmPluginHost;
use tokio::sync::{broadcast, Notify};

use crate::events::JobEvent;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub storage: Arc<Storage>,
    pub datasets: Arc<Datasets>,
    pub costs: Arc<CostLedger>,
    pub cache: Arc<HttpCache>,
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
    /// Fan-out of job status transitions to SSE subscribers.
    pub events: broadcast::Sender<JobEvent>,
}

impl AppState {
    pub async fn init(config: Config) -> anyhow::Result<Self> {
        let storage = Arc::new(Storage::connect(&config.storage).await?);
        let datasets = Arc::new(Datasets::new(storage.pool()));
        let costs = Arc::new(CostLedger::new(storage.pool()));
        let cache = Arc::new(HttpCache::new(storage.pool(), &config.cache));
        let governor = Arc::new(Governor::new(&config.governor));

        let http = Arc::new(HttpEngine::new(&config.http, governor, cache.clone())?);
        let browser = Arc::new(BrowserEngine::new(&config.browser));
        let claude = Arc::new(ClaudeEngine::new(&config.claude));
        let fetch = Fetcher::new(http.clone(), browser.clone(), claude.clone());
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

        let registry: HashMap<String, Arc<dyn ScrapeApp>> = crate::registry::apps()
            .into_iter()
            .map(|app| (app.name().to_string(), app))
            .collect();
        tracing::info!(
            apps = ?registry.keys().collect::<Vec<_>>(),
            "registered scraping apps"
        );

        let webhook_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let (events, _) = broadcast::channel(512);

        Ok(Self {
            config: Arc::new(config),
            storage,
            datasets,
            costs,
            cache,
            engines: Arc::new(engines),
            plugins,
            search,
            registry: Arc::new(registry),
            notify: Arc::new(Notify::new()),
            webhook_client,
            events,
        })
    }
}
