use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pumper_core::{
    Config, CostLedger, Datasets, EngineSet, Fetcher, Governor, HttpCache, HttpClient, NoPlugins,
    NoSearch, Plugins, ResearchCache, Resilience, ScrapeApp, Search, Storage, TierMemory,
};
use pumper_engine_archive::ArchiveEngine;
use pumper_engine_browser::BrowserEngine;
use pumper_engine_claude::ClaudeEngine;
use pumper_engine_http::HttpEngine;
use pumper_engine_remote::RemoteEngine;
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
    /// Extraction health: per-source degradation detection. Read by the worker's
    /// post-run hooks to suppress pushes and indexing for a source we no longer
    /// stand behind, and by `/sources` for the health table.
    pub health: Arc<Resilience>,
    /// Live politeness governor — exposed so the `/hosts` diagnostics can read
    /// the current learned penalty and `DELETE /hosts/{host}/memory` can clear it.
    pub governor: Arc<Governor>,
    pub engines: Arc<EngineSet>,
    /// Sandboxed WASM plugin host.
    pub plugins: Arc<dyn Plugins>,
    /// Embedded full-text search index.
    pub search: Arc<dyn Search>,
    pub registry: Arc<HashMap<String, Arc<dyn ScrapeApp>>>,
    /// Read-only listing entries for dynamic WASM apps discovered in
    /// `[plugins] app_dir` at boot (M28 v1 slice): surfaced on `GET /apps` with
    /// `dynamic: true, runnable: false`, and matched by the enqueue handler to
    /// reject them with a typed error instead of a blank 404. Never runnable —
    /// there is no execution path until the component-model host lands.
    pub dynamic_apps: Arc<Vec<serde_json::Value>>,
    /// Prepared trigger evaluation sets, keyed by (source kind, app/source) and
    /// stamped with `Storage::trigger_generation`. Every job completion asks it
    /// twice (dataset + terminal) and every inbound ingress event once, so the
    /// common "no triggers configured for this app" answer must not be a
    /// database round trip. Invalidated implicitly by the generation counter
    /// the storage layer bumps on create/enable-toggle/delete — see
    /// `crate::triggers::TriggerEvalCache`.
    pub trigger_cache: Arc<crate::triggers::TriggerEvalCache>,
    /// Pinged on enqueue so the worker picks up work without waiting a poll tick.
    pub notify: Arc<Notify>,
    /// Bounded pool a finished job's derived/outbound fan-out (search indexing,
    /// watch webhooks, dataset triggers, saved-search alerts, the terminal
    /// event) runs on, so that work no longer holds one of the worker's scrape
    /// permits. Drained on shutdown — see `crate::fanout`.
    pub fanout: Arc<crate::fanout::FanoutPool>,
    /// Bounded pool every outbound webhook delivery runs on — fresh dispatches,
    /// DLQ drain retries and manual replays alike. Deliveries used to be bare
    /// `tokio::spawn`s, i.e. outside the process's lifecycle entirely: shutdown
    /// exited with POSTs in flight and tests could only poll on a deadline.
    /// A SEPARATE instance from `fanout` on purpose — see the sizing rationale on
    /// `crate::webhook::DELIVERY_CONCURRENCY`. Drained on shutdown right after
    /// `fanout` (which is what produces deliveries).
    pub deliveries: Arc<crate::fanout::FanoutPool>,
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
    /// DataHub emission history: monotonic ok/failed counters plus the last
    /// success and the last failure kept SEPARATELY (a success must not erase a
    /// failure — that hid flapping), and the full-sync overlap flag. Surfaced on
    /// `GET /datahub/status`. In-memory only — emission is best-effort telemetry.
    pub datahub_last: crate::datahub::StatusCell,
    /// M26 governance state: `cost:pause`d apps + last poll summary, surfaced on
    /// `GET /datahub/status`. In-memory only — re-derived from DataHub each poll.
    pub datahub_govern: crate::datahub::GovernCell,
    /// Latest data-contract verdict per `<app>/<dataset>` (M20), recorded at the
    /// worker's publish seam and surfaced on `/catalog/health` + `/sources`.
    /// In-memory only — a verdict is per-run telemetry, re-established by the
    /// next run; std Mutex, only quick insert/clone, no await held.
    pub contract_verdicts: Arc<std::sync::Mutex<HashMap<String, serde_json::Value>>>,
}

/// The externally-supplied inputs of an [`AppState`]: the pieces `init` builds
/// from the real environment (storage connection, engines, plugin host, search
/// index, app registry). [`AppState::from_parts`] derives everything else
/// (stores, buses, tokens) purely from these — no IO, no spawned tasks — so a
/// test can assemble a headless state over a temp store with fake engines.
pub struct AppStateParts {
    pub config: Config,
    pub storage: Arc<Storage>,
    pub governor: Arc<Governor>,
    pub engines: Arc<EngineSet>,
    pub plugins: Arc<dyn Plugins>,
    pub search: Arc<dyn Search>,
    pub registry: HashMap<String, Arc<dyn ScrapeApp>>,
}

impl AppState {
    /// Pure assembly: derives the SQLite-backed stores and in-memory buses from
    /// the supplied parts. Spawns nothing; performs no IO beyond building a
    /// reqwest client. `init` layers the real environment on top.
    pub fn from_parts(parts: AppStateParts) -> anyhow::Result<Self> {
        let AppStateParts {
            config,
            storage,
            governor,
            engines,
            plugins,
            search,
            registry,
        } = parts;
        let datasets = Arc::new(
            Datasets::new(storage.pool())
                .with_derived_max_depth(config.derived.max_depth)
                .with_max_group_scan(config.derived.max_group_scan),
        );
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
        let health = Arc::new(Resilience::new(storage.pool(), &config.resilience));
        let webhook_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let events = Arc::new(EventBus::new(EVENT_BROADCAST_CAPACITY, EVENT_RING_CAPACITY));
        // Dynamic-app discovery (M28 v1). The one exception to "no IO" here,
        // and only when `[plugins] app_dir` is explicitly set (default: unset →
        // zero IO, so test states stay pure): scans the dir once and freezes the
        // read-only listing for the process lifetime.
        let dynamic_apps = Arc::new(crate::registry::dynamic_app_entries(
            &config.plugins,
            &registry,
        ));
        let fanout_concurrency = config.worker.fanout_concurrency;
        let fanout_max_queued = config.worker.fanout_max_queued;

        Ok(Self {
            config: Arc::new(config),
            storage,
            datasets,
            costs,
            cache,
            research_cache,
            tiers,
            health,
            governor,
            engines,
            plugins,
            search,
            registry: Arc::new(registry),
            dynamic_apps,
            trigger_cache: Arc::new(crate::triggers::TriggerEvalCache::new()),
            notify: Arc::new(Notify::new()),
            fanout: Arc::new(crate::fanout::FanoutPool::new(
                fanout_concurrency,
                fanout_max_queued,
            )),
            deliveries: Arc::new(crate::fanout::FanoutPool::new(
                crate::webhook::DELIVERY_CONCURRENCY,
                crate::webhook::DELIVERY_MAX_QUEUED,
            )),
            webhook_client,
            events,
            progress: Arc::new(ProgressStore::new()),
            shutdown: CancellationToken::new(),
            job_cancels: Arc::new(std::sync::Mutex::new(HashMap::new())),
            metrics_cache: Arc::new(tokio::sync::Mutex::new(None)),
            datahub_last: Arc::new(std::sync::Mutex::new(Default::default())),
            datahub_govern: Default::default(),
            contract_verdicts: Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
    }

    pub async fn init(config: Config) -> anyhow::Result<Self> {
        let storage = Arc::new(Storage::connect(&config.storage).await?);
        let governor = Arc::new(Governor::new(&config.governor));
        // The HTTP engine shares the cache the state will own; build it here and
        // hand the same instance to from_parts via config-derived construction
        // order (HttpCache::new is cheap and pool-backed, so two handles over the
        // same pool are equivalent — the cache table is the state).
        let cache = Arc::new(HttpCache::new(storage.pool(), &config.cache));

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
        // Tier-zero archive engine (`[archive]`, default OFF). Its CDX and
        // snapshot requests run through the SAME HttpEngine, so archive.org is
        // governed/cached/capped exactly like any other host.
        let archive: Option<Arc<dyn HttpClient>> = config
            .archive
            .enabled
            .then(|| Arc::new(ArchiveEngine::new(&config.archive, http.clone())) as _);
        // Learned API-recipe source (`[recipes]`, M05): always wired so a
        // per-request `use_recipes` opt-in works even with the global switch
        // off; with neither opt-in the fetcher never consults it.
        let recipes: Arc<dyn pumper_core::RecipeSource> = Arc::new(storage.recipes());
        // Remote fetch fabric (M17, `[remote]`, default OFF). Wired at the
        // live-HTTP tier position only when enabled AND nodes are configured
        // (enabled + no nodes = a serve-only node: /fetch-proxy answers peers,
        // nothing is dispatched). The engine round-robins peer /fetch-proxy
        // endpoints and falls back to the local HttpEngine on any node error;
        // its proxy POSTs also run through the local engine, so peers are
        // governed like any host.
        let remote: Option<Arc<dyn HttpClient>> = (config.remote.enabled
            && !config.remote.nodes.is_empty())
        .then(|| Arc::new(RemoteEngine::new(&config.remote, http.clone())) as _);
        let fetch = Fetcher::new(
            http.clone(),
            browser.clone(),
            claude.clone(),
            governor.clone(),
            &config.fetcher,
        )
        .with_archive(archive)
        .with_remote(remote)
        .with_recipes(Some(recipes), &config.recipes);
        let engines = Arc::new(EngineSet::new(http, browser, claude, fetch));

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

        let state = Self::from_parts(AppStateParts {
            config,
            storage,
            governor,
            engines,
            plugins,
            search,
            registry,
        })?;

        if state.health.enabled() {
            tracing::info!(
                enforce = state.health.enforcing(),
                "extraction-health detection enabled ({})",
                if state.health.enforcing() {
                    "verdicts gate writes, pushes and indexing"
                } else {
                    "soak mode: verdicts recorded, nothing gated"
                }
            );
        }

        // Restore the governor's learned per-host penalties from the last
        // write-behind snapshot so politeness survives a restart.
        match state.tiers.load_penalties().await {
            Ok(saved) => {
                for (host, penalty_ms) in saved {
                    state
                        .governor
                        .restore_penalty(&host, Duration::from_millis(penalty_ms));
                }
            }
            Err(e) => tracing::warn!("failed to restore host penalties: {e}"),
        }

        // Write-behind: periodically snapshot the governor's learned penalties
        // into the host-profile table so they persist across restarts.
        let persist_secs = state.config.fetcher.host_penalty_persist_secs;
        if persist_secs > 0 {
            let governor = state.governor.clone();
            let tiers = state.tiers.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(persist_secs));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    let snapshot: Vec<(String, u64)> = governor
                        .snapshot_penalties()
                        .into_iter()
                        .map(|(host, penalty)| {
                            (host, penalty.as_millis().min(u64::MAX as u128) as u64)
                        })
                        .collect();
                    if let Err(e) = tiers.save_penalties(&snapshot).await {
                        tracing::warn!("host penalty write-behind failed: {e}");
                    }
                }
            });
        }

        Ok(state)
    }
}
