//! Shared test harness — compiled only with the `test-support` feature.
//!
//! Depend on it from `[dev-dependencies]`:
//! `pumper-core = { workspace = true, features = ["test-support"] }`
//! (crates/core itself uses a self-referential dev-dependency for its own
//! integration tests). Never enable the feature from a normal `[dependencies]`
//! entry — this module ships panicking engine stubs.
//!
//! Provides the three primitives every storage-backed test used to re-roll:
//! - [`TempStore`] — RAII temp-dir SQLite with the full migration chain
//!   (replaces the copy-pasted `fresh_db` + 41 manual `remove_dir_all`s).
//! - [`Dead`] — a panicking `HttpClient`/`Browser`/`Researcher` for pure
//!   write-path tests, plus [`dead_engines`] for a whole `EngineSet`.
//! - [`TestContext`] — an `AppContext` builder so a new field to the 18-field
//!   struct is a one-site edit in test code.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::app::{AppContext, NoProgress};
use crate::cache::ResearchCache;
use crate::config::{FetcherConfig, GovernorConfig, ResilienceConfig, StorageConfig};
use crate::costs::{CostLedger, SpentTotal};
use crate::datasets::Datasets;
use crate::engine::{
    Browser, EngineSet, HttpClient, HttpRequest, HttpResponse, RenderRequest, RenderedPage,
    ResearchOutput, ResearchRequest, Researcher,
};
use crate::error::Result;
use crate::fetcher::Fetcher;
use crate::governor::Governor;
use crate::plugin::NoPlugins;
use crate::resilience::Resilience;
use crate::storage::Storage;
use crate::tiers::TierMemory;

/// A fresh temp-dir SQLite with the full migration chain, removed on drop.
///
/// Field order is load-bearing: `storage` is declared first so the pool closes
/// before the `TempDir` tries to delete the database file (Windows locks open
/// files).
pub struct TempStore {
    pub storage: Storage,
    dir: tempfile::TempDir,
}

impl TempStore {
    pub async fn new(tag: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix(&format!("pumper-{tag}-"))
            .tempdir()
            .expect("create temp dir");
        let cfg = StorageConfig {
            database_path: dir.path().join("pumper.db"),
            artifacts_dir: dir.path().join("artifacts"),
            ..StorageConfig::default()
        };
        let storage = Storage::connect(&cfg).await.expect("connect + migrate");
        Self { storage, dir }
    }

    pub fn datasets(&self) -> Datasets {
        Datasets::new(self.storage.pool())
    }

    /// The temp root (the database and artifacts dir live under it).
    pub fn path(&self) -> &std::path::Path {
        self.dir.path()
    }
}

/// Engines that must never be called — for pure write-path tests. Any fetch,
/// render, or research call is a test bug and panics with a clear message.
pub struct Dead;

#[async_trait]
impl HttpClient for Dead {
    async fn fetch(&self, _: HttpRequest) -> Result<HttpResponse> {
        panic!("no fetching in a write-path test")
    }
}
#[async_trait]
impl Browser for Dead {
    async fn render(&self, _: RenderRequest) -> Result<RenderedPage> {
        panic!("no rendering in a write-path test")
    }
}
#[async_trait]
impl Researcher for Dead {
    async fn research(&self, _: ResearchRequest) -> Result<ResearchOutput> {
        panic!("no research in a write-path test")
    }
}

/// A full `EngineSet` of [`Dead`] engines (governor + fetcher at defaults).
pub fn dead_engines() -> Arc<EngineSet> {
    Arc::new(EngineSet {
        http: Arc::new(Dead),
        browser: Arc::new(Dead),
        claude: Arc::new(Dead),
        fetch: Fetcher::new(
            Arc::new(Dead),
            Arc::new(Dead),
            Arc::new(Dead),
            Arc::new(Governor::new(&GovernorConfig::default())),
            &FetcherConfig::default(),
        ),
    })
}

/// Builder for a test `AppContext`. Defaults: empty params, [`Dead`] engines,
/// default-config `Resilience`, no budget, `NoPlugins`/`NoProgress`, artifacts
/// under `<storage.artifacts_dir>/<app>/job`.
pub struct TestContext<'a> {
    storage: &'a Storage,
    app: String,
    params: Value,
    engines: Option<Arc<EngineSet>>,
    health: Option<Arc<Resilience>>,
    budget_usd: Option<f64>,
    artifacts_dir: Option<std::path::PathBuf>,
}

impl<'a> TestContext<'a> {
    pub fn new(storage: &'a Storage, app: &str) -> Self {
        Self {
            storage,
            app: app.to_string(),
            params: json!({}),
            engines: None,
            health: None,
            budget_usd: None,
            artifacts_dir: None,
        }
    }

    /// Override the per-job artifacts dir (default: `<artifacts root>/<app>/job`).
    pub fn artifacts_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.artifacts_dir = Some(dir);
        self
    }

    pub fn params(mut self, params: Value) -> Self {
        self.params = params;
        self
    }

    pub fn engines(mut self, engines: Arc<EngineSet>) -> Self {
        self.engines = Some(engines);
        self
    }

    pub fn health(mut self, health: Arc<Resilience>) -> Self {
        self.health = Some(health);
        self
    }

    pub fn budget_usd(mut self, budget: f64) -> Self {
        self.budget_usd = Some(budget);
        self
    }

    pub fn build(self) -> AppContext {
        let pool = self.storage.pool();
        AppContext {
            job_id: Uuid::new_v4(),
            app: self.app.clone(),
            params: self.params,
            engines: self.engines.unwrap_or_else(dead_engines),
            datasets: Arc::new(Datasets::new(pool.clone())),
            costs: Arc::new(CostLedger::new(pool.clone())),
            budget_usd: self.budget_usd,
            spent_usd: Arc::new(SpentTotal::default()),
            research_cache: Arc::new(ResearchCache::new(pool.clone(), 0)),
            tiers: Arc::new(TierMemory::new(pool.clone(), 0)),
            health: self.health.unwrap_or_else(|| {
                Arc::new(Resilience::new(pool.clone(), &ResilienceConfig::default()))
            }),
            plugins: Arc::new(NoPlugins),
            progress: Arc::new(NoProgress),
            artifacts_dir: self
                .artifacts_dir
                .unwrap_or_else(|| self.storage.artifacts_dir.join(&self.app).join("job")),
        }
    }
}
