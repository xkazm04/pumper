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

use crate::app::{AppContext, NoCheckpoints, NoProgress};
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

    /// Overridden so `Dead` keeps its contract — *any* engine call is a test bug
    /// and panics. The trait default returns a typed `Error::Browser` instead,
    /// which quietly turned "the app reached the browser" into an ordinary error
    /// a test could mistake for the refusal it was asserting on.
    async fn transact(&self, _: crate::TransactRequest) -> Result<crate::TransactEvidence> {
        panic!("no transacting in a write-path test")
    }
}
#[async_trait]
impl Researcher for Dead {
    async fn research(&self, _: ResearchRequest) -> Result<ResearchOutput> {
        panic!("no research in a write-path test")
    }
}

/// A `Researcher` that **replays recorded transcripts** instead of spawning the
/// `claude` CLI — the offline stand-in for the model in tests and evals.
///
/// Each script entry is keyed by a substring that must appear in the request
/// prompt (the URL, the fixture id, …). A prompt matching no entry is a *panic*,
/// not a default: that is how a regression in what we **send** (a dropped URL, a
/// mangled prompt template) fails the eval, while the replayed output is how a
/// regression in how we **handle** the answer fails it. Requests are recorded so
/// a test can assert on the budget clamp, turn caps and call count.
#[derive(Default)]
pub struct ScriptedResearcher {
    script: Vec<(String, ResearchOutput)>,
    calls: std::sync::Mutex<Vec<ResearchRequest>>,
}

impl ScriptedResearcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replays `out` for any prompt containing `key` (first match wins).
    /// An empty `key` matches every prompt.
    #[must_use]
    pub fn on(mut self, key: impl Into<String>, out: ResearchOutput) -> Self {
        self.script.push((key.into(), out));
        self
    }

    /// Replays plain text (no JSON body, no cost) for any prompt.
    #[must_use]
    pub fn always_text(self, text: impl Into<String>) -> Self {
        self.on("", research_output(text))
    }

    /// Every request this researcher was handed, in order.
    pub fn calls(&self) -> Vec<ResearchRequest> {
        self.calls.lock().expect("scripted researcher lock").clone()
    }

    pub fn call_count(&self) -> usize {
        self.calls.lock().expect("scripted researcher lock").len()
    }
}

#[async_trait]
impl Researcher for ScriptedResearcher {
    async fn research(&self, req: ResearchRequest) -> Result<ResearchOutput> {
        let hit = self
            .script
            .iter()
            .find(|(key, _)| key.is_empty() || req.prompt.contains(key.as_str()))
            .map(|(_, out)| out.clone());
        self.calls
            .lock()
            .expect("scripted researcher lock")
            .push(req.clone());
        hit.ok_or_else(|| {
            crate::Error::App(format!(
                "no recorded transcript matches this prompt — the request changed. \
                 Scripted keys: {:?}. Prompt began: {:?}",
                self.script.iter().map(|(k, _)| k).collect::<Vec<_>>(),
                req.prompt.chars().take(200).collect::<String>(),
            ))
        })
    }
}

/// A [`ResearchOutput`] carrying `text` (and its parsed JSON when it parses),
/// so a test or eval fixture doesn't restate the six telemetry fields.
pub fn research_output(text: impl Into<String>) -> ResearchOutput {
    let text = text.into();
    let json = serde_json::from_str::<Value>(&text).ok();
    ResearchOutput {
        text,
        json,
        cost_usd: Some(0.0),
        duration_ms: Some(0),
        num_turns: Some(1),
        session_id: None,
    }
}

/// A full `EngineSet` of [`Dead`] engines (governor + fetcher at defaults).
pub fn dead_engines() -> Arc<EngineSet> {
    engines_with(Arc::new(Dead), Arc::new(Dead), Arc::new(Dead))
}

/// An `EngineSet` wired from the three engines you care about, with a `Fetcher`
/// over the same trio at default config. The single place test code builds an
/// engine set, so `EngineSet`'s private researcher field stays private.
pub fn engines_with(
    http: Arc<dyn HttpClient>,
    browser: Arc<dyn Browser>,
    claude: Arc<dyn Researcher>,
) -> Arc<EngineSet> {
    let fetch = Fetcher::new(
        http.clone(),
        browser.clone(),
        claude.clone(),
        Arc::new(Governor::new(&GovernorConfig::default())),
        &FetcherConfig::default(),
    );
    Arc::new(EngineSet::new(http, browser, claude, fetch))
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
    research_cache_ttl_secs: u64,
    restored: Option<Value>,
    vcr: crate::vcr::Vcr,
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
            research_cache_ttl_secs: 0,
            restored: None,
            vcr: crate::vcr::Vcr::Off,
        }
    }

    /// Runs the context in a VCR mode (default: `Off`).
    pub fn vcr(mut self, vcr: crate::vcr::Vcr) -> Self {
        self.vcr = vcr;
        self
    }

    /// Hands the context a restored checkpoint, as the worker does on re-claim.
    pub fn restored(mut self, state: Value) -> Self {
        self.restored = Some(state);
        self
    }

    /// Enables the research cache behind [`AppContext::research`] (default: off,
    /// matching `ResearchCache`'s "ttl 0 = disabled"). Set it to exercise the
    /// chokepoint's dedupe path.
    pub fn research_cache_ttl_secs(mut self, ttl: u64) -> Self {
        self.research_cache_ttl_secs = ttl;
        self
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
            research_cache: Arc::new(ResearchCache::new(
                pool.clone(),
                self.research_cache_ttl_secs,
            )),
            tiers: Arc::new(TierMemory::new(pool.clone(), 0)),
            health: self.health.unwrap_or_else(|| {
                Arc::new(Resilience::new(pool.clone(), &ResilienceConfig::default()))
            }),
            recipes: Arc::new(crate::recipes::RecipeStore::new(pool.clone())),
            plugins: Arc::new(NoPlugins),
            progress: Arc::new(NoProgress),
            checkpoints: Arc::new(NoCheckpoints),
            restored: self.restored,
            vcr: self.vcr,
            artifacts_dir: self
                .artifacts_dir
                .unwrap_or_else(|| self.storage.artifacts_dir.join(&self.app).join("job")),
        }
    }
}
