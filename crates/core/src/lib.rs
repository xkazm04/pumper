// SQLite-backed modules (sqlx) — gated behind `storage` (default-on). The
// AppContext/ScrapeApp runtime lives here too because AppContext owns the
// Datasets store. Embedders that only need the engines + Fetcher build with
// `default-features = false` and get everything below the `storage` line.
#[cfg(feature = "storage")]
pub mod app;
#[cfg(feature = "storage")]
pub mod backup;
#[cfg(feature = "storage")]
pub mod cache;
#[cfg(feature = "storage")]
pub mod costs;
#[cfg(feature = "storage")]
pub mod datasets;
#[cfg(feature = "storage")]
pub mod storage;
#[cfg(feature = "test-support")]
pub mod testing;
#[cfg(feature = "storage")]
pub mod tiers;

pub mod catalog;
pub mod config;
pub mod crawl;
pub mod engine;
pub mod error;
pub mod extract;
pub mod fetcher;
pub mod governor;
pub mod jitter;
pub mod job;
pub mod json_salvage;
pub mod lru;
pub mod markdown;
pub mod plugin;
pub mod resilience;
pub mod search;
pub mod simhash;

#[cfg(feature = "storage")]
pub use app::{
    AppContext, CheckpointSink, NoCheckpoints, NoProgress, ProgressReporter, Requirement, ScrapeApp,
};
#[cfg(feature = "storage")]
pub use cache::{HttpCache, ResearchCache, StaleEntry};
#[cfg(feature = "storage")]
pub use costs::{CostEvent, CostLedger, CostSummary, SpentTotal};
#[cfg(feature = "storage")]
pub use datasets::{
    diff_values, trust_label, ChangeKind, Datasets, DupPair, Record, Revision, RevisionPage,
    UpsertSummary, TRUST_STABLE,
};
pub use resilience::{
    doc_signals, signals_batch, CohortDrift, Diagnosis, DocSignals, FetchHealth, ObservedDoc,
    RunReport, RunVerdict, SourceState, SourceVerdict,
};
#[cfg(feature = "storage")]
pub use resilience::{HealthStore, Resilience, SourceHealth, SourceRun};
#[cfg(feature = "storage")]
pub use storage::{
    Delivery, EnqueueOptions, IngressSource, JobTimingStats, NewSchedule, NewTrigger, SavedSearch,
    Schedule, Storage, Trigger, Watch, MAX_CHECKPOINT_BYTES,
};
#[cfg(feature = "storage")]
pub use tiers::{HostProfile, TierMemory};

pub use catalog::{
    Catalog, PlanCreate, PlanDisable, PlanOrphan, PlanUpdate, ReconcilePlan, Source,
    CATALOG_MANAGED_BY,
};
pub use config::Config;
pub use crawl::{
    crawl, CrawlConfig, CrawlPageRecord, CrawlProgressSnapshot, CrawlStats, PageSink, PageSource,
    ProgressFn, RevisitSeed,
};
pub use engine::{
    profile_browser_dir, profile_cookies_path, profile_dir, validate_profile_name, Browser,
    EngineSet, HttpClient, HttpMethod, HttpRequest, HttpResponse, PageAction, RenderRequest,
    RenderedPage, ResearchOutput, ResearchRequest, Researcher, FETCHED_VIA_HEADER,
    PROFILE_BROWSER_DIR, PROFILE_COOKIES_FILE, PROFILE_NAME_MAX_LEN, SNAPSHOT_TS_HEADER,
};
pub use error::{Error, Result};
pub use extract::{
    extract_batch, extract_batch_with_report, extract_one, extract_one_with_report, CoercionStatus,
    CompiledRuleSet, DocReport, FieldRule, FieldStatus, Rule, RuleSet, Transform,
};
pub use fetcher::{
    FetchOutcome, FetchRequest, FetchStrategy, FetchTier, Fetcher, TierTrace, TierVerdict,
};
pub use governor::Governor;
pub use jitter::lcg_fraction;
pub use job::{Job, JobStatus};
pub use json_salvage::salvage_json;
pub use lru::{lru_touch, lru_touch_evict};
pub use markdown::html_to_markdown;
pub use plugin::{NoPlugins, Plugins};
pub use search::{
    FacetCount, NoSearch, Search, SearchDoc, SearchFacets, SearchHit, SearchRequest,
    SearchResponse, SearchSort,
};
pub use simhash::{dom_simhash, dom_simhash_str, drift, hamming, simhash, simhash_value};
