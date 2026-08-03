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
#[cfg(feature = "storage")]
pub mod vcr;

pub mod catalog;
pub mod config;
pub mod crawl;
pub mod engine;
pub mod error;
pub mod extract;
pub mod fetcher;
pub mod governor;
pub mod induce;
pub mod jitter;
pub mod job;
pub mod json_salvage;
pub mod lru;
pub mod markdown;
pub mod plugin;
pub mod recipes;
pub mod resilience;
#[cfg(feature = "storage")]
pub mod retention;
pub mod search;
pub mod simhash;

#[cfg(feature = "storage")]
pub use app::{
    AppContext, AppManifest, CheckpointSink, CostClass, ManifestExample, NoCheckpoints, NoProgress,
    ProgressReporter, Requirement, ScrapeApp,
};
#[cfg(feature = "storage")]
pub use cache::{HttpCache, KeyFreshness, ResearchCache, StaleEntry};
#[cfg(feature = "storage")]
pub use costs::{extract_yields, CostEvent, CostLedger, CostSummary, SpentTotal, YieldEntry};
#[cfg(feature = "storage")]
pub use datasets::{
    derived_would_cycle, diff_values, filters_match, parse_aggregate, parse_aggregates,
    parse_filter_spec, parse_filter_specs, project_value, trust_label, validate_group, Aggregate,
    ChangeKind, Datasets, DerivedBackfill, DerivedGroup, DerivedLookup, DerivedSpec, DupPair,
    Provenance, Record, Revision, RevisionPage, UpsertSummary, TRUST_STABLE,
};
pub use resilience::{
    doc_signals, signals_batch, CohortDrift, Diagnosis, DocSignals, FetchHealth, ObservedDoc,
    RunReport, RunVerdict, SourceState, SourceVerdict,
};
#[cfg(feature = "storage")]
pub use resilience::{HealthStore, Resilience, SourceHealth, SourceRun};
#[cfg(feature = "storage")]
pub use retention::{
    artifact_is_reclaimable, artifact_usage, delete_artifacts, keep_reason,
    plan_artifact_retention, scan_artifact_tree, AppReclaim, ArtifactFile, ArtifactRef, KeepReason,
    RetentionPlan,
};
#[cfg(feature = "storage")]
pub use storage::{
    Delivery, EnqueueOptions, IngressSource, JobStages, JobTimingStats, LedgerPruned,
    LedgerRetention, NewDerivedSpec, NewSchedule, NewTrigger, PluginHook, RevisionCount,
    SavedSearch, Schedule, SearchMaterialize, Storage, Trigger, TriggerPluginHooks, Watch,
    YieldSummary, LEDGER_TABLES, MAX_CHECKPOINT_BYTES,
};
#[cfg(feature = "storage")]
pub use tiers::{
    plan_weather_import, HostProfile, TierMemory, WeatherEntry, WeatherPlan,
    WEATHER_IMPORT_PENALTY_CAP_MS,
};
#[cfg(feature = "storage")]
pub use vcr::{Cassette, CassetteEntry, Recorder, Vcr};

pub use catalog::{
    Catalog, Contract, ContractRange, ContractVerdict, PlanCreate, PlanDisable, PlanOrphan,
    PlanUpdate, ReconcilePlan, Source, CATALOG_MANAGED_BY,
};
pub use config::Config;
pub use crawl::{
    change_weight, crawl, due_score, CrawlConfig, CrawlPageRecord, CrawlProgressSnapshot,
    CrawlStats, PageSink, PageSource, ProgressFn, RevisitCadence, RevisitSeed,
};
pub use engine::{
    filled_fields_js, parse_filled_fields, profile_browser_dir, profile_cookies_path, profile_dir,
    validate_profile_name, Browser, CapturedCall, EngineSet, FilledField, HttpClient, HttpMethod,
    HttpRequest, HttpResponse, PageAction, RenderRequest, RenderedPage, ResearchOutput,
    ResearchRequest, Researcher, TransactEvidence, TransactRequest, FETCHED_VIA_HEADER,
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
pub use induce::{induce, ContainerStats, FieldSupport, InduceOptions, Induction};
pub use jitter::lcg_fraction;
pub use job::{Job, JobStatus};
pub use json_salvage::salvage_json;
pub use lru::{lru_touch, lru_touch_evict};
pub use markdown::html_to_markdown;
pub use plugin::{NoPlugins, Plugins};
#[cfg(feature = "storage")]
pub use recipes::RecipeStore;
pub use recipes::{discover_recipes, payload_overlaps, ApiRecipe, RecipeSource};
pub use search::{
    FacetCount, NoSearch, Search, SearchDoc, SearchFacets, SearchHit, SearchRequest,
    SearchResponse, SearchSort,
};
pub use simhash::{dom_simhash, dom_simhash_str, drift, hamming, simhash, simhash_value};
