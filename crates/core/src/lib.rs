// SQLite-backed modules (sqlx) — gated behind `storage` (default-on). The
// AppContext/ScrapeApp runtime lives here too because AppContext owns the
// Datasets store. Embedders that only need the engines + Fetcher build with
// `default-features = false` and get everything below the `storage` line.
#[cfg(feature = "storage")]
pub mod app;
#[cfg(feature = "storage")]
pub mod cache;
#[cfg(feature = "storage")]
pub mod costs;
#[cfg(feature = "storage")]
pub mod datasets;
#[cfg(feature = "storage")]
pub mod storage;

pub mod config;
pub mod crawl;
pub mod engine;
pub mod error;
pub mod extract;
pub mod fetcher;
pub mod governor;
pub mod job;
pub mod markdown;
pub mod plugin;
pub mod search;
pub mod simhash;

#[cfg(feature = "storage")]
pub use app::{AppContext, ScrapeApp};
#[cfg(feature = "storage")]
pub use cache::{HttpCache, ResearchCache};
#[cfg(feature = "storage")]
pub use costs::{CostEvent, CostLedger, CostSummary};
#[cfg(feature = "storage")]
pub use datasets::{diff_values, ChangeKind, Datasets, DupPair, Record, Revision, UpsertSummary};
#[cfg(feature = "storage")]
pub use storage::{EnqueueOptions, Schedule, Storage, Watch};

pub use config::Config;
pub use crawl::{crawl, CrawlConfig, CrawlPage, CrawlStats};
pub use simhash::{hamming, simhash, simhash_value};
pub use extract::{extract_batch, extract_one, CompiledRuleSet, FieldRule, Rule, RuleSet, Transform};
pub use engine::{
    Browser, EngineSet, HttpClient, HttpMethod, HttpRequest, HttpResponse, RenderRequest,
    RenderedPage, Researcher, ResearchOutput, ResearchRequest,
};
pub use error::{Error, Result};
pub use fetcher::{FetchOutcome, FetchRequest, FetchStrategy, Fetcher};
pub use governor::Governor;
pub use job::{Job, JobStatus};
pub use markdown::html_to_markdown;
pub use plugin::{NoPlugins, Plugins};
pub use search::{NoSearch, Search, SearchDoc, SearchHit};
