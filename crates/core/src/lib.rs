pub mod app;
pub mod cache;
pub mod config;
pub mod crawl;
pub mod datasets;
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
pub mod storage;

pub use app::{AppContext, ScrapeApp};
pub use cache::HttpCache;
pub use config::Config;
pub use crawl::{crawl, CrawlConfig, CrawlPage, CrawlStats};
pub use datasets::{ChangeKind, Datasets, DupPair, Record, UpsertSummary};
pub use simhash::{hamming, simhash, simhash_value};
pub use extract::{extract_batch, extract_one, CompiledRuleSet, Rule, RuleSet};
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
pub use storage::{EnqueueOptions, Schedule, Storage};
