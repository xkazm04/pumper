//! Full-text search capability. Every scraped record can be indexed and queried
//! with BM25 ranking. `core` defines only the trait; the implementation
//! (`engine-search`) embeds Tantivy — a Lucene-class search engine that runs
//! **in-process as a library**, with no external service to deploy or operate.
//! Python has no equivalent: full-text search there means running Elasticsearch
//! (a separate JVM service) or the slow, unmaintained pure-Python Whoosh.

use async_trait::async_trait;
use serde::Serialize;

use crate::Result;

/// A document to index. `body` is the searchable text; the rest is stored for
/// display in results.
#[derive(Debug, Clone)]
pub struct SearchDoc {
    pub id: String,
    pub app: String,
    pub dataset: String,
    pub url: String,
    pub title: String,
    pub body: String,
    /// Unix seconds the record was last written — the recency dimension for
    /// `sort=newest` and `since=` filtering. The record's stored timestamp, or
    /// now for docs with none (job-result docs).
    pub indexed_at: i64,
}

impl SearchDoc {
    /// Stable doc id for a dataset record: `<app>:<dataset>:<key>`. The live index
    /// path, the delete path, and the offline backfill must all agree on this
    /// exactly, or a re-index duplicates and a delete misses.
    pub fn dataset_id(app: &str, dataset: &str, key: &str) -> String {
        format!("{app}:{dataset}:{key}")
    }

    /// Builds the search document for a stored dataset record, pulling url/title
    /// from the record's conventional fields. `indexed_at` is the record's stored
    /// timestamp in unix seconds (the recency dimension). Shared by the worker's
    /// post-job indexing and the `search-backfill` bin so the two produce
    /// identical docs.
    pub fn from_dataset_record(
        app: &str,
        dataset: &str,
        key: &str,
        rec: &serde_json::Value,
        indexed_at: i64,
    ) -> SearchDoc {
        let pick = |keys: &[&str]| -> String {
            keys.iter()
                .find_map(|k| rec.get(*k).and_then(serde_json::Value::as_str))
                .unwrap_or("")
                .to_string()
        };
        SearchDoc {
            id: Self::dataset_id(app, dataset, key),
            app: app.to_string(),
            dataset: dataset.to_string(),
            url: pick(&["_url", "url"]),
            title: pick(&["title", "name", "headline", "full_name"]),
            body: rec.to_string(),
            indexed_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    pub id: String,
    pub app: String,
    pub dataset: String,
    pub url: String,
    pub title: String,
    pub score: f32,
    /// Highlighted body fragment for this hit — matched terms wrapped in
    /// `<b>` tags. Empty when the document predates body storage.
    pub snippet: String,
}

/// Result ordering for a search.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SearchSort {
    /// BM25 relevance, highest first (the default).
    #[default]
    Score,
    /// Most recently indexed first — recency over relevance on a changing corpus.
    Newest,
}

/// A full-text query with optional app/dataset scoping.
#[derive(Debug, Clone, Default)]
pub struct SearchRequest {
    pub q: String,
    pub limit: usize,
    /// Restrict hits to one app.
    pub app: Option<String>,
    /// Restrict hits to one dataset.
    pub dataset: Option<String>,
    /// Typo tolerance: match terms within edit distance 1. Quoted phrases
    /// (`"exact phrase"`) work in either mode via the query syntax.
    pub fuzzy: bool,
    /// Result ordering (relevance or recency).
    pub sort: SearchSort,
    /// Only hits indexed at/after this unix-seconds instant (a "what's new" feed).
    pub since: Option<i64>,
    /// Skip this many ranked hits before `limit` — page 2 = `offset: limit`.
    pub offset: usize,
}

impl SearchRequest {
    pub fn new(q: impl Into<String>, limit: usize) -> Self {
        Self { q: q.into(), limit, ..Default::default() }
    }
}

/// One facet bucket: a field value and how many matching docs carry it.
#[derive(Debug, Clone, Serialize)]
pub struct FacetCount {
    pub value: String,
    pub count: u64,
}

/// Facet breakdowns over the matching documents (sampled on large result sets).
#[derive(Debug, Clone, Default, Serialize)]
pub struct SearchFacets {
    pub apps: Vec<FacetCount>,
    pub datasets: Vec<FacetCount>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SearchResponse {
    pub hits: Vec<SearchHit>,
    pub facets: SearchFacets,
    /// Total documents matching the query, independent of `limit`/`offset` — the
    /// denominator for paging (was silently reported as the page size).
    pub total: u64,
}

#[async_trait]
pub trait Search: Send + Sync {
    /// Indexes a batch of documents (re-indexing an existing `id` replaces it)
    /// and commits so the results are immediately queryable.
    async fn index(&self, docs: Vec<SearchDoc>) -> Result<()>;

    /// Runs a full-text query, returning ranked hits plus app/dataset facets
    /// over the matching set.
    async fn query(&self, req: SearchRequest) -> Result<SearchResponse>;

    /// Removes documents by id and commits.
    async fn delete_ids(&self, ids: &[String]) -> Result<()>;

    /// Removes every document of one app's dataset and commits — the cleanup
    /// path when a dataset is retired or re-imported from scratch.
    async fn delete_dataset(&self, app: &str, dataset: &str) -> Result<()>;

    /// Number of documents currently in the index. Zero on a fresh, wiped, or
    /// disabled index — the signal that a backfill is needed (an emptied index
    /// otherwise looks healthy: queries return 200 with fewer hits).
    async fn doc_count(&self) -> Result<u64>;

    /// Forces any deferred writes to commit and become queryable. `index()` may
    /// defer its commit for throughput, so a caller that must see its own writes
    /// immediately (a saved-search runner, an offline backfill before it reports)
    /// calls this. Default: no-op (implementations that commit synchronously need
    /// nothing here).
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Fallback used when search is disabled.
pub struct NoSearch;

#[async_trait]
impl Search for NoSearch {
    async fn index(&self, _docs: Vec<SearchDoc>) -> Result<()> {
        Ok(())
    }
    async fn query(&self, _req: SearchRequest) -> Result<SearchResponse> {
        Ok(SearchResponse::default())
    }
    async fn delete_ids(&self, _ids: &[String]) -> Result<()> {
        Ok(())
    }
    async fn delete_dataset(&self, _app: &str, _dataset: &str) -> Result<()> {
        Ok(())
    }
    async fn doc_count(&self) -> Result<u64> {
        Ok(0)
    }
}
