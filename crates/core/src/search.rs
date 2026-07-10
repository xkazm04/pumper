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

/// A full-text query with optional app/dataset scoping.
#[derive(Debug, Clone, Default)]
pub struct SearchRequest {
    pub q: String,
    pub limit: usize,
    /// Restrict hits to one app.
    pub app: Option<String>,
    /// Restrict hits to one dataset.
    pub dataset: Option<String>,
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
}
