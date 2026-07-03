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
}

#[async_trait]
pub trait Search: Send + Sync {
    /// Indexes a batch of documents (re-indexing an existing `id` replaces it)
    /// and commits so the results are immediately queryable.
    async fn index(&self, docs: Vec<SearchDoc>) -> Result<()>;

    /// Runs a full-text query, returning up to `limit` ranked hits.
    async fn query(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>>;
}

/// Fallback used when search is disabled.
pub struct NoSearch;

#[async_trait]
impl Search for NoSearch {
    async fn index(&self, _docs: Vec<SearchDoc>) -> Result<()> {
        Ok(())
    }
    async fn query(&self, _query: &str, _limit: usize) -> Result<Vec<SearchHit>> {
        Ok(Vec::new())
    }
}
