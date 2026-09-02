//! Full-text search capability. Every scraped record can be indexed and queried
//! with BM25 ranking. `core` defines only the trait; the implementation
//! (`engine-search`) embeds Tantivy — a Lucene-class search engine that runs
//! **in-process as a library**, with no external service to deploy or operate.
//! Python has no equivalent: full-text search there means running Elasticsearch
//! (a separate JVM service) or the slow, unmaintained pure-Python Whoosh.

use async_trait::async_trait;
use serde::Serialize;
use std::collections::BTreeMap;

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

impl SearchHit {
    /// The hit's source-record key inside its own dataset: the doc id minus its
    /// `<app>:<dataset>:` prefix (see [`SearchDoc::dataset_id`]). Falls back to
    /// the whole id for non-dataset docs (job-result docs).
    pub fn source_key(&self) -> &str {
        let prefix_len = self.app.len() + self.dataset.len() + 2;
        if self.id.len() > prefix_len
            && self.id.starts_with(&self.app)
            && self.id[self.app.len()..].starts_with(':')
            && self.id[self.app.len() + 1..].starts_with(&self.dataset)
            && self.id[prefix_len - 1..].starts_with(':')
        {
            &self.id[prefix_len..]
        } else {
            &self.id
        }
    }

    /// The dataset-record value this hit materializes to (M13 "queries as
    /// datasets"): stable display fields plus source provenance. The BM25 score
    /// is bucketed to one decimal so per-run ranking jitter does not churn fake
    /// `changed` revisions out of the view's change feed.
    pub fn materialize_value(&self) -> serde_json::Value {
        serde_json::json!({
            "title": self.title,
            "snippet": self.snippet,
            "url": self.url,
            "score": (f64::from(self.score) * 10.0).round() / 10.0,
            "source": {
                "app": self.app,
                "dataset": self.dataset,
                "key": self.source_key(),
            },
        })
    }
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
    /// Only hits whose extracted money amount (whole US dollars, index-time
    /// conservative extraction) is >= this. Docs with NO extracted amount never
    /// match an amount filter — the field is absent, not zero.
    pub amount_gte: Option<u64>,
    /// Only hits whose extracted amount is <= this (whole US dollars).
    pub amount_lte: Option<u64>,
    /// Only hits whose extracted deadline-like date (`event_date`, unix seconds
    /// UTC midnight) is at/after this. Absent field never matches.
    pub date_after: Option<i64>,
    /// Only hits whose extracted deadline-like date is at/before this.
    pub date_before: Option<i64>,
    /// Compute the app/dataset facet breakdowns. **Off by default** — facets
    /// require sampling far more docs than `limit` (decoding each), which is pure
    /// waste for a caller that reads only hit ids (the saved-search runner) or
    /// none at all. The `/search` HTTP route opts in; nothing else should.
    pub facets: bool,
}

impl SearchRequest {
    pub fn new(q: impl Into<String>, limit: usize) -> Self {
        Self {
            q: q.into(),
            limit,
            ..Default::default()
        }
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

/// Physical footprint of the index, for operator telemetry. `doc_count` alone
/// cannot answer "is this thing growing without bound?" — a corpus that upserts
/// keeps a flat doc count while ghost documents and unmerged segments pile up on
/// disk. Both fields are best-effort observations, never estimates: an
/// implementation that cannot measure them reports zero.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct SearchIndexStats {
    /// Bytes the index occupies on disk (sum of the index directory's files).
    pub disk_bytes: u64,
    /// Searchable segments the reader currently sees. A steadily climbing count
    /// means merges are not keeping up with writes.
    pub segment_count: u64,
}

/// One typed fact an enricher pulled out of a document (N11).
///
/// `kind` is the entity's NAME (`amount`, `event_date`, `currency`, `ico`, ...)
/// and is deliberately an open string: adding a kind is installing a plugin, not
/// changing the index schema -- the whole point of the `entities` JSON field.
/// `value` is a JSON scalar (or array); `span` is the byte range in the text the
/// enricher read, when it can name one.
///
/// Doctrine, inherited from the built-in regex pass: **no match = no entity**.
/// An enricher that found nothing returns an empty vec; it never emits a kind
/// with a null/zero placeholder, because an absent entity and a zero one are
/// different facts and a filter must not match the first.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Entity {
    pub kind: String,
    pub value: serde_json::Value,
    /// Byte range `[start, end)` in the enriched text, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<(usize, usize)>,
}

impl Entity {
    /// An entity with no span (the shape a whole-document enricher produces).
    pub fn new(kind: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Self {
            kind: kind.into(),
            value: value.into(),
            span: None,
        }
    }

    /// The same, carrying the byte range it was read from.
    pub fn spanned(
        kind: impl Into<String>,
        value: impl Into<serde_json::Value>,
        span: (usize, usize),
    ) -> Self {
        Self {
            kind: kind.into(),
            value: value.into(),
            span: Some(span),
        }
    }
}

/// The entity kind carrying a document's money amount (whole US dollars), and
/// the one the `amount` fast field is fed from. Named here rather than spelled
/// in three crates so an enricher, the index and a query cannot disagree.
pub const ENTITY_AMOUNT: &str = "amount";
/// The entity kind carrying a document's deadline-like date (unix seconds, UTC
/// midnight) -- the `event_date` fast field's source.
pub const ENTITY_EVENT_DATE: &str = "event_date";
/// The reserved name of the built-in regex enricher in `[search] enrichers`.
pub const ENRICHER_BUILTIN: &str = "builtin";
/// The prefix that makes a `[search] enrichers` entry a WASM plugin.
pub const ENRICHER_PLUGIN_PREFIX: &str = "plugin:";

/// One parsed `[search] enrichers` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnricherSpec {
    /// The shipped regex pass (`amount` + `event_date`).
    Builtin,
    /// A core-module WASM plugin, by the name the plugin host loaded it under.
    Plugin(String),
}

/// Parses one `[search] enrichers` entry, or says why it is not one.
///
/// Refuses by NAME rather than degrading: `"plugin:"` with nothing after it, or
/// an unknown bare word, would otherwise be indistinguishable from a working
/// entry once the list was built -- the enricher simply never runs, and a search
/// that returns no `currency` hits looks like a corpus with no currencies in it.
pub fn parse_enricher_spec(entry: &str) -> std::result::Result<EnricherSpec, String> {
    let entry = entry.trim();
    if entry == ENRICHER_BUILTIN {
        return Ok(EnricherSpec::Builtin);
    }
    if let Some(name) = entry.strip_prefix(ENRICHER_PLUGIN_PREFIX) {
        let name = name.trim();
        if name.is_empty() {
            return Err(format!(
                "`{ENRICHER_PLUGIN_PREFIX}` needs a plugin name after it (e.g. \
                 `{ENRICHER_PLUGIN_PREFIX}enrich-money-date`)"
            ));
        }
        return Ok(EnricherSpec::Plugin(name.to_string()));
    }
    Err(format!(
        "unknown enricher '{entry}': write `{ENRICHER_BUILTIN}` or \
         `{ENRICHER_PLUGIN_PREFIX}<plugin-name>`"
    ))
}

/// An index-time enrichment pass over a document's text (N11).
///
/// Runs BEFORE the index writer lock is taken, so an implementation may be slow
/// without serializing every other indexing path -- but it must be honest about
/// failure: a plugin that traps yields NO entities and is counted, never an
/// error that fails the batch. The index is a derived artifact; losing one
/// document's `currency` field is not worth losing the document.
#[async_trait]
pub trait Enricher: Send + Sync {
    /// The name this enricher reports under in `[search] enrichers` and in
    /// `GET /search/status`.
    fn name(&self) -> &str;

    /// Entities for one document's text. `now` is the document's own timestamp
    /// (`SearchDoc::indexed_at`), which the date rules judge "upcoming" against.
    async fn enrich(&self, text: &str, now: i64) -> Vec<Entity>;

    /// Entities for a whole batch, `result[i]` for `texts[i]`.
    ///
    /// Provided as a loop over [`enrich`](Enricher::enrich); overridden by
    /// implementations whose work is worth doing in one go (the built-in regex
    /// pass hands the whole batch to one blocking thread, which is where that
    /// CPU work has to stay).
    async fn enrich_batch(&self, texts: &[String], now: i64) -> Vec<Vec<Entity>> {
        let mut out = Vec::with_capacity(texts.len());
        for text in texts {
            out.push(self.enrich(text, now).await);
        }
        out
    }
}

/// What one enricher did, for `GET /search/status`.
///
/// `failures` is the field that makes the fail-open path visible: an enricher
/// that traps on every document keeps the index healthy and would otherwise be
/// indistinguishable from one that honestly finds nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct EnricherStat {
    pub name: String,
    /// Documents this enricher was offered.
    pub docs: u64,
    /// Entities it emitted (before collision merging).
    pub entities: u64,
    /// Documents it failed on (trap, malformed output, unknown plugin) -- each
    /// one yielded no entities and did NOT fail the index.
    pub failures: u64,
}

/// Merges one enricher's output into a document's entity map, FIRST WRITER
/// WINS.
///
/// Order is the configuration's: `[search] enrichers` is a list, and an earlier
/// entry outranks a later one on a colliding kind. That makes `["builtin",
/// "plugin:x"]` mean "the shipped rules decide `amount`, the plugin may add
/// anything else" -- the compatibility guarantee -- while an operator who wants
/// the plugin to own `amount` puts it first and can see that they did.
pub fn merge_entities(into: &mut BTreeMap<String, serde_json::Value>, entities: Vec<Entity>) {
    for entity in entities {
        into.entry(entity.kind).or_insert(entity.value);
    }
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

    /// Physical index telemetry (bytes on disk, segment count). Default: zeros —
    /// an implementation with no on-disk index reports nothing rather than a
    /// made-up number.
    async fn index_stats(&self) -> Result<SearchIndexStats> {
        Ok(SearchIndexStats::default())
    }

    /// What each configured enricher has done since boot (N11). Default: empty
    /// -- an implementation with no enrichment reports nothing rather than a
    /// row of zeros that reads as "ran and found nothing".
    fn enricher_stats(&self) -> Vec<EnricherStat> {
        Vec::new()
    }

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

#[cfg(test)]
mod enricher_tests {
    use super::*;

    /// The anti-pattern: accepting any string as an enricher name and letting an
    /// unknown one become a no-op. `plugin:` with nothing behind it, or a typo
    /// like `builtins`, then produces an index with the field silently missing --
    /// which reads exactly like a corpus that has no such entities in it.
    #[test]
    fn an_unknown_enricher_is_refused_by_name_not_silently_skipped() {
        assert_eq!(parse_enricher_spec("builtin"), Ok(EnricherSpec::Builtin));
        assert_eq!(parse_enricher_spec("  builtin "), Ok(EnricherSpec::Builtin));
        assert_eq!(
            parse_enricher_spec("plugin:enrich-money-date"),
            Ok(EnricherSpec::Plugin("enrich-money-date".into()))
        );

        let err = parse_enricher_spec("plugin:").expect_err("a bare prefix names no plugin");
        assert!(err.contains("needs a plugin name"), "{err}");
        let err = parse_enricher_spec("builtins").expect_err("a typo is not an enricher");
        assert!(err.contains("unknown enricher 'builtins'"), "{err}");
        let err = parse_enricher_spec("").expect_err("the empty entry names nothing");
        assert!(err.contains("unknown enricher"), "{err}");
    }

    /// The ordering contract `[search] enrichers` is written against: an EARLIER
    /// entry owns a colliding kind. The anti-pattern is last-writer-wins, where
    /// appending a plugin to the end of the list silently takes `amount` away
    /// from the shipped regexes that every existing query was calibrated on.
    #[test]
    fn an_earlier_enricher_keeps_a_colliding_kind_instead_of_being_overwritten() {
        let mut entities = BTreeMap::new();
        merge_entities(
            &mut entities,
            vec![
                Entity::new(ENTITY_AMOUNT, 250_000u64),
                Entity::new(ENTITY_EVENT_DATE, 1_767_225_600i64),
            ],
        );
        merge_entities(
            &mut entities,
            vec![
                // A later enricher's competing amount loses...
                Entity::new(ENTITY_AMOUNT, 7u64),
                // ...but a kind nobody claimed yet is added, which is the whole
                // point of installing one.
                Entity::spanned("currency", "czk", (3, 6)),
            ],
        );
        assert_eq!(entities[ENTITY_AMOUNT], serde_json::json!(250_000));
        assert_eq!(entities["currency"], serde_json::json!("czk"));
        assert_eq!(entities.len(), 3);
    }

    /// A span is metadata about where a value came from; it must not leak into
    /// the stored value or a `currency` field becomes an object nothing can
    /// filter on.
    #[test]
    fn a_span_stays_out_of_the_merged_value() {
        let mut entities = BTreeMap::new();
        merge_entities(
            &mut entities,
            vec![Entity::spanned("ico", "12345678", (0, 8))],
        );
        assert_eq!(entities["ico"], serde_json::json!("12345678"));
    }
}
