//! Embedded full-text search (implements `pumper_core::Search`) using Tantivy.
//! The index is a memory-mapped directory on disk — no external service. BM25
//! ranking over the title + body fields; re-indexing an id replaces the prior
//! document.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pumper_core::config::SearchConfig;
use pumper_core::{
    Error, FacetCount, Result, Search, SearchDoc, SearchFacets, SearchHit, SearchRequest,
    SearchResponse,
};
use std::ops::Bound;

use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, RangeQuery, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, FAST, INDEXED, STORED, STRING, TEXT};
use tantivy::{doc, Document, Index, IndexReader, IndexWriter, Order, TantivyDocument, Term};

use pumper_core::SearchSort;

/// Facet counts are computed over at most this many top-ranked matches — an
/// honest sample that stays cheap on large result sets.
const FACET_SAMPLE: usize = 1_000;

#[derive(Clone, Copy)]
struct Fields {
    id: Field,
    app: Field,
    dataset: Field,
    url: Field,
    title: Field,
    body: Field,
    indexed_at: Field,
}

/// Field names the current build's schema expects. An opened index missing any of
/// these (or with body not stored) is an older schema and is rebuilt.
const SCHEMA_FIELDS: &[&str] = &["id", "app", "dataset", "url", "title", "body", "indexed_at"];

pub struct TantivyIndex {
    index: Index,
    schema: Schema,
    fields: Fields,
    writer: Arc<Mutex<IndexWriter>>,
    reader: IndexReader,
}

/// True when the opened index matches the current build's schema: every expected
/// field is present and `body` is stored (snippet-capable). A mismatch — an older
/// index missing a field this build added (e.g. `indexed_at`) — triggers a
/// rebuild. Generalizes the old body-stored probe so future field additions are
/// deliberate schema versions rather than silent incompatibilities.
fn schema_is_current(index: &Index) -> bool {
    let schema = index.schema();
    let all_present = SCHEMA_FIELDS.iter().all(|name| schema.get_field(name).is_ok());
    let body_stored = schema
        .get_field("body")
        .map(|f| schema.get_field_entry(f).is_stored())
        .unwrap_or(false);
    all_present && body_stored
}

impl TantivyIndex {
    pub fn new(cfg: &SearchConfig) -> Result<Self> {
        let mut builder = Schema::builder();
        // `id` is a single indexed term so we can delete-before-insert (upsert).
        builder.add_text_field("id", STRING | STORED);
        builder.add_text_field("app", STRING | STORED);
        builder.add_text_field("dataset", STRING | STORED);
        builder.add_text_field("url", STRING | STORED);
        builder.add_text_field("title", TEXT | STORED);
        // Body is stored so hits can carry highlighted snippets.
        builder.add_text_field("body", TEXT | STORED);
        // Recency dimension: FAST for order-by + range, INDEXED for the range
        // query, STORED so it can be returned. Unix seconds.
        builder.add_i64_field("indexed_at", INDEXED | STORED | FAST);
        let schema = builder.build();

        std::fs::create_dir_all(&cfg.dir)?;
        let index = match Index::open_in_dir(&cfg.dir) {
            Ok(index) if schema_is_current(&index) => index,
            Ok(_) => {
                // Older schema (missing a field this build added, or body not
                // stored): rebuild EMPTY. Previously indexed docs are gone until
                // re-indexed — the worker only refills a dataset when its app next
                // runs, so rebuild explicitly.
                tracing::warn!(
                    dir = %cfg.dir.display(),
                    "search index schema outdated; rebuilt EMPTY — previously indexed \
                     documents are gone. Rebuild from stored records with: \
                     cargo run -p pumper-server --bin search-backfill"
                );
                std::fs::remove_dir_all(&cfg.dir)?;
                std::fs::create_dir_all(&cfg.dir)?;
                Index::create_in_dir(&cfg.dir, schema.clone())
                    .map_err(|e| Error::App(format!("recreate search index: {e}")))?
            }
            Err(_) => Index::create_in_dir(&cfg.dir, schema.clone())
                .map_err(|e| Error::App(format!("create search index: {e}")))?,
        };
        // Resolve fields from the index's own schema (robust across reopens).
        let s = index.schema();
        let field = |name: &str| {
            s.get_field(name)
                .map_err(|e| Error::App(format!("search schema missing '{name}': {e}")))
        };
        let fields = Fields {
            id: field("id")?,
            app: field("app")?,
            dataset: field("dataset")?,
            url: field("url")?,
            title: field("title")?,
            body: field("body")?,
            indexed_at: field("indexed_at")?,
        };

        let writer: IndexWriter = index
            .writer(50_000_000)
            .map_err(|e| Error::App(format!("search writer: {e}")))?;
        let reader = index
            .reader()
            .map_err(|e| Error::App(format!("search reader: {e}")))?;

        tracing::info!(dir = %cfg.dir.display(), "opened search index");
        Ok(Self {
            index,
            schema: s,
            fields,
            writer: Arc::new(Mutex::new(writer)),
            reader,
        })
    }
}

impl TantivyIndex {
    /// Runs `edit` against the index writer on a blocking thread, then commits and
    /// reloads the reader. The lock → edit → commit → reload epilogue lives here
    /// once so the mutating paths can't drift apart.
    ///
    /// A poisoned writer lock is recovered rather than unwrapped: a single
    /// panicking write must not permanently disable all indexing and deletes for
    /// the process while reads keep succeeding and mask it.
    async fn write_then_commit<F>(&self, what: &'static str, edit: F) -> Result<()>
    where
        F: FnOnce(&mut IndexWriter) -> Result<()> + Send + 'static,
    {
        let writer = self.writer.clone();
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut w = writer.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            edit(&mut w)?;
            w.commit().map_err(|e| Error::App(format!("commit: {e}")))?;
            reader.reload().map_err(|e| Error::App(format!("reader reload: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::App(format!("{what} task panicked: {e}")))?
    }
}

#[async_trait]
impl Search for TantivyIndex {
    async fn index(&self, docs: Vec<SearchDoc>) -> Result<()> {
        if docs.is_empty() {
            return Ok(());
        }
        let f = self.fields;
        self.write_then_commit("index", move |w| {
            for d in docs {
                // Upsert: drop any prior document with this id, then add.
                w.delete_term(Term::from_field_text(f.id, &d.id));
                w.add_document(doc!(
                    f.id => d.id,
                    f.app => d.app,
                    f.dataset => d.dataset,
                    f.url => d.url,
                    f.title => d.title,
                    f.body => d.body,
                    f.indexed_at => d.indexed_at,
                ))
                .map_err(|e| Error::App(format!("add_document: {e}")))?;
            }
            Ok(())
        })
        .await
    }

    async fn delete_ids(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let f = self.fields;
        let ids = ids.to_vec();
        self.write_then_commit("delete", move |w| {
            for id in &ids {
                w.delete_term(Term::from_field_text(f.id, id));
            }
            Ok(())
        })
        .await
    }

    async fn delete_dataset(&self, app: &str, dataset: &str) -> Result<()> {
        let f = self.fields;
        let (app, dataset) = (app.to_string(), dataset.to_string());
        self.write_then_commit("delete", move |w| {
            // Dataset names may repeat across apps — delete the conjunction,
            // not the bare dataset term.
            let query = BooleanQuery::new(vec![
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(f.app, &app),
                        IndexRecordOption::Basic,
                    )) as Box<dyn Query>,
                ),
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(f.dataset, &dataset),
                        IndexRecordOption::Basic,
                    )),
                ),
            ]);
            w.delete_query(Box::new(query))
                .map_err(|e| Error::App(format!("delete_query: {e}")))?;
            Ok(())
        })
        .await
    }

    async fn doc_count(&self) -> Result<u64> {
        // num_docs reflects the last committed segment set the reader has loaded.
        Ok(self.reader.searcher().num_docs())
    }

    async fn query(&self, req: SearchRequest) -> Result<SearchResponse> {
        let index = self.index.clone();
        let reader = self.reader.clone();
        let schema = self.schema.clone();
        let f = self.fields;
        tokio::task::spawn_blocking(move || -> Result<SearchResponse> {
            let searcher = reader.searcher();
            let mut parser = QueryParser::for_index(&index, vec![f.title, f.body]);
            if req.fuzzy {
                // Edit-distance-1 matching with transposition counted as one
                // edit — catches the common single-typo case. Quoted phrases
                // still parse as exact phrase queries.
                parser.set_field_fuzzy(f.title, false, 1, true);
                parser.set_field_fuzzy(f.body, false, 1, true);
            }
            let parsed = parser
                .parse_query(&req.q)
                .map_err(|e| Error::BadRequest(format!("bad search query: {e}")))?;

            // Scope by app/dataset via exact term filters.
            let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Must, parsed)];
            if let Some(app) = &req.app {
                clauses.push((
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(f.app, app),
                        IndexRecordOption::Basic,
                    )),
                ));
            }
            if let Some(dataset) = &req.dataset {
                clauses.push((
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(f.dataset, dataset),
                        IndexRecordOption::Basic,
                    )),
                ));
            }
            // Recency floor: only docs indexed at/after `since` (a "what's new"
            // feed). Half-open [since, ∞) range on the fast i64 field.
            if let Some(since) = req.since {
                clauses.push((
                    Occur::Must,
                    Box::new(RangeQuery::new(
                        Bound::Included(Term::from_field_i64(f.indexed_at, since)),
                        Bound::Unbounded,
                    )),
                ));
            }
            let query: Box<dyn Query> = if clauses.len() == 1 {
                clauses.pop().unwrap().1
            } else {
                Box::new(BooleanQuery::new(clauses))
            };

            let sample_size = req.limit.max(FACET_SAMPLE);
            // Order by relevance or recency. The recency collector yields the
            // fast-field value in place of a BM25 score; normalize both to
            // `(f32 score, DocAddress)` so the hit-building loop is shared (a
            // newest-sorted hit carries score 0.0 — it was not ranked by relevance).
            let top: Vec<(f32, tantivy::DocAddress)> = match req.sort {
                SearchSort::Score => searcher
                    .search(&query, &TopDocs::with_limit(sample_size).order_by_score())
                    .map_err(|e| Error::App(format!("search: {e}")))?,
                SearchSort::Newest => searcher
                    .search(
                        &query,
                        &TopDocs::with_limit(sample_size)
                            .order_by_fast_field::<i64>("indexed_at", Order::Desc),
                    )
                    .map_err(|e| Error::App(format!("search: {e}")))?
                    .into_iter()
                    .map(|(_ts, addr)| (0.0_f32, addr))
                    .collect(),
            };

            // Highlighted body fragments; best-effort (empty on failure).
            let snippets =
                tantivy::snippet::SnippetGenerator::create(&searcher, &*query, f.body).ok();

            let mut hits = Vec::with_capacity(req.limit.min(top.len()));
            let mut app_counts: std::collections::BTreeMap<String, u64> = Default::default();
            let mut dataset_counts: std::collections::BTreeMap<String, u64> = Default::default();
            for (i, (score, address)) in top.iter().enumerate() {
                let doc: TantivyDocument = searcher
                    .doc(*address)
                    .map_err(|e| Error::App(format!("fetch doc: {e}")))?;
                // Stored fields serialize as {"field": ["value"], ...}.
                let json: serde_json::Value =
                    serde_json::from_str(&doc.to_json(&schema)).unwrap_or(serde_json::Value::Null);
                let get = |name: &str| {
                    json.get(name)
                        .and_then(|a| a.get(0))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                };
                let (app, dataset) = (get("app"), get("dataset"));
                *app_counts.entry(app.clone()).or_insert(0) += 1;
                *dataset_counts.entry(dataset.clone()).or_insert(0) += 1;
                if i < req.limit {
                    let snippet = snippets
                        .as_ref()
                        .map(|g| g.snippet_from_doc(&doc).to_html())
                        .unwrap_or_default();
                    hits.push(SearchHit {
                        id: get("id"),
                        app,
                        dataset,
                        url: get("url"),
                        title: get("title"),
                        score: *score,
                        snippet,
                    });
                }
            }
            let to_facets = |counts: std::collections::BTreeMap<String, u64>| {
                let mut list: Vec<FacetCount> = counts
                    .into_iter()
                    .map(|(value, count)| FacetCount { value, count })
                    .collect();
                list.sort_by(|a, b| b.count.cmp(&a.count));
                list
            };
            Ok(SearchResponse {
                hits,
                facets: SearchFacets {
                    apps: to_facets(app_counts),
                    datasets: to_facets(dataset_counts),
                },
            })
        })
        .await
        .map_err(|e| Error::App(format!("query task panicked: {e}")))?
    }
}
