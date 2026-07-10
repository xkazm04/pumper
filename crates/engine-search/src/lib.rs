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
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, STORED, STRING, TEXT};
use tantivy::{doc, Document, Index, IndexReader, IndexWriter, TantivyDocument, Term};

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
}

pub struct TantivyIndex {
    index: Index,
    schema: Schema,
    fields: Fields,
    writer: Arc<Mutex<IndexWriter>>,
    reader: IndexReader,
}

/// True when the opened index already stores the body field (snippet-capable).
fn body_is_stored(index: &Index) -> bool {
    index
        .schema()
        .get_field("body")
        .map(|f| index.schema().get_field_entry(f).is_stored())
        .unwrap_or(false)
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
        let schema = builder.build();

        std::fs::create_dir_all(&cfg.dir)?;
        let index = match Index::open_in_dir(&cfg.dir) {
            Ok(index) if body_is_stored(&index) => index,
            Ok(_) => {
                // Pre-snippet index (body not stored): rebuild. The index is a
                // derived artifact — it refills as jobs run.
                tracing::warn!(
                    dir = %cfg.dir.display(),
                    "search index schema outdated (body not stored); rebuilding empty"
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

#[async_trait]
impl Search for TantivyIndex {
    async fn index(&self, docs: Vec<SearchDoc>) -> Result<()> {
        if docs.is_empty() {
            return Ok(());
        }
        let writer = self.writer.clone();
        let reader = self.reader.clone();
        let f = self.fields;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut w = writer.lock().unwrap();
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
                ))
                .map_err(|e| Error::App(format!("add_document: {e}")))?;
            }
            w.commit().map_err(|e| Error::App(format!("commit: {e}")))?;
            reader.reload().map_err(|e| Error::App(format!("reader reload: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::App(format!("index task panicked: {e}")))?
    }

    async fn query(&self, req: SearchRequest) -> Result<SearchResponse> {
        let index = self.index.clone();
        let reader = self.reader.clone();
        let schema = self.schema.clone();
        let f = self.fields;
        tokio::task::spawn_blocking(move || -> Result<SearchResponse> {
            let searcher = reader.searcher();
            let parser = QueryParser::for_index(&index, vec![f.title, f.body]);
            let parsed = parser
                .parse_query(&req.q)
                .map_err(|e| Error::App(format!("bad search query: {e}")))?;

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
            let query: Box<dyn Query> = if clauses.len() == 1 {
                clauses.pop().unwrap().1
            } else {
                Box::new(BooleanQuery::new(clauses))
            };

            let sample_size = req.limit.max(FACET_SAMPLE);
            let top = searcher
                .search(&query, &TopDocs::with_limit(sample_size).order_by_score())
                .map_err(|e| Error::App(format!("search: {e}")))?;

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
