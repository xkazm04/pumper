//! Embedded full-text search (implements `pumper_core::Search`) using Tantivy.
//! The index is a memory-mapped directory on disk — no external service. BM25
//! ranking over the title + body fields; re-indexing an id replaces the prior
//! document.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pumper_core::config::SearchConfig;
use pumper_core::{Error, Result, Search, SearchDoc, SearchHit};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, STORED, STRING, TEXT};
use tantivy::{doc, Document, Index, IndexReader, IndexWriter, TantivyDocument, Term};

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

    async fn query(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let index = self.index.clone();
        let reader = self.reader.clone();
        let schema = self.schema.clone();
        let f = self.fields;
        let query = query.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<SearchHit>> {
            let searcher = reader.searcher();
            let parser = QueryParser::for_index(&index, vec![f.title, f.body]);
            let parsed = parser
                .parse_query(&query)
                .map_err(|e| Error::App(format!("bad search query: {e}")))?;
            let top = searcher
                .search(&parsed, &TopDocs::with_limit(limit).order_by_score())
                .map_err(|e| Error::App(format!("search: {e}")))?;

            // Highlighted body fragments; best-effort (empty on failure).
            let snippets = tantivy::snippet::SnippetGenerator::create(&searcher, &parsed, f.body).ok();

            let mut hits = Vec::with_capacity(top.len());
            for (score, address) in top {
                let doc: TantivyDocument = searcher
                    .doc(address)
                    .map_err(|e| Error::App(format!("fetch doc: {e}")))?;
                let snippet = snippets
                    .as_ref()
                    .map(|g| g.snippet_from_doc(&doc).to_html())
                    .unwrap_or_default();
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
                hits.push(SearchHit {
                    id: get("id"),
                    app: get("app"),
                    dataset: get("dataset"),
                    url: get("url"),
                    title: get("title"),
                    score,
                    snippet,
                });
            }
            Ok(hits)
        })
        .await
        .map_err(|e| Error::App(format!("query task panicked: {e}")))?
    }
}
