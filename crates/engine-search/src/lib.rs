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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use tantivy::collector::{Count, MultiCollector, TopDocs};
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, RangeQuery, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, Value, FAST, INDEXED, STORED, STRING, TEXT,
};
use tantivy::{doc, Index, IndexReader, IndexWriter, Order, TantivyDocument, Term};

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

/// Background-commit cadence: the committer flushes at most this often, so a
/// burst of jobs amortizes into a handful of commits instead of one fsync each.
/// Small enough that search freshness lags by no more than this on the happy
/// path; a hard kill loses at most this window of uncommitted `index()` writes
/// (an accepted cost for a derived artifact — the backfill bin rebuilds it).
const COMMIT_INTERVAL: Duration = Duration::from_millis(250);
/// Commit early (don't wait for the interval) once this many docs are pending, to
/// bound the writer's in-memory buffer during a large backfill.
const COMMIT_PENDING_THRESHOLD: usize = 512;

pub struct TantivyIndex {
    index: Index,
    fields: Fields,
    writer: Arc<Mutex<IndexWriter>>,
    reader: IndexReader,
    /// Uncommitted `index()` docs since the last commit. Only mutated while the
    /// writer lock is held (or reset by a commit that holds it), so it stays
    /// consistent with the writer's actual uncommitted set.
    pending: Arc<AtomicUsize>,
    /// Wakes the background committer immediately (threshold crossed / flush).
    wake: Arc<Notify>,
    /// Signals the committer to do a final commit and stop (on Drop).
    shutdown: Arc<Notify>,
}

impl Drop for TantivyIndex {
    fn drop(&mut self) {
        // Let the committer flush the uncommitted tail and exit.
        self.shutdown.notify_one();
    }
}

/// Commits the writer and reloads the reader, then clears the pending count. Runs
/// the fsync on a blocking thread. Shared by the background committer and the
/// synchronous paths.
async fn commit_and_reload(
    writer: Arc<Mutex<IndexWriter>>,
    reader: IndexReader,
    pending: Arc<AtomicUsize>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut w = writer.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        w.commit().map_err(|e| Error::App(format!("commit: {e}")))?;
        reader.reload().map_err(|e| Error::App(format!("reader reload: {e}")))?;
        // Safe under the writer lock: no `index()` can add between the commit and
        // this reset, so it can't clear a doc that wasn't just committed.
        pending.store(0, Ordering::Relaxed);
        Ok(())
    })
    .await
    .map_err(|e| Error::App(format!("commit task panicked: {e}")))?
}

/// The background committer: commits pending `index()` writes on an interval, or
/// sooner when woken (pending threshold crossed / explicit flush wake), and does a
/// final commit on shutdown so a graceful stop doesn't drop the tail.
fn spawn_committer(
    writer: Arc<Mutex<IndexWriter>>,
    reader: IndexReader,
    pending: Arc<AtomicUsize>,
    wake: Arc<Notify>,
    shutdown: Arc<Notify>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(COMMIT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    if pending.load(Ordering::Relaxed) > 0 {
                        let _ = commit_and_reload(writer.clone(), reader.clone(), pending.clone()).await;
                    }
                    break;
                }
                _ = interval.tick() => {}
                _ = wake.notified() => {}
            }
            if pending.load(Ordering::Relaxed) == 0 {
                continue;
            }
            if let Err(e) = commit_and_reload(writer.clone(), reader.clone(), pending.clone()).await {
                tracing::warn!("background search commit failed: {e}");
            }
        }
    });
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
        let writer = Arc::new(Mutex::new(writer));
        let pending = Arc::new(AtomicUsize::new(0));
        let wake = Arc::new(Notify::new());
        let shutdown = Arc::new(Notify::new());
        spawn_committer(writer.clone(), reader.clone(), pending.clone(), wake.clone(), shutdown.clone());
        Ok(Self { index, fields, writer, reader, pending, wake, shutdown })
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
        let pending = self.pending.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut w = writer.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            edit(&mut w)?;
            w.commit().map_err(|e| Error::App(format!("commit: {e}")))?;
            reader.reload().map_err(|e| Error::App(format!("reader reload: {e}")))?;
            // This commit flushed every uncommitted write, incl. deferred index()s.
            pending.store(0, Ordering::Relaxed);
            Ok(())
        })
        .await
        .map_err(|e| Error::App(format!("{what} task panicked: {e}")))?
    }

    /// Applies an edit to the writer but does NOT commit — the background
    /// committer flushes it within `COMMIT_INTERVAL` (or sooner past the pending
    /// threshold). This is the amortization: a burst of `index()` calls shares one
    /// commit/fsync instead of paying one each. Callers that need immediate
    /// visibility (the saved-search runner, the backfill bin) call `flush`.
    async fn write_deferred<F>(&self, what: &'static str, added: usize, edit: F) -> Result<()>
    where
        F: FnOnce(&mut IndexWriter) -> Result<()> + Send + 'static,
    {
        let writer = self.writer.clone();
        let pending = self.pending.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut w = writer.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            edit(&mut w)?;
            pending.fetch_add(added, Ordering::Relaxed);
            Ok(())
        })
        .await
        .map_err(|e| Error::App(format!("{what} task panicked: {e}")))??;
        if self.pending.load(Ordering::Relaxed) >= COMMIT_PENDING_THRESHOLD {
            self.wake.notify_one();
        }
        Ok(())
    }
}

#[async_trait]
impl Search for TantivyIndex {
    async fn index(&self, docs: Vec<SearchDoc>) -> Result<()> {
        if docs.is_empty() {
            return Ok(());
        }
        let f = self.fields;
        let added = docs.len();
        // Deferred: the background committer flushes this, so hundreds of small
        // jobs no longer pay a full commit/fsync each.
        self.write_deferred("index", added, move |w| {
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

    async fn flush(&self) -> Result<()> {
        // Force a commit now and make prior deferred index() writes visible.
        // Reuses the commit epilogue with an empty edit.
        self.write_then_commit("flush", |_w| Ok(())).await
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

            // Rank enough docs to cover the requested page — and the facet sample
            // ONLY when facets are wanted. Facets decode every sampled doc, so a
            // facet-less query (the saved-search runner, the default UI page) ranks
            // and decodes just the `offset+limit` window instead of ≥1000 docs.
            let want_facets = req.facets;
            let page = req.offset.saturating_add(req.limit);
            let sample_size = if want_facets { page.max(FACET_SAMPLE) } else { page };
            // One collector pass yields both the ranked window and the EXACT match
            // total (via a Count collector) — so `total` is the real denominator
            // for paging, not the page size. Order by relevance or recency; the
            // recency collector yields the fast-field value in place of a BM25
            // score, normalized to `(f32, DocAddress)` (score 0.0) so the
            // hit-building loop is shared.
            let (top, total): (Vec<(f32, tantivy::DocAddress)>, u64) = match req.sort {
                SearchSort::Score => {
                    let mut multi = MultiCollector::new();
                    let count_h = multi.add_collector(Count);
                    let top_h =
                        multi.add_collector(TopDocs::with_limit(sample_size).order_by_score());
                    let mut fruits = searcher
                        .search(&query, &multi)
                        .map_err(|e| Error::App(format!("search: {e}")))?;
                    let total = count_h.extract(&mut fruits) as u64;
                    (top_h.extract(&mut fruits), total)
                }
                SearchSort::Newest => {
                    let mut multi = MultiCollector::new();
                    let count_h = multi.add_collector(Count);
                    let top_h = multi.add_collector(
                        TopDocs::with_limit(sample_size)
                            .order_by_fast_field::<i64>("indexed_at", Order::Desc),
                    );
                    let mut fruits = searcher
                        .search(&query, &multi)
                        .map_err(|e| Error::App(format!("search: {e}")))?;
                    let total = count_h.extract(&mut fruits) as u64;
                    let top = top_h
                        .extract(&mut fruits)
                        .into_iter()
                        .map(|(_ts, addr)| (0.0_f32, addr))
                        .collect();
                    (top, total)
                }
            };

            // Highlighted body fragments; best-effort (empty on failure).
            let snippets =
                tantivy::snippet::SnippetGenerator::create(&searcher, &*query, f.body).ok();

            let mut hits = Vec::with_capacity(req.limit.min(top.len()));
            let mut app_counts: std::collections::BTreeMap<String, u64> = Default::default();
            let mut dataset_counts: std::collections::BTreeMap<String, u64> = Default::default();
            for (i, (score, address)) in top.iter().enumerate() {
                let in_window = i >= req.offset && i < req.offset + req.limit;
                // Decode only the docs we use: the page window always, plus every
                // sampled doc when counting facets. (Without facets, sample_size ==
                // the window, so this skips nothing — the guard just makes intent
                // explicit and future-proofs a larger sample.)
                if !in_window && !want_facets {
                    continue;
                }
                let doc: TantivyDocument = searcher
                    .doc(*address)
                    .map_err(|e| Error::App(format!("fetch doc: {e}")))?;
                // Read stored fields directly off the doc — no full-doc
                // to_json/from_str round-trip (which serialized the whole body just
                // to read a handful of short fields).
                let get = |field| {
                    doc.get_first(field).and_then(|v| v.as_str()).unwrap_or("").to_string()
                };
                let (app, dataset) = (get(f.app), get(f.dataset));
                if want_facets {
                    *app_counts.entry(app.clone()).or_insert(0) += 1;
                    *dataset_counts.entry(dataset.clone()).or_insert(0) += 1;
                }
                if in_window {
                    let snippet = snippets
                        .as_ref()
                        .map(|g| g.snippet_from_doc(&doc).to_html())
                        .unwrap_or_default();
                    hits.push(SearchHit {
                        id: get(f.id),
                        app,
                        dataset,
                        url: get(f.url),
                        title: get(f.title),
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
                total,
            })
        })
        .await
        .map_err(|e| Error::App(format!("query task panicked: {e}")))?
    }
}
