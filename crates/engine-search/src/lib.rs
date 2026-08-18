//! Embedded full-text search (implements `pumper_core::Search`) using Tantivy.
//! The index is a memory-mapped directory on disk — no external service. BM25
//! ranking over the title + body fields; re-indexing an id replaces the prior
//! document.

pub mod enrich;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pumper_core::config::SearchConfig;
use pumper_core::{
    Error, FacetCount, Result, Search, SearchDoc, SearchFacets, SearchHit, SearchIndexStats,
    SearchRequest, SearchResponse,
};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use tantivy::collector::{Count, MultiCollector, TopDocs};
use tantivy::directory::{DirectoryLock, MmapDirectory, INDEX_WRITER_LOCK};
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, RangeQuery, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, Value, FAST, INDEXED, STORED, STRING, TEXT,
};
use tantivy::{doc, Directory, Index, IndexReader, IndexWriter, Order, TantivyDocument, Term};

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
    amount: Field,
    event_date: Field,
}

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
    /// The index directory, kept so `index_stats` can measure the on-disk
    /// footprint (Tantivy's `Index` does not expose its path portably).
    dir: std::path::PathBuf,
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
        let mut w = writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        w.commit().map_err(|e| Error::App(format!("commit: {e}")))?;
        reader
            .reload()
            .map_err(|e| Error::App(format!("reader reload: {e}")))?;
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
            if let Err(e) = commit_and_reload(writer.clone(), reader.clone(), pending.clone()).await
            {
                tracing::warn!("background search commit failed: {e}");
            }
        }
    });
}

/// The current build's index schema — the single authority. `TantivyIndex::new`
/// creates the index from it and [`schema_is_current`] compares against it, so
/// the two can never drift. `amount`/`event_date` are the entity-typed
/// enrichment fields (M14); adding a field here IS the schema-version bump — an
/// index built before it fails the equality check below, is wiped empty on open,
/// and must be rebuilt via the `search-backfill` bin.
fn build_schema() -> Schema {
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
    // Entity-typed enrichment (M14): both OPTIONAL per doc — absent when
    // extraction found nothing (never guessed). `amount` = largest currency
    // amount in the doc, whole US dollars. `event_date` = earliest upcoming
    // deadline-like date, unix seconds (UTC midnight). FAST + INDEXED for
    // range predicates; STORED so hits could surface them later.
    builder.add_u64_field("amount", INDEXED | STORED | FAST);
    builder.add_i64_field("event_date", INDEXED | STORED | FAST);
    builder.build()
}

/// True when the opened index matches the current build's schema EXACTLY —
/// same fields, same names, same TYPES, same stored/indexed/fast options,
/// in the same order — via Tantivy's structural `Schema` equality.
///
/// The gate must see the real target, not a proxy for it. The old check tested
/// field-name PRESENCE only: a field whose TYPE changed (e.g. `amount` retyped
/// from u64 to text) or an index carrying EXTRA fields (a newer index opened by
/// an older build — a downgrade) both passed the name check and then surfaced
/// as a runtime query error instead of triggering the rebuild that recovers
/// them. Comparing the whole schema closes that: any structural difference is
/// drift, and drift rebuilds. `build_schema` is the sole authority both this and
/// index creation read, so a fresh index always matches and only a real change
/// trips it.
fn schema_is_current(index: &Index) -> bool {
    index.schema() == build_schema()
}

// ---- Index lifecycle: opening, and the two destructive recoveries -----------

/// Tantivy's index manifest. `Index::exists` is literally "does this file
/// exist", so its presence — not a non-empty directory — is what makes
/// `Index::create_in_dir` refuse to create.
const META_FILE: &str = "meta.json";

/// Why an existing index directory could not be opened, as far as recovery is
/// concerned. Split from the Tantivy error so the decision is testable against a
/// directory alone.
#[derive(Debug, PartialEq, Eq)]
enum OpenFailure {
    /// No `meta.json`: never initialized (a fresh or previously emptied dir).
    /// `create_in_dir` succeeds as-is — nothing to move aside.
    Uninitialized,
    /// `meta.json` present but unreadable/unparseable. `open_in_dir` fails AND
    /// `create_in_dir` fails (it refuses a directory that already has a
    /// `meta.json`), so without moving the directory aside the server cannot
    /// boot at all.
    Corrupt,
}

fn classify_open_failure(dir: &Path) -> OpenFailure {
    if dir.join(META_FILE).exists() {
        OpenFailure::Corrupt
    } else {
        OpenFailure::Uninitialized
    }
}

/// The recovery an open decided on, computed while the (failed or outdated)
/// `Index` handle is still alive so the handle can be dropped *before* anything
/// touches the directory — on Windows an open handle makes remove/rename fail.
enum Recovery {
    /// Usable as opened.
    None,
    /// Opened fine, but the on-disk schema predates this build: wipe + recreate.
    SchemaDrift,
    /// Nothing there yet: plain create.
    Fresh,
    /// Present but unopenable (carries the Tantivy error text): quarantine.
    Corrupt(String),
}

/// First free `<dir>.corrupt.<n>` sibling. A counter rather than a timestamp so
/// the quarantine name is deterministic (tests can name it) and so two
/// quarantines in the same second cannot collide.
fn quarantine_path(dir: &Path) -> PathBuf {
    let stem = dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("search-index")
        .to_string();
    (0u32..)
        .map(|n| dir.with_file_name(format!("{stem}.corrupt.{n}")))
        .find(|candidate| !candidate.exists())
        .expect("a free .corrupt.<n> suffix always exists")
}

/// Takes the index directory's exclusive writer lock BEFORE anything
/// destructive runs, and fails loudly (naming the conflict) when someone else
/// holds it.
///
/// **What this lock does guarantee.** It is Tantivy's own `INDEX_WRITER_LOCK` —
/// the same lock `Index::writer()` holds for the life of an `IndexWriter`, and
/// on an `MmapDirectory` it is a *real OS advisory lock*
/// (`try_lock_exclusive` — `flock` on Unix, `LockFileEx` on Windows) taken on an
/// open handle to `.tantivy-writer.lock` inside the index dir, not merely the
/// file's existence. So a new-schema binary started while an old-schema server
/// is running finds the lock held by that server's writer and refuses to wipe
/// the index under it, on every platform. Because the OS owns the lock, a
/// crashed or `SIGKILL`ed holder releases it automatically: there is no stale
/// lock to clear by hand, and the lock file being *present* means nothing on its
/// own.
///
/// **What it does not.** It is advisory: it only excludes processes that ask for
/// it — Tantivy writers and this function. A stray `rm -rf`, a backup tool, or a
/// second copy of the directory is unaffected. It excludes *writers* only: a
/// peer holding just an `IndexReader` takes no lock, so a wipe can still pull
/// files out from under a reader-only process. And `flock`-family locks are
/// unreliable on network filesystems (NFS/SMB), where two hosts may both believe
/// they hold it — the local-first deployment this service targets is the case it
/// is honest for.
///
/// **Platform asymmetry, honestly.** Holding the lock means holding an open file
/// handle *inside* the index directory, and Windows refuses to rename or delete
/// a directory that contains an open handle. That is why the destructive steps
/// [`drain_dir`] the directory's contents in place instead of moving or removing
/// the directory itself — on Unix either would work (an unlinked inode survives
/// its open handles), but only draining works on both.
fn claim_index_dir(dir: &Path, reason: &str) -> Result<DirectoryLock> {
    let directory = MmapDirectory::open(dir)
        .map_err(|e| Error::App(format!("open search index dir {}: {e}", dir.display())))?;
    directory.acquire_lock(&INDEX_WRITER_LOCK).map_err(|e| {
        Error::App(format!(
            "refusing to rebuild the search index at {} ({reason}): its Tantivy writer lock \
             ({}) is held by another process, so a pumper server (or the search-backfill / \
             reindex bin) is using this index — rebuilding would delete the index under it. \
             Stop that process and retry. The lock is an OS lock, so it cannot be left behind \
             by a crash. ({e})",
            dir.display(),
            INDEX_WRITER_LOCK.filepath.display(),
        ))
    })
}

/// Empties `dir` **in place** — the directory itself, and the writer-lock file
/// this process is holding inside it, stay put. With `into = Some(path)` each
/// entry is moved there (quarantine) instead of deleted (wipe).
///
/// Draining rather than `remove_dir_all`/`rename` on the directory itself is
/// what keeps the claim live across the destructive step: the lock's file lives
/// inside `dir`, so moving or removing the directory would release the lock
/// mid-rebuild — and on Windows would fail outright, because the lock handle is
/// open.
fn drain_dir(dir: &Path, into: Option<&Path>) -> std::io::Result<()> {
    if let Some(into) = into {
        std::fs::create_dir_all(into)?;
    }
    let lock_file = INDEX_WRITER_LOCK.filepath.as_os_str();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name() == lock_file {
            continue;
        }
        let path = entry.path();
        match into {
            Some(into) => std::fs::rename(&path, into.join(entry.file_name()))?,
            None if entry.file_type()?.is_dir() => std::fs::remove_dir_all(&path)?,
            None => std::fs::remove_file(&path)?,
        }
    }
    Ok(())
}

/// Opens the index at `dir`, recovering from the two states that need a
/// destructive step first. Every destructive branch is guarded by
/// [`claim_index_dir`] and logs the loss it is about to take.
fn open_or_recover(dir: &Path, schema: &Schema) -> Result<Index> {
    std::fs::create_dir_all(dir)?;
    let opened = Index::open_in_dir(dir);
    let plan = match &opened {
        Ok(index) if schema_is_current(index) => Recovery::None,
        Ok(_) => Recovery::SchemaDrift,
        Err(e) => match classify_open_failure(dir) {
            OpenFailure::Uninitialized => Recovery::Fresh,
            OpenFailure::Corrupt => Recovery::Corrupt(e.to_string()),
        },
    };
    if matches!(plan, Recovery::None) {
        return opened.map_err(|e| Error::App(format!("open search index: {e}")));
    }
    // Release the outdated/failed handle before touching the directory.
    drop(opened);

    match plan {
        Recovery::None => unreachable!("returned above"),
        Recovery::Fresh => Index::create_in_dir(dir, schema.clone())
            .map_err(|e| Error::App(format!("create search index: {e}"))),
        Recovery::SchemaDrift => {
            let claim = claim_index_dir(dir, "the on-disk schema predates this build")?;
            tracing::warn!(
                dir = %dir.display(),
                "search index schema outdated; rebuilding EMPTY — previously indexed \
                 documents are gone. Rebuild from stored records with: \
                 cargo run -p pumper-server --bin search-backfill"
            );
            drain_dir(dir, None)?;
            let index = Index::create_in_dir(dir, schema.clone())
                .map_err(|e| Error::App(format!("recreate search index: {e}")))?;
            // Released before the caller opens the real writer, which takes this
            // very lock.
            drop(claim);
            Ok(index)
        }
        Recovery::Corrupt(err) => {
            let claim = claim_index_dir(dir, "its meta.json is present but unreadable")?;
            let aside = quarantine_path(dir);
            drain_dir(dir, Some(&aside))?;
            tracing::error!(
                dir = %dir.display(),
                quarantined = %aside.display(),
                "search index could not be opened ({err}); moved its files aside and \
                 started an EMPTY index in their place — boot continues, but every previously \
                 indexed document is gone until: \
                 cargo run -p pumper-server --bin search-backfill"
            );
            let index = Index::create_in_dir(dir, schema.clone())
                .map_err(|e| Error::App(format!("recreate search index: {e}")))?;
            drop(claim);
            Ok(index)
        }
    }
}

/// A document with its index-time entity enrichment already computed (M14):
/// conservative regex-only extraction over title+body. No match = field ABSENT on
/// the doc (a range filter then simply never matches it).
struct EnrichedDoc {
    doc: SearchDoc,
    amount: Option<u64>,
    event_date: Option<i64>,
}

/// Enriches a batch. Pure and lock-free by construction — this is the work that
/// used to run inside the writer-lock closure.
fn enrich_docs(docs: Vec<SearchDoc>) -> Vec<EnrichedDoc> {
    docs.into_iter()
        .map(|doc| {
            let text = format!("{}\n{}", doc.title, doc.body);
            let (amount, event_date) = enrich::enrich_fields(&text, doc.indexed_at);
            EnrichedDoc {
                doc,
                amount,
                event_date,
            }
        })
        .collect()
}

/// Total bytes of every regular file under `dir` (recursively). Best-effort: an
/// unreadable entry contributes 0 rather than failing the whole measurement —
/// telemetry must never take down `/search/status`.
fn dir_size_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total = total.saturating_add(dir_size_bytes(&entry.path()));
        } else {
            total = total.saturating_add(meta.len());
        }
    }
    total
}

impl TantivyIndex {
    pub fn new(cfg: &SearchConfig) -> Result<Self> {
        let schema = build_schema();

        // Opening is where the index's two destructive recoveries live (schema
        // drift → wipe, corrupt meta.json → quarantine). Both are guarded by the
        // directory's writer lock, so a new-schema binary can never delete a
        // running server's index behind its back.
        let index = open_or_recover(&cfg.dir, &schema)?;
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
            amount: field("amount")?,
            event_date: field("event_date")?,
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
        spawn_committer(
            writer.clone(),
            reader.clone(),
            pending.clone(),
            wake.clone(),
            shutdown.clone(),
        );
        Ok(Self {
            index,
            dir: cfg.dir.clone(),
            fields,
            writer,
            reader,
            pending,
            wake,
            shutdown,
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
        let pending = self.pending.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut w = writer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            edit(&mut w)?;
            w.commit().map_err(|e| Error::App(format!("commit: {e}")))?;
            reader
                .reload()
                .map_err(|e| Error::App(format!("reader reload: {e}")))?;
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
            let mut w = writer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        // Enrichment runs BEFORE the writer lock is taken, on its own blocking
        // thread. It is pure CPU work (regex scans over every body) that needs
        // nothing from the writer, and running it inside the lock both serialized
        // it against every other indexing path and — until the char-boundary fix —
        // put a panic site inside the lock, where it poisoned the mutex and took
        // the whole batch with it.
        let prepared = tokio::task::spawn_blocking(move || enrich_docs(docs))
            .await
            .map_err(|e| Error::App(format!("enrich task panicked: {e}")))?;
        // Deferred: the background committer flushes this, so hundreds of small
        // jobs no longer pay a full commit/fsync each. The lock section below is
        // index operations only.
        self.write_deferred("index", added, move |w| {
            for p in prepared {
                let d = p.doc;
                // Upsert: drop any prior document with this id, then add.
                w.delete_term(Term::from_field_text(f.id, &d.id));
                let mut tdoc = doc!(
                    f.id => d.id,
                    f.app => d.app,
                    f.dataset => d.dataset,
                    f.url => d.url,
                    f.title => d.title,
                    f.body => d.body,
                    f.indexed_at => d.indexed_at,
                );
                if let Some(a) = p.amount {
                    tdoc.add_u64(f.amount, a);
                }
                if let Some(ts) = p.event_date {
                    tdoc.add_i64(f.event_date, ts);
                }
                w.add_document(tdoc)
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

    async fn index_stats(&self) -> Result<SearchIndexStats> {
        // Segments as the reader currently sees them (committed set) — the same
        // vantage point `doc_count` reports from.
        let segment_count = self.reader.searcher().segment_readers().len() as u64;
        let dir = self.dir.clone();
        let disk_bytes = tokio::task::spawn_blocking(move || dir_size_bytes(&dir))
            .await
            .map_err(|e| Error::App(format!("index stats task panicked: {e}")))?;
        Ok(SearchIndexStats {
            disk_bytes,
            segment_count,
        })
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
            // Entity-typed range predicates (M14). Docs where extraction found
            // no amount / no deadline have the field ABSENT and never match a
            // range clause — filtering by amount implies "has an amount".
            if req.amount_gte.is_some() || req.amount_lte.is_some() {
                let lower = req
                    .amount_gte
                    .map(|v| Bound::Included(Term::from_field_u64(f.amount, v)))
                    .unwrap_or(Bound::Unbounded);
                let upper = req
                    .amount_lte
                    .map(|v| Bound::Included(Term::from_field_u64(f.amount, v)))
                    .unwrap_or(Bound::Unbounded);
                clauses.push((Occur::Must, Box::new(RangeQuery::new(lower, upper))));
            }
            if req.date_after.is_some() || req.date_before.is_some() {
                let lower = req
                    .date_after
                    .map(|v| Bound::Included(Term::from_field_i64(f.event_date, v)))
                    .unwrap_or(Bound::Unbounded);
                let upper = req
                    .date_before
                    .map(|v| Bound::Included(Term::from_field_i64(f.event_date, v)))
                    .unwrap_or(Bound::Unbounded);
                clauses.push((Occur::Must, Box::new(RangeQuery::new(lower, upper))));
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
            let sample_size = if want_facets {
                page.max(FACET_SAMPLE)
            } else {
                page
            };
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
                    doc.get_first(field)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
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
                list.sort_by_key(|f| std::cmp::Reverse(f.count));
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

#[cfg(test)]
mod tests {
    use super::{
        build_schema, classify_open_failure, drain_dir, quarantine_path, schema_is_current,
        OpenFailure,
    };
    use std::path::PathBuf;
    use tantivy::schema::{Schema, FAST, INDEXED, STORED, STRING, TEXT};
    use tantivy::Index;

    fn scratch(tag: &str) -> PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "pumper-search-unit-{tag}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The anti-pattern: naming the quarantine after the wall clock, which makes
    /// the path untestable and collides when two quarantines land in one second.
    #[test]
    fn quarantine_path_counts_up_instead_of_timestamping() {
        let dir = scratch("quarantine");
        let first = quarantine_path(&dir);
        assert_eq!(
            first.file_name().unwrap().to_str().unwrap(),
            format!("{}.corrupt.0", dir.file_name().unwrap().to_str().unwrap()),
            "the first quarantine is a deterministic sibling of the index dir"
        );
        assert_eq!(first.parent(), dir.parent(), "quarantine is a SIBLING");
        std::fs::create_dir_all(&first).unwrap();
        let second = quarantine_path(&dir);
        assert!(
            second.to_str().unwrap().ends_with(".corrupt.1"),
            "an occupied suffix is skipped, never overwritten: {second:?}"
        );
        let _ = std::fs::remove_dir_all(&first);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The anti-pattern: treating "couldn't open" as "corrupt" and quarantining a
    /// directory that was simply never initialized (leaving a `.corrupt.N`
    /// carcass on every first boot).
    #[test]
    fn empty_dir_is_uninitialized_not_corrupt() {
        let dir = scratch("classify");
        assert_eq!(classify_open_failure(&dir), OpenFailure::Uninitialized);
        // A leftover lock file from a crash is still not a corrupt index.
        std::fs::File::create(dir.join(".tantivy-writer.lock")).unwrap();
        assert_eq!(classify_open_failure(&dir), OpenFailure::Uninitialized);
        // Only a present meta.json makes `create_in_dir` refuse, which is what
        // quarantining exists to unblock.
        std::fs::write(dir.join("meta.json"), b"{ not json").unwrap();
        assert_eq!(classify_open_failure(&dir), OpenFailure::Corrupt);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The anti-pattern the strengthened gate closes: `schema_is_current` tested
    /// field-name PRESENCE only, so a field whose TYPE changed — or an index
    /// carrying EXTRA fields (a downgrade) — passed the drift gate and surfaced
    /// later as a runtime query error instead of triggering a rebuild. The gate
    /// now compares the whole schema structurally.
    #[test]
    fn schema_drift_is_caught_by_type_not_only_by_field_name() {
        // The canonical schema an index this build created always matches.
        let dir = scratch("schema-match");
        let index = Index::create_in_dir(&dir, build_schema()).unwrap();
        assert!(
            schema_is_current(&index),
            "an index built from build_schema() is current"
        );
        drop(index);
        let _ = std::fs::remove_dir_all(&dir);

        // Same field NAMES and `body` stored (so the old presence check passed),
        // but `amount` is retyped u64 -> text. Structural equality rejects it.
        let mut b = Schema::builder();
        b.add_text_field("id", STRING | STORED);
        b.add_text_field("app", STRING | STORED);
        b.add_text_field("dataset", STRING | STORED);
        b.add_text_field("url", STRING | STORED);
        b.add_text_field("title", TEXT | STORED);
        b.add_text_field("body", TEXT | STORED);
        b.add_i64_field("indexed_at", INDEXED | STORED | FAST);
        b.add_text_field("amount", TEXT | STORED); // <- drifted type
        b.add_i64_field("event_date", INDEXED | STORED | FAST);
        let dir = scratch("schema-retyped");
        let drifted = Index::create_in_dir(&dir, b.build()).unwrap();
        assert!(
            !schema_is_current(&drifted),
            "a field whose TYPE changed must read as drift, not as current"
        );
        drop(drifted);
        let _ = std::fs::remove_dir_all(&dir);

        // Downgrade: every canonical field present AND stored, plus an EXTRA
        // field a newer build added. The old check passed; equality rejects it.
        let mut b = Schema::builder();
        b.add_text_field("id", STRING | STORED);
        b.add_text_field("app", STRING | STORED);
        b.add_text_field("dataset", STRING | STORED);
        b.add_text_field("url", STRING | STORED);
        b.add_text_field("title", TEXT | STORED);
        b.add_text_field("body", TEXT | STORED);
        b.add_i64_field("indexed_at", INDEXED | STORED | FAST);
        b.add_u64_field("amount", INDEXED | STORED | FAST);
        b.add_i64_field("event_date", INDEXED | STORED | FAST);
        b.add_text_field("summary", TEXT | STORED); // <- extra field (downgrade)
        let dir = scratch("schema-extra");
        let newer = Index::create_in_dir(&dir, b.build()).unwrap();
        assert!(
            !schema_is_current(&newer),
            "an index with EXTRA fields (a downgrade) must read as drift"
        );
        drop(newer);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The anti-pattern: `remove_dir_all`/`rename` on the index directory
    /// itself, which takes the writer-lock file with it — releasing the claim
    /// halfway through the rebuild (and failing outright on Windows, where the
    /// lock's handle is open inside that directory).
    #[test]
    fn drain_keeps_the_lock_file_not_only_the_directory() {
        let dir = scratch("drain");
        std::fs::write(dir.join(".tantivy-writer.lock"), b"").unwrap();
        std::fs::write(dir.join("meta.json"), b"{}").unwrap();
        std::fs::create_dir_all(dir.join("seg")).unwrap();
        std::fs::write(dir.join("seg/a.idx"), b"x").unwrap();

        // Quarantine: everything moves aside except the lock.
        let aside = quarantine_path(&dir);
        drain_dir(&dir, Some(&aside)).unwrap();
        assert!(dir.join(".tantivy-writer.lock").exists(), "claim survives");
        assert!(!dir.join("meta.json").exists(), "the bad manifest is gone");
        assert!(aside.join("meta.json").exists(), "…but preserved aside");
        assert!(aside.join("seg/a.idx").exists(), "subdirectories move too");

        // Wipe: everything is deleted except the lock.
        std::fs::write(dir.join("meta.json"), b"{}").unwrap();
        drain_dir(&dir, None).unwrap();
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "only the lock file is left"
        );
        assert!(dir.join(".tantivy-writer.lock").exists());
        let _ = std::fs::remove_dir_all(&aside);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
