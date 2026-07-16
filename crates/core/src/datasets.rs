//! Persistent, queryable dataset store with change detection. Apps upsert typed
//! records keyed by a stable id; the store hashes each value and reports whether
//! it is new, changed, or unchanged versus the last run. This is the substrate
//! for both dedup (skip records already seen) and monitoring (act only on
//! diffs), turning one-off scrapes into datasets that accrue over time.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

use crate::{Error, Result};

/// Upper bound on the pairs returned by `duplicate_pairs`, so a pathological
/// dataset can't produce an unbounded result list.
const MAX_DUP_PAIRS: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    New,
    Changed,
    Unchanged,
}

impl ChangeKind {
    /// True when the record is new or its content changed — i.e. worth acting on.
    pub fn is_fresh(self) -> bool {
        matches!(self, ChangeKind::New | ChangeKind::Changed)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Record {
    pub key: String,
    pub data: Value,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Set when a full-snapshot sync no longer contained this key.
    pub removed_at: Option<DateTime<Utc>>,
}

/// One entry in a record's revision history: what changed, when, and the
/// field-level diff versus the previous revision.
#[derive(Debug, Clone, Serialize)]
pub struct Revision {
    pub app: String,
    pub dataset: String,
    pub key: String,
    pub revision: i64,
    /// 'new' | 'changed' | 'removed'
    pub change: String,
    /// Full record snapshot at this revision (None for 'removed').
    pub data: Option<Value>,
    /// Field-level diff vs the previous revision: `{ "path": {"from": .., "to": ..} }`.
    pub diff: Option<Value>,
    pub created_at: DateTime<Utc>,
}

/// A keyset page of revisions plus the cursor to fetch the next page (None at
/// the end). The tiebreak differs by feed — rowid for the cross-key change feed,
/// per-key `revision` for a single record's history — so the cursor is built
/// inside the store rather than reconstructed from a `Revision` field.
#[derive(Debug, Clone, Serialize)]
pub struct RevisionPage {
    pub items: Vec<Revision>,
    pub next_cursor: Option<String>,
}

/// A predicate over the JSON `data` column, letting callers build filtered views
/// of a dataset without denormalizing fields into real columns. Paths are SQLite
/// JSON paths (`$.status`) and are *bound as parameters*, never interpolated, so
/// a caller cannot inject SQL through one.
///
/// Every variant is NULL-rejecting: a record whose field is absent or null never
/// matches. That is the semantics a filter wants — "closing before X" should not
/// surface records with no close date.
#[derive(Debug, Clone)]
pub enum JsonFilter {
    /// `data->path` equals `value` exactly (case-sensitive).
    Eq { path: String, value: String },
    /// `data->path` contains `value` as a case-insensitive substring. Plain
    /// substring semantics — `%` and `_` are literal, not wildcards.
    Contains { path: String, value: String },
    /// `data->path >= value` compared as text (lexicographic).
    Gte { path: String, value: String },
    /// `data->path <= value` compared as text (lexicographic).
    Lte { path: String, value: String },
    /// Numeric `>= value` on *any* of `paths` (OR). The `json_type` guard keeps a
    /// field that happens to hold a string out of the comparison, because SQLite
    /// sorts every TEXT value above every number and would otherwise match it.
    NumGteAny { paths: Vec<String>, value: f64 },
}

/// A near-duplicate record pair and their SimHash Hamming distance.
#[derive(Debug, Clone, Serialize)]
pub struct DupPair {
    pub a: String,
    pub b: String,
    pub distance: u32,
}

/// Outcome of upserting a batch: the fresh records, plus a count of unchanged.
/// `removed` is only populated by full-snapshot syncs (see
/// `AppContext::sync_many` / `Datasets::detect_removed`).
#[derive(Debug, Default, Serialize)]
pub struct UpsertSummary {
    pub new: Vec<String>,
    pub changed: Vec<String>,
    pub unchanged: usize,
    pub removed: Vec<String>,
}

impl UpsertSummary {
    /// Keys that are new or changed, in upsert order.
    pub fn fresh_keys(&self) -> impl Iterator<Item = &String> {
        self.new.iter().chain(self.changed.iter())
    }
}

pub struct Datasets {
    pool: SqlitePool,
}

/// Records committed per write transaction in the batch write paths
/// (`upsert_many`, `detect_removed`). Trades throughput (fewer commits/fsyncs and
/// write-lock acquisitions) against how long one batch holds the write lock
/// against other apps' workers. 500 records of non-commit work is a few tens of
/// ms — well inside the 5s `busy_timeout`.
const UPSERT_CHUNK: usize = 500;

impl Datasets {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Upserts one record; returns whether it was new, changed, or unchanged.
    /// New and Changed upserts also append a revision (with a field-level diff
    /// for changes). A previously-removed record that reappears is revived and
    /// reported as Changed even if its content matches the old snapshot.
    pub async fn upsert(
        &self,
        app: &str,
        dataset: &str,
        key: &str,
        value: &Value,
    ) -> Result<ChangeKind> {
        let hash = hash_value(value);
        let sim = crate::simhash::simhash_value(value) as i64;
        let now = Utc::now();

        // The read → write → add_revision sequence must be atomic: as three
        // separate autocommit statements, concurrent same-key writers (per-app
        // worker concurrency can exceed 1) either collided on the PK and aborted
        // the batch, or diffed against a stale base and corrupted the revision
        // chain the change-feed relies on. BEGIN IMMEDIATE takes the write lock up
        // front so writers serialize (busy_timeout makes the second wait); a plain
        // DEFERRED begin would instead fail the read-then-write upgrade with
        // SQLITE_BUSY_SNAPSHOT under WAL.
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let result =
            Self::upsert_in_tx(&mut conn, app, dataset, key, value, hash.as_str(), sim, now).await;
        match result {
            Ok(kind) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
                Ok(kind)
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    /// Transactional body of `upsert`: the SELECT + record write + revision append
    /// run on one connection already inside a write transaction, so they commit
    /// (or roll back) as a unit.
    async fn upsert_in_tx(
        conn: &mut sqlx::SqliteConnection,
        app: &str,
        dataset: &str,
        key: &str,
        value: &Value,
        hash: &str,
        sim: i64,
        now: DateTime<Utc>,
    ) -> Result<ChangeKind> {
        let existing: Option<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT hash, data, removed_at FROM records WHERE app = ?1 AND dataset = ?2 AND key = ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .fetch_optional(&mut *conn)
        .await?;

        match existing {
            Some((prev, _, removed_at)) if prev.as_str() == hash && removed_at.is_none() => {
                sqlx::query(
                    "UPDATE records SET last_seen = ?4 WHERE app = ?1 AND dataset = ?2 AND key = ?3",
                )
                .bind(app)
                .bind(dataset)
                .bind(key)
                .bind(ts(now))
                .execute(&mut *conn)
                .await?;
                Ok(ChangeKind::Unchanged)
            }
            Some((_, old_data, _)) => {
                sqlx::query(
                    "UPDATE records SET hash = ?4, data = ?5, simhash = ?6, last_seen = ?7, \
                     updated_at = ?7, removed_at = NULL WHERE app = ?1 AND dataset = ?2 AND key = ?3",
                )
                .bind(app)
                .bind(dataset)
                .bind(key)
                .bind(hash)
                .bind(value.to_string())
                .bind(sim)
                .bind(ts(now))
                .execute(&mut *conn)
                .await?;
                let old: Value = serde_json::from_str(&old_data).unwrap_or(Value::Null);
                let diff = diff_values(&old, value);
                Self::add_revision(
                    &mut *conn,
                    app,
                    dataset,
                    key,
                    "changed",
                    Some(value),
                    Some(&diff),
                    now,
                )
                .await?;
                Ok(ChangeKind::Changed)
            }
            None => {
                sqlx::query(
                    "INSERT INTO records (app, dataset, key, hash, data, simhash, first_seen, last_seen, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?7)",
                )
                .bind(app)
                .bind(dataset)
                .bind(key)
                .bind(hash)
                .bind(value.to_string())
                .bind(sim)
                .bind(ts(now))
                .execute(&mut *conn)
                .await?;
                Self::add_revision(&mut *conn, app, dataset, key, "new", Some(value), None, now)
                    .await?;
                Ok(ChangeKind::New)
            }
        }
    }

    /// Appends the next revision for a record (revision numbers are per-key,
    /// starting at 1). Runs on the caller-supplied executor so it can share the
    /// caller's transaction — the per-key `MAX(revision)` subquery must see the
    /// same in-flight state as the record write it accompanies.
    async fn add_revision<'e, E>(
        executor: E,
        app: &str,
        dataset: &str,
        key: &str,
        change: &str,
        data: Option<&Value>,
        diff: Option<&Value>,
        when: DateTime<Utc>,
    ) -> Result<()>
    where
        E: sqlx::SqliteExecutor<'e>,
    {
        sqlx::query(
            "INSERT INTO record_revisions (app, dataset, key, revision, change, data, diff, created_at) \
             VALUES (?1, ?2, ?3, \
                     (SELECT COALESCE(MAX(revision), 0) + 1 FROM record_revisions \
                      WHERE app = ?1 AND dataset = ?2 AND key = ?3), \
                     ?4, ?5, ?6, ?7)",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .bind(change)
        .bind(data.map(Value::to_string))
        .bind(diff.map(Value::to_string))
        .bind(ts(when))
        .execute(executor)
        .await?;
        Ok(())
    }

    /// A record's revision history, newest first.
    pub async fn history(
        &self,
        app: &str,
        dataset: &str,
        key: &str,
        limit: i64,
    ) -> Result<Vec<Revision>> {
        let rows: Vec<RevisionRow> = sqlx::query_as(
            "SELECT app, dataset, key, revision, change, data, diff, created_at \
             FROM record_revisions WHERE app = ?1 AND dataset = ?2 AND key = ?3 \
             ORDER BY revision DESC LIMIT ?4",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Revision::try_from).collect()
    }

    /// Change feed: revisions across a dataset (or all of an app's datasets when
    /// `dataset` is None), newest first, optionally only those after `since`.
    pub async fn changes_since(
        &self,
        app: &str,
        dataset: Option<&str>,
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<Revision>> {
        let rows: Vec<RevisionRow> = sqlx::query_as(
            "SELECT app, dataset, key, revision, change, data, diff, created_at \
             FROM record_revisions \
             WHERE app = ?1 AND (?2 IS NULL OR dataset = ?2) AND (?3 IS NULL OR created_at > ?3) \
             ORDER BY created_at DESC LIMIT ?4",
        )
        .bind(app)
        .bind(dataset)
        .bind(since.map(ts))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Revision::try_from).collect()
    }

    /// Keyset page of a single record's revision history, newest first. `after`
    /// is the previous page's last (created_at-as-stored, revision); None starts
    /// at the newest. Revisions are per-key monotonic, so `revision` is a unique,
    /// stable tiebreak within the (app, dataset, key).
    pub async fn history_page(
        &self,
        app: &str,
        dataset: &str,
        key: &str,
        after: Option<(String, i64)>,
        limit: i64,
    ) -> Result<RevisionPage> {
        let (after_ts, after_rev) = after.map(|(t, r)| (Some(t), Some(r))).unwrap_or((None, None));
        let rows: Vec<RevisionRow> = sqlx::query_as(
            "SELECT app, dataset, key, revision, change, data, diff, created_at \
             FROM record_revisions WHERE app = ?1 AND dataset = ?2 AND key = ?3 \
             AND (?4 IS NULL OR created_at < ?4 OR (created_at = ?4 AND revision < ?5)) \
             ORDER BY revision DESC LIMIT ?6",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .bind(after_ts)
        .bind(after_rev)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let items: Vec<Revision> = rows.into_iter().map(Revision::try_from).collect::<Result<_>>()?;
        let next_cursor = ((items.len() as i64) == limit)
            .then(|| items.last())
            .flatten()
            .map(|r| format!("{}|{}", ts(r.created_at), r.revision));
        Ok(RevisionPage { items, next_cursor })
    }

    /// Keyset page of the change feed (revisions across a dataset, or all of an
    /// app's datasets when `dataset` is None), newest first, optionally only
    /// those after `since`. `after` is the previous page's last (created_at, rowid);
    /// rowid is the stable tiebreak because a batch can share a microsecond stamp.
    pub async fn changes_page(
        &self,
        app: &str,
        dataset: Option<&str>,
        since: Option<DateTime<Utc>>,
        after: Option<(String, i64)>,
        limit: i64,
    ) -> Result<RevisionPage> {
        let (after_ts, after_rowid) =
            after.map(|(t, r)| (Some(t), Some(r))).unwrap_or((None, None));
        let rows: Vec<RevisionFeedRow> = sqlx::query_as(
            "SELECT rowid AS rowid, app, dataset, key, revision, change, data, diff, created_at \
             FROM record_revisions \
             WHERE app = ?1 AND (?2 IS NULL OR dataset = ?2) AND (?3 IS NULL OR created_at > ?3) \
             AND (?4 IS NULL OR created_at < ?4 OR (created_at = ?4 AND rowid < ?5)) \
             ORDER BY created_at DESC, rowid DESC LIMIT ?6",
        )
        .bind(app)
        .bind(dataset)
        .bind(since.map(ts))
        .bind(after_ts)
        .bind(after_rowid)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let next_cursor = ((rows.len() as i64) == limit)
            .then(|| rows.last())
            .flatten()
            .map(|r| format!("{}|{}", r.inner.created_at, r.rowid));
        let items: Vec<Revision> = rows
            .into_iter()
            .map(|r| Revision::try_from(r.inner))
            .collect::<Result<_>>()?;
        Ok(RevisionPage { items, next_cursor })
    }

    /// Full-snapshot removal detection: marks live records whose key is absent
    /// from `present` as removed (sets `removed_at` and appends a 'removed'
    /// revision). Returns the removed keys. Call after upserting a batch that
    /// represents the complete current state of the dataset.
    pub async fn detect_removed(
        &self,
        app: &str,
        dataset: &str,
        present: &[String],
    ) -> Result<Vec<String>> {
        // An empty snapshot almost always means the scrape failed, not that the
        // entire dataset genuinely disappeared. Refuse to tombstone everything —
        // callers that legitimately empty a dataset should delete explicitly.
        if present.is_empty() {
            return Ok(Vec::new());
        }
        let live: Vec<String> = sqlx::query_scalar(
            "SELECT key FROM records WHERE app = ?1 AND dataset = ?2 AND removed_at IS NULL",
        )
        .bind(app)
        .bind(dataset)
        .fetch_all(&self.pool)
        .await?;
        let present: std::collections::HashSet<&str> =
            present.iter().map(String::as_str).collect();
        let to_remove: Vec<String> =
            live.into_iter().filter(|k| !present.contains(k.as_str())).collect();
        if to_remove.is_empty() {
            return Ok(Vec::new());
        }
        let now = Utc::now();

        // Two fixes over the old per-key pair of autocommit writes:
        //   (1) Atomicity — the `UPDATE removed_at` and its `removed` revision now
        //       run in ONE transaction, so a crash between them can't tombstone a
        //       record with no revision. That was a permanent signal loss: the
        //       next sync sees `removed_at` already set and the key still absent,
        //       so it never revisits the key and the change feed / watches / dataset
        //       triggers never fire for that removal. `upsert` was hardened for
        //       exactly this reason; `detect_removed` writes the same two rows and
        //       had been missed.
        //   (2) Cost — chunked commits instead of 2 write transactions per key
        //       (a 2k-key removal was 4k commits).
        let mut conn = self.pool.acquire().await?;
        for chunk in to_remove.chunks(UPSERT_CHUNK) {
            sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
            let mut chunk_result: Result<()> = Ok(());
            for key in chunk {
                if let Err(e) = Self::remove_in_tx(&mut conn, app, dataset, key, now).await {
                    chunk_result = Err(e);
                    break;
                }
            }
            match chunk_result {
                Ok(()) => {
                    sqlx::query("COMMIT").execute(&mut *conn).await?;
                }
                Err(e) => {
                    let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                    return Err(e);
                }
            }
        }
        Ok(to_remove)
    }

    /// Transactional body of one removal: tombstone the record and append its
    /// `removed` revision on one connection inside a write transaction, so the two
    /// commit as a unit (mirrors `upsert_in_tx`).
    async fn remove_in_tx(
        conn: &mut sqlx::SqliteConnection,
        app: &str,
        dataset: &str,
        key: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE records SET removed_at = ?4 WHERE app = ?1 AND dataset = ?2 AND key = ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .bind(ts(now))
        .execute(&mut *conn)
        .await?;
        Self::add_revision(&mut *conn, app, dataset, key, "removed", None, None, now).await?;
        Ok(())
    }

    /// Upserts many records, returning a summary of new/changed/unchanged.
    ///
    /// This is the most-executed write path in the product (every ingest run
    /// upserts its whole listing). Rather than one `BEGIN IMMEDIATE` transaction
    /// per record — a WAL commit/fsync and a database-wide write-lock acquisition
    /// each, so a 5k-record batch was 5k commits — records are committed in chunks
    /// of `UPSERT_CHUNK` on a single held connection: ~10 commits for that batch,
    /// and the write lock is taken ~10 times instead of 5k (the mechanism behind
    /// cross-app write stalls during a large sync). Each record keeps its exact
    /// per-record read→write→revision semantics via `upsert_in_tx`.
    ///
    /// A failure rolls back its own chunk and propagates; chunks committed before
    /// it stay committed (the same partial-progress-then-error shape the old
    /// per-record loop had). The chunk size bounds how long the write lock is held
    /// against other apps' workers — 500 records of non-commit work stays well
    /// inside the 5s `busy_timeout`.
    pub async fn upsert_many(
        &self,
        app: &str,
        dataset: &str,
        items: &[(String, Value)],
    ) -> Result<UpsertSummary> {
        let mut summary = UpsertSummary::default();
        if items.is_empty() {
            return Ok(summary);
        }
        let mut conn = self.pool.acquire().await?;
        for chunk in items.chunks(UPSERT_CHUNK) {
            sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
            // Accumulate this chunk separately so a mid-chunk failure that rolls
            // back doesn't leave the returned summary claiming uncommitted rows.
            let mut chunk_summary = UpsertSummary::default();
            let mut chunk_result: Result<()> = Ok(());
            for (key, value) in chunk {
                let hash = hash_value(value);
                let sim = crate::simhash::simhash_value(value) as i64;
                let now = Utc::now();
                match Self::upsert_in_tx(&mut conn, app, dataset, key, value, hash.as_str(), sim, now)
                    .await
                {
                    Ok(ChangeKind::New) => chunk_summary.new.push(key.clone()),
                    Ok(ChangeKind::Changed) => chunk_summary.changed.push(key.clone()),
                    Ok(ChangeKind::Unchanged) => chunk_summary.unchanged += 1,
                    Err(e) => {
                        chunk_result = Err(e);
                        break;
                    }
                }
            }
            match chunk_result {
                Ok(()) => {
                    sqlx::query("COMMIT").execute(&mut *conn).await?;
                    summary.new.extend(chunk_summary.new);
                    summary.changed.extend(chunk_summary.changed);
                    summary.unchanged += chunk_summary.unchanged;
                }
                Err(e) => {
                    let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                    return Err(e);
                }
            }
        }
        Ok(summary)
    }

    /// Finds near-duplicate record pairs within a dataset using SimHash Hamming
    /// distance (semantic dedup — catches near-identical content, not just exact
    /// matches). O(n²) scan, fine for local datasets. Records with no textual
    /// content (simhash 0) are skipped.
    pub async fn duplicate_pairs(
        &self,
        app: &str,
        dataset: &str,
        max_distance: u32,
    ) -> Result<Vec<DupPair>> {
        // Only compare live records — tombstoned rows are gone and reporting them
        // as duplicates is noise.
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT key, simhash FROM records \
             WHERE app = ?1 AND dataset = ?2 AND removed_at IS NULL",
        )
        .bind(app)
        .bind(dataset)
        .fetch_all(&self.pool)
        .await?;
        let mut pairs = Vec::new();
        'scan: for i in 0..rows.len() {
            if rows[i].1 == 0 {
                continue;
            }
            for j in (i + 1)..rows.len() {
                if rows[j].1 == 0 {
                    continue;
                }
                let distance = crate::simhash::hamming(rows[i].1 as u64, rows[j].1 as u64);
                if distance <= max_distance {
                    pairs.push(DupPair {
                        a: rows[i].0.clone(),
                        b: rows[j].0.clone(),
                        distance,
                    });
                    // Bound the result: a pathological dataset must not return an
                    // unbounded pair list.
                    if pairs.len() >= MAX_DUP_PAIRS {
                        break 'scan;
                    }
                }
            }
        }
        pairs.sort_by_key(|p| p.distance);
        Ok(pairs)
    }

    /// Recomputes every record's SimHash from its stored JSON, rewriting only the
    /// rows whose fingerprint actually changed. Returns that count.
    ///
    /// This is the one-shot to run after the SimHash token hash changes: old and
    /// new fingerprints are not comparable, so a table holding a mix of both
    /// yields meaningless Hamming distances and silently wrong near-dup results.
    /// Only the derived `simhash` column is touched — `data`, `hash` and the
    /// timestamps are left alone so the change-feed sees no spurious revisions.
    /// Run with the worker stopped; the whole rewrite is one transaction.
    pub async fn reindex_simhashes(&self) -> Result<usize> {
        let rows: Vec<(String, String, String, String, i64)> =
            sqlx::query_as("SELECT app, dataset, key, data, simhash FROM records")
                .fetch_all(&self.pool)
                .await?;

        let mut tx = self.pool.begin().await?;
        let mut changed = 0usize;
        for (app, dataset, key, data, old_sim) in rows {
            let value: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
            let sim = crate::simhash::simhash_value(&value) as i64;
            if sim == old_sim {
                continue;
            }
            sqlx::query(
                "UPDATE records SET simhash = ?4 WHERE app = ?1 AND dataset = ?2 AND key = ?3",
            )
            .bind(&app)
            .bind(&dataset)
            .bind(&key)
            .bind(sim)
            .execute(&mut *tx)
            .await?;
            changed += 1;
        }
        tx.commit().await?;
        Ok(changed)
    }

    /// Number of records in a dataset (removed rows included) — the bound the
    /// duplicate scan checks before its O(n²) pairwise SimHash comparison.
    pub async fn record_count(&self, app: &str, dataset: &str) -> Result<i64> {
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM records WHERE app = ?1 AND dataset = ?2")
                .bind(app)
                .bind(dataset)
                .fetch_one(&self.pool)
                .await?;
        Ok(n)
    }

    /// Dedup helper: true if this key has been recorded before.
    pub async fn seen(&self, app: &str, dataset: &str, key: &str) -> Result<bool> {
        let found: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM records WHERE app = ?1 AND dataset = ?2 AND key = ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(found.is_some())
    }

    pub async fn get(&self, app: &str, dataset: &str, key: &str) -> Result<Option<Record>> {
        let row: Option<RecordRow> = sqlx::query_as(
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at \
             FROM records WHERE app = ?1 AND dataset = ?2 AND key = ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Record::try_from).transpose()
    }

    /// Lists records in a dataset, most-recently-updated first. Removed records
    /// are included (with `removed_at` set) so exports stay complete; filter on
    /// `removed_at` for the live view.
    pub async fn list(&self, app: &str, dataset: &str, limit: i64) -> Result<Vec<Record>> {
        let rows: Vec<RecordRow> = sqlx::query_as(
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at \
             FROM records WHERE app = ?1 AND dataset = ?2 ORDER BY updated_at DESC LIMIT ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Record::try_from).collect()
    }

    /// Keyset page of records ordered (updated_at DESC, key DESC). `after` is
    /// the previous page's last (updated_at-as-stored, key); None starts from
    /// the top. Stable under concurrent writes, unlike OFFSET.
    pub async fn list_page(
        &self,
        app: &str,
        dataset: &str,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<Record>> {
        let (after_ts, after_key) = after.map(|(t, k)| (Some(t), Some(k))).unwrap_or((None, None));
        let rows: Vec<RecordRow> = sqlx::query_as(
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at \
             FROM records WHERE app = ?1 AND dataset = ?2 \
             AND (?3 IS NULL OR updated_at < ?3 OR (updated_at = ?3 AND key < ?4)) \
             ORDER BY updated_at DESC, key DESC LIMIT ?5",
        )
        .bind(app)
        .bind(dataset)
        .bind(after_ts)
        .bind(after_key)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Record::try_from).collect()
    }

    /// Keyset page of *live* records (`removed_at IS NULL`) matching every filter,
    /// ordered like `list_page` — (updated_at DESC, key DESC) — so the same
    /// `<stored-ts>|<key>` cursor pages it. Filters are ANDed.
    ///
    /// Predicates run through `json_extract` on the `data` column, so this is a
    /// full scan of the `(app, dataset)` partition with no index on the filtered
    /// fields. That is the right trade while datasets are in the thousands: zero
    /// schema coupling to any app's record shape, and filters can be added without
    /// a migration. If a dataset grows to where the scan hurts, the escape hatch is
    /// a generated column over the hot path plus an index on it — the query here
    /// would not have to change.
    pub async fn list_filtered(
        &self,
        app: &str,
        dataset: &str,
        filters: &[JsonFilter],
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<Record>> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at \
             FROM records WHERE removed_at IS NULL AND app = ",
        );
        qb.push_bind(app);
        qb.push(" AND dataset = ");
        qb.push_bind(dataset);

        push_json_filters(&mut qb, filters);

        if let Some((after_ts, after_key)) = &after {
            qb.push(" AND (updated_at < ");
            qb.push_bind(after_ts.as_str());
            qb.push(" OR (updated_at = ");
            qb.push_bind(after_ts.as_str());
            qb.push(" AND key < ");
            qb.push_bind(after_key.as_str());
            qb.push("))");
        }

        qb.push(" ORDER BY updated_at DESC, key DESC LIMIT ");
        qb.push_bind(limit);

        let rows: Vec<RecordRow> = qb.build_query_as().fetch_all(&self.pool).await?;
        rows.into_iter().map(Record::try_from).collect()
    }

    /// Live records matching `filters`, ordered ascending by a JSON path (then
    /// key for determinism) with the LIMIT applied to the *sorted* rows in SQL.
    ///
    /// This is the correctness-critical difference from [`list_filtered`], which
    /// orders by `updated_at DESC`: a caller that wants the N soonest-closing or
    /// N smallest-award rows must sort in SQL *before* the LIMIT, or the LIMIT
    /// picks an arbitrary window (by update recency) and the subsequent in-memory
    /// sort only reorders that wrong subset. No cursor — this is a top-N view.
    pub async fn list_filtered_ordered(
        &self,
        app: &str,
        dataset: &str,
        filters: &[JsonFilter],
        order_by_path: &str,
        limit: i64,
    ) -> Result<Vec<Record>> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at \
             FROM records WHERE removed_at IS NULL AND app = ",
        );
        qb.push_bind(app);
        qb.push(" AND dataset = ");
        qb.push_bind(dataset);
        push_json_filters(&mut qb, filters);
        qb.push(" ORDER BY json_extract(data, ");
        qb.push_bind(order_by_path);
        qb.push(") ASC, key ASC LIMIT ");
        qb.push_bind(limit);

        let rows: Vec<RecordRow> = qb.build_query_as().fetch_all(&self.pool).await?;
        rows.into_iter().map(Record::try_from).collect()
    }

    /// Count of live records matching `filters` — the true total behind a capped
    /// list, so a view can report the real window size instead of saturating at
    /// its scan/return cap.
    pub async fn count_filtered(
        &self,
        app: &str,
        dataset: &str,
        filters: &[JsonFilter],
    ) -> Result<i64> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT COUNT(*) FROM records WHERE removed_at IS NULL AND app = ",
        );
        qb.push_bind(app);
        qb.push(" AND dataset = ");
        qb.push_bind(dataset);
        push_json_filters(&mut qb, filters);

        let count: i64 = qb.build_query_scalar().fetch_one(&self.pool).await?;
        Ok(count)
    }

    /// Distinct `(app, dataset)` pairs that have at least one live record — the
    /// set the search-backfill walks to rebuild the index from stored records.
    pub async fn list_all_datasets(&self) -> Result<Vec<(String, String)>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT app, dataset FROM records WHERE removed_at IS NULL \
             ORDER BY app, dataset",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Distinct dataset names for an app.
    pub async fn datasets(&self, app: &str) -> Result<Vec<String>> {
        let names: Vec<String> =
            sqlx::query_scalar("SELECT DISTINCT dataset FROM records WHERE app = ?1 ORDER BY dataset")
                .bind(app)
                .fetch_all(&self.pool)
                .await?;
        Ok(names)
    }
}

/// Appends the ` AND …` predicate clauses for a set of [`JsonFilter`]s onto a
/// query builder. Shared by `list_filtered`, `list_filtered_ordered`, and
/// `count_filtered` so the three can never interpret a filter differently.
fn push_json_filters<'a>(qb: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>, filters: &'a [JsonFilter]) {
    for filter in filters {
        match filter {
            JsonFilter::Eq { path, value } => {
                qb.push(" AND json_extract(data, ");
                qb.push_bind(path.as_str());
                qb.push(") = ");
                qb.push_bind(value.as_str());
            }
            JsonFilter::Contains { path, value } => {
                qb.push(" AND instr(lower(COALESCE(json_extract(data, ");
                qb.push_bind(path.as_str());
                qb.push("), '')), lower(");
                qb.push_bind(value.as_str());
                qb.push(")) > 0");
            }
            // Compare numerically when the JSON field is a number, else as text.
            // SQLite sorts all numbers below all text, so a plain `>=` of a numeric
            // field against a text-bound value always fails; the text branch
            // preserves the existing ISO-date behavior unchanged.
            JsonFilter::Gte { path, value } => {
                qb.push(" AND (CASE WHEN json_type(data, ");
                qb.push_bind(path.as_str());
                qb.push(") IN ('integer','real') THEN json_extract(data, ");
                qb.push_bind(path.as_str());
                qb.push(") >= CAST(");
                qb.push_bind(value.as_str());
                qb.push(" AS REAL) ELSE json_extract(data, ");
                qb.push_bind(path.as_str());
                qb.push(") >= ");
                qb.push_bind(value.as_str());
                qb.push(" END)");
            }
            JsonFilter::Lte { path, value } => {
                qb.push(" AND (CASE WHEN json_type(data, ");
                qb.push_bind(path.as_str());
                qb.push(") IN ('integer','real') THEN json_extract(data, ");
                qb.push_bind(path.as_str());
                qb.push(") <= CAST(");
                qb.push_bind(value.as_str());
                qb.push(" AS REAL) ELSE json_extract(data, ");
                qb.push_bind(path.as_str());
                qb.push(") <= ");
                qb.push_bind(value.as_str());
                qb.push(" END)");
            }
            // `(0 OR ...)` is the honest reading of "matches any of these paths":
            // with no paths, nothing matches. NULL never satisfies the comparison,
            // so records missing all the money fields drop out.
            JsonFilter::NumGteAny { paths, value } => {
                qb.push(" AND (0");
                for path in paths {
                    qb.push(" OR (json_type(data, ");
                    qb.push_bind(path.as_str());
                    qb.push(") IN ('integer', 'real') AND json_extract(data, ");
                    qb.push_bind(path.as_str());
                    qb.push(") >= ");
                    qb.push_bind(*value);
                    qb.push(")");
                }
                qb.push(")");
            }
        }
    }
}

#[derive(sqlx::FromRow)]
struct RecordRow {
    key: String,
    data: String,
    first_seen: String,
    last_seen: String,
    updated_at: String,
    removed_at: Option<String>,
}

impl TryFrom<RecordRow> for Record {
    type Error = Error;

    fn try_from(r: RecordRow) -> Result<Record> {
        Ok(Record {
            key: r.key,
            data: serde_json::from_str(&r.data).unwrap_or(Value::Null),
            first_seen: parse_ts(&r.first_seen)?,
            last_seen: parse_ts(&r.last_seen)?,
            updated_at: parse_ts(&r.updated_at)?,
            removed_at: r.removed_at.as_deref().map(parse_ts).transpose()?,
        })
    }
}

#[derive(sqlx::FromRow)]
struct RevisionRow {
    app: String,
    dataset: String,
    key: String,
    revision: i64,
    change: String,
    data: Option<String>,
    diff: Option<String>,
    created_at: String,
}

/// The change feed needs a stable per-row tiebreak; `record_revisions` has no
/// single-column surrogate key, so we page on the implicit `rowid` (monotonic
/// with insert order) carried alongside the flattened revision columns.
#[derive(sqlx::FromRow)]
struct RevisionFeedRow {
    rowid: i64,
    #[sqlx(flatten)]
    inner: RevisionRow,
}

impl TryFrom<RevisionRow> for Revision {
    type Error = Error;

    fn try_from(r: RevisionRow) -> Result<Revision> {
        Ok(Revision {
            app: r.app,
            dataset: r.dataset,
            key: r.key,
            revision: r.revision,
            change: r.change,
            data: r.data.as_deref().and_then(|s| serde_json::from_str(s).ok()),
            diff: r.diff.as_deref().and_then(|s| serde_json::from_str(s).ok()),
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

/// Field-level diff between two JSON values. Nested objects are flattened to
/// dot-notation paths; arrays and scalars are compared wholesale at their
/// path. Each entry is `"path": {"from": old, "to": new}`; fields only present
/// on one side diff against `null`. The root path is `$`.
pub fn diff_values(old: &Value, new: &Value) -> Value {
    let mut out = serde_json::Map::new();
    diff_into("", old, new, &mut out);
    Value::Object(out)
}

fn diff_into(path: &str, old: &Value, new: &Value, out: &mut serde_json::Map<String, Value>) {
    match (old, new) {
        (Value::Object(a), Value::Object(b)) => {
            let keys: std::collections::BTreeSet<&String> = a.keys().chain(b.keys()).collect();
            for k in keys {
                let p = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
                diff_into(
                    &p,
                    a.get(k).unwrap_or(&Value::Null),
                    b.get(k).unwrap_or(&Value::Null),
                    out,
                );
            }
        }
        (a, b) if a == b => {}
        (a, b) => {
            let p = if path.is_empty() { "$" } else { path };
            out.insert(
                p.to_string(),
                serde_json::json!({ "from": a, "to": b }),
            );
        }
    }
}

/// serde_json's default `Map` is a `BTreeMap`, so `to_string` emits keys in
/// sorted order — a stable canonical form to hash.
fn hash_value(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Fixed-width RFC 3339 UTC micros — the stored timestamp format. Public so
/// keyset cursors built from a `Record` round-trip to the exact stored string.
pub fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| Error::Parse(format!("bad timestamp '{s}': {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn diff_reports_changed_added_and_dropped_fields() {
        let old = json!({ "title": "A", "close": "2026-01-01", "amount": 100 });
        let new = json!({ "title": "A", "close": "2026-02-01", "status": "open" });
        let diff = diff_values(&old, &new);
        assert_eq!(diff["close"], json!({ "from": "2026-01-01", "to": "2026-02-01" }));
        assert_eq!(diff["amount"], json!({ "from": 100, "to": null }));
        assert_eq!(diff["status"], json!({ "from": null, "to": "open" }));
        assert!(diff.get("title").is_none(), "unchanged fields are omitted");
    }

    #[test]
    fn diff_flattens_nested_objects_to_dot_paths() {
        let old = json!({ "meta": { "agency": "DOE", "codes": [1, 2] } });
        let new = json!({ "meta": { "agency": "DOD", "codes": [1, 2] } });
        let diff = diff_values(&old, &new);
        assert_eq!(diff["meta.agency"], json!({ "from": "DOE", "to": "DOD" }));
        assert!(diff.get("meta.codes").is_none());
    }

    #[test]
    fn diff_compares_arrays_and_scalars_wholesale() {
        let diff = diff_values(&json!([1, 2]), &json!([1, 3]));
        assert_eq!(diff["$"], json!({ "from": [1, 2], "to": [1, 3] }));
        let same = diff_values(&json!("x"), &json!("x"));
        assert_eq!(same, json!({}));
    }
}
