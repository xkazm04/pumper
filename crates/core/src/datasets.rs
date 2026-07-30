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
    /// How much this record is stood behind: `stable`, `provisional` (written
    /// while its source was degrading) or `quarantined`. Always populated —
    /// stored `NULL` reads back as `stable` (see [`trust_label`]).
    pub trust: String,
}

/// The trust value a stored `NULL` means.
///
/// `records.trust` is `NULL`-defaulted and `NULL` *means* `stable`: a semantic
/// default, not a sentinel, so every row written before the column existed is
/// correct by construction and no backfill is required. (`0004_simhash.sql`
/// added a derived column with a `DEFAULT 0` sentinel and no backfill, which
/// silently disabled near-dup detection for 3,367 rows — this is the shape that
/// does not repeat it.) Every reader must treat `NULL` and `"stable"` as the
/// same value, and this is the one place that decides it.
pub const TRUST_STABLE: &str = "stable";

/// Normalizes a stored trust value: `NULL` and an empty string are `stable`.
pub fn trust_label(stored: Option<&str>) -> String {
    match stored {
        Some(t) if !t.trim().is_empty() => t.to_string(),
        _ => TRUST_STABLE.to_string(),
    }
}

/// The `trust` predicate shared by the filtered read paths.
///
/// One bound parameter, no dynamic SQL: `NULL` matches everything, `'stable'`
/// matches rows whose stamp is missing *or* literally `stable` (the `NULL`
/// equivalence), and any other value matches exactly. Written as a single
/// expression so the record list and the change feed cannot interpret the filter
/// differently — the failure mode there would be a consumer that believes it
/// filtered and did not.
const TRUST_PREDICATE: &str =
    "(?T IS NULL OR (CASE WHEN ?T = 'stable' THEN COALESCE(trust, 'stable') ELSE trust END) = ?T)";

/// [`TRUST_PREDICATE`] with its placeholder bound to parameter index `n`.
fn trust_predicate(n: usize) -> String {
    TRUST_PREDICATE.replace("?T", &format!("?{n}"))
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
    /// Trust of the write that produced this revision — so the era a degrading
    /// source wrote stays exactly identifiable after the fact.
    pub trust: String,
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
    /// Max derived-spec chain depth (a derived dataset that is itself the
    /// source of another spec). Depth 1 is the first derived hop; writes past
    /// the cap are skipped with a warning, never an error — the source ingest
    /// must not fail because a spec chain is too deep. See `[derived] max_depth`.
    derived_max_depth: u32,
    /// Max source rows one aggregate group may re-scan during incremental
    /// maintenance. A group past this bound gets a `stale: true` derived row
    /// instead of a partially-computed (wrong) number; backfill — which pages
    /// the whole source anyway — computes it exactly and clears the flag.
    /// See `[derived] max_group_scan`.
    max_group_scan: i64,
}

/// Default for [`Datasets::derived_max_depth`] — mirrors
/// `config::DerivedConfig::default()`.
const DERIVED_MAX_DEPTH_DEFAULT: u32 = 3;

/// Default for [`Datasets::max_group_scan`] — mirrors
/// `config::DerivedConfig::default()`.
const DERIVED_MAX_GROUP_SCAN_DEFAULT: i64 = 10_000;

/// Records committed per write transaction in the batch write paths
/// (`upsert_many`, `detect_removed`). Trades throughput (fewer commits/fsyncs and
/// write-lock acquisitions) against how long one batch holds the write lock
/// against other apps' workers. 500 records of non-commit work is a few tens of
/// ms — well inside the 5s `busy_timeout`.
const UPSERT_CHUNK: usize = 500;

impl Datasets {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            derived_max_depth: DERIVED_MAX_DEPTH_DEFAULT,
            max_group_scan: DERIVED_MAX_GROUP_SCAN_DEFAULT,
        }
    }

    /// Overrides the derived-chain depth cap (from `[derived] max_depth`).
    pub fn with_derived_max_depth(mut self, max_depth: u32) -> Self {
        self.derived_max_depth = max_depth;
        self
    }

    /// Overrides the per-group recompute scan bound (from
    /// `[derived] max_group_scan`). Clamped to at least 1.
    pub fn with_max_group_scan(mut self, max_group_scan: i64) -> Self {
        self.max_group_scan = max_group_scan.max(1);
        self
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
        self.upsert_trusted(app, dataset, key, value, None).await
    }

    /// [`upsert`](Self::upsert) stamping a trust value on the record and its
    /// revision. `None` writes `NULL`, which *means* `stable` — see
    /// [`trust_label`].
    pub async fn upsert_trusted(
        &self,
        app: &str,
        dataset: &str,
        key: &str,
        value: &Value,
        trust: Option<&str>,
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
        let result = Self::upsert_in_tx(
            &mut conn,
            app,
            dataset,
            key,
            value,
            hash.as_str(),
            sim,
            now,
            trust,
        )
        .await;
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
    #[allow(clippy::too_many_arguments)]
    async fn upsert_in_tx(
        conn: &mut sqlx::SqliteConnection,
        app: &str,
        dataset: &str,
        key: &str,
        value: &Value,
        hash: &str,
        sim: i64,
        now: DateTime<Utc>,
        trust: Option<&str>,
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
                // Unchanged content, but trust still moves: a source that entered
                // `degraded` since the last run is no longer stood behind, even for
                // the records it re-confirmed. Leaving a stale `stable` stamp here
                // would let a filtered read serve them as trusted.
                sqlx::query(
                    "UPDATE records SET last_seen = ?4, trust = ?5 \
                     WHERE app = ?1 AND dataset = ?2 AND key = ?3",
                )
                .bind(app)
                .bind(dataset)
                .bind(key)
                .bind(ts(now))
                .bind(trust)
                .execute(&mut *conn)
                .await?;
                Ok(ChangeKind::Unchanged)
            }
            Some((_, old_data, _)) => {
                sqlx::query(
                    "UPDATE records SET hash = ?4, data = ?5, simhash = ?6, last_seen = ?7, \
                     updated_at = ?7, removed_at = NULL, trust = ?8 \
                     WHERE app = ?1 AND dataset = ?2 AND key = ?3",
                )
                .bind(app)
                .bind(dataset)
                .bind(key)
                .bind(hash)
                .bind(value.to_string())
                .bind(sim)
                .bind(ts(now))
                .bind(trust)
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
                    trust,
                )
                .await?;
                Ok(ChangeKind::Changed)
            }
            None => {
                sqlx::query(
                    "INSERT INTO records (app, dataset, key, hash, data, simhash, first_seen, last_seen, updated_at, trust) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?7, ?8)",
                )
                .bind(app)
                .bind(dataset)
                .bind(key)
                .bind(hash)
                .bind(value.to_string())
                .bind(sim)
                .bind(ts(now))
                .bind(trust)
                .execute(&mut *conn)
                .await?;
                Self::add_revision(
                    &mut *conn,
                    app,
                    dataset,
                    key,
                    "new",
                    Some(value),
                    None,
                    now,
                    trust,
                )
                .await?;
                Ok(ChangeKind::New)
            }
        }
    }

    /// Appends the next revision for a record (revision numbers are per-key,
    /// starting at 1). Runs on the caller-supplied executor so it can share the
    /// caller's transaction — the per-key `MAX(revision)` subquery must see the
    /// same in-flight state as the record write it accompanies.
    #[allow(clippy::too_many_arguments)]
    async fn add_revision<'e, E>(
        executor: E,
        app: &str,
        dataset: &str,
        key: &str,
        change: &str,
        data: Option<&Value>,
        diff: Option<&Value>,
        when: DateTime<Utc>,
        trust: Option<&str>,
    ) -> Result<()>
    where
        E: sqlx::SqliteExecutor<'e>,
    {
        sqlx::query(
            "INSERT INTO record_revisions (app, dataset, key, revision, change, data, diff, created_at, trust) \
             VALUES (?1, ?2, ?3, \
                     (SELECT COALESCE(MAX(revision), 0) + 1 FROM record_revisions \
                      WHERE app = ?1 AND dataset = ?2 AND key = ?3), \
                     ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .bind(change)
        .bind(data.map(Value::to_string))
        .bind(diff.map(Value::to_string))
        .bind(ts(when))
        .bind(trust)
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
            "SELECT app, dataset, key, revision, change, data, diff, created_at, trust \
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
    /// `trust` filters as in [`changes_page`](Self::changes_page). The worker's
    /// post-run hooks pass `None` and gate on the source's state instead: a push
    /// is irreversible, so it is suppressed at the source rather than filtered.
    pub async fn changes_since(
        &self,
        app: &str,
        dataset: Option<&str>,
        since: Option<DateTime<Utc>>,
        limit: i64,
        trust: Option<&str>,
    ) -> Result<Vec<Revision>> {
        let rows: Vec<RevisionRow> = sqlx::query_as(&format!(
            "SELECT app, dataset, key, revision, change, data, diff, created_at, trust \
             FROM record_revisions \
             WHERE app = ?1 AND (?2 IS NULL OR dataset = ?2) AND (?3 IS NULL OR created_at > ?3) \
             AND {} \
             ORDER BY created_at DESC LIMIT ?4",
            trust_predicate(5)
        ))
        .bind(app)
        .bind(dataset)
        .bind(since.map(ts))
        .bind(limit)
        .bind(trust)
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
        let (after_ts, after_rev) = after
            .map(|(t, r)| (Some(t), Some(r)))
            .unwrap_or((None, None));
        let rows: Vec<RevisionRow> = sqlx::query_as(
            "SELECT app, dataset, key, revision, change, data, diff, created_at, trust \
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
        let items: Vec<Revision> = rows
            .into_iter()
            .map(Revision::try_from)
            .collect::<Result<_>>()?;
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
    /// `trust` filters the feed: `None` returns everything, `Some("stable")`
    /// only what we stand behind (the default for the HTTP surface — a pull API
    /// is re-readable, so it filters rather than suppressing, and a consumer that
    /// wants everything can always ask).
    pub async fn changes_page(
        &self,
        app: &str,
        dataset: Option<&str>,
        since: Option<DateTime<Utc>>,
        after: Option<(String, i64)>,
        limit: i64,
        trust: Option<&str>,
    ) -> Result<RevisionPage> {
        let (after_ts, after_rowid) = after
            .map(|(t, r)| (Some(t), Some(r)))
            .unwrap_or((None, None));
        let rows: Vec<RevisionFeedRow> = sqlx::query_as(&format!(
            "SELECT rowid AS rowid, app, dataset, key, revision, change, data, diff, created_at, trust \
             FROM record_revisions \
             WHERE app = ?1 AND (?2 IS NULL OR dataset = ?2) AND (?3 IS NULL OR created_at > ?3) \
             AND (?4 IS NULL OR created_at < ?4 OR (created_at = ?4 AND rowid < ?5)) \
             AND {} \
             ORDER BY created_at DESC, rowid DESC LIMIT ?6",
            trust_predicate(7)
        ))
        .bind(app)
        .bind(dataset)
        .bind(since.map(ts))
        .bind(after_ts)
        .bind(after_rowid)
        .bind(limit)
        .bind(trust)
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
        let present: std::collections::HashSet<&str> = present.iter().map(String::as_str).collect();
        let to_remove: Vec<String> = live
            .into_iter()
            .filter(|k| !present.contains(k.as_str()))
            .collect();
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
        drop(conn);
        // Aggregate derived specs (M11 v2) count only live rows, so removals
        // shrink groups: recompute the affected groups. Fail-open like the
        // upsert-side hook — a broken spec must never fail the sync.
        self.apply_derived_removed(app, dataset, &to_remove).await;
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
        // A tombstone carries no trust stamp: removal detection is suppressed
        // entirely while a source is degrading, so every `removed` revision that
        // reaches here was written by a source we stand behind.
        Self::add_revision(
            &mut *conn, app, dataset, key, "removed", None, None, now, None,
        )
        .await?;
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
        self.upsert_many_trusted(app, dataset, items, None).await
    }

    /// [`upsert_many`](Self::upsert_many) stamping every record and revision with
    /// a trust value. `None` writes `NULL`, which *means* `stable`.
    pub async fn upsert_many_trusted(
        &self,
        app: &str,
        dataset: &str,
        items: &[(String, Value)],
        trust: Option<&str>,
    ) -> Result<UpsertSummary> {
        self.upsert_many_at_depth(app, dataset, items, trust, 0)
            .await
    }

    /// [`upsert_many_trusted`] carrying the derived-chain depth: 0 for a source
    /// ingest, +1 per derived hop. Boxed because the derived hook recurses
    /// (derived writes are themselves upserts that can match further specs);
    /// the recursion is bounded by `derived_max_depth`.
    fn upsert_many_at_depth<'a>(
        &'a self,
        app: &'a str,
        dataset: &'a str,
        items: &'a [(String, Value)],
        trust: Option<&'a str>,
        depth: u32,
    ) -> futures::future::BoxFuture<'a, Result<UpsertSummary>> {
        Box::pin(async move {
            let summary = self.upsert_many_inner(app, dataset, items, trust).await?;
            // Fresh keys flow through matching enabled derived specs in the
            // same flow. Fail-open: a broken spec must never fail the source
            // ingest, so derived errors are logged, not propagated.
            if summary.new.len() + summary.changed.len() > 0 {
                self.apply_derived(app, dataset, items, &summary, depth)
                    .await;
            }
            Ok(summary)
        })
    }

    /// The chunked write loop shared by every batch upsert (no derived hook).
    async fn upsert_many_inner(
        &self,
        app: &str,
        dataset: &str,
        items: &[(String, Value)],
        trust: Option<&str>,
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
                match Self::upsert_in_tx(
                    &mut conn,
                    app,
                    dataset,
                    key,
                    value,
                    hash.as_str(),
                    sim,
                    now,
                    trust,
                )
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

    /// Permanently deletes one record and its entire revision history in one
    /// transaction; returns whether the record existed.
    ///
    /// Distinct from full-snapshot removal (`detect_removed`), which *tombstones*
    /// (sets `removed_at` + a `removed` revision) so the disappearance is a
    /// change-feed signal. This is a hard delete — the row and all its history are
    /// gone — for an explicit operator action or a data-removal request. The
    /// caller is responsible for dropping the record's search doc.
    pub async fn delete_record(&self, app: &str, dataset: &str, key: &str) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let outcome = Self::delete_record_in_tx(&mut conn, app, dataset, key).await;
        match outcome {
            Ok(existed) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
                Ok(existed)
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    async fn delete_record_in_tx(
        conn: &mut sqlx::SqliteConnection,
        app: &str,
        dataset: &str,
        key: &str,
    ) -> Result<bool> {
        let existed =
            sqlx::query("DELETE FROM records WHERE app = ?1 AND dataset = ?2 AND key = ?3")
                .bind(app)
                .bind(dataset)
                .bind(key)
                .execute(&mut *conn)
                .await?
                .rows_affected()
                > 0;
        sqlx::query("DELETE FROM record_revisions WHERE app = ?1 AND dataset = ?2 AND key = ?3")
            .bind(app)
            .bind(dataset)
            .bind(key)
            .execute(&mut *conn)
            .await?;
        Ok(existed)
    }

    /// Permanently deletes an entire dataset — every record and all revision
    /// history — in one transaction; returns the number of records removed. For
    /// retiring a dataset or a full re-import. The caller drops the search docs.
    pub async fn delete_dataset(&self, app: &str, dataset: &str) -> Result<u64> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let outcome: Result<u64> = async {
            let removed = sqlx::query("DELETE FROM records WHERE app = ?1 AND dataset = ?2")
                .bind(app)
                .bind(dataset)
                .execute(&mut *conn)
                .await?
                .rows_affected();
            sqlx::query("DELETE FROM record_revisions WHERE app = ?1 AND dataset = ?2")
                .bind(app)
                .bind(dataset)
                .execute(&mut *conn)
                .await?;
            Ok(removed)
        }
        .await;
        match outcome {
            Ok(removed) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
                Ok(removed)
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    /// Trims revision history: deletes revisions created before `older_than`, but
    /// always keeps at least the newest `keep_min_per_key` revisions of every
    /// record so the diff chain and `history` stay usable. Returns the number
    /// pruned. Records themselves are untouched — only history shrinks.
    ///
    /// `record_revisions` stores a full snapshot per revision and is append-only,
    /// so on a scheduled source it grows without bound (≈ GB/year per active
    /// dataset); this is the knob that bounds it. The retention janitor calls it,
    /// but it is safe to call directly.
    pub async fn prune_revisions(
        &self,
        older_than: DateTime<Utc>,
        keep_min_per_key: i64,
    ) -> Result<u64> {
        let keep = keep_min_per_key.max(0);
        // A revision is pruned when it is older than the cutoff AND it is not
        // among the newest `keep` for its key — i.e. more than `keep` revisions of
        // that key have a revision number >= this one. Correlated subquery (no
        // window function, no DELETE-target alias) for SQLite portability; the
        // (app,dataset,key,revision) PK backs both predicates.
        let result = sqlx::query(
            "DELETE FROM record_revisions \
             WHERE created_at < ?1 \
               AND (SELECT COUNT(*) FROM record_revisions AS n \
                    WHERE n.app = record_revisions.app \
                      AND n.dataset = record_revisions.dataset \
                      AND n.key = record_revisions.key \
                      AND n.revision >= record_revisions.revision) > ?2",
        )
        .bind(ts(older_than))
        .bind(keep)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
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
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at, trust \
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
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at, trust \
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
    /// `trust` filters as in [`changes_page`](Self::changes_page); `None` (the
    /// default for this surface) returns every record with its stamp populated.
    pub async fn list_page(
        &self,
        app: &str,
        dataset: &str,
        after: Option<(String, String)>,
        limit: i64,
        trust: Option<&str>,
    ) -> Result<Vec<Record>> {
        let (after_ts, after_key) = after
            .map(|(t, k)| (Some(t), Some(k)))
            .unwrap_or((None, None));
        let rows: Vec<RecordRow> = sqlx::query_as(&format!(
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at, trust \
             FROM records WHERE app = ?1 AND dataset = ?2 \
             AND (?3 IS NULL OR updated_at < ?3 OR (updated_at = ?3 AND key < ?4)) \
             AND {} \
             ORDER BY updated_at DESC, key DESC LIMIT ?5",
            trust_predicate(6)
        ))
        .bind(app)
        .bind(dataset)
        .bind(after_ts)
        .bind(after_key)
        .bind(limit)
        .bind(trust)
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
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at, trust \
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
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at, trust \
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
        let names: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT dataset FROM records WHERE app = ?1 ORDER BY dataset",
        )
        .bind(app)
        .fetch_all(&self.pool)
        .await?;
        Ok(names)
    }

    // ── derived ──────────────────────────────────────────────────────────────
    // Derived datasets (M11 v1): filter/project(/single-key lookup) specs that
    // recompute incrementally on each upstream delta, riding the upsert flow
    // itself. CRUD lives on `Storage`; the store owns the hot-path read
    // (enabled specs for one source) and the recompute/backfill mechanics.

    /// Enabled specs whose source is `(app, dataset)` — the recompute set.
    async fn enabled_derived(&self, app: &str, dataset: &str) -> Result<Vec<DerivedSpec>> {
        let rows: Vec<DerivedRow> = sqlx::query_as(&format!(
            "SELECT {DERIVED_COLUMNS} FROM derived \
             WHERE source_app = ?1 AND source_dataset = ?2 AND enabled = 1 \
             ORDER BY created_at, id"
        ))
        .bind(app)
        .bind(dataset)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(DerivedSpec::try_from).collect()
    }

    /// Feeds a batch's fresh keys through the matching enabled specs, upserting
    /// the shaped rows into each spec's target dataset at `depth + 1`.
    ///
    /// Fail-open by design: every error path here logs and continues — a
    /// misconfigured spec must degrade the *derived* dataset, never the source
    /// ingest that triggered it. The depth cap is what prevents an unbounded
    /// cascade: derived writes recurse through `upsert_many_at_depth`, and a
    /// hop that would exceed `derived_max_depth` is skipped loudly.
    async fn apply_derived(
        &self,
        app: &str,
        dataset: &str,
        items: &[(String, Value)],
        summary: &UpsertSummary,
        depth: u32,
    ) {
        let specs = match self.enabled_derived(app, dataset).await {
            Ok(s) if !s.is_empty() => s,
            Ok(_) => return,
            Err(e) => {
                tracing::warn!(app, dataset, "derived: failed to load specs: {e}");
                return;
            }
        };
        if depth + 1 > self.derived_max_depth {
            tracing::warn!(
                app,
                dataset,
                max_depth = self.derived_max_depth,
                "derived: chain depth cap reached; downstream specs skipped"
            );
            return;
        }
        // Fresh keys only — unchanged records were already propagated by the
        // run that made them fresh, and the target's own change detection
        // dedups any no-op recompute for free.
        let by_key: std::collections::HashMap<&str, &Value> =
            items.iter().map(|(k, v)| (k.as_str(), v)).collect();
        for spec in &specs {
            if let Some(group) = &spec.group {
                // Aggregate spec (v2): recompute only the groups this batch
                // touched, from source truth. Same fail-open stance.
                if let Err(e) = self
                    .apply_derived_group(spec, group, &by_key, summary, depth)
                    .await
                {
                    tracing::warn!(spec = %spec.id, "derived: group recompute failed: {e}");
                }
                continue;
            }
            let mut out: Vec<(String, Value)> = Vec::new();
            for key in summary.fresh_keys() {
                let Some(data) = by_key.get(key.as_str()) else {
                    continue;
                };
                match self.derive_row(spec, key, data).await {
                    Ok(Some(row)) => out.push(row),
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(spec = %spec.id, key = %key, "derived: row skipped: {e}");
                    }
                }
            }
            if out.is_empty() {
                continue;
            }
            if let Err(e) = self
                .upsert_many_at_depth(app, &spec.target_dataset, &out, None, depth + 1)
                .await
            {
                tracing::warn!(spec = %spec.id, target = %spec.target_dataset,
                               "derived: target upsert failed: {e}");
            }
        }
    }

    /// Applies one spec to one source record: filter → project → lookup-merge.
    /// `Ok(None)` = filtered out; the derived key is the source key (1:1).
    async fn derive_row(
        &self,
        spec: &DerivedSpec,
        key: &str,
        data: &Value,
    ) -> Result<Option<(String, Value)>> {
        let filters = parse_filter_specs(&spec.filters)?;
        if !filters_match(&filters, data) {
            return Ok(None);
        }
        let mut value = project_value(&spec.project, data);
        if let Some(lookup) = &spec.lookup {
            // Single-key join: resolve the key expression against the SOURCE
            // record, fetch from the sibling dataset, merge under `merge_as`.
            // A missing key/record merges nothing — the row still lands, so a
            // late-arriving lookup side fills in on the next source delta.
            if let Some(lk) = lookup_json_path(data, &lookup.key_expr).and_then(value_text) {
                if let Some(rec) = self.get(&spec.source_app, &lookup.dataset, &lk).await? {
                    if rec.removed_at.is_none() {
                        if let Value::Object(map) = &mut value {
                            map.insert(lookup.merge_as.clone(), rec.data);
                        }
                    }
                }
            }
        }
        Ok(Some((key.to_string(), value)))
    }

    /// Materializes one spec over the existing live source rows in bounded
    /// keyset batches (`POST /derived/{id}/backfill`). Runs at depth 1, so a
    /// backfill's downstream cascade obeys the same cap as the live path.
    pub async fn backfill_derived(
        &self,
        spec: &DerivedSpec,
        batch: i64,
    ) -> Result<DerivedBackfill> {
        let batch = batch.clamp(1, MAX_BACKFILL_BATCH);
        if let Some(group) = &spec.group {
            return self.backfill_derived_group(spec, group, batch).await;
        }
        let mut report = DerivedBackfill::default();
        let mut after: Option<(String, String)> = None;
        loop {
            let page = self
                .list_page(&spec.source_app, &spec.source_dataset, after, batch, None)
                .await?;
            let n = page.len() as i64;
            let mut items: Vec<(String, Value)> = Vec::new();
            for rec in &page {
                if rec.removed_at.is_some() {
                    continue;
                }
                report.scanned += 1;
                if let Some(row) = self.derive_row(spec, &rec.key, &rec.data).await? {
                    items.push(row);
                }
            }
            report.matched += items.len() as u64;
            if !items.is_empty() {
                let s = self
                    .upsert_many_at_depth(&spec.source_app, &spec.target_dataset, &items, None, 1)
                    .await?;
                report.new += s.new.len() as u64;
                report.changed += s.changed.len() as u64;
                report.unchanged += s.unchanged as u64;
            }
            if n < batch {
                break;
            }
            after = page.last().map(|r| (ts(r.updated_at), r.key.clone()));
        }
        Ok(report)
    }

    // ── derived: group_by + aggregates (M11 v2) ──────────────────────────────

    /// Incremental maintenance for one aggregate spec: derive the set of group
    /// tuples this batch touched (new tuple for every fresh key, plus the OLD
    /// tuple of changed keys — a record that moved groups dirties both sides),
    /// then recompute each affected group from source truth. Recompute is exact
    /// because it re-reads the source rows of the group, never applies deltas;
    /// the `max_group_scan` bound is what keeps it honest — an oversized group
    /// gets a `stale: true` row instead of a number we didn't fully compute.
    async fn apply_derived_group(
        &self,
        spec: &DerivedSpec,
        group: &DerivedGroup,
        by_key: &std::collections::HashMap<&str, &Value>,
        summary: &UpsertSummary,
        depth: u32,
    ) -> Result<()> {
        let filters = parse_filter_specs(&spec.filters)?;
        let aggs = parse_aggregates(&group.aggregates)?;
        let mut tuples: std::collections::HashSet<Vec<String>> = Default::default();
        for key in summary.fresh_keys() {
            let Some(data) = by_key.get(key.as_str()) else {
                continue;
            };
            let new_tuple = group_tuple(&group.group_by, data);
            if let Some(t) = &new_tuple {
                tuples.insert(t.clone());
            }
            // Changed keys may have LEFT a group: reconstruct the old tuple
            // from the latest revision's field diff (`from` values overlay the
            // new record). A key missing from the diff didn't move.
            if summary.changed.iter().any(|k| k == key) {
                if let Some(old) = self
                    .old_group_tuple(spec, group, key, data, new_tuple.as_deref())
                    .await?
                {
                    tuples.insert(old);
                }
            }
        }
        self.recompute_groups(spec, group, &filters, &aggs, tuples, depth)
            .await
    }

    /// The group tuple a changed record belonged to BEFORE this write, read
    /// from the diff stored on its newest revision. `None` when the old row
    /// had no complete tuple (or didn't change group fields).
    async fn old_group_tuple(
        &self,
        spec: &DerivedSpec,
        group: &DerivedGroup,
        key: &str,
        new_data: &Value,
        new_tuple: Option<&[String]>,
    ) -> Result<Option<Vec<String>>> {
        let revs = self
            .history(&spec.source_app, &spec.source_dataset, key, 1)
            .await?;
        let Some(diff) = revs.first().and_then(|r| r.diff.as_ref()) else {
            return Ok(None);
        };
        let mut old: Vec<String> = Vec::with_capacity(group.group_by.len());
        let mut moved = false;
        for path in &group.group_by {
            let rel = path.trim_start_matches("$.");
            let v = match diff.get(rel) {
                Some(entry) => {
                    moved = true;
                    entry.get("from").and_then(group_value_text)
                }
                None => lookup_json_path(new_data, path).and_then(|v| group_value_text(v)),
            };
            match v {
                Some(v) => old.push(v),
                None => return Ok(None),
            }
        }
        if !moved || Some(old.as_slice()) == new_tuple {
            return Ok(None);
        }
        Ok(Some(old))
    }

    /// Recomputes each tuple's group row from the live source rows and upserts
    /// the batch into the target at `depth + 1`.
    async fn recompute_groups(
        &self,
        spec: &DerivedSpec,
        group: &DerivedGroup,
        filters: &[JsonFilter],
        aggs: &std::collections::BTreeMap<String, Aggregate>,
        tuples: std::collections::HashSet<Vec<String>>,
        depth: u32,
    ) -> Result<()> {
        if tuples.is_empty() {
            return Ok(());
        }
        let mut out: Vec<(String, Value)> = Vec::new();
        for tuple in tuples {
            match self.recompute_group_row(spec, group, filters, aggs, &tuple).await {
                Ok(row) => out.push(row),
                Err(e) => {
                    tracing::warn!(spec = %spec.id, "derived: group row skipped: {e}");
                }
            }
        }
        if out.is_empty() {
            return Ok(());
        }
        self.upsert_many_at_depth(&spec.source_app, &spec.target_dataset, &out, None, depth + 1)
            .await?;
        Ok(())
    }

    /// Builds one group's derived row from source truth: scan the group's live
    /// source rows (bounded at `max_group_scan + 1`) and evaluate every
    /// aggregate. Over the bound, the row is `{group fields, stale: true}` with
    /// NO aggregate fields — absent, not wrong. The derived key is the group
    /// values joined with `|` (escaped, see [`group_row_key`]).
    async fn recompute_group_row(
        &self,
        spec: &DerivedSpec,
        group: &DerivedGroup,
        filters: &[JsonFilter],
        aggs: &std::collections::BTreeMap<String, Aggregate>,
        tuple: &[String],
    ) -> Result<(String, Value)> {
        let rows = self
            .group_source_rows(
                &spec.source_app,
                &spec.source_dataset,
                filters,
                &group.group_by,
                tuple,
                self.max_group_scan + 1,
            )
            .await?;
        let mut data = serde_json::Map::new();
        for (path, value) in group.group_by.iter().zip(tuple) {
            data.insert(group_field_name(path).to_string(), Value::String(value.clone()));
        }
        if rows.len() as i64 > self.max_group_scan {
            data.insert("stale".into(), Value::Bool(true));
            return Ok((group_row_key(tuple), Value::Object(data)));
        }
        data.insert("stale".into(), Value::Bool(false));
        for (out, agg) in aggs {
            let v = match agg {
                Aggregate::Count => Value::from(rows.len() as u64),
                Aggregate::Sum(path) => {
                    let sum: f64 = rows
                        .iter()
                        .filter_map(|r| lookup_json_path(r, path).and_then(Value::as_f64))
                        .sum();
                    // Whole sums render as integers so counts-of-cents style
                    // data doesn't grow a spurious `.0`.
                    if sum.fract() == 0.0 && sum.abs() < (i64::MAX as f64) {
                        Value::from(sum as i64)
                    } else {
                        Value::from(sum)
                    }
                }
            };
            data.insert(out.clone(), v);
        }
        Ok((group_row_key(tuple), Value::Object(data)))
    }

    /// Live source rows of ONE group (spec filters ANDed with the group-value
    /// predicates), capped at `limit`. Group matching compares
    /// `CAST(json_extract(...) AS TEXT)` against the tuple's canonical text —
    /// the SQL twin of [`group_value_text`] — and the `json_type` guard keeps
    /// bool/object/array fields out on both sides.
    async fn group_source_rows(
        &self,
        app: &str,
        dataset: &str,
        filters: &[JsonFilter],
        group_by: &[String],
        tuple: &[String],
        limit: i64,
    ) -> Result<Vec<Value>> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT data FROM records WHERE removed_at IS NULL AND app = ",
        );
        qb.push_bind(app);
        qb.push(" AND dataset = ");
        qb.push_bind(dataset);
        push_json_filters(&mut qb, filters);
        for (path, value) in group_by.iter().zip(tuple) {
            qb.push(" AND json_type(data, ");
            qb.push_bind(path.as_str());
            qb.push(") IN ('text','integer','real') AND CAST(json_extract(data, ");
            qb.push_bind(path.as_str());
            qb.push(") AS TEXT) = ");
            qb.push_bind(value.as_str());
        }
        qb.push(" LIMIT ");
        qb.push_bind(limit);
        let raw: Vec<(String,)> = qb.build_query_as().fetch_all(&self.pool).await?;
        Ok(raw
            .into_iter()
            .map(|(d,)| serde_json::from_str(&d).unwrap_or(Value::Null))
            .collect())
    }

    /// Removal-side maintenance: recompute the groups the removed records
    /// belonged to. Called by [`detect_removed`](Self::detect_removed) after
    /// tombstoning (fail-open — a broken spec never fails the sync); the
    /// tombstoned rows still carry their data, and the recompute scan excludes
    /// them via `removed_at IS NULL`, so the groups shrink exactly.
    async fn apply_derived_removed(&self, app: &str, dataset: &str, removed: &[String]) {
        let specs = match self.enabled_derived(app, dataset).await {
            Ok(s) if s.iter().any(|s| s.group.is_some()) => s,
            Ok(_) => return,
            Err(e) => {
                tracing::warn!(app, dataset, "derived: failed to load specs: {e}");
                return;
            }
        };
        if self.derived_max_depth < 1 {
            return;
        }
        for spec in specs.iter().filter(|s| s.group.is_some()) {
            let group = spec.group.as_ref().expect("filtered on group");
            let (filters, aggs) = match (
                parse_filter_specs(&spec.filters),
                parse_aggregates(&group.aggregates),
            ) {
                (Ok(f), Ok(a)) => (f, a),
                (Err(e), _) | (_, Err(e)) => {
                    tracing::warn!(spec = %spec.id, "derived: bad spec skipped: {e}");
                    continue;
                }
            };
            let mut tuples: std::collections::HashSet<Vec<String>> = Default::default();
            for key in removed {
                match self.get(app, dataset, key).await {
                    Ok(Some(rec)) => {
                        if filters_match(&filters, &rec.data) {
                            if let Some(t) = group_tuple(&group.group_by, &rec.data) {
                                tuples.insert(t);
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(spec = %spec.id, key = %key, "derived: removed-key read failed: {e}");
                    }
                }
            }
            if let Err(e) = self.recompute_groups(spec, group, &filters, &aggs, tuples, 0).await {
                tracing::warn!(spec = %spec.id, "derived: removal recompute failed: {e}");
            }
        }
    }

    /// Backfill for an aggregate spec: one full pass over the live source rows
    /// (keyset pages of `batch`) accumulating every group in memory, then one
    /// exact upsert per group — always fresh (`stale: false`), so a backfill is
    /// also the repair path for groups the live bound marked stale. Groups
    /// whose last source row disappeared before the backfill keep their old
    /// derived row (backfill only sees groups that still have rows); the live
    /// removal hook is what zeroes a group as it empties.
    async fn backfill_derived_group(
        &self,
        spec: &DerivedSpec,
        group: &DerivedGroup,
        batch: i64,
    ) -> Result<DerivedBackfill> {
        let filters = parse_filter_specs(&spec.filters)?;
        let aggs = parse_aggregates(&group.aggregates)?;
        let mut report = DerivedBackfill::default();
        // tuple -> (count, per-aggregate sums keyed like `aggs`)
        let mut groups: std::collections::HashMap<Vec<String>, (u64, std::collections::BTreeMap<String, f64>)> =
            Default::default();
        let mut after: Option<(String, String)> = None;
        loop {
            let page = self
                .list_page(&spec.source_app, &spec.source_dataset, after, batch, None)
                .await?;
            let n = page.len() as i64;
            for rec in &page {
                if rec.removed_at.is_some() {
                    continue;
                }
                report.scanned += 1;
                if !filters_match(&filters, &rec.data) {
                    continue;
                }
                let Some(tuple) = group_tuple(&group.group_by, &rec.data) else {
                    continue;
                };
                let entry = groups.entry(tuple).or_default();
                entry.0 += 1;
                for (out, agg) in &aggs {
                    if let Aggregate::Sum(path) = agg {
                        if let Some(v) = lookup_json_path(&rec.data, path).and_then(Value::as_f64) {
                            *entry.1.entry(out.clone()).or_default() += v;
                        }
                    }
                }
            }
            if n < batch {
                break;
            }
            after = page.last().map(|r| (ts(r.updated_at), r.key.clone()));
        }
        let mut out: Vec<(String, Value)> = Vec::new();
        for (tuple, (count, sums)) in &groups {
            let mut data = serde_json::Map::new();
            for (path, value) in group.group_by.iter().zip(tuple) {
                data.insert(group_field_name(path).to_string(), Value::String(value.clone()));
            }
            data.insert("stale".into(), Value::Bool(false));
            for (name, agg) in &aggs {
                let v = match agg {
                    Aggregate::Count => Value::from(*count),
                    Aggregate::Sum(_) => {
                        let sum = sums.get(name).copied().unwrap_or(0.0);
                        if sum.fract() == 0.0 && sum.abs() < (i64::MAX as f64) {
                            Value::from(sum as i64)
                        } else {
                            Value::from(sum)
                        }
                    }
                };
                data.insert(name.clone(), v);
            }
            out.push((group_row_key(tuple), Value::Object(data)));
        }
        report.matched = out.len() as u64;
        if !out.is_empty() {
            let s = self
                .upsert_many_at_depth(&spec.source_app, &spec.target_dataset, &out, None, 1)
                .await?;
            report.new += s.new.len() as u64;
            report.changed += s.changed.len() as u64;
            report.unchanged += s.unchanged as u64;
        }
        Ok(report)
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

// ── derived: types + pure helpers ────────────────────────────────────────────

/// Ceiling on one backfill batch (rows read per keyset page / write chunk).
pub const MAX_BACKFILL_BATCH: i64 = 1000;

/// Single-key join half of a derived spec: resolve `key_expr` (a `$.path` into
/// the SOURCE record) to a key, fetch that record from `dataset` (same app),
/// and merge its data under the `merge_as` field of the derived row.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct DerivedLookup {
    pub dataset: String,
    pub key_expr: String,
    pub merge_as: String,
}

/// Aggregate half of a derived spec (M11 v2): bucket the source rows by the
/// text value of each `group_by` path and evaluate `aggregates`
/// (`{out_field: "count" | "sum($.path)"}`) per bucket. One derived row per
/// group, keyed by the joined group values; rows whose group fields are
/// missing or non-scalar (bool/object/array/null) belong to no group.
/// Mutually exclusive with `lookup` and `project` — enforced at create time
/// and structurally by sharing the stored `lookup` column.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct DerivedGroup {
    /// `$.path` group keys, in order. Values group by their canonical text
    /// (strings bare, numbers rendered) — the string "42" and the number 42
    /// share a group, and float keys are discouraged.
    pub group_by: Vec<String>,
    /// `{out_field: "count" | "sum($.path)"}` — order-stable via BTreeMap so
    /// the derived row (and its change detection) is deterministic.
    pub aggregates: std::collections::BTreeMap<String, String>,
}

/// One parsed aggregate expression. v2 scope is count + sum.
#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    Count,
    /// Numeric sum over a `$.path`; rows where the path is missing or
    /// non-numeric contribute nothing (0), mirroring SQL `SUM` over NULLs.
    Sum(String),
}

/// Parses one aggregate expression: `count` or `sum($.path)`.
pub fn parse_aggregate(expr: &str) -> Result<Aggregate> {
    let expr = expr.trim();
    if expr == "count" {
        return Ok(Aggregate::Count);
    }
    if let Some(inner) = expr
        .strip_prefix("sum(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        let path = inner.trim();
        if path.starts_with("$.") {
            return Ok(Aggregate::Sum(path.to_string()));
        }
        return Err(Error::BadRequest(format!(
            "sum path '{path}' must be a JSON path starting with '$.' (in '{expr}')"
        )));
    }
    Err(Error::BadRequest(format!(
        "unknown aggregate '{expr}' (expected 'count' or 'sum($.path)')"
    )))
}

/// Parses a whole aggregate map, preserving output-field order.
pub fn parse_aggregates(
    aggregates: &std::collections::BTreeMap<String, String>,
) -> Result<std::collections::BTreeMap<String, Aggregate>> {
    aggregates
        .iter()
        .map(|(out, expr)| Ok((out.clone(), parse_aggregate(expr)?)))
        .collect()
}

/// Validates an aggregate spec at create time so a stored spec can always be
/// evaluated: paths well-formed, expressions parseable, and the derived row's
/// field names (group segments + aggregate outs + the reserved `stale`) free
/// of collisions.
pub fn validate_group(group: &DerivedGroup) -> Result<()> {
    let bad = |msg: String| Error::BadRequest(msg);
    if group.group_by.is_empty() {
        return Err(bad("group_by must name at least one JSON path".into()));
    }
    if group.aggregates.is_empty() {
        return Err(bad("aggregates must define at least one output".into()));
    }
    let mut names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    names.insert("stale");
    for path in &group.group_by {
        if !path.starts_with("$.") {
            return Err(bad(format!(
                "group_by path '{path}' must be a JSON path starting with '$.'"
            )));
        }
        if !names.insert(group_field_name(path)) {
            return Err(bad(format!(
                "group_by path '{path}' collides with another derived-row field \
                 ('stale' is reserved)"
            )));
        }
    }
    for (out, expr) in &group.aggregates {
        parse_aggregate(expr)?;
        if !names.insert(out.as_str()) {
            return Err(bad(format!(
                "aggregate output '{out}' collides with another derived-row field \
                 ('stale' is reserved)"
            )));
        }
    }
    Ok(())
}

/// The derived-row field a group path lands under: its last segment
/// (`$.meta.state` → `state`).
pub(crate) fn group_field_name(path: &str) -> &str {
    path.trim_start_matches("$.").rsplit('.').next().unwrap_or(path)
}

/// A group value's canonical text: strings bare, numbers rendered — the
/// in-memory twin of the SQL `CAST(json_extract(...) AS TEXT)` predicate.
/// Bool/null/object/array yield `None`: such rows belong to no group (the SQL
/// side excludes them via `json_type`).
fn group_value_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// A record's group tuple: the canonical text of every `group_by` path, or
/// `None` when any is missing/non-scalar (the row belongs to no group).
pub(crate) fn group_tuple(group_by: &[String], data: &Value) -> Option<Vec<String>> {
    group_by
        .iter()
        .map(|p| lookup_json_path(data, p).and_then(group_value_text))
        .collect()
}

/// Derived key for a group row: the group values joined with `|`, with `\` and
/// `|` escaped so distinct tuples can never collide on one key.
pub(crate) fn group_row_key(tuple: &[String]) -> String {
    tuple
        .iter()
        .map(|v| v.replace('\\', "\\\\").replace('|', "\\|"))
        .collect::<Vec<_>>()
        .join("|")
}

/// A dataset declared as a transformation of another dataset in the same app:
/// filter (ANDed `$.path:op:value` specs, the `?filter=` grammar) → project
/// (`{out_field: "$.path"}`; empty = passthrough) → optional single-key lookup.
/// Derived rows key 1:1 by the source key and land in
/// `(source_app, target_dataset)`.
#[derive(Debug, Clone, Serialize)]
pub struct DerivedSpec {
    pub id: String,
    pub source_app: String,
    pub source_dataset: String,
    pub target_dataset: String,
    pub filters: Vec<String>,
    /// `{out_field: "$.path"}` — order-stable via BTreeMap so projection is
    /// deterministic (and hashing/change detection with it).
    pub project: std::collections::BTreeMap<String, String>,
    pub lookup: Option<DerivedLookup>,
    /// Aggregate half (M11 v2). Stored in the same column as `lookup` (a spec
    /// is either row-shaped or group-shaped, never both), so v2 needed no
    /// migration; at most one of `lookup`/`group` is `Some`.
    pub group: Option<DerivedGroup>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

/// Outcome of a backfill run over the existing source rows.
#[derive(Debug, Default, Serialize)]
pub struct DerivedBackfill {
    /// Live source rows examined.
    pub scanned: u64,
    /// Rows that passed the spec's filters and were upserted.
    pub matched: u64,
    pub new: u64,
    pub changed: u64,
    pub unchanged: u64,
}

pub(crate) const DERIVED_COLUMNS: &str =
    "id, source_app, source_dataset, target_dataset, filters, project, lookup, enabled, created_at";

#[derive(sqlx::FromRow)]
pub(crate) struct DerivedRow {
    pub(crate) id: String,
    pub(crate) source_app: String,
    pub(crate) source_dataset: String,
    pub(crate) target_dataset: String,
    pub(crate) filters: String,
    pub(crate) project: String,
    pub(crate) lookup: Option<String>,
    pub(crate) enabled: i64,
    pub(crate) created_at: String,
}

impl TryFrom<DerivedRow> for DerivedSpec {
    type Error = Error;

    fn try_from(r: DerivedRow) -> Result<DerivedSpec> {
        // The `lookup` column holds either shape; their required fields are
        // disjoint, so the untagged parse is unambiguous.
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum StoredJoin {
            Lookup(DerivedLookup),
            Group(DerivedGroup),
        }
        let (lookup, group) = match r
            .lookup
            .as_deref()
            .and_then(|s| serde_json::from_str::<StoredJoin>(s).ok())
        {
            Some(StoredJoin::Lookup(l)) => (Some(l), None),
            Some(StoredJoin::Group(g)) => (None, Some(g)),
            None => (None, None),
        };
        Ok(DerivedSpec {
            id: r.id,
            source_app: r.source_app,
            source_dataset: r.source_dataset,
            target_dataset: r.target_dataset,
            filters: serde_json::from_str(&r.filters).unwrap_or_default(),
            project: serde_json::from_str(&r.project).unwrap_or_default(),
            lookup,
            group,
            enabled: r.enabled != 0,
            created_at: parse_ts(&r.created_at)?,
        })
    }
}

/// Parses one `<path>:<op>:<value>` filter spec (the `?filter=` grammar) into a
/// [`JsonFilter`]. Core-level twin of the HTTP layer's parser so stored derived
/// specs evaluate without the server crate; same grammar, same ops
/// (`eq|contains|gte|lte|numgte`), value keeps any `:` after the op.
pub fn parse_filter_spec(spec: &str) -> Result<JsonFilter> {
    let bad = |msg: String| Error::BadRequest(msg);
    let mut parts = spec.splitn(3, ':');
    let path = parts.next().unwrap_or("");
    let (Some(op), Some(value)) = (parts.next(), parts.next()) else {
        return Err(bad(format!(
            "filter '{spec}' must be '<path>:<op>:<value>' (e.g. $.state:eq:CA)"
        )));
    };
    let check_path = |p: &str| -> Result<()> {
        if p.starts_with("$.") {
            Ok(())
        } else {
            Err(bad(format!(
                "filter path '{p}' must be a JSON path starting with '$.' (in '{spec}')"
            )))
        }
    };
    Ok(match op {
        "eq" => {
            check_path(path)?;
            JsonFilter::Eq {
                path: path.into(),
                value: value.into(),
            }
        }
        "contains" => {
            check_path(path)?;
            JsonFilter::Contains {
                path: path.into(),
                value: value.into(),
            }
        }
        "gte" => {
            check_path(path)?;
            JsonFilter::Gte {
                path: path.into(),
                value: value.into(),
            }
        }
        "lte" => {
            check_path(path)?;
            JsonFilter::Lte {
                path: path.into(),
                value: value.into(),
            }
        }
        "numgte" => {
            let paths: Vec<String> = path.split(',').map(|p| p.trim().to_string()).collect();
            for p in &paths {
                check_path(p)?;
            }
            let value: f64 = value
                .parse()
                .map_err(|_| bad(format!("numgte value '{value}' is not a number")))?;
            JsonFilter::NumGteAny { paths, value }
        }
        other => return Err(bad(format!("unknown filter op '{other}' (in '{spec}')"))),
    })
}

/// Parses a whole spec list ([`parse_filter_spec`] per entry, ANDed by callers).
pub fn parse_filter_specs(specs: &[String]) -> Result<Vec<JsonFilter>> {
    specs.iter().map(|s| parse_filter_spec(s)).collect()
}

/// Resolves a `$.a.b` JSON path against `data` (objects only — the `?filter=`
/// grammar has no array indexing, matching the store-level SQL semantics).
fn lookup_json_path<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = data;
    for seg in path.trim_start_matches("$.").split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Scalar-to-text projection mirroring SQLite's `->>`: strings stay bare,
/// numbers/bools render, null/objects/arrays don't participate.
fn value_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// True when one filter holds against a record's JSON. Semantics mirror the SQL
/// `push_json_filters` mapping (and the ingress payload matcher): `Eq` exact
/// text, `Contains` case-insensitive substring, `Gte`/`Lte` lexicographic text,
/// `NumGteAny` numeric `>=` on any of its paths (non-numbers never match).
fn filter_matches_value(filter: &JsonFilter, data: &Value) -> bool {
    match filter {
        JsonFilter::Eq { path, value } => {
            lookup_json_path(data, path).and_then(value_text).as_deref() == Some(value.as_str())
        }
        JsonFilter::Contains { path, value } => lookup_json_path(data, path)
            .and_then(value_text)
            .is_some_and(|t| t.to_lowercase().contains(&value.to_lowercase())),
        JsonFilter::Gte { path, value } => lookup_json_path(data, path)
            .and_then(value_text)
            .is_some_and(|t| t.as_str() >= value.as_str()),
        JsonFilter::Lte { path, value } => lookup_json_path(data, path)
            .and_then(value_text)
            .is_some_and(|t| t.as_str() <= value.as_str()),
        JsonFilter::NumGteAny { paths, value } => paths.iter().any(|p| {
            lookup_json_path(data, p)
                .and_then(Value::as_f64)
                .is_some_and(|n| n >= *value)
        }),
    }
}

/// True when EVERY filter holds (AND). Empty = match everything.
pub fn filters_match(filters: &[JsonFilter], data: &Value) -> bool {
    filters.iter().all(|f| filter_matches_value(f, data))
}

/// Applies a projection map to a source record: `{out_field: "$.path"}` builds
/// a fresh object of the extracted values (missing/unresolvable paths are
/// omitted, so absent fields don't materialize as nulls). An empty map is a
/// passthrough of the whole record.
pub fn project_value(project: &std::collections::BTreeMap<String, String>, data: &Value) -> Value {
    if project.is_empty() {
        return data.clone();
    }
    let mut out = serde_json::Map::new();
    for (field, path) in project {
        if let Some(v) = lookup_json_path(data, path) {
            if !v.is_null() {
                out.insert(field.clone(), v.clone());
            }
        }
    }
    Value::Object(out)
}

/// Spec-create-time cycle guard (the trigger DAG guard's approach, applied to
/// datasets instead of trigger ids): adding `source → target` closes a cycle
/// iff `target` already reaches `source` through existing spec edges within
/// the same app — including the self-loop `source == target`. Chains that stay
/// acyclic remain bounded at runtime by the depth cap regardless.
pub fn derived_would_cycle(existing: &[DerivedSpec], source: &str, target: &str) -> bool {
    if source == target {
        return true;
    }
    // DFS over target-reachable datasets.
    let mut stack: Vec<&str> = vec![target];
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    while let Some(node) = stack.pop() {
        if !seen.insert(node) {
            continue;
        }
        for spec in existing.iter().filter(|s| s.source_dataset == node) {
            if spec.target_dataset == source {
                return true;
            }
            stack.push(&spec.target_dataset);
        }
    }
    false
}

#[derive(sqlx::FromRow)]
struct RecordRow {
    key: String,
    data: String,
    first_seen: String,
    last_seen: String,
    updated_at: String,
    removed_at: Option<String>,
    trust: Option<String>,
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
            trust: trust_label(r.trust.as_deref()),
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
    trust: Option<String>,
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
            trust: trust_label(r.trust.as_deref()),
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
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
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
            out.insert(p.to_string(), serde_json::json!({ "from": a, "to": b }));
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
        assert_eq!(
            diff["close"],
            json!({ "from": "2026-01-01", "to": "2026-02-01" })
        );
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
    fn a_missing_trust_stamp_means_stable() {
        // The whole point of the NULL-means-stable choice: every row written before
        // the column existed is already correct, so no backfill is required. If
        // this equivalence ever breaks, 5,000+ pre-migration records silently
        // become untrusted and drop out of every filtered read.
        assert_eq!(trust_label(None), TRUST_STABLE);
        assert_eq!(trust_label(Some("")), TRUST_STABLE);
        assert_eq!(trust_label(Some("   ")), TRUST_STABLE);
        assert_eq!(trust_label(Some("stable")), TRUST_STABLE);
        // Real stamps pass through unchanged.
        assert_eq!(trust_label(Some("provisional")), "provisional");
        assert_eq!(trust_label(Some("quarantined")), "quarantined");
    }

    #[test]
    fn the_trust_predicate_treats_null_and_stable_alike() {
        // `stable` must match both an unstamped row and an explicitly stable one,
        // any other value matches exactly, and no filter matches everything.
        let sql = trust_predicate(3);
        assert!(sql.contains("?3"), "{sql}");
        assert!(
            !sql.contains("?T"),
            "placeholder must be substituted: {sql}"
        );
        assert!(sql.contains("COALESCE(trust, 'stable')"), "{sql}");
        assert!(
            sql.starts_with("(?3 IS NULL OR"),
            "no filter must match everything: {sql}"
        );
    }

    // ── derived: pure helpers ────────────────────────────────────────────────

    fn spec(source: &str, target: &str) -> DerivedSpec {
        DerivedSpec {
            id: format!("{source}->{target}"),
            source_app: "app".into(),
            source_dataset: source.into(),
            target_dataset: target.into(),
            filters: Vec::new(),
            project: Default::default(),
            lookup: None,
            group: None,
            enabled: true,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn derived_cycle_guard_rejects_self_direct_and_transitive_loops() {
        // Self-loop: a dataset can never derive from itself.
        assert!(derived_would_cycle(&[], "a", "a"));
        // Direct: a→b exists; adding b→a closes the loop.
        let specs = vec![spec("a", "b")];
        assert!(derived_would_cycle(&specs, "b", "a"));
        // Transitive: a→b→c exists; adding c→a closes it.
        let specs = vec![spec("a", "b"), spec("b", "c")];
        assert!(derived_would_cycle(&specs, "c", "a"));
        // Acyclic extensions are allowed: a fan-out and a longer chain.
        assert!(!derived_would_cycle(&specs, "a", "d"));
        assert!(!derived_would_cycle(&specs, "c", "d"));
    }

    #[test]
    fn filter_spec_parser_mirrors_the_http_grammar() {
        assert!(matches!(
            parse_filter_spec("$.state:eq:CA").unwrap(),
            JsonFilter::Eq { path, value } if path == "$.state" && value == "CA"
        ));
        // Value keeps its colons after the op.
        assert!(matches!(
            parse_filter_spec("$.seen:eq:2026-07-17T10:30:00Z").unwrap(),
            JsonFilter::Eq { value, .. } if value == "2026-07-17T10:30:00Z"
        ));
        assert!(matches!(
            parse_filter_spec("$.a,$.b:numgte:5").unwrap(),
            JsonFilter::NumGteAny { paths, value } if paths.len() == 2 && value == 5.0
        ));
        // Malformed shapes are rejected, not silently ignored.
        assert!(parse_filter_spec("$.state").is_err());
        assert!(parse_filter_spec("state:eq:CA").is_err());
        assert!(parse_filter_spec("$.state:like:CA").is_err());
        assert!(parse_filter_spec("$.n:numgte:lots").is_err());
    }

    #[test]
    fn filters_match_evaluates_in_memory_with_sql_parity() {
        let data = json!({ "state": "CA", "meta": { "amount": 42 }, "title": "Solar Grant" });
        let f = |s: &str| vec![parse_filter_spec(s).unwrap()];
        assert!(filters_match(&f("$.state:eq:CA"), &data));
        assert!(!filters_match(&f("$.state:eq:NY"), &data));
        assert!(filters_match(&f("$.title:contains:solar"), &data));
        assert!(filters_match(&f("$.meta.amount:numgte:40"), &data));
        assert!(!filters_match(&f("$.meta.amount:numgte:50"), &data));
        // Missing paths never match (NULL-rejecting, like the SQL).
        assert!(!filters_match(&f("$.nope:eq:x"), &data));
        // AND semantics; empty set matches everything.
        let both = vec![
            parse_filter_spec("$.state:eq:CA").unwrap(),
            parse_filter_spec("$.meta.amount:numgte:50").unwrap(),
        ];
        assert!(!filters_match(&both, &data));
        assert!(filters_match(&[], &data));
    }

    #[test]
    fn projection_extracts_paths_and_empty_map_is_passthrough() {
        let data = json!({ "title": "A", "meta": { "state": "CA" }, "noise": true });
        let mut project = std::collections::BTreeMap::new();
        project.insert("name".to_string(), "$.title".to_string());
        project.insert("state".to_string(), "$.meta.state".to_string());
        project.insert("missing".to_string(), "$.not.there".to_string());
        assert_eq!(
            project_value(&project, &data),
            json!({ "name": "A", "state": "CA" }),
            "resolved paths land; missing paths are omitted, not null"
        );
        assert_eq!(project_value(&Default::default(), &data), data);
    }

    #[test]
    fn aggregate_parser_accepts_count_and_sum_only() {
        assert_eq!(parse_aggregate("count").unwrap(), Aggregate::Count);
        assert_eq!(
            parse_aggregate("sum($.amount)").unwrap(),
            Aggregate::Sum("$.amount".into())
        );
        assert_eq!(
            parse_aggregate(" sum( $.a.b ) ").unwrap(),
            Aggregate::Sum("$.a.b".into())
        );
        // v2 scope is count + sum; everything else is refused loudly.
        assert!(parse_aggregate("avg($.x)").is_err());
        assert!(parse_aggregate("sum(amount)").is_err(), "path must be $.-rooted");
        assert!(parse_aggregate("sum($.x").is_err(), "unbalanced parens");
        assert!(parse_aggregate("").is_err());
    }

    #[test]
    fn group_validation_guards_paths_names_and_reserved_stale() {
        let mk = |group_by: &[&str], aggs: &[(&str, &str)]| DerivedGroup {
            group_by: group_by.iter().map(|s| s.to_string()).collect(),
            aggregates: aggs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        assert!(validate_group(&mk(&["$.state"], &[("n", "count")])).is_ok());
        assert!(validate_group(&mk(&[], &[("n", "count")])).is_err());
        assert!(validate_group(&mk(&["$.state"], &[])).is_err());
        assert!(validate_group(&mk(&["state"], &[("n", "count")])).is_err());
        // `stale` is reserved, and outs can't shadow group fields.
        assert!(validate_group(&mk(&["$.state"], &[("stale", "count")])).is_err());
        assert!(validate_group(&mk(&["$.stale"], &[("n", "count")])).is_err());
        assert!(validate_group(&mk(&["$.state"], &[("state", "count")])).is_err());
        // Two paths ending in the same segment collide on the derived row.
        assert!(validate_group(&mk(&["$.a.id", "$.b.id"], &[("n", "count")])).is_err());
    }

    #[test]
    fn group_tuples_and_keys_are_canonical_and_collision_free() {
        let data = json!({ "state": "CA", "meta": { "n": 42 }, "flag": true });
        assert_eq!(
            group_tuple(&["$.state".into(), "$.meta.n".into()], &data),
            Some(vec!["CA".to_string(), "42".to_string()]),
            "strings bare, numbers rendered"
        );
        // Missing or non-scalar (bool) group fields exclude the row entirely.
        assert_eq!(group_tuple(&["$.nope".into()], &data), None);
        assert_eq!(group_tuple(&["$.flag".into()], &data), None);
        // Joined keys escape the separator so tuples can't collide.
        assert_eq!(group_row_key(&["CA".into(), "42".into()]), "CA|42");
        assert_ne!(
            group_row_key(&["a|b".into(), "c".into()]),
            group_row_key(&["a".into(), "b|c".into()])
        );
        assert_eq!(group_field_name("$.meta.state"), "state");
    }

    #[test]
    fn diff_compares_arrays_and_scalars_wholesale() {
        let diff = diff_values(&json!([1, 2]), &json!([1, 3]));
        assert_eq!(diff["$"], json!({ "from": [1, 2], "to": [1, 3] }));
        let same = diff_values(&json!("x"), &json!("x"));
        assert_eq!(same, json!({}));
    }
}
