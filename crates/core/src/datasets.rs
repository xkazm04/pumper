//! Persistent, queryable dataset store with change detection. Apps upsert typed
//! records keyed by a stable id; the store hashes each value and reports whether
//! it is new, changed, or unchanged versus the last run. This is the substrate
//! for both dedup (skip records already seen) and monitoring (act only on
//! diffs), turning one-off scrapes into datasets that accrue over time.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

use crate::retention::ArtifactRef;
use crate::store_instrument::{StoreInstrument, StoreOp};
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

/// How far down the trust ladder a label sits. Higher = less stood behind.
///
/// An UNRECOGNIZED label ranks below every known one on purpose: a value this
/// build does not understand must never be treated as at-least-as-trustworthy
/// as `stable`, because the only way that mistake surfaces is as a laundered
/// row nobody audits.
fn trust_rank(label: &str) -> u8 {
    match label {
        TRUST_STABLE => 0,
        "provisional" => 1,
        "quarantined" => 2,
        _ => 3,
    }
}

/// The weakest trust across a set of inputs — the stamp a value *derived* from
/// all of them may carry. `None` (in or out) means `stable`, matching the
/// column's NULL semantics and [`crate::resilience::SourceState::trust`].
///
/// Trust does not survive a join by majority vote: a derived row is only as
/// stood-behind as the least stood-behind row that fed it, so one `provisional`
/// input makes the whole output `provisional`. Deriving a `stable`-looking row
/// out of a quarantined one is laundering, and this is the one function that
/// decides it.
pub fn weakest_trust<'a>(labels: impl IntoIterator<Item = Option<&'a str>>) -> Option<String> {
    let weakest = labels
        .into_iter()
        .map(|l| trust_label(l))
        .max_by_key(|l| trust_rank(l))?;
    (weakest != TRUST_STABLE).then_some(weakest)
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

/// Appends `AND <TRUST_PREDICATE>` to a dynamically-built query, binding the
/// filter value into each of its `?T` slots. A no-op when `trust` is `None`.
///
/// Splits the ONE predicate constant rather than restating it: a hand-written
/// second copy for the QueryBuilder paths is exactly the divergence
/// [`TRUST_PREDICATE`]'s doc warns about — a consumer that believes it filtered
/// and did not.
fn push_trust_filter(qb: &mut sqlx::QueryBuilder<'_, sqlx::Sqlite>, trust: Option<&str>) {
    let Some(t) = trust else { return };
    let mut parts = TRUST_PREDICATE.split("?T");
    qb.push(" AND ");
    qb.push(parts.next().unwrap_or(""));
    for part in parts {
        qb.push_bind(t.to_string());
        qb.push(part);
    }
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
    /// Derivation stamp of the write that produced this revision. Every field
    /// is honest-Null: absent means "unknown", never a fabricated value (M12).
    #[serde(flatten)]
    pub provenance: Provenance,
}

/// Derivation stamp on a revision: where the value came from, mechanically.
/// Every field is optional and `None` means UNKNOWN — a write path stamps only
/// what it truly knows (the migration-0030 twin of the `trust` NULL-means-
/// stable choice, so pre-existing revisions need no backfill and no field is
/// ever invented).
#[derive(Debug, Clone, Default, Serialize)]
pub struct Provenance {
    /// Producing job (uuid as text) — carries schedule/trigger lineage via the
    /// jobs table.
    pub job_id: Option<String>,
    /// URL the record's content was fetched from.
    pub source_url: Option<String>,
    /// sha256 (hex) of the archived source body on disk.
    pub artifact_sha: Option<String>,
    /// sha256 (hex) of the canonical RuleSet JSON that extracted the value —
    /// see [`rules_hash`]; replayable via the `rules_versions` registry.
    pub rules_hash: Option<String>,
}

impl Provenance {
    /// True when nothing is known — the stamp of a legacy or anonymous write.
    pub fn is_empty(&self) -> bool {
        self.job_id.is_none()
            && self.source_url.is_none()
            && self.artifact_sha.is_none()
            && self.rules_hash.is_none()
    }

    /// True when the revision can be re-derived: the archived body AND the
    /// exact ruleset are both pinned. Anything less is not reproducible and
    /// must be refused, not approximated.
    pub fn replayable(&self) -> bool {
        self.artifact_sha.is_some() && self.rules_hash.is_some()
    }
}

/// A revision that claims to be reproducible, and where it says its archived
/// body lives. Produced by [`Datasets::replayable_revisions`] for the store
/// integrity report; pairing it with the filesystem is what turns a claim into
/// a verified one (or a named finding).
#[derive(Debug, Clone, Serialize)]
pub struct ReplayableRevision {
    pub app: String,
    pub dataset: String,
    pub key: String,
    pub revision: i64,
    #[serde(flatten)]
    pub reference: ArtifactRef,
}

/// Canonical content hash of a RuleSet (or any JSON value): sha256 over the
/// serde_json string form, whose object keys are BTreeMap-sorted — the same
/// canonicalization [`hash_value`] uses for record change detection, so two
/// semantically identical rulesets can never hash apart on key order.
pub fn rules_hash(rules: &Value) -> String {
    hash_value(rules)
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

/// What a dataset-wide hard delete would destroy — or, after execution, what it
/// actually destroyed.
///
/// `preview` is carried in the payload rather than only in the request that
/// asked for it: a summary that reads identically whether it counted or deleted
/// will eventually be pasted into a ticket as proof of what happened. Every
/// count carries its predicate — the app and dataset it counted, and the `as_of`
/// moment it counted at, because retention populations move while you read them.
#[derive(Debug, Clone, Serialize)]
pub struct DatasetDeletion {
    /// True when nothing was written and the counts are a forecast.
    pub preview: bool,
    pub app: String,
    pub dataset: String,
    /// Rows in `records` (tombstoned ones included — they are rows).
    pub records: u64,
    /// Rows in `record_revisions`: the full history, which the delete takes with it.
    pub revisions: u64,
    /// When the counts were taken, inside the deleting transaction.
    pub as_of: DateTime<Utc>,
}

/// Which mode [`Datasets::delete_dataset_mode`] runs in.
#[derive(Debug, Clone, Copy)]
pub enum DeleteMode {
    /// Count the population and roll back.
    Preview,
    /// Destroy the population, unless `expect_records` disagrees with what is
    /// actually there. `None` skips the yield guard — for an in-process caller
    /// acting on a dataset it owns, which has no operator and no preview.
    Execute { expect_records: Option<u64> },
}

/// The three outcomes of a dataset-wide delete, kept apart so a caller cannot
/// mistake one for another: it counted, it destroyed, or it refused.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum DeleteVerdict {
    /// Counted only. Nothing was written.
    Preview(DatasetDeletion),
    /// Committed. The counts are what was actually removed.
    Deleted(DatasetDeletion),
    /// Refused: the population moved between the preview and this call, so the
    /// operator would be destroying something other than what they consented
    /// to. Nothing was written; `found` is a fresh preview to re-read.
    YieldChanged {
        expected: u64,
        found: DatasetDeletion,
    },
}

impl DeleteVerdict {
    /// Whether this verdict's transaction may commit. Only [`Self::Deleted`]
    /// wrote anything; a preview and a refusal must both roll back, so that no
    /// trail entry ever claims a deletion that did not happen.
    pub fn wrote(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }

    /// The counts, whichever arm this is.
    pub fn counts(&self) -> &DatasetDeletion {
        match self {
            Self::Preview(d) | Self::Deleted(d) => d,
            Self::YieldChanged { found, .. } => found,
        }
    }
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

/// Authorization to run full-snapshot removal detection
/// ([`Datasets::detect_removed`]).
///
/// Removal detection tombstones every live key absent from a snapshot, which is
/// the single most destructive thing an ingest can do: a half-broken run
/// produces a short-but-nonempty batch and the whole tail of the dataset
/// disappears. The store's own `present.is_empty()` guard only covers a *fully*
/// empty snapshot — a partial batch is precisely the case it misses — so the
/// real protection is the source's health state.
///
/// That protection used to live one layer **above** the store, in
/// `AppContext::sync_many_with_provenance`. Any caller that hand-rolled
/// `upsert_many` + `detect_removed` bypassed it silently, and one did (the peer
/// app, reconstructing a fake "present" set from the live keys). Requiring this
/// token turns the check from a convention into a precondition: there is no way
/// to reach removal detection without having asked the health state, because
/// there is no other way to obtain the token.
///
/// It carries no data — its whole value is that it cannot be forged.
#[derive(Debug, Clone, Copy)]
pub struct RemovalGuard {
    _sealed: (),
}

impl RemovalGuard {
    /// The guarded seam. `Some` only when the source's health state permits
    /// removals; `None` means this run must be downgraded to a plain upsert and
    /// leave every existing record — live or tombstoned — exactly as it is.
    ///
    /// What "degrading" means is [`SourceState::suppresses_removals`] and is
    /// unchanged; this only moves *where* the answer is enforced.
    pub fn for_source_state(state: crate::resilience::SourceState) -> Option<Self> {
        (!state.suppresses_removals()).then_some(Self { _sealed: () })
    }

    /// The store's own materialized-view path: the capped search result set IS
    /// the complete snapshot of the view, and there is no external source whose
    /// health could be degrading. Crate-private on purpose — nothing outside
    /// the store may mint a guard without asking a health state.
    pub(crate) fn for_self_derived_snapshot() -> Self {
        Self { _sealed: () }
    }
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
    /// The store's self-instrument, so dataset writes land in the SAME rings as
    /// the job queue's — keyed by table, which is what makes "the `records`
    /// table is big AND its writes are degrading" one finding instead of two
    /// unrelated numbers.
    ///
    /// Defaults to a private instrument so `Datasets::new` stays a pure
    /// constructor; the server shares `Storage`'s via
    /// [`Datasets::with_instrument`]. A `Datasets` nobody wired is measured
    /// into a ring nobody reads — wasteful, never wrong.
    instrument: Arc<StoreInstrument>,
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

/// Bound parameters one generated statement may carry.
///
/// SQLite's `SQLITE_MAX_VARIABLE_NUMBER` is 32766 since 3.32 but **999** on
/// anything older, and the limit is a compile-time property of whatever libsqlite
/// this binary links against — not something the query builder can discover.
/// 900 is under the conservative bound on every build, so a batched statement
/// can never fail with `too many SQL variables` on someone else's SQLite.
const MAX_BIND_PARAMS: usize = 900;

/// How many rows a multi-row statement with `cols` bound columns may carry
/// without crossing [`MAX_BIND_PARAMS`]. `fixed` reserves the parameters bound
/// outside the row list (e.g. the `app`/`dataset`/`last_seen` of an IN-list
/// UPDATE). Always at least 1, so a wide row still makes progress.
fn rows_per_statement(cols: usize, fixed: usize) -> usize {
    (MAX_BIND_PARAMS.saturating_sub(fixed) / cols.max(1)).max(1)
}

impl Datasets {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            derived_max_depth: DERIVED_MAX_DEPTH_DEFAULT,
            max_group_scan: DERIVED_MAX_GROUP_SCAN_DEFAULT,
            instrument: Arc::new(StoreInstrument::new()),
        }
    }

    /// Shares the store's self-instrument, so this handle's writes are measured
    /// into the same rings `/metrics` and the doctor report render.
    pub fn with_instrument(mut self, instrument: Arc<StoreInstrument>) -> Self {
        self.instrument = instrument;
        self
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
        self.upsert_stamped(app, dataset, key, value, trust, None)
            .await
    }

    /// [`upsert_trusted`](Self::upsert_trusted) additionally stamping the
    /// revision with a [`Provenance`] (M12). `None` (and every `None` field)
    /// writes `NULL` = unknown — never fabricate a stamp.
    pub async fn upsert_stamped(
        &self,
        app: &str,
        dataset: &str,
        key: &str,
        value: &Value,
        trust: Option<&str>,
        prov: Option<&Provenance>,
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
        // Measured: the acquisition and the transaction are timed under their
        // own phases, so a writer parked on the pool never reads as a slow
        // statement (disjoint remedies — pool sizing versus the write itself).
        self.instrument
            .metered(&self.pool, StoreOp::DatasetWrite, |mut conn| async move {
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
                    prov,
                )
                .await;
                match result {
                    Ok(kind) => {
                        sqlx::query("COMMIT").execute(&mut *conn).await?;
                        // One record considered; `Unchanged` still cost a read
                        // and the write lock, so it counts as touched.
                        Ok((kind, 1))
                    }
                    Err(e) => {
                        let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                        Err(e)
                    }
                }
            })
            .await
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
        prov: Option<&Provenance>,
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
                    prov,
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
                    prov,
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
        prov: Option<&Provenance>,
    ) -> Result<()>
    where
        E: sqlx::SqliteExecutor<'e>,
    {
        // A `None` stamp binds four NULLs — identical to a `Provenance` whose
        // fields are all `None`, so "no stamp" and "unknown stamp" cannot drift.
        let p = prov.cloned().unwrap_or_default();
        sqlx::query(
            "INSERT INTO record_revisions (app, dataset, key, revision, change, data, diff, created_at, trust, \
                                           job_id, source_url, artifact_sha, rules_hash) \
             VALUES (?1, ?2, ?3, \
                     (SELECT COALESCE(MAX(revision), 0) + 1 FROM record_revisions \
                      WHERE app = ?1 AND dataset = ?2 AND key = ?3), \
                     ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .bind(change)
        .bind(data.map(Value::to_string))
        .bind(diff.map(Value::to_string))
        .bind(ts(when))
        .bind(trust)
        .bind(p.job_id)
        .bind(p.source_url)
        .bind(p.artifact_sha)
        .bind(p.rules_hash)
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
            "SELECT app, dataset, key, revision, change, data, diff, created_at, trust, \
                    job_id, source_url, artifact_sha, rules_hash \
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
            "SELECT app, dataset, key, revision, change, data, diff, created_at, trust, \
                    job_id, source_url, artifact_sha, rules_hash \
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
    /// at the newest.
    ///
    /// Ordered `(created_at DESC, revision DESC)` — matching the keyset
    /// predicate's leading column — not bare `revision DESC`. Revision numbers
    /// are per-key monotonic *by write order*, but `created_at` is a wall-clock
    /// stamp: an import that backdates timestamps, or plain clock skew across a
    /// batch, can write a later revision with an earlier `created_at`. With the
    /// old `ORDER BY revision DESC` a page boundary was cut by revision while
    /// the predicate excluded rows by `created_at` first — a skewed row could
    /// fall on the wrong side of the cut and be skipped or repeated across
    /// pages. Leading both the ORDER BY and the predicate on `created_at` keeps
    /// them in lockstep; `revision` still breaks ties within one `created_at`
    /// (a whole upsert-chunk shares one stamp — see `docs/features/datasets.md`
    /// § Conventions) and remains a unique, stable tiebreak within the
    /// (app, dataset, key). The cursor format (`created_at|revision`) is
    /// unchanged, so this is a pure ordering fix, not a cursor migration.
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
            "SELECT app, dataset, key, revision, change, data, diff, created_at, trust, \
                    job_id, source_url, artifact_sha, rules_hash \
             FROM record_revisions WHERE app = ?1 AND dataset = ?2 AND key = ?3 \
             AND (?4 IS NULL OR created_at < ?4 OR (created_at = ?4 AND revision < ?5)) \
             ORDER BY created_at DESC, revision DESC LIMIT ?6",
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

    /// Test-only: overwrites one revision's `created_at`, so a test can
    /// construct clock-skewed history — a later revision stamped earlier than
    /// a prior one (an import backdate, or drifted clocks across a batch) —
    /// deterministically, without waiting on real time. Compiled only behind
    /// `test-support` (never in a normal build); see
    /// `history_page_survives_clock_skew_without_skip_or_repeat`.
    #[cfg(feature = "test-support")]
    pub async fn set_revision_created_at_for_test(
        &self,
        app: &str,
        dataset: &str,
        key: &str,
        revision: i64,
        created_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE record_revisions SET created_at = ?1 \
             WHERE app = ?2 AND dataset = ?3 AND key = ?4 AND revision = ?5",
        )
        .bind(ts(created_at))
        .bind(app)
        .bind(dataset)
        .bind(key)
        .bind(revision)
        .execute(&self.pool)
        .await?;
        Ok(())
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
            "SELECT rowid AS rowid, app, dataset, key, revision, change, data, diff, created_at, trust, \
                    job_id, source_url, artifact_sha, rules_hash \
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
    ///
    /// Requires a [`RemovalGuard`], which can only be obtained by asking the
    /// source's health state — see that type for why the check is a parameter
    /// rather than a convention one layer up. A caller that already knows
    /// exactly which records disappeared wants
    /// [`tombstone_keys`](Self::tombstone_keys) instead: that is removal by
    /// name, not inference from a snapshot, and needs no guard.
    pub async fn detect_removed(
        &self,
        app: &str,
        dataset: &str,
        present: &[String],
        _guard: RemovalGuard,
    ) -> Result<Vec<String>> {
        // Defence in depth behind the health guard: an empty snapshot almost
        // always means the scrape failed, not that the entire dataset genuinely
        // disappeared. Refuse to tombstone everything — callers that legitimately
        // empty a dataset should delete explicitly.
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
        self.apply_tombstones(app, dataset, to_remove).await
    }

    /// Tombstones exactly the named keys that are currently live — the same two
    /// writes `detect_removed` makes (`removed_at` + a `removed` revision), and
    /// therefore the same change-feed / watch / trigger signal. Returns the keys
    /// actually tombstoned, in the order given; unknown and already-tombstoned
    /// keys are skipped.
    ///
    /// Removal by **name**, not by inference. A caller here already holds the
    /// per-record removal fact (a peer feed's `removed` revisions), so there is
    /// no short snapshot to misread and nothing for a [`RemovalGuard`] to
    /// protect against — which is exactly why this exists: without it, such a
    /// caller reconstructs a fake "present" set out of the live keys and drives
    /// `detect_removed` with it, re-entering the inference path it never needed
    /// and bypassing the health check on the way.
    pub async fn tombstone_keys(
        &self,
        app: &str,
        dataset: &str,
        keys: &[String],
    ) -> Result<Vec<String>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut live: std::collections::HashSet<String> = std::collections::HashSet::new();
        let unique: Vec<&str> = {
            let mut seen = std::collections::HashSet::new();
            keys.iter()
                .map(String::as_str)
                .filter(|k| seen.insert(*k))
                .collect()
        };
        for slice in unique.chunks(rows_per_statement(1, 2)) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT key FROM records WHERE removed_at IS NULL AND app = ",
            );
            qb.push_bind(app);
            qb.push(" AND dataset = ");
            qb.push_bind(dataset);
            qb.push(" AND key IN (");
            push_key_list(&mut qb, slice);
            qb.push(")");
            let found: Vec<(String,)> = qb.build_query_as().fetch_all(&self.pool).await?;
            live.extend(found.into_iter().map(|(k,)| k));
        }
        let to_remove: Vec<String> = unique
            .into_iter()
            .filter(|k| live.contains(*k))
            .map(str::to_string)
            .collect();
        self.apply_tombstones(app, dataset, to_remove).await
    }

    /// The shared write half of both removal paths: tombstone every key in
    /// `to_remove` and append its `removed` revision.
    ///
    /// Two properties, both learned the hard way:
    ///   (1) Atomicity — the `UPDATE removed_at` and its `removed` revision run
    ///       in ONE transaction, so a crash between them can't tombstone a
    ///       record with no revision. That was a permanent signal loss: the next
    ///       sync sees `removed_at` already set and the key still absent, so it
    ///       never revisits the key and the change feed / watches / dataset
    ///       triggers never fire for that removal. `upsert` was hardened for
    ///       exactly this reason; the removal path writes the same two rows and
    ///       had been missed.
    ///   (2) Cost — chunked commits instead of 2 write transactions per key
    ///       (a 2k-key removal was 4k commits).
    async fn apply_tombstones(
        &self,
        app: &str,
        dataset: &str,
        to_remove: Vec<String>,
    ) -> Result<Vec<String>> {
        if to_remove.is_empty() {
            return Ok(Vec::new());
        }
        let now = Utc::now();
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

    /// Materializes a saved search's current result set into a dataset (M13
    /// "queries as datasets"): upserts one record per hit — key = the search doc
    /// id (globally unique `<app>:<dataset>:<key>`), value =
    /// [`SearchHit::materialize_value`](crate::SearchHit::materialize_value) —
    /// then tombstones previously-live view records absent from the result set
    /// via [`detect_removed`](Self::detect_removed). The capped result set IS
    /// the full snapshot of the view, so falling out of the results is the
    /// removal signal. `cap` bounds both the writes and the removal scan
    /// (`[search] max_materialize_results`); hits past it are dropped, never
    /// silently widened. An EMPTY result set upserts nothing and — per the
    /// `detect_removed` guard — tombstones nothing: a query gone quiet (or an
    /// index wipe) must not erase the view.
    ///
    /// Returns the upsert summary and the removed keys.
    pub async fn materialize_search_hits(
        &self,
        app: &str,
        dataset: &str,
        hits: &[crate::SearchHit],
        cap: usize,
    ) -> Result<(UpsertSummary, Vec<String>)> {
        let hits = &hits[..hits.len().min(cap.max(1))];
        let items: Vec<(String, Value)> = hits
            .iter()
            .map(|h| (h.id.clone(), h.materialize_value()))
            .collect();
        let summary = self.upsert_many(app, dataset, &items).await?;
        let present: Vec<String> = items.into_iter().map(|(k, _)| k).collect();
        let removed = self
            .detect_removed(
                app,
                dataset,
                &present,
                RemovalGuard::for_self_derived_snapshot(),
            )
            .await?;
        Ok((summary, removed))
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
            &mut *conn, app, dataset, key, "removed", None, None, now, None, None,
        )
        .await?;
        Ok(())
    }

    /// Upserts many records, returning a summary of new/changed/unchanged.
    ///
    /// This is the most-executed write path in the product (every ingest run
    /// upserts its whole listing), and it is **set-shaped**, not row-shaped:
    ///
    /// - Records are committed in chunks of `UPSERT_CHUNK` on a single held
    ///   connection, so a 5k-record batch is ~10 commits and ~10 write-lock
    ///   acquisitions rather than 5k of each.
    /// - Within a chunk the statement count is bounded by the *bind-parameter
    ///   limit*, not by the record count: two batched reads (current hashes,
    ///   next revision numbers), one multi-row upsert for the records whose
    ///   content moved, one IN-list `UPDATE` for the re-confirmed ones, and one
    ///   multi-row insert for the revisions. That is ~20 statements per 500-row
    ///   chunk instead of ~1,500 — and since the whole chunk runs under
    ///   `BEGIN IMMEDIATE`, statements-per-chunk *is* write-lock hold time, the
    ///   mechanism behind cross-app write stalls during a large sync.
    ///
    /// Verdicts are decided in memory by [`plan_chunk`], which reproduces the
    /// per-record read→write→revision sequence exactly — including a key that
    /// appears twice inside one batch.
    ///
    /// A failure rolls back its own chunk and propagates; chunks committed before
    /// it stay committed (the same partial-progress-then-error shape the
    /// per-record loop had).
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
        self.upsert_many_at_depth(app, dataset, items, trust, None, &DerivedPaths::NONE, 0)
            .await
    }

    /// [`upsert_many_trusted`](Self::upsert_many_trusted) stamping every
    /// revision this batch appends with ONE shared [`Provenance`] (M12). The
    /// stamp is batch-level by design: a listing sync knows its producing
    /// job (and possibly the ruleset), but not a distinct source URL per
    /// record — per-record facts a batch writer doesn't know stay `None`
    /// (honest-Null), and a writer that does know them per record uses
    /// [`upsert_stamped`](Self::upsert_stamped) row by row.
    pub async fn upsert_many_stamped(
        &self,
        app: &str,
        dataset: &str,
        items: &[(String, Value)],
        trust: Option<&str>,
        prov: Option<&Provenance>,
    ) -> Result<UpsertSummary> {
        self.upsert_many_at_depth(app, dataset, items, trust, prov, &DerivedPaths::NONE, 0)
            .await
    }

    /// [`upsert_many_stamped`](Self::upsert_many_stamped) declaring which record
    /// paths are **derived** — see [`DerivedPaths`]. Opt-in per write; passing
    /// [`DerivedPaths::NONE`] is byte-for-byte the plain batch upsert.
    pub async fn upsert_many_derived(
        &self,
        app: &str,
        dataset: &str,
        items: &[(String, Value)],
        trust: Option<&str>,
        prov: Option<&Provenance>,
        derived: &DerivedPaths,
    ) -> Result<UpsertSummary> {
        self.upsert_many_at_depth(app, dataset, items, trust, prov, derived, 0)
            .await
    }

    /// [`upsert_many_trusted`] carrying the derived-chain depth: 0 for a source
    /// ingest, +1 per derived hop. Boxed because the derived hook recurses
    /// (derived writes are themselves upserts that can match further specs);
    /// the recursion is bounded by `derived_max_depth`.
    #[allow(clippy::too_many_arguments)]
    fn upsert_many_at_depth<'a>(
        &'a self,
        app: &'a str,
        dataset: &'a str,
        items: &'a [(String, Value)],
        trust: Option<&'a str>,
        prov: Option<&'a Provenance>,
        derived: &'a DerivedPaths,
        depth: u32,
    ) -> futures::future::BoxFuture<'a, Result<UpsertSummary>> {
        Box::pin(async move {
            let summary = self
                .upsert_many_inner(app, dataset, items, trust, prov, derived)
                .await?;
            // Fresh keys flow through matching enabled derived specs in the
            // same flow. Fail-open: a broken spec must never fail the source
            // ingest, so derived errors are logged, not propagated.
            if summary.new.len() + summary.changed.len() > 0 {
                self.apply_derived(app, dataset, items, &summary, depth, trust, prov)
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
        prov: Option<&Provenance>,
        derived: &DerivedPaths,
    ) -> Result<UpsertSummary> {
        let mut summary = UpsertSummary::default();
        if items.is_empty() {
            return Ok(summary);
        }
        // Fingerprint the whole batch BEFORE taking any write lock. Hashing and
        // SimHashing are pure CPU; doing them inside `BEGIN IMMEDIATE` — as the
        // per-record loop did — billed every other app's writer for this batch's
        // CPU, and on a large sync that dominates the lock hold time.
        let prints = fingerprint_batch(items, derived);
        self.instrument
            .metered(&self.pool, StoreOp::DatasetWrite, |mut conn| async move {
                for (chunk, prints) in items.chunks(UPSERT_CHUNK).zip(prints.chunks(UPSERT_CHUNK)) {
                    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
                    // Accumulate this chunk separately so a mid-chunk failure that rolls
                    // back doesn't leave the returned summary claiming uncommitted rows.
                    let chunk_result = Self::upsert_chunk_in_tx(
                        &mut conn, app, dataset, chunk, prints, trust, prov,
                    )
                    .await;
                    match chunk_result {
                        Ok(chunk_summary) => {
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
                let touched = items.len() as u64;
                Ok((summary, touched))
            })
            .await
    }

    /// One chunk of a batch upsert, inside the caller's write transaction:
    /// **two** batched reads, then **set-shaped** writes — instead of the
    /// SELECT + INSERT/UPDATE + revision-INSERT triple this used to issue per
    /// record (~1,500 statements per 500-row chunk, each one holding the
    /// DB-wide write lock for the whole chunk).
    ///
    /// Read the whole chunk's current state once, decide every verdict in
    /// memory ([`plan_chunk`]), then emit the writes as multi-row statements.
    /// The reads happen inside the same `BEGIN IMMEDIATE` as the writes, so the
    /// read→write→revision sequence is exactly as atomic as it was per record:
    /// a concurrent same-key writer still waits for the COMMIT.
    #[allow(clippy::too_many_arguments)]
    async fn upsert_chunk_in_tx(
        conn: &mut sqlx::SqliteConnection,
        app: &str,
        dataset: &str,
        chunk: &[(String, Value)],
        prints: &[Fingerprint],
        trust: Option<&str>,
        prov: Option<&Provenance>,
    ) -> Result<UpsertSummary> {
        // One timestamp for the chunk. Per-record `Utc::now()` bought nothing —
        // the stored format is microsecond RFC 3339 and a chunk lands inside one
        // transaction anyway — and every ordered read already tiebreaks a shared
        // stamp (`list_page` by key, `changes_page` by rowid).
        let now = Utc::now();
        let keys: Vec<&str> = {
            let mut seen = std::collections::HashSet::new();
            chunk
                .iter()
                .map(|(k, _)| k.as_str())
                .filter(|k| seen.insert(*k))
                .collect()
        };
        let mut state = Self::read_key_states(conn, app, dataset, &keys).await?;
        let mut next_revision = Self::read_next_revisions(conn, app, dataset, &keys).await?;
        let plans = plan_chunk(chunk, prints, &mut state, &mut next_revision);
        Self::write_chunk(conn, app, dataset, chunk, prints, &plans, now, trust, prov).await?;
        Ok(summarize_chunk(chunk, &plans))
    }

    /// Current store state of every key in the chunk, read in one statement per
    /// [`MAX_BIND_PARAMS`]-sized slice of the IN-list. Replaces the per-record
    /// `SELECT hash, data, removed_at`.
    async fn read_key_states(
        conn: &mut sqlx::SqliteConnection,
        app: &str,
        dataset: &str,
        keys: &[&str],
    ) -> Result<std::collections::HashMap<String, KeyState>> {
        let mut out = std::collections::HashMap::with_capacity(keys.len());
        for slice in keys.chunks(rows_per_statement(1, 2)) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT key, hash, data, removed_at FROM records WHERE app = ",
            );
            qb.push_bind(app);
            qb.push(" AND dataset = ");
            qb.push_bind(dataset);
            qb.push(" AND key IN (");
            push_key_list(&mut qb, slice);
            qb.push(")");
            let rows: Vec<(String, String, String, Option<String>)> =
                qb.build_query_as().fetch_all(&mut *conn).await?;
            for (key, hash, data, removed_at) in rows {
                out.insert(
                    key,
                    KeyState {
                        hash,
                        data,
                        removed: removed_at.is_some(),
                    },
                );
            }
        }
        Ok(out)
    }

    /// The revision number each key's next revision must take, read in one
    /// statement per IN-list slice. Replaces the per-row
    /// `(SELECT COALESCE(MAX(revision), 0) + 1 …)` subquery — same value, same
    /// transaction, so the same serialization guarantee holds. Keys with no
    /// history are absent and start at 1.
    async fn read_next_revisions(
        conn: &mut sqlx::SqliteConnection,
        app: &str,
        dataset: &str,
        keys: &[&str],
    ) -> Result<std::collections::HashMap<String, i64>> {
        let mut out = std::collections::HashMap::new();
        for slice in keys.chunks(rows_per_statement(1, 2)) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT key, MAX(revision) FROM record_revisions WHERE app = ",
            );
            qb.push_bind(app);
            qb.push(" AND dataset = ");
            qb.push_bind(dataset);
            qb.push(" AND key IN (");
            push_key_list(&mut qb, slice);
            qb.push(") GROUP BY key");
            let rows: Vec<(String, i64)> = qb.build_query_as().fetch_all(&mut *conn).await?;
            for (key, max_revision) in rows {
                out.insert(key, max_revision + 1);
            }
        }
        Ok(out)
    }

    /// Emits a planned chunk as multi-row statements: one upsert covering every
    /// key whose content moved, one IN-list `UPDATE` for the keys that only need
    /// their `last_seen`/`trust` refreshed, and one multi-row insert for the
    /// revisions.
    #[allow(clippy::too_many_arguments)]
    async fn write_chunk(
        conn: &mut sqlx::SqliteConnection,
        app: &str,
        dataset: &str,
        chunk: &[(String, Value)],
        prints: &[Fingerprint],
        plans: &[PlannedWrite],
        now: DateTime<Utc>,
        trust: Option<&str>,
        prov: Option<&Provenance>,
    ) -> Result<()> {
        let now_s = ts(now);
        let (content, touched_only) = collapse_record_writes(chunk, plans);

        // Content writes. `ON CONFLICT DO UPDATE` covers New and Changed in one
        // statement shape: a new key inserts (first_seen = now), an existing one
        // updates and is revived (`removed_at = NULL`) — exactly what the two
        // per-record statements did, and `first_seen` is left alone by the update
        // branch so a revived record keeps its original first sighting.
        for slice in content.chunks(rows_per_statement(10, 0)) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO records (app, dataset, key, hash, data, simhash, \
                 first_seen, last_seen, updated_at, trust) ",
            );
            qb.push_values(slice.iter(), |mut b, (idx, _plan)| {
                let print = &prints[*idx];
                b.push_bind(app)
                    .push_bind(dataset)
                    .push_bind(chunk[*idx].0.as_str())
                    .push_bind(print.hash.as_str())
                    .push_bind(print.json.as_str())
                    .push_bind(print.sim)
                    .push_bind(now_s.as_str())
                    .push_bind(now_s.as_str())
                    .push_bind(now_s.as_str())
                    .push_bind(trust);
            });
            qb.push(
                " ON CONFLICT(app, dataset, key) DO UPDATE SET \
                 hash = excluded.hash, data = excluded.data, simhash = excluded.simhash, \
                 last_seen = excluded.last_seen, updated_at = excluded.updated_at, \
                 removed_at = NULL, trust = excluded.trust",
            );
            qb.build().execute(&mut *conn).await?;
        }

        // Unchanged content, but trust still moves: a source that entered
        // `degraded` since the last run is no longer stood behind, even for the
        // records it re-confirmed. Leaving a stale `stable` stamp here would let
        // a filtered read serve them as trusted.
        for slice in touched_only.chunks(rows_per_statement(1, 4)) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("UPDATE records SET last_seen = ");
            qb.push_bind(now_s.as_str());
            qb.push(", trust = ");
            qb.push_bind(trust);
            qb.push(" WHERE app = ");
            qb.push_bind(app);
            qb.push(" AND dataset = ");
            qb.push_bind(dataset);
            qb.push(" AND key IN (");
            push_key_list(&mut qb, slice);
            qb.push(")");
            qb.build().execute(&mut *conn).await?;
        }

        // Revisions, in item order — one per New/Changed *occurrence*, so a key
        // written twice in one batch keeps both links of its chain.
        let p = prov.cloned().unwrap_or_default();
        let revisions: Vec<&PlannedWrite> = plans
            .iter()
            .filter(|p| p.kind != ChangeKind::Unchanged)
            .collect();
        for slice in revisions.chunks(rows_per_statement(13, 0)) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO record_revisions (app, dataset, key, revision, change, data, diff, \
                 created_at, trust, job_id, source_url, artifact_sha, rules_hash) ",
            );
            qb.push_values(slice.iter(), |mut b, plan| {
                b.push_bind(app)
                    .push_bind(dataset)
                    .push_bind(chunk[plan.idx].0.as_str())
                    .push_bind(plan.revision)
                    .push_bind(if plan.kind == ChangeKind::New {
                        "new"
                    } else {
                        "changed"
                    })
                    .push_bind(prints[plan.idx].json.as_str())
                    .push_bind(plan.diff.as_ref().map(Value::to_string))
                    .push_bind(now_s.as_str())
                    .push_bind(trust)
                    .push_bind(p.job_id.clone())
                    .push_bind(p.source_url.clone())
                    .push_bind(p.artifact_sha.clone())
                    .push_bind(p.rules_hash.clone());
            });
            qb.build().execute(&mut *conn).await?;
        }
        Ok(())
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
    ///
    /// The **unguarded** verb, for an app deleting a dataset it owns (the
    /// `_job` snapshot sweeps). The operator door is
    /// [`delete_dataset_mode`](Self::delete_dataset_mode), which is the same
    /// code with a preview mode and a yield guard in front of it; both share
    /// this function's transaction and predicate, so neither can drift from the
    /// other.
    pub async fn delete_dataset(&self, app: &str, dataset: &str) -> Result<u64> {
        match self
            .delete_dataset_mode(
                app,
                dataset,
                DeleteMode::Execute {
                    expect_records: None,
                },
            )
            .await?
        {
            DeleteVerdict::Deleted(done) => Ok(done.records),
            // Unreachable by construction: `expect_records: None` cannot refuse,
            // and `Execute` never previews. A panic here would be a library bug
            // in a delete path, so it degrades to a typed error instead.
            other => Err(crate::Error::App(format!(
                "delete_dataset: unguarded execute returned {other:?}"
            ))),
        }
    }

    /// The dataset-wide hard delete as a **mode**: count the population, then
    /// either report it (writing nothing) or destroy it (reporting what was
    /// actually destroyed).
    ///
    /// One function, one predicate, one transaction — because a preview that
    /// counts through a second implementation is a forecast of a different
    /// operation, and it passes review exactly when it has drifted
    /// (registry: data-retention/dry-run-preview, "same predicate, or it is a
    /// lie"). The count and the `DELETE` run inside the same `BEGIN IMMEDIATE`,
    /// so the guard below cannot be raced by a concurrent writer.
    ///
    /// - [`DeleteMode::Preview`] counts and rolls back. Nothing is written, and
    ///   the verdict says so in the payload rather than only in the request that
    ///   asked for it.
    /// - [`DeleteMode::Execute`] with `expect_records: Some(n)` refuses unless
    ///   the live record count is exactly `n` — the yield guard that turns a
    ///   preview from advice into a precondition. `None` skips the guard and is
    ///   reserved for in-process callers acting on their own datasets.
    ///
    /// Revision history is deleted with the records; the caller drops the search
    /// docs, and (at the HTTP door) exports the history first.
    pub async fn delete_dataset_mode(
        &self,
        app: &str,
        dataset: &str,
        mode: DeleteMode,
    ) -> Result<DeleteVerdict> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let outcome: Result<DeleteVerdict> = async {
            // The population, by the same predicate the DELETEs below use.
            let records: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM records WHERE app = ?1 AND dataset = ?2")
                    .bind(app)
                    .bind(dataset)
                    .fetch_one(&mut *conn)
                    .await?;
            let revisions: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM record_revisions WHERE app = ?1 AND dataset = ?2",
            )
            .bind(app)
            .bind(dataset)
            .fetch_one(&mut *conn)
            .await?;
            let found = DatasetDeletion {
                preview: true,
                app: app.to_string(),
                dataset: dataset.to_string(),
                records: records.max(0) as u64,
                revisions: revisions.max(0) as u64,
                as_of: Utc::now(),
            };
            let expect = match mode {
                DeleteMode::Preview => return Ok(DeleteVerdict::Preview(found)),
                DeleteMode::Execute { expect_records } => expect_records,
            };
            if let Some(expected) = expect {
                if expected != found.records {
                    return Ok(DeleteVerdict::YieldChanged { expected, found });
                }
            }
            let removed_records =
                sqlx::query("DELETE FROM records WHERE app = ?1 AND dataset = ?2")
                    .bind(app)
                    .bind(dataset)
                    .execute(&mut *conn)
                    .await?
                    .rows_affected();
            let removed_revisions =
                sqlx::query("DELETE FROM record_revisions WHERE app = ?1 AND dataset = ?2")
                    .bind(app)
                    .bind(dataset)
                    .execute(&mut *conn)
                    .await?
                    .rows_affected();
            // What it ACTUALLY destroyed, not the forecast — the execution is the
            // record of truth, so the numbers come from `rows_affected`.
            Ok(DeleteVerdict::Deleted(DatasetDeletion {
                preview: false,
                records: removed_records,
                revisions: removed_revisions,
                ..found
            }))
        }
        .await;
        match outcome {
            // A preview and a refusal both roll back: neither may leave a trace
            // claiming a deletion that did not happen.
            Ok(verdict) => {
                let sql = if verdict.wrote() {
                    "COMMIT"
                } else {
                    "ROLLBACK"
                };
                sqlx::query(sql).execute(&mut *conn).await?;
                Ok(verdict)
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    /// One keyset page of a whole dataset's revision history, oldest key first,
    /// for the export-before-delete at the HTTP delete door. Ordered
    /// `(key, revision)` — a total order over the table's own primary key, so a
    /// full walk cannot skip or repeat a row the way a `created_at` walk can
    /// under clock skew. `after` is the previous page's last `(key, revision)`.
    pub async fn dataset_revisions_page(
        &self,
        app: &str,
        dataset: &str,
        after: Option<(String, i64)>,
        limit: i64,
    ) -> Result<Vec<Revision>> {
        let (after_key, after_rev) = after
            .map(|(k, r)| (Some(k), Some(r)))
            .unwrap_or((None, None));
        let rows: Vec<RevisionRow> = sqlx::query_as(
            "SELECT app, dataset, key, revision, change, data, diff, created_at, trust, \
                    job_id, source_url, artifact_sha, rules_hash \
             FROM record_revisions WHERE app = ?1 AND dataset = ?2 \
             AND (?3 IS NULL OR key > ?3 OR (key = ?3 AND revision > ?4)) \
             ORDER BY key ASC, revision ASC LIMIT ?5",
        )
        .bind(app)
        .bind(dataset)
        .bind(after_key)
        .bind(after_rev)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Revision::try_from).collect()
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
    /// matches). Records with no textual content (simhash 0) are skipped.
    ///
    /// Candidates come from the shared banded index
    /// ([`BandedIndex`](crate::simhash::BandedIndex), the crawler's near-dup
    /// bucketing) and are then verified by exact Hamming, so the pair set is
    /// identical to the all-pairs scan this replaces — `MAX_DUP_PAIRS` capped
    /// input, not just output. The grants unified layer runs `link_duplicates`
    /// over the whole corpus every run, where the pairwise scan was quadratic.
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
        Ok(banded_duplicate_pairs(&rows, max_distance))
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
        self.list_filtered_trust(app, dataset, filters, after, limit, None)
            .await
    }

    /// [`list_filtered`](Self::list_filtered) additionally restricted to one
    /// trust level (`stable` | `provisional` | `quarantined`; `None` = every
    /// row). Uses the same [`TRUST_PREDICATE`] as the record list and the change
    /// feed, so `stable` keeps the `NULL`-means-stable equivalence.
    ///
    /// Needed by shared datasets that several sources write into — `grants/unified`
    /// is written by three apps, so a run gated to `provisional` by ITS source's
    /// health leaves provisional rows sitting next to stable ones in the dataset
    /// every consumer reads.
    pub async fn list_filtered_trust(
        &self,
        app: &str,
        dataset: &str,
        filters: &[JsonFilter],
        after: Option<(String, String)>,
        limit: i64,
        trust: Option<&str>,
    ) -> Result<Vec<Record>> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at, trust \
             FROM records WHERE removed_at IS NULL AND app = ",
        );
        qb.push_bind(app);
        qb.push(" AND dataset = ");
        qb.push_bind(dataset);

        push_json_filters(&mut qb, filters);
        push_trust_filter(&mut qb, trust);

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

    /// Unified keyset page for the `GET /datasets/{app}/{ds}` read surface —
    /// default, cursor, filtered, and export all route through this one
    /// function so they cannot disagree about what `trust=` or a tombstone
    /// means. `filters` may be empty (no predicate, matching plain `list_page`);
    /// `trust` is [`TRUST_PREDICATE`] as everywhere else; `include_removed`
    /// toggles whether tombstoned rows (`removed_at` set) are returned.
    ///
    /// Before this existed, the route layer had three call sites — the
    /// no-cursor path (`list`, no trust support, tombstones always included),
    /// the cursor path (`list_page`, trust supported, tombstones always
    /// included), and the filtered path (`list_filtered`, no trust support,
    /// tombstones always excluded) — that each answered "is this row live?"
    /// and "does trust apply?" differently depending on which query params a
    /// caller happened to pass. That is exactly the divergence
    /// [`TRUST_PREDICATE`]'s own doc warns against, just one query param over.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_records_view(
        &self,
        app: &str,
        dataset: &str,
        filters: &[JsonFilter],
        after: Option<(String, String)>,
        limit: i64,
        trust: Option<&str>,
        include_removed: bool,
    ) -> Result<Vec<Record>> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT key, data, first_seen, last_seen, updated_at, removed_at, trust \
             FROM records WHERE app = ",
        );
        qb.push_bind(app);
        qb.push(" AND dataset = ");
        qb.push_bind(dataset);
        if !include_removed {
            qb.push(" AND removed_at IS NULL");
        }

        push_json_filters(&mut qb, filters);
        push_trust_filter(&mut qb, trust);

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

    /// Distinct `(app, dataset)` pairs that have at least one **live** record —
    /// the "what is currently servable" view. Used by the watch registry
    /// (`GET /watches`) and the DataHub governance poll, both of which reason
    /// about datasets that still have something to serve.
    ///
    /// A dataset whose every record is tombstoned is deliberately absent. If you
    /// are cleaning up after such a dataset rather than serving it, you want
    /// [`list_all_datasets_including_removed`](Self::list_all_datasets_including_removed).
    pub async fn list_all_datasets(&self) -> Result<Vec<(String, String)>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT app, dataset FROM records WHERE removed_at IS NULL \
             ORDER BY app, dataset",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Distinct `(app, dataset)` pairs that have at least one record of any kind,
    /// tombstoned rows included — the set a full search rebuild must walk.
    ///
    /// The distinction is load-bearing, not cosmetic: `search-backfill --all`
    /// exists to purge documents whose records are gone, and a dataset that is
    /// *entirely* tombstoned is precisely the state that needs purging. Resolving
    /// its targets through [`list_all_datasets`](Self::list_all_datasets) made
    /// that dataset invisible to the one tool that could repair it, so its stale
    /// documents kept answering `/search` forever while the rebuild reported
    /// success. Matches the (unfiltered) shape of [`datasets`](Self::datasets),
    /// which is what the `--app` scope has always used.
    pub async fn list_all_datasets_including_removed(&self) -> Result<Vec<(String, String)>> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT DISTINCT app, dataset FROM records ORDER BY app, dataset")
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

    // ── provenance ───────────────────────────────────────────────────────────
    // M12 reproducible records: content-addressed RuleSet registry + the
    // per-key provenance summary behind `GET /provenance/{app}/{dataset}/{key}`.
    // The stamps themselves ride the upsert flow (`upsert_stamped` /
    // `upsert_many_stamped`) and come back on every `Revision`.

    /// Registers a RuleSet (any JSON value) in the content-addressed
    /// `rules_versions` registry and returns its canonical hash — the value to
    /// stamp as [`Provenance::rules_hash`]. Idempotent: the hash IS the
    /// identity, so re-registering the same rules is a no-op. Re-derivation
    /// replays the *registered* rules for a revision's hash, never the app's
    /// current config — rules evolve; the registry is what pins history.
    pub async fn register_rules(&self, rules: &Value) -> Result<String> {
        let hash = rules_hash(rules);
        sqlx::query(
            "INSERT OR IGNORE INTO rules_versions (hash, rules, created_at) VALUES (?1, ?2, ?3)",
        )
        .bind(&hash)
        .bind(rules.to_string())
        .bind(ts(Utc::now()))
        .execute(&self.pool)
        .await?;
        Ok(hash)
    }

    /// The registered RuleSet JSON for a hash, or `None` when that ruleset was
    /// never registered (its revisions are stamped but not replayable).
    pub async fn rules_by_hash(&self, hash: &str) -> Result<Option<Value>> {
        let raw: Option<String> =
            sqlx::query_scalar("SELECT rules FROM rules_versions WHERE hash = ?1")
                .bind(hash)
                .fetch_optional(&self.pool)
                .await?;
        raw.map(|s| {
            serde_json::from_str(&s).map_err(|e| {
                Error::parse_from(format!("stored rules for '{hash}' unparseable: {e}"), e)
            })
        })
        .transpose()
    }

    /// Stamp coverage of one record's revision chain, computed in SQL so the
    /// numbers cover the WHOLE chain even when the caller pages it:
    /// `(total, with job_id, replayable = artifact_sha AND rules_hash)`.
    pub async fn provenance_coverage(
        &self,
        app: &str,
        dataset: &str,
        key: &str,
    ) -> Result<(i64, i64, i64)> {
        let row: (i64, i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), \
                    COALESCE(SUM(job_id IS NOT NULL), 0), \
                    COALESCE(SUM(artifact_sha IS NOT NULL AND rules_hash IS NOT NULL), 0) \
             FROM record_revisions WHERE app = ?1 AND dataset = ?2 AND key = ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// Every archived body a live reader still addresses — the veto list
    /// artifact retention must respect (see [`crate::retention`]).
    ///
    /// **Two readers, two questions, two arms.** They are not the same question,
    /// and conflating them is what made this query wrong for the platform's
    /// highest-volume corpus:
    ///
    /// 1. **`rederive` (historical).** `POST /provenance/.../rederive` replays a
    ///    *revision* through the ruleset pinned in its stamp and verifies the
    ///    file against the stamped sha, so this arm needs the **snapshot**: where
    ///    the body was when that revision was written. It is gated on
    ///    `artifact_sha AND rules_hash` because those are exactly the conditions
    ///    under which rederive will accept the record — unchanged.
    /// 2. **`read_source_artifact` (current).** `AppContext::read_source_artifact`
    ///    resolves `<app>/<job_id>/<artifact_path>` from a **live record's own
    ///    data** and never consults a stamp at all. So this arm asks only "does a
    ///    live record still address this body", which also covers "where rederive
    ///    will look today, after a crawl revisit moved the body to a new job_id".
    ///
    /// **What changed and why (round 24).** Arm 2 used to require the key to
    /// *also* have a replayable revision. `rules_hash` means "a RuleSet made a
    /// provenance claim about this record" — a question about extraction, not
    /// about addressability. The crawl stamps neither half on `pages` and
    /// deliberately leaves `rules_hash` as `None` on `page_versions` ("unknown,
    /// never a fabricated pin"), and nothing but the crawl writes the crawl's
    /// keys — so **zero crawl bodies were pinnable, at any age, under any
    /// config**, while 11 `read_source_artifact` call sites across four apps read
    /// exactly that corpus. The data the pin needs was already present; only the
    /// gate excluded it.
    ///
    /// **This makes retention reclaim less.** Every body a live record addresses
    /// is now kept regardless of age. What stays reclaimable: bodies no live
    /// record points at — the superseded copies a crawl revisit abandons in an
    /// older job directory (the growth driver this module was written for),
    /// bodies of **tombstoned** records (`removed_at IS NOT NULL`, which no read
    /// surface returns, hence the filter on arm 2), bodies of records that have
    /// been deleted or pruned outright, and anything written without a record.
    /// Retention is narrowed here, not disabled; the counter-test
    /// `a_body_no_live_record_addresses_is_still_reclaimable` is what keeps that
    /// true.
    ///
    /// **Full scan of `record_revisions` + `records`.** On-demand only — the
    /// retention janitor and the read-only reports, never a request hot path.
    pub async fn pinned_artifact_refs(&self) -> Result<HashSet<ArtifactRef>> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT DISTINCT app, \
                    json_extract(data, '$.job_id'), \
                    json_extract(data, '$.artifact_path') \
             FROM record_revisions \
             WHERE artifact_sha IS NOT NULL AND rules_hash IS NOT NULL \
               AND json_extract(data, '$.job_id') IS NOT NULL \
               AND json_extract(data, '$.artifact_path') IS NOT NULL \
             UNION \
             SELECT DISTINCT app, \
                    json_extract(data, '$.job_id'), \
                    json_extract(data, '$.artifact_path') \
             FROM records \
             WHERE removed_at IS NULL \
               AND json_extract(data, '$.job_id') IS NOT NULL \
               AND json_extract(data, '$.artifact_path') IS NOT NULL \
               AND json_extract(data, '$.artifact_path') <> ''",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(app, job_id, name)| ArtifactRef { app, job_id, name })
            .collect())
    }

    // ── store integrity (read-only; `datasets doctor`) ───────────────────────
    // Every query below is a SELECT. Several are FULL SCANS of `record_revisions`
    // or `records` — the audit is an on-demand operator tool, never on a hot path
    // and never on the worker loop.

    /// Every replayable revision with the body location it claims, newest first.
    /// The doctor pairs this with the filesystem to find revisions whose stamped
    /// body is gone — a provenance claim the store can no longer honour.
    pub async fn replayable_revisions(&self, limit: i64) -> Result<Vec<ReplayableRevision>> {
        let rows: Vec<(String, String, String, i64, String, String)> = sqlx::query_as(
            "SELECT app, dataset, key, revision, \
                    json_extract(data, '$.job_id'), \
                    json_extract(data, '$.artifact_path') \
             FROM record_revisions \
             WHERE artifact_sha IS NOT NULL AND rules_hash IS NOT NULL \
               AND json_extract(data, '$.job_id') IS NOT NULL \
               AND json_extract(data, '$.artifact_path') IS NOT NULL \
             ORDER BY created_at DESC LIMIT ?1",
        )
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(app, dataset, key, revision, job_id, name)| ReplayableRevision {
                    reference: ArtifactRef {
                        app: app.clone(),
                        job_id,
                        name,
                    },
                    app,
                    dataset,
                    key,
                    revision,
                },
            )
            .collect())
    }

    /// Revisions stamped with exactly ONE of `artifact_sha` / `rules_hash`.
    ///
    /// Neither half alone is reproducible, so `rederive` refuses them — the write
    /// path recorded work it cannot cash in. Not the same thing as an unstamped
    /// legacy revision, which is honestly Null and claims nothing. Returns
    /// `(app, dataset, count)`.
    pub async fn half_stamped_revisions(&self) -> Result<Vec<(String, String, i64)>> {
        Ok(sqlx::query_as(
            "SELECT app, dataset, COUNT(*) FROM record_revisions \
             WHERE (artifact_sha IS NULL) <> (rules_hash IS NULL) \
             GROUP BY app, dataset ORDER BY COUNT(*) DESC",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// `rules_hash` values stamped on revisions but absent from the
    /// content-addressed `rules_versions` registry. Re-derivation refuses these
    /// with "stamped but never registered": the historical ruleset is gone, so
    /// replaying would mean using today's rules and calling it reproduction.
    /// Returns `(rules_hash, revisions affected)`.
    pub async fn unregistered_rules_hashes(&self) -> Result<Vec<(String, i64)>> {
        Ok(sqlx::query_as(
            "SELECT r.rules_hash, COUNT(*) FROM record_revisions r \
             WHERE r.rules_hash IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM rules_versions v WHERE v.hash = r.rules_hash) \
             GROUP BY r.rules_hash ORDER BY COUNT(*) DESC",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// Per-dataset stamp coverage over the WHOLE store:
    /// `(app, dataset, revisions, with_job_id, replayable)`. The dataset-wide
    /// twin of [`provenance_coverage`](Self::provenance_coverage), which answers
    /// the same question for one record.
    pub async fn provenance_coverage_by_dataset(
        &self,
    ) -> Result<Vec<(String, String, i64, i64, i64)>> {
        Ok(sqlx::query_as(
            "SELECT app, dataset, COUNT(*), \
                    COALESCE(SUM(job_id IS NOT NULL), 0), \
                    COALESCE(SUM(artifact_sha IS NOT NULL AND rules_hash IS NOT NULL), 0) \
             FROM record_revisions GROUP BY app, dataset ORDER BY app, dataset",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// Live records that HAVE textual content but carry no SimHash fingerprint,
    /// per dataset. `duplicate_pairs` skips `simhash = 0` rows as "no textual
    /// content", so a dataset full of them has a near-duplicate report that is
    /// quietly incomplete rather than empty. Remediation is the `reindex` binary,
    /// and every row counted here is one `reindex_simhashes` will actually
    /// rewrite — a record that genuinely hashes to 0 is excluded, because
    /// reindex skips unchanged rows and the finding could otherwise never clear
    /// (see [`doctor::simhash_zero_is_a_missing_fingerprint`]).
    ///
    /// [`doctor::simhash_zero_is_a_missing_fingerprint`]: crate::doctor::simhash_zero_is_a_missing_fingerprint
    pub async fn missing_simhash_counts(&self) -> Result<Vec<(String, String, i64)>> {
        // `simhash` is `INTEGER NOT NULL DEFAULT 0`, so `0` is the ONLY possible
        // un-fingerprinted marker — but it is also the honest hash of a record
        // with no textual leaves. Only the JSON can tell those apart, so the
        // rows are recomputed rather than counted in SQL. This reads `data` only
        // for rows already at 0, which is empty on a healthy store.
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT app, dataset, data FROM records \
             WHERE removed_at IS NULL AND simhash = 0 \
             ORDER BY app, dataset",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut counts: std::collections::BTreeMap<(String, String), i64> = Default::default();
        for (app, dataset, data) in rows {
            let value: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
            if crate::doctor::simhash_zero_is_a_missing_fingerprint(&value) {
                *counts.entry((app, dataset)).or_default() += 1;
            }
        }
        let mut out: Vec<(String, String, i64)> = counts
            .into_iter()
            .map(|((app, dataset), n)| (app, dataset, n))
            .collect();
        out.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| (&a.0, &a.1).cmp(&(&b.0, &b.1))));
        Ok(out)
    }

    /// Live (non-tombstoned) records across every dataset — one aggregate, so the
    /// doctor can compare the store against the search index's `doc_count`
    /// without a per-record read.
    pub async fn live_record_count(&self) -> Result<i64> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM records WHERE removed_at IS NULL")
            .fetch_one(&self.pool)
            .await?;
        Ok(n)
    }

    /// Derived specs whose source `(app, dataset)` holds no records at all — they
    /// recompute forever over nothing, and the target dataset they advertise will
    /// never fill. Returns `(id, source, target)`.
    pub async fn orphan_derived_specs(&self) -> Result<Vec<(String, String, String)>> {
        Ok(sqlx::query_as(
            "SELECT d.id, d.source_app || '/' || d.source_dataset, d.target_dataset \
             FROM derived d \
             WHERE NOT EXISTS (SELECT 1 FROM records r \
                               WHERE r.app = d.source_app AND r.dataset = d.source_dataset) \
             ORDER BY d.created_at, d.id",
        )
        .fetch_all(&self.pool)
        .await?)
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
        Ok(specs_from_rows(rows, "enabled_derived"))
    }

    /// The [`Provenance`] every revision of one derived batch carries: the
    /// derivation itself (`rules_hash` = the registered
    /// [`derived_spec_fingerprint`]) plus the producing job of the SOURCE write
    /// that triggered it, so a derived row points back at both the spec that
    /// shaped it and the run that fed it.
    ///
    /// `source_url`/`artifact_sha` stay Null on purpose: a derived row was not
    /// fetched from anywhere and has no archived body, and inventing either
    /// would make it claim to be [`Provenance::replayable`] when it is not.
    /// Registration failure degrades to an unstamped `rules_hash` rather than
    /// stamping a hash the `rules_versions` registry does not hold (which is
    /// exactly what the doctor's `unregistered_rules` finding hunts).
    async fn derived_provenance(
        &self,
        spec: &DerivedSpec,
        source: Option<&Provenance>,
    ) -> Provenance {
        let fingerprint = derived_spec_fingerprint(spec);
        let rules_hash = match self.register_rules(&fingerprint).await {
            Ok(h) => Some(h),
            Err(e) => {
                tracing::warn!(spec = %spec.id, "derived: spec fingerprint not registered: {e}");
                None
            }
        };
        Provenance {
            job_id: source.and_then(|p| p.job_id.clone()),
            source_url: None,
            artifact_sha: None,
            rules_hash,
        }
    }

    /// Writes one derived batch, splitting it by the trust each row inherited
    /// so a single quarantined input cannot drag its whole batch down *and* a
    /// stable-looking stamp can never cover a weaker row. One upsert per
    /// distinct trust (almost always exactly one).
    async fn upsert_derived_rows(
        &self,
        spec: &DerivedSpec,
        rows: Vec<DerivedRowOut>,
        prov: &Provenance,
        depth: u32,
    ) -> Result<UpsertSummary> {
        let mut total = UpsertSummary::default();
        for (trust, items) in partition_by_trust(rows) {
            let s = self
                .upsert_many_at_depth(
                    &spec.source_app,
                    &spec.target_dataset,
                    &items,
                    trust.as_deref(),
                    Some(prov),
                    // A derived-spec recompute writes exactly what the spec
                    // projects — nothing in it is "someone else's join" — so it
                    // hashes its whole value, as it always has.
                    &DerivedPaths::NONE,
                    depth,
                )
                .await?;
            total.new.extend(s.new);
            total.changed.extend(s.changed);
            total.unchanged += s.unchanged;
        }
        Ok(total)
    }

    /// Feeds a batch's fresh keys through the matching enabled specs, upserting
    /// the shaped rows into each spec's target dataset at `depth + 1`.
    ///
    /// Fail-open by design: every error path here logs and continues — a
    /// misconfigured spec must degrade the *derived* dataset, never the source
    /// ingest that triggered it. The depth cap is what prevents an unbounded
    /// cascade: derived writes recurse through `upsert_many_at_depth`, and a
    /// hop that would exceed `derived_max_depth` is skipped loudly.
    ///
    /// `trust`/`prov` are the SOURCE write's stamps: derived rows inherit the
    /// former (weakened further by anything they join to, see
    /// [`weakest_trust`]) and carry a derivation-identifying twin of the
    /// latter (see [`Datasets::derived_provenance`]).
    #[allow(clippy::too_many_arguments)]
    async fn apply_derived(
        &self,
        app: &str,
        dataset: &str,
        items: &[(String, Value)],
        summary: &UpsertSummary,
        depth: u32,
        trust: Option<&str>,
        prov: Option<&Provenance>,
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
                    .apply_derived_group(spec, group, &by_key, summary, depth, prov)
                    .await
                {
                    tracing::warn!(spec = %spec.id, "derived: group recompute failed: {e}");
                }
                continue;
            }
            let filters = match parse_filter_specs(&spec.filters) {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!(spec = %spec.id, "derived: unparseable filters, spec skipped: {e}");
                    continue;
                }
            };
            let fresh: Vec<(&str, &Value, Option<&str>)> = summary
                .fresh_keys()
                .filter_map(|key| {
                    by_key
                        .get(key.as_str())
                        .map(|data| (key.as_str(), *data, trust))
                })
                .collect();
            let out = match self.derive_rows(spec, &filters, &fresh).await {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(spec = %spec.id, "derived: batch skipped: {e}");
                    continue;
                }
            };
            if out.is_empty() {
                continue;
            }
            let stamp = self.derived_provenance(spec, prov).await;
            if let Err(e) = self.upsert_derived_rows(spec, out, &stamp, depth + 1).await {
                tracing::warn!(spec = %spec.id, target = %spec.target_dataset,
                               "derived: target upsert failed: {e}");
            }
        }
    }

    /// Applies one spec to a WHOLE batch of source records: filter → project →
    /// lookup-merge, keyed 1:1 by the source key. Filtered-out records simply
    /// do not appear in the output.
    ///
    /// Batch-shaped on purpose. The join used to be one `SELECT … WHERE key = ?`
    /// **per source record**, so a 50k-row backfill with a lookup issued 50k
    /// point queries; the keys are now collected across the batch, deduped, and
    /// read in `IN (…)` chunks bounded by [`MAX_BIND_PARAMS`] — the same idiom
    /// the batch upsert's `read_key_states` uses. `filters` arrives already
    /// parsed, so the spec's grammar is parsed once per batch, never per row.
    ///
    /// A missing lookup key/record merges nothing — the row still lands, so a
    /// late-arriving lookup side fills in on the next source delta.
    async fn derive_rows(
        &self,
        spec: &DerivedSpec,
        filters: &[JsonFilter],
        rows: &[(&str, &Value, Option<&str>)],
    ) -> Result<Vec<DerivedRowOut>> {
        // Pass 1 (pure): filter + project, remembering each survivor's join key.
        let mut shaped: Vec<(String, Value, Option<&str>, Option<String>)> = Vec::new();
        for (key, data, trust) in rows {
            if !filters_match(filters, data) {
                continue;
            }
            let value = project_value(&spec.project, data);
            let join_key = spec
                .lookup
                .as_ref()
                .and_then(|l| lookup_json_path(data, &l.key_expr))
                .and_then(value_text);
            shaped.push((key.to_string(), value, *trust, join_key));
        }
        // Pass 2 (one query per bind-limited chunk, not per row): resolve the
        // join side for every distinct key this batch asked for.
        let joined = match &spec.lookup {
            Some(lookup) if !shaped.is_empty() => {
                let mut keys: Vec<&str> = shaped
                    .iter()
                    .filter_map(|(_, _, _, k)| k.as_deref())
                    .collect();
                keys.sort_unstable();
                keys.dedup();
                self.live_records_by_key(&spec.source_app, &lookup.dataset, &keys)
                    .await?
            }
            _ => Default::default(),
        };
        // Pass 3 (pure): merge + settle the inherited trust.
        Ok(shaped
            .into_iter()
            .map(|(key, mut value, source_trust, join_key)| {
                let mut joined_trust: Option<String> = None;
                if let (Some(lookup), Some(jk)) = (&spec.lookup, join_key) {
                    if let Some((data, trust)) = joined.get(&jk) {
                        joined_trust = Some(trust.clone());
                        if let Value::Object(map) = &mut value {
                            map.insert(lookup.merge_as.clone(), data.clone());
                        }
                    }
                }
                let trust = weakest_trust([source_trust, joined_trust.as_deref()]);
                (key, value, trust)
            })
            .collect())
    }

    /// `(data, trust)` of every LIVE record among `keys`, read in one statement
    /// per bind-limited chunk. Tombstoned rows are excluded in SQL — the
    /// per-record join checked `removed_at.is_none()` in Rust, same semantics.
    async fn live_records_by_key(
        &self,
        app: &str,
        dataset: &str,
        keys: &[&str],
    ) -> Result<std::collections::HashMap<String, (Value, String)>> {
        let mut out = std::collections::HashMap::with_capacity(keys.len());
        for slice in keys.chunks(rows_per_statement(1, 2)) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT key, data, trust FROM records WHERE removed_at IS NULL AND app = ",
            );
            qb.push_bind(app);
            qb.push(" AND dataset = ");
            qb.push_bind(dataset);
            qb.push(" AND key IN (");
            push_key_list(&mut qb, slice);
            qb.push(")");
            let rows: Vec<(String, String, Option<String>)> =
                qb.build_query_as().fetch_all(&self.pool).await?;
            for (key, data, trust) in rows {
                out.insert(
                    key,
                    (
                        serde_json::from_str(&data).unwrap_or(Value::Null),
                        trust_label(trust.as_deref()),
                    ),
                );
            }
        }
        Ok(out)
    }

    /// Materializes one spec over the existing live source rows in bounded
    /// keyset batches, with the default row budget
    /// ([`BackfillOpts::default`]). See
    /// [`backfill_derived_budgeted`](Self::backfill_derived_budgeted).
    pub async fn backfill_derived(
        &self,
        spec: &DerivedSpec,
        batch: i64,
    ) -> Result<DerivedBackfill> {
        self.backfill_derived_budgeted(
            spec,
            &BackfillOpts {
                batch,
                ..Default::default()
            },
        )
        .await
    }

    /// Materializes one spec over the existing live source rows in keyset pages
    /// of `opts.batch` (`POST /derived/{id}/backfill`). Runs at depth 1, so a
    /// backfill's downstream cascade obeys the same cap as the live path.
    ///
    /// **Budgeted and resumable.** The whole loop runs inside one HTTP request,
    /// so an unbounded pass over a large source is a request that never returns
    /// and, if the client gives up, restarts from zero. It now stops after
    /// `opts.max_rows` scanned rows and hands back `done: false` plus the
    /// keyset `cursor` to pass to the next call. Resuming is safe because the
    /// work is idempotent per row: every page recomputes its rows from source
    /// truth and the target's own change detection turns a repeat into
    /// `unchanged`, so a resumed run, a re-run from scratch and an overlapping
    /// retry all converge on the same rows.
    ///
    /// **Aggregate specs cannot resume mid-corpus** — a group's members are
    /// spread across the whole scan order, so a partial pass would write
    /// partial totals. They therefore treat the budget as a *ceiling*: over it,
    /// the backfill fails with a `BadRequest` naming the limit and writes
    /// NOTHING, rather than publishing a number it did not finish computing.
    pub async fn backfill_derived_budgeted(
        &self,
        spec: &DerivedSpec,
        opts: &BackfillOpts,
    ) -> Result<DerivedBackfill> {
        let batch = opts.batch.clamp(1, MAX_BACKFILL_BATCH);
        let max_rows = opts.max_rows.max(1);
        if let Some(group) = &spec.group {
            return self
                .backfill_derived_group(spec, group, batch, max_rows)
                .await;
        }
        // Parsed ONCE for the whole backfill — the live path hoists it per
        // batch and the group path always did; only this loop re-parsed the
        // spec's filter grammar for every source record it scanned.
        let filters = parse_filter_specs(&spec.filters)?;
        let stamp = self.derived_provenance(spec, None).await;
        let mut report = DerivedBackfill::default();
        let mut after = opts.cursor.as_deref().and_then(parse_backfill_cursor);
        let mut examined: i64 = 0;
        loop {
            let page = self
                .list_page(&spec.source_app, &spec.source_dataset, after, batch, None)
                .await?;
            let n = page.len() as i64;
            examined += n;
            let live: Vec<(&str, &Value, Option<&str>)> = page
                .iter()
                .filter(|r| r.removed_at.is_none())
                .map(|r| (r.key.as_str(), &r.data, Some(r.trust.as_str())))
                .collect();
            report.scanned += live.len() as u64;
            let items = self.derive_rows(spec, &filters, &live).await?;
            report.matched += items.len() as u64;
            if !items.is_empty() {
                let s = self.upsert_derived_rows(spec, items, &stamp, 1).await?;
                report.new += s.new.len() as u64;
                report.changed += s.changed.len() as u64;
                report.unchanged += s.unchanged as u64;
            }
            // A short page means the source is exhausted, whatever the budget.
            if n < batch {
                report.done = true;
                break;
            }
            let next = page.last().map(backfill_cursor);
            if examined >= max_rows {
                report.cursor = next;
                break;
            }
            after = next.as_deref().and_then(parse_backfill_cursor);
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
    #[allow(clippy::too_many_arguments)]
    async fn apply_derived_group(
        &self,
        spec: &DerivedSpec,
        group: &DerivedGroup,
        by_key: &std::collections::HashMap<&str, &Value>,
        summary: &UpsertSummary,
        depth: u32,
        prov: Option<&Provenance>,
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
        self.recompute_groups(spec, group, &filters, &aggs, tuples, depth, prov)
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
                None => lookup_json_path(new_data, path).and_then(group_value_text),
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
    #[allow(clippy::too_many_arguments)]
    async fn recompute_groups(
        &self,
        spec: &DerivedSpec,
        group: &DerivedGroup,
        filters: &[JsonFilter],
        aggs: &std::collections::BTreeMap<String, Aggregate>,
        tuples: std::collections::HashSet<Vec<String>>,
        depth: u32,
        prov: Option<&Provenance>,
    ) -> Result<()> {
        if tuples.is_empty() {
            return Ok(());
        }
        let mut out: Vec<DerivedRowOut> = Vec::new();
        for tuple in tuples {
            match self
                .recompute_group_row(spec, group, filters, aggs, &tuple)
                .await
            {
                Ok(row) => out.push(row),
                Err(e) => {
                    tracing::warn!(spec = %spec.id, "derived: group row skipped: {e}");
                }
            }
        }
        if out.is_empty() {
            return Ok(());
        }
        let stamp = self.derived_provenance(spec, prov).await;
        self.upsert_derived_rows(spec, out, &stamp, depth + 1)
            .await?;
        Ok(())
    }

    /// Builds one group's derived row from source truth: scan the group's live
    /// source rows (bounded at `max_group_scan + 1`) and evaluate every
    /// aggregate. Over the bound, the row is `{group fields, stale: true}` with
    /// NO aggregate fields — absent, not wrong. The derived key is the group
    /// values joined with `|` (escaped, see [`group_row_key`]).
    ///
    /// The row's trust is [`weakest_trust`] over the members that were scanned:
    /// an aggregate is a claim about its whole group, so one provisional member
    /// makes the number provisional. (An oversized group carries the weakest
    /// trust of the rows we *looked at* — the row is already `stale: true` and
    /// makes no aggregate claim.)
    async fn recompute_group_row(
        &self,
        spec: &DerivedSpec,
        group: &DerivedGroup,
        filters: &[JsonFilter],
        aggs: &std::collections::BTreeMap<String, Aggregate>,
        tuple: &[String],
    ) -> Result<DerivedRowOut> {
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
        let trust = weakest_trust(rows.iter().map(|(_, t)| Some(t.as_str())));
        let mut data = serde_json::Map::new();
        for (path, value) in group.group_by.iter().zip(tuple) {
            data.insert(
                group_field_name(path).to_string(),
                Value::String(value.clone()),
            );
        }
        if rows.len() as i64 > self.max_group_scan {
            data.insert("stale".into(), Value::Bool(true));
            return Ok((group_row_key(tuple), Value::Object(data), trust));
        }
        data.insert("stale".into(), Value::Bool(false));
        for (out, agg) in aggs {
            let v = match agg {
                Aggregate::Count => Value::from(rows.len() as u64),
                Aggregate::Sum(path) => {
                    let sum: f64 = rows
                        .iter()
                        .filter_map(|(r, _)| lookup_json_path(r, path).and_then(Value::as_f64))
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
        Ok((group_row_key(tuple), Value::Object(data), trust))
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
    ) -> Result<Vec<(Value, String)>> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT data, trust FROM records WHERE removed_at IS NULL AND app = ",
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
        let raw: Vec<(String, Option<String>)> = qb.build_query_as().fetch_all(&self.pool).await?;
        Ok(raw
            .into_iter()
            .map(|(d, t)| {
                (
                    serde_json::from_str(&d).unwrap_or(Value::Null),
                    trust_label(t.as_deref()),
                )
            })
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
            if let Err(e) = self
                .recompute_groups(spec, group, &filters, &aggs, tuples, 0, None)
                .await
            {
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
    ///
    /// `max_rows` is a ceiling, not a budget: an aggregate needs the whole
    /// corpus in one pass, so exceeding it is an error that writes nothing —
    /// never a partial total published as if it were final.
    async fn backfill_derived_group(
        &self,
        spec: &DerivedSpec,
        group: &DerivedGroup,
        batch: i64,
        max_rows: i64,
    ) -> Result<DerivedBackfill> {
        let filters = parse_filter_specs(&spec.filters)?;
        let aggs = parse_aggregates(&group.aggregates)?;
        let mut report = DerivedBackfill::default();
        // tuple -> (count, per-aggregate sums keyed like `aggs`, weakest member
        // trust). The trust accumulator is the streaming twin of
        // `recompute_group_row`'s `weakest_trust` over the scanned members.
        let mut groups: std::collections::HashMap<Vec<String>, GroupAccumulator> =
            Default::default();
        let mut after: Option<(String, String)> = None;
        let mut examined: i64 = 0;
        loop {
            let page = self
                .list_page(&spec.source_app, &spec.source_dataset, after, batch, None)
                .await?;
            let n = page.len() as i64;
            examined += n;
            if examined > max_rows {
                return Err(Error::BadRequest(format!(
                    "derived spec '{}' aggregates, and an aggregate backfill must scan the whole \
                     source in ONE pass (a group's members are spread across the scan order, so a \
                     partial pass would publish partial totals). The source exceeds max_rows={}; \
                     nothing was written — re-run with a larger max_rows.",
                    spec.id, max_rows
                )));
            }
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
                entry.2 = weakest_trust([entry.2.as_deref(), Some(rec.trust.as_str())]);
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
        let mut out: Vec<DerivedRowOut> = Vec::new();
        for (tuple, (count, sums, trust)) in &groups {
            let mut data = serde_json::Map::new();
            for (path, value) in group.group_by.iter().zip(tuple) {
                data.insert(
                    group_field_name(path).to_string(),
                    Value::String(value.clone()),
                );
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
            out.push((group_row_key(tuple), Value::Object(data), trust.clone()));
        }
        report.matched = out.len() as u64;
        if !out.is_empty() {
            let stamp = self.derived_provenance(spec, None).await;
            let s = self.upsert_derived_rows(spec, out, &stamp, 1).await?;
            report.new += s.new.len() as u64;
            report.changed += s.changed.len() as u64;
            report.unchanged += s.unchanged as u64;
        }
        // A group backfill either completes its single pass or errors out
        // above — it never hands back a cursor.
        report.done = true;
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
    path.trim_start_matches("$.")
        .rsplit('.')
        .next()
        .unwrap_or(path)
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

/// Outcome of ONE backfill request over the existing source rows.
#[derive(Debug, Default, Serialize)]
pub struct DerivedBackfill {
    /// Live source rows examined **by this request**.
    pub scanned: u64,
    /// Rows that passed the spec's filters and were upserted.
    pub matched: u64,
    pub new: u64,
    pub changed: u64,
    pub unchanged: u64,
    /// True when the source was scanned to its end. `false` means the row
    /// budget stopped this request early — the counters describe this slice,
    /// not the whole spec.
    pub done: bool,
    /// Keyset cursor to resume from, present exactly when `done` is false.
    /// Feed it back as `cursor` on the next request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Rows one backfill request scans before it stops and hands back a cursor.
///
/// The backfill runs synchronously inside an HTTP request, so the ceiling is
/// what keeps that request bounded. 50k pages a large corpus in a handful of
/// calls while staying well inside any sane proxy timeout.
pub const DEFAULT_BACKFILL_MAX_ROWS: i64 = 50_000;

/// One backfill request's bounds and resume point.
#[derive(Debug, Clone)]
pub struct BackfillOpts {
    /// Rows per keyset page (clamped to `1..=`[`MAX_BACKFILL_BATCH`]).
    pub batch: i64,
    /// Rows this request may scan before returning a cursor (aggregate specs
    /// treat it as a hard ceiling — see
    /// [`Datasets::backfill_derived_budgeted`]).
    pub max_rows: i64,
    /// Resume point from a previous response's `cursor`; `None` starts over.
    pub cursor: Option<String>,
}

impl Default for BackfillOpts {
    fn default() -> Self {
        Self {
            batch: 500,
            max_rows: DEFAULT_BACKFILL_MAX_ROWS,
            cursor: None,
        }
    }
}

/// The resume cursor for a page's last record: `<updated_at>|<key>`, the same
/// `updated_at|key` keyset encoding the record and job list endpoints use. The
/// timestamp is fixed-width and holds no `|`, so splitting on the FIRST one is
/// unambiguous even for a key that contains a pipe.
pub fn backfill_cursor(rec: &Record) -> String {
    format!("{}|{}", ts(rec.updated_at), rec.key)
}

/// Parses a [`backfill_cursor`]. A blank or separator-less value starts from
/// the top rather than failing the request — the same forgiving contract the
/// HTTP cursor parsers have.
pub fn parse_backfill_cursor(cursor: &str) -> Option<(String, String)> {
    let trimmed = cursor.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed
        .split_once('|')
        .map(|(t, k)| (t.to_string(), k.to_string()))
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

/// Parses the stored `lookup` column into its two mutually-exclusive shapes.
///
/// The column holds either a [`DerivedLookup`] or a [`DerivedGroup`]; their
/// required fields are disjoint, so the untagged parse is unambiguous. An
/// absent/blank column is a plain filter/project spec.
///
/// **A value that is present and unparseable is an error, never `(None, None)`.**
/// Swallowing the parse degraded a lookup/group spec into a *passthrough*: the
/// spec kept running and kept writing rows of the wrong shape — an aggregate
/// dataset silently refilled with raw source rows, with no error anywhere. A
/// spec we cannot read is a spec we must not run.
pub fn parse_stored_join(
    raw: Option<&str>,
) -> Result<(Option<DerivedLookup>, Option<DerivedGroup>)> {
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum StoredJoin {
        Lookup(DerivedLookup),
        Group(DerivedGroup),
    }
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok((None, None));
    };
    match serde_json::from_str::<StoredJoin>(raw) {
        Ok(StoredJoin::Lookup(l)) => Ok((Some(l), None)),
        Ok(StoredJoin::Group(g)) => Ok((None, Some(g))),
        Err(e) => Err(Error::parse_from(
            format!(
                "derived spec's stored lookup/group column is unparseable ({e}); \
             refusing to run it as a passthrough"
            ),
            e,
        )),
    }
}

/// One group's streaming accumulator during an aggregate backfill: `(member
/// count, per-aggregate sums keyed like the parsed aggregates, weakest member
/// trust)`.
type GroupAccumulator = (u64, std::collections::BTreeMap<String, f64>, Option<String>);

/// One shaped derived row on its way to the target dataset: `(key, data,
/// inherited trust)`, where the trust is `None` for `stable` exactly as the
/// column's NULL means.
pub(crate) type DerivedRowOut = (String, Value, Option<String>);

/// Groups derived rows by the trust they inherited, so each group can be
/// written with its own stamp. Ordered (BTreeMap) so the write order of a batch
/// is deterministic.
pub(crate) fn partition_by_trust(
    rows: Vec<DerivedRowOut>,
) -> std::collections::BTreeMap<Option<String>, Vec<(String, Value)>> {
    let mut out: std::collections::BTreeMap<Option<String>, Vec<(String, Value)>> =
        Default::default();
    for (key, data, trust) in rows {
        out.entry(trust).or_default().push((key, data));
    }
    out
}

/// Parses stored spec rows, dropping — **loudly** — any row this build cannot
/// read. A corrupt spec must not run (that is [`parse_stored_join`]'s job) and
/// must not take its siblings down with it: one unreadable row would otherwise
/// fail the whole `enabled_derived` read and silently disable every other
/// spec on the same source.
pub(crate) fn specs_from_rows(rows: Vec<DerivedRow>, context: &str) -> Vec<DerivedSpec> {
    rows.into_iter()
        .filter_map(|r| {
            let id = r.id.clone();
            match DerivedSpec::try_from(r) {
                Ok(spec) => Some(spec),
                Err(e) => {
                    tracing::error!(
                        spec = %id,
                        context,
                        "derived: spec is unreadable and was SKIPPED (not run as a passthrough): {e}"
                    );
                    None
                }
            }
        })
        .collect()
}

/// Canonical JSON identity of a derivation — every input that decides what the
/// derived rows look like (the spec id, its source/target and its
/// filter/project/lookup/group shape).
///
/// Hashed with [`rules_hash`] and registered in `rules_versions`, this is a
/// derived revision's [`Provenance::rules_hash`]: the derived-dataset twin of
/// the extractor's RuleSet stamp, and the same idiom (migration 0030) — the
/// hash IS the identity, so re-registration is free and an edited spec hashes
/// apart from the rows written under its previous shape.
pub fn derived_spec_fingerprint(spec: &DerivedSpec) -> Value {
    serde_json::json!({
        "kind": "derived_spec",
        "id": spec.id,
        "source_app": spec.source_app,
        "source_dataset": spec.source_dataset,
        "target_dataset": spec.target_dataset,
        "filters": spec.filters,
        "project": spec.project,
        "lookup": spec.lookup.as_ref().and_then(|l| serde_json::to_value(l).ok()),
        "group": spec.group.as_ref().and_then(|g| serde_json::to_value(g).ok()),
    })
}

impl TryFrom<DerivedRow> for DerivedSpec {
    type Error = Error;

    fn try_from(r: DerivedRow) -> Result<DerivedSpec> {
        let (lookup, group) = parse_stored_join(r.lookup.as_deref())?;
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
    // 0030 provenance stamp — every column NULL-means-unknown.
    job_id: Option<String>,
    source_url: Option<String>,
    artifact_sha: Option<String>,
    rules_hash: Option<String>,
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
            provenance: Provenance {
                job_id: r.job_id,
                source_url: r.source_url,
                artifact_sha: r.artifact_sha,
                rules_hash: r.rules_hash,
            },
        })
    }
}

/// Near-duplicate pairs among `(key, simhash)` rows, via band-bucketed
/// candidate generation plus exact Hamming verification.
///
/// **Output contract, preserved exactly from the all-pairs scan** (consumers
/// index into this list and the grants link layer stores the first pairs it
/// sees, so a reordering is a data change):
///
/// - a pair is `(a, b)` with `a` the earlier row, `b` the later one;
/// - pairs are enumerated in `(a-index, b-index)` ascending order, and the
///   `MAX_DUP_PAIRS` cap truncates *that* order — the cap is on the walk, not on
///   the sorted result;
/// - the final `sort_by_key(distance)` is stable, so equal-distance pairs keep
///   enumeration order.
///
/// The index is built over ALL rows first and then queried per row, rather than
/// built incrementally, precisely to keep that a-major walk order: an
/// incremental build would enumerate b-major and cap a different subset.
/// Rows with simhash 0 (no textual content) never enter the index and are never
/// queried.
fn banded_duplicate_pairs(rows: &[(String, i64)], max_distance: u32) -> Vec<DupPair> {
    // Rows with simhash 0 carry no textual content — "empty", not "identical" —
    // so they never enter the index and are never queried. That makes the index
    // slot a COMPACTION of the row index, not the row index: `slot_of_row` is
    // what keeps the `j > i` skip honest. Conflating the two silently compares a
    // row against itself and against rows it already covered.
    let mut index = crate::simhash::BandedIndex::new(max_distance);
    let mut slot_of_row: Vec<Option<usize>> = Vec::with_capacity(rows.len());
    for (i, (_, sim)) in rows.iter().enumerate() {
        if *sim == 0 {
            slot_of_row.push(None);
        } else {
            slot_of_row.push(Some(index.len()));
            index.insert(*sim as u64, i);
        }
    }
    let mut pairs = Vec::new();
    for (i, (key, sim)) in rows.iter().enumerate() {
        let Some(slot) = slot_of_row[i] else {
            continue;
        };
        // Slots ascend and start past this row's own slot, so this visits
        // exactly the `j > i` the inner loop did, in the same order.
        index.for_each_neighbor_after(*sim as u64, slot, |_, &j| {
            pairs.push(DupPair {
                a: key.clone(),
                b: rows[j].0.clone(),
                distance: crate::simhash::hamming(*sim as u64, rows[j].1 as u64),
            });
            // Bound the result: a pathological dataset must not return an
            // unbounded pair list.
            pairs.len() < MAX_DUP_PAIRS
        });
        if pairs.len() >= MAX_DUP_PAIRS {
            break;
        }
    }
    pairs.sort_by_key(|p| p.distance);
    pairs
}

// ── batch upsert planning ───────────────────────────────────────────────────
// The pure half of `upsert_many`: what the store currently holds for a chunk's
// keys, what each item resolves to, and which record writes that collapses into.
// Kept out of the async write path so the verdict logic is unit-testable against
// the per-record semantics it replaces.

/// The store's state for one key while a chunk is planned: content hash, stored
/// JSON (needed only to diff a change), and whether it is tombstoned.
#[derive(Debug, Clone)]
struct KeyState {
    hash: String,
    data: String,
    removed: bool,
}

/// One item's resolved verdict plus everything the batched statements need.
#[derive(Debug, Clone)]
struct PlannedWrite {
    /// Position of the item within the chunk (indexes both `chunk` and its
    /// parallel [`Fingerprint`] slice).
    idx: usize,
    kind: ChangeKind,
    /// Field-level diff — `Some` only for [`ChangeKind::Changed`].
    diff: Option<Value>,
    /// Revision number this write appends (0 and unused for Unchanged).
    revision: i64,
    /// Unchanged by change detection, but the stored bytes differ — only
    /// reachable when the writer declared [`DerivedPaths`], because otherwise
    /// an equal hash *is* equal bytes. The record body is rewritten so readers
    /// see the fresh derived data; no revision is appended, and the verdict
    /// stays [`ChangeKind::Unchanged`].
    refresh: bool,
}

/// Record paths a producer declares **derived**: written into the record for
/// readers, but recomputed from *another* dataset rather than observed at this
/// record's own source. Excluded from the change-detection hash, so a movement
/// in them is not a change *in this record*.
///
/// The disease this cures: eu-sedia writes a `history` block joined from
/// cordis's weekly rollup into every Horizon topic before upsert. Hashing the
/// whole value meant every cordis run marked every joined topic `changed` in
/// the next eu-sedia run — and watches, triggers, webhooks, the revision trail
/// and the yield ledger all read that as a real SEDIA publication.
///
/// **Opt-in per write, default off.** [`DerivedPaths::NONE`] hashes exactly
/// what the store has always hashed, so every producer that declares nothing is
/// byte-identical. Declaring a path changes three things and nothing else:
/// - the hash covers the value *minus* those paths;
/// - the stored `data` (and the revision snapshot) still carry the **full**
///   value — this is a change-detection seam, not a projection;
/// - a write whose only movement is derived rewrites the record body without
///   appending a revision, so reads stay fresh while the change feed stays
///   quiet. `updated_at` moves with the body, `last_seen` as always.
///
/// Paths are `.`-separated and resolved through objects only (`history`,
/// `history.stats`). A path that is absent from a value is a no-op.
///
/// **Transition cost.** The first write after a producer adopts this re-hashes
/// every stored record whose hash included a now-derived path, so those records
/// report `changed` **once** — bounded by the number of records carrying the
/// path — and settle from then on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DerivedPaths(Vec<String>);

impl DerivedPaths {
    /// Declare nothing — the default every existing write path passes.
    pub const NONE: DerivedPaths = DerivedPaths(Vec::new());

    pub fn new<I, S>(paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        DerivedPaths(
            paths
                .into_iter()
                .map(Into::into)
                .filter(|p| !p.is_empty())
                .collect(),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The value change detection hashes: `value` with every declared path
    /// removed.
    ///
    /// Borrowed — and therefore byte-identical to hashing `value` itself — when
    /// nothing is declared or nothing matched. That equivalence is the whole
    /// safety argument for adding this to a shared write path.
    fn hash_input<'v>(&self, value: &'v Value) -> std::borrow::Cow<'v, Value> {
        if self.0.is_empty() {
            return std::borrow::Cow::Borrowed(value);
        }
        let mut stripped = value.clone();
        let mut removed_any = false;
        for path in &self.0 {
            removed_any |= remove_path(&mut stripped, path);
        }
        if removed_any {
            std::borrow::Cow::Owned(stripped)
        } else {
            std::borrow::Cow::Borrowed(value)
        }
    }
}

/// Removes the `.`-separated `path` from `value`, walking objects only.
/// Returns whether anything was actually removed — an absent path is a no-op,
/// never an error.
fn remove_path(value: &mut Value, path: &str) -> bool {
    let mut cursor = value;
    let mut segments = path.split('.').peekable();
    while let Some(segment) = segments.next() {
        let Value::Object(map) = cursor else {
            return false;
        };
        if segments.peek().is_none() {
            return map.remove(segment).is_some();
        }
        match map.get_mut(segment) {
            Some(next) => cursor = next,
            None => return false,
        }
    }
    false
}

/// Everything about a record that can be derived from its value alone: the
/// content hash change detection compares, the SimHash near-dup fingerprint, and
/// the canonical JSON text stored in `data`.
///
/// Computed once per item, **before** the write transaction opens and once
/// rather than once per use. Both hashes are pure CPU over the record's JSON;
/// computing them under `BEGIN IMMEDIATE` made every other app's writer wait
/// through this batch's hashing, which on a large sync is most of the lock hold.
#[derive(Debug, Clone)]
struct Fingerprint {
    /// Hash of the **change-detection input** — the value minus any declared
    /// [`DerivedPaths`]. Equal to `hash_value(json)` whenever nothing is
    /// declared, which is every existing producer.
    hash: String,
    sim: i64,
    /// The **full** value as stored: derived paths included.
    json: String,
}

/// Fingerprints a whole batch. Hash canonicalization is untouched — the same
/// [`hash_value`] / [`crate::simhash::simhash_value`] — so stored hashes and
/// fingerprints stay comparable with everything written before.
///
/// The SimHash stays over the **full** value on purpose: it is a similarity
/// fingerprint of the record as stored, and `/duplicates` compares stored
/// records. Only change detection gets the narrowed input.
fn fingerprint_batch(items: &[(String, Value)], derived: &DerivedPaths) -> Vec<Fingerprint> {
    items
        .iter()
        .map(|(_, value)| Fingerprint {
            hash: hash_value(&derived.hash_input(value)),
            sim: crate::simhash::simhash_value(value) as i64,
            json: value.to_string(),
        })
        .collect()
}

/// Resolves a chunk into per-item verdicts against the store state read in one
/// batch, walking items **in order** and threading each verdict back into
/// `state` — so the sequence is identical to the per-record
/// read→write→revision loop this replaces.
///
/// Order is the whole point. Batching the read without feeding the writes back
/// into it breaks the case a per-record loop got right for free: a key that
/// appears TWICE in one batch. The second occurrence must be judged against what
/// the first one just wrote (New then Changed, with a diff v1→v2), not against
/// the pre-batch snapshot — which would report it New a second time and collide
/// on the revision number.
fn plan_chunk(
    items: &[(String, Value)],
    prints: &[Fingerprint],
    state: &mut std::collections::HashMap<String, KeyState>,
    next_revision: &mut std::collections::HashMap<String, i64>,
) -> Vec<PlannedWrite> {
    let mut plans = Vec::with_capacity(items.len());
    for (idx, (key, value)) in items.iter().enumerate() {
        let print = &prints[idx];
        let previous = state.get(key);
        let kind = match previous {
            // Unchanged content AND live: nothing to write but the sighting.
            Some(p) if p.hash == print.hash && !p.removed => ChangeKind::Unchanged,
            // Different content, or a tombstone reappearing — a revived record
            // is Changed even when its content matches the old snapshot.
            Some(_) => ChangeKind::Changed,
            None => ChangeKind::New,
        };
        // Unchanged by change detection, yet the stored bytes differ: the only
        // way that happens is a declared derived path moving. Refresh the body
        // so readers get the current derived data, without a revision — the
        // change feed must not learn about it. With no derived paths declared an
        // equal hash IS equal bytes, so this is always false for every existing
        // producer.
        let refresh =
            kind == ChangeKind::Unchanged && previous.is_some_and(|p| p.data != print.json);
        let diff = match (kind, previous) {
            (ChangeKind::Changed, Some(p)) => {
                let old: Value = serde_json::from_str(&p.data).unwrap_or(Value::Null);
                Some(diff_values(&old, value))
            }
            _ => None,
        };
        let revision = if kind == ChangeKind::Unchanged {
            0
        } else {
            next_revision_for(next_revision, key)
        };
        if kind != ChangeKind::Unchanged || refresh {
            state.insert(
                key.clone(),
                KeyState {
                    hash: print.hash.clone(),
                    data: print.json.clone(),
                    removed: false,
                },
            );
        }
        plans.push(PlannedWrite {
            idx,
            kind,
            diff,
            revision,
            refresh,
        });
    }
    plans
}

/// The revision number `key`'s next revision takes, advancing the counter. A key
/// with no history starts at 1 — the same value the `COALESCE(MAX(revision), 0)
/// + 1` subquery produced.
fn next_revision_for(next: &mut std::collections::HashMap<String, i64>, key: &str) -> i64 {
    let slot = next.entry(key.to_string()).or_insert(1);
    let revision = *slot;
    *slot += 1;
    revision
}

/// Splits a planned chunk into the record writes it needs: `(content, touched)`.
///
/// `content` is one entry per key whose content moved, carrying that key's
/// **last** New/Changed plan — the row the sequential loop would leave behind —
/// in first-appearance order. `touched` is the keys that were only ever
/// Unchanged, which need `last_seen`/`trust` refreshed and nothing else.
///
/// Collapsing by *last write* rather than *last occurrence* is the correctness
/// point: a key that goes Changed then Unchanged inside one batch must still
/// have its new content written. Taking the last occurrence would keep the stale
/// row and silently drop the change the revision chain already recorded.
///
/// A derived-only `refresh` counts as a content write here (the body has to be
/// rewritten) even though its verdict is Unchanged and it appends no revision.
fn collapse_record_writes<'p>(
    chunk: &'p [(String, Value)],
    plans: &'p [PlannedWrite],
) -> (Vec<(usize, &'p PlannedWrite)>, Vec<&'p str>) {
    let mut content: Vec<(usize, &PlannedWrite)> = Vec::new();
    let mut at: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    let mut unchanged_only: Vec<&str> = Vec::new();
    let mut seen_unchanged: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for plan in plans {
        let key = chunk[plan.idx].0.as_str();
        if plan.kind == ChangeKind::Unchanged && !plan.refresh {
            if !at.contains_key(key) && seen_unchanged.insert(key) {
                unchanged_only.push(key);
            }
            continue;
        }
        match at.get(key) {
            Some(&pos) => content[pos] = (plan.idx, plan),
            None => {
                at.insert(key, content.len());
                content.push((plan.idx, plan));
            }
        }
    }
    // A key that ended up with a content write also gets its last_seen from it.
    unchanged_only.retain(|k| !at.contains_key(k));
    (content, unchanged_only)
}

/// The per-item summary of a planned chunk, in item order — one entry per
/// *occurrence*, matching what the per-record loop pushed.
fn summarize_chunk(chunk: &[(String, Value)], plans: &[PlannedWrite]) -> UpsertSummary {
    let mut summary = UpsertSummary::default();
    for plan in plans {
        let key = &chunk[plan.idx].0;
        match plan.kind {
            ChangeKind::New => summary.new.push(key.clone()),
            ChangeKind::Changed => summary.changed.push(key.clone()),
            ChangeKind::Unchanged => summary.unchanged += 1,
        }
    }
    summary
}

/// Pushes a comma-separated list of bound key parameters (the body of an
/// `IN (…)`). Bound, never interpolated — a record key is caller data.
fn push_key_list<'a>(qb: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>, keys: &[&'a str]) {
    let mut sep = qb.separated(", ");
    for key in keys {
        sep.push_bind(*key);
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
        .map_err(|e| Error::parse_from(format!("bad timestamp '{s}': {e}"), e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A derived value is only as trusted as its weakest input — the whole
    /// point of the function. Averaging, majority or "first wins" would let a
    /// quarantined row wash into a stable-looking derived one.
    #[test]
    fn weakest_trust_is_the_floor_not_the_majority() {
        // Stable in, stable out (expressed as None, the column's NULL).
        assert_eq!(weakest_trust([None, Some("stable")]), None);
        // One weak input decides the whole row, whichever side it arrives on.
        assert_eq!(
            weakest_trust([None, Some("provisional")]),
            Some("provisional".to_string())
        );
        assert_eq!(
            weakest_trust([Some("provisional"), None]),
            Some("provisional".to_string())
        );
        // Quarantined beats provisional: the floor, not the last value seen.
        assert_eq!(
            weakest_trust([Some("provisional"), Some("quarantined"), None]),
            Some("quarantined".to_string())
        );
        // An unknown label is treated as the weakest thing there is, never as
        // "probably fine".
        assert_eq!(
            weakest_trust([Some("quarantined"), Some("martian")]),
            Some("martian".to_string())
        );
        // Nothing in, nothing claimed.
        assert_eq!(weakest_trust([]), None);
    }

    /// An unreadable `lookup`/`group` column must ERROR, because the old
    /// `.ok()` turned it into `(None, None)` — a lookup/aggregate spec silently
    /// demoted to a whole-record passthrough that kept writing wrong-shaped
    /// rows into the target dataset.
    #[test]
    fn corrupt_stored_join_errors_not_silently_passthrough() {
        // Absent / blank = an honest plain filter+project spec.
        assert!(matches!(parse_stored_join(None), Ok((None, None))));
        assert!(matches!(parse_stored_join(Some("   ")), Ok((None, None))));
        // Both real shapes still parse.
        let (lookup, group) =
            parse_stored_join(Some(r#"{"dataset":"d","key_expr":"$.k","merge_as":"m"}"#)).unwrap();
        assert!(lookup.is_some() && group.is_none());
        let (lookup, group) = parse_stored_join(Some(
            r#"{"group_by":["$.state"],"aggregates":{"n":"count"}}"#,
        ))
        .unwrap();
        assert!(group.is_some() && lookup.is_none());
        // Truncated JSON, and well-formed JSON of neither shape: both loud.
        assert!(parse_stored_join(Some(r#"{"dataset":"d","key_e"#)).is_err());
        assert!(parse_stored_join(Some(r#"{"nonsense":true}"#)).is_err());
    }

    /// The fingerprint must move when the derivation moves — otherwise rows
    /// written under an edited spec claim the provenance of the old one.
    #[test]
    fn derived_fingerprint_tracks_the_shape_not_just_the_id() {
        let base = DerivedSpec {
            id: "d1".into(),
            source_app: "app".into(),
            source_dataset: "src".into(),
            target_dataset: "tgt".into(),
            filters: vec!["$.state:eq:CA".into()],
            project: [("n".to_string(), "$.n".to_string())].into_iter().collect(),
            lookup: None,
            group: None,
            enabled: true,
            created_at: Utc::now(),
        };
        let h = rules_hash(&derived_spec_fingerprint(&base));
        // `created_at`/`enabled` are not part of the derivation's identity.
        let same = DerivedSpec {
            enabled: false,
            created_at: Utc::now(),
            ..base.clone()
        };
        assert_eq!(rules_hash(&derived_spec_fingerprint(&same)), h);
        // The filter set is.
        let moved = DerivedSpec {
            filters: vec!["$.state:eq:NY".into()],
            ..base.clone()
        };
        assert_ne!(rules_hash(&derived_spec_fingerprint(&moved)), h);
        // So is the projection.
        let reshaped = DerivedSpec {
            project: [("n".to_string(), "$.other".to_string())]
                .into_iter()
                .collect(),
            ..base
        };
        assert_ne!(rules_hash(&derived_spec_fingerprint(&reshaped)), h);
    }

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

    #[test]
    fn the_builder_trust_filter_is_the_predicate_not_a_second_copy() {
        // The QueryBuilder paths must emit the SAME predicate as the static-SQL
        // ones. A hand-written second copy is how "filtered" and "not filtered"
        // silently diverge, so this asserts the generated fragment is
        // TRUST_PREDICATE with its ?T slots bound.
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT 1 WHERE x = 1");
        push_trust_filter(&mut qb, Some("stable"));
        let sql = qb.sql().to_string();
        let expected = format!(
            "SELECT 1 WHERE x = 1 AND {}",
            TRUST_PREDICATE
                .replace("?T", "?")
                // QueryBuilder numbers its binds from 1.
                .replacen('?', "?1", 1)
        );
        // Placeholders are numbered ?1..?3 in order; compare the shape.
        assert_eq!(sql.matches('?').count(), 3, "{sql}");
        assert!(sql.contains("COALESCE(trust, 'stable')"), "{sql}");
        assert!(sql.starts_with("SELECT 1 WHERE x = 1 AND ("), "{sql}");
        assert!(expected.contains("COALESCE(trust, 'stable')"));

        // None must add nothing at all — an unfiltered read stays unfiltered.
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT 1 WHERE x = 1");
        push_trust_filter(&mut qb, None);
        assert_eq!(qb.sql(), "SELECT 1 WHERE x = 1");
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
        assert!(
            parse_aggregate("sum(amount)").is_err(),
            "path must be $.-rooted"
        );
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

    // ── banded duplicate detection ──────────────────────────────────────────

    /// The all-pairs scan the banded index replaced, kept as the reference
    /// implementation. Same walk order, same cap, same stable final sort.
    fn all_pairs_scan(rows: &[(String, i64)], max_distance: u32) -> Vec<DupPair> {
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
                    if pairs.len() >= MAX_DUP_PAIRS {
                        break 'scan;
                    }
                }
            }
        }
        pairs.sort_by_key(|p| p.distance);
        pairs
    }

    fn as_triples(pairs: &[DupPair]) -> Vec<(&str, &str, u32)> {
        pairs
            .iter()
            .map(|p| (p.a.as_str(), p.b.as_str(), p.distance))
            .collect()
    }

    /// A fixture with *known* near-duplicates: a base hash, exact copies, and
    /// neighbours at controlled Hamming distances (including one at exactly the
    /// threshold and one just past it), interleaved with unrelated hashes and
    /// content-free rows (simhash 0, which must never be compared).
    fn dup_fixture() -> Vec<(String, i64)> {
        let base: u64 = 0x0f1e_2d3c_4b5a_6978;
        let flip = |bits: &[u32]| -> i64 {
            let mut h = base;
            for b in bits {
                h ^= 1u64 << b;
            }
            h as i64
        };
        vec![
            ("exact-a".to_string(), base as i64),
            ("far".to_string(), 0x1234_5678_9abc_def0u64 as i64),
            ("d1".to_string(), flip(&[0])),
            ("empty-1".to_string(), 0),
            ("d3".to_string(), flip(&[5, 33, 61])),
            ("exact-b".to_string(), base as i64),
            ("d7".to_string(), flip(&[1, 2, 3, 40, 41, 42, 63])),
            ("empty-2".to_string(), 0),
            ("d2".to_string(), flip(&[17, 18])),
            ("far2".to_string(), !base as i64),
            ("d4".to_string(), flip(&[7, 23, 39, 55])),
            (
                "d12".to_string(),
                flip(&[0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44]),
            ),
        ]
    }

    #[test]
    fn banded_duplicate_pairs_match_the_all_pairs_scan_not_a_bucketed_approximation() {
        // Band bucketing is only a speedup if it decides identically. Sweeping
        // the whole distance range the API allows (the route clamps at 20) plus
        // 0 (exact match) catches an off-by-one in the band widths, which would
        // show up as a MISSING pair — a silent false negative, the failure mode
        // a "did it find some duplicates?" test never notices.
        let rows = dup_fixture();
        for distance in 0..=20u32 {
            let banded = banded_duplicate_pairs(&rows, distance);
            let reference = all_pairs_scan(&rows, distance);
            assert_eq!(
                as_triples(&banded),
                as_triples(&reference),
                "distance {distance}: banded pair set differs from the all-pairs scan"
            );
        }
    }

    #[test]
    fn banded_duplicate_pairs_keep_the_cap_and_the_walk_order() {
        // 150 identical records make 11,175 pairs — past MAX_DUP_PAIRS. The cap
        // truncates the (a-index, b-index) WALK, not the distance-sorted result,
        // and the banded walk must truncate at exactly the same place: consumers
        // (grants `link_duplicates`) persist the pairs they are handed.
        let rows: Vec<(String, i64)> = (0..150)
            .map(|i| (format!("k{i:03}"), 0x0f1e_2d3c_4b5a_6978u64 as i64))
            .collect();
        let banded = banded_duplicate_pairs(&rows, 3);
        let reference = all_pairs_scan(&rows, 3);
        assert_eq!(banded.len(), MAX_DUP_PAIRS, "cap must still bind");
        assert_eq!(as_triples(&banded), as_triples(&reference));
    }

    #[test]
    fn content_free_records_are_never_reported_as_duplicates_of_each_other() {
        // simhash 0 means "no textual content", not "identical content". Two
        // empty records are not duplicates, and the banded index must not bucket
        // them together — the regression a naive "index everything" port makes.
        let rows = vec![
            ("empty-1".to_string(), 0),
            ("empty-2".to_string(), 0),
            ("real".to_string(), 0x00ff_00ff_00ff_00ffu64 as i64),
        ];
        assert!(banded_duplicate_pairs(&rows, 20).is_empty());
    }

    #[test]
    fn content_free_rows_shift_index_slots_without_shifting_the_pair_walk() {
        // simhash-0 rows are not indexed, so an index slot is a COMPACTION of
        // the row index. Using the row index as the "already covered" threshold
        // then skips real neighbours (here: the three leading empties would hide
        // every pair among the first rows after them). The fixture front-loads
        // the empties so the two indexes are maximally out of step.
        let h: u64 = 0x0f1e_2d3c_4b5a_6978;
        let rows = vec![
            ("empty-1".to_string(), 0),
            ("empty-2".to_string(), 0),
            ("empty-3".to_string(), 0),
            ("a".to_string(), h as i64),
            ("b".to_string(), (h ^ 1) as i64),
            ("c".to_string(), (h ^ 0b11) as i64),
        ];
        assert_eq!(
            as_triples(&banded_duplicate_pairs(&rows, 3)),
            as_triples(&all_pairs_scan(&rows, 3))
        );
        assert_eq!(banded_duplicate_pairs(&rows, 3).len(), 3, "a-b, a-c, b-c");
    }
}
