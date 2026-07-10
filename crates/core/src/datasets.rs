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
        let existing: Option<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT hash, data, removed_at FROM records WHERE app = ?1 AND dataset = ?2 AND key = ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        match existing {
            Some((prev, _, removed_at)) if prev == hash && removed_at.is_none() => {
                sqlx::query(
                    "UPDATE records SET last_seen = ?4 WHERE app = ?1 AND dataset = ?2 AND key = ?3",
                )
                .bind(app)
                .bind(dataset)
                .bind(key)
                .bind(ts(now))
                .execute(&self.pool)
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
                .bind(&hash)
                .bind(value.to_string())
                .bind(sim)
                .bind(ts(now))
                .execute(&self.pool)
                .await?;
                let old: Value = serde_json::from_str(&old_data).unwrap_or(Value::Null);
                let diff = diff_values(&old, value);
                self.add_revision(app, dataset, key, "changed", Some(value), Some(&diff), now)
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
                .bind(&hash)
                .bind(value.to_string())
                .bind(sim)
                .bind(ts(now))
                .execute(&self.pool)
                .await?;
                self.add_revision(app, dataset, key, "new", Some(value), None, now)
                    .await?;
                Ok(ChangeKind::New)
            }
        }
    }

    /// Appends the next revision for a record (revision numbers are per-key,
    /// starting at 1).
    async fn add_revision(
        &self,
        app: &str,
        dataset: &str,
        key: &str,
        change: &str,
        data: Option<&Value>,
        diff: Option<&Value>,
        when: DateTime<Utc>,
    ) -> Result<()> {
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
        .execute(&self.pool)
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
        let live: Vec<String> = sqlx::query_scalar(
            "SELECT key FROM records WHERE app = ?1 AND dataset = ?2 AND removed_at IS NULL",
        )
        .bind(app)
        .bind(dataset)
        .fetch_all(&self.pool)
        .await?;
        let present: std::collections::HashSet<&str> =
            present.iter().map(String::as_str).collect();
        let now = Utc::now();
        let mut removed = Vec::new();
        for key in live {
            if present.contains(key.as_str()) {
                continue;
            }
            sqlx::query(
                "UPDATE records SET removed_at = ?4 WHERE app = ?1 AND dataset = ?2 AND key = ?3",
            )
            .bind(app)
            .bind(dataset)
            .bind(&key)
            .bind(ts(now))
            .execute(&self.pool)
            .await?;
            self.add_revision(app, dataset, &key, "removed", None, None, now)
                .await?;
            removed.push(key);
        }
        Ok(removed)
    }

    /// Upserts many records, returning a summary of new/changed/unchanged.
    pub async fn upsert_many(
        &self,
        app: &str,
        dataset: &str,
        items: &[(String, Value)],
    ) -> Result<UpsertSummary> {
        let mut summary = UpsertSummary::default();
        for (key, value) in items {
            match self.upsert(app, dataset, key, value).await? {
                ChangeKind::New => summary.new.push(key.clone()),
                ChangeKind::Changed => summary.changed.push(key.clone()),
                ChangeKind::Unchanged => summary.unchanged += 1,
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
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT key, simhash FROM records WHERE app = ?1 AND dataset = ?2")
                .bind(app)
                .bind(dataset)
                .fetch_all(&self.pool)
                .await?;
        let mut pairs = Vec::new();
        for i in 0..rows.len() {
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
                }
            }
        }
        pairs.sort_by_key(|p| p.distance);
        Ok(pairs)
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
