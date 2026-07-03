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
}

/// A near-duplicate record pair and their SimHash Hamming distance.
#[derive(Debug, Clone, Serialize)]
pub struct DupPair {
    pub a: String,
    pub b: String,
    pub distance: u32,
}

/// Outcome of upserting a batch: the fresh records, plus a count of unchanged.
#[derive(Debug, Default, Serialize)]
pub struct UpsertSummary {
    pub new: Vec<String>,
    pub changed: Vec<String>,
    pub unchanged: usize,
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
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT hash FROM records WHERE app = ?1 AND dataset = ?2 AND key = ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        match existing {
            Some(prev) if prev == hash => {
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
            Some(_) => {
                sqlx::query(
                    "UPDATE records SET hash = ?4, data = ?5, simhash = ?6, last_seen = ?7, \
                     updated_at = ?7 WHERE app = ?1 AND dataset = ?2 AND key = ?3",
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
                Ok(ChangeKind::New)
            }
        }
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
            "SELECT key, data, first_seen, last_seen, updated_at \
             FROM records WHERE app = ?1 AND dataset = ?2 AND key = ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Record::try_from).transpose()
    }

    /// Lists records in a dataset, most-recently-updated first.
    pub async fn list(&self, app: &str, dataset: &str, limit: i64) -> Result<Vec<Record>> {
        let rows: Vec<RecordRow> = sqlx::query_as(
            "SELECT key, data, first_seen, last_seen, updated_at \
             FROM records WHERE app = ?1 AND dataset = ?2 ORDER BY updated_at DESC LIMIT ?3",
        )
        .bind(app)
        .bind(dataset)
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
        })
    }
}

/// serde_json's default `Map` is a `BTreeMap`, so `to_string` emits keys in
/// sorted order — a stable canonical form to hash.
fn hash_value(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
}

fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| Error::Parse(format!("bad timestamp '{s}': {e}")))
}
