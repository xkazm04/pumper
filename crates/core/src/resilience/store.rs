//! Persistence for extraction health, and the [`Resilience`] service that ties
//! the detector to it.
//!
//! Write shape, per run: one `source_runs` row, one `field_sketches` row per
//! field, and one `doc_fingerprints` upsert per key. The last is the only new
//! per-record write, and it goes through the chunked-transaction pattern on one
//! held connection — never one transaction per row, which is how a 5k-record
//! batch turns into 5k commits and stalls every other app's worker.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::config::ResilienceConfig;
use crate::datasets::ts;
use crate::Result;

use super::detect::{self, Baseline, InvariantCheck};
use super::invariants::{self, Invariant};
use super::sketch::{self, FieldSketch, CLS, LEN_BUCKETS, MINHASH_K};
use super::{
    source_id, CohortDrift, Diagnosis, DocSignals, ObservedDoc, RunReport, RunVerdict, SourceState,
    SourceVerdict,
};

/// Rows per write transaction, matching the dataset store's `UPSERT_CHUNK`: a
/// few tens of ms of work per commit, well inside the 5s `busy_timeout`.
const WRITE_CHUNK: usize = 500;

/// Live records sampled when mining invariants. Enough for the confidence and
/// support thresholds to mean something without reading a whole corpus.
const MINE_SAMPLE: i64 = 2000;

/// One source's health row — what `GET /sources` serves.
#[derive(Debug, Clone, Serialize)]
pub struct SourceHealth {
    pub id: String,
    pub app: String,
    pub dataset: String,
    pub state: SourceState,
    pub degradation_score: f64,
    pub state_since: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_verdict: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_verdict_at: Option<String>,
    pub tripped_of_last3: i64,
    /// Whether this source has ever produced a cohort large enough to be judged
    /// statistically, over its retained runs.
    ///
    /// `false` is **not** the same as unhealthy — it means *unmonitored*: the
    /// distributional tests have never applied, so `state: "healthy"` on this row
    /// says only "nothing conclusive has fired", which for a source this thin is
    /// close to saying nothing at all.
    pub monitored: bool,
    pub updated_at: String,
}

/// One recorded run — what `GET /sources/{id}/runs` serves.
#[derive(Debug, Clone, Serialize)]
pub struct SourceRun {
    pub job_id: String,
    pub docs: i64,
    pub fetch_ok_rate: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub d_text: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub d_dom: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub d_val: Option<f64>,
    pub compared: i64,
    pub verdict: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnosis: Option<String>,
    pub score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasons: Option<Value>,
    pub state_after: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    pub created_at: String,
}

/// SQLite persistence for the health tables.
pub struct HealthStore {
    pool: SqlitePool,
    /// The cohort floor the `monitored` flag is computed against, carried here
    /// so every read answers "is this source actually being watched" with the
    /// same number the detector judged its runs by.
    min_cohort_docs: u32,
}

impl HealthStore {
    pub fn new(pool: SqlitePool, min_cohort_docs: u32) -> Self {
        Self {
            pool,
            min_cohort_docs,
        }
    }

    /// The source's row, creating it on first sight.
    pub async fn ensure_source(&self, app: &str, dataset: &str) -> Result<SourceHealth> {
        let id = source_id(app, dataset);
        let now = ts(Utc::now());
        sqlx::query(
            "INSERT INTO sources (id, app, dataset, state, degradation_score, state_since, \
                                  created_at, updated_at) \
             VALUES (?1, ?2, ?3, 'healthy', 0, ?4, ?4, ?4) \
             ON CONFLICT(id) DO NOTHING",
        )
        .bind(&id)
        .bind(app)
        .bind(dataset)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        // The INSERT is a no-op for an existing source, so read back rather than
        // assuming: the caller needs the *current* state, not a fresh one.
        self.source(&id).await?.ok_or_else(|| {
            crate::Error::App(format!("source '{id}' vanished between insert and read"))
        })
    }

    pub async fn source(&self, id: &str) -> Result<Option<SourceHealth>> {
        let row: Option<SourceRow> = sqlx::query_as(SOURCE_COLUMNS_SQL)
            .bind(id)
            .bind(self.min_cohort_docs as i64)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(SourceHealth::from))
    }

    /// Just the state — the read on the hot path of every gated write, so it
    /// stays one indexed lookup.
    ///
    /// A source with **no row** is `Healthy`: it has never reported a run, which
    /// is the ordinary state of a source on its first pass and says nothing bad.
    /// A row whose state string this build cannot parse is
    /// [`SourceState::Unknown`] — that one is a read that failed, not a source
    /// that is fine.
    pub async fn state(&self, app: &str, dataset: &str) -> Result<SourceState> {
        let state: Option<String> = sqlx::query_scalar("SELECT state FROM sources WHERE id = ?1")
            .bind(source_id(app, dataset))
            .fetch_optional(&self.pool)
            .await?;
        Ok(state
            .as_deref()
            .map(SourceState::parse)
            .unwrap_or(SourceState::Healthy))
    }

    /// Health rows, optionally filtered by state or app, worst state first.
    pub async fn list_sources(
        &self,
        state: Option<&str>,
        app: Option<&str>,
        limit: i64,
    ) -> Result<Vec<SourceHealth>> {
        let rows: Vec<SourceRow> = sqlx::query_as(&format!(
            "{SOURCE_COLUMNS_LIST_SQL} \
             WHERE (?1 IS NULL OR state = ?1) AND (?2 IS NULL OR app = ?2) \
             ORDER BY degradation_score DESC, id ASC LIMIT ?3"
        ))
        .bind(state)
        .bind(app)
        .bind(limit)
        .bind(self.min_cohort_docs as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(SourceHealth::from).collect())
    }

    /// Manual state override — the unquarantine / retire switch. Recorded with a
    /// reason because the only other thing that moves state is the detector.
    pub async fn set_state_manual(
        &self,
        id: &str,
        state: SourceState,
        reason: &str,
    ) -> Result<bool> {
        let now = ts(Utc::now());
        let affected = sqlx::query(
            "UPDATE sources SET state = ?2, state_since = ?3, state_reason = ?4, \
                                tripped_of_last3 = 0, updated_at = ?3 WHERE id = ?1",
        )
        .bind(id)
        .bind(state.as_str())
        .bind(&now)
        .bind(reason)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected > 0)
    }

    /// A source's recorded runs, newest first.
    pub async fn runs(&self, id: &str, limit: i64) -> Result<Vec<SourceRun>> {
        let rows: Vec<RunRow> = sqlx::query_as(
            "SELECT job_id, docs, fetch_ok_rate, d_text, d_dom, d_val, compared, verdict, \
                    diagnosis, score, reasons, state_after, build_id, created_at \
             FROM source_runs WHERE source_id = ?1 ORDER BY created_at DESC LIMIT ?2",
        )
        .bind(id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(SourceRun::from).collect())
    }

    /// Tripped runs among the last `n` *judged* runs. Derived from the stored
    /// rows rather than accumulated in a counter, so it cannot drift out of sync
    /// with the history it claims to summarize.
    async fn recent_trips(&self, id: &str, n: i64, degrade_score: f64) -> Result<u32> {
        let scores: Vec<f64> = sqlx::query_scalar(
            "SELECT score FROM source_runs WHERE source_id = ?1 \
             AND verdict IN ('ok', 'broken', 'self_inflicted') \
             ORDER BY created_at DESC LIMIT ?2",
        )
        .bind(id)
        .bind(n)
        .fetch_all(&self.pool)
        .await?;
        Ok(scores.iter().filter(|s| **s >= degrade_score).count() as u32)
    }

    /// Consecutive clean **judged** runs since `since` (the source's
    /// `state_since`), newest first — the evidence that climbs the ladder back
    /// up.
    ///
    /// Scoped to runs *after* the transition (`created_at > since`, so the run
    /// that caused the transition is never re-counted toward leaving it) and
    /// stopped at the first tripped run, so a source that trips once mid-recovery
    /// starts its streak over. Each rung therefore costs `recovery_runs` runs of
    /// its own. Runs the
    /// detector could not judge are excluded by the same verdict filter
    /// `recent_trips` uses: a source must never heal on evidence nobody looked at.
    async fn clean_streak_before(
        &self,
        id: &str,
        since: &str,
        degrade_score: f64,
        limit: i64,
    ) -> Result<u32> {
        let scores: Vec<f64> = sqlx::query_scalar(
            "SELECT score FROM source_runs WHERE source_id = ?1 \
             AND verdict IN ('ok', 'broken', 'self_inflicted') AND created_at > ?2 \
             ORDER BY created_at DESC LIMIT ?3",
        )
        .bind(id)
        .bind(since)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(detect::consecutive_clean(&scores, degrade_score))
    }

    /// The rolling baseline: per-field sketches from the last `window` runs this
    /// source was judged `ok`, newest first.
    pub async fn baseline(&self, id: &str, window: u32) -> Result<Baseline> {
        let job_ids: Vec<String> = sqlx::query_scalar(
            "SELECT job_id FROM source_runs WHERE source_id = ?1 AND verdict = 'ok' \
             ORDER BY created_at DESC LIMIT ?2",
        )
        .bind(id)
        .bind(window as i64)
        .fetch_all(&self.pool)
        .await?;
        if job_ids.is_empty() {
            return Ok(Baseline::default());
        }
        // Newest-first ordering comes from the job_id list, not from a second
        // ORDER BY: the sketch rows carry no run timestamp of their own.
        let order: BTreeMap<&str, usize> = job_ids
            .iter()
            .enumerate()
            .map(|(i, j)| (j.as_str(), i))
            .collect();

        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT job_id, field, n, matched, empty, error, container_empty, coerced, \
                    coercion_failed, len_sum, len_sumsq, len_hist, cls, distinct_ratio, minhash \
             FROM field_sketches WHERE source_id = ",
        );
        qb.push_bind(id);
        qb.push(" AND job_id IN (");
        let mut sep = qb.separated(", ");
        for job_id in &job_ids {
            sep.push_bind(job_id);
        }
        qb.push(")");
        let rows: Vec<SketchRow> = qb.build_query_as().fetch_all(&self.pool).await?;

        let mut per_field: BTreeMap<String, Vec<(usize, FieldSketch)>> = BTreeMap::new();
        for row in rows {
            let rank = order
                .get(row.job_id.as_str())
                .copied()
                .unwrap_or(usize::MAX);
            per_field
                .entry(row.field.clone())
                .or_default()
                .push((rank, row.into()));
        }
        let mut baseline = Baseline::default();
        for (field, mut runs) in per_field {
            runs.sort_by_key(|(rank, _)| *rank);
            baseline
                .fields
                .insert(field, runs.into_iter().map(|(_, s)| s).collect());
        }
        Ok(baseline)
    }

    /// This run's per-field sketches, for the API's "run vs baseline" view.
    pub async fn run_sketches(
        &self,
        id: &str,
        job_id: &str,
    ) -> Result<BTreeMap<String, FieldSketch>> {
        let rows: Vec<SketchRow> = sqlx::query_as(
            "SELECT job_id, field, n, matched, empty, error, container_empty, coerced, \
                    coercion_failed, len_sum, len_sumsq, len_hist, cls, distinct_ratio, minhash \
             FROM field_sketches WHERE source_id = ?1 AND job_id = ?2",
        )
        .bind(id)
        .bind(job_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.field.clone(), r.into()))
            .collect())
    }

    /// Stored fingerprints for the keys this run saw. Chunked because the key set
    /// is the size of the batch and SQLite binds one parameter per key.
    pub async fn fingerprints(
        &self,
        id: &str,
        keys: &[String],
    ) -> Result<BTreeMap<String, DocSignals>> {
        let mut out = BTreeMap::new();
        for chunk in keys.chunks(WRITE_CHUNK) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT key, text_simhash, dom_simhash, val_simhash FROM doc_fingerprints \
                 WHERE source_id = ",
            );
            qb.push_bind(id);
            qb.push(" AND key IN (");
            let mut sep = qb.separated(", ");
            for key in chunk {
                sep.push_bind(key);
            }
            qb.push(")");
            let rows: Vec<(String, i64, i64, i64)> =
                qb.build_query_as().fetch_all(&self.pool).await?;
            for (key, text, dom, val) in rows {
                out.insert(
                    key,
                    DocSignals {
                        text_simhash: text as u64,
                        dom_simhash: dom as u64,
                        val_simhash: val as u64,
                    },
                );
            }
        }
        Ok(out)
    }

    /// Upserts this run's fingerprints in chunked transactions on one held
    /// connection.
    pub async fn put_fingerprints(&self, id: &str, docs: &[ObservedDoc]) -> Result<()> {
        if docs.is_empty() {
            return Ok(());
        }
        let now = ts(Utc::now());
        let mut conn = self.pool.acquire().await?;
        for chunk in docs.chunks(WRITE_CHUNK) {
            sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
            let mut result: Result<()> = Ok(());
            for doc in chunk {
                let write = sqlx::query(
                    "INSERT INTO doc_fingerprints (source_id, key, text_simhash, dom_simhash, \
                                                   val_simhash, seen_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                     ON CONFLICT(source_id, key) DO UPDATE SET \
                       text_simhash = excluded.text_simhash, dom_simhash = excluded.dom_simhash, \
                       val_simhash = excluded.val_simhash, seen_at = excluded.seen_at",
                )
                .bind(id)
                .bind(&doc.key)
                .bind(doc.signals.text_simhash as i64)
                .bind(doc.signals.dom_simhash as i64)
                .bind(doc.signals.val_simhash as i64)
                .bind(&now)
                .execute(&mut *conn)
                .await;
                if let Err(e) = write {
                    result = Err(e.into());
                    break;
                }
            }
            match result {
                Ok(()) => {
                    sqlx::query("COMMIT").execute(&mut *conn).await?;
                }
                Err(e) => {
                    let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Writes the run row, its sketches, and the source's new state as one unit —
    /// a state transition with no run row explaining it is unauditable.
    #[allow(clippy::too_many_arguments)]
    async fn commit_run(
        &self,
        id: &str,
        job_id: Uuid,
        report: &RunReport<'_>,
        eval: &detect::RunEvaluation,
        sketches: &BTreeMap<String, FieldSketch>,
        state: SourceState,
        state_changed: bool,
        trips: u32,
    ) -> Result<()> {
        let now = ts(Utc::now());
        let job = job_id.to_string();
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let outcome: Result<()> = async {
            sqlx::query(
                "INSERT OR REPLACE INTO source_runs (source_id, job_id, docs, fetch_ok_rate, \
                     d_text, d_dom, d_val, compared, verdict, diagnosis, score, reasons, \
                     state_after, build_id, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            )
            .bind(id)
            .bind(&job)
            .bind(report.docs.len() as i64)
            .bind(report.fetch.rate())
            .bind(eval.drift.map(|d| d.text))
            .bind(eval.drift.map(|d| d.dom))
            .bind(eval.drift.map(|d| d.value))
            .bind(eval.drift.map_or(0, |d| d.compared) as i64)
            .bind(eval.verdict.as_str())
            .bind(eval.diagnosis.map(Diagnosis::as_str))
            .bind(eval.score)
            .bind(serde_json::to_string(&eval.reasons).ok())
            .bind(state.as_str())
            .bind(report.build_id.as_deref())
            .bind(&now)
            .execute(&mut *conn)
            .await?;

            for chunk in sketches.iter().collect::<Vec<_>>().chunks(WRITE_CHUNK) {
                for (field, sketch) in chunk {
                    sqlx::query(
                        "INSERT OR REPLACE INTO field_sketches (source_id, job_id, field, n, \
                             matched, empty, error, container_empty, coerced, coercion_failed, \
                             len_sum, len_sumsq, len_hist, cls, distinct_ratio, minhash, created_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                    )
                    .bind(id)
                    .bind(&job)
                    .bind(field.as_str())
                    .bind(sketch.n as i64)
                    .bind(sketch.matched as i64)
                    .bind(sketch.empty as i64)
                    .bind(sketch.error as i64)
                    .bind(sketch.container_empty as i64)
                    .bind(sketch.coerced as i64)
                    .bind(sketch.coercion_failed as i64)
                    .bind(sketch.len_sum)
                    .bind(sketch.len_sumsq)
                    .bind(encode_u16(&sketch.len_hist))
                    .bind(encode_f32(&sketch.cls))
                    .bind(sketch.distinct_ratio as f64)
                    .bind(encode_u64(&sketch.minhash))
                    .bind(&now)
                    .execute(&mut *conn)
                    .await?;
                }
            }

            // `state_since` only moves on an actual transition, so "how long has
            // this been degraded" stays answerable.
            sqlx::query(
                "UPDATE sources SET state = ?2, degradation_score = ?3, \
                    state_since = CASE WHEN ?4 THEN ?5 ELSE state_since END, \
                    state_reason = CASE WHEN ?4 THEN NULL ELSE state_reason END, \
                    last_verdict = ?6, last_verdict_at = ?5, tripped_of_last3 = ?7, \
                    updated_at = ?5 WHERE id = ?1",
            )
            .bind(id)
            .bind(state.as_str())
            .bind(eval.score)
            .bind(state_changed)
            .bind(&now)
            .bind(eval.verdict.as_str())
            .bind(trips as i64)
            .execute(&mut *conn)
            .await?;
            Ok(())
        }
        .await;
        match outcome {
            Ok(()) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
                Ok(())
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    pub async fn invariants(&self, id: &str) -> Result<Vec<Invariant>> {
        let rows: Vec<(String, String, i64, f64)> = sqlx::query_as(
            "SELECT field, spec, support, confidence FROM field_invariants WHERE source_id = ?1",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|(field, spec, support, confidence)| {
                // A spec that no longer deserializes (an older shape) is dropped
                // rather than failing the run — it will be re-mined.
                let kind = serde_json::from_str(&spec).ok()?;
                Some(Invariant {
                    field,
                    kind,
                    support: support as u32,
                    confidence,
                })
            })
            .collect())
    }

    /// Replaces a source's invariants. Whole-set replacement because a stale
    /// invariant that no longer holds is worse than a missing one: it would score
    /// a violation against a regularity the source has abandoned.
    pub async fn put_invariants(&self, id: &str, invariants: &[Invariant]) -> Result<()> {
        let now = ts(Utc::now());
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let outcome: Result<()> = async {
            sqlx::query("DELETE FROM field_invariants WHERE source_id = ?1")
                .bind(id)
                .execute(&mut *conn)
                .await?;
            for inv in invariants {
                sqlx::query(
                    "INSERT OR REPLACE INTO field_invariants (source_id, field, kind, spec, \
                         support, confidence, learned_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )
                .bind(id)
                .bind(&inv.field)
                .bind(inv.kind.name())
                .bind(serde_json::to_string(&inv.kind).unwrap_or_default())
                .bind(inv.support as i64)
                .bind(inv.confidence)
                .bind(&now)
                .execute(&mut *conn)
                .await?;
            }
            Ok(())
        }
        .await;
        match outcome {
            Ok(()) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
                Ok(())
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    /// When the source's invariants were last mined.
    async fn invariants_learned_at(&self, id: &str) -> Result<Option<DateTime<Utc>>> {
        let latest: Option<String> =
            sqlx::query_scalar("SELECT MAX(learned_at) FROM field_invariants WHERE source_id = ?1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .flatten();
        Ok(latest
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|d| d.with_timezone(&Utc)))
    }

    /// A sample of a source's live records, for mining.
    async fn sample_records(&self, app: &str, dataset: &str) -> Result<Vec<Value>> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT data FROM records WHERE app = ?1 AND dataset = ?2 AND removed_at IS NULL \
             ORDER BY updated_at DESC LIMIT ?3",
        )
        .bind(app)
        .bind(dataset)
        .bind(MINE_SAMPLE)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .filter_map(|d| serde_json::from_str(d).ok())
            .collect())
    }

    // ── The repair audit trail (N12 §6.4, §8.3) ────────────────────────────
    //
    // Every candidate and every verdict is written, accepted or not. The
    // design's own falsifier — "of PROMOTED repairs, the fraction that
    // reproduce the pre-mutation values" — is uncomputable unless the
    // rejections sit on the record beside the promotions, so a store that kept
    // only successes would make the safety argument unfalsifiable by
    // construction.

    /// Binds a source to the profile a repair would write back to. `None`
    /// clears it, which is how a source reverts to `repairable: false`.
    pub async fn set_source_profile(&self, source_id: &str, profile: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE sources SET profile = ?2, updated_at = ?3 WHERE id = ?1")
            .bind(source_id)
            .bind(profile)
            .bind(ts(Utc::now()))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// The anti-oscillation state of one source, as of `now`.
    ///
    /// `promotions_30d` is **recomputed from the attempt ledger**, not read
    /// from the counter column: a counter has to be reset by somebody, and a
    /// reset that does not happen turns a 30-day budget into a permanent ban.
    /// The column is refreshed from the same number so `GET /sources` and this
    /// call can never disagree.
    pub async fn repair_guard(&self, source_id: &str, now: DateTime<Utc>) -> Result<RepairGuard> {
        let row: Option<(Option<String>, Option<String>)> =
            sqlx::query_as("SELECT repair_blocked_until, profile FROM sources WHERE id = ?1")
                .bind(source_id)
                .fetch_optional(&self.pool)
                .await?;
        let (blocked_until, profile) = row.unwrap_or((None, None));
        let since = ts(now - Duration::days(30));
        let promotions_30d: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM repair_attempts \
             WHERE source_id = ?1 AND outcome = 'promoted' AND created_at >= ?2",
        )
        .bind(source_id)
        .bind(&since)
        .fetch_one(&self.pool)
        .await?;
        sqlx::query("UPDATE sources SET promotions_30d = ?2 WHERE id = ?1")
            .bind(source_id)
            .bind(promotions_30d)
            .execute(&self.pool)
            .await?;
        let blocked = blocked_until
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc));
        Ok(RepairGuard {
            blocked_until: blocked,
            blocked: blocked.is_some_and(|b| b > now),
            promotions_30d: promotions_30d.max(0) as u32,
            profile,
        })
    }

    /// Blocks further repair of this source until `until` — the post-rollback
    /// cooldown.
    pub async fn block_repair(&self, source_id: &str, until: DateTime<Utc>) -> Result<()> {
        sqlx::query("UPDATE sources SET repair_blocked_until = ?2, updated_at = ?3 WHERE id = ?1")
            .bind(source_id)
            .bind(ts(until))
            .bind(ts(Utc::now()))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Opens a repair attempt and returns its id.
    pub async fn start_repair_attempt(
        &self,
        source_id: &str,
        job_id: Option<&str>,
        diagnosis: &str,
        diagnosis_hash: &str,
        stage: &str,
    ) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO repair_attempts \
                 (id, source_id, job_id, diagnosis, diagnosis_hash, stage, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(&id)
        .bind(source_id)
        .bind(job_id)
        .bind(diagnosis)
        .bind(diagnosis_hash)
        .bind(stage)
        .bind(ts(Utc::now()))
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// Records one candidate and the verdict every gate returned.
    pub async fn record_repair_candidate(
        &self,
        attempt_id: &str,
        idx: i64,
        origin: &str,
        rules: &Value,
        verdict: &super::repair::CandidateVerdict,
        profile_version: Option<i64>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO repair_candidates \
                 (attempt_id, idx, origin, rules, holdout_match_rate, golden_exact, golden_total, \
                  invariant_violations, agreement_group, lint, verdict, evidence, profile_version, \
                  created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        )
        .bind(attempt_id)
        .bind(idx)
        .bind(origin)
        .bind(rules.to_string())
        .bind(verdict.holdout.rate)
        .bind(verdict.golden.exact_ok as i64)
        .bind(verdict.golden.checked as i64)
        .bind(verdict.invariant_violations.len() as i64)
        .bind(verdict.agreement.group as i64)
        .bind(serde_json::to_string(&verdict.lint).unwrap_or_else(|_| "[]".into()))
        .bind(
            verdict
                .rejected_for
                .clone()
                .unwrap_or_else(|| "accepted".to_string()),
        )
        .bind(serde_json::to_string(verdict).ok())
        .bind(profile_version)
        .bind(ts(Utc::now()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Consecutive clean shadow runs for one diagnosis, newest first.
    ///
    /// Counted from the attempt ledger rather than kept as a streak counter,
    /// for the same reason `promotions_30d` is: a counter has to be reset by
    /// somebody, and the run that fails to reset it is the run that promotes on
    /// a streak that never happened. The count stops at the first attempt for
    /// this diagnosis whose outcome was not `shadow_clean`, so one bad run
    /// zeroes the streak without anything having to write a zero.
    pub async fn consecutive_clean_shadow_runs(
        &self,
        source_id: &str,
        diagnosis_hash: &str,
    ) -> Result<u32> {
        let outcomes: Vec<Option<String>> = sqlx::query_scalar(
            "SELECT outcome FROM repair_attempts              WHERE source_id = ?1 AND diagnosis_hash = ?2              ORDER BY created_at DESC LIMIT 100",
        )
        .bind(source_id)
        .bind(diagnosis_hash)
        .fetch_all(&self.pool)
        .await?;
        Ok(outcomes
            .iter()
            .take_while(|o| o.as_deref() == Some("shadow_clean"))
            .count() as u32)
    }

    /// Closes an attempt with its outcome.
    pub async fn finish_repair_attempt(
        &self,
        attempt_id: &str,
        stage: &str,
        outcome: &str,
        promoted_version: Option<i64>,
        cost_usd: f64,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE repair_attempts \
             SET stage = ?2, outcome = ?3, promoted_version = ?4, cost_usd = ?5, finished_at = ?6 \
             WHERE id = ?1",
        )
        .bind(attempt_id)
        .bind(stage)
        .bind(outcome)
        .bind(promoted_version)
        .bind(cost_usd)
        .bind(ts(Utc::now()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// One source's repair attempts, newest first.
    pub async fn repair_attempts(&self, source_id: &str, limit: i64) -> Result<Vec<RepairAttempt>> {
        #[allow(clippy::type_complexity)]
        let rows: Vec<(
            String,
            String,
            String,
            String,
            Option<String>,
            Option<i64>,
            f64,
            String,
            Option<String>,
        )> = sqlx::query_as(
            "SELECT id, diagnosis, diagnosis_hash, stage, outcome, promoted_version, cost_usd, \
                    created_at, finished_at \
             FROM repair_attempts WHERE source_id = ?1 ORDER BY created_at DESC LIMIT ?2",
        )
        .bind(source_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    diagnosis,
                    diagnosis_hash,
                    stage,
                    outcome,
                    promoted_version,
                    cost_usd,
                    created_at,
                    finished_at,
                )| RepairAttempt {
                    id,
                    source_id: source_id.to_string(),
                    diagnosis,
                    diagnosis_hash,
                    stage,
                    outcome,
                    promoted_version,
                    cost_usd,
                    created_at,
                    finished_at,
                },
            )
            .collect())
    }

    /// The candidates of one attempt, in proposal order.
    pub async fn repair_candidates(&self, attempt_id: &str) -> Result<Vec<Value>> {
        let rows: Vec<(i64, String, String, Option<String>)> = sqlx::query_as(
            "SELECT idx, origin, verdict, evidence FROM repair_candidates \
             WHERE attempt_id = ?1 ORDER BY idx",
        )
        .bind(attempt_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(idx, origin, verdict, evidence)| {
                serde_json::json!({
                    "idx": idx,
                    "origin": origin,
                    "verdict": verdict,
                    "evidence": evidence.and_then(|e| serde_json::from_str::<Value>(&e).ok()),
                })
            })
            .collect())
    }

    /// Stamps which profile version produced a recorded run.
    ///
    /// Separate from `observe` on purpose. `RunReport` is built by the app, and
    /// the app that knows the profile version is the one that resolved it —
    /// which is not necessarily the one reporting the run. Writing it as its own
    /// UPDATE keeps the stamp additive: a run nobody stamped keeps
    /// `profile_version = NULL`, which means "not profile-backed", not "version
    /// zero".
    ///
    /// Returns whether a row was actually stamped, so a caller can tell a
    /// stamped run from a run that had already been pruned.
    pub async fn stamp_profile_version(
        &self,
        source_id: &str,
        job_id: &str,
        version: i64,
    ) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE source_runs SET profile_version = ?3 WHERE source_id = ?1 AND job_id = ?2",
        )
        .bind(source_id)
        .bind(job_id)
        .bind(version)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    /// The profile version a recorded run ran under, when one was stamped.
    pub async fn run_profile_version(&self, source_id: &str, job_id: &str) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, Option<i64>>(
            "SELECT profile_version FROM source_runs WHERE source_id = ?1 AND job_id = ?2",
        )
        .bind(source_id)
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?
        .flatten())
    }

    /// Drops sketches and run rows beyond the newest `keep` runs per source — the
    /// `prune_revisions` sibling the retention janitor calls. Returns rows removed.
    pub async fn prune(&self, keep: u32) -> Result<u64> {
        let keep = keep.max(1) as i64;
        // "More than `keep` runs of this source are at least as new as mine" is
        // the same correlated-subquery shape `prune_revisions` uses — no window
        // function, no DELETE-target alias, so it stays portable.
        let newer_than_mine = "(SELECT COUNT(*) FROM source_runs AS n \
             WHERE n.source_id = {t}.source_id \
               AND n.created_at >= (SELECT r.created_at FROM source_runs AS r \
                                    WHERE r.source_id = {t}.source_id AND r.job_id = {t}.job_id))";
        let sketches = sqlx::query(&format!(
            "DELETE FROM field_sketches WHERE {} > ?1",
            newer_than_mine.replace("{t}", "field_sketches")
        ))
        .bind(keep)
        .execute(&self.pool)
        .await?
        .rows_affected();
        let runs = sqlx::query(&format!(
            "DELETE FROM source_runs WHERE {} > ?1",
            newer_than_mine.replace("{t}", "source_runs")
        ))
        .bind(keep)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(sketches + runs)
    }
}

/// A source's anti-oscillation state, as of a moment.
#[derive(Debug, Clone, Serialize)]
pub struct RepairGuard {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_until: Option<DateTime<Utc>>,
    /// Whether the cooldown is live right now.
    pub blocked: bool,
    /// Promotions inside the trailing 30 days, recomputed from the ledger.
    pub promotions_30d: u32,
    /// The profile a repair would write back to; `None` = `repairable: false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

/// One recorded repair attempt.
#[derive(Debug, Clone, Serialize)]
pub struct RepairAttempt {
    pub id: String,
    pub source_id: String,
    pub diagnosis: String,
    pub diagnosis_hash: String,
    pub stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub promoted_version: Option<i64>,
    pub cost_usd: f64,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

/// Extraction-health detection as an app-facing service: config plus the store.
///
/// Constructed once per process and shared through `AppContext`. A disabled
/// service ([`Resilience::disabled`]) is a complete no-op with no database
/// dependency at all, which is what embedders and unit tests get.
pub struct Resilience {
    cfg: ResilienceConfig,
    store: Option<HealthStore>,
}

impl Resilience {
    pub fn new(pool: SqlitePool, cfg: &ResilienceConfig) -> Self {
        Self {
            cfg: cfg.clone(),
            store: cfg
                .enabled
                .then(|| HealthStore::new(pool, cfg.min_cohort_docs)),
        }
    }

    /// A service that detects nothing and gates nothing.
    pub fn disabled() -> Self {
        Self {
            cfg: ResilienceConfig {
                enabled: false,
                ..ResilienceConfig::default()
            },
            store: None,
        }
    }

    pub fn config(&self) -> &ResilienceConfig {
        &self.cfg
    }

    pub fn enabled(&self) -> bool {
        self.store.is_some()
    }

    /// Whether verdicts are allowed to gate anything. `false` is the shipping
    /// default: compute and store everything, change nothing.
    pub fn enforcing(&self) -> bool {
        self.cfg.enabled && self.cfg.enforce
    }

    pub fn store(&self) -> Option<&HealthStore> {
        self.store.as_ref()
    }

    /// The source's state: `Healthy` when detection is off or the source has
    /// never reported a run, and [`SourceState::Unknown`] when the read itself
    /// failed.
    ///
    /// Fail-open is deliberate and load-bearing: this sits on the write path of
    /// every app, and a health lookup that errors must never be able to stop a
    /// working pipeline. The cost of failing open is one unsuppressed run; the
    /// cost of failing closed is the whole fleet stopping on a locked database.
    ///
    /// What changed is *what the failure is called*, not what it does. A failed
    /// read used to answer `Healthy` — a state with a meaning ("this source is
    /// producing what it always did") that a database error is in no position to
    /// claim, and one that reached the job result, the worker's logs and the
    /// preview as an ordinary green. `Unknown` gates nothing, stamps nothing and
    /// diverts nothing, so it is the identical fail-open, reported honestly.
    pub async fn state(&self, app: &str, dataset: &str) -> SourceState {
        let Some(store) = &self.store else {
            return SourceState::Healthy;
        };
        match store.state(app, dataset).await {
            Ok(state) => state,
            Err(e) => {
                tracing::warn!(
                    "health state read failed for {app}/{dataset}, reported as `unknown` \
                     (gates nothing, exactly as before): {e}"
                );
                SourceState::Unknown
            }
        }
    }

    /// The state that governs enforcement: always `Healthy` while `enforce` is
    /// off, so soak mode cannot gate anything even by accident.
    pub async fn enforced_state(&self, app: &str, dataset: &str) -> SourceState {
        if !self.enforcing() {
            return SourceState::Healthy;
        }
        self.state(app, dataset).await
    }

    /// Judges one run and records it: sketches, drifts, invariant checks, score,
    /// state transition. `Ok(None)` when detection is disabled.
    ///
    /// Call this **before** writing the batch. The state it returns is what the
    /// write must be gated on, and computing it afterwards would stamp trust and
    /// infer removals from a verdict that did not exist yet.
    pub async fn observe(&self, app: &str, run: &RunReport<'_>) -> Result<Option<SourceVerdict>> {
        let Some(store) = &self.store else {
            return Ok(None);
        };
        let id = source_id(app, run.dataset);
        let source = store.ensure_source(app, run.dataset).await?;
        let previous_state = source.state;

        let sketches = sketch::sketch_run(run.docs.iter().map(|d| (&d.values, &d.report)));
        let drift = self.cohort_drift(store, &id, run.docs).await?;
        let baseline = store.baseline(&id, self.cfg.window_runs).await?;
        let invariants = store.invariants(&id).await?;
        let checks: Vec<InvariantCheck> =
            invariants::check(&invariants, run.docs.iter().map(|d| &d.values), &sketches);

        let eval = detect::evaluate(
            &self.cfg,
            &detect::RunInput {
                docs: run.docs.len() as u32,
                fetch: run.fetch,
                sketches: &sketches,
                baseline: &baseline,
                invariants: &checks,
                drift,
            },
        );

        // An unjudged run moves nothing: not the state, not the trip count, and
        // not the recovery streak — a source cannot heal on a run nobody judged.
        let (state, trips) = if eval.verdict.judged() {
            let tripped = eval.tripped(&self.cfg);
            let prior = store.recent_trips(&id, 2, self.cfg.degrade_score).await?;
            let trips = prior + u32::from(tripped);
            // The streak is counted from the run rows, not accumulated in a
            // column, for the same reason `recent_trips` is: a counter drifts out
            // of sync with the history it summarizes. `state_since` is read from
            // the row as it stood *before* this run, so each rung costs a full
            // streak of its own.
            let clean_streak = if tripped {
                0
            } else {
                store
                    .clean_streak_before(
                        &id,
                        &source.state_since,
                        self.cfg.degrade_score,
                        self.cfg.recovery_runs as i64,
                    )
                    .await?
                    + 1
            };
            let recovery = detect::Recovery {
                clean_streak,
                required: self.cfg.recovery_runs,
            };
            (
                detect::next_state(
                    previous_state,
                    tripped,
                    eval.severe(&self.cfg),
                    trips,
                    recovery,
                ),
                trips,
            )
        } else {
            (previous_state, source.tripped_of_last3 as u32)
        };

        store
            .commit_run(
                &id,
                run.job_id,
                run,
                &eval,
                &sketches,
                state,
                state != previous_state,
                trips,
            )
            .await?;

        // Fingerprints are the reference the *next* run compares against, so a run
        // we could not judge must not become that reference: comparing tomorrow's
        // page against today's bot wall would read as a redesign.
        if eval.verdict != RunVerdict::Inconclusive {
            store.put_fingerprints(&id, run.docs).await?;
        }

        if eval.verdict.baselines() {
            self.maybe_mine(store, &id, app, run.dataset, &sketches)
                .await;
        }

        Ok(Some(SourceVerdict {
            source_id: id,
            verdict: eval.verdict,
            diagnosis: eval.diagnosis,
            score: eval.score,
            state,
            previous_state,
            statistical_coverage: eval.statistical_coverage,
            cohort: eval.adequacy,
            reasons: eval.reasons,
            drift: eval.drift,
        }))
    }

    /// Cohort drift: the median per-key drift over keys present in both this run
    /// and the last, or `None` when nothing is comparable.
    ///
    /// The median rather than the mean because a handful of genuinely-changed
    /// records is normal on every source, and a mean lets them carry the verdict.
    async fn cohort_drift(
        &self,
        store: &HealthStore,
        id: &str,
        docs: &[ObservedDoc],
    ) -> Result<Option<CohortDrift>> {
        let keys: Vec<String> = docs.iter().map(|d| d.key.clone()).collect();
        let prior = store.fingerprints(id, &keys).await?;
        if prior.is_empty() {
            return Ok(None);
        }
        let (mut text, mut dom, mut value) = (Vec::new(), Vec::new(), Vec::new());
        for doc in docs {
            let Some(before) = prior.get(&doc.key) else {
                continue;
            };
            text.push(crate::simhash::drift(
                before.text_simhash,
                doc.signals.text_simhash,
            ));
            dom.push(crate::simhash::drift(
                before.dom_simhash,
                doc.signals.dom_simhash,
            ));
            value.push(crate::simhash::drift(
                before.val_simhash,
                doc.signals.val_simhash,
            ));
        }
        if text.is_empty() {
            return Ok(None);
        }
        Ok(Some(CohortDrift {
            text: sketch::median(&text).unwrap_or(0.0),
            dom: sketch::median(&dom).unwrap_or(0.0),
            value: sketch::median(&value).unwrap_or(0.0),
            compared: text.len() as u32,
        }))
    }

    /// Re-mines invariants when they are stale, from live records only. Mining is
    /// best-effort: a failure warns and leaves the previous set in place, because
    /// the previous set is still evidence and an empty set is not.
    async fn maybe_mine(
        &self,
        store: &HealthStore,
        id: &str,
        app: &str,
        dataset: &str,
        sketches: &BTreeMap<String, FieldSketch>,
    ) {
        let due = match store.invariants_learned_at(id).await {
            Ok(None) => true,
            Ok(Some(at)) => {
                Utc::now() - at > Duration::days(self.cfg.invariant_refresh_days.max(1))
            }
            Err(e) => {
                tracing::warn!("invariant staleness check failed for {id}: {e}");
                return;
            }
        };
        if !due {
            return;
        }
        let fields: Vec<String> = sketches.keys().cloned().collect();
        match store.sample_records(app, dataset).await {
            Ok(records) if !records.is_empty() => {
                let mined = invariants::mine(&self.cfg, &records, &fields);
                // Nothing mined means the thresholds were not met; keeping the
                // previous set would let an expired invariant outlive its
                // evidence, so the empty result is written.
                if let Err(e) = store.put_invariants(id, &mined).await {
                    tracing::warn!("invariant write failed for {id}: {e}");
                } else {
                    tracing::debug!(source = id, mined = mined.len(), "mined field invariants");
                }
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("invariant mining sample failed for {id}: {e}"),
        }
    }
}

// ---- row mapping -----------------------------------------------------------

// `monitored` — "has this source ever produced a judgeable cohort" — is a
// correlated EXISTS over its retained runs. Derived rather than stored for the
// same reason `recent_trips` is: a cached flag drifts out of sync with the
// history it claims to summarize, and retention moves that history underneath it.
const SOURCE_COLUMNS_SQL: &str = "SELECT id, app, dataset, state, degradation_score, state_since, \
     state_reason, last_verdict, last_verdict_at, tripped_of_last3, \
     EXISTS (SELECT 1 FROM source_runs r WHERE r.source_id = sources.id AND r.docs >= ?2) \
        AS monitored, \
     updated_at \
     FROM sources WHERE id = ?1";

const SOURCE_COLUMNS_LIST_SQL: &str =
    "SELECT id, app, dataset, state, degradation_score, state_since, state_reason, last_verdict, \
     last_verdict_at, tripped_of_last3, \
     EXISTS (SELECT 1 FROM source_runs r WHERE r.source_id = sources.id AND r.docs >= ?4) \
        AS monitored, \
     updated_at FROM sources";

#[derive(sqlx::FromRow)]
struct SourceRow {
    id: String,
    app: String,
    dataset: String,
    state: String,
    degradation_score: f64,
    state_since: String,
    state_reason: Option<String>,
    last_verdict: Option<String>,
    last_verdict_at: Option<String>,
    tripped_of_last3: i64,
    monitored: i64,
    updated_at: String,
}

impl From<SourceRow> for SourceHealth {
    fn from(r: SourceRow) -> Self {
        Self {
            id: r.id,
            app: r.app,
            dataset: r.dataset,
            state: SourceState::parse(&r.state),
            degradation_score: r.degradation_score,
            state_since: r.state_since,
            state_reason: r.state_reason,
            last_verdict: r.last_verdict,
            last_verdict_at: r.last_verdict_at,
            tripped_of_last3: r.tripped_of_last3,
            monitored: r.monitored != 0,
            updated_at: r.updated_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct RunRow {
    job_id: String,
    docs: i64,
    fetch_ok_rate: f64,
    d_text: Option<f64>,
    d_dom: Option<f64>,
    d_val: Option<f64>,
    compared: i64,
    verdict: String,
    diagnosis: Option<String>,
    score: f64,
    reasons: Option<String>,
    state_after: String,
    build_id: Option<String>,
    created_at: String,
}

impl From<RunRow> for SourceRun {
    fn from(r: RunRow) -> Self {
        Self {
            job_id: r.job_id,
            docs: r.docs,
            fetch_ok_rate: r.fetch_ok_rate,
            d_text: r.d_text,
            d_dom: r.d_dom,
            d_val: r.d_val,
            compared: r.compared,
            verdict: r.verdict,
            diagnosis: r.diagnosis,
            score: r.score,
            reasons: r
                .reasons
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok()),
            state_after: r.state_after,
            build_id: r.build_id,
            created_at: r.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct SketchRow {
    job_id: String,
    field: String,
    n: i64,
    matched: i64,
    empty: i64,
    error: i64,
    container_empty: i64,
    coerced: i64,
    coercion_failed: i64,
    len_sum: f64,
    len_sumsq: f64,
    len_hist: Vec<u8>,
    cls: Vec<u8>,
    distinct_ratio: f64,
    minhash: Vec<u8>,
}

impl From<SketchRow> for FieldSketch {
    fn from(r: SketchRow) -> Self {
        Self {
            n: r.n as u32,
            matched: r.matched as u32,
            empty: r.empty as u32,
            error: r.error as u32,
            container_empty: r.container_empty as u32,
            coerced: r.coerced as u32,
            coercion_failed: r.coercion_failed as u32,
            len_sum: r.len_sum,
            len_sumsq: r.len_sumsq,
            len_hist: decode_u16(&r.len_hist),
            cls: decode_f32(&r.cls),
            distinct_ratio: r.distinct_ratio as f32,
            minhash: decode_u64(&r.minhash),
        }
    }
}

// Fixed-width little-endian encodings. Explicit rather than serde because these
// are hot, fixed-size, and stored as BLOBs: 32 + 16 + 512 bytes per sketch row.

fn encode_u16(values: &[u16; LEN_BUCKETS]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn decode_u16(bytes: &[u8]) -> [u16; LEN_BUCKETS] {
    let mut out = [0u16; LEN_BUCKETS];
    for (slot, chunk) in out.iter_mut().zip(bytes.chunks_exact(2)) {
        *slot = u16::from_le_bytes([chunk[0], chunk[1]]);
    }
    out
}

fn encode_f32(values: &[f32; CLS]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn decode_f32(bytes: &[u8]) -> [f32; CLS] {
    let mut out = [0f32; CLS];
    for (slot, chunk) in out.iter_mut().zip(bytes.chunks_exact(4)) {
        *slot = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
    }
    out
}

fn encode_u64(values: &[u64; MINHASH_K]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn decode_u64(bytes: &[u8]) -> [u64; MINHASH_K] {
    // Defaults to the empty-minhash sentinel, so a short/corrupt blob reads as
    // "no values seen" rather than as a spurious similarity.
    let mut out = [u64::MAX; MINHASH_K];
    for (slot, chunk) in out.iter_mut().zip(bytes.chunks_exact(8)) {
        *slot = u64::from_le_bytes(chunk.try_into().unwrap_or([0; 8]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::FieldStatus;
    use crate::resilience::sketch::SketchBuilder;

    #[test]
    fn sketch_blobs_round_trip_exactly() {
        let mut b = SketchBuilder::new();
        for i in 0..40 {
            b.push(
                &FieldStatus::Matched,
                None,
                &serde_json::json!(format!("${i}.99")),
            );
        }
        let sketch = b.finish();
        assert_eq!(decode_u16(&encode_u16(&sketch.len_hist)), sketch.len_hist);
        assert_eq!(decode_f32(&encode_f32(&sketch.cls)), sketch.cls);
        assert_eq!(decode_u64(&encode_u64(&sketch.minhash)), sketch.minhash);
        // Fixed widths, so a sketch row's blob cost is knowable up front.
        assert_eq!(encode_u64(&sketch.minhash).len(), MINHASH_K * 8);
    }

    #[test]
    fn a_truncated_minhash_blob_reads_as_no_values_not_as_similarity() {
        // A short blob must not decode to 64 zeros, which would compare as
        // *identical* to every other short blob.
        let decoded = decode_u64(&[1u8; 8]);
        assert_eq!(decoded[0], u64::from_le_bytes([1; 8]));
        assert!(decoded[1..].iter().all(|&h| h == u64::MAX));
    }

    #[test]
    fn a_disabled_service_gates_nothing_and_needs_no_database() {
        let r = Resilience::disabled();
        assert!(!r.enabled());
        assert!(!r.enforcing());
        assert!(r.store().is_none());
    }
}
