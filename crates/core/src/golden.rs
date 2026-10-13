//! Golden documents: a frozen body plus the values it must still produce, the
//! one exact check the repair plane has (`docs/features/resilient-extraction.md`
//! §3.2, "Golden documents").
//!
//! Two references answer two questions:
//!
//! - **Frozen** — the pinned body re-extracted with the source's ACTIVE ruleset.
//!   A mismatch means *our* code or rules changed a known result.
//! - **Live** — the source's current record against the pinned `expected`
//!   values, on its **stable** fields only. A mismatch means the *site* moved a
//!   value that has not moved before.
//!
//! A stable field is one whose churn — the share of the key's revisions whose
//! diff touches it — is under [`STABLE_CHURN`]. Anything else changes too often
//! to anchor on.
//!
//! Generations are kept two deep. Generation n+1 is pinned only after the source
//! has been continuously healthy for `[resilience] golden_refresh_days` since n
//! was pinned, and only if the current values equal **both** n and n−1 on every
//! stable field, so a slowly boiled value has to fool a reference about two
//! refresh periods old. Generation 0 is pinned `enrol` (refreshable) or, when
//! the operator freezes it, `operator` — which is never re-pinned
//! automatically ([`pin_is_refreshable`]).
//!
//! Bodies live under `data/golden/<source id, `/` → `_`>/<sha>.html` — beside
//! `data/artifacts`, outside artifact retention.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::resilience::RunVerdict;

/// Churn at or above which a field is not an anchor.
pub const STABLE_CHURN: f64 = 0.05;
/// Generations kept per key.
pub const KEEP_GENERATIONS: i64 = 2;
/// Share of golden keys that must disagree for a run to be `Broken` rather than
/// suspect.
pub const BROKEN_SHARE: f64 = 0.5;
/// `pinned_by` of a **frozen** generation an operator pinned
/// (`POST /sources/{id}/golden {"frozen": true}`): never re-pinned automatically.
pub const PINNED_BY_OPERATOR: &str = "operator";
/// `pinned_by` of an enrolment pin (`POST /sources/{id}/golden`, the default):
/// generation 0 of a lineage the refresh keeps current.
pub const PINNED_BY_ENROL: &str = "enrol";
/// `pinned_by` of a generation the refresh pinned.
pub const PINNED_BY_REFRESH: &str = "refresh";
/// Directory name of the golden store, beside the artifacts directory.
pub const GOLDEN_DIR: &str = "golden";

// ---- pure --------------------------------------------------------------------

/// The values a record stands for: its top-level fields minus the underscore
/// meta fields (`_url`, `_observed_at`, `_fetched_via`, …) the write path stamps
/// on. A non-object is returned as it is.
pub fn expected_of(record: &Value) -> Value {
    match record {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(k, _)| !k.starts_with('_'))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Whether one revision diff (`diff_values` output: dot paths, `$` for the
/// root) touches a top-level `field`.
pub fn diff_touches(diff: &Value, field: &str) -> bool {
    let Value::Object(paths) = diff else {
        return false;
    };
    paths.keys().any(|p| {
        p == "$" || p == field || (p.starts_with(field) && p[field.len()..].starts_with('.'))
    })
}

/// The fields of `fields` whose churn over `diffs` is below [`STABLE_CHURN`].
///
/// `diffs` is one entry per revision: `None` for a revision that changed no
/// field (a `new` one), `Some(diff)` for a change. With no revisions at all the
/// churn is unknown, and an unknown churn is not a stable one — the result is
/// empty.
pub fn stable_fields_from(fields: &[String], diffs: &[Option<Value>]) -> Vec<String> {
    if diffs.is_empty() {
        return Vec::new();
    }
    let total = diffs.len() as f64;
    let mut out: Vec<String> = fields
        .iter()
        .filter(|f| {
            let touched = diffs
                .iter()
                .filter(|d| d.as_ref().is_some_and(|d| diff_touches(d, f)))
                .count() as f64;
            touched / total < STABLE_CHURN
        })
        .cloned()
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The top-level fields present on either side of any record, underscore meta
/// fields excluded — the candidates [`stable_fields_from`] is asked about.
pub fn candidate_fields<'a>(records: impl IntoIterator<Item = &'a Value>) -> Vec<String> {
    let mut out: Vec<String> = records
        .into_iter()
        .filter_map(Value::as_object)
        .flat_map(|m| m.keys().filter(|k| !k.starts_with('_')).cloned())
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The fields of `fields` on which `observed` disagrees with `expected`. A
/// field absent on one side reads as `null` there.
pub fn disagreeing_fields(expected: &Value, observed: &Value, fields: &[String]) -> Vec<String> {
    fields
        .iter()
        .filter(|f| field_of(expected, f) != field_of(observed, f))
        .cloned()
        .collect()
}

fn field_of<'a>(v: &'a Value, field: &str) -> &'a Value {
    v.get(field).unwrap_or(&Value::Null)
}

/// What the golden keys present in a run say about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoldenVerdict {
    /// Every stable field of every golden key agreed.
    Ok,
    /// Some golden key disagreed on a stable field, fewer than half of them.
    Suspect,
    /// At least [`BROKEN_SHARE`] of the golden keys disagreed.
    Broken,
}

impl GoldenVerdict {
    /// The run verdict and score this stands for.
    ///
    /// `RunVerdict` has no "suspect", and a new stored verdict for it would be a
    /// second name for a tripped-but-not-severe run, so `Suspect` is recorded as
    /// `broken` at exactly `degrade_score` (it trips; it is severe only if the
    /// operator set `quarantine_score` no higher than `degrade_score`), and
    /// `Broken` as `broken` at 1.0. `Ok` is [`RunVerdict::GoldenOk`]: judged,
    /// never baseline material.
    pub fn run_verdict(self, degrade_score: f64) -> (RunVerdict, f64) {
        match self {
            Self::Ok => (RunVerdict::GoldenOk, 0.0),
            Self::Suspect => (RunVerdict::Broken, degrade_score),
            Self::Broken => (RunVerdict::Broken, 1.0),
        }
    }
}

/// One golden key present in a run.
#[derive(Debug, Clone, Copy)]
pub struct GoldenObservation<'a> {
    pub expected: &'a Value,
    pub observed: &'a Value,
    pub stable_fields: &'a [String],
}

/// The cohort-less verdict: `Ok` when every stable field of every golden key in
/// the run agrees, `Broken` when at least half the keys disagree on some stable
/// field, `Suspect` otherwise. `None` when no key carries a stable field — a
/// golden with nothing to compare vouches for nothing.
pub fn golden_verdict(observations: &[GoldenObservation<'_>]) -> Option<GoldenVerdict> {
    let usable: Vec<&GoldenObservation> = observations
        .iter()
        .filter(|o| !o.stable_fields.is_empty())
        .collect();
    if usable.is_empty() {
        return None;
    }
    let disagreeing = usable
        .iter()
        .filter(|o| !disagreeing_fields(o.expected, o.observed, o.stable_fields).is_empty())
        .count();
    Some(if disagreeing == 0 {
        GoldenVerdict::Ok
    } else if disagreeing as f64 >= BROKEN_SHARE * usable.len() as f64 {
        GoldenVerdict::Broken
    } else {
        GoldenVerdict::Suspect
    })
}

/// Test name of the reason a golden-judged run records.
pub const GOLDEN_REASON: &str = "golden_disagreement";

/// Replaces a `below_cohort` evaluation with the golden verdict, when the
/// goldens present in the run have one. Returns whether it did.
///
/// The run keeps `statistical_coverage: false` and its cohort label — the
/// goldens judged it, the distributions did not — and records one reason: the
/// share of golden keys that disagreed, against [`BROKEN_SHARE`]. The diagnosis
/// stays `None`: a disagreeing golden says *that* a value moved, not which of
/// the design's failure classes moved it.
pub fn apply_golden_verdict(
    eval: &mut crate::resilience::RunEvaluation,
    observations: &[GoldenObservation<'_>],
    degrade_score: f64,
) -> bool {
    if eval.verdict != RunVerdict::BelowCohort {
        return false;
    }
    let Some(verdict) = golden_verdict(observations) else {
        return false;
    };
    let usable: Vec<&GoldenObservation> = observations
        .iter()
        .filter(|o| !o.stable_fields.is_empty())
        .collect();
    let disagreeing = usable
        .iter()
        .filter(|o| !disagreeing_fields(o.expected, o.observed, o.stable_fields).is_empty())
        .count();
    let (run_verdict, score) = verdict.run_verdict(degrade_score);
    eval.verdict = run_verdict;
    eval.score = score;
    eval.diagnosis = None;
    eval.reasons.push(crate::resilience::Reason {
        test: GOLDEN_REASON.to_string(),
        field: None,
        value: disagreeing as f64 / usable.len() as f64,
        threshold: BROKEN_SHARE,
    });
    true
}

/// Whether generation n+1 may be pinned from `current`: it must equal
/// generation n **and** generation n−1 on every stable field (`previous` absent
/// → it must equal n). A value that boiled slowly enough to fool n still has to
/// fool n−1. With no stable field there is nothing to vouch for the new
/// generation, so nothing is pinned.
pub fn repin_allowed(
    current: &Value,
    newest: &Value,
    previous: Option<&Value>,
    stable_fields: &[String],
) -> bool {
    !stable_fields.is_empty()
        && disagreeing_fields(newest, current, stable_fields).is_empty()
        && previous.is_none_or(|p| disagreeing_fields(p, current, stable_fields).is_empty())
}

/// Whether a generation pinned by `pinned_by` may ever be replaced by the
/// refresh: an enrolment pin and a refresh-written one may; a frozen operator
/// pin never is, and neither is a value this build does not know (a
/// `pinned_by` it cannot read vouches for nothing it may overwrite).
pub fn pin_is_refreshable(pinned_by: &str) -> bool {
    matches!(pinned_by, PINNED_BY_ENROL | PINNED_BY_REFRESH)
}

/// Whether a key's newest generation, pinned at `pinned_at`, is due a refresh:
/// it is [refreshable](pin_is_refreshable), and the source has been healthy without a
/// break since then (`state_since` — which moves on every transition, manual
/// ones included — is no later than `pinned_at`) for at least `refresh_days`.
pub fn refresh_due(
    pinned_by: &str,
    state: crate::resilience::SourceState,
    state_since: chrono::DateTime<chrono::Utc>,
    pinned_at: chrono::DateTime<chrono::Utc>,
    now: chrono::DateTime<chrono::Utc>,
    refresh_days: u32,
) -> bool {
    pin_is_refreshable(pinned_by)
        && state == crate::resilience::SourceState::Healthy
        && state_since <= pinned_at
        && now - pinned_at >= chrono::Duration::days(i64::from(refresh_days))
}

/// `data/golden` for an artifacts root of `data/artifacts`: the golden store
/// sits beside the artifacts directory, so artifact retention never walks it.
pub fn golden_root(artifacts_root: &Path) -> PathBuf {
    artifacts_root
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(GOLDEN_DIR)
}

/// `<golden root>/<source id with / → _>/<sha>.html`, or `None` when the id
/// does not make a safe single segment or `body_sha` is not 64 hex.
pub fn golden_body_path(root: &Path, source_id: &str, body_sha: &str) -> Option<PathBuf> {
    let segment = source_id.replace(['/', '\\'], "_");
    let segment_ok = !segment.is_empty() && segment != "." && segment != "..";
    let sha_ok = body_sha.len() == 64 && body_sha.chars().all(|c| c.is_ascii_hexdigit());
    (segment_ok && sha_ok).then(|| root.join(segment).join(format!("{body_sha}.html")))
}

/// One pinned generation of one key (`golden_docs`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GoldenDoc {
    pub source_id: String,
    pub key: String,
    pub generation: i64,
    pub url: String,
    pub body_sha: String,
    pub expected: Value,
    pub stable_fields: Vec<String>,
    pub rules_hash: String,
    pub pinned_by: String,
    pub pinned_at: String,
}

/// Per key: the newest generation and the one before it (`rows` in any order).
pub fn generations_by_key(
    rows: Vec<GoldenDoc>,
) -> BTreeMap<String, (GoldenDoc, Option<GoldenDoc>)> {
    let mut grouped: BTreeMap<String, Vec<GoldenDoc>> = BTreeMap::new();
    for row in rows {
        grouped.entry(row.key.clone()).or_default().push(row);
    }
    grouped
        .into_iter()
        .filter_map(|(key, mut gens)| {
            gens.sort_by_key(|g| std::cmp::Reverse(g.generation));
            let mut it = gens.into_iter();
            let newest = it.next()?;
            Some((key, (newest, it.next())))
        })
        .collect()
}

/// One field that did not match its golden value.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GoldenMismatch {
    pub key: String,
    /// `null` when the whole document could not be compared (its frozen body
    /// is gone, or its live record is).
    pub field: Option<String>,
    pub expected: Value,
    pub got: Value,
    /// `frozen` (code / rules drift) or `live` (site drift).
    pub kind: &'static str,
}

/// The mismatches of one comparison, as [`GoldenMismatch`] rows.
pub fn mismatches(
    key: &str,
    kind: &'static str,
    expected: &Value,
    got: &Value,
    fields: &[String],
) -> Vec<GoldenMismatch> {
    disagreeing_fields(expected, got, fields)
        .into_iter()
        .map(|f| GoldenMismatch {
            key: key.to_string(),
            expected: field_of(expected, &f).clone(),
            got: field_of(got, &f).clone(),
            field: Some(f),
            kind,
        })
        .collect()
}

#[cfg(feature = "storage")]
pub use store::*;

#[cfg(feature = "storage")]
mod store {
    use super::*;

    use chrono::{DateTime, Utc};
    use sqlx::SqlitePool;

    use crate::datasets::{body_sha256, pinned_body_path, ts, Datasets};
    use crate::resilience::SourceState;

    /// Why a golden operation was refused.
    #[derive(Debug)]
    pub enum GoldenError {
        /// The caller's input cannot be pinned (400).
        BadRequest(String),
        /// A store failure.
        Store(crate::Error),
    }

    impl From<crate::Error> for GoldenError {
        fn from(e: crate::Error) -> Self {
            Self::Store(e)
        }
    }

    impl From<sqlx::Error> for GoldenError {
        fn from(e: sqlx::Error) -> Self {
            Self::Store(e.into())
        }
    }

    impl From<std::io::Error> for GoldenError {
        fn from(e: std::io::Error) -> Self {
            Self::Store(e.into())
        }
    }

    type GResult<T> = std::result::Result<T, GoldenError>;

    #[derive(sqlx::FromRow)]
    struct GoldenRow {
        source_id: String,
        key: String,
        generation: i64,
        url: String,
        body_sha: String,
        expected: String,
        stable_fields: String,
        rules_hash: String,
        pinned_by: String,
        pinned_at: String,
    }

    impl From<GoldenRow> for GoldenDoc {
        fn from(r: GoldenRow) -> Self {
            Self {
                source_id: r.source_id,
                key: r.key,
                generation: r.generation,
                url: r.url,
                body_sha: r.body_sha,
                // A row this build cannot read compares as `null` / no stable
                // field — it vouches for nothing rather than for garbage.
                expected: serde_json::from_str(&r.expected).unwrap_or(Value::Null),
                stable_fields: serde_json::from_str(&r.stable_fields).unwrap_or_default(),
                rules_hash: r.rules_hash,
                pinned_by: r.pinned_by,
                pinned_at: r.pinned_at,
            }
        }
    }

    /// Every golden generation a source holds.
    pub async fn goldens(pool: &SqlitePool, source_id: &str) -> crate::Result<Vec<GoldenDoc>> {
        let rows: Vec<GoldenRow> = sqlx::query_as(
            "SELECT source_id, key, generation, url, body_sha, expected, stable_fields, \
                    rules_hash, pinned_by, pinned_at \
             FROM golden_docs WHERE source_id = ?1 ORDER BY key, generation DESC",
        )
        .bind(source_id)
        .fetch_all(pool)
        .await?;
        Ok(rows.into_iter().map(GoldenDoc::from).collect())
    }

    /// The fields of `keys`' records whose churn over their revision history is
    /// under [`STABLE_CHURN`] — one set for the whole key list.
    pub async fn stable_fields(
        datasets: &Datasets,
        app: &str,
        dataset: &str,
        keys: &[&str],
    ) -> crate::Result<Vec<String>> {
        let mut records = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(r) = datasets.get(app, dataset, key).await? {
                records.push(r.data);
            }
        }
        let fields = candidate_fields(records.iter());
        let diffs = datasets.revision_diffs(app, dataset, keys).await?;
        Ok(stable_fields_from(&fields, &diffs))
    }

    /// The newest body pin of one document key: `(body_sha, original_url)`.
    async fn latest_pin(
        pool: &SqlitePool,
        source_id: &str,
        key: &str,
    ) -> crate::Result<Option<(String, Option<String>)>> {
        Ok(sqlx::query_as(
            "SELECT body_sha, original_url FROM body_pins \
             WHERE source_id = ?1 AND document_key = ?2 \
             ORDER BY captured_at DESC, observation_id DESC LIMIT 1",
        )
        .bind(source_id)
        .bind(key)
        .fetch_optional(pool)
        .await?)
    }

    /// Reads a pinned body and verifies it against its sha.
    async fn read_pinned(
        artifacts_root: &Path,
        app: &str,
        key: &str,
        body_sha: &str,
    ) -> GResult<String> {
        let path = pinned_body_path(artifacts_root, app, body_sha).ok_or_else(|| {
            GoldenError::BadRequest(format!("key '{key}': unsafe pinned body address"))
        })?;
        let body = tokio::fs::read_to_string(&path).await.map_err(|e| {
            GoldenError::BadRequest(format!(
                "key '{key}': pinned body {body_sha} unreadable: {e}"
            ))
        })?;
        if body_sha256(&body) != body_sha {
            return Err(GoldenError::BadRequest(format!(
                "key '{key}': pinned body {body_sha} no longer matches its sha"
            )));
        }
        Ok(body)
    }

    /// Writes a golden body at its content address (no rewrite when present).
    async fn write_golden_body(
        golden_root: &Path,
        source_id: &str,
        body_sha: &str,
        body: &str,
    ) -> GResult<()> {
        let path = golden_body_path(golden_root, source_id, body_sha).ok_or_else(|| {
            GoldenError::BadRequest(format!("source '{source_id}' has no safe golden address"))
        })?;
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Ok(());
        }
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        tokio::fs::write(&path, body.as_bytes()).await?;
        Ok(())
    }

    /// Deletes the body files of `dropped` generations that no remaining row
    /// references. An operator pin's body is never deleted.
    async fn drop_unreferenced_bodies(
        pool: &SqlitePool,
        golden_root: &Path,
        source_id: &str,
        dropped: &[GoldenDoc],
    ) {
        for doc in dropped {
            if !pin_is_refreshable(&doc.pinned_by) {
                continue;
            }
            let still: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM golden_docs WHERE source_id = ?1 AND body_sha = ?2",
            )
            .bind(source_id)
            .bind(&doc.body_sha)
            .fetch_one(pool)
            .await
            .unwrap_or(1);
            if still > 0 {
                continue;
            }
            if let Some(path) = golden_body_path(golden_root, source_id, &doc.body_sha) {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    tracing::warn!(source = source_id, "golden body not removed: {e}");
                }
            }
        }
    }

    async fn insert(conn: &mut sqlx::SqliteConnection, doc: &GoldenDoc) -> crate::Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO golden_docs (source_id, key, generation, url, body_sha, \
                 expected, stable_fields, rules_hash, pinned_by, pinned_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )
        .bind(&doc.source_id)
        .bind(&doc.key)
        .bind(doc.generation)
        .bind(&doc.url)
        .bind(&doc.body_sha)
        .bind(doc.expected.to_string())
        .bind(serde_json::to_string(&doc.stable_fields).unwrap_or_else(|_| "[]".into()))
        .bind(&doc.rules_hash)
        .bind(&doc.pinned_by)
        .bind(&doc.pinned_at)
        .execute(&mut *conn)
        .await?;
        Ok(())
    }

    /// Replaces generations in one `BEGIN IMMEDIATE`: for each doc, deletes the
    /// generations of its key below `floor(doc)` (or all of them, for `None`),
    /// then inserts it. One transaction, so a key is never left half-pinned.
    async fn write_generations(
        pool: &SqlitePool,
        docs: &[GoldenDoc],
        floor: impl Fn(&GoldenDoc) -> Option<i64>,
    ) -> crate::Result<()> {
        let mut conn = pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let outcome: crate::Result<()> = async {
            for doc in docs {
                sqlx::query(
                    "DELETE FROM golden_docs WHERE source_id = ?1 AND key = ?2 \
                     AND (?3 IS NULL OR generation < ?3)",
                )
                .bind(&doc.source_id)
                .bind(&doc.key)
                .bind(floor(doc))
                .execute(&mut *conn)
                .await?;
                insert(&mut conn, doc).await?;
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

    /// `POST /sources/{id}/golden`: pins generation 0 of each key from its
    /// current record and its newest pinned body — `pinned_by: "enrol"`
    /// (refreshable) by default, `"operator"` (frozen, never re-pinned) when
    /// `frozen`.
    ///
    /// A pin restarts the key's lineage: any older generations of the
    /// key are replaced. Every key is validated before anything is written, so
    /// one unpinnable key refuses the whole request.
    pub async fn pin(
        datasets: &Datasets,
        artifacts_root: &Path,
        source_id: &str,
        keys: &[String],
        frozen: bool,
        now: DateTime<Utc>,
    ) -> GResult<Vec<GoldenDoc>> {
        let pool = datasets.pool();
        let (app, dataset) = source_id.split_once('/').ok_or_else(|| {
            GoldenError::BadRequest(format!("'{source_id}' is not an <app>/<dataset> id"))
        })?;
        let mut keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        keys.sort_unstable();
        keys.dedup();
        if keys.is_empty() {
            return Err(GoldenError::BadRequest(
                "'keys' must name at least one record key".into(),
            ));
        }
        let rules_hash = datasets
            .active_rules_hash(source_id)
            .await?
            .ok_or_else(|| {
                GoldenError::BadRequest(format!(
                    "source '{source_id}' has no active ruleset (import one first: \
                 POST /sources/{{id}}/rules/import); only enrolled runs keep the bodies a \
                 golden document is pinned from"
                ))
            })?;

        // Validate every key, reading each body, before any write.
        let mut staged = Vec::with_capacity(keys.len());
        for key in &keys {
            let record = datasets.get(app, dataset, key).await?.ok_or_else(|| {
                GoldenError::BadRequest(format!("key '{key}' has no record in {source_id}"))
            })?;
            let (sha, url) = latest_pin(pool, source_id, key).await?.ok_or_else(|| {
                GoldenError::BadRequest(format!(
                    "key '{key}' has no pinned body (only enrolled urls-mode runs pin bodies)"
                ))
            })?;
            let body = read_pinned(artifacts_root, app, key, &sha).await?;
            staged.push((key.to_string(), record.data, sha, url, body));
        }
        let stable = stable_fields(datasets, app, dataset, &keys).await?;
        let root = golden_root(artifacts_root);
        let pinned_at = ts(now);
        let mut docs = Vec::with_capacity(staged.len());
        for (key, data, sha, url, body) in staged {
            write_golden_body(&root, source_id, &sha, &body).await?;
            docs.push(GoldenDoc {
                source_id: source_id.to_string(),
                url: url.unwrap_or_else(|| key.clone()),
                key,
                generation: 0,
                body_sha: sha,
                expected: expected_of(&data),
                stable_fields: stable.clone(),
                rules_hash: rules_hash.clone(),
                pinned_by: if frozen {
                    PINNED_BY_OPERATOR
                } else {
                    PINNED_BY_ENROL
                }
                .to_string(),
                pinned_at: pinned_at.clone(),
            });
        }
        let replaced: Vec<GoldenDoc> = goldens(pool, source_id)
            .await?
            .into_iter()
            .filter(|g| docs.iter().any(|d| d.key == g.key))
            .collect();
        // `None`: an operator pin replaces every older generation of its key.
        write_generations(pool, &docs, |_| None).await?;
        drop_unreferenced_bodies(pool, &root, source_id, &replaced).await;
        Ok(docs)
    }

    /// The refresh: after a healthy run, pins generation n+1 for every key that
    /// [`refresh_due`] and [`repin_allowed`] both allow, keeping two generations.
    /// Returns the keys re-pinned.
    ///
    /// Cheap first: one read of the source's (few) golden rows, and it returns
    /// before any other query when no key's newest generation is refreshable.
    pub async fn refresh(
        datasets: &Datasets,
        artifacts_root: &Path,
        source_id: &str,
        refresh_days: u32,
        now: DateTime<Utc>,
    ) -> crate::Result<Vec<String>> {
        let pool = datasets.pool();
        let by_key = generations_by_key(goldens(pool, source_id).await?);
        if !by_key
            .values()
            .any(|(newest, _)| pin_is_refreshable(&newest.pinned_by))
        {
            return Ok(Vec::new());
        }
        let Some((app, dataset)) = source_id.split_once('/') else {
            return Ok(Vec::new());
        };
        let source: Option<(String, String)> =
            sqlx::query_as("SELECT state, state_since FROM sources WHERE id = ?1")
                .bind(source_id)
                .fetch_optional(pool)
                .await?;
        let Some((state, since)) = source else {
            return Ok(Vec::new());
        };
        let state = SourceState::parse(&state);
        let Some(since) = parse_ts(&since) else {
            return Ok(Vec::new());
        };
        let due: Vec<&(GoldenDoc, Option<GoldenDoc>)> = by_key
            .values()
            .filter(|(newest, _)| {
                parse_ts(&newest.pinned_at).is_some_and(|at| {
                    refresh_due(&newest.pinned_by, state, since, at, now, refresh_days)
                })
            })
            .collect();
        if due.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<&str> = by_key.keys().map(String::as_str).collect();
        let stable_now = stable_fields(datasets, app, dataset, &keys).await?;
        let root = golden_root(artifacts_root);
        let pinned_at = ts(now);
        let mut repinned = Vec::new();
        for (newest, previous) in due {
            let Some(record) = datasets.get(app, dataset, &newest.key).await? else {
                continue;
            };
            let current = expected_of(&record.data);
            if !repin_allowed(
                &current,
                &newest.expected,
                previous.as_ref().map(|p| &p.expected),
                &newest.stable_fields,
            ) {
                continue;
            }
            let Some((sha, url)) = latest_pin(pool, source_id, &newest.key).await? else {
                continue;
            };
            let body = match read_pinned(artifacts_root, app, &newest.key, &sha).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(source = source_id, key = %newest.key, "golden refresh skipped: {e:?}");
                    continue;
                }
            };
            if let Err(e) = write_golden_body(&root, source_id, &sha, &body).await {
                tracing::warn!(source = source_id, key = %newest.key, "golden refresh skipped: {e:?}");
                continue;
            }
            let doc = GoldenDoc {
                source_id: source_id.to_string(),
                key: newest.key.clone(),
                generation: newest.generation + 1,
                url: url.unwrap_or_else(|| newest.url.clone()),
                body_sha: sha,
                expected: current,
                stable_fields: stable_now.clone(),
                rules_hash: datasets
                    .active_rules_hash(source_id)
                    .await?
                    .unwrap_or_else(|| newest.rules_hash.clone()),
                pinned_by: PINNED_BY_REFRESH.to_string(),
                pinned_at: pinned_at.clone(),
            };
            let floor = doc.generation - (KEEP_GENERATIONS - 1);
            let dropped: Vec<GoldenDoc> = goldens(pool, source_id)
                .await?
                .into_iter()
                .filter(|g| g.key == doc.key && g.generation < floor)
                .collect();
            // Keep two generations: n (now the previous) and n+1. Only the
            // older rows are deleted; an operator pin's body file never is.
            write_generations(pool, std::slice::from_ref(&doc), |_| Some(floor)).await?;
            drop_unreferenced_bodies(pool, &root, source_id, &dropped).await;
            repinned.push(doc.key);
        }
        Ok(repinned)
    }

    /// `GET /sources/{id}/golden/check`.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct GoldenCheck {
        pub frozen: FrozenTally,
        pub live: LiveTally,
        pub mismatches: Vec<GoldenMismatch>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub struct FrozenTally {
        pub keys: usize,
        /// Frozen bodies whose re-extraction reproduced `expected` exactly.
        pub exact: usize,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub struct LiveTally {
        pub keys: usize,
        /// Live records that agree with `expected` on every stable field.
        pub agree: usize,
    }

    /// Checks a source's newest golden generations, frozen and live. Read-only.
    ///
    /// Frozen: each frozen body re-extracted with `active` (the source's active
    /// ruleset), against its own URL, compared on every expected field — exact
    /// or not. Live: the current record against `expected` on the stable fields.
    pub async fn check(
        datasets: &Datasets,
        artifacts_root: &Path,
        source_id: &str,
        active: &crate::RuleSet,
    ) -> crate::Result<GoldenCheck> {
        let pool = datasets.pool();
        let compiled = active.compile()?;
        let (app, dataset) = source_id.split_once('/').unwrap_or((source_id, ""));
        let root = golden_root(artifacts_root);
        let by_key = generations_by_key(goldens(pool, source_id).await?);
        let mut out = GoldenCheck {
            frozen: FrozenTally {
                keys: by_key.len(),
                exact: 0,
            },
            live: LiveTally {
                keys: by_key.len(),
                agree: 0,
            },
            mismatches: Vec::new(),
        };
        for (key, (golden, _)) in &by_key {
            // Frozen: the body we pinned, through the rules we run today.
            let body = match golden_body_path(&root, source_id, &golden.body_sha) {
                Some(path) => tokio::fs::read_to_string(&path)
                    .await
                    .ok()
                    .filter(|b| body_sha256(b) == golden.body_sha),
                None => None,
            };
            match body {
                Some(body) => {
                    let (values, _) = crate::extract::extract_one_with_report_at(
                        &compiled,
                        &body,
                        Some(&golden.url),
                    );
                    let got = expected_of(&values);
                    let fields = candidate_fields([&golden.expected, &got]);
                    let rows = mismatches(key, "frozen", &golden.expected, &got, &fields);
                    if rows.is_empty() {
                        out.frozen.exact += 1;
                    }
                    out.mismatches.extend(rows);
                }
                None => out.mismatches.push(GoldenMismatch {
                    key: key.clone(),
                    field: None,
                    expected: golden.expected.clone(),
                    got: Value::Null,
                    kind: "frozen",
                }),
            }
            // Live: what the source holds now, on the fields that do not churn.
            match datasets.get(app, dataset, key).await? {
                Some(record) => {
                    let got = expected_of(&record.data);
                    let rows =
                        mismatches(key, "live", &golden.expected, &got, &golden.stable_fields);
                    if rows.is_empty() {
                        out.live.agree += 1;
                    }
                    out.mismatches.extend(rows);
                }
                None => out.mismatches.push(GoldenMismatch {
                    key: key.clone(),
                    field: None,
                    expected: golden.expected.clone(),
                    got: Value::Null,
                    kind: "live",
                }),
            }
        }
        Ok(out)
    }

    fn parse_ts(s: &str) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|d| d.with_timezone(&Utc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fields(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn expected_values_drop_the_underscore_meta_fields() {
        let rec = json!({"title": "A", "points": 3, "_url": "https://x", "_observed_at": "t"});
        assert_eq!(expected_of(&rec), json!({"title": "A", "points": 3}));
    }

    #[test]
    fn a_field_touched_in_one_revision_of_twenty_is_not_stable() {
        // 1 of 20 is exactly 5%: the threshold is strict.
        let mut diffs: Vec<Option<Value>> = vec![None];
        diffs.extend((0..18).map(|_| Some(json!({"points": {"from": 1, "to": 2}}))));
        diffs.push(Some(json!({"title": {"from": "a", "to": "b"}})));
        let stable = stable_fields_from(&fields(&["points", "title", "by"]), &diffs);
        assert_eq!(stable, fields(&["by"]));
        // 1 of 21 is under 5%.
        diffs.push(None);
        let stable = stable_fields_from(&fields(&["points", "title", "by"]), &diffs);
        assert_eq!(stable, fields(&["by", "title"]));
    }

    #[test]
    fn nested_paths_and_the_root_count_as_touching_the_field() {
        assert!(diff_touches(&json!({"price.amount": {}}), "price"));
        assert!(!diff_touches(&json!({"priced": {}}), "price"));
        assert!(diff_touches(&json!({"$": {}}), "anything"));
    }

    #[test]
    fn no_history_means_no_stable_field_not_every_field() {
        assert!(stable_fields_from(&fields(&["title"]), &[]).is_empty());
    }

    fn obs<'a>(
        expected: &'a Value,
        observed: &'a Value,
        stable: &'a [String],
    ) -> GoldenObservation<'a> {
        GoldenObservation {
            expected,
            observed,
            stable_fields: stable,
        }
    }

    #[test]
    fn golden_verdict_is_ok_suspect_or_broken_by_share_of_disagreeing_keys() {
        let stable = fields(&["title", "by"]);
        let good = json!({"title": "A", "by": "pg", "points": 1});
        let churned = json!({"title": "A", "by": "pg", "points": 99});
        let bad = json!({"title": "Hacker News", "by": "pg"});

        // Only the stable fields are compared: churn on `points` is not drift.
        assert_eq!(
            golden_verdict(&[obs(&good, &churned, &stable), obs(&good, &good, &stable)]),
            Some(GoldenVerdict::Ok)
        );
        // One key in three disagrees: suspect.
        assert_eq!(
            golden_verdict(&[
                obs(&good, &bad, &stable),
                obs(&good, &good, &stable),
                obs(&good, &good, &stable),
            ]),
            Some(GoldenVerdict::Suspect)
        );
        // Half disagree: broken.
        assert_eq!(
            golden_verdict(&[obs(&good, &bad, &stable), obs(&good, &good, &stable)]),
            Some(GoldenVerdict::Broken)
        );
        // Nothing stable to compare: no opinion at all.
        assert_eq!(golden_verdict(&[obs(&good, &bad, &[])]), None);
        assert_eq!(golden_verdict(&[]), None);
    }

    #[test]
    fn a_golden_suspect_trips_without_being_severe_and_ok_never_baselines() {
        assert_eq!(
            GoldenVerdict::Suspect.run_verdict(0.6),
            (RunVerdict::Broken, 0.6)
        );
        assert_eq!(
            GoldenVerdict::Broken.run_verdict(0.6),
            (RunVerdict::Broken, 1.0)
        );
        let (ok, score) = GoldenVerdict::Ok.run_verdict(0.6);
        assert_eq!((ok, score), (RunVerdict::GoldenOk, 0.0));
        assert!(ok.judged() && !ok.baselines());
    }

    fn below_cohort() -> crate::resilience::RunEvaluation {
        crate::resilience::RunEvaluation {
            verdict: RunVerdict::BelowCohort,
            diagnosis: None,
            score: 0.0,
            reasons: vec![],
            drift: None,
            statistical_coverage: false,
            adequacy: crate::resilience::CohortAdequacy::Chronic,
        }
    }

    #[test]
    fn no_goldens_stays_below_cohort() {
        let mut eval = below_cohort();
        assert!(!apply_golden_verdict(&mut eval, &[], 0.6));
        assert_eq!(eval.verdict, RunVerdict::BelowCohort);
        assert!(eval.reasons.is_empty());
        // Goldens with nothing stable to compare are the same as none.
        let v = json!({"title": "A"});
        assert!(!apply_golden_verdict(&mut eval, &[obs(&v, &v, &[])], 0.6));
        assert_eq!(eval.verdict, RunVerdict::BelowCohort);
    }

    #[test]
    fn goldens_judge_a_below_cohort_run_and_nothing_else() {
        let stable = fields(&["title"]);
        let good = json!({"title": "A"});
        let bad = json!({"title": "Hacker News"});

        let mut eval = below_cohort();
        assert!(apply_golden_verdict(
            &mut eval,
            &[obs(&good, &good, &stable)],
            0.6
        ));
        assert_eq!(eval.verdict, RunVerdict::GoldenOk);
        assert!(
            !eval.verdict.baselines(),
            "golden-ok never enters the baseline"
        );
        assert!(!eval.statistical_coverage, "still honest about the cohort");
        assert_eq!(eval.reasons.last().unwrap().test, GOLDEN_REASON);

        let mut eval = below_cohort();
        let three = [
            obs(&good, &bad, &stable),
            obs(&good, &good, &stable),
            obs(&good, &good, &stable),
        ];
        assert!(apply_golden_verdict(&mut eval, &three, 0.6));
        assert_eq!((eval.verdict, eval.score), (RunVerdict::Broken, 0.6));

        let mut eval = below_cohort();
        assert!(apply_golden_verdict(
            &mut eval,
            &[obs(&good, &bad, &stable)],
            0.6
        ));
        assert_eq!((eval.verdict, eval.score), (RunVerdict::Broken, 1.0));

        // A run the ordinary rules judged is left alone.
        let mut judged = below_cohort();
        judged.verdict = RunVerdict::Inconclusive;
        assert!(!apply_golden_verdict(
            &mut judged,
            &[obs(&good, &bad, &stable)],
            0.6
        ));
        assert_eq!(judged.verdict, RunVerdict::Inconclusive);
    }

    #[test]
    fn boiled_value_that_fooled_n_but_not_n_minus_1_blocks_repin() {
        let stable = fields(&["title"]);
        let n_minus_1 = json!({"title": "Original"});
        // The value boiled once, generation n was pinned from the boiled value…
        let n = json!({"title": "Boiled"});
        let current = json!({"title": "Boiled"});
        // …so the current value matches n, but not n−1: refused.
        assert!(!repin_allowed(&current, &n, Some(&n_minus_1), &stable));
        // Equal to both: allowed.
        assert!(repin_allowed(
            &n_minus_1,
            &n_minus_1,
            Some(&n_minus_1),
            &stable
        ));
        // No n−1: equal to n is enough.
        assert!(repin_allowed(&current, &n, None, &stable));
        // Different from n: refused, whatever n−1 says.
        assert!(!repin_allowed(&json!({"title": "X"}), &n, None, &stable));
        // Nothing stable to vouch for it: refused.
        assert!(!repin_allowed(&current, &n, None, &[]));
    }

    #[test]
    fn an_enrolment_pin_refreshes_but_a_frozen_pin_never_does() {
        assert!(pin_is_refreshable(PINNED_BY_ENROL));
        assert!(pin_is_refreshable(PINNED_BY_REFRESH));
        assert!(!pin_is_refreshable(PINNED_BY_OPERATOR));
        // An unknown value is not licence to overwrite.
        assert!(!pin_is_refreshable("future_origin"));
        assert!(!pin_is_refreshable(""));
    }

    #[test]
    fn an_operator_pin_is_never_refreshed_and_a_break_restarts_the_clock() {
        use crate::resilience::SourceState::{Healthy, Suspect};
        let t = |d: i64| {
            chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap() + chrono::Duration::days(d)
        };
        // Healthy since before the pin, 30 days on: due.
        assert!(refresh_due(
            PINNED_BY_REFRESH,
            Healthy,
            t(0),
            t(1),
            t(31),
            30
        ));
        // Not yet 30 days.
        assert!(!refresh_due(
            PINNED_BY_REFRESH,
            Healthy,
            t(0),
            t(1),
            t(30),
            30
        ));
        // The source left healthy after the pin (state_since moved): not continuous.
        assert!(!refresh_due(
            PINNED_BY_REFRESH,
            Healthy,
            t(5),
            t(1),
            t(60),
            30
        ));
        assert!(!refresh_due(
            PINNED_BY_REFRESH,
            Suspect,
            t(0),
            t(1),
            t(60),
            30
        ));
        // An operator's pin is never replaced automatically.
        assert!(!refresh_due(
            PINNED_BY_OPERATOR,
            Healthy,
            t(0),
            t(1),
            t(365),
            30
        ));
    }

    #[test]
    fn golden_bodies_sit_beside_the_artifacts_dir_not_inside_it() {
        let root = golden_root(Path::new("data/artifacts"));
        assert_eq!(root, Path::new("data").join("golden"));
        let sha = "a".repeat(64);
        assert_eq!(
            golden_body_path(&root, "extractor/hn-items", &sha).unwrap(),
            root.join("extractor_hn-items").join(format!("{sha}.html"))
        );
        assert!(golden_body_path(&root, "extractor/hn-items", "../x").is_none());
        assert!(golden_body_path(&root, "..", &sha).is_none());
    }

    #[test]
    fn generations_group_newest_first_per_key() {
        let doc = |key: &str, generation: i64| GoldenDoc {
            source_id: "s/d".into(),
            key: key.into(),
            generation,
            url: String::new(),
            body_sha: String::new(),
            expected: Value::Null,
            stable_fields: vec![],
            rules_hash: String::new(),
            pinned_by: PINNED_BY_REFRESH.into(),
            pinned_at: String::new(),
        };
        let by = generations_by_key(vec![doc("a", 0), doc("a", 2), doc("a", 1), doc("b", 0)]);
        assert_eq!(by["a"].0.generation, 2);
        assert_eq!(by["a"].1.as_ref().unwrap().generation, 1);
        assert!(by["b"].1.is_none());
    }
}
