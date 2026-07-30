//! Record provenance (M12 reproducible records): the derivation chain behind
//! one dataset record — which job wrote each revision, from which URL, over
//! which archived body, extracted by which registered ruleset — plus a
//! read-only re-derivation that replays the archived body through the
//! *historical* ruleset and reports whether the stored value reproduces.
//!
//! Everything here is honest-Null: a stamp field a write path didn't know is
//! `null`, never invented, and re-derivation refuses (409, with the reason)
//! whenever the artifact + rules pair isn't fully pinned — an approximate
//! replay would be a fabricated provenance claim.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::datasets::rules_hash;
use pumper_core::{diff_values, Revision, RuleSet};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use utoipa::IntoParams;

use crate::routes::error::ApiError;
use crate::state::AppState;

/// Ceiling on the revisions one provenance response walks — the chain is
/// paged reading, but this endpoint is a per-record audit view, not a feed.
const MAX_CHAIN: i64 = 500;
const DEFAULT_CHAIN: i64 = 50;

#[derive(Deserialize, IntoParams)]
pub(crate) struct ProvenanceQuery {
    /// Max revisions returned, newest first (default 50, max 500). Coverage
    /// counters always describe the WHOLE chain, not just the returned page.
    limit: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/provenance/{app}/{dataset}/{key}",
    tag = "provenance",
    params(
        ("app" = String, Path, description = "App namespace"),
        ("dataset" = String, Path, description = "Dataset name"),
        ("key" = String, Path, description = "Record key"),
        ProvenanceQuery,
    ),
    responses(
        (status = 200, description = "`{key, trust, coverage, chain: [..]}` — each chain entry \
            carries the revision's provenance stamp (job_id/source_url/artifact_sha/rules_hash, \
            null = unknown) and, when a job is stamped, its schedule/trigger lineage"),
        (status = 404, description = "No such record"),
    )
)]
pub(crate) async fn get_provenance(
    State(state): State<AppState>,
    Path((app, dataset, key)): Path<(String, String, String)>,
    Query(query): Query<ProvenanceQuery>,
) -> Result<Json<Value>, ApiError> {
    let record = state
        .datasets
        .get(&app, &dataset, &key)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "no such record".into()))?;
    let limit = query.limit.unwrap_or(DEFAULT_CHAIN).clamp(1, MAX_CHAIN);
    let chain = state.datasets.history(&app, &dataset, &key, limit).await?;
    let (revisions, with_job, replayable) = state
        .datasets
        .provenance_coverage(&app, &dataset, &key)
        .await?;

    // Job lineage join: each stamped job_id resolves once to its schedule /
    // trigger lineage. A job the queue no longer knows (pruned, other install)
    // joins to null — the stamp itself is still the truth we stored.
    let mut jobs: HashMap<String, Option<Value>> = HashMap::new();
    for rev in &chain {
        let Some(id) = &rev.provenance.job_id else {
            continue;
        };
        if jobs.contains_key(id) {
            continue;
        }
        let lineage = match uuid::Uuid::parse_str(id) {
            Ok(uuid) => state.storage.get(uuid).await?.map(|job| {
                json!({
                    "app": job.app,
                    "status": job.status,
                    "schedule_id": job.schedule_id,
                    "trigger_id": job.trigger_id,
                    "created_at": job.created_at,
                })
            }),
            Err(_) => None,
        };
        jobs.insert(id.clone(), lineage);
    }

    let chain: Vec<Value> = chain
        .iter()
        .map(|rev| {
            let job = rev
                .provenance
                .job_id
                .as_ref()
                .and_then(|id| jobs.get(id).cloned())
                .flatten();
            json!({
                "revision": rev.revision,
                "change": rev.change,
                "created_at": rev.created_at,
                "trust": rev.trust,
                "provenance": {
                    "job_id": rev.provenance.job_id,
                    "source_url": rev.provenance.source_url,
                    "artifact_sha": rev.provenance.artifact_sha,
                    "rules_hash": rev.provenance.rules_hash,
                    "replayable": rev.provenance.replayable(),
                },
                "job": job,
            })
        })
        .collect();

    Ok(Json(json!({
        "app": app,
        "dataset": dataset,
        "key": key,
        "trust": record.trust,
        "removed_at": record.removed_at,
        "coverage": {
            "revisions": revisions,
            "with_job": with_job,
            "replayable": replayable,
        },
        "chain": chain,
    })))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct RederiveQuery {
    /// Revision to re-derive (default: the newest revision carrying data).
    revision: Option<i64>,
}

#[utoipa::path(
    post,
    path = "/provenance/{app}/{dataset}/{key}/rederive",
    tag = "provenance",
    params(
        ("app" = String, Path, description = "App namespace"),
        ("dataset" = String, Path, description = "Dataset name"),
        ("key" = String, Path, description = "Record key"),
        RederiveQuery,
    ),
    responses(
        (status = 200, description = "`{verdict: reproduced|diverged, diff?}` — the archived body \
            replayed through the revision's registered ruleset, compared field-by-field against \
            the stored snapshot. Read-only: nothing is written either way."),
        (status = 404, description = "No such record / revision"),
        (status = 409, description = "Not replayable, with the reason: stamp incomplete \
            (artifact_sha/rules_hash unknown), ruleset not registered, archived body missing, \
            or body no longer matching its stamped hash"),
    )
)]
pub(crate) async fn rederive_provenance(
    State(state): State<AppState>,
    Path((app, dataset, key)): Path<(String, String, String)>,
    Query(query): Query<RederiveQuery>,
) -> Result<Json<Value>, ApiError> {
    let not_replayable = |reason: String| ApiError(StatusCode::CONFLICT, reason);

    let record = state
        .datasets
        .get(&app, &dataset, &key)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "no such record".into()))?;
    let chain = state
        .datasets
        .history(&app, &dataset, &key, MAX_CHAIN)
        .await?;
    let rev: &Revision = match query.revision {
        Some(n) => chain
            .iter()
            .find(|r| r.revision == n)
            .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no revision {n}")))?,
        None => chain
            .iter()
            .find(|r| r.data.is_some())
            .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "record has no data revisions".into()))?,
    };
    let stored = rev.data.as_ref().ok_or_else(|| {
        not_replayable(format!(
            "revision {} is a removal — it carries no snapshot to reproduce",
            rev.revision
        ))
    })?;

    // Re-derivation needs BOTH pins. Anything less is refused with the exact
    // gap — replaying current rules or an unverified body would fabricate a
    // reproducibility claim.
    let missing: Vec<&str> = [
        ("artifact_sha", rev.provenance.artifact_sha.is_none()),
        ("rules_hash", rev.provenance.rules_hash.is_none()),
    ]
    .iter()
    .filter_map(|(name, gone)| gone.then_some(*name))
    .collect();
    if !missing.is_empty() {
        return Err(not_replayable(format!(
            "revision {} is not replayable: {} unknown (honest-Null stamp — the write path \
             did not know it, and it will not be invented)",
            rev.revision,
            missing.join(" and ")
        )));
    }
    let artifact_sha = rev.provenance.artifact_sha.as_deref().expect("checked");
    let stamped_rules_hash = rev.provenance.rules_hash.as_deref().expect("checked");

    // Historical ruleset, pinned by hash in the content-addressed registry —
    // never the app's current config.
    let rules_json = state
        .datasets
        .rules_by_hash(stamped_rules_hash)
        .await?
        .ok_or_else(|| {
            not_replayable(format!(
                "rules {stamped_rules_hash} not in the rules_versions registry — the ruleset \
                 was stamped but never registered, so the historical rules cannot be replayed"
            ))
        })?;
    debug_assert_eq!(rules_hash(&rules_json), stamped_rules_hash);
    let rules: RuleSet = serde_json::from_value(rules_json)
        .map_err(|e| not_replayable(format!("registered rules unparseable as a RuleSet: {e}")))?;
    let compiled = rules
        .compile()
        .map_err(|e| not_replayable(format!("registered rules no longer compile: {e}")))?;

    // Locate the archived body via the record's own artifact convention
    // (`artifact_path` + `job_id` under data/artifacts/<app>/<job_id>/ — the
    // crawl→extract seam), then verify it IS the stamped body by hash.
    let body = load_artifact(&state, &app, &record.data)
        .map_err(|reason| not_replayable(format!("archived body unavailable: {reason}")))?;
    let body_sha = format!("{:x}", Sha256::digest(body.as_bytes()));
    if body_sha != artifact_sha {
        return Err(not_replayable(format!(
            "archived body hash {body_sha} does not match the stamped artifact_sha \
             {artifact_sha} — the file on disk is not the body this revision was derived from"
        )));
    }

    // Replay, then compare against the snapshot minus its `_`-prefixed meta
    // stamps (`_url`, `_observed_at`, … are written by the pipeline around the
    // rules, not produced by them) — reported, not hidden.
    let replayed = pumper_core::extract_one(&compiled, &body);
    let (stored_cmp, ignored_meta) = strip_meta(stored);
    let verdict_reproduced = canonical(&stored_cmp) == canonical(&replayed);
    let mut out = json!({
        "app": app,
        "dataset": dataset,
        "key": key,
        "revision": rev.revision,
        "artifact_sha": artifact_sha,
        "rules_hash": stamped_rules_hash,
        "ignored_meta_fields": ignored_meta,
        "verdict": if verdict_reproduced { "reproduced" } else { "diverged" },
    });
    if !verdict_reproduced {
        out["diff"] = diff_values(&stored_cmp, &replayed);
    }
    Ok(Json(out))
}

/// Reads the archived body a record's data points at, guarding every untrusted
/// segment against path traversal (the `read_source_artifact` rules).
fn load_artifact(state: &AppState, app: &str, data: &Value) -> Result<String, String> {
    let artifact = data
        .get("artifact_path")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "record carries no artifact_path".to_string())?;
    let job_id = data
        .get("job_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "record carries no job_id to locate its artifact dir".to_string())?;
    for (what, s) in [("app", app), ("job_id", job_id), ("artifact_path", artifact)] {
        if s.is_empty()
            || s == "."
            || s == ".."
            || s.contains('/')
            || s.contains('\\')
            || std::path::Path::new(s).is_absolute()
        {
            return Err(format!("unsafe {what} segment: {s:?}"));
        }
    }
    let path = state
        .storage
        .artifacts_dir
        .join(app)
        .join(job_id)
        .join(artifact);
    std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))
}

/// Splits a stored snapshot into (comparable value, ignored `_meta` keys):
/// top-level keys starting with `_` are pipeline stamps, not rule output.
fn strip_meta(stored: &Value) -> (Value, Vec<String>) {
    let Value::Object(map) = stored else {
        return (stored.clone(), Vec::new());
    };
    let mut kept = Map::new();
    let mut ignored = Vec::new();
    for (k, v) in map {
        if k.starts_with('_') {
            ignored.push(k.clone());
        } else {
            kept.insert(k.clone(), v.clone());
        }
    }
    (Value::Object(kept), ignored)
}

/// Byte-canonical form for the reproduced-vs-diverged verdict: serde_json maps
/// are BTreeMaps, so `to_string` is key-sorted and deterministic.
fn canonical(v: &Value) -> String {
    v.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strip_meta_removes_only_underscore_prefixed_top_level_keys() {
        let (kept, ignored) = strip_meta(&json!({
            "title": "A", "_url": "https://x", "_observed_at": "t", "price": 3,
            "nested": { "_inner": "stays" }
        }));
        assert_eq!(
            kept,
            json!({ "title": "A", "price": 3, "nested": { "_inner": "stays" } })
        );
        assert_eq!(ignored, vec!["_observed_at".to_string(), "_url".to_string()]);
        // Non-objects pass through untouched.
        let (kept, ignored) = strip_meta(&json!([1, 2]));
        assert_eq!(kept, json!([1, 2]));
        assert!(ignored.is_empty());
    }

    #[test]
    fn canonical_is_key_order_stable() {
        assert_eq!(
            canonical(&json!({ "b": 1, "a": 2 })),
            canonical(&json!({ "a": 2, "b": 1 }))
        );
    }
}
