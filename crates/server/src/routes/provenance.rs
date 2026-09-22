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
        (status = 200, description = "`{verdict: reproduced|diverged, diverged, base_url, diff?}` \
            — the archived body replayed through the revision's registered ruleset, compared \
            field-by-field against the stored snapshot. An enrolled source's revision resolves \
            its PINNED body (`body_pins`) and replays it with the pin's original URL as \
            `base_url`; any other revision uses the record's `artifact_path` + `job_id` with \
            `base_url: null`. When the record's HEAD revision has no `artifact_sha` (data that \
            predates enrolment) but its key was pinned after that revision, the latest pinned \
            observation is replayed instead: `replayed_from: \"latest_observation\"`, \
            `observation_id` names it, and `rules_from` says whether the ruleset is the pinning \
            job's reported `rules_hash` (`observation_job`) or the active head (`active`); \
            otherwise `replayed_from: \"revision\"`, `observation_id: null`, `rules_from: \
            \"revision\"`. Read-only: nothing is written either way."),
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
        None => chain.iter().find(|r| r.data.is_some()).ok_or_else(|| {
            ApiError(StatusCode::NOT_FOUND, "record has no data revisions".into())
        })?,
    };
    let stored = rev.data.as_ref().ok_or_else(|| {
        not_replayable(format!(
            "revision {} is a removal — it carries no snapshot to reproduce",
            rev.revision
        ))
    })?;

    // An enrolled source's record whose head revision predates enrolment
    // carries no `artifact_sha` (an equal-value run appends no revision), but
    // its body IS pinned: fall back to the latest observation of this key.
    let head_revision = chain.iter().find(|r| r.data.is_some()).map(|r| r.revision);
    let latest_pin = match rev.provenance.artifact_sha {
        Some(_) => None,
        None => {
            state
                .datasets
                .latest_body_pin_for_key(&format!("{app}/{dataset}"), &key)
                .await?
        }
    };
    let source = replay_source(
        rev.provenance.artifact_sha.as_deref(),
        head_revision == Some(rev.revision),
        rev.created_at,
        latest_pin.as_ref(),
    );
    let replay = match source {
        ReplaySource::LatestObservation => {
            let pin = latest_pin.expect("replay_source chose a pin");
            replay_from_observation(&state, &app, &dataset, pin).await?
        }
        ReplaySource::Revision => replay_from_revision(&state, &app, &record.data, rev).await?,
    };
    let PreparedReplay {
        compiled,
        rules_hash: used_rules_hash,
        rules_from,
        artifact_sha,
        body,
        base,
        observation_id,
    } = replay;
    let artifact_sha = artifact_sha.as_str();
    let stamped_rules_hash = used_rules_hash.as_str();

    // Replay, then compare against the snapshot minus its `_`-prefixed meta
    // stamps (`_url`, `_observed_at`, … are written by the pipeline around the
    // rules, not produced by them) — reported, not hidden.
    let replayed = replay_body(&compiled, &body, base.as_deref());
    let (stored_cmp, ignored_meta) = strip_meta(stored);
    let verdict_reproduced = canonical(&stored_cmp) == canonical(&replayed);
    let mut out = json!({
        "app": app,
        "dataset": dataset,
        "key": key,
        "revision": rev.revision,
        "artifact_sha": artifact_sha,
        "rules_hash": stamped_rules_hash,
        "rules_from": rules_from,
        "base_url": base,
        "replayed_from": source.as_str(),
        "observation_id": observation_id,
        "ignored_meta_fields": ignored_meta,
        "verdict": if verdict_reproduced { "reproduced" } else { "diverged" },
        "diverged": !verdict_reproduced,
    });
    if !verdict_reproduced {
        out["diff"] = diff_values(&stored_cmp, &replayed);
    }
    Ok(Json(out))
}

/// Which body a rederive replays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplaySource {
    /// The body the revision's own `artifact_sha` names (or, lacking one, the
    /// honest 409 that follows).
    Revision,
    /// The latest pinned observation of the record's document key.
    LatestObservation,
}

impl ReplaySource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Revision => "revision",
            Self::LatestObservation => "latest_observation",
        }
    }
}

/// Chooses the body a rederive replays.
///
/// THE ANTI-PATTERN THIS CLOSES: enrolling a source with existing data (inline
/// run → import → `rules_ref` run) re-confirmed every value, so no revision
/// was appended and every record's head revision kept the inline run's
/// `artifact_sha: NULL` — rederive 409'd on every pre-existing row although
/// each one's body was pinned in `body_pins`.
///
/// The fallback is taken only when all three hold: the revision has no
/// `artifact_sha`; it is the record's HEAD data revision (an older snapshot
/// must never be judged against a newer body); and a pin of this key exists
/// that was captured at or after the revision was written (so the pinned run
/// re-confirmed exactly this snapshot — a changed value would have appended a
/// newer, stamped revision). Anything else stays on the revision, whose
/// missing stamp produces the existing honest 409.
pub(crate) fn replay_source(
    rev_artifact_sha: Option<&str>,
    rev_is_head: bool,
    rev_created_at: chrono::DateTime<chrono::Utc>,
    latest_pin: Option<&pumper_core::datasets::BodyPin>,
) -> ReplaySource {
    if rev_artifact_sha.is_some() || !rev_is_head {
        return ReplaySource::Revision;
    }
    let pinned_after = latest_pin
        .and_then(|p| chrono::DateTime::parse_from_rfc3339(&p.captured_at).ok())
        .is_some_and(|at| at.with_timezone(&chrono::Utc) >= rev_created_at);
    if pinned_after {
        ReplaySource::LatestObservation
    } else {
        ReplaySource::Revision
    }
}

/// Everything a rederive needs, however the body was located.
struct PreparedReplay {
    compiled: pumper_core::CompiledRuleSet,
    rules_hash: String,
    /// `"revision"` (the revision's stamp), `"observation_job"` (the hash the
    /// pinning job's result reports it ran), or `"active"` (the source's head).
    rules_from: &'static str,
    artifact_sha: String,
    body: String,
    base: Option<String>,
    observation_id: Option<String>,
}

/// Loads, parses and compiles a registered ruleset, as a 409 when it cannot.
async fn compile_registered(
    state: &AppState,
    hash: &str,
) -> Result<pumper_core::CompiledRuleSet, ApiError> {
    let not_replayable = |reason: String| ApiError(StatusCode::CONFLICT, reason);
    let rules_json = state.datasets.rules_by_hash(hash).await?.ok_or_else(|| {
        not_replayable(format!(
            "rules {hash} not in the rules_versions registry — the ruleset \
             was stamped but never registered, so the historical rules cannot be replayed"
        ))
    })?;
    debug_assert_eq!(rules_hash(&rules_json), hash);
    let rules: RuleSet = serde_json::from_value(rules_json)
        .map_err(|e| not_replayable(format!("registered rules unparseable as a RuleSet: {e}")))?;
    rules
        .compile()
        .map_err(|e| not_replayable(format!("registered rules no longer compile: {e}")))
}

/// The replay of the latest pinned observation of a key: its body, its
/// `original_url` as base, and the ruleset that observation ran — the hash the
/// pinning job's own result reports (`rules_hash`, which the extractor writes
/// from the same registration it stamps with), else the source's active head.
async fn replay_from_observation(
    state: &AppState,
    app: &str,
    dataset: &str,
    pin: pumper_core::datasets::BodyPin,
) -> Result<PreparedReplay, ApiError> {
    let not_replayable = |reason: String| ApiError(StatusCode::CONFLICT, reason);
    let job_rules = match pin.job_id.as_deref().map(uuid::Uuid::parse_str) {
        Some(Ok(id)) => state.storage.get(id).await?.and_then(|job| {
            job.result
                .as_ref()
                .and_then(|r| r.get("rules_hash"))
                .and_then(Value::as_str)
                .map(str::to_string)
        }),
        _ => None,
    };
    let (hash, rules_from) = match job_rules {
        Some(h) => (h, "observation_job"),
        None => {
            let head = state
                .datasets
                .ruleset_head(&format!("{app}/{dataset}"))
                .await?
                .ok_or_else(|| {
                    not_replayable(format!(
                        "observation {} names no ruleset and the source has no active head",
                        pin.observation_id
                    ))
                })?;
            (head.rules_hash, "active")
        }
    };
    let compiled = compile_registered(state, &hash).await?;
    let body = load_pinned_body(state, &pin)
        .map_err(|reason| not_replayable(format!("pinned body unavailable: {reason}")))?;
    verify_body(&body, &pin.body_sha)?;
    Ok(PreparedReplay {
        compiled,
        rules_hash: hash,
        rules_from,
        artifact_sha: pin.body_sha,
        body,
        base: pin.original_url,
        observation_id: Some(pin.observation_id),
    })
}

/// 409 unless `body` IS the body `sha` names.
fn verify_body(body: &str, sha: &str) -> Result<(), ApiError> {
    let body_sha = format!("{:x}", Sha256::digest(body.as_bytes()));
    if body_sha != sha {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!(
                "archived body hash {body_sha} does not match the stamped artifact_sha \
                 {sha} — the file on disk is not the body this revision was derived from"
            ),
        ));
    }
    Ok(())
}

/// The replay a revision's own stamp names.
async fn replay_from_revision(
    state: &AppState,
    app: &str,
    record_data: &Value,
    rev: &Revision,
) -> Result<PreparedReplay, ApiError> {
    let not_replayable = |reason: String| ApiError(StatusCode::CONFLICT, reason);
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
    let compiled = compile_registered(state, stamped_rules_hash).await?;

    // Locate the archived body. An enrolled source's revision cites a PINNED
    // body (`body_pins` → `<app>/bodies/<sha>.html`), fetched from the URL the
    // pin recorded — which is also the base its links resolved against. Any
    // other revision uses the record's own artifact convention (`artifact_path`
    // + `job_id` under data/artifacts/<app>/<job_id>/ — the crawl→extract
    // seam). Either way the body is then verified to BE the stamped one.
    let pinned = state
        .datasets
        .body_pin(artifact_sha, rev.provenance.source_url.as_deref())
        .await?;
    let (body, base) = match pinned {
        Some(pin) => {
            let body = load_pinned_body(state, &pin)
                .map_err(|reason| not_replayable(format!("pinned body unavailable: {reason}")))?;
            (body, pin.original_url)
        }
        None => (
            load_artifact(state, app, record_data)
                .map_err(|reason| not_replayable(format!("archived body unavailable: {reason}")))?,
            None,
        ),
    };
    verify_body(&body, artifact_sha)?;
    Ok(PreparedReplay {
        compiled,
        rules_hash: stamped_rules_hash.to_string(),
        rules_from: "revision",
        artifact_sha: artifact_sha.to_string(),
        body,
        base,
        observation_id: None,
    })
}

/// Replays one body through a compiled ruleset. With a `base` (a pinned body's
/// original URL) `url_absolute` resolves exactly as it did when the row was
/// written; without one the replay is the historical base-less extraction —
/// resolving against something invented here would fabricate a match.
fn replay_body(compiled: &pumper_core::CompiledRuleSet, body: &str, base: Option<&str>) -> Value {
    match base {
        Some(url) => pumper_core::extract::extract_one_with_report_at(compiled, body, Some(url)).0,
        None => pumper_core::extract_one(compiled, body),
    }
}

/// Reads a pinned body from its content address. The address is built only
/// from the pin's source app and a 64-hex sha (see `pinned_body_path`), so a
/// stored row cannot steer the read outside the artifacts root.
fn load_pinned_body(
    state: &AppState,
    pin: &pumper_core::datasets::BodyPin,
) -> Result<String, String> {
    let app = pin
        .source_id
        .split_once('/')
        .map_or(pin.source_id.as_str(), |(a, _)| a);
    let path =
        pumper_core::datasets::pinned_body_path(&state.storage.artifacts_dir, app, &pin.body_sha)
            .ok_or_else(|| format!("unsafe pinned-body address for {}", pin.observation_id))?;
    std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))
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
    for (what, s) in [
        ("app", app),
        ("job_id", job_id),
        ("artifact_path", artifact),
    ] {
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
        assert_eq!(
            ignored,
            vec!["_observed_at".to_string(), "_url".to_string()]
        );
        // Non-objects pass through untouched.
        let (kept, ignored) = strip_meta(&json!([1, 2]));
        assert_eq!(kept, json!([1, 2]));
        assert!(ignored.is_empty());
    }

    /// Serves one fixed body for every URL.
    struct OnePage(String);

    #[async_trait::async_trait]
    impl pumper_core::HttpClient for OnePage {
        async fn fetch(
            &self,
            req: pumper_core::HttpRequest,
        ) -> pumper_core::Result<pumper_core::HttpResponse> {
            Ok(pumper_core::HttpResponse {
                status: 200,
                headers: Default::default(),
                body: self.0.clone(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    /// The anti-pattern: every extractor revision was unreplayable — no
    /// `artifact_sha` was ever stamped, and rederive replayed without the
    /// document's URL, so a `url_absolute` field could only ever "diverge".
    #[tokio::test]
    async fn an_enrolled_extractor_row_rederives_not_diverges() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use http_body_util::BodyExt;
        use pumper_core::ScrapeApp;
        use std::sync::Arc;
        use tower::ServiceExt;

        let (state, store) = crate::routes::health::ruleset_pointer_tests::headless_state().await;
        // Enrol extractor/d, as `POST /sources/{id}/rules/import` does.
        state
            .health
            .store()
            .unwrap()
            .ensure_source("extractor", "d")
            .await
            .unwrap();
        let rules: RuleSet = serde_json::from_value(json!({
            "link": {"type": "css", "selector": "a", "attr": "href",
                     "transforms": [{"op": "url_absolute"}]}
        }))
        .unwrap();
        let hash = state.datasets.register_ruleset(&rules).await.unwrap();
        state
            .datasets
            .import_ruleset("extractor/d", &hash)
            .await
            .unwrap();

        let site = Arc::new(OnePage(
            "<html><a href=\"/item/1\">one</a></html>".to_string(),
        ));
        let ctx = pumper_core::testing::TestContext::new(&store.storage, "extractor")
            .params(json!({"rules_ref": "active", "urls": ["https://s.test/p"], "dataset": "d"}))
            .engines(pumper_core::testing::engines_with(
                site,
                Arc::new(pumper_core::testing::Dead),
                Arc::new(pumper_core::testing::Dead),
            ))
            .build();
        let out = app_extractor::Extractor.run(ctx).await.unwrap();
        assert_eq!(out["new"], 1, "{out}");

        let req = Request::builder()
            .method("POST")
            .uri("/provenance/extractor/d/https:%2F%2Fs.test%2Fp/rederive")
            .body(Body::empty())
            .unwrap();
        let resp = crate::routes::router(state.clone())
            .oneshot(req)
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["verdict"], "reproduced", "{body}");
        assert_eq!(body["diverged"], false);
        assert_eq!(body["base_url"], "https://s.test/p");
        assert_eq!(body["rules_hash"], json!(hash));
        // A stamped revision replays its own body; every key is present.
        assert_eq!(body["replayed_from"], "revision");
        assert_eq!(body["observation_id"], Value::Null);
        assert_eq!(body["rules_from"], "revision");
    }

    fn pin_at(at: &str) -> pumper_core::datasets::BodyPin {
        pumper_core::datasets::BodyPin {
            body_sha: "s".into(),
            observation_id: "j:k".into(),
            source_id: "extractor/d".into(),
            job_id: Some("j".into()),
            document_key: Some("k".into()),
            original_url: Some("k".into()),
            captured_at: at.into(),
        }
    }

    /// The smoke-test defect: enrolling existing data re-confirmed every value,
    /// so the head revision kept `artifact_sha: NULL` and rederive 409'd on
    /// every row although each body was pinned.
    #[test]
    fn enrolling_existing_data_makes_rows_replayable_not_409() {
        let written = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let later = pin_at("2026-01-02T00:00:00.000000Z");
        let earlier = pin_at("2025-12-31T00:00:00.000000Z");
        // The fix: an unstamped head revision re-confirmed by a later pin.
        assert_eq!(
            replay_source(None, true, written, Some(&later)),
            ReplaySource::LatestObservation
        );
        // A stamped revision always replays its own body.
        assert_eq!(
            replay_source(Some("sha"), true, written, Some(&later)),
            ReplaySource::Revision
        );
        // An OLDER snapshot is never judged against a newer body.
        assert_eq!(
            replay_source(None, false, written, Some(&later)),
            ReplaySource::Revision
        );
        // No pin, or a pin from before the revision: the honest 409 path.
        assert_eq!(
            replay_source(None, true, written, None),
            ReplaySource::Revision
        );
        assert_eq!(
            replay_source(None, true, written, Some(&earlier)),
            ReplaySource::Revision
        );
        assert_eq!(
            replay_source(None, true, written, Some(&pin_at("not a time"))),
            ReplaySource::Revision
        );
        assert_eq!(
            ReplaySource::LatestObservation.as_str(),
            "latest_observation"
        );
        assert_eq!(ReplaySource::Revision.as_str(), "revision");
    }

    /// The Director's smoke, exactly: inline run → import → `rules_ref` run
    /// with identical values (no revision appended) → rederive.
    #[tokio::test]
    async fn rederive_after_enrolling_existing_data_replays_the_latest_observation() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use http_body_util::BodyExt;
        use pumper_core::storage::EnqueueOptions;
        use pumper_core::ScrapeApp;
        use std::sync::Arc;
        use tower::ServiceExt;

        let (state, store) = crate::routes::health::ruleset_pointer_tests::headless_state().await;
        let rules = json!({
            "title": {"type": "css", "selector": "h1"},
            "link": {"type": "css", "selector": "a", "attr": "href",
                     "transforms": [{"op": "url_absolute"}]}
        });
        let page = "<html><h1>T</h1><a href=\"/item/1\">one</a></html>".to_string();
        let run = |params: Value, job_id: Option<uuid::Uuid>| {
            let mut ctx = pumper_core::testing::TestContext::new(&store.storage, "extractor")
                .params(params)
                .engines(pumper_core::testing::engines_with(
                    Arc::new(OnePage(page.clone())),
                    Arc::new(pumper_core::testing::Dead),
                    Arc::new(pumper_core::testing::Dead),
                ))
                .build();
            if let Some(id) = job_id {
                ctx.job_id = id;
            }
            async move { app_extractor::Extractor.run(ctx).await.unwrap() }
        };
        let rederive = |state: crate::state::AppState| async move {
            let req = Request::builder()
                .method("POST")
                .uri("/provenance/extractor/d/https:%2F%2Fs.test%2Fp/rederive")
                .body(Body::empty())
                .unwrap();
            let resp = crate::routes::router(state).oneshot(req).await.unwrap();
            let status = resp.status();
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            (
                status,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        };

        // 1. Inline run: writes revision 1 with no artifact_sha.
        let out = run(
            json!({"rules": rules, "urls": ["https://s.test/p"], "dataset": "d"}),
            None,
        )
        .await;
        assert_eq!(out["new"], 1, "{out}");
        let (status, body) = rederive(state.clone()).await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "control: not replayable yet: {body}"
        );

        // 2. Import (the observe path created the source row).
        let parsed: RuleSet = serde_json::from_value(rules.clone()).unwrap();
        let hash = state.datasets.register_ruleset(&parsed).await.unwrap();
        state
            .datasets
            .import_ruleset("extractor/d", &hash)
            .await
            .unwrap();

        // 3. rules_ref run with identical values, as a real job row so its
        //    result carries the rules_hash it ran.
        let params = json!({"rules_ref": "active", "urls": ["https://s.test/p"], "dataset": "d"});
        state
            .storage
            .enqueue(
                "extractor",
                EnqueueOptions {
                    params: params.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let job = state.storage.claim_next(&[], 0.0).await.unwrap().unwrap();
        let out = run(params, Some(job.id)).await;
        assert_eq!(out["unchanged"], 1, "identical values, no revision: {out}");
        state
            .storage
            .complete(job.id, job.attempts, out)
            .await
            .unwrap();
        let revs = state
            .datasets
            .history("extractor", "d", "https://s.test/p", 10)
            .await
            .unwrap();
        assert_eq!(revs.len(), 1, "the head is still the inline run's revision");
        assert_eq!(revs[0].provenance.artifact_sha, None);

        // 4. Rederive: replayed from the pinned observation, not a 409.
        let (status, body) = rederive(state.clone()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["diverged"], false, "{body}");
        assert_eq!(body["replayed_from"], "latest_observation");
        assert_eq!(
            body["observation_id"],
            json!(format!("{}:https://s.test/p", job.id))
        );
        assert_eq!(body["rules_from"], "observation_job");
        assert_eq!(body["rules_hash"], json!(hash));
        assert_eq!(body["base_url"], "https://s.test/p");

        // …and the source now reads as repairable: its pins exist.
        let facts = state
            .datasets
            .source_repair_facts(&["extractor/d"])
            .await
            .unwrap();
        let f = &facts["extractor/d"];
        assert_eq!(
            crate::routes::health::repairable_reason(
                "extractor",
                f.active_rules_hash.as_deref(),
                f.pinned_bodies
            ),
            None
        );
    }

    #[test]
    fn canonical_is_key_order_stable() {
        assert_eq!(
            canonical(&json!({ "b": 1, "a": 2 })),
            canonical(&json!({ "a": 2, "b": 1 }))
        );
    }
}
