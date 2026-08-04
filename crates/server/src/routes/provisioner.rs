//! `app-provisioner`'s proposal lifecycle (`provisioner/proposals`): list what
//! is waiting for review, re-validate a proposal's drafted rules against a
//! FRESH fetch, and promote an accepted one into a paste-ready catalog TOML
//! fragment.
//!
//! `app-provisioner` (`crates/apps/provisioner`) NEVER writes
//! `catalog/data-sources.toml` and NEVER creates a schedule — see that
//! crate's module doc and its `never_writes_catalog_invariant_...` test. This
//! module does not relax that: `promote_proposal` only ever RETURNS the TOML
//! fragment (rendered server-side by [`app_provisioner::catalog_toml`] from
//! the stored `catalog_row`, never re-invented here). Going live is still the
//! real contract in ONBOARDING.md Path B — write the app crate, register it,
//! and hand-add the `[[source]]` entry — which this route cannot shortcut.
//! See `docs/features/apps.md` "provisioner: proposal lifecycle".

use app_provisioner::{
    catalog_toml, may_promote, proposal_is_expired, validate_sample, STATUS_FAILED,
    STATUS_PROMOTED, STATUS_VALIDATED,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use pumper_core::datasets::Provenance;
use pumper_core::{FetchRequest, FetchStrategy, Record, RuleSet, Source};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use crate::routes::error::{default_limit, keyset_cursor, parse_cursor, ApiError};
use crate::state::AppState;

/// The one app/dataset pair this whole module reads and writes. Named once so
/// every handler agrees with `app-provisioner`'s own `ctx.upsert_with_provenance
/// ("proposals", ...)` call (`crates/apps/provisioner/src/lib.rs`) about where a
/// proposal record lives.
const APP: &str = "provisioner";
const DATASET: &str = "proposals";

/// Fetches one proposal record, or a `404` naming the key — the one lookup
/// every handler in this module starts with.
async fn require_proposal(state: &AppState, key: &str) -> Result<Record, ApiError> {
    state
        .datasets
        .get(APP, DATASET, key)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no proposal '{key}'")))
}

/// The record's lifecycle `status`, defaulting to `planned` for a record
/// written before this field existed (none can exist pre-M44-lifecycle in
/// practice, but a missing field must never be a panic).
fn proposal_status(data: &Value) -> String {
    data.get("status")
        .and_then(Value::as_str)
        .unwrap_or(app_provisioner::STATUS_PLANNED)
        .to_string()
}

/// The list row a reviewer scans a backlog by: status, the frozen compile-time
/// verdict, the catalog-scale confidence, the sampled engine/url, and age —
/// never the full compiled rule set / samples / dry-run report (those stay
/// reachable at `GET /datasets/provisioner/proposals` or `.../history`, the
/// generic dataset surface). `expired` is computed here, at read time, against
/// `[provisioner] proposal_max_age_secs` — nothing stamps it onto the stored
/// record (see `ProvisionerConfig`'s doc for why lazy-on-list beats a tick).
fn proposal_summary(record: &Record, max_age_secs: u64, now: DateTime<Utc>) -> Value {
    let status = proposal_status(&record.data);
    let age_secs = (now - record.updated_at).num_seconds().max(0);
    let expired = proposal_is_expired(&status, age_secs, max_age_secs as i64);
    let catalog_row = record.data.get("catalog_row");
    json!({
        "key": record.key,
        "prompt": record.data.get("prompt"),
        "status": status,
        "expired": expired,
        "verdict": record.data.get("verdict"),
        "accepted": record.data.get("accepted"),
        "catalog_confidence": record.data.get("catalog_confidence"),
        "engine": catalog_row.and_then(|r| r.get("engine")),
        "url": catalog_row.and_then(|r| r.get("url")),
        "intended_dataset": record.data.get("intended_dataset"),
        "first_seen": record.first_seen,
        "updated_at": record.updated_at,
        "age_secs": age_secs,
    })
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct ProposalsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

/// Lists provisioner proposals, newest-touched first — the backlog a reviewer
/// works through before hand-applying ONBOARDING.md Path B.
#[utoipa::path(
    get,
    path = "/provisioner/proposals",
    tag = "provisioner",
    params(ProposalsQuery),
    responses((status = 200, description = "Dual-mode: bare `[ProposalSummary]` array, or `{items, next_cursor}` when `cursor` is present. Each summary: `key, prompt, status` (planned|validated|failed|promoted), `expired` (computed against `[provisioner] proposal_max_age_secs`; only a still-`planned` proposal can be flagged), the frozen compile-time `verdict`/`accepted`, `catalog_confidence`, the sampled `engine`/`url`, `intended_dataset`, and `age_secs`."))
)]
pub(crate) async fn list_proposals(
    State(state): State<AppState>,
    Query(query): Query<ProposalsQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 1000);
    let max_age_secs = state.config.provisioner.proposal_max_age_secs;
    let now = Utc::now();
    let Some(cursor) = &query.cursor else {
        let records = state
            .datasets
            .list_records_view(APP, DATASET, &[], None, limit, None, false)
            .await?;
        let items: Vec<Value> = records
            .iter()
            .map(|r| proposal_summary(r, max_age_secs, now))
            .collect();
        return Ok(Json(json!(items)));
    };
    let after = parse_cursor(cursor);
    let records = state
        .datasets
        .list_records_view(APP, DATASET, &[], after, limit, None, false)
        .await?;
    let next_cursor = keyset_cursor(&records, limit, |r| {
        format!("{}|{}", pumper_core::datasets::ts(r.updated_at), r.key)
    });
    let items: Vec<Value> = records
        .iter()
        .map(|r| proposal_summary(r, max_age_secs, now))
        .collect();
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

/// Builds the fetch this route re-validates against: the SAME shape-learning
/// request `run()`'s sampling stage issues (`Auto` strategy, markdown alongside
/// html so the claude tier's fallback still lands, recipes opportunistically
/// tried) but deliberately with `archive_max_age` left unset — validation exists
/// to catch drift the original compile could not have seen, so it must never be
/// satisfied by a snapshot as old as (or older than) that compile itself.
fn fresh_validate_request(url: &str) -> FetchRequest {
    let mut req = FetchRequest::new(url);
    req.strategy = FetchStrategy::Auto;
    req.to_markdown = true;
    req.use_recipes = true;
    req
}

/// Re-runs a proposal's drafted `RuleSet` against a freshly fetched sample of
/// its primary URL and records the verdict as the proposal's new `status`
/// (`validated` | `failed`), leaving the original compile-time `accepted` /
/// `verdict` untouched — those are the frozen record of what the compile saw,
/// this is what THIS check just saw.
#[utoipa::path(
    post,
    path = "/provisioner/proposals/{key}/validate",
    tag = "provisioner",
    params(("key" = String, Path, description = "Proposal key")),
    responses(
        (status = 200, description = "`{key, status, validation}` — `status` is `validated` or `failed`; `validation` carries the fresh `sample` (fetch tier, body field, byte count, per-tier trace) and the `dry_run` report (same shape as the compile-time `sample_stats`), plus `checked_at`."),
        (status = 404, description = "No proposal with this key", body = Object),
        (status = 400, description = "The stored rule_set no longer parses, the catalog_row has no url, the fresh fetch failed, or it yielded no sampleable body", body = Object),
    )
)]
pub(crate) async fn validate_proposal(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let record = require_proposal(&state, &key).await?;
    let bad = |msg: String| ApiError(StatusCode::BAD_REQUEST, msg);

    let rules: RuleSet = serde_json::from_value(record.data["rule_set"].clone()).map_err(|e| {
        bad(format!(
            "proposal '{key}': stored rule_set no longer parses: {e}"
        ))
    })?;
    let url = record.data["catalog_row"]["url"]
        .as_str()
        .ok_or_else(|| {
            bad(format!(
                "proposal '{key}': catalog_row has no url to validate"
            ))
        })?
        .to_string();

    let outcome = state
        .engines
        .fetch
        .fetch(fresh_validate_request(&url))
        .await
        .map_err(|e| bad(format!("proposal '{key}': fetch of {url} failed: {e}")))?;
    let (sample, dry) =
        validate_sample(outcome, &rules).map_err(|e| bad(format!("proposal '{key}': {e}")))?;

    let status = if dry.accepted {
        STATUS_VALIDATED
    } else {
        STATUS_FAILED
    };
    let validation = json!({
        "checked_at": Utc::now().to_rfc3339(),
        "sample": sample,
        "dry_run": dry,
        "accepted": dry.accepted,
    });
    let mut data = record.data.clone();
    data["status"] = json!(status);
    data["validation"] = validation.clone();

    let prov = Provenance {
        source_url: Some(url),
        ..Provenance::default()
    };
    state
        .datasets
        .upsert_stamped(APP, DATASET, &key, &data, None, Some(&prov))
        .await?;

    Ok(Json(
        json!({ "key": key, "status": status, "validation": validation }),
    ))
}

/// Promotes a proposal: renders the paste-ready `[[source]]` TOML fragment
/// server-side from the stored `catalog_row` (via [`app_provisioner::catalog_toml`]
/// — the SAME renderer `run()` used to produce the fragment in the first
/// place, so the two can never drift) and marks the record `promoted`. Still
/// writes nothing to `catalog/data-sources.toml`: a human pastes the returned
/// fragment and completes ONBOARDING.md Path B.
#[utoipa::path(
    post,
    path = "/provisioner/proposals/{key}/promote",
    tag = "provisioner",
    params(("key" = String, Path, description = "Proposal key")),
    responses(
        (status = 200, description = "`{key, status: \"promoted\", catalog_toml}` — the fragment to paste into catalog/data-sources.toml after finishing ONBOARDING.md Path B (app crate + registry entry). Writes nothing to the catalog file itself."),
        (status = 404, description = "No proposal with this key", body = Object),
        (status = 409, description = "The proposal's best evidence says its rule set does not bind (a failed re-validation, or a rejected compile-time verdict that was never re-validated) — promoting it would hand out a fragment for a draft already known not to work", body = Object),
        (status = 400, description = "The stored catalog_row no longer parses", body = Object),
    )
)]
pub(crate) async fn promote_proposal(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let record = require_proposal(&state, &key).await?;
    let status = proposal_status(&record.data);
    let accepted = record.data["accepted"].as_bool().unwrap_or(false);
    if !may_promote(&status, accepted) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!(
                "proposal '{key}' cannot be promoted: status='{status}', compile-time \
                 accepted={accepted}. Validate it (POST /provisioner/proposals/{key}/validate) \
                 until it passes, or re-propose with a revised prompt."
            ),
        ));
    }

    let row: Source = serde_json::from_value(record.data["catalog_row"].clone()).map_err(|e| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("proposal '{key}': stored catalog_row no longer parses: {e}"),
        )
    })?;
    let toml = catalog_toml(&row);

    let mut data = record.data.clone();
    data["status"] = json!(STATUS_PROMOTED);
    data["promoted_at"] = json!(Utc::now().to_rfc3339());
    data["catalog_toml"] = json!(toml);
    state
        .datasets
        .upsert_stamped(APP, DATASET, &key, &data, None, None)
        .await?;

    Ok(Json(
        json!({ "key": key, "status": STATUS_PROMOTED, "catalog_toml": toml }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn record_with(data: Value, updated_at: DateTime<Utc>) -> Record {
        Record {
            key: "k".into(),
            data,
            first_seen: updated_at,
            last_seen: updated_at,
            updated_at,
            removed_at: None,
            trust: "stable".into(),
        }
    }

    /// A record written before the `status` field existed (or one whose JSON
    /// was hand-edited to drop it) must read as `planned`, never panic — the
    /// same defensive-default posture the rest of this route module's field
    /// reads take.
    #[test]
    fn proposal_status_defaults_to_planned_when_the_field_is_absent() {
        assert_eq!(proposal_status(&json!({})), "planned");
        assert_eq!(
            proposal_status(&json!({"status": "validated"})),
            "validated"
        );
    }

    #[test]
    fn summary_surfaces_the_reviewer_scan_fields_and_computes_age_and_expiry() {
        let now = Utc::now();
        let old = now - Duration::seconds(200);
        let data = json!({
            "prompt": "track widget prices",
            "status": "planned",
            "verdict": "accepted",
            "accepted": true,
            "catalog_confidence": 4,
            "catalog_row": {"engine": "http", "url": "https://a.example"},
            "intended_dataset": "proposed:track-widget-prices",
        });
        let rec = record_with(data, old);
        let s = proposal_summary(&rec, 100, now);
        assert_eq!(s["key"], json!("k"));
        assert_eq!(s["engine"], json!("http"));
        assert_eq!(s["url"], json!("https://a.example"));
        assert_eq!(s["age_secs"], json!(200));
        assert_eq!(
            s["expired"],
            json!(true),
            "planned + past the configured window => expired"
        );

        // The same age, but already validated: never expired.
        let validated = record_with(json!({"status": "validated"}), old);
        assert_eq!(
            proposal_summary(&validated, 100, now)["expired"],
            json!(false)
        );
    }

    #[test]
    fn fresh_validate_request_never_sets_an_archive_window() {
        let req = fresh_validate_request("https://a.example");
        assert_eq!(req.strategy, FetchStrategy::Auto);
        assert!(req.to_markdown);
        assert!(req.use_recipes);
        assert!(
            req.archive_max_age.is_none(),
            "validation must never be satisfiable by a stale archive snapshot"
        );
    }
}
