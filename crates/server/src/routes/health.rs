//! Extraction health: per-source degradation detection. `/catalog/health`
//! answers "did this source run recently"; these endpoints answer "was what it
//! produced right" — a source appears here once it has reported a run.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::routes::error::{default_limit, ApiError};
use crate::state::AppState;

/// Runs returned per source on the detail view — enough to see the ladder being
/// climbed without paging.
const SOURCE_RUN_PREVIEW: i64 = 10;

#[derive(Deserialize, IntoParams)]
pub(crate) struct SourcesQuery {
    /// Only sources in this state (`healthy|suspect|degraded|quarantined|probation|retired`).
    state: Option<String>,
    /// Only sources served by this app.
    app: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
}

/// Extraction-health table: one row per `(app, dataset)` source, worst
/// degradation score first.
///
/// `/catalog/health` answers "did this source run recently"; this answers "was
/// what it produced right". A source appears here once it has reported a run.
#[utoipa::path(
    get,
    path = "/sources",
    tag = "sources",
    params(SourcesQuery),
    responses(
        (status = 200, description = "`{enabled, enforcing, contracts_enforce, count, sources: \
            [{id, app, dataset, state, degradation_score, state_since, last_verdict, \
            tripped_of_last3, ..., contract?}]}`. \
            `enforcing: false` means verdicts are recorded but nothing is gated. `contract` is \
            the latest declared-contract verdict (`{verdict: pass|warn|block, violations, ...}`) \
            for sources with a `[source.contract]` catalog block that have run since boot."),
        (status = 503, description = "Detection is disabled ([resilience] enabled = false)", body = Object),
    )
)]
pub(crate) async fn list_sources(
    State(state): State<AppState>,
    Query(query): Query<SourcesQuery>,
) -> Result<Json<Value>, ApiError> {
    let store = health_store(&state)?;
    let sources = store
        .list_sources(
            query.state.as_deref(),
            query.app.as_deref(),
            query.limit.clamp(1, 500),
        )
        .await?;
    // Declared-contract verdicts (M20) ride along per row: the inferred health
    // in this table and the declared floor are the two halves of "was the
    // output right", so they read together.
    let sources: Vec<Value> = sources
        .iter()
        .map(|s| {
            let mut row = serde_json::to_value(s).unwrap_or(Value::Null);
            if let Value::Object(map) = &mut row {
                if let Some(v) = contract_verdict(&state, &s.id) {
                    map.insert("contract".into(), v);
                }
            }
            row
        })
        .collect();
    Ok(Json(json!({
        "enabled": true,
        "enforcing": state.health.enforcing(),
        "contracts_enforce": state.config.contracts.enforce,
        "count": sources.len(),
        "sources": sources,
    })))
}

/// One source's health in full: its state, the last runs with the tests behind
/// each verdict, this run's per-field sketch against the baseline, and the mined
/// invariants.
#[utoipa::path(
    get,
    path = "/sources/{id}",
    tag = "sources",
    params(("id" = String, Path, description = "Source id, `<app>/<dataset>`")),
    responses(
        (status = 200, description = "`{source, runs, fields, invariants, statistical_coverage}`. \
            `fields` pairs the latest run's per-field sketch with its baseline; \
            `statistical_coverage: false` means the source never reaches the cohort \
            floor and is monitored only by the assumption-free rules."),
        (status = 404, description = "Unknown source", body = Object),
        (status = 503, description = "Detection is disabled", body = Object),
    )
)]
pub(crate) async fn get_source(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let store = health_store(&state)?;
    let source = store
        .source(&id)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("unknown source '{id}'")))?;
    let runs = store.runs(&id, SOURCE_RUN_PREVIEW).await?;
    let cfg = state.health.config();
    let baseline = store.baseline(&id, cfg.window_runs).await?;
    // The latest run's sketches, whatever its verdict — the point of this view is
    // to show what the last run looked like next to what the source normally does.
    let latest = match runs.first() {
        Some(run) => store.run_sketches(&id, &run.job_id).await?,
        None => Default::default(),
    };
    let fields: Vec<Value> = latest
        .iter()
        .map(|(field, sketch)| {
            let (base_misses, base_docs) = baseline.pooled_misses(field);
            json!({
                "field": field,
                "docs": sketch.n,
                "miss_rate": sketch.miss_rate(),
                "coercion_failure_rate": sketch.coercion_failure_rate(),
                "distinct_ratio": sketch.distinct_ratio,
                "mean_len": sketch.mean_len(),
                "baseline_runs": baseline.runs(field),
                "baseline_miss_rate":
                    if base_docs == 0 { Value::Null } else { json!(base_misses as f64 / base_docs as f64) },
                "baseline_distinct_ratio":
                    pumper_core::resilience::sketch::median(&baseline.series(field, |s| s.distinct_ratio as f64)),
            })
        })
        .collect();
    let coverage = runs
        .first()
        .map(|r| r.docs >= cfg.min_cohort_docs as i64)
        .unwrap_or(false);
    Ok(Json(json!({
        "source": source,
        "contract": contract_verdict(&state, &id).unwrap_or(Value::Null),
        "enforcing": state.health.enforcing(),
        "statistical_coverage": coverage,
        "runs": runs,
        "fields": fields,
        "invariants": store.invariants(&id).await?,
        "see_also": "/catalog/health — freshness (did it run?)",
    })))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct SourceRunsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
}

/// A source's verdict history, newest first. Each run carries the `reasons`
/// array: every test that ran, its value and its threshold, so a verdict explains
/// itself without re-running anything.
#[utoipa::path(
    get,
    path = "/sources/{id}/runs",
    tag = "sources",
    params(("id" = String, Path, description = "Source id, `<app>/<dataset>`"), SourceRunsQuery),
    responses(
        (status = 200, description = "`{id, count, runs: [{job_id, docs, fetch_ok_rate, d_text, \
            d_dom, d_val, verdict, diagnosis, score, reasons, state_after, build_id, created_at}]}`"),
        (status = 503, description = "Detection is disabled", body = Object),
    )
)]
pub(crate) async fn source_runs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<SourceRunsQuery>,
) -> Result<Json<Value>, ApiError> {
    let runs = health_store(&state)?
        .runs(&id, query.limit.clamp(1, 500))
        .await?;
    Ok(Json(json!({ "id": id, "count": runs.len(), "runs": runs })))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct SourceStateBody {
    /// `healthy|suspect|degraded|quarantined|probation|retired`.
    state: String,
    /// Why — recorded on the row, because the only other thing that moves state
    /// is the detector.
    reason: Option<String>,
}

/// Manual state override: un-quarantine a source that has been fixed, or retire a
/// dead one.
///
/// This is the only way out of `quarantined`. Quarantine is deliberately terminal
/// without an operator — a stuck source is an acceptable outcome, a source that
/// silently un-quarantines itself and resumes pushing garbage downstream is not.
#[utoipa::path(
    post,
    path = "/sources/{id}/state",
    tag = "sources",
    params(("id" = String, Path, description = "Source id, `<app>/<dataset>`")),
    request_body = SourceStateBody,
    responses(
        (status = 200, description = "`{id, state, reason}`"),
        (status = 400, description = "Unrecognized state", body = Object),
        (status = 404, description = "Unknown source", body = Object),
        (status = 503, description = "Detection is disabled", body = Object),
    )
)]
pub(crate) async fn set_source_state(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<SourceStateBody>,
) -> Result<Json<Value>, ApiError> {
    let store = health_store(&state)?;
    // Parse strictly here, unlike the fail-open read path: an operator typo must
    // not silently reset a source to healthy.
    let parsed = pumper_core::SourceState::parse(&body.state);
    if parsed.as_str() != body.state {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "unknown state '{}' — expected one of healthy|suspect|degraded|quarantined|probation|retired",
                body.state
            ),
        ));
    }
    let reason = body.reason.unwrap_or_else(|| "manual override".to_string());
    if !store.set_state_manual(&id, parsed, &reason).await? {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("unknown source '{id}'"),
        ));
    }
    Ok(Json(
        json!({ "id": id, "state": parsed.as_str(), "reason": reason }),
    ))
}

/// The latest publish-time data-contract verdict for a `<app>/<dataset>`
/// source id (M20), recorded by the worker seam. In-memory: null-absent before
/// the first contracted run since boot. None when no verdict exists — sources
/// without a declared contract simply carry no `contract` key.
fn contract_verdict(state: &AppState, id: &str) -> Option<Value> {
    state
        .contract_verdicts
        .lock()
        .expect("contract verdict lock")
        .get(id)
        .cloned()
}

/// The health store, or 503 when detection is switched off — a health question
/// asked of a disabled detector has no honest answer, and returning an empty list
/// would read as "everything is fine".
fn health_store(state: &AppState) -> Result<&pumper_core::HealthStore, ApiError> {
    state.health.store().ok_or_else(|| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "extraction-health detection is disabled ([resilience] enabled = false)".into(),
        )
    })
}
