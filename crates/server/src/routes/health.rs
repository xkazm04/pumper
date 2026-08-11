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
        (status = 200, description = "`{enabled, enforcing, contracts_enforce, count, unmonitored, \
            sources: [{id, app, dataset, state, degradation_score, state_since, last_verdict, \
            tripped_of_last3, monitored, ..., contract?}]}`. \
            `monitored: false` means the source has never produced a cohort at or above \
            `[resilience] min_cohort_docs`, so the distributional tests have never applied to \
            it — `state: \"healthy\"` on such a row means *unwatched*, not *verified*, and \
            `unmonitored` counts those rows. \
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
    // Counted before the rows are flattened to JSON: a caller reading the table
    // should see, in one number, how much of it is not actually being watched.
    let unmonitored = sources.iter().filter(|s| !s.monitored).count();
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
        "unmonitored": unmonitored,
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
            `statistical_coverage: false` means the *latest run* was below the cohort \
            floor (recorded with verdict `below_cohort`: it moved neither the state nor \
            the baseline), and `source.monitored: false` means no run ever cleared it, so \
            the source is watched only by the assumption-free rules."),
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

#[derive(Deserialize, IntoParams)]
pub(crate) struct EnforcementPreviewQuery {
    /// Only sources served by this app.
    app: Option<String>,
    /// Stored runs replayed per source, newest-backwards (default 60, max 1000).
    runs: Option<i64>,
    /// Sources to replay (default 500).
    limit: Option<i64>,
}

/// **What `[resilience] enforce = true` would have done.** Read-only replay of
/// the stored verdicts — it changes nothing, gates nothing, and re-judges
/// nothing.
///
/// `enforce` ships `false`, and this is the answer to the only question that
/// gates turning it on. Soak mode is a no-op strictly *downstream*: every run is
/// judged, the ladder moves, and the verdict/score/`reasons` are written whether
/// or not enforcement is on — the single thing `enforce` changes is that the
/// four gated consumers read `Healthy` instead of the real state. So this replays
/// the recorded `state_after` of each run, in order, and reports what each state
/// would have gated.
///
/// **Fidelity, not re-simulation.** Every verdict here is the one recorded at the
/// time by the rules in force at the time. Runs the detector could not judge
/// (`inconclusive`, `content_empty`, `below_cohort`) moved nothing, and are never
/// credited with a transition; a state change across such a run is reported with
/// `cause: "outside"` (an operator override, or a pruned run), not attributed to
/// it.
#[utoipa::path(
    get,
    path = "/enforcement/preview",
    tag = "sources",
    params(EnforcementPreviewQuery),
    responses(
        (status = 200, description = "`{enforcing, ready, not_ready: [{id, state, gates, since}], \
            unmonitored, totals, sources: [{id, state, gates, live_state, monitored, \
            window_opens_at, window_opens_in, runs_replayed, unjudged_runs, \
            transitions: [{at, from, to, cause, verdict, score, diagnosis, reasons, gates}], \
            consequences}]}`. \
            `ready: true` means no source's current state gates anything, so flipping \
            `[resilience] enforce` would change nothing about the next run; `not_ready` names \
            the sources that make it false. Counts are **runs and the documents in them**, \
            never deliveries — how many webhooks a suppressed run would have sent is not \
            stored. Deletes and writes nothing."),
        (status = 503, description = "Detection is disabled ([resilience] enabled = false)", body = Object),
    )
)]
pub(crate) async fn enforcement_preview(
    State(state): State<AppState>,
    Query(query): Query<EnforcementPreviewQuery>,
) -> Result<Json<Value>, ApiError> {
    let store = health_store(&state)?;
    let preview = pumper_core::preview_fleet(
        store,
        state.health.enforcing(),
        query.app.as_deref(),
        preview_runs(query.runs),
        query.limit.unwrap_or(500).clamp(1, 500),
    )
    .await?;
    Ok(Json(serde_json::to_value(preview).unwrap_or(Value::Null)))
}

/// Resolves `?runs=` to the bound this route's own param documentation promises
/// (default 60, 1..=1000), matching the `limit` clamp beside it.
///
/// `preview_fleet` clamps identically today, so this is not a live unbounded
/// read — it is the promise being made *where it is documented*, at the boundary
/// the caller talks to, instead of depending on a core internal that a refactor
/// could drop without any test noticing. The clamp is silent on purpose: an
/// out-of-range `runs` is a browse-surface convenience, not the data-loss case
/// that earns a 400 (see `parse_cursor_arg`).
pub(crate) fn preview_runs(requested: Option<i64>) -> i64 {
    requested
        .unwrap_or(pumper_core::resilience::preview::DEFAULT_REPLAY_RUNS)
        .clamp(1, MAX_REPLAY_RUNS)
}

/// The documented ceiling on `?runs=` — see [`preview_runs`].
const MAX_REPLAY_RUNS: i64 = 1000;

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
/// The operator path out of `quarantined`, and the only way to `retired`. A
/// source also leaves quarantine on its own after `[resilience] recovery_runs`
/// consecutive clean *judged* runs — into `probation`, never straight to
/// `healthy`, so a premature release is stamped `provisional` rather than silent.
/// This endpoint is the shortcut for an operator who already knows it is fixed.
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
    super::error::lock_advisory(&state.contract_verdicts, "contract_verdicts")
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

#[cfg(test)]
mod tests {
    use super::{preview_runs, MAX_REPLAY_RUNS};
    use pumper_core::resilience::preview::DEFAULT_REPLAY_RUNS;

    /// The anti-pattern: a caller-supplied `runs` passing through the route
    /// untouched while the param's own documentation promised `default 60, max
    /// 1000`. The bound has to be expressed where it is documented, not left to
    /// a core internal a refactor could quietly drop.
    #[test]
    fn preview_runs_honours_the_documented_bound_not_the_raw_param() {
        assert_eq!(preview_runs(None), DEFAULT_REPLAY_RUNS);
        assert_eq!(preview_runs(Some(120)), 120);
        assert_eq!(preview_runs(Some(MAX_REPLAY_RUNS)), MAX_REPLAY_RUNS);
        // Above the ceiling, and the pathological end of it.
        assert_eq!(preview_runs(Some(50_000)), MAX_REPLAY_RUNS);
        assert_eq!(preview_runs(Some(i64::MAX)), MAX_REPLAY_RUNS);
        // Below the floor: zero and negative are a "replay nothing" that would
        // report a confidently empty preview.
        assert_eq!(preview_runs(Some(0)), 1);
        assert_eq!(preview_runs(Some(-7)), 1);
        assert_eq!(preview_runs(Some(i64::MIN)), 1);
    }
}
