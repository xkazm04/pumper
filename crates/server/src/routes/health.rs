//! Extraction health: per-source degradation detection. `/catalog/health`
//! answers "did this source run recently"; these endpoints answer "was what it
//! produced right" — a source appears here once it has reported a run.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use pumper_core::catalog::{Catalog, ContractsStatus, Source};
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
    /// Matches the stored column, so `unknown` — the rendered state of a row this
    /// build cannot read — is never a stored value to filter on.
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
        (status = 200, description = "`{enabled, enforcing, contracts_enforce, contracts, count, \
            unmonitored, sources: [{id, app, dataset, state, degradation_score, state_since, \
            last_verdict, tripped_of_last3, monitored, ..., contract?, active_rules_hash, \
            repairable, repairable_reason}]}`. \
            `active_rules_hash` is the source's active ruleset head (null = not enrolled). \
            `repairable: false` carries exactly one `repairable_reason` (`not an extractor \
            source`, `inline rules` = no ruleset head, or `no pinned bodies`); \
            `repairable: true` carries `repairable_reason: null`. \
            `monitored: false` means the source has never produced a cohort at or above \
            `[resilience] min_cohort_docs`, so the distributional tests have never applied to \
            it — `state: \"healthy\"` on such a row means *unwatched*, not *verified*, and \
            `unmonitored` counts those rows. `state: \"unknown\"` is narrower and means the \
            health could not be READ (a stored state this build does not recognize, or a \
            failed lookup); it gates nothing, exactly as a failed read always did, but it is \
            no longer reported as `healthy`. \
            `enforcing: false` means verdicts are recorded but nothing is gated. \
            `contracts_enforce` is the configured `[contracts] enforce`; `contracts` \
            (`{enforce_configured, enforce_observed, catalog_ok, catalog_error?, declared, \
            reason?}`) is what enforcement can be **observed** to do — `enforce_observed: false` \
            beside `enforce_configured: true` means the catalog would not parse, so the \
            publish seam fails open and checks nothing. `contract` is the latest \
            declared-contract verdict (`{verdict: pass|warn|block, violations, job_id, \
            checked_at, age_secs, stale, stale_reason?, ...}`) for sources with a \
            `[source.contract]` catalog block that have run since boot; verdicts are held in \
            memory and never expire on their own, so `stale: true` (age past the source's \
            freshness window, or the source is no longer live) marks a verdict that describes \
            a run that is no longer current, and `stale: null` one that cannot be judged."),
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
    // The catalog is what makes a *recorded* verdict a *current* one: it carries
    // the freshness window to age it against, and it is the only thing that knows
    // the source is still live. Loaded fail-open, exactly like the worker seam —
    // an unreadable catalog degrades the rendering, it never 500s this table.
    let (catalog, contracts) = ContractsStatus::load(state.config.contracts.enforce);
    let windows = verdict_windows(catalog.as_ref(), super::query::CATALOG_STALE_GRACE);
    let now = chrono::Utc::now();
    let ids: Vec<&str> = sources.iter().map(|s| s.id.as_str()).collect();
    let facts = state.datasets.source_repair_facts(&ids).await?;
    let sources: Vec<Value> = sources
        .iter()
        .map(|s| {
            let mut row = serde_json::to_value(s).unwrap_or(Value::Null);
            if let Value::Object(map) = &mut row {
                if let Some(v) = contract_verdict(&state, &s.id) {
                    let window = window_for(windows.as_ref(), &s.id);
                    map.insert("contract".into(), verdict_with_age(v, window, now));
                }
                insert_repair_fields(map, &s.app, facts.get(&s.id));
            }
            row
        })
        .collect();
    Ok(Json(json!({
        "enabled": true,
        "enforcing": state.health.enforcing(),
        // Configured intent, kept for compatibility; `contracts` beside it is
        // what is actually being observed.
        "contracts_enforce": state.config.contracts.enforce,
        "contracts": contracts,
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
        (status = 200, description = "`{source, contract, runs, fields, invariants, \
            statistical_coverage}`. `source` carries `active_rules_hash`, `repairable` and \
            `repairable_reason` exactly as the `/sources` rows do. `contract` is the latest declared-contract verdict with its \
            own `age_secs`/`stale`/`stale_reason?` attached (null when none recorded since boot); \
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
    // Same treatment as the table: a verdict on the detail view carries its age
    // and its staleness, never a bare `pass`.
    let (catalog, _) = ContractsStatus::load(state.config.contracts.enforce);
    let windows = verdict_windows(catalog.as_ref(), super::query::CATALOG_STALE_GRACE);
    let contract = contract_verdict(&state, &id)
        .map(|v| verdict_with_age(v, window_for(windows.as_ref(), &id), chrono::Utc::now()))
        .unwrap_or(Value::Null);
    let facts = state.datasets.source_repair_facts(&[id.as_str()]).await?;
    let mut source_row = serde_json::to_value(&source).unwrap_or(Value::Null);
    if let Value::Object(map) = &mut source_row {
        insert_repair_fields(map, &source.app, facts.get(&id));
    }
    Ok(Json(json!({
        "source": source_row,
        "contract": contract,
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
    // not silently reset a source to healthy. `unknown` is rejected alongside the
    // typos — it is the answer a FAILED read gives, never a rung, and writing it
    // into the column would manufacture the very unreadable row it exists to
    // report.
    let parsed = pumper_core::SourceState::parse(&body.state);
    if parsed == pumper_core::SourceState::Unknown || parsed.as_str() != body.state {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "unsettable state '{}' — expected one of healthy|suspect|degraded|quarantined|probation|retired \
                 (`unknown` is read-only: it is what a health read that failed reports)",
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

// ── ruleset pointer (Stage 2: pointer + reprint) ────────────────────────────

/// The only app whose sources carry a declarative, registrable RuleSet.
const EXTRACTOR_APP: &str = "extractor";

/// Pinned documents one dry run replays (the replay mode's default page cap).
const DRY_RUN_MAX_DOCS: usize = 500;

/// Why a source cannot be repaired, or `None` when it can.
///
/// Exactly one reason, checked in dependency order — a hardcoded app has no
/// rules to point at, a rule-backed source with no pointer runs inline rules,
/// and an enrolled source with no retained bodies has nothing to replay:
/// `"not an extractor source"` → `"inline rules"` → `"no pinned bodies"`.
pub(crate) fn repairable_reason(
    app: &str,
    active_rules_hash: Option<&str>,
    pinned_bodies: i64,
) -> Option<&'static str> {
    if app != EXTRACTOR_APP {
        return Some("not an extractor source");
    }
    if active_rules_hash.is_none() {
        return Some("inline rules");
    }
    if pinned_bodies <= 0 {
        return Some("no pinned bodies");
    }
    None
}

/// Writes `active_rules_hash`, `repairable` and `repairable_reason` onto a
/// rendered source row — every key always present, `null` when unknown.
fn insert_repair_fields(
    map: &mut serde_json::Map<String, Value>,
    app: &str,
    facts: Option<&pumper_core::datasets::SourceRepairFacts>,
) {
    let active = facts.and_then(|f| f.active_rules_hash.clone());
    let pins = facts.map_or(0, |f| f.pinned_bodies);
    let reason = repairable_reason(app, active.as_deref(), pins);
    map.insert("active_rules_hash".into(), json!(active));
    map.insert("repairable".into(), json!(reason.is_none()));
    map.insert("repairable_reason".into(), json!(reason));
}

/// A refused pointer operation, as its HTTP answer.
fn ruleset_error(e: pumper_core::datasets::RulesetError) -> ApiError {
    use pumper_core::datasets::RulesetError as R;
    match e {
        R::BadRef(m) => ApiError(StatusCode::BAD_REQUEST, m),
        R::Unknown(m) => ApiError(StatusCode::NOT_FOUND, m),
        R::Conflict(m) => ApiError(StatusCode::CONFLICT, m),
        R::Store(e) => ApiError::from(e),
    }
}

/// 404 unless the source has a `sources` row — pointer endpoints never create
/// one (only a reported run does).
async fn require_source(state: &AppState, id: &str) -> Result<(), ApiError> {
    let facts = state.datasets.source_repair_facts(&[id]).await?;
    if facts.contains_key(id) {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("unknown source '{id}' (a source exists once it has reported a run)"),
        ))
    }
}

/// Parses and compiles a caller-supplied RuleSet — both are the caller's
/// input, so both failures are a 400, never the 500 a core parse error maps to.
fn parse_candidate_rules(rules: &Value) -> Result<pumper_core::RuleSet, ApiError> {
    let parsed: pumper_core::RuleSet = serde_json::from_value(rules.clone())
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("bad rules: {e}")))?;
    parsed.compile().map_err(|e| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("rules do not compile: {e}"),
        )
    })?;
    Ok(parsed)
}

/// The registered RuleSet behind a hash (a 409 when the registry lost it — a
/// pointer to a body that is not there is a store inconsistency, stated).
async fn registered_rules(state: &AppState, hash: &str) -> Result<pumper_core::RuleSet, ApiError> {
    let json = state.datasets.rules_by_hash(hash).await?.ok_or_else(|| {
        ApiError(
            StatusCode::CONFLICT,
            format!("ruleset {hash} is not in the rules_versions registry"),
        )
    })?;
    serde_json::from_value(json).map_err(|e| {
        ApiError(
            StatusCode::CONFLICT,
            format!("registered ruleset {hash} is not a RuleSet: {e}"),
        )
    })
}

/// The dry-run diff of `candidate` against the source's active head over its
/// pinned bodies. Writes nothing.
async fn dry_run_diff(
    state: &AppState,
    id: &str,
    candidate: &pumper_core::RuleSet,
    candidate_hash: &str,
) -> Result<Value, ApiError> {
    let head = state.datasets.ruleset_head(id).await?;
    let baseline = match &head {
        Some(h) => Some(registered_rules(state, &h.rules_hash).await?),
        None => None,
    };
    let replay = app_extractor::replay_pinned_bodies(
        &state.datasets,
        &state.storage.artifacts_dir,
        id,
        candidate,
        baseline.as_ref(),
        DRY_RUN_MAX_DOCS,
    )
    .await?;
    Ok(json!({
        "id": id,
        "dry_run": true,
        "candidate_hash": candidate_hash,
        "active": head,
        "replay": replay,
    }))
}

/// A source's ruleset pointer: the active head and its full lineage.
#[utoipa::path(
    get,
    path = "/sources/{id}/rules",
    tag = "sources",
    params(("id" = String, Path, description = "Source id, `<app>/<dataset>`")),
    responses(
        (status = 200, description = "`{id, active: {rules_hash, parent_hash, origin, epoch, \
            activated_at, licence_at_move} | null, lineage: [same, newest epoch first]}`. \
            `active: null` = not enrolled (the source runs inline rules)."),
        (status = 404, description = "Unknown source", body = Object),
    )
)]
pub(crate) async fn get_source_rules(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_source(&state, &id).await?;
    let active = state.datasets.ruleset_head(&id).await?;
    let lineage = state.datasets.ruleset_lineage(&id).await?;
    Ok(Json(
        json!({ "id": id, "active": active, "lineage": lineage }),
    ))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct RulesImportBody {
    /// An extractor job that wrote this source's dataset with inline
    /// `params.rules`.
    job_id: String,
}

/// Enrols a source: registers a job's inline `params.rules` (under its
/// canonical hash) as the source's FIRST ruleset head, origin `import`.
///
/// The running schedule's output does not change — it may switch to
/// `"rules_ref": "active"` afterwards and extract byte-identical records.
#[utoipa::path(
    post,
    path = "/sources/{id}/rules/import",
    tag = "sources",
    params(("id" = String, Path, description = "Source id, `<app>/<dataset>`")),
    request_body = RulesImportBody,
    responses(
        (status = 200, description = "`{id, active}` — the new head (epoch 1)"),
        (status = 400, description = "Bad job id; the job is not an extractor job writing this \
            source's dataset; or it carries no inline rules", body = Object),
        (status = 404, description = "Unknown source or job", body = Object),
        (status = 409, description = "The source already has an active ruleset", body = Object),
    )
)]
pub(crate) async fn import_source_rules(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<RulesImportBody>,
) -> Result<Json<Value>, ApiError> {
    let rules = import_rules_of_job(&state, &id, &body.job_id).await?;
    let hash = state.datasets.register_ruleset(&rules).await?;
    let head = state
        .datasets
        .import_ruleset(&id, &hash)
        .await
        .map_err(ruleset_error)?;
    Ok(Json(json!({ "id": id, "active": head })))
}

/// The inline RuleSet an import may register for `source_id`, from `job_id`.
async fn import_rules_of_job(
    state: &AppState,
    source_id: &str,
    job_id: &str,
) -> Result<pumper_core::RuleSet, ApiError> {
    let (app, dataset) = source_id.split_once('/').ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("source id '{source_id}' is not `<app>/<dataset>`"),
        )
    })?;
    let uuid = uuid::Uuid::parse_str(job_id)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("bad job_id: {e}")))?;
    let job = state
        .storage
        .get(uuid)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("unknown job '{job_id}'")))?;
    let job_dataset = job
        .params
        .get("dataset")
        .and_then(Value::as_str)
        .unwrap_or("extracted");
    if let Some(refusal) = import_refusal(app, dataset, &job.app, job_dataset, &job.params) {
        return Err(ApiError(StatusCode::BAD_REQUEST, refusal));
    }
    parse_candidate_rules(&job.params["rules"])
}

/// Why a job cannot be imported as `source_app/source_dataset`'s first head,
/// or `None` when it can. Pure, so every refusal is pinned by a test.
pub(crate) fn import_refusal(
    source_app: &str,
    source_dataset: &str,
    job_app: &str,
    job_dataset: &str,
    params: &Value,
) -> Option<String> {
    if source_app != EXTRACTOR_APP {
        return Some(format!(
            "source '{source_app}/{source_dataset}' is not an extractor source — only \
             extractor rulesets can be imported"
        ));
    }
    if job_app != EXTRACTOR_APP {
        return Some(format!("job is a '{job_app}' job, not an extractor job"));
    }
    if job_dataset != source_dataset {
        return Some(format!(
            "job writes dataset '{job_dataset}', not this source's '{source_dataset}'"
        ));
    }
    if !params.get("rules").is_some_and(Value::is_object) {
        return Some(
            "job carries no inline params.rules (a rules_ref job is already on the pointer)".into(),
        );
    }
    None
}

fn default_dry_run() -> bool {
    true
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct RulesActivateBody {
    /// The candidate RuleSet.
    #[schema(value_type = Object)]
    rules: Value,
    /// Default `true`: replay-diff only, nothing written.
    #[serde(default = "default_dry_run")]
    dry_run: bool,
}

/// Proposes a new ruleset for a source. **Dry run by default**: replays
/// `rules` against the active head over the source's pinned bodies and returns
/// the diff, writing nothing. `"dry_run": false` compiles, registers and
/// activates it (origin `human`).
#[utoipa::path(
    post,
    path = "/sources/{id}/rules",
    tag = "sources",
    params(("id" = String, Path, description = "Source id, `<app>/<dataset>`")),
    request_body = RulesActivateBody,
    responses(
        (status = 200, description = "Dry run: `{id, dry_run: true, candidate_hash, active, \
            replay: {source, docs, docs_matching, truncated, missing, missing_keys, baseline, \
            fields, inner_fields, regressed_urls, regressions}}`. Activation: `{id, dry_run: \
            false, active}` — the new head."),
        (status = 400, description = "Rules do not parse or compile; not an extractor source", body = Object),
        (status = 404, description = "Unknown source", body = Object),
        (status = 409, description = "That ruleset is already the active one", body = Object),
    )
)]
pub(crate) async fn propose_source_rules(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<RulesActivateBody>,
) -> Result<Json<Value>, ApiError> {
    require_extractor_source(&state, &id).await?;
    let rules = parse_candidate_rules(&body.rules)?;
    if body.dry_run {
        let hash = pumper_core::datasets::canonical_rules_hash(&rules);
        return Ok(Json(dry_run_diff(&state, &id, &rules, &hash).await?));
    }
    let hash = state.datasets.register_ruleset(&rules).await?;
    let head = state
        .datasets
        .activate_ruleset(&id, &hash, pumper_core::datasets::RulesetOrigin::Human)
        .await
        .map_err(ruleset_error)?;
    Ok(Json(json!({ "id": id, "dry_run": false, "active": head })))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct RulesRollbackBody {
    /// A hash or unique prefix (>= 8 hex) in this source's lineage.
    to: String,
    /// Default `true`: replay-diff only, nothing written.
    #[serde(default = "default_dry_run")]
    dry_run: bool,
}

/// Moves a source's pointer back to a ruleset in its own lineage. Same shape as
/// `POST /sources/{id}/rules`: dry run by default; `"dry_run": false` activates
/// it (origin `rollback`).
#[utoipa::path(
    post,
    path = "/sources/{id}/rules/rollback",
    tag = "sources",
    params(("id" = String, Path, description = "Source id, `<app>/<dataset>`")),
    request_body = RulesRollbackBody,
    responses(
        (status = 200, description = "Dry run: `{id, dry_run: true, candidate_hash, active, \
            replay}`. Activation: `{id, dry_run: false, active}`."),
        (status = 400, description = "`to` shorter than 8 hex, not hex, or ambiguous", body = Object),
        (status = 404, description = "Unknown source, or `to` matches nothing in the lineage", body = Object),
        (status = 409, description = "`to` is already the active ruleset", body = Object),
    )
)]
pub(crate) async fn rollback_source_rules(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<RulesRollbackBody>,
) -> Result<Json<Value>, ApiError> {
    require_extractor_source(&state, &id).await?;
    let hash = state
        .datasets
        .resolve_rules_ref(&id, &body.to)
        .await
        .map_err(ruleset_error)?;
    if body.dry_run {
        let rules = registered_rules(&state, &hash).await?;
        return Ok(Json(dry_run_diff(&state, &id, &rules, &hash).await?));
    }
    let head = state
        .datasets
        .activate_ruleset(&id, &hash, pumper_core::datasets::RulesetOrigin::Rollback)
        .await
        .map_err(ruleset_error)?;
    Ok(Json(json!({ "id": id, "dry_run": false, "active": head })))
}

/// 404 for an unknown source, 400 for a source whose app has no RuleSet.
async fn require_extractor_source(state: &AppState, id: &str) -> Result<(), ApiError> {
    require_source(state, id).await?;
    let app = id.split_once('/').map_or(id, |(a, _)| a);
    if app != EXTRACTOR_APP {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("source '{id}' is not an extractor source — it has no ruleset to move"),
        ));
    }
    Ok(())
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

/// What the catalog says about the source a stored verdict is keyed to — the
/// only input that can turn a recorded verdict into a *current* one.
///
/// The verdict map has exactly one mutation in the workspace (the worker's
/// `insert`): no remove, no clear, no generation. So a verdict outlives the run
/// that produced it, the dataset that stopped producing, and the source that was
/// retired — and without this the last `pass` is served forever as an
/// unqualified green.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SourceWindow {
    /// The catalog would not load, so nothing about a verdict can be judged.
    CatalogUnreadable,
    /// No **live** catalog source declares this `<app>/<dataset>` any more —
    /// retired, renamed or deleted. `/sources` joins by health-store id, which
    /// keeps rows through `retired`, so this is reachable there.
    NotLive,
    /// Live, and declares a freshness expectation (seconds).
    Secs(i64),
    /// Live, but declares no cadence and no `max_staleness_hours`: its verdicts
    /// are unjudgeable, which is not the same as fresh.
    NoExpectation,
}

impl SourceWindow {
    /// The window of a source known to be live — the same expression
    /// `/catalog/health` judges dataset writes by, so the two halves of "is this
    /// still true" cannot drift apart.
    pub(crate) fn of_live(source: &Source, grace: i64) -> Self {
        match source.freshness_window_secs(grace) {
            Some(secs) => Self::Secs(secs),
            None => Self::NoExpectation,
        }
    }
}

/// The `age_secs` + `stale` pair a rendered verdict carries, plus the reason
/// when the pair cannot be completed. Mirrors the shape `/catalog/health`
/// already computes for dataset freshness and `proposal_summary` computes for
/// proposal expiry.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VerdictFreshness {
    pub age_secs: Option<i64>,
    /// `None` = cannot be judged (and the row says why); never a silent `false`.
    pub stale: Option<bool>,
    pub reason: Option<&'static str>,
}

/// Ages a recorded contract verdict against its source's declared freshness
/// window, using the `checked_at` the worker already stamps (`worker.rs`) — no
/// new timestamp.
///
/// The anti-pattern this closes: the verdict blob rode onto `/sources` and
/// `/catalog/health` verbatim, carrying `checked_at`/`job_id` that **no consumer
/// derived anything from**, ~45 lines from where the identical `age_secs` +
/// `stale` pair is computed for dataset freshness.
pub(crate) fn verdict_freshness(
    checked_at: Option<&str>,
    window: SourceWindow,
    now: DateTime<Utc>,
) -> VerdictFreshness {
    let age_secs = checked_at
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| (now - t.with_timezone(&Utc)).num_seconds().max(0));
    let Some(age) = age_secs else {
        return VerdictFreshness {
            age_secs: None,
            stale: None,
            reason: Some("verdict carries no parsable checked_at"),
        };
    };
    let (stale, reason) = match window {
        // Retirement outranks age: a verdict for a source that no longer exists
        // is stale however recently it was written.
        SourceWindow::NotLive => (
            Some(true),
            Some("no live catalog source declares this contract (retired or removed)"),
        ),
        SourceWindow::CatalogUnreadable => (
            None,
            Some("catalog unreadable: verdict freshness cannot be judged"),
        ),
        SourceWindow::NoExpectation => (
            None,
            Some("source declares no freshness expectation (cadence and contract are both silent)"),
        ),
        SourceWindow::Secs(w) => (Some(age > w), None),
    };
    VerdictFreshness {
        age_secs: Some(age),
        stale,
        reason,
    }
}

/// Renders a stored verdict with its own age attached. Non-object verdicts are
/// returned untouched (nothing but the worker writes this map, but a read
/// surface must not panic on a shape it did not write).
pub(crate) fn verdict_with_age(
    mut verdict: Value,
    window: SourceWindow,
    now: DateTime<Utc>,
) -> Value {
    let Value::Object(map) = &mut verdict else {
        return verdict;
    };
    let fresh = verdict_freshness(map.get("checked_at").and_then(Value::as_str), window, now);
    map.insert("age_secs".into(), json!(fresh.age_secs));
    map.insert("stale".into(), json!(fresh.stale));
    if let Some(reason) = fresh.reason {
        map.insert("stale_reason".into(), json!(reason));
    }
    verdict
}

/// `<app>/<dataset>` → window, for every live catalog source that declares a
/// contract. `None` = the catalog would not load.
fn verdict_windows(catalog: Option<&Catalog>, grace: i64) -> Option<HashMap<String, SourceWindow>> {
    catalog.map(|c| {
        c.contracted()
            .map(|s| {
                (
                    format!("{}/{}", s.app, s.dataset),
                    SourceWindow::of_live(s, grace),
                )
            })
            .collect()
    })
}

/// The window for one source id: unreadable catalog → unjudgeable; readable but
/// unlisted → not live.
fn window_for(windows: Option<&HashMap<String, SourceWindow>>, id: &str) -> SourceWindow {
    match windows {
        None => SourceWindow::CatalogUnreadable,
        Some(map) => map.get(id).copied().unwrap_or(SourceWindow::NotLive),
    }
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
    use super::{preview_runs, verdict_freshness, verdict_with_age, SourceWindow, MAX_REPLAY_RUNS};
    use pumper_core::catalog::{Catalog, ContractsStatus};
    use pumper_core::resilience::preview::DEFAULT_REPLAY_RUNS;
    use serde_json::{json, Value};

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

    fn at(now: chrono::DateTime<chrono::Utc>, secs_ago: i64) -> String {
        pumper_core::datasets::ts(now - chrono::Duration::seconds(secs_ago))
    }

    /// The anti-pattern: the verdict map has exactly one mutation in the whole
    /// workspace (the worker's `insert`) — no remove, no clear, no generation —
    /// so a `{"verdict":"pass"}` from a dataset that stopped producing was
    /// served as an unqualified green forever, right beside the `checked_at` that
    /// proved it old.
    #[test]
    fn stale_verdict_is_distinguishable_from_a_fresh_pass() {
        let now = chrono::Utc::now();
        let day = 86_400;
        // Fresh: inside the window, judged, and judged NOT stale.
        let fresh = verdict_freshness(Some(&at(now, 60)), SourceWindow::Secs(2 * day), now);
        assert_eq!(fresh.stale, Some(false));
        assert!(fresh.age_secs.unwrap() >= 60 && fresh.age_secs.unwrap() < 120);
        assert_eq!(fresh.reason, None);

        // A dataset that stopped producing: same verdict blob, three days on.
        let old = verdict_freshness(Some(&at(now, 3 * day)), SourceWindow::Secs(2 * day), now);
        assert_eq!(old.stale, Some(true));
        assert!(old.age_secs.unwrap() >= 3 * day);

        // A retired source: recent verdict, but nothing live declares it. The
        // health store keeps `retired` rows, so `/sources` joins to exactly this.
        let retired = verdict_freshness(Some(&at(now, 60)), SourceWindow::NotLive, now);
        assert_eq!(retired.stale, Some(true));
        assert!(retired.reason.unwrap().contains("no live catalog source"));

        // Unjudgeable is never a silent `false`.
        for window in [SourceWindow::CatalogUnreadable, SourceWindow::NoExpectation] {
            let f = verdict_freshness(Some(&at(now, 10 * day)), window, now);
            assert_eq!(f.stale, None, "{window:?} must not claim fresh");
            assert!(f.reason.is_some(), "{window:?} must say why");
        }
        // No parsable stamp at all: no age, no verdict on staleness.
        let unstamped = verdict_freshness(None, SourceWindow::Secs(day), now);
        assert_eq!(unstamped.age_secs, None);
        assert_eq!(unstamped.stale, None);
        assert_eq!(
            verdict_freshness(Some("not-a-timestamp"), SourceWindow::Secs(day), now).stale,
            None
        );
    }

    /// The rendered blob must carry the pair, not just compute it: `/sources`
    /// and `/catalog/health` both hand the stored JSON straight to the client.
    #[test]
    fn rendered_verdict_carries_its_age_not_just_its_verdict() {
        let now = chrono::Utc::now();
        let stored = json!({
            "verdict": "pass",
            "violations": [],
            "job_id": "0195f0f0-0000-7000-8000-000000000000",
            "checked_at": at(now, 5 * 86_400),
        });
        let rendered = verdict_with_age(stored, SourceWindow::Secs(86_400), now);
        assert_eq!(rendered["verdict"], "pass");
        assert_eq!(rendered["stale"], json!(true));
        assert!(rendered["age_secs"].as_i64().unwrap() >= 5 * 86_400);
        // The stamps the worker writes survive untouched.
        assert!(rendered["checked_at"].is_string());
        assert!(rendered["job_id"].is_string());
        // A shape this surface did not write is passed through, never panicked on.
        assert_eq!(
            verdict_with_age(Value::Null, SourceWindow::Secs(1), now),
            Value::Null
        );
    }

    /// `/sources` never loaded the catalog, so `contracts_enforce: true` was a
    /// claim it could not observe — true even when the catalog would not parse
    /// and the publish seam was checking nothing at all.
    #[test]
    fn sources_reports_observed_enforcement_not_only_configured_intent() {
        let broken = ContractsStatus::unreadable("expected `=`", true);
        let rendered = serde_json::to_value(&broken).expect("serializes");
        assert_eq!(rendered["enforce_configured"], json!(true));
        assert_eq!(rendered["enforce_observed"], json!(false));
        assert_eq!(rendered["catalog_ok"], json!(false));
        assert!(rendered["catalog_error"].is_string());
        assert!(rendered["reason"].is_string());
        // A readable catalog with enforcement on is observed, and says so.
        let ok = serde_json::to_value(ContractsStatus::of(&Catalog::default(), true))
            .expect("serializes");
        assert_eq!(ok["enforce_observed"], json!(true));
    }
}

#[cfg(test)]
pub(super) mod ruleset_pointer_tests {
    use super::{import_refusal, repairable_reason};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use pumper_core::storage::EnqueueOptions;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tower::ServiceExt;

    const ID: &str = "extractor%2Fd";

    /// A headless `AppState` over a temp store (the `e2e::harness` assembly,
    /// which is private to that module tree). Keep the store alive.
    pub(crate) async fn headless_state() -> (crate::state::AppState, pumper_core::testing::TempStore)
    {
        let store = pumper_core::testing::TempStore::new("routes-ruleset").await;
        let mut config = pumper_core::config::Config::default();
        config.storage.database_path = store.path().join("pumper.db");
        config.storage.artifacts_dir = store.storage.artifacts_dir.clone();
        let state = crate::state::AppState::from_parts(crate::state::AppStateParts {
            config,
            storage: Arc::new(store.storage.clone()),
            governor: Arc::new(pumper_core::Governor::new(
                &pumper_core::config::GovernorConfig::default(),
            )),
            engines: pumper_core::testing::dead_engines(),
            plugins: Arc::new(pumper_core::NoPlugins),
            search: Arc::new(pumper_core::NoSearch),
            registry: Default::default(),
        })
        .expect("assemble AppState");
        (state, store)
    }

    /// The anti-pattern: a `repairable` flag computed from whichever fact was
    /// checked first, so a hardcoded app read as "inline rules" and an enrolled
    /// source with no bodies read as repairable.
    #[test]
    fn repairable_reason_names_the_first_missing_prerequisite_not_any() {
        assert_eq!(
            repairable_reason("mpsv-ispv", Some("h"), 10),
            Some("not an extractor source"),
            "a hardcoded app has no rules to point at, whatever else is true"
        );
        assert_eq!(
            repairable_reason("extractor", None, 10),
            Some("inline rules")
        );
        assert_eq!(
            repairable_reason("extractor", Some("h"), 0),
            Some("no pinned bodies")
        );
        assert_eq!(repairable_reason("extractor", Some("h"), 1), None);
    }

    #[test]
    fn import_refuses_every_job_that_is_not_this_sources_inline_extraction() {
        let rules = json!({ "rules": { "t": { "type": "css", "selector": "h1" } } });
        assert_eq!(
            import_refusal("extractor", "d", "extractor", "d", &rules),
            None
        );
        assert!(import_refusal("crawl", "pages", "extractor", "pages", &rules).is_some());
        assert!(import_refusal("extractor", "d", "crawl", "d", &rules).is_some());
        assert!(
            import_refusal("extractor", "d", "extractor", "other", &rules)
                .is_some_and(|m| m.contains("'other'"))
        );
        assert!(import_refusal(
            "extractor",
            "d",
            "extractor",
            "d",
            &json!({ "rules_ref": "active" })
        )
        .is_some_and(|m| m.contains("no inline")));
    }

    async fn call(
        router: &axum::Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(b) => {
                req = req.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&b).unwrap())
            }
            None => Body::empty(),
        };
        let resp = router
            .clone()
            .oneshot(req.body(body).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn pointer_rows(state: &crate::state::AppState) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM source_rulesets")
            .fetch_one(&state.storage.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn the_pointer_moves_one_row_per_activation_and_dry_runs_write_nothing() {
        let (state, _store) = headless_state().await;
        let router = crate::routes::router(state.clone());
        let first = json!({ "t": { "type": "css", "selector": "h1" } });
        let second = json!({ "t": { "type": "css", "selector": "h2" } });

        // No source row yet: every pointer route is a 404, and none creates one.
        let job = state
            .storage
            .enqueue(
                "extractor",
                EnqueueOptions {
                    params: json!({ "rules": first, "urls": ["https://a.test/"], "dataset": "d" }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let (status, _) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules/import"),
            Some(json!({ "job_id": job.id.to_string() })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = call(&router, "GET", &format!("/sources/{ID}/rules"), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        state
            .health
            .store()
            .unwrap()
            .ensure_source("extractor", "d")
            .await
            .unwrap();

        // Import: the first head.
        let (status, body) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules/import"),
            Some(json!({ "job_id": job.id.to_string() })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["active"]["epoch"], 1);
        assert_eq!(body["active"]["origin"], "import");
        let first_hash = body["active"]["rules_hash"].as_str().unwrap().to_string();
        assert_eq!(pointer_rows(&state).await, 1);

        // A second import is a conflict.
        let (status, body) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules/import"),
            Some(json!({ "job_id": job.id.to_string() })),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["code"], "conflict");

        // Dry run (the default): a diff, and zero pointer writes.
        let (status, body) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules"),
            Some(json!({ "rules": second })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], true);
        assert_eq!(body["active"]["rules_hash"], json!(first_hash));
        assert!(body["replay"]["fields"].is_array(), "{body}");
        assert_eq!(pointer_rows(&state).await, 1, "a dry run writes nothing");

        // Activation: exactly one more row.
        let (status, body) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules"),
            Some(json!({ "rules": second, "dry_run": false })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["active"]["epoch"], 2);
        assert_eq!(body["active"]["origin"], "human");
        assert_eq!(body["active"]["parent_hash"], json!(first_hash));
        assert_eq!(pointer_rows(&state).await, 2);

        // Rollback: dry by default, then one more row.
        let to = &first_hash[..10];
        let (status, body) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules/rollback"),
            Some(json!({ "to": to })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["candidate_hash"], json!(first_hash));
        assert_eq!(pointer_rows(&state).await, 2);
        let (status, body) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules/rollback"),
            Some(json!({ "to": to, "dry_run": false })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["active"]["epoch"], 3);
        assert_eq!(body["active"]["origin"], "rollback");
        assert_eq!(pointer_rows(&state).await, 3);

        // Refusals: a short prefix is a 400, an unknown one a 404.
        let (status, _) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules/rollback"),
            Some(json!({ "to": "abc" })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules/rollback"),
            Some(json!({ "to": "0000000000" })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        // Rules that do not compile are the caller's 400, not a 500.
        let (status, _) = call(
            &router,
            "POST",
            &format!("/sources/{ID}/rules"),
            Some(json!({ "rules": { "t": { "type": "css", "selector": "[[" } } })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // The pointer reads back, and the source rows carry the repair fields.
        let (status, body) = call(&router, "GET", &format!("/sources/{ID}/rules"), None).await;
        assert_eq!(status, StatusCode::OK);
        let epochs: Vec<i64> = body["lineage"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["epoch"].as_i64().unwrap())
            .collect();
        assert_eq!(epochs, vec![3, 2, 1], "epoch strictly increases");
        assert_eq!(body["active"]["rules_hash"], json!(first_hash));
        let (_, body) = call(&router, "GET", &format!("/sources/{ID}"), None).await;
        assert_eq!(body["source"]["active_rules_hash"], json!(first_hash));
        assert_eq!(body["source"]["repairable"], false);
        assert_eq!(body["source"]["repairable_reason"], "no pinned bodies");
        let (_, body) = call(&router, "GET", "/sources", None).await;
        let row = &body["sources"][0];
        assert_eq!(row["active_rules_hash"], json!(first_hash));
        assert!(
            row.get("repairable_reason").is_some(),
            "the key is present even when null"
        );
    }
}
