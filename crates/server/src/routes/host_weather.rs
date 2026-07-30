//! Host weather (M01 v1): export/import of the learned per-host intelligence
//! (tier pins, HTTP strikes, politeness penalties, observation counts) as a
//! versioned JSON bundle, so N pumper deployments can share what they learned
//! about the open web instead of each paying the cold-start tax alone.
//!
//! v1 is deliberately manual: `GET /host-weather/export` hands you a bundle,
//! `POST /host-weather/import` merges one — dry-run by DEFAULT (`?apply=true`
//! to write). There is NO federation service and NO auto-sync; peer URLs,
//! scheduled pulls, signatures, and faster decay of imported state are the
//! documented next slice. The bundle carries provenance fields (`node_id`,
//! `generated_at`, schema version) so those layers can build on this shape
//! without a version bump.
//!
//! The merge itself is conservative by construction — see
//! `pumper_core::plan_weather_import` for the precedence rules (never
//! downgrade a better-observed local pin, strike/penalty raises only,
//! severity caps). Remote intel is a prior, not truth: hosts behave
//! differently per egress IP/geo, so one local observation must always be
//! able to override anything imported (which is why imports never touch the
//! local `observations` count).

use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash, Hasher};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::{plan_weather_import, WeatherEntry, WeatherPlan};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::routes::error::ApiError;
use crate::state::AppState;

/// The bundle schema identifier. Import rejects anything else — a future v2
/// changes this string rather than silently reinterpreting fields.
const WEATHER_SCHEMA: &str = "pumper.host-weather/1";

/// Default `?min_observations=` floor: matches the tier router's strike limit,
/// so a host travels only once it carries at least a pin's worth of evidence.
const DEFAULT_MIN_OBSERVATIONS: i64 = 3;

/// Ceiling on entries accepted per import call — a bundle is host-level
/// intel, not a bulk data channel.
const MAX_IMPORT_ENTRIES: usize = 10_000;

/// A stable-ish identity for this node, for bundle provenance: a hash of the
/// database path. Not a security boundary (nothing is signed in v1) — it lets
/// an operator tell bundles from different deployments apart and gives the
/// future signed/revocable layer a field to inherit.
fn node_id(state: &AppState) -> String {
    let mut h = DefaultHasher::new();
    state.config.storage.database_path.hash(&mut h);
    format!("{:016x}", h.finish())
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct ExportQuery {
    /// Minimum locally-recorded observations for a host to be exported.
    /// The floor keeps thin/noisy hosts (one lucky loss, penalty-only
    /// snapshot rows) from travelling between deployments. Default 3.
    min_observations: Option<i64>,
}

/// Exports the learned host intelligence as a versioned host-weather bundle.
///
/// Each entry carries the tier pin, strike count, the LIVE politeness penalty
/// (governor value merged over the persisted snapshot), and the local
/// observation count that import-side count-weighted merging keys on.
/// `challenge_fingerprints` is part of the schema but empty in v1 (pumper
/// does not persist per-host challenge fingerprints yet).
#[utoipa::path(
    get,
    path = "/host-weather/export",
    tag = "hosts",
    params(ExportQuery),
    responses((status = 200, description = "`{schema, generated_at, node_id, min_observations, \
        entries: [{host, preferred_tier, http_strikes, penalty_ms (live), observations, \
        challenge_fingerprints, updated_at}]}`"))
)]
pub(crate) async fn export_host_weather(
    State(state): State<AppState>,
    Query(query): Query<ExportQuery>,
) -> Result<Json<Value>, ApiError> {
    let min_observations = query.min_observations.unwrap_or(DEFAULT_MIN_OBSERVATIONS);
    if min_observations < 0 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "min_observations must be >= 0".into(),
        ));
    }
    let profiles = state.tiers.export_weather(min_observations).await?;
    let mut entries = Vec::with_capacity(profiles.len());
    for p in profiles {
        // Live governor penalty is authoritative and fresher than the row's
        // write-behind snapshot; export the stricter of the two so a bundle
        // never understates locally-earned spacing.
        let live = state.governor.penalty(&p.host).await.as_millis();
        let live = live.min(i64::MAX as u128) as i64;
        entries.push(WeatherEntry {
            host: p.host,
            preferred_tier: p.preferred_tier,
            http_strikes: p.http_strikes,
            penalty_ms: p.penalty_ms.max(live),
            observations: p.observations,
            challenge_fingerprints: Vec::new(),
            updated_at: Some(p.updated_at),
        });
    }
    Ok(Json(json!({
        "schema": WEATHER_SCHEMA,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "node_id": node_id(&state),
        "min_observations": min_observations,
        "entries": entries,
    })))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct ImportQuery {
    /// Write the merge. DEFAULT FALSE: without `?apply=true` the call is a
    /// pure dry-run — the full per-host plan is computed and returned, and
    /// nothing (tier memory, governor, penalty snapshots) is touched.
    #[serde(default)]
    apply: bool,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct ImportBody {
    /// Must be `pumper.host-weather/1`.
    schema: String,
    /// Exporting node's id — echoed back as `source_node_id` for provenance.
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)] // provenance passthrough; not merged on in v1
    generated_at: Option<String>,
    #[schema(value_type = Vec<Object>)]
    entries: Vec<WeatherEntry>,
}

/// Imports a host-weather bundle with a conservative, count-weighted merge.
///
/// Dry-run by default (`?apply=false`): the response's `actions` show exactly
/// what an applied import would change, per host. Precedence (see
/// `pumper_core::plan_weather_import`): a locally-observed pin is NEVER
/// downgraded; a remote pin is adopted only when strictly better-observed;
/// strikes only rise and are capped below the pin threshold; penalties only
/// rise and are capped at the import severity ceiling (60s).
#[utoipa::path(
    post,
    path = "/host-weather/import",
    tag = "hosts",
    params(ImportQuery),
    request_body = ImportBody,
    responses(
        (status = 200, description = "`{applied, source_node_id, considered, changed, noops, \
            actions: [{host, adopt_pin, raise_strikes, raise_penalty_ms, notes}]}` — `actions` \
            lists only the hosts an applied import would change (`changed`); dominated entries \
            are counted in `noops`."),
        (status = 400, description = "Unknown schema, empty/oversized bundle, or a blank host", body = Object),
    )
)]
pub(crate) async fn import_host_weather(
    State(state): State<AppState>,
    Query(query): Query<ImportQuery>,
    Json(body): Json<ImportBody>,
) -> Result<Json<Value>, ApiError> {
    if body.schema != WEATHER_SCHEMA {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "unknown bundle schema {:?}; this build imports {WEATHER_SCHEMA:?}",
                body.schema
            ),
        ));
    }
    if body.entries.len() > MAX_IMPORT_ENTRIES {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "bundle has {} entries; the import ceiling is {MAX_IMPORT_ENTRIES}",
                body.entries.len()
            ),
        ));
    }

    let mut actions: Vec<WeatherPlan> = Vec::new();
    let mut noops = 0usize;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let considered = body.entries.len();
    for entry in &body.entries {
        let host = entry.host.trim().to_lowercase();
        if host.is_empty() {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "bundle entry with an empty host".into(),
            ));
        }
        // Duplicate hosts in one bundle would make the merge order-dependent;
        // first entry wins, the rest are dominated by definition.
        if !seen.insert(host.clone()) {
            noops += 1;
            continue;
        }
        let local = state.tiers.get(&host).await?;
        let live_ms = state.governor.penalty(&host).await.as_millis();
        let live_ms = live_ms.min(u64::MAX as u128) as u64;
        let plan = plan_weather_import(local.as_ref(), live_ms, entry);
        if plan.is_noop() {
            noops += 1;
            continue;
        }
        actions.push(plan);
    }

    if query.apply {
        let mut penalties: Vec<(String, u64)> = Vec::new();
        for plan in &actions {
            state.tiers.apply_weather(plan).await?;
            if let Some(ms) = plan.raise_penalty_ms {
                // Raise the live governor (never lowers) and persist the
                // write-behind snapshot so the import survives a restart.
                state
                    .governor
                    .raise_penalty(&plan.host, std::time::Duration::from_millis(ms));
                penalties.push((plan.host.clone(), ms));
            }
        }
        state.tiers.save_penalties(&penalties).await?;
    }

    Ok(Json(json!({
        "applied": query.apply,
        "source_node_id": body.node_id,
        "considered": considered,
        "changed": actions.len(),
        "noops": noops,
        "actions": actions,
    })))
}
