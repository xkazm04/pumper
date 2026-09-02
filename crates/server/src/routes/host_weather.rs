//! Host weather (M01 v1): export/import of the learned per-host intelligence
//! (tier pins, HTTP strikes, politeness penalties, observation counts) as a
//! versioned JSON bundle, so N pumper deployments can share what they learned
//! about the open web instead of each paying the cold-start tax alone.
//!
//! `GET /host-weather/export` hands you a bundle, `POST /host-weather/import`
//! merges one — dry-run by DEFAULT (`?apply=true` to write).
//!
//! ## N16: the bundle is signed
//!
//! Export now emits `pumper.host-weather/2`: a **signed mesh envelope**
//! (`{schema, node_id, generated_at, legacy_id, payload, sig}`) whose payload is
//! the old flat body's `{min_observations, entries}`. `node_id` is this node's
//! ed25519 key fingerprint (`crate::node`), not the database-path hash it used
//! to be — that value survives one release as `legacy_id`.
//!
//! Import accepts both, under an explicit trust policy
//! (`crate::routes::mesh::trust_for`): a `/2` bundle is *verified* only against
//! a `[[peer]]` row that pinned the signing key; a `/1` bundle, or a `/2` one
//! whose key nobody pinned, is accepted only when a peer row says
//! `allow_unsigned = true` — or when this node has no `[[peer]]` rows at all, in
//! which case nothing has changed from before the mesh existed. `GET
//! /host-weather/export?schema=1` still emits the old flat unsigned bundle, so
//! a fleet can be upgraded one node at a time.
//!
//! There is still NO push: a peer PULLS this export on its own schedule
//! (`docs/features/mesh.md`).
//!
//! The merge itself is conservative by construction — see
//! `pumper_core::plan_weather_import` for the precedence rules (never
//! downgrade a better-observed local pin, strike/penalty raises only,
//! severity caps). Remote intel is a prior, not truth: hosts behave
//! differently per egress IP/geo, so one local observation must always be
//! able to override anything imported (which is why imports never touch the
//! local `observations` count).

use std::collections::BTreeSet;

use app_peer::envelope::{open_envelope, SCHEMA_WEATHER_V1, SCHEMA_WEATHER_V2};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::{plan_weather_import, WeatherEntry, WeatherPlan};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use crate::routes::error::ApiError;
use crate::routes::mesh::trust_for;
use crate::state::AppState;

/// Default `?min_observations=` floor: matches the tier router's strike limit,
/// so a host travels only once it carries at least a pin's worth of evidence.
const DEFAULT_MIN_OBSERVATIONS: i64 = 3;

/// Ceiling on entries accepted per import call — a bundle is host-level
/// intel, not a bulk data channel.
const MAX_IMPORT_ENTRIES: usize = 10_000;

#[derive(Deserialize, IntoParams)]
pub(crate) struct ExportQuery {
    /// Minimum locally-recorded observations for a host to be exported.
    /// The floor keeps thin/noisy hosts (one lucky loss, penalty-only
    /// snapshot rows) from travelling between deployments. Default 3.
    min_observations: Option<i64>,
    /// Emit the legacy flat, UNSIGNED `pumper.host-weather/1` bundle instead of
    /// the signed envelope. For rolling a fleet forward one node at a time; a
    /// peer on a current build should never ask for it.
    schema: Option<u32>,
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
    responses((status = 200, description = "Signed envelope `{schema: \
        \"pumper.host-weather/2\", node_id, legacy_id, generated_at, sig, payload: \
        {min_observations, entries: [{host, preferred_tier, http_strikes, penalty_ms (live), \
        observations, challenge_fingerprints, updated_at}]}}`. With `?schema=1`, the legacy \
        flat unsigned `pumper.host-weather/1` body instead."))
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
    let payload = json!({
        "min_observations": min_observations,
        "entries": entries,
    });
    if query.schema == Some(1) {
        // Legacy flat body, byte-compatible with the pre-N16 export.
        let id = crate::node::identity(&state).map_err(identity_unavailable)?;
        return Ok(Json(json!({
            "schema": SCHEMA_WEATHER_V1,
            "generated_at": chrono::Utc::now().to_rfc3339(),
            "node_id": id.legacy_id(),
            "min_observations": min_observations,
            "entries": payload["entries"],
        })));
    }
    let id = crate::node::identity(&state).map_err(identity_unavailable)?;
    Ok(Json(id.seal(SCHEMA_WEATHER_V2, payload)))
}

/// A node that cannot read its own key cannot sign, and signing an export with
/// a key minted on the spot would hand peers a bundle their pins reject. A 500
/// naming the key file is the honest answer.
fn identity_unavailable(e: anyhow::Error) -> ApiError {
    ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("node identity unavailable: {e}"),
    )
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct ImportQuery {
    /// Write the merge. DEFAULT FALSE: without `?apply=true` the call is a
    /// pure dry-run — the full per-host plan is computed and returned, and
    /// nothing (tier memory, governor, penalty snapshots) is touched.
    #[serde(default)]
    apply: bool,
}

/// A verified bundle's usable contents. Deliberately built AFTER the signature
/// check, from the opened payload.
struct ImportedBundle {
    node_id: Option<String>,
    entries: Vec<WeatherEntry>,
}

/// Reads the `entries` array out of an opened bundle payload.
///
/// Extracted and typed rather than deserialised through a body struct because
/// the payload arrives as an opaque `Value`: the SIGNATURE is over the bytes,
/// so the envelope must be verified before anything reinterprets its contents.
/// A body struct would have made serde the first reader and the verifier the
/// second, which is the wrong order.
pub(crate) fn weather_entries(payload: &Value) -> Result<Vec<WeatherEntry>, String> {
    let raw = payload
        .get("entries")
        .ok_or_else(|| "bundle payload has no `entries` array".to_string())?;
    serde_json::from_value(raw.clone()).map_err(|e| format!("bundle `entries` is unreadable: {e}"))
}

/// Imports a host-weather bundle with a conservative, count-weighted merge.
///
/// Dry-run by default (`?apply=false`): the response's `actions` show exactly
/// what an applied import would change, per host. Precedence (see
/// `pumper_core::plan_weather_import`): a locally-observed pin is NEVER
/// downgraded; a remote pin is adopted only when strictly better-observed;
/// strikes only rise and are capped below the pin threshold; penalties only
/// rise and are capped at the import severity ceiling (60s).
///
/// N16: the bundle is verified BEFORE any of that. A forged or unverifiable
/// bundle is a `400` naming why — never a merge with a warning.
#[utoipa::path(
    post,
    path = "/host-weather/import",
    tag = "hosts",
    params(ImportQuery),
    request_body = Object,
    responses(
        (status = 200, description = "`{applied, source_node_id, verified, schema, considered, \
            changed, noops, actions: [{host, adopt_pin, raise_strikes, raise_penalty_ms, \
            notes}]}` — `actions` lists only the hosts an applied import would change \
            (`changed`); dominated entries are counted in `noops`. `verified` is false for an \
            accepted-but-unsigned bundle."),
        (status = 400, description = "Unknown schema, refused signature, empty/oversized bundle, \
            or a blank host", body = Object),
    )
)]
pub(crate) async fn import_host_weather(
    State(state): State<AppState>,
    Query(query): Query<ImportQuery>,
    Json(raw): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let claimed_node = raw.get("node_id").and_then(Value::as_str);
    let trust = trust_for(&state.config.peer, claimed_node);
    let opened = open_envelope(&raw, SCHEMA_WEATHER_V2, Some(SCHEMA_WEATHER_V1), &trust)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    let schema = raw
        .get("schema")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let body_entries =
        weather_entries(&opened.payload).map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    let body = ImportedBundle {
        node_id: opened.node_id.clone(),
        entries: body_entries,
    };
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
        // Honest verdict: `true` only when a pinned key actually verified the
        // signature. An accepted legacy bundle says `false` rather than
        // borrowing the word from a check that did not happen.
        "verified": opened.verified,
        "schema": schema,
        "considered": considered,
        "changed": actions.len(),
        "noops": noops,
        "actions": actions,
    })))
}
