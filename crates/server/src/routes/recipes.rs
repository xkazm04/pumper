//! API X-ray recipes (M05): `GET /recipes` — the JSON-API endpoints the
//! browser tier discovered behind rendered pages.
//!
//! Recipes are written by the discovery pass over `capture_network` renders
//! (`AppContext::xray`, `pumper_core::recipes`) and stay `validated: false`
//! until a replay proves them. This route is the read surface; the fetcher's
//! pre-HTTP "api_recipe" tier consumes them (`Fetcher::try_recipe`, opt-in via
//! `[recipes] enabled`, `[fetcher] xray` or `FetchRequest.use_recipes`).
//!
//! The discovery caller ships with N14: the `extractor` runs the heuristic over
//! the JSON calls an *escalated* render observed, scored against the records it
//! extracted from that same page (`[fetcher] xray`, default OFF — with it off
//! nothing captures and this table stays empty, as it always has).
//!
//! Each row carries the full validation state, so a reader can tell a candidate
//! nothing has tried yet (`validated: false`, `validation_reason: null`) from
//! one that was tried and refused (`validation_reason` naming why, plus
//! `consecutive_failures`; at `[recipes] max_failures` the candidate is burned
//! and never replayed again).

use app_peer::envelope::{open_envelope, SCHEMA_RECIPES_V1};
// One implementation of the bundle shape, shared with the puller that consumes
// it (`app_peer::mesh`). Two copies of "what a recipe looks like on the wire"
// is how an export and an import drift apart without either side changing.
use app_peer::mesh::{exportable_recipe, importable_recipe};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use crate::routes::error::ApiError;
use crate::routes::mesh::trust_for;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub(crate) struct RecipesQuery {
    /// Filter to one host (lowercased exact match).
    host: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_limit() -> i64 {
    100
}

/// Discovered API recipes, best overlap score first.
#[utoipa::path(
    get,
    path = "/recipes",
    tag = "recipes",
    params(
        ("host" = Option<String>, Query, description = "Filter to one host"),
        ("limit" = Option<i64>, Query, description = "Max rows (default 100, cap 500)"),
    ),
    responses(
        (status = 200, description = "`{recipes: [{id, host, url_template, params, json_paths, \
            score, validated, validation_reason, validated_at, consecutive_failures, \
            discovered_at, last_seen_at}]}`"),
    )
)]
pub(crate) async fn list_recipes(
    State(state): State<AppState>,
    Query(query): Query<RecipesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let host = query.host.as_deref().map(str::to_lowercase);
    let recipes = state.storage.recipes().list(host.as_deref(), limit).await?;
    Ok(Json(json!({ "recipes": recipes })))
}

// ── mesh export / import (N16) ──────────────────────────────────────────────

/// Ceiling on recipes carried in one bundle. Host-level intelligence, not a
/// data channel — the same rule the weather bundle follows.
const MAX_RECIPE_BUNDLE: usize = 5_000;

#[derive(Debug, Deserialize, IntoParams)]
pub(crate) struct RecipeExportQuery {
    /// Export only recipes a local replay has PROVEN (`validated = true`).
    /// Default true: an unproven candidate is this node's guess, and shipping
    /// guesses around a fleet multiplies them instead of the knowledge.
    #[serde(default = "default_validated_only")]
    validated_only: bool,
    /// Max recipes in the bundle (default 500, cap 5000).
    limit: Option<i64>,
}

fn default_validated_only() -> bool {
    true
}

/// Exports discovered API recipes as a signed mesh bundle.
#[utoipa::path(
    get,
    path = "/recipes/export",
    tag = "recipes",
    params(RecipeExportQuery),
    responses((status = 200, description = "Signed envelope `{schema: \"pumper.recipes/1\", \
        node_id, legacy_id, generated_at, sig, payload: {entries: [{host, url_template, params, \
        json_paths, score, validated_at_origin}]}}`."))
)]
pub(crate) async fn export_recipes(
    State(state): State<AppState>,
    Query(query): Query<RecipeExportQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query
        .limit
        .unwrap_or(500)
        .clamp(1, MAX_RECIPE_BUNDLE as i64);
    let rows = state.storage.recipes().list(None, limit).await?;
    let entries: Vec<Value> = rows
        .iter()
        .filter(|r| {
            !query.validated_only || r.get("validated").and_then(Value::as_bool) == Some(true)
        })
        .filter_map(exportable_recipe)
        .collect();
    let id = crate::node::identity(&state).map_err(|e| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("node identity unavailable: {e}"),
        )
    })?;
    Ok(Json(id.seal(
        SCHEMA_RECIPES_V1,
        json!({
            "validated_only": query.validated_only,
            "entries": entries,
        }),
    )))
}

#[derive(Debug, Deserialize, IntoParams)]
pub(crate) struct RecipeImportQuery {
    /// Write the import. DEFAULT FALSE — the same dry-run-by-default shape the
    /// weather bundle has.
    #[serde(default)]
    apply: bool,
}

/// Imports a recipe bundle. Dry-run by default (`?apply=true` to write).
#[utoipa::path(
    post,
    path = "/recipes/import",
    tag = "recipes",
    params(RecipeImportQuery),
    request_body = Object,
    responses(
        (status = 200, description = "`{applied, source_node_id, verified, considered, \
            imported, skipped, notes: [..]}` — every imported recipe lands \
            `validated: false` regardless of what the origin claimed."),
        (status = 400, description = "Unknown schema, refused signature, or an oversized bundle",
            body = Object),
    )
)]
pub(crate) async fn import_recipes(
    State(state): State<AppState>,
    Query(query): Query<RecipeImportQuery>,
    Json(raw): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let claimed_node = raw.get("node_id").and_then(Value::as_str);
    let trust = trust_for(&state.config.peer, claimed_node);
    let opened = open_envelope(&raw, SCHEMA_RECIPES_V1, None, &trust)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    let entries = opened
        .payload
        .get("entries")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if entries.len() > MAX_RECIPE_BUNDLE {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "bundle has {} recipes; the import ceiling is {MAX_RECIPE_BUNDLE}",
                entries.len()
            ),
        ));
    }

    let mut imported = 0usize;
    let mut skipped = 0usize;
    let mut notes: Vec<String> = Vec::new();
    for entry in &entries {
        match importable_recipe(entry) {
            Ok(recipe) => {
                if query.apply {
                    state.storage.recipes().upsert(&recipe).await?;
                }
                imported += 1;
            }
            Err(why) => {
                skipped += 1;
                // Bounded: a hostile bundle must not turn one response into a
                // megabyte of complaints.
                if notes.len() < 20 {
                    notes.push(why);
                }
            }
        }
    }
    Ok(Json(json!({
        "applied": query.apply,
        "source_node_id": opened.node_id,
        "verified": opened.verified,
        "considered": entries.len(),
        "imported": imported,
        "skipped": skipped,
        "notes": notes,
        "validated": false,
    })))
}
