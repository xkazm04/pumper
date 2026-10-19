//! API X-ray recipes (M05): `GET /recipes` — the JSON-API endpoints the
//! browser tier discovered behind rendered pages — plus the two operator
//! actions on one.
//!
//! Recipes are written by the discovery pass over `capture_network` renders
//! (`AppContext::xray`, `pumper_core::recipes`) and stay `state: "candidate"`
//! until a successful replay proves them. The fetcher's pre-HTTP "api_recipe"
//! tier that consumes them IS wired (`Fetcher::try_recipe`, opt-in via
//! `[recipes] enabled`, `[fetcher] xray` or `FetchRequest.use_recipes`).
//!
//! The discovery caller ships with N14: the `extractor` runs the heuristic over
//! the JSON calls an *escalated* render observed, scored against the records it
//! extracted from that same page (`[fetcher] xray`, default OFF — with it off
//! nothing captures and this table stays empty).
//!
//! ## Read by default, writes explicit and gated
//!
//! The list is the read surface and it shows the *strikes* — `state`,
//! `consecutive_failures`, `last_failure_at` — and the last replay verdict —
//! `validation_reason`, `validated_at` — because an operator who can see that a
//! recipe is failing, and why, is the only one who can act on it. A candidate
//! nothing has tried yet (`validation_reason: null`) reads differently from one
//! that was tried and refused. The two actions are separate verbs on separate
//! paths:
//!
//! - `POST /recipes/{id}/retire` — take it out of selection now, without
//!   waiting for `[recipes] retire_after` live strikes to accumulate.
//! - `POST /recipes/{id}/revive` — put it back, as a **candidate**. Revival is
//!   probation, not trust.
//!
//! Both are reversible, and that is deliberate: **there is no `DELETE`**.
//! Retirement is the terminal state and it destroys nothing — a retired recipe
//! costs a fetch nothing (it is invisible to `best_for_host`) while its strike
//! count survives as the evidence for the revive decision.

// One implementation of the bundle shape, in core, shared with the puller that
// consumes it (`app-peer`). Two copies of "what a recipe looks like on the wire"
// is how an export and an import drift apart without either side changing.
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::mesh::{exportable_recipe, importable_recipe, open_envelope, SCHEMA_RECIPES_V1};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use pumper_core::recipes::RecipeState;

use crate::routes::error::ApiError;
use crate::routes::mesh::trust_for;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub(crate) struct RecipesQuery {
    /// Filter to one host (lowercased exact match).
    host: Option<String>,
    /// Filter to one lifecycle state (`candidate` | `validated` | `retired`).
    state: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_limit() -> i64 {
    100
}

/// Parses the `?state=` filter. An unknown word is a **400**, never an empty
/// page: answering `?state=bogus` with `{"recipes": []}` tells an operator
/// their hosts are clean when it means the server did not understand the word.
fn parse_state_filter(raw: Option<&str>) -> Result<Option<RecipeState>, ApiError> {
    match raw {
        None => Ok(None),
        Some(raw) => RecipeState::parse(raw).map(Some).ok_or_else(|| {
            ApiError(
                StatusCode::BAD_REQUEST,
                format!("unknown state '{raw}' — expected candidate, validated or retired"),
            )
        }),
    }
}

/// Discovered API recipes, best overlap score first.
#[utoipa::path(
    get,
    path = "/recipes",
    tag = "recipes",
    params(
        ("host" = Option<String>, Query, description = "Filter to one host"),
        ("state" = Option<String>, Query,
            description = "Filter to one lifecycle state: candidate | validated | retired"),
        ("limit" = Option<i64>, Query, description = "Max rows (default 100, cap 500)"),
    ),
    responses(
        (status = 200, description = "`{recipes: [{id, host, url_template, params, json_paths, \
            score, validated, state, consecutive_failures, last_failure_at, retired_at, \
            validation_reason, validated_at, discovered_at, last_seen_at}]}`", body = crate::routes::dto::RecipeListResponse),
        (status = 400, description = "Unknown `state` value", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn list_recipes(
    State(state): State<AppState>,
    Query(query): Query<RecipesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let host = query.host.as_deref().map(str::to_lowercase);
    let filter = parse_state_filter(query.state.as_deref())?;
    let recipes = state
        .storage
        .recipes()
        .list(host.as_deref(), filter, limit)
        .await?;
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
        json_paths, score, validated_at_origin}]}}`.", body = crate::routes::dto::SignedBundle))
)]
pub(crate) async fn export_recipes(
    State(state): State<AppState>,
    Query(query): Query<RecipeExportQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query
        .limit
        .unwrap_or(500)
        .clamp(1, MAX_RECIPE_BUNDLE as i64);
    let rows = state.storage.recipes().list(None, None, limit).await?;
    let entries: Vec<Value> = rows
        .iter()
        // A retired recipe is out of selection here (0046 lifecycle); handing it
        // to a peer would put back into play what this node took out of it.
        .filter(|r| r.get("state").and_then(Value::as_str) != Some("retired"))
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
            `validated: false` regardless of what the origin claimed.", body = crate::routes::dto::RecipeImportResponse),
        (status = 400, description = "Unknown schema, refused signature, or an oversized bundle",
            body = crate::routes::dto::ErrorEnvelope),
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

/// Retires a recipe: out of selection now, reversible by `revive`.
#[utoipa::path(
    post,
    path = "/recipes/{id}/retire",
    tag = "recipes",
    params(("id" = String, Path, description = "Recipe id")),
    responses(
        (status = 200, description = "The refreshed row (`state: \"retired\"`)", body = crate::routes::dto::RecipeDto),
        (status = 404, description = "Recipe not found", body = Object),
    )
)]
pub(crate) async fn retire_recipe(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let recipes = state.storage.recipes();
    if !recipes.retire(&id).await? {
        return Err(not_found());
    }
    // Re-read rather than echo: the response is the row as stored, so a client
    // never has to guess whether the stamp it sent is the stamp that landed.
    recipes.get(&id).await?.map(Json).ok_or_else(not_found)
}

/// Revives a retired recipe — back to **candidate**, never straight to
/// validated. Strikes and the failure stamp are cleared with it.
#[utoipa::path(
    post,
    path = "/recipes/{id}/revive",
    tag = "recipes",
    params(("id" = String, Path, description = "Recipe id")),
    responses(
        (status = 200, description = "The refreshed row (`state: \"candidate\"`, \
            `consecutive_failures: 0`)", body = crate::routes::dto::RecipeDto),
        (status = 404, description = "Recipe not found", body = Object),
    )
)]
pub(crate) async fn revive_recipe(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    state
        .storage
        .recipes()
        .revive(&id)
        .await?
        .map(Json)
        .ok_or_else(not_found)
}

fn not_found() -> ApiError {
    ApiError(StatusCode::NOT_FOUND, "recipe not found".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A filter the server does not understand must not read as "nothing
    /// matched" — that is the difference between an empty result and a typo.
    #[test]
    fn an_unknown_state_filter_is_a_400_not_an_empty_page() {
        assert_eq!(parse_state_filter(None).unwrap(), None);
        assert_eq!(
            parse_state_filter(Some("retired")).unwrap(),
            Some(RecipeState::Retired)
        );
        for bogus in ["bogus", "", "deleted", "all"] {
            let err = parse_state_filter(Some(bogus)).expect_err("must reject");
            assert_eq!(err.0, StatusCode::BAD_REQUEST, "{bogus}");
            assert!(
                err.1.contains("candidate, validated or retired"),
                "{}",
                err.1
            );
        }
    }
}
