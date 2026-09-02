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
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::recipes::ApiRecipe;
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

/// Keeps only the fields a peer can act on, dropping the ones that are this
/// node's local bookkeeping.
///
/// Extracted and tested because the temptation is to ship the row verbatim, and
/// two of its columns are actively misleading elsewhere: `validated` is a claim
/// about a replay THIS node made from ITS egress IP, and `consecutive_failures`
/// counts strikes against a host from here. A peer that adopted either would be
/// inheriting a verdict it never earned — see `docs/features/mesh.md`.
pub(crate) fn exportable_recipe(row: &Value) -> Option<Value> {
    let host = row.get("host").and_then(Value::as_str)?;
    let url_template = row.get("url_template").and_then(Value::as_str)?;
    if host.trim().is_empty() || url_template.trim().is_empty() {
        return None;
    }
    Some(json!({
        "host": host,
        "url_template": url_template,
        "params": row.get("params").cloned().unwrap_or(Value::Null),
        "json_paths": row.get("json_paths").cloned().unwrap_or(Value::Null),
        "score": row.get("score").cloned().unwrap_or(Value::Null),
        // Provenance, not a verdict: "the exporter had proven this locally".
        // Import never copies it into the local `validated` column.
        "validated_at_origin": row.get("validated").cloned().unwrap_or(Value::Bool(false)),
    }))
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
    let limit = query.limit.unwrap_or(500).clamp(1, MAX_RECIPE_BUNDLE as i64);
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

/// One bundle entry, turned into a local candidate.
///
/// The import is deliberately lossy in one direction: every imported recipe
/// lands `validated = false`, whatever the origin said. Validation is a claim
/// about a replay from a particular egress IP against a live host, so adopting
/// a peer's verdict would let one node's luck (or one node's compromise) pin a
/// tier for the whole fleet. The local validator proves it here, cheaply,
/// exactly as it would prove a locally-discovered candidate.
pub(crate) fn importable_recipe(entry: &Value) -> Result<ApiRecipe, String> {
    let host = entry
        .get("host")
        .and_then(Value::as_str)
        .map(|h| h.trim().to_lowercase())
        .filter(|h| !h.is_empty())
        .ok_or_else(|| "recipe entry has no host".to_string())?;
    let url_template = entry
        .get("url_template")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|u| !u.trim().is_empty())
        .ok_or_else(|| format!("recipe entry for {host} has no url_template"))?;
    if !(url_template.starts_with("http://") || url_template.starts_with("https://")) {
        return Err(format!(
            "recipe entry for {host}: url_template {url_template:?} is not an http(s) URL"
        ));
    }
    let json_paths: Vec<String> = entry
        .get("json_paths")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Ok(ApiRecipe {
        // Empty: the store mints a LOCAL id. Carrying the origin's id would
        // make two nodes' primary keys collide the first time both discovered
        // the same endpoint independently.
        id: String::new(),
        host,
        url_template,
        params: entry.get("params").cloned().unwrap_or(Value::Null),
        json_paths,
        score: entry.get("score").and_then(Value::as_f64).unwrap_or(0.0),
        // Never trusted from the wire — see the function docs.
        validated: false,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_exported_recipe_drops_this_nodes_local_verdicts() {
        let row = json!({
            "id": "local-uuid",
            "host": "api.example",
            "url_template": "https://api.example/v1?q={q}",
            "params": {"q": "x"},
            "json_paths": ["$.items[*].title"],
            "score": 0.8,
            "validated": true,
            "validation_reason": "replay ok",
            "consecutive_failures": 3,
            "discovered_at": "2026-01-01T00:00:00Z",
        });
        let out = exportable_recipe(&row).expect("exports");
        assert!(out.get("id").is_none(), "a local primary key must not travel");
        assert!(out.get("consecutive_failures").is_none());
        assert!(out.get("validation_reason").is_none());
        assert!(out.get("validated").is_none(), "the verdict is not a field a peer may adopt");
        assert_eq!(out["validated_at_origin"], true, "it travels as provenance instead");
        assert_eq!(out["host"], "api.example");
    }

    #[test]
    fn a_row_without_a_host_or_template_is_not_exportable() {
        assert!(exportable_recipe(&json!({"url_template": "https://a/"})).is_none());
        assert!(exportable_recipe(&json!({"host": "a", "url_template": "  "})).is_none());
    }

    #[test]
    fn an_imported_recipe_is_never_validated_however_loudly_the_bundle_claims_it() {
        let entry = json!({
            "host": "API.Example",
            "url_template": "https://api.example/v1?q={q}",
            "params": {"q": "x"},
            "json_paths": ["$.items[*].title"],
            "score": 0.9,
            "validated": true,
            "validated_at_origin": true,
        });
        let r = importable_recipe(&entry).expect("imports");
        assert!(
            !r.validated,
            "a peer's replay verdict was earned from ITS egress IP, not this node's"
        );
        assert_eq!(r.host, "api.example", "hosts are normalised on the way in");
        assert!(r.id.is_empty(), "the store mints a local id, avoiding a cross-node PK collision");
        assert_eq!(r.json_paths, vec!["$.items[*].title".to_string()]);
    }

    #[test]
    fn a_non_http_template_is_refused_not_stored() {
        let err = importable_recipe(&json!({
            "host": "a.example",
            "url_template": "file:///etc/passwd",
        }))
        .expect_err("must refuse");
        assert!(err.contains("not an http(s) URL"), "{err}");
        assert!(importable_recipe(&json!({"url_template": "https://a/"})).is_err());
        assert!(importable_recipe(&json!({"host": "a"})).is_err());
    }
}
