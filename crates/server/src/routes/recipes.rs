//! API X-ray recipes (M05): `GET /recipes` — the JSON-API endpoints the
//! browser tier discovered behind rendered pages.
//!
//! Recipes are written by the discovery pass over `capture_network` renders
//! (`AppContext::xray`, `pumper_core::recipes`) and stay `validated: false`
//! until a successful replay proves them. This route is the read surface; the
//! fetcher's pre-HTTP "api_recipe" tier that consumes them IS wired
//! (`Fetcher::try_recipe`, opt-in via `[recipes] enabled` or
//! `FetchRequest.use_recipes`). What is NOT wired is discovery: no app calls
//! `xray` yet, so this table stays empty until a discovery caller ships.

use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::routes::error::ApiError;
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
            score, validated, discovered_at, last_seen_at}]}`"),
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
