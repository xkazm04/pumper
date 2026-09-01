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
