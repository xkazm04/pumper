//! API X-ray recipes (M05): `GET /recipes` — the JSON-API endpoints the
//! browser tier discovered behind rendered pages — plus the two operator
//! actions on one.
//!
//! Recipes are written by the discovery pass over `capture_network` renders
//! (`AppContext::xray`, `pumper_core::recipes`) and stay `state: "candidate"`
//! until a successful replay proves them. The fetcher's pre-HTTP "api_recipe"
//! tier that consumes them IS wired (`Fetcher::try_recipe`, opt-in via
//! `[recipes] enabled` or `FetchRequest.use_recipes`). What is NOT wired is
//! discovery: no app calls `xray` yet, so this table stays empty until a
//! discovery caller ships.
//!
//! ## Read by default, writes explicit and gated
//!
//! The list is the read surface and it now shows the *strikes* — `state`,
//! `consecutive_failures`, `last_failure_at` — because an operator who can see
//! that a recipe is failing is the only one who can act on it. The two actions
//! are separate verbs on separate paths:
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

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use pumper_core::recipes::RecipeState;

use crate::routes::error::ApiError;
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
            discovered_at, last_seen_at}]}`"),
        (status = 400, description = "Unknown `state` value", body = Object),
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

/// Retires a recipe: out of selection now, reversible by `revive`.
#[utoipa::path(
    post,
    path = "/recipes/{id}/retire",
    tag = "recipes",
    params(("id" = String, Path, description = "Recipe id")),
    responses(
        (status = 200, description = "The refreshed row (`state: \"retired\"`)"),
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
            `consecutive_failures: 0`)"),
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
