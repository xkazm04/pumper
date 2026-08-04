//! Derived datasets (M11): CRUD for filter/project(/lookup) specs and v2
//! group_by/aggregate specs that recompute incrementally on each upstream
//! upsert, plus the bounded backfill that materializes a new spec over the
//! existing source rows. The recompute itself rides `Datasets::upsert_many`
//! (and `detect_removed` for aggregate groups) in core — these routes only
//! manage the specs and trigger backfills.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::{DerivedLookup, DerivedSpec, NewDerivedSpec};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::routes::error::{ApiError, EnabledBody};
use crate::state::AppState;

#[derive(Deserialize, IntoParams)]
pub(crate) struct DerivedQuery {
    /// Filter by source app.
    app: Option<String>,
}

#[utoipa::path(
    get,
    path = "/derived",
    tag = "derived",
    params(DerivedQuery),
    responses((status = 200, description = "`{specs: [DerivedSpec]}`"))
)]
pub(crate) async fn list_derived(
    State(state): State<AppState>,
    Query(query): Query<DerivedQuery>,
) -> Result<Json<Value>, ApiError> {
    let specs = state
        .storage
        .list_derived_specs(query.app.as_deref())
        .await?;
    Ok(Json(json!({ "specs": specs })))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateDerivedBody {
    /// App namespace the source AND the derived dataset live in.
    source_app: String,
    source_dataset: String,
    /// Dataset the shaped rows are upserted into (same app).
    target_dataset: String,
    /// `$.path:op:value` predicate specs (the `?filter=` grammar), ANDed.
    filters: Option<Vec<String>>,
    /// `{out_field: "$.path"}` projection; empty/absent = whole-record passthrough.
    project: Option<std::collections::BTreeMap<String, String>>,
    /// Optional single-key join: `{dataset, key_expr, merge_as}`.
    lookup: Option<LookupBody>,
    /// Aggregate spec (v2): `$.path` group keys. Requires `aggregates`;
    /// mutually exclusive with `lookup` and `project`.
    group_by: Option<Vec<String>>,
    /// Aggregate outputs: `{out_field: "count" | "sum($.path)"}`. Requires
    /// `group_by`.
    aggregates: Option<std::collections::BTreeMap<String, String>>,
}

/// Body twin of core's [`DerivedLookup`] (which doesn't carry a utoipa schema).
#[derive(Deserialize, ToSchema)]
pub(crate) struct LookupBody {
    dataset: String,
    /// `$.path` into the SOURCE record that yields the lookup key.
    key_expr: String,
    /// Field of the derived row the looked-up record is merged under.
    merge_as: String,
}

impl From<&LookupBody> for DerivedLookup {
    fn from(b: &LookupBody) -> Self {
        DerivedLookup {
            dataset: b.dataset.clone(),
            key_expr: b.key_expr.clone(),
            merge_as: b.merge_as.clone(),
        }
    }
}

#[utoipa::path(
    post,
    path = "/derived",
    tag = "derived",
    request_body = CreateDerivedBody,
    responses(
        (status = 201, description = "Created spec", body = Object),
        (status = 400, description = "Invalid filters/project/lookup, or the spec would create a cycle", body = Object),
    )
)]
pub(crate) async fn create_derived(
    State(state): State<AppState>,
    Json(body): Json<CreateDerivedBody>,
) -> Result<(StatusCode, Json<DerivedSpec>), ApiError> {
    let bad = |msg: String| ApiError(StatusCode::BAD_REQUEST, msg);
    for (name, v) in [
        ("source_app", &body.source_app),
        ("source_dataset", &body.source_dataset),
        ("target_dataset", &body.target_dataset),
    ] {
        if v.trim().is_empty() {
            return Err(bad(format!("{name} must not be empty")));
        }
    }
    let filters = body.filters.unwrap_or_default();
    // Validate the filter grammar now — a spec that stores unparseable filters
    // would silently skip rows at recompute time.
    pumper_core::parse_filter_specs(&filters)?;
    let project = body.project.unwrap_or_default();
    for (field, path) in &project {
        if !path.starts_with("$.") {
            return Err(bad(format!(
                "project field '{field}' must map to a JSON path starting with '$.' (got '{path}')"
            )));
        }
    }
    if let Some(lookup) = &body.lookup {
        if lookup.dataset.trim().is_empty() || lookup.merge_as.trim().is_empty() {
            return Err(bad(
                "lookup.dataset and lookup.merge_as must not be empty".into()
            ));
        }
        if !lookup.key_expr.starts_with("$.") {
            return Err(bad(format!(
                "lookup.key_expr must be a JSON path starting with '$.' (got '{}')",
                lookup.key_expr
            )));
        }
    }
    // Aggregate half (v2): `group_by` and `aggregates` come together or not at
    // all. Content validation (paths, expressions, name collisions) and the
    // lookup/project exclusivity live in core's create path.
    let group: Option<pumper_core::DerivedGroup> = match (&body.group_by, &body.aggregates) {
        (Some(group_by), Some(aggregates)) => Some(pumper_core::DerivedGroup {
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
        }),
        (None, None) => None,
        _ => {
            return Err(bad(
                "group_by and aggregates must be provided together".into()
            ))
        }
    };
    // Cycle rejection happens inside create_derived_spec (the one write path a
    // spec can enter through); a rejected spec surfaces as 400 via BadRequest.
    let lookup: Option<DerivedLookup> = body.lookup.as_ref().map(DerivedLookup::from);
    let spec = state
        .storage
        .create_derived_spec(&NewDerivedSpec {
            source_app: &body.source_app,
            source_dataset: &body.source_dataset,
            target_dataset: &body.target_dataset,
            filters: &filters,
            project: &project,
            lookup: lookup.as_ref(),
            group: group.as_ref(),
        })
        .await?;
    Ok((StatusCode::CREATED, Json(spec)))
}

#[utoipa::path(
    get,
    path = "/derived/{id}",
    tag = "derived",
    params(("id" = String, Path, description = "Spec id")),
    responses(
        (status = 200, description = "The spec", body = Object),
        (status = 404, description = "Unknown spec", body = Object),
    )
)]
pub(crate) async fn get_derived(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<DerivedSpec>, ApiError> {
    match state.storage.get_derived_spec(&id).await? {
        Some(spec) => Ok(Json(spec)),
        None => Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("unknown derived spec '{id}'"),
        )),
    }
}

#[utoipa::path(
    delete,
    path = "/derived/{id}",
    tag = "derived",
    params(("id" = String, Path, description = "Spec id")),
    responses(
        (status = 200, description = "Deleted"),
        (status = 404, description = "Unknown spec", body = Object),
    )
)]
pub(crate) async fn delete_derived(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_derived_spec(&id).await? {
        Ok(Json(json!({ "deleted": id })))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("unknown derived spec '{id}'"),
        ))
    }
}

#[utoipa::path(
    post,
    path = "/derived/{id}/enabled",
    tag = "derived",
    params(("id" = String, Path, description = "Spec id")),
    request_body = EnabledBody,
    responses(
        (status = 200, description = "Kill-switch flipped"),
        (status = 404, description = "Unknown spec", body = Object),
    )
)]
pub(crate) async fn set_derived_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.set_derived_enabled(&id, body.enabled).await? {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("unknown derived spec '{id}'"),
        ))
    }
}

#[derive(Deserialize, ToSchema, Default)]
pub(crate) struct BackfillBody {
    /// Rows per keyset batch (clamped to 1..=1000; default 500).
    batch: Option<i64>,
    /// Rows this request may scan before it stops and returns a `cursor`
    /// (default 50000). Aggregate specs treat it as a hard ceiling instead:
    /// they need one full pass, so exceeding it is a 400 that writes nothing.
    max_rows: Option<i64>,
    /// Resume point from a previous response's `cursor`. Absent starts over —
    /// which is always safe, the work is idempotent per row.
    cursor: Option<String>,
}

#[utoipa::path(
    post,
    path = "/derived/{id}/backfill",
    tag = "derived",
    request_body(content = BackfillBody, description = "Optional `{batch, max_rows, cursor}`"),
    params(("id" = String, Path, description = "Spec id")),
    responses(
        (status = 200, description = "Backfill slice: `{scanned, matched, new, changed, unchanged, done, cursor?}`. `done: false` means the row budget stopped this request — call again with `cursor` to continue."),
        (status = 400, description = "An aggregate spec's source exceeds `max_rows` (a partial aggregate pass is refused, not written)", body = Object),
        (status = 404, description = "Unknown spec", body = Object),
        (status = 409, description = "Spec is disabled", body = Object),
    )
)]
pub(crate) async fn backfill_derived(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<BackfillBody>>,
) -> Result<Json<pumper_core::DerivedBackfill>, ApiError> {
    let Some(spec) = state.storage.get_derived_spec(&id).await? else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("unknown derived spec '{id}'"),
        ));
    };
    // The kill-switch governs the backfill too: a disabled spec must be fully
    // inert, not "off live but still materializable".
    if !spec.enabled {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!("derived spec '{id}' is disabled"),
        ));
    }
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let opts = pumper_core::BackfillOpts {
        batch: body.batch.unwrap_or(500),
        max_rows: body
            .max_rows
            .unwrap_or(pumper_core::DEFAULT_BACKFILL_MAX_ROWS),
        cursor: body.cursor,
    };
    let report = state
        .datasets
        .backfill_derived_budgeted(&spec, &opts)
        .await?;
    Ok(Json(report))
}
