//! Full-text search and saved searches (standing alerts): query with facets,
//! index status, saved-search CRUD + enable/disable, and index-doc/dataset
//! removal.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::routes::error::{default_limit, keyset_cursor, parse_cursor, ApiError, EnabledBody};
use crate::state::AppState;

#[derive(Deserialize, IntoParams)]
pub(crate) struct SearchQuery {
    q: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
    /// Restrict hits to one app.
    app: Option<String>,
    /// Restrict hits to one dataset.
    dataset: Option<String>,
    /// Typo tolerance (edit distance 1). Quoted phrases stay exact.
    #[serde(default)]
    fuzzy: bool,
    /// Ordering: `score` (relevance, default) or `newest` (most recently indexed).
    sort: Option<String>,
    /// Only hits indexed at/after this unix-seconds instant (a "what's new" feed).
    since: Option<i64>,
    /// Skip this many ranked hits before `limit` (page 2 = `offset=limit`). Capped.
    offset: Option<usize>,
    /// Only hits whose index-time-extracted money amount (whole US dollars) is
    /// >= this. Docs where no amount was extracted never match.
    amount_gte: Option<u64>,
    /// Only hits whose extracted amount is <= this (whole US dollars).
    amount_lte: Option<u64>,
    /// Only hits whose extracted deadline (`event_date`, unix seconds) is
    /// at/after this. Docs with no extracted deadline never match.
    date_after: Option<i64>,
    /// Only hits whose extracted deadline is at/before this (unix seconds).
    date_before: Option<i64>,
}

fn default_search_limit() -> usize {
    20
}

/// Full-text search across everything indexed from job results (BM25 ranked),
/// with highlighted snippets and app/dataset facets over the matching set.
#[utoipa::path(
    get,
    path = "/search",
    tag = "search",
    params(SearchQuery),
    responses(
        (status = 200, description = "`{query, total, count, hits, facets}` — BM25 ranked (or `sort=newest`), highlighted snippets. `total` is the full match count; `count` is the returned page size. `offset` pages (offset=limit → page 2); `sort=newest` orders by index time; `since=<unix-secs>` filters to recent docs. Entity filters (index-time regex extraction; docs without an extracted value never match): `amount_gte`/`amount_lte` in whole US dollars, `date_before`/`date_after` on the extracted deadline (unix seconds)."),
        (status = 400, description = "Empty query", body = Object),
    )
)]
pub(crate) async fn search(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<Value>, ApiError> {
    if query.q.trim().is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "query 'q' is required".into(),
        ));
    }
    let sort = match query.sort.as_deref() {
        None | Some("score") => pumper_core::SearchSort::Score,
        Some("newest") => pumper_core::SearchSort::Newest,
        Some(other) => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!("unknown sort '{other}' (expected 'score' or 'newest')"),
            ))
        }
    };
    let req = pumper_core::SearchRequest {
        q: query.q.clone(),
        limit: query.limit.clamp(1, 100),
        app: query.app,
        dataset: query.dataset,
        fuzzy: query.fuzzy,
        sort,
        since: query.since,
        // Clamp like `limit`: deep Tantivy offsets get progressively costlier.
        offset: query.offset.unwrap_or(0).min(SEARCH_MAX_OFFSET),
        amount_gte: query.amount_gte,
        amount_lte: query.amount_lte,
        date_after: query.date_after,
        date_before: query.date_before,
        // The HTTP surface is the one facet consumer — the response exposes them.
        facets: true,
    };
    let results = state.search.query(req).await?;
    Ok(Json(json!({
        "query": query.q,
        // The real match count (was hits.len(), i.e. the page size).
        "total": results.total,
        "count": results.hits.len(),
        "hits": results.hits,
        "facets": results.facets,
    })))
}

/// Upper bound on `GET /search?offset=` — deep offsets get costlier in Tantivy.
const SEARCH_MAX_OFFSET: usize = 10_000;

#[utoipa::path(
    get,
    path = "/search/status",
    tag = "search",
    responses((status = 200, description = "`{enabled, doc_count, disk_bytes, segment_count}` — index telemetry. `doc_count: 0` on an enabled index means it was wiped (schema drift) or never populated; rebuild with the `search-backfill` bin. `disk_bytes` is the index directory's on-disk size and `segment_count` the searchable segments the reader sees — `doc_count` flat while those climb is the growth signal upserts hide. Both are 0 when search is disabled (`NoSearch` measures nothing rather than guessing)."))
)]
pub(crate) async fn search_status(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let doc_count = state.search.doc_count().await?;
    // Physical footprint alongside the logical count: an index that upserts keeps
    // doc_count flat while bytes/segments grow, so doc_count alone cannot show
    // unbounded growth.
    let stats = state.search.index_stats().await?;
    Ok(Json(json!({
        "enabled": state.config.search.enabled,
        "doc_count": doc_count,
        "disk_bytes": stats.disk_bytes,
        "segment_count": stats.segment_count,
    })))
}

// ---- Saved searches (standing alerts) ---------------------------------------

#[derive(Deserialize, IntoParams)]
pub(crate) struct SavedSearchesQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/searches",
    tag = "search",
    params(SavedSearchesQuery),
    responses((status = 200, description = "Dual-mode: `{searches: [SavedSearch]}`, or `{items, next_cursor}` when `cursor` is present."))
)]
pub(crate) async fn list_saved_searches(
    State(state): State<AppState>,
    Query(query): Query<SavedSearchesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        // Legacy bare-array mode is still capped: an uncursored list must not
        // stream an entire table.
        let searches = state
            .storage
            .list_saved_searches_page(false, None, limit)
            .await?;
        return Ok(Json(json!({ "searches": searches })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .storage
        .list_saved_searches_page(false, after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |s| {
        format!("{}|{}", pumper_core::datasets::ts(s.created_at), s.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateSavedSearchBody {
    /// Full-text query (same syntax as GET /search).
    query: String,
    /// Optional scope: only this app / dataset.
    app: Option<String>,
    dataset: Option<String>,
    /// Webhook that receives `search.matched` events for NEW matches.
    url: String,
    /// If set, delivery bodies are HMAC-SHA256 signed with this secret.
    secret: Option<String>,
    /// If set, each run also snapshots the query's result set into this dataset
    /// (M13 "queries as datasets"): one record per hit keyed by search doc id,
    /// hits that fall out of the results tombstoned — so the view's deltas feed
    /// watches/triggers/`?filter=`/export. Capped by `[search]
    /// max_materialize_results`.
    materialize: Option<MaterializeBody>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct MaterializeBody {
    /// App namespace the view dataset lives under (e.g. `search`).
    app: String,
    /// View dataset name (e.g. `view-ai-grants`).
    dataset: String,
}

#[utoipa::path(
    post,
    path = "/searches",
    tag = "search",
    request_body = CreateSavedSearchBody,
    responses(
        (status = 201, description = "Created saved search", body = Object),
        (status = 400, description = "Empty query or url not http(s)", body = Object),
    )
)]
pub(crate) async fn create_saved_search(
    State(state): State<AppState>,
    Json(body): Json<CreateSavedSearchBody>,
) -> Result<(StatusCode, Json<pumper_core::SavedSearch>), ApiError> {
    if body.query.trim().is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "'query' is required".into(),
        ));
    }
    if !body.url.starts_with("http://") && !body.url.starts_with("https://") {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "url must be http(s)".into(),
        ));
    }
    let materialize = match &body.materialize {
        None => None,
        Some(m) => {
            let (app, dataset) = (m.app.trim(), m.dataset.trim());
            if app.is_empty() || dataset.is_empty() {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "materialize requires non-empty 'app' and 'dataset'".into(),
                ));
            }
            // Feedback-loop guard: a view materializing into the very dataset
            // its query is scoped to would re-materialize its own records if
            // that dataset ever gets (back)indexed — refuse the shape outright.
            if body.app.as_deref() == Some(app) && body.dataset.as_deref() == Some(dataset) {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "materialize target must differ from the search's own app/dataset scope".into(),
                ));
            }
            Some(pumper_core::SearchMaterialize {
                app: app.to_string(),
                dataset: dataset.to_string(),
            })
        }
    };
    let search = state
        .storage
        .create_saved_search(
            body.query.trim(),
            body.app.as_deref(),
            body.dataset.as_deref(),
            &body.url,
            body.secret.as_deref(),
            materialize.as_ref(),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(search)))
}

#[utoipa::path(
    delete,
    path = "/searches/{id}",
    tag = "search",
    params(("id" = String, Path, description = "Saved search id")),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`)"),
        (status = 404, description = "Saved search not found", body = Object),
    )
)]
pub(crate) async fn delete_saved_search(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_saved_search(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            "saved search not found".into(),
        ))
    }
}

#[utoipa::path(
    post,
    path = "/searches/{id}/enabled",
    tag = "search",
    params(("id" = String, Path, description = "Saved search id")),
    request_body = EnabledBody,
    responses(
        (status = 200, description = "`{id, enabled}`"),
        (status = 404, description = "Saved search not found", body = Object),
    )
)]
pub(crate) async fn set_saved_search_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state
        .storage
        .set_saved_search_enabled(&id, body.enabled)
        .await?
    {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            "saved search not found".into(),
        ))
    }
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct DeleteDocsBody {
    ids: Vec<String>,
}

/// Removes specific documents from the search index by id.
#[utoipa::path(
    delete,
    path = "/search/docs",
    tag = "search",
    request_body = DeleteDocsBody,
    responses(
        (status = 200, description = "`{deleted: <count>}`"),
        (status = 400, description = "`ids` must be non-empty", body = Object),
    )
)]
pub(crate) async fn delete_search_docs(
    State(state): State<AppState>,
    Json(body): Json<DeleteDocsBody>,
) -> Result<Json<Value>, ApiError> {
    if body.ids.is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "'ids' must be non-empty".into(),
        ));
    }
    let count = body.ids.len();
    state.search.delete_ids(&body.ids).await?;
    Ok(Json(json!({ "deleted": count })))
}

/// Removes every indexed document of one app's dataset.
#[utoipa::path(
    delete,
    path = "/search/datasets/{app}/{dataset}",
    tag = "search",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses((status = 200, description = "`{app, dataset, deleted: true}`"))
)]
pub(crate) async fn delete_search_dataset(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    state.search.delete_dataset(&app, &dataset).await?;
    Ok(Json(
        json!({ "app": app, "dataset": dataset, "deleted": true }),
    ))
}
