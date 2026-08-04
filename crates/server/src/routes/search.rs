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

/// Upper bound on `?offset=` — deep offsets get progressively costlier in
/// Tantivy. Also published in the MCP `search` tool's schema.
pub(crate) const SEARCH_MAX_OFFSET: usize = 10_000;

/// The raw query surface both search callers speak: `GET /search`'s extractor
/// and the MCP `search` tool's JSON arguments. Every field is as the caller sent
/// it — defaulting, clamping, and the `sort` vocabulary belong to
/// [`build_search_request`], so the two surfaces cannot drift into two
/// grammars.
#[derive(Default)]
pub(crate) struct SearchInput {
    pub q: String,
    pub limit: Option<usize>,
    pub app: Option<String>,
    pub dataset: Option<String>,
    pub fuzzy: bool,
    pub sort: Option<String>,
    pub since: Option<i64>,
    pub offset: Option<usize>,
    pub amount_gte: Option<u64>,
    pub amount_lte: Option<u64>,
    pub date_after: Option<i64>,
    pub date_before: Option<i64>,
    /// Compute app/dataset breakdowns. Only the HTTP route returns them.
    pub facets: bool,
}

/// Validates + clamps a [`SearchInput`] into the core `SearchRequest`. `Err` is
/// one caller-facing message (a 400 body over HTTP, a tool error over MCP).
///
/// This is the single place the query surface's rules live: `q` required,
/// `limit` 1–100 (default 20), `offset` capped at [`SEARCH_MAX_OFFSET`], and
/// `sort` restricted to `score` | `newest` — an unknown sort is refused rather
/// than silently falling back to relevance.
pub(crate) fn build_search_request(
    input: SearchInput,
) -> Result<pumper_core::SearchRequest, String> {
    if input.q.trim().is_empty() {
        return Err("query 'q' is required".into());
    }
    let sort = match input.sort.as_deref() {
        None | Some("score") => pumper_core::SearchSort::Score,
        Some("newest") => pumper_core::SearchSort::Newest,
        Some(other) => {
            return Err(format!(
                "unknown sort '{other}' (expected 'score' or 'newest')"
            ))
        }
    };
    Ok(pumper_core::SearchRequest {
        q: input.q,
        limit: input
            .limit
            .unwrap_or_else(default_search_limit)
            .clamp(1, 100),
        app: input.app,
        dataset: input.dataset,
        fuzzy: input.fuzzy,
        sort,
        since: input.since,
        // Clamp like `limit`: deep Tantivy offsets get progressively costlier.
        offset: input.offset.unwrap_or(0).min(SEARCH_MAX_OFFSET),
        amount_gte: input.amount_gte,
        amount_lte: input.amount_lte,
        date_after: input.date_after,
        date_before: input.date_before,
        facets: input.facets,
    })
}

/// Full-text search across everything indexed from job results (BM25 ranked),
/// with highlighted snippets and app/dataset facets over the matching set.
#[utoipa::path(
    get,
    path = "/search",
    tag = "search",
    params(SearchQuery),
    responses(
        (status = 200, description = "`{query, total, count, hits, facets}` — BM25 ranked (or `sort=newest`), highlighted snippets. `total` is the full match count; `count` is the returned page size. `offset` pages (offset=limit → page 2, clamped to 10000); `sort=newest` orders by index time; `since=<unix-secs>` filters to recent docs. Entity filters (index-time regex extraction; docs without an extracted value never match): `amount_gte`/`amount_lte` in whole US dollars, `date_before`/`date_after` on the extracted deadline (unix seconds). The MCP `search` tool takes these same params through the same parser, and returns everything but `facets`."),
        (status = 400, description = "Empty query, or a `sort` other than `score`/`newest`", body = Object),
    )
)]
pub(crate) async fn search(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<Value>, ApiError> {
    let q = query.q.clone();
    // Same builder the MCP `search` tool calls — one grammar, one set of clamps.
    let req = build_search_request(SearchInput {
        q: query.q,
        limit: Some(query.limit),
        app: query.app,
        dataset: query.dataset,
        fuzzy: query.fuzzy,
        sort: query.sort,
        since: query.since,
        offset: query.offset,
        amount_gte: query.amount_gte,
        amount_lte: query.amount_lte,
        date_after: query.date_after,
        date_before: query.date_before,
        // The HTTP surface is the one facet consumer — the response exposes them.
        facets: true,
    })
    .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    let results = state.search.query(req).await?;
    Ok(Json(json!({
        "query": q,
        // The real match count (was hits.len(), i.e. the page size).
        "total": results.total,
        "count": results.hits.len(),
        "hits": results.hits,
        "facets": results.facets,
    })))
}

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

#[cfg(test)]
mod tests {
    use super::{build_search_request, SearchInput, SEARCH_MAX_OFFSET};
    use pumper_core::SearchSort;

    fn input(q: &str) -> SearchInput {
        SearchInput {
            q: q.into(),
            ..Default::default()
        }
    }

    /// The anti-pattern: each caller clamping for itself, so one surface accepts
    /// `limit=100000` or a 1-million-deep offset the other refuses.
    #[test]
    fn limits_and_offsets_are_clamped_not_taken_at_face_value() {
        let req = build_search_request(SearchInput {
            limit: Some(100_000),
            offset: Some(SEARCH_MAX_OFFSET * 10),
            ..input("grants")
        })
        .unwrap();
        assert_eq!(req.limit, 100);
        assert_eq!(req.offset, SEARCH_MAX_OFFSET);

        let req = build_search_request(SearchInput {
            limit: Some(0),
            ..input("grants")
        })
        .unwrap();
        assert_eq!(req.limit, 1, "a zero page is clamped up, not left empty");

        // Omitted = the documented defaults, from one place.
        let req = build_search_request(input("grants")).unwrap();
        assert_eq!((req.limit, req.offset), (20, 0));
        assert_eq!(req.sort, SearchSort::Score);
        assert!(!req.facets, "facets are opt-in");
    }

    /// The anti-pattern: an unrecognized `sort` silently falling back to
    /// relevance, so a caller that asked for recency gets ranking and never
    /// learns it was ignored.
    #[test]
    fn unknown_sort_is_refused_not_silently_scored() {
        assert_eq!(
            build_search_request(SearchInput {
                sort: Some("newest".into()),
                ..input("grants")
            })
            .unwrap()
            .sort,
            SearchSort::Newest
        );
        let err = build_search_request(SearchInput {
            sort: Some("sideways".into()),
            ..input("grants")
        })
        .unwrap_err();
        assert!(err.contains("sideways") && err.contains("newest"), "{err}");
    }

    /// The anti-pattern: a blank/whitespace `q` reaching Tantivy's parser and
    /// coming back as an opaque 500 instead of a caller error.
    #[test]
    fn blank_query_is_rejected_not_parsed() {
        assert!(build_search_request(input("   "))
            .unwrap_err()
            .contains("q"));
    }

    /// The entity filters are carried through verbatim — the builder validates
    /// the surface, it does not reinterpret the ranges.
    #[test]
    fn entity_filters_pass_through_unmodified() {
        let req = build_search_request(SearchInput {
            amount_gte: Some(100_000),
            amount_lte: Some(5_000_000),
            date_after: Some(1_800_000_000),
            date_before: Some(1_900_000_000),
            since: Some(1_700_000_000),
            fuzzy: true,
            app: Some("grants".into()),
            dataset: Some("unified".into()),
            ..input("rural health")
        })
        .unwrap();
        assert_eq!(req.amount_gte, Some(100_000));
        assert_eq!(req.amount_lte, Some(5_000_000));
        assert_eq!(req.date_after, Some(1_800_000_000));
        assert_eq!(req.date_before, Some(1_900_000_000));
        assert_eq!(req.since, Some(1_700_000_000));
        assert!(req.fuzzy);
        assert_eq!(req.app.as_deref(), Some("grants"));
        assert_eq!(req.dataset.as_deref(), Some("unified"));
    }
}
