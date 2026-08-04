//! Change-detected dataset records: list (filtered/paged), delete dataset/record,
//! streamed export (json/ndjson/csv), near-duplicate scan, the change feed, and
//! per-record revision history.

use std::convert::Infallible;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use crate::routes::error::{default_limit, keyset_cursor, parse_cursor, parse_since, ApiError};
use crate::state::AppState;

/// Cursor variant for revision feeds whose tiebreak is numeric (a rowid or a
/// per-key revision number). A malformed or empty cursor pages from the top.
fn parse_cursor_i64(cursor: &str) -> Option<(String, i64)> {
    parse_cursor(cursor).and_then(|(t, k)| k.parse().ok().map(|n| (t, n)))
}

#[utoipa::path(
    get,
    path = "/apps/{name}/datasets",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    responses((status = 200, description = "`{app, datasets: [name]}`"))
)]
pub(crate) async fn list_datasets(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let names = state.datasets.datasets(&name).await?;
    Ok(Json(json!({ "app": name, "datasets": names })))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct RecordsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence switches to `{items, next_cursor}`.
    cursor: Option<String>,
    /// Trust filter: `all` (default here — every record carries its own `trust`
    /// field, so the raw dataset view stays complete), `stable`, `provisional` or
    /// `quarantined`. Applies to every read shape on this route — default,
    /// cursor-paged, and `filter=`-narrowed alike.
    #[serde(default = "default_trust_all")]
    trust: String,
    /// Tombstone inclusion: `exclude` (default) or `include`. Matches the
    /// filtered read path and `/grants` — before this param existed the
    /// unfiltered page always included removed records while a filtered one
    /// never did, so adding `?filter=` silently changed what "the dataset"
    /// meant. See docs/features/datasets.md § Querying & export.
    #[serde(default = "default_removed_exclude")]
    removed: String,
}

/// `GET /datasets/...` returns everything by default: the records carry their own
/// stamp, so a consumer can see and decide.
pub(crate) fn default_trust_all() -> String {
    "all".to_string()
}

/// `GET /changes` returns only what we stand behind by default. A pull API is
/// re-readable and therefore recoverable, so it filters rather than suppressing —
/// and a consumer that wants everything can always ask for `trust=all`.
pub(crate) fn default_trust_stable() -> String {
    pumper_core::datasets::TRUST_STABLE.to_string()
}

/// Maps the query value to the store's filter: `all` means no predicate.
pub(crate) fn trust_filter(raw: &str) -> Option<&str> {
    (raw != "all").then_some(raw)
}

/// Default for `?removed=` on every `/datasets/{app}/{ds}` read shape: exclude
/// tombstones. Matches `list_filtered`/`list_filtered_trust` (and `/grants`,
/// which is built on the latter) — this is the one place that used to
/// disagree, because the unfiltered `list`/`list_page` paths always included
/// removed rows while the filtered path never did.
pub(crate) fn default_removed_exclude() -> String {
    "exclude".to_string()
}

/// Parses `?removed=` into the boolean `list_records_view` wants. Anything but
/// `include`/`exclude` is the client's mistake, not a silent fallback — a typo
/// here (`?removed=all`) must not quietly resolve to "excluded" and hide the
/// records the caller was asking to see.
pub(crate) fn parse_removed(raw: &str) -> Result<bool, ApiError> {
    match raw {
        "include" => Ok(true),
        "exclude" => Ok(false),
        other => Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("removed '{other}' must be 'include' or 'exclude'"),
        )),
    }
}

/// Pulls the repeatable `filter` query params out of the raw pair list. axum's
/// typed `Query<Struct>` (serde_urlencoded) collapses repeated keys, so the
/// generic `?filter=…&filter=…` surface is read from the full pair vector
/// instead.
pub(crate) fn filter_specs(pairs: &[(String, String)]) -> Vec<String> {
    pairs
        .iter()
        .filter(|(k, _)| k == "filter")
        .map(|(_, v)| v.clone())
        .collect()
}

/// Parses repeatable `filter` specs into store-level [`JsonFilter`]s (all ANDed).
///
/// Grammar, one per `?filter=` param: `<path>:<op>:<value>` where `path` is a
/// JSON path like `$.state` and `op` is one of `eq` (exact text), `contains`
/// (case-insensitive substring), `gte` / `lte` (text, lexicographic), or
/// `numgte` (numeric `>=` on any of `path`'s comma-separated fields — an OR). The
/// value keeps any `:` after the op, so timestamps/URLs pass through. Malformed
/// specs map to `400` (the shared `Error::BadRequest` path). Example:
/// `?filter=$.state:eq:CA&filter=$.amount:numgte:1000`.
pub(crate) fn parse_filters(
    specs: &[String],
) -> Result<Vec<pumper_core::datasets::JsonFilter>, ApiError> {
    use pumper_core::datasets::JsonFilter;
    let bad = |msg: String| ApiError(StatusCode::BAD_REQUEST, msg);
    let mut out = Vec::with_capacity(specs.len());
    for spec in specs {
        // splitn(3): path and op never contain ':'; the value keeps the rest.
        let mut parts = spec.splitn(3, ':');
        let path = parts.next().unwrap_or("");
        let (Some(op), Some(value)) = (parts.next(), parts.next()) else {
            return Err(bad(format!(
                "filter '{spec}' must be '<path>:<op>:<value>' (e.g. $.state:eq:CA)"
            )));
        };
        let check_path = |p: &str| -> Result<(), ApiError> {
            if p.starts_with("$.") {
                Ok(())
            } else {
                Err(bad(format!(
                    "filter path '{p}' must be a JSON path starting with '$.' (in '{spec}')"
                )))
            }
        };
        let filter = match op {
            "eq" => {
                check_path(path)?;
                JsonFilter::Eq {
                    path: path.into(),
                    value: value.into(),
                }
            }
            "contains" => {
                check_path(path)?;
                JsonFilter::Contains {
                    path: path.into(),
                    value: value.into(),
                }
            }
            "gte" => {
                check_path(path)?;
                JsonFilter::Gte {
                    path: path.into(),
                    value: value.into(),
                }
            }
            "lte" => {
                check_path(path)?;
                JsonFilter::Lte {
                    path: path.into(),
                    value: value.into(),
                }
            }
            "numgte" => {
                let num: f64 = value.parse().map_err(|_| {
                    bad(format!(
                        "filter '{spec}': '{value}' is not a number for op 'numgte'"
                    ))
                })?;
                let paths: Vec<String> = path
                    .split(',')
                    .map(str::trim)
                    .filter(|p| !p.is_empty())
                    .map(String::from)
                    .collect();
                if paths.is_empty() {
                    return Err(bad(format!(
                        "filter '{spec}': 'numgte' needs at least one path"
                    )));
                }
                for p in &paths {
                    check_path(p)?;
                }
                JsonFilter::NumGteAny { paths, value: num }
            }
            other => {
                return Err(bad(format!(
                    "filter '{spec}': unknown op '{other}' (eq | contains | gte | lte | numgte)"
                )))
            }
        };
        out.push(filter);
    }
    Ok(out)
}

#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        RecordsQuery,
        ("filter" = Option<Vec<String>>, Query, description = "Repeatable `<path>:<op>:<value>` predicate, all ANDed (e.g. `$.state:eq:CA`, `$.amount:numgte:1000`). ops: eq | contains | gte | lte | numgte. Pushed into SQL."),
    ),
    responses(
        (status = 200, description = "Dual-mode: bare `[Record]` array, or `{items, next_cursor}` when `cursor` is present. Every shape honors `trust=` and `removed=` identically — default, cursor-paged, and `filter=`-narrowed."),
        (status = 400, description = "Malformed `filter` or `removed` value", body = Object),
    )
)]
pub(crate) async fn list_records(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<RecordsQuery>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 1000);
    let filters = parse_filters(&filter_specs(&pairs))?;
    let trust = trust_filter(&query.trust);
    let include_removed = parse_removed(&query.removed)?;
    // One function for every shape on this route (default, cursor, filtered):
    // `list_records_view` honors `trust=`/`removed=` identically regardless of
    // whether `filter=` is present, closing the read-path split where adding a
    // `filter=` used to silently change both.
    let Some(cursor) = &query.cursor else {
        let records = state
            .datasets
            .list_records_view(
                &app,
                &dataset,
                &filters,
                None,
                limit,
                trust,
                include_removed,
            )
            .await?;
        return Ok(Json(json!(records)));
    };
    let after = parse_cursor(cursor);
    let records = state
        .datasets
        .list_records_view(
            &app,
            &dataset,
            &filters,
            after,
            limit,
            trust,
            include_removed,
        )
        .await?;
    let next_cursor = keyset_cursor(&records, limit, |r| {
        format!("{}|{}", pumper_core::datasets::ts(r.updated_at), r.key)
    });
    Ok(Json(
        json!({ "items": records, "next_cursor": next_cursor }),
    ))
}

#[utoipa::path(
    delete,
    path = "/datasets/{app}/{dataset}",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses((status = 200, description = "`{app, dataset, deleted}` — records removed (with their full revision history and search docs). Hard delete; use for retiring or re-importing a dataset."))
)]
pub(crate) async fn delete_dataset_route(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let deleted = state.datasets.delete_dataset(&app, &dataset).await?;
    // Drop the dataset's search docs too (best-effort — the records are already
    // gone; a stale search doc would just return a hit for a deleted record).
    if let Err(e) = state.search.delete_dataset(&app, &dataset).await {
        tracing::warn!(%app, %dataset, "dataset deleted but search cleanup failed: {e}");
    }
    Ok(Json(
        json!({ "app": app, "dataset": dataset, "deleted": deleted }),
    ))
}

#[utoipa::path(
    delete,
    path = "/datasets/{app}/{dataset}/records/{key}",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        ("key" = String, Path, description = "Record key"),
    ),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`) — the record and its full revision history."),
        (status = 404, description = "Record not found", body = Object),
    )
)]
pub(crate) async fn delete_record_route(
    State(state): State<AppState>,
    Path((app, dataset, key)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    let existed = state.datasets.delete_record(&app, &dataset, &key).await?;
    if !existed {
        return Err(ApiError(StatusCode::NOT_FOUND, "record not found".into()));
    }
    if let Err(e) = state
        .search
        .delete_ids(&[pumper_core::SearchDoc::dataset_id(&app, &dataset, &key)])
        .await
    {
        tracing::warn!(%app, %dataset, %key, "record deleted but search cleanup failed: {e}");
    }
    Ok(Json(json!({ "deleted": true })))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct ExportQuery {
    /// 'json' (default) | 'ndjson' | 'csv'. All three stream in constant memory.
    format: Option<String>,
    /// Trust filter, same vocabulary and default (`all`) as `GET
    /// /datasets/{app}/{ds}` — an export is a complete copy by default (every
    /// record carries its own `trust` field), but it now honors an explicit
    /// `trust=stable` etc. instead of silently ignoring it.
    #[serde(default = "default_trust_all")]
    trust: String,
    /// Tombstone inclusion, same vocabulary and default (`exclude`) as `GET
    /// /datasets/{app}/{ds}`.
    #[serde(default = "default_removed_exclude")]
    removed: String,
}

#[derive(Clone, Copy)]
enum ExportFormat {
    /// A single streamed JSON array — `[{record},{record},...]`.
    Json,
    /// One JSON object per line.
    Ndjson,
    /// RFC-4180 rows with a fixed header.
    Csv,
}

impl ExportFormat {
    fn extension(self) -> &'static str {
        match self {
            ExportFormat::Json => "json",
            ExportFormat::Ndjson => "ndjson",
            ExportFormat::Csv => "csv",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            ExportFormat::Json => "application/json",
            ExportFormat::Ndjson => "application/x-ndjson",
            ExportFormat::Csv => "text/csv; charset=utf-8",
        }
    }
}

#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}/export",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        ExportQuery,
        ("filter" = Option<Vec<String>>, Query, description = "Repeatable `<path>:<op>:<value>` predicate, all ANDed (same grammar as `GET /datasets/{app}/{dataset}`). Pushed into SQL, so a filtered export streams only matching rows — a targeted export instead of the whole corpus."),
    ),
    responses(
        (status = 200, description = "Streamed export as a JSON array, NDJSON, or CSV (per `format`); constant memory, no row cap. `content-disposition: attachment`."),
        (status = 400, description = "Unknown format, malformed `filter`, or bad `trust`/`removed` value", body = Object),
    )
)]
pub(crate) async fn export_records(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<ExportQuery>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Response, ApiError> {
    let format = match query.format.as_deref().unwrap_or("json") {
        "json" => ExportFormat::Json,
        "ndjson" => ExportFormat::Ndjson,
        "csv" => ExportFormat::Csv,
        other => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!("unknown format '{other}' (json | ndjson | csv)"),
            ))
        }
    };
    // Validate filters/trust/removed up front so a bad spec is a clean 400,
    // not a mid-stream abort.
    let filters = parse_filters(&filter_specs(&pairs))?;
    let trust = query.trust.clone();
    let include_removed = parse_removed(&query.removed)?;
    Ok(stream_export(
        state,
        app,
        dataset,
        format,
        filters,
        trust,
        include_removed,
    ))
}

/// Streams the whole dataset in keyset-paged batches — constant memory
/// regardless of dataset size, with no row cap or silent truncation. `json`
/// frames the batches as one array (`[`, comma-separated records, `]`); `ndjson`
/// and `csv` stream line-oriented output.
fn stream_export(
    state: AppState,
    app: String,
    dataset: String,
    format: ExportFormat,
    filters: Vec<pumper_core::datasets::JsonFilter>,
    trust: String,
    include_removed: bool,
) -> Response {
    const BATCH: i64 = 1_000;
    let filename = format!("attachment; filename=\"{dataset}.{}\"", format.extension());
    let content_type = format.content_type();
    let stream = async_stream::stream! {
        match format {
            ExportFormat::Csv => yield Ok::<_, Infallible>(axum::body::Bytes::from_static(
                b"key,first_seen,last_seen,updated_at,removed_at,data\n",
            )),
            ExportFormat::Json => yield Ok(axum::body::Bytes::from_static(b"[")),
            ExportFormat::Ndjson => {}
        }
        let trust = trust_filter(&trust).map(str::to_string);
        let mut after: Option<(String, String)> = None;
        let mut first = true;
        loop {
            let batch = match state
                .datasets
                .list_records_view(&app, &dataset, &filters, after.clone(), BATCH, trust.as_deref(), include_removed)
                .await
            {
                Ok(batch) => batch,
                Err(e) => {
                    tracing::warn!(app = %app, dataset = %dataset, "export stream aborted: {e}");
                    break;
                }
            };
            let Some(last) = batch.last() else { break };
            after = Some((pumper_core::datasets::ts(last.updated_at), last.key.clone()));
            let short = (batch.len() as i64) < BATCH;
            let mut chunk = String::new();
            for record in &batch {
                match format {
                    ExportFormat::Csv => csv_row(&mut chunk, record),
                    ExportFormat::Ndjson => {
                        if let Ok(line) = serde_json::to_string(record) {
                            chunk.push_str(&line);
                            chunk.push('\n');
                        }
                    }
                    ExportFormat::Json => {
                        if let Ok(line) = serde_json::to_string(record) {
                            if !first {
                                chunk.push(',');
                            }
                            first = false;
                            chunk.push_str(&line);
                        }
                    }
                }
            }
            yield Ok(axum::body::Bytes::from(chunk));
            if short {
                break;
            }
        }
        if let ExportFormat::Json = format {
            yield Ok(axum::body::Bytes::from_static(b"]"));
        }
    };
    (
        [
            ("content-type", content_type.to_string()),
            ("content-disposition", filename),
        ],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

/// Appends one CSV row: fixed columns, RFC-4180 quoting for key and data.
fn csv_row(out: &mut String, record: &pumper_core::Record) {
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    out.push_str(&format!(
        "{},{},{},{},{},{}\n",
        quote(&record.key),
        record.first_seen.to_rfc3339(),
        record.last_seen.to_rfc3339(),
        record.updated_at.to_rfc3339(),
        record
            .removed_at
            .map(|d| d.to_rfc3339())
            .unwrap_or_default(),
        quote(&record.data.to_string()),
    ));
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct DupQuery {
    #[serde(default = "default_distance")]
    distance: u32,
}

fn default_distance() -> u32 {
    3
}

/// Upper bound on dataset size for the duplicate scan. The comparison is an
/// O(n²) pairwise SimHash sweep held in memory, so a dataset past this size is
/// rejected (413) rather than pinning a core; page or narrow the dataset, or run
/// the scan offline. 10k rows ≈ 50M Hamming comparisons — sub-second, bounded.
const DUP_SCAN_MAX: i64 = 10_000;

/// Near-duplicate record pairs (SimHash Hamming distance ≤ `distance`).
#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}/duplicates",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        DupQuery,
    ),
    responses(
        (status = 200, description = "`{app, dataset, max_distance, pairs}`"),
        (status = 413, description = "Dataset over the 10k O(n^2) scan cap", body = Object),
    )
)]
pub(crate) async fn dataset_duplicates(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<DupQuery>,
) -> Result<Json<Value>, ApiError> {
    let count = state.datasets.record_count(&app, &dataset).await?;
    if count > DUP_SCAN_MAX {
        return Err(ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "dataset has {count} records; the duplicate scan is O(n²) and capped at \
                 {DUP_SCAN_MAX}. Narrow the dataset or run the scan offline."
            ),
        ));
    }
    let distance = query.distance.min(20);
    let pairs = state
        .datasets
        .duplicate_pairs(&app, &dataset, distance)
        .await?;
    Ok(Json(json!({
        "app": app,
        "dataset": dataset,
        "max_distance": distance,
        "pairs": pairs,
    })))
}

// ---- Change intelligence ---------------------------------------------------

#[derive(Deserialize, IntoParams)]
pub(crate) struct ChangesQuery {
    /// RFC 3339 lower bound; only revisions after this instant are returned.
    since: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    /// Pages the full feed past the legacy 1000-row clamp; `since` still applies.
    cursor: Option<String>,
    /// Trust filter: `stable` (default), `all`, `provisional` or `quarantined`.
    /// Revisions written while a source was degrading are held back from the
    /// default feed; nothing written before extraction health existed is affected,
    /// because an unstamped revision *is* stable.
    #[serde(default = "default_trust_stable")]
    trust: String,
}

/// Change feed for a dataset: new/changed/removed revisions, newest first,
/// each carrying the field-level diff versus its previous revision.
#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}/changes",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        ChangesQuery,
    ),
    responses((status = 200, description = "Dual-mode: `{app, dataset, count, changes}` (clamped 1000), or `{items, next_cursor}` when `cursor` is present (pages the full feed)."))
)]
pub(crate) async fn dataset_changes(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<ChangesQuery>,
) -> Result<Json<Value>, ApiError> {
    let since = parse_since(query.since.as_deref())?;
    let trust = trust_filter(&query.trust);
    let Some(cursor) = &query.cursor else {
        let changes = state
            .datasets
            .changes_since(
                &app,
                Some(&dataset),
                since,
                query.limit.clamp(1, 1000),
                trust,
            )
            .await?;
        return Ok(Json(json!({
            "app": app,
            "dataset": dataset,
            "count": changes.len(),
            "trust": query.trust,
            "changes": changes,
        })));
    };
    let after = parse_cursor_i64(cursor);
    let page = state
        .datasets
        .changes_page(
            &app,
            Some(&dataset),
            since,
            after,
            query.limit.clamp(1, 1000),
            trust,
        )
        .await?;
    Ok(Json(
        json!({ "items": page.items, "next_cursor": page.next_cursor }),
    ))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct HistoryQuery {
    /// Record key (query param, since keys may contain URL-hostile characters).
    key: String,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    /// Pages the full history past the legacy 500-row clamp.
    cursor: Option<String>,
}

/// A single record's revision history, newest first.
#[utoipa::path(
    get,
    path = "/datasets/{app}/{dataset}/history",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        HistoryQuery,
    ),
    responses((status = 200, description = "Dual-mode: `{app, dataset, key, count, revisions}` (clamped 500), or `{items, next_cursor}` when `cursor` is present."))
)]
pub(crate) async fn record_history(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Value>, ApiError> {
    let Some(cursor) = &query.cursor else {
        let revisions = state
            .datasets
            .history(&app, &dataset, &query.key, query.limit.clamp(1, 500))
            .await?;
        return Ok(Json(json!({
            "app": app,
            "dataset": dataset,
            "key": query.key,
            "count": revisions.len(),
            "revisions": revisions,
        })));
    };
    let after = parse_cursor_i64(cursor);
    let page = state
        .datasets
        .history_page(&app, &dataset, &query.key, after, query.limit.clamp(1, 500))
        .await?;
    Ok(Json(
        json!({ "items": page.items, "next_cursor": page.next_cursor }),
    ))
}
