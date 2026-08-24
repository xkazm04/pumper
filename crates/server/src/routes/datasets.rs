//! Change-detected dataset records: list (filtered/paged), delete dataset/record,
//! streamed export (json/ndjson/csv), near-duplicate scan, the change feed, and
//! per-record revision history.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use crate::routes::error::{
    bad_cursor_message, default_limit, keyset_cursor, parse_cursor, parse_cursor_arg, parse_since,
    ApiError,
};
use crate::state::AppState;

/// Cursor variant for the revision feeds whose tiebreak is numeric (a rowid or
/// a per-key revision number): `/changes` and `/history`.
///
/// Strict, via [`parse_cursor_arg`] — blank means "first page", anything else
/// unparseable is a 400. A non-numeric tiebreak (`t|abc`) is exactly as
/// malformed as a missing separator and gets the same body, so a client sees
/// one cursor error shape across both routes.
fn parse_cursor_i64_arg(cursor: &str) -> Result<Option<(String, i64)>, ApiError> {
    let Some((ts, tiebreak)) = parse_cursor_arg(cursor)? else {
        return Ok(None);
    };
    tiebreak
        .parse::<i64>()
        .map(|n| Some((ts, n)))
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, bad_cursor_message(cursor)))
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

#[derive(Deserialize, IntoParams)]
pub(crate) struct DeleteDatasetQuery {
    /// The echo: the exact `<app>/<dataset>` string, trimmed, compared
    /// case-sensitively against the identity the store resolved. Absent means
    /// "preview only".
    confirm: Option<String>,
    /// The record count the preview reported. The delete refuses unless the live
    /// count is exactly this, so an operator can only destroy the population they
    /// were actually shown. Absent means "preview only".
    expect_records: Option<u64>,
}

/// How many rows the export-before-delete reads per page. Bounded so a
/// million-revision dataset exports in constant memory.
const DELETE_EXPORT_PAGE: i64 = 500;

/// Directory under `artifacts_dir` where a deleted dataset's export lands.
const DELETED_EXPORT_DIR: &str = "deleted-datasets";

#[utoipa::path(
    delete,
    path = "/datasets/{app}/{dataset}",
    tag = "datasets",
    params(
        ("app" = String, Path, description = "App name"),
        ("dataset" = String, Path, description = "Dataset name"),
        DeleteDatasetQuery,
    ),
    responses(
        (status = 200, description = "`{preview: false, app, dataset, deleted, records, revisions, export, as_of}` — the receipt: what was ACTUALLY destroyed, and the NDJSON export written before it was. Search docs are dropped too."),
        (status = 400, description = "`confirm` did not match `<app>/<dataset>`", body = Object),
        (status = 409, description = "The record count moved since the preview — nothing was deleted; re-preview and retry", body = Object),
        (status = 428, description = "Two-step gate: no `confirm`/`expect_records`, so this call PREVIEWED and deleted nothing. Body is `{preview: true, records, revisions, confirm, expect_records, as_of}` — the exact parameters to retry with.", body = Object),
        (status = 500, description = "The pre-delete export could not be written; nothing was deleted", body = Object),
    )
)]
/// Hard-deletes a whole dataset behind a two-step gate.
///
/// This route used to destroy every record and its full revision history on a
/// bare `DELETE`, with no echo, no preview and no receipt — the single most
/// destructive verb in the API, reachable by a stale browser tab or a copied
/// curl line. Three rungs now stand in front of it, in cost order (registry:
/// data-retention/confirm-by-echo, "echo is one rung of a ladder"):
///
/// 1. **Preview.** Without both parameters the call counts and returns 428.
///    Nothing is written, and the payload says `preview: true` so a saved
///    response can never be read as proof of a deletion.
/// 2. **The echo** (`confirm=<app>/<dataset>`). Its honest value is narrow and
///    worth stating: `app` and `dataset` are already in the path, so this rung
///    proves intent, not comprehension — it is what makes an accidental bare
///    `DELETE` inert. It is compared against the identity the store resolved
///    (`counts.app`/`counts.dataset`, the strings the `WHERE` clause binds),
///    trimmed, exactly.
/// 3. **The yield guard** (`expect_records=<n>`). This is the rung that measures
///    comprehension: `n` cannot be known without having read the target, and it
///    is re-checked inside the deleting transaction, so a population that moved
///    between the preview and the delete is a 409 rather than a surprise.
///
/// Authentication is a fourth rung this server cannot climb: it has no identity
/// concept at all (the only credential anywhere is the ingress HMAC, which
/// authenticates a webhook *sender*, not an operator). That gap is recorded in
/// `.ai/registry-conformance.md` rather than papered over with an invented
/// scheme — these rungs contain the accident, not an attacker on the port.
///
/// Before anything is destroyed, every record and every revision is written to
/// an NDJSON export under `artifacts_dir/deleted-datasets/`, and its path is in
/// the receipt. If that write fails, nothing is deleted.
pub(crate) async fn delete_dataset_route(
    State(state): State<AppState>,
    Path((app, dataset)): Path<(String, String)>,
    Query(query): Query<DeleteDatasetQuery>,
) -> Result<Response, ApiError> {
    use pumper_core::datasets::{DeleteMode, DeleteVerdict};

    // Rung 1. One code path computes the population — this is a MODE of the
    // deleter, not a second implementation that counts, so the numbers shown
    // here are the numbers the delete acts on.
    let previewed = state
        .datasets
        .delete_dataset_mode(&app, &dataset, DeleteMode::Preview)
        .await?;
    let counts = previewed.counts();
    let expected_confirm = format!("{}/{}", counts.app, counts.dataset);
    let (Some(confirm), Some(expect_records)) = (query.confirm.as_deref(), query.expect_records)
    else {
        return Ok((
            StatusCode::PRECONDITION_REQUIRED,
            Json(json!({
                "preview": true,
                "code": "confirmation_required",
                "error": format!(
                    "this deletes {} record(s) and {} revision(s) permanently. Retry with \
                     ?confirm={}&expect_records={} to proceed.",
                    counts.records, counts.revisions, expected_confirm, counts.records
                ),
                "app": counts.app,
                "dataset": counts.dataset,
                "records": counts.records,
                "revisions": counts.revisions,
                "expect_records": counts.records,
                "confirm": expected_confirm,
                "as_of": pumper_core::datasets::ts(counts.as_of),
            })),
        )
            .into_response());
    };

    // Rung 2. Trimmed, case-sensitive — the identifier's own equality rule
    // everywhere else in this API. The expected string is NOT restated here:
    // the preview above is where it is rendered.
    if confirm.trim() != expected_confirm {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "confirm must be the exact '<app>/<dataset>' string this DELETE targets — \
             call without ?confirm= to see it and the record count"
                .into(),
        ));
    }

    // Export BEFORE the delete: a hard delete of a whole history is the one
    // operation in this service with no restore path, and an export is the
    // cheapest one that needs no schema change. A failure here refuses the
    // delete — an unrecoverable destruction whose safety net silently did not
    // write is worse than no net at all.
    let export = export_before_delete(&state, &app, &dataset, counts.records).await?;

    // Rung 3, re-checked inside the deleting transaction.
    let verdict = state
        .datasets
        .delete_dataset_mode(
            &app,
            &dataset,
            DeleteMode::Execute {
                expect_records: Some(expect_records),
            },
        )
        .await?;
    let done = match verdict {
        DeleteVerdict::Deleted(done) => done,
        DeleteVerdict::YieldChanged { expected, found } => {
            tracing::warn!(
                %app, %dataset, expected, found = found.records,
                "dataset delete refused: the population moved since the preview"
            );
            return Err(ApiError(
                StatusCode::CONFLICT,
                format!(
                    "the dataset moved since the preview: you confirmed {expected} record(s), \
                     it now holds {}. Nothing was deleted — re-preview and retry.",
                    found.records
                ),
            ));
        }
        // `Execute` never previews; a `Preview` arm here would be a library bug.
        DeleteVerdict::Preview(_) => {
            return Err(ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                crate::routes::error::INTERNAL_MESSAGE.into(),
            ))
        }
    };
    // Drop the dataset's search docs too (best-effort — the records are already
    // gone; a stale search doc would just return a hit for a deleted record).
    if let Err(e) = state.search.delete_dataset(&app, &dataset).await {
        tracing::warn!(%app, %dataset, "dataset deleted but search cleanup failed: {e}");
    }
    tracing::info!(
        %app, %dataset, records = done.records, revisions = done.revisions,
        export = %export.display(),
        "dataset hard-deleted after confirmation"
    );
    Ok(Json(json!({
        "preview": false,
        "app": done.app,
        "dataset": done.dataset,
        // `deleted` is the record count, unchanged from before the gate existed.
        "deleted": done.records,
        "records": done.records,
        "revisions": done.revisions,
        "export": export.display().to_string(),
        "as_of": pumper_core::datasets::ts(done.as_of),
    }))
    .into_response())
}

/// Writes every record and every revision of `app/dataset` to one NDJSON file
/// under `artifacts_dir/deleted-datasets/`, and returns its path.
///
/// Line 1 is a header stating what this file is and what it claims to contain;
/// the rest are `{"kind":"record"|"revision", ...}` objects, read and written a
/// page at a time so the memory cost does not scale with the dataset.
async fn export_before_delete(
    state: &AppState,
    app: &str,
    dataset: &str,
    records_expected: u64,
) -> Result<std::path::PathBuf, ApiError> {
    use tokio::io::AsyncWriteExt;

    let failed = |what: &str, e: &dyn std::fmt::Display| {
        tracing::error!(%app, %dataset, "pre-delete export failed ({what}): {e}");
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the pre-delete export could not be written, so nothing was deleted".into(),
        )
    };
    let dir = state.storage.artifacts_dir.join(DELETED_EXPORT_DIR);
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| failed("mkdir", &e))?;
    // `app` and `dataset` come straight off the URL path, so they are composed
    // into a filename only after every separator and traversal character is
    // mapped away — the store is happy to bind "../.." as a name.
    let path = dir.join(format!(
        "{}__{}__{}.ndjson",
        file_component(app),
        file_component(dataset),
        chrono::Utc::now().format("%Y%m%dT%H%M%S%3fZ")
    ));
    let file = tokio::fs::File::create(&path)
        .await
        .map_err(|e| failed("create", &e))?;
    let mut out = tokio::io::BufWriter::new(file);
    let write = |value: Value| -> Vec<u8> {
        let mut line = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
        line.push(b'\n');
        line
    };
    out.write_all(&write(json!({
        "kind": "header",
        "export": "pumper.dataset.pre-delete",
        "version": 1,
        "app": app,
        "dataset": dataset,
        "exported_at": pumper_core::datasets::ts(chrono::Utc::now()),
        "records_expected": records_expected,
    })))
    .await
    .map_err(|e| failed("header", &e))?;

    let mut after: Option<(String, String)> = None;
    loop {
        let page = state
            .datasets
            .list_records_view(
                app,
                dataset,
                &[],
                after.clone(),
                DELETE_EXPORT_PAGE,
                None,
                true,
            )
            .await?;
        let Some(last) = page.last() else { break };
        after = Some((pumper_core::datasets::ts(last.updated_at), last.key.clone()));
        let full = page.len() as i64 == DELETE_EXPORT_PAGE;
        for record in &page {
            let mut value = serde_json::to_value(record).unwrap_or(Value::Null);
            if let Some(obj) = value.as_object_mut() {
                obj.insert("kind".into(), json!("record"));
            }
            out.write_all(&write(value))
                .await
                .map_err(|e| failed("record", &e))?;
        }
        if !full {
            break;
        }
    }

    let mut after: Option<(String, i64)> = None;
    loop {
        let page = state
            .datasets
            .dataset_revisions_page(app, dataset, after.clone(), DELETE_EXPORT_PAGE)
            .await?;
        let Some(last) = page.last() else { break };
        after = Some((last.key.clone(), last.revision));
        let full = page.len() as i64 == DELETE_EXPORT_PAGE;
        for revision in &page {
            let mut value = serde_json::to_value(revision).unwrap_or(Value::Null);
            if let Some(obj) = value.as_object_mut() {
                obj.insert("kind".into(), json!("revision"));
            }
            out.write_all(&write(value))
                .await
                .map_err(|e| failed("revision", &e))?;
        }
        if !full {
            break;
        }
    }
    // Flush AND fsync: this file is the only copy of what the next statement
    // destroys, so "the OS has it in a buffer" is not good enough.
    out.flush().await.map_err(|e| failed("flush", &e))?;
    out.into_inner()
        .sync_all()
        .await
        .map_err(|e| failed("fsync", &e))?;
    Ok(path)
}

/// Maps one untrusted name onto a single safe filename component: everything
/// outside `[A-Za-z0-9_-]` becomes `_`, an empty name becomes `_`, and the
/// result is capped so a long name cannot blow the path limit. The dot is
/// mapped too — not because `..` can traverse without a separator, but because
/// the only reader of this name is a human scanning a directory listing, and
/// leaving `..` in a filename this route composes is a shape nobody should have
/// to reason about twice. Mapping rather than refusing: a dataset whose name has
/// a slash is still deletable, it just exports under a flattened filename.
fn file_component(raw: &str) -> String {
    let mapped: String = raw
        .chars()
        .take(64)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if mapped.is_empty() {
        "_".to_string()
    } else {
        mapped
    }
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
        (status = 200, description = "Streamed export as a JSON array, NDJSON, or CSV (per `format`); constant memory, no row cap. `content-disposition: attachment`. A mid-stream store error aborts the connection without a clean end (no closing `]` for json) rather than emitting a truncated-but-valid-looking body; per-row serialization failures are counted and logged, not silently dropped."),
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

/// One page's outcome inside the export walk: a batch of records, or a store
/// error that must abort the whole export.
enum ExportOutcome {
    Batch(Vec<pumper_core::Record>),
    Failed(pumper_core::Error),
}

/// Formats one page of records into `format`'s wire framing, appending to
/// `chunk` and advancing `first` (the JSON array's leading-comma tracker).
/// Returns how many rows in this batch FAILED to serialize — never dropped
/// without a trace, always counted so the caller can log it. `serde_json`
/// realistically never fails on a `Record` (its `data` is already a validated
/// `Value`), but a silent `if let Ok(..)` here would still make an export
/// under-report row-for-row without a signal, so failures are counted rather
/// than assumed impossible.
fn format_batch(
    format: ExportFormat,
    batch: &[pumper_core::Record],
    first: &mut bool,
) -> (String, usize) {
    let mut chunk = String::new();
    let mut failed = 0usize;
    for record in batch {
        match format {
            ExportFormat::Csv => csv_row(&mut chunk, record),
            ExportFormat::Ndjson | ExportFormat::Json => {
                if append_row(&mut chunk, first, format, serde_json::to_string(record)) {
                    failed += 1;
                }
            }
        }
    }
    (chunk, failed)
}

/// Appends one already-attempted JSON serialization to `chunk` per `format`'s
/// framing (`ndjson`: one object per line; `json`: comma-separated array
/// elements). Returns `true` when `serialized` was `Err` — the row is skipped,
/// never silently written as if it were empty. Takes the `Result` rather than
/// re-serializing internally so a test can exercise the failure branch without
/// needing a `Record` whose `Value` genuinely fails to serialize (in practice
/// none does — `Value` cannot hold NaN/Infinity — but the failure path still
/// needs a caller-visible signal instead of the vanished row this replaces).
///
/// `append_row_serialization_failure_is_counted_not_silently_dropped` pins this.
fn append_row(
    chunk: &mut String,
    first: &mut bool,
    format: ExportFormat,
    serialized: Result<String, serde_json::Error>,
) -> bool {
    let Ok(line) = serialized else { return true };
    match format {
        ExportFormat::Ndjson => {
            chunk.push_str(&line);
            chunk.push('\n');
        }
        ExportFormat::Json => {
            if !*first {
                chunk.push(',');
            }
            *first = false;
            chunk.push_str(&line);
        }
        ExportFormat::Csv => unreachable!("csv rows go through csv_row, never json serialization"),
    }
    false
}

/// Whether the export's closing JSON-array terminator (`]`) may be emitted.
/// Only `true` for `json` when every page streamed successfully — a
/// mid-stream store error must never be masked by a valid-looking closing
/// bracket, which is exactly what made a truncated export indistinguishable
/// from a complete one (200 OK, parseable JSON, silently missing the tail).
///
/// `export_terminator_not_emitted_after_mid_stream_abort` pins this.
fn export_may_emit_terminator(format: ExportFormat, aborted: bool) -> bool {
    matches!(format, ExportFormat::Json) && !aborted
}

/// Streams the whole dataset in keyset-paged batches — constant memory
/// regardless of dataset size, with no row cap. `json` frames the batches as
/// one array (`[`, comma-separated records, `]`); `ndjson` and `csv` stream
/// line-oriented output. A mid-stream store read failure yields a stream
/// `Err`, which axum/hyper surface as an aborted response (the connection
/// closes without the chunked-encoding terminator) instead of a clean 200 —
/// so a truncated export is detectable as a transfer failure, never a
/// valid-looking short body.
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
            ExportFormat::Csv => yield Ok::<_, std::io::Error>(axum::body::Bytes::from_static(
                b"key,first_seen,last_seen,updated_at,removed_at,data\n",
            )),
            ExportFormat::Json => yield Ok(axum::body::Bytes::from_static(b"[")),
            ExportFormat::Ndjson => {}
        }
        let trust = trust_filter(&trust).map(str::to_string);
        let mut after: Option<(String, String)> = None;
        let mut first = true;
        let mut row_failures: usize = 0;
        let mut aborted = false;
        loop {
            let outcome = match state
                .datasets
                .list_records_view(&app, &dataset, &filters, after.clone(), BATCH, trust.as_deref(), include_removed)
                .await
            {
                Ok(batch) => ExportOutcome::Batch(batch),
                Err(e) => ExportOutcome::Failed(e),
            };
            let batch = match outcome {
                ExportOutcome::Batch(batch) => batch,
                ExportOutcome::Failed(e) => {
                    aborted = true;
                    // error, not warn: this is a truncated export in flight, not a
                    // recoverable condition — the response is about to end without
                    // its closing terminator specifically so the truncation is
                    // detectable, and that fact belongs at error severity.
                    tracing::error!(app = %app, dataset = %dataset, "export stream aborted mid-read, response truncated without a clean end: {e}");
                    yield Err(std::io::Error::other(e.to_string()));
                    break;
                }
            };
            let Some(last) = batch.last() else { break };
            after = Some((pumper_core::datasets::ts(last.updated_at), last.key.clone()));
            let short = (batch.len() as i64) < BATCH;
            let (chunk, failed) = format_batch(format, &batch, &mut first);
            row_failures += failed;
            yield Ok(axum::body::Bytes::from(chunk));
            if short {
                break;
            }
        }
        if row_failures > 0 {
            tracing::error!(app = %app, dataset = %dataset, row_failures, "export completed but {row_failures} record(s) failed to serialize and were skipped");
        }
        if export_may_emit_terminator(format, aborted) {
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
    responses(
        (status = 200, description = "Dual-mode: `{app, dataset, count, changes}` (clamped 1000), or `{items, next_cursor}` when `cursor` is present (pages the full feed)."),
        (status = 400, description = "Malformed `since` (not RFC 3339) or `cursor` (not the `<created_at>|<rowid>` token from `next_cursor`). A blank `cursor=` is valid and starts at the first page.", body = Object),
    )
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
    let after = parse_cursor_i64_arg(cursor)?;
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
    responses(
        (status = 200, description = "Dual-mode: `{app, dataset, key, count, revisions}` (clamped 500), or `{items, next_cursor}` when `cursor` is present."),
        (status = 400, description = "Malformed `cursor` (not the `<created_at>|<revision>` token from `next_cursor`). A blank `cursor=` is valid and starts at the first page.", body = Object),
    )
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
    let after = parse_cursor_i64_arg(cursor)?;
    let page = state
        .datasets
        .history_page(&app, &dataset, &query.key, after, query.limit.clamp(1, 500))
        .await?;
    Ok(Json(
        json!({ "items": page.items, "next_cursor": page.next_cursor }),
    ))
}

#[cfg(test)]
mod cursor_arg_tests {
    use super::*;

    /// The anti-pattern: `/changes` and `/history` used to decode a corrupt
    /// cursor to `None` — indistinguishable from "no cursor" — and answer 200
    /// with page one. A mirror walking the feed then restarted at the newest
    /// revision every run, re-deduped everything it had already applied, and
    /// reported `capped:true, new:0` forever with no error to look at.
    #[test]
    fn bad_cursor_400_not_page_one() {
        for bad in [
            "garbage",
            "no-separator",
            "2026-07-26T00:00:00.000000Z|abc",
            "|",
        ] {
            let err = parse_cursor_i64_arg(bad).err().unwrap_or_else(|| {
                panic!("{bad:?} must be rejected, not silently paged from the top")
            });
            assert_eq!(err.0, StatusCode::BAD_REQUEST, "for {bad:?}");
            assert!(
                err.1.contains("next_cursor"),
                "names the format for {bad:?}"
            );
        }
    }

    /// A non-numeric tiebreak is as malformed as a missing separator — both
    /// routes' tiebreaks (feed rowid, history revision) are integers, and
    /// `t|abc` used to fall through `.ok()` to page one just like `garbage`.
    #[test]
    fn numeric_tiebreak_required_but_a_real_cursor_round_trips() {
        assert_eq!(parse_cursor_i64_arg("").unwrap(), None);
        assert_eq!(
            parse_cursor_i64_arg("2026-07-26T00:00:00.000000Z|41").unwrap(),
            Some(("2026-07-26T00:00:00.000000Z".into(), 41))
        );
        // Negative rowids never occur, but they parse — the guard is on SHAPE,
        // not on plausibility, so it can never reject a cursor the API issued.
        assert_eq!(
            parse_cursor_i64_arg("2026-07-26T00:00:00.000000Z|-1").unwrap(),
            Some(("2026-07-26T00:00:00.000000Z".into(), -1))
        );
    }
}

#[cfg(test)]
mod export_honesty_tests {
    use super::*;

    fn record(key: &str) -> pumper_core::Record {
        let now = chrono::Utc::now();
        pumper_core::Record {
            key: key.to_string(),
            data: json!({ "v": 1 }),
            first_seen: now,
            last_seen: now,
            updated_at: now,
            removed_at: None,
            trust: "stable".to_string(),
        }
    }

    /// A serde_json::Error the tests can hand `append_row` without needing a
    /// `Record` whose `Value` genuinely fails to serialize (none does).
    fn a_serde_json_error() -> serde_json::Error {
        serde_json::from_str::<i32>("not a number").unwrap_err()
    }

    #[test]
    fn append_row_success_appends_the_line_for_ndjson_and_json() {
        let mut chunk = String::new();
        let mut first = true;
        let failed = append_row(
            &mut chunk,
            &mut first,
            ExportFormat::Ndjson,
            Ok(r#"{"key":"a"}"#.to_string()),
        );
        assert!(!failed);
        assert_eq!(chunk, "{\"key\":\"a\"}\n");

        let mut chunk = String::new();
        let mut first = true;
        append_row(
            &mut chunk,
            &mut first,
            ExportFormat::Json,
            Ok(r#"{"key":"a"}"#.to_string()),
        );
        append_row(
            &mut chunk,
            &mut first,
            ExportFormat::Json,
            Ok(r#"{"key":"b"}"#.to_string()),
        );
        assert_eq!(
            chunk, r#"{"key":"a"},{"key":"b"}"#,
            "json rows comma-joined"
        );
    }

    /// The anti-pattern this defends: a row that fails to serialize used to be
    /// silently skipped (`if let Ok(line) = ... { .. }` with no else), so an
    /// export could under-count its own rows with no signal anywhere. A failed
    /// row must be reported back to the caller, not swallowed.
    #[test]
    fn append_row_serialization_failure_is_counted_not_silently_dropped() {
        let mut chunk = String::new();
        let mut first = true;
        let failed = append_row(
            &mut chunk,
            &mut first,
            ExportFormat::Ndjson,
            Err(a_serde_json_error()),
        );
        assert!(failed, "a failed serialization must be reported, not eaten");
        assert!(
            chunk.is_empty(),
            "no partial/garbage bytes for a failed row"
        );
    }

    #[test]
    fn format_batch_counts_zero_failures_for_ordinary_records() {
        let batch = vec![record("a"), record("b"), record("c")];
        let mut first = true;
        let (chunk, failed) = format_batch(ExportFormat::Ndjson, &batch, &mut first);
        assert_eq!(failed, 0);
        assert_eq!(chunk.lines().count(), 3);
    }

    /// The anti-pattern this defends: the export streamer used to yield the
    /// json array's closing `]` unconditionally after the batch loop, even
    /// when the loop `break`-ed out because a mid-stream store read failed —
    /// producing a 200 OK with syntactically valid-but-truncated JSON,
    /// indistinguishable from a genuinely complete (and possibly just short)
    /// export.
    #[test]
    fn export_terminator_not_emitted_after_mid_stream_abort() {
        assert!(
            !export_may_emit_terminator(ExportFormat::Json, true),
            "an aborted json export must not get the closing ']' — that is what \
             made a truncated body look complete"
        );
        assert!(
            export_may_emit_terminator(ExportFormat::Json, false),
            "a json export that read every page cleanly still needs its ']'"
        );
        // ndjson/csv have no array terminator to begin with — the function must
        // say so regardless of the abort flag, not accidentally start emitting
        // one for a format that never had one.
        assert!(!export_may_emit_terminator(ExportFormat::Ndjson, false));
        assert!(!export_may_emit_terminator(ExportFormat::Csv, false));
    }
}

/// Pins the wire shape `clients/typescript` (`@pumper/sync`) is built against.
/// The fixtures under `clients/typescript/test/fixtures/*.json` are the one
/// shared contract: this module asserts the server's *actual* serialization of
/// `Record`/`Revision` covers every field a fixture has, so a Rust-side field
/// rename/removal breaks here; `clients/typescript/test/conformance.test.ts`
/// asserts the SDK's parsers accept the same fixtures, so a TypeScript-side
/// regression breaks there. Neither half proves the two are wired together
/// over real HTTP — that needs a live-server run — but a shape drift between
/// them (the actual regression class this SDK went through: `removed=`
/// gaining a default, `trust=` gaining teeth on `/export`) cannot land on one
/// side without failing its half of this pin.
#[cfg(test)]
mod sdk_fixture_conformance_tests {
    use chrono::{DateTime, Utc};
    use pumper_core::datasets::{Provenance, Revision};
    use serde_json::Value;

    // Paths are relative to this file's directory (crates/server/src/routes/).
    const RECORD_FIXTURE: &str =
        include_str!("../../../../clients/typescript/test/fixtures/record.json");
    const RECORD_REMOVED_FIXTURE: &str =
        include_str!("../../../../clients/typescript/test/fixtures/record-removed.json");
    const REVISION_PAGE_FIXTURE: &str =
        include_str!("../../../../clients/typescript/test/fixtures/revision-page.json");

    fn parse_ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Every key the fixture (== what the SDK's `PumperRecord`/`PumperRevision`
    /// types declare) expects must be present on the server's actual
    /// serialization — this is the direction that matters: an SDK field the
    /// server no longer emits is a silent `undefined` on the consumer side,
    /// which is exactly the failure mode ("silently mirrors nothing") the
    /// restoration brief called out for the old `records` field.
    fn assert_covers(fixture: &Value, actual: &Value, what: &str) {
        let (Value::Object(f), Value::Object(a)) = (fixture, actual) else {
            panic!("{what}: fixture and actual must both be JSON objects");
        };
        for key in f.keys() {
            assert!(
                a.contains_key(key),
                "{what}: server no longer serializes field '{key}' that the SDK fixture (and its \
                 TypeScript type) expects — this is the drift class that made the old SDK read a \
                 field the API stopped returning"
            );
        }
    }

    #[test]
    fn record_fixture_fields_are_a_subset_of_the_actual_record_shape() {
        let fixture: Value = serde_json::from_str(RECORD_FIXTURE).unwrap();
        let now = Utc::now();
        let record = pumper_core::Record {
            key: "k".into(),
            data: serde_json::json!({"x": 1}),
            first_seen: now,
            last_seen: now,
            updated_at: now,
            removed_at: None,
            trust: "stable".into(),
        };
        let actual = serde_json::to_value(&record).unwrap();
        assert_covers(&fixture, &actual, "live record");
        assert_eq!(
            fixture["removed_at"],
            Value::Null,
            "live fixture must model removed_at: null"
        );
    }

    #[test]
    fn removed_record_fixture_models_a_non_null_removed_at() {
        let fixture: Value = serde_json::from_str(RECORD_REMOVED_FIXTURE).unwrap();
        assert_ne!(
            fixture["removed_at"],
            Value::Null,
            "the tombstone fixture must exercise removed_at: Some(_), the shape \
             PumperClient.exportRecords relies on to detect a removal during a snapshot"
        );
        // parses as a real timestamp, not a placeholder string
        parse_ts(fixture["removed_at"].as_str().unwrap());
    }

    #[test]
    fn revision_page_fixture_fields_are_a_subset_of_the_actual_revision_shape() {
        let fixture: Value = serde_json::from_str(REVISION_PAGE_FIXTURE).unwrap();
        let items = fixture["items"].as_array().unwrap();
        assert_eq!(
            items.len(),
            2,
            "fixture must cover both a data-carrying and a removed revision"
        );

        let now = Utc::now();
        let changed = Revision {
            app: "a".into(),
            dataset: "d".into(),
            key: "k".into(),
            revision: 1,
            change: "changed".into(),
            data: Some(serde_json::json!({"x": 1})),
            diff: Some(serde_json::json!({"$.x": {"from": 0, "to": 1}})),
            created_at: now,
            trust: "stable".into(),
            // A FULLY populated stamp, because `Provenance` is `#[serde(flatten)]`
            // with no `skip_serializing_if`: every field is emitted (as `null`
            // when unknown), so the four provenance keys are part of the wire
            // shape whether or not a producer knows them.
            provenance: Provenance {
                job_id: Some("job-uuid".into()),
                source_url: Some("https://origin.example/x".into()),
                artifact_sha: Some("sha".into()),
                rules_hash: Some("rules".into()),
            },
        };
        let removed = Revision {
            change: "removed".into(),
            data: None,
            diff: None,
            ..changed.clone()
        };

        let actual_changed = serde_json::to_value(&changed).unwrap();
        let actual_removed = serde_json::to_value(&removed).unwrap();

        let fixture_changed = items.iter().find(|r| r["change"] == "changed").unwrap();
        let fixture_removed = items.iter().find(|r| r["change"] == "removed").unwrap();

        assert_covers(fixture_changed, &actual_changed, "'changed' revision");
        assert_covers(fixture_removed, &actual_removed, "'removed' revision");

        // The lifecycle invariant `sync.ts` depends on: a 'removed' revision
        // never carries a post-image, so the SDK never dereferences `.data` on
        // a tombstone.
        assert_eq!(fixture_removed["data"], Value::Null);
        assert_ne!(fixture_changed["data"], Value::Null);
    }

    /// The four provenance fields, pinned by NAME on both wire shapes.
    ///
    /// **Consumer: `app_peer::mirror_provenance`.** The `peer` app reads
    /// `source_url`, `rules_hash` and `artifact_sha` straight off these feed
    /// items to stamp each mirrored record (carrying the origin's derivation
    /// through, and deliberately dropping the sha). A rename or removal on the
    /// wire silently breaks every mirror's provenance — mirrored records would
    /// go back to claiming unknown origins — and until these fields were in the
    /// fixture, nothing failed: `assert_covers` is one-way (fixture ⊆ actual),
    /// so fields the fixture omitted were unpinned in BOTH directions. Do not
    /// prune them as unused.
    #[test]
    fn provenance_fields_are_pinned_on_the_wire_for_the_peer_app() {
        let fixture: Value = serde_json::from_str(REVISION_PAGE_FIXTURE).unwrap();
        let items = fixture["items"].as_array().unwrap();
        const PROVENANCE: [&str; 4] = ["job_id", "source_url", "artifact_sha", "rules_hash"];

        for item in items {
            for field in PROVENANCE {
                assert!(
                    item.get(field).is_some(),
                    "revision fixture is missing '{field}': the peer app's \
                     mirror_provenance reads it off exactly this shape"
                );
            }
        }

        // `Provenance` is flattened with no `skip_serializing_if`, so a revision
        // that knows nothing still emits all four as `null` — "unknown", never
        // absent. One fixture item must model each side of that.
        let known = items
            .iter()
            .find(|r| r["change"] == "changed")
            .expect("a data-carrying revision");
        assert_eq!(
            known["artifact_sha"].as_str().map(str::len),
            Some(64),
            "one item must carry a real artifact_sha — the field the mirror \
             deliberately does NOT copy, so its presence upstream is load-bearing"
        );
        assert!(known["source_url"].is_string() && known["rules_hash"].is_string());
        let unknown = items
            .iter()
            .find(|r| r["change"] == "removed")
            .expect("a tombstone revision");
        assert_eq!(
            unknown["source_url"],
            Value::Null,
            "and one must model honest-Null provenance, which the mirror keeps as \
             unknown rather than inventing the feed URL"
        );
    }
}
