//! Runtime introspection and tooling: learned per-host tier memory, the session
//! profile vault, the WASM plugin host, and the declarative extraction preview.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use pumper_core::HostProfile;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::routes::error::{default_limit, error_code, keyset_cursor, parse_cursor, ApiError};
use crate::state::AppState;

// ---- Host profiles (learned tier memory + politeness) -----------------------

#[derive(Deserialize, IntoParams)]
pub(crate) struct HostsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

/// Serializes a stored profile with the **live** governor penalty merged in
/// (the row's `penalty_ms` is only the last write-behind snapshot; the
/// in-memory value is authoritative and fresher).
async fn host_json(state: &AppState, mut profile: HostProfile) -> Value {
    let live = state.governor.penalty(&profile.host).await;
    profile.penalty_ms = live.as_millis().min(i64::MAX as u128) as i64;
    json!(profile)
}

/// Paginated list of learned host state: preferred tier, HTTP strikes, live
/// politeness penalty, and last-outcome timestamps. Most-recently-active first.
#[utoipa::path(
    get,
    path = "/hosts",
    tag = "hosts",
    params(HostsQuery),
    responses((status = 200, description = "Dual-mode: `{hosts: [...]}` without `cursor=`, \
        `{items, next_cursor}` with it. Each host: `{host, preferred_tier, http_strikes, \
        penalty_ms (live), updated_at, penalty_updated_at}`"))
)]
pub(crate) async fn list_hosts(
    State(state): State<AppState>,
    Query(query): Query<HostsQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let after = query.cursor.as_deref().and_then(parse_cursor);
    let profiles = state.tiers.list_page(after, limit).await?;
    let next_cursor = keyset_cursor(&profiles, limit, |p| {
        format!("{}|{}", p.updated_at, p.host)
    });
    let mut items = Vec::with_capacity(profiles.len());
    for p in profiles {
        items.push(host_json(&state, p).await);
    }
    // Dual-mode, matching every other list endpoint: no cursor ⇒ legacy
    // `{hosts: [...]}` shape; cursor present ⇒ `{items, next_cursor}`.
    if query.cursor.is_none() {
        Ok(Json(json!({ "hosts": items })))
    } else {
        Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
    }
}

/// One host's learned profile. 404 when the host has no learned state (no tier
/// memory row and no live penalty).
#[utoipa::path(
    get,
    path = "/hosts/{host}",
    tag = "hosts",
    params(("host" = String, Path, description = "Hostname (case-insensitive)")),
    responses(
        (status = 200, description = "`{host, preferred_tier, http_strikes, penalty_ms (live), \
            updated_at, penalty_updated_at}`"),
        (status = 404, description = "No learned state for this host", body = Object),
    )
)]
pub(crate) async fn get_host(
    State(state): State<AppState>,
    Path(host): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let host = host.to_lowercase();
    if let Some(profile) = state.tiers.get(&host).await? {
        return Ok(Json(host_json(&state, profile).await));
    }
    // No stored row, but a live penalty may exist ahead of the next snapshot.
    let live = state.governor.penalty(&host).await;
    if !live.is_zero() {
        return Ok(Json(json!({
            "host": host,
            "preferred_tier": Value::Null,
            "http_strikes": 0,
            "penalty_ms": live.as_millis().min(i64::MAX as u128) as i64,
            "updated_at": Value::Null,
            "penalty_updated_at": Value::Null,
        })));
    }
    Err(ApiError(StatusCode::NOT_FOUND, "unknown host".into()))
}

/// Resets a host's learned state: drops its tier memory (strikes + browser pin +
/// persisted penalty) and clears the live governor penalty. 404 when unknown.
#[utoipa::path(
    delete,
    path = "/hosts/{host}/memory",
    tag = "hosts",
    params(("host" = String, Path, description = "Hostname (case-insensitive)")),
    responses(
        (status = 200, description = "`{host, reset: true}`"),
        (status = 404, description = "No learned state for this host", body = Object),
    )
)]
pub(crate) async fn delete_host_memory(
    State(state): State<AppState>,
    Path(host): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let host = host.to_lowercase();
    let forgot = state.tiers.forget(&host).await?;
    let cleared = state.governor.clear(&host);
    if forgot || cleared {
        Ok(Json(json!({ "host": host, "reset": true })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "unknown host".into()))
    }
}

// ---- Session profiles -----------------------------------------------------

/// One profile of the session vault (`[fetcher] profiles_dir`), as it exists on
/// disk. Profiles are created implicitly by the first fetch that names them —
/// there is no create/delete API in phase 1.
#[derive(serde::Serialize, ToSchema)]
struct ProfileInfo {
    /// Directory name — exactly the string a request's `profile` field takes.
    name: String,
    /// A persistent HTTP cookie jar exists (`cookies.json`).
    has_cookies: bool,
    /// A Chrome user-data-dir exists (`browser/`).
    has_browser_dir: bool,
    /// Most recent mtime across the profile dir, its jar, and its browser dir
    /// (RFC 3339). `None` when no mtime is readable.
    last_used: Option<String>,
}

/// Lists the profiles in the session vault — see [fetching.md]. Read-only
/// diagnostics: it reports what is on disk, it does not create anything.
#[utoipa::path(
    get,
    path = "/profiles",
    tag = "profiles",
    responses((
        status = 200,
        description = "`{profiles: [{name, has_cookies, has_browser_dir, last_used}]}`, \
                       alphabetical. Empty (not an error) when the vault dir does not exist yet.",
        body = Object,
    ))
)]
pub(crate) async fn list_profiles(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let root = state.config.fetcher.profiles_dir.clone();
    let mut entries = match tokio::fs::read_dir(&root).await {
        Ok(entries) => entries,
        // No vault dir yet simply means no profiles — it is created on the first
        // profiled fetch, so this is an empty list, not a failure.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Json(json!({ "profiles": [] })));
        }
        Err(e) => {
            return Err(ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("reading {}: {e}", root.display()),
            ))
        }
    };

    let mut profiles: Vec<ProfileInfo> = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(|e| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("reading {}: {e}", root.display()))
    })? {
        let Ok(name) = entry.file_name().into_string() else { continue };
        // Only directories whose names are valid profiles — anything else in the
        // vault dir isn't ours and can't be named by a request anyway.
        if pumper_core::validate_profile_name(&name).is_err() {
            continue;
        }
        if !entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let dir = entry.path();
        let cookies = dir.join(pumper_core::PROFILE_COOKIES_FILE);
        let browser = dir.join(pumper_core::PROFILE_BROWSER_DIR);
        let has_cookies = tokio::fs::metadata(&cookies).await.map(|m| m.is_file()).unwrap_or(false);
        let has_browser_dir =
            tokio::fs::metadata(&browser).await.map(|m| m.is_dir()).unwrap_or(false);
        // Last use ≈ the newest mtime among the profile dir and its artifacts:
        // the jar is rewritten after cookie-setting responses, and Chrome churns
        // its user-data-dir on every render.
        let mut newest: Option<std::time::SystemTime> = None;
        for path in [&dir, &cookies, &browser] {
            if let Ok(mtime) = tokio::fs::metadata(path).await.and_then(|m| m.modified()) {
                newest = Some(newest.map_or(mtime, |cur: std::time::SystemTime| cur.max(mtime)));
            }
        }
        let last_used = newest.map(|t| chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339());
        profiles.push(ProfileInfo { name, has_cookies, has_browser_dir, last_used });
    }
    profiles.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(json!({ "profiles": profiles })))
}

// ---- WASM plugins ---------------------------------------------------------

#[utoipa::path(
    get,
    path = "/plugins",
    tag = "plugins",
    responses((status = 200, description = "`{plugins: [{name, ...}]}` — each entry is a plugin's self-describing manifest (name/version/description/params_schema/output_schema) when it exports `describe`, else just `{name}`."))
)]
pub(crate) async fn list_plugins(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "plugins": state.plugins.manifests() }))
}

/// Hot-swap: rescan the plugin directory and reload every `.wasm` module.
#[utoipa::path(
    post,
    path = "/plugins/reload",
    tag = "plugins",
    responses((status = 200, description = "`{loaded: <count>}`"))
)]
pub(crate) async fn reload_plugins(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let loaded = state.plugins.reload().await?;
    Ok(Json(json!({ "loaded": loaded })))
}

// ---- Declarative extraction preview -----------------------------------------

/// Time budget for a preview `url` fetch. A preview must stay interactive, so a
/// slow origin is abandoned rather than blocking the request.
const PREVIEW_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Body budget for a preview `url` fetch. Past this the document is rejected
/// (413) instead of parsed — previews validate rules, they are not a bulk pull.
const PREVIEW_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

#[derive(Deserialize, ToSchema)]
pub(crate) struct PreviewBody {
    /// A `RuleSet`: a bare `{field: rule}` map (the same shape apps take), e.g.
    /// `{"title": {"type": "css", "selector": "h1"}}`.
    #[schema(value_type = Object)]
    rules: Value,
    /// Inline document to run the rules against. Provide exactly one of
    /// `html` or `url`.
    html: Option<String>,
    /// URL to fetch (HTTP tier only — no browser/Claude escalation) and run the
    /// rules against. Provide exactly one of `html` or `url`.
    url: Option<String>,
}

/// Compiles a `RuleSet` and runs it against one document without enqueuing a
/// job — the fast feedback loop for authoring selectors. Rules are compiled
/// field-by-field so every bad field is reported at once (not just the first);
/// the response pairs the extracted values with the per-field match report
/// (matched | empty | error), so a selector that silently matches nothing is
/// visible before a job fetches anything.
///
/// `url` mode fetches through the shared HTTP tier only (`FetchStrategy::Http`):
/// a preview never spends money on the Claude tier or waits on a browser render,
/// and is bounded by a modest time and body budget.
#[utoipa::path(
    post,
    path = "/extract/preview",
    tag = "extract",
    request_body = PreviewBody,
    responses(
        (status = 200, description = "`{values, report, fields_matched, fields_total}` — extracted values plus the report: `report.fields` is the per-field match status (`matched`|`empty`|`container_empty`|`error`) and `report.coercion` the post-transform outcome (`coerced`|`coercion_failed`|`no_transforms`) for fields with a transform chain."),
        (status = 400, description = "Bad request: not exactly one of html|url, non-object `rules`, non-http(s) url, fetch failure/timeout, or rule compile errors — the body then carries a `fields: [{field, error}]` list covering every bad field.", body = Object),
        (status = 413, description = "Fetched body over the preview size budget", body = Object),
    )
)]
pub(crate) async fn extract_preview(
    State(state): State<AppState>,
    Json(body): Json<PreviewBody>,
) -> Result<Response, ApiError> {
    // Exactly one document source.
    let doc = match (body.html, body.url) {
        (Some(_), Some(_)) => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "provide exactly one of 'html' or 'url', not both".into(),
            ))
        }
        (None, None) => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "provide exactly one of 'html' or 'url'".into(),
            ))
        }
        (Some(html), None) => html,
        (None, Some(url)) => fetch_preview_doc(&state, &url).await?,
    };

    // Compile field-by-field so ALL bad fields are reported, not just the first.
    // `rules` must be an object mapping field -> rule; each value is deserialized
    // into a `FieldRule` and then compiled on its own (as a single-field
    // `RuleSet`), collecting both deserialize and compile-time errors per field.
    let Value::Object(map) = body.rules else {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "'rules' must be a JSON object mapping field -> rule".into(),
        ));
    };
    let mut fields: std::collections::BTreeMap<String, pumper_core::FieldRule> =
        std::collections::BTreeMap::new();
    let mut errors: Vec<Value> = Vec::new();
    for (name, rule_val) in map {
        match serde_json::from_value::<pumper_core::FieldRule>(rule_val) {
            Ok(field_rule) => {
                let one = std::collections::BTreeMap::from([(name.clone(), field_rule.clone())]);
                match (pumper_core::RuleSet { fields: one }).compile() {
                    Ok(_) => {
                        fields.insert(name, field_rule);
                    }
                    Err(e) => errors.push(json!({ "field": name, "error": e.to_string() })),
                }
            }
            Err(e) => errors.push(json!({ "field": name, "error": e.to_string() })),
        }
    }
    if !errors.is_empty() {
        // Structured compile diagnostics: the same `{error, code}` envelope as
        // ApiError, plus a per-field list so every bad selector is fixed at once.
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "rule compilation failed",
                "code": error_code(StatusCode::BAD_REQUEST),
                "fields": errors,
            })),
        )
            .into_response());
    }

    // Every field compiled on its own, so the combined compile cannot fail.
    let compiled = (pumper_core::RuleSet { fields })
        .compile()
        .map_err(ApiError::from)?;
    let (values, report) = pumper_core::extract_one_with_report(&compiled, &doc);
    let fields_total = report.fields.len();
    let fields_matched = report
        .fields
        .values()
        .filter(|s| {
            matches!(
                s,
                pumper_core::FieldStatus::Matched | pumper_core::FieldStatus::ContainerEmpty
            )
        })
        .count();
    Ok(Json(json!({
        "values": values,
        "report": report,
        "fields_matched": fields_matched,
        "fields_total": fields_total,
    }))
    .into_response())
}

/// Fetches a preview document through the shared HTTP tier only, under a modest
/// time and size budget. No browser/Claude escalation — a preview must stay
/// cheap and never spend money.
async fn fetch_preview_doc(state: &AppState, url: &str) -> Result<String, ApiError> {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(ApiError(StatusCode::BAD_REQUEST, "'url' must be http(s)".into()));
    }
    let mut req = pumper_core::FetchRequest::new(url);
    req.strategy = pumper_core::FetchStrategy::Http;
    let outcome = tokio::time::timeout(PREVIEW_FETCH_TIMEOUT, state.engines.fetch.fetch(req))
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::BAD_REQUEST,
                format!("fetch exceeded the {}s preview budget", PREVIEW_FETCH_TIMEOUT.as_secs()),
            )
        })?
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("failed to fetch url: {e}")))?;
    let html = outcome.html.unwrap_or_default();
    if html.len() > PREVIEW_MAX_BODY_BYTES {
        return Err(ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "fetched body is {} bytes; the preview budget is {PREVIEW_MAX_BODY_BYTES} bytes",
                html.len()
            ),
        ));
    }
    Ok(html)
}
