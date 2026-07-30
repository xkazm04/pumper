//! Curated query surfaces layered over the generic dataset store: the
//! cross-source grants corpus (filtered list + closing-soon view), the
//! data-source catalog (sources + freshness health), and DataHub status/sync.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use crate::routes::error::{default_limit, keyset_cursor, parse_cursor, ApiError};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Grants query surface
//
// `grants/unified` is the cross-source corpus that grants-gov, ca-grants, and
// eu-sedia all normalize into (see the `grants-common` crate, which owns these
// two names).
// Until now it was reachable only through the generic dataset API, so every
// consumer had to export the whole corpus and filter client-side. These two
// routes push the filters into SQL.
// ---------------------------------------------------------------------------

/// Virtual app namespace holding the cross-source grants datasets. Mirrors
/// `grants_common::{UNIFIED_APP, UNIFIED_DATASET}`; duplicated as literals rather
/// than taking a server dependency on a library crate for two strings.
const GRANTS_APP: &str = "grants";
const GRANTS_DATASET: &str = "unified";

/// Upper bound on `GET /grants?limit=`. The default is `default_limit` (50).
const GRANTS_MAX_LIMIT: i64 = 500;

/// Default closing-soon window, in days, matching the grants-gov digest.
const CLOSING_SOON_DEFAULT_DAYS: i64 = 14;
/// Rows the closing-soon view returns, ordered soonest-first in SQL. `count`
/// reports the full window size independently, so the cap is not a silent
/// truncation of the total.
const CLOSING_SOON_CAP: usize = 200;

/// Filters over `grants/unified`. All optional, all ANDed; with none set the
/// route lists the whole live corpus.
#[derive(Deserialize, IntoParams)]
pub(crate) struct GrantsQuery {
    /// Normalized status, exact match: `open` | `forecasted` | `closed`.
    status: Option<String>,
    /// Case-insensitive substring of the agency name (e.g. `health`).
    agency: Option<String>,
    /// Source app, exact match: `grants-gov` | `ca-grants` | `eu-sedia`.
    source: Option<String>,
    /// Closes on or before this `YYYY-MM-DD`. Records with no close date are excluded.
    closing_before: Option<String>,
    /// Closes on or after this `YYYY-MM-DD`. Records with no close date are excluded.
    closing_after: Option<String>,
    /// Minimum money: keeps records whose `award_ceiling` OR `total_funding` is >= this.
    min_award: Option<f64>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

/// A blank query param (`?status=`) means "unset", not "match the empty string" —
/// otherwise a UI that always serializes its filter form would match nothing.
fn filter_value(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

/// Grant dates are canonical `YYYY-MM-DD`, which sorts lexicographically — that is
/// what lets the closing-window filters compare as text. Reject anything else
/// rather than silently comparing a malformed string.
fn parse_grant_date(value: &str, field: &str) -> Result<chrono::NaiveDate, ApiError> {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("'{field}' must be a YYYY-MM-DD date, got '{value}'"),
        )
    })
}

/// Translates the query params into store-level JSON predicates.
fn grant_filters(query: &GrantsQuery) -> Result<Vec<pumper_core::datasets::JsonFilter>, ApiError> {
    use pumper_core::datasets::JsonFilter;
    let mut filters = Vec::new();
    if let Some(status) = filter_value(&query.status) {
        filters.push(JsonFilter::Eq {
            path: "$.status".into(),
            value: status.into(),
        });
    }
    if let Some(source) = filter_value(&query.source) {
        filters.push(JsonFilter::Eq {
            path: "$.source".into(),
            value: source.into(),
        });
    }
    if let Some(agency) = filter_value(&query.agency) {
        filters.push(JsonFilter::Contains {
            path: "$.agency".into(),
            value: agency.into(),
        });
    }
    if let Some(before) = filter_value(&query.closing_before) {
        parse_grant_date(before, "closing_before")?;
        filters.push(JsonFilter::Lte {
            path: "$.close_date".into(),
            value: before.into(),
        });
    }
    if let Some(after) = filter_value(&query.closing_after) {
        parse_grant_date(after, "closing_after")?;
        filters.push(JsonFilter::Gte {
            path: "$.close_date".into(),
            value: after.into(),
        });
    }
    // A grant's "size" is reported inconsistently across sources: some publish a
    // per-award ceiling, some only a program total. Matching either keeps a
    // funder's largest number in play instead of demanding one specific field.
    if let Some(min) = query.min_award {
        filters.push(JsonFilter::NumGteAny {
            paths: vec!["$.award_ceiling".into(), "$.total_funding".into()],
            value: min,
        });
    }
    Ok(filters)
}

#[utoipa::path(
    get,
    path = "/grants",
    tag = "grants",
    params(GrantsQuery),
    responses(
        (status = 200, description = "Live records from `grants/unified` matching every filter, newest-updated first. Dual-mode: `{grants: [Record]}`, or `{items, next_cursor}` when `cursor` is present (even empty)."),
        (status = 400, description = "Malformed `closing_before` / `closing_after` date", body = Object),
    )
)]
pub(crate) async fn list_grants(
    State(state): State<AppState>,
    Query(query): Query<GrantsQuery>,
) -> Result<Json<Value>, ApiError> {
    let filters = grant_filters(&query)?;
    let limit = query.limit.clamp(1, GRANTS_MAX_LIMIT);
    let Some(cursor) = &query.cursor else {
        let grants = state
            .datasets
            .list_filtered(GRANTS_APP, GRANTS_DATASET, &filters, None, limit)
            .await?;
        return Ok(Json(json!({ "grants": grants })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .datasets
        .list_filtered(GRANTS_APP, GRANTS_DATASET, &filters, after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |r| {
        format!("{}|{}", pumper_core::datasets::ts(r.updated_at), r.key)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct ClosingSoonQuery {
    /// Window size in days from today. Default 14, clamped to 1..=365.
    days: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/grants/closing-soon",
    tag = "grants",
    params(ClosingSoonQuery),
    responses((status = 200, description = "`{days, count, grants}` — live open grants closing within the window, soonest first. Each grant is its unified record `data` plus `key` and `days_left`. `count` is the window total; `grants` is capped at 200."))
)]
pub(crate) async fn closing_soon(
    State(state): State<AppState>,
    Query(query): Query<ClosingSoonQuery>,
) -> Result<Json<Value>, ApiError> {
    use pumper_core::datasets::JsonFilter;
    let days = query
        .days
        .unwrap_or(CLOSING_SOON_DEFAULT_DAYS)
        .clamp(1, 365);
    let today = chrono::Utc::now().date_naive();
    let until = today + chrono::Duration::days(days);

    // Computed on read rather than materialized as a dataset: a read view can
    // never go stale between syncs — which a "closing soon" list, whose membership
    // changes with the calendar and not with the data, absolutely would if it were
    // snapshotted.
    let filters = vec![
        JsonFilter::Eq {
            path: "$.status".into(),
            value: "open".into(),
        },
        JsonFilter::Gte {
            path: "$.close_date".into(),
            value: today.to_string(),
        },
        JsonFilter::Lte {
            path: "$.close_date".into(),
            value: until.to_string(),
        },
    ];
    // Order by close_date ASC and cap in SQL, so the returned rows are genuinely
    // the soonest-closing across the whole corpus — not an arbitrary
    // most-recently-updated slice that an in-memory sort would only reorder. The
    // true window total comes from a separate COUNT, so `count` reflects every
    // matching grant rather than saturating at the return cap.
    let count = state
        .datasets
        .count_filtered(GRANTS_APP, GRANTS_DATASET, &filters)
        .await?;
    let records = state
        .datasets
        .list_filtered_ordered(
            GRANTS_APP,
            GRANTS_DATASET,
            &filters,
            "$.close_date",
            CLOSING_SOON_CAP as i64,
        )
        .await?;

    // SQL already returns these soonest-first; just attach key + days_left.
    let grants: Vec<Value> = records
        .into_iter()
        .filter_map(|r| {
            let close = r.data.get("close_date").and_then(Value::as_str)?;
            let close = chrono::NaiveDate::parse_from_str(close, "%Y-%m-%d").ok()?;
            let days_left = (close - today).num_days();
            let mut grant = r.data.as_object()?.clone();
            grant.insert("key".into(), json!(r.key));
            grant.insert("days_left".into(), json!(days_left));
            Some(Value::Object(grant))
        })
        .collect();
    Ok(Json(
        json!({ "days": days, "count": count, "grants": grants }),
    ))
}

// ---- Data-source catalog --------------------------------------------------

#[derive(Deserialize, IntoParams)]
pub(crate) struct CatalogQuery {
    /// Filter to one jurisdiction id (e.g. `us`, `eu`, `cz`).
    market: Option<String>,
    /// Filter to one status (`live` | `planned` | `blocked`).
    status: Option<String>,
    /// Filter to one category (e.g. `open-calls`, `labor-market`).
    category: Option<String>,
}

/// The data-source catalog: the machine-readable list of every pipeline this
/// service scrapes (`catalog/data-sources.toml`), so a downstream app can query
/// "which markets are launch-grade" instead of scraping a TOML out of a sibling
/// repo. A server-crate test cross-checks it against the live registry, so a
/// `live` entry can't drift from what the app actually schedules.
#[utoipa::path(
    get,
    path = "/catalog/sources",
    tag = "catalog",
    params(CatalogQuery),
    responses(
        (status = 200, description = "`{count, sources: [Source]}` — data pipelines, optionally filtered by `market` / `status` / `category`."),
        (status = 500, description = "Catalog file malformed", body = Object),
    )
)]
pub(crate) async fn catalog_sources(
    Query(query): Query<CatalogQuery>,
) -> Result<Json<Value>, ApiError> {
    let catalog = pumper_core::Catalog::load().map_err(|e| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("catalog load: {e}"),
        )
    })?;
    let want = |field: &str, filter: &Option<String>| -> bool {
        filter
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .is_none_or(|f| f == field)
    };
    let sources: Vec<&pumper_core::Source> = catalog
        .sources
        .iter()
        .filter(|s| {
            want(&s.market, &query.market)
                && want(&s.status, &query.status)
                && want(&s.category, &query.category)
        })
        .collect();
    Ok(Json(json!({ "count": sources.len(), "sources": sources })))
}

/// Grace multiplier on a source's cadence window before it is flagged stale —
/// tolerates one missed run (e.g. a daily source is stale only past ~2 days).
const CATALOG_STALE_GRACE: i64 = 2;

/// Freshness monitor for the catalog: for every **live** source that declares a
/// `dataset` and a cadence with a freshness expectation, report when its dataset
/// was last written and whether that exceeds the cadence window (× a grace
/// multiplier). Turns the catalog's `status`/`confidence`/`cadence` from
/// aspirational documentation into a self-checking signal — the one thing
/// ("how fresh") the catalog couldn't answer about itself.
#[utoipa::path(
    get,
    path = "/catalog/health",
    tag = "catalog",
    responses((status = 200, description = "`{checked, stale, contracts_enforce, sources: [{id, app, dataset, cadence, expected_max_age_secs, last_write_at, age_secs, stale, monitored, reason?, contract?}]}` — per-source freshness for live sources; `monitored:false` when no dataset or no freshness window. `expected_max_age_secs` is the stale threshold (cadence × grace, tightened by a declared contract's `max_staleness_hours`). `contract` appears on sources declaring a `[source.contract]` block: `{declared, enforce, last_verdict}` where `last_verdict` is the worker's most recent publish-time evaluation (`{verdict: pass|warn|block, violations, ...}`, null before the first run since boot)."))
)]
pub(crate) async fn catalog_health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let catalog = pumper_core::Catalog::load().map_err(|e| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("catalog load: {e}"),
        )
    })?;
    let now = chrono::Utc::now();
    let mut out = Vec::new();
    let mut stale_count = 0usize;
    for s in catalog.live() {
        let base = json!({
            "id": s.id, "app": s.app, "dataset": s.dataset, "cadence": s.cadence,
        });
        let mut row = base.as_object().unwrap().clone();
        // Declared data contract (M20): declaration + the latest publish-time
        // verdict recorded by the worker (in-memory; null until the first run
        // after boot).
        if let Some(contract) = &s.contract {
            let latest = state
                .contract_verdicts
                .lock()
                .expect("contract verdict lock")
                .get(&format!("{}/{}", s.app, s.dataset))
                .cloned();
            row.insert(
                "contract".into(),
                json!({
                    "declared": contract,
                    "enforce": state.config.contracts.enforce,
                    "last_verdict": latest.unwrap_or(Value::Null),
                }),
            );
        }
        // Not monitorable: no dataset/app, or no freshness window from either
        // the cadence or a declared contract. A contract's `max_staleness_hours`
        // tightens (never loosens) the cadence-derived window, and supplies one
        // when the cadence has none.
        let cadence_window = s.cadence_secs().map(|secs| secs * CATALOG_STALE_GRACE);
        let contract_window = s
            .contract
            .as_ref()
            .and_then(|c| c.max_staleness_hours)
            .map(|h| h * 3600);
        let expected = match (cadence_window, contract_window) {
            (Some(c), Some(k)) => Some(c.min(k)),
            (w, k) => w.or(k),
        };
        if s.dataset.is_empty() || s.app.is_empty() || expected.is_none() {
            row.insert("monitored".into(), json!(false));
            row.insert(
                "reason".into(),
                json!(if expected.is_none() {
                    "cadence has no freshness expectation"
                } else {
                    "no app/dataset to check"
                }),
            );
            out.push(Value::Object(row));
            continue;
        }
        let expected = expected.unwrap();
        // Newest write in this source's dataset (list is updated_at DESC).
        let last = state
            .datasets
            .list(&s.app, &s.dataset, 1)
            .await?
            .first()
            .map(|r| r.updated_at);
        row.insert("monitored".into(), json!(true));
        row.insert("expected_max_age_secs".into(), json!(expected));
        match last {
            Some(ts) => {
                let age = (now - ts).num_seconds().max(0);
                let stale = age > expected;
                if stale {
                    stale_count += 1;
                }
                row.insert("last_write_at".into(), json!(pumper_core::datasets::ts(ts)));
                row.insert("age_secs".into(), json!(age));
                row.insert("stale".into(), json!(stale));
            }
            None => {
                // Live source that has never written its dataset — stale by definition.
                stale_count += 1;
                row.insert("last_write_at".into(), Value::Null);
                row.insert("age_secs".into(), Value::Null);
                row.insert("stale".into(), json!(true));
                row.insert("reason".into(), json!("dataset has never been written"));
            }
        }
        out.push(Value::Object(row));
    }
    Ok(Json(json!({
        "checked": out.len(),
        "stale": stale_count,
        "contracts_enforce": state.config.contracts.enforce,
        "sources": out,
        // The two halves of source liveness: this answers "did it run recently",
        // `/sources` answers "was what it produced right". Neither subsumes the
        // other, so each points at the other.
        "see_also": "/sources — extraction health (was the output right?)",
    })))
}

// ---- Catalog GitOps reconciler (M19) --------------------------------------

/// Guardrail on unforced applies: a plan disabling more schedules than this is
/// probably a bad TOML edit (mass status-flip), so `POST /catalog/reconcile`
/// refuses it unless `?force=true`. Creates/updates are additive and carry no
/// such blast radius.
const MAX_UNFORCED_DISABLES: usize = 3;

/// The catalog as control plane, read side: diff `catalog/data-sources.toml`
/// (desired state) against the live schedules table (actual state). Pure
/// dry-run — never writes. Hand-made and code-seeded schedules (no
/// `managed_by` tag) are only ever *read*: an exact app+cron match counts as
/// coverage, anything else is left alone.
#[utoipa::path(
    get,
    path = "/catalog/reconcile",
    tag = "catalog",
    responses(
        (status = 200, description = "`{empty, create, update, disable, orphan, covered_by_untagged, in_sync, auto_reconcile}` — the reconciliation plan. `orphan` is report-only (never applied)."),
        (status = 500, description = "Catalog file malformed", body = Object),
    )
)]
pub(crate) async fn catalog_reconcile(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let plan = crate::scheduler::catalog_reconcile_plan(&state)
        .await
        .map_err(|e| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("catalog reconcile: {e}"),
            )
        })?;
    let mut body = serde_json::to_value(&plan).expect("plan serializes");
    let obj = body.as_object_mut().expect("plan is an object");
    obj.insert("empty".into(), json!(plan.is_empty()));
    obj.insert(
        "auto_reconcile".into(),
        json!(state.config.catalog.auto_reconcile),
    );
    Ok(Json(body))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct ReconcileApplyQuery {
    /// Required when the plan disables more than 3 schedules — a blast-radius
    /// guard against a bad TOML edit mass-disabling pipelines.
    force: Option<bool>,
}

/// Applies the current reconcile plan: creates missing catalog-managed
/// schedules, corrects drifted crons, disables schedules for sources flipped
/// away from `live`. Every write is SQL-fenced on `managed_by = "catalog"` so
/// untagged (hand-made / code-seeded) schedules can never be touched; orphans
/// are reported but never applied. Idempotent — re-applying a clean state is a
/// no-op.
#[utoipa::path(
    post,
    path = "/catalog/reconcile",
    tag = "catalog",
    params(ReconcileApplyQuery),
    responses(
        (status = 200, description = "`{applied: {created, updated, disabled, orphans_untouched, errors}, plan}` — what was done, plus the plan it executed."),
        (status = 409, description = "Plan disables too many schedules; retry with `?force=true`", body = Object),
        (status = 500, description = "Catalog file malformed", body = Object),
    )
)]
pub(crate) async fn catalog_reconcile_apply(
    State(state): State<AppState>,
    Query(query): Query<ReconcileApplyQuery>,
) -> Result<Json<Value>, ApiError> {
    let plan = crate::scheduler::catalog_reconcile_plan(&state)
        .await
        .map_err(|e| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("catalog reconcile: {e}"),
            )
        })?;
    if plan.disable.len() > MAX_UNFORCED_DISABLES && !query.force.unwrap_or(false) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!(
                "plan disables {} schedules (> {MAX_UNFORCED_DISABLES}) — likely a bad catalog \
                 edit; review GET /catalog/reconcile and re-POST with ?force=true to proceed",
                plan.disable.len()
            ),
        ));
    }
    let applied = crate::scheduler::apply_reconcile_plan(&state, &plan).await;
    Ok(Json(json!({ "applied": applied, "plan": plan })))
}

/// DataHub emitter configuration and the most recent emission outcome.
#[utoipa::path(
    get,
    path = "/datahub/status",
    tag = "datahub",
    responses((status = 200, description = "`{enabled, gms_url, env, token_set, emit_schema, emit_profile, last_emission}` — last_emission is `{kind: job|sync, at, ok, entities?|error?}` or null before any emission."))
)]
pub(crate) async fn datahub_status(State(state): State<AppState>) -> Json<Value> {
    Json(crate::datahub::status(&state))
}

/// One-shot metadata backfill: pushes every stored dataset (entity, properties,
/// and per-config profile/schema) to the configured DataHub GMS. Run it once
/// after connecting a fresh instance; job completions keep it current after that.
#[utoipa::path(
    post,
    path = "/datahub/sync",
    tag = "datahub",
    responses(
        (status = 200, description = "`{kind: \"sync\", at, ok, datasets, entities?|error?}` — the emission summary (also on /datahub/status)"),
        (status = 409, description = "[datahub] is disabled in config"),
    )
)]
pub(crate) async fn datahub_sync(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    if !state.config.datahub.enabled {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "[datahub] is disabled — set enabled = true and gms_url in config".into(),
        ));
    }
    Ok(Json(crate::datahub::full_sync(&state).await))
}
