//! Curated query surfaces layered over the generic dataset store: the
//! cross-source grants corpus (filtered list + closing-soon view), the
//! data-source catalog (sources + freshness health), and DataHub status/sync.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use crate::routes::datasets::{default_trust_all, trust_filter};
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
    /// Trust filter over the shared corpus: `all` (default — every record carries
    /// its own `trust` field), `stable`, `provisional` or `quarantined`.
    ///
    /// `grants/unified` is written by three independent sources, and each run's
    /// contribution is stamped with THAT source's extraction health
    /// (`grants_common::contribution_target`): a degrading source's rows land here
    /// stamped `provisional`, a quarantined source's are diverted out to
    /// `grants/unified@q` entirely. `trust=stable` is how a consumer asks for only
    /// the rows we stand behind — the same vocabulary as `/datasets` and `/changes`.
    #[serde(default = "default_trust_all")]
    trust: String,
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
    let trust = trust_filter(&query.trust);
    let Some(cursor) = &query.cursor else {
        let grants = state
            .datasets
            .list_filtered_trust(GRANTS_APP, GRANTS_DATASET, &filters, None, limit, trust)
            .await?;
        return Ok(Json(json!({ "grants": grants })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .datasets
        .list_filtered_trust(GRANTS_APP, GRANTS_DATASET, &filters, after, limit, trust)
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
    responses((status = 200, description = "`{days, count, grants}` — live open grants closing within the window, soonest first. Each grant is its unified record `data` plus `key` and `days_left`. `count` is the window total; `grants` is capped at 200. \"Still open\" is decided at `deadline_end_utc` — the exact `close_at` instant, or midday UTC the day after a date-only `close_date` — so a grant in that anywhere-on-Earth tail is listed with `days_left: 0`, exactly as `grants/unified` still calls it `open`."))
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
    let now = chrono::Utc::now();
    let today = now.date_naive();
    let until = today + chrono::Duration::days(days);
    let status_open = JsonFilter::Eq {
        path: "$.status".into(),
        value: "open".into(),
    };

    // Computed on read rather than materialized as a dataset: a read view can
    // never go stale between syncs — which a "closing soon" list, whose membership
    // changes with the calendar and not with the data, absolutely would if it were
    // snapshotted.
    //
    // "Still open" is decided where the producers decide it — at
    // `deadline_end_utc`, not at `Utc::now().date_naive()`. A date-only deadline
    // `D` is over at `D+1T12:00:00Z` (the moment `D` has ended everywhere on
    // Earth), so `grants/unified` still says `open` for the whole of `D+1`'s
    // first half while a `close_date >= today` filter had already dropped the
    // row at `00:00Z`: ~12 hours a day in which the corpus and this view
    // disagreed about live money. The SQL window therefore starts one day
    // early, and that one-day tail — small, and sorted first — is judged in
    // Rust by the shared predicate before it is counted or returned.
    let filters = vec![
        status_open.clone(),
        JsonFilter::Gte {
            path: "$.close_date".into(),
            value: today.to_string(),
        },
        JsonFilter::Lte {
            path: "$.close_date".into(),
            value: until.to_string(),
        },
    ];
    let tail_filters = vec![
        status_open,
        JsonFilter::Eq {
            path: "$.close_date".into(),
            value: (today - chrono::Duration::days(1)).to_string(),
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
    let tail = state
        .datasets
        .list_filtered_ordered(
            GRANTS_APP,
            GRANTS_DATASET,
            &tail_filters,
            "$.close_date",
            CLOSING_SOON_TAIL_CAP,
        )
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

    // The tail rows that are genuinely still open come first (they close
    // soonest), then SQL's soonest-first window; attach key + days_left.
    let still_open_tail: Vec<_> = tail
        .into_iter()
        .filter(|r| still_claimable(&r.data, now))
        .collect();
    let count = count + still_open_tail.len() as i64;
    let grants: Vec<Value> = still_open_tail
        .into_iter()
        .chain(records)
        .take(CLOSING_SOON_CAP)
        .filter_map(|r| {
            let close = r.data.get("close_date").and_then(Value::as_str)?;
            let close = chrono::NaiveDate::parse_from_str(close, "%Y-%m-%d").ok()?;
            let mut grant = r.data.as_object()?.clone();
            grant.insert("key".into(), json!(r.key));
            grant.insert("days_left".into(), json!(days_left(close, today)));
            Some(Value::Object(grant))
        })
        .collect();
    Ok(Json(
        json!({ "days": days, "count": count, "grants": grants }),
    ))
}

/// Rows in the one-day anywhere-on-Earth tail the closing-soon view re-judges
/// in Rust. A day's worth of deadlines, not a corpus, so a generous bound.
const CLOSING_SOON_TAIL_CAP: i64 = 1000;

/// Whether a unified grant row is still claimable at `now` — decided at
/// `grants_common::deadline_end_utc`, the same instant the corpus sweep and the
/// producers' digests use, so this view cannot disagree with `grants/unified`.
/// An unparseable deadline is never a lapsed one (the sweep's rule too).
fn still_claimable(data: &Value, now: chrono::DateTime<chrono::Utc>) -> bool {
    let close_date = data.get("close_date").and_then(Value::as_str);
    let close_at = data.get("close_at").and_then(Value::as_str);
    match grants_common::deadline_end_utc(close_date, close_at) {
        Some(end) => end > now,
        None => true,
    }
}

/// Whole days until `close`, floored at 0: a grant in the anywhere-on-Earth
/// tail is closing *today* from the caller's point of view, never `-1`.
fn days_left(close: chrono::NaiveDate, today: chrono::NaiveDate) -> i64 {
    (close - today).num_days().max(0)
}

#[cfg(test)]
mod closing_soon_tests {
    use super::*;
    use chrono::TimeZone;

    /// THE REFUTED BEHAVIOR: the route filtered `close_date >= today` (UTC
    /// date), so a date-only deadline of yesterday was gone at `00:00Z` while
    /// `grants/unified` — judged at `D+1T12:00:00Z` — still called it open.
    #[test]
    fn a_grant_in_the_anywhere_on_earth_tail_is_still_claimable_not_yesterdays() {
        let row = json!({ "close_date": "2026-09-01", "close_at": Value::Null });
        let early = chrono::Utc.with_ymd_and_hms(2026, 9, 2, 3, 0, 0).unwrap();
        let late = chrono::Utc.with_ymd_and_hms(2026, 9, 2, 13, 0, 0).unwrap();
        assert!(
            still_claimable(&row, early),
            "D+1T03:00Z: still on D somewhere"
        );
        assert!(!still_claimable(&row, late), "D+1T13:00Z: over everywhere");
        // A zoned deadline retires to the second.
        let zoned = json!({ "close_date": "2026-09-01", "close_at": "2026-09-01T17:00:00Z" });
        let before = chrono::Utc.with_ymd_and_hms(2026, 9, 1, 16, 59, 0).unwrap();
        let after = chrono::Utc.with_ymd_and_hms(2026, 9, 1, 17, 1, 0).unwrap();
        assert!(still_claimable(&zoned, before));
        assert!(!still_claimable(&zoned, after));
        // No parseable deadline is never a lapsed one.
        assert!(still_claimable(&json!({ "close_date": "soon" }), late));
    }

    #[test]
    fn days_left_is_floored_at_zero_for_the_tail() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        let yesterday = chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let next_week = chrono::NaiveDate::from_ymd_opt(2026, 9, 9).unwrap();
        assert_eq!(days_left(yesterday, today), 0);
        assert_eq!(days_left(today, today), 0);
        assert_eq!(days_left(next_week, today), 7);
    }
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
///
/// `pub(crate)` because `/sources` ages contract verdicts against the same
/// window: one grace, both surfaces.
pub(crate) const CATALOG_STALE_GRACE: i64 = 2;

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
    responses((status = 200, description = "`{checked, stale, contracts_enforce, sources: [{id, app, dataset, cadence, expected_max_age_secs, last_write_at, age_secs, stale, monitored, reason?, contract?}]}` — per-source freshness for live sources; `monitored:false` when no dataset or no freshness window. `expected_max_age_secs` is the stale threshold (cadence × grace, tightened by a declared contract's `max_staleness_hours`). `contract` appears on sources declaring a `[source.contract]` block: `{declared, enforce, last_verdict}` where `last_verdict` is the worker's most recent publish-time evaluation (`{verdict: pass|warn|block, violations, job_id, checked_at, age_secs, stale, stale_reason?, ...}`, null before the first run since boot). Verdicts live in memory and never expire on their own, so they are aged against this same `expected_max_age_secs` window: `stale: true` marks a verdict describing a run that is no longer current, `stale: null` one that cannot be judged (the source declares no freshness expectation)."))
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
        // Not monitorable: no dataset/app, or no freshness window from either
        // the cadence or a declared contract. A contract's `max_staleness_hours`
        // tightens (never loosens) the cadence-derived window, and supplies one
        // when the cadence has none — see `Source::freshness_window_secs`.
        let expected = s.freshness_window_secs(CATALOG_STALE_GRACE);
        // Declared data contract (M20): declaration + the latest publish-time
        // verdict recorded by the worker (in-memory; null until the first run
        // after boot). The verdict is aged against the SAME window as the
        // dataset write below — an old verdict is not a current one.
        if let Some(contract) = &s.contract {
            let latest = super::error::lock_advisory(&state.contract_verdicts, "contract_verdicts")
                .get(&format!("{}/{}", s.app, s.dataset))
                .cloned()
                .map(|v| {
                    super::health::verdict_with_age(
                        v,
                        super::health::SourceWindow::of_live(s, CATALOG_STALE_GRACE),
                        now,
                    )
                });
            row.insert(
                "contract".into(),
                json!({
                    "declared": contract,
                    "enforce": state.config.contracts.enforce,
                    "last_verdict": latest.unwrap_or(Value::Null),
                }),
            );
        }
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

/// DataHub emitter configuration, emission history, and governance state.
#[utoipa::path(
    get,
    path = "/datahub/status",
    tag = "datahub",
    responses((status = 200, description = "`{enabled, gms_url, env, token_set, emit_schema, emit_profile, emit_flows, last_emission, emissions, govern}`. `emissions` = `{ok, failed, last, last_success, last_error, sync_running}` — successes and failures are counted and kept in SEPARATE slots, so a success cannot hide the last failure; entries are `{kind: job|sync, at, ok, entities?|error?}`. `last_emission` mirrors `emissions.last`. `govern` = `{enabled, interval_secs, paused_apps, last_poll, recent_actions}`, where last_poll is the most recent governance poll summary (`{at, ok, datasets_polled, poll_ms, budget_secs, schedules_disabled, syncs_enqueued, paused_apps, actions}`) or its error, and `recent_actions` is the newest 20 rows of the DURABLE audit trail (`{id, action, target, dataset, subject, evidence, detail, created_at}`, age-bounded at 90 days). Everything except `recent_actions` is in-memory: a restart zeroes it."))
)]
pub(crate) async fn datahub_status(State(state): State<AppState>) -> Json<Value> {
    Json(crate::datahub::status_json(&state).await)
}

/// **What the DataHub governance actuator would do right now**, without doing
/// any of it. Reads the same remote state a poll reads; disables nothing,
/// enqueues nothing, pauses nothing.
///
/// Deliberately works with `[datahub] govern = false` — it is the answer to the
/// only question that gates turning governance on, and that answer has to be
/// available before the switch is flipped.
#[utoipa::path(
    get,
    path = "/datahub/governance/preview",
    tag = "datahub",
    responses(
        (status = 200, description = "`{at, governing, gms_url, env, datasets_polled, poll_ms, budget_secs, quiet, would: {disable_schedules: [{app, dataset, evidence, schedule_ids, note}], pause_apps, resume_apps, enqueue_syncs: [{app, dataset, evidence, registered, idempotency_key, note}]}, paused_now, read_errors, poll_would_abort, totals}`. \
            `quiet: true` means a poll right now would change nothing. `schedule_ids` names the exact catalog-managed rows a deprecation would disable (hand-made schedules are never listed — they are never touched). Unlike a real poll, a read error here does not abort: it is reported in `read_errors`, and `poll_would_abort` says whether a real poll would consequently have done nothing at all. Writes nothing."),
        (status = 409, description = "[datahub] is disabled in config (no GMS to read)", body = Object),
    )
)]
pub(crate) async fn datahub_governance_preview(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    if !state.config.datahub.enabled {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "[datahub] is disabled — set enabled = true and gms_url in config (govern may stay \
             false: this preview reads DataHub but acts on nothing)"
                .into(),
        ));
    }
    Ok(Json(crate::datahub::governance_preview(&state).await))
}

/// One-shot metadata backfill: pushes every stored dataset (entity, properties,
/// and per-config profile/schema) to the configured DataHub GMS. Run it once
/// after connecting a fresh instance; job completions keep it current after that.
#[utoipa::path(
    post,
    path = "/datahub/sync",
    tag = "datahub",
    responses(
        (status = 200, description = "`{kind: \"sync\", at, ok, datasets, flows, trigger_edges, entities?|error?}` — the emission summary (also on /datahub/status)"),
        (status = 409, description = "[datahub] is disabled in config, or a full sync is already running (one at a time — retry when it finishes)"),
    )
)]
pub(crate) async fn datahub_sync(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    if !state.config.datahub.enabled {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "[datahub] is disabled — set enabled = true and gms_url in config".into(),
        ));
    }
    match crate::datahub::full_sync(&state).await {
        crate::datahub::SyncOutcome::Ran(summary) => Ok(Json(summary)),
        crate::datahub::SyncOutcome::Busy => Err(ApiError(
            StatusCode::CONFLICT,
            "a DataHub full sync is already running — one at a time; retry when it finishes \
             (progress: GET /datahub/status → emissions.sync_running)"
                .into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Market query surface (N33)
//
// `market/profile` is the cross-FAMILY product: one row per state × trade
// joining `trades/operator_economics` (keyed `<ST>:<trade>`) to
// `census/market_blend` (keyed `{naics4}:{state_fips}`). The join is done by
// `trades_common::market`; this route is the one-call read, and the MCP
// `market_profile` tool answers through the same function so the two surfaces
// cannot disagree about what "not found" means.
// ---------------------------------------------------------------------------

/// Virtual app namespace holding the cross-family market product. Mirrors
/// `trades_common::market::{MARKET_APP, PROFILE_DATASET}`; duplicated as
/// literals rather than taking a server dependency on a library crate for two
/// strings (the same judgment `GRANTS_APP` documents above).
const MARKET_APP: &str = "market";
const PROFILE_DATASET: &str = "profile";

/// How many rows of one state the case-insensitive fallback may scan. A state
/// holds one row per enabled trade (5 today); 200 is far past any plausible
/// taxonomy and keeps a mistyped trade from reading a whole dataset.
const MARKET_TRADE_SCAN: i64 = 200;

/// The canonical record key for a state × trade profile. **Pure**, so the HTTP
/// route, the MCP tool and any future consumer spell the key exactly one way —
/// the key grammar is the product's contract, and a second spelling of it is
/// how two surfaces come to disagree about which row exists.
///
/// The state segment is upper-cased (`tx` → `TX`, the stored form); the trade
/// segment is only trimmed, because trade labels are display strings
/// (`Pool service`) whose casing the taxonomy owns — matching one
/// case-insensitively is [`find_market_profile`]'s job, not the key's.
pub(crate) fn market_profile_key(state: &str, trade: &str) -> String {
    format!("{}:{}", state.trim().to_uppercase(), trade.trim())
}

/// One state × trade profile, or `None` when there is no live row for it.
///
/// Two lookups, in order: the exact key, then a case-insensitive scan of that
/// state's rows so an agent asking for `hvac` finds `HVAC` instead of a 404 it
/// cannot act on. Tombstoned rows are excluded at both steps — `Datasets::get`
/// and `list` both return removed records by design, and a removed profile is
/// not an answer.
pub(crate) async fn find_market_profile(
    state: &AppState,
    st: &str,
    trade: &str,
) -> Result<Option<Value>, pumper_core::Error> {
    let key = market_profile_key(st, trade);
    if let Some(rec) = state
        .datasets
        .get(MARKET_APP, PROFILE_DATASET, &key)
        .await?
    {
        if rec.removed_at.is_none() {
            return Ok(Some(rec.data));
        }
    }
    let wanted = trade.trim().to_lowercase();
    if wanted.is_empty() {
        return Ok(None);
    }
    let rows = state
        .datasets
        .list_filtered(
            MARKET_APP,
            PROFILE_DATASET,
            &[pumper_core::datasets::JsonFilter::Eq {
                path: "$.state".into(),
                value: st.trim().to_uppercase(),
            }],
            None,
            MARKET_TRADE_SCAN,
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter(|r| r.removed_at.is_none())
        .find(|r| {
            r.data
                .get("trade")
                .and_then(Value::as_str)
                .is_some_and(|t| t.to_lowercase() == wanted)
        })
        .map(|r| r.data))
}

/// **One state × trade market profile**: the economics half and the density
/// half of "should I launch as a plumber in Texas" in a single row.
#[utoipa::path(
    get,
    path = "/market/profile/{state}/{trade}",
    tag = "market",
    params(
        ("state" = String, Path, description = "USPS state code, any case (`TX`, `tx`). `US` is not a profile: the national roll-up lives in `trades/operator_economics`."),
        ("trade" = String, Path, description = "Canonical trade label, matched case-insensitively (`Plumbing`, `hvac`, `Pool service`)."),
    ),
    responses(
        (status = 200, description = "The `market/profile` record: `{state, state_fips, trade, soc_code, naics4, density_grain, density_key, coverage, total_market_per_10k, total_market_per_10k_basis, economics: {wage_band, wage_grain, pricing, pricing_locality, tax, compliance, valuation}, density, succession, formation, vintages}`. \
            `coverage` is `both` or `economics_only`, and an `economics_only` row has `density: null` — never zeros, which would read as \"nobody operates here\" when the fact is \"the census publishes no cell for this trade\". `density_grain` is always `naics4`: the nonemployer series is published at 4-digit NAICS, so Plumbing, Electrical and HVAC (all 238220) share one density block. `vintages` names the year each input came from, which `updated_at` does not."),
        (status = 404, description = "No profile for that state × trade", body = Object),
    )
)]
pub(crate) async fn market_profile(
    State(state): State<AppState>,
    axum::extract::Path((st, trade)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    match find_market_profile(&state, &st, &trade).await? {
        Some(profile) => Ok(Json(profile)),
        None => Err(ApiError(
            StatusCode::NOT_FOUND,
            format!(
                "no market/profile row for '{}' — the product is derived from \
                 trades/operator_economics and census/market_blend, so it exists only for a \
                 state a trades app has covered and a trade in the live taxonomy. Run any \
                 trades app (or any census app) to republish it, and GET \
                 /datasets/market/profile to see which rows exist.",
                market_profile_key(&st, &trade)
            ),
        )),
    }
}

#[cfg(test)]
mod market_tests {
    use super::market_profile_key;

    /// The key grammar IS the product's contract, and the anti-pattern is a
    /// second spelling of it: a route that upper-cases the trade too would ask
    /// for `TX:PLUMBING` and get a 404 for a row that exists. The state segment
    /// is normalized (the store holds USPS codes); the trade segment is a
    /// display label the taxonomy owns, so it is only trimmed — matching it
    /// case-insensitively is `find_market_profile`'s fallback, not the key's.
    #[test]
    fn the_key_normalizes_the_state_and_leaves_the_trade_label_alone() {
        assert_eq!(market_profile_key("tx", "Plumbing"), "TX:Plumbing");
        assert_eq!(
            market_profile_key(" TX ", " Pool service "),
            "TX:Pool service"
        );
        assert_eq!(market_profile_key("Tx", "hvac"), "TX:hvac");
        // Not "TX:HVAC": upper-casing the label would miss the stored row.
        assert_ne!(market_profile_key("tx", "hvac"), "TX:HVAC");
    }
}
