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
    /// Funding-program key, exact match — the `program_key` of `GET
    /// /grants/programs` (`aln:93.912`, `family:HORIZON-CL4-DATA-01`,
    /// `<agency>|<program title>`). Every posting of one program, across cycles
    /// and portals, in one query. Rows the identity rules could not name a
    /// program for carry no `program_key` and never match.
    program: Option<String>,
    /// **Applicant fit (N31)**: the key of a `grants/profiles` row. Switches the
    /// route to the fit-joined view — every opportunity this applicant has a
    /// verdict for, each with its `fit` block attached, driven from
    /// `grants/fits`. Combine it with `verdict=` to narrow; combining it with a
    /// corpus filter (`status`, `agency`, `source`, `program`, the closing
    /// window, `min_award`) is a **400**, because the two sides live in
    /// different datasets and ANDing two capped reads would return part of the
    /// answer while looking like all of it.
    profile: Option<String>,
    /// Narrows `profile=` to one verdict: `eligible` | `likely` | `blocked` |
    /// `unknown`. Ignored when `profile` is absent.
    verdict: Option<String>,
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
    if let Some(program) = filter_value(&query.program) {
        filters.push(JsonFilter::Eq {
            path: format!("$.{}", grants_common::programs::PROGRAM_KEY_FIELD),
            value: program.into(),
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
        (status = 200, description = "Live records from `grants/unified` matching every filter, newest-updated first. Dual-mode: `{grants: [Record]}`, or `{items, next_cursor}` when `cursor` is present (even empty). With `profile=` set the route instead returns `{profile, grants, retired}` (or `{items, next_cursor, retired}`), driven from `grants/fits`: each element is the unified record plus a `fit` block, and `retired` counts the verdicts whose opportunity has left the corpus.", body = crate::routes::dto::GrantsResponse),
        (status = 400, description = "Malformed `closing_before` / `closing_after` date, an unrecognized `verdict`, or `profile` combined with a corpus filter", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn list_grants(
    State(state): State<AppState>,
    Query(query): Query<GrantsQuery>,
) -> Result<Json<Value>, ApiError> {
    // N31: `profile=` reads the applicant's verdicts, not the corpus. The whole
    // branch lives at the end of this file with the rest of the fit surface.
    if let Some(profile) = filter_value(&query.profile) {
        return grants_for_profile(&state, &query, profile).await;
    }
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
    responses((status = 200, description = "`{days, count, grants}` — live open grants closing within the window, soonest first. Each grant is its unified record `data` plus `key` and `days_left`. `count` is the window total; `grants` is capped at 200. \"Still open\" is decided at `deadline_end_utc` — the exact `close_at` instant, or midday UTC the day after a date-only `close_date` — so a grant in that anywhere-on-Earth tail is listed with `days_left: 0`, exactly as `grants/unified` still calls it `open`.", body = crate::routes::dto::ClosingSoonResponse))
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
        (status = 200, description = "`{count, sources: [Source]}` — data pipelines, optionally filtered by `market` / `status` / `category`.", body = crate::routes::dto::CatalogSourcesResponse),
        (status = 500, description = "Catalog file malformed", body = crate::routes::dto::ErrorEnvelope),
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
    responses((status = 200, description = "`{checked, stale, contracts_enforce, sources: [{id, app, dataset, cadence, expected_max_age_secs, last_write_at, age_secs, stale, monitored, reason?, contract?}]}` — per-source freshness for live sources; `monitored:false` when no dataset or no freshness window. `expected_max_age_secs` is the stale threshold (cadence × grace, tightened by a declared contract's `max_staleness_hours`). `contract` appears on sources declaring a `[source.contract]` block: `{declared, enforce, last_verdict}` where `last_verdict` is the worker's most recent publish-time evaluation (`{verdict: pass|warn|block, violations, job_id, checked_at, age_secs, stale, stale_reason?, ...}`, null before the first run since boot). Verdicts live in memory and never expire on their own, so they are aged against this same `expected_max_age_secs` window: `stale: true` marks a verdict describing a run that is no longer current, `stale: null` one that cannot be judged (the source declares no freshness expectation).", body = crate::routes::dto::CatalogHealthResponse))
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
        (status = 200, description = "`{empty, create, update, disable, orphan, covered_by_untagged, in_sync, auto_reconcile}` — the reconciliation plan. `orphan` is report-only (never applied).", body = crate::routes::dto::ReconcilePlanDto),
        (status = 500, description = "Catalog file malformed", body = crate::routes::dto::ErrorEnvelope),
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
        (status = 200, description = "`{applied: {created, updated, disabled, orphans_untouched, errors}, plan}` — what was done, plus the plan it executed.", body = crate::routes::dto::ReconcileApplyResponse),
        (status = 409, description = "Plan disables too many schedules; retry with `?force=true`", body = crate::routes::dto::ErrorEnvelope),
        (status = 500, description = "Catalog file malformed", body = crate::routes::dto::ErrorEnvelope),
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
    responses((status = 200, description = "`{enabled, gms_url, env, token_set, emit_schema, emit_profile, emit_flows, last_emission, emissions, govern}`. `emissions` = `{ok, failed, last, last_success, last_error, sync_running}` — successes and failures are counted and kept in SEPARATE slots, so a success cannot hide the last failure; entries are `{kind: job|sync, at, ok, entities?|error?}`. `last_emission` mirrors `emissions.last`. `govern` = `{enabled, interval_secs, paused_apps, last_poll, recent_actions}`, where last_poll is the most recent governance poll summary (`{at, ok, datasets_polled, poll_ms, budget_secs, schedules_disabled, syncs_enqueued, paused_apps, actions}`) or its error, and `recent_actions` is the newest 20 rows of the DURABLE audit trail (`{id, action, target, dataset, subject, evidence, detail, created_at}`, age-bounded at 90 days). Everything except `recent_actions` is in-memory: a restart zeroes it.", body = crate::routes::dto::DatahubStatusResponse))
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
            `quiet: true` means a poll right now would change nothing. `schedule_ids` names the exact catalog-managed rows a deprecation would disable (hand-made schedules are never listed — they are never touched). Unlike a real poll, a read error here does not abort: it is reported in `read_errors`, and `poll_would_abort` says whether a real poll would consequently have done nothing at all. Writes nothing.", body = crate::routes::dto::GovernancePreview),
        (status = 409, description = "[datahub] is disabled in config (no GMS to read)", body = crate::routes::dto::ErrorEnvelope),
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
        (status = 200, description = "`{kind: \"sync\", at, ok, datasets, flows, trigger_edges, entities?|error?}` — the emission summary (also on /datahub/status)", body = crate::routes::dto::DatahubSyncResponse),
        (status = 409, description = "[datahub] is disabled in config, or a full sync is already running (one at a time — retry when it finishes)", body = crate::routes::dto::ErrorEnvelope),
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
// Program registry (`grants/programs`) — appended for N29. Builders append at
// the END of this file, in wave order, so two branches touching it merge
// without reordering anything above.
// ---------------------------------------------------------------------------

/// The cross-source program registry, in the same virtual namespace as the
/// corpus. Mirrors `grants_common::programs::PROGRAMS_DATASET`.
const PROGRAMS_DATASET: &str = "programs";

/// Filters over `grants/programs`. All optional, all ANDed.
#[derive(Deserialize, IntoParams)]
pub(crate) struct ProgramsQuery {
    /// Case-insensitive substring of the program's agency, as the latest
    /// posting published it.
    agency: Option<String>,
    /// Keeps programs whose `sources[]` names this source app. Matched as a
    /// substring of the serialized array, which is exact enough for the three
    /// source ids in play (`grants-gov` | `ca-grants` | `eu-sedia`) and needs no
    /// array-containment operator in the store.
    source: Option<String>,
    /// Programs expected to REOPEN on or before this `YYYY-MM-DD`. Only
    /// programs that earned a projection have a `next_expected_open`, so this
    /// filter answers "what is coming back in the next N days" and silently
    /// excludes the programs we cannot honestly predict — which is the point.
    next_expected_before: Option<String>,
    /// Exact `program_key`, for fetching one program without knowing its shape.
    program: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

/// Translates the program filters into store-level JSON predicates.
fn program_filters(
    query: &ProgramsQuery,
) -> Result<Vec<pumper_core::datasets::JsonFilter>, ApiError> {
    use pumper_core::datasets::JsonFilter;
    let mut filters = Vec::new();
    if let Some(program) = filter_value(&query.program) {
        filters.push(JsonFilter::Eq {
            path: "$.program_key".into(),
            value: program.into(),
        });
    }
    if let Some(agency) = filter_value(&query.agency) {
        filters.push(JsonFilter::Contains {
            path: "$.agency".into(),
            value: agency.into(),
        });
    }
    if let Some(source) = filter_value(&query.source) {
        filters.push(JsonFilter::Contains {
            path: "$.sources".into(),
            value: source.into(),
        });
    }
    if let Some(before) = filter_value(&query.next_expected_before) {
        // Same rule as the closing-window filters: the stored dates are
        // canonical `YYYY-MM-DD` and compare lexicographically, so anything
        // else is rejected rather than silently compared as a malformed string.
        parse_grant_date(before, "next_expected_before")?;
        filters.push(JsonFilter::Lte {
            path: "$.next_expected_open".into(),
            value: before.into(),
        });
    }
    Ok(filters)
}

/// One row per funding **program** — the entity the postings belong to.
///
/// `grants/unified` answers "which opportunities are open"; this answers the
/// question a grant-seeker actually has: *does this program come back, when,
/// and does this agency move its deadlines*. It is materialized by the
/// once-per-cycle corpus pass (`grants_common::programs`), not computed on read,
/// because it folds four datasets — the live corpus, `grants/recurrence_links`,
/// `grants/events` and `cordis/topic_stats` — and a per-request join of those
/// would be a full scan of each.
#[utoipa::path(
    get,
    path = "/grants/programs",
    tag = "grants",
    params(ProgramsQuery),
    responses(
        (status = 200, description = "Live records from `grants/programs`. Dual-mode: `{programs: [Record]}`, or `{items, next_cursor}` when `cursor` is present (even empty). Each `data` carries `{program_key, title, agency, sources[], cycles_observed, dated_cycles, period_days, next_expected_open, next_expected_close, prediction_basis, opportunities[], recurrence_linked, deadline_extended_count, closed_early_count, extension_rate, last_award_ceiling, win_history}`. `period_days` and the next window are `null` unless the program's own dated cycles support them, with `prediction_basis` saying why; `extension_rate` is `null` when the lifecycle-event read was windowed rather than complete; `win_history` is Horizon-only.", body = crate::routes::dto::ProgramsResponse),
        (status = 400, description = "Malformed `next_expected_before` date", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn list_programs(
    State(state): State<AppState>,
    Query(query): Query<ProgramsQuery>,
) -> Result<Json<Value>, ApiError> {
    let filters = program_filters(&query)?;
    let limit = query.limit.clamp(1, GRANTS_MAX_LIMIT);
    let Some(cursor) = &query.cursor else {
        let programs = state
            .datasets
            .list_filtered(GRANTS_APP, PROGRAMS_DATASET, &filters, None, limit)
            .await?;
        return Ok(Json(json!({ "programs": programs })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .datasets
        .list_filtered(GRANTS_APP, PROGRAMS_DATASET, &filters, after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |r| {
        format!("{}|{}", pumper_core::datasets::ts(r.updated_at), r.key)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

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
            `coverage` is `both` or `economics_only`, and an `economics_only` row has `density: null` — never zeros, which would read as \"nobody operates here\" when the fact is \"the census publishes no cell for this trade\". `density_grain` is always `naics4`: the nonemployer series is published at 4-digit NAICS, so Plumbing, Electrical and HVAC (all 238220) share one density block. `vintages` names the year each input came from, which `updated_at` does not.", body = crate::routes::dto::MarketProfileResponse),
        (status = 404, description = "No profile for that state × trade", body = crate::routes::dto::ErrorEnvelope),
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

// ---------------------------------------------------------------------------
// Applicant fit engine (`grants/profiles` + `grants/fits`) — appended for N31.
// Builders append at the END of this file, in wave order, so two branches
// touching it merge without reordering anything above.
//
// The engine itself is `grants_common::fit`: pure gates, one verdict per
// (profile, opportunity) pair, written by the producer apps. These routes are
// the door onto it — the profile writer, the profile reader, the fit reader,
// and the `profile=` join on `GET /grants`.
//
// `POST /grants/profiles` needs no entry in `auth::required_scope`: that
// function's default for a mutating path is `Admin`, so a route it has never
// heard of is guarded before anyone remembers the file exists. Adding a case
// for this path would only weaken it.
// ---------------------------------------------------------------------------

/// Operator-authored applicant profiles. Mirrors
/// `grants_common::fit::PROFILES_DATASET`.
const PROFILES_DATASET: &str = "profiles";
/// One scored verdict per profile × opportunity. Mirrors
/// `grants_common::fit::FITS_DATASET`.
const FITS_DATASET: &str = "fits";

/// The filters that read the CORPUS, and therefore cannot be combined with
/// `profile=`, which reads the fit set. Named once so the refusal message and
/// the check cannot drift apart.
const CORPUS_FILTERS: &[&str] = &[
    "status",
    "agency",
    "source",
    "program",
    "closing_before",
    "closing_after",
    "min_award",
];

/// Which corpus filters this query set, in `CORPUS_FILTERS` order.
///
/// **Pure**, and extracted because the anti-pattern is the silent version:
/// applying `profile=` by intersecting two capped reads answers *some* of the
/// question and states nothing about what fell outside either window. A
/// grant-seeker who filters "eligible AND closing this month" and silently gets
/// four of the eleven has been given a wrong answer, not a partial one.
fn corpus_filters_set(query: &GrantsQuery) -> Vec<&'static str> {
    let present = [
        filter_value(&query.status).is_some(),
        filter_value(&query.agency).is_some(),
        filter_value(&query.source).is_some(),
        filter_value(&query.program).is_some(),
        filter_value(&query.closing_before).is_some(),
        filter_value(&query.closing_after).is_some(),
        query.min_award.is_some(),
    ];
    CORPUS_FILTERS
        .iter()
        .zip(present)
        .filter(|(_, set)| *set)
        .map(|(name, _)| *name)
        .collect()
}

/// Validates a `verdict=` param against the closed vocabulary, so a typo is a
/// 400 rather than a confident empty result set.
fn verdict_filter(value: &Option<String>) -> Result<Option<&'static str>, ApiError> {
    let Some(raw) = filter_value(value) else {
        return Ok(None);
    };
    grants_common::fit::Verdict::parse(raw)
        .map(|v| Some(v.as_str()))
        .ok_or_else(|| {
            ApiError(
                StatusCode::BAD_REQUEST,
                format!(
                    "'verdict' must be one of: {}, got '{raw}'",
                    grants_common::fit::Verdict::ALL
                        .iter()
                        .map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(" | ")
                ),
            )
        })
}

#[utoipa::path(
    post,
    path = "/grants/profiles",
    tag = "grants",
    request_body = Object,
    responses(
        (status = 200, description = "`{key, created, profile}` — the canonical stored profile. Body: `{name, org_type (nonprofit|gov|tribal|smb|university|individual), country (ISO-3166 alpha-2), state?, ein?, uei?, ntee?, budget_band? {min?, max?}, focus_tags?[], cost_share_capacity? (true|false|null), programs_watched?[]}`. The record key is the slugged `name`, so re-POSTing the same name UPDATES that profile (`created: false`). Every absent optional field is stored as an explicit `null`: absent means UNKNOWN, and unknown never blocks a fit. `ein`/`uei`/`ntee` are stored, never verified — IRS EO BMF verification is not built.", body = crate::routes::dto::GrantProfileWritten),
        (status = 400, description = "Validation failed — every error at once, including any UNKNOWN field (a typo'd field is refused, not dropped)", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn create_grant_profile(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let (key, profile) = grants_common::fit::validate_profile(&body)
        .map_err(|errors| ApiError(StatusCode::BAD_REQUEST, errors.join("; ")))?;
    let existed = state
        .datasets
        .get(GRANTS_APP, PROFILES_DATASET, &key)
        .await?
        .is_some_and(|r| r.removed_at.is_none());
    state
        .datasets
        .upsert_stamped(GRANTS_APP, PROFILES_DATASET, &key, &profile, None, None)
        .await?;
    Ok(Json(json!({
        "key": key,
        "created": !existed,
        "profile": profile,
    })))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct ProfilesQuery {
    #[serde(default = "default_limit")]
    limit: i64,
}

#[utoipa::path(
    get,
    path = "/grants/profiles",
    tag = "grants",
    params(ProfilesQuery),
    responses((status = 200, description = "`{profiles: [Record]}` — every live applicant profile, newest-updated first.", body = crate::routes::dto::GrantProfileListResponse))
)]
pub(crate) async fn list_grant_profiles(
    State(state): State<AppState>,
    Query(query): Query<ProfilesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, GRANTS_MAX_LIMIT);
    let profiles = state
        .datasets
        .list_filtered(GRANTS_APP, PROFILES_DATASET, &[], None, limit)
        .await?;
    Ok(Json(json!({ "profiles": profiles })))
}

/// Filters over `grants/fits`.
#[derive(Deserialize, IntoParams)]
pub(crate) struct FitsQuery {
    /// Profile key (the slugged name `POST /grants/profiles` returned).
    profile: Option<String>,
    /// `eligible` | `likely` | `blocked` | `unknown`. An unrecognized value is a
    /// 400, never a confident empty page.
    verdict: Option<String>,
    /// Source app of the opportunity the verdict is about.
    source: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

fn fit_filters(
    profile: Option<&str>,
    verdict: Option<&str>,
    source: Option<&str>,
) -> Vec<pumper_core::datasets::JsonFilter> {
    use pumper_core::datasets::JsonFilter;
    let mut filters = Vec::new();
    for (path, value) in [
        ("$.profile", profile),
        ("$.verdict", verdict),
        ("$.source", source),
    ] {
        if let Some(value) = value {
            filters.push(JsonFilter::Eq {
                path: path.into(),
                value: value.into(),
            });
        }
    }
    filters
}

/// **Which grants can this applicant actually apply for, and why.**
#[utoipa::path(
    get,
    path = "/grants/fits",
    tag = "grants",
    params(FitsQuery),
    responses(
        (status = 200, description = "Live records from `grants/fits`. Dual-mode: `{fits: [Record]}`, or `{items, next_cursor}` when `cursor` is present (even empty). Each `data` is `{profile, unified_key, source, verdict, score, method, reasons[], blockers[], unknowns[]}`. `verdict` is `eligible` only when every gate had published evidence; `unknown` whenever the deciding fields are Null — a missing field never produces a `blocked`. `unknowns[]` names the absent field per gate, which is the list that says which source field to enrich next. The row is deliberately verdict-shaped and copies nothing from the opportunity, so a `changed` revision (and the alert it fires) means the FIT moved, not that an agency fixed a typo.", body = crate::routes::dto::FitsResponse),
        (status = 400, description = "Unrecognized `verdict`", body = crate::routes::dto::ErrorEnvelope),
    )
)]
pub(crate) async fn list_fits(
    State(state): State<AppState>,
    Query(query): Query<FitsQuery>,
) -> Result<Json<Value>, ApiError> {
    let verdict = verdict_filter(&query.verdict)?;
    let filters = fit_filters(
        filter_value(&query.profile),
        verdict,
        filter_value(&query.source),
    );
    let limit = query.limit.clamp(1, GRANTS_MAX_LIMIT);
    let Some(cursor) = &query.cursor else {
        let fits = state
            .datasets
            .list_filtered(GRANTS_APP, FITS_DATASET, &filters, None, limit)
            .await?;
        return Ok(Json(json!({ "fits": fits })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .datasets
        .list_filtered(GRANTS_APP, FITS_DATASET, &filters, after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |r| {
        format!("{}|{}", pumper_core::datasets::ts(r.updated_at), r.key)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

/// `GET /grants?profile=` — the corpus read through one applicant's verdicts.
///
/// Driven from `grants/fits`, then hydrated from `grants/unified`, so paging is
/// exact: one fit row in, at most one grant out, and the keyset cursor is the
/// fit dataset's own.
///
/// **It refuses to be combined with the corpus filters**, and that refusal is
/// the honest half of the feature. The two sides live in different datasets, so
/// `profile=` + `closing_before=` could only be served by intersecting two
/// capped reads — which returns *some* of the answer while looking exactly like
/// all of it. A 400 that names the offending params is a worse UX and a correct
/// one; narrow with `verdict=` here, or filter the corpus and read
/// `GET /grants/fits` beside it.
async fn grants_for_profile(
    state: &AppState,
    query: &GrantsQuery,
    profile: &str,
) -> Result<Json<Value>, ApiError> {
    let conflicting = corpus_filters_set(query);
    if !conflicting.is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "'profile' reads grants/fits and {} read grants/unified — the two cannot be \
                 ANDed in one page without silently dropping matches outside either window. \
                 Narrow this call with 'verdict=', or drop 'profile' and read GET /grants/fits \
                 beside the filtered corpus.",
                conflicting.join(", ")
            ),
        ));
    }
    let verdict = verdict_filter(&query.verdict)?;
    let filters = fit_filters(Some(profile), verdict, None);
    let limit = query.limit.clamp(1, GRANTS_MAX_LIMIT);
    let after = query.cursor.as_deref().and_then(parse_cursor);
    let fits = state
        .datasets
        .list_filtered(GRANTS_APP, FITS_DATASET, &filters, after, limit)
        .await?;
    let next_cursor = keyset_cursor(&fits, limit, |r| {
        format!("{}|{}", pumper_core::datasets::ts(r.updated_at), r.key)
    });

    let mut grants = Vec::new();
    // A fit whose opportunity has left the corpus is COUNTED, not silently
    // dropped: the gap between "12 fits" and "10 grants" is a real fact about
    // the store, and swallowing it is how a page quietly shrinks.
    let mut retired = 0usize;
    for fit in &fits {
        let Some(unified_key) = fit.data.get("unified_key").and_then(Value::as_str) else {
            retired += 1;
            continue;
        };
        match state
            .datasets
            .get(GRANTS_APP, GRANTS_DATASET, unified_key)
            .await?
        {
            Some(rec) if rec.removed_at.is_none() => {
                let mut item = serde_json::to_value(&rec).unwrap_or(Value::Null);
                if let Value::Object(map) = &mut item {
                    map.insert("fit".into(), fit.data.clone());
                }
                grants.push(item);
            }
            _ => retired += 1,
        }
    }
    if query.cursor.is_some() {
        return Ok(Json(
            json!({ "items": grants, "next_cursor": next_cursor, "retired": retired }),
        ));
    }
    Ok(Json(json!({
        "profile": profile,
        "grants": grants,
        "retired": retired,
    })))
}

#[cfg(test)]
mod fit_tests {
    use super::*;

    fn query() -> GrantsQuery {
        GrantsQuery {
            status: None,
            agency: None,
            source: None,
            program: None,
            profile: None,
            verdict: None,
            closing_before: None,
            closing_after: None,
            min_award: None,
            trust: default_trust_all(),
            limit: default_limit(),
            cursor: None,
        }
    }

    /// A blank param (`?status=`) means "unset" everywhere else on this route,
    /// and it has to mean "unset" here too — otherwise a UI that always
    /// serializes its whole filter form gets a 400 for a form it never filled
    /// in, on the one route where the refusal is the feature.
    #[test]
    fn a_blank_corpus_filter_does_not_conflict_with_profile() {
        let mut q = query();
        q.status = Some(String::new());
        q.agency = Some("   ".into());
        assert!(corpus_filters_set(&q).is_empty());
        q.status = Some("open".into());
        q.min_award = Some(1.0);
        assert_eq!(corpus_filters_set(&q), vec!["status", "min_award"]);
    }

    /// A typo'd verdict is a 400, never a confident empty page — the anti-pattern
    /// is `?verdict=eligable` reading as "no grants fit you".
    #[test]
    fn an_unrecognized_verdict_is_refused_not_an_empty_page() {
        assert_eq!(
            verdict_filter(&Some("eligable".into())).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            verdict_filter(&Some("ELIGIBLE".into())).unwrap(),
            Some("eligible")
        );
        assert_eq!(verdict_filter(&None).unwrap(), None);
        assert_eq!(verdict_filter(&Some(String::new())).unwrap(), None);
    }
}
