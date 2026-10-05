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

/// Virtual app namespace holding the cross-source grants datasets, taken from
/// the crate that owns them. These were literals while the only thing the server
/// wanted from `grants-common` was two strings; the deadline rule below is not
/// two strings, so the dependency is declared and the names come with it.
const GRANTS_APP: &str = grants_common::UNIFIED_APP;
const GRANTS_DATASET: &str = grants_common::UNIFIED_DATASET;

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

/// One closing-soon row: its unified record, plus `key` and the countdown
/// `grants_common::deadline_countdown` computes from the ONE canonical deadline
/// instant.
///
/// The anti-pattern this closes: `days_left` used to be
/// `NaiveDate::parse_from_str(close_date) - Utc::now().date_naive()`, and
/// `close_at` appeared nowhere in this file. A zoned deadline eighteen hours
/// away printed "1 day", and `grants_common::deadline_end_utc`'s own doc names a
/// caller that re-derives the rule from `date_naive()` as reopening the timezone
/// bug it exists to close. The corpus sweep, the batch sweep and the per-source
/// digests all decide at that instant; this row now does too.
///
/// Four fields, and the last is the one an operator could not get before:
/// - `closes_at` - the canonical RFC3339 instant, or null when nothing parses.
/// - `hours_left` / `days_left` - null together with `closes_at`, never negative.
/// - `lapsed` - the stored `status` still says `open`, but the deadline has
///   provably passed. That state is normal rather than corrupt: the corpus-wide
///   sweep is claimed by one producer per UTC cycle
///   (`grants_common::claim_corpus_pass`), so a lapsed row carries `open` until
///   the next cycle's winner runs.
///
/// A row whose deadline does not parse keeps every field null and STAYS on the
/// page. Dropping it made `grants` shorter than the SQL page while `count` still
/// included it, which is a bug this route has already had once.
pub(crate) fn closing_soon_grant(
    key: &str,
    data: &Value,
    now: chrono::DateTime<chrono::Utc>,
) -> Value {
    let field = |name: &str| data.get(name).and_then(Value::as_str);
    let countdown = grants_common::deadline_countdown(field("close_date"), field("close_at"), now);
    let mut grant = match data.as_object() {
        Some(obj) => obj.clone(),
        None => serde_json::Map::new(),
    };
    grant.insert("key".into(), json!(key));
    grant.insert(
        "closes_at".into(),
        match countdown {
            Some(c) => json!(c
                .closes_at
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
            None => Value::Null,
        },
    );
    for (name, value) in [
        ("days_left", countdown.map(|c| c.days_left)),
        ("hours_left", countdown.map(|c| c.hours_left)),
    ] {
        grant.insert(
            name.into(),
            match value {
                Some(n) => json!(n),
                None => Value::Null,
            },
        );
    }
    grant.insert("lapsed".into(), json!(countdown.is_some_and(|c| c.lapsed)));
    Value::Object(grant)
}

/// The store predicate for the closing-soon window, as ONE value shared by the
/// COUNT and the page so the two can never select different sets.
///
/// The bounds come from `grants_common::closing_window_bounds`, which states the
/// canonical instant set as a pair of printed dates precisely so it stays a text
/// compare SQL can push down. Before, both bounds were `Utc::now().date_naive()`
/// arithmetic, so for the first twelve hours of every UTC day the grant closing
/// today was open in `grants/unified` and absent from this view.
fn closing_soon_filters(
    now: chrono::DateTime<chrono::Utc>,
    days: i64,
) -> Vec<pumper_core::datasets::JsonFilter> {
    use pumper_core::datasets::JsonFilter;
    let (lower, upper) = grants_common::closing_window_bounds(now, days);
    vec![
        JsonFilter::Eq {
            path: "$.status".into(),
            value: "open".into(),
        },
        JsonFilter::Gte {
            path: "$.close_date".into(),
            value: lower,
        },
        JsonFilter::Lte {
            path: "$.close_date".into(),
            value: upper,
        },
    ]
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
    responses((status = 200, description = "`{days, count, expired_unswept, grants}` — live open grants closing within the window, soonest first. Each grant is its unified record `data` plus `key`, `closes_at`, `hours_left`, `days_left` and `lapsed`, every one of them decided at the canonical deadline instant. `count` is the window total; `grants` is capped at 200."))
)]
pub(crate) async fn closing_soon(
    State(state): State<AppState>,
    Query(query): Query<ClosingSoonQuery>,
) -> Result<Json<Value>, ApiError> {
    let days = query
        .days
        .unwrap_or(CLOSING_SOON_DEFAULT_DAYS)
        .clamp(1, 365);
    // One clock reading for the whole response: the window bounds and every
    // row's countdown answer the same question, and reading `now` twice is how a
    // row lands outside the window that selected it.
    let now = chrono::Utc::now();

    // Computed on read rather than materialized as a dataset: a read view can
    // never go stale between syncs — which a "closing soon" list, whose membership
    // changes with the calendar and not with the data, absolutely would if it were
    // snapshotted.
    let filters = closing_soon_filters(now, days);
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

    // SQL already returns these soonest-first. Every row the query returned
    // stays in the page: `count` was computed from the same filters, and a
    // close_date that is not YYYY-MM-DD used to vanish here, so the page was
    // shorter than both the cap and the count with nothing saying why.
    let grants: Vec<Value> = records
        .into_iter()
        .map(|r| closing_soon_grant(&r.key, &r.data, now))
        .collect();
    // Sweep lag as a number instead of a question. Rows still stamped `open`
    // whose canonical deadline has provably passed: the corpus-wide sweep is
    // claimed by one producer per UTC cycle, so this is a normal transient, and
    // an operator previously had no way to tell it from a live listing.
    //
    // Counted over the returned page, which is sound because the page is ordered
    // by `$.close_date` ASC: lapsed rows carry the earliest dates in the window
    // and so sit at the front of it. Only a window holding more than
    // CLOSING_SOON_CAP lapsed rows could hide one, which is already the cap's
    // documented truncation.
    let expired_unswept = grants.iter().filter(|g| g["lapsed"] == json!(true)).count();
    Ok(Json(
        json!({ "days": days, "count": count, "expired_unswept": expired_unswept, "grants": grants }),
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

#[cfg(test)]
mod closing_soon_tests {
    use super::{closing_soon_filters, closing_soon_grant};
    use serde_json::json;

    fn at(s: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    /// The anti-pattern: `filter_map` dropped a row whose `close_date` the route
    /// could not read. The SQL `count` still included it, so the page was a
    /// silent short list.
    ///
    /// Strengthened when the route moved onto the canonical instant. The guard
    /// is unchanged - an unreadable deadline is a null countdown, never a
    /// missing row - but "unreadable" now means what `grants_common` means by
    /// it, which is strictly less than what a bare `%Y-%m-%d` parse meant. A
    /// US-format date used to vanish into `days_left: null`; the canonical door
    /// reads it, so the case is restated with a genuinely unparseable value and
    /// the old one is pinned to its new, better answer.
    #[test]
    fn unreadable_close_date_stays_on_the_page_as_nulls_not_a_gap() {
        let now = at("2026-10-01T09:00:00Z");

        let bad = closing_soon_grant(
            "eu-sedia:1",
            &json!({ "close_date": "Deadline\u{2014}see website" }),
            now,
        );
        assert_eq!(bad["key"], "eu-sedia:1");
        for field in ["days_left", "hours_left", "closes_at"] {
            assert!(
                bad[field].is_null(),
                "{field}: an unreadable deadline is null, not a missing row"
            );
        }
        assert_eq!(
            bad["lapsed"], false,
            "an unknown deadline is never a lapsed one"
        );
        assert_eq!(bad["close_date"], "Deadline\u{2014}see website");

        // The old expression returned null here; the canonical parser reads it.
        let us = closing_soon_grant("grants-gov:4", &json!({ "close_date": "10/01/2026" }), now);
        assert_eq!(us["days_left"], 0);
        assert_eq!(us["closes_at"], "2026-10-02T12:00:00Z");

        let good = closing_soon_grant("grants-gov:2", &json!({ "close_date": "2026-10-01" }), now);
        assert_eq!(good["days_left"], 0);
        assert_eq!(good["hours_left"], 27);
        assert_eq!(good["lapsed"], false);

        let bare = closing_soon_grant("ca-grants:3", &json!("not-an-object"), now);
        assert_eq!(bare["key"], "ca-grants:3");
        assert!(bare["days_left"].is_null());
    }

    /// THE defect, at the row level. A grants.gov grant printed `2026-10-04` is
    /// open in `grants/unified` until `2026-10-05T12:00:00Z`, and this view used
    /// to lose it at `00:00Z` - roughly twelve hours a day, in the direction that
    /// hides claimable money.
    ///
    /// The forbidden expression, named so a revert fails here: `days_left` must
    /// NOT be `(NaiveDate::parse_from_str(close_date) - now.date_naive())`,
    /// which is `-1` for this row and would have printed a negative day count or
    /// a dropped row.
    #[test]
    fn the_last_day_grant_is_present_and_counted_from_the_instant() {
        let now = at("2026-10-05T03:00:00Z");
        let row = closing_soon_grant(
            "grants-gov:1",
            &json!({ "status": "open", "close_date": "2026-10-04" }),
            now,
        );
        assert_eq!(row["closes_at"], "2026-10-05T12:00:00Z");
        assert_eq!(row["hours_left"], 9);
        assert_eq!(
            row["days_left"], 0,
            "claimable TODAY, not -1 and not absent"
        );
        assert_eq!(row["lapsed"], false);

        // And the window that selects it agrees, from both sides of midday.
        let bound = |now: &str| match &closing_soon_filters(at(now), 14)[1] {
            pumper_core::datasets::JsonFilter::Gte { path, value } => {
                assert_eq!(path, "$.close_date");
                value.clone()
            }
            other => panic!("the lower bound moved: {other:?}"),
        };
        assert_eq!(bound("2026-10-05T03:00:00Z"), "2026-10-04");
        assert_eq!(bound("2026-10-05T13:00:00Z"), "2026-10-05");
    }

    /// A zoned deadline is reported as the instant the source published, not
    /// rounded up to the anywhere-on-Earth end of its printed day.
    #[test]
    fn a_sedia_topic_is_counted_to_the_hour_not_to_the_day() {
        let now = at("2026-10-05T23:00:00Z");
        let row = closing_soon_grant(
            "eu-sedia:7",
            &json!({
                "status": "open",
                "close_date": "2026-10-06",
                "close_at": "2026-10-06T17:00:00Z",
            }),
            now,
        );
        assert_eq!(
            row["closes_at"], "2026-10-06T17:00:00Z",
            "the zoned instant, NOT 2026-10-07T12:00:00Z"
        );
        assert_eq!(row["hours_left"], 18);
        assert_eq!(row["days_left"], 0, "eighteen hours is not a day");
    }

    /// Sweep lag, visible. A row whose canonical deadline has passed while its
    /// stored `status` is still `open` is RETURNED and marked, never silently
    /// dropped - and the response's `expired_unswept` is exactly the count of
    /// those marks over the page.
    #[test]
    fn a_lapsed_row_is_marked_and_counted_not_dropped() {
        let now = at("2026-10-05T03:00:00Z");
        let page: Vec<_> = [
            json!({ "status": "open", "close_date": "2026-10-04", "close_at": "2026-10-04T23:00:00Z" }),
            json!({ "status": "open", "close_date": "2026-10-04" }),
            json!({ "status": "open", "close_date": "2026-10-20" }),
        ]
        .iter()
        .enumerate()
        .map(|(i, data)| closing_soon_grant(&format!("k{i}"), data, now))
        .collect();

        assert_eq!(
            page.len(),
            3,
            "every row the page selected stays on the page"
        );
        assert_eq!(
            page[0]["lapsed"], true,
            "its zoned deadline passed at 23:00Z"
        );
        assert_eq!(
            page[0]["hours_left"], 0,
            "a spent countdown is 0, never negative"
        );
        assert_eq!(
            page[1]["lapsed"], false,
            "no zoned instant: still in the tail"
        );
        assert_eq!(page[2]["lapsed"], false);

        let expired_unswept = page.iter().filter(|g| g["lapsed"] == json!(true)).count();
        assert_eq!(expired_unswept, 1);
    }

    /// `count` and `grants` are two reads of ONE predicate value. The route used
    /// to build the filter list inline and would have let a second clock reading
    /// or a second bound expression drift between them.
    #[test]
    fn count_and_page_cannot_select_different_sets() {
        let now = at("2026-10-05T03:00:00Z");
        let filters = closing_soon_filters(now, 14);
        assert_eq!(filters.len(), 3);
        assert_eq!(
            format!("{filters:?}"),
            format!("{:?}", closing_soon_filters(now, 14)),
            "pure in `now`"
        );
        // The route holds exactly one `filters` binding and hands the same
        // reference to `count_filtered` and `list_filtered_ordered`; this pins
        // the shape it is built from.
        let code = route_code();
        assert!(
            !code.contains("Utc::now().date_naive()"),
            "the forbidden expression: the window must not be re-derived from a bare date"
        );
        assert_eq!(
            code.matches("closing_soon_filters(now, days)").count(),
            1,
            "one predicate value, built once"
        );
    }

    /// The executable part of this module's own file: everything above the test
    /// module, with comment lines removed.
    ///
    /// Both exclusions are load-bearing, and the matcher above was briefly wrong
    /// without them. A source guard that reads raw text matches the doc comment
    /// that NAMES the anti-pattern, and it matches the assertion string in the
    /// guard itself - so the gate goes red on a clean file and tells you nothing.
    /// Stripping comments is the rule the repo already states for source-scanning
    /// gates; cutting the test module is the same rule applied to a guard that
    /// has to quote what it forbids.
    fn route_code() -> String {
        let src = include_str!("query.rs");
        let executable = src.split("#[cfg(test)]").next().unwrap_or(src);
        executable
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Seeded violation: the guard above only means something if it can go red.
    #[test]
    fn the_forbidden_expression_guard_can_still_fail() {
        let seeded = format!(
            "{}\nlet today = chrono::Utc::now().date_naive();",
            route_code()
        );
        assert!(
            seeded.contains("Utc::now().date_naive()"),
            "a reverted route must trip the guard"
        );
        // `parse_grant_date` still uses a `%Y-%m-%d` parse, and should: it
        // validates a user-supplied `closing_before` param, which IS a printed
        // date. What must not come back is that parse being used to DECIDE a
        // deadline, which is what the subtraction below spelled.
        assert!(
            !route_code().contains(".num_days()"),
            "the other half of the old rule - a date subtraction as the countdown - is gone"
        );
    }
}
