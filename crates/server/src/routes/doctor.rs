//! `GET /datasets/doctor` — the store integrity report.
//!
//! This route only **gathers** facts; [`pumper_core::doctor::diagnose`] decides
//! what is worth saying about them, and that split is what lets the "a clean
//! store reports nothing" property be tested without a database.
//!
//! **Read-only.** Every query is a `SELECT`, every filesystem touch is a `stat`,
//! and nothing here writes, repairs or prunes — the report tells you what to run,
//! it never runs it. Several of the queries are **full scans** (`record_revisions`,
//! `records`, the whole artifact tree). That is why this is an on-demand operator
//! endpoint and is never called from the worker, the scheduler or any hot path.

use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use pumper_core::doctor::{
    diagnose, DatasetCoverage, MissingBody, SearchFacts, StoreFacts, TableGrowth,
    UNBOUNDED_GROWTH_DAYS,
};

use super::error::ApiError;
use crate::state::AppState;

/// Ceiling on the replayable revisions whose bodies are stat'd in one report.
/// The check is a `stat` per revision, so it is bounded on purpose: the report is
/// a health signal, not an exhaustive audit, and it says which it was.
const MAX_BODIES_CHECKED: i64 = 5_000;

/// Which `[storage]` key bounds each append-only table. Paired here rather than
/// in the config so a finding can name the exact key to set; `LEDGER_TABLES` is
/// the source of the table list, so a new table with no mapping shows up as
/// `""` rather than being silently dropped.
fn retention_key_for(table: &str) -> &'static str {
    match table {
        "cost_events" => "cost_event_retention_days",
        "webhook_deliveries" => "webhook_delivery_retention_days",
        "job_yield" => "job_yield_retention_days",
        "saved_search_seen" => "saved_search_seen_retention_days",
        "record_revisions" => "revision_retention_days",
        _ => "",
    }
}

fn configured_window(cfg: &pumper_core::config::StorageConfig, table: &str) -> u64 {
    match table {
        "cost_events" => cfg.cost_event_retention_days,
        // Delivered and dead have separate knobs; the table is bounded only when
        // BOTH terminal states are, since either alone leaves the other growing.
        "webhook_deliveries" => cfg
            .webhook_delivery_retention_days
            .min(cfg.webhook_dead_letter_retention_days),
        "job_yield" => cfg.job_yield_retention_days,
        "saved_search_seen" => cfg.saved_search_seen_retention_days,
        "record_revisions" => cfg.revision_retention_days,
        _ => 0,
    }
}

/// The search index's state, gathered from the two cheapest sources there are:
/// Tantivy's `doc_count` is a sum over segment metadata the reader already
/// holds, and the record side is one `COUNT(*)` aggregate — never a `list()`,
/// since this route already runs five full scans plus an artifact walk.
///
/// A `doc_count` that cannot be read is reported as `None` rather than `0`: the
/// doctor must not manufacture a finding out of its own failure to measure.
async fn search_facts(state: &AppState) -> Result<SearchFacts, ApiError> {
    Ok(SearchFacts {
        enabled: state.config.search.enabled,
        doc_count: state.search.doc_count().await.ok(),
        live_records: state.datasets.live_record_count().await?,
    })
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct DoctorQuery {
    /// Skip the artifact-tree walk and the per-revision body checks. Use on a
    /// very large archive when only the SQL-side checks are wanted.
    skip_artifacts: Option<bool>,
}

#[utoipa::path(
    get,
    path = "/datasets/doctor",
    tag = "datasets",
    params(DoctorQuery),
    responses((status = 200, description = "Read-only store integrity report: `findings` (each \
        with a concrete remediation; EMPTY on a healthy store), plus descriptive `coverage`, \
        `tables`, `search` (index enabled/doc_count vs live record count) and per-app `artifacts` \
        byte usage. Mutates and repairs nothing. Performs full scans — on-demand only.")),
)]
pub(crate) async fn datasets_doctor(
    State(state): State<AppState>,
    Query(q): Query<DoctorQuery>,
) -> Result<Json<Value>, ApiError> {
    let ds = &state.datasets;
    let skip_artifacts = q.skip_artifacts.unwrap_or(false);

    let mut facts = StoreFacts {
        half_stamped: ds.half_stamped_revisions().await?,
        unregistered_rules: ds.unregistered_rules_hashes().await?,
        null_simhash: ds.missing_simhash_counts().await?,
        search: Some(search_facts(&state).await?),
        orphan_derived: ds.orphan_derived_specs().await?,
        stale_rebuild_tables: state.storage.stale_rebuild_tables().await?,
        coverage: ds
            .provenance_coverage_by_dataset()
            .await?
            .into_iter()
            .map(
                |(app, dataset, revisions, with_job_id, replayable)| DatasetCoverage {
                    app,
                    dataset,
                    revisions,
                    with_job_id,
                    replayable,
                },
            )
            .collect(),
        ..Default::default()
    };

    let now = chrono::Utc::now();
    facts.tables = state
        .storage
        .ledger_stats()
        .await?
        .into_iter()
        .map(|s| TableGrowth {
            oldest_days: s.oldest.map(|o| (now - o).num_days()),
            retention_days: configured_window(&state.config.storage, &s.table),
            config_key: retention_key_for(&s.table),
            table: s.table,
            rows: s.rows,
        })
        .collect();

    // Artifact side: which provenance claims the filesystem can still back, and
    // what the tree costs per app (the numbers that make retention's decisions
    // inspectable).
    let mut artifacts = json!({ "scanned": false });
    if !skip_artifacts {
        let claims = ds.replayable_revisions(MAX_BODIES_CHECKED).await?;
        facts.replayable_checked = claims.len() as i64;
        let root = state.storage.artifacts_dir.clone();
        let (missing, usage) = tokio::task::spawn_blocking(move || {
            let missing: Vec<MissingBody> = claims
                .into_iter()
                .filter_map(|c| {
                    let path = c.reference.path(&root);
                    (!path.is_file()).then(|| MissingBody {
                        app: c.app,
                        dataset: c.dataset,
                        key: c.key,
                        revision: c.revision,
                        path: path.display().to_string(),
                    })
                })
                .collect();
            let files = pumper_core::retention::scan_artifact_tree(&root);
            // Pins are irrelevant to a usage report, so it is built with an empty
            // veto set — `artifact_usage` never proposes a deletion either way.
            let usage = pumper_core::retention::artifact_usage(&files, &Default::default());
            (missing, usage)
        })
        .await
        .map_err(|e| {
            ApiError(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("artifact scan panicked: {e}"),
            )
        })?;
        facts.missing_bodies = missing;
        artifacts = json!({
            "scanned": true,
            "root": state.storage.artifacts_dir.display().to_string(),
            "bodies_checked": facts.replayable_checked,
            "check_limit": MAX_BODIES_CHECKED,
            "per_app": usage,
            "total_bytes": usage.iter().map(|a| a.bytes).sum::<u64>(),
        });
    }

    let findings = diagnose(&facts);
    let instrument = state.storage.instrument();
    let store = store_report(
        &instrument.snapshot(),
        &state.storage.size_facts().await?,
        &instrument.recent_passes(),
        &pass_totals(&instrument),
        state.activity.raw(),
        instrument.pool_saturated(),
    );
    Ok(Json(json!({
        "read_only": true,
        "generated_at": now.to_rfc3339(),
        "healthy": findings.is_empty(),
        "findings": findings,
        "artifacts": artifacts,
        "search": facts.search,
        "tables": facts.tables,
        "coverage": facts.coverage,
        "store": store,
        "thresholds": { "unbounded_growth_days": UNBOUNDED_GROWTH_DAYS },
    })))
}

/// Every `(task, outcome)` pass total, for [`store_report`].
fn pass_totals(inst: &pumper_core::StoreInstrument) -> Vec<(&'static str, &'static str, u64)> {
    let mut out = Vec::new();
    for task in pumper_core::MaintenanceTask::ALL {
        for outcome in pumper_core::PassOutcome::ALL {
            out.push((
                task.as_str(),
                outcome.as_str(),
                inst.pass_count(*task, *outcome),
            ));
        }
    }
    out
}

fn ms(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

/// The store's on-demand diagnostic report: the pull-mode consumer of the
/// self-instrument.
///
/// This is what turns "it feels slow" into "the `records` table's write p95 is
/// 40x its baseline and 300 of the last 1,000 writes hit a lock" — the
/// difference between an afternoon of guessing and a one-line fix. It lives on
/// the existing read-only store-integrity route rather than in a new route
/// family, because that is already the surface an operator reaches for and
/// already the one `just doctor` points at.
///
/// **Every figure names its recomputation** (derivation-names-recomputation):
/// the `derived_by` block states, once, how each number was produced, so a
/// figure quoted out of this report can be re-derived and re-questioned rather
/// than believed. `p95_ms` is not "the p95" in the abstract — it is the
/// nearest-rank element of a 256-record window whose `samples` and
/// `window_secs` are printed beside it.
///
/// **The census is partial and says so.** `measured` enumerates exactly which
/// families are instrumented; `unmeasured` states in prose what is not, so
/// nobody reads a p95 here as a claim about the whole store.
///
/// Pure: takes the snapshot rather than the state, so the shape is tested
/// without a database, the same split `diagnose` uses.
fn store_report(
    reports: &[pumper_core::KeyReport],
    size: &pumper_core::StoreSize,
    passes: &[pumper_core::MaintenancePass],
    pass_totals: &[(&'static str, &'static str, u64)],
    activity_raw: i64,
    pool_saturated: bool,
) -> Value {
    let operations: Vec<Value> = reports
        .iter()
        .map(|r| {
            json!({
                "op": r.op.as_str(),
                "table": r.table,
                "phase": r.phase.as_str(),
                "samples": r.samples,
                "lifetime": r.lifetime,
                "window_wrapped": r.wrapped,
                "window_secs": r.window_secs,
                "window_rows": r.window_rows,
                "p50_ms": ms(r.p50_micros),
                "p95_ms": ms(r.p95_micros),
                "worst_ms": ms(r.worst_micros),
                "slow_line_ms": ms(r.slow_line_micros),
                "slow_lifetime": r.slow_lifetime,
                "busy_lifetime": r.busy_lifetime,
                "errors_lifetime": r.errors_lifetime,
                "rows_lifetime": r.rows_lifetime,
            })
        })
        .collect();
    let mut totals = serde_json::Map::new();
    for (task, outcome, n) in pass_totals {
        totals
            .entry(*task)
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .expect("object")
            .insert((*outcome).to_string(), json!(n));
    }
    json!({
        "measured": pumper_core::StoreOp::ALL
            .iter()
            .map(|op| op.as_str())
            .collect::<Vec<_>>(),
        "unmeasured": "A partial census, stated: only the families in `measured` are \
            instrumented. Schedules, watches, triggers, deliveries, saved searches, the HTTP \
            and research caches, and the search index are not measured at all — a percentile \
            here says nothing about them.",
        "derived_by": {
            "p50_ms": "sort the key's window ascending, take element ceil(0.50*n) — an \
                observed sample, never an interpolation",
            "p95_ms": "sort the key's window ascending, take element ceil(0.95*n); read it \
                with `samples`, since a p95 over 7 samples IS the 7th sample",
            "window_secs": "newest minus oldest record stamp in the key's window, whole \
                seconds; 0 means the window spans under a second, not that it is empty",
            "slow_lifetime": "operations at or past `slow_line_ms` for this key, counted \
                since process start — a LIFETIME count, unaffected by the window wrapping",
            "worst_ms": "the largest single duration since process start, kept outside the \
                window so 'worst ever' does not evaporate when the ring wraps",
            "phase": "`acquire` is the wait for a pooled connection, `execute` is the \
                statements; they are never summed, because one indicts pool sizing and the \
                other indicts the query",
            "busy_lifetime": "operations that ended in SQLITE_BUSY/SQLITE_LOCKED (or a pool \
                timeout on acquire), classified by the driver's result code, never by \
                message text",
            "size.main_bytes": "PRAGMA page_count x PRAGMA page_size — pages ALLOCATED, not \
                bytes in live rows; the difference is exactly `free_bytes`",
            "size.wal_bytes": "the -wal sidecar's size on disk (stat), which is the harm \
                figure the maintenance gate escalates on",
        },
        "size": size,
        // The gate's own inputs, so a deferral count that never stops climbing
        // can be diagnosed rather than guessed at. `inflight_raw` is the
        // UNCLAMPED counter on purpose: a negative reading is an unbalanced
        // guard — a real bug — and laundering it to 0 here would hide the one
        // failure that silently disables maintenance for the process's life.
        "activity": {
            "inflight": activity_raw.max(0),
            "inflight_raw": activity_raw,
            "pool_saturated": pool_saturated,
            "means": "in-flight foreground work: HTTP requests being handled plus jobs                 currently running. A pass runs only when this reads 0 (and the pool is not                 saturated) and the minimum interval has elapsed",
        },
        "operations": operations,
        "maintenance": {
            "passes": Value::Object(totals),
            "recent": passes,
            "outcomes_mean": "ran = it executed (work=0 is still a run), deferred = the \
                activity gate said busy and no escalation rung applied, failed = attempted \
                and errored",
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumper_core::config::StorageConfig;
    use pumper_core::LEDGER_TABLES;

    /// Every table the retention machinery knows about must map to a real config
    /// key, or a finding would tell the operator to set `""`. Inventory test, so
    /// adding a table to `LEDGER_TABLES` without a key fails here.
    #[test]
    fn every_ledger_table_names_the_key_that_bounds_it() {
        let cfg = StorageConfig::default();
        for table in LEDGER_TABLES {
            assert!(
                !retention_key_for(table).is_empty(),
                "no [storage] key mapped for {table}"
            );
            assert_eq!(
                configured_window(&cfg, table),
                0,
                "{table} must be unbounded by default"
            );
        }
    }

    /// The diagnostic surface's whole job is to make a number re-derivable. A
    /// figure with no stated recomputation is a number to be believed, which is
    /// exactly what "the store feels slow" conversations already have too many
    /// of — so every derived key in the report must appear in `derived_by`.
    #[test]
    fn every_derived_figure_names_its_recomputation() {
        let inst = pumper_core::StoreInstrument::new();
        inst.record(
            pumper_core::StoreOp::JobClaim,
            pumper_core::StorePhase::Execute,
            std::time::Duration::from_millis(7),
            1,
            pumper_core::OpOutcome::Ok,
        );
        let report = store_report(
            &inst.snapshot(),
            &pumper_core::StoreSize::default(),
            &inst.recent_passes(),
            &pass_totals(&inst),
            0,
            false,
        );
        let derived = report["derived_by"].as_object().expect("derived_by block");
        for figure in [
            "p50_ms",
            "p95_ms",
            "window_secs",
            "slow_lifetime",
            "worst_ms",
            "busy_lifetime",
        ] {
            let how = derived
                .get(figure)
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("{figure} is derived but names no recomputation"));
            assert!(
                how.len() > 20,
                "{figure}'s recomputation is not stated: {how}"
            );
        }
        // And the report actually carries the keys it explains.
        let ops = report["operations"].as_array().expect("operations");
        assert_eq!(
            ops.len(),
            pumper_core::StoreOp::ALL.len() * pumper_core::StorePhase::ALL.len(),
            "every key is reported, including the ones with no traffic"
        );
        let claim = ops
            .iter()
            .find(|o| o["op"] == "job_claim" && o["phase"] == "execute")
            .expect("the claim key");
        assert_eq!(claim["p95_ms"], 7.0);
        assert_eq!(claim["samples"], 1);
        assert_eq!(claim["table"], "jobs", "the join key with the table report");
        assert_eq!(
            claim["slow_line_ms"], 5.0,
            "the count carries its predicate"
        );
    }

    /// An honest partial census beats a fake total one. The report must name
    /// what it measures AND say in prose what it does not, or a p95 taken from
    /// seven families gets quoted as a claim about two hundred statements.
    #[test]
    fn the_report_admits_which_populations_it_does_not_measure() {
        let inst = pumper_core::StoreInstrument::new();
        let report = store_report(
            &inst.snapshot(),
            &pumper_core::StoreSize::default(),
            &[],
            &pass_totals(&inst),
            0,
            false,
        );
        let measured = report["measured"].as_array().expect("measured list");
        assert_eq!(measured.len(), pumper_core::StoreOp::ALL.len());
        let unmeasured = report["unmeasured"].as_str().expect("an unmeasured note");
        for absent in ["schedules", "triggers", "deliveries", "search"] {
            assert!(
                unmeasured.to_lowercase().contains(absent),
                "the note does not admit {absent}: {unmeasured}"
            );
        }
        // Maintenance totals are present for every pair even before anything
        // has run — "never ran" must be readable, not merely absent.
        let passes = report["maintenance"]["passes"]
            .as_object()
            .expect("pass totals");
        assert_eq!(passes.len(), pumper_core::MaintenanceTask::ALL.len());
        assert_eq!(passes["wal_checkpoint"]["deferred"], 0);
        assert_eq!(passes["wal_checkpoint"]["ran"], 0);
    }

    /// `webhook_deliveries` has two knobs and is bounded only when BOTH are set —
    /// bounding `delivered` alone still leaves the dead-letter tail growing.
    #[test]
    fn deliveries_count_as_bounded_only_when_both_knobs_are_set() {
        let mut cfg = StorageConfig {
            webhook_delivery_retention_days: 30,
            ..StorageConfig::default()
        };
        assert_eq!(configured_window(&cfg, "webhook_deliveries"), 0);
        cfg.webhook_dead_letter_retention_days = 90;
        assert_eq!(configured_window(&cfg, "webhook_deliveries"), 30);
    }
}
