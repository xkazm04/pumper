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
    Ok(Json(json!({
        "read_only": true,
        "generated_at": now.to_rfc3339(),
        "healthy": findings.is_empty(),
        "findings": findings,
        "artifacts": artifacts,
        "search": facts.search,
        "tables": facts.tables,
        "coverage": facts.coverage,
        "thresholds": { "unbounded_growth_days": UNBOUNDED_GROWTH_DAYS },
    })))
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
