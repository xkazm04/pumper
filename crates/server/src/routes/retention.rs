//! Retention: the dry run, and the one plan builder the janitor also uses.
//!
//! `GET /retention/preview` answers "what would retention delete, and how many
//! bytes would that free, per app" **without deleting anything**. It is the same
//! [`pumper_core::retention`] calculation `retention_janitor` executes — shared
//! through [`artifact_retention_plan`] rather than reimplemented — so the preview
//! can never promise a different outcome than the janitor produces.
//!
//! The scan is a full walk of the artifact tree (documented in
//! `retention::scan_artifact_tree`). It runs on `spawn_blocking` and is
//! on-demand only: this endpoint and the six-hourly janitor tick, never a hot
//! path.

use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use pumper_core::retention::{plan_artifact_retention, ArtifactFile, RetentionPlan};
use pumper_core::Result as CoreResult;

use super::error::ApiError;
use crate::state::AppState;

/// Builds the artifact retention plan for a given age cutoff: read the pins from
/// the provenance graph, scan the tree, and let the pins veto the cutoff.
///
/// Returns the scanned files alongside the plan because executing a plan needs
/// the sizes, and re-scanning between plan and delete would open a window where
/// a body written in between is deleted without ever having been planned.
pub(crate) async fn artifact_retention_plan(
    state: &AppState,
    days: u64,
) -> CoreResult<(Vec<ArtifactFile>, RetentionPlan)> {
    let pinned = state.datasets.pinned_artifact_refs().await?;
    let root = state.storage.artifacts_dir.clone();
    let files =
        tokio::task::spawn_blocking(move || pumper_core::retention::scan_artifact_tree(&root))
            .await
            .map_err(|e| pumper_core::Error::App(format!("artifact scan panicked: {e}")))?;
    let cutoff = chrono::Utc::now() - chrono::Duration::days(days as i64);
    let protect_cassettes = !state.config.storage.artifact_retention_include_cassettes;
    let plan = plan_artifact_retention(&files, &pinned, cutoff, protect_cassettes);
    Ok((files, plan))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct RetentionPreviewQuery {
    /// Age cutoff in days to preview. Defaults to the configured
    /// `[storage] artifact_retention_days`; pass a value to model a window the
    /// deployment has not enabled.
    days: Option<u64>,
}

#[utoipa::path(
    get,
    path = "/retention/preview",
    tag = "retention",
    params(RetentionPreviewQuery),
    responses((status = 200, description = "Dry run: reclaimable bytes per app under the given \
        age cutoff, with pinned bytes broken out, plus current row counts of the append-only \
        ledgers and the configured retention windows. Deletes nothing.")),
)]
pub(crate) async fn retention_preview(
    State(state): State<AppState>,
    Query(q): Query<RetentionPreviewQuery>,
) -> Result<Json<Value>, ApiError> {
    let cfg = &state.config.storage;
    let days = q.days.unwrap_or(cfg.artifact_retention_days);
    let (_files, plan) = artifact_retention_plan(&state, days).await?;
    let ledgers: Vec<Value> = state
        .storage
        .ledger_row_counts()
        .await?
        .into_iter()
        .map(|(table, rows)| json!({ "table": table, "rows": rows }))
        .collect();
    Ok(Json(json!({
        "dry_run": true,
        "artifacts": {
            "root": state.storage.artifacts_dir.display().to_string(),
            "cutoff_days": days,
            "enabled": cfg.artifact_retention_days > 0,
            "cassettes_protected": !cfg.artifact_retention_include_cassettes,
            "total_files": plan.total_files,
            "total_bytes": plan.total_bytes,
            "reclaimable_files": plan.reclaimable_files,
            "reclaimable_bytes": plan.reclaimable_bytes,
            "pinned_files": plan.pinned_files,
            "pinned_bytes": plan.pinned_bytes,
            "per_app": plan.apps,
        },
        "ledgers": ledgers,
        "config": {
            "revision_retention_days": cfg.revision_retention_days,
            "artifact_retention_days": cfg.artifact_retention_days,
            "cost_event_retention_days": cfg.cost_event_retention_days,
            "webhook_delivery_retention_days": cfg.webhook_delivery_retention_days,
            "webhook_dead_letter_retention_days": cfg.webhook_dead_letter_retention_days,
            "job_yield_retention_days": cfg.job_yield_retention_days,
            "saved_search_seen_retention_days": cfg.saved_search_seen_retention_days,
        },
    })))
}
