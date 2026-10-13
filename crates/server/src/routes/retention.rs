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
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::IntoParams;

use pumper_core::retention::{plan_artifact_retention, ArtifactFile, RetentionPlan};
use pumper_core::Result as CoreResult;

use super::error::ApiError;
use crate::state::AppState;

/// Day count `Duration::days` can apply. `days` is a `u64` on the query and in
/// config; casting it with `as i64` wraps, and a wrapped negative count moves
/// the cutoff the other way. `Duration::days` panics when the span does not
/// fit in a `Duration`, so an oversized window is refused here instead.
pub(crate) fn retention_day_count(days: u64) -> Result<i64, String> {
    let days = i64::try_from(days)
        .map_err(|_| "retention days do not fit in a signed day count".to_string())?;
    // `Duration::days` panics when the span does not fit. `try_days` is that
    // check without the panic, so the preview can answer 400 and the janitor
    // can record a failure.
    if chrono::Duration::try_days(days).is_none() {
        return Err("retention days are outside the range a cutoff can express".into());
    }
    Ok(days)
}

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
    let cutoff = chrono::Utc::now()
        - chrono::Duration::days(retention_day_count(days).map_err(pumper_core::Error::App)?);
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
        age cutoff, plus current row counts of the append-only ledgers and the configured \
        retention windows. Deletes nothing. `artifacts` carries four byte/file classes that \
        **partition the tree exactly** — `reclaimable_*` (what a sweep would delete), `pinned_*` \
        (a live record or a replayable revision still addresses it; age cannot override this), \
        `cassette_*` (protected VCR cassettes, unless `artifact_retention_include_cassettes`) and \
        `within_window_*` (younger than the cutoff) — so `total_bytes` is fully accounted for and \
        a sweep that frees little is explainable. `per_app` repeats the same four classes.")),
)]
pub(crate) async fn retention_preview(
    State(state): State<AppState>,
    Query(q): Query<RetentionPreviewQuery>,
) -> Result<Json<Value>, ApiError> {
    let cfg = &state.config.storage;
    let days = q.days.unwrap_or(cfg.artifact_retention_days);
    // The plan builder refuses the same values. Checking here keeps the
    // refusal a 400 with the reason, instead of a redacted 500.
    if let Err(msg) = retention_day_count(days) {
        return Err(ApiError(StatusCode::BAD_REQUEST, msg));
    }
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
            // The other two classes, so the four `*_bytes` add up to
            // `total_bytes` exactly. Without them the difference read as bytes
            // nobody could account for — and cassettes and young bodies are
            // precisely what explains a sweep that frees less than expected.
            "cassette_files": plan.cassette_files,
            "cassette_bytes": plan.cassette_bytes,
            "within_window_files": plan.within_window_files,
            "within_window_bytes": plan.within_window_bytes,
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

#[cfg(test)]
mod retention_day_tests {
    use super::retention_day_count;

    /// The anti-pattern: `days as i64` wraps `u64::MAX` to `-1`, and
    /// `Duration::days` panics once the span does not fit in a `Duration`.
    /// Either one makes a preview describe the wrong window.
    #[test]
    fn retention_days_past_the_cutoff_range_are_refused() {
        assert_eq!(retention_day_count(90).unwrap(), 90);
        assert_eq!(retention_day_count(0).unwrap(), 0);
        assert!(retention_day_count(u64::MAX).is_err());
        // One past the widest span `Duration::try_days` accepts. That is the
        // input on which `Duration::days` panics.
        let mut too_wide = 1i64;
        while chrono::Duration::try_days(too_wide).is_some() {
            too_wide = too_wide.saturating_mul(2);
            if too_wide == i64::MAX {
                break;
            }
        }
        assert!(
            chrono::Duration::try_days(too_wide).is_none(),
            "expected a day count Duration cannot express"
        );
        assert!(retention_day_count(too_wide as u64).is_err());
        assert!(
            (u64::MAX as i64) < 0,
            "the old cast wraps, which is the window this test refuses"
        );
    }
}
