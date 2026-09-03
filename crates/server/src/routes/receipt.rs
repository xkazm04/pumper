//! Job receipt (`GET /jobs/{id}/receipt`): what one run cost, where its
//! wall-clock went, and what it actually changed — in one read-only document.
//!
//! Everything here already existed, spread across seven surfaces a caller had
//! to join by hand: the cost ledger (`cost_events`), M04 yield (`job_yield`),
//! the run's revisions (`record_revisions.job_id`), the contract verdicts held
//! in memory, the extraction-health verdicts (`source_runs`), the artifact
//! directory on disk, the delivery log, and the trigger hops. This joins them
//! **for one job**, each by an index seek on that job id (migration 0035) — a
//! receipt is a per-job audit view, not a metrics query, and it must not get
//! slower as the corpus grows.
//!
//! Honest nulls, following `provenance.rs`: a number this server cannot know is
//! `null` and the reason is named in `unknown[]`. Nothing is inferred, averaged
//! or back-filled. The three structural gaps, always reported when they apply:
//!
//! - **Stage timings** exist only for runs that completed their fan-out after
//!   migration 0034; an older or failed job has `stages: null`.
//! - **Revisions** are counted from the `job_id` provenance stamp (0030). A
//!   write path that doesn't stamp one is invisible here — the receipt says so
//!   rather than falling back to "revisions in this app during the run window",
//!   which would attribute another job's writes to this one.
//! - **Watch and saved-search deliveries** are logged against the watch /
//!   search id, not the job, so they cannot be attributed to a run at all.

use std::collections::BTreeMap;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::{Job, JobStatus};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::routes::error::ApiError;
use crate::state::AppState;

/// Ceiling on artifact entries listed. A job that wrote more than this gets a
/// truncation flag rather than an unbounded directory dump.
const MAX_ARTIFACTS: usize = 200;

#[utoipa::path(
    get,
    path = "/jobs/{id}/receipt",
    tag = "jobs",
    params(("id" = Uuid, Path, description = "Job id")),
    responses(
        (status = 200, description = "`{job, stages, cost, yield, changes, verdicts, artifacts, \
            deliveries, trigger_hops, unknown}` — one run's cost, per-stage wall-clock, and \
            what it changed. `cost.egress` is `[{node, calls}]`: which remote-fabric peer nodes \
            this run's fetches actually left from (empty when nothing went through a peer, i.e. \
            always on a deployment with `[remote]` off). Any figure this server cannot know is \
            `null` and the reason is listed in `unknown`; nothing is inferred."),
        (status = 404, description = "Job not found", body = Object),
    )
)]
pub(crate) async fn job_receipt(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let job = state
        .storage
        .get(id)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "job not found".into()))?;
    // Reasons this receipt is incomplete, in the caller's words rather than as
    // a silent null.
    let mut unknown: Vec<String> = Vec::new();

    // ── cost ────────────────────────────────────────────────────────────────
    let events = state.costs.job_events(id).await?;
    let mut by_engine: BTreeMap<String, (i64, f64)> = BTreeMap::new();
    for e in &events {
        let slot = by_engine.entry(e.engine.clone()).or_insert((0, 0.0));
        slot.0 += 1;
        slot.1 += e.cost_usd;
    }
    let total_usd: f64 = events.iter().map(|e| e.cost_usd).sum();
    let cost = json!({
        "total_usd": total_usd,
        "calls": events.len(),
        "budget_usd": job.budget_usd,
        "by_engine": by_engine.into_iter().map(|(engine, (calls, cost_usd))| {
            json!({ "engine": engine, "calls": calls, "cost_usd": cost_usd })
        }).collect::<Vec<_>>(),
        // Which peer nodes this run's fetches actually left from. `by_engine`
        // says "http" whether a body came off this machine or a peer in another
        // country, which is precisely the thing the remote fabric exists to
        // change — so a run that used it has to be able to show it.
        "egress": egress_by_node(&events),
    });
    if events.is_empty() {
        // What an empty ledger actually means. The previous wording here said
        // "free tiers (http, cached, replayed) do not write ledger rows" — which
        // is false: `AppContext::fetch` meters EVERY fetch, free ones as $0.00
        // rows carrying the engine, the URL and the trail. So an empty ledger
        // means this run made no metered engine call *through the AppContext
        // seam* at all — an app that reached `ctx.engines.http` directly, or one
        // that fetched nothing.
        unknown.push(
            "cost: this run wrote no ledger rows. Every fetch through AppContext::fetch writes \
             one (free tiers as $0.00), so this means the run made no such call — an app that \
             used a raw engine handle instead of the metered seam, or one that fetched nothing. \
             It does NOT mean 'fetched but not priced'."
                .into(),
        );
    }

    // ── stage timings (W-D) ─────────────────────────────────────────────────
    let stages = state.storage.job_stages(id).await?;
    if stages.is_none() {
        unknown.push(stage_gap_reason(&job));
    }

    // ── yield (what the result reported) ────────────────────────────────────
    let yields = state.storage.job_yield_entries(id).await?;

    // ── changes (what the revision log records) ─────────────────────────────
    let counts = state.storage.job_revision_counts(id).await?;
    let mut changes: BTreeMap<(String, String), Map<String, Value>> = BTreeMap::new();
    for c in &counts {
        let row = changes
            .entry((c.app.clone(), c.dataset.clone()))
            .or_default();
        row.insert(c.change.clone(), json!(c.count));
    }
    let changes: Vec<Value> = changes
        .into_iter()
        .map(|((app, dataset), by_change)| {
            let total: i64 = by_change.values().filter_map(Value::as_i64).sum();
            json!({ "app": app, "dataset": dataset, "total": total, "by_change": by_change })
        })
        .collect();
    if changes.is_empty() && !yields.is_empty() {
        unknown.push(
            "changes: no revision carries this job's provenance stamp, yet the result reported \
             yield counts — the writing path did not stamp `job_id` (or predates migration \
             0030). The `yield` block is what the result claimed; it is not corroborated here."
                .into(),
        );
    }

    // ── verdicts ────────────────────────────────────────────────────────────
    let health: Vec<_> = state.storage.job_health_verdicts(id).await?;
    // Contract verdicts live in memory, keyed `<app>/<dataset>`, and are
    // overwritten by each run — keep only the ones this job actually produced.
    let contracts = contract_verdicts_for(&state, id);
    if health.is_empty() {
        unknown.push(
            "verdicts.health: this run recorded no extraction-health verdict (detection off, \
             or the app never called observe_extraction). The source's CURRENT state is not \
             reported here — it may have changed since this run."
                .into(),
        );
    }
    if contracts.is_empty() {
        unknown.push(
            "verdicts.contracts: none recorded for this job. Contract verdicts are in-memory \
             and per-run — a server restart, or a later run of the same dataset, erases this \
             job's verdict entirely."
                .into(),
        );
    }

    // ── artifacts ───────────────────────────────────────────────────────────
    let dir = state
        .storage
        .artifacts_dir
        .join(&job.app)
        .join(id.to_string());
    let artifacts = match read_artifacts(&dir).await {
        Ok(Some((files, total_bytes, truncated))) => json!({
            "dir": dir.to_string_lossy(),
            "files": files,
            "count": files_len(&files),
            "total_bytes": total_bytes,
            "truncated": truncated,
        }),
        Ok(None) => {
            unknown.push(format!(
                "artifacts: no directory at {} — this run saved none, or they were pruned",
                dir.to_string_lossy()
            ));
            Value::Null
        }
        Err(e) => {
            unknown.push(format!(
                "artifacts: {} could not be read ({e}); the size total would be a guess",
                dir.to_string_lossy()
            ));
            Value::Null
        }
    };

    // ── outbound ────────────────────────────────────────────────────────────
    let deliveries = state.storage.job_deliveries(id).await?;
    unknown.push(
        "deliveries: only this job's own callback and the global failure firehose are keyed by \
         job id. Watch (`dataset.changed`) and saved-search (`search.matched`) deliveries are \
         logged against the watch / search id, so no delivery can be attributed to the run \
         that caused it — see GET /webhooks/deliveries."
            .into(),
    );
    let hops = state.storage.triggered_hops(id).await?;
    if hops.is_empty() && job.status == JobStatus::Succeeded {
        unknown.push(
            "trigger_hops: none recorded. Either this run fired no trigger, or it ran before \
             migration 0035 added the `source_job_id` lineage column (older hops are not \
             recoverable)."
                .into(),
        );
    }

    Ok(Json(json!({
        "job": {
            "id": job.id,
            "app": job.app,
            "status": job.status,
            "attempts": job.attempts,
            "max_attempts": job.max_attempts,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "wall_ms": wall_ms(&job),
            "schedule_id": job.schedule_id,
            "trigger_id": job.trigger_id,
            "error": job.error,
        },
        "stages": stages,
        "cost": cost,
        "yield": yields,
        "changes": changes,
        "verdicts": { "health": health, "contracts": contracts },
        "artifacts": artifacts,
        "deliveries": deliveries,
        "trigger_hops": hops.iter().map(|h| json!({
            "job_id": h.id,
            "app": h.app,
            "trigger_id": h.trigger_id,
            "status": h.status,
            "created_at": h.created_at,
        })).collect::<Vec<_>>(),
        "unknown": unknown,
    })))
}

/// Which remote-fabric nodes served this run's fetches, `{node: calls}`, sorted
/// by node. Empty when nothing left through a peer — including every run on a
/// deployment with `[remote]` off, which is the overwhelmingly common case.
///
/// **Why this reads a marker out of `detail`.** A peer-served fetch leaves one
/// trail line, `"<EGRESS_TRAIL_PREFIX><node>"`, which `AppContext::fetch` folds
/// into that fetch's `cost_events.detail` exactly as it folds an archive
/// snapshot's note. There is one writer (the fetcher's HTTP-tier seam) and one
/// reader (here), both through `pumper_core::fetcher::EGRESS_TRAIL_PREFIX` — the
/// same single-constant discipline the archive provenance headers use, and the
/// reason this is a marker contract rather than "parsing the escalation prose".
///
/// The structurally better shape is a typed `served_by` on `FetchOutcome`
/// alongside `snapshot`, read by `fetch_cost_detail`. That needs the literal
/// `FetchOutcome`/`TierTrace` constructions in `core/src/app.rs`, `core/src/vcr.rs`
/// and `apps/provisioner` updated in the same change — see the known gap in
/// `docs/features/fetching.md`.
fn egress_by_node(events: &[pumper_core::CostEvent]) -> Vec<Value> {
    let mut by_node: BTreeMap<&str, i64> = BTreeMap::new();
    for node in events
        .iter()
        .filter_map(|e| e.detail.as_deref())
        .flat_map(egress_nodes)
    {
        *by_node.entry(node).or_insert(0) += 1;
    }
    by_node
        .into_iter()
        .map(|(node, calls)| json!({ "node": node, "calls": calls }))
        .collect()
}

/// Every node named by egress markers in one cost event's `detail`.
///
/// `detail` is a `"; "`-joined trail, so the marker is matched at a **segment**
/// boundary rather than anywhere in the string: a target URL or an error message
/// that happens to contain the phrase must not be mistaken for provenance.
fn egress_nodes(detail: &str) -> impl Iterator<Item = &str> {
    detail.split("; ").filter_map(|part| {
        part.strip_prefix(pumper_core::fetcher::EGRESS_TRAIL_PREFIX)
            .map(str::trim)
            .filter(|node| !node.is_empty())
    })
}

/// A job's queue-visible wall clock, or `None` when it hasn't both started and
/// finished.
///
/// Deliberately **not** "now minus started_at" for a running job: that is a
/// reading of the clock, not of the run, and it would turn a stuck job's
/// receipt into a number that grows every time you refresh it.
fn wall_ms(job: &Job) -> Option<i64> {
    let (start, end) = (job.started_at?, job.finished_at?);
    Some((end - start).num_milliseconds())
}

/// Why a job has no stage row — the distinction matters: an old job was never
/// measured, a failed job never reached the stages, and a running one hasn't
/// finished them.
fn stage_gap_reason(job: &Job) -> String {
    match job.status {
        JobStatus::Succeeded => "stages: not recorded for this run — it completed before stage \
             timings existed (migration 0034), or its fan-out did not finish"
            .into(),
        JobStatus::Running | JobStatus::Queued => {
            "stages: the run has not finished; timings are stamped at the end of its fan-out".into()
        }
        _ => format!(
            "stages: a {} job has no fan-out, so only a total would be measurable and no stage \
             breakdown exists",
            job.status.as_str()
        ),
    }
}

/// The in-memory contract verdicts produced by THIS job. The map holds the
/// latest verdict per `<app>/<dataset>` regardless of which run wrote it, so
/// filtering on the stamped `job_id` is what keeps a neighbouring run's verdict
/// out of this receipt.
fn contract_verdicts_for(state: &AppState, id: Uuid) -> Vec<Value> {
    let map = super::error::lock_advisory(&state.contract_verdicts, "contract_verdicts");
    let wanted = id.to_string();
    let mut out: Vec<Value> = map
        .iter()
        .filter(|(_, v)| v.get("job_id").and_then(Value::as_str) == Some(wanted.as_str()))
        .map(|(key, v)| {
            let mut row = v.clone();
            if let Value::Object(obj) = &mut row {
                obj.insert("source".into(), Value::String(key.clone()));
            }
            row
        })
        .collect();
    out.sort_by_key(|v| {
        v.get("source")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    });
    out
}

fn files_len(files: &Value) -> usize {
    files.as_array().map(Vec::len).unwrap_or(0)
}

/// Lists a job's artifact directory with each file's byte size.
///
/// `Ok(None)` = the directory does not exist (nothing was saved, or it was
/// pruned) — distinct from `Err` (it exists but could not be read), because
/// only the first is an honest "this run wrote none". Sizes come from the
/// filesystem, never from a stored guess.
async fn read_artifacts(dir: &std::path::Path) -> std::io::Result<Option<(Value, u64, bool)>> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut files: Vec<(String, u64)> = Vec::new();
    let mut total = 0u64;
    let mut truncated = false;
    while let Some(entry) = entries.next_entry().await? {
        let meta = entry.metadata().await?;
        if !meta.is_file() {
            continue;
        }
        total += meta.len();
        if files.len() < MAX_ARTIFACTS {
            files.push((entry.file_name().to_string_lossy().into_owned(), meta.len()));
        } else {
            truncated = true;
        }
    }
    files.sort();
    let listed = files
        .into_iter()
        .map(|(name, bytes)| json!({ "name": name, "bytes": bytes }))
        .collect::<Vec<_>>();
    Ok(Some((Value::Array(listed), total, truncated)))
}

#[cfg(test)]
mod tests {
    use super::{egress_by_node, egress_nodes, stage_gap_reason, wall_ms};
    use chrono::{Duration, Utc};
    use pumper_core::{CostEvent, Job, JobStatus};

    fn event(detail: Option<&str>) -> CostEvent {
        CostEvent {
            job_id: uuid::Uuid::nil().to_string(),
            app: "fake".into(),
            engine: "http".into(),
            url: Some("https://example.test/p".into()),
            cost_usd: 0.0,
            detail: detail.map(str::to_string),
            created_at: Utc::now(),
        }
    }

    /// The anti-pattern: **a marker matched anywhere in a string**. `detail` is a
    /// `"; "`-joined trail that also carries target URLs and raw error text, so
    /// a substring search would happily read provenance out of an error message
    /// that merely quotes the phrase. Segment boundaries are the contract.
    #[test]
    fn an_egress_marker_is_read_at_a_segment_boundary_not_anywhere_in_the_text() {
        let real: Vec<&str> =
            egress_nodes("egress via remote node http://node-b:8088; http tier thin: status 200")
                .collect();
        assert_eq!(real, ["http://node-b:8088"]);

        // Mid-segment prose that quotes the phrase is NOT provenance.
        let quoted: Vec<&str> =
            egress_nodes("http tier error: the string 'egress via remote node x' was logged")
                .collect();
        assert!(quoted.is_empty(), "{quoted:?}");

        // An empty node name says nothing, so it is not attribution.
        assert!(egress_nodes("egress via remote node ").next().is_none());
        assert!(egress_nodes("").next().is_none());
    }

    /// The overwhelmingly common case — `[remote]` off — must produce an empty
    /// block, never a null or a fabricated "local" node.
    #[test]
    fn a_run_that_used_no_peer_reports_an_empty_egress_block() {
        assert!(egress_by_node(&[]).is_empty());
        assert!(
            egress_by_node(&[event(None), event(Some("http tier thin: status 200"))]).is_empty()
        );
    }

    #[test]
    fn egress_is_counted_per_node_across_a_runs_fetches() {
        let rows = egress_by_node(&[
            event(Some("egress via remote node http://b:2")),
            event(Some("egress via remote node http://a:1")),
            event(Some("egress via remote node http://b:2")),
            event(None),
        ]);
        assert_eq!(rows.len(), 2);
        // Sorted by node, so a receipt does not shuffle between reads.
        assert_eq!(rows[0]["node"], "http://a:1");
        assert_eq!(rows[0]["calls"], 1);
        assert_eq!(rows[1]["node"], "http://b:2");
        assert_eq!(rows[1]["calls"], 2);
    }

    fn job(status: JobStatus) -> Job {
        let now = Utc::now();
        Job {
            id: uuid::Uuid::new_v4(),
            app: "fake".into(),
            params: serde_json::json!({}),
            status,
            attempts: 1,
            max_attempts: 1,
            priority: 0,
            callback_url: None,
            callback_secret: None,
            budget_usd: None,
            schedule_id: None,
            trigger_id: None,
            target_key: None,
            result: None,
            error: None,
            created_at: now,
            available_at: now,
            started_at: None,
            finished_at: None,
        }
    }

    /// The anti-pattern: a receipt that reports an unfinished job's elapsed
    /// time as if it were a measured duration, so the same job "costs" more
    /// every time the page is refreshed.
    #[test]
    fn an_unfinished_run_has_no_wall_clock_rather_than_a_growing_one() {
        let mut running = job(JobStatus::Running);
        running.started_at = Some(Utc::now() - Duration::seconds(90));
        assert_eq!(wall_ms(&running), None);

        let mut queued = job(JobStatus::Queued);
        queued.finished_at = None;
        assert_eq!(wall_ms(&queued), None);

        let mut done = job(JobStatus::Succeeded);
        let start = Utc::now();
        done.started_at = Some(start);
        done.finished_at = Some(start + Duration::milliseconds(1500));
        assert_eq!(wall_ms(&done), Some(1500));
    }

    /// A missing stage row has three different causes and the receipt must not
    /// collapse them into one shrug.
    #[test]
    fn a_missing_stage_row_names_which_kind_of_missing_it_is() {
        let succeeded = stage_gap_reason(&job(JobStatus::Succeeded));
        let running = stage_gap_reason(&job(JobStatus::Running));
        let failed = stage_gap_reason(&job(JobStatus::Failed));
        assert!(succeeded.contains("0034"), "{succeeded}");
        assert!(running.contains("has not finished"), "{running}");
        assert!(failed.contains("failed"), "{failed}");
        assert_ne!(succeeded, running);
        assert_ne!(running, failed);
    }
}
