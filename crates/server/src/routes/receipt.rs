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
use pumper_core::{Job, JobStatus, RevisionCount};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::routes::error::ApiError;
use crate::routes::jobs::roll_up;
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
            what it changed. `coverage` diffs the two: `declared` is the namespaces the run's \
            `index_datasets` named, `undeclared` is every `(app, dataset)` outside the \
            job's own app that the run wrote WITHOUT naming — records no watch, dataset \
            trigger or saved search scoped to that namespace can reach. \
            `coverage.complete: false` means the diff could not be computed, so an empty \
            `undeclared` is not a clean bill. `cost.egress` is `[{node, calls}]`: which remote-fabric peer nodes \
            this run's fetches actually left from (empty when nothing went through a peer, i.e. \
            always on a deployment with `[remote]` off). Any figure this server cannot know is \
            `null` and the reason is listed in `unknown`; nothing is inferred.", body = crate::routes::dto::JobReceipt),
        (status = 404, description = "Job not found", body = crate::routes::dto::ErrorEnvelope),
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
    let (cost, floor_note) = cost_block(&events, job.budget_usd);
    if let Some(note) = floor_note {
        unknown.push(note);
    }
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

    // ── coverage (what it WROTE vs what it DECLARED) ──────────────────
    // Both populations above already existed and were never compared. The
    // declaration is the run's own `index_datasets` (the same parse the worker
    // does); the observation is `counts`, already read for the `changes` block
    // — no second query. A namespace written and not declared reaches no watch,
    // no dataset trigger and no saved search scoped to it, and until this block
    // the only detector for that was a human noticing.
    let index_specs =
        crate::datahub::index_dataset_specs(job.result.as_ref().unwrap_or(&Value::Null));
    let declared_apps = crate::worker::run_indexed_apps(&job.app, &index_specs);
    let observed: Vec<(String, String)> = counts
        .iter()
        .map(|c| (c.app.clone(), c.dataset.clone()))
        .collect();
    let coverage_complete = coverage_is_complete(&job, &counts);
    let coverage = coverage_block(
        &declared_apps,
        &job.app,
        &index_specs,
        &observed,
        coverage_complete,
    );
    if !coverage_complete {
        unknown.push(coverage_gap_reason(&job));
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
            // N18: WHERE this job ran. `null` = this coordinator's own worker,
            // which is every job on a single-process install.
            "executor_id": job.executor_id,
            "error": job.error,
            // The one place a human deliberately READS a failure rather than a
            // machine classifying it, so it is where a lost cause type costs
            // most: `error` is the sentence, `cause_kind` is what produced it
            // (`toml::de::Error`, `reqwest::Error`). `null` = the failure
            // carried no typed cause, which is most of them.
            "cause_kind": job.cause_kind,
            "requeue_reason": job.requeue_reason,
        },
        "stages": stages,
        "cost": cost,
        "yield": yields,
        "changes": changes,
        "coverage": coverage,
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

/// The receipt's `cost` block, plus the one `unknown[]` line the money owes
/// the reader when the total is a floor rather than a price (`None` when the
/// total is exact — an exact total must not be footnoted, because a footnote on
/// every receipt is a footnote on none).
///
/// Extracted from the handler so the arithmetic this document publishes is
/// testable without a database, and so there is ONE place where the receipt's
/// money is shaped.
fn cost_block(
    events: &[pumper_core::CostEvent],
    budget_usd: Option<f64>,
) -> (Value, Option<String>) {
    // ONE summer, shared with GET /jobs/{id}/costs. The per-engine breakdown
    // goes through it too — grouping first and then folding `cost_usd` by hand
    // is the same bug one indentation level down.
    let roll = roll_up(events);
    let mut by_engine: BTreeMap<&str, Vec<&pumper_core::CostEvent>> = BTreeMap::new();
    for e in events {
        by_engine.entry(e.engine.as_str()).or_default().push(e);
    }
    let mut cost = json!({
        // Unchanged, and still the plain sum of what we can price: a consumer
        // reading `total_usd` keeps the number it has always read. What is new
        // is that the document now says how to read it.
        "total_usd": roll.billed_usd,
        "calls": events.len(),
        "budget_usd": budget_usd,
        "by_engine": by_engine.into_iter().map(|(engine, group)| {
            let calls = group.len() as i64;
            let group = roll_up(group);
            json!({ "engine": engine, "calls": calls, "cost_usd": group.billed_usd })
        }).collect::<Vec<_>>(),
        // Which peer nodes this run's fetches actually left from. `by_engine`
        // says "http" whether a body came off this machine or a peer in another
        // country, which is precisely the thing the remote fabric exists to
        // change — so a run that used it has to be able to show it.
        "egress": egress_by_node(events),
        // N15: how many of this run's fetches the MODEL drove, through the
        // self-hosted `fetch` MCP tool, rather than the app's own code. Always
        // present and always a number: `0` on every ordinary run is the honest
        // answer, and it is what makes a run where the Claude tier fetched
        // through the ladder distinguishable from one where it fetched around
        // it (which, before N15, every research run did).
        "self_hosted_fetches": self_hosted_fetches(events),
    });
    crate::routes::jobs::merge_certainty_fields(&mut cost, &roll);
    (cost, cost_floor_note(&roll))
}

/// The `unknown[]` line a floored total owes the reader, or `None` when the
/// total is exact.
///
/// This module's contract is that a number this server cannot know is `null`
/// with its reason named — and `total_usd` was the one number in the document
/// that was exempt from it. It is not nulled (a floor is real information), it
/// is **labelled**: the counts are in `cost`, and the sentence is here.
///
/// An exact total gets no entry. A footnote on every receipt is a footnote on
/// none, and the `unknown[]` list is read by humans deciding what to distrust.
fn cost_floor_note(roll: &pumper_core::costs::CostRollup) -> Option<String> {
    if roll.uncertain_calls() == 0 {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    if roll.unmetered_calls > 0 {
        parts.push(format!(
            "{} unmetered (a paid call whose bill this server cannot read)",
            roll.unmetered_calls
        ));
    }
    if roll.unknown_calls > 0 {
        parts.push(format!(
            "{} unknown (a call that may or may not have been billed at all)",
            roll.unknown_calls
        ));
    }
    Some(format!(
        "cost: total_usd is a floor, not a price — this run holds {}. The run cost AT LEAST          ${:.4}; how much more is not knowable from the ledger. See cost.total_certainty          ({}) and the per-call spend_certainty in GET /jobs/{{id}}/costs.",
        parts.join(" and "),
        roll.billed_usd,
        roll.total_certainty(),
    ))
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
/// Fetches this run's Claude subprocess made for itself through the MCP `fetch`
/// tool (N15), counted off the zero-cost marker rows that tool writes beside
/// the real ledger row.
///
/// Deliberately counts the marker and not the engine: `AppContext::fetch` also
/// writes a priced row for the same fetch, and summing both would double-count
/// the run's egress in the one place an operator goes to find out what it cost.
fn self_hosted_fetches(events: &[pumper_core::CostEvent]) -> usize {
    events
        .iter()
        .filter(|e| e.detail.as_deref() == Some(crate::mcp::SELF_HOSTED_FETCH_DETAIL))
        .count()
}

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

/// The write/declaration diff for one run: what its result said it would put
/// records under, and which `(app, dataset)` pairs it actually wrote that no
/// declaration names.
///
/// `declared` is `run_indexed_apps(job.app, index_specs)` — the namespaces the
/// worker scopes the hook batch and the saved searches by. `declared_datasets`
/// is the exact spec list, because a declared APP does not launder an
/// undeclared DATASET inside it: per-record search documents are minted per
/// spec, so an unnamed dataset is unsearchable even in a namespace that is
/// otherwise reachable.
fn coverage_block(
    declared_apps: &[String],
    job_app: &str,
    declared_specs: &[(String, String)],
    observed: &[(String, String)],
    complete: bool,
) -> Value {
    let undeclared: Vec<Value> =
        crate::worker::undeclared_write_namespaces(observed, job_app, declared_specs)
            .into_iter()
            .map(|(app, dataset)| json!({ "app": app, "dataset": dataset }))
            .collect();
    json!({
        "declared": declared_apps,
        "declared_datasets": declared_specs
            .iter()
            .map(|(app, dataset)| json!({ "app": app, "dataset": dataset }))
            .collect::<Vec<_>>(),
        "undeclared": undeclared,
        // Whether the diff may be trusted at all. An empty `undeclared` means
        // "nothing rogue" only when this is true — otherwise it means "this
        // receipt cannot see what the run wrote", which is a different fact.
        "complete": complete,
    })
}

/// Whether the coverage diff is trustworthy. It rests entirely on the `job_id`
/// provenance stamp (migration 0030), so it is blind in exactly two situations
/// and must say so rather than report a clean bill. See `coverage_gap_reason`.
fn coverage_is_complete(job: &Job, counts: &[RevisionCount]) -> bool {
    // An unfinished or failed run's writes are not final; there is no verdict
    // to give yet.
    if job.status != JobStatus::Succeeded {
        return false;
    }
    // No stamped revision at all. "Wrote nothing" and "wrote through a path
    // that does not stamp `job_id`" are indistinguishable from here, and
    // reporting the second as declared-clean is exactly the fail-silent this
    // block exists to end. ONE stamped revision is enough to make the diff
    // mean something; zero is not. Measured cost of getting this wrong: the
    // first draft of this block called the MPSV e2e run declared-clean while
    // seeing none of its four writes, because the producer wrote unstamped.
    !counts.is_empty()
}

/// Which kind of blindness `coverage.complete: false` is. Two causes, never
/// collapsed into one shrug — same discipline as `stage_gap_reason`.
fn coverage_gap_reason(job: &Job) -> String {
    if job.status != JobStatus::Succeeded {
        format!(
            "coverage: a {} job's writes are not final, so the write/declaration diff is not a \
             verdict. `coverage.undeclared` being empty here does NOT mean this run stayed \
             inside its declaration.",
            job.status.as_str()
        )
    } else {
        "coverage: no revision carries this job's provenance stamp (migration 0030), so the \
         observed set is empty for one of two reasons this server cannot tell apart — the run \
         wrote nothing, or it wrote through a path that does not stamp `job_id`. An empty \
         `coverage.undeclared` is therefore not a clean bill."
            .into()
    }
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
    use super::{
        cost_block, coverage_block, coverage_gap_reason, coverage_is_complete, egress_by_node,
        egress_nodes, self_hosted_fetches, stage_gap_reason, wall_ms,
    };
    use crate::routes::jobs::{certainty_fields, roll_up};
    use chrono::{Duration, Utc};
    use pumper_core::costs::SpendCertainty;
    use pumper_core::{CostEvent, Job, JobStatus, RevisionCount};
    use serde_json::json;

    /// One priced call on the ledger, with the certainty the engine reported.
    fn priced(engine: &str, cost_usd: f64, spend_certainty: SpendCertainty) -> CostEvent {
        CostEvent {
            job_id: uuid::Uuid::nil().to_string(),
            app: "fake".into(),
            engine: engine.into(),
            url: None,
            cost_usd,
            spend_certainty,
            detail: None,
            created_at: Utc::now(),
        }
    }

    /// Acceptance 1. The receipt's `cost` block carries the certainty the
    /// ledger row already holds. A run that spent $0.30 and then had a call
    /// killed mid-generation is NOT a $0.30 run.
    #[test]
    fn a_receipt_whose_total_is_a_floor_publishes_the_counts_that_make_it_one() {
        let events = [
            priced("claude", 0.30, SpendCertainty::Billed),
            priced("claude", 0.0, SpendCertainty::Unmetered),
        ];
        let (cost, _) = cost_block(&events, None);
        assert_eq!(cost["billed_usd"], json!(0.30));
        assert_eq!(cost["unmetered_calls"], json!(1));
        assert_eq!(cost["unknown_calls"], json!(0));
        assert_eq!(cost["total_certainty"], json!("at_least"));
    }

    /// Acceptance 2. The module's own doctrine — "a number this server cannot
    /// know is `null` and the reason is named in `unknown[]`" — applied to the
    /// one number it used to exempt.
    #[test]
    fn a_floored_total_names_its_reason_in_unknown_rather_than_passing_as_a_price() {
        let events = [
            priced("claude", 0.30, SpendCertainty::Billed),
            priced("claude", 0.0, SpendCertainty::Unmetered),
        ];
        let note = cost_block(&events, None).1.expect("a floor is footnoted");
        assert!(note.starts_with("cost:"), "{note}");
        assert!(note.contains("unmetered"), "{note}");
        assert!(
            note.contains("at least") || note.contains("floor"),
            "the note has to say the total is a lower bound: {note}"
        );
    }

    /// Acceptance 3. Over-labelling is its own dishonesty: an exact total must
    /// not be footnoted, because a footnote on every receipt is a footnote on
    /// none.
    #[test]
    fn an_exact_total_is_not_footnoted() {
        let events = [
            priced("claude", 0.30, SpendCertainty::Billed),
            priced("http", 0.0, SpendCertainty::Free),
        ];
        let (cost, note) = cost_block(&events, None);
        assert_eq!(cost["total_certainty"], json!("exact"));
        assert_eq!(note, None, "{note:?}");
        // An empty ledger is exact too — zero calls is zero uncertainty.
        assert_eq!(cost_block(&[], None).1, None);
    }

    /// Acceptance 4. The receipt and `GET /jobs/{id}/costs` show the same job's
    /// money on two screens. The four-tuple is asserted equal across the two
    /// surfaces, so they cannot drift apart again.
    #[test]
    fn the_receipt_and_the_costs_endpoint_cannot_disagree_about_one_jobs_money() {
        for events in [
            vec![],
            vec![priced("claude", 0.30, SpendCertainty::Billed)],
            vec![
                priced("claude", 0.30, SpendCertainty::Billed),
                priced("claude", 0.0, SpendCertainty::Unmetered),
                priced("claude", 0.0, SpendCertainty::Unknown),
                priced("http", 0.0, SpendCertainty::Free),
            ],
        ] {
            let (cost, _) = cost_block(&events, None);
            let from_costs_endpoint = certainty_fields(&roll_up(&events));
            for (key, value) in &from_costs_endpoint {
                assert_eq!(&cost[key], value, "{key} differs between the two surfaces");
            }
        }
    }

    /// Acceptance 5. The change is additive: every field the block published
    /// before keeps its exact value, `total_usd` included, in both the exact
    /// and the floored case. No consumer breaks.
    ///
    /// The goldens are written out by hand rather than re-folded from the
    /// events — a test that recomputes the number it is pinning pins nothing.
    #[test]
    fn the_blocks_existing_fields_are_byte_identical_after_the_change() {
        let exact = vec![
            priced("claude", 0.30, SpendCertainty::Billed),
            priced("http", 0.0, SpendCertainty::Free),
        ];
        let floored = vec![
            priced("claude", 0.30, SpendCertainty::Billed),
            priced("claude", 0.0, SpendCertainty::Unmetered),
        ];
        let cases = [
            (
                exact,
                json!([
                    { "engine": "claude", "calls": 1, "cost_usd": 0.30 },
                    { "engine": "http", "calls": 1, "cost_usd": 0.0 },
                ]),
            ),
            (
                floored,
                json!([{ "engine": "claude", "calls": 2, "cost_usd": 0.30 }]),
            ),
        ];
        for (events, by_engine) in cases {
            let (cost, _) = cost_block(&events, Some(1.0));
            assert_eq!(cost["total_usd"], json!(0.30));
            assert_eq!(cost["calls"], json!(2));
            assert_eq!(cost["budget_usd"], json!(1.0));
            assert_eq!(cost["by_engine"], by_engine);
            assert_eq!(cost["egress"], json!([]));
        }
    }

    fn event(detail: Option<&str>) -> CostEvent {
        CostEvent {
            job_id: uuid::Uuid::nil().to_string(),
            app: "fake".into(),
            engine: "http".into(),
            url: Some("https://example.test/p".into()),
            cost_usd: 0.0,
            spend_certainty: pumper_core::costs::SpendCertainty::Free,
            detail: detail.map(str::to_string),
            created_at: Utc::now(),
        }
    }

    /// The anti-pattern: **a marker matched anywhere in a string**. `detail` is a
    /// `"; "`-joined trail that also carries target URLs and raw error text, so
    /// a substring search would happily read provenance out of an error message
    /// that merely quotes the phrase. Segment boundaries are the contract.
    /// The anti-pattern: counting the priced ledger row `AppContext::fetch`
    /// wrote AND the marker the MCP tool wrote, so one model-driven fetch shows
    /// up as two on the one surface an operator uses to find out what a run
    /// actually did.
    #[test]
    fn a_self_hosted_fetch_is_not_counted_twice() {
        let events = vec![
            event(Some("http tier thin: status 200")),
            event(None),
            event(Some(crate::mcp::SELF_HOSTED_FETCH_DETAIL)),
            event(Some("cost_unreported")),
        ];
        assert_eq!(self_hosted_fetches(&events), 1);
        // An ordinary run reports zero rather than nothing at all.
        assert_eq!(self_hosted_fetches(&events[..2]), 0);
        assert_eq!(self_hosted_fetches(&[]), 0);
    }

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
            requeue_reason: None,
            cause_kind: None,
            result: None,
            error: None,
            input_request: None,
            waiting_since: None,
            waiting_expires_at: None,
            resumed_input: None,
            executor_id: None,
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

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(a, d)| (a.to_string(), d.to_string()))
            .collect()
    }

    /// The block names the declared scope AND only the pairs outside it. The
    /// anti-pattern: reporting every write whose app is not `job.app`, which
    /// would flag the four declared `cz-labour` datasets on every MPSV run.
    #[test]
    fn coverage_names_the_declared_scope_and_only_the_writes_outside_it() {
        let declared_specs = pairs(&[("cz-labour", "salary_gap"), ("mpsv-vpm", "region_agg")]);
        let declared_apps = vec!["mpsv-vpm".to_string(), "cz-labour".to_string()];
        let observed = pairs(&[
            ("cz-labour", "salary_gap"),
            ("cz-labour", "skill_demand"),
            ("mpsv-vpm", "vacancy_samples"),
        ]);
        let block = coverage_block(&declared_apps, "mpsv-vpm", &declared_specs, &observed, true);
        assert_eq!(
            block["declared"],
            serde_json::json!(["mpsv-vpm", "cz-labour"])
        );
        assert_eq!(
            block["undeclared"],
            serde_json::json!([{ "app": "cz-labour", "dataset": "skill_demand" }]),
            "the job's own namespace is its own business; a declared pair is declared: {block}"
        );
        assert_eq!(block["complete"], serde_json::json!(true));
    }

    /// Fail-open, never fail-silent. A run whose writes this receipt cannot see
    /// must not come back looking declared-clean — that is indistinguishable
    /// from a genuinely clean run, which is the whole defect class.
    #[test]
    fn an_unseeable_run_is_not_reported_as_declared_clean() {
        // Succeeded, but nothing carries this job's stamp. "Wrote nothing" and
        // "wrote unstamped" are one picture from here, so this is NOT a clean
        // bill — reporting it as one is the defect this whole block closes.
        let blind = job(JobStatus::Succeeded);
        assert!(!coverage_is_complete(&blind, &[]));
        let reason = coverage_gap_reason(&blind);
        assert!(reason.contains("0030"), "{reason}");
        assert!(reason.contains("not a clean bill"), "{reason}");

        // Still running: its writes are not final, so no verdict is available.
        let running = job(JobStatus::Running);
        assert!(!coverage_is_complete(&running, &[]));
        let reason = coverage_gap_reason(&running);
        assert!(reason.contains("not final"), "{reason}");
        assert_ne!(reason, coverage_gap_reason(&blind));

        // One stamped revision is enough to make the diff mean something.
        assert!(coverage_is_complete(
            &job(JobStatus::Succeeded),
            &[RevisionCount {
                app: "cz-labour".into(),
                dataset: "salary_gap".into(),
                change: "new".into(),
                count: 1,
            }]
        ));
    }
}
