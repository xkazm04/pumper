//! Enforcement preview: what `[resilience] enforce = true` **would** have done.
//!
//! `enforce` ships `false`, and before this there was no way to answer the only
//! question that gates turning it on: *what would this have done to my fleet?*
//!
//! The evidence is already on disk. Soak mode is a no-op strictly **downstream**:
//! [`Resilience::observe`](super::store::Resilience::observe) judges every run,
//! moves the source's state and writes the verdict, score and self-explaining
//! `reasons` to `source_runs` **regardless** of `enforce` — the only thing
//! `enforce` changes is that
//! [`enforced_state`](super::store::Resilience::enforced_state) answers `Healthy`
//! to the four consumers that gate on it. So a preview is a *replay of stored
//! rows*, not a re-run of the detector:
//!
//! - **No detection re-runs.** Every verdict, score and state here is the one
//!   that was recorded at the time, by the rules in force at the time. Re-judging
//!   today's history against today's thresholds would answer a different question
//!   ("what would these rules say now") and would be worthless as a rollout gate.
//! - **`state_after` IS the would-be state.** The ladder is not gated on
//!   `enforce`; only its consequences are. The state column on each run row is
//!   exactly the state `enforced_state` would have returned to that run's write.
//! - **An unjudged run moved nothing.** `inconclusive`, `content_empty` and
//!   `below_cohort` runs neither move the state nor enter the baseline, so they
//!   are never credited with a transition — see [`TransitionCause`].
//!
//! # Side effects
//!
//! None, by construction: every statement in this module is a `SELECT`. That is
//! asserted, not merely intended — `crates/core/tests/enforcement_preview.rs`
//! snapshots every health table plus the database file bytes around a preview.

use serde::Serialize;
use serde_json::Value;

use crate::Result;

use super::store::{HealthStore, SourceRun};
use super::{RunVerdict, SourceState};

/// Runs replayed per source when the caller does not say. Deep enough to cover a
/// month of daily runs, bounded so a fleet-wide preview stays one quick pass.
pub const DEFAULT_REPLAY_RUNS: i64 = 60;

/// The four enforcement consequences, each named after the live call site that
/// applies it. This list is the preview's contract with the runtime: if a fifth
/// consumer ever gates on `enforced_state`, it belongs here too, and
/// `every_enforcement_consequence_is_previewed` fails until it is.
pub const CONSEQUENCES: &[(&str, &str)] = &[
    ("diverted_writes", "core::app::AppContext::write_target"),
    (
        "withheld_removals",
        "core::app::AppContext::sync_many_with_provenance",
    ),
    ("suppressed_pushes", "server::worker::suppress_unhealthy"),
    (
        "skipped_index_writes",
        "server::worker::dataset_search_docs",
    ),
];

/// Why the replayed state moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionCause {
    /// This run was judged and the ladder moved on its verdict. The run's
    /// `reasons` explain it.
    Verdict,
    /// The state changed without a judged run explaining it — an operator
    /// `POST /sources/{id}/state`, or the run that caused it having been pruned
    /// out of the retained window. Reported rather than attributed: crediting an
    /// unjudged run with a move it did not make is exactly the kind of tidy lie
    /// a rollout gate must not tell.
    Outside,
}

/// One would-be state transition, with the evidence that triggered it.
#[derive(Debug, Clone, Serialize)]
pub struct PreviewTransition {
    pub job_id: String,
    pub at: String,
    pub from: SourceState,
    pub to: SourceState,
    pub cause: TransitionCause,
    pub verdict: String,
    pub score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnosis: Option<String>,
    /// The stored `reasons` array — every test that ran, its value and its
    /// threshold. Passed through verbatim; nothing is recomputed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasons: Option<Value>,
    /// What entering `to` starts gating, by [`CONSEQUENCES`] name.
    pub gates: Vec<&'static str>,
}

/// A count of runs and of the documents in them.
///
/// **Runs, not deliveries.** How many webhooks a suppressed run would have sent
/// is not stored anywhere — it depends on the watches registered at that moment
/// — so this counts the runs whose revisions never would have reached the push
/// seam, and the documents they carried. Inventing a delivery count would be a
/// fabrication.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct RunCount {
    pub runs: u64,
    pub docs: u64,
}

impl RunCount {
    fn add(&mut self, docs: i64) {
        self.runs += 1;
        self.docs += docs.max(0) as u64;
    }

    fn merge(&mut self, other: RunCount) {
        self.runs += other.runs;
        self.docs += other.docs;
    }
}

/// What enforcement would have done, counted over the replayed window.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct PreviewConsequences {
    /// Runs whose batch would have landed in `<dataset>@q` instead of the live
    /// dataset (`write_target`).
    pub diverted_writes: RunCount,
    /// Runs whose full-snapshot sync would have been downgraded to a plain
    /// upsert, so nothing missing from the batch was tombstoned
    /// (`sync_many_with_provenance`, the withheld `RemovalGuard`).
    pub withheld_removals: RunCount,
    /// Runs whose revisions would never have reached watches or triggers
    /// (`suppress_unhealthy`).
    pub suppressed_pushes: RunCount,
    /// Runs whose revisions would not have been indexed (`dataset_search_docs`).
    pub skipped_index_writes: RunCount,
    /// Runs whose records would have carried a non-null trust stamp
    /// (`provisional` or `quarantined`). Not a suppression — a label.
    pub trust_stamped: RunCount,
}

impl PreviewConsequences {
    fn record(&mut self, state: SourceState, docs: i64) {
        if state.diverts_writes() {
            self.diverted_writes.add(docs);
        }
        if state.suppresses_removals() {
            self.withheld_removals.add(docs);
        }
        if state.suppresses_pushes() {
            self.suppressed_pushes.add(docs);
        }
        if state.skips_search_index() {
            self.skipped_index_writes.add(docs);
        }
        if state.trust().is_some() {
            self.trust_stamped.add(docs);
        }
    }

    fn merge(&mut self, other: PreviewConsequences) {
        self.diverted_writes.merge(other.diverted_writes);
        self.withheld_removals.merge(other.withheld_removals);
        self.suppressed_pushes.merge(other.suppressed_pushes);
        self.skipped_index_writes.merge(other.skipped_index_writes);
        self.trust_stamped.merge(other.trust_stamped);
    }

    /// Whether anything at all would have been gated.
    pub fn any(&self) -> bool {
        [
            self.diverted_writes,
            self.withheld_removals,
            self.suppressed_pushes,
            self.skipped_index_writes,
        ]
        .iter()
        .any(|c| c.runs > 0)
    }
}

/// What the enumerated [`CONSEQUENCES`] a state applies, by name.
pub fn gates_of(state: SourceState) -> Vec<&'static str> {
    let mut out = Vec::new();
    if state.diverts_writes() {
        out.push(CONSEQUENCES[0].0);
    }
    if state.suppresses_removals() {
        out.push(CONSEQUENCES[1].0);
    }
    if state.suppresses_pushes() {
        out.push(CONSEQUENCES[2].0);
    }
    if state.skips_search_index() {
        out.push(CONSEQUENCES[3].0);
    }
    out
}

/// One source's replay.
#[derive(Debug, Clone, Serialize)]
pub struct SourcePreview {
    pub id: String,
    pub runs_replayed: u64,
    /// Runs the detector could not judge (`inconclusive`, `content_empty`,
    /// `below_cohort`). They moved nothing, and are counted here rather than
    /// silently folded into the clean ones.
    pub unjudged_runs: u64,
    /// The oldest replayed run's timestamp, and the state it was already in.
    /// The window opens mid-history — retention prunes older runs — so this is
    /// where the replay starts, not where the source started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_opens_at: Option<String>,
    pub window_opens_in: SourceState,
    /// The state the last replayed run left the source in — what enforcement
    /// would be gating on right now.
    pub state: SourceState,
    /// What `state` gates, by [`CONSEQUENCES`] name. Empty means this source is
    /// ready for `enforce = true`.
    pub gates: Vec<&'static str>,
    /// The live `sources.state` row. Differs from `state` only when something
    /// outside the run history moved it (an operator override, or the deciding
    /// run being pruned) — surfaced so the preview never quietly disagrees with
    /// `GET /sources`.
    pub live_state: SourceState,
    /// Whether this source has ever produced a cohort large enough to judge. A
    /// preview of an unmonitored source is a preview of very little evidence.
    pub monitored: bool,
    pub transitions: Vec<PreviewTransition>,
    pub consequences: PreviewConsequences,
}

/// A source that is not ready for `enforce = true`, and why.
#[derive(Debug, Clone, Serialize)]
pub struct NotReady {
    pub id: String,
    pub state: SourceState,
    pub gates: Vec<&'static str>,
    /// The transition that put it there, if it is inside the replayed window.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<PreviewTransition>,
}

/// The whole answer to "is my fleet ready for `enforce = true`".
#[derive(Debug, Clone, Serialize)]
pub struct FleetPreview {
    /// Whether enforcement is already on — in which case this is a description
    /// of what DID happen, not of what would have.
    pub enforcing: bool,
    pub runs_per_source: i64,
    pub sources_replayed: usize,
    /// True when no source's current state gates anything: flipping `enforce`
    /// today would change nothing about the next run.
    pub ready: bool,
    /// The sources that make `ready` false, named.
    pub not_ready: Vec<NotReady>,
    /// Sources that have never cleared the cohort floor. They gate nothing, so
    /// they do not block readiness — but a preview of a source nobody could
    /// judge is weak evidence, so they are named rather than counted silently.
    pub unmonitored: Vec<String>,
    /// Summed over every replayed source.
    pub totals: PreviewConsequences,
    pub sources: Vec<SourcePreview>,
}

/// Replays one source's stored runs, **oldest first**.
///
/// Pure: the caller supplies the rows. Nothing here re-judges anything — every
/// verdict, score and state is read off the row it was written to.
pub fn replay(
    id: &str,
    live_state: SourceState,
    monitored: bool,
    runs_oldest_first: &[SourceRun],
) -> SourcePreview {
    let opens_in = runs_oldest_first
        .first()
        .map(|r| SourceState::parse(&r.state_after))
        .unwrap_or(live_state);
    let mut carried = opens_in;
    let mut transitions = Vec::new();
    let mut consequences = PreviewConsequences::default();
    let mut unjudged = 0u64;

    for (i, run) in runs_oldest_first.iter().enumerate() {
        let after = SourceState::parse(&run.state_after);
        let judged = verdict_of(run).judged();
        if !judged {
            unjudged += 1;
        }
        // The first row cannot be a transition: there is no previous state to
        // have moved from, only the state the window opened in.
        if i > 0 && after != carried {
            transitions.push(PreviewTransition {
                job_id: run.job_id.clone(),
                at: run.created_at.clone(),
                from: carried,
                to: after,
                cause: if judged {
                    TransitionCause::Verdict
                } else {
                    TransitionCause::Outside
                },
                verdict: run.verdict.clone(),
                score: run.score,
                diagnosis: run.diagnosis.clone(),
                reasons: judged.then(|| run.reasons.clone()).flatten(),
                gates: gates_of(after),
            });
        }
        carried = after;
        // Gating is applied to the write of the run that settled the state —
        // `observe_extraction` runs BEFORE the upsert — so each run's
        // consequences are the ones its own `state_after` implies.
        consequences.record(after, run.docs);
    }

    SourcePreview {
        id: id.to_string(),
        runs_replayed: runs_oldest_first.len() as u64,
        unjudged_runs: unjudged,
        window_opens_at: runs_oldest_first.first().map(|r| r.created_at.clone()),
        window_opens_in: opens_in,
        state: carried,
        gates: gates_of(carried),
        live_state,
        monitored,
        transitions,
        consequences,
    }
}

/// The stored verdict string as a [`RunVerdict`]. An unrecognized value reads as
/// `Inconclusive` — "we cannot tell what this run said" must never be rounded up
/// to "it was fine", because a judged verdict is what moves a state.
fn verdict_of(run: &SourceRun) -> RunVerdict {
    match run.verdict.as_str() {
        "ok" => RunVerdict::Ok,
        "broken" => RunVerdict::Broken,
        "self_inflicted" => RunVerdict::SelfInflicted,
        "content_empty" => RunVerdict::ContentEmpty,
        "below_cohort" => RunVerdict::BelowCohort,
        _ => RunVerdict::Inconclusive,
    }
}

/// Replays the whole fleet. **Read-only**: `list_sources` + one `runs` read per
/// source, and nothing else.
pub async fn preview_fleet(
    store: &HealthStore,
    enforcing: bool,
    app: Option<&str>,
    runs_per_source: i64,
    source_limit: i64,
) -> Result<FleetPreview> {
    let runs_per_source = runs_per_source.clamp(1, 1000);
    let rows = store.list_sources(None, app, source_limit).await?;
    let mut sources = Vec::with_capacity(rows.len());
    let mut totals = PreviewConsequences::default();
    let mut not_ready = Vec::new();
    let mut unmonitored = Vec::new();

    for row in rows {
        // `runs` is newest-first; the ladder only makes sense forwards.
        let mut runs = store.runs(&row.id, runs_per_source).await?;
        runs.reverse();
        let preview = replay(&row.id, row.state, row.monitored, &runs);
        totals.merge(preview.consequences);
        if !preview.gates.is_empty() {
            not_ready.push(NotReady {
                id: preview.id.clone(),
                state: preview.state,
                gates: preview.gates.clone(),
                since: preview
                    .transitions
                    .iter()
                    .rev()
                    .find(|t| t.to == preview.state)
                    .cloned(),
            });
        }
        if !preview.monitored {
            unmonitored.push(preview.id.clone());
        }
        sources.push(preview);
    }

    Ok(FleetPreview {
        enforcing,
        runs_per_source,
        sources_replayed: sources.len(),
        ready: not_ready.is_empty(),
        not_ready,
        unmonitored,
        totals,
        sources,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(job: &str, at: &str, verdict: &str, state: &str, docs: i64, score: f64) -> SourceRun {
        SourceRun {
            job_id: job.into(),
            docs,
            fetch_ok_rate: 1.0,
            d_text: None,
            d_dom: None,
            d_val: None,
            compared: 0,
            verdict: verdict.into(),
            diagnosis: None,
            score,
            reasons: Some(serde_json::json!([{ "test": "t", "value": score, "threshold": 0.6 }])),
            state_after: state.into(),
            build_id: None,
            created_at: at.into(),
        }
    }

    #[test]
    fn a_replay_counts_consequences_per_run_not_once_per_source() {
        // Two quarantined runs of 40 and 60 docs are 2 diverted runs / 100
        // diverted documents — not "this source is quarantined, count 1".
        let runs = [
            run("a", "2026-08-01T00:00:00Z", "ok", "healthy", 50, 0.1),
            run("b", "2026-08-02T00:00:00Z", "broken", "suspect", 50, 0.9),
            run("c", "2026-08-03T00:00:00Z", "broken", "degraded", 50, 0.9),
            run(
                "d",
                "2026-08-04T00:00:00Z",
                "broken",
                "quarantined",
                40,
                0.95,
            ),
            run(
                "e",
                "2026-08-05T00:00:00Z",
                "broken",
                "quarantined",
                60,
                0.95,
            ),
        ];
        let p = replay("extractor/products", SourceState::Quarantined, true, &runs);
        assert_eq!(p.runs_replayed, 5);
        assert_eq!(
            p.consequences.diverted_writes,
            RunCount { runs: 2, docs: 100 }
        );
        // degraded + 2 quarantined all suppress pushes and removals.
        assert_eq!(
            p.consequences.suppressed_pushes,
            RunCount { runs: 3, docs: 150 }
        );
        assert_eq!(
            p.consequences.skipped_index_writes,
            RunCount { runs: 3, docs: 150 }
        );
        // `suspect` is inert by design — it must not show up as a consequence.
        assert_eq!(p.consequences.trust_stamped.runs, 3);
        assert!(p.consequences.any());
    }

    #[test]
    fn an_unjudged_run_is_not_credited_with_a_transition_it_did_not_cause() {
        // A `below_cohort` run moves nothing. If the state nonetheless differs
        // across it, something outside the run history moved it (an operator
        // override, or a pruned run) — and saying "this run degraded the source"
        // would be a tidy lie in the one report meant to be trusted.
        let runs = [
            run("a", "2026-08-01T00:00:00Z", "ok", "healthy", 50, 0.1),
            run(
                "b",
                "2026-08-02T00:00:00Z",
                "below_cohort",
                "degraded",
                2,
                0.0,
            ),
            run(
                "c",
                "2026-08-03T00:00:00Z",
                "broken",
                "quarantined",
                50,
                0.9,
            ),
        ];
        let p = replay("extractor/thin", SourceState::Quarantined, false, &runs);
        assert_eq!(p.unjudged_runs, 1);
        assert_eq!(p.transitions.len(), 2);
        assert_eq!(p.transitions[0].cause, TransitionCause::Outside);
        assert!(
            p.transitions[0].reasons.is_none(),
            "an unjudged run has no verdict to explain a move with"
        );
        assert_eq!(p.transitions[1].cause, TransitionCause::Verdict);
        assert!(p.transitions[1].reasons.is_some());
    }

    #[test]
    fn a_window_that_opens_mid_quarantine_is_not_reported_as_a_transition() {
        // Retention prunes older runs, so the oldest retained run is usually not
        // the run that caused the state. Emitting a `healthy -> quarantined`
        // transition at the window edge would invent an event.
        let runs = [
            run("a", "2026-08-01T00:00:00Z", "ok", "quarantined", 50, 0.1),
            run("b", "2026-08-02T00:00:00Z", "ok", "quarantined", 50, 0.1),
        ];
        let p = replay("extractor/old", SourceState::Quarantined, true, &runs);
        assert_eq!(p.window_opens_in, SourceState::Quarantined);
        assert!(p.transitions.is_empty());
        assert_eq!(p.consequences.diverted_writes.runs, 2);
    }

    #[test]
    fn a_source_with_no_retained_runs_reports_its_live_state_and_nothing_else() {
        let p = replay("extractor/silent", SourceState::Degraded, false, &[]);
        assert_eq!(p.state, SourceState::Degraded);
        assert_eq!(p.window_opens_in, SourceState::Degraded);
        assert!(p.window_opens_at.is_none());
        assert_eq!(p.runs_replayed, 0);
        assert!(!p.consequences.any());
        // …but it is still not ready: the gates come from the state, not from
        // having recent evidence.
        assert!(!p.gates.is_empty());
    }

    #[test]
    fn every_enforcement_consequence_is_previewed() {
        // Inventory: the four things `enforced_state` gates in the runtime. A
        // fifth consumer that reads `enforced_state` and is not listed here is
        // a consequence the preview would silently omit — which is the one
        // failure mode a rollout gate cannot have.
        const EXPECTED: &[(&str, &str)] = &[
            ("diverted_writes", "core::app::AppContext::write_target"),
            (
                "withheld_removals",
                "core::app::AppContext::sync_many_with_provenance",
            ),
            ("suppressed_pushes", "server::worker::suppress_unhealthy"),
            (
                "skipped_index_writes",
                "server::worker::dataset_search_docs",
            ),
        ];
        assert_eq!(CONSEQUENCES, EXPECTED);
        // Every consequence name is reachable from some state, so none of them
        // is a label nothing can ever produce.
        let all: std::collections::BTreeSet<&str> = [
            SourceState::Healthy,
            SourceState::Suspect,
            SourceState::Degraded,
            SourceState::Quarantined,
            SourceState::Probation,
            SourceState::Retired,
        ]
        .into_iter()
        .flat_map(gates_of)
        .collect();
        for (name, _) in CONSEQUENCES {
            assert!(all.contains(name), "{name} is never produced by any state");
        }
        // And the inert rungs really are inert.
        assert!(gates_of(SourceState::Healthy).is_empty());
        assert!(gates_of(SourceState::Suspect).is_empty());
        assert!(gates_of(SourceState::Probation).is_empty());
    }

    #[test]
    fn an_unparseable_verdict_reads_as_unjudged_not_as_clean() {
        let runs = [run(
            "a",
            "2026-08-01T00:00:00Z",
            "future_variant",
            "healthy",
            5,
            0.0,
        )];
        let p = replay("extractor/x", SourceState::Healthy, true, &runs);
        assert_eq!(p.unjudged_runs, 1);
    }
}
