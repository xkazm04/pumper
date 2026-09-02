//! Example app: agentic web research via the Claude Code CLI engine.
//! Serves as the template for research-style use cases where a crawler
//! can't cut it — the agent searches, reads pages, and synthesizes.
//!
//! Durable execution (M23 port): the run is chunked into bounded agentic
//! steps (`turns_per_step` CLI turns each). After every budget-consuming step
//! the app persists a checkpoint through `ctx.checkpoint_now` — session id,
//! cumulative spend, partial text, and the step/turn cursor — so a crash,
//! reap, timeout, or shutdown-suspend costs a *session resume* (via the
//! engine's `resume_session` path) instead of re-paying the whole research
//! from zero. A checkpoint that already carries the final result is returned
//! as-is on re-claim: zero new metered spend. Budget math never double-counts
//! restored spend: the job's ledger-seeded `spent_usd` already includes prior
//! attempts' cost events, and the checkpointed `spent_usd` is folded into the
//! *report* only, never re-metered.

use async_trait::async_trait;
use pumper_core::config::ResearchConfig;
use pumper_core::extract::extracted_nothing;
use pumper_core::{
    salvage_json, AppContext, AppManifest, ChangeKind, CostClass, Error, FetchRequest,
    ManifestExample, Provenance, ResearchRequest, Result, ScrapeApp,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub struct Research;

/// Checkpoint blob version — bump on shape change; a mismatch restores fresh.
const STATE_VERSION: u32 = 1;

/// Default CLI turns per agentic step when `max_turns` is set but
/// `turns_per_step` isn't: small enough that an interruption loses one
/// bounded chunk, large enough that a typical research run finishes in 1–2.
const DEFAULT_CHUNK_TURNS: u32 = 8;

/// Hard cap on research steps per job — the loop's unconditional bound.
const MAX_STEPS: u32 = 12;

/// Max chars of raw model text carried in a checkpoint as partial findings.
const PARTIAL_CAP_CHARS: usize = 4000;

/// The resumable state persisted through the checkpoint seam after every
/// budget-consuming step. Advisory shape: `plan_from_restore` starts fresh on
/// anything it can't fully trust.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RunState {
    /// Shape version (see [`STATE_VERSION`]).
    v: u32,
    /// The agent session accumulated so far; resuming it is the whole point.
    #[serde(default)]
    session_id: Option<String>,
    /// Cursor: completed research steps.
    #[serde(default)]
    steps_done: u32,
    /// Cursor: CLI turns consumed so far (bounds the next step's chunk).
    #[serde(default)]
    turns_used: u64,
    /// Cumulative Claude spend across all attempts — folded into the report's
    /// `cost_usd`, NEVER re-metered (the ledger already holds those events).
    #[serde(default)]
    spent_usd: f64,
    /// Cumulative agent wall time across all attempts. Restored like `steps`,
    /// `spent_usd` and `turns_used` so the result reports ONE grain: it used to
    /// be reset to 0 on every re-claim, which published a resumed run's
    /// this-attempt-only duration next to three cumulative counters.
    ///
    /// Additive and `#[serde(default)]`, so a v1 blob written before this field
    /// existed still restores — a missing value means exactly what the old code
    /// did (0), which is why this is not a [`STATE_VERSION`] bump: bumping would
    /// discard live checkpoints and re-buy their research.
    #[serde(default)]
    duration_ms: u64,
    /// Truncated raw text of the last unfinished step (partial findings).
    #[serde(default)]
    partial: Option<String>,
    /// Set once the run finished: the complete job result. A restore that
    /// finds this returns it directly — no new metered call.
    #[serde(default)]
    result: Option<Value>,
}

impl RunState {
    fn fresh(session_id: Option<String>) -> Self {
        RunState {
            v: STATE_VERSION,
            session_id,
            steps_done: 0,
            turns_used: 0,
            spent_usd: 0.0,
            duration_ms: 0,
            partial: None,
            result: None,
        }
    }

    fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// What a re-claimed attempt does with the restored checkpoint.
#[derive(Debug, PartialEq)]
enum Plan {
    /// No/unusable checkpoint — start the research from zero.
    Fresh,
    /// The prior attempt already finished: return its result, spend nothing.
    Done(Value),
    /// Mid-run checkpoint: resume the session where it left off.
    Resume(RunState),
}

/// Resume-vs-fresh decision over the advisory restored blob. Anything that
/// doesn't parse as a current-version, sane [`RunState`] means Fresh — never
/// an error (the seam's poisoned-checkpoint escape counts failures upstream).
fn plan_from_restore(restored: Option<&Value>) -> Plan {
    let Some(v) = restored else {
        return Plan::Fresh;
    };
    let Ok(state) = serde_json::from_value::<RunState>(v.clone()) else {
        return Plan::Fresh;
    };
    if state.v != STATE_VERSION || !state.spent_usd.is_finite() || state.spent_usd < 0.0 {
        return Plan::Fresh;
    }
    match state.result.clone() {
        Some(result) => Plan::Done(result),
        None => Plan::Resume(state),
    }
}

/// Turns to charge the run for one completed step, given what the engine
/// reported and the `--max-turns` cap that step was launched with.
///
/// **Why the reported count is clamped to the step's own cap.** `num_turns`
/// comes straight off the CLI result envelope (`engine-claude/src/lib.rs:396`),
/// and on a `--resume`d session that counter is not unambiguously
/// per-invocation — it may carry the session's cumulative total. The two
/// readings break in opposite directions: summing a cumulative counter across
/// up to [`MAX_STEPS`] steps over-counts and truncates the run long before
/// `max_turns`, while trusting a per-invocation counter as cumulative
/// under-counts and lets the run overshoot it. The one bound that holds under
/// BOTH readings is the cap this app itself passed as `--max-turns`: a step
/// cannot have used more turns than it was allowed. So charge
/// `min(reported, cap)` — exact under the per-invocation reading, conservative
/// (never an overshoot of the caller's total) under the cumulative one. Do not
/// "simplify" this back to the raw reported value without first pinning the
/// CLI's resume semantics with a live test.
///
/// A step with no cap is the uncapped single-call path, which the loop exits
/// after one step; an engine that omits the count advances by the whole chunk
/// so the turn budget still terminates the loop.
fn step_turns_used(reported: Option<u64>, cap: Option<u64>) -> u64 {
    match (reported, cap) {
        (Some(reported), Some(cap)) => reported.min(cap),
        (Some(reported), None) => reported,
        (None, Some(cap)) => cap,
        (None, None) => 1,
    }
}

/// Folds one completed research step into the state: advances spend and turn
/// cursors, keeps the freshest session id (a step that returned none keeps the
/// prior one so the session stays resumable). `step_cap` is the `--max-turns`
/// the step ran under — see [`step_turns_used`].
fn fold_output(
    state: &mut RunState,
    cost_usd: Option<f64>,
    num_turns: Option<u64>,
    session_id: Option<String>,
    step_cap: Option<u64>,
    duration_ms: Option<u64>,
) {
    state.spent_usd += cost_usd.unwrap_or(0.0);
    state.duration_ms += duration_ms.unwrap_or(0);
    state.turns_used += step_turns_used(num_turns, step_cap);
    state.steps_done += 1;
    if session_id.is_some() {
        state.session_id = session_id;
    }
}

/// Turn budget for the next step.
#[derive(Debug, PartialEq)]
enum StepPlan {
    /// Run a step with this `--max-turns` cap (None = uncapped single call).
    Run(Option<u32>),
    /// The job's total turn budget is spent.
    Exhausted,
}

/// Why the loop stopped, surfaced in the result so a caller can tell a
/// finished report from a truncated one instead of inferring it from
/// `structured: false`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopReason {
    /// `final_parsed` matched the promised shape — a real finish.
    Completed,
    /// Hit [`MAX_STEPS`] before the report ever parsed.
    StepCap,
    /// The turn budget (`max_turns`) ran out.
    TurnsExhausted,
    /// The job's `$` budget hit zero between steps.
    BudgetExhausted,
    /// A step returned no session id to resume, so continuing would restart
    /// from zero instead of building on prior context.
    NoSession,
    /// Neither a turn cap nor a per-step chunk was set — the original
    /// single-call behavior, not a truncation.
    SingleCall,
}

impl StopReason {
    fn as_str(self) -> &'static str {
        match self {
            StopReason::Completed => "completed",
            StopReason::StepCap => "step_cap",
            StopReason::TurnsExhausted => "turns_exhausted",
            StopReason::BudgetExhausted => "budget_exhausted",
            StopReason::NoSession => "no_session",
            StopReason::SingleCall => "single_call",
        }
    }
}

fn next_step_turns(max_turns: Option<u32>, per_step: Option<u32>, used: u64) -> StepPlan {
    match max_turns {
        Some(total) => {
            let remaining = u64::from(total).saturating_sub(used);
            if remaining == 0 {
                return StepPlan::Exhausted;
            }
            let chunk = u64::from(per_step.unwrap_or(DEFAULT_CHUNK_TURNS)).max(1);
            StepPlan::Run(Some(chunk.min(remaining) as u32))
        }
        // No total cap: honor an explicit per-step chunk (loop is bounded by
        // MAX_STEPS), else one uncapped call — the pre-port behavior.
        None => StepPlan::Run(per_step),
    }
}

/// Spend budget for the next step.
#[derive(Debug, PartialEq)]
enum BudgetPlan {
    /// Run a step under this per-call `--max-budget-usd` (None = no ceiling).
    Run(Option<f64>),
    /// The run's spend ceiling is reached — stop and keep the partial.
    Exhausted,
}

/// Smallest headroom worth starting a metered step with. A step launched with
/// less than this cannot search, fetch and synthesize before its own
/// `--max-budget-usd` aborts it, so the money buys nothing: stopping with
/// `budget_exhausted` and keeping the partial beats paying for a call that
/// cannot finish.
const MIN_STEP_BUDGET_USD: f64 = 0.01;

/// The run's spend wall, consulted before **every** step.
///
/// `max_budget_usd` is documented (param text, manifest example, feature docs)
/// as a **per-run** ceiling, so it is enforced here against the run's own
/// cumulative `spent_usd` — which includes spend restored from a checkpoint —
/// and each call's ceiling is the *remaining* headroom, not the whole budget.
/// Passing the full value to every one of up to [`MAX_STEPS`] calls, as this
/// app used to, made the enforced ceiling `MAX_STEPS * max_budget_usd`.
///
/// `job_remaining` is [`AppContext::remaining_budget_usd`]: `None` whenever the
/// job carries no `budget_usd`, which is why it cannot be the only wall.
/// Whichever headroom is tighter governs.
fn next_step_budget(
    run_ceiling: Option<f64>,
    spent_usd: f64,
    job_remaining: Option<f64>,
) -> BudgetPlan {
    let run_headroom = run_ceiling.map(|ceiling| (ceiling - spent_usd).max(0.0));
    let headroom = match (run_headroom, job_remaining) {
        (Some(run), Some(job)) => Some(run.min(job)),
        (Some(run), None) => Some(run),
        (None, Some(job)) => Some(job),
        (None, None) => None,
    };
    match headroom {
        None => BudgetPlan::Run(None),
        Some(headroom) if headroom < MIN_STEP_BUDGET_USD => BudgetPlan::Exhausted,
        Some(headroom) => BudgetPlan::Run(Some(headroom)),
    }
}

/// Char-boundary-safe truncation for the checkpointed partial text.
fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    s.chars().take(max).collect()
}

#[async_trait]
impl ScrapeApp for Research {
    fn name(&self) -> &'static str {
        "research"
    }

    fn description(&self) -> &'static str {
        "Web research via Claude Code CLI. Params: {\"query\": \"...\", \
         \"role\": \"research|compose\", \"model\": \"claude-...\", \
         \"effort\": \"low|medium|high|xhigh|max\", \"max_turns\": 25, \
         \"turns_per_step\": 8 (CLI turns per checkpointed step; the run is \
         chunked so an interrupted job resumes its agent session instead of \
         restarting), \"session_id\": \"...\" (resume a prior run's session_id \
         to drill down on its accumulated context instead of researching from \
         scratch — the query is then a follow-up question), \
         \"max_budget_usd\": 0.0 (per-run Claude spend ceiling — a TOTAL over \
         every step of the run, including spend restored from a checkpoint; \
         the run stops with stop_reason=budget_exhausted and returns its \
         partial findings rather than exceeding it)}. Progress is \
         checkpointed durably after every step: a crashed/reaped/suspended job \
         resumes where it left off without re-spending restored budget. \
         Every structured run also becomes DATASETS: one `research/findings` record \
         per key finding (keyed `{slug(topic)}#{i}`) and one `research/sources` \
         record per cited URL, both provenance-stamped (`persist`, default true). \
         `snapshot_sources` archives each cited URL through the metered tiered \
         fetcher and stamps `artifact_sha` so the citation is re-derivable; \
         `watch_sources` proposes a `POST /schedules` body per cited URL."
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": { "type": "string", "minLength": 1 },
                    "role": { "type": "string", "enum": ["research", "compose"] },
                    "model": { "type": "string" },
                    "effort": { "type": "string", "enum": ["low", "medium", "high", "xhigh", "max"] },
                    "max_turns": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Total CLI turns for the whole run, spread across the checkpointed steps — not a per-step limit."
                    },
                    "turns_per_step": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "CLI turns per checkpointed research step (default 8 when max_turns is set). Smaller = finer-grained resume, more session round-trips."
                    },
                    "session_id": {
                        "type": "string",
                        "description": "Resume a prior run's session to drill down; the query becomes a follow-up."
                    },
                    "max_budget_usd": {
                        "type": "number",
                        "minimum": 0,
                        "description": "Total Claude spend ceiling for the whole run (all steps, plus spend restored from a checkpoint) — not a per-call limit. Each step is capped at the remaining headroom; when less than a cent is left the run stops with stop_reason=budget_exhausted and returns its partial findings."
                    },
                    "topic": {
                        "type": "string",
                        "description": "Key space for this run's findings records (default: the query). A follow-up run passes the ORIGINAL query here so it updates that topic's findings instead of forking a second copy under its own slug."
                    },
                    "persist": {
                        "type": "boolean",
                        "description": "Write research/findings + research/sources records for this run (default true). Nothing is written for an unstructured report - there are no key findings or citations to write."
                    },
                    "snapshot_sources": {
                        "type": "boolean",
                        "description": "Fetch each cited URL through the tiered fetcher (metered) and save it as a source-N.md artifact, stamping artifact_sha on the source record so the citation is re-derivable. Default false."
                    },
                    "archive_max_age": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Snapshot fetches only: accept an archived body up to N seconds old instead of going live."
                    },
                    "watch_sources": {
                        "type": "boolean",
                        "description": "Emit one ready-to-POST /schedules body per cited URL in watch_requests. This app creates no schedules itself. Default false."
                    },
                    "watch_cron": {
                        "type": "string",
                        "description": "Cron (6 fields, with seconds) for the proposed source watches. Default \"0 0 7 * * *\"."
                    },
                    "max_watched_sources": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Per-run ceiling on per-source actions (record, snapshot, watch proposal). Default: [research] max_watched_sources (20). A run that cites more reports sources_truncated: true."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Bounded web research run with a hard spend ceiling",
                    params: json!({
                        "query": "Current Czech VAT registration thresholds for sole traders, with sources",
                        "effort": "medium",
                        "max_turns": 15,
                        "max_budget_usd": 0.5
                    }),
                },
                ManifestExample {
                    description:
                        "Follow-up question against a prior run's accumulated session context",
                    params: json!({
                        "query": "And how did those thresholds change in the last 3 years?",
                        "session_id": "prior-run-session-id",
                        "max_budget_usd": 0.25
                    }),
                },
                ManifestExample {
                    description: "Living knowledge base: persist findings + sources, archive every citation, and propose a daily watch per source",
                    params: json!({
                        "query": "Current Czech VAT registration thresholds for sole traders, with sources",
                        "snapshot_sources": true,
                        "watch_sources": true,
                        "max_budget_usd": 0.5
                    }),
                },
            ],
            output_shape: Some(
                "{query, topic, report: {summary, key_findings, sources}, structured, resumed, \
                 resumed_from_checkpoint, steps, cost_usd, duration_ms, num_turns, session_id, \
                 stop_reason, datasets: {persisted, findings, sources, findings_new, \
                 findings_changed, sources_new, sources_changed, error}, snapshots: {attempted, \
                 saved, failed}, watch_requests[], sources_truncated, index_datasets[]} — the research report is NESTED under `report`, and only when \
                 `structured` is true; when it is false `report` is the agent's raw answer as a \
                 bare string, so `summary`/`key_findings`/`sources` are never top-level keys. \
                 `session_id` is resumable — pass it back as the `session_id` param to drill \
                 down on the context it built. `steps`, `cost_usd`, `duration_ms` and \
                 `num_turns` are all cumulative across resumed attempts. `resumed` means this \
                 run continued a session the CALLER named; `resumed_from_checkpoint` means the \
                 runtime re-claimed an interrupted attempt (the two are independent, and a \
                 re-claim uses the checkpoint's session, not the caller's). `stop_reason` \
                 explains why the loop ended (completed, step_cap, turns_exhausted, \
                 budget_exhausted, no_session, single_call) — `structured: false` alone can't \
                 distinguish a truncated report from other causes. A run that produced no \
                 content at all fails the job instead of returning an empty report. \
                 `topic` is the key space this run's records live under (the `topic` param, \
                 else `query`): `datasets.findings` are the keys written to \
                 `research/findings` (`{slug(topic)}#{index}`) and `datasets.sources` the \
                 cited URLs written to `research/sources`; both are empty when `persist` is \
                 false or the report was unstructured, and `datasets.error` names a store \
                 failure that was reported rather than allowed to discard a paid-for report. \
                 `snapshots` counts `snapshot_sources` fetches (attempted/saved/failed - a \
                 failed snapshot is recorded on the source record, never fatal). \
                 `watch_requests` are ready-to-POST `/schedules` bodies the run PROPOSES for \
                 the cited URLs (`watch_sources`); this app creates no schedules itself. \
                 `sources_truncated` says the per-run source cap bit.",
            ),
            cost_class: CostClass::Claude,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let query = ctx.require_str("query")?.to_string();
        let max_turns = ctx
            .params
            .get("max_turns")
            .and_then(Value::as_u64)
            .map(|turns| turns as u32);
        let turns_per_step = ctx
            .params
            .get("turns_per_step")
            .and_then(Value::as_u64)
            .map(|turns| turns as u32)
            .filter(|&t| t > 0);
        // Resume a prior run's agent session so a follow-up drills down on the
        // context it already built, instead of re-paying the full search+fetch+
        // synthesize loop. The prior run returns `session_id` in its result.
        let caller_session = ctx
            .params
            .get("session_id")
            .and_then(Value::as_str)
            .map(String::from);
        let caller_resumed = caller_session.is_some();
        let max_budget_usd = ctx.params.get("max_budget_usd").and_then(Value::as_f64);
        // Model/effort are chosen by the caller: default to the "research" role,
        // which `[claude.roles]` configures as Sonnet @ effort "high" (see
        // `ClaudeConfig::default` in core/src/config.rs — NOT "normal
        // reasoning", which this comment claimed for a cost-relevant knob).
        // An app can pass "compose" for Opus @ xhigh, or override
        // model/effort directly.
        let role = ctx
            .params
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("research")
            .to_string();
        let model = ctx
            .params
            .get("model")
            .and_then(Value::as_str)
            .map(String::from);
        let effort = ctx
            .params
            .get("effort")
            .and_then(Value::as_str)
            .map(String::from);

        // Durable-execution restore: a prior attempt's checkpoint comes back on
        // re-claim. Done → return the stored result with zero new spend.
        // Resume → continue the agent session mid-run. Anything doubtful →
        // fresh (the seam's poisoned-checkpoint escape counts failures).
        let (mut state, resumed_from_checkpoint) = match plan_from_restore(ctx.restore()) {
            Plan::Done(mut result) => {
                if let Value::Object(map) = &mut result {
                    map.insert("resumed_from_checkpoint".into(), Value::Bool(true));
                }
                save_report_artifact(&ctx, &result).await;
                return Ok(result);
            }
            Plan::Resume(state) => (state, true),
            Plan::Fresh => (RunState::fresh(caller_session), false),
        };

        // A resumed turn is a follow-up: the agent already holds the topic and its
        // sources in session, so a full "you are a web research agent…" preamble
        // would waste turns re-establishing context. All prompts pin the SAME
        // JSON shape so a resumed report is held to the same contract.
        let shape = "Respond with ONLY a JSON object (no markdown fences, no prose) of this \
             shape:\n{\"summary\": string, \"key_findings\": string[], \
             \"sources\": [{\"url\": string, \"title\": string}]}";
        let report_schema = json!({
            "type": "object",
            "required": ["summary", "key_findings", "sources"],
            "properties": {
                "summary": { "type": "string" },
                "key_findings": { "type": "array", "items": { "type": "string" } },
                "sources": { "type": "array" }
            }
        });

        let mut final_parsed: Option<Value> = None;
        let mut last_text = state.partial.clone().unwrap_or_default();
        // The step-cap check below is the loop's own initial value, so no
        // branch leaves `stop_reason` unset.
        let mut stop_reason = StopReason::StepCap;

        loop {
            if state.steps_done >= MAX_STEPS {
                break;
            }
            let step_turns = match next_step_turns(max_turns, turns_per_step, state.turns_used) {
                StepPlan::Run(t) => t,
                StepPlan::Exhausted => {
                    stop_reason = StopReason::TurnsExhausted;
                    break;
                }
            };
            // Stop gracefully at the spend ceiling and return the partial
            // (checkpointed) findings instead of erroring the job. Checked
            // before EVERY step, including the first one of a resumed attempt:
            // restored `spent_usd` counts against the run ceiling, and the job
            // ceiling has nothing to say when the job carries no `budget_usd`.
            let step_ceiling = match next_step_budget(
                max_budget_usd,
                state.spent_usd,
                ctx.remaining_budget_usd().await?,
            ) {
                BudgetPlan::Run(ceiling) => ceiling,
                BudgetPlan::Exhausted => {
                    stop_reason = StopReason::BudgetExhausted;
                    break;
                }
            };

            let prompt = match (&state.session_id, state.steps_done) {
                (None, _) => format!(
                    "You are a web research agent. Research the topic below using web search and \
                     page fetches. Cross-check important claims across at least two sources.\n\n\
                     Topic: {query}\n\n{shape}"
                ),
                (Some(_), 0) => format!(
                    "Follow-up on the research so far. Using the context you already have (search \
                     further only if needed):\n\n{query}\n\n{shape}"
                ),
                (Some(_), _) => format!(
                    "Continue the research in progress on this topic — you already hold its \
                     context in session; search further only where needed, then finish:\n\n\
                     {query}\n\n{shape}"
                ),
            };

            let mut request = ResearchRequest::new(prompt).with_role(role.clone());
            request.max_turns = step_turns;
            request.model = model.clone();
            request.effort = effort.clone();
            request.resume_session = state.session_id.clone();
            // The run's REMAINING headroom, not the whole run ceiling: the last
            // step must not be able to overshoot the total.
            request.max_budget_usd = step_ceiling;
            // Actually use the json_schema guardrail so the model is steered to
            // the shape we promise downstream instead of accepting any object.
            request.json_schema = Some(report_schema.clone());

            // The budget-consuming step. Only NEW spend is metered here —
            // restored spend already lives in the job's ledger + spent_usd.
            let output = ctx.research(request).await?;
            fold_output(
                &mut state,
                output.cost_usd,
                output.num_turns,
                output.session_id.clone(),
                step_turns.map(u64::from),
                output.duration_ms,
            );
            last_text = output.text.clone();

            // Before giving up on structure, salvage a fenced/prose-wrapped object
            // from the raw text — no re-run, no extra cost, on text already paid
            // for. `structured` still means "matched the promised shape".
            let parsed = output.json.clone().or_else(|| salvage_json(&output.text));
            if parsed.as_ref().is_some_and(is_report_shaped) {
                final_parsed = parsed;
                stop_reason = StopReason::Completed;
                break;
            }

            // Checkpoint the unfinished step: session id, cumulative spend,
            // partial findings, and the step/turn cursor. Forced write — losing
            // it means re-paying the step on resume. Persistence failure never
            // fails the job (the sink reports, the seam counts).
            state.partial = Some(truncate_chars(&output.text, PARTIAL_CAP_CHARS));
            ctx.checkpoint_now(state.to_value()).await;

            if state.session_id.is_none() {
                // No session to resume — looping would restart from zero.
                stop_reason = StopReason::NoSession;
                break;
            }
            if max_turns.is_none() && turns_per_step.is_none() {
                // Uncapped single-call mode (pre-port behavior).
                stop_reason = StopReason::SingleCall;
                break;
            }
        }

        let structured = final_parsed.is_some();
        // A run that produced nothing at any price is a failure, not an empty
        // success: letting it return `Ok` marks the job SUCCEEDED, fires the
        // result webhook and indexes a search doc for `{report: ""}`. The
        // result is deliberately NOT checkpointed here — a `result` in the
        // blob would make the next attempt take `Plan::Done` and hand the same
        // nothing back as a success.
        if produced_nothing(structured, &last_text) {
            return Err(empty_run_failure(
                stop_reason,
                state.steps_done,
                state.spent_usd,
            ));
        }
        let report = match final_parsed {
            Some(v) => v,
            None => Value::String(last_text),
        };

        // N25: the report becomes datasets. `topic` is the KEY SPACE, not the
        // question: a follow-up run asks something different ("Source X changed,
        // update the findings") but belongs to the same body of knowledge, so it
        // passes the original query back as `topic` and updates those records
        // instead of forking a second copy under its own slug.
        let topic = ctx
            .params
            .get("topic")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .unwrap_or(query.as_str())
            .to_string();
        let kb = build_knowledge(&ctx, &topic, &report, &KbOptions::from_params(&ctx.params)).await;

        let result = json!({
            "query": query,
            "topic": topic,
            "report": report,
            "structured": structured,
            "resumed": resumed_callers_session(caller_resumed, resumed_from_checkpoint),
            "resumed_from_checkpoint": resumed_from_checkpoint,
            "steps": state.steps_done,
            "cost_usd": state.spent_usd,
            "duration_ms": state.duration_ms,
            "num_turns": state.turns_used,
            "session_id": state.session_id,
            "stop_reason": stop_reason.as_str(),
            "datasets": kb.datasets,
            "snapshots": kb.snapshots,
            "watch_requests": kb.watch_requests,
            "sources_truncated": kb.sources_truncated,
            // Routes this run's `research/findings` + `research/sources`
            // revisions into the search index and the saved-search alerts, the
            // same way the extractor routes its own product dataset.
            "index_datasets": [
                { "app": "research", "dataset": FINDINGS_DATASET },
                { "app": "research", "dataset": SOURCES_DATASET },
            ],
        });

        // Final checkpoint carries the whole result: a crash between here and
        // job completion costs a restore-and-return, not a re-run.
        state.partial = None;
        state.result = Some(result.clone());
        ctx.checkpoint_now(state.to_value()).await;

        save_report_artifact(&ctx, &result).await;
        Ok(result)
    }
}

/// True when a research report matches the promised shape — a `summary` string
/// plus `key_findings` and `sources` arrays — **and carries something to read**.
/// Guards against marking a hallucinated or wrong-shape object as `structured`.
///
/// The content half of the check exists because the `json_schema` guardrail is
/// good at its job: a model that has nothing to say still answers in the
/// promised shape, and `{"summary": "", "key_findings": [], "sources": []}` used
/// to be stamped `structured: true` / `stop_reason: completed`. That is the most
/// expensive lie this app can tell — the job goes green, the result webhook
/// fires and a search doc is written for a report that says nothing.
///
/// The bar is deliberately low: ONE non-blank summary or key finding is enough.
/// A real summary with an empty `sources` array is thin, not empty — sourcing
/// quality is a judgement for the consumer, and rejecting it here would spend
/// more steps re-asking a model that already answered.
fn is_report_shaped(v: &Value) -> bool {
    let Some(summary) = v.get("summary").and_then(Value::as_str) else {
        return false;
    };
    let Some(findings) = v.get("key_findings").and_then(Value::as_array) else {
        return false;
    };
    if !v.get("sources").is_some_and(Value::is_array) {
        return false;
    }
    !summary.trim().is_empty() || findings.iter().any(has_content)
}

/// Whether one `key_findings` entry carries anything. The promised shape is
/// `string[]`, but a model that returns richer entries has still said
/// something, so only blanks, nulls and empty containers count as nothing.
fn has_content(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::String(s) => !s.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
        Value::Bool(_) | Value::Number(_) => true,
    }
}

/// Whether this attempt actually continued the session the CALLER named.
///
/// The published `resumed` key used to report the `session_id` param verbatim,
/// but a re-claimed attempt takes [`Plan::Resume`] and **discards** that param
/// in favour of the checkpoint's session — so a crashed job whose params
/// happened to carry a `session_id` claimed a caller drill-down that never
/// happened. The two resume kinds are independent and both are published:
/// `resumed_from_checkpoint` is the runtime re-claiming an interrupted attempt.
fn resumed_callers_session(caller_named_a_session: bool, resumed_from_checkpoint: bool) -> bool {
    caller_named_a_session && !resumed_from_checkpoint
}

/// Whether the run produced nothing a consumer could use.
///
/// Narrow on purpose: **only** an unstructured run whose whole accumulated text
/// is empty or whitespace. A truncated run that carries real prose — the
/// `budget_exhausted` partial above all — is a *success*: the caller paid for
/// those findings and `stop_reason` already says the report is unfinished.
/// Widening this to "unstructured" or "truncated" would throw away work that
/// was bought and is worth reading.
fn produced_nothing(structured: bool, accumulated_text: &str) -> bool {
    !structured && accumulated_text.trim().is_empty()
}

/// The failure a content-free run reports, carrying what it cost.
///
/// A run that stopped on the spend ceiling with nothing to show is
/// **deterministic**: the checkpoint holds the spend, so every retry restores
/// it, re-hits the same wall and produces the same nothing while the resume
/// counter climbs toward the checkpoint-discarding cap. That is exactly the
/// case [`pumper_core::Error::BudgetExhausted`] is typed for (see
/// `core/src/error.rs:200-206`), so it is reported as terminal rather than
/// retried into the ground. Every other empty run — a CLI hiccup, a session
/// that vanished, a step cap hit on silence — can genuinely differ next
/// attempt, so it stays a retryable `Error::App`.
///
/// Either way the message names the spend: a failed run must not also lose the
/// record of what it cost (the cost events themselves are already in the
/// ledger — `ctx.research` meters each call as it happens).
fn empty_run_failure(stop_reason: StopReason, steps: u32, spent_usd: f64) -> Error {
    let why = format!(
        "research produced no content: {} step(s) ran, ${spent_usd:.4} spent, stop_reason={}",
        steps,
        stop_reason.as_str()
    );
    match stop_reason {
        StopReason::BudgetExhausted => Error::BudgetExhausted(why),
        _ => Error::App(why),
    }
}

/// Best-effort `report.json` dump.
///
/// The artifact is decorative — every byte of it is already in the job result —
/// but writing it with `?` turned a transient `tokio::fs` failure into a
/// *retryable* `Error::Io` on a run that had already finished. The retry
/// restores the finished checkpoint, takes [`Plan::Done`], and hits the same
/// write; `max_resume_failures` of those and the worker discards the checkpoint
/// and re-runs the whole research at full price. A JSON dump nobody reads is
/// not worth a paid re-run, so a failure here is logged and swallowed.
async fn save_report_artifact(ctx: &AppContext, result: &Value) {
    let bytes = match serde_json::to_vec_pretty(result) {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!("research: report.json could not be serialized: {e}");
            return;
        }
    };
    if let Err(e) = ctx.save_artifact("report.json", &bytes).await {
        tracing::warn!("research: report.json artifact write failed (result unaffected): {e}");
    }
}

// ── N25: research as a living knowledge base ─────────────────────────────────
//
// Everything below turns one paid run's report into two datasets:
// `research/findings` (one record per key finding) and `research/sources` (one
// record per cited URL). The run's own loop above is untouched — this stage
// reads the finished report and writes, so a failure here can never cost a
// re-run of the model.
//
// NOTE on the self-hosted fetch loop (N15): `[claude] self_hosted_tools` makes
// the research SUBPROCESS fetch through this node's `fetch` MCP tool under a
// per-job token. `snapshot_sources` below is a different thing entirely — it is
// this app calling `ctx.fetch` directly, after the subprocess is gone, to
// archive the URLs the report cited. The two are independent: snapshots behave
// identically with `self_hosted_tools` on or off.

/// Dataset holding one record per key finding, keyed `{slug(topic)}#{index}`.
const FINDINGS_DATASET: &str = "findings";
/// Dataset holding one record per cited URL, keyed by the URL itself.
const SOURCES_DATASET: &str = "sources";
/// Default cron for a proposed source watch: once a day, 07:00 UTC.
const DEFAULT_WATCH_CRON: &str = "0 0 7 * * *";
/// Max chars of a slug — it is a key, not a title.
const SLUG_CAP_CHARS: usize = 60;

/// One source the report cited.
#[derive(Debug, Clone, PartialEq)]
struct Cited {
    url: String,
    title: Option<String>,
}

/// What a snapshot attempt produced for one cited URL.
#[derive(Debug, Clone, PartialEq)]
enum Snapshot {
    /// Not attempted (`snapshot_sources` off, or the budget ran out first).
    Skipped,
    /// The body was fetched, saved and hashed — the citation is re-derivable.
    Saved {
        artifact: String,
        chars: usize,
        sha256: String,
        fetched_url: String,
    },
    /// Attempted and failed. The reason is recorded rather than dropped, and it
    /// never fails the run: the report was already paid for.
    Failed { reason: String },
}

/// A stable record key from free text: lowercase, non-alphanumerics collapsed
/// to single dashes, trimmed, capped. This is what makes a re-run of the same
/// topic UPDATE its findings instead of appending a second copy of them, so it
/// must stay deterministic — never fold in a timestamp, a session id or a job
/// id.
fn slug(text: &str) -> String {
    let mut out = String::new();
    let mut len = 0usize;
    let mut pending_dash = false;
    for c in text.trim().to_lowercase().chars() {
        if !c.is_alphanumeric() {
            pending_dash = true;
            continue;
        }
        let width = usize::from(pending_dash && len > 0) + 1;
        if len + width > SLUG_CAP_CHARS {
            break;
        }
        if pending_dash && len > 0 {
            out.push('-');
        }
        pending_dash = false;
        out.push(c);
        len += width;
    }
    if out.is_empty() {
        "topic".to_string()
    } else {
        out
    }
}

/// Record key for the i-th key finding of a topic.
fn finding_key(topic_slug: &str, index: usize) -> String {
    format!("{topic_slug}#{index}")
}

/// The sources a report cited, in order, deduped by URL.
///
/// Deliberately tolerant of shape (the array is model output): an entry may be
/// a bare URL string or an object with `url`/`title`. Deliberately intolerant of
/// scheme: anything that is not `http(s)` is dropped rather than stored, because
/// every downstream use of this list — the snapshot fetch, the proposed watch,
/// the record key — treats it as a URL to fetch, and a `file:///etc/passwd` in a
/// model's citation list must never become one.
fn cited_sources(report: &Value) -> Vec<Cited> {
    let Some(items) = report.get("sources").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out: Vec<Cited> = Vec::new();
    for item in items {
        let (url, title) = match item {
            Value::String(s) => (s.trim().to_string(), None),
            Value::Object(_) => (
                item.get("url")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
                item.get("title")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .map(String::from),
            ),
            _ => continue,
        };
        if !is_web_url(&url) {
            continue;
        }
        if out.iter().any(|c| c.url == url) {
            continue;
        }
        out.push(Cited { url, title });
    }
    out
}

/// Whether a cited string is a URL this node would actually fetch.
fn is_web_url(url: &str) -> bool {
    (url.starts_with("http://") || url.starts_with("https://")) && url.len() > "https://".len()
}

/// Applies the per-run fan-out ceiling to the cited list, reporting whether it
/// bit. ONE cap governs every per-source action (record, snapshot, watch) so
/// `sources_truncated` has a single meaning.
fn cap_sources(cited: &[Cited], cap: usize) -> (&[Cited], bool) {
    if cited.len() > cap {
        (&cited[..cap], true)
    } else {
        (cited, false)
    }
}

/// The `findings` records for one report. Each record is about the FINDING, not
/// about the run that produced it: the query text, the session and the spend
/// live in the job result and in the revision's `job_id` provenance. Putting a
/// per-run value in here would mark every record `changed` on every run and turn
/// watches, triggers and the yield ledger into a churn feed.
fn finding_records(
    topic: &str,
    findings: &[Value],
    source_urls: &[String],
) -> Vec<(String, Value)> {
    let topic_slug = slug(topic);
    findings
        .iter()
        .enumerate()
        .filter(|(_, f)| has_content(f))
        .map(|(i, finding)| {
            (
                finding_key(&topic_slug, i),
                json!({
                    "topic": topic,
                    "index": i,
                    "finding": finding.clone(),
                    "sources": source_urls,
                }),
            )
        })
        .collect()
}

/// The `sources` record for one cited URL and whatever its snapshot produced.
fn source_record(cited: &Cited, snapshot: &Snapshot) -> Value {
    let mut rec = json!({
        "url": cited.url,
        "title": cited.title,
        "snapshot": Value::Null,
        "snapshot_error": Value::Null,
    });
    let map = rec.as_object_mut().expect("json! built an object");
    match snapshot {
        Snapshot::Skipped => {}
        Snapshot::Saved {
            artifact,
            chars,
            sha256,
            fetched_url,
        } => {
            map.insert(
                "snapshot".into(),
                json!({
                    "artifact": artifact,
                    "chars": chars,
                    "sha256": sha256,
                    "fetched_url": fetched_url,
                }),
            );
        }
        Snapshot::Failed { reason } => {
            map.insert("snapshot_error".into(), json!(reason));
        }
    }
    rec
}

/// The provenance of a `sources` record.
///
/// `source_url` is always the cited URL — that is what the record is about, and
/// on a snapshot it is the POST-REDIRECT url the body actually came from.
/// `artifact_sha` is stamped **only** when a body was really fetched and saved:
/// a citation nobody archived is not re-derivable, and claiming a hash for it is
/// exactly the fabrication `Provenance`'s contract forbids.
fn source_provenance(cited: &Cited, snapshot: &Snapshot) -> Provenance {
    match snapshot {
        Snapshot::Saved {
            sha256,
            fetched_url,
            ..
        } => Provenance {
            source_url: Some(fetched_url.clone()),
            artifact_sha: Some(sha256.clone()),
            ..Provenance::default()
        },
        _ => Provenance {
            source_url: Some(cited.url.clone()),
            ..Provenance::default()
        },
    }
}

/// Whether there is still money to spend on the NEXT snapshot fetch. `None` is
/// "this job carries no ceiling", not "this job is broke".
fn snapshot_affordable(remaining: Option<f64>) -> bool {
    !matches!(remaining, Some(r) if r <= 0.0)
}

/// The `POST /schedules` body that would put one cited URL under a standing
/// `watch`. The app EMITS these rather than creating them: an app has no
/// schedule-writing seam (`AppContext` exposes datasets, artifacts and engines,
/// not the job store), exactly as `provisioner` emits a `planned` catalog row
/// and schedules nothing. See docs/features/apps.md §research for the recipe.
fn watch_request(url: &str, cron: &str) -> Value {
    json!({ "app": "watch", "cron": cron, "params": { "url": url } })
}

/// Artifact name for the i-th snapshotted source.
fn snapshot_artifact_name(index: usize) -> String {
    format!("source-{index}.md")
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// The N25 params, parsed once from `ctx.params`.
struct KbOptions {
    /// Write `findings`/`sources` records at all (default true).
    persist: bool,
    /// Fetch and archive each cited URL (default false — it is metered).
    snapshot: bool,
    /// Emit a `POST /schedules` body per cited URL (default false).
    watch: bool,
    /// Per-run ceiling on per-source actions.
    cap: usize,
    /// Passed through to the snapshot fetch: accept an archived body up to N
    /// seconds old instead of going live (`readable`'s archive tier).
    archive_max_age: Option<u64>,
    /// Cron for the proposed watches.
    watch_cron: String,
}

impl KbOptions {
    fn from_params(params: &Value) -> Self {
        Self {
            persist: params
                .get("persist")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            snapshot: params
                .get("snapshot_sources")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            watch: params
                .get("watch_sources")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            // The operator's `[research] max_watched_sources` is the DEFAULT
            // here (see the plumbing note in docs/features/apps.md §research):
            // `ResearchConfig::default()` is the single definition of the
            // number, so the app and the config key cannot drift apart.
            cap: params
                .get("max_watched_sources")
                .and_then(Value::as_u64)
                .map(|n| n as usize)
                .unwrap_or_else(|| ResearchConfig::default().max_watched_sources),
            archive_max_age: params.get("archive_max_age").and_then(Value::as_u64),
            watch_cron: params
                .get("watch_cron")
                .and_then(Value::as_str)
                .filter(|c| !c.trim().is_empty())
                .unwrap_or(DEFAULT_WATCH_CRON)
                .to_string(),
        }
    }
}

/// What the knowledge-base stage produced, in the shape the result publishes.
struct Knowledge {
    datasets: Value,
    snapshots: Value,
    watch_requests: Vec<Value>,
    sources_truncated: bool,
}

impl Knowledge {
    /// The empty stage: an unstructured run has no findings and no citations to
    /// persist, so it publishes the same keys with nothing in them rather than
    /// dropping keys a consumer codes against.
    fn empty() -> Self {
        Self {
            datasets: json!({
                "persisted": false,
                "findings": [],
                "sources": [],
                "findings_new": 0,
                "findings_changed": 0,
                "sources_new": 0,
                "sources_changed": 0,
                "error": Value::Null,
            }),
            snapshots: json!({ "attempted": 0, "saved": 0, "failed": 0 }),
            watch_requests: Vec::new(),
            sources_truncated: false,
        }
    }
}

/// Snapshots one cited URL through the metered tiered fetcher and saves it as a
/// job artifact, so the citation can be re-read exactly as it was when the
/// report cited it.
///
/// Every failure mode returns [`Snapshot::Failed`] instead of an error: the
/// research report has already been paid for, and losing it because a cited page
/// 404s would re-buy the whole run on the next attempt.
async fn snapshot_source(
    ctx: &AppContext,
    index: usize,
    cited: &Cited,
    archive_max_age: Option<u64>,
) -> Snapshot {
    let mut req = FetchRequest::new(&cited.url);
    req.to_markdown = true;
    req.archive_max_age = archive_max_age;
    let outcome = match ctx.fetch(req).await {
        Ok(outcome) => outcome,
        Err(e) => {
            return Snapshot::Failed {
                reason: format!("fetch failed: {e}"),
            }
        }
    };
    let markdown = outcome
        .markdown
        .clone()
        .or_else(|| outcome.text.clone())
        .unwrap_or_default();
    if extracted_nothing(&markdown) {
        // Same judgement `watch` and `readable` make: a 200 that yields no
        // readable text is a failed extraction, and hashing the empty body
        // would stamp `artifact_sha` on a snapshot of nothing.
        return Snapshot::Failed {
            reason: format!(
                "extracted no content (engine {}, status {:?})",
                outcome.engine, outcome.status
            ),
        };
    }
    let artifact = snapshot_artifact_name(index);
    if let Err(e) = ctx.save_artifact(&artifact, markdown.as_bytes()).await {
        return Snapshot::Failed {
            reason: format!("artifact write failed: {e}"),
        };
    }
    Snapshot::Saved {
        artifact,
        chars: markdown.chars().count(),
        sha256: hex_sha256(markdown.as_bytes()),
        fetched_url: outcome.url.clone(),
    }
}

/// Turns a finished report into the two datasets, the snapshots and the
/// proposed watches. Never fails the run: a store or filesystem failure here is
/// reported in `datasets.error` and logged, because the model call it would
/// discard is the most expensive thing this app does.
async fn build_knowledge(
    ctx: &AppContext,
    topic: &str,
    report: &Value,
    opts: &KbOptions,
) -> Knowledge {
    let cited = cited_sources(report);
    let (capped, sources_truncated) = cap_sources(&cited, opts.cap);

    let mut snapshots = Vec::with_capacity(capped.len());
    let mut attempted = 0usize;
    let mut saved = 0usize;
    let mut failed = 0usize;
    for (i, c) in capped.iter().enumerate() {
        if !opts.snapshot {
            snapshots.push(Snapshot::Skipped);
            continue;
        }
        // The run's remaining headroom is re-read per source: a snapshot is a
        // metered fetch like any other, and a 20-URL citation list must not be
        // able to walk past the job's ceiling one page at a time.
        let remaining = ctx.remaining_budget_usd().await.unwrap_or(None);
        if !snapshot_affordable(remaining) {
            snapshots.push(Snapshot::Skipped);
            continue;
        }
        attempted += 1;
        let snap = snapshot_source(ctx, i, c, opts.archive_max_age).await;
        match &snap {
            Snapshot::Saved { .. } => saved += 1,
            Snapshot::Failed { .. } => failed += 1,
            Snapshot::Skipped => {}
        }
        snapshots.push(snap);
    }

    let watch_requests = if opts.watch {
        capped
            .iter()
            .map(|c| watch_request(&c.url, &opts.watch_cron))
            .collect()
    } else {
        Vec::new()
    };

    let mut out = Knowledge {
        snapshots: json!({ "attempted": attempted, "saved": saved, "failed": failed }),
        watch_requests,
        sources_truncated,
        ..Knowledge::empty()
    };
    if !opts.persist {
        return out;
    }

    let source_urls: Vec<String> = capped.iter().map(|c| c.url.clone()).collect();
    let mut error: Option<String> = None;
    let mut written_sources: Vec<String> = Vec::new();
    let (mut sources_new, mut sources_changed) = (0usize, 0usize);
    for (c, snap) in capped.iter().zip(snapshots.iter()) {
        let record = source_record(c, snap);
        match ctx
            .upsert_with_provenance(SOURCES_DATASET, &c.url, &record, source_provenance(c, snap))
            .await
        {
            Ok(change) => {
                match change {
                    ChangeKind::New => sources_new += 1,
                    ChangeKind::Changed => sources_changed += 1,
                    ChangeKind::Unchanged => {}
                }
                written_sources.push(c.url.clone());
            }
            Err(e) => {
                tracing::warn!("research: sources upsert failed for {}: {e}", c.url);
                error.get_or_insert_with(|| e.to_string());
            }
        }
    }

    let findings = report
        .get("key_findings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let items = finding_records(topic, &findings, &source_urls);
    let mut written_findings: Vec<String> = Vec::new();
    let (mut findings_new, mut findings_changed) = (0usize, 0usize);
    if !items.is_empty() {
        match ctx.upsert_many(FINDINGS_DATASET, &items).await {
            Ok(summary) => {
                findings_new = summary.new.len();
                findings_changed = summary.changed.len();
                written_findings = items.iter().map(|(k, _)| k.clone()).collect();
            }
            Err(e) => {
                tracing::warn!("research: findings upsert failed: {e}");
                error.get_or_insert_with(|| e.to_string());
            }
        }
    }

    out.datasets = json!({
        "persisted": true,
        "findings": written_findings,
        "sources": written_sources,
        "findings_new": findings_new,
        "findings_changed": findings_changed,
        "sources_new": sources_new,
        "sources_changed": sources_changed,
        "error": error,
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_matching_the_promised_shape_is_structured() {
        // Realistic slice of what the research agent returns on a good run.
        let v = json!({
            "summary": "Rust 1.80 stabilized LazyLock, replacing most lazy_static uses.",
            "key_findings": ["LazyLock is in std::sync as of 1.80"],
            "sources": [{"url": "https://blog.rust-lang.org/2024/07/25/Rust-1.80.0.html",
                         "title": "Announcing Rust 1.80.0"}]
        });
        assert!(is_report_shaped(&v));
    }

    #[test]
    fn some_json_came_back_is_not_the_same_as_structured() {
        // A hallucinated or wrong-shape object must not be stamped trustworthy.
        assert!(!is_report_shaped(&json!({"answer": "42"})));
        // Right keys, wrong types.
        assert!(!is_report_shaped(&json!({
            "summary": ["not", "a", "string"], "key_findings": [], "sources": []
        })));
        // One promised key missing.
        assert!(!is_report_shaped(
            &json!({"summary": "s", "key_findings": []})
        ));
    }

    #[test]
    fn an_in_shape_refusal_with_nothing_in_it_is_not_structured() {
        // What the json_schema guardrail turns a model that has nothing to say
        // into: perfect shape, zero research. Stamping it `completed` reports a
        // finished report that says nothing.
        assert!(!is_report_shaped(&json!({
            "summary": "", "key_findings": [], "sources": []
        })));
        // Whitespace is not content either.
        assert!(!is_report_shaped(&json!({
            "summary": "   \n\t ", "key_findings": ["", "  "], "sources": []
        })));
        // One real finding is enough even without a summary…
        assert!(is_report_shaped(&json!({
            "summary": "", "key_findings": ["VAT threshold is 2m CZK"], "sources": []
        })));
        // …and a real summary is enough without sources: thin is not empty.
        assert!(is_report_shaped(&json!({
            "summary": "The threshold rose in 2023.", "key_findings": [], "sources": []
        })));
    }

    #[test]
    fn bare_prose_or_null_is_not_structured() {
        assert!(!is_report_shaped(&Value::String("a prose report".into())));
        assert!(!is_report_shaped(&Value::Null));
    }

    // ── Durable-execution checkpoint port ───────────────────────────────────

    #[test]
    fn checkpoint_state_round_trips_through_the_blob() {
        let state = RunState {
            v: STATE_VERSION,
            session_id: Some("sess-abc".into()),
            steps_done: 2,
            turns_used: 16,
            spent_usd: 0.37,
            duration_ms: 4200,
            partial: Some("partial findings so far".into()),
            result: None,
        };
        let blob = state.to_value();
        // Exactly what the sink stores and `restore()` hands back.
        match plan_from_restore(Some(&blob)) {
            Plan::Resume(restored) => assert_eq!(restored, state),
            other => panic!("expected Resume, got {other:?}"),
        }
    }

    #[test]
    fn resume_vs_fresh_decision_over_advisory_blobs() {
        // No checkpoint → fresh first attempt.
        assert_eq!(plan_from_restore(None), Plan::Fresh);
        // Unrecognizable shapes → fresh, never an error (poisoned escape).
        assert_eq!(plan_from_restore(Some(&json!("garbage"))), Plan::Fresh);
        assert_eq!(plan_from_restore(Some(&json!({"foo": 1}))), Plan::Fresh);
        // Future/foreign version → fresh.
        assert_eq!(
            plan_from_restore(Some(&json!({"v": 99, "session_id": "s"}))),
            Plan::Fresh
        );
        // Insane spend → fresh.
        assert_eq!(
            plan_from_restore(Some(&json!({"v": 1, "spent_usd": -3.0}))),
            Plan::Fresh
        );
        // Sane mid-run state → resume.
        assert!(matches!(
            plan_from_restore(Some(
                &json!({"v": 1, "session_id": "s", "steps_done": 1, "spent_usd": 0.1})
            )),
            Plan::Resume(_)
        ));
    }

    #[test]
    fn finished_checkpoint_returns_stored_result_without_new_spend() {
        // A checkpoint carrying `result` means the prior attempt finished:
        // the plan is Done(result) — the run returns it and never reaches a
        // metered call, so restored budget cannot be spent twice.
        let done = RunState {
            v: STATE_VERSION,
            session_id: Some("sess-abc".into()),
            steps_done: 1,
            turns_used: 8,
            spent_usd: 0.42,
            duration_ms: 900,
            partial: None,
            result: Some(json!({"query": "q", "structured": true, "cost_usd": 0.42})),
        };
        match plan_from_restore(Some(&done.to_value())) {
            Plan::Done(result) => assert_eq!(result["cost_usd"], json!(0.42)),
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn restored_budget_is_folded_once_and_only_new_spend_accrues() {
        // A restored attempt starts from the checkpointed cumulative spend…
        let mut state = RunState {
            v: STATE_VERSION,
            session_id: Some("sess-abc".into()),
            steps_done: 1,
            turns_used: 8,
            spent_usd: 0.30,
            duration_ms: 1000,
            partial: None,
            result: None,
        };
        // …and folds ONLY the new step's cost on top: 0.30 + 0.20 = 0.50.
        fold_output(
            &mut state,
            Some(0.20),
            Some(7),
            Some("sess-abc2".into()),
            Some(8),
            Some(250),
        );
        assert!((state.spent_usd - 0.50).abs() < 1e-9);
        assert_eq!(state.steps_done, 2);
        assert_eq!(state.turns_used, 15);
        assert_eq!(state.session_id.as_deref(), Some("sess-abc2"));
    }

    #[test]
    fn fold_keeps_prior_session_and_uses_fallback_turns_when_engine_omits_them() {
        let mut state = RunState::fresh(Some("caller-sess".into()));
        fold_output(&mut state, None, None, None, Some(8), None);
        // Missing session id must not clobber a resumable one.
        assert_eq!(state.session_id.as_deref(), Some("caller-sess"));
        // Unknown turn count advances conservatively by the chunk size, so the
        // turn budget still terminates the loop.
        assert_eq!(state.turns_used, 8);
        assert_eq!(state.spent_usd, 0.0);
    }

    #[test]
    fn step_turn_budget_chunks_and_exhausts() {
        // 15 total in default chunks of 8: 8, then 7, then exhausted.
        assert_eq!(next_step_turns(Some(15), None, 0), StepPlan::Run(Some(8)));
        assert_eq!(next_step_turns(Some(15), None, 8), StepPlan::Run(Some(7)));
        assert_eq!(next_step_turns(Some(15), None, 15), StepPlan::Exhausted);
        // Explicit chunk wins over the default.
        assert_eq!(
            next_step_turns(Some(10), Some(3), 3),
            StepPlan::Run(Some(3))
        );
        // No total cap, no chunk: one uncapped call (pre-port behavior).
        assert_eq!(next_step_turns(None, None, 0), StepPlan::Run(None));
        // No total cap with a chunk: capped steps (loop bounded by MAX_STEPS).
        assert_eq!(next_step_turns(None, Some(5), 40), StepPlan::Run(Some(5)));
    }

    #[test]
    fn turns_charged_are_capped_by_the_step_and_not_a_session_cumulative_count() {
        // Per-invocation reading: the reported count is the truth.
        assert_eq!(step_turns_used(Some(5), Some(8)), 5);
        // Session-cumulative reading: a counter that already includes prior
        // steps must not be charged in full again — the step's own
        // `--max-turns` bounds what it could possibly have used.
        assert_eq!(step_turns_used(Some(37), Some(8)), 8);
        // Engine omitted the count: advance by the whole chunk so the turn
        // budget still terminates the loop.
        assert_eq!(step_turns_used(None, Some(8)), 8);
        // Uncapped single call: nothing to clamp against.
        assert_eq!(step_turns_used(Some(42), None), 42);
        assert_eq!(step_turns_used(None, None), 1);
    }

    #[test]
    fn max_budget_usd_is_a_run_total_not_a_per_call_ceiling() {
        // Fresh run: the first call may use the whole ceiling…
        assert_eq!(
            next_step_budget(Some(0.50), 0.0, None),
            BudgetPlan::Run(Some(0.50))
        );
        // …and every later call only what is left of it. Before the fix each
        // of up to MAX_STEPS calls got the full 0.50.
        match next_step_budget(Some(0.50), 0.30, None) {
            BudgetPlan::Run(Some(ceiling)) => assert!((ceiling - 0.20).abs() < 1e-9),
            other => panic!("expected a clamped ceiling, got {other:?}"),
        }
        // Spent out (and over-spent) stops the run instead of issuing a call.
        assert_eq!(
            next_step_budget(Some(0.50), 0.50, None),
            BudgetPlan::Exhausted
        );
        assert_eq!(
            next_step_budget(Some(0.50), 0.90, None),
            BudgetPlan::Exhausted
        );
        // A remainder too small to finish a step is not worth paying for.
        assert_eq!(
            next_step_budget(Some(0.50), 0.4999, None),
            BudgetPlan::Exhausted
        );
    }

    #[test]
    fn the_tighter_of_the_run_and_job_ceilings_governs() {
        // No ceiling anywhere: uncapped, as before.
        assert_eq!(next_step_budget(None, 0.0, None), BudgetPlan::Run(None));
        // Job budget only (the path that already worked) still walls the run.
        assert_eq!(
            next_step_budget(None, 0.0, Some(0.0)),
            BudgetPlan::Exhausted
        );
        assert_eq!(
            next_step_budget(None, 0.0, Some(2.0)),
            BudgetPlan::Run(Some(2.0))
        );
        // Job headroom tighter than the run's remaining ceiling wins…
        assert_eq!(
            next_step_budget(Some(5.0), 0.0, Some(0.75)),
            BudgetPlan::Run(Some(0.75))
        );
        // …and vice versa.
        assert_eq!(
            next_step_budget(Some(0.25), 0.0, Some(9.0)),
            BudgetPlan::Run(Some(0.25))
        );
    }

    #[test]
    fn resumed_reports_the_callers_session_not_a_runtime_reclaim() {
        // The caller asked to drill down on a session and got it.
        assert!(resumed_callers_session(true, false));
        // A re-claimed attempt uses the CHECKPOINT's session; the caller's
        // `session_id` param was discarded, so claiming a drill-down would be
        // a lie even though the param is there.
        assert!(!resumed_callers_session(true, true));
        // Plain runs and plain re-claims are not caller resumes at all.
        assert!(!resumed_callers_session(false, false));
        assert!(!resumed_callers_session(false, true));
    }

    #[test]
    fn duration_is_cumulative_like_the_other_three_counters() {
        // `steps`, `cost_usd` and `num_turns` all survive a re-claim; duration
        // used to restart at 0, publishing two grains in one result.
        let mut state = RunState {
            v: STATE_VERSION,
            session_id: Some("sess-abc".into()),
            steps_done: 1,
            turns_used: 8,
            spent_usd: 0.30,
            duration_ms: 12_000,
            partial: None,
            result: None,
        };
        fold_output(&mut state, None, Some(1), None, Some(8), Some(3_500));
        assert_eq!(state.duration_ms, 15_500);
        // A v1 blob written before the field existed restores as 0 rather than
        // failing to parse — no STATE_VERSION bump, no discarded checkpoints.
        match plan_from_restore(Some(
            &json!({"v": 1, "session_id": "s", "steps_done": 1, "spent_usd": 0.1}),
        )) {
            Plan::Resume(restored) => assert_eq!(restored.duration_ms, 0),
            other => panic!("expected Resume, got {other:?}"),
        }
    }

    #[test]
    fn nothing_produced_means_empty_text_not_merely_unfinished() {
        // The failure case: no structure and no text at all.
        assert!(produced_nothing(false, ""));
        assert!(produced_nothing(false, "  \n\t "));
        // NOT failures — a paid-for partial is worth returning, and this is the
        // exact boundary that keeps the budget_exhausted partial a success.
        assert!(!produced_nothing(false, "partial findings, not json"));
        assert!(!produced_nothing(true, ""));
    }

    #[test]
    fn an_empty_run_that_spent_the_ceiling_fails_terminally_not_retryably() {
        // Deterministic: the checkpoint holds the spend, so every retry
        // restores it, re-hits the wall and produces the same nothing.
        let spent = empty_run_failure(StopReason::BudgetExhausted, 2, 0.5);
        assert!(spent.is_terminal_for_job(), "{spent}");
        // Everything else can differ next attempt.
        for reason in [
            StopReason::NoSession,
            StopReason::StepCap,
            StopReason::TurnsExhausted,
            StopReason::SingleCall,
        ] {
            let e = empty_run_failure(reason, 1, 0.0);
            assert!(!e.is_terminal_for_job(), "{reason:?} should be retryable");
        }
        // What it cost is never lost in the failure.
        let msg = empty_run_failure(StopReason::NoSession, 3, 0.4237).to_string();
        assert!(msg.contains("$0.4237"), "{msg}");
        assert!(msg.contains("3 step(s)"), "{msg}");
        assert!(msg.contains("no_session"), "{msg}");
    }

    #[test]
    fn partial_truncation_is_char_boundary_safe() {
        assert_eq!(truncate_chars("héllo", 3), "hél");
        assert_eq!(truncate_chars("short", 100), "short");
    }

    // ── N25: research as a living knowledge base ────────────────────────────

    mod knowledge_base {
        use super::*;

        /// A realistic slice of a structured report's `sources` array: objects
        /// with titles, one duplicate, one bare string, and two entries no node
        /// should ever fetch.
        fn report_with_sources() -> Value {
            json!({
                "summary": "s",
                "key_findings": ["f1", "f2"],
                "sources": [
                    {"url": "https://a.example/one", "title": "One"},
                    {"url": "https://a.example/one", "title": "One again"},
                    "https://b.example/two",
                    {"url": "file:///etc/passwd", "title": "Local"},
                    {"url": "javascript:alert(1)"},
                    {"url": "  https://c.example/three  ", "title": "   "},
                    {"title": "no url at all"},
                    42
                ]
            })
        }

        #[test]
        fn a_slug_is_stable_so_a_rerun_updates_rather_than_forks() {
            // The same topic must produce the same key space on every run —
            // this is the whole mechanism behind "a second run updates".
            assert_eq!(
                slug("Current Czech VAT thresholds, with sources"),
                slug("current czech VAT   thresholds,   with sources!!")
            );
            assert_eq!(slug("Rust 1.80 - what changed?"), "rust-1-80-what-changed");
            // No leading/trailing dashes, and a punctuation-only topic still
            // yields a usable key rather than an empty one.
            assert_eq!(slug("  ...hello... "), "hello");
            assert_eq!(slug("???"), "topic");
            assert_eq!(slug(""), "topic");
            // Capped: a key, not a title.
            assert!(slug(&"word ".repeat(60)).chars().count() <= SLUG_CAP_CHARS);
        }

        #[test]
        fn finding_keys_are_topic_scoped_and_indexed() {
            assert_eq!(finding_key("czech-vat", 0), "czech-vat#0");
            assert_eq!(finding_key("czech-vat", 7), "czech-vat#7");
        }

        #[test]
        fn a_citation_that_is_not_a_web_url_is_dropped_not_stored() {
            // Every downstream use of this list treats the entry as a URL to
            // fetch, so `file://` / `javascript:` must never reach it — and a
            // duplicate must not become two records with the same key.
            let cited = cited_sources(&report_with_sources());
            let urls: Vec<&str> = cited.iter().map(|c| c.url.as_str()).collect();
            assert_eq!(
                urls,
                vec![
                    "https://a.example/one",
                    "https://b.example/two",
                    "https://c.example/three"
                ]
            );
            // A blank title is absent, not an empty string.
            assert_eq!(cited[0].title.as_deref(), Some("One"));
            assert_eq!(cited[1].title, None);
            assert_eq!(cited[2].title, None);
        }

        #[test]
        fn an_unstructured_report_cites_nothing_rather_than_erroring() {
            assert!(cited_sources(&Value::String("prose, not json".into())).is_empty());
            assert!(cited_sources(&json!({"summary": "s"})).is_empty());
            assert!(cited_sources(&json!({"sources": "not an array"})).is_empty());
        }

        #[test]
        fn capping_sources_says_so_instead_of_silently_acting_on_a_prefix() {
            let cited: Vec<Cited> = (0..5)
                .map(|i| Cited {
                    url: format!("https://x.example/{i}"),
                    title: None,
                })
                .collect();
            let (kept, truncated) = cap_sources(&cited, 3);
            assert_eq!(kept.len(), 3);
            assert!(truncated);
            let (kept, truncated) = cap_sources(&cited, 5);
            assert_eq!(kept.len(), 5);
            assert!(!truncated, "an exactly-full list is not truncated");
            let (kept, truncated) = cap_sources(&cited, 50);
            assert_eq!(kept.len(), 5);
            assert!(!truncated);
        }

        #[test]
        fn the_default_cap_is_the_operators_config_default_not_a_second_number() {
            // The app and `[research] max_watched_sources` must not drift: the
            // config struct is the one definition of the default.
            let opts = KbOptions::from_params(&json!({}));
            assert_eq!(opts.cap, ResearchConfig::default().max_watched_sources);
            assert_eq!(opts.cap, 20);
            // …and a caller may tighten it per run.
            assert_eq!(
                KbOptions::from_params(&json!({"max_watched_sources": 3})).cap,
                3
            );
        }

        #[test]
        fn the_side_effects_are_off_by_default_and_persistence_is_on() {
            let opts = KbOptions::from_params(&json!({}));
            assert!(opts.persist, "a structured run's records are the point");
            assert!(!opts.snapshot, "snapshots are metered fetches");
            assert!(!opts.watch, "watches are standing commitments");
            assert_eq!(opts.watch_cron, DEFAULT_WATCH_CRON);
            assert!(!KbOptions::from_params(&json!({"persist": false})).persist);
            // A blank cron falls back rather than proposing an invalid schedule.
            assert_eq!(
                KbOptions::from_params(&json!({"watch_cron": "   "})).watch_cron,
                DEFAULT_WATCH_CRON
            );
        }

        #[test]
        fn a_finding_record_carries_no_per_run_value_so_a_rerun_is_unchanged() {
            // The anti-pattern: stamping the session id, the query text or the
            // spend into the record. Change detection would then mark every
            // record `changed` on every run and turn watches, triggers and the
            // yield ledger into a churn feed.
            let findings = vec![json!("f1"), json!("f2")];
            let urls = vec!["https://a.example/one".to_string()];
            let first = finding_records("czech vat", &findings, &urls);
            let second = finding_records("czech vat", &findings, &urls);
            assert_eq!(first, second);
            assert_eq!(first[0].0, "czech-vat#0");
            assert_eq!(first[1].0, "czech-vat#1");
            let rendered = serde_json::to_string(&first[0].1).unwrap();
            for per_run in ["session", "job_id", "cost", "duration"] {
                assert!(
                    !rendered.contains(per_run),
                    "`{per_run}` is a per-run value and must not be in the record: {rendered}"
                );
            }
            // Empty findings are dropped rather than stored as blank records.
            let sparse = finding_records("t", &[json!(""), json!("real"), Value::Null], &urls);
            assert_eq!(sparse.len(), 1);
            // …and the index is the report's, so the key still names position 1.
            assert_eq!(sparse[0].0, "t#1");
        }

        #[test]
        fn an_unarchived_citation_never_claims_an_artifact_sha() {
            let cited = Cited {
                url: "https://a.example/one".into(),
                title: Some("One".into()),
            };
            for snap in [
                Snapshot::Skipped,
                Snapshot::Failed {
                    reason: "fetch failed: 404".into(),
                },
            ] {
                let prov = source_provenance(&cited, &snap);
                assert_eq!(prov.source_url.as_deref(), Some("https://a.example/one"));
                assert_eq!(
                    prov.artifact_sha, None,
                    "nothing was archived, so nothing is re-derivable"
                );
                assert!(!prov.replayable());
            }
            let saved = Snapshot::Saved {
                artifact: "source-0.md".into(),
                chars: 12,
                sha256: "abc".into(),
                fetched_url: "https://a.example/one/final".into(),
            };
            let prov = source_provenance(&cited, &saved);
            assert_eq!(prov.artifact_sha.as_deref(), Some("abc"));
            // The POST-REDIRECT url is what the body actually came from.
            assert_eq!(
                prov.source_url.as_deref(),
                Some("https://a.example/one/final")
            );
        }

        #[test]
        fn a_failed_snapshot_is_recorded_on_the_record_not_dropped() {
            let cited = Cited {
                url: "https://a.example/one".into(),
                title: None,
            };
            let rec = source_record(
                &cited,
                &Snapshot::Failed {
                    reason: "fetch failed: 404".into(),
                },
            );
            assert_eq!(rec["snapshot"], Value::Null);
            assert_eq!(rec["snapshot_error"], json!("fetch failed: 404"));
            // Skipped is honest absence in BOTH fields, not a fabricated error.
            let rec = source_record(&cited, &Snapshot::Skipped);
            assert_eq!(rec["snapshot"], Value::Null);
            assert_eq!(rec["snapshot_error"], Value::Null);
        }

        #[test]
        fn no_job_ceiling_is_headroom_not_a_broke_job() {
            assert!(snapshot_affordable(None));
            assert!(snapshot_affordable(Some(0.01)));
            assert!(!snapshot_affordable(Some(0.0)));
            assert!(!snapshot_affordable(Some(-1.0)));
        }

        #[test]
        fn a_watch_request_is_a_postable_schedules_body() {
            // It has to be copy-pasteable into `POST /schedules`, because this
            // app cannot create the schedule itself.
            let req = watch_request("https://a.example/one", DEFAULT_WATCH_CRON);
            assert_eq!(req["app"], json!("watch"));
            assert_eq!(req["cron"], json!("0 0 7 * * *"));
            assert_eq!(req["params"]["url"], json!("https://a.example/one"));
        }

        #[test]
        fn snapshot_artifacts_are_single_safe_segments() {
            // `save_artifact` refuses anything with a separator; the name is
            // built from an index, so this pins that it stays that way.
            let name = snapshot_artifact_name(3);
            assert_eq!(name, "source-3.md");
            assert!(!name.contains('/') && !name.contains('\\') && !name.contains(".."));
        }
    }

    // ── run() loop, wired through TestContext/ScriptedResearcher ───────────

    mod run_loop {
        use super::*;
        use pumper_core::testing::{
            engines_with, research_output, Dead, ScriptedResearcher, TempStore, TestContext,
        };
        use pumper_core::{AppContext, Storage};
        use std::sync::Arc;

        async fn ctx_with_researcher(
            storage: &Storage,
            params: Value,
            researcher: Arc<dyn pumper_core::Researcher>,
        ) -> AppContext {
            TestContext::new(storage, "research")
                .params(params)
                .engines(engines_with(Arc::new(Dead), Arc::new(Dead), researcher))
                .build()
        }

        #[tokio::test]
        async fn a_shaped_reply_completes_on_the_first_step() {
            let store = TempStore::new("research-completed").await;
            let researcher = Arc::new(
                ScriptedResearcher::new()
                    .always_text(r#"{"summary":"s","key_findings":["f"],"sources":[]}"#),
            );
            let ctx = ctx_with_researcher(
                &store.storage,
                json!({"query": "what happened"}),
                researcher,
            )
            .await;
            let result = Research.run(ctx).await.unwrap();
            assert_eq!(result["stop_reason"], json!("completed"));
            assert_eq!(result["structured"], json!(true));
            assert_eq!(result["steps"], json!(1));
        }

        #[tokio::test]
        async fn an_unshaped_but_nonempty_reply_still_succeeds_at_no_session() {
            // research_output() defaults session_id to None: the loop's
            // "nothing to resume" exit, not a step cap or budget wall.
            //
            // This is the deliberate boundary of the empty-run failure: the
            // reply never made the promised shape, but it is REAL TEXT the
            // caller paid for, and `stop_reason` already says the report is
            // unfinished. Only a run with nothing at all fails (see
            // `an_empty_reply_fails_the_job_instead_of_reporting_it_as_done`).
            let store = TempStore::new("research-nosession").await;
            let researcher =
                Arc::new(ScriptedResearcher::new().on("", research_output("not json at all")));
            let ctx = ctx_with_researcher(
                &store.storage,
                json!({"query": "q", "turns_per_step": 1}),
                researcher,
            )
            .await;
            let result = Research.run(ctx).await.unwrap();
            assert_eq!(result["stop_reason"], json!("no_session"));
            assert_eq!(result["structured"], json!(false));
            assert_eq!(result["steps"], json!(1));
            assert_eq!(result["report"], json!("not json at all"));
        }

        #[tokio::test]
        async fn an_empty_reply_fails_the_job_instead_of_reporting_it_as_done() {
            // Nothing came back at any price. Returning Ok marks the job
            // SUCCEEDED, fires the result webhook and indexes a search doc for
            // `{report: ""}` — the fleet's answer everywhere else is to fail.
            let store = TempStore::new("research-empty").await;
            let mut out = research_output("   \n  ");
            out.cost_usd = Some(0.25);
            let researcher = Arc::new(ScriptedResearcher::new().on("", out));
            let ctx = ctx_with_researcher(
                &store.storage,
                json!({"query": "q", "turns_per_step": 1}),
                researcher.clone(),
            )
            .await;
            let err = Research
                .run(ctx)
                .await
                .expect_err("an empty research run is a failure, not an empty success");
            let msg = err.to_string();
            assert!(msg.contains("produced no content"), "{msg}");
            // Criterion: the failure still reports what it cost.
            assert!(msg.contains("$0.2500"), "{msg}");
            assert!(
                !err.is_terminal_for_job(),
                "an empty step can differ next attempt: {msg}"
            );
        }

        #[tokio::test]
        async fn a_reclaimed_attempt_reports_the_checkpoints_grain_not_a_caller_resume() {
            // The params carry a `session_id`, but Plan::Resume discarded it in
            // favour of the checkpoint's — so `resumed` must be false while
            // `resumed_from_checkpoint` is true, and duration must continue
            // from the restored total instead of restarting at 0.
            let store = TempStore::new("research-reclaim-grain").await;
            let mut out =
                research_output(r#"{"summary":"done","key_findings":["f"],"sources":[]}"#);
            out.session_id = Some("sess-checkpoint".into());
            out.duration_ms = Some(2_000);
            let researcher = Arc::new(ScriptedResearcher::new().on("", out));
            let ctx = TestContext::new(&store.storage, "research")
                .params(json!({"query": "q", "session_id": "caller-sess"}))
                .engines(engines_with(
                    Arc::new(Dead),
                    Arc::new(Dead),
                    researcher.clone(),
                ))
                .restored(json!({
                    "v": STATE_VERSION,
                    "session_id": "sess-checkpoint",
                    "steps_done": 1,
                    "turns_used": 8,
                    "spent_usd": 0.10,
                    "duration_ms": 9_000,
                    "partial": "found so far",
                }))
                .build();
            let result = Research.run(ctx).await.unwrap();
            assert_eq!(result["resumed_from_checkpoint"], json!(true));
            assert_eq!(
                result["resumed"],
                json!(false),
                "the caller's session_id was discarded by the re-claim"
            );
            assert_eq!(result["duration_ms"], json!(11_000));
            assert_eq!(result["steps"], json!(2));
            // The engine really was asked to resume the CHECKPOINT's session.
            assert_eq!(
                researcher.calls()[0].resume_session.as_deref(),
                Some("sess-checkpoint")
            );
        }

        #[tokio::test]
        async fn a_failed_report_dump_does_not_fail_a_finished_run() {
            // The artifact is decorative; with `?` a transient fs failure made
            // it retryable, and the retry restores the finished checkpoint and
            // fails on the SAME write until the checkpoint is discarded and the
            // whole research is re-bought. Both write sites are covered here.
            let store = TempStore::new("research-artifact-fail").await;
            // A regular FILE where the artifacts dir should be: create_dir_all
            // under it cannot succeed on any platform.
            let blocker = std::env::temp_dir().join(format!(
                "research-artifact-blocker-{}-{:?}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::write(&blocker, b"not a directory").unwrap();
            // Without this the test could pass vacuously on a platform where
            // the write actually succeeds.
            assert!(
                std::fs::create_dir_all(blocker.join("job")).is_err(),
                "the artifact dir must be un-creatable for this test to mean anything"
            );

            // Site 1 — a fresh run that finished.
            let researcher = Arc::new(
                ScriptedResearcher::new()
                    .always_text(r#"{"summary":"s","key_findings":["f"],"sources":[]}"#),
            );
            let ctx = TestContext::new(&store.storage, "research")
                .params(json!({"query": "q"}))
                .engines(engines_with(Arc::new(Dead), Arc::new(Dead), researcher))
                .artifacts_dir(blocker.join("job"))
                .build();
            let result = Research
                .run(ctx)
                .await
                .expect("a finished run is not undone by a failed artifact dump");
            assert_eq!(result["stop_reason"], json!("completed"));

            // Site 2 — the Plan::Done restore path, which re-dumps the stored
            // result and used to fail on the very same write.
            let done = json!({
                "v": STATE_VERSION,
                "session_id": "sess-1",
                "steps_done": 1,
                "spent_usd": 0.42,
                "result": {"query": "q", "structured": true, "cost_usd": 0.42},
            });
            let ctx = TestContext::new(&store.storage, "research")
                .params(json!({"query": "q"}))
                .engines(engines_with(Arc::new(Dead), Arc::new(Dead), Arc::new(Dead)))
                .artifacts_dir(blocker.join("job"))
                .restored(done)
                .build();
            let restored = Research
                .run(ctx)
                .await
                .expect("a restored finished result is not undone by a failed artifact dump");
            assert_eq!(restored["resumed_from_checkpoint"], json!(true));

            let _ = std::fs::remove_file(&blocker);
        }

        #[tokio::test]
        async fn step_cap_is_recorded_when_the_agent_never_shapes_a_report() {
            // A resumable session that never returns the promised shape must
            // stop at MAX_STEPS, not loop forever — and the result must say
            // why, distinct from a budget or turn-budget exit.
            let store = TempStore::new("research-stepcap").await;
            let mut out = research_output("still thinking, not json");
            out.session_id = Some("sess-forever".into());
            let researcher = Arc::new(ScriptedResearcher::new().on("", out));
            let ctx = ctx_with_researcher(
                &store.storage,
                json!({"query": "q", "turns_per_step": 1}),
                researcher.clone(),
            )
            .await;
            let result = Research.run(ctx).await.unwrap();
            assert_eq!(result["stop_reason"], json!("step_cap"));
            assert_eq!(result["structured"], json!(false));
            assert_eq!(result["steps"], json!(MAX_STEPS));
            assert_eq!(researcher.call_count(), MAX_STEPS as usize);
        }

        /// A researcher that bills like the CLI does under `--max-budget-usd`:
        /// it never charges more than the ceiling it was handed. The scripted
        /// stand-in ignores that ceiling, and a run-total test must not assume
        /// the clamp away — it is half of what keeps the total honest.
        struct BudgetHonoringResearcher {
            wanted_usd: f64,
            text: String,
            session_id: String,
            ceilings: std::sync::Mutex<Vec<Option<f64>>>,
        }

        impl BudgetHonoringResearcher {
            fn new(wanted_usd: f64, text: &str) -> Self {
                Self {
                    wanted_usd,
                    text: text.to_string(),
                    session_id: "sess-1".into(),
                    ceilings: std::sync::Mutex::new(Vec::new()),
                }
            }

            fn ceilings(&self) -> Vec<Option<f64>> {
                self.ceilings.lock().expect("ceiling lock").clone()
            }
        }

        #[async_trait]
        impl pumper_core::Researcher for BudgetHonoringResearcher {
            async fn research(
                &self,
                req: pumper_core::ResearchRequest,
            ) -> Result<pumper_core::ResearchOutput> {
                self.ceilings
                    .lock()
                    .expect("ceiling lock")
                    .push(req.max_budget_usd);
                let billed = req
                    .max_budget_usd
                    .map_or(self.wanted_usd, |ceiling| self.wanted_usd.min(ceiling));
                Ok(pumper_core::ResearchOutput {
                    text: self.text.clone(),
                    json: None,
                    cost_usd: Some(billed),
                    duration_ms: Some(1),
                    num_turns: Some(1),
                    session_id: Some(self.session_id.clone()),
                })
            }
        }

        #[tokio::test]
        async fn total_run_spend_stops_at_max_budget_usd_not_at_it_per_call() {
            // The manifest's own example: a `max_budget_usd` and NO job
            // `budget_usd`. Before the fix the ceiling was handed to every one
            // of up to MAX_STEPS calls, so 0.50 bought $6.00 of research.
            let store = TempStore::new("research-run-ceiling").await;
            let researcher = Arc::new(BudgetHonoringResearcher::new(0.20, "partial findings"));
            let ctx = ctx_with_researcher(
                &store.storage,
                json!({"query": "q", "turns_per_step": 1, "max_budget_usd": 0.5}),
                researcher.clone(),
            )
            .await;
            let result = Research.run(ctx).await.unwrap();
            let spent = result["cost_usd"].as_f64().unwrap();
            assert!(spent <= 0.5 + 1e-9, "run spent {spent}, ceiling was 0.50");
            assert_eq!(result["stop_reason"], json!("budget_exhausted"));
            // 0.20 + 0.20 + the 0.10 remainder — then no headroom left.
            assert_eq!(researcher.ceilings().len(), 3);
        }

        #[tokio::test]
        async fn each_call_gets_the_remaining_headroom_not_the_whole_run_ceiling() {
            let store = TempStore::new("research-headroom").await;
            let researcher = Arc::new(BudgetHonoringResearcher::new(0.20, "partial findings"));
            let ctx = ctx_with_researcher(
                &store.storage,
                json!({"query": "q", "turns_per_step": 1, "max_budget_usd": 0.5}),
                researcher.clone(),
            )
            .await;
            Research.run(ctx).await.unwrap();
            let ceilings: Vec<f64> = researcher
                .ceilings()
                .into_iter()
                .map(|c| c.expect("every call carries a ceiling"))
                .collect();
            assert_eq!(ceilings.len(), 3);
            for (got, want) in ceilings.iter().zip([0.5, 0.3, 0.1]) {
                assert!((got - want).abs() < 1e-6, "ceilings were {ceilings:?}");
            }
        }

        #[tokio::test]
        async fn restored_spend_counts_against_the_ceiling_before_the_first_step() {
            // A re-claimed attempt whose checkpoint already spent the ceiling
            // must not buy another step: `Dead` panics if the loop calls the
            // researcher. Before the fix the wall was skipped while
            // `steps_done == 0` and no-op'd anyway without a job budget.
            let store = TempStore::new("research-restored-ceiling").await;
            let restored = json!({
                "v": STATE_VERSION,
                "session_id": "sess-1",
                "steps_done": 0,
                "turns_used": 0,
                "spent_usd": 0.60,
                "partial": "what the prior attempt found",
            });
            let ctx = TestContext::new(&store.storage, "research")
                .params(json!({"query": "q", "turns_per_step": 1, "max_budget_usd": 0.5}))
                .engines(engines_with(Arc::new(Dead), Arc::new(Dead), Arc::new(Dead)))
                .restored(restored)
                .build();
            let result = Research.run(ctx).await.unwrap();
            assert_eq!(result["stop_reason"], json!("budget_exhausted"));
            assert_eq!(result["steps"], json!(0));
            assert_eq!(result["cost_usd"], json!(0.60));
            // The paid-for partial still comes back.
            assert_eq!(result["report"], json!("what the prior attempt found"));
        }

        #[tokio::test]
        async fn max_turns_is_a_run_total_even_when_the_engine_reports_a_session_count() {
            // Each step is capped at 1 turn but the envelope reports 9 — the
            // shape a session-cumulative counter has. Charging 9 to a 3-turn
            // budget truncates the run after ONE step; charging the step's own
            // cap spends the budget as the caller asked.
            let store = TempStore::new("research-turn-total").await;
            let mut out = research_output("still thinking, not json");
            out.session_id = Some("sess-1".into());
            out.num_turns = Some(9);
            let researcher = Arc::new(ScriptedResearcher::new().on("", out));
            let ctx = ctx_with_researcher(
                &store.storage,
                json!({"query": "q", "max_turns": 3, "turns_per_step": 1}),
                researcher.clone(),
            )
            .await;
            let result = Research.run(ctx).await.unwrap();
            assert_eq!(result["stop_reason"], json!("turns_exhausted"));
            assert_eq!(result["steps"], json!(3));
            assert_eq!(result["num_turns"], json!(3));
            assert_eq!(researcher.call_count(), 3);
        }

        #[tokio::test]
        async fn budget_exhaustion_between_steps_stops_the_loop_and_keeps_the_partial() {
            // First step spends the whole job budget; the pre-step budget
            // check before step 2 must break the loop rather than call the
            // researcher again.
            let store = TempStore::new("research-budget").await;
            let mut out = research_output("partial findings, not json");
            out.session_id = Some("sess-1".into());
            out.cost_usd = Some(1.0);
            let researcher = Arc::new(ScriptedResearcher::new().on("", out));
            let ctx = TestContext::new(&store.storage, "research")
                .params(json!({"query": "q", "turns_per_step": 1}))
                .engines(engines_with(
                    Arc::new(Dead),
                    Arc::new(Dead),
                    researcher.clone(),
                ))
                .budget_usd(1.0)
                .build();
            let result = Research.run(ctx).await.unwrap();
            assert_eq!(result["stop_reason"], json!("budget_exhausted"));
            assert_eq!(result["steps"], json!(1));
            assert_eq!(researcher.call_count(), 1);
        }
    }
}
