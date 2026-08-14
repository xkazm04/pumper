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
use pumper_core::{
    salvage_json, AppContext, AppManifest, CostClass, ManifestExample, ResearchRequest, Result,
    ScrapeApp,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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
) {
    state.spent_usd += cost_usd.unwrap_or(0.0);
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
         resumes where it left off without re-spending restored budget."
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
            ],
            output_shape: Some(
                "{summary, key_findings: [..], sources: [..], session_id, cost_usd, steps, \
                 resumed_from_checkpoint, stop_reason} — structured research output; \
                 `session_id` is resumable; `cost_usd` is cumulative across resumed attempts; \
                 `stop_reason` explains why the loop ended (completed, step_cap, \
                 turns_exhausted, budget_exhausted, no_session, single_call) — `structured: \
                 false` alone can't distinguish a truncated report from other causes",
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
        // Model/effort are chosen by the caller: default to the "research" role
        // (Sonnet, normal reasoning); an app can pass "compose" for Opus @ xhigh,
        // or override model/effort directly.
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
                ctx.save_artifact("report.json", &serde_json::to_vec_pretty(&result)?)
                    .await?;
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
        let mut duration_ms: u64 = 0;
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
            );
            duration_ms += output.duration_ms.unwrap_or(0);
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
        let report = match final_parsed {
            Some(v) => v,
            None => Value::String(last_text),
        };
        let result = json!({
            "query": query,
            "report": report,
            "structured": structured,
            "resumed": caller_resumed,
            "resumed_from_checkpoint": resumed_from_checkpoint,
            "steps": state.steps_done,
            "cost_usd": state.spent_usd,
            "duration_ms": duration_ms,
            "num_turns": state.turns_used,
            "session_id": state.session_id,
            "stop_reason": stop_reason.as_str(),
        });

        // Final checkpoint carries the whole result: a crash between here and
        // job completion costs a restore-and-return, not a re-run.
        state.partial = None;
        state.result = Some(result.clone());
        ctx.checkpoint_now(state.to_value()).await;

        ctx.save_artifact("report.json", &serde_json::to_vec_pretty(&result)?)
            .await?;
        Ok(result)
    }
}

/// True when a research report matches the promised shape: a `summary` string
/// plus `key_findings` and `sources` arrays. Guards against marking a
/// hallucinated or wrong-shape object as `structured`.
fn is_report_shaped(v: &Value) -> bool {
    v.get("summary").is_some_and(Value::is_string)
        && v.get("key_findings").is_some_and(Value::is_array)
        && v.get("sources").is_some_and(Value::is_array)
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
        );
        assert!((state.spent_usd - 0.50).abs() < 1e-9);
        assert_eq!(state.steps_done, 2);
        assert_eq!(state.turns_used, 15);
        assert_eq!(state.session_id.as_deref(), Some("sess-abc2"));
    }

    #[test]
    fn fold_keeps_prior_session_and_uses_fallback_turns_when_engine_omits_them() {
        let mut state = RunState::fresh(Some("caller-sess".into()));
        fold_output(&mut state, None, None, None, Some(8));
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
    fn partial_truncation_is_char_boundary_safe() {
        assert_eq!(truncate_chars("héllo", 3), "hél");
        assert_eq!(truncate_chars("short", 100), "short");
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
        async fn an_unshaped_reply_with_no_session_id_stops_immediately() {
            // research_output() defaults session_id to None: the loop's
            // "nothing to resume" exit, not a step cap or budget wall.
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
