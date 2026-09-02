//! Workflow runs (N03): a declared multi-step DAG whose steps are ordinary
//! jobs, with fan-in join barriers, `{{steps.X.result.path}}` param templating,
//! one budget envelope and one rolled-up receipt.
//!
//! **Why this is not a trigger.** `docs/features/triggers.md` declares fan-in
//! barriers, `${…}` templating and named pipeline grouping as non-goals, and
//! that is the right call for *standing reactive edges*: a trigger answers "when
//! X happens, also do Y", one edge at a time, with no notion of a plan. What it
//! cannot express is a **submitted plan** — crawl three seeds in parallel, and
//! when all three are done, extract. This module is that missing half, and it
//! deliberately does not touch the trigger engine: a workflow step's terminal
//! event still fires whatever triggers watch it.
//!
//! **The barrier lives in one place.** [`on_step_terminal`] is called from
//! `worker::finalize_with_stages`, beside `fire_terminal_triggers` — the one
//! point every terminal path reaches, including the reaper's, the cancel door's
//! and the shutdown drain's. Nothing evaluates a barrier anywhere else.
//!
//! **Exactly-once is a guarded UPDATE, not a lock.** Two upstreams of a diamond
//! can finish on different worker tasks at the same instant and both compute the
//! same ready join step. Both then call
//! [`Storage::claim_workflow_step`](pumper_core::Storage::claim_workflow_step),
//! whose `WHERE status = 'pending'` lets exactly one of them win; only the
//! winner enqueues. The same idiom fences step completion
//! (`finish_workflow_step`) and run completion (`finish_workflow_run`), so a
//! repeated terminal event or a boot re-evaluation is a no-op rather than a
//! second fan-out.
//!
//! **Fail-open, like every other post-terminal hook.** A storage error inside
//! the hook is logged and swallowed: a workflow that cannot advance must not
//! also cost the job its callback, its webhook or its triggers.

use std::collections::{BTreeMap, BTreeSet};

use pumper_core::{EnqueueOptions, Job, JobStatus, NewWorkflowRun, WorkflowDef, WorkflowStepRow};
use serde::Serialize;
use serde_json::{json, Map, Value};
use tracing::{info, warn};

use crate::events::JobEvent;
use crate::state::AppState;

/// Ceiling on steps in one spec. A DAG this size is already past the point
/// where a workflow is the right tool, and the bound keeps validation (which is
/// O(steps + edges)) and the advance loop trivially cheap.
pub const MAX_STEPS: usize = 100;

/// Ceiling on advance iterations for one run, as a multiple of its step count.
/// The loop only re-enters after it changed a row, so this can only be reached
/// by a bug; it exists so that bug is a logged refusal rather than a spin.
const ADVANCE_ROUNDS_PER_STEP: usize = 3;

/// Prefix of a step job's dedup key. Together with `(run, step)` it makes a
/// re-enqueue after a crash return the original job instead of doubling work.
const STEP_IDEMPOTENCY_PREFIX: &str = "wf";

// ── the spec ────────────────────────────────────────────────────────────────

/// What a run does when a step ends badly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    /// Every step that has not started is skipped and the run fails. Default:
    /// a plan whose input step failed has no business spending the rest of its
    /// envelope on steps that cannot use that input.
    FailFast,
    /// Only the steps that actually depended on the failure are skipped;
    /// independent branches run to completion. The run still ends `failed`.
    Continue,
}

impl OnFailure {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "fail_fast" => Some(Self::FailFast),
            "continue" => Some(Self::Continue),
            _ => None,
        }
    }
}

/// One declared step.
#[derive(Debug, Clone, PartialEq)]
pub struct StepSpec {
    pub app: String,
    /// Params template. Rendered per run through [`render_params`].
    pub params: Value,
    /// The join barrier: every named step must have SUCCEEDED before this one
    /// is enqueueable. Empty = a root step, enqueued when the run opens.
    pub after: Vec<String>,
    pub budget_usd: Option<f64>,
    pub priority: Option<i64>,
    pub max_attempts: Option<i64>,
}

/// A validated plan.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowSpec {
    pub steps: BTreeMap<String, StepSpec>,
    /// Envelope for the whole run. `None` = no ceiling, exactly as on a job.
    pub budget_usd: Option<f64>,
    pub on_failure: OnFailure,
}

/// Where a step is in its life. The barrier reads only this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepState {
    /// Declared, barrier not yet satisfied (or not yet claimed).
    Pending,
    /// Claimed and enqueued as a job that has not ended.
    Queued,
    Succeeded,
    Failed,
    Cancelled,
    /// Never ran: an upstream ended badly, or fail-fast cut the run short.
    Skipped,
}

impl StepState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Queued => "queued",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "queued" => Some(Self::Queued),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }

    /// Still able to produce work. The run is over exactly when no step is.
    pub fn is_open(self) -> bool {
        matches!(self, Self::Pending | Self::Queued)
    }

    /// Ended without a result other steps can consume. `skipped` counts: a step
    /// that never ran cannot satisfy a barrier, and treating it as merely
    /// "not succeeded yet" would park its dependants forever.
    pub fn is_bad_ending(self) -> bool {
        matches!(self, Self::Failed | Self::Cancelled | Self::Skipped)
    }
}

/// Parses and validates a spec document. `Err` is the caller-facing list of
/// every violation, so a malformed plan is one 422 rather than a discovery
/// sequence.
///
/// The anti-pattern this refuses: a spec that *parses* but whose graph cannot
/// run — a barrier naming a step that does not exist, or a cycle. Both would
/// otherwise open a run that immediately deadlocks with every step `pending`
/// and nothing to blame.
pub fn parse_spec(doc: &Value) -> Result<WorkflowSpec, Vec<String>> {
    let mut errors: Vec<String> = Vec::new();
    let Some(obj) = doc.as_object() else {
        return Err(vec!["spec: must be an object".into()]);
    };
    let on_failure = match obj.get("on_failure") {
        None | Some(Value::Null) => OnFailure::FailFast,
        Some(Value::String(s)) => match OnFailure::parse(s) {
            Some(v) => v,
            None => {
                errors.push(format!(
                    "spec/on_failure: '{s}' is not one of fail_fast | continue"
                ));
                OnFailure::FailFast
            }
        },
        Some(_) => {
            errors.push("spec/on_failure: must be a string (fail_fast | continue)".into());
            OnFailure::FailFast
        }
    };
    let budget_usd = match parse_budget(obj.get("budget_usd"), "spec/budget_usd") {
        Ok(b) => b,
        Err(e) => {
            errors.push(e);
            None
        }
    };
    let Some(steps_obj) = obj.get("steps").and_then(Value::as_object) else {
        errors.push("spec/steps: must be an object of {step_name: {app, ...}}".into());
        return Err(errors);
    };
    if steps_obj.is_empty() {
        errors.push("spec/steps: a workflow with no steps has nothing to run".into());
    }
    if steps_obj.len() > MAX_STEPS {
        errors.push(format!(
            "spec/steps: {} steps exceeds the {MAX_STEPS}-step ceiling",
            steps_obj.len()
        ));
    }
    let mut steps: BTreeMap<String, StepSpec> = BTreeMap::new();
    for (name, raw) in steps_obj {
        if name.trim().is_empty() {
            errors.push("spec/steps: a step name may not be blank".into());
            continue;
        }
        let Some(step_obj) = raw.as_object() else {
            errors.push(format!("spec/steps/{name}: must be an object"));
            continue;
        };
        let Some(app) = step_obj.get("app").and_then(Value::as_str) else {
            errors.push(format!("spec/steps/{name}/app: required, must be a string"));
            continue;
        };
        let params = step_obj.get("params").cloned().unwrap_or_else(|| json!({}));
        let after = match parse_after(step_obj.get("after"), name) {
            Ok(a) => a,
            Err(e) => {
                errors.push(e);
                Vec::new()
            }
        };
        let budget_usd = match parse_budget(
            step_obj.get("budget_usd"),
            &format!("spec/steps/{name}/budget_usd"),
        ) {
            Ok(b) => b,
            Err(e) => {
                errors.push(e);
                None
            }
        };
        steps.insert(
            name.clone(),
            StepSpec {
                app: app.to_string(),
                params,
                after,
                budget_usd,
                priority: step_obj.get("priority").and_then(Value::as_i64),
                max_attempts: step_obj.get("max_attempts").and_then(Value::as_i64),
            },
        );
    }
    // Graph checks: every barrier name must exist, and the graph must be a DAG.
    for (name, step) in &steps {
        for dep in &step.after {
            if dep == name {
                errors.push(format!(
                    "spec/steps/{name}/after: a step cannot depend on itself"
                ));
            } else if !steps.contains_key(dep) {
                errors.push(format!(
                    "spec/steps/{name}/after: '{dep}' is not a step in this spec"
                ));
            }
        }
    }
    if let Some(cycle) = find_cycle(&steps) {
        errors.push(format!(
            "spec/steps: the dependency graph has a cycle ({}) — no step in it could ever start",
            cycle.join(" -> ")
        ));
    }
    // Envelope arithmetic: a plan whose declared step budgets already exceed the
    // run budget is refused at the door, not discovered when the last step is
    // starved. Steps without their own budget draw from the remainder, so only
    // the DECLARED sum is checkable here.
    if let Some(envelope) = budget_usd {
        let declared: f64 = steps.values().filter_map(|s| s.budget_usd).sum();
        if declared > envelope {
            errors.push(format!(
                "spec/budget_usd: the declared step budgets sum to {declared} which exceeds the \
                 run envelope of {envelope} — the last steps to run could never be funded"
            ));
        }
    }
    if errors.is_empty() {
        Ok(WorkflowSpec {
            steps,
            budget_usd,
            on_failure,
        })
    } else {
        Err(errors)
    }
}

/// `{all_of: [names]}` (or a bare array, or absent) → the barrier's names.
///
/// `any_of` is explicitly out of the v1 slice and refused by NAME rather than
/// ignored: silently dropping an `any_of` would turn a quorum barrier into an
/// `all_of` one, which is a different plan that happens to parse.
fn parse_after(raw: Option<&Value>, step: &str) -> Result<Vec<String>, String> {
    let names = match raw {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::Array(items)) => items,
        Some(Value::Object(obj)) => {
            if obj.contains_key("any_of") {
                return Err(format!(
                    "spec/steps/{step}/after: `any_of` is not supported in this release (only \
                     `all_of`). Refused rather than treated as `all_of`, which would be a \
                     different plan."
                ));
            }
            match obj.get("all_of") {
                Some(Value::Array(items)) => items,
                _ => {
                    return Err(format!(
                        "spec/steps/{step}/after: must be {{\"all_of\": [step names]}} or an array"
                    ))
                }
            }
        }
        Some(_) => {
            return Err(format!(
                "spec/steps/{step}/after: must be {{\"all_of\": [step names]}} or an array"
            ))
        }
    };
    let mut out = Vec::new();
    for item in names {
        match item.as_str() {
            Some(s) if !s.trim().is_empty() => out.push(s.to_string()),
            _ => {
                return Err(format!(
                    "spec/steps/{step}/after: every entry must be a non-empty step name"
                ))
            }
        }
    }
    Ok(out)
}

/// A budget field: absent → `None`, otherwise strictly positive.
///
/// The same refusal `validate_budget_usd` makes at the jobs door, for the same
/// reason: `None` means "no ceiling", so `0` cannot ALSO mean "spend nothing"
/// without turning the most cautious spec into the least limited run.
fn parse_budget(raw: Option<&Value>, pointer: &str) -> Result<Option<f64>, String> {
    match raw {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match v.as_f64() {
            Some(b) if b.is_finite() && b > 0.0 => Ok(Some(b)),
            _ => Err(format!(
                "{pointer}: must be a positive number of dollars (an omitted budget means NO \
                 ceiling, so 0 cannot mean 'spend nothing')"
            )),
        },
    }
}

/// The first dependency cycle found, as a path, or `None` for a DAG.
fn find_cycle(steps: &BTreeMap<String, StepSpec>) -> Option<Vec<String>> {
    let mut done: BTreeSet<&str> = BTreeSet::new();
    for start in steps.keys() {
        let mut path: Vec<&str> = Vec::new();
        let mut on_path: BTreeSet<&str> = BTreeSet::new();
        if let Some(cycle) = visit(start, steps, &mut done, &mut path, &mut on_path) {
            return Some(cycle);
        }
    }
    None
}

fn visit<'a>(
    node: &'a str,
    steps: &'a BTreeMap<String, StepSpec>,
    done: &mut BTreeSet<&'a str>,
    path: &mut Vec<&'a str>,
    on_path: &mut BTreeSet<&'a str>,
) -> Option<Vec<String>> {
    if done.contains(node) {
        return None;
    }
    if on_path.contains(node) {
        let start = path.iter().position(|n| *n == node).unwrap_or(0);
        let mut cycle: Vec<String> = path[start..].iter().map(|s| s.to_string()).collect();
        cycle.push(node.to_string());
        return Some(cycle);
    }
    on_path.insert(node);
    path.push(node);
    if let Some(spec) = steps.get(node) {
        for dep in &spec.after {
            if let Some((key, _)) = steps.get_key_value(dep) {
                if let Some(cycle) = visit(key.as_str(), steps, done, path, on_path) {
                    return Some(cycle);
                }
            }
        }
    }
    path.pop();
    on_path.remove(node);
    done.insert(steps.get_key_value(node).map(|(k, _)| k.as_str())?);
    None
}

// ── the barrier, as pure functions ──────────────────────────────────────────

/// Steps whose join barrier is satisfied right now: `pending`, with every name
/// in `after` `succeeded`.
///
/// The anti-pattern this replaces (and the one the whole card exists to prevent):
/// firing a downstream step on *each* upstream completion. A diamond's join
/// would then run twice — once per source — doing duplicate work and producing
/// two receipts for one plan. Readiness is a question about the WHOLE
/// predecessor set, evaluated against persisted state, not about the event that
/// happened to arrive.
pub fn ready_steps(spec: &WorkflowSpec, states: &BTreeMap<String, StepState>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (name, step) in &spec.steps {
        if states.get(name).copied() != Some(StepState::Pending) {
            continue;
        }
        if step
            .after
            .iter()
            .all(|dep| states.get(dep).copied() == Some(StepState::Succeeded))
        {
            out.push(name.clone());
        }
    }
    out
}

/// Steps that must be marked `skipped` now, each with the reason to persist.
///
/// Two policies, one function: `fail_fast` skips every step that has not
/// started as soon as anything ends badly; `continue` skips only the steps
/// whose barrier can no longer be satisfied, which propagates transitively
/// because `skipped` is itself a bad ending.
pub fn cascade_skips(
    spec: &WorkflowSpec,
    states: &BTreeMap<String, StepState>,
) -> Vec<(String, String)> {
    let bad: Vec<&String> = spec
        .steps
        .keys()
        .filter(|n| {
            states
                .get(*n)
                .copied()
                .is_some_and(StepState::is_bad_ending)
        })
        .collect();
    if bad.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (name, step) in &spec.steps {
        if states.get(name).copied() != Some(StepState::Pending) {
            continue;
        }
        match spec.on_failure {
            OnFailure::FailFast => out.push((
                name.clone(),
                format!(
                    "on_failure = fail_fast: step '{}' ended badly, so this step never started",
                    bad[0]
                ),
            )),
            OnFailure::Continue => {
                let blocking: Vec<&str> = step
                    .after
                    .iter()
                    .filter(|dep| {
                        states
                            .get(*dep)
                            .copied()
                            .is_some_and(StepState::is_bad_ending)
                    })
                    .map(String::as_str)
                    .collect();
                if !blocking.is_empty() {
                    out.push((
                        name.clone(),
                        format!(
                            "join barrier can never be satisfied: {} ended badly",
                            blocking.join(", ")
                        ),
                    ));
                }
            }
        }
    }
    out
}

/// The run's verdict once no step is open, or `None` while work remains.
///
/// A run whose steps all succeeded is `succeeded`. Any failure or skip makes it
/// `failed` (a plan that did not do what it declared did not succeed, even under
/// `on_failure = continue`); a run is `cancelled` only when nothing failed and
/// something was cancelled.
pub fn run_verdict(states: &BTreeMap<String, StepState>) -> Option<(&'static str, Option<String>)> {
    if states.values().any(|s| s.is_open()) {
        return None;
    }
    let failed: Vec<&str> = states
        .iter()
        .filter(|(_, s)| matches!(s, StepState::Failed))
        .map(|(n, _)| n.as_str())
        .collect();
    let skipped: Vec<&str> = states
        .iter()
        .filter(|(_, s)| matches!(s, StepState::Skipped))
        .map(|(n, _)| n.as_str())
        .collect();
    let cancelled: Vec<&str> = states
        .iter()
        .filter(|(_, s)| matches!(s, StepState::Cancelled))
        .map(|(n, _)| n.as_str())
        .collect();
    if !failed.is_empty() || !skipped.is_empty() {
        let mut parts = Vec::new();
        if !failed.is_empty() {
            parts.push(format!("failed: {}", failed.join(", ")));
        }
        if !skipped.is_empty() {
            parts.push(format!("never ran: {}", skipped.join(", ")));
        }
        return Some(("failed", Some(parts.join("; "))));
    }
    if !cancelled.is_empty() {
        return Some((
            "cancelled",
            Some(format!("cancelled: {}", cancelled.join(", "))),
        ));
    }
    Some(("succeeded", None))
}

// ── templating ──────────────────────────────────────────────────────────────

/// Renders a params template against the results of completed steps.
///
/// `{{steps.NAME.result}}` and `{{steps.NAME.result.a.b}}` are the only forms.
/// A whole-string template substitutes the JSON **value** (so a template can
/// hand an array or an object straight through); a token embedded in a larger
/// string interpolates its text (scalars verbatim, containers as compact JSON).
///
/// **A miss is a refusal, never a blank.** The anti-pattern is rendering an
/// unresolvable reference to `null` or to the literal `{{…}}`: the step then
/// runs with silently wrong params and the plan produces a plausible, wrong
/// result. Naming the missing path is the whole value of having a template.
pub fn render_params(template: &Value, results: &BTreeMap<String, Value>) -> Result<Value, String> {
    match template {
        Value::String(s) => render_string(s, results),
        Value::Array(items) => items
            .iter()
            .map(|v| render_params(v, results))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(obj) => {
            let mut out = Map::new();
            for (k, v) in obj {
                out.insert(k.clone(), render_params(v, results)?);
            }
            Ok(Value::Object(out))
        }
        other => Ok(other.clone()),
    }
}

/// True when this document contains at least one `{{…}}` token — the test that
/// decides whether a step's params can be schema-validated at create time or
/// only once rendered.
pub fn has_template(v: &Value) -> bool {
    match v {
        Value::String(s) => s.contains("{{"),
        Value::Array(items) => items.iter().any(has_template),
        Value::Object(obj) => obj.values().any(has_template),
        _ => false,
    }
}

fn render_string(s: &str, results: &BTreeMap<String, Value>) -> Result<Value, String> {
    let Some(rest) = s.strip_prefix("{{") else {
        return interpolate(s, results);
    };
    // Whole-string form: the value passes through with its JSON type intact.
    if let Some(expr) = rest.strip_suffix("}}") {
        if !expr.contains("}}") {
            return resolve(expr.trim(), results);
        }
    }
    interpolate(s, results)
}

fn interpolate(s: &str, results: &BTreeMap<String, Value>) -> Result<Value, String> {
    if !s.contains("{{") {
        return Ok(Value::String(s.to_string()));
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            return Err(format!(
                "params template: unterminated '{{{{' in \"{s}\" — every reference must close \
                 with '}}}}'"
            ));
        };
        let value = resolve(after[..close].trim(), results)?;
        match value {
            Value::String(text) => out.push_str(&text),
            other => out.push_str(&other.to_string()),
        }
        rest = &after[close + 2..];
    }
    out.push_str(rest);
    Ok(Value::String(out))
}

/// One `steps.NAME.result[.path]` reference against the completed results.
fn resolve(expr: &str, results: &BTreeMap<String, Value>) -> Result<Value, String> {
    let mut parts = expr.split('.');
    if parts.next() != Some("steps") {
        return Err(format!(
            "params template: '{{{{{expr}}}}}' is not a supported reference — the only form is \
             {{{{steps.<step>.result[.path]}}}}"
        ));
    }
    let Some(step) = parts.next().filter(|s| !s.is_empty()) else {
        return Err(format!(
            "params template: '{{{{{expr}}}}}' names no step (expected steps.<step>.result...)"
        ));
    };
    if parts.next() != Some("result") {
        return Err(format!(
            "params template: '{{{{{expr}}}}}' must address the step's `result` \
             (steps.{step}.result[.path]); nothing else about a step is templatable"
        ));
    }
    let Some(root) = results.get(step) else {
        return Err(format!(
            "params template: '{{{{{expr}}}}}' refers to step '{step}', which has not produced a \
             result. Only steps listed in this step's `after` are available when it is rendered."
        ));
    };
    let mut cursor = root;
    let mut walked = format!("steps.{step}.result");
    for key in parts {
        let next = match cursor {
            Value::Object(obj) => obj.get(key),
            Value::Array(items) => key.parse::<usize>().ok().and_then(|i| items.get(i)),
            _ => None,
        };
        let Some(next) = next else {
            return Err(format!(
                "params template: '{{{{{expr}}}}}' — '{walked}' has no '{key}'. The reference is \
                 refused rather than rendered as null, which would run the step with silently \
                 wrong params."
            ));
        };
        cursor = next;
        walked.push('.');
        walked.push_str(key);
    }
    Ok(cursor.clone())
}

// ── budget envelope ─────────────────────────────────────────────────────────

/// The ceiling one step's job may carry, given the run's envelope and what the
/// run has already spent. `Err` = the envelope is exhausted and the step must
/// not be enqueued at all.
///
/// A step's own `budget_usd` is a cap, never a grant: it is clamped to what is
/// left of the envelope, so the sum of what the steps may spend can never
/// exceed the run budget no matter how the spec was written.
pub fn step_budget(
    step_budget_usd: Option<f64>,
    run_budget_usd: Option<f64>,
    spent_usd: f64,
) -> Result<Option<f64>, String> {
    let Some(envelope) = run_budget_usd else {
        return Ok(step_budget_usd);
    };
    let remaining = envelope - spent_usd.max(0.0);
    // NaN-safe deliberately: a corrupted ledger total must refuse the step, not
    // slip past a bare `<= 0.0` comparison that NaN always answers `false` to.
    if !remaining.is_finite() || remaining <= 0.0 {
        return Err(format!(
            "workflow budget envelope of ${envelope:.4} is exhausted (spent ${spent_usd:.4}); \
             this step was not enqueued"
        ));
    }
    Ok(Some(match step_budget_usd {
        Some(step) => step.min(remaining),
        None => remaining,
    }))
}

// ── engine ──────────────────────────────────────────────────────────────────

/// Opens a run of `def` and enqueues its root steps.
///
/// The `(run, created)` pair mirrors `enqueue_dedup`: an idempotency replay
/// returns the original run rather than a second execution of the same plan.
pub async fn start_run(
    state: &AppState,
    def: &WorkflowDef,
    budget_usd: Option<f64>,
    idempotency_key: Option<&str>,
    principal_id: Option<&str>,
) -> anyhow::Result<(pumper_core::WorkflowRun, bool)> {
    let spec = parse_spec(&def.spec).map_err(|errs| anyhow::anyhow!(errs.join("; ")))?;
    let (run, created) = state
        .storage
        .create_workflow_run(NewWorkflowRun {
            def_id: &def.id,
            budget_usd: budget_usd.or(spec.budget_usd),
            idempotency_key,
            principal_id,
        })
        .await?;
    if !created {
        return Ok((run, false));
    }
    let cells: Vec<(String, Vec<String>)> = spec
        .steps
        .iter()
        .map(|(name, step)| (name.clone(), step.after.clone()))
        .collect();
    state.storage.insert_workflow_steps(&run.id, &cells).await?;
    emit(state, &run.id, &def.name, "running");
    advance(state, &run.id).await?;
    let run = state
        .storage
        .get_workflow_run(&run.id)
        .await?
        .unwrap_or(run);
    Ok((run, true))
}

/// The worker hook: a step job reached a terminal state.
///
/// Called from `worker::finalize_with_stages` beside `fire_terminal_triggers`.
/// One indexed lookup decides whether this job is a step at all — the
/// overwhelmingly common answer is "no", and that costs one seek on
/// `idx_workflow_steps_job`.
pub async fn on_step_terminal(state: &AppState, job: &Job) {
    if !job.status.is_terminal() {
        return;
    }
    let found = match state.storage.workflow_step_for_job(job.id).await {
        Ok(found) => found,
        Err(e) => {
            warn!(job = %job.id, "workflow: step lookup failed, run not advanced: {e}");
            return;
        }
    };
    let Some((run_id, step)) = found else {
        return;
    };
    let status = match job.status {
        JobStatus::Succeeded => StepState::Succeeded,
        JobStatus::Cancelled => StepState::Cancelled,
        _ => StepState::Failed,
    };
    // The idempotence fence: `false` = this cell was already closed (a repeated
    // terminal event, a boot re-evaluation), so the fan-out below has already
    // happened and must not happen again.
    match state
        .storage
        .finish_workflow_step(
            &run_id,
            &step,
            status.as_str(),
            job.result.as_ref(),
            job.error.as_deref(),
        )
        .await
    {
        Ok(true) => {}
        Ok(false) => return,
        Err(e) => {
            warn!(run = %run_id, step = %step, "workflow: step outcome not recorded: {e}");
            return;
        }
    }
    if let Err(e) = state.storage.refresh_workflow_spend(&run_id).await {
        warn!(run = %run_id, "workflow: envelope spend not refreshed: {e}");
    }
    if let Err(e) = advance(state, &run_id).await {
        warn!(run = %run_id, step = %step, "workflow: run not advanced: {e}");
    }
}

/// Drives a run as far as it can go right now: cascade skips, enqueue every
/// step whose barrier is satisfied, then close the run if nothing is open.
///
/// Re-entrant and idempotent — every write is a guarded UPDATE — so it is safe
/// to call from two terminal events at once, and safe to call again at boot.
pub async fn advance(state: &AppState, run_id: &str) -> anyhow::Result<()> {
    let mut rounds = 0usize;
    loop {
        let Some(run) = state.storage.get_workflow_run(run_id).await? else {
            return Ok(());
        };
        if run.status != "running" {
            return Ok(());
        }
        let Some(def) = state.storage.get_workflow(&run.def_id).await? else {
            // The plan was deleted mid-run. The run cannot be advanced and must
            // not sit `running` forever pretending otherwise.
            state
                .storage
                .finish_workflow_run(
                    run_id,
                    "failed",
                    Some("the workflow definition was deleted while this run was open"),
                )
                .await?;
            return Ok(());
        };
        let spec = match parse_spec(&def.spec) {
            Ok(spec) => spec,
            Err(errs) => {
                state
                    .storage
                    .finish_workflow_run(
                        run_id,
                        "failed",
                        Some(&format!(
                            "stored spec no longer validates: {}",
                            errs.join("; ")
                        )),
                    )
                    .await?;
                emit(state, run_id, &def.name, "failed");
                return Ok(());
            }
        };
        rounds += 1;
        if rounds > spec.steps.len() * ADVANCE_ROUNDS_PER_STEP + 4 {
            anyhow::bail!(
                "advance made no terminating progress after {rounds} rounds — refusing to spin"
            );
        }
        let rows = state.storage.workflow_steps(run_id).await?;
        let states = states_of(&rows);
        let results = results_of(&rows);

        // 1. Cascade: steps that can never run are closed before anything else,
        //    so readiness and the verdict both see the final picture.
        let skips = cascade_skips(&spec, &states);
        if !skips.is_empty() {
            let mut changed = false;
            for (step, reason) in skips {
                changed |= state
                    .storage
                    .finish_workflow_step(run_id, &step, "skipped", None, Some(&reason))
                    .await?;
            }
            if changed {
                continue;
            }
        }

        // 2. Enqueue everything whose barrier is satisfied.
        let ready = ready_steps(&spec, &states);
        let mut enqueued_any = false;
        let mut failed_any = false;
        for step in ready {
            match enqueue_step(state, &run, &spec, &step, &results).await {
                Ok(true) => enqueued_any = true,
                Ok(false) => {}
                Err(reason) => {
                    // The step could not be created at all (template miss, bad
                    // params, exhausted envelope). That is the step's failure,
                    // recorded as one — not a silent hole in the plan.
                    if state
                        .storage
                        .finish_workflow_step(run_id, &step, "failed", None, Some(&reason))
                        .await?
                    {
                        failed_any = true;
                    }
                }
            }
        }
        if failed_any {
            continue;
        }
        if enqueued_any {
            state.notify.notify_one();
            return Ok(());
        }

        // 3. Nothing left to start: is the run over?
        if let Some((status, error)) = run_verdict(&states) {
            if state
                .storage
                .finish_workflow_run(run_id, status, error.as_deref())
                .await?
            {
                info!(run = %run_id, workflow = %def.name, status, "workflow run finished");
                emit(state, run_id, &def.name, status);
            }
        }
        return Ok(());
    }
}

/// Claims one step and enqueues its job. `Ok(false)` = another task claimed it
/// first (the join fired on the other upstream's terminal event).
async fn enqueue_step(
    state: &AppState,
    run: &pumper_core::WorkflowRun,
    spec: &WorkflowSpec,
    step: &str,
    results: &BTreeMap<String, Value>,
) -> Result<bool, String> {
    let Some(step_spec) = spec.steps.get(step) else {
        return Err(format!("step '{step}' is not in the stored spec"));
    };
    // Render and validate BEFORE claiming: a step refused at its own door must
    // be reported as failed, and claiming first would leave it `queued` with no
    // job if the process died between the two writes.
    let params = render_params(&step_spec.params, results)?;
    if state.registry.get(&step_spec.app).is_none() {
        return Err(format!(
            "step '{step}' targets app '{}', which is not registered on this node",
            step_spec.app
        ));
    }
    let params = crate::routes::merge_params(
        state
            .registry
            .get(&step_spec.app)
            .map(|a| a.default_params())
            .unwrap_or_else(|| json!({})),
        Some(params),
    );
    crate::mcp::validate_app_params(&state.registry, &step_spec.app, &params)?;
    let budget = step_budget(step_spec.budget_usd, run.budget_usd, run.spent_usd)?;

    let claimed = state
        .storage
        .claim_workflow_step(&run.id, step)
        .await
        .map_err(|e| format!("claim failed: {e}"))?;
    if !claimed {
        return Ok(false);
    }
    let opts = EnqueueOptions {
        params,
        max_attempts: step_spec.max_attempts.unwrap_or(1).clamp(1, 25),
        delay_secs: 0,
        priority: step_spec.priority.unwrap_or(0),
        callback_url: None,
        callback_secret: None,
        budget_usd: budget,
        idempotency_key: Some(format!("{STEP_IDEMPOTENCY_PREFIX}:{}:{step}", run.id)),
        schedule_id: None,
        trigger_id: None,
        source_job_id: None,
        workflow_run_id: Some(run.id.clone()),
        workflow_step: Some(step.to_string()),
        root_id: Some(run.root_id.clone()),
    };
    let (job, _created) = state
        .storage
        .enqueue_dedup_as(&step_spec.app, opts, run.principal_id.as_deref())
        .await
        .map_err(|e| format!("enqueue failed: {e}"))?;
    state
        .storage
        .set_workflow_step_job(&run.id, step, &job.id.to_string())
        .await
        .map_err(|e| format!("step job id not recorded: {e}"))?;
    info!(run = %run.id, step, job = %job.id, app = %step_spec.app, "workflow step enqueued");
    Ok(true)
}

/// Cancels a run: every open step is closed, and each one that already has a
/// job goes through the ordinary `DELETE /jobs/{id}` door so a running step is
/// stopped exactly the way an operator stops any other job.
pub async fn cancel_run(state: &AppState, run_id: &str) -> anyhow::Result<usize> {
    let open = state.storage.open_workflow_steps(run_id).await?;
    let mut cancelled_jobs = 0usize;
    for (step, job_id) in &open {
        if let Some(id) = job_id.as_deref().and_then(|j| j.parse::<uuid::Uuid>().ok()) {
            match crate::routes::cancel_job(
                axum::extract::State(state.clone()),
                axum::extract::Path(id),
            )
            .await
            {
                Ok(_) => cancelled_jobs += 1,
                Err(e) => warn!(run = %run_id, step, "workflow: step job cancel refused: {e:?}"),
            }
        }
        state
            .storage
            .finish_workflow_step(
                run_id,
                step,
                "cancelled",
                None,
                Some("the workflow run was cancelled"),
            )
            .await?;
    }
    state
        .storage
        .finish_workflow_run(run_id, "cancelled", Some("cancelled by request"))
        .await?;
    if let Ok(Some(run)) = state.storage.get_workflow_run(run_id).await {
        if let Ok(Some(def)) = state.storage.get_workflow(&run.def_id).await {
            emit(state, run_id, &def.name, "cancelled");
        }
    }
    Ok(cancelled_jobs)
}

/// The step matrix plus the rolled-up receipt for one run: cost from
/// `cost_events` over the run's job set, yield from `job_yield`.
///
/// Honest nulls, following `routes/receipt.rs`: a step that has no job has no
/// cost, and that is reported as `null`, not as `$0`.
pub async fn run_report(state: &AppState, run_id: &str) -> anyhow::Result<Option<Value>> {
    let Some(run) = state.storage.get_workflow_run(run_id).await? else {
        return Ok(None);
    };
    let def = state.storage.get_workflow(&run.def_id).await?;
    let rows = state.storage.workflow_steps(run_id).await?;
    let mut steps = Vec::with_capacity(rows.len());
    let mut total_cost = 0.0f64;
    let mut priced_steps = 0usize;
    let mut totals: BTreeMap<&str, i64> = BTreeMap::new();
    for row in &rows {
        let job_uuid = row
            .job_id
            .as_deref()
            .and_then(|j| j.parse::<uuid::Uuid>().ok());
        let (cost, yields) = match job_uuid {
            Some(id) => {
                let cost = state.costs.job_total(id).await.unwrap_or(0.0);
                total_cost += cost;
                priced_steps += 1;
                let entries = state
                    .storage
                    .job_yield_entries(id)
                    .await
                    .unwrap_or_default();
                for e in &entries {
                    for (key, v) in [
                        ("new", e.new),
                        ("changed", e.changed),
                        ("unchanged", e.unchanged),
                        ("removed", e.removed),
                    ] {
                        if let Some(v) = v {
                            *totals.entry(key).or_insert(0) += v;
                        }
                    }
                }
                (json!(cost), json!(entries))
            }
            None => (Value::Null, Value::Null),
        };
        steps.push(json!({
            "step": row.step,
            "status": row.status,
            "job_id": row.job_id,
            "depends_on": row.depends_on,
            "cost_usd": cost,
            "yield": yields,
            "error": row.error,
            "finished_at": row.finished_at,
        }));
    }
    let mut unknown: Vec<String> = Vec::new();
    if priced_steps < rows.len() {
        unknown.push(format!(
            "cost: {} of {} steps never became a job (pending, skipped, or refused at their own \
             door), so they have no cost — reported as null, not as $0",
            rows.len() - priced_steps,
            rows.len()
        ));
    }
    if totals.is_empty() {
        unknown.push(
            "yield: no step's result reported UpsertSummary-shaped counts, so what this run \
             changed cannot be rolled up here. See each step job's own receipt."
                .into(),
        );
    }
    Ok(Some(json!({
        "run": run,
        "workflow": def.as_ref().map(|d| json!({ "id": d.id, "name": d.name, "cron": d.cron })),
        "steps": steps,
        "receipt": {
            "cost_usd": total_cost,
            "budget_usd": run.budget_usd,
            "steps_total": rows.len(),
            "steps_priced": priced_steps,
            "yield": totals,
        },
        "unknown": unknown,
    })))
}

/// Persisted step states, keyed by step name.
fn states_of(rows: &[WorkflowStepRow]) -> BTreeMap<String, StepState> {
    rows.iter()
        .map(|r| {
            (
                r.step.clone(),
                // An unrecognised status is treated as OPEN, never as succeeded:
                // a barrier that opens on a status nobody wrote is the one
                // failure mode a join must not have.
                StepState::parse(&r.status).unwrap_or(StepState::Pending),
            )
        })
        .collect()
}

/// Results of the steps that succeeded — the only ones a template may read.
fn results_of(rows: &[WorkflowStepRow]) -> BTreeMap<String, Value> {
    rows.iter()
        .filter(|r| r.status == "succeeded")
        .map(|r| (r.step.clone(), r.result.clone().unwrap_or(Value::Null)))
        .collect()
}

/// Publishes a `workflow.<status>` event on the same bus job transitions ride,
/// so `GET /events` and the MCP live stream see run lifecycle for free.
///
/// The event's `job_id` slot carries the RUN id and `app` carries the workflow
/// name — the bus's two identity fields, used for what they mean here. A run id
/// that will not parse as a UUID (it always will; it is one) degrades to the nil
/// UUID rather than dropping the event.
fn emit(state: &AppState, run_id: &str, name: &str, status: &str) {
    let id = run_id.parse::<uuid::Uuid>().unwrap_or(uuid::Uuid::nil());
    state
        .events
        .emit(JobEvent::new(id, name, format!("workflow.{status}")));
}

// ── scheduled workflows ─────────────────────────────────────────────────────

/// Fires a run for every enabled workflow whose cron is due, guarded on "the
/// newest run is still open" — the same overlap rule schedules use.
///
/// Called once per scheduler tick. Deliberately NOT a row in the `schedules`
/// table: a schedule targets an app and carries app params, and widening it to
/// "app or workflow" would put a nullable second target on every schedule read
/// in the system. The cron lives on the plan it schedules.
pub async fn reconcile_scheduled(
    state: &AppState,
    last_pass: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) {
    let defs = match state.storage.scheduled_workflows().await {
        Ok(defs) => defs,
        Err(e) => {
            warn!("workflow: scheduled plans could not be listed: {e}");
            return;
        }
    };
    for def in defs {
        let Some(cron) = def.cron.as_deref() else {
            continue;
        };
        let parsed = match <cron::Schedule as std::str::FromStr>::from_str(cron) {
            Ok(s) => s,
            Err(e) => {
                warn!(workflow = %def.name, cron, "workflow: invalid cron, never fires: {e}");
                continue;
            }
        };
        // Due = a firing landed between the previous pass and now. With no
        // previous pass (first tick after boot) nothing is due: a boot must not
        // fire every scheduled plan at once.
        let Some(since) = last_pass else { continue };
        if !parsed.after(&since).take(1).any(|t| t <= now) {
            continue;
        }
        match state.storage.latest_workflow_run(&def.id).await {
            Ok(Some((id, status))) if status == "running" => {
                info!(workflow = %def.name, run = %id, "workflow: firing held, newest run still open");
                continue;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(workflow = %def.name, "workflow: overlap guard unreadable, not firing: {e}");
                continue;
            }
        }
        match start_run(state, &def, None, None, None).await {
            Ok((run, true)) => {
                info!(workflow = %def.name, run = %run.id, "scheduled workflow run fired")
            }
            Ok((_, false)) => {}
            Err(e) => warn!(workflow = %def.name, "workflow: scheduled run failed to start: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(doc: Value) -> WorkflowSpec {
        parse_spec(&doc).expect("spec should validate")
    }

    fn diamond() -> WorkflowSpec {
        spec(json!({
            "steps": {
                "a": { "app": "hackernews" },
                "b": { "app": "hackernews" },
                "join": { "app": "hackernews", "after": { "all_of": ["a", "b"] } }
            }
        }))
    }

    fn states(pairs: &[(&str, StepState)]) -> BTreeMap<String, StepState> {
        pairs.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    /// The whole card, as one test. A join must fire when the LAST upstream
    /// lands, and must not be ready when only one has — the anti-pattern being
    /// "fire the downstream on each upstream completion", which runs the join
    /// once per source and bills the plan twice.
    #[test]
    fn a_join_is_ready_once_all_upstreams_succeeded_not_once_per_upstream() {
        let spec = diamond();
        let one_done = states(&[
            ("a", StepState::Succeeded),
            ("b", StepState::Queued),
            ("join", StepState::Pending),
        ]);
        assert_eq!(ready_steps(&spec, &one_done), Vec::<String>::new());

        let both_done = states(&[
            ("a", StepState::Succeeded),
            ("b", StepState::Succeeded),
            ("join", StepState::Pending),
        ]);
        assert_eq!(ready_steps(&spec, &both_done), vec!["join".to_string()]);

        // And once claimed it is no longer ready, so a second terminal event
        // arriving for the other upstream cannot produce a second enqueue.
        let claimed = states(&[
            ("a", StepState::Succeeded),
            ("b", StepState::Succeeded),
            ("join", StepState::Queued),
        ]);
        assert_eq!(ready_steps(&spec, &claimed), Vec::<String>::new());
    }

    #[test]
    fn root_steps_are_ready_at_open_and_dependants_are_not() {
        let spec = diamond();
        let fresh = states(&[
            ("a", StepState::Pending),
            ("b", StepState::Pending),
            ("join", StepState::Pending),
        ]);
        assert_eq!(
            ready_steps(&spec, &fresh),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    /// A skipped upstream can never satisfy a barrier. Treating `skipped` as
    /// merely "not succeeded yet" would park the whole downstream forever with
    /// the run stuck `running` and nothing to blame.
    #[test]
    fn a_skipped_upstream_blocks_rather_than_parks_its_dependants() {
        let spec = diamond();
        let st = states(&[
            ("a", StepState::Succeeded),
            ("b", StepState::Skipped),
            ("join", StepState::Pending),
        ]);
        assert!(ready_steps(&spec, &st).is_empty());
        let skips = cascade_skips(&spec, &st);
        assert_eq!(skips.len(), 1);
        assert_eq!(skips[0].0, "join");
        assert!(run_verdict(&states(&[
            ("a", StepState::Succeeded),
            ("b", StepState::Skipped),
            ("join", StepState::Skipped),
        ]))
        .is_some());
    }

    #[test]
    fn fail_fast_cascades_to_every_unstarted_step_and_continue_only_to_dependants() {
        let doc = json!({
            "steps": {
                "a": { "app": "hackernews" },
                "b": { "app": "hackernews" },
                "after_a": { "app": "hackernews", "after": ["a"] }
            }
        });
        let mut fast = spec(doc.clone());
        fast.on_failure = OnFailure::FailFast;
        let st = states(&[
            ("a", StepState::Failed),
            ("b", StepState::Pending),
            ("after_a", StepState::Pending),
        ]);
        let names: Vec<String> = cascade_skips(&fast, &st)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(names, vec!["after_a".to_string(), "b".to_string()]);

        let mut cont = spec(doc);
        cont.on_failure = OnFailure::Continue;
        let names: Vec<String> = cascade_skips(&cont, &st)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(
            names,
            vec!["after_a".to_string()],
            "an independent branch must keep running under on_failure = continue"
        );
    }

    #[test]
    fn a_run_is_over_only_when_no_step_is_open() {
        assert!(run_verdict(&states(&[("a", StepState::Queued)])).is_none());
        assert!(run_verdict(&states(&[("a", StepState::Pending)])).is_none());
        assert_eq!(
            run_verdict(&states(&[("a", StepState::Succeeded)])).map(|(s, _)| s),
            Some("succeeded")
        );
        assert_eq!(
            run_verdict(&states(&[
                ("a", StepState::Succeeded),
                ("b", StepState::Failed)
            ]))
            .map(|(s, _)| s),
            Some("failed")
        );
        assert_eq!(
            run_verdict(&states(&[("a", StepState::Cancelled)])).map(|(s, _)| s),
            Some("cancelled")
        );
        // A plan that silently did not do half of what it declared did not
        // succeed, even though nothing "failed".
        assert_eq!(
            run_verdict(&states(&[
                ("a", StepState::Succeeded),
                ("b", StepState::Skipped)
            ]))
            .map(|(s, _)| s),
            Some("failed")
        );
    }

    // ── spec validation ─────────────────────────────────────────────────────

    #[test]
    fn a_cycle_is_refused_at_the_door_not_discovered_as_a_deadlocked_run() {
        let err = parse_spec(&json!({
            "steps": {
                "a": { "app": "x", "after": ["b"] },
                "b": { "app": "x", "after": ["a"] }
            }
        }))
        .unwrap_err();
        assert!(err.iter().any(|e| e.contains("cycle")), "{err:?}");
    }

    #[test]
    fn a_barrier_naming_an_unknown_step_is_refused() {
        let err = parse_spec(&json!({
            "steps": { "a": { "app": "x", "after": ["ghost"] } }
        }))
        .unwrap_err();
        assert!(
            err.iter().any(|e| e.contains("'ghost' is not a step")),
            "{err:?}"
        );
    }

    /// `any_of` is out of the v1 slice. Refused by name — silently reading it
    /// as `all_of` would turn a quorum barrier into a different plan that
    /// happens to parse.
    #[test]
    fn any_of_is_refused_by_name_not_silently_read_as_all_of() {
        let err = parse_spec(&json!({
            "steps": {
                "a": { "app": "x" },
                "b": { "app": "x", "after": { "any_of": ["a"] } }
            }
        }))
        .unwrap_err();
        assert!(err.iter().any(|e| e.contains("any_of")), "{err:?}");
    }

    #[test]
    fn a_zero_budget_is_refused_rather_than_read_as_unlimited() {
        for doc in [
            json!({ "steps": { "a": { "app": "x" } }, "budget_usd": 0 }),
            json!({ "steps": { "a": { "app": "x", "budget_usd": -1 } } }),
        ] {
            assert!(parse_spec(&doc).is_err(), "{doc}");
        }
    }

    #[test]
    fn step_budgets_summing_past_the_envelope_are_refused_at_create() {
        let err = parse_spec(&json!({
            "budget_usd": 1.0,
            "steps": {
                "a": { "app": "x", "budget_usd": 0.75 },
                "b": { "app": "x", "budget_usd": 0.75 }
            }
        }))
        .unwrap_err();
        assert!(err.iter().any(|e| e.contains("envelope")), "{err:?}");
    }

    // ── templating ──────────────────────────────────────────────────────────

    fn results() -> BTreeMap<String, Value> {
        let mut m = BTreeMap::new();
        m.insert(
            "crawl".to_string(),
            json!({ "pages": 12, "urls": ["a", "b"], "meta": { "host": "example.test" } }),
        );
        m
    }

    #[test]
    fn a_whole_string_template_substitutes_the_json_value_not_its_text() {
        let out = render_params(
            &json!({ "n": "{{steps.crawl.result.pages}}", "urls": "{{steps.crawl.result.urls}}" }),
            &results(),
        )
        .unwrap();
        assert_eq!(out["n"], json!(12), "a number must stay a number");
        assert_eq!(out["urls"], json!(["a", "b"]));
    }

    #[test]
    fn an_embedded_token_interpolates_into_the_surrounding_text() {
        let out = render_params(
            &json!(
                "crawled {{steps.crawl.result.pages}} pages of {{steps.crawl.result.meta.host}}"
            ),
            &results(),
        )
        .unwrap();
        assert_eq!(out, json!("crawled 12 pages of example.test"));
    }

    /// The anti-pattern: an unresolvable reference rendered as `null` or left
    /// as the literal `{{…}}`. The step then runs with silently wrong params
    /// and the plan produces a plausible, wrong result.
    #[test]
    fn a_template_miss_is_refused_not_rendered_as_null() {
        for tpl in [
            json!({ "x": "{{steps.crawl.result.nope}}" }),
            json!({ "x": "{{steps.ghost.result}}" }),
            json!({ "x": "{{steps.crawl.params}}" }),
            json!({ "x": "{{crawl.result}}" }),
            json!({ "x": "{{steps.crawl.result.pages" }),
        ] {
            let err = render_params(&tpl, &results()).unwrap_err();
            assert!(err.starts_with("params template:"), "{tpl} -> {err}");
        }
    }

    #[test]
    fn a_template_free_document_passes_through_untouched() {
        let doc = json!({ "a": 1, "b": ["x", { "c": true }], "d": null });
        assert_eq!(render_params(&doc, &BTreeMap::new()).unwrap(), doc);
        assert!(!has_template(&doc));
        assert!(has_template(&json!({ "a": ["{{steps.x.result}}"] })));
    }

    // ── envelope ────────────────────────────────────────────────────────────

    /// A step's own budget is a CAP, never a grant: whatever the spec says, the
    /// step may not be handed more than the envelope has left.
    #[test]
    fn a_step_budget_is_clamped_to_the_envelope_remainder_not_granted_over_it() {
        assert_eq!(step_budget(Some(5.0), Some(1.0), 0.25), Ok(Some(0.75)));
        assert_eq!(step_budget(Some(0.1), Some(1.0), 0.25), Ok(Some(0.1)));
        assert_eq!(step_budget(None, Some(1.0), 0.25), Ok(Some(0.75)));
        // No envelope: the step's own ceiling stands, `None` = uncapped, exactly
        // as at the jobs door.
        assert_eq!(step_budget(Some(2.0), None, 0.0), Ok(Some(2.0)));
        assert_eq!(step_budget(None, None, 0.0), Ok(None));
    }

    #[test]
    fn an_exhausted_envelope_refuses_the_step_rather_than_enqueuing_it_uncapped() {
        assert!(step_budget(None, Some(1.0), 1.0).is_err());
        assert!(step_budget(Some(0.5), Some(1.0), 1.5).is_err());
    }
}
