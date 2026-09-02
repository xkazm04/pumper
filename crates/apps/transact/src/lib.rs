//! Transact (M06 + N01 v2): execute a declarative browser flow up to the final
//! confirmation state and emit a redacted evidence bundle — then either STOP
//! there (`submit: false`, the dry run) or PARK there for a human decision
//! (`submit: true`, the approval lifecycle).
//!
//! ## The two modes
//!
//! - **`submit: false`** — unchanged from v1. Navigate, fill, click, wait, stop
//!   before the irreversible action, write `evidence.json` + `dom.html`, report
//!   `would_submit`. Nothing is staged and nothing can ever be submitted.
//! - **`submit: true`** — the same dry run, byte for byte, plus a `pending` row
//!   in the transactions ledger keyed on `idempotency_key`. The job then
//!   **parks** (N02 `waiting`) with the evidence bundle as its `input_request`.
//!   `POST /transactions/{id}/approve` (admin scope, `[transact] allow_live`)
//!   resumes it, and only the resumed attempt calls `Browser::commit`, which
//!   re-probes the live page and refuses unless it still hashes to the digest
//!   that was reviewed.
//!
//! ## Why the app cannot submit on its own say-so
//!
//! The commit attempt does not trust its resume input for authority. It re-reads
//! the ledger row by `idempotency_key` and refuses unless the row itself says
//! `approved` — so a job whose params or resume payload were tampered with still
//! cannot submit, and the `state = 'approved' -> 'submitted'` transition is
//! SQL-guarded, which is what makes "exactly once per key" true rather than
//! intended.
//!
//! ## What this slice deliberately does NOT do
//!
//! - No WASM policy predicates for auto-approval: every release is a human (or
//!   an admin-scoped agent) acting through the approve door.
//! - No screenshots — the render path still does not expose capture, so the
//!   receipt is DOM-only and says so rather than claiming a path it cannot honor.
//! - **No session-handle resume.** The commit does not inherit the staging run's
//!   live tab; it rebuilds the flow deterministically from the same params under
//!   the same profile. That is why `steps` must be genuinely reversible: they run
//!   again (twice more, in fact — once to re-probe and once to submit).
//!
//! ## Secrets
//!
//! The filled-field summary proves a field was filled without republishing what
//! was typed into it: password inputs (and credential/card `autocomplete`
//! fields) are masked **in the page** by `pumper_core::filled_fields_js`, so the
//! plaintext never reaches this process, `evidence.json`, `jobs.result`, an SSE
//! event or a webhook payload. Only `{found, redacted, value_len}` survives, and
//! the approval digest is computed over those — never over a value.
//!
//! The job's **params** are a different matter: they still hold whatever the
//! caller put in a `type` step's `text`, because that is the job model's
//! storage posture for every app (params are persisted verbatim). Redacting
//! them is a job-model change, not a transact one.

use async_trait::async_trait;
use pumper_core::engine::{
    evidence_digest, profile_name_pattern, unknown_transact_fields, ApprovedTransaction,
    TRANSACT_FIELDS,
};
use pumper_core::transactions::{
    by_key, mark_submitted, stage_pending, NewTransaction, TransactionState,
};
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, ManifestExample, Result, ScrapeApp,
    TransactEvidence, TransactRequest,
};
use serde_json::{json, Value};

/// The artifact the post-submit receipt is written to.
const RECEIPT_ARTIFACT: &str = "receipt.json";

/// What a parked transact job is asking for. The shape is the app's contract
/// with the approve door and with any MCP client reading `input_request`, so it
/// is built in one named function rather than inline at the park.
fn approval_request(transaction_id: &str, evidence_sha: &str, evidence: &Value) -> Value {
    json!({
        "kind": "transaction_approval",
        "transaction_id": transaction_id,
        "evidence_sha": evidence_sha,
        "approve": format!("POST /transactions/{transaction_id}/approve"),
        "reject": format!("POST /transactions/{transaction_id}/reject"),
        "prompt": "Review the evidence bundle below. Approving performs the irreversible \
                   `would_submit` action under the named profile, exactly once.",
        "evidence": evidence,
    })
}

pub struct Transact;

#[async_trait]
impl ScrapeApp for Transact {
    fn name(&self) -> &'static str {
        "transact"
    }

    fn description(&self) -> &'static str {
        "Execute a declarative browser flow: steps (fill/click/wait) run to the final \
         confirmation state, then the flow stops BEFORE the irreversible submit_action and \
         emits an evidence bundle (evidence.json + dom.html artifacts: DOM snapshot, \
         filled-field values, the exact would-be action). submit:false ends there. \
         submit:true additionally stages a `pending` transaction and PARKS the job for \
         approval; POST /transactions/{id}/approve resumes it and performs the action once, \
         refusing if the live page drifted from the reviewed evidence. Params: {\"url\": \
         \"...\", \"idempotency_key\": \"...\", \"steps\": [PageAction...], \
         \"submit_action\": PageAction, \"confirm_selector\": \"...\", \"profile\": \
         \"vault-profile\", \"submit\": false}"
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "type": "object",
                "required": ["url", "idempotency_key", "submit_action"],
                // The door, not the run: a `submit: true`, a blank key or a
                // typo'd param used to enqueue fine and fail (or worse,
                // half-run) minutes later. Enqueue validates this schema, so
                // they are 422s before a job row exists.
                "patternProperties": {
                    // Host-injected envelopes (`_trigger`) stay legal.
                    "^_": {}
                },
                "additionalProperties": false,
                "properties": {
                    "url": { "type": "string", "description": "Page the flow starts on." },
                    "idempotency_key": {
                        "type": "string", "minLength": 1, "pattern": "\\S",
                        "description": "Caller-chosen key recorded with the evidence bundle and \
                                        UNIQUE in the transactions ledger; must contain a \
                                        non-whitespace character. One key can only ever own one \
                                        transaction, so it submits at most once — ever."
                    },
                    "profile": {
                        "type": "string",
                        // The engine's own rule, not a second copy of it:
                        // `profile_name_pattern()` is generated from
                        // `validate_profile_name`'s alphabet + PROFILE_NAME_MAX_LEN
                        // and pinned to it by a test in core. Without it a typo'd
                        // profile passed the door and became a job that failed on
                        // the most expensive tier.
                        "pattern": profile_name_pattern(),
                        "description": "Session-vault profile to act under (logins/cookies). \
                                        1-64 chars of ASCII letters, digits, '-' or '_' — the \
                                        same rule the engine enforces, so a typo is a 422 here \
                                        rather than a failed job on the browser tier."
                    },
                    "steps": {
                        "type": "array",
                        "description": "Reversible PageActions (type/click/wait_for_selector/\
                                        wait_ms/scroll...) executed in order.",
                        "items": { "type": "object", "required": ["action"] }
                    },
                    "submit_action": {
                        "type": "object", "required": ["action"],
                        "description": "The exact irreversible PageAction — captured into the \
                                        evidence as would_submit, and performed ONLY by an \
                                        approved commit, never by the staging run."
                    },
                    "submit": {
                        "type": "boolean", "default": false,
                        "description": "false (default) = a plain dry run that ends at the \
                                        evidence bundle. true = stage a `pending` transaction \
                                        and PARK for approval; the action still cannot run \
                                        without POST /transactions/{id}/approve, which needs \
                                        admin scope and [transact] allow_live = true."
                    },
                    "wait_for_selector": { "type": "string" },
                    "confirm_selector": {
                        "type": "string",
                        "description": "Selector proving the submission landed, waited for AFTER \
                                        the irreversible action during the approved commit. \
                                        Omitted = the receipt reports confirm_selector_found: \
                                        null rather than inventing a success."
                    },
                    "extra_wait_ms": { "type": "integer", "minimum": 0 },
                    "max_body_bytes": { "type": "integer", "minimum": 0 }
                }
            })),
            examples: vec![ManifestExample {
                description: "Dry-run a newsletter signup: fill the email, advance to the \
                              confirmation step, capture evidence, and report the submit \
                              click that was NOT performed.",
                params: json!({
                    "url": "https://portal.example/newsletter",
                    "idempotency_key": "newsletter-signup-2026-07-31",
                    "profile": "portal_login",
                    "steps": [
                        { "action": "type", "selector": "#email", "text": "team@example.com" },
                        { "action": "click", "selector": "#next" },
                        { "action": "wait_for_selector", "selector": "#confirm-panel" }
                    ],
                    "submit_action": { "action": "click", "selector": "#confirm-submit" },
                    "submit": false
                }),
            }],
            output_shape: Some(
                "{dry_run: true, idempotency_key, profile, url, final_url, \
                 steps: {requested, attempted, completed, deadline_hit, outcomes: [ok|\
                 selector_missing|action_failed|partial]}, steps_completed (= steps.completed, \
                 SUCCESSES not attempts), wait_for_selector_found, \
                 filled_fields: [{selector, value (null when redacted/empty), found, redacted, \
                 value_len, truncated}], would_submit: PageAction, \
                 submit_target: {selector, found, visible, enabled, \
                 tag, label}, dom_truncated, nav_timed_out, \
                 artifacts: {evidence: \"evidence.json\", dom: \"dom.html\"}, \
                 transaction_id (submit:true only), transaction_state}. An approved commit \
                 instead returns {submitted, transaction_id, idempotency_key, profile, url, \
                 final_url, approved_evidence_sha, observed_evidence_sha, submit_action, \
                 submit_outcome, confirm_selector, confirm_selector_found, steps, \
                 artifacts: {receipt: \"receipt.json\", dom: \"dom-after.html\"}}",
            ),
            // Browser-only by design: the flow never escalates to a metered
            // engine, so per CostClass's own contract runs are Free.
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        // A misspelled key is not a harmless no-op for a flow that ACTS: serde
        // drops it silently, so `"step"` (singular) runs a ZERO-step flow and
        // still hands back a plausible landing-page bundle. The schema rejects
        // it at enqueue; this is the app-side twin that also covers the paths
        // which bypass enqueue-time validation (trigger-fired jobs).
        let unknown = unknown_transact_fields(&ctx.params);
        if !unknown.is_empty() {
            return Err(Error::Transact(format!(
                "unknown transact params {unknown:?}: a misspelled key is silently dropped by \
                 the deserializer, so the flow would run WITHOUT those steps and still emit an \
                 evidence bundle a human might approve off. Known keys: {TRANSACT_FIELDS:?} \
                 (host-injected keys starting with '_', e.g. `_trigger`, are allowed)."
            )));
        }
        let req: TransactRequest = serde_json::from_value(ctx.params.clone())?;
        // Reject before ANY browser work: an empty idempotency key and a bad
        // profile name are typed errors, not partial executions. Each is
        // deterministic, so `Error::Transact` is terminal for the job — a
        // refusal fails ONCE instead of riding the retry ladder. The engine
        // re-validates too (defense in depth).
        req.validate()?;

        // Is this the resumed attempt of a park an approval released? The
        // presence of a resume input decides WHICH half of the lifecycle runs,
        // and the ledger — not the input — decides whether it may submit.
        if req.submit && ctx.restore_input().is_some() {
            return commit(&ctx, req).await;
        }
        stage(&ctx, req).await
    }
}

/// The staging half: today's dry run, plus (when `submit: true`) a `pending`
/// ledger row and a park.
///
/// The dry run is byte-for-byte the v1 path — same engine call, same artifacts,
/// same result keys. `submit: true` adds to it; it never changes it.
async fn stage(ctx: &AppContext, req: TransactRequest) -> Result<Value> {
    let submit = req.submit;
    let idempotency_key = req.idempotency_key.clone();
    let evidence = ctx.engines.browser.transact(req).await?;

    // Cost provenance: browser flows are free, but a transact run should
    // still be visible in the job's cost trail like any engine use.
    ctx.meter(
        "browser",
        Some(&evidence.url),
        0.0,
        Some("transact_dry_run"),
    )
    .await;

    // Big payloads to artifacts (repo convention): the DOM snapshot and the
    // full evidence bundle live beside the job, not inside jobs.result.
    ctx.save_artifact("dom.html", evidence.dom_html.as_bytes())
        .await?;
    let bundle = evidence_bundle(&evidence);
    ctx.save_artifact(
        "evidence.json",
        serde_json::to_vec_pretty(&bundle)?.as_slice(),
    )
    .await?;

    let mut result = stage_result(&evidence);
    if !submit {
        return Ok(result);
    }

    // The digest binds the approval to what the reviewer actually saw: the
    // submit target's clickability and each field's shape, never a value.
    let evidence_sha = evidence_digest(evidence.submit_target.as_ref(), &evidence.filled_fields);
    let row = stage_pending(
        &ctx.datasets.pool(),
        NewTransaction {
            idempotency_key: &idempotency_key,
            app: &ctx.app,
            job_id: Some(&ctx.job_id.to_string()),
            profile: evidence.profile.as_deref(),
            evidence_sha: &evidence_sha,
        },
    )
    .await?;
    // A key that already reached a terminal state is not re-openable, so this
    // run has nothing to wait for — and must not park forever pretending it
    // does. Report the ledger's verdict and finish.
    if row.state != TransactionState::Pending {
        result["transaction_id"] = json!(row.id);
        result["transaction_state"] = json!(row.state);
        result["note"] = json!(format!(
            "idempotency_key '{idempotency_key}' already resolved as '{}': nothing was staged \
             and nothing will be submitted. One key owns one transaction, for its whole life.",
            row.state.as_str()
        ));
        return Ok(result);
    }

    result["transaction_id"] = json!(row.id);
    result["transaction_state"] = json!(row.state);
    // Park (N02). The forced checkpoint carries nothing the ledger does not
    // already own — the ledger row IS the resume state — but the request the
    // human answers carries the whole bundle, so an approver reading
    // `GET /jobs/{id}` or MCP `wait_job` sees what they are deciding about.
    Err(ctx
        .await_input(
            json!({ "transaction_id": row.id, "evidence_sha": evidence_sha }),
            approval_request(&row.id, &evidence_sha, &bundle),
        )
        .await)
}

/// The commit half: the attempt an approval resumed.
///
/// Authority comes from the LEDGER, never from the resume input. A resumed
/// attempt whose row is not `approved` refuses terminally, so neither a
/// hand-posted `POST /jobs/{id}/resume` nor a tampered param can reach a live
/// submit button.
async fn commit(ctx: &AppContext, req: TransactRequest) -> Result<Value> {
    let pool = ctx.datasets.pool();
    let key = req.idempotency_key.clone();
    let row = by_key(&pool, &key).await?.ok_or_else(|| {
        Error::Transact(format!(
            "no transaction is staged for idempotency_key '{key}', so there is no approval to \
             act on. A resume cannot mint one: the ledger is the only thing that can release an \
             irreversible action."
        ))
    })?;
    if row.state != TransactionState::Approved {
        return Err(Error::Transact(format!(
            "transaction {} is '{}', not 'approved': this run was resumed without an approval \
             that the ledger recognises, so nothing was submitted. Approve through \
             POST /transactions/{}/approve, which is the only door that can move a row to \
             'approved'.",
            row.id,
            row.state.as_str(),
            row.id
        )));
    }

    let receipt = ctx
        .engines
        .browser
        .commit(
            req,
            ApprovedTransaction {
                transaction_id: row.id.clone(),
                evidence_sha: row.evidence_sha.clone(),
            },
        )
        .await?;
    ctx.meter("browser", Some(&receipt.url), 0.0, Some("transact_commit"))
        .await;

    ctx.save_artifact("dom-after.html", receipt.dom_html.as_bytes())
        .await?;
    let bundle = receipt_bundle(&receipt);
    ctx.save_artifact(
        RECEIPT_ARTIFACT,
        serde_json::to_vec_pretty(&bundle)?.as_slice(),
    )
    .await?;

    // The ledger only learns `submitted` when the action actually ran. A
    // receipt that says otherwise leaves the row `approved` — an honest "we
    // were released and did not manage it", never a submission nobody made.
    if receipt.submitted {
        mark_submitted(&pool, &row.id, RECEIPT_ARTIFACT).await?;
    }
    let mut out = bundle;
    out["artifacts"] = json!({ "receipt": RECEIPT_ARTIFACT, "dom": "dom-after.html" });
    Ok(out)
}

/// The evidence bundle written to `evidence.json` and handed to the approver.
fn evidence_bundle(evidence: &TransactEvidence) -> Value {
    json!({
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "dry_run": evidence.dry_run,
        "idempotency_key": evidence.idempotency_key,
        "profile": evidence.profile,
        "url": evidence.url,
        "final_url": evidence.final_url,
        "steps": step_block(evidence),
        "steps_completed": evidence.steps_completed,
        "wait_for_selector_found": evidence.wait_for_selector_found,
        "filled_fields": evidence.filled_fields,
        "would_submit": evidence.would_submit,
        "submit_target": evidence.submit_target,
        "dom": {
            "artifact": "dom.html",
            "bytes_captured": evidence.dom_bytes,
            "bytes_stored": evidence.dom_html.len(),
            "truncated": evidence.dom_truncated,
        },
        "screenshot_path": evidence.screenshot_path,
        "nav_timed_out": evidence.nav_timed_out,
        "dom_artifact": "dom.html",
    })
}

/// The steps block is the bundle's honesty core: requested / attempted /
/// completed are three different numbers, and only `completed` counts steps
/// that actually worked. A flow whose selectors all missed reports
/// `completed: 0` with `outcomes: ["selector_missing", ...]`.
fn step_block(evidence: &TransactEvidence) -> Value {
    json!({
        "requested": evidence.steps_requested,
        "attempted": evidence.steps_attempted,
        "completed": evidence.steps_completed,
        "deadline_hit": evidence.steps_deadline_hit,
        "outcomes": evidence.step_outcomes,
    })
}

/// The staging run's job result.
fn stage_result(evidence: &TransactEvidence) -> Value {
    json!({
        "dry_run": evidence.dry_run,
        "idempotency_key": evidence.idempotency_key,
        "profile": evidence.profile,
        "url": evidence.url,
        "final_url": evidence.final_url,
        "steps": step_block(evidence),
        "steps_completed": evidence.steps_completed,
        "wait_for_selector_found": evidence.wait_for_selector_found,
        "filled_fields": evidence.filled_fields,
        "would_submit": evidence.would_submit,
        "submit_target": evidence.submit_target,
        "dom_truncated": evidence.dom_truncated,
        "nav_timed_out": evidence.nav_timed_out,
        "artifacts": { "evidence": "evidence.json", "dom": "dom.html" },
    })
}

/// The post-submit receipt written to `receipt.json` and returned as the job's
/// result.
fn receipt_bundle(receipt: &pumper_core::TransactReceipt) -> Value {
    json!({
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "submitted": receipt.submitted,
        "transaction_id": receipt.transaction_id,
        "idempotency_key": receipt.idempotency_key,
        "profile": receipt.profile,
        "url": receipt.url,
        "final_url": receipt.final_url,
        "approved_evidence_sha": receipt.approved_evidence_sha,
        "observed_evidence_sha": receipt.observed_evidence_sha,
        "submit_action": receipt.submit_action,
        "submit_outcome": receipt.submit_outcome,
        "confirm_selector": receipt.confirm_selector,
        "confirm_selector_found": receipt.confirm_selector_found,
        "steps": {
            "requested": receipt.steps_requested,
            "attempted": receipt.steps_attempted,
            "completed": receipt.steps_completed,
            "deadline_hit": receipt.steps_deadline_hit,
            "outcomes": receipt.step_outcomes,
        },
        "dom": {
            "artifact": "dom-after.html",
            "bytes_captured": receipt.dom_bytes,
            "bytes_stored": receipt.dom_html.len(),
            "truncated": receipt.dom_truncated,
        },
        "screenshot_path": Value::Null,
        "nav_timed_out": receipt.nav_timed_out,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumper_core::engine::{parse_filled_fields, StepOutcome, SubmitTarget};
    use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
    use pumper_core::transactions::{get, list, mark_approved};
    use pumper_core::{
        Browser, Error, PageAction, RenderRequest, RenderedPage, Storage, TransactEvidence,
        TransactReceipt,
    };
    use std::sync::Arc;

    fn dry_run_params() -> Value {
        json!({
            "url": "https://portal.example/newsletter",
            "idempotency_key": "newsletter-1",
            "steps": [
                { "action": "type", "selector": "#email", "text": "team@example.com" },
                { "action": "click", "selector": "#next" }
            ],
            "submit_action": { "action": "click", "selector": "#confirm-submit" }
        })
    }

    /// A browser that answers `transact` with canned evidence and records the
    /// request; `render` is a test bug (the app must use the transact seam).
    /// `outcomes` scripts what each step "did", so a healthy flow and a flow
    /// whose every selector missed can be compared through the same app code.
    struct ScriptedBrowser {
        seen: std::sync::Mutex<Vec<TransactRequest>>,
        /// Every `commit` this browser was asked to perform. The count IS the
        /// exactly-once instrument: a second approval that reached the engine
        /// would show up here.
        committed: std::sync::Mutex<Vec<ApprovedTransaction>>,
        outcomes: Vec<StepOutcome>,
        submit_found: Option<bool>,
    }

    impl ScriptedBrowser {
        /// Every step succeeded and the submit button is on the page.
        fn healthy() -> Self {
            Self {
                seen: std::sync::Mutex::new(Vec::new()),
                committed: std::sync::Mutex::new(Vec::new()),
                outcomes: vec![StepOutcome::Ok, StepOutcome::Ok],
                submit_found: Some(true),
            }
        }

        /// Every selector missed and the submit target is nowhere to be seen.
        fn all_selectors_missing() -> Self {
            Self {
                seen: std::sync::Mutex::new(Vec::new()),
                committed: std::sync::Mutex::new(Vec::new()),
                outcomes: vec![StepOutcome::SelectorMissing, StepOutcome::SelectorMissing],
                submit_found: Some(false),
            }
        }
    }

    #[async_trait]
    impl Browser for ScriptedBrowser {
        async fn render(&self, _: RenderRequest) -> pumper_core::Result<RenderedPage> {
            panic!("transact app must call Browser::transact, not render")
        }

        async fn transact(&self, req: TransactRequest) -> pumper_core::Result<TransactEvidence> {
            req.validate()?;
            let completed = self.outcomes.iter().filter(|o| o.is_ok()).count();
            let evidence = TransactEvidence {
                dry_run: true,
                idempotency_key: req.idempotency_key.clone(),
                profile: req.profile.clone(),
                url: req.url.clone(),
                final_url: Some(format!("{}?step=confirm", req.url)),
                steps_requested: req.steps.len(),
                steps_attempted: self.outcomes.len(),
                steps_completed: completed,
                step_outcomes: self.outcomes.clone(),
                steps_deadline_hit: false,
                wait_for_selector_found: Some(completed > 0),
                // Built through the REAL decode path, from a probe result that
                // (as a drifting page might) still carries the password's
                // plaintext alongside `redacted: true`. Nothing downstream of
                // `parse_filled_fields` may ever see it.
                filled_fields: parse_filled_fields(
                    &["#email".to_string(), "#password".to_string()],
                    Some(&json!([
                        {"selector": "#email", "value": "team@example.com",
                         "found": completed > 0, "value_len": 16},
                        {"selector": "#password", "value": "hunter2-secret",
                         "found": completed > 0, "redacted": true, "value_len": 14},
                    ])),
                ),
                submit_target: req.submit_action.selector().map(|s| SubmitTarget {
                    selector: s.to_string(),
                    found: self.submit_found,
                    visible: self.submit_found,
                    enabled: self.submit_found,
                    tag: Some("button".into()),
                    label: Some("Confirm".into()),
                }),
                would_submit: req.submit_action.clone(),
                dom_html: "<form>confirm</form>".into(),
                dom_bytes: "<form>confirm</form>".len(),
                dom_truncated: false,
                screenshot_path: None,
                nav_timed_out: false,
            };
            self.seen.lock().unwrap().push(req);
            Ok(evidence)
        }

        async fn commit(
            &self,
            req: TransactRequest,
            approved: ApprovedTransaction,
        ) -> pumper_core::Result<TransactReceipt> {
            let receipt = TransactReceipt {
                submitted: self.submit_found.unwrap_or(false),
                transaction_id: approved.transaction_id.clone(),
                idempotency_key: req.idempotency_key.clone(),
                profile: req.profile.clone(),
                url: req.url.clone(),
                final_url: Some(format!("{}?done=1", req.url)),
                approved_evidence_sha: approved.evidence_sha.clone(),
                observed_evidence_sha: approved.evidence_sha.clone(),
                submit_action: req.submit_action.clone(),
                submit_outcome: if self.submit_found.unwrap_or(false) {
                    StepOutcome::Ok
                } else {
                    StepOutcome::SelectorMissing
                },
                confirm_selector: req.confirm_selector.clone(),
                confirm_selector_found: req.confirm_selector.as_ref().map(|_| true),
                steps_requested: req.steps.len(),
                steps_attempted: req.steps.len(),
                steps_completed: req.steps.len(),
                step_outcomes: vec![StepOutcome::Ok; req.steps.len()],
                steps_deadline_hit: false,
                dom_html: "<p>submitted</p>".into(),
                dom_bytes: "<p>submitted</p>".len(),
                dom_truncated: false,
                nav_timed_out: false,
            };
            self.committed.lock().unwrap().push(approved);
            Ok(receipt)
        }
    }

    /// A context whose resume input is set — i.e. the attempt an approval
    /// released, as the worker would hand it back through `restore_input()`.
    async fn resumed_ctx(
        storage: &Storage,
        params: Value,
        browser: Arc<dyn Browser>,
        input: Value,
    ) -> AppContext {
        let mut ctx = ctx_with_browser(storage, params, browser).await;
        ctx.resumed_input = Some(input);
        ctx
    }

    fn submit_params() -> Value {
        let mut params = dry_run_params();
        params["submit"] = json!(true);
        params["profile"] = json!("portal_login");
        params["confirm_selector"] = json!("#thanks");
        params
    }

    async fn ctx_with_browser(
        storage: &Storage,
        params: Value,
        browser: Arc<dyn Browser>,
    ) -> AppContext {
        TestContext::new(storage, "transact")
            .params(params)
            .engines(engines_with(Arc::new(Dead), browser, Arc::new(Dead)))
            .build()
    }

    /// The v1 behaviour, inverted by N01: `submit: true` used to be a refusal
    /// before any browser work. It is now a STAGING request — the same dry run,
    /// plus a `pending` ledger row and a park. What still cannot happen is a
    /// submission: the staging run reaches `Browser::transact`, never `commit`.
    #[tokio::test]
    async fn submit_true_stages_a_pending_transaction_and_parks() {
        let store = TempStore::new("transact-stage").await;
        let browser = Arc::new(ScriptedBrowser::healthy());
        let ctx = ctx_with_browser(&store.storage, submit_params(), browser.clone()).await;
        let err = Transact.run(ctx).await.unwrap_err();

        // A park, not a failure: the worker turns this into `waiting`.
        let request = match &err {
            Error::AwaitingInput(request) => request.clone(),
            other => panic!("expected a park, got {other:?}"),
        };
        assert_eq!(request["kind"], json!("transaction_approval"));
        assert!(request["evidence"]["would_submit"]["selector"] == json!("#confirm-submit"));
        let tx_id = request["transaction_id"].as_str().unwrap().to_string();

        // The ledger holds exactly one pending row, bound to the digest of the
        // evidence the approver was just handed.
        let pool = store.storage.pool();
        let row = get(&pool, &tx_id).await.unwrap().unwrap();
        assert_eq!(row.state, TransactionState::Pending);
        assert_eq!(row.idempotency_key, "newsletter-1");
        assert_eq!(row.profile.as_deref(), Some("portal_login"));
        assert_eq!(row.evidence_sha, request["evidence_sha"].as_str().unwrap());
        assert!(row.submitted_at.is_none() && row.receipt_path.is_none());

        // The staging run performed no submission: one transact, zero commits.
        assert_eq!(browser.seen.lock().unwrap().len(), 1);
        assert!(
            browser.committed.lock().unwrap().is_empty(),
            "staging must never reach the commit path"
        );
    }

    /// The whole point of the card: one idempotency key submits at most once,
    /// ever. The second approval finds a terminal row, the app refuses to park
    /// on it again, and — critically — the engine's commit path is never
    /// entered a second time.
    #[tokio::test]
    async fn duplicate_key_not_resubmitted() {
        let store = TempStore::new("transact-once").await;
        let pool = store.storage.pool();
        let browser = Arc::new(ScriptedBrowser::healthy());

        // Stage, approve, commit.
        let err = Transact
            .run(ctx_with_browser(&store.storage, submit_params(), browser.clone()).await)
            .await
            .unwrap_err();
        let request = err.awaited_request().expect("parked").clone();
        let tx_id = request["transaction_id"].as_str().unwrap().to_string();
        assert!(mark_approved(&pool, &tx_id, Some("principal-1"))
            .await
            .unwrap());

        let out = Transact
            .run(
                resumed_ctx(
                    &store.storage,
                    submit_params(),
                    browser.clone(),
                    json!({ "transaction_id": tx_id }),
                )
                .await,
            )
            .await
            .unwrap();
        assert_eq!(out["submitted"], json!(true));
        assert_eq!(out["transaction_id"], json!(tx_id));
        assert_eq!(out["confirm_selector_found"], json!(true));
        assert_eq!(browser.committed.lock().unwrap().len(), 1);
        let row = get(&pool, &tx_id).await.unwrap().unwrap();
        assert_eq!(row.state, TransactionState::Submitted);
        assert_eq!(row.receipt_path.as_deref(), Some("receipt.json"));

        // Re-staging the same key does NOT park a second approval.
        let out = Transact
            .run(ctx_with_browser(&store.storage, submit_params(), browser.clone()).await)
            .await
            .expect("a resolved key finishes instead of parking again");
        assert_eq!(out["transaction_state"], json!("submitted"));
        assert!(out["note"].as_str().unwrap().contains("already resolved"));

        // And a second resume of the terminal row cannot reach the engine.
        let err = Transact
            .run(
                resumed_ctx(
                    &store.storage,
                    submit_params(),
                    browser.clone(),
                    json!({ "transaction_id": tx_id }),
                )
                .await,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Transact(_)), "got {err:?}");
        assert!(err.to_string().contains("not 'approved'"));
        assert_eq!(
            browser.committed.lock().unwrap().len(),
            1,
            "exactly one submission per idempotency key, ever"
        );
        assert_eq!(list(&pool, None, 10).await.unwrap().len(), 1);
    }

    /// Authority lives in the LEDGER, not in the resume payload. A job resumed
    /// by hand — `POST /jobs/{id}/resume` is an ordinary route — must not reach
    /// a live submit button just because it carries a plausible-looking input.
    #[tokio::test]
    async fn resume_without_an_approved_row_not_submitted() {
        let store = TempStore::new("transact-forged").await;
        let browser = Arc::new(ScriptedBrowser::healthy());
        // Park first, so a PENDING row exists — the strongest version of the
        // attack: everything is in place except the approval itself.
        let err = Transact
            .run(ctx_with_browser(&store.storage, submit_params(), browser.clone()).await)
            .await
            .unwrap_err();
        let tx_id = err.awaited_request().unwrap()["transaction_id"]
            .as_str()
            .unwrap()
            .to_string();

        let err = Transact
            .run(
                resumed_ctx(
                    &store.storage,
                    submit_params(),
                    browser.clone(),
                    json!({ "transaction_id": tx_id, "approval": "looks-official" }),
                )
                .await,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Transact(_)), "got {err:?}");
        assert!(err.to_string().contains("not 'approved'"));
        assert!(
            browser.committed.lock().unwrap().is_empty(),
            "a resume input is not an approval"
        );
    }

    /// A commit that came back without a submission must leave the ledger
    /// saying so. Recording `submitted` off the back of a receipt that reports
    /// `submitted: false` would put a submission nobody made into the audit
    /// trail — the one lie this ledger exists to prevent.
    #[tokio::test]
    async fn a_commit_that_did_not_submit_is_not_recorded_as_submitted() {
        let store = TempStore::new("transact-blocked").await;
        let pool = store.storage.pool();
        // This browser's submit target is missing, so its receipt says false.
        let browser = Arc::new(ScriptedBrowser::all_selectors_missing());
        let err = Transact
            .run(ctx_with_browser(&store.storage, submit_params(), browser.clone()).await)
            .await
            .unwrap_err();
        let tx_id = err.awaited_request().unwrap()["transaction_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(mark_approved(&pool, &tx_id, None).await.unwrap());

        let out = Transact
            .run(
                resumed_ctx(
                    &store.storage,
                    submit_params(),
                    browser.clone(),
                    json!({ "transaction_id": tx_id }),
                )
                .await,
            )
            .await
            .unwrap();
        assert_eq!(out["submitted"], json!(false));
        let row = get(&pool, &tx_id).await.unwrap().unwrap();
        assert_eq!(row.state, TransactionState::Approved);
        assert!(row.receipt_path.is_none());
    }

    /// The anti-pattern: a typo'd `profile` was typed `Error::Profile`, which
    /// the worker classes **retryable**, so the app that ACTS on live pages
    /// spent its whole backoff ladder on four identical refusals of a name that
    /// could never become legal. It must fail ONCE, before any browser work
    /// (`Dead::transact` panics, so reaching the engine fails this test).
    #[tokio::test]
    async fn typod_profile_refused_before_any_browser_work_and_fails_once() {
        let store = TempStore::new("transact-bad-profile").await;
        let mut params = dry_run_params();
        params["profile"] = json!("portal login"); // a space is not legal
        let ctx = ctx_with_browser(&store.storage, params, Arc::new(Dead)).await;
        let err = Transact.run(ctx).await.unwrap_err();
        assert!(matches!(err, Error::BadRequest(_)), "got {err:?}");
        assert!(
            err.is_terminal_for_job(),
            "a refusal that cannot change between attempts must not be retried: {err}"
        );
        assert!(
            err.to_string().contains("portal login"),
            "the refusal names the offending profile: {err}"
        );
    }

    #[tokio::test]
    async fn missing_idempotency_key_is_a_typed_rejection() {
        let store = TempStore::new("transact-nokey").await;
        let mut params = dry_run_params();
        params["idempotency_key"] = json!("   ");
        let ctx = ctx_with_browser(&store.storage, params, Arc::new(Dead)).await;
        let err = Transact.run(ctx).await.unwrap_err();
        assert!(matches!(err, Error::Transact(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn dry_run_threads_key_and_profile_and_saves_the_evidence_bundle() {
        let store = TempStore::new("transact-dryrun").await;
        let browser = Arc::new(ScriptedBrowser::healthy());
        let mut params = dry_run_params();
        params["profile"] = json!("portal_login");
        let ctx = ctx_with_browser(&store.storage, params, browser.clone()).await;
        let artifacts_dir = ctx.artifacts_dir.clone();
        let out = Transact.run(ctx).await.unwrap();

        // Result: dry-run, key threaded, the would-be action reported verbatim.
        assert_eq!(out["dry_run"], json!(true));
        assert_eq!(out["idempotency_key"], json!("newsletter-1"));
        assert_eq!(out["steps_completed"], json!(2));
        assert_eq!(out["would_submit"]["action"], json!("click"));
        assert_eq!(out["would_submit"]["selector"], json!("#confirm-submit"));
        // Nothing was staged: a dry run touches no ledger.
        assert!(out.get("transaction_id").is_none());
        assert_eq!(
            list(&store.storage.pool(), None, 10).await.unwrap().len(),
            0
        );

        // The engine saw the profile + key, and the submit action was carried
        // as data, never appended to the executable steps.
        let seen = browser.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].profile.as_deref(), Some("portal_login"));
        assert_eq!(seen[0].idempotency_key, "newsletter-1");
        assert!(!seen[0].submit);
        assert_eq!(seen[0].steps.len(), 2);
        assert!(
            matches!(&seen[0].submit_action, PageAction::Click { selector } if selector == "#confirm-submit")
        );

        // Evidence bundle artifacts landed.
        let evidence = std::fs::read_to_string(artifacts_dir.join("evidence.json")).unwrap();
        let evidence: Value = serde_json::from_str(&evidence).unwrap();
        assert_eq!(evidence["dry_run"], json!(true));
        assert_eq!(
            evidence["would_submit"]["selector"],
            json!("#confirm-submit")
        );
        assert_eq!(
            evidence["filled_fields"][0]["value"],
            json!("team@example.com")
        );
        let dom = std::fs::read_to_string(artifacts_dir.join("dom.html")).unwrap();
        assert_eq!(dom, "<form>confirm</form>");
    }

    /// The anti-pattern: a typo'd `"step"` (singular) passed the schema, serde
    /// silently dropped it, and the flow ran ZERO steps — then emitted a
    /// perfectly plausible landing-page evidence bundle a human might approve
    /// off. Rejected before any engine call (`Dead::transact` would panic).
    #[tokio::test]
    async fn unknown_field_not_silently_dropped() {
        let store = TempStore::new("transact-typo").await;
        let mut params = dry_run_params();
        params["step"] = params["steps"].clone();
        params["steps"] = json!([]);
        let ctx = ctx_with_browser(&store.storage, params, Arc::new(Dead)).await;
        let err = Transact.run(ctx).await.unwrap_err();
        assert!(matches!(err, Error::Transact(_)), "got {err:?}");
        assert!(
            err.to_string().contains("step"),
            "the error names the offending key: {err}"
        );
        // Deterministic => terminal: a refusal must fail ONCE, not ride the
        // backoff ladder re-deriving itself on every attempt.
        assert!(err.is_terminal_for_job());
    }

    /// Trigger-fired jobs carry a `_trigger` envelope in their params AND skip
    /// the enqueue-time schema validator, so an over-eager unknown-field
    /// rejection would break every triggered transact.
    #[tokio::test]
    async fn trigger_envelope_is_not_an_unknown_field() {
        let store = TempStore::new("transact-trigger").await;
        let mut params = dry_run_params();
        params["_trigger"] = json!({ "depth": 1, "chain": ["T1"] });
        let ctx =
            ctx_with_browser(&store.storage, params, Arc::new(ScriptedBrowser::healthy())).await;
        let out = Transact.run(ctx).await.expect("triggered flows still run");
        assert_eq!(out["dry_run"], json!(true));
    }

    /// The door, not the run: `submit: true`, a blank idempotency key and an
    /// unknown top-level key must be 422s at enqueue (the server validates this
    /// schema before a job row exists), never a job that fails minutes later.
    #[test]
    fn the_manifest_schema_closes_the_door_on_bad_params() {
        let schema = Transact.manifest().params_schema.expect("declared");
        assert!(
            schema["properties"]["submit"]["const"].is_null(),
            "submit: true is a legal staging request now — the door must not refuse it"
        );
        assert_eq!(
            schema["properties"]["submit"]["default"],
            json!(false),
            "omitting submit must still mean a plain dry run"
        );
        assert_eq!(
            schema["additionalProperties"],
            json!(false),
            "a typo'd key must not pass the door"
        );
        assert!(
            schema["patternProperties"]["^_"].is_object(),
            "host-injected `_trigger` must still pass"
        );
        assert_eq!(
            schema["properties"]["idempotency_key"]["pattern"],
            json!("\\S"),
            "an all-whitespace key must not pass"
        );
        // A typo'd profile is a 422 at the door, not a job that fails on the
        // browser tier — and the door's rule IS the engine's rule.
        assert_eq!(
            schema["properties"]["profile"]["pattern"],
            json!(profile_name_pattern()),
            "the door must enforce the engine's own profile-name rule"
        );
        // Every property the schema declares is one the request understands,
        // so `additionalProperties: false` can never reject a legal field.
        for key in schema["properties"].as_object().unwrap().keys() {
            assert!(
                TRANSACT_FIELDS.contains(&key.as_str()),
                "schema declares '{key}', which TransactRequest does not accept"
            );
        }
    }

    /// The anti-pattern, end to end: a flow that types a password republished
    /// that password into `evidence.json` on disk AND into `jobs.result` —
    /// whence every SSE subscriber, webhook payload and HMAC callback. The
    /// bundle must prove the field was filled without carrying what was typed.
    #[tokio::test]
    async fn password_value_not_republished_into_the_evidence_or_result() {
        let store = TempStore::new("transact-secret").await;
        let ctx = ctx_with_browser(
            &store.storage,
            dry_run_params(),
            Arc::new(ScriptedBrowser::healthy()),
        )
        .await;
        let artifacts_dir = ctx.artifacts_dir.clone();
        let out = Transact.run(ctx).await.unwrap();

        let result_json = serde_json::to_string(&out).unwrap();
        let bundle_raw = std::fs::read_to_string(artifacts_dir.join("evidence.json")).unwrap();
        for (surface, text) in [("job result", &result_json), ("evidence.json", &bundle_raw)] {
            assert!(
                !text.contains("hunter2-secret"),
                "{surface} republished the secret: {text}"
            );
        }

        // The reviewer still learns the field was filled, and how long it was.
        let pw = &out["filled_fields"][1];
        assert_eq!(pw["selector"], json!("#password"));
        assert_eq!(pw["found"], json!(true));
        assert_eq!(pw["redacted"], json!(true));
        assert_eq!(pw["value"], Value::Null);
        assert_eq!(pw["value_len"], json!(14));
        // Non-secret fields are untouched.
        assert_eq!(out["filled_fields"][0]["value"], json!("team@example.com"));
        assert_eq!(out["filled_fields"][0]["redacted"], json!(false));
    }

    /// The anti-pattern: a flow whose every selector 404'd produced an evidence
    /// bundle **indistinguishable** from a clean run — same `steps_completed`,
    /// no per-step outcomes, no word on whether the submit button even exists.
    /// A human approves a live submit off this bundle, so the two runs must not
    /// read the same.
    #[tokio::test]
    async fn failed_flow_evidence_not_identical_to_a_clean_flow() {
        async fn run_with(tag: &str, browser: Arc<ScriptedBrowser>) -> (Value, Value) {
            let store = TempStore::new(tag).await;
            let mut params = dry_run_params();
            params["profile"] = json!("portal_login");
            params["wait_for_selector"] = json!("#confirm-panel");
            let ctx = ctx_with_browser(&store.storage, params, browser).await;
            let artifacts_dir = ctx.artifacts_dir.clone();
            let out = Transact.run(ctx).await.unwrap();
            let bundle: Value = serde_json::from_str(
                &std::fs::read_to_string(artifacts_dir.join("evidence.json")).unwrap(),
            )
            .unwrap();
            (out, bundle)
        }

        let (good, good_bundle) =
            run_with("transact-ok", Arc::new(ScriptedBrowser::healthy())).await;
        let (bad, bad_bundle) = run_with(
            "transact-miss",
            Arc::new(ScriptedBrowser::all_selectors_missing()),
        )
        .await;

        // Same two steps requested and attempted in both runs...
        assert_eq!(good["steps"]["requested"], json!(2));
        assert_eq!(bad["steps"]["requested"], json!(2));
        assert_eq!(bad["steps"]["attempted"], json!(2));
        // ...but only successes count as completed.
        assert_eq!(good["steps_completed"], json!(2));
        assert_eq!(
            bad["steps_completed"],
            json!(0),
            "a flow whose selectors all missed completed NOTHING"
        );
        assert_eq!(
            bad["steps"]["outcomes"],
            json!(["selector_missing", "selector_missing"])
        );

        // The confirmation state and the submit target are honest, not echoed.
        assert_eq!(good["wait_for_selector_found"], json!(true));
        assert_eq!(bad["wait_for_selector_found"], json!(false));
        assert_eq!(good["submit_target"]["found"], json!(true));
        assert_eq!(bad["submit_target"]["found"], json!(false));
        // `would_submit` is identical in both — which is exactly why echoing it
        // alone could never tell the runs apart.
        assert_eq!(good["would_submit"], bad["would_submit"]);

        // The persisted bundles differ too (this is the artifact a human reads).
        assert_ne!(good_bundle["steps"], bad_bundle["steps"]);
        assert_ne!(good_bundle["submit_target"], bad_bundle["submit_target"]);
        // The identity the flow ran under, and the DOM's size, are recorded.
        assert_eq!(good_bundle["profile"], json!("portal_login"));
        assert_eq!(good_bundle["dom"]["truncated"], json!(false));
        assert_eq!(
            good_bundle["dom"]["bytes_captured"],
            json!("<form>confirm</form>".len())
        );
    }
}
