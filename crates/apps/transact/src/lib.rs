//! Transact (M06, v1 slice): execute a declarative browser flow **dry-run
//! only** — navigate, fill, click, wait, up to the final confirmation state —
//! then STOP before the irreversible submit and emit an evidence bundle
//! (DOM snapshot, filled-field summary, the exact would-be action) for human
//! review.
//!
//! ## What this slice deliberately does NOT do
//!
//! - It never performs the irreversible action. `submit_action` is a separate
//!   field the executor has no code path to run; stop-before-submit is
//!   structural, not a flag check.
//! - `submit: true` is REJECTED with a typed `Error::Transact` before any
//!   browser work: live submission requires the human-approval design
//!   (pending-approval transactions + `POST /transactions/{id}/approve` + a
//!   `transactions` table deduping on `idempotency_key`) — the documented next
//!   slice.
//!
//! The idempotency key and session profile are threaded through NOW so the
//! seam is right: the key is recorded with every evidence bundle, and the
//! profile binds the flow to a vault identity exactly like a render.
//!
//! ## Secrets
//!
//! The filled-field summary proves a field was filled without republishing what
//! was typed into it: password inputs (and credential/card `autocomplete`
//! fields) are masked **in the page** by `pumper_core::filled_fields_js`, so the
//! plaintext never reaches this process, `evidence.json`, `jobs.result`, an SSE
//! event or a webhook payload. Only `{found, redacted, value_len}` survives.
//!
//! The job's **params** are a different matter: they still hold whatever the
//! caller put in a `type` step's `text`, because that is the job model's
//! storage posture for every app (params are persisted verbatim). Redacting
//! them is a job-model change, not a transact one.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, ManifestExample, Result, ScrapeApp, TransactRequest,
};
use serde_json::{json, Value};

pub struct Transact;

#[async_trait]
impl ScrapeApp for Transact {
    fn name(&self) -> &'static str {
        "transact"
    }

    fn description(&self) -> &'static str {
        "Execute a declarative browser flow DRY-RUN ONLY: steps (fill/click/wait) run to the \
         final confirmation state, then the flow stops BEFORE the irreversible submit_action \
         and emits an evidence bundle (evidence.json + dom.html artifacts: DOM snapshot, \
         filled-field values, the exact would-be action). submit:true is rejected — live \
         submission needs the human-approval slice. Params: {\"url\": \"...\", \
         \"idempotency_key\": \"...\", \"steps\": [PageAction...], \"submit_action\": \
         PageAction, \"profile\": \"vault-profile\", \"submit\": false}"
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "type": "object",
                "required": ["url", "idempotency_key", "submit_action"],
                "properties": {
                    "url": { "type": "string", "description": "Page the flow starts on." },
                    "idempotency_key": {
                        "type": "string", "minLength": 1,
                        "description": "Caller-chosen key recorded with the evidence bundle; \
                                        will dedup live submissions in the next slice."
                    },
                    "profile": {
                        "type": "string",
                        "description": "Session-vault profile to act under (logins/cookies)."
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
                                        evidence as would_submit, NEVER executed in this slice."
                    },
                    "submit": {
                        "type": "boolean", "default": false,
                        "description": "Must be false: true is rejected until the \
                                        human-approval slice exists."
                    },
                    "wait_for_selector": { "type": "string" },
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
                 artifacts: {evidence: \"evidence.json\", dom: \"dom.html\"}, next_slice: \"...\"}",
            ),
            // Browser-only by design: the flow never escalates to a metered
            // engine, so per CostClass's own contract runs are Free.
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let req: TransactRequest = serde_json::from_value(ctx.params.clone())?;
        // Reject before ANY browser work: submit:true, empty idempotency key,
        // and bad profile names are typed errors, not partial executions. The
        // engine re-validates too (defense in depth).
        req.validate()?;

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
        // The steps block is the bundle's honesty core: requested / attempted /
        // completed are three different numbers, and only `completed` counts
        // steps that actually worked. A flow whose selectors all missed reports
        // `completed: 0` with `outcomes: ["selector_missing", ...]`.
        let steps = json!({
            "requested": evidence.steps_requested,
            "attempted": evidence.steps_attempted,
            "completed": evidence.steps_completed,
            "deadline_hit": evidence.steps_deadline_hit,
            "outcomes": evidence.step_outcomes,
        });
        let bundle = json!({
            "generated_at": chrono::Utc::now().to_rfc3339(),
            "dry_run": evidence.dry_run,
            "idempotency_key": evidence.idempotency_key,
            "profile": evidence.profile,
            "url": evidence.url,
            "final_url": evidence.final_url,
            "steps": steps.clone(),
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
        });
        ctx.save_artifact(
            "evidence.json",
            serde_json::to_vec_pretty(&bundle)?.as_slice(),
        )
        .await?;

        Ok(json!({
            "dry_run": evidence.dry_run,
            "idempotency_key": evidence.idempotency_key,
            "profile": evidence.profile,
            "url": evidence.url,
            "final_url": evidence.final_url,
            "steps": steps,
            "steps_completed": evidence.steps_completed,
            "wait_for_selector_found": evidence.wait_for_selector_found,
            "filled_fields": evidence.filled_fields,
            "would_submit": evidence.would_submit,
            "submit_target": evidence.submit_target,
            "dom_truncated": evidence.dom_truncated,
            "nav_timed_out": evidence.nav_timed_out,
            "artifacts": { "evidence": "evidence.json", "dom": "dom.html" },
            "next_slice": "live submission requires human approval: pending-approval \
                           transactions + an explicit approve endpoint, deduped on \
                           idempotency_key (not yet implemented)",
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumper_core::engine::{parse_filled_fields, StepOutcome, SubmitTarget};
    use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
    use pumper_core::{
        Browser, Error, PageAction, RenderRequest, RenderedPage, Storage, TransactEvidence,
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
        outcomes: Vec<StepOutcome>,
        submit_found: Option<bool>,
    }

    impl ScriptedBrowser {
        /// Every step succeeded and the submit button is on the page.
        fn healthy() -> Self {
            Self {
                seen: std::sync::Mutex::new(Vec::new()),
                outcomes: vec![StepOutcome::Ok, StepOutcome::Ok],
                submit_found: Some(true),
            }
        }

        /// Every selector missed and the submit target is nowhere to be seen.
        fn all_selectors_missing() -> Self {
            Self {
                seen: std::sync::Mutex::new(Vec::new()),
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

    #[tokio::test]
    async fn submit_true_is_rejected_before_any_browser_work() {
        let store = TempStore::new("transact-reject").await;
        let mut params = dry_run_params();
        params["submit"] = json!(true);
        // Dead engines: reaching the browser at all would panic the test —
        // proving the rejection happens BEFORE any engine call.
        let ctx = ctx_with_browser(&store.storage, params, Arc::new(Dead)).await;
        let err = Transact.run(ctx).await.unwrap_err();
        assert!(
            matches!(err, Error::Transact(_)),
            "typed rejection, got {err:?}"
        );
        assert!(
            err.to_string().contains("human-approval"),
            "error must point at the approval design: {err}"
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
        assert!(out["next_slice"]
            .as_str()
            .unwrap()
            .contains("human approval"));

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
