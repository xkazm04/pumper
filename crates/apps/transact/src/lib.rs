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
                "{dry_run: true, idempotency_key, url, final_url, steps_completed, \
                 filled_fields: [{selector, value, found}], would_submit: PageAction, \
                 nav_timed_out, artifacts: {evidence: \"evidence.json\", dom: \"dom.html\"}, \
                 next_slice: \"...\"}",
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
        ctx.meter("browser", Some(&evidence.url), 0.0, Some("transact_dry_run"))
            .await;

        // Big payloads to artifacts (repo convention): the DOM snapshot and the
        // full evidence bundle live beside the job, not inside jobs.result.
        ctx.save_artifact("dom.html", evidence.dom_html.as_bytes())
            .await?;
        let bundle = json!({
            "generated_at": chrono::Utc::now().to_rfc3339(),
            "dry_run": evidence.dry_run,
            "idempotency_key": evidence.idempotency_key,
            "url": evidence.url,
            "final_url": evidence.final_url,
            "steps_completed": evidence.steps_completed,
            "filled_fields": evidence.filled_fields,
            "would_submit": evidence.would_submit,
            "screenshot_path": evidence.screenshot_path,
            "nav_timed_out": evidence.nav_timed_out,
            "dom_artifact": "dom.html",
        });
        ctx.save_artifact("evidence.json", serde_json::to_vec_pretty(&bundle)?.as_slice())
            .await?;

        Ok(json!({
            "dry_run": evidence.dry_run,
            "idempotency_key": evidence.idempotency_key,
            "url": evidence.url,
            "final_url": evidence.final_url,
            "steps_completed": evidence.steps_completed,
            "filled_fields": evidence.filled_fields,
            "would_submit": evidence.would_submit,
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
    struct ScriptedBrowser {
        seen: std::sync::Mutex<Vec<TransactRequest>>,
    }

    #[async_trait]
    impl Browser for ScriptedBrowser {
        async fn render(&self, _: RenderRequest) -> pumper_core::Result<RenderedPage> {
            panic!("transact app must call Browser::transact, not render")
        }

        async fn transact(&self, req: TransactRequest) -> pumper_core::Result<TransactEvidence> {
            req.validate()?;
            let evidence = TransactEvidence {
                dry_run: true,
                idempotency_key: req.idempotency_key.clone(),
                url: req.url.clone(),
                final_url: Some(format!("{}?step=confirm", req.url)),
                steps_completed: req.steps.len(),
                filled_fields: vec![pumper_core::FilledField {
                    selector: "#email".into(),
                    value: Some("team@example.com".into()),
                    found: true,
                }],
                would_submit: req.submit_action.clone(),
                dom_html: "<form>confirm</form>".into(),
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
        assert!(matches!(err, Error::Transact(_)), "typed rejection, got {err:?}");
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
        let browser = Arc::new(ScriptedBrowser {
            seen: std::sync::Mutex::new(Vec::new()),
        });
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
        assert!(out["next_slice"].as_str().unwrap().contains("human approval"));

        // The engine saw the profile + key, and the submit action was carried
        // as data, never appended to the executable steps.
        let seen = browser.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].profile.as_deref(), Some("portal_login"));
        assert_eq!(seen[0].idempotency_key, "newsletter-1");
        assert!(!seen[0].submit);
        assert_eq!(seen[0].steps.len(), 2);
        assert!(matches!(&seen[0].submit_action, PageAction::Click { selector } if selector == "#confirm-submit"));

        // Evidence bundle artifacts landed.
        let evidence = std::fs::read_to_string(artifacts_dir.join("evidence.json")).unwrap();
        let evidence: Value = serde_json::from_str(&evidence).unwrap();
        assert_eq!(evidence["dry_run"], json!(true));
        assert_eq!(evidence["would_submit"]["selector"], json!("#confirm-submit"));
        assert_eq!(evidence["filled_fields"][0]["value"], json!("team@example.com"));
        let dom = std::fs::read_to_string(artifacts_dir.join("dom.html")).unwrap();
        assert_eq!(dom, "<form>confirm</form>");
    }
}
