//! The obligations the engine **traits** carry, independent of any engine.
//!
//! The cross-engine battery lives in `crates/server/src/e2e/engine_conformance.rs`
//! (core depends on no engine crate, so it cannot see the implementors). What
//! belongs *here* is the contract the traits define on their own: the default
//! method bodies every non-opted-in engine inherits, and the shared harness's
//! `Dead` engines.

use async_trait::async_trait;
use pumper_core::engine::unsupported_transact;
use pumper_core::testing::Dead;
use pumper_core::{
    Browser, Error, HttpClient, HttpRequest, RenderRequest, RenderedPage, Result, TransactEvidence,
    TransactRequest,
};

/// A `Browser` that implements nothing but `render` — every wrapper, mock and
/// future engine that has not opted into flows.
struct NoFlows;

#[async_trait]
impl Browser for NoFlows {
    async fn render(&self, _req: RenderRequest) -> Result<RenderedPage> {
        Err(Error::Browser("not under test".into()))
    }
}

fn flow() -> TransactRequest {
    TransactRequest {
        url: "https://example.test/apply".into(),
        profile: None,
        steps: Vec::new(),
        submit_action: pumper_core::PageAction::Click {
            selector: "#submit".into(),
        },
        submit: false,
        idempotency_key: "contract-probe".into(),
        wait_for_selector: None,
        extra_wait_ms: None,
        max_body_bytes: None,
    }
}

/// THE retry-class bug. `Browser::transact`'s default returned `Error::Browser`,
/// which `is_terminal_for_job` classes retryable — so a job that reached an
/// engine with no flow support was re-queued with backoff and re-reached the
/// identical refusal on every attempt.
///
/// Which engine is behind the trait object is fixed for the life of the job, so
/// the second attempt can never learn anything the first did not. The trait's
/// doc said it should "fail loudly"; it failed loudly four times and billed for
/// the privilege.
#[tokio::test]
async fn an_unsupported_flow_fails_once_instead_of_riding_the_retry_ladder() {
    let err = NoFlows
        .transact(flow())
        .await
        .expect_err("an engine without flow support must refuse");
    assert!(
        err.is_terminal_for_job(),
        "the refusal came back retryable: {err}"
    );
    assert!(matches!(err, Error::Transact(_)), "got {err:?}");
}

/// The refusal has ONE producer, so a wrapper that wants to decline explicitly
/// cannot mint a *retryable* version of the same sentence by hand — which is
/// precisely how the default drifted out of its own retry class.
#[test]
fn the_flow_refusal_has_one_producer_and_it_is_terminal() {
    let err = unsupported_transact("https://example.test/x");
    assert!(err.is_terminal_for_job());
    assert!(
        err.to_string().contains("https://example.test/x"),
        "the refusal must still name what was refused: {err}"
    );
}

/// A failure *during* a flow stays retryable — the boundary `Error::Transact`
/// documents. Widening terminality to every browser failure would take away the
/// retries that make a flaky render survivable.
#[test]
fn a_failure_during_a_flow_is_still_retryable() {
    assert!(!Error::Browser("chrome died mid-flow".into()).is_terminal_for_job());
}

/// `Dead`'s contract is that **any** engine call is a test bug. `transact` was
/// given an override for exactly this reason, with the hazard written down —
/// and `fetch_bytes`, the sibling default one trait over, was not. A write-path
/// test that accidentally reached it got a plausible `Err` instead of the
/// intended panic, which is indistinguishable from the refusal such a test is
/// usually asserting on.
#[tokio::test]
#[should_panic(expected = "no binary fetching in a write-path test")]
async fn dead_panics_on_a_binary_fetch_like_every_other_engine_call() {
    let _ = Dead
        .fetch_bytes(HttpRequest::get("https://example.test/x"))
        .await;
}

/// The sibling that already had the override, kept in the same file so the pair
/// is visibly a pair: if a third default-bodied method is ever added to these
/// traits, this is the list it has to join.
#[tokio::test]
#[should_panic(expected = "no transacting in a write-path test")]
async fn dead_panics_on_a_transact_like_every_other_engine_call() {
    let _: Result<TransactEvidence> = Dead.transact(flow()).await;
}
