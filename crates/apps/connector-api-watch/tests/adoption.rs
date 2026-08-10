//! Seam adoption for the connector docs watcher: durable-execution resume (M23)
//! and the M12 provenance stamp on the `connector_docs` write.
//!
//! Both tests run with the harness's panicking engines, which is the point: a
//! connector the checkpoint says is already done must not be fetched again, and
//! a run that fetches nothing must still report the prior attempt's findings.

use app_connector_api_watch::ConnectorApiWatch;
use pumper_core::testing::{TempStore, TestContext};
use pumper_core::{Provenance, ScrapeApp};
use serde_json::{json, Value};

/// Writes a watch-list manifest into the temp dir and returns its path.
async fn manifest(store: &TempStore, connectors: Value) -> String {
    let path = store.path().join("connector-docs.json");
    tokio::fs::write(&path, json!({ "connectors": connectors }).to_string())
        .await
        .unwrap();
    path.to_string_lossy().into_owned()
}

#[tokio::test]
async fn a_checkpointed_connector_is_never_re_fetched_or_re_summarized() {
    let store = TempStore::new("caw-resume").await;
    let list = manifest(
        &store,
        json!([{ "slug": "stripe", "label": "Stripe", "docs_url": "https://x.test/stripe" }]),
    )
    .await;

    // The prior attempt finished `stripe` (and paid Claude for its summary).
    // With Dead engines, any re-fetch panics — so a passing run proves the
    // resume actually skipped the connector rather than merely re-deriving the
    // same answer.
    let ctx = TestContext::new(&store.storage, "connector-api-watch")
        .params(json!({ "manifest": list, "summarize": true }))
        .restored(json!({
            "v": 1,
            "done": ["stripe"],
            "changes": [{ "connector": "stripe", "summary": "auth moved to OAuth" }],
            "errors": [],
        }))
        .build();
    let out = ConnectorApiWatch.run(ctx).await.unwrap();

    assert_eq!(out["resumed_from_checkpoint"], true);
    assert_eq!(
        out["scanned"], 1,
        "the done connector still counts as scanned"
    );
    // The prior attempt's findings are carried into this attempt's changes.json
    // hand-off — a resume must not silently drop events it already paid for.
    assert_eq!(out["changed"], 1);
    assert_eq!(out["changes"][0]["connector"], "stripe");
}

#[tokio::test]
async fn an_unusable_checkpoint_restarts_the_sweep_rather_than_erroring() {
    let store = TempStore::new("caw-poison").await;
    // Empty watch list: a fresh sweep does nothing and must not error, which is
    // what distinguishes "restarted" from "resumed" here.
    let list = manifest(&store, json!([])).await;

    let ctx = TestContext::new(&store.storage, "connector-api-watch")
        .params(json!({ "manifest": list }))
        .restored(json!({ "v": 99, "done": ["stripe"] }))
        .build();
    let out = ConnectorApiWatch.run(ctx).await.unwrap();
    assert_eq!(out["resumed_from_checkpoint"], false);
    assert_eq!(out["scanned"], 0);
}

/// The provenance contract this app's single-record write can honestly state:
/// the docs URL it fetched and the sha256 of the document it stored. No RuleSet
/// is involved, so `rules_hash` must stay Null rather than be invented.
#[test]
fn the_stamp_states_only_what_a_doc_fetch_knows() {
    let prov = Provenance {
        source_url: Some("https://docs.stripe.com/api".into()),
        artifact_sha: Some("a".repeat(64)),
        ..Provenance::default()
    };
    assert!(!prov.is_empty());
    assert!(
        prov.rules_hash.is_none(),
        "no ruleset produced this record — the pin must not be fabricated"
    );
    assert!(
        !prov.replayable(),
        "replayable requires BOTH an archived body and a ruleset pin"
    );
}
