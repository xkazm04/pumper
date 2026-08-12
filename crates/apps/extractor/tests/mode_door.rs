//! The enqueue door and the app must refuse the SAME params objects.
//!
//! `extractor` carries four mode roots (`replay`, `induce`, `rules`+`urls`,
//! `rules`+`source`) and used to execute whichever one won a fixed precedence
//! order, returning `200` for the rest. Two layers now say no:
//!
//! 1. the manifest's params schema, compiled by the server's shared enqueue
//!    check (`mcp::validate_app_params` → `jsonschema::validator_for`) — this
//!    file compiles it with the same crate, so "the door 422s it" is a measured
//!    fact rather than a reading of the JSON;
//! 2. `resolve_run_mode` inside the app, for the paths that reach `run()`
//!    without passing a door (direct calls, an app invoked as a library).
//!
//! Both must agree, or a job refused at one layer and accepted at the other is
//! a door that depends on which entrance you used.

use app_extractor::Extractor;
use pumper_core::ScrapeApp;
use serde_json::{json, Value};

/// The validator the server builds — same crate, same schema, same draft.
fn admits(params: &Value) -> bool {
    let schema = Extractor.manifest().params_schema.expect("params schema");
    let validator = jsonschema::validator_for(&schema)
        .expect("the manifest schema must compile — an unusable schema is skipped, i.e. no door");
    validator.is_valid(params)
}

/// Runs the real app far enough to see whether it accepted the mode. The app is
/// never given a store here: a params object that survives mode resolution
/// fails later for its own reasons, and that is not what this asserts.
async fn app_refuses_the_mode(params: Value) -> bool {
    let store = pumper_core::testing::TempStore::new("extract-mode-door").await;
    let ctx = pumper_core::testing::TestContext::new(&store.storage, "extractor")
        .params(params)
        .build();
    match Extractor.run(ctx).await {
        Err(e) => e.to_string().contains("conflicting extractor modes"),
        Ok(_) => false,
    }
}

fn rules() -> Value {
    json!({ "title": { "type": "css", "selector": "h1" } })
}

fn root(name: &str) -> Value {
    match name {
        "rules" => rules(),
        "urls" => json!(["https://example.test/a"]),
        "source" => json!({ "app": "crawl", "dataset": "pages" }),
        "replay" => json!({ "rules": rules() }),
        "induce" => json!({ "url_pattern": "^https://example\\.test/" }),
        other => panic!("unknown mode root {other}"),
    }
}

#[test]
fn the_door_admits_each_legal_mode() {
    // The control arm: without it, a schema that refuses EVERYTHING would pass
    // the exclusivity assertions below while breaking the app entirely.
    assert!(admits(&json!({ "replay": root("replay") })), "replay mode");
    assert!(admits(&json!({ "induce": root("induce") })), "induce mode");
    assert!(
        admits(&json!({ "rules": rules(), "urls": root("urls") })),
        "urls mode"
    );
    assert!(
        admits(&json!({ "rules": rules(), "source": root("source") })),
        "source mode"
    );
    // Mode-neutral params ride along with any of them.
    assert!(
        admits(&json!({
            "rules": rules(), "source": root("source"),
            "dataset": "extracted", "concurrency": 8, "strategy": "http",
            "_trigger": { "keys": ["https://example.test/a"] }
        })),
        "a real trigger-fired source job"
    );
}

#[tokio::test]
async fn the_door_refuses_every_conflicting_pair_the_app_refuses() {
    let pairs = [
        ("replay", "induce"),
        ("replay", "rules"),
        ("replay", "urls"),
        ("replay", "source"),
        ("induce", "rules"),
        ("induce", "urls"),
        ("induce", "source"),
        ("urls", "source"),
    ];
    for (a, b) in pairs {
        // `urls`+`source` is only reachable as a write job, which needs rules;
        // the other pairs are conflicts with or without it.
        let params = if (a, b) == ("urls", "source") {
            json!({ "rules": rules(), "urls": root("urls"), "source": root("source") })
        } else {
            json!({ a: root(a), b: root(b) })
        };
        assert!(
            !admits(&params),
            "the enqueue door must 422 `{a}` + `{b}`: {params}"
        );
        assert!(
            app_refuses_the_mode(params.clone()).await,
            "the app must refuse `{a}` + `{b}` too: {params}"
        );
    }
}

#[test]
fn the_door_refuses_the_silent_replay_that_looked_like_a_write() {
    // The exact object from the finding: a caller asks for an extraction AND
    // pastes a replay block. It used to validate, enqueue, and run read-only.
    let params = json!({
        "rules": rules(),
        "urls": root("urls"),
        "replay": root("replay"),
    });
    assert!(!admits(&params), "three roots, one job: {params}");
}

#[test]
fn a_write_job_with_no_input_list_is_refused_at_the_door() {
    // `rules` alone reached the worker and failed there with "param 'urls' must
    // be a non-empty array". The shape is knowable at enqueue time, so it is a
    // 422 now — the failure moved from a burnt job attempt to the request.
    assert!(!admits(&json!({ "rules": rules() })));
    // ...and an input list with no rules is equally incomplete.
    assert!(!admits(&json!({ "urls": root("urls") })));
    assert!(!admits(&json!({ "source": root("source") })));
}

#[test]
fn the_concurrency_ceiling_is_a_door_refusal_not_a_silent_rewrite() {
    assert!(admits(
        &json!({ "rules": rules(), "urls": root("urls"), "concurrency": 64 })
    ));
    assert!(!admits(
        &json!({ "rules": rules(), "urls": root("urls"), "concurrency": 65 })
    ));
    assert!(!admits(
        &json!({ "rules": rules(), "urls": root("urls"), "concurrency": 0 })
    ));
}
