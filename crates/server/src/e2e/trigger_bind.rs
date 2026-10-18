//! N04 param binding and fan-out, end to end: the event has to be able to
//! steer the target job's own params, and a binding that missed has to be
//! visible where it was authored rather than as a job that fails minutes later.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::NewTrigger;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::routes;

async fn get_json(router: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Outcomes recorded against one trigger, newest first.
fn outcomes(body: &Value) -> Vec<&str> {
    body["decisions"]
        .as_array()
        .expect("decisions array")
        .iter()
        .map(|d| d["outcome"].as_str().expect("outcome string"))
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn bound_trigger(
    state: &crate::state::AppState,
    name: &str,
    source_id: &str,
    bind: Value,
    each: Option<&str>,
) -> pumper_core::Trigger {
    state
        .storage
        .create_trigger(&NewTrigger {
            name: Some(name),
            source_kind: "external",
            source_app: source_id,
            source_dataset: None,
            on_change: None,
            on_status: None,
            target_app: "fake",
            params: &json!({ "mode": "static" }),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: None,
            bind: bind.as_object(),
            each,
        })
        .await
        .expect("create trigger")
}

/// The claim N04 exists to make true: an inbound webhook payload steers the
/// target job's OWN top-level params. Before this, `merged_params` inserted
/// exactly one key (`_trigger`), so the shipped GitHub example had to hard-code
/// the URL it crawled and the event could not change it.
#[tokio::test]
async fn an_ingress_payload_steers_the_target_url() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let src = state
        .storage
        .create_ingress_source("github", "hush")
        .await
        .unwrap();
    let trigger = bound_trigger(
        &state,
        "push-to-docs",
        &src.id,
        json!({ "url": "/_trigger/payload/repository/html_url" }),
        None,
    )
    .await;

    let payload = json!({
        "ref": "refs/heads/main",
        "repository": { "html_url": "https://acme.dev/docs" }
    });
    let fired =
        crate::triggers::fire_external_triggers(&state, &src.id, &src.name, "ev-1", &payload).await;
    assert_eq!(fired, 1);

    let jobs = state
        .storage
        .jobs_by_trigger(&trigger.id, 10)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].params["url"], "https://acme.dev/docs",
        "the payload steered the target's own param: {}",
        jobs[0].params
    );
    assert_eq!(
        jobs[0].params["mode"], "static",
        "the static template still applies underneath the binding"
    );
    assert_eq!(
        jobs[0].params["_trigger"]["payload"]["ref"], "refs/heads/main",
        "the envelope is unchanged — binding LIFTS, it does not replace"
    );
}

/// The anti-pattern: a bind pointer that resolved to nothing enqueued the hop
/// anyway, so the target ran with the static template's stale value and the
/// ledger said `fired` — the one thing that was not true.
#[tokio::test]
async fn bind_miss_is_ledgered_not_enqueued() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let src = state
        .storage
        .create_ingress_source("github", "hush")
        .await
        .unwrap();
    let trigger = bound_trigger(
        &state,
        "push-to-docs",
        &src.id,
        json!({ "url": "/_trigger/payload/repository/html_url" }),
        None,
    )
    .await;

    // A payload from the same source that simply does not carry the field.
    let payload = json!({ "ref": "refs/heads/main", "zen": "keep it logically awesome" });
    let fired =
        crate::triggers::fire_external_triggers(&state, &src.id, &src.name, "ev-1", &payload).await;
    assert_eq!(fired, 0, "a binding that did not bind must not enqueue");

    let jobs = state
        .storage
        .jobs_by_trigger(&trigger.id, 10)
        .await
        .unwrap();
    assert!(jobs.is_empty(), "no hop exists: {jobs:?}");

    let router = routes::router(state);
    let (status, body) = get_json(&router, &format!("/triggers/{}/runs", trigger.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(outcomes(&body), vec!["bind_miss"]);
    let detail = body["decisions"][0]["detail"].as_str().unwrap_or_default();
    assert!(
        detail.contains("/_trigger/payload/repository/html_url"),
        "the ledger names the pointer that missed: {detail}"
    );
    assert!(
        detail.contains("url"),
        "…and the param it was filling: {detail}"
    );
}

/// One event, one job per record it carries — capped, and the cap declared in
/// every hop's envelope rather than left to be inferred from a hop count the
/// target cannot see.
#[tokio::test]
async fn each_fans_out_capped_and_says_so() {
    let (mut state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    {
        // A cap small enough to bite, so the truncation path is the one under
        // test rather than an untested branch behind a default of 50.
        let cfg = Arc::make_mut(&mut state.config);
        cfg.triggers.fan_out_cap = 2;
    }
    let src = state
        .storage
        .create_ingress_source("airtable", "hush")
        .await
        .unwrap();
    let trigger = bound_trigger(
        &state,
        "row-per-job",
        &src.id,
        json!({ "url": "/_trigger/item/url" }),
        Some("/_trigger/payload/records"),
    )
    .await;

    let payload = json!({ "records": [
        { "url": "https://a.example" },
        { "url": "https://b.example" },
        { "url": "https://c.example" }
    ] });
    let fired =
        crate::triggers::fire_external_triggers(&state, &src.id, &src.name, "ev-1", &payload).await;
    assert_eq!(fired, 2, "one hop per element, capped at fan_out_cap");

    let jobs = state
        .storage
        .jobs_by_trigger(&trigger.id, 10)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 2);
    let mut urls: Vec<&str> = jobs
        .iter()
        .map(|j| j.params["url"].as_str().unwrap())
        .collect();
    urls.sort_unstable();
    assert_eq!(urls, vec!["https://a.example", "https://b.example"]);
    for job in &jobs {
        assert_eq!(
            job.params["_trigger"]["fan_out_total"], 3,
            "the total stays EXACT while the hop list is capped"
        );
        assert_eq!(job.params["_trigger"]["fan_out_truncated"], true);
    }
    let mut indexes: Vec<i64> = jobs
        .iter()
        .map(|j| j.params["_trigger"]["item_index"].as_i64().unwrap())
        .collect();
    indexes.sort_unstable();
    assert_eq!(indexes, vec![0, 1], "each hop knows which element it is");

    // Redelivery of the same event id creates nothing new: the per-element key
    // (`…:i:{index}`) dedups per element, not per batch.
    let again =
        crate::triggers::fire_external_triggers(&state, &src.id, &src.name, "ev-1", &payload).await;
    assert_eq!(again, 0);
    let jobs = state
        .storage
        .jobs_by_trigger(&trigger.id, 10)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 2, "still two hops, not four");
}

/// An `each` array with no elements is zero hops — recorded, because "nothing
/// fired" with no ledger row at all is the exact silence this ledger ends.
#[tokio::test]
async fn an_empty_fan_out_array_is_recorded_rather_than_silent() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let src = state
        .storage
        .create_ingress_source("airtable", "hush")
        .await
        .unwrap();
    let trigger = bound_trigger(
        &state,
        "row-per-job",
        &src.id,
        Value::Null,
        Some("/_trigger/payload/records"),
    )
    .await;
    let fired = crate::triggers::fire_external_triggers(
        &state,
        &src.id,
        &src.name,
        "ev-1",
        &json!({ "records": [] }),
    )
    .await;
    assert_eq!(fired, 0);
    let router = routes::router(state);
    let (_, body) = get_json(&router, &format!("/triggers/{}/runs", trigger.id)).await;
    assert_eq!(outcomes(&body), vec!["fan_out_empty"]);
}

/// Pointer syntax is decidable at create time, so it is decided there — a
/// dotted `$.path` (the FILTER grammar) would otherwise present as a
/// `bind_miss` on every event forever, with nothing saying the syntax was the
/// problem rather than the data.
#[tokio::test]
async fn the_create_door_refuses_a_pointer_that_is_not_a_pointer() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let router = routes::router(state);
    let post = |body: Value| {
        let router = router.clone();
        async move {
            let resp = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/triggers")
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = resp.status();
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            (
                status,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        }
    };
    let base = json!({
        "source_kind": "external",
        "source_app": "*",
        "target_app": "fake",
    });

    let mut bad = base.clone();
    bad["bind"] = json!({ "url": "payload.url" });
    let (status, body) = post(bad).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("JSON pointer"),
        "{body}"
    );

    // `_trigger` is the envelope the cycle guards live in; a bind may not
    // overwrite it, for the same reason a sandboxed transform may not.
    let mut reserved = base.clone();
    reserved["bind"] = json!({ "_trigger": "/x" });
    let (status, body) = post(reserved).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("_trigger"),
        "{body}"
    );

    // `each` on a terminal-job trigger: that envelope is a fixed scalar
    // summary, so the pointer could only ever read the static template.
    let job_each = json!({
        "source_kind": "job",
        "source_app": "fake",
        "target_app": "fake",
        "each": "/_trigger/keys",
    });
    let (status, body) = post(job_each).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"].as_str().unwrap_or_default().contains("each"),
        "{body}"
    );

    // The good one is accepted.
    let mut ok = base;
    ok["bind"] = json!({ "url": "/_trigger/payload/url" });
    let (status, _) = post(ok).await;
    assert_eq!(status, StatusCode::CREATED);
}
