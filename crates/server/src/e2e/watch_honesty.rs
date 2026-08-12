//! Watches are the notify-on-change surface, and they could be dead on arrival
//! in both directions with no signal at all.
//!
//! - `POST /watches {app: "grants"}` **404'd**, because `grants` is a virtual
//!   namespace and not a registered app — yet `worker::notify_watches` matches
//!   watches against exactly that entry app, so the place every grant revision
//!   lands was the one place you could not watch.
//! - `{app: "ca-grants", dataset: "unified"}` was **accepted**, sat enabled
//!   forever and could never fire: ca-grants publishes its unified records into
//!   `grants`.
//! - `?app=` on `/watches` and `/triggers` took any string and answered `200`
//!   with an empty list — the unvalidated-filter anti-pattern the same file's
//!   `validate_delivery_status` exists to kill.
//! - And there was no path from a watch to its deliveries, so "did watch X ever
//!   deliver?" had no answer over the API.
//!
//! Everything here runs over the real router, and the delivery leg drives a real
//! `dataset.changed` dispatch so the `(kind, ref_id)` the query uses is the one
//! the dispatcher actually writes.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::{AppContext, Result as CoreResult, ScrapeApp};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, TestReceiver};
use crate::routes;
use crate::state::AppState;

/// A source app that publishes its records into a DIFFERENT namespace — the
/// ca-grants/`grants` shape, which is what both the false 404 and the
/// accepted-but-dead watch turn on.
struct SourceApp;

#[async_trait::async_trait]
impl ScrapeApp for SourceApp {
    fn name(&self) -> &'static str {
        "ca-grants"
    }
    fn description(&self) -> &'static str {
        "publishes into the unified grants namespace"
    }
    async fn run(&self, ctx: AppContext) -> CoreResult<Value> {
        // Its own raw dataset, under its own name...
        ctx.upsert("opportunities", "raw-1", &json!({ "n": 1 }))
            .await?;
        // ...and the unified contribution, under the VIRTUAL namespace.
        ctx.datasets
            .upsert("grants", "unified", "uni-1", &json!({ "n": 1 }))
            .await?;
        Ok(json!({
            "ok": true,
            // The declaration that puts `grants` into the run's fan-out batch.
            "index_datasets": [{ "app": "grants", "dataset": "unified" }],
        }))
    }
}

async fn post_json(router: &axum::Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

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

/// Runs one `ca-grants` job through the real worker seam, so its writes and its
/// `index_datasets` declaration reach the fan-out exactly as in production.
async fn run_source_job(state: &AppState) -> uuid::Uuid {
    let job = state
        .storage
        .enqueue("ca-grants", Default::default())
        .await
        .expect("enqueue");
    crate::worker::run_one(state).await;
    job.id
}

// ---- the namespace gate ------------------------------------------------------

/// The headline refusal, inverted. `grants` is where the records land and where
/// the fan-out matches watches, so it has to be watchable — and the source app's
/// *own* datasets stay watchable under the source app.
#[tokio::test]
async fn the_namespace_the_records_land_under_is_watchable_not_a_404() {
    let (state, _store) = test_state(vec![Arc::new(SourceApp)]).await;
    let router = routes::router(state.clone());
    run_source_job(&state).await;

    let (status, body) = post_json(
        &router,
        "/watches",
        json!({ "app": "grants", "dataset": "unified", "url": "http://127.0.0.1:1/hook" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "a watch on the virtual namespace the fan-out delivers under must be \
         creatable — this used to be a flat 404: {body}"
    );
    assert_eq!(body["app"], "grants");

    // And the source app's own namespace is unaffected.
    let (status, _) = post_json(
        &router,
        "/watches",
        json!({ "app": "ca-grants", "dataset": "opportunities", "url": "http://127.0.0.1:1/h" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}

/// The inverse bug: accepted, enabled, and structurally incapable of firing.
/// The refusal has to name the namespace to use instead, or the caller is
/// exactly as stuck as before.
#[tokio::test]
async fn a_watch_that_could_never_fire_is_refused_with_the_namespace_to_use() {
    let (state, _store) = test_state(vec![Arc::new(SourceApp)]).await;
    let router = routes::router(state.clone());
    run_source_job(&state).await;

    let (status, body) = post_json(
        &router,
        "/watches",
        json!({ "app": "ca-grants", "dataset": "unified", "url": "http://127.0.0.1:1/hook" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "ca-grants publishes `unified` into `grants`, so this watch can never \
         fire: {body}"
    );
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("grants"),
        "the refusal names where those records actually land: {msg}"
    );

    // Nothing was stored: a refused watch must not exist.
    let (_, list) = get_json(&router, "/watches").await;
    assert_eq!(list["watches"].as_array().map(Vec::len), Some(0), "{list}");
}

/// A typo still gets a 404 — the gate widened, it did not disappear — and the
/// message carries the accepted values.
#[tokio::test]
async fn an_unknown_namespace_is_still_refused_and_names_the_alternatives() {
    let (state, _store) = test_state(vec![Arc::new(SourceApp)]).await;
    let router = routes::router(state.clone());
    run_source_job(&state).await;

    let (status, body) = post_json(
        &router,
        "/watches",
        json!({ "app": "grnats", "url": "http://127.0.0.1:1/hook" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(msg.contains("grnats") && msg.contains("grants"), "{msg}");
}

// ---- the unvalidated list filters -------------------------------------------

/// `?app=` used to bind straight into `WHERE app = ?`, so a typo answered
/// `200` with an empty list — which reads as "you have no watches on that app".
#[tokio::test]
async fn a_bogus_app_filter_is_a_400_not_an_empty_200() {
    let (state, _store) = test_state(vec![Arc::new(SourceApp)]).await;
    let router = routes::router(state.clone());
    run_source_job(&state).await;

    for surface in ["/watches", "/triggers"] {
        let (status, body) = get_json(&router, &format!("{surface}?app=grnats")).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{surface} must refuse an unmatchable filter, not answer an empty \
             list: {body}"
        );
        let msg = body["error"].as_str().unwrap_or_default();
        assert!(msg.contains("grnats"), "names what was rejected: {msg}");
        assert!(msg.contains("ca-grants"), "names the way out: {msg}");

        // Real values, and the unfiltered forms, all still work.
        for ok in ["", "?app=", "?app=ca-grants", "?app=grants"] {
            let (status, _) = get_json(&router, &format!("{surface}{ok}")).await;
            assert_eq!(status, StatusCode::OK, "{surface}{ok}");
        }
    }
}

/// A trigger source is deliberately wider than a watch namespace: an `external`
/// trigger's `source_app` is an ingress source id, or `*`. Validating trigger
/// filters against the watch set alone would 400 a filter that returns rows —
/// a worse bug than the one being fixed.
#[tokio::test]
async fn a_trigger_filter_accepts_ingress_sources_not_only_app_namespaces() {
    let (state, _store) = test_state(vec![Arc::new(SourceApp)]).await;
    let router = routes::router(state.clone());
    let source = state
        .storage
        .create_ingress_source("partner-feed", "s3cret")
        .await
        .expect("create ingress source");

    let (status, body) = post_json(
        &router,
        "/triggers",
        json!({
            "source_kind": "external",
            "source_app": source.id,
            "target_app": "ca-grants",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, body) = get_json(&router, &format!("/triggers?app={}", source.id)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an ingress source id is a legitimate trigger filter: {body}"
    );
    assert_eq!(body["triggers"].as_array().map(Vec::len), Some(1), "{body}");

    // And the wildcard an external trigger may store.
    let (status, _) = get_json(&router, "/triggers?app=*").await;
    assert_eq!(status, StatusCode::OK);
}

// ---- watch → delivery traceability ------------------------------------------

/// The whole point: a real change under the virtual namespace fires the watch,
/// and the delivery is reachable FROM the watch — not just findable by status in
/// a global log with no way back to the subscription.
#[tokio::test]
async fn a_watch_can_be_traced_to_the_deliveries_it_produced() {
    let (state, _store) = test_state(vec![Arc::new(SourceApp)]).await;
    let router = routes::router(state.clone());
    let receiver = TestReceiver::spawn(vec![200]).await;

    // Seed the namespace so the watch is creatable, then subscribe to it.
    run_source_job(&state).await;
    let (status, watch) = post_json(
        &router,
        "/watches",
        json!({ "app": "grants", "dataset": "unified", "url": receiver.url() }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{watch}");
    let watch_id = watch["id"].as_str().expect("watch id").to_string();

    // Never fired yet — and the listing says so explicitly rather than looking
    // identical to a healthy watch.
    let (_, list) = get_json(&router, "/watches").await;
    assert!(
        list["watches"][0]["last_delivery"].is_null(),
        "a watch that has never delivered must be distinguishable: {list}"
    );
    let (status, empty) = get_json(&router, &format!("/watches/{watch_id}/deliveries")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty["count"], json!(0), "{empty}");

    // A second run changes the unified record, so the watch fires for real.
    let job = state
        .storage
        .enqueue(
            "ca-grants",
            pumper_core::EnqueueOptions {
                params: json!({ "cycle": 2 }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    state
        .datasets
        .upsert("grants", "unified", "uni-1", &json!({ "n": 2 }))
        .await
        .unwrap();
    crate::worker::run_one(&state).await;
    let _ = job;
    receiver.wait_hits(1, Duration::from_secs(5)).await;

    // The delivery is reachable from the watch.
    let (status, body) = get_json(&router, &format!("/watches/{watch_id}/deliveries")).await;
    assert_eq!(status, StatusCode::OK);
    let deliveries = body["deliveries"].as_array().expect("deliveries array");
    assert_eq!(
        deliveries.len(),
        1,
        "the watch's own delivery log answers 'did this ever deliver?': {body}"
    );
    assert_eq!(deliveries[0]["ref_id"], json!(watch_id));
    assert_eq!(deliveries[0]["event"], "dataset.changed");
    let delivery_id = deliveries[0]["id"].as_str().expect("delivery id");

    // ...and the enrichment now names it instead of null.
    let (_, list) = get_json(&router, "/watches").await;
    let last = &list["watches"][0]["last_delivery"];
    assert_eq!(last["id"], json!(delivery_id), "{list}");
    assert!(!last["status"].is_null() && !last["at"].is_null(), "{list}");

    // The status filter is the same vocabulary as the global delivery log, with
    // the same refusal for a value that does not exist.
    let (status, filtered) = get_json(
        &router,
        &format!("/watches/{watch_id}/deliveries?status=dead"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(filtered["count"], json!(0), "{filtered}");
    let (status, _) = get_json(
        &router,
        &format!("/watches/{watch_id}/deliveries?status=dead-letter"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// A deleted or mistyped watch id must not answer `200 {count: 0}` — "this
/// watch never delivered" is the exact wrong answer here, and it is the same
/// rule `GET /triggers/{id}/runs` already follows.
#[tokio::test]
async fn deliveries_for_an_unknown_watch_are_a_404_not_an_empty_log() {
    let (state, _store) = test_state(vec![Arc::new(SourceApp)]).await;
    let router = routes::router(state);
    let (status, body) = get_json(&router, "/watches/no-such-watch/deliveries").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}
