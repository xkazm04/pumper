//! The Czech labour products (`cz-labour/salary_gap`, `salary_nowcast`,
//! `vacancy_lifecycle`) had twelve datasets and zero discovery: neither MPSV app
//! declared `index_datasets`, so nothing about the labour namespace was
//! reachable from the outside.
//!
//! The half that is easy to see is search — no per-record document, so no saved
//! search and no alert. The half that matters more is the **hook batch**:
//! `worker::load_run_changes` is scoped by `run_indexed_apps`, i.e. the job's own
//! app plus the virtual apps its result declares. `mpsv-vpm` writes the trio
//! through `ctx.datasets` under `cz-labour`, a namespace it never declared — so
//! those revisions were never even LOADED after a run. No watch and no dataset
//! trigger on `cz-labour` could fire, whatever the operator subscribed to.
//!
//! Everything here runs the real router, the real worker seam and the real
//! `notify_watches` dispatch. The producer is a stand-in for `mpsv-vpm`'s run
//! (the real one needs a 188 MB live fetch, which this suite does not do), but
//! its declaration and its write targets are the REAL app's:
//! [`app_mpsv_vpm::INDEXED_DATASETS`] and
//! [`app_mpsv_vpm::index_datasets_spec`]. If the app stops declaring
//! `cz-labour`, these tests stop passing.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::{AppContext, Result as CoreResult, ScrapeApp};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state_indexed, TestReceiver};
use crate::routes;
use crate::state::AppState;

/// The record key every dataset below is written under — one cell, so the
/// assertions are about reachability, not about volume.
const CELL: &str = "5223|MZDOVA";

/// Stands in for an `mpsv-vpm` run: writes one record into **each dataset the
/// real app declares**, and returns the real app's own `index_datasets` value.
struct LabourProducer;

#[async_trait::async_trait]
impl ScrapeApp for LabourProducer {
    fn name(&self) -> &'static str {
        "mpsv-vpm"
    }

    fn description(&self) -> &'static str {
        "writes the cz-labour products the way the real app does"
    }

    async fn run(&self, ctx: AppContext) -> CoreResult<Value> {
        let cycle = ctx.params.get("cycle").and_then(Value::as_i64).unwrap_or(1);
        for (app, dataset) in app_mpsv_vpm::INDEXED_DATASETS {
            ctx.datasets
                .upsert(
                    app,
                    dataset,
                    CELL,
                    &json!({
                        "title": format!("CZ-ISCO 5223 MZDOVA — {dataset} cycle {cycle}"),
                        "cycle": cycle,
                    }),
                )
                .await?;
        }
        Ok(json!({
            "ok": true,
            "index_datasets": app_mpsv_vpm::index_datasets_spec(),
        }))
    }
}

/// A `Search` that keeps every document it was handed — the only way to assert
/// that the per-record docs really carry the app's titles rather than being
/// dropped somewhere between the result and the index.
#[derive(Default)]
struct CapturingSearch {
    docs: Mutex<Vec<pumper_core::SearchDoc>>,
}

#[async_trait::async_trait]
impl pumper_core::Search for CapturingSearch {
    async fn index(&self, docs: Vec<pumper_core::SearchDoc>) -> CoreResult<()> {
        self.docs.lock().expect("docs lock").extend(docs);
        Ok(())
    }
    async fn query(
        &self,
        _: pumper_core::SearchRequest,
    ) -> CoreResult<pumper_core::SearchResponse> {
        Ok(pumper_core::SearchResponse::default())
    }
    async fn delete_ids(&self, _: &[String]) -> CoreResult<()> {
        Ok(())
    }
    async fn delete_dataset(&self, _: &str, _: &str) -> CoreResult<()> {
        Ok(())
    }
    async fn doc_count(&self) -> CoreResult<u64> {
        Ok(self.docs.lock().expect("docs lock").len() as u64)
    }
}

async fn labour_state() -> (
    AppState,
    pumper_core::testing::TempStore,
    Arc<CapturingSearch>,
) {
    let search = Arc::new(CapturingSearch::default());
    let (state, store) =
        test_state_indexed(vec![Arc::new(LabourProducer)], search.clone(), |_| {}).await;
    (state, store, search)
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

/// Runs one producer job through the real worker seam, so its writes and its
/// declaration reach the fan-out exactly as in production.
async fn run_cycle(state: &AppState, cycle: i64) {
    state
        .storage
        .enqueue(
            "mpsv-vpm",
            pumper_core::EnqueueOptions {
                params: json!({ "cycle": cycle }),
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");
    crate::worker::run_one(state).await;
}

/// The headline: a watch on the shared labour namespace is creatable AND fires.
/// Before the declaration existed the second half was structurally impossible —
/// `load_run_changes` never loaded a `cz-labour` revision, so the watch sat
/// enabled forever with nothing to match against.
#[tokio::test]
async fn a_watch_on_the_cz_labour_namespace_is_creatable_and_actually_fires() {
    let (state, _store, _search) = labour_state().await;
    let router = routes::router(state.clone());
    let receiver = TestReceiver::spawn(vec![200]).await;

    // Cycle 1 seeds the namespace, which is what makes it nameable.
    run_cycle(&state, 1).await;

    let (status, watch) = post_json(
        &router,
        "/watches",
        json!({
            "app": "cz-labour",
            "dataset": "salary_nowcast",
            "url": receiver.url(),
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "the namespace the nowcast records land under must be watchable: {watch}"
    );
    let watch_id = watch["id"].as_str().expect("watch id").to_string();

    // Never fired yet.
    let (_, empty) = get_json(&router, &format!("/watches/{watch_id}/deliveries")).await;
    assert_eq!(empty["count"], json!(0), "{empty}");

    // Cycle 2 changes the nowcast cell — the change an operator subscribed to.
    run_cycle(&state, 2).await;
    receiver.wait_hits(1, Duration::from_secs(5)).await;

    let (status, body) = get_json(&router, &format!("/watches/{watch_id}/deliveries")).await;
    assert_eq!(status, StatusCode::OK);
    let deliveries = body["deliveries"].as_array().expect("deliveries");
    assert_eq!(
        deliveries.len(),
        1,
        "a cz-labour revision must reach the watch — this is what the \
         `index_datasets` declaration buys beyond search: {body}"
    );
    assert_eq!(deliveries[0]["event"], "dataset.changed");
    assert_eq!(deliveries[0]["ref_id"], json!(watch_id));
}

/// A watch pointed at the SOURCE app for a dataset that lands in `cz-labour`
/// must be refused with the namespace to use — the accepted-but-dead shape
/// `watch_target_refusal` exists to kill, now that `cz-labour` really holds
/// those records.
#[tokio::test]
async fn watching_the_source_app_for_a_cz_labour_dataset_names_the_real_namespace() {
    let (state, _store, _search) = labour_state().await;
    let router = routes::router(state.clone());
    run_cycle(&state, 1).await;

    let (status, body) = post_json(
        &router,
        "/watches",
        json!({
            "app": "mpsv-vpm",
            "dataset": "salary_nowcast",
            "url": "http://127.0.0.1:1/hook",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("cz-labour"),
        "the refusal must name where those records actually land: {msg}"
    );

    // The app's OWN dataset stays watchable under the app.
    let (status, _) = post_json(
        &router,
        "/watches",
        json!({
            "app": "mpsv-vpm",
            "dataset": "region_agg",
            "url": "http://127.0.0.1:1/hook",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}

/// **Known gap, pinned rather than hidden.** `cz-labour` becomes nameable only
/// once it holds records (`namespace_index` unions the registry, the declared
/// `registry::VIRTUAL_NAMESPACES` seed, and every namespace with rows). It is
/// not in that seed, so on a fresh install — before mpsv-vpm has ever run — the
/// watch an operator would set up FIRST is a 404.
///
/// The refusal at least carries the way out (it lists the accepted values), and
/// the fix is one line in `registry::VIRTUAL_NAMESPACES` naming `cz-labour` with
/// `mpsv-vpm` as its publisher. When that lands, this assertion flips to
/// `CREATED` and this test should be deleted.
#[tokio::test]
async fn cz_labour_is_not_watchable_before_its_first_run_the_bootstrap_gap() {
    let (state, _store, _search) = labour_state().await;
    let router = routes::router(state);

    let (status, body) = post_json(
        &router,
        "/watches",
        json!({ "app": "cz-labour", "url": "http://127.0.0.1:1/hook" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "documenting today's bootstrap behaviour, not endorsing it: {body}"
    );
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("mpsv-vpm"),
        "the refusal must still leave the caller unstuck: {msg}"
    );
}

/// The search half: one document per changed record of each declared dataset,
/// carrying the producer's own title — not an untitled JSON dump.
#[tokio::test]
async fn each_declared_dataset_is_indexed_as_its_own_titled_document() {
    let (state, _store, search) = labour_state().await;
    run_cycle(&state, 1).await;

    let docs = search.docs.lock().expect("docs lock").clone();
    for (app, dataset) in app_mpsv_vpm::INDEXED_DATASETS {
        let id = format!("{app}:{dataset}:{CELL}");
        let doc = docs
            .iter()
            .find(|d| d.id == id)
            .unwrap_or_else(|| panic!("no search doc for {id}; got {:?}", ids(&docs)));
        assert_eq!(doc.app, *app);
        assert_eq!(doc.dataset, *dataset);
        assert!(
            doc.title.contains("CZ-ISCO 5223"),
            "the doc's title comes from the record's own `title` field, which is \
             the only line the producer controls: {:?}",
            doc.title
        );
    }
}

fn ids(docs: &[pumper_core::SearchDoc]) -> Vec<&str> {
    docs.iter().map(|d| d.id.as_str()).collect()
}
