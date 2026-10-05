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

/// The namespace/dataset pair the rogue arm writes: a REAL virtual namespace
/// (`cz-labour` is declared) with a dataset that is NOT in `INDEXED_DATASETS`.
/// Picked this way on purpose — it keeps the control honest, because a detector
/// that merely flagged "any app that is not `job.app`" would also flag the four
/// declared `cz-labour` datasets and be useless.
const ROGUE_APP: &str = "cz-labour";
const ROGUE_DATASET: &str = "skill_demand";

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
        // The ROGUE arm: one record under a namespace/dataset pair the result's
        // `index_datasets` does NOT name. Without it this suite is structurally
        // incapable of staging the bug its own module doc describes — the
        // producer's write targets would be the same constant the assertions
        // read, so no oracle here could ever disagree with the implementation.
        // `cz-labour/skill_demand` is deliberately one of the eight names
        // `indexed_datasets_exclude_the_high_cardinality_and_identity_less_tables`
        // lists as excluded: a real app starting to write it is exactly the
        // move nothing in this repo noticed.
        if ctx.params.get("rogue").and_then(Value::as_bool) == Some(true) {
            ctx.datasets
                .upsert(
                    ROGUE_APP,
                    ROGUE_DATASET,
                    CELL,
                    &json!({ "title": "undeclared skill demand", "cycle": cycle }),
                )
                .await?;
        }
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
    run_cycle_maybe_rogue(state, cycle, false).await;
}

/// `run_cycle`, plus the job id and the switch that makes the producer write
/// outside its own declaration. Returns the job id so the run can be read back
/// through `GET /jobs/{id}/receipt` — the surface an operator actually reads.
async fn run_cycle_maybe_rogue(state: &AppState, cycle: i64, rogue: bool) -> uuid::Uuid {
    let job = state
        .storage
        .enqueue(
            "mpsv-vpm",
            pumper_core::EnqueueOptions {
                params: json!({ "cycle": cycle, "rogue": rogue }),
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");
    crate::worker::run_one(state).await;
    job.id
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

// ── the write/declaration diff ──────────────────────────────────────────────

/// Pulls `coverage` off the run's receipt, which is where an operator reads it.
async fn coverage_of(router: &axum::Router, id: uuid::Uuid) -> Value {
    let (status, receipt) = get_json(router, &format!("/jobs/{id}/receipt")).await;
    assert_eq!(status, StatusCode::OK, "receipt: {receipt}");
    receipt["coverage"].clone()
}

/// The headline. A run writes one record under `cz-labour/skill_demand`, a pair
/// its result's `index_datasets` never names, and the receipt says so.
///
/// Before this existed there was NO signal of any kind: `run_indexed_apps` knew
/// the declared set, `job_revision_counts` knew the observed set, and nothing
/// in the repo diffed them. The class had already bitten `ca-grants` and `peer`
/// — both found by a human, after shipping.
#[tokio::test]
async fn a_write_outside_the_declaration_is_named_on_the_receipt() {
    let (state, _store, _search) = labour_state().await;
    let router = routes::router(state.clone());
    let id = run_cycle_maybe_rogue(&state, 1, true).await;

    let coverage = coverage_of(&router, id).await;
    let undeclared = coverage["undeclared"]
        .as_array()
        .unwrap_or_else(|| panic!("coverage.undeclared is absent: {coverage}"));
    assert_eq!(
        undeclared.len(),
        1,
        "exactly the rogue pair, not every write: {coverage}"
    );
    assert_eq!(undeclared[0]["app"], ROGUE_APP);
    assert_eq!(undeclared[0]["dataset"], ROGUE_DATASET);
    assert_eq!(
        coverage["complete"],
        json!(true),
        "every revision of this run carries a job_id stamp, so the diff is \
         complete and may be trusted: {coverage}"
    );
}

/// The control arm, in the same run: the four pairs the result DOES declare
/// stay out of `undeclared`, and `declared` is exactly `run_indexed_apps` —
/// the job's own app plus the virtual apps its result names, in that order.
#[tokio::test]
async fn the_declared_namespaces_are_not_reported_and_declared_lists_the_run_apps() {
    let (state, _store, _search) = labour_state().await;
    let router = routes::router(state.clone());
    let id = run_cycle_maybe_rogue(&state, 1, false).await;

    let coverage = coverage_of(&router, id).await;
    assert_eq!(
        coverage["undeclared"],
        json!([]),
        "a run that wrote only what it declared has nothing to report: {coverage}"
    );
    assert_eq!(
        coverage["declared"],
        json!(["mpsv-vpm", "cz-labour"]),
        "`declared` is run_indexed_apps(job.app, index_specs): the job's own \
         app first, then the virtual apps the result names: {coverage}"
    );
}
