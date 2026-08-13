//! `GET /datasets/doctor` driven as a route, over a real seeded store.
//!
//! The doctor had **zero** route-level tests: its pure core was unit-tested and
//! the live smoke assertion only checked that the request returned. Everything
//! between those two — which queries the route actually issues, what it feeds
//! `diagnose`, and whether `healthy` reflects the store an operator is looking
//! at — was unguarded. That is the half where the search-index blindness lived:
//! `AppState.search` was right there and the route never touched it.
//!
//! Two properties are pinned here, and they pull against each other on purpose:
//! the report must SPEAK UP when the index is empty over a populated store, and
//! it must stay SILENT on a clean one — including the entirely valid deployment
//! where `[search] enabled = false`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::{NoSearch, Result, SearchDoc, SearchRequest, SearchResponse};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state_indexed, FakeApp};
use crate::routes;

/// A `Search` whose corpus size is the only thing under test.
struct SizedSearch(u64);

#[async_trait::async_trait]
impl pumper_core::Search for SizedSearch {
    async fn index(&self, _docs: Vec<SearchDoc>) -> Result<()> {
        Ok(())
    }
    async fn query(&self, _req: SearchRequest) -> Result<SearchResponse> {
        Ok(SearchResponse::default())
    }
    async fn delete_ids(&self, _ids: &[String]) -> Result<()> {
        Ok(())
    }
    async fn delete_dataset(&self, _app: &str, _dataset: &str) -> Result<()> {
        Ok(())
    }
    async fn doc_count(&self) -> Result<u64> {
        Ok(self.0)
    }
}

/// Seeds `records` (or not), then drives the real route through the real router.
async fn doctor_report(
    search: Arc<dyn pumper_core::Search>,
    search_enabled: bool,
    seed_records: usize,
) -> (StatusCode, Value) {
    let (state, _store) = test_state_indexed(vec![Arc::new(FakeApp)], search, |c| {
        c.search.enabled = search_enabled;
    })
    .await;

    for i in 0..seed_records {
        state
            .datasets
            .upsert(
                "grants",
                "unified",
                &format!("k{i}"),
                &json!({ "title": format!("Grant {i}"), "url": format!("https://x/{i}") }),
            )
            .await
            .unwrap();
    }

    let router = routes::router(state);
    let resp = router
        .oneshot(
            Request::builder()
                .uri("/datasets/doctor?skip_artifacts=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn checks(body: &Value) -> Vec<&str> {
    body["findings"]
        .as_array()
        .map(|a| a.iter().filter_map(|f| f["check"].as_str()).collect())
        .unwrap_or_default()
}

/// The user moment: *"Search had been returning nothing for a week. `just
/// doctor` said the store was healthy the whole time."* The signal existed —
/// but only on `/search`'s own response, which means the operator learns about
/// it from a user rather than from the diagnostic that exists to be run first.
#[tokio::test]
async fn an_empty_index_over_a_seeded_store_is_not_reported_healthy() {
    let (status, body) = doctor_report(Arc::new(SizedSearch(0)), true, 3).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["healthy"], false,
        "an enabled-but-empty index over a populated store is not healthy: {body}"
    );
    assert!(
        checks(&body).contains(&"search_index_empty"),
        "expected a search finding, got {:?}",
        checks(&body)
    );

    let finding = body["findings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["check"] == "search_index_empty")
        .unwrap();
    assert_eq!(finding["severity"], "warn");
    assert_eq!(finding["count"], 3, "the count is the live record count");
    assert!(
        finding["remediation"]
            .as_str()
            .unwrap_or_default()
            .contains("search-backfill"),
        "the remediation must name the recovery binary: {finding}"
    );
    assert!(
        !finding["examples"].as_array().unwrap().is_empty(),
        "a finding without examples is a bare count"
    );

    // The descriptive block is present too, so the numbers behind the verdict
    // are inspectable without a second request.
    assert_eq!(body["search"]["enabled"], true);
    assert_eq!(body["search"]["doc_count"], 0);
    assert_eq!(body["search"]["live_records"], 3);
}

/// `[search] enabled = false` is a supported deployment: `NoSearch` answers
/// every call with silent success and reports 0 documents by design. Keying the
/// check on `doc_count == 0` alone would make every search-less store
/// permanently unhealthy — a report that always says something gets ignored.
#[tokio::test]
async fn a_store_with_search_disabled_is_still_healthy() {
    let (status, body) = doctor_report(Arc::new(NoSearch), false, 3).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["healthy"], true,
        "search disabled is a deployment choice, not a defect: {body}"
    );
    assert!(checks(&body).is_empty(), "{:?}", checks(&body));
    assert_eq!(body["search"]["enabled"], false);
}

/// The load-bearing property, re-proved at the route rather than over
/// hand-built facts: a clean store — records present, index populated and in
/// step — produces ZERO findings and `healthy: true`.
#[tokio::test]
async fn a_clean_store_is_not_given_something_to_say() {
    let (status, body) = doctor_report(Arc::new(SizedSearch(3)), true, 3).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        checks(&body).is_empty(),
        "a clean store must report nothing, got {:?}",
        checks(&body)
    );
    assert_eq!(body["healthy"], true);
    assert_eq!(body["read_only"], true);
}

/// An index far smaller than the store is the NORMAL state, not a defect: the
/// live path only maintains datasets an app names in `index_datasets`, so any
/// ratio-based check would fire on a correct deployment forever. Only
/// zero-versus-nonzero means anything.
#[tokio::test]
async fn an_index_smaller_than_the_store_is_not_a_finding() {
    let (_, body) = doctor_report(Arc::new(SizedSearch(1)), true, 50).await;
    assert!(
        !checks(&body).contains(&"search_index_empty"),
        "a partially-indexed store must not be flagged: {body}"
    );
    assert_eq!(body["healthy"], true);
}

/// An empty store has nothing to index, so an empty index over it is correct.
#[tokio::test]
async fn an_empty_store_with_an_empty_index_is_not_a_finding() {
    let (_, body) = doctor_report(Arc::new(SizedSearch(0)), true, 0).await;
    assert!(checks(&body).is_empty(), "{:?}", checks(&body));
    assert_eq!(body["healthy"], true);
    assert_eq!(body["search"]["live_records"], 0);
}

/// The doctor repairs nothing. Driving the route must not change the store —
/// including the new search-side gathering, which reads `doc_count` and one
/// aggregate and must never trigger an index write.
#[tokio::test]
async fn driving_the_route_repairs_nothing() {
    let (state, _store) =
        test_state_indexed(vec![Arc::new(FakeApp)], Arc::new(SizedSearch(0)), |c| {
            c.search.enabled = true;
        })
        .await;
    state
        .datasets
        .upsert("grants", "unified", "k", &json!({ "title": "a grant" }))
        .await
        .unwrap();

    let snapshot = |state: crate::state::AppState| async move {
        let recs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM records")
            .fetch_one(&state.storage.pool())
            .await
            .unwrap();
        let revs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM record_revisions")
            .fetch_one(&state.storage.pool())
            .await
            .unwrap();
        let sims: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(simhash), 0) FROM records")
            .fetch_one(&state.storage.pool())
            .await
            .unwrap();
        (recs, revs, sims)
    };

    let before = snapshot(state.clone()).await;
    let resp = routes::router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/datasets/doctor?skip_artifacts=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        before,
        snapshot(state).await,
        "the doctor must not repair on read"
    );
}
