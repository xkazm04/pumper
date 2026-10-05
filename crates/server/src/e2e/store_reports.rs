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
//!
//! A third was pinned here *wrongly* for the whole life of this file. Every test
//! below drives `?skip_artifacts=true`, which drops the body sweep — and four of
//! them asserted `healthy: true` over it. They were not wrong about their own
//! property (a disabled index is no defect, a partial index is no defect, an
//! empty store is no defect); they were wrong to read those properties off
//! `healthy`, because `healthy` was `findings.is_empty()` and the skipped check
//! cannot produce a finding. Each now asserts its own property against
//! `findings` / `checks[]`, and the skipped sweep is asserted for what it is:
//! `verdict: "incomplete"`, with `missing_artifact_bodies` `unverifiable`.

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

/// Seeds `records` (or not), then drives the real route through the real
/// router with `?skip_artifacts=true` — the request every test here has always
/// made, and the one whose verdict is now `incomplete` by construction.
async fn doctor_report(
    search: Arc<dyn pumper_core::Search>,
    search_enabled: bool,
    seed_records: usize,
) -> (StatusCode, Value) {
    doctor_report_with(search, search_enabled, seed_records, true).await
}

/// The same, with the artifact sweep under the caller's control — so the two
/// requests against one store can be compared.
async fn doctor_report_with(
    search: Arc<dyn pumper_core::Search>,
    search_enabled: bool,
    seed_records: usize,
    skip_artifacts: bool,
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
                .uri(if skip_artifacts {
                    "/datasets/doctor?skip_artifacts=true"
                } else {
                    "/datasets/doctor"
                })
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

/// One roster entry out of `checks[]`. The roster is total, so a missing entry
/// is a failure rather than a `None`.
fn outcome_of<'a>(body: &'a Value, check: &str) -> &'a Value {
    body["checks"]
        .as_array()
        .unwrap_or_else(|| panic!("the report publishes a check roster: {body}"))
        .iter()
        .find(|c| c["check"] == check)
        .unwrap_or_else(|| panic!("{check} is absent from the roster: {}", body["checks"]))
}

/// What a `?skip_artifacts=true` report must say about the sweep it skipped.
/// Asserted by every test here that takes that shortcut, because the shortcut
/// is exactly what used to be free.
fn assert_the_skipped_sweep_is_declared(body: &Value) {
    assert_eq!(
        body["verdict"], "incomplete",
        "skip_artifacts=true left a check unable to run, so the verdict cannot be clean: {body}"
    );
    assert_eq!(
        body["healthy"], false,
        "`healthy` is `verdict == clean`, and one check never ran: {body}"
    );
    let entry = outcome_of(body, "missing_artifact_bodies");
    assert_eq!(entry["outcome"], "unverifiable", "{entry}");
    let reason = entry["reason"].as_str().unwrap_or_default();
    assert!(
        reason.contains("skip_artifacts=true"),
        "the reason must name the flag that disarmed the check: {entry}"
    );
}

/// The `schema` block, over a real migrated store driven as a route: the one
/// surface that says whether the schema the process booted on is still the one
/// the chain promises. A clean store answers `current` with every drift list
/// empty and raises NO finding - an extra table would be descriptive, never a
/// fault. `Storage::connect` refuses the unsound cases outright
/// (`crates/core/tests/schema_verdict.rs`), so this is the half that proves the
/// report is reachable and shaped as documented.
#[tokio::test]
async fn the_doctor_reports_the_schema_the_process_booted_on() {
    let (status, body) = doctor_report(Arc::new(SizedSearch(3)), true, 3).await;
    assert_eq!(status, StatusCode::OK);
    let schema = &body["schema"];
    assert_eq!(schema["state"], "current", "{body}");
    for empty in ["pending", "ahead", "tables_missing", "tables_unexpected"] {
        assert_eq!(
            schema[empty].as_array().map(|a| a.len()),
            Some(0),
            "`{empty}` is not empty on a freshly replayed store: {schema}"
        );
    }
    assert!(
        schema["applied"].as_array().is_some_and(|a| !a.is_empty()),
        "the applied ledger is reported: {schema}"
    );
    assert!(
        !checks(&body).contains(&"schema_drift"),
        "a sound schema must raise no finding: {:?}",
        checks(&body)
    );
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
    assert_eq!(
        body["verdict"], "findings",
        "an actionable finding outranks the skipped sweep in the rollup: {body}"
    );
    assert_eq!(
        outcome_of(&body, "search_index_empty")["outcome"],
        "findings"
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
///
/// The property is "no finding, and the search check is CLEAN" — a deliberately
/// absent subsystem is not an unexamined one. This test used to read that off
/// `healthy: true`, which it could only do because the request it makes also
/// skips the artifact sweep.
#[tokio::test]
async fn a_store_with_search_disabled_raises_no_search_finding() {
    let (status, body) = doctor_report(Arc::new(NoSearch), false, 3).await;
    assert_eq!(status, StatusCode::OK);
    assert!(checks(&body).is_empty(), "{:?}", checks(&body));
    assert_eq!(
        outcome_of(&body, "search_index_empty")["outcome"],
        "clean",
        "a disabled index is clean, never unverifiable: {}",
        body["checks"]
    );
    assert_eq!(body["search"]["enabled"], false);
    assert_the_skipped_sweep_is_declared(&body);
}

/// The load-bearing property, re-proved at the route rather than over
/// hand-built facts: a clean store — records present, index populated and in
/// step — produces ZERO findings. Every check the request actually ran reports
/// `clean`; the one it skipped reports `unverifiable`, and nothing in between.
#[tokio::test]
async fn a_clean_store_is_not_given_something_to_say() {
    let (status, body) = doctor_report(Arc::new(SizedSearch(3)), true, 3).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        checks(&body).is_empty(),
        "a clean store must report nothing, got {:?}",
        checks(&body)
    );
    assert_eq!(body["read_only"], true);
    let roster = body["checks"].as_array().expect("a roster");
    assert_eq!(roster.len(), 9, "{roster:?}");
    for entry in roster {
        let expected = if entry["check"] == "missing_artifact_bodies" {
            "unverifiable"
        } else {
            "clean"
        };
        assert_eq!(entry["outcome"], expected, "{entry}");
    }
    assert_the_skipped_sweep_is_declared(&body);
}

/// The card's whole claim, at the surface an operator reads: ONE clean store,
/// TWO requests, two different verdicts. `?skip_artifacts=true` is the cheapest
/// way to query the doctor and the one that examines least, and it used to
/// answer `healthy: true` — nine checks' worth of confidence from eight. The
/// full request over the same store is the one entitled to say `clean`.
#[tokio::test]
async fn the_cheap_request_cannot_buy_a_clean_verdict() {
    let (status, skipped) = doctor_report_with(Arc::new(SizedSearch(3)), true, 3, true).await;
    assert_eq!(status, StatusCode::OK);
    assert_the_skipped_sweep_is_declared(&skipped);

    let (status, swept) = doctor_report_with(Arc::new(SizedSearch(3)), true, 3, false).await;
    assert_eq!(status, StatusCode::OK);
    assert!(checks(&swept).is_empty(), "{:?}", checks(&swept));
    assert_eq!(
        swept["verdict"], "clean",
        "a store whose every check ran and found nothing IS clean: {swept}"
    );
    assert_eq!(swept["healthy"], true);
    assert_eq!(
        outcome_of(&swept, "missing_artifact_bodies")["outcome"],
        "clean",
        "the sweep ran over every replayable revision: {}",
        swept["artifacts"]
    );
    // And the numbers behind that `clean` are inspectable, denominator included.
    assert_eq!(swept["artifacts"]["scanned"], true);
    assert_eq!(swept["artifacts"]["bodies_checked"], 0);
    assert_eq!(swept["artifacts"]["replayable_total"], 0);
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
    assert_eq!(outcome_of(&body, "search_index_empty")["outcome"], "clean");
    assert_the_skipped_sweep_is_declared(&body);
}

/// An empty store has nothing to index, so an empty index over it is correct.
#[tokio::test]
async fn an_empty_store_with_an_empty_index_is_not_a_finding() {
    let (_, body) = doctor_report(Arc::new(SizedSearch(0)), true, 0).await;
    assert!(checks(&body).is_empty(), "{:?}", checks(&body));
    assert_eq!(outcome_of(&body, "search_index_empty")["outcome"], "clean");
    assert_eq!(body["search"]["live_records"], 0);
    assert_the_skipped_sweep_is_declared(&body);
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
