//! `GET /jobs/{id}/receipt` over a real run: one job, one document joining its
//! cost, stage timings, yield, revisions, verdicts, artifacts, deliveries and
//! trigger hops.
//!
//! The assertions that matter are the honest ones — a receipt that quietly
//! invents a number is worse than no receipt, so every gap has to be *named*
//! in `unknown[]` rather than rendered as a zero.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::{AppContext, EnqueueOptions, JobStatus, NewTrigger, Result, ScrapeApp};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::state::AppState;
use crate::worker;

/// One GET through the real router.
async fn get(state: &AppState, uri: &str) -> (StatusCode, Value) {
    let resp = crate::routes::router(state.clone())
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

async fn get_json(state: &AppState, uri: &str) -> Value {
    let (status, body) = get(state, uri).await;
    assert_eq!(status, StatusCode::OK, "GET {uri} -> {body}");
    body
}

/// An app that spends, writes a dataset record, and saves an artifact — so the
/// receipt has something on every axis to report.
struct ReceiptApp;

#[async_trait::async_trait]
impl ScrapeApp for ReceiptApp {
    fn name(&self) -> &'static str {
        "receipted"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        ctx.meter("claude", None, 0.25, Some("one research call"))
            .await;
        ctx.save_artifact("page.html", b"<html>hello</html>")
            .await?;
        let summary = ctx
            .sync_many("d", &[("k1".to_string(), json!({ "n": 1 }))])
            .await?;
        Ok(json!({ "new": summary.new.len(), "changed": summary.changed.len() }))
    }
}

#[tokio::test]
async fn a_receipt_joins_cost_stages_yield_changes_and_artifacts_for_one_run() {
    let (state, _store) = test_state(vec![Arc::new(ReceiptApp)]).await;
    // A dataset trigger, so the receipt has a hop to attribute.
    state
        .storage
        .create_trigger(&NewTrigger {
            name: Some("hop"),
            source_kind: "dataset",
            source_app: "receipted",
            source_dataset: Some("d"),
            on_change: None,
            on_status: None,
            target_app: "receipted",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: None,
        })
        .await
        .unwrap();
    let job = state
        .storage
        .enqueue("receipted", EnqueueOptions::default())
        .await
        .unwrap();

    assert!(worker::run_one(&state).await);
    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Succeeded
    );

    let body = get_json(&state, &format!("/jobs/{}/receipt", job.id)).await;

    // ── the job itself ──
    assert_eq!(body["job"]["app"], "receipted");
    assert_eq!(body["job"]["status"], "succeeded");
    assert!(
        body["job"]["wall_ms"].as_i64().unwrap() >= 0,
        "a finished job has a measured wall clock"
    );

    // ── cost ──
    let total = body["cost"]["total_usd"]
        .as_f64()
        .expect("total is a number");
    assert!((total - 0.25).abs() < 1e-9, "metered spend, got {total}");
    assert_eq!(body["cost"]["calls"], 1);
    assert_eq!(body["cost"]["by_engine"][0]["engine"], "claude");

    // ── stages (W-D) ──
    assert!(
        body["stages"]["run_ms"].is_number(),
        "the receipt is the home of the stage timings: {}",
        body["stages"]
    );
    assert!(body["stages"]["total_ms"].is_number());

    // ── yield + changes: two independent witnesses of the same write ──
    assert_eq!(
        body["yield"][0]["new"], 1,
        "the result reported one new record"
    );
    let change = &body["changes"][0];
    assert_eq!(change["app"], "receipted");
    assert_eq!(change["dataset"], "d");
    assert_eq!(
        change["by_change"]["new"], 1,
        "revisions are counted from this job's provenance stamp, not a time window"
    );

    // ── artifacts, sized from disk ──
    assert_eq!(body["artifacts"]["count"], 1);
    assert_eq!(body["artifacts"]["files"][0]["name"], "page.html");
    assert_eq!(
        body["artifacts"]["files"][0]["bytes"],
        "<html>hello</html>".len()
    );
    assert_eq!(body["artifacts"]["total_bytes"], "<html>hello</html>".len());

    // ── the hop this run caused ──
    let hops = body["trigger_hops"].as_array().expect("hops array");
    assert_eq!(hops.len(), 1, "the dataset trigger fired exactly one hop");
    assert_eq!(hops[0]["app"], "receipted");
    assert_ne!(hops[0]["job_id"], json!(job.id.to_string()));

    // ── the gaps are named, not zeroed ──
    let unknown = body["unknown"].as_array().expect("unknown array");
    let text = unknown
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        text.contains("deliveries:"),
        "the receipt must say that watch/search deliveries cannot be attributed to a job: \
         {text}"
    );
}

/// A job that never ran has no stages, no cost and no changes — and must say
/// which kind of "nothing" each of those is instead of reporting zeros that
/// read as measurements.
#[tokio::test]
async fn a_queued_job_reports_missing_stamps_as_named_unknowns_not_zeros() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let job = state
        .storage
        .enqueue("fake", EnqueueOptions::default())
        .await
        .unwrap();

    let body = get_json(&state, &format!("/jobs/{}/receipt", job.id)).await;

    assert_eq!(body["job"]["status"], "queued");
    assert!(
        body["job"]["wall_ms"].is_null(),
        "an unstarted job has no duration — not 0"
    );
    assert!(body["stages"].is_null());
    let text = body["unknown"]
        .as_array()
        .expect("unknown array")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        text.contains("has not finished"),
        "the missing stage row must name WHY it is missing: {text}"
    );
    assert!(
        text.contains("cost:"),
        "an empty ledger must be explained, since $0.00 also means 'nothing metered': {text}"
    );
    assert!(body["artifacts"].is_null(), "no run, no artifact directory");
}

/// The receipt is read-only and per-job: asking for one that doesn't exist is a
/// 404, not an empty shell that looks like a real (all-zero) run.
#[tokio::test]
async fn an_unknown_job_is_a_404_not_an_empty_receipt() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let (status, body) = get(&state, &format!("/jobs/{}/receipt", uuid::Uuid::new_v4())).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["error"].is_string(),
        "a missing job gets the error envelope, not an all-zero receipt: {body}"
    );
}
