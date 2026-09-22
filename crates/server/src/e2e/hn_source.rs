//! Stage 3 end to end: retirement through the run path and the scheduler, the
//! manual state change's health event, and the live HN test source.
//!
//! `DeadSite` is registered under the name `extractor` so the scheduler's
//! retirement rule (extractor schedules only, source `extractor/<dataset>`)
//! applies to it; it reports what a urls-mode run over a dead site reports —
//! every URL of its roster answered 404 — through the real
//! `observe_extraction`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{TimeZone, Utc};
use pumper_core::resilience::source_id;
use pumper_core::{
    AppContext, EnqueueOptions, FetchHealth, NewSchedule, Result, ScrapeApp, SourceState,
};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state_with, TestReceiver};
use crate::state::AppState;
use crate::{routes, scheduler, worker};

const DATASET: &str = "hn-items";

struct DeadSite;

#[async_trait::async_trait]
impl ScrapeApp for DeadSite {
    fn name(&self) -> &'static str {
        "extractor"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let fetch = FetchHealth {
            attempted: 40,
            ok: 0,
            permanent: 40,
            roster: Some(40),
        };
        let verdict = ctx.observe_extraction(DATASET, &[], fetch).await?;
        Ok(json!({ "health": verdict }))
    }
}

async fn deliveries(state: &AppState) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries WHERE kind = 'health'")
        .fetch_one(&state.storage.pool())
        .await
        .unwrap()
}

async fn run_dead(state: &AppState) -> uuid::Uuid {
    let job = state
        .storage
        .enqueue(
            "extractor",
            EnqueueOptions {
                params: json!({ "dataset": DATASET }),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .id;
    assert!(worker::run_one(state).await);
    job
}

async fn backdate(state: &AppState, sql: &str, days_ago: i64) {
    sqlx::query(sql)
        .bind(pumper_core::datasets::ts(
            Utc::now() - chrono::Duration::days(days_ago),
        ))
        .execute(&state.storage.pool())
        .await
        .unwrap();
}

#[tokio::test]
async fn auto_retirement_sends_source_retired_through_the_run_path() {
    let rx = TestReceiver::spawn(vec![]).await;
    let url = rx.url();
    let src = source_id("extractor", DATASET);
    let listed = src.clone();
    let (state, _store) = test_state_with(vec![Arc::new(DeadSite)], move |c| {
        c.resilience.enabled = true;
        c.resilience.auto_retire = vec![listed];
        c.webhooks.health_url = Some(url);
    })
    .await;

    // Three dead weekly runs; the first two are moved into the past.
    let first = run_dead(&state).await;
    backdate(&state, "UPDATE sources SET state_since = ?1", 30).await;
    backdate(
        &state,
        &format!("UPDATE source_runs SET created_at = ?1 WHERE job_id = '{first}'"),
        14,
    )
    .await;
    let second = run_dead(&state).await;
    backdate(
        &state,
        &format!("UPDATE source_runs SET created_at = ?1 WHERE job_id = '{second}'"),
        7,
    )
    .await;
    assert_eq!(deliveries(&state).await, 0, "two dead runs: still healthy");
    let third = run_dead(&state).await;

    assert_eq!(
        state.health.state("extractor", DATASET).await,
        SourceState::Retired
    );
    let hits = rx.wait_hits(1, Duration::from_secs(5)).await;
    assert_eq!(hits.len(), 1, "one entry, one alert");
    assert_eq!(hits[0].0["x-pumper-event"], "source.retired");
    let body: Value = serde_json::from_slice(&hits[0].1).unwrap();
    assert_eq!(body["source_id"], src);
    assert_eq!(body["from"], "healthy");
    assert_eq!(body["to"], "retired");
    assert_eq!(body["job_id"], third.to_string());
}

async fn post_state(router: &axum::Router, id: &str, body: Value) -> StatusCode {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/sources/{id}/state"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn a_manual_state_change_into_degraded_emits_one_source_degraded() {
    let rx = TestReceiver::spawn(vec![]).await;
    let url = rx.url();
    let (state, _store) = test_state_with(vec![Arc::new(DeadSite)], move |c| {
        c.resilience.enabled = true;
        c.webhooks.health_url = Some(url);
    })
    .await;
    state
        .health
        .store()
        .unwrap()
        .ensure_source("extractor", DATASET)
        .await
        .unwrap();
    let router = routes::router(state.clone());

    let status = post_state(
        &router,
        "extractor%2Fhn-items",
        json!({"state": "degraded", "reason": "selector drift seen by hand"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let hits = rx.wait_hits(1, Duration::from_secs(5)).await;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0["x-pumper-event"], "source.degraded");
    let body: Value = serde_json::from_slice(&hits[0].1).unwrap();
    assert_eq!(
        (&body["from"], &body["to"], &body["job_id"], &body["reason"]),
        (
            &json!("healthy"),
            &json!("degraded"),
            &Value::Null,
            &json!("selector drift seen by hand")
        )
    );

    // Degraded → degraded is not an entry, and recovery is never an alert.
    assert_eq!(
        post_state(
            &router,
            "extractor%2Fhn-items",
            json!({"state": "degraded"})
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        post_state(&router, "extractor%2Fhn-items", json!({"state": "healthy"})).await,
        StatusCode::OK
    );
    state.deliveries.drain(Duration::from_secs(5)).await;
    assert_eq!(rx.hits_so_far().len(), 1, "still exactly one delivery");
    assert_eq!(deliveries(&state).await, 1);
}

#[tokio::test]
async fn a_retired_sources_schedule_is_skipped_and_a_restore_resumes_it() {
    let (state, _store) = test_state_with(vec![Arc::new(DeadSite)], |c| {
        c.resilience.enabled = true;
        // Soak mode: retirement stops scheduling whether or not `enforce` is on.
        c.resilience.enforce = false;
    })
    .await;
    let schedule = state
        .storage
        .create_schedule(NewSchedule {
            app: "extractor",
            cron: "0 0 * * * *",
            params: json!({ "dataset": DATASET }),
            priority: 0,
            timezone: None,
            misfire_policy: "fire_once",
            max_attempts: Some(1),
            budget_usd: None,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE schedules SET created_at = ?1 WHERE id = ?2")
        .bind(
            Utc.with_ymd_and_hms(2026, 7, 13, 10, 30, 0)
                .unwrap()
                .to_rfc3339(),
        )
        .bind(&schedule.id)
        .execute(&state.storage.pool())
        .await
        .unwrap();
    let hs = state.health.store().unwrap();
    hs.ensure_source("extractor", DATASET).await.unwrap();
    let src = source_id("extractor", DATASET);
    assert!(hs
        .set_state_manual(&src, SourceState::Retired, "gone")
        .await
        .unwrap());

    let jobs = |state: AppState, id: String| async move {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM jobs WHERE schedule_id = ?1")
            .bind(id)
            .fetch_one(&state.storage.pool())
            .await
            .unwrap()
    };

    let at_11 = Utc.with_ymd_and_hms(2026, 7, 13, 11, 0, 5).unwrap();
    let tally = scheduler::reconcile(&state, &mut HashMap::new(), None, at_11)
        .await
        .unwrap();
    assert_eq!((tally.fired, tally.skipped), (0, 1), "{tally:?}");
    assert_eq!(jobs(state.clone(), schedule.id.clone()).await, 0);
    let row = state
        .storage
        .get_schedule(&schedule.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.skipped_count, 1,
        "the eaten firing is recorded as a skip"
    );
    assert_eq!(row.last_run, None, "nothing ran, so last_run does not move");

    // The operator restores it: the next firing runs.
    assert!(hs
        .set_state_manual(&src, SourceState::Healthy, "back")
        .await
        .unwrap());
    let at_12 = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 5).unwrap();
    let tally = scheduler::reconcile(&state, &mut HashMap::new(), Some(at_11), at_12)
        .await
        .unwrap();
    assert_eq!(tally.fired, 1, "{tally:?}");
    assert_eq!(jobs(state.clone(), schedule.id.clone()).await, 1);
}

#[test]
fn only_an_extractor_schedule_over_a_retired_source_is_blocked() {
    use scheduler::{retirement_dataset, schedule_blocked_by_retirement};
    let p = json!({ "dataset": DATASET });
    assert!(schedule_blocked_by_retirement(
        "extractor",
        &p,
        SourceState::Retired
    ));
    assert_eq!(
        retirement_dataset("extractor", &json!({})).as_deref(),
        Some("extracted")
    );
    // Every other rung fires — `unknown` (a failed read) included.
    for s in [
        SourceState::Healthy,
        SourceState::Suspect,
        SourceState::Degraded,
        SourceState::Quarantined,
        SourceState::Probation,
        SourceState::Unknown,
    ] {
        assert!(!schedule_blocked_by_retirement("extractor", &p, s), "{s:?}");
    }
    // Other apps' params say nothing about a source id.
    assert!(!schedule_blocked_by_retirement(
        "crawl",
        &p,
        SourceState::Retired
    ));
    assert_eq!(retirement_dataset("crawl", &p), None);
}

/// The live half of the HN test source: one real item page, fetched once,
/// still extracts all five fields with the checked-in ruleset. A network test
/// (and a third party's markup), so it is not part of the per-change gate.
#[tokio::test]
#[ignore = "live network: fetches news.ycombinator.com; run with --ignored"]
async fn live_hn_item_still_matches_the_ruleset() {
    const RULESET: &str = include_str!("../../../apps/extractor/tests/fixtures/hn/ruleset.json");
    let url = "https://news.ycombinator.com/item?id=8863";
    let body = reqwest::Client::new()
        .get(url)
        .header("user-agent", "pumper-test (live HN ruleset check)")
        .send()
        .await
        .expect("fetch")
        .text()
        .await
        .expect("body");
    let rules: pumper_core::RuleSet = serde_json::from_str(RULESET).unwrap();
    let (values, report) = pumper_core::extract::extract_one_with_report_at(
        &rules.compile().unwrap(),
        &body,
        Some(url),
    );
    for field in ["title", "url", "by", "time", "points"] {
        assert_eq!(
            report.fields.get(field),
            Some(&pumper_core::FieldStatus::Matched),
            "`{field}` no longer matches the live page: {values}"
        );
    }
    assert_eq!(values["by"], "dhouston");
    assert_eq!(
        values["title"],
        "My YC app: Dropbox - Throw away your USB drive"
    );
}
