//! Every door that creates future work applies the target app's declared
//! params schema — over real HTTP where a door is an endpoint, and through the
//! real reconcile/fire paths where it is not.
//!
//! The anti-pattern: `POST /apps/{name}/jobs` enforced the schema (422, pointer
//! paths) while `POST /schedules` stored the body verbatim, the scheduler
//! enqueued it hours later, and trigger hops never checked at all — so the same
//! app ran with params one of its own doors had already refused, and the only
//! trace was a failed job nobody could connect back to the schedule row or the
//! trigger template.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use http_body_util::BodyExt;
use pumper_core::{AppContext, AppManifest, EnqueueOptions, Result, ScrapeApp};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::test_state;
use crate::state::AppState;
use crate::{routes, scheduler};

/// An app with a real `params_schema` and non-trivial defaults — the fixture
/// the whole door-parity question needs (the shared `FakeApp` declares neither).
struct SchemaApp;

#[async_trait::async_trait]
impl ScrapeApp for SchemaApp {
    fn name(&self) -> &'static str {
        "schema-app"
    }
    fn description(&self) -> &'static str {
        "declares a params schema and defaults"
    }
    fn default_params(&self) -> Value {
        json!({ "query": "default-query", "mode": "full" })
    }
    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": { "type": "string" },
                    "rows": { "type": "integer", "maximum": 10 }
                }
            })),
            ..Default::default()
        }
    }
    async fn run(&self, _ctx: AppContext) -> Result<Value> {
        Ok(json!({ "ok": true }))
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

/// The refuted claim: "the params door is `POST /apps/{name}/jobs`". A schedule
/// is a standing order for the same job, so it gets the same answer — with the
/// same 422 and the same JSON-pointer paths, not a 201 and a broken cron.
#[tokio::test]
async fn schedule_door_refuses_what_the_job_door_refuses_not_a_201() {
    let (state, _store) = test_state(vec![Arc::new(SchemaApp)]).await;
    let router = routes::router(state);

    let bad = json!({ "rows": 5000 });
    let (job_status, job_body) =
        post_json(&router, "/apps/schema-app/jobs", json!({ "params": bad })).await;
    assert_eq!(
        job_status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "baseline: the job door refuses these params"
    );

    let (sched_status, sched_body) = post_json(
        &router,
        "/schedules",
        json!({ "app": "schema-app", "cron": "0 * * * * *", "params": bad }),
    )
    .await;
    assert_eq!(
        sched_status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "the schedule door must answer the same as the job door: {sched_body}"
    );
    let msg = sched_body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("params/rows"),
        "the 422 carries the JSON-pointer path, as the job door's does: {msg}"
    );
    assert_eq!(
        job_body["error"].as_str().unwrap_or_default(),
        msg,
        "one check, one message — the two doors cannot phrase the same refusal differently"
    );

    // And nothing was stored: a refused schedule must not exist.
    let (_, list) = get_json(&router, "/schedules").await;
    assert_eq!(
        list.as_array().map(Vec::len),
        Some(0),
        "a 422'd schedule must not be persisted: {list}"
    );
}

/// The scheduler runs jobs with the app's defaults UNDER the schedule's own
/// params — the same shallow merge `POST /apps/{name}/jobs` performs. Before
/// this, the scheduler REPLACED wholesale, so a schedule that set one key
/// silently dropped every default the HTTP door would have kept.
#[tokio::test]
async fn scheduled_run_merges_over_defaults_not_replaces_them() {
    let (state, _store) = test_state(vec![Arc::new(SchemaApp)]).await;
    let router = routes::router(state.clone());

    let (status, created) = post_json(
        &router,
        "/schedules",
        json!({ "app": "schema-app", "cron": "0 * * * * *", "params": { "rows": 5 } }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["id"].as_str().expect("schedule id").to_string();
    backdate(&state, &id).await;

    scheduler::reconcile(&state, &mut HashMap::new(), None, Utc::now())
        .await
        .unwrap();

    let job = state
        .storage
        .list(Some("schema-app"), None, 1)
        .await
        .unwrap()
        .pop()
        .expect("the schedule fired");
    assert_eq!(job.params["rows"], json!(5), "the schedule's own key wins");
    assert_eq!(
        job.params["mode"],
        json!("full"),
        "a default the schedule did not mention survives — this is the merge"
    );
    assert_eq!(job.params["query"], json!("default-query"));
}

/// A row that predates the create-time check (or was edited in SQL) must not
/// silently brick: the fire path skips it WITHOUT eating the firing, and
/// `GET /schedules` says `invalid_params` instead of `ok`.
#[tokio::test]
async fn legacy_invalid_schedule_is_skipped_visibly_not_enqueued_or_silently_ok() {
    let (state, _store) = test_state(vec![Arc::new(SchemaApp)]).await;
    // Straight into SQL: this is exactly the row shape the door now refuses.
    let schedule = state
        .storage
        .create_schedule(pumper_core::NewSchedule {
            app: "schema-app",
            cron: "0 * * * * *",
            params: json!({ "query": "x", "rows": 9999 }),
            priority: 0,
            timezone: None,
            misfire_policy: "fire_once",
            max_attempts: Some(1),
        })
        .await
        .expect("storage takes the row the API would refuse");
    backdate(&state, &schedule.id).await;

    scheduler::reconcile(&state, &mut HashMap::new(), None, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        state
            .storage
            .list(Some("schema-app"), None, 10)
            .await
            .unwrap()
            .len(),
        0,
        "invalid params must not become a job that fails minutes later"
    );

    let last_run: Option<String> =
        sqlx::query_scalar("SELECT last_run FROM schedules WHERE id = ?1")
            .bind(&schedule.id)
            .fetch_one(&state.storage.pool())
            .await
            .unwrap();
    assert!(
        last_run.is_none(),
        "the skip must not eat the firing — fixing the params has to make it fire, not wait an hour"
    );

    let router = routes::router(state);
    let (status, body) = get_json(&router, "/schedules").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body[0]["health"], "invalid_params",
        "the skip is observable on the schedule itself, not only in the logs: {body}"
    );
}

/// A trigger whose resolved params cannot satisfy the target app's schema
/// records a `bad_params` decision instead of enqueueing a hop that was always
/// going to fail — and `fired` stays honest.
#[tokio::test]
async fn trigger_with_a_bad_template_records_bad_params_instead_of_firing() {
    let (state, _store) = test_state(vec![Arc::new(SchemaApp)]).await;
    let trigger = state
        .storage
        .create_trigger(&pumper_core::NewTrigger {
            name: Some("bad-template"),
            source_kind: "job",
            source_app: "schema-app",
            source_dataset: None,
            on_change: None,
            on_status: Some("succeeded"),
            target_app: "schema-app",
            // `rows` exceeds the declared maximum: no `_trigger` merge can fix it.
            params: &json!({ "query": "q", "rows": 4242 }),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: None,
        })
        .await
        .expect("create trigger");

    let source = state
        .storage
        .enqueue(
            "schema-app",
            EnqueueOptions {
                params: json!({ "query": "q" }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let claimed = state.storage.claim_next(&[], 0.0).await.unwrap().unwrap();
    state
        .storage
        .complete(claimed.id, claimed.attempts, json!({ "ok": true }))
        .await
        .unwrap();
    let source = state.storage.get(source.id).await.unwrap().unwrap();

    crate::triggers::fire_terminal_triggers(&state, &source).await;
    assert_eq!(
        state
            .storage
            .list(Some("schema-app"), None, 10)
            .await
            .unwrap()
            .len(),
        1,
        "a hop that cannot pass the door is not enqueued: only the source job exists"
    );

    let decisions = state
        .storage
        .list_trigger_runs_page(&trigger.id, None, 10)
        .await
        .unwrap();
    let outcomes: Vec<&str> = decisions.iter().map(|d| d.outcome.as_str()).collect();
    assert_eq!(
        outcomes,
        vec!["bad_params"],
        "the refusal is recorded where the template was authored, not as a silent nothing"
    );
    assert!(
        pumper_core::storage::TRIGGER_OUTCOMES.contains(&"bad_params"),
        "the outcome vocabulary is a contract; the new value has to be in it"
    );
    let detail = decisions[0].detail.as_deref().unwrap_or_default();
    assert!(
        detail.contains("params/rows"),
        "the ledger row carries the door's own pointer path: {detail}"
    );
}

/// Makes a fresh every-minute schedule due by backdating its creation.
async fn backdate(state: &AppState, id: &str) {
    sqlx::query("UPDATE schedules SET created_at = ?1 WHERE id = ?2")
        .bind((Utc::now() - chrono::Duration::minutes(5)).to_rfc3339())
        .bind(id)
        .execute(&state.storage.pool())
        .await
        .unwrap();
}
