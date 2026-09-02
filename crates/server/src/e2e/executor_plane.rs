//! N18 end-to-end: two "executors" draining the same queue through the real
//! routes, and the three failure modes the plane is built on.
//!
//! Nothing here is a mock of the plane. The requests go through
//! `routes::router` — the same wiring, the same secret guard, the same
//! `(status='running', attempts, executor_id)` fence — driven by a small
//! [`TestExecutor`] that does exactly what `executor_main`'s loop does, minus
//! the process boundary. What is deliberately *not* exercised is the executor
//! binary's own transport (reqwest, retries); that is one `--executor` process
//! away and is stated in the report as unverified.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::config::EXECUTOR_SECRET_HEADER;
use pumper_core::{AppContext, AppManifest, CostClass, EnqueueOptions, Result, ScrapeApp};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::test_state_with;
use crate::routes;

const SECRET: &str = "plane-secret";

/// A result-only app, i.e. one this plane may legitimately hand out: it fetches
/// nothing, writes no dataset, and returns everything it produced.
struct RemoteFake;

#[async_trait]
impl ScrapeApp for RemoteFake {
    fn name(&self) -> &'static str {
        "remote-fake"
    }
    fn manifest(&self) -> AppManifest {
        AppManifest {
            cost_class: CostClass::Free,
            ..Default::default()
        }
    }
    fn executor(&self) -> bool {
        true
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        Ok(json!({ "ran": ctx.params.get("i").cloned().unwrap_or(Value::Null) }))
    }
}

/// One executor, driven over the real router.
struct TestExecutor {
    id: String,
    router: axum::Router,
}

/// What a claim answered: a job envelope, or nothing.
type Claimed = Option<Value>;

impl TestExecutor {
    fn new(id: &str, router: &axum::Router) -> Self {
        Self {
            id: id.to_string(),
            router: router.clone(),
        }
    }

    async fn post(&self, uri: &str, secret: &str, body: Value) -> (StatusCode, Value) {
        let resp = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .header(EXECUTOR_SECRET_HEADER, secret)
                    .body(Body::from(body.to_string()))
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

    async fn claim(&self) -> Claimed {
        let (status, body) = self
            .post(
                "/executors/claim",
                SECRET,
                json!({ "executor_id": self.id, "capabilities": [] }),
            )
            .await;
        match status {
            StatusCode::OK => Some(body),
            StatusCode::NO_CONTENT => None,
            other => panic!("unexpected claim status {other}: {body}"),
        }
    }

    async fn checkpoint(&self, job: &Value, state: Value) -> StatusCode {
        self.post(
            &format!("/jobs/{}/checkpoint", job["job_id"].as_str().unwrap()),
            SECRET,
            json!({ "executor_id": self.id, "attempt": job["attempt"], "state": state }),
        )
        .await
        .0
    }

    async fn heartbeat(&self, job: &Value) -> StatusCode {
        self.post(
            &format!("/jobs/{}/heartbeat", job["job_id"].as_str().unwrap()),
            SECRET,
            json!({ "executor_id": self.id, "attempt": job["attempt"] }),
        )
        .await
        .0
    }

    async fn finish_ok(&self, job: &Value, result: Value) -> (StatusCode, Value) {
        self.post(
            &format!("/jobs/{}/finish", job["job_id"].as_str().unwrap()),
            SECRET,
            json!({
                "executor_id": self.id,
                "attempt": job["attempt"],
                "result": result,
                "run_ms": 7,
            }),
        )
        .await
    }
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

async fn plane_state(
    stale_after_secs: u64,
) -> (crate::state::AppState, pumper_core::testing::TempStore) {
    test_state_with(vec![Arc::new(RemoteFake)], move |c| {
        c.executors.enabled = true;
        c.executors.secret = SECRET.to_string();
        // Keep the long poll short: these tests hit an empty queue on purpose.
        c.executors.claim_wait_secs = 1;
        c.worker.stale_after_secs = stale_after_secs;
        c.worker.heartbeat_secs = 1;
    })
    .await
}

/// **The gate.** Two executors drain one queue through the routes: every job
/// runs "remotely", finalizes on the coordinator, and its receipt says where it
/// ran.
#[tokio::test]
async fn two_executors_drain_one_queue_and_the_coordinator_finalizes() {
    let (state, _store) = plane_state(0).await;
    for i in 0..4 {
        state
            .storage
            .enqueue(
                "remote-fake",
                EnqueueOptions {
                    params: json!({ "i": i }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let router = routes::router(state.clone());
    let a = TestExecutor::new("vps-a", &router);
    let b = TestExecutor::new("vps-b", &router);

    let mut finished = Vec::new();
    for round in 0..4 {
        let who = if round % 2 == 0 { &a } else { &b };
        let job = who.claim().await.expect("a queued job is available");
        assert_eq!(job["app"], "remote-fake");
        let (status, body) = who
            .finish_ok(&job, json!({ "ran": job["params"]["i"].clone() }))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["outcome"], "succeeded");
        finished.push((job["job_id"].as_str().unwrap().to_string(), who.id.clone()));
    }
    // The queue is empty, and an empty queue answers 204 rather than blocking
    // forever or inventing a job.
    assert!(a.claim().await.is_none());

    // Both executors did work, and every job is succeeded with its result and
    // the executor that ran it — the fan-out ran on the coordinator.
    assert!(finished.iter().any(|(_, who)| who == "vps-a"));
    assert!(finished.iter().any(|(_, who)| who == "vps-b"));
    // The fan-out is off-slot (bounded pool), so wait for the terminal write to
    // land the way every other fan-out test does.
    settle(&state).await;
    for (id, who) in &finished {
        let (status, job) = get_json(&router, &format!("/jobs/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(job["status"], "succeeded", "{job}");
        assert_eq!(job["executor_id"], who.as_str());
        let (status, receipt) = get_json(&router, &format!("/jobs/{id}/receipt")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            receipt["job"]["executor_id"],
            who.as_str(),
            "the receipt has to say WHERE a job ran, or 'the share of jobs executed remotely' \
             is unanswerable after the fact: {receipt}"
        );
        // The only span the coordinator cannot measure is the one the executor
        // reported — and it must survive as a number, not become a null.
        assert_eq!(receipt["stages"]["run_ms"], 7, "{receipt}");
    }

    // `GET /executors` sees both, with their claim history.
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/executors")
                .header(EXECUTOR_SECRET_HEADER, SECRET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    let ids: Vec<&str> = body["executors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"vps-a") && ids.contains(&"vps-b"), "{body}");
    assert_eq!(body["eligible_apps"], json!(["remote-fake"]));
}

/// **The gate, second half.** An executor that stops heartbeating is reaped, its
/// job is re-claimed *with its checkpoint*, and its own late `finish` is refused
/// by the fence rather than overwriting the attempt that now owns the job.
#[tokio::test]
async fn a_reaped_executors_job_resumes_elsewhere_and_its_late_finish_is_refused() {
    let (state, _store) = plane_state(1).await;
    state
        .storage
        .enqueue(
            "remote-fake",
            EnqueueOptions {
                // Attempt headroom, so the reap re-queues rather than failing
                // permanently — the point of the test is the *resume*.
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let router = routes::router(state.clone());
    let dead = TestExecutor::new("vps-dead", &router);
    let live = TestExecutor::new("vps-live", &router);

    let job = dead.claim().await.expect("the queued job");
    assert_eq!(job["attempt"], 1);
    assert!(
        job["restored"].is_null(),
        "a first attempt restores nothing"
    );
    // It gets far enough to checkpoint, then dies: no more heartbeats.
    assert_eq!(
        dead.checkpoint(&job, json!({ "page": 3 })).await,
        StatusCode::OK
    );
    assert_eq!(dead.heartbeat(&job).await, StatusCode::OK);

    // The lease goes stale and the coordinator's reaper — untouched by N18 —
    // re-queues the job exactly as it does for a local task that wedged.
    tokio::time::sleep(std::time::Duration::from_millis(1400)).await;
    crate::worker::reap_once(&state).await;
    let row = state
        .storage
        .get(job["job_id"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap()
        .expect("job row");
    assert_eq!(
        row.status,
        pumper_core::JobStatus::Queued,
        "a dead executor's job must return to the queue, not sit running forever"
    );

    // The reap applied FAILURE semantics, so the row carries the jittered
    // retry backoff (~20s for attempt 1). Fast-forward that clock rather than
    // sleeping through it: the backoff ladder is `Storage::fail`'s tested
    // behaviour, and it is not what this test is about.
    sqlx::query("UPDATE jobs SET available_at = ?1")
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&state.storage.pool())
        .await
        .unwrap();

    // Another executor picks it up and is handed the checkpoint the dead one
    // pushed — the whole economic argument for executor loss being cheap.
    let resumed = live.claim().await.expect("the re-queued job");
    assert_eq!(resumed["attempt"], 2, "the re-claim advances the attempt");
    assert_eq!(resumed["restored"], json!({ "page": 3 }), "{resumed}");

    // And the corpse speaks: the dead executor comes back and reports its
    // result for attempt 1. Refused — twice over (wrong attempt AND wrong
    // executor), with a 409 rather than a silent discard.
    let (status, body) = dead.finish_ok(&job, json!({ "ran": "zombie" })).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "a late finish from a reaped executor must not overwrite the live attempt: {body}"
    );
    // A heartbeat from it is refused for the same reason, which is what tells a
    // partitioned executor to stop spending.
    assert_eq!(dead.heartbeat(&job).await, StatusCode::CONFLICT);
    // The live attempt still owns the job and can finish it.
    let (status, body) = live.finish_ok(&resumed, json!({ "ran": "for real" })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["outcome"], "succeeded");
}

/// The plane's two doors that are not about jobs at all: it does not exist when
/// disabled, and it refuses a wrong secret before it refuses anything else.
#[tokio::test]
async fn a_disabled_plane_is_absent_and_a_wrong_secret_is_refused() {
    let (state, _store) = test_state_with(vec![Arc::new(RemoteFake)], |_| {}).await;
    let router = routes::router(state);
    let anon = TestExecutor::new("nobody", &router);
    let (status, _) = anon
        .post(
            "/executors/claim",
            SECRET,
            json!({ "executor_id": "nobody" }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a disabled plane does not exist as far as a caller is concerned — there is no \
         'configured but open' state"
    );

    let (state, _store) = plane_state(0).await;
    let router = routes::router(state);
    let anon = TestExecutor::new("nobody", &router);
    let (status, _) = anon
        .post(
            "/executors/claim",
            "not-the-secret",
            json!({ "executor_id": "nobody" }),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // An anonymous executor cannot be fenced, reaped or reported on, so it is
    // refused at the door rather than handed a job nobody can attribute.
    let (status, _) = anon
        .post("/executors/claim", SECRET, json!({ "executor_id": "  " }))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Waits for the off-slot fan-out to finish the work a `finish` handed it.
///
/// The completion write lands synchronously inside `finish`; everything after it
/// (indexing, gates, hooks, the terminal event) runs on the bounded pool, so a
/// test that reads `GET /jobs/{id}` immediately is racing the pool, not the
/// queue. Draining it is how every other fan-out test settles.
async fn settle(state: &crate::state::AppState) {
    state.fanout.drain(std::time::Duration::from_secs(5)).await;
}
