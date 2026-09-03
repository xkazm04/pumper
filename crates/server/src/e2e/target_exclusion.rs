//! In-flight exclusion **by target**, and the paired proof that it did not
//! become a serialiser.
//!
//! The anti-pattern: the only exclusion the queue had was per *app*, and it
//! exists for fairness (one busy app must not own every slot), not for mutual
//! exclusion. Within one app's budget, two jobs that write the same dataset
//! rows claimed two slots and ran at once — a scheduled run overlapping a manual
//! re-run of the same source, two trigger hops onto one target — and nothing
//! anywhere refused them. `idempotency_key` does not cover it: it refuses a
//! second *enqueue*, and says nothing about two rows already in the table.
//!
//! Driven through the two doors that legitimately skip dedup (the cron fire
//! path and `POST /apps/{name}/jobs` with no idempotency key) and the REAL
//! worker loop, because the guarantee is a property of the claim statement — a
//! test that called `claim_next` by hand would pass with the check in the worker
//! instead of in the SQL, which is the one place it must not be.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use http_body_util::BodyExt;
use pumper_core::{AppContext, JobStatus, Result, ScrapeApp};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state_with, wait_status, WorkerLoop};
use crate::state::AppState;
use crate::{routes, scheduler};

/// Names its target from `params.target` and records the peak number of its own
/// concurrent runs. Each run holds until `WANT` of them overlap or the deadline
/// passes, so "they overlapped" is proven by a condition and "they did not" costs
/// exactly one deadline rather than being a hopeful sleep.
struct TargetProbe {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

/// How many concurrent runs a probe waits for before finishing.
const WANT: usize = 2;
/// How long it waits for them. Long enough that a genuine overlap is never
/// missed on a loaded machine, short enough that the held case is quick.
const OVERLAP_WINDOW: Duration = Duration::from_millis(750);

#[async_trait::async_trait]
impl ScrapeApp for TargetProbe {
    fn name(&self) -> &'static str {
        "targeted"
    }
    fn description(&self) -> &'static str {
        "records its own peak concurrency, per target"
    }
    /// An app that CAN name what it acts on. A job without a `target` param
    /// names none — the opt-out that every app has by default, kept exercisable
    /// here because "a null key is never held" is half the contract.
    fn target_key(&self, params: &Value) -> Option<String> {
        params
            .get("target")
            .and_then(Value::as_str)
            .map(str::to_string)
    }
    async fn run(&self, _ctx: AppContext) -> Result<Value> {
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + OVERLAP_WINDOW;
        while self.live.load(Ordering::SeqCst) < WANT && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.live.fetch_sub(1, Ordering::SeqCst);
        Ok(json!({ "ok": true }))
    }
}

/// The state every case here shares: global concurrency well above 2 and NO
/// per-app cap, so nothing but the target exclusion can hold two jobs apart.
async fn probe_state() -> (AppState, pumper_core::testing::TempStore, Arc<AtomicUsize>) {
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let app = Arc::new(TargetProbe {
        live,
        peak: peak.clone(),
    });
    let (state, store) = test_state_with(vec![app], |c| {
        c.worker.concurrency = 4;
        c.worker.default_app_concurrency = 0;
    })
    .await;
    (state, store, peak)
}

/// `POST /apps/targeted/jobs` with no idempotency key — one of the two doors
/// that deliberately creates a second row for the same target.
async fn post_job(router: &axum::Router, params: Value) -> uuid::Uuid {
    let req = Request::builder()
        .method("POST")
        .uri("/apps/targeted/jobs")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({ "params": params })).unwrap(),
        ))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    body["id"].as_str().expect("job id").parse().unwrap()
}

/// The other door: a due schedule fired through the real reconcile pass, which
/// calls plain `enqueue` (no key) because the overlap guard covers its own
/// stacking. Returns the job it created.
async fn fire_schedule(state: &AppState, router: &axum::Router, params: Value) -> uuid::Uuid {
    let req = Request::builder()
        .method("POST")
        .uri("/schedules")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "app": "targeted",
                "cron": "0 * * * * *",
                "params": params,
            }))
            .unwrap(),
        ))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let created: Value = serde_json::from_slice(&bytes).unwrap();
    let id = created["id"].as_str().expect("schedule id").to_string();
    sqlx::query("UPDATE schedules SET created_at = ?1 WHERE id = ?2")
        .bind((Utc::now() - chrono::Duration::minutes(5)).to_rfc3339())
        .bind(&id)
        .execute(&state.storage.pool())
        .await
        .unwrap();
    scheduler::reconcile(state, &mut HashMap::new(), None, Utc::now())
        .await
        .unwrap();
    let job = state
        .storage
        .list(Some("targeted"), None, 50)
        .await
        .unwrap()
        .into_iter()
        .find(|j| j.schedule_id.is_some())
        .expect("the schedule fired");
    job.id
}

/// THE measurable. Two jobs of one app against one target, from two doors, with
/// four slots free and no per-app cap: the peak number of concurrent runs must
/// be 1, and the two rows' run windows must not overlap.
///
/// Before the exclusion: 2.
#[tokio::test]
async fn two_jobs_against_one_target_never_run_at_the_same_time() {
    let (state, _store, peak) = probe_state().await;
    let router = routes::router(state.clone());
    let scheduled = fire_schedule(&state, &router, json!({ "target": "alpha" })).await;
    let manual = post_job(&router, json!({ "target": "alpha" })).await;

    let worker = WorkerLoop::start(&state);
    let a = wait_status(
        &state,
        scheduled,
        JobStatus::Succeeded,
        Duration::from_secs(30),
    )
    .await;
    let b = wait_status(
        &state,
        manual,
        JobStatus::Succeeded,
        Duration::from_secs(30),
    )
    .await;
    worker.shutdown(&state, Duration::from_secs(15)).await;

    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "two jobs sharing a target_key ran concurrently"
    );
    // The row-level statement of the same fact, which is what an operator (and a
    // dataset's provenance) can actually check after the run.
    let (first, second) = if a.started_at <= b.started_at {
        (&a, &b)
    } else {
        (&b, &a)
    };
    assert!(
        first.finished_at.unwrap() <= second.started_at.unwrap(),
        "the run windows overlap: {first:?} / {second:?}"
    );
    // Non-starvation: being held is not being dropped. Both rows carry the key.
    assert_eq!(a.target_key.as_deref(), Some("alpha"));
    assert_eq!(
        b.target_key.as_deref(),
        Some("alpha"),
        "both doors stamp the key, or the exclusion only covers one of them"
    );
}

/// The paired assertion, and the one that keeps the fix from becoming the
/// per-app cap in disguise: two jobs of the SAME app with different targets must
/// still run at the same time. Without this, a peak of 1 above proves nothing —
/// a serialiser would pass it too, and would cost throughput silently and
/// continuously in exchange for preventing a rare double-write.
#[tokio::test]
async fn two_jobs_against_different_targets_still_run_concurrently() {
    let (state, _store, peak) = probe_state().await;
    let router = routes::router(state.clone());
    let alpha = post_job(&router, json!({ "target": "alpha" })).await;
    let beta = post_job(&router, json!({ "target": "beta" })).await;

    let worker = WorkerLoop::start(&state);
    wait_status(&state, alpha, JobStatus::Succeeded, Duration::from_secs(30)).await;
    wait_status(&state, beta, JobStatus::Succeeded, Duration::from_secs(30)).await;
    worker.shutdown(&state, Duration::from_secs(15)).await;

    assert_eq!(
        peak.load(Ordering::SeqCst),
        2,
        "different targets must still overlap; the exclusion has widened into a serialiser"
    );
}

/// The opt-out, which is what makes this safe to land with two apps overriding
/// `target_key` and every other app untouched: a job that names no target is
/// never held, not even by another job that also names none.
#[tokio::test]
async fn jobs_without_a_target_key_are_never_held() {
    let (state, _store, peak) = probe_state().await;
    let router = routes::router(state.clone());
    let one = post_job(&router, json!({})).await;
    let two = post_job(&router, json!({})).await;

    let worker = WorkerLoop::start(&state);
    let a = wait_status(&state, one, JobStatus::Succeeded, Duration::from_secs(30)).await;
    wait_status(&state, two, JobStatus::Succeeded, Duration::from_secs(30)).await;
    worker.shutdown(&state, Duration::from_secs(15)).await;

    assert_eq!(a.target_key, None, "no target param, no key");
    assert_eq!(
        peak.load(Ordering::SeqCst),
        2,
        "a NULL target_key must not be a shared key: every app that has not \
         overridden target_key would be serialised against every other"
    );
}

/// The operator's half: the queue depth splits into *behind* and *held*, and
/// only the first is a capacity finding. A held job needs no worker — it needs
/// the run in front of it to finish.
#[tokio::test]
async fn a_held_job_is_reported_separately_from_a_backlog() {
    let (state, _store, _peak) = probe_state().await;
    let router = routes::router(state.clone());
    post_job(&router, json!({ "target": "alpha" })).await;
    post_job(&router, json!({ "target": "alpha" })).await;
    post_job(&router, json!({ "target": "beta" })).await;
    assert_eq!(
        state.storage.held_by_target().await.unwrap(),
        0,
        "nothing is running yet, so nothing is held — three jobs are simply due"
    );

    // Claim one: its target is now held, and exactly one of the remaining two is
    // waiting on it rather than on a worker.
    let claimed = state.storage.claim_next(&[], 0.0).await.unwrap().unwrap();
    assert_eq!(claimed.target_key.as_deref(), Some("alpha"));
    assert_eq!(
        state.storage.held_by_target().await.unwrap(),
        1,
        "the second alpha job is held; the beta job is not"
    );
    // And the claim agrees with the gauge: the next row it hands out is the beta
    // one, not the held alpha one.
    let next = state.storage.claim_next(&[], 0.0).await.unwrap().unwrap();
    assert_eq!(next.target_key.as_deref(), Some("beta"));
    assert!(
        state.storage.claim_next(&[], 0.0).await.unwrap().is_none(),
        "the held job is not claimable while its target is running"
    );
}
