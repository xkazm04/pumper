//! **When** a failed job runs again, once that decision can see the error.
//!
//! Two anti-patterns, one axis. The job ladder computed `10 * 2^attempts` from
//! the attempt number and nothing else, so:
//!
//! - a `429` whose `Retry-After: 600` the HTTP engine had already read,
//!   honoured as a floor, and then given up on (`capped_retry_sleep`) was
//!   re-queued in 10 seconds — running the whole fetch back into the rate limit
//!   the origin had asked us to wait ten minutes for, once per attempt;
//! - an *ours*-error — one `Error::is_router_failure` already stops the tier
//!   ladder for, because pumper was the variable and not the origin — rode
//!   10/20/40 producing the identical sentence three times, which is the exact
//!   amplification that predicate exists to end, ended one layer down and not
//!   here.
//!
//! Driven on the trait-level fake seam: a scripted `HttpClient` an app reaches
//! through `ctx.engines.http` and propagates with `?`, which is how every
//! API-shaped app in `crates/apps/` raises a transport failure.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use pumper_core::engine::{HttpClient, HttpRequest, HttpResponse};
use pumper_core::testing::engines_with;
use pumper_core::{AppContext, EnqueueOptions, Error, JobStatus, Result, ScrapeApp};
use serde_json::{json, Value};

use super::harness::test_state_engines;
use crate::state::AppState;
use crate::worker;

/// How far the harness will fast-forward a job's `available_at` — one minute,
/// the window in which a rate-limited origin is asked again.
///
/// A retry the ladder placed inside this window really would have run inside it,
/// so advancing the row to `now` is a faithful simulation of waiting. A retry
/// placed *beyond* it is not fast-forwarded, because the whole point of the
/// server-stated wait is that we do not get to shorten it.
const OBSERVED_WINDOW: chrono::Duration = chrono::Duration::seconds(60);

/// An `HttpClient` that always fails the same way, counting its invocations.
struct FailingHttp {
    calls: Arc<AtomicUsize>,
    make: fn() -> Error,
}

#[async_trait]
impl HttpClient for FailingHttp {
    async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err((self.make)())
    }
}

/// The shape of every API-backed app in the tree: one engine call, propagated
/// with `?` so the typed error is what the worker sees.
struct ApiApp;

#[async_trait]
impl ScrapeApp for ApiApp {
    fn name(&self) -> &'static str {
        "api-app"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        ctx.engines
            .http
            .fetch(HttpRequest::get("https://origin.test/api"))
            .await?;
        Ok(json!({ "ok": true }))
    }
}

/// The failure `engine-http` mints when it has read a `Retry-After` it cannot
/// fit inside the fetch budget: retryable, and carrying the number.
fn rate_limited() -> Error {
    Error::http_after(
        "https://origin.test/api exhausted its end-to-end fetch budget — last error: status 429",
        Duration::from_secs(600),
    )
}

/// A configuration failure — pumper's own, identical on every attempt, and
/// already the thing that stops the tier ladder.
fn ours() -> Error {
    Error::Config("missing api key for origin.test".into())
}

async fn state_for(
    make: fn() -> Error,
) -> (AppState, pumper_core::testing::TempStore, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let http = Arc::new(FailingHttp {
        calls: calls.clone(),
        make,
    });
    let (state, store) = test_state_engines(
        vec![Arc::new(ApiApp)],
        engines_with(
            http,
            Arc::new(pumper_core::testing::Dead),
            Arc::new(pumper_core::testing::Dead),
        ),
    )
    .await;
    (state, store, calls)
}

/// Runs the job to a terminal state, or until its next attempt is placed beyond
/// the observed window.
///
/// Between passes it pulls a due-soon `available_at` forward to now, which is
/// what waiting would have done. It refuses to pull one that sits past
/// [`OBSERVED_WINDOW`] — so a policy that honours a ten-minute wait ends the
/// loop rather than being fast-forwarded through the very delay under test.
async fn drive(state: &AppState, id: uuid::Uuid) -> pumper_core::Job {
    for _ in 0..10 {
        if !worker::run_one(state).await {
            break;
        }
        let job = state.storage.get(id).await.unwrap().unwrap();
        if job.status != JobStatus::Queued {
            break;
        }
        if job.available_at > Utc::now() + OBSERVED_WINDOW {
            break;
        }
        sqlx::query("UPDATE jobs SET available_at = ?1 WHERE id = ?2")
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(&state.storage.pool())
            .await
            .unwrap();
    }
    state.storage.get(id).await.unwrap().unwrap()
}

/// THE measurable. A rate-limited origin that stated 600 seconds, and a job with
/// three attempts: engine invocations inside the first minute, and where the
/// next attempt was placed.
///
/// Before the policy: 3 invocations (10s and 20s both fall inside the window),
/// and `available_at` ~10 seconds out. After: 1, and at least 600 seconds out.
#[tokio::test]
async fn a_stated_rate_limit_is_not_re_asked_on_the_ladders_schedule() {
    let (state, _store, calls) = state_for(rate_limited).await;
    let job = state
        .storage
        .enqueue(
            "api-app",
            EnqueueOptions {
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let before = Utc::now();
    let row = drive(&state, job.id).await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the origin asked us to wait 600s; every extra call is a request it \
         explicitly refused"
    );
    assert_eq!(row.status, JobStatus::Queued, "the job keeps its attempts");
    assert_eq!(row.attempts, 1);
    let delay = (row.available_at - before).num_seconds();
    assert!(
        delay >= 600,
        "the next attempt is {delay}s out; the server asked for 600"
    );
    assert_eq!(
        row.requeue_reason.as_deref(),
        Some(pumper_core::storage::REQUEUE_REASON_STATED),
        "the row says which arm chose the delay, or the ladder is smarter and \
         unpredictable at the same time"
    );
}

/// The second number, and the direct continuation of the router-failure axis one
/// layer up: a config failure is identical on attempt 2 and 3, so the ladder can
/// only re-derive it and bill for the backoff in between.
///
/// Before: 3 attempts. After: 1.
#[tokio::test]
async fn an_ours_error_fails_once_instead_of_three_times() {
    let (state, _store, calls) = state_for(ours).await;
    let job = state
        .storage
        .enqueue(
            "api-app",
            EnqueueOptions {
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let row = drive(&state, job.id).await;

    assert_eq!(
        row.status,
        JobStatus::Failed,
        "an ours-error is terminal here"
    );
    assert_eq!(row.attempts, 1, "it must not ride 10/20/40 to say it again");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        row.requeue_reason.as_deref(),
        Some(pumper_core::storage::REQUEUE_REASON_OURS),
        "a job that stopped being retried must say why on its own row"
    );
    // The sentence is still the origin-naming one the fetch produced — the
    // policy changed WHEN, not what the row says happened.
    assert!(
        row.error
            .as_deref()
            .unwrap_or_default()
            .starts_with("config:"),
        "{:?}",
        row.error
    );
}

/// The control for both numbers above: a failure the policy has no opinion about
/// still rides the ladder it always rode, three attempts and all.
#[tokio::test]
async fn an_ordinary_transport_failure_still_rides_the_whole_ladder() {
    let (state, _store, calls) = state_for(|| Error::http("connection reset")).await;
    let job = state
        .storage
        .enqueue(
            "api-app",
            EnqueueOptions {
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let row = drive(&state, job.id).await;

    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(
        row.attempts, 3,
        "the ladder must be intact for theirs-errors"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}
