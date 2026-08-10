//! Budget exhaustion is a **terminal** job failure, not a transient one.
//!
//! The anti-pattern these tests defend against: the worker treated every app
//! error as retryable, so a job whose `budget_usd` ran out three seconds into
//! attempt 1 was re-queued with backoff. Attempt 2 re-seeded `spent_usd` from
//! the cost ledger (the money really was spent), re-refused on its first metered
//! call, and so on — every remaining attempt and every backoff second burned
//! producing the same refusal, with nothing an operator could do to make the
//! retry succeed.
//!
//! Second lie, same path: a DataHub `cost:pause` tag forces the job's budget to
//! `$0`, so the refusal read "job budget of $0.00 exhausted" — money that was
//! never spent, and a pointer away from the tag actually holding the app.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pumper_core::{AppContext, EnqueueOptions, JobStatus, ResearchRequest, Result, ScrapeApp};
use serde_json::{json, Value};

use super::harness::test_state;
use crate::worker;

/// Spends its whole ceiling, then asks for one more metered call — the shape of
/// every real budget exhaustion (a fan-out that runs out of money part-way).
/// Counts its own runs so "how many attempts did this cost?" is observable.
struct SpendsThenAsksForMore {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ScrapeApp for SpendsThenAsksForMore {
    fn name(&self) -> &'static str {
        "burner"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        // First half of the run succeeds and spends the ceiling...
        let spend = ctx.remaining_budget_usd().await?.unwrap_or(0.0);
        ctx.meter("claude", None, spend, Some("first half of the run"))
            .await;
        // ...then the second half asks for more and is refused.
        ctx.research(ResearchRequest::new("the expensive second half"))
            .await?;
        Ok(json!({ "unreachable": true }))
    }
}

/// The core assertion: ONE attempt, permanently failed, remaining attempts
/// un-burned — a human can read `attempts: 1 / max_attempts: 5` and see that
/// the runtime stopped on purpose rather than exhausting the ladder.
#[tokio::test]
async fn budget_exhaustion_fails_once_not_into_retry_burn() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (state, _store) =
        test_state(vec![Arc::new(SpendsThenAsksForMore { runs: runs.clone() })]).await;
    let job = state
        .storage
        .enqueue(
            "burner",
            EnqueueOptions {
                budget_usd: Some(0.50),
                max_attempts: 5,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(worker::run_one(&state).await, "the job is claimed");

    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        JobStatus::Failed,
        "budget exhaustion is terminal — the job must not be re-queued for a retry \
         that would re-read the same ledger and re-refuse"
    );
    assert_eq!(row.attempts, 1, "exactly one attempt was spent");
    assert_eq!(
        row.max_attempts, 5,
        "the attempt budget is left visibly un-burned"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1, "the app ran once");
    let err = row.error.unwrap_or_default();
    assert!(
        err.contains("budget") && err.contains("0.50"),
        "the stored reason must name the ceiling: {err}"
    );

    // And it stays failed: nothing becomes claimable later.
    assert!(
        !worker::run_one(&state).await,
        "a terminally-failed job must not come back as due work"
    );
}

/// A retryable failure from the same runtime must still walk the ladder — the
/// classification is narrow, and over-classifying would silently take every
/// job's retries away.
#[tokio::test]
async fn an_ordinary_app_error_is_still_retried() {
    let (state, _store) = test_state(vec![Arc::new(super::harness::FakeApp)]).await;
    let job = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params: json!({ "fail": "transient upstream 503" }),
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(worker::run_one(&state).await);
    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        JobStatus::Queued,
        "an ordinary failure is re-queued for a retry with backoff"
    );
    assert_eq!(row.attempts, 1);
}

/// An app whose paid work is held by governance must say so. The refusal an
/// operator reads is the one place the `$0` forced budget stops being an
/// implementation detail and starts being a diagnosis.
#[tokio::test]
async fn governance_paused_job_says_paused_not_budget_exhausted() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (state, _store) =
        test_state(vec![Arc::new(SpendsThenAsksForMore { runs: runs.clone() })]).await;
    // What the governance poll leaves behind when it sees a `cost:pause` tag.
    state
        .datahub_govern
        .lock()
        .unwrap()
        .paused_apps
        .insert("burner".to_string());

    let job = state
        .storage
        .enqueue(
            "burner",
            EnqueueOptions {
                // The caller asked for real money; governance overrides it to $0.
                budget_usd: Some(5.00),
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(worker::run_one(&state).await);
    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.attempts, 1, "a governance hold is not worth 3 attempts");
    let err = row.error.unwrap_or_default();
    assert!(
        err.contains("cost:pause") && err.contains("burner"),
        "the refusal must name the governance tag and the app: {err}"
    );
    // The exact lie this replaces was `job budget of $0.00 exhausted`. The
    // claim form is `budget of $…`; the message may (and does) still use the
    // word "exhausted" to explicitly deny it.
    assert!(
        !err.contains("budget of $"),
        "and must not claim a spend that never happened: {err}"
    );

    // The job never reached the model, and the $0 clamp means it never spent.
    let total = state.costs.job_total(job.id).await.unwrap();
    assert_eq!(total, 0.0, "a paused job spends nothing");
}

/// The whole point of failing once is that the operator finds out once. A
/// terminal failure still runs the normal terminal fan-out (callback + events),
/// exactly like an attempts-exhausted failure does.
#[tokio::test]
async fn a_terminal_budget_failure_still_notifies() {
    let receiver = super::harness::TestReceiver::spawn(vec![200]).await;
    let (state, _store) = test_state(vec![Arc::new(SpendsThenAsksForMore {
        runs: Arc::new(AtomicUsize::new(0)),
    })])
    .await;
    let job = state
        .storage
        .enqueue(
            "burner",
            EnqueueOptions {
                budget_usd: Some(0.25),
                max_attempts: 2,
                callback_url: Some(receiver.url()),
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(worker::run_one(&state).await);
    let hits = receiver.wait_hits(1, Duration::from_secs(5)).await;
    let body: Value = serde_json::from_slice(&hits[0].1).expect("json callback body");
    assert_eq!(body["id"], job.id.to_string());
    assert_eq!(body["status"], "failed");
}
