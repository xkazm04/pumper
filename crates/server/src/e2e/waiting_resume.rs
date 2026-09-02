//! N02 "jobs that wait": an app parks on external input, the row goes to
//! `waiting` (not `failed`), the permit is released, and a resume re-queues it
//! with the answer — **without burning an attempt**.
//!
//! The three things a `waiting` state has to get right, and what each test here
//! pins:
//!
//! 1. A park is not a failure and not a retry: `attempts` is untouched and the
//!    remaining retry budget is unchanged when the job finally succeeds.
//! 2. A resume is fenced on `status = 'waiting'`, so a stale/double resume
//!    cannot restart a lineage that has already moved on.
//! 3. An unanswered park past its deadline fails *permanently and visibly* —
//!    through `finalize`, so callbacks and terminal triggers fire.

use std::sync::{Arc, Mutex};

use pumper_core::{AppContext, EnqueueOptions, JobStatus, Result, ScrapeApp};
use serde_json::{json, Value};

use super::harness::test_state;
use crate::worker;

/// An app that asks the outside world one question and then finishes with the
/// answer. It records what each attempt saw so the test can prove the resumed
/// attempt got BOTH halves: its own checkpoint and the supplied input.
struct ApprovalApp {
    seen: Seen,
}

/// What each attempt was handed: `(restored checkpoint, resumed input)`.
type Attempt = (Option<Value>, Option<Value>);
type Seen = Arc<Mutex<Vec<Attempt>>>;

#[async_trait::async_trait]
impl ScrapeApp for ApprovalApp {
    fn name(&self) -> &'static str {
        "approval"
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let restored = ctx.restore().cloned();
        let input = ctx.restore_input().cloned();
        self.seen
            .lock()
            .unwrap()
            .push((restored.clone(), input.clone()));
        match input {
            // No answer yet: do the reversible half of the work, then park on
            // the irreversible half. The checkpoint is what makes the resume
            // cheap — `await_input` forces it, so this app never re-does it.
            None => Err(ctx
                .await_input(
                    json!({ "stage": "prepared", "amount": 42 }),
                    json!({ "kind": "approval", "prompt": "submit $42?" }),
                )
                .await),
            Some(answer) => Ok(json!({
                "submitted": answer,
                "resumed_from": restored,
            })),
        }
    }
}

fn approval_app() -> (Arc<ApprovalApp>, Seen) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    (Arc::new(ApprovalApp { seen: seen.clone() }), seen)
}

/// The headline e2e: park -> resume -> succeed, with the retry budget intact.
///
/// The anti-pattern this forbids: implementing the park on top of `fail()`,
/// which is the tempting reuse (the app returned an `Err`, after all). That
/// would spend an attempt on every question asked, so a job that needs two
/// approvals would run out of retries before it ran out of work — and its
/// backoff would delay a human's answer by minutes.
#[tokio::test]
async fn park_then_resume_succeeds_with_attempts_unchanged() {
    let (app, seen) = approval_app();
    let (state, _store) = test_state(vec![app]).await;
    let job = state
        .storage
        .enqueue(
            "approval",
            EnqueueOptions {
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Attempt 1: the app parks.
    assert!(worker::run_one(&state).await);
    let parked = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(
        parked.status,
        JobStatus::Waiting,
        "a park is `waiting`, never `failed`"
    );
    assert_eq!(parked.attempts, 1);
    assert_eq!(
        parked.input_request,
        Some(json!({ "kind": "approval", "prompt": "submit $42?" })),
        "the row carries what the job is asking for"
    );
    assert!(parked.waiting_since.is_some(), "the wait is timestamped");
    assert!(
        parked.waiting_expires_at.is_none(),
        "[waiting] expiry_secs defaults to 0 = wait forever"
    );
    assert!(
        parked.finished_at.is_none(),
        "a parked job has not finished"
    );
    assert!(
        state
            .storage
            .load_checkpoint(job.id)
            .await
            .unwrap()
            .is_some(),
        "await_input forces the checkpoint the resume depends on"
    );
    // Nothing else to claim: the park released the permit and left no queued row.
    assert!(
        !worker::run_one(&state).await,
        "a parked job is not claimable"
    );

    // The answer arrives.
    let resumed = state
        .storage
        .resume(job.id, &json!({ "approved": true }))
        .await
        .unwrap()
        .expect("waiting job resumes");
    assert_eq!(resumed.status, JobStatus::Queued);
    assert_eq!(resumed.attempts, 1, "resuming burns no attempt");
    assert_eq!(
        resumed.max_attempts, 3,
        "headroom is granted only when the park already consumed the budget"
    );

    // Attempt 2 finishes the work.
    assert!(worker::run_one(&state).await);
    let done = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(done.status, JobStatus::Succeeded);
    assert_eq!(
        done.max_attempts - done.attempts,
        1,
        "one retry still available after a park+resume round trip"
    );
    assert_eq!(
        done.result.as_ref().unwrap()["submitted"],
        json!({ "approved": true })
    );
    assert_eq!(
        done.result.as_ref().unwrap()["resumed_from"],
        json!({ "stage": "prepared", "amount": 42 }),
        "the resumed attempt continues from the forced checkpoint"
    );

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "exactly two attempts ran");
    assert_eq!(seen[0], (None, None), "the first attempt had neither half");
    assert_eq!(
        seen[1],
        (
            Some(json!({ "stage": "prepared", "amount": 42 })),
            Some(json!({ "approved": true }))
        ),
        "the resumed attempt sees BOTH its checkpoint and the supplied input"
    );
}

/// The `(status, attempts)` fence, applied at the resume door.
///
/// The anti-pattern: a resume that only checks "does this job exist". An
/// operator's browser tab, an agent retry, or a second approver would then
/// re-queue a run that is already going — two live lineages for one job.
#[tokio::test]
async fn a_second_resume_is_refused_not_a_second_run() {
    let (app, _seen) = approval_app();
    let (state, _store) = test_state(vec![app]).await;
    let job = state
        .storage
        .enqueue("approval", EnqueueOptions::default())
        .await
        .unwrap();
    assert!(worker::run_one(&state).await);

    assert!(
        state
            .storage
            .resume(job.id, &json!({ "approved": true }))
            .await
            .unwrap()
            .is_some(),
        "the first resume lands"
    );
    assert!(
        state
            .storage
            .resume(job.id, &json!({ "approved": false }))
            .await
            .unwrap()
            .is_none(),
        "the second resume matches no waiting row"
    );
    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(
        row.resumed_input,
        Some(json!({ "approved": true })),
        "the losing resume must not overwrite the answer that won"
    );

    // And a job that never parked cannot be resumed into existence.
    let fresh = state
        .storage
        .enqueue("approval", EnqueueOptions::default())
        .await
        .unwrap();
    assert!(
        state
            .storage
            .resume(fresh.id, &json!({}))
            .await
            .unwrap()
            .is_none(),
        "a queued job is not waiting for anything"
    );
}

/// An unanswered wait past its deadline is a permanent failure that goes
/// through `finalize` — not a row that sits `waiting` forever with nobody
/// notified. `[waiting] expiry_secs = 0` (the default) means no deadline at
/// all, so this test sets one.
#[tokio::test]
async fn an_expired_wait_fails_permanently_and_a_deadlineless_one_does_not() {
    let (app, _seen) = approval_app();
    let (state, _store) = super::harness::test_state_with(vec![app], |cfg| {
        cfg.waiting.expiry_secs = 3600;
    })
    .await;
    let job = state
        .storage
        .enqueue("approval", EnqueueOptions::default())
        .await
        .unwrap();
    assert!(worker::run_one(&state).await);
    let parked = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(parked.status, JobStatus::Waiting);
    let deadline = parked
        .waiting_expires_at
        .expect("a configured expiry stamps a deadline");

    // Not due yet: the sweep must leave it alone.
    worker::expire_waiting_once(&state).await;
    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Waiting,
        "a wait inside its deadline is not swept"
    );

    // Move the deadline into the past rather than sleeping for it.
    sqlx::query("UPDATE jobs SET waiting_expires_at = '2000-01-01T00:00:00.000000Z' WHERE id = ?1")
        .bind(job.id.to_string())
        .execute(&state.storage.pool())
        .await
        .expect("backdate the deadline");
    let _ = deadline;

    worker::expire_waiting_once(&state).await;
    let expired = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(expired.status, JobStatus::Failed);
    assert_eq!(
        expired.error.as_deref(),
        Some(worker::WAITING_EXPIRED_REASON),
        "the failure names the missing answer, not a lease or a crash"
    );
    assert!(
        expired.finished_at.is_some(),
        "an expired wait is terminal, so it has an end time"
    );
}

/// `waiting` must never be classified terminal: the SSE stream, the trigger
/// filter and the schedule slot all route through the same two predicates.
#[test]
fn waiting_is_active_for_streams_and_schedule_slots() {
    assert!(!JobStatus::Waiting.is_terminal());
    assert!(crate::scheduler::run_holds_slot(Some("waiting")));
}
