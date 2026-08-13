//! Which attempt's recording ends up in a `record: true` job's cassette.
//!
//! `crates/core/tests/vcr.rs` pins the recorder's own attempt policy; this pins
//! the WIRING — that the worker picks a fresh recorder for an attempt starting
//! from scratch and a resuming one for an attempt that restored a durable
//! checkpoint, and that it applies the policy before the app runs.
//!
//! The apps here record cassette entries through `ctx.vcr` directly rather than
//! through a fetch, because the e2e harness deliberately wires panicking
//! engines. What is under test is the attempt policy, not the fetcher — the
//! fetch path into the recorder is covered by the core suite.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pumper_core::vcr::{fetch_entry, Vcr};
use pumper_core::{
    AppContext, Cassette, EnqueueOptions, Error, FetchOutcome, FetchRequest, JobStatus, Result,
    ScrapeApp,
};
use serde_json::{json, Value};

use super::harness::{test_state, WorkerLoop};
use crate::worker;

/// Records one synthetic fetch into whatever cassette this attempt was given.
async fn record(ctx: &AppContext, url: &str, html: &str) {
    let Vcr::Record(recorder) = &ctx.vcr else {
        panic!("this app only runs under `record: true`");
    };
    recorder
        .record(fetch_entry(&FetchOutcome {
            url: url.to_string(),
            engine: "http",
            status: Some(200),
            html: Some(html.to_string()),
            markdown: None,
            text: None,
            escalations: Vec::new(),
            trace: Vec::new(),
            cost_usd: None,
            snapshot: None,
        }))
        .await;
}

/// The cassette a finished job left on disk.
async fn cassette_of(
    store: &pumper_core::testing::TempStore,
    app: &str,
    id: uuid::Uuid,
) -> Cassette {
    let dir = store.storage.artifacts_dir.join(app).join(id.to_string());
    Cassette::load(&dir, id)
        .await
        .expect("the job must have written a cassette")
}

/// Asserts the recorded body for `url` was produced by `tag` and by nothing else.
fn assert_recorded_by(cassette: &Cassette, url: &str, tag: &str) {
    let entry = cassette
        .resolve("GET", url, url)
        .unwrap_or_else(|e| panic!("{url} must be replayable: {e}"));
    let html = entry.body.as_ref().unwrap()["html"].as_str().unwrap();
    assert!(
        html.contains(tag),
        "{url} should carry {tag}'s recording, got: {html}"
    );
}

// ── Fresh attempt: the failed attempt's recording is discarded ───────────────

/// Attempt 1 records half the run and fails; attempt 2 starts from scratch and
/// records the whole run.
struct RetryingRecorder {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ScrapeApp for RetryingRecorder {
    fn name(&self) -> &'static str {
        "taper"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let n = self.runs.fetch_add(1, Ordering::SeqCst) + 1;
        record(&ctx, "https://x/page-1", &format!("attempt-{n} page-1")).await;
        if n == 1 {
            return Err(Error::App("upstream hiccup, half-way through".into()));
        }
        record(&ctx, "https://x/page-2", &format!("attempt-{n} page-2")).await;
        Ok(json!({ "ok": true }))
    }
}

/// The anti-pattern: a retried record job's cassette held attempt 1's partial
/// recording, and because entries load first-wins, replaying the job served the
/// data from the attempt that FAILED — while reporting itself deterministic.
#[tokio::test]
async fn retry_does_not_replay_the_failed_attempt() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (state, store) = test_state(vec![Arc::new(RetryingRecorder { runs: runs.clone() })]).await;
    let job = state
        .storage
        .enqueue(
            "taper",
            EnqueueOptions {
                params: json!({ "record": true }),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(worker::run_one(&state).await, "attempt 1 claims");
    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Failed,
        "attempt 1 failed as scripted"
    );

    // `POST /jobs/{id}/retry`: re-queued with one more attempt, available now.
    state.storage.retry(job.id).await.unwrap().expect("retried");
    assert!(worker::run_one(&state).await, "attempt 2 claims");
    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(runs.load(Ordering::SeqCst), 2, "two attempts ran");

    let cassette = cassette_of(&store, "taper", job.id).await;
    assert_eq!(
        cassette.len(),
        2,
        "the cassette is attempt 2's run, not both attempts' entries"
    );
    assert_recorded_by(&cassette, "https://x/page-1", "attempt-2");
    assert_recorded_by(&cassette, "https://x/page-2", "attempt-2");
    let stale = cassette
        .resolve("GET", "https://x/page-1", "page-1")
        .unwrap()
        .body
        .clone();
    assert!(
        !stale.unwrap()["html"]
            .as_str()
            .unwrap()
            .contains("attempt-1"),
        "the failed attempt's recording must not survive into the replay"
    );
}

// ── Resumed attempt: the earlier recording IS the work being skipped ─────────

/// Records page 1, checkpoints, then hangs so the graceful-shutdown drain has
/// to suspend it. The resumed attempt picks up at page 2 and never re-fetches
/// page 1 — so page 1's recording has to survive, or the replay has a hole.
struct SuspendingRecorder {
    /// Fires once page 1 is recorded and checkpointed.
    ready: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ScrapeApp for SuspendingRecorder {
    fn name(&self) -> &'static str {
        "suspender"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        if ctx.restore().is_some() {
            record(&ctx, "https://x/page-2", "attempt-2 page-2").await;
            return Ok(json!({ "resumed": true }));
        }
        record(&ctx, "https://x/page-1", "attempt-1 page-1").await;
        assert!(ctx.checkpoint_now(json!({ "page": 2 })).await);
        self.ready.notify_waiters();
        std::future::pending::<()>().await;
        unreachable!()
    }
}

/// The complement, and the case a naive "always truncate" gets wrong. A
/// shutdown suspend re-queues WITHOUT burning an attempt, and the resumed
/// attempt deliberately skips the work the suspended one already did. Its
/// recordings are therefore live work, not dead work: **a suspended recording
/// resumes, it does not restart.**
#[tokio::test]
async fn a_suspended_recording_resumes_instead_of_restarting() {
    let ready = Arc::new(tokio::sync::Notify::new());
    let (state, store) = test_state(vec![Arc::new(SuspendingRecorder {
        ready: ready.clone(),
    })])
    .await;
    let job = state
        .storage
        .enqueue(
            "suspender",
            EnqueueOptions {
                params: json!({ "record": true }),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    let notified = ready.notified();
    let worker_loop = WorkerLoop::start(&state);
    tokio::pin!(notified);
    tokio::time::timeout(Duration::from_secs(10), &mut notified)
        .await
        .expect("attempt 1 must record page 1 and checkpoint");
    // The drain's cooperative suspend: re-queued, checkpoint intact.
    worker_loop.shutdown(&state, Duration::from_secs(15)).await;
    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "a suspended job is re-queued, not failed"
    );

    assert!(worker::run_one(&state).await, "the suspended job re-claims");
    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Succeeded
    );

    let cassette = cassette_of(&store, "suspender", job.id).await;
    assert_eq!(cassette.len(), 2, "both halves of the job are replayable");
    assert_recorded_by(&cassette, "https://x/page-1", "attempt-1");
    assert_recorded_by(&cassette, "https://x/page-2", "attempt-2");
}

// ── A resolve-time replay miss is terminal, like the load-time one ───────────

/// Under `record: true` it records one URL; under `replay_of` it fetches a
/// DIFFERENT one — the resolve-time miss, reached through the real chokepoint.
struct MissesOnReplay {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ScrapeApp for MissesOnReplay {
    fn name(&self) -> &'static str {
        "misser"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        if matches!(ctx.vcr, Vcr::Record(_)) {
            record(&ctx, "https://x/recorded", "the recorded page").await;
            return Ok(json!({ "recorded": true }));
        }
        let out = ctx
            .fetch(FetchRequest::new("https://x/never-recorded"))
            .await?;
        Ok(json!({ "fetched": out.url }))
    }
}

/// **The anti-pattern.** A replay that reached an unrecorded request was
/// re-queued and re-ran from the top, `max_attempts` times — re-doing every
/// live-free step that preceded the miss and missing again in exactly the same
/// place, because a cassette and a job's params are both immutable for the life
/// of the job.
///
/// The asymmetry that gave it away is asserted here too: the **load**-time miss
/// (no cassette at all) was already permanent. A miss must fail ONCE, with the
/// remaining attempts visibly un-burned.
#[tokio::test]
async fn a_resolve_time_replay_miss_fails_once_instead_of_burning_every_attempt() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (state, _store) = test_state(vec![Arc::new(MissesOnReplay { runs: runs.clone() })]).await;

    let recorded = state
        .storage
        .enqueue(
            "misser",
            EnqueueOptions {
                params: json!({ "record": true }),
                max_attempts: 5,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");
    assert!(worker::run_one(&state).await, "the recording run claims");
    assert_eq!(
        state
            .storage
            .get(recorded.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Succeeded
    );

    let replay = state
        .storage
        .enqueue(
            "misser",
            EnqueueOptions {
                params: json!({ "replay_of": recorded.id.to_string() }),
                max_attempts: 5,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");
    assert!(worker::run_one(&state).await, "the replay run claims");

    let job = state.storage.get(replay.id).await.unwrap().unwrap();
    assert_eq!(
        job.status,
        JobStatus::Failed,
        "a miss is deterministic: the job is failed, not re-queued for a retry"
    );
    assert_eq!(
        (job.attempts, job.max_attempts),
        (1, 5),
        "the backoff ladder must be left un-burned — a retry re-reads the same cassette"
    );
    let error = job.error.unwrap_or_default();
    assert!(
        error.contains("never-recorded"),
        "the failure names the request that missed: {error}"
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "one run per job — the replay job must not have run four more times"
    );
}
