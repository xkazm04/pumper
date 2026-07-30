//! Durable execution (M23): the worker hands a re-claimed attempt its last
//! persisted checkpoint, clears it on completion, and discards a poisoned blob
//! after `max_resume_failures` restored attempts have all failed.

use std::sync::{Arc, Mutex};

use pumper_core::{AppContext, EnqueueOptions, Error, JobStatus, Result, ScrapeApp};
use serde_json::{json, Value};
use uuid::Uuid;

use super::harness::test_state;
use crate::worker;

/// An app that records what `ctx.restore()` handed each attempt, checkpoints a
/// per-attempt cursor, and fails until attempt `succeed_on` (0 = never).
struct CheckpointingApp {
    restores: Arc<Mutex<Vec<Option<Value>>>>,
    succeed_on: usize,
}

#[async_trait::async_trait]
impl ScrapeApp for CheckpointingApp {
    fn name(&self) -> &'static str {
        "cp"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let attempt = {
            let mut restores = self.restores.lock().unwrap();
            restores.push(ctx.restore().cloned());
            restores.len()
        };
        assert!(
            ctx.checkpoint_now(json!({ "cursor": attempt })).await,
            "owning attempt's checkpoint must land"
        );
        if self.succeed_on != 0 && attempt >= self.succeed_on {
            Ok(json!({ "resumed_from": ctx.restore().cloned() }))
        } else {
            Err(Error::App(format!("planned failure on attempt {attempt}")))
        }
    }
}

/// Fast-forwards a job's retry backoff so `run_one` can claim it now.
async fn make_due(state: &crate::state::AppState, id: Uuid) {
    sqlx::query("UPDATE jobs SET available_at = '2000-01-01T00:00:00.000000Z' WHERE id = ?1")
        .bind(id.to_string())
        .execute(&state.storage.pool())
        .await
        .expect("fast-forward backoff");
}

#[tokio::test]
async fn checkpoint_restores_on_reclaim_and_clears_on_complete() {
    let restores = Arc::new(Mutex::new(Vec::new()));
    let app = Arc::new(CheckpointingApp {
        restores: restores.clone(),
        succeed_on: 2,
    });
    let (state, _store) = test_state(vec![app]).await;
    let job = state
        .storage
        .enqueue(
            "cp",
            EnqueueOptions {
                max_attempts: 5,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Attempt 1: fresh start (no restore), checkpoints, fails → retry queued.
    assert!(worker::run_one(&state).await);
    make_due(&state, job.id).await;
    // Attempt 2: the worker hands back attempt 1's checkpoint; app succeeds.
    assert!(worker::run_one(&state).await);

    let seen = restores.lock().unwrap().clone();
    assert_eq!(seen[0], None, "first attempt starts fresh");
    assert_eq!(
        seen[1],
        Some(json!({ "cursor": 1 })),
        "re-claim restores the prior attempt's checkpoint"
    );
    let row = state.storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Succeeded);
    assert_eq!(
        row.result.unwrap()["resumed_from"],
        json!({ "cursor": 1 }),
        "app saw the restored state"
    );
    assert!(
        state
            .storage
            .load_checkpoint(job.id)
            .await
            .unwrap()
            .is_none(),
        "completion clears the checkpoint"
    );
}

#[tokio::test]
async fn poisoned_checkpoint_is_discarded_after_max_resume_failures() {
    let restores = Arc::new(Mutex::new(Vec::new()));
    let app = Arc::new(CheckpointingApp {
        restores: restores.clone(),
        succeed_on: 0, // never succeeds — every restored attempt fails
    });
    let (state, _store) = test_state(vec![app]).await;
    assert_eq!(state.config.worker.max_resume_failures, 3, "default escape");
    let job = state
        .storage
        .enqueue(
            "cp",
            EnqueueOptions {
                max_attempts: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Attempt 1 fresh; attempts 2–4 restore (resume_failures 1, 2, 3); attempt 5
    // hits the escape: the blob is discarded and the run starts fresh again.
    for _ in 0..5 {
        assert!(worker::run_one(&state).await);
        make_due(&state, job.id).await;
    }
    let seen = restores.lock().unwrap().clone();
    assert_eq!(seen.len(), 5);
    assert_eq!(seen[0], None, "fresh first attempt");
    for (i, restore) in seen.iter().enumerate().take(4).skip(1) {
        assert!(
            restore.is_some(),
            "attempt {} should restore (escape not yet reached)",
            i + 1
        );
    }
    assert_eq!(
        seen[4], None,
        "after 3 failed restored attempts the checkpoint is discarded — fresh start"
    );
}
