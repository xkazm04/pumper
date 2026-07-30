//! Integration tests for the durable-execution checkpoint store: the
//! attempts-lineage write guard, resume-failure counting (the poisoned-blob
//! escape's input), clearing, and the size cap — against a real temp-dir SQLite
//! with the full migration chain.

use pumper_core::testing::TempStore;
use pumper_core::EnqueueOptions;
use serde_json::json;

#[tokio::test]
async fn checkpoint_write_is_lineage_guarded_like_complete() {
    let store = TempStore::new("cp-lineage").await;
    let storage = &store.storage;
    let job = storage
        .enqueue(
            "crawl",
            EnqueueOptions {
                max_attempts: 5,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");
    let claimed = storage.claim_next(&[], 0.0).await.unwrap().expect("claim");
    assert_eq!(claimed.id, job.id);

    // The owning attempt's write lands.
    assert!(storage
        .save_checkpoint(job.id, claimed.attempts, &json!({"cursor": 10}))
        .await
        .unwrap());
    let (state, failures) = storage.load_checkpoint(job.id).await.unwrap().unwrap();
    assert_eq!(state, json!({"cursor": 10}));
    assert_eq!(failures, 0);

    // A stale attempt number (job reset/reaped and re-claimed elsewhere) is
    // discarded — same fence as `complete`.
    assert!(!storage
        .save_checkpoint(job.id, claimed.attempts - 1, &json!({"cursor": 3}))
        .await
        .unwrap());
    // A write against a non-running job is discarded too.
    storage
        .complete(job.id, claimed.attempts, json!({}))
        .await
        .unwrap();
    assert!(!storage
        .save_checkpoint(job.id, claimed.attempts, &json!({"cursor": 99}))
        .await
        .unwrap());
    // The live checkpoint is untouched by both stale writes.
    let (state, _) = storage.load_checkpoint(job.id).await.unwrap().unwrap();
    assert_eq!(state, json!({"cursor": 10}));
}

#[tokio::test]
async fn resume_failures_survive_overwrites_and_clear_removes_the_row() {
    let store = TempStore::new("cp-resumes").await;
    let storage = &store.storage;
    let job = storage
        .enqueue(
            "crawl",
            EnqueueOptions {
                max_attempts: 5,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");
    let claimed = storage.claim_next(&[], 0.0).await.unwrap().expect("claim");

    assert!(storage
        .save_checkpoint(job.id, claimed.attempts, &json!({"n": 1}))
        .await
        .unwrap());
    // Two restores handed out...
    assert_eq!(storage.bump_checkpoint_resumes(job.id).await.unwrap(), 1);
    assert_eq!(storage.bump_checkpoint_resumes(job.id).await.unwrap(), 2);
    // ...and an overwrite by the still-owning attempt preserves the counter —
    // it counts restores, not writes.
    assert!(storage
        .save_checkpoint(job.id, claimed.attempts, &json!({"n": 2}))
        .await
        .unwrap());
    let (state, failures) = storage.load_checkpoint(job.id).await.unwrap().unwrap();
    assert_eq!(state, json!({"n": 2}));
    assert_eq!(failures, 2);

    storage.clear_checkpoint(job.id).await.unwrap();
    assert!(storage.load_checkpoint(job.id).await.unwrap().is_none());
    // Bumping a missing row is a harmless 0, and clearing is idempotent.
    assert_eq!(storage.bump_checkpoint_resumes(job.id).await.unwrap(), 0);
    storage.clear_checkpoint(job.id).await.unwrap();
}

#[tokio::test]
async fn oversized_checkpoint_is_rejected_with_an_error() {
    let store = TempStore::new("cp-size").await;
    let storage = &store.storage;
    let job = storage
        .enqueue("crawl", EnqueueOptions::default())
        .await
        .expect("enqueue");
    let claimed = storage.claim_next(&[], 0.0).await.unwrap().expect("claim");

    let blob = json!({ "big": "x".repeat(pumper_core::MAX_CHECKPOINT_BYTES) });
    let err = storage
        .save_checkpoint(job.id, claimed.attempts, &blob)
        .await
        .expect_err("over-cap blob must be rejected");
    assert!(err.to_string().contains("checkpoint too large"), "{err}");
    assert!(storage.load_checkpoint(job.id).await.unwrap().is_none());
}
