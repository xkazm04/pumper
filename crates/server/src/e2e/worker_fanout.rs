//! The worker's success fan-out, end to end: a real job through `run_one` —
//! claim → FakeApp writes via `ctx.sync_many` → attempt-fenced complete →
//! events on the bus → watch webhook (signed) → dataset-trigger hop with
//! `_trigger` provenance. This is the composed path whose ordering the
//! `suppress_unhealthy` comment calls "the enforcement".

use std::sync::Arc;
use std::time::Duration;

use pumper_core::{EnqueueOptions, JobStatus, NewTrigger};
use serde_json::json;

use super::harness::{test_state, FakeApp, TestReceiver};
use crate::worker;

#[tokio::test]
async fn a_job_run_fans_out_to_events_watches_and_trigger_hops() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let rx = TestReceiver::spawn(vec![]).await;

    // A watch on the dataset the app writes, and a dataset-trigger edge
    // fake/d --fresh--> fake (the hop job just queues; we assert its shape).
    state
        .storage
        .create_watch("fake", "d", &rx.url(), Some("s3cr3t"))
        .await
        .expect("create watch");
    let trigger = state
        .storage
        .create_trigger(&NewTrigger {
            name: Some("fanout-hop"),
            source_kind: "dataset",
            source_app: "fake",
            source_dataset: Some("d"),
            on_change: None,
            on_status: None,
            target_app: "fake",
            params: &json!({ "hop": true }),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
        })
        .await
        .expect("create trigger");

    let job = state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params: json!({
                    "dataset": "d",
                    "sync": [
                        { "key": "k1", "data": { "n": 1 } },
                        { "key": "k2", "data": { "n": 2 } },
                        { "key": "k3", "data": { "n": 3 } },
                    ]
                }),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(
        worker::run_one(&state).await,
        "one queued job must be claimed"
    );

    // Terminal state, attempt-fenced.
    let done = state.storage.get(job.id).await.unwrap().expect("job row");
    assert_eq!(done.status, JobStatus::Succeeded);
    assert_eq!(done.result.as_ref().unwrap()["synced"], 3);

    // The bus saw running → succeeded for this job, in order.
    let events: Vec<String> = match state.events.replay(0) {
        crate::events::Replay::Events(evs) => evs
            .iter()
            .filter(|(_, e)| e.job_id == job.id)
            .map(|(_, e)| e.status.clone())
            .collect(),
        _ => panic!("expected buffered events, got a reset"),
    };
    assert_eq!(events, vec!["running", "succeeded"], "bus event sequence");

    // The watch fired once with the change batch, signed.
    let hits = rx.wait_hits(1, Duration::from_secs(5)).await;
    let (headers, body) = &hits[0];
    assert_eq!(headers["x-pumper-event"], "dataset.changed");
    assert!(
        headers.contains_key("x-pumper-signature"),
        "watch has a secret → must be signed"
    );
    let payload: serde_json::Value = serde_json::from_slice(body).unwrap();
    assert_eq!(payload["app"], "fake");
    assert_eq!(payload["dataset"], "d");
    assert_eq!(payload["count"], 3);

    // The trigger enqueued exactly one hop job carrying `_trigger` provenance.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let hop = loop {
        let jobs: Vec<(String,)> =
            sqlx::query_as("SELECT params FROM jobs WHERE app = 'fake' AND id != ?1")
                .bind(job.id.to_string())
                .fetch_all(&state.storage.pool())
                .await
                .unwrap();
        if let Some((params,)) = jobs.first() {
            break serde_json::from_str::<serde_json::Value>(params).unwrap();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "trigger hop job never enqueued"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(hop["hop"], true, "trigger params merged into the hop job");
    let trig = &hop["_trigger"];
    assert_eq!(trig["trigger_id"], json!(trigger.id));
    assert_eq!(trig["source_job_id"], json!(job.id.to_string()));
    assert_eq!(
        trig["chain"].as_array().map(Vec::len),
        Some(1),
        "one-hop chain"
    );
}
