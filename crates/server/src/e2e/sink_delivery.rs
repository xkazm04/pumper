//! Sink connectors (M22), end to end through the worker's watch fan-out: the
//! `file` sink lands NDJSON on disk and the `slack` sink posts an
//! incoming-webhook message — both logged in `webhook_deliveries` under the
//! same machinery as plain webhooks (the delivery id in the file envelope is
//! the proof: it resolves to a `delivered` log row).

use std::sync::Arc;
use std::time::Duration;

use pumper_core::EnqueueOptions;
use serde_json::json;

use super::harness::{test_state, FakeApp, TestReceiver};
use crate::worker;

/// Enqueues a scripted 2-record sync on the fake app and runs it.
async fn run_sync_job(state: &crate::state::AppState) {
    state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params: json!({
                    "dataset": "d",
                    "sync": [
                        { "key": "k1", "data": { "n": 1 } },
                        { "key": "k2", "data": { "n": 2 } },
                    ]
                }),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");
    assert!(worker::run_one(state).await, "job must be claimed");
}

#[tokio::test]
async fn file_sink_appends_ndjson_and_logs_the_delivery() {
    let (state, store) = test_state(vec![Arc::new(FakeApp)]).await;
    let watch = state
        .storage
        .create_watch("fake", "d", "", None, "file")
        .await
        .expect("create file-sink watch");

    run_sync_job(&state).await;

    // data/sinks/ sits beside the artifacts dir; the filename is the watch id.
    let path = store
        .path()
        .join("sinks")
        .join(format!("{}.ndjson", watch.id));
    // Generous on purpose: this budget bounds a HANG, it does not measure
    // latency. At 5s it flaked under full-suite parallel load while passing in
    // isolation every time — a blown deadline here must mean the sink never
    // wrote, never that the box was busy.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let content = loop {
        if let Ok(content) = tokio::fs::read_to_string(&path).await {
            break content;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "file sink never wrote {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };

    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1, "one change batch → one NDJSON line");
    let envelope: serde_json::Value = serde_json::from_str(lines[0]).expect("line is valid JSON");
    assert_eq!(envelope["event"], "dataset.changed");
    assert_eq!(envelope["payload"]["app"], "fake");
    assert_eq!(envelope["payload"]["dataset"], "d");
    assert_eq!(envelope["payload"]["count"], 2);

    // Same machinery as webhooks: the envelope's delivery id resolves to a
    // `delivered` row whose url is the file:// pseudo-URL (DLQ-replayable).
    let delivery_id = envelope["delivery_id"].as_str().expect("delivery id");
    // Generous on purpose: this budget bounds a HANG, it does not measure
    // latency. At 5s it flaked under full-suite parallel load while passing in
    // isolation every time — a blown deadline here must mean the sink never
    // wrote, never that the box was busy.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(d) = state.storage.get_delivery(delivery_id).await.unwrap() {
            if d.status == "delivered" {
                assert_eq!(d.url, format!("file://{}.ndjson", watch.id));
                assert_eq!(d.kind, "change");
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "file-sink delivery row never became delivered"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn slack_sink_posts_a_compact_summary_message() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let rx = TestReceiver::spawn(vec![]).await;
    state
        .storage
        .create_watch("fake", "d", &rx.url(), None, "slack")
        .await
        .expect("create slack-sink watch");

    run_sync_job(&state).await;

    let hits = rx.wait_hits(1, Duration::from_secs(5)).await;
    let (headers, body) = &hits[0];
    assert_eq!(headers["content-type"], "application/json");
    let msg: serde_json::Value = serde_json::from_slice(body).expect("slack JSON body");
    let text = msg["text"].as_str().expect("incoming-webhook text field");
    assert!(text.contains("fake/d"), "summary names app/dataset: {text}");
    assert!(
        text.contains("2 revisions"),
        "summary carries count: {text}"
    );
    assert!(
        msg.get("changes").is_none() && msg.get("payload").is_none(),
        "slack gets the summary only, never the raw revision batch"
    );
}
