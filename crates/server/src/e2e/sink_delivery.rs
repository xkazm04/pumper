//! Sink connectors (M22), end to end through the worker's watch fan-out: the
//! `file` sink lands NDJSON on disk and the `slack` sink posts an
//! incoming-webhook message — both logged in `webhook_deliveries` under the
//! same machinery as plain webhooks (the delivery id in the file envelope is
//! the proof: it resolves to a `delivered` log row).
//!
//! These assertions used to be deadline polls (raised to 30s after repeated
//! flakes) because deliveries were detached `tokio::spawn`s with nothing to wait
//! on. They now ride `AppState::deliveries`, which `worker::run_one` drains — so
//! when `run_sync_job` returns, every delivery this job produced is *finished*.
//! The polls are gone, not re-tuned: a hang here is now a deadlock, not a race.

use std::sync::Arc;

use pumper_core::EnqueueOptions;
use serde_json::json;

use super::harness::{test_state, FakeApp, TestReceiver};
use crate::worker;

/// Enqueues a scripted 2-record sync on the fake app and runs it. Returns only
/// after the job's fan-out AND every delivery it queued have completed.
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
        .create_watch("fake", "d", "", None, "file", 0)
        .await
        .expect("create file-sink watch");

    run_sync_job(&state).await;

    // data/sinks/ sits beside the artifacts dir; the filename is the watch id.
    let path = store
        .path()
        .join("sinks")
        .join(format!("{}.ndjson", watch.id));
    // No poll: `run_sync_job` drained the delivery pool, so the append either
    // happened or never will.
    let content = tokio::fs::read_to_string(&path)
        .await
        .unwrap_or_else(|e| panic!("file sink never wrote {}: {e}", path.display()));
    assert!(
        content.ends_with('\n'),
        "a drained delivery leaves a COMPLETE NDJSON line, not a partial write: {content:?}"
    );

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
    let d = state
        .storage
        .get_delivery(delivery_id)
        .await
        .unwrap()
        .expect("the envelope's delivery id resolves to a log row");
    assert_eq!(
        d.status, "delivered",
        "a drained delivery has recorded its outcome"
    );
    assert_eq!(d.url, format!("file://{}.ndjson", watch.id));
    assert_eq!(d.kind, "change");
}

#[tokio::test]
async fn slack_sink_posts_a_compact_summary_message() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let rx = TestReceiver::spawn(vec![]).await;
    state
        .storage
        .create_watch("fake", "d", &rx.url(), None, "slack", 0)
        .await
        .expect("create slack-sink watch");

    run_sync_job(&state).await;

    // Drained, not polled: the POST is complete by the time `run_sync_job`
    // returns, so an empty `hits` here means it never went out.
    let hits = rx.hits_so_far();
    assert_eq!(hits.len(), 1, "one change batch → one slack post");
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

// ── plugin sinks (N10) ───────────────────────────────────────────────────────

/// A canned connector: whatever verdict the test hands it, plus a record of the
/// envelope it was given.
///
/// A stub rather than a real `.wasm` because what this file proves is the
/// SERVER's half — that a connector's verdict reaches the same delivery ladder
/// every other sink rides. The sandbox half (an undeclared import cannot link)
/// is proved where it lives, in `engine-wasm`.
struct StubConnector {
    verdict: serde_json::Value,
    seen: std::sync::Mutex<Vec<(String, serde_json::Value)>>,
}

#[async_trait::async_trait]
impl pumper_core::Plugins for StubConnector {
    async fn run(
        &self,
        name: &str,
        input: &str,
        params: &serde_json::Value,
    ) -> pumper_core::Result<serde_json::Value> {
        let envelope: serde_json::Value =
            serde_json::from_str(input).expect("the host hands the module JSON");
        self.seen.lock().expect("seen").push((
            name.to_string(),
            json!({ "doc": envelope, "params": params }),
        ));
        Ok(self.verdict.clone())
    }
    fn list(&self) -> Vec<String> {
        vec!["sink-stub".into()]
    }
    async fn reload(&self) -> pumper_core::Result<usize> {
        Ok(1)
    }
}

/// **The gate the item promises**: a `plugin:` sink that reports a PERMANENT
/// refusal lands in the DLQ exactly like any other sink's permanent failure —
/// `dead` immediately, not after five backed-off retries — and the delivery row
/// is a replayable `plugin://` row like the `file://` ones above.
#[tokio::test]
async fn a_permanent_plugin_sink_refusal_dead_letters_like_any_other_sink() {
    let connector = Arc::new(StubConnector {
        verdict: json!({"delivered": false, "permanent": true, "error": "422 schema"}),
        seen: std::sync::Mutex::new(Vec::new()),
    });
    let (state, _store) =
        super::harness::test_state_with_plugins(vec![Arc::new(FakeApp)], connector.clone()).await;
    state
        .storage
        .create_watch(
            "fake",
            "d",
            "http://localhost:3000/deliveries",
            None,
            "plugin:sink-stub",
            0,
        )
        .await
        .expect("create plugin-sink watch");

    run_sync_job(&state).await;

    // The module got the delivery envelope AND the operator's target — the
    // whole configuration path, which is what makes one module reusable.
    let seen = connector.seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 1, "one change batch → one connector call");
    let (name, call) = &seen[0];
    assert_eq!(name, "sink-stub");
    assert_eq!(call["params"]["target"], "http://localhost:3000/deliveries");
    assert_eq!(call["doc"]["event"], "dataset.changed");
    assert_eq!(
        call["doc"]["body"]["count"], 2,
        "the body is the UNSHAPED payload, exactly what a webhook sink receives"
    );
    assert!(
        call["doc"]["delivery_id"].is_string(),
        "the stable idempotency key a connector dedups on"
    );

    // …and the outcome rode the ordinary ladder into the ordinary DLQ.
    let dead = state
        .storage
        .list_deliveries(Some("dead"), 10)
        .await
        .expect("list dead deliveries");
    assert_eq!(
        dead.len(),
        1,
        "a permanent refusal is `dead` NOW — not after the 5-rung ladder"
    );
    assert_eq!(
        dead[0].url,
        "plugin://sink-stub?target=http://localhost:3000/deliveries"
    );
    assert_eq!(dead[0].kind, "change");
    assert!(
        dead[0]
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("422 schema"),
        "the connector's own reason survives into the log: {:?}",
        dead[0].last_error
    );
}

/// The negative that keeps the test above from passing for the wrong reason: a
/// connector that says `delivered` produces a `delivered` row, not a dead one.
#[tokio::test]
async fn a_delivering_plugin_sink_logs_a_delivered_row() {
    let connector = Arc::new(StubConnector {
        verdict: json!({"delivered": true}),
        seen: std::sync::Mutex::new(Vec::new()),
    });
    let (state, _store) =
        super::harness::test_state_with_plugins(vec![Arc::new(FakeApp)], connector).await;
    state
        .storage
        .create_watch("fake", "d", "", None, "plugin:sink-stub", 0)
        .await
        .expect("create plugin-sink watch");

    run_sync_job(&state).await;

    let dead = state
        .storage
        .list_deliveries(Some("dead"), 10)
        .await
        .expect("list");
    assert!(dead.is_empty(), "nothing dead-letters on a good delivery");
    let delivered = state
        .storage
        .list_deliveries(Some("delivered"), 10)
        .await
        .expect("list");
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered[0].url, "plugin://sink-stub",
        "a watch with no target logs the bare pseudo-URL"
    );
}
