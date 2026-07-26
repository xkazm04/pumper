//! The webhook WIRE contract — the headers and HMAC external receivers verify
//! against. A change to the signature base string or header set must turn this
//! red: the signature is recomputed here independently, not via `webhook::sign`.

use std::time::Duration;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::harness::{test_state, TestReceiver};
use crate::webhook;

fn expect_sig(secret: &[u8], ts: &str, delivery_id: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac key");
    mac.update(format!("{ts}.{delivery_id}.").as_bytes());
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

#[tokio::test]
async fn signature_and_headers_match_the_documented_contract() {
    let (state, _store) = test_state(vec![]).await;
    let rx = TestReceiver::spawn(vec![]).await;
    let payload = serde_json::json!({ "event": "test.event", "n": 42 });

    webhook::dispatch_event(
        state.webhook_client.clone(),
        state.storage.clone(),
        "test",
        "ref-1",
        &rx.url(),
        "test.event",
        &payload,
        Some("s3cr3t".into()),
    );

    let hits = rx.wait_hits(1, Duration::from_secs(5)).await;
    let (headers, body) = &hits[0];

    assert_eq!(body, &serde_json::to_vec(&payload).unwrap(), "body is the raw payload JSON");
    assert_eq!(headers["x-pumper-event"], "test.event");
    assert_eq!(headers["content-type"], "application/json");
    let delivery_id = &headers["x-pumper-delivery-id"];
    let ts = &headers["x-pumper-timestamp"];
    ts.parse::<i64>().expect("timestamp is unix seconds");
    assert_eq!(
        headers["x-pumper-signature"],
        expect_sig(b"s3cr3t", ts, delivery_id, body),
        "signature base is HMAC(secret, \"{{ts}}.{{delivery_id}}.\" ++ body)"
    );

    // The delivery is logged and marked delivered under the SAME id the
    // receiver saw — the id is the cross-system idempotency key.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(d) = state.storage.get_delivery(delivery_id).await.unwrap() {
            if d.status == "delivered" {
                assert_eq!(d.attempts, 1);
                break;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "delivery row never became delivered");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn retry_keeps_the_delivery_id_stable_and_lands_on_the_second_attempt() {
    let (state, _store) = test_state(vec![]).await;
    // First attempt gets a 500; the in-process retry loop re-sends.
    let rx = TestReceiver::spawn(vec![500]).await;

    webhook::dispatch_event(
        state.webhook_client.clone(),
        state.storage.clone(),
        "test",
        "ref-retry",
        &rx.url(),
        "test.event",
        &serde_json::json!({ "n": 1 }),
        Some("s3cr3t".into()),
    );

    // Backoff before attempt 2 is ~2s.
    let hits = rx.wait_hits(2, Duration::from_secs(10)).await;
    let id0 = &hits[0].0["x-pumper-delivery-id"];
    let id1 = &hits[1].0["x-pumper-delivery-id"];
    assert_eq!(id0, id1, "delivery id must be stable across retries (idempotency key)");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(d) = state.storage.get_delivery(id0).await.unwrap() {
            if d.status == "delivered" {
                assert_eq!(d.attempts, 2, "one failed + one delivered attempt");
                break;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "retried delivery never delivered");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
