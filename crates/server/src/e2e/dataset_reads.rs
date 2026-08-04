//! `GET /datasets/{app}/{ds}` — pins that `trust=` and `removed=` mean the
//! same thing on every read shape (default, cursor-paged, `filter=`-narrowed)
//! and on `/export`. Before this, the four shapes disagreed: the unfiltered
//! page ignored `trust=` entirely and always included tombstones; adding
//! `filter=` picked up `trust=` but flipped to excluding tombstones; `/export`
//! ignored `trust=` altogether. This test seeds one dataset with a stable-live,
//! a provisional-live, and a stable-but-tombstoned record, then walks the
//! {default, cursor, filter} × {trust=all/stable} × {removed=include/exclude}
//! matrix and asserts every combination answers identically regardless of
//! which shape asked.

use std::collections::HashSet;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::state::AppState;

const APP: &str = "fake";
const DATASET: &str = "d";

/// Seeds the three records the whole matrix reasons about: a stable live
/// record, a provisional live record, and a stable record that is then
/// tombstoned (so it is live-in-history but `removed_at`-set).
async fn seed(state: &AppState) {
    state
        .datasets
        .upsert_trusted(APP, DATASET, "stable-live", &json!({"v": 1}), None)
        .await
        .unwrap();
    state
        .datasets
        .upsert_trusted(
            APP,
            DATASET,
            "provisional-live",
            &json!({"v": 2}),
            Some("provisional"),
        )
        .await
        .unwrap();
    state
        .datasets
        .upsert_trusted(APP, DATASET, "stable-removed", &json!({"v": 3}), None)
        .await
        .unwrap();
    state
        .datasets
        .tombstone_keys(APP, DATASET, &["stable-removed".to_string()])
        .await
        .unwrap();
}

async fn get_bytes(state: &AppState, uri: &str) -> (StatusCode, Vec<u8>) {
    let resp = crate::routes::router(state.clone())
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, bytes.to_vec())
}

async fn get_json(state: &AppState, uri: &str) -> Value {
    let (status, bytes) = get_bytes(state, uri).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} -> {} {}",
        status,
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

/// Keys out of a bare-array response body (`[Record]`).
fn keys_from_array(body: &Value) -> HashSet<String> {
    body.as_array()
        .unwrap_or_else(|| panic!("expected a bare array, got {body}"))
        .iter()
        .map(|r| r["key"].as_str().unwrap().to_string())
        .collect()
}

/// Keys out of a dual-mode `{items, next_cursor}` response body.
fn keys_from_items(body: &Value) -> HashSet<String> {
    body["items"]
        .as_array()
        .unwrap_or_else(|| panic!("expected {{items, next_cursor}}, got {body}"))
        .iter()
        .map(|r| r["key"].as_str().unwrap().to_string())
        .collect()
}

fn set(keys: &[&str]) -> HashSet<String> {
    keys.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn default_cursor_and_filtered_reads_agree_on_trust_and_removed() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    seed(&state).await;

    // (query suffix, expected keys) — applied identically to the default page,
    // the (even-empty) cursor page, and a filter that matches every record.
    let cases: &[(&str, &[&str])] = &[
        ("trust=stable&removed=exclude", &["stable-live"]),
        (
            "trust=stable&removed=include",
            &["stable-live", "stable-removed"],
        ),
        (
            "trust=all&removed=exclude",
            &["stable-live", "provisional-live"],
        ),
        (
            "trust=all&removed=include",
            &["stable-live", "provisional-live", "stable-removed"],
        ),
        // Defaults (no trust=, no removed=): trust=all, removed=exclude.
        ("", &["stable-live", "provisional-live"]),
    ];

    for (query, expected) in cases {
        let expected = set(expected);

        let default_uri = format!("/datasets/{APP}/{DATASET}?{query}");
        let default_body = get_json(&state, &default_uri).await;
        assert_eq!(
            keys_from_array(&default_body),
            expected,
            "default page mismatch for '{query}'"
        );

        let cursor_uri = format!("/datasets/{APP}/{DATASET}?{query}&cursor=");
        let cursor_body = get_json(&state, &cursor_uri).await;
        assert_eq!(
            keys_from_items(&cursor_body),
            expected,
            "cursor page mismatch for '{query}' — must match the default page exactly"
        );

        // A filter matching every seeded record (v is always present) must not
        // change what trust=/removed= mean.
        let filtered_uri = format!("/datasets/{APP}/{DATASET}?{query}&filter=$.v:gte:0");
        let filtered_body = get_json(&state, &filtered_uri).await;
        assert_eq!(
            keys_from_array(&filtered_body),
            expected,
            "filtered page mismatch for '{query}' — adding filter= must not change trust=/removed= semantics"
        );
    }
}

#[tokio::test]
async fn removed_query_param_rejects_unknown_values() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    seed(&state).await;

    let (status, body) =
        get_bytes(&state, &format!("/datasets/{APP}/{DATASET}?removed=maybe")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["code"], "bad_request");
}

#[tokio::test]
async fn export_honors_trust_and_removed_across_all_three_formats() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    seed(&state).await;

    // json: bare array.
    let (status, bytes) = get_bytes(
        &state,
        &format!("/datasets/{APP}/{DATASET}/export?format=json&trust=stable&removed=exclude"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(keys_from_array(&body), set(&["stable-live"]));

    // ndjson: one JSON object per line.
    let (status, bytes) = get_bytes(
        &state,
        &format!("/datasets/{APP}/{DATASET}/export?format=ndjson&trust=stable&removed=include"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(bytes).unwrap();
    let ndjson_keys: HashSet<String> = text
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["key"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(
        ndjson_keys,
        set(&["stable-live", "stable-removed"]),
        "ndjson export must honor removed=include"
    );

    // csv: header + one row per record; count rows rather than parsing quoted
    // CSV fully (the fixture data has no embedded commas/quotes).
    let (status, bytes) = get_bytes(
        &state,
        &format!("/datasets/{APP}/{DATASET}/export?format=csv&trust=all&removed=exclude"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(bytes).unwrap();
    let mut lines = text.lines();
    assert_eq!(
        lines.next(),
        Some("key,first_seen,last_seen,updated_at,removed_at,data"),
        "csv header"
    );
    let rows: Vec<&str> = lines.filter(|l| !l.is_empty()).collect();
    assert_eq!(
        rows.len(),
        2,
        "csv trust=all removed=exclude must yield stable-live + provisional-live: {rows:?}"
    );
}
