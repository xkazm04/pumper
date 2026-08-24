//! `DELETE /datasets/{app}/{ds}` — the two-step gate in front of the most
//! destructive verb in the API.
//!
//! Before this suite the route hard-deleted every record and every revision on
//! a bare `DELETE`, with no echo, no preview and no receipt: a stale tab or a
//! copied curl line was enough. These tests pin all four rungs by asking, after
//! every refusal, whether the data is still there — a gate that returns the
//! right status while destroying the rows would pass a status-only assertion.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::state::AppState;

const APP: &str = "fake";
const DATASET: &str = "d";

/// Three records, each upserted twice so every one carries real revision
/// history — the population the delete claims to destroy is records AND
/// revisions, and a fixture with one revision each cannot tell them apart.
async fn seed(state: &AppState) {
    for (i, key) in ["a", "b", "c"].iter().enumerate() {
        state
            .datasets
            .upsert(APP, DATASET, key, &json!({"v": i}))
            .await
            .unwrap();
        state
            .datasets
            .upsert(APP, DATASET, key, &json!({"v": i + 100}))
            .await
            .unwrap();
    }
}

async fn delete(state: &AppState, uri: &str) -> (StatusCode, Value) {
    let resp = crate::routes::router(state.clone())
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// The question every refusal has to answer: is the data still there?
async fn survivors(state: &AppState) -> (i64, usize) {
    let records = state.datasets.record_count(APP, DATASET).await.unwrap();
    let revisions = state
        .datasets
        .dataset_revisions_page(APP, DATASET, None, 1000)
        .await
        .unwrap()
        .len();
    (records, revisions)
}

#[tokio::test]
async fn a_bare_delete_previews_and_destroys_nothing() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    seed(&state).await;

    let (status, body) = delete(&state, &format!("/datasets/{APP}/{DATASET}")).await;

    assert_eq!(status, StatusCode::PRECONDITION_REQUIRED, "body: {body}");
    assert_eq!(body["preview"], json!(true));
    assert_eq!(body["code"], json!("confirmation_required"));
    // The counts, and the exact parameters to retry with — the preview is where
    // the expected echo is rendered, so the operator never has to guess it.
    assert_eq!(body["records"], json!(3));
    assert_eq!(body["revisions"], json!(6));
    assert_eq!(body["expect_records"], json!(3));
    assert_eq!(body["confirm"], json!(format!("{APP}/{DATASET}")));
    assert!(
        body["as_of"].as_str().is_some_and(|s| !s.is_empty()),
        "every count carries the moment it was taken: {body}"
    );
    // A preview that says 200 + `deleted: 0` is the shape that gets pasted into
    // a ticket as proof of a deletion. This one cannot be misread.
    assert!(body.get("deleted").is_none(), "body: {body}");
    assert_eq!(survivors(&state).await, (3, 6));
}

#[tokio::test]
async fn a_wrong_echo_is_refused_and_the_dataset_survives() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    seed(&state).await;

    for confirm in [
        DATASET,                                      // the narrow half of the scope
        APP,                                          // the wide half
        &format!("{APP}/{DATASET}x"),                 // a near miss
        &format!("{APP}/{DATASET}/"),                 // a trailing separator is not "close enough"
        &format!("{}/{DATASET}", APP.to_uppercase()), // case is not folded
    ] {
        let (status, body) = delete(
            &state,
            &format!("/datasets/{APP}/{DATASET}?confirm={confirm}&expect_records=3"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "confirm={confirm}: {body}");
        assert_eq!(body["code"], json!("bad_request"));
        assert_eq!(
            survivors(&state).await,
            (3, 6),
            "confirm={confirm} was refused but the rows went anyway"
        );
    }
}

#[tokio::test]
async fn surrounding_whitespace_in_the_echo_is_trimmed_not_rejected() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    seed(&state).await;

    let (status, body) = delete(
        &state,
        &format!("/datasets/{APP}/{DATASET}?confirm=%20{APP}/{DATASET}%20&expect_records=3"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(survivors(&state).await, (0, 0));
}

#[tokio::test]
async fn a_stale_record_count_refuses_the_delete() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    seed(&state).await;

    // The operator previewed 2 records; a fourth write landed since. They
    // consented to destroying something that no longer exists.
    let (status, body) = delete(
        &state,
        &format!("/datasets/{APP}/{DATASET}?confirm={APP}/{DATASET}&expect_records=2"),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    assert_eq!(body["code"], json!("conflict"));
    assert!(
        body["error"].as_str().unwrap_or_default().contains('3'),
        "the refusal names what is actually there: {body}"
    );
    assert_eq!(survivors(&state).await, (3, 6));
}

#[tokio::test]
async fn a_confirmed_delete_destroys_the_dataset_and_receipts_the_export() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    seed(&state).await;

    let (status, body) = delete(
        &state,
        &format!("/datasets/{APP}/{DATASET}?confirm={APP}/{DATASET}&expect_records=3"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["preview"], json!(false));
    // The receipt is what it ACTUALLY destroyed, counted by the DELETEs
    // themselves, not the forecast the preview made.
    assert_eq!(body["deleted"], json!(3));
    assert_eq!(body["records"], json!(3));
    assert_eq!(body["revisions"], json!(6));
    assert_eq!(survivors(&state).await, (0, 0));

    // The export is the restore path this hard delete used to lack.
    let export = body["export"].as_str().expect("an export path");
    let text = std::fs::read_to_string(export).expect("the export file exists");
    let lines: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).expect("one JSON object per line"))
        .collect();
    assert_eq!(lines[0]["kind"], json!("header"));
    assert_eq!(lines[0]["app"], json!(APP));
    assert_eq!(lines[0]["records_expected"], json!(3));
    let records = lines.iter().filter(|l| l["kind"] == "record").count();
    let revisions = lines.iter().filter(|l| l["kind"] == "revision").count();
    assert_eq!(
        (records, revisions),
        (3, 6),
        "the export must hold everything the delete destroyed"
    );
    // Every revision's payload survives, not just its identity — an export that
    // kept the keys and dropped the snapshots would restore nothing.
    assert!(lines
        .iter()
        .filter(|l| l["kind"] == "revision")
        .all(|l| l["data"].is_object()));
}

#[tokio::test]
async fn deleting_an_empty_or_unknown_dataset_is_a_no_op_not_a_surprise() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    seed(&state).await;

    // A typo'd dataset name previews ZERO — which is exactly the signal that
    // the operator is aimed at the wrong target.
    let (status, body) = delete(&state, &format!("/datasets/{APP}/typo")).await;
    assert_eq!(status, StatusCode::PRECONDITION_REQUIRED, "body: {body}");
    assert_eq!(body["records"], json!(0));

    let (status, body) = delete(
        &state,
        &format!("/datasets/{APP}/typo?confirm={APP}/typo&expect_records=0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["deleted"], json!(0));
    // And the real dataset next door is untouched.
    assert_eq!(survivors(&state).await, (3, 6));
}

#[tokio::test]
async fn an_untrusted_dataset_name_cannot_escape_the_export_directory() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let hostile = "..%2F..%2Fescaped";
    state
        .datasets
        .upsert(APP, "../../escaped", "k", &json!({"v": 1}))
        .await
        .unwrap();

    let (status, body) = delete(
        &state,
        &format!("/datasets/{APP}/{hostile}?confirm={APP}/../../escaped&expect_records=1"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    let export = std::path::PathBuf::from(body["export"].as_str().expect("an export path"));
    assert!(
        export.starts_with(&state.storage.artifacts_dir),
        "the export escaped the artifacts root: {}",
        export.display()
    );
    let name = export
        .file_name()
        .expect("a filename")
        .to_string_lossy()
        .to_string();
    assert!(
        !name.contains("..") && !name.contains('/') && !name.contains('\\'),
        "the dataset name reached the filename unflattened: {name}"
    );
    assert!(export.exists());
}
