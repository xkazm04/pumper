//! `DELETE /hosts/{host}/memory` vs the 60s host-penalty write-behind pass.
//!
//! Both surfaces read the live governor and then write `tier_memory`, so an
//! operator's reset used to be undoable by a background tick — silently, with a
//! `200 {reset: true}` still on the wire. These drive the real router and the
//! real pass (`state::persist_host_penalties`), never a re-implementation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use super::harness::test_state;
use crate::routes;
use crate::state::{persist_host_penalties, AppState};

const HOST: &str = "hostile.example";

async fn delete_json(router: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
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

/// One write-behind pass exactly as `AppState::init`'s loop runs it.
async fn write_behind(state: &AppState) {
    persist_host_penalties(&state.governor, &state.tiers, &state.host_memory_lock)
        .await
        .expect("write-behind pass");
}

/// A host the governor has learned to back off from, persisted once.
async fn seed_penalized(state: &AppState) {
    state
        .governor
        .penalize(HOST, Some(Duration::from_secs(30)))
        .await;
    write_behind(state).await;
    assert!(
        state.tiers.get(HOST).await.unwrap().is_some(),
        "the write-behind pass must persist a live penalty"
    );
}

#[tokio::test]
async fn reset_survives_a_racing_write_behind_pass() {
    let (state, _store) = test_state(vec![]).await;
    seed_penalized(&state).await;

    // The write-behind loop, running flat out for the whole reset instead of
    // once a minute — the same race, made certain to happen.
    let stop = Arc::new(AtomicBool::new(false));
    let ticker = tokio::spawn({
        let state = state.clone();
        let stop = stop.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                write_behind(&state).await;
                tokio::task::yield_now().await;
            }
        }
    });

    let router = routes::router(state.clone());
    let (status, body) = delete_json(&router, &format!("/hosts/{HOST}/memory")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["reset"], true);

    // Let a good number of further passes land on the other side of the reset.
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    stop.store(true, Ordering::Relaxed);
    ticker.await.unwrap();

    assert!(
        state.tiers.get(HOST).await.unwrap().is_none(),
        "a background pass must not re-create the row the operator deleted"
    );
    assert_eq!(
        state.governor.penalty(HOST).await,
        Duration::ZERO,
        "the live penalty is gone too"
    );
}

#[tokio::test]
async fn write_behind_between_reset_halves_does_not_resurrect_the_row() {
    let (state, _store) = test_state(vec![]).await;
    seed_penalized(&state).await;

    // The two halves of `routes::runtime::reset_host_memory`, in its documented
    // order, with a write-behind pass landing between them. Order is the fix:
    // clearing the LIVE penalty first means the racing pass has nothing to say
    // about this host. (Row first, live second — the old order — had the pass
    // re-insert the row that had just been deleted.)
    assert!(state.governor.clear(HOST), "half 1: live state cleared");
    write_behind(&state).await;

    let mid = state
        .tiers
        .get(HOST)
        .await
        .unwrap()
        .expect("the row is deleted by half 2, not by the pass");
    assert_eq!(
        mid.penalty_ms, 0,
        "the racing pass cannot re-assert a penalty the governor no longer holds"
    );

    assert!(
        state.tiers.forget(HOST).await.unwrap(),
        "half 2: the row is deleted"
    );
    write_behind(&state).await;

    assert!(
        state.tiers.get(HOST).await.unwrap().is_none(),
        "the reset stands"
    );
}

#[tokio::test]
async fn resetting_an_unknown_host_is_still_a_404() {
    let (state, _store) = test_state(vec![]).await;
    let router = routes::router(state);
    let (status, body) = delete_json(&router, "/hosts/never.seen.example/memory").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "unknown host");
}
