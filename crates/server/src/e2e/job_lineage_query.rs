//! `GET /jobs` — the lineage slice the `jobs` table was already indexed for.
//!
//! The queue list is the product's triage surface and until this suite it could
//! be sliced by `app` and `status` and nothing else, while four migrations
//! (0012, 0014, 0035, 0041) had already built indexes on `schedule_id`,
//! `trigger_id`, `source_job_id` and `target_key`. Worse than absent: the
//! params were **silently dropped** by serde, so `?schedule_id=S` answered
//! `200` with the UNFILTERED list — a wrong answer wearing a right answer's
//! status code.
//!
//! What each test here pins:
//!
//! - the four lineage predicates actually narrow, and an id that matches
//!   nothing answers an EMPTY list rather than everything (the silent-drop
//!   signature);
//! - `since`/`until` are a half-open window and a malformed one is a `400`
//!   naming the param, the same shape every other date param on this API uses;
//! - `rollup=status` answers from ONE `GROUP BY`, asserted against the store's
//!   own `job_status_counts` op counter — a handler that paged rows to count
//!   them would leave that counter untouched;
//! - a query param this endpoint does not understand is a `400`, not a
//!   plausible wrong answer;
//! - **guard**: with none of the new params the two existing response modes
//!   (bare array, `{items,next_cursor}`) are byte-for-byte what they were.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use super::harness::{test_state, FakeApp};
use crate::state::AppState;

/// One job row, written straight to the table so `created_at` and the lineage
/// columns are all under the test's control (the enqueue door stamps `now()`
/// and leaves `schedule_id`/`trigger_id` null for a manual run).
#[allow(clippy::too_many_arguments)]
async fn seed_job(
    state: &AppState,
    id: &str,
    app: &str,
    status: &str,
    created_at: &str,
    schedule_id: Option<&str>,
    trigger_id: Option<&str>,
    target_key: Option<&str>,
) {
    let pool = state.storage.pool();
    sqlx::query(
        "INSERT INTO jobs (id, app, params, status, attempts, max_attempts, priority, \
         created_at, available_at, schedule_id, trigger_id, target_key) \
         VALUES (?1, ?2, '{}', ?3, 0, 1, 0, ?4, ?4, ?5, ?6, ?7)",
    )
    .bind(id)
    .bind(app)
    .bind(status)
    .bind(created_at)
    .bind(schedule_id)
    .bind(trigger_id)
    .bind(target_key)
    .execute(&pool)
    .await
    .expect("seed job");
}

/// A uuid-shaped id that is readable in a failure message: `…-0000-0000-00NN`.
fn jid(n: u32) -> String {
    format!("00000000-0000-4000-8000-{n:012}")
}

async fn get(state: &AppState, uri: &str) -> (StatusCode, Vec<u8>) {
    let resp = crate::routes::router(state.clone())
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, bytes.to_vec())
}

async fn get_json(state: &AppState, uri: &str) -> Value {
    let (status, bytes) = get(state, uri).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} -> {status} {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("json body")
}

/// Ids out of a bare-array or `{items}` response, in response order.
fn ids(body: &Value) -> Vec<String> {
    let arr = body
        .as_array()
        .or_else(|| body.get("items").and_then(Value::as_array))
        .unwrap_or_else(|| panic!("neither a bare array nor {{items}}: {body}"));
    arr.iter()
        .map(|j| j["id"].as_str().expect("job id").to_string())
        .collect()
}

/// Three jobs from schedule `S`, two from schedule `T`, spread over three days
/// so the window tests have something to exclude. Trigger and target columns
/// ride along on the same rows.
async fn seed_fleet(state: &AppState) {
    // schedule S — newest first will be 3, 2, 1
    seed_job(
        state,
        &jid(1),
        "fake",
        "succeeded",
        "2026-10-01T00:00:00.000000Z",
        Some("S"),
        None,
        Some("K"),
    )
    .await;
    seed_job(
        state,
        &jid(2),
        "fake",
        "failed",
        "2026-10-02T00:00:00.000000Z",
        Some("S"),
        Some("TRIG"),
        None,
    )
    .await;
    seed_job(
        state,
        &jid(3),
        "fake",
        "succeeded",
        "2026-10-03T00:00:00.000000Z",
        Some("S"),
        None,
        None,
    )
    .await;
    // schedule T
    seed_job(
        state,
        &jid(4),
        "other",
        "succeeded",
        "2026-10-02T12:00:00.000000Z",
        Some("T"),
        Some("TRIG"),
        Some("K"),
    )
    .await;
    seed_job(
        state,
        &jid(5),
        "other",
        "failed",
        "2026-10-03T12:00:00.000000Z",
        Some("T"),
        None,
        None,
    )
    .await;
}

/// Case 1. The silent-drop signature, stated as an assertion: a `schedule_id`
/// that names three of five jobs must return three, newest first — not five
/// with a 200.
#[tokio::test]
async fn schedule_id_narrows_to_that_schedules_runs_newest_first() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp) as _]).await;
    seed_fleet(&state).await;

    let body = get_json(&state, "/jobs?schedule_id=S").await;
    assert_eq!(
        ids(&body),
        vec![jid(3), jid(2), jid(1)],
        "?schedule_id=S must answer S's three runs newest-first; an unfiltered \
         five-row answer is the serde silent-drop this endpoint used to have"
    );

    // And the same through the cursor mode, so the two response shapes cannot
    // disagree about what a filter means.
    let paged = get_json(&state, "/jobs?schedule_id=S&cursor=").await;
    assert_eq!(ids(&paged), vec![jid(3), jid(2), jid(1)]);
}

/// Case 2. An id that matches nothing is an empty list — not a 404, and
/// emphatically not the unfiltered list.
#[tokio::test]
async fn an_unmatched_schedule_id_is_empty_not_everything() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp) as _]).await;
    seed_fleet(&state).await;

    let (status, bytes) = get(&state, "/jobs?schedule_id=does-not-exist").await;
    assert_eq!(status, StatusCode::OK, "an empty slice is not a 404");
    let body: Value = serde_json::from_slice(&bytes).expect("json");
    assert!(
        ids(&body).is_empty(),
        "an unmatched filter must answer [], not the whole queue: {body}"
    );

    let paged = get_json(&state, "/jobs?schedule_id=does-not-exist&cursor=").await;
    assert!(ids(&paged).is_empty());
    assert_eq!(paged["next_cursor"], Value::Null);
}

/// Case 3's HTTP half (the `EXPLAIN QUERY PLAN` half lives in
/// `crates/core/tests/jobs.rs`): `trigger_id` composes with `status`, and the
/// two predicates intersect rather than either one winning.
#[tokio::test]
async fn trigger_id_composes_with_status() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp) as _]).await;
    seed_fleet(&state).await;

    let body = get_json(&state, "/jobs?trigger_id=TRIG&status=failed").await;
    assert_eq!(
        ids(&body),
        vec![jid(2)],
        "TRIG fired jobs 2 and 4; only 2 failed"
    );
    let all = get_json(&state, "/jobs?trigger_id=TRIG").await;
    assert_eq!(ids(&all), vec![jid(4), jid(2)]);
}

/// Case 4. `since`/`until` are a HALF-OPEN window `[since, until)`, and a
/// malformed value is a 400 naming the param — the same shape every other date
/// param on this API answers with.
#[tokio::test]
async fn since_until_is_half_open_and_a_bad_date_is_a_400() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp) as _]).await;
    seed_fleet(&state).await;

    // [10-01, 10-02) contains only job 1: job 2 sits exactly on the exclusive
    // upper bound, which is the whole point of half-open — paging a day at a
    // time must not return the boundary row twice.
    let body = get_json(
        &state,
        "/jobs?since=2026-10-01T00:00:00Z&until=2026-10-02T00:00:00Z",
    )
    .await;
    assert_eq!(
        ids(&body),
        vec![jid(1)],
        "until is EXCLUSIVE, so the job stamped exactly at it is the next \
         window's, not this one's"
    );

    // since is INCLUSIVE.
    let from_second = get_json(&state, "/jobs?since=2026-10-02T00:00:00Z").await;
    assert!(
        from_second
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["id"] == jid(2).as_str()),
        "since is inclusive: the job stamped exactly at it belongs to the window"
    );

    let (status, bytes) = get(&state, "/jobs?since=yesterday").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a date this API cannot read is the caller's mistake, not a silent \
         unfiltered list"
    );
    let msg = String::from_utf8_lossy(&bytes);
    assert!(
        msg.contains("since"),
        "the refusal has to name the param it refused: {msg}"
    );

    let (status, bytes) = get(&state, "/jobs?until=tomorrow").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        String::from_utf8_lossy(&bytes).contains("until"),
        "...and name 'until' when 'until' is the bad one"
    );
}

/// Case 5. `rollup=status` answers from ONE aggregate, not by paging rows and
/// counting them client-side-in-the-handler. The proof is the store's own
/// `job_status_counts` op counter: the aggregate path is metered, the row-list
/// path is not, so a handler that counted rows would leave it untouched.
#[tokio::test]
async fn rollup_status_counts_from_one_group_by_not_by_paging_rows() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp) as _]).await;
    seed_fleet(&state).await;

    let before = status_counts_ops(&state);
    let body = get_json(&state, "/jobs?rollup=status&schedule_id=S").await;
    let after = status_counts_ops(&state);

    assert_eq!(
        body["total"], 3,
        "S has three runs — the rollup counts the same rows the list returns: {body}"
    );
    assert_eq!(body["counts"]["succeeded"], 2);
    assert_eq!(body["counts"]["failed"], 1);
    assert!(
        body["counts"].get("queued").is_none(),
        "a status with no rows is ABSENT, not a zero — the counts are what the \
         window holds: {body}"
    );
    assert_eq!(
        after - before,
        1,
        "the rollup must be ONE metered aggregate; {} of them means the handler \
         is paging rows to count them (or not aggregating at all)",
        after - before
    );

    // The rollup honours the whole predicate set, window included.
    let windowed = get_json(
        &state,
        "/jobs?rollup=status&since=2026-10-03T00:00:00.000000Z",
    )
    .await;
    assert_eq!(windowed["total"], 2, "jobs 3 and 5: {windowed}");
}

/// Lifetime count of `job_status_counts` execute-phase operations on this
/// store — the counter that separates "aggregated" from "paged and counted".
fn status_counts_ops(state: &AppState) -> u64 {
    state
        .storage
        .instrument()
        .snapshot()
        .into_iter()
        .filter(|k| {
            k.op == pumper_core::store_instrument::StoreOp::JobStatusCounts
                && k.phase == pumper_core::store_instrument::StorePhase::Execute
        })
        .map(|k| k.lifetime)
        .sum()
}

/// Case 6. `target_key` is the dedup identity, and it is shared ACROSS apps —
/// so the filter must cross the app boundary rather than being an `app=`
/// synonym.
#[tokio::test]
async fn target_key_crosses_apps() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp) as _]).await;
    seed_fleet(&state).await;

    let body = get_json(&state, "/jobs?target_key=K").await;
    assert_eq!(
        ids(&body),
        vec![jid(4), jid(1)],
        "K is held by one 'fake' job and one 'other' job; a dedup target is not \
         scoped to an app: {body}"
    );
}

/// The sharp edge the whole card turns on: a param this endpoint does not
/// understand must be REFUSED, not dropped. Otherwise every future typo
/// (`?scheduleId=`, `?schedule=`) is a wrong answer with a 200, which is the
/// exact class `parse_removed` and `parse_feed_order` already refuse elsewhere
/// on this API.
#[tokio::test]
async fn an_unknown_query_param_is_refused_not_dropped() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp) as _]).await;
    seed_fleet(&state).await;

    for uri in [
        "/jobs?scheduleId=S",
        "/jobs?schedule=S",
        "/jobs?source_job_id=x",
    ] {
        let (status, bytes) = get(&state, uri).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "GET {uri} answered {status} — a param this endpoint cannot honour \
             must not come back as a plausible unfiltered list"
        );
        let msg = String::from_utf8_lossy(&bytes);
        assert!(
            msg.contains("unknown") && msg.contains("app"),
            "the refusal must name the offender AND list what IS understood, \
             or the caller cannot fix it: {msg}"
        );
    }

    // Every param the endpoint DOES understand stays accepted — the other half
    // of the inventory, so this guard cannot be satisfied by refusing
    // everything.
    for uri in [
        "/jobs?app=fake",
        "/jobs?status=failed",
        "/jobs?limit=10",
        "/jobs?cursor=",
        "/jobs?schedule_id=S",
        "/jobs?trigger_id=TRIG",
        "/jobs?target_key=K",
        "/jobs?since=2026-10-01T00:00:00Z",
        "/jobs?until=2026-10-09T00:00:00Z",
        "/jobs?rollup=status",
    ] {
        let (status, _) = get(&state, uri).await;
        assert_eq!(status, StatusCode::OK, "GET {uri} must still be understood");
    }
}

/// Case 7 — **GUARD**, green before this card and required to stay green: with
/// none of the new params, both existing response modes are exactly what they
/// were. The bare array stays a bare array (no envelope crept in), the cursor
/// mode keeps `{items,next_cursor}`, and a `Job` keeps precisely its current
/// field set.
#[tokio::test]
async fn absent_params_leave_both_legacy_response_modes_untouched() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp) as _]).await;
    seed_fleet(&state).await;

    let bare = get_json(&state, "/jobs").await;
    assert!(
        bare.is_array(),
        "no ?cursor= is still the LEGACY bare array, not an envelope: {bare}"
    );
    assert_eq!(
        ids(&bare),
        vec![jid(5), jid(3), jid(4), jid(2), jid(1)],
        "unfiltered, newest-first, all five"
    );

    const JOB_FIELDS: &[&str] = &[
        "id",
        "app",
        "params",
        "status",
        "attempts",
        "max_attempts",
        "priority",
        // `callback_secret` is deliberately ABSENT: the column exists, the
        // wire field does not. Listing it here would turn this golden into a
        // demand that the server start leaking it.
        "callback_url",
        "budget_usd",
        "schedule_id",
        "trigger_id",
        "target_key",
        "result",
        "error",
        "requeue_reason",
        "cause_kind",
        "created_at",
        "available_at",
        "started_at",
        "finished_at",
    ];
    let mut got: Vec<&str> = bare[0]
        .as_object()
        .expect("a job object")
        .keys()
        .map(String::as_str)
        .collect();
    got.sort_unstable();
    let mut want: Vec<&str> = JOB_FIELDS.to_vec();
    want.sort_unstable();
    assert_eq!(
        got, want,
        "the Job wire shape is a golden: this card adds query params, not fields"
    );

    let paged = get_json(&state, "/jobs?cursor=&limit=50").await;
    assert!(
        paged.get("items").is_some() && paged.get("next_cursor").is_some(),
        "?cursor= (even empty) is still {{items,next_cursor}}: {paged}"
    );
    assert_eq!(ids(&paged), vec![jid(5), jid(3), jid(4), jid(2), jid(1)]);
    assert_eq!(
        paged["next_cursor"],
        Value::Null,
        "five rows under a limit of fifty is the last page"
    );
}
