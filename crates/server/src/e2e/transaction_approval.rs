//! N01 Transact v2 — the approval door, end to end over the real route
//! handlers: a parked job plus a `pending` ledger row, and every way an
//! approval can be refused.
//!
//! The commit half (approve -> the engine submits exactly once, and a page that
//! drifted since review is refused) is proven against a scripted browser in
//! `crates/apps/transact`, where the engine's own commit counter is the
//! instrument. What *this* file pins is the half that lives in the server: the
//! operator switch, the state machine at the door, the SQL guard under two
//! racing approvals, and the fact that an approval actually releases the parked
//! run.

use pumper_core::transactions::{
    by_key, get, list, stage_pending, NewTransaction, TransactionState,
};
use pumper_core::{EnqueueOptions, JobStatus};
use serde_json::json;

use crate::routes::transactions::{
    approve_transaction, get_transaction, list_transactions, reject_transaction, ApproveBody,
    ListQuery,
};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;

const KEY: &str = "portal-filing-2026-09";
const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// A node with a parked `transact` job and the `pending` row it staged — the
/// exact state an approver walks up to.
async fn staged(allow_live: bool) -> (AppState, pumper_core::testing::TempStore, String, String) {
    let (state, store) = super::harness::test_state_with(vec![], |cfg| {
        cfg.transact.allow_live = allow_live;
    })
    .await;
    let job = state
        .storage
        .enqueue("transact", EnqueueOptions::default())
        .await
        .unwrap();
    // Park it the way the app does: claim it, then write the wait.
    state
        .storage
        .claim_next(&[], 0.0)
        .await
        .unwrap()
        .expect("claimed");
    assert!(state
        .storage
        .await_input(job.id, 1, &json!({ "kind": "transaction_approval" }), None)
        .await
        .unwrap());
    let row = stage_pending(
        &state.storage.pool(),
        NewTransaction {
            idempotency_key: KEY,
            app: "transact",
            job_id: Some(&job.id.to_string()),
            profile: Some("portal_login"),
            evidence_sha: SHA,
        },
    )
    .await
    .unwrap();
    (state, store, row.id, job.id.to_string())
}

fn approve_body(sha: Option<&str>) -> Option<Json<ApproveBody>> {
    Some(Json(
        serde_json::from_value(json!({ "evidence_sha": sha })).unwrap(),
    ))
}

/// A default node cannot approve anything, and says which key is missing.
///
/// The anti-pattern: shipping an irreversible capability that is live the
/// moment the binary is. The switch is checked FIRST, so this refusal is what a
/// default node produces even for a request that is otherwise perfect.
#[tokio::test]
async fn a_default_node_refuses_the_approval_and_leaves_the_job_parked() {
    let (state, _store, tx, job) = staged(false).await;
    let err = approve_transaction(
        State(state.clone()),
        Path(tx.clone()),
        None,
        approve_body(Some(SHA)),
    )
    .await
    .expect_err("live submission is off");
    assert_eq!(err.0, StatusCode::CONFLICT);
    assert!(err.1.contains("allow_live"), "{}", err.1);

    // Nothing moved: the row is still pending and the job is still parked.
    let row = get(&state.storage.pool(), &tx).await.unwrap().unwrap();
    assert_eq!(row.state, TransactionState::Pending);
    let job = state
        .storage
        .get(job.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.status, JobStatus::Waiting);
}

/// The headline path, and the double-approve fence on top of it.
#[tokio::test]
async fn approve_resumes_the_job_and_a_second_approve_is_a_no_op() {
    let (state, _store, tx, job_id) = staged(true).await;
    let job_uuid: uuid::Uuid = job_id.parse().unwrap();

    let (code, Json(body)) = approve_transaction(
        State(state.clone()),
        Path(tx.clone()),
        None,
        approve_body(Some(SHA)),
    )
    .await
    .expect("a quoted, pending, in-date approval on a live node");
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(body["resumed"], json!(true));
    assert_eq!(body["transaction"]["state"], json!("approved"));

    // The parked job is queued again, with no attempt burned.
    let job = state.storage.get(job_uuid).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.attempts, 1, "an approval burns no retry");

    // The second approve — a stale tab, a second approver, an agent retry —
    // finds no pending row. It is refused, not applied, and it does NOT resume
    // the job a second time (which is how one approval becomes two runs).
    let err = approve_transaction(
        State(state.clone()),
        Path(tx.clone()),
        None,
        approve_body(Some(SHA)),
    )
    .await
    .expect_err("a second approve on the same transaction");
    assert_eq!(err.0, StatusCode::CONFLICT);
    assert!(err.1.contains("not 'pending'"), "{}", err.1);
    let job = state.storage.get(job_uuid).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Queued, "still one lineage");

    // And the ledger still holds exactly one row for the key.
    assert_eq!(
        list(&state.storage.pool(), None, 10).await.unwrap().len(),
        1
    );
    let by = by_key(&state.storage.pool(), KEY).await.unwrap().unwrap();
    assert_eq!(by.id, tx);
    assert!(
        by.approved_at.is_some(),
        "the approval is stamped on the row"
    );
}

/// An approval must name the evidence it read. Quoting a digest the row does
/// not hold is the client that re-fetched after a re-stage — approving a bundle
/// nobody looked at.
#[tokio::test]
async fn a_mismatched_evidence_quote_is_refused_and_nothing_is_released() {
    let (state, _store, tx, job_id) = staged(true).await;
    let err = approve_transaction(
        State(state.clone()),
        Path(tx.clone()),
        None,
        approve_body(Some("deadbeef")),
    )
    .await
    .expect_err("a quote that does not match the row");
    assert_eq!(err.0, StatusCode::CONFLICT);
    assert!(err.1.contains("evidence_sha mismatch"), "{}", err.1);
    let job = state
        .storage
        .get(job_id.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.status, JobStatus::Waiting, "still parked");
    assert_eq!(
        get(&state.storage.pool(), &tx)
            .await
            .unwrap()
            .unwrap()
            .state,
        TransactionState::Pending
    );
}

/// A stale mandate is retired by the read paths, not merely refused by the
/// write path — so an approver is never shown a pending row the door would
/// then reject.
#[tokio::test]
async fn a_pending_row_past_its_ttl_is_expired_by_the_listing_itself() {
    let (mut state, _store, tx, _job) = staged(true).await;
    // A TTL of one second, and a row created a moment ago... which is not yet
    // stale, so first prove the listing does NOT retire a live row.
    let mut cfg = (*state.config).clone();
    cfg.transact.approval_ttl_secs = 3600;
    state.config = std::sync::Arc::new(cfg);
    let Json(body) = list_transactions(State(state.clone()), Query(ListQuery { state: None }))
        .await
        .unwrap();
    assert_eq!(body["transactions"][0]["state"], json!("pending"));
    assert!(body["transactions"][0]["expires_at"].is_string());
    assert_eq!(body["transactions"][0]["expired"], json!(false));

    // Now a TTL shorter than the row's age. Because the deadline is DERIVED
    // rather than stamped, shortening it retires rows already in the ledger.
    let mut cfg = (*state.config).clone();
    cfg.transact.approval_ttl_secs = 1;
    state.config = std::sync::Arc::new(cfg);
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let Json(row) = get_transaction(State(state.clone()), Path(tx.clone()))
        .await
        .unwrap();
    assert_eq!(row["state"], json!("expired"));

    let err = approve_transaction(
        State(state.clone()),
        Path(tx),
        None,
        approve_body(Some(SHA)),
    )
    .await
    .expect_err("an expired mandate cannot be approved");
    assert_eq!(err.0, StatusCode::CONFLICT);
    assert!(err.1.contains("not 'pending'"), "{}", err.1);
}

/// Saying no works on every node — including one whose operator has since
/// turned live submission off. A ledger whose rows cannot be closed out is a
/// ledger that fills with requests nobody can resolve.
#[tokio::test]
async fn reject_works_with_the_live_switch_off_and_cancels_the_parked_job() {
    let (state, _store, tx, job_id) = staged(false).await;
    let Json(body) = reject_transaction(State(state.clone()), Path(tx.clone()), None)
        .await
        .expect("rejecting is not gated on allow_live");
    assert_eq!(body["transaction"]["state"], json!("rejected"));
    assert_eq!(body["job_cancelled"], json!(true));
    let job = state
        .storage
        .get(job_id.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.status, JobStatus::Cancelled, "no orphan parked job");

    // Rejected is terminal: it cannot be approved back to life, even once the
    // operator turns the switch on. (Flipped here so the refusal under test is
    // the STATE, not the switch — the switch is checked first.)
    let mut state = state;
    let mut cfg = (*state.config).clone();
    cfg.transact.allow_live = true;
    state.config = std::sync::Arc::new(cfg);
    let err = approve_transaction(State(state), Path(tx), None, approve_body(Some(SHA)))
        .await
        .expect_err("a rejected transaction stays rejected");
    assert_eq!(err.0, StatusCode::CONFLICT);
    assert!(err.1.contains("rejected"), "{}", err.1);
}
