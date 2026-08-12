//! The scheduler's overlap guard — the piece with the subtle invariant:
//! while a schedule's previous job is still active, a due firing is HELD and
//! `last_run` is NOT touched, so the firing stays due and fires on the first
//! tick after the job finishes. A regression that advances `last_run` there
//! silently drops the firing forever — and only this test would notice.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use pumper_core::{JobStatus, NewSchedule};
use serde_json::json;

use super::harness::{test_state, FakeApp};
use crate::scheduler;

#[tokio::test]
async fn overlap_guard_holds_a_due_firing_and_releases_it_after_completion() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;

    // Every-minute cron, created now; make it due by backdating created_at.
    let schedule = state
        .storage
        .create_schedule(NewSchedule {
            app: "fake",
            cron: "0 * * * * *",
            params: json!({}),
            priority: 0,
            timezone: None,
            misfire_policy: "fire_once",
            max_attempts: Some(1),
            budget_usd: None,
        })
        .await
        .expect("create schedule");
    sqlx::query("UPDATE schedules SET created_at = ?1 WHERE id = ?2")
        .bind((Utc::now() - chrono::Duration::minutes(5)).to_rfc3339())
        .bind(&schedule.id)
        .execute(&state.storage.pool())
        .await
        .unwrap();

    let mut cron_cache = HashMap::new();

    // First reconcile: due → exactly one job enqueued, last_run advanced.
    scheduler::reconcile(&state, &mut cron_cache, None, Utc::now())
        .await
        .unwrap();
    let count = jobs_for(&state, "fake").await;
    assert_eq!(count, 1, "due schedule fires once");
    let last_run_1 = last_run(&state, &schedule.id).await.expect("last_run set");

    // Second reconcile a minute later, previous job still queued (active):
    // the guard holds the firing AND leaves last_run untouched.
    let later = Utc::now() + chrono::Duration::minutes(2);
    scheduler::reconcile(&state, &mut cron_cache, None, later)
        .await
        .unwrap();
    assert_eq!(
        jobs_for(&state, "fake").await,
        1,
        "no stacked run while one is active"
    );
    assert_eq!(
        last_run(&state, &schedule.id).await.expect("last_run"),
        last_run_1,
        "held firing must NOT advance last_run — it stays due"
    );

    // Finish the job; the held firing now fires on the next tick.
    let job_id: String = sqlx::query_scalar("SELECT id FROM jobs LIMIT 1")
        .fetch_one(&state.storage.pool())
        .await
        .unwrap();
    let job = state
        .storage
        .get(job_id.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    let claimed = state
        .storage
        .claim_next(&[], 0.0)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(claimed.id, job.id);
    assert!(state
        .storage
        .complete(claimed.id, claimed.attempts, json!({ "ok": true }))
        .await
        .unwrap());
    assert_eq!(
        state.storage.get(job.id).await.unwrap().unwrap().status,
        JobStatus::Succeeded
    );

    scheduler::reconcile(&state, &mut cron_cache, None, later)
        .await
        .unwrap();
    assert_eq!(
        jobs_for(&state, "fake").await,
        2,
        "released firing fires after completion"
    );
}

async fn jobs_for(state: &crate::state::AppState, app: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE app = ?1")
        .bind(app)
        .fetch_one(&state.storage.pool())
        .await
        .unwrap()
}

async fn last_run(state: &crate::state::AppState, id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT last_run FROM schedules WHERE id = ?1")
        .bind(id)
        .fetch_one(&state.storage.pool())
        .await
        .unwrap()
}
