//! App-declared schedules (P.4): what only a database can answer about
//! `AppContext::request_schedule` + `apply_schedule_requests`.
//!
//! The unit tests in `app.rs` cover the pure half — ownership tag, derived id,
//! cron validation. This covers the writes: that the per-run cap bounds the
//! rows, that re-running the same job re-syncs one row instead of minting a
//! schedule per night, and that the `managed_by` fence leaves a hand-made
//! schedule with the same id untouched.

use pumper_core::app::{apply_schedule_requests, schedule_request};
use pumper_core::testing::TempStore;
use serde_json::{json, Value};

fn watch(url: &str) -> Value {
    json!({"app": "watch", "cron": "0 0 6 * * *", "params": {"url": url}})
}

#[tokio::test]
async fn an_apps_requests_become_real_rows_capped_by_the_per_run_ceiling() {
    let store = TempStore::new("app-schedules-cap").await;
    let requests: Vec<Value> = (0..5).map(|i| watch(&format!("https://a/{i}"))).collect();

    let out = apply_schedule_requests(&store.storage, "research", &requests, 3).await;
    assert_eq!(
        out.created, 3,
        "the ceiling bounds the rows, not the asking"
    );
    assert!(out.rejected.is_empty());
    assert_eq!(out.not_owned, 0);

    let rows = store.storage.list_schedules().await.expect("list");
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(row.app, "watch");
        assert_eq!(
            row.managed_by.as_deref(),
            Some("app:research"),
            "every app-declared row carries the fence tag"
        );
        assert!(row.enabled, "an app-declared schedule is created running");
    }

    // A cap of zero refuses every app-declared schedule rather than writing one.
    let none = apply_schedule_requests(&store.storage, "research", &requests, 0).await;
    assert_eq!(none.created, 0);
    assert_eq!(store.storage.list_schedules().await.expect("list").len(), 3);
}

#[tokio::test]
async fn re_running_the_same_job_re_syncs_one_row_not_a_schedule_per_night() {
    let store = TempStore::new("app-schedules-idempotent").await;
    let requests = vec![watch("https://a/")];
    for _ in 0..3 {
        let out = apply_schedule_requests(&store.storage, "research", &requests, 20).await;
        assert_eq!(out.created, 1);
    }
    assert_eq!(
        store.storage.list_schedules().await.expect("list").len(),
        1,
        "a nightly run asking to watch the same URL must not mint a schedule a night"
    );
}

#[tokio::test]
async fn the_fence_leaves_a_schedule_owned_by_somebody_else_untouched() {
    let store = TempStore::new("app-schedules-fence").await;
    let body = watch("https://a/");
    let req = schedule_request("research", &body).expect("well formed");

    // Somebody else already owns that id (a hand-made row is `managed_by` NULL;
    // this is the harder case — another manager's row).
    store
        .storage
        .upsert_managed_schedule(&req.id, "watch", "0 0 3 * * *", &json!({}), true, "catalog")
        .await
        .expect("seed");

    let out = apply_schedule_requests(&store.storage, "research", &[body], 20).await;
    assert_eq!(out.created, 0);
    assert_eq!(out.not_owned, 1, "the fence held and said so");
    let rows = store.storage.list_schedules().await.expect("list");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cron, "0 0 3 * * *", "the owner's cron survived");
    assert_eq!(rows[0].managed_by.as_deref(), Some("catalog"));
}

#[tokio::test]
async fn an_unusable_request_is_reported_rather_than_dropped() {
    let store = TempStore::new("app-schedules-reject").await;
    let out = apply_schedule_requests(
        &store.storage,
        "research",
        &[
            json!({"app": "watch", "cron": "every tuesday"}),
            watch("https://a/"),
        ],
        20,
    )
    .await;
    assert_eq!(out.created, 1);
    assert_eq!(out.rejected.len(), 1);
    assert!(
        out.rejected[0].contains("invalid cron"),
        "{:?}",
        out.rejected
    );
}
