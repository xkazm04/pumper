//! Storage layer of the durable event log and its cursor subscriptions (N05),
//! against a real temp-dir SQLite with the full migration chain.
//!
//! The three properties the whole feature rests on, each named after the
//! anti-pattern it defends:
//!
//! - **The sequence survives a restart.** The in-memory bus used to start its
//!   counter at 0 on every boot, so a client's `Last-Event-ID` pointed at
//!   somebody else's event and the only honest answer was `reset`.
//! - **A cursor only moves forward.** Two drains (or a drain racing a restart)
//!   must never be able to rewind a cursor and re-deliver, or skip.
//! - **A managed upsert is fenced on its owner.** The peer reconcile's raw
//!   `sqlx` INSERT had this clause hand-written beside the storage layer's;
//!   this is the storage-layer writer it was ported onto.

use pumper_core::{NewEvent, Storage};
use serde_json::json;

fn ev(seq: i64, kind: &str, app: &str, subject: &str) -> NewEvent {
    NewEvent {
        seq,
        kind: kind.into(),
        app: app.into(),
        subject_id: subject.into(),
        payload: json!({ "seq": seq }),
    }
}

async fn seeded(storage: &Storage) {
    storage
        .append_events(&[
            ev(1, "job.queued", "fake", "j1"),
            ev(2, "job.succeeded", "fake", "j1"),
            ev(3, "dataset.changed", "grants", "unified"),
        ])
        .await
        .expect("append");
}

#[tokio::test]
async fn max_seq_survives_the_process_that_wrote_it() {
    let store = pumper_core::testing::TempStore::new("event-log-seq").await;
    let storage = &store.storage;
    assert_eq!(
        storage.max_event_seq().await.expect("empty log"),
        0,
        "an empty log seeds a fresh counter, not a phantom offset"
    );
    seeded(storage).await;
    // This is what a boot reads: the counter picks up at 3, so the NEXT event is
    // 4 and a client holding `Last-Event-ID: 2` asks for a real gap.
    assert_eq!(storage.max_event_seq().await.expect("seeded log"), 3);
    assert_eq!(storage.count_events().await.expect("count"), 3);
}

#[tokio::test]
async fn re_appending_a_seq_is_ignored_not_a_failed_batch() {
    let store = pumper_core::testing::TempStore::new("event-log-dup").await;
    let storage = &store.storage;
    seeded(storage).await;
    // A retried flush re-offers rows the log already has, alongside a new one.
    let landed = storage
        .append_events(&[
            ev(2, "job.succeeded", "fake", "j1"),
            ev(4, "external", "src", "e1"),
        ])
        .await
        .expect("append with an overlap");
    assert_eq!(landed, 1, "only the genuinely new row lands");
    assert_eq!(storage.count_events().await.expect("count"), 4);
}

#[tokio::test]
async fn events_after_pages_forward_and_filters() {
    let store = pumper_core::testing::TempStore::new("event-log-page").await;
    let storage = &store.storage;
    seeded(storage).await;

    let page = storage
        .events_after(0, None, None, 2)
        .await
        .expect("page 1");
    let seqs: Vec<i64> = page.iter().map(|e| e.seq).collect();
    assert_eq!(
        seqs,
        vec![1, 2],
        "ascending, because a cursor reads forward"
    );

    let page = storage
        .events_after(2, None, None, 10)
        .await
        .expect("page 2");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].kind, "dataset.changed");
    assert_eq!(page[0].payload["seq"], 3, "the payload round-trips as JSON");

    let filtered = storage
        .events_after(0, Some("job.queued"), None, 10)
        .await
        .expect("kind filter");
    assert_eq!(filtered.len(), 1);
    let filtered = storage
        .events_after(0, None, Some("grants"), 10)
        .await
        .expect("app filter");
    assert_eq!(filtered.len(), 1);
}

#[tokio::test]
async fn retention_off_deletes_nothing() {
    let store = pumper_core::testing::TempStore::new("event-log-retention").await;
    let storage = &store.storage;
    seeded(storage).await;
    // `0` is "keep forever", never "keep zero days" — the difference between a
    // knob left at its default and a log wiped on the first janitor pass.
    assert_eq!(storage.prune_events(0).await.expect("off"), 0);
    assert_eq!(storage.prune_events(-1).await.expect("negative"), 0);
    assert_eq!(storage.count_events().await.expect("count"), 3);
    // Nothing here is a week old, so a live window is also a no-op.
    assert_eq!(storage.prune_events(7).await.expect("7d"), 0);
    assert_eq!(storage.count_events().await.expect("count"), 3);
}

#[tokio::test]
async fn a_cursor_never_moves_backwards() {
    let store = pumper_core::testing::TempStore::new("event-log-cursor").await;
    let storage = &store.storage;
    let sub = storage
        .create_subscription(
            Some("downstream"),
            &json!({ "kinds": ["job.succeeded"] }),
            "webhook",
            "https://example.test/hook",
            Some("s3cret"),
            0,
            None,
        )
        .await
        .expect("create subscription");
    assert_eq!(sub.cursor_seq, 0);
    assert!(sub.enabled);

    assert!(storage
        .advance_subscription_cursor(&sub.id, 5)
        .await
        .expect("advance"));
    // The rewind a racing drain would perform: refused, silently and safely.
    assert!(
        !storage
            .advance_subscription_cursor(&sub.id, 3)
            .await
            .expect("rewind"),
        "a lower seq must not move the cursor back"
    );
    let after = storage
        .get_subscription(&sub.id)
        .await
        .expect("get")
        .expect("row");
    assert_eq!(after.cursor_seq, 5);
    assert!(after.last_delivered_at.is_some());

    // An error is recorded WITHOUT touching the cursor: an undelivered event
    // must still be there for the next tick.
    storage
        .record_subscription_error(&sub.id, "receiver down")
        .await
        .expect("record error");
    let after = storage
        .get_subscription(&sub.id)
        .await
        .expect("get")
        .expect("row");
    assert_eq!(after.cursor_seq, 5);
    assert_eq!(after.last_error.as_deref(), Some("receiver down"));

    // ...and a later success clears it rather than leaving a stale string.
    assert!(storage
        .advance_subscription_cursor(&sub.id, 6)
        .await
        .expect("advance"));
    let after = storage
        .get_subscription(&sub.id)
        .await
        .expect("get")
        .expect("row");
    assert!(after.last_error.is_none());

    assert!(storage
        .set_subscription_enabled(&sub.id, false)
        .await
        .expect("disable"));
    assert!(storage
        .list_subscriptions(true)
        .await
        .expect("enabled only")
        .is_empty());
    assert_eq!(
        storage.list_subscriptions(false).await.expect("all").len(),
        1
    );
    assert!(storage.delete_subscription(&sub.id).await.expect("delete"));
}

#[tokio::test]
async fn a_watch_carries_a_cursor_like_any_other_subscription() {
    let store = pumper_core::testing::TempStore::new("event-log-watch").await;
    let storage = &store.storage;
    let watch = storage
        .create_watch("fake", "*", "https://example.test/w", None, "webhook", 0)
        .await
        .expect("create watch");
    assert_eq!(watch.cursor_seq, 0, "a new watch starts at the beginning");

    assert!(storage
        .advance_watch_cursor(&watch.id, 4)
        .await
        .expect("advance"));
    assert!(
        !storage
            .advance_watch_cursor(&watch.id, 2)
            .await
            .expect("rewind"),
        "the watch cursor has the same monotonic fence"
    );
    let all = storage
        .enabled_watches_all()
        .await
        .expect("enabled watches");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].cursor_seq, 4);

    storage
        .set_watch_enabled(&watch.id, false)
        .await
        .expect("disable");
    assert!(
        storage
            .enabled_watches_all()
            .await
            .expect("enabled watches")
            .is_empty(),
        "a disabled watch leaves the drain's view"
    );
}

#[tokio::test]
async fn a_managed_upsert_cannot_take_over_someone_elses_schedule() {
    let store = pumper_core::testing::TempStore::new("event-log-schedule").await;
    let storage = &store.storage;

    assert!(storage
        .upsert_managed_schedule(
            "peer:a",
            "peer",
            "0 0 * * * *",
            &json!({ "n": 1 }),
            true,
            "peer"
        )
        .await
        .expect("insert"));
    // Same owner: an update, not a duplicate.
    assert!(storage
        .upsert_managed_schedule(
            "peer:a",
            "peer",
            "0 30 * * * *",
            &json!({ "n": 2 }),
            false,
            "peer"
        )
        .await
        .expect("update"));
    let rows = storage.list_schedules().await.expect("list");
    let row = rows.iter().find(|s| s.id == "peer:a").expect("row");
    assert_eq!(row.cron, "0 30 * * * *");
    assert!(!row.enabled);
    assert_eq!(row.params["n"], 2);

    // A DIFFERENT owner is refused outright — the fence the raw peer INSERT had
    // to hand-write, now in one place with a test on it.
    assert!(
        !storage
            .upsert_managed_schedule(
                "peer:a",
                "peer",
                "0 0 1 * * *",
                &json!({ "n": 3 }),
                true,
                "catalog",
            )
            .await
            .expect("foreign upsert"),
        "a reconcile owned by `catalog` must not overwrite a `peer`-owned row"
    );
    let rows = storage.list_schedules().await.expect("list");
    let row = rows.iter().find(|s| s.id == "peer:a").expect("row");
    assert_eq!(row.cron, "0 30 * * * *", "the peer row is untouched");
}
