//! The durable event log and its cursor subscriptions (N05), end to end.
//!
//! Two gates, both named after what was broken before:
//!
//! 1. **A restart no longer costs a client its place.** The bus's sequence used
//!    to start at `0` on every boot, and a client reconnecting with
//!    `Last-Event-ID` was either lied to (a fresh event carrying an id the
//!    previous process had already used) or told `reset` — "throw away your view
//!    and resync". The test boots a SECOND `AppState` over the SAME store, the
//!    way a restart does, and asserts the sequence continues and the gap is
//!    served from the table with **zero** `reset`.
//! 2. **Every kind reaches every sink through the same ladder.** A subscription
//!    with a `plugin:` sink (N10) delivers, and a permanent refusal dead-letters
//!    immediately — the DLQ semantics of the webhook sink, on an event kind that
//!    before N05 had no durable delivery path at all.

use std::sync::Arc;

use pumper_core::config::GovernorConfig;
use pumper_core::testing::{dead_engines, TempStore};
use pumper_core::{Config, EnqueueOptions, Governor, NoPlugins, NoSearch, ScrapeApp};
use serde_json::{json, Value};

use super::harness::{test_state, FakeApp};
use crate::events::JobEvent;
use crate::state::{AppState, AppStateParts};

/// A second `AppState` over an EXISTING store — a restart, as far as the data is
/// concerned: new bus, empty ring, same `events` table. The one thing `init`
/// does that `from_parts` cannot (it is sync and does no IO) is seed the
/// sequence, so the test does it here exactly as `init` does.
async fn restart(store: &TempStore, apps: Vec<Arc<dyn ScrapeApp>>) -> AppState {
    let mut config = Config::default();
    config.storage.database_path = store
        .storage
        .artifacts_dir
        .parent()
        .unwrap()
        .join("pumper.db");
    config.worker.poll_interval_secs = 1;
    let mut registry = std::collections::HashMap::new();
    for app in apps {
        registry.insert(app.name().to_string(), app);
    }
    let state = AppState::from_parts(AppStateParts {
        config,
        storage: Arc::new(store.storage.clone()),
        governor: Arc::new(Governor::new(&GovernorConfig::default())),
        engines: dead_engines(),
        plugins: Arc::new(NoPlugins),
        search: Arc::new(NoSearch),
        registry,
    })
    .expect("assemble restarted AppState");
    let seq = state.storage.max_event_seq().await.expect("max seq");
    state.events.seed_seq(seq as u64);
    state
}

async fn run_sync_job(state: &AppState) {
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
    assert!(crate::worker::run_one(state).await, "job must be claimed");
}

/// **The gate the item promises.** Across a restart: the sequence continues, the
/// gap is served from the table, and no `reset` is produced.
#[tokio::test]
async fn last_event_id_resumes_from_the_table_across_a_restart_with_zero_resets() {
    let (state, store) = test_state(vec![Arc::new(FakeApp)]).await;
    assert!(state.events.logs(), "the log ships ON");

    // A real run, so the log holds real events (queued/running/succeeded plus
    // the `dataset.changed` its fan-out emits), not synthetic ones.
    run_sync_job(&state).await;
    // The fan-out already drained; flush anything the terminal event queued.
    state
        .events
        .persist_pending(&state.storage)
        .await
        .expect("flush");
    let before = state.events.latest_seq();
    assert!(before >= 3, "a run emits several events, got {before}");
    let logged = state.storage.count_events().await.expect("count");
    assert_eq!(
        logged as u64, before,
        "every emitted event is in the log, not just the ones something was listening for"
    );

    // ---- the restart -----------------------------------------------------
    let after = restart(&store, vec![Arc::new(FakeApp)]).await;

    // The counter picked up where the last process stopped. Before N05 this was
    // 0, so the next event would have re-used an id already on the wire.
    assert_eq!(
        after.events.latest_seq(),
        before,
        "the sequence survives the process that assigned it"
    );

    // The new process's ring is EMPTY, so this can only be answered by the
    // table — which is the whole point.
    let replay = crate::routes::replay_or_log(&after, 1)
        .await
        .expect("a gap inside the retention window is NOT a reset");
    let seqs: Vec<u64> = replay.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(
        seqs,
        (2..=before).collect::<Vec<u64>>(),
        "every event after the client's cursor, in order, from the log"
    );
    assert!(
        replay
            .iter()
            .any(|(_, ev)| ev.status == crate::subscriptions::DATASET_CHANGED),
        "a domain event replays like a job transition — one log, one vocabulary"
    );

    // A client that was fully caught up gets an empty replay, not a reset.
    let none = crate::routes::replay_or_log(&after, before)
        .await
        .expect("a current cursor is never a reset");
    assert!(none.is_empty());

    // And the next emit continues the sequence rather than colliding with it.
    let next = after
        .events
        .emit(JobEvent::new(uuid::Uuid::new_v4(), "fake", "queued"));
    assert_eq!(next, before + 1);
}

/// The pull surface answers what the stream answered, from the same log.
#[tokio::test]
async fn the_log_page_walks_forward_and_says_when_you_are_caught_up() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    run_sync_job(&state).await;
    state
        .events
        .persist_pending(&state.storage)
        .await
        .expect("flush");

    let head = state.events.latest_seq() as i64;
    let page = state
        .storage
        .events_after(0, None, None, 2)
        .await
        .expect("page");
    assert_eq!(page.len(), 2);
    assert!(
        page[0].seq < page[1].seq,
        "ascending — a cursor reads forward"
    );

    // Filtering by kind is what makes a narrow consumer cheap.
    let changed = state
        .storage
        .events_after(0, Some(crate::subscriptions::DATASET_CHANGED), None, 50)
        .await
        .expect("kind page");
    assert_eq!(changed.len(), 1, "one changed dataset → one event");
    assert_eq!(changed[0].app, "fake");
    assert_eq!(changed[0].subject_id, "d");
    assert_eq!(changed[0].payload["result"]["count"], 2);

    let tail = state
        .storage
        .events_after(head, None, None, 50)
        .await
        .expect("tail");
    assert!(tail.is_empty(), "past the head there is nothing to read");
}

// ── subscriptions with a plugin sink ─────────────────────────────────────────

/// A canned connector, same shape as `sink_delivery`'s: whatever verdict the
/// test hands it, plus a record of the envelope.
struct StubConnector {
    verdict: Value,
    seen: std::sync::Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl pumper_core::Plugins for StubConnector {
    async fn run(&self, _name: &str, input: &str, _params: &Value) -> pumper_core::Result<Value> {
        let envelope: Value = serde_json::from_str(input).expect("the host hands the module JSON");
        self.seen.lock().expect("seen").push(envelope);
        Ok(self.verdict.clone())
    }
    fn list(&self) -> Vec<String> {
        vec!["sink-stub".into()]
    }
    async fn reload(&self) -> pumper_core::Result<usize> {
        Ok(1)
    }
}

/// **The second gate.** A subscription with a `plugin:` sink delivers a
/// `job.succeeded` — a kind that had NO durable delivery path before N05 — and a
/// permanent refusal dead-letters immediately, exactly as the webhook sink's
/// does. The cursor advances either way, because the delivery row exists and the
/// DLQ owns what happens next.
#[tokio::test]
async fn a_plugin_sink_subscription_delivers_and_dead_letters_like_a_webhook() {
    let connector = Arc::new(StubConnector {
        verdict: json!({ "delivered": false, "permanent": true, "error": "422 schema" }),
        seen: std::sync::Mutex::new(Vec::new()),
    });
    let (state, _store) =
        super::harness::test_state_with_plugins(vec![Arc::new(FakeApp)], connector.clone()).await;

    let sub = state
        .storage
        .create_subscription(
            Some("agent"),
            &json!({ "kinds": ["job.succeeded"] }),
            "plugin:sink-stub",
            "",
            None,
            0,
            None,
        )
        .await
        .expect("create subscription");

    run_sync_job(&state).await;

    // The connector saw the event envelope — one call, for the one matching kind.
    let seen = connector.seen.lock().expect("seen").clone();
    assert_eq!(
        seen.len(),
        1,
        "the selector kept ONE of the run's events, not all of them: {seen:?}"
    );
    assert_eq!(seen[0]["event"], "job.succeeded");
    assert_eq!(seen[0]["body"]["kind"], "job.succeeded");
    assert!(
        seen[0]["body"]["seq"].is_number(),
        "a cursor consumer is told where it now is: {}",
        seen[0]["body"]
    );
    assert!(seen[0]["delivery_id"].is_string());

    // Permanent refusal → `dead` NOW, on the ordinary ladder, in the ordinary
    // DLQ, under the subscription's own delivery kind.
    let dead = state
        .storage
        .list_deliveries(Some("dead"), 10)
        .await
        .expect("list dead");
    assert_eq!(
        dead.len(),
        1,
        "a permanent refusal dead-letters immediately"
    );
    assert_eq!(
        dead[0].kind,
        pumper_core::storage::DELIVERY_KIND_SUBSCRIPTION
    );
    assert_eq!(dead[0].ref_id, sub.id);
    assert_eq!(dead[0].url, "plugin://sink-stub");
    assert!(dead[0]
        .last_error
        .as_deref()
        .unwrap_or_default()
        .contains("422 schema"));

    // The cursor advanced past the delivered event even though the receiver
    // refused: the delivery row is the handoff, and the DLQ owns the rest. If
    // the cursor stalled here, one dead receiver would freeze the subscription
    // forever AND duplicate the retry ladder.
    let after = state
        .storage
        .get_subscription(&sub.id)
        .await
        .expect("get")
        .expect("row");
    assert!(
        after.cursor_seq >= 1,
        "the cursor moves on a durable handoff, not on the receiver's answer"
    );
    assert!(
        after.last_error.is_none(),
        "a refused DELIVERY is not a drain error — the DLQ records it: {:?}",
        after.last_error
    );

    // …and the subscription's own delivery log answers "did this ever deliver?".
    let rows = state
        .storage
        .list_deliveries_for_ref_page(
            pumper_core::storage::DELIVERY_KIND_SUBSCRIPTION,
            &sub.id,
            None,
            None,
            10,
        )
        .await
        .expect("per-subscription deliveries");
    assert_eq!(rows.len(), 1);
}

/// The negative that keeps the test above from passing for the wrong reason: a
/// selector that matches nothing delivers nothing — and still advances its
/// cursor, so it does not rescan the log on every tick forever.
#[tokio::test]
async fn a_selector_that_matches_nothing_delivers_nothing_but_still_advances() {
    let connector = Arc::new(StubConnector {
        verdict: json!({ "delivered": true }),
        seen: std::sync::Mutex::new(Vec::new()),
    });
    let (state, _store) =
        super::harness::test_state_with_plugins(vec![Arc::new(FakeApp)], connector.clone()).await;
    let sub = state
        .storage
        .create_subscription(
            None,
            &json!({ "kinds": ["nothing.happens.here"] }),
            "plugin:sink-stub",
            "",
            None,
            0,
            None,
        )
        .await
        .expect("create subscription");

    run_sync_job(&state).await;

    assert!(
        connector.seen.lock().expect("seen").is_empty(),
        "an unmatched selector delivers nothing"
    );
    let after = state
        .storage
        .get_subscription(&sub.id)
        .await
        .expect("get")
        .expect("row");
    assert_eq!(
        after.cursor_seq as u64,
        state.events.latest_seq(),
        "an unmatched event is still CONSUMED — otherwise a narrow selector \
         rescans the whole log on every tick, forever"
    );
}
