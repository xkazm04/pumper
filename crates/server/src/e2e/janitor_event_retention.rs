//! The event log is pruned by the store janitor, on a deployment where every
//! other janitor knob is off (FIXES-WAVE-3 §3, P.5).
//!
//! THE REFUTED ARRANGEMENT: the prune lived in the outbox drain
//! (`subscriptions::drain`), gated by a process-wide hourly instant, because
//! the store janitor was believed to return early unless one of its own knobs
//! was enabled — that early return is `retention_janitor`, a different loop.
//! The consequence was retention whose cadence was "however often a job
//! finished", outside the activity gate every other janitor delete respects.

use pumper_core::{Config, NewEvent};
use serde_json::json;

use super::harness::test_state;

/// Backdates every log row by `days`, which `append_events` cannot do (it
/// stamps `created_at` itself — correctly, since a caller-supplied timestamp on
/// an append-only log is a lie waiting to happen).
async fn age_every_event(pool: &sqlx::SqlitePool, days: i64) {
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(days))
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    sqlx::query("UPDATE events SET created_at = ?1")
        .bind(cutoff)
        .execute(pool)
        .await
        .expect("backdate");
}

fn event(seq: i64) -> NewEvent {
    NewEvent {
        seq,
        kind: "job.progress".into(),
        app: "fake".into(),
        subject_id: format!("job-{seq}"),
        payload: json!({"n": seq}),
    }
}

#[tokio::test]
async fn the_janitor_prunes_the_event_log_with_every_other_knob_off() {
    let (state, store) = test_state(vec![]).await;
    // Everything else a janitor pass could do is off / empty: no revision or
    // artifact retention, no ledger knobs, no cache to purge. Only the event
    // log has work, so a pass that prunes nothing is the old behaviour.
    let defaults = Config::default();
    assert_eq!(
        state.config.events.log_retention_days, defaults.events.log_retention_days,
        "the retention window under test is the shipped default"
    );

    let events: Vec<NewEvent> = (1..=5).map(event).collect();
    store.storage.append_events(&events).await.expect("append");
    assert_eq!(store.storage.count_events().await.expect("count"), 5);

    // Fresh rows are inside the window: a pass must delete none of them.
    let (purged, failures) = crate::store_janitor_pass(&state).await;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(purged, 0);
    assert_eq!(store.storage.count_events().await.expect("count"), 5);

    age_every_event(
        &store.storage.pool(),
        state.config.events.log_retention_days + 1,
    )
    .await;
    let (purged, failures) = crate::store_janitor_pass(&state).await;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(purged, 5, "the janitor's own pass reclaims the aged log");
    assert_eq!(store.storage.count_events().await.expect("count"), 0);
}

#[tokio::test]
async fn retention_off_keeps_the_log_forever_rather_than_keeping_zero_days() {
    let (mut state, store) = test_state(vec![]).await;
    let mut config = (*state.config).clone();
    config.events.log_retention_days = 0;
    state.config = std::sync::Arc::new(config);

    store
        .storage
        .append_events(&[event(1)])
        .await
        .expect("append");
    age_every_event(&store.storage.pool(), 3650).await;

    let (purged, failures) = crate::store_janitor_pass(&state).await;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(purged, 0);
    assert_eq!(
        store.storage.count_events().await.expect("count"),
        1,
        "`0` means keep forever, never keep zero days"
    );
}
