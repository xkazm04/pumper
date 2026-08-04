//! The trigger evaluation-set cache, from the outside: a job completion with
//! nothing configured must not touch the `triggers` table at all, and a trigger
//! CRUD operation must be visible to the very NEXT firing decision — there is
//! no acceptable window in which a cache serves a pre-mutation answer.

use std::sync::Arc;

use pumper_core::{EnqueueOptions, JobStatus, NewTrigger};
use serde_json::json;

use super::harness::{test_state, FakeApp};
use crate::state::AppState;
use crate::triggers::fire_terminal_triggers;

/// A finished source job of app `fake`, the input to a terminal-trigger hop.
async fn succeeded_source(state: &AppState) -> pumper_core::Job {
    let job = state
        .storage
        .enqueue("fake", EnqueueOptions::default())
        .await
        .unwrap();
    let mut job = state.storage.get(job.id).await.unwrap().unwrap();
    job.status = JobStatus::Succeeded;
    job
}

async fn job_trigger(state: &AppState, name: &str) -> pumper_core::Trigger {
    state
        .storage
        .create_trigger(&NewTrigger {
            name: Some(name),
            source_kind: "job",
            source_app: "fake",
            source_dataset: None,
            on_change: None,
            on_status: Some("succeeded"),
            target_app: "fake",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: None,
        })
        .await
        .unwrap()
}

/// How many hops this trigger has enqueued so far.
async fn hop_count(state: &AppState, trigger_id: &str) -> usize {
    state
        .storage
        .jobs_by_trigger(trigger_id, 100)
        .await
        .unwrap()
        .len()
}

/// The waste this cache exists to remove: every single job completion used to
/// run `SELECT … FROM triggers` — twice — only to learn that a fleet with no
/// triggers configured still has none.
#[tokio::test]
async fn repeat_completions_with_no_triggers_do_not_requery_the_table() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let job = succeeded_source(&state).await;

    for _ in 0..10 {
        fire_terminal_triggers(&state, &job).await;
    }
    assert_eq!(
        state.trigger_cache.db_loads(),
        1,
        "one cold load, then the empty set answers every later completion"
    );
}

/// The coherence bar: a create / disable / delete must be visible to the very
/// next firing decision, with no stale window a test can catch.
#[tokio::test]
async fn trigger_crud_is_visible_to_the_next_firing_decision_not_the_one_after() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let job = succeeded_source(&state).await;

    // Warm the cache with the EMPTY set — the state that would hide a create.
    fire_terminal_triggers(&state, &job).await;
    let cold_loads = state.trigger_cache.db_loads();

    // CREATE → the next decision fires it.
    let trigger = job_trigger(&state, "chained").await;
    fire_terminal_triggers(&state, &job).await;
    assert_eq!(
        hop_count(&state, &trigger.id).await,
        1,
        "a trigger created after the cache warmed must still fire"
    );
    assert!(
        state.trigger_cache.db_loads() > cold_loads,
        "the create invalidated the cached set"
    );

    // DISABLE → the next decision drops it. A fresh source job so the hop is
    // not merely dedup-suppressed by the idempotency key.
    let job2 = succeeded_source(&state).await;
    assert!(state
        .storage
        .set_trigger_enabled(&trigger.id, false)
        .await
        .unwrap());
    fire_terminal_triggers(&state, &job2).await;
    assert_eq!(
        hop_count(&state, &trigger.id).await,
        1,
        "a disabled trigger must not fire off a cached enabled set"
    );

    // RE-ENABLE → back in the set immediately.
    assert!(state
        .storage
        .set_trigger_enabled(&trigger.id, true)
        .await
        .unwrap());
    fire_terminal_triggers(&state, &job2).await;
    assert_eq!(
        hop_count(&state, &trigger.id).await,
        2,
        "re-enabling is visible to the next decision too"
    );

    // DELETE → gone from the set immediately.
    let job3 = succeeded_source(&state).await;
    assert!(state.storage.delete_trigger(&trigger.id).await.unwrap());
    fire_terminal_triggers(&state, &job3).await;
    assert_eq!(
        hop_count(&state, &trigger.id).await,
        2,
        "a deleted trigger must not fire off a cached set"
    );
}

/// The cache is per (kind, app): warming one app's set must not make another
/// app's triggers invisible, nor leak them into the wrong app's decisions.
#[tokio::test]
async fn one_apps_warm_cache_does_not_answer_for_another_app() {
    let (state, _store) = test_state(vec![Arc::new(FakeApp)]).await;
    let trigger = job_trigger(&state, "fake-only").await;

    // A completion of a DIFFERENT app warms its own (empty) entry.
    let mut other = state
        .storage
        .enqueue("other", EnqueueOptions::default())
        .await
        .unwrap();
    other.status = JobStatus::Succeeded;
    fire_terminal_triggers(&state, &other).await;
    assert_eq!(hop_count(&state, &trigger.id).await, 0);

    // …and `fake`'s own completion still sees `fake`'s trigger.
    let job = succeeded_source(&state).await;
    fire_terminal_triggers(&state, &job).await;
    assert_eq!(hop_count(&state, &trigger.id).await, 1);
}
