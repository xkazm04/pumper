//! Does persisted politeness state tell the truth? These run against a real
//! temp-dir SQLite with the full migration chain, because every failure mode
//! here is a *storage* lie — a row that outlived the thing it described.
//!
//! Three anti-patterns, one per test:
//!  1. a host that recovered stays throttled forever (zombie penalty),
//!  2. a penalty nobody has confirmed in months comes back at full strength,
//!  3. `tier_memory` accrues one permanent row per host ever fetched.

use std::time::Duration;

use pumper_core::testing::TempStore;
use pumper_core::TierMemory;

/// Aging horizon short enough to cross in a test, long enough that a slow
/// machine can't cross it by accident between two adjacent statements.
const TTL_SECS: u64 = 1;
const PAST_TTL: Duration = Duration::from_millis(1_200);

#[tokio::test]
async fn zombie_penalty_not_resurrected_on_boot() {
    let store = TempStore::new("host-zombie").await;
    // Aging disabled: this test is purely about the decay-to-zero lie, so no
    // TTL can be blamed for the row disappearing.
    let tiers = TierMemory::new(store.storage.pool(), 0);

    // A 429 storm teaches the governor a 5s penalty; the write-behind pass
    // persists it. (Two hosts, so we also prove the pass is not "delete all".)
    tiers
        .persist_penalty_snapshot(&[
            ("slow.example".into(), 5_000),
            ("busy.example".into(), 2_000),
        ])
        .await
        .unwrap();
    assert_eq!(
        tiers.get("slow.example").await.unwrap().unwrap().penalty_ms,
        5_000
    );

    // slow.example recovers: repeated healthy responses halve its penalty away,
    // so the governor no longer reports it at all. The next pass must say so.
    tiers
        .persist_penalty_snapshot(&[("busy.example".into(), 2_000)])
        .await
        .unwrap();

    let recovered = tiers.get("slow.example").await.unwrap().unwrap();
    assert_eq!(
        recovered.penalty_ms, 0,
        "a decayed penalty must be zeroed in the store, not left behind"
    );
    assert!(
        recovered.penalty_updated_at.is_some(),
        "zeroing IS a write; it must be dated so the GC clock starts"
    );
    // Still penalized hosts are untouched by the same pass.
    assert_eq!(
        tiers.get("busy.example").await.unwrap().unwrap().penalty_ms,
        2_000
    );

    // Boot restore — the whole point: the recovered host is NOT resurrected.
    let loaded = tiers.load_penalties().await.unwrap();
    assert_eq!(
        loaded,
        vec![("busy.example".to_string(), 2_000)],
        "only hosts that are still penalized come back"
    );

    // An empty snapshot (every host recovered) zeroes the rest rather than
    // being treated as "nothing to say".
    tiers.persist_penalty_snapshot(&[]).await.unwrap();
    assert!(tiers.load_penalties().await.unwrap().is_empty());
    assert_eq!(
        tiers.get("busy.example").await.unwrap().unwrap().penalty_ms,
        0
    );
}

#[tokio::test]
async fn additive_save_penalties_never_zeroes_hosts_it_was_not_told_about() {
    // The host-weather import writes what it merged and knows nothing about the
    // rest of the table; only the authoritative write-behind pass may zero.
    let store = TempStore::new("host-additive").await;
    let tiers = TierMemory::new(store.storage.pool(), 0);

    tiers
        .persist_penalty_snapshot(&[("a.example".into(), 4_000), ("b.example".into(), 7_000)])
        .await
        .unwrap();
    tiers
        .save_penalties(&[("b.example".into(), 9_000)])
        .await
        .unwrap();

    let mut loaded = tiers.load_penalties().await.unwrap();
    loaded.sort();
    assert_eq!(
        loaded,
        vec![
            ("a.example".to_string(), 4_000),
            ("b.example".to_string(), 9_000)
        ],
        "a partial write must raise its own host and leave the others alone"
    );
}

#[tokio::test]
async fn aged_penalty_not_restored_on_boot() {
    let store = TempStore::new("host-aged-penalty").await;
    let tiers = TierMemory::new(store.storage.pool(), TTL_SECS);

    tiers
        .persist_penalty_snapshot(&[("ancient.example".into(), 30_000)])
        .await
        .unwrap();
    assert_eq!(tiers.load_penalties().await.unwrap().len(), 1);

    // Nothing re-confirms the penalty for longer than the host-memory TTL. It is
    // an observation like any other, and it expires like any other.
    tokio::time::sleep(PAST_TTL).await;
    assert!(
        tiers.load_penalties().await.unwrap().is_empty(),
        "a penalty older than the host-memory TTL must not be restored"
    );

    // The row itself is still there and still honest about what it holds — the
    // restore declined it, the diagnostics did not lose it.
    let row = tiers.get("ancient.example").await.unwrap().unwrap();
    assert_eq!(row.penalty_ms, 30_000);

    // A fresh confirmation makes it restorable again.
    tiers
        .persist_penalty_snapshot(&[("ancient.example".into(), 30_000)])
        .await
        .unwrap();
    assert_eq!(tiers.load_penalties().await.unwrap().len(), 1);
}

#[tokio::test]
async fn prune_drops_empty_stale_rows_not_learned_state() {
    let store = TempStore::new("host-prune").await;
    let tiers = TierMemory::new(store.storage.pool(), TTL_SECS);

    // Four rows, one per fate.
    // 1. quiet.example — one http win, then silence: the growth driver.
    tiers.record("quiet.example", "http", false).await.unwrap();
    // 2. pinned.example — three strikes, browser pin.
    for _ in 0..3 {
        tiers
            .record("pinned.example", "browser", true)
            .await
            .unwrap();
    }
    // 3. hostile.example — a live learned penalty, no tier evidence.
    // 4. striking.example — strikes below the pin threshold.
    tiers
        .record("striking.example", "browser", true)
        .await
        .unwrap();
    tiers
        .persist_penalty_snapshot(&[("hostile.example".into(), 8_000)])
        .await
        .unwrap();

    // Nothing is stale yet: a GC that runs before the horizon must do nothing.
    assert_eq!(
        tiers.prune_stale().await.unwrap(),
        0,
        "nothing is stale yet"
    );

    tokio::time::sleep(PAST_TTL).await;
    // hostile.example is still penalized at prune time (the pass below asserts
    // that a penalty protects the row regardless of age).
    assert_eq!(
        tiers.prune_stale().await.unwrap(),
        1,
        "only the row that says nothing is reclaimed"
    );
    assert!(tiers.get("quiet.example").await.unwrap().is_none());

    // Everything that still carries learned state survives, however old — these
    // are exactly the rows `GET /hosts` reports.
    for host in ["pinned.example", "striking.example", "hostile.example"] {
        assert!(
            tiers.get(host).await.unwrap().is_some(),
            "{host} still carries learned state and must survive the GC"
        );
    }
    assert_eq!(
        tiers
            .get("pinned.example")
            .await
            .unwrap()
            .unwrap()
            .preferred_tier
            .as_deref(),
        Some("browser"),
        "an aged pin is a routing decision applied on read; the row is unchanged"
    );

    // Once the penalty decays away, the hostile host's row becomes reclaimable
    // too — but only after its own zeroing has aged out (the zeroing is a write).
    tiers.persist_penalty_snapshot(&[]).await.unwrap();
    assert_eq!(
        tiers.prune_stale().await.unwrap(),
        0,
        "a just-zeroed row is fresh; the GC clock restarts at the write"
    );
    tokio::time::sleep(PAST_TTL).await;
    assert_eq!(tiers.prune_stale().await.unwrap(), 1);
    assert!(tiers.get("hostile.example").await.unwrap().is_none());
}

#[tokio::test]
async fn prune_is_a_no_op_when_aging_is_disabled() {
    // `host_memory_ttl_secs = 0` means "never age" — the operator asked for
    // pin-forever semantics, so the GC must not quietly delete anything either.
    let store = TempStore::new("host-prune-off").await;
    let tiers = TierMemory::new(store.storage.pool(), 0);
    tiers.record("quiet.example", "http", false).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(tiers.prune_stale().await.unwrap(), 0);
    assert!(tiers.get("quiet.example").await.unwrap().is_some());
}
