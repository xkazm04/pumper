//! Integration test for the self-learning tier router memory against a real
//! temp-dir SQLite with the full migration chain.

use pumper_core::TierMemory;

/// Fresh temp-dir SQLite with the full migration chain.
use pumper_core::testing::TempStore;

async fn fresh_db(tag: &str) -> TempStore {
    TempStore::new(tag).await
}

#[tokio::test]
async fn learns_browser_after_three_strikes_and_resets_on_http_win() {
    let store = pumper_core::testing::TempStore::new("tier-test").await;
    let storage = &store.storage;
    // ttl 0 disables aging — this test covers the classic strike/reset path.
    let tiers = TierMemory::new(storage.pool(), 0);

    assert_eq!(tiers.preferred("spa.example").await.unwrap(), None);

    // Two HTTP losses: strikes accumulate but no preference yet.
    tiers.record("SPA.example", "browser", true).await.unwrap();
    tiers.record("spa.example", "browser", true).await.unwrap();
    assert_eq!(tiers.preferred("spa.example").await.unwrap(), None);

    // Third consecutive loss flips the host to the browser tier.
    tiers.record("spa.example", "claude", true).await.unwrap();
    assert_eq!(
        tiers.preferred("Spa.Example").await.unwrap().as_deref(),
        Some("browser"),
        "case-insensitive host"
    );

    // A browser win with no HTTP attempt teaches nothing (skip path).
    tiers.record("spa.example", "browser", false).await.unwrap();
    assert_eq!(
        tiers.preferred("spa.example").await.unwrap().as_deref(),
        Some("browser")
    );

    // One HTTP win fully resets the record.
    tiers.record("spa.example", "http", false).await.unwrap();
    assert_eq!(tiers.preferred("spa.example").await.unwrap(), None);

    // Unrelated hosts are untouched.
    assert_eq!(tiers.preferred("plain.example").await.unwrap(), None);
}

#[tokio::test]
async fn aged_out_pin_lapses_and_earns_a_fresh_strike_count() {
    let store = fresh_db("tier-age").await;
    let storage = &store.storage;
    // 1-second aging horizon: strikes written "now" are already considered
    // stale by an in-the-future check the moment we advance past the TTL.
    let tiers = TierMemory::new(storage.pool(), 1);

    // Pin the host to the browser tier (3 strikes).
    for _ in 0..3 {
        tiers.record("aged.example", "browser", true).await.unwrap();
    }
    assert_eq!(
        tiers.preferred("aged.example").await.unwrap().as_deref(),
        Some("browser")
    );

    // Wait past the TTL: the pin lapses (reads back as None) so the host gets a
    // fresh crack at the cheap HTTP tier.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert_eq!(
        tiers.preferred("aged.example").await.unwrap(),
        None,
        "aged pin must lapse"
    );

    // A single fresh loss must NOT immediately re-pin (stale strikes reset to 1).
    tiers.record("aged.example", "browser", true).await.unwrap();
    assert_eq!(
        tiers.preferred("aged.example").await.unwrap(),
        None,
        "one fresh loss after aging out must not re-pin"
    );
    let profile = tiers.get("aged.example").await.unwrap().unwrap();
    assert_eq!(
        profile.http_strikes, 1,
        "stale strikes reset to a single fresh strike"
    );
}

#[tokio::test]
async fn observations_count_outcomes_and_gate_the_weather_export() {
    let store = fresh_db("tier-weather").await;
    let tiers = TierMemory::new(store.storage.pool(), 0);

    // 3 outcomes (2 losses + 1 win) — every recorded outcome counts, and a
    // win resets strikes without resetting the evidence counter.
    tiers.record("deep.example", "browser", true).await.unwrap();
    tiers.record("deep.example", "browser", true).await.unwrap();
    tiers.record("deep.example", "http", false).await.unwrap();
    // 1 outcome; and a skip-path record (browser win, no http attempt) that
    // must not count because it teaches nothing.
    tiers.record("thin.example", "claude", true).await.unwrap();
    tiers
        .record("thin.example", "browser", false)
        .await
        .unwrap();
    // Penalty-only snapshot rows never accrue observations.
    tiers
        .save_penalties(&[("penalty.example".into(), 1000)])
        .await
        .unwrap();

    let deep = tiers.get("deep.example").await.unwrap().unwrap();
    assert_eq!(deep.observations, 3);
    assert_eq!(deep.http_strikes, 0, "win reset strikes, kept observations");
    assert_eq!(
        tiers
            .get("thin.example")
            .await
            .unwrap()
            .unwrap()
            .observations,
        1
    );

    // The export floor keeps thin and penalty-only hosts home.
    let exported = tiers.export_weather(2).await.unwrap();
    assert_eq!(exported.len(), 1, "only the well-observed host travels");
    assert_eq!(exported[0].host, "deep.example");
    // Floor 0 exports everything, including the penalty-only row.
    assert_eq!(tiers.export_weather(0).await.unwrap().len(), 3);
}

#[tokio::test]
async fn penalties_persist_and_reload_and_forget_resets() {
    let store = fresh_db("tier-penalty").await;
    let storage = &store.storage;
    let tiers = TierMemory::new(storage.pool(), 0);

    // A strike host plus a penalty-only host.
    for _ in 0..3 {
        tiers.record("pin.example", "browser", true).await.unwrap();
    }
    tiers
        .save_penalties(&[("pin.example".into(), 2000), ("slow.example".into(), 5000)])
        .await
        .unwrap();

    // load_penalties returns every non-zero learned penalty for boot restore.
    let mut loaded = tiers.load_penalties().await.unwrap();
    loaded.sort();
    assert_eq!(
        loaded,
        vec![("pin.example".into(), 2000), ("slow.example".into(), 5000)]
    );

    // The penalty-only host has a profile row but no strikes/pin.
    let slow = tiers.get("slow.example").await.unwrap().unwrap();
    assert_eq!(slow.penalty_ms, 5000);
    assert_eq!(slow.http_strikes, 0);
    assert_eq!(slow.preferred_tier, None);
    assert!(slow.penalty_updated_at.is_some());

    // The pinned host keeps its pin AND carries its penalty snapshot.
    let pin = tiers.get("pin.example").await.unwrap().unwrap();
    assert_eq!(pin.preferred_tier.as_deref(), Some("browser"));
    assert_eq!(pin.penalty_ms, 2000);

    // list_page returns both, most-recently-active first.
    let page = tiers.list_page(None, 50).await.unwrap();
    assert_eq!(page.len(), 2);

    // forget() drops the row; a second forget is a no-op (returns false).
    assert!(tiers.forget("pin.example").await.unwrap());
    assert!(!tiers.forget("pin.example").await.unwrap());
    assert!(tiers.get("pin.example").await.unwrap().is_none());
}
