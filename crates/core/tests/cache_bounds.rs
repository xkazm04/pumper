//! The three stores that grew without end on a default deployment:
//! `revalidations` (pruned only from inside a pass that ships disabled),
//! `research_cache` (no purge path at all), and `http_cache` (expiry-only, and
//! a continuously-revalidated entry never expires).
//!
//! Everything here runs against a real temp-dir SQLite with the full migration
//! chain — the bug in every case was the *reachability* of a pruner, so a mock
//! would prove nothing.

use std::collections::HashMap;
use std::time::Duration;

use pumper_core::config::CacheConfig;
use pumper_core::engine::ResearchOutput;
use pumper_core::testing::TempStore;
use pumper_core::{HttpCache, HttpRequest, HttpResponse, ResearchCache};

fn cache(store: &TempStore, max_rows: u64) -> HttpCache {
    HttpCache::new(
        store.storage.pool(),
        &CacheConfig {
            enabled: true,
            ttl_secs: 3600,
            max_rows,
        },
    )
}

fn resp(body: &str) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: HashMap::new(),
        body: body.into(),
        final_url: "https://example.test/".into(),
        cache_hit: false,
    }
}

/// Stores `n` entries under distinct keys, oldest first, and returns the keys.
async fn fill(cache: &HttpCache, n: usize) -> Vec<String> {
    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let url = format!("https://example.test/page/{i}");
        let key = HttpCache::key(&HttpRequest::get(&url));
        cache
            .put(&key, &url, &resp("body"), Duration::from_secs(3600))
            .await
            .unwrap();
        keys.push(key);
        // `created_at` is RFC-3339 micros, but SQLite ties are broken by key —
        // a small gap keeps the oldest-first order unambiguous for the assert.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    keys
}

async fn rows(store: &TempStore) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM http_cache")
        .fetch_one(&store.storage.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn revalidations_pruned_without_refresher() {
    // The demand path (a 304 on an expired entry) appends here on every
    // conditional GET, but the only pruner used to live inside the refresher
    // pass — unreachable at the shipping `[refresher] enabled = false`, so the
    // log grew forever on exactly the default deployment.
    let store = TempStore::new("cache-revalidations").await;
    let cache = cache(&store, 0);
    let key = HttpCache::key(&HttpRequest::get("https://example.test/feed"));

    cache.record_revalidation(&key, false).await;
    cache.record_revalidation(&key, true).await;
    // Backdate one observation well past any retention window.
    sqlx::query("UPDATE revalidations SET checked_at = '2020-01-01T00:00:00.000000Z' WHERE id = 1")
        .execute(&store.storage.pool())
        .await
        .unwrap();

    let pruned = cache.prune_revalidations(30).await.unwrap();
    assert_eq!(pruned, 1, "only the observation past the window goes");
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM revalidations")
        .fetch_one(&store.storage.pool())
        .await
        .unwrap();
    assert_eq!(
        left, 1,
        "recent observations still feed the freshness model"
    );
}

#[tokio::test]
async fn expired_research_answers_are_purged_not_kept_forever() {
    let store = TempStore::new("cache-research").await;
    // TTL 0 would disable the cache entirely; a 1-hour TTL keeps `put` live and
    // the row is backdated below to simulate an answer that has aged out.
    let research = ResearchCache::new(store.storage.pool(), 3600);
    let out = ResearchOutput {
        text: "an expensive answer".into(),
        json: None,
        cost_usd: Some(1.25),
        duration_ms: None,
        num_turns: None,
        session_id: None,
    };
    research.put("k-old", &out).await.unwrap();
    research.put("k-fresh", &out).await.unwrap();
    sqlx::query(
        "UPDATE research_cache SET expires_at = '2020-01-01T00:00:00.000000Z' WHERE key = 'k-old'",
    )
    .execute(&store.storage.pool())
    .await
    .unwrap();

    // Before: an expired answer was unreadable through get() yet kept its full
    // text and JSON on disk forever — invisible AND expensive.
    assert!(research.get("k-old").await.unwrap().is_none());

    assert_eq!(research.purge_expired().await.unwrap(), 1);
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM research_cache")
        .fetch_one(&store.storage.pool())
        .await
        .unwrap();
    assert_eq!(left, 1, "the still-live answer is kept");
    assert!(research.get("k-fresh").await.unwrap().is_some());
}

#[tokio::test]
async fn http_cache_row_cap_evicts_oldest_not_freshest() {
    let store = TempStore::new("cache-cap").await;
    let cache = cache(&store, 4);
    let keys = fill(&cache, 7).await;

    let evicted = cache.evict_over_cap().await.unwrap();
    assert_eq!(evicted, 3, "exactly the overage, no more");
    assert_eq!(rows(&store).await, 4);

    // The three oldest went; the four newest — the ones a working set is
    // actually made of — survive and are still readable.
    for old in &keys[..3] {
        assert!(cache.get(old, None).await.unwrap().is_none());
    }
    for fresh in &keys[3..] {
        assert!(
            cache.get(fresh, None).await.unwrap().is_some(),
            "a fresh entry must survive eviction"
        );
    }

    // At the cap the pass is a no-op — eviction triggers on overage only.
    assert_eq!(cache.evict_over_cap().await.unwrap(), 0);
    assert_eq!(rows(&store).await, 4);
}

#[tokio::test]
async fn a_refreshed_entry_outlives_an_older_untouched_one() {
    // Eviction age is `created_at`, which `refresh()` moves forward on every
    // 304 — so the janitor and the refresher do not fight over the same entry:
    // what the refresher keeps confirming is what the janitor keeps.
    let store = TempStore::new("cache-refresh-age").await;
    let cache = cache(&store, 1);
    let keys = fill(&cache, 2).await;

    // The OLDER entry is revalidated (304 => refresh), so it is now the
    // most-recently-confirmed of the two.
    cache
        .refresh(&keys[0], Duration::from_secs(3600))
        .await
        .unwrap();

    assert_eq!(cache.evict_over_cap().await.unwrap(), 1);
    assert!(
        cache.get(&keys[0], None).await.unwrap().is_some(),
        "the entry the refresher keeps confirming must survive"
    );
    assert!(cache.get(&keys[1], None).await.unwrap().is_none());
}

#[tokio::test]
async fn zero_max_rows_keeps_the_unbounded_behaviour() {
    // Opt-out is explicit: `0` means "no ceiling", the shape every other bound
    // in this codebase uses for off.
    let store = TempStore::new("cache-unbounded").await;
    let cache = cache(&store, 0);
    fill(&cache, 5).await;
    assert_eq!(cache.evict_over_cap().await.unwrap(), 0);
    assert_eq!(rows(&store).await, 5);
}
