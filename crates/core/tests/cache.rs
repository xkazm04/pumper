//! Integration test for the HTTP response cache's per-entry TTL against a real
//! temp-dir SQLite with the full migration chain. Proves `put`'s write honors
//! the caller-supplied TTL (the `ttl_override` path in engine-http feeds this).

use std::collections::HashMap;
use std::time::Duration;

use pumper_core::config::{CacheConfig, StorageConfig};
use pumper_core::{HttpCache, HttpRequest, HttpResponse, Storage};

fn resp() -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: HashMap::new(),
        body: "hello world".into(),
        final_url: "https://example.com/".into(),
        cache_hit: false,
    }
}

#[tokio::test]
async fn put_honors_explicit_ttl() {
    let dir = std::env::temp_dir().join(format!("pumper-cache-test-{}", uuid::Uuid::new_v4()));
    let cfg = StorageConfig {
        database_path: dir.join("pumper.db"),
        artifacts_dir: dir.join("artifacts"),
        ..StorageConfig::default()
    };
    let storage = Storage::connect(&cfg).await.expect("connect + migrate");
    let cache = HttpCache::new(storage.pool(), &CacheConfig { enabled: true, ttl_secs: 3600 });

    let req = HttpRequest::get("https://example.com/");
    let key = HttpCache::key(&req);
    let response = resp();

    // A zero TTL stores an already-expired entry: the write happened, but a
    // subsequent read finds nothing live. This is what a short ttl_override
    // buys a monitor — the body is refreshed rather than served stale.
    cache.put(&key, &req.url, &response, Duration::ZERO).await.unwrap();
    assert!(cache.get(&key, None).await.unwrap().is_none(), "zero TTL must not read back live");

    // A generous TTL keeps the same entry live.
    cache
        .put(&key, &req.url, &response, Duration::from_secs(3600))
        .await
        .unwrap();
    let hit = cache.get(&key, None).await.unwrap().expect("long TTL should read back live");
    assert_eq!(hit.body, "hello world");

    // Read-staleness cap (the ttl_override-on-read guarantee): a generous max_age
    // still hits the fresh entry...
    assert!(
        cache.get(&key, Some(Duration::from_secs(3600))).await.unwrap().is_some(),
        "fresh entry passes a generous max_age"
    );
    // ...but once the entry has aged past a tiny max_age it is a miss, even though
    // its stored TTL is still live — a short-TTL reader is not served stale content.
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        cache.get(&key, Some(Duration::from_millis(5))).await.unwrap().is_none(),
        "entry older than max_age must be treated as stale"
    );

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}
