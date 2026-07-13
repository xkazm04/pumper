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
    assert!(cache.get(&key).await.unwrap().is_none(), "zero TTL must not read back live");

    // A generous TTL keeps the same entry live.
    cache
        .put(&key, &req.url, &response, Duration::from_secs(3600))
        .await
        .unwrap();
    let hit = cache.get(&key).await.unwrap().expect("long TTL should read back live");
    assert_eq!(hit.body, "hello world");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}
