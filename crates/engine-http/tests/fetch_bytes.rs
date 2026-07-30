//! Live tests of the binary `fetch_bytes` seam (engine-traits#2-LITE) against a
//! local in-process axum server: raw bytes come back exactly (no charset
//! mangling), the hard size cap rejects an oversized body with a typed error,
//! and a non-2xx status is an error rather than an error page's bytes. Also
//! proves the trait's default impl stays "unsupported" for engines that never
//! opted in.

use std::path::Path;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use pumper_core::config::{CacheConfig, GovernorConfig, HttpConfig, StorageConfig};
use pumper_core::{Error, Governor, HttpCache, HttpClient, HttpRequest, HttpResponse, Storage};
use pumper_engine_http::HttpEngine;

/// Bytes that are NOT valid UTF-8 — a text-path decode would mangle them into
/// U+FFFD, so getting them back verbatim proves the path is truly binary.
const BINARY: &[u8] = &[0x50, 0x4B, 0x03, 0x04, 0xFF, 0xFE, 0x00, 0xD8, 0xF8, 0x01];

async fn spawn_server() -> String {
    let app = Router::new()
        .route("/blob.zip", get(|| async { BINARY.to_vec() }))
        .route("/big", get(|| async { vec![0u8; 4096] }))
        .route(
            "/missing",
            get(|| async { (StatusCode::NOT_FOUND, "not here") }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn new_engine(root: &Path) -> HttpEngine {
    let storage = Storage::connect(&StorageConfig {
        database_path: root.join("pumper.db"),
        artifacts_dir: root.join("artifacts"),
        ..StorageConfig::default()
    })
    .await
    .expect("storage");
    let cache = Arc::new(HttpCache::new(storage.pool(), &CacheConfig::default()));
    let governor = Arc::new(Governor::new(&GovernorConfig::default()));
    // Leak the pool with the engine for the test's lifetime.
    std::mem::forget(storage);
    HttpEngine::new(&HttpConfig::default(), governor, cache, root.join("profiles"))
        .expect("engine")
}

fn temp_root(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "pumper-fetch-bytes-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("temp root");
    root
}

#[tokio::test]
async fn fetch_bytes_returns_raw_bytes_caps_bodies_and_rejects_non_2xx() {
    let root = temp_root("live");
    let base = spawn_server().await;
    let engine = new_engine(&root).await;

    // Raw bytes exactly as served — including non-UTF-8 bytes a text decode
    // would have replaced.
    let bytes = engine
        .fetch_bytes(HttpRequest::get(format!("{base}/blob.zip")))
        .await
        .expect("binary fetch");
    assert_eq!(bytes, BINARY, "bytes must round-trip verbatim");

    // The per-request cap rejects an oversized body with a typed error that
    // names the cap.
    let mut capped = HttpRequest::get(format!("{base}/big"));
    capped.max_body_bytes = Some(1024);
    let err = engine.fetch_bytes(capped).await.unwrap_err();
    assert!(matches!(err, Error::Http(_)));
    assert!(
        err.to_string().contains("max_body_bytes cap of 1024"),
        "error names the cap: {err}"
    );

    // A non-2xx is an error, not the error page's bytes.
    let err = engine
        .fetch_bytes(HttpRequest::get(format!("{base}/missing")))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("status 404"),
        "error names the status: {err}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// An engine that never overrode `fetch_bytes` gets the trait's loud
/// "unsupported" default — wrappers/mocks keep compiling, binary fetches
/// reaching them fail with a typed error instead of silently mis-decoding.
#[tokio::test]
async fn fetch_bytes_default_impl_is_a_loud_unsupported_error() {
    struct TextOnly;
    #[async_trait::async_trait]
    impl HttpClient for TextOnly {
        async fn fetch(&self, _req: HttpRequest) -> pumper_core::Result<HttpResponse> {
            unreachable!("not exercised")
        }
    }
    let err = TextOnly
        .fetch_bytes(HttpRequest::get("https://example.com/a.zip"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Http(_)));
    assert!(err.to_string().contains("fetch_bytes"), "{err}");
}
