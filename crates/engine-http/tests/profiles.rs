//! Live test of the session vault against a local cookie-setting server: two
//! profiles get two jars, the jars are persisted to `<vault>/<name>/cookies.json`,
//! and a **fresh engine** (the "restart") replays those cookies. No network — the
//! server is an in-process axum app on an ephemeral port.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use pumper_core::config::{CacheConfig, GovernorConfig, HttpConfig, StorageConfig};
use pumper_core::{Governor, HttpCache, HttpClient, HttpRequest, Storage};
use pumper_engine_http::HttpEngine;

/// `GET /login?sid=x` sets a **session** cookie (no Expires) — the login case the
/// vault exists for, and the one an in-memory jar loses on restart.
async fn login(Query(q): Query<HashMap<String, String>>) -> impl IntoResponse {
    let sid = q.get("sid").cloned().unwrap_or_default();
    ([("set-cookie", format!("sid={sid}; Path=/"))], "ok")
}

/// `GET /echo` reflects the `Cookie` header the client sent (or `none`).
async fn echo(headers: HeaderMap) -> String {
    headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none")
        .to_string()
}

async fn spawn_server() -> String {
    let app = Router::new().route("/login", get(login)).route("/echo", get(echo));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

/// A real `HttpEngine` over a throwaway SQLite cache, rooted at `vault`.
async fn new_engine(root: &Path, vault: PathBuf) -> HttpEngine {
    let storage = Storage::connect(&StorageConfig {
        database_path: root.join("pumper.db"),
        artifacts_dir: root.join("artifacts"),
    })
    .await
    .expect("storage");
    let cache = Arc::new(HttpCache::new(storage.pool(), &CacheConfig::default()));
    let governor = Arc::new(Governor::new(&GovernorConfig::default()));
    // Leak the pool with the engine for the test's lifetime.
    std::mem::forget(storage);
    HttpEngine::new(&HttpConfig::default(), governor, cache, vault).expect("engine")
}

fn profiled(url: &str, profile: &str) -> HttpRequest {
    let mut req = HttpRequest::get(url);
    req.profile = Some(profile.to_string());
    req
}

#[tokio::test]
async fn profiles_keep_separate_persistent_cookie_jars_across_a_restart() {
    let root = std::env::temp_dir().join(format!(
        "pumper-vault-http-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("temp root");
    let vault = root.join("profiles");
    let base = spawn_server().await;

    let engine = new_engine(&root, vault.clone()).await;

    // Two profiles log in as different users against the same host.
    engine
        .fetch(profiled(&format!("{base}/login?sid=alpha-1"), "alpha"))
        .await
        .expect("alpha login");
    engine
        .fetch(profiled(&format!("{base}/login?sid=beta-1"), "beta"))
        .await
        .expect("beta login");

    // Each profile replays only its own cookie...
    let alpha = engine.fetch(profiled(&format!("{base}/echo"), "alpha")).await.expect("alpha echo");
    assert_eq!(alpha.body, "sid=alpha-1", "alpha replays its own session");
    let beta = engine.fetch(profiled(&format!("{base}/echo"), "beta")).await.expect("beta echo");
    assert_eq!(beta.body, "sid=beta-1", "beta replays its own session (no bleed from alpha)");

    // ...and a profile-less request stays anonymous (the default in-memory jar).
    let anon = engine
        .fetch(HttpRequest::get(format!("{base}/echo")))
        .await
        .expect("anonymous echo");
    assert_eq!(anon.body, "none", "profile-less requests carry no profile cookies");

    // The debounced write-behind flushes both jars (trailing edge).
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let alpha_jar = vault.join("alpha").join("cookies.json");
    let beta_jar = vault.join("beta").join("cookies.json");
    assert!(alpha_jar.is_file(), "alpha jar written to {}", alpha_jar.display());
    assert!(beta_jar.is_file(), "beta jar written to {}", beta_jar.display());
    let alpha_json = std::fs::read_to_string(&alpha_jar).unwrap();
    let beta_json = std::fs::read_to_string(&beta_jar).unwrap();
    assert!(alpha_json.contains("alpha-1") && !alpha_json.contains("beta-1"));
    assert!(beta_json.contains("beta-1") && !beta_json.contains("alpha-1"));

    // "Restart": a brand-new engine over the same vault (nothing in memory).
    drop(engine);
    let restarted = new_engine(&root, vault.clone()).await;
    let after = restarted
        .fetch(profiled(&format!("{base}/echo"), "alpha"))
        .await
        .expect("alpha echo after restart");
    assert_eq!(
        after.body, "sid=alpha-1",
        "the session cookie survived the restart via the persisted jar"
    );

    // An unsafe profile name is a typed error, and never creates a directory.
    let err = restarted
        .fetch(profiled(&format!("{base}/echo"), "../escape"))
        .await
        .expect_err("unsafe profile name must be rejected");
    assert!(matches!(err, pumper_core::Error::Profile(_)), "got {err:?}");

    let _ = std::fs::remove_dir_all(&root);
}
