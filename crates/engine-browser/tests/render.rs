//! Live tests: launch real Chrome. Require Chrome and network access — fine for
//! this local-only project. Each test uses its own profile dir so they don't
//! contend for the single-instance user-data lock when run in parallel.

use pumper_core::config::BrowserConfig;
use pumper_core::{Browser, RenderRequest};
use pumper_engine_browser::BrowserEngine;

/// Base config pointing at the standard Windows Chrome install when present
/// (Chrome isn't reliably on PATH on Windows), else auto-detect.
fn test_cfg(profile: &str) -> BrowserConfig {
    let mut cfg = BrowserConfig::default();
    cfg.user_data_dir = std::env::temp_dir().join(profile);
    let default_chrome =
        std::path::PathBuf::from(r"C:\Program Files\Google\Chrome\Application\chrome.exe");
    if default_chrome.exists() {
        cfg.chrome_executable = Some(default_chrome);
    }
    cfg
}

/// A private session-vault root for a test (profiles land in `<root>/<name>/browser`).
fn vault(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(name)
}

#[tokio::test]
async fn renders_example_dot_com() {
    let engine = BrowserEngine::new(
        &test_cfg("pumper-browser-test-profile"),
        vault("pumper-vault-basic"),
    );
    let mut request = RenderRequest::new("https://example.com");
    request.evaluate = Some("document.title".into());

    let page = engine.render(request).await.expect("render should succeed");

    assert!(
        page.html.contains("Example Domain"),
        "unexpected html: {}",
        &page.html[..page.html.len().min(500)]
    );
    let title = page
        .evaluated
        .as_ref()
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    assert!(title.contains("Example"), "unexpected evaluated title: {title:?}");
    // A clean load reports honest wait outcomes.
    assert!(!page.nav_timed_out, "example.com should not time out");
    assert_eq!(page.selector_found, None, "no selector requested");
}

/// Direction 2: request interception drops images/fonts/media by default, and
/// `load_all_resources` opts a single render back into loading everything.
#[tokio::test]
async fn blocks_heavy_resources_and_opt_out_loads_them() {
    let engine = BrowserEngine::new(
        &test_cfg("pumper-browser-test-blocking"),
        vault("pumper-vault-blocking"),
    );
    // A page with real images/fonts so interception has something to drop.
    let url = "https://en.wikipedia.org/wiki/Main_Page";

    let blocked = engine
        .render(RenderRequest::new(url))
        .await
        .expect("blocking render should succeed");
    assert!(
        blocked.blocked_resources > 0,
        "expected some image/font/media requests to be blocked, got {}",
        blocked.blocked_resources
    );

    let mut opt_out = RenderRequest::new(url);
    opt_out.load_all_resources = true;
    let loaded = engine
        .render(opt_out)
        .await
        .expect("opt-out render should succeed");
    assert_eq!(
        loaded.blocked_resources, 0,
        "load_all_resources must block nothing"
    );
}

/// Direction 1: the relaunchable holder is reused across sequential renders
/// (the shared-instance path) — two renders on one engine both succeed.
#[tokio::test]
async fn reuses_browser_across_renders() {
    let engine = BrowserEngine::new(
        &test_cfg("pumper-browser-test-reuse"),
        vault("pumper-vault-reuse"),
    );
    for _ in 0..2 {
        let page = engine
            .render(RenderRequest::new("https://example.com"))
            .await
            .expect("render should succeed on a reused holder");
        assert!(page.html.contains("Example Domain"));
    }
}

/// Session vault: two named profiles each get their own Chrome (bound to their
/// own user-data-dir under the vault), both render, and interleaving them does
/// not thrash — the per-profile holders stay live. Also asserts the on-disk
/// layout the `/profiles` endpoint reports (`<vault>/<name>/browser`).
#[tokio::test]
async fn renders_under_named_profiles_with_separate_user_data_dirs() {
    let root = vault("pumper-vault-profiles");
    let _ = std::fs::remove_dir_all(&root);
    let engine = BrowserEngine::new(&test_cfg("pumper-browser-test-vault"), root.clone());

    // Interleave the two profiles: A, B, A — with per-profile holders, the
    // second A render reuses A's Chrome instead of relaunching.
    for name in ["alpha", "beta", "alpha"] {
        let mut req = RenderRequest::new("https://example.com");
        req.profile = Some(name.to_string());
        let page = engine
            .render(req)
            .await
            .unwrap_or_else(|e| panic!("render under profile {name} failed: {e}"));
        assert!(page.html.contains("Example Domain"), "profile {name} rendered the page");
    }

    for name in ["alpha", "beta"] {
        let dir = root.join(name).join("browser");
        assert!(dir.is_dir(), "profile chrome dir missing: {}", dir.display());
    }

    // An unsafe profile name is rejected before Chrome is ever launched.
    let mut bad = RenderRequest::new("https://example.com");
    bad.profile = Some("../escape".into());
    assert!(engine.render(bad).await.is_err(), "unsafe profile name must be rejected");
}
