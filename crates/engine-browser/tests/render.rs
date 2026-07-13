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

#[tokio::test]
async fn renders_example_dot_com() {
    let engine = BrowserEngine::new(&test_cfg("pumper-browser-test-profile"));
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

/// Direction 1: the relaunchable holder is reused across sequential renders
/// (the shared-instance path) — two renders on one engine both succeed.
#[tokio::test]
async fn reuses_browser_across_renders() {
    let engine = BrowserEngine::new(&test_cfg("pumper-browser-test-reuse"));
    for _ in 0..2 {
        let page = engine
            .render(RenderRequest::new("https://example.com"))
            .await
            .expect("render should succeed on a reused holder");
        assert!(page.html.contains("Example Domain"));
    }
}
