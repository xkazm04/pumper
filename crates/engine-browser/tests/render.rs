//! Live test: launches real Chrome and renders example.com.
//! Requires Chrome and network access — fine for this local-only project.

use pumper_core::config::BrowserConfig;
use pumper_core::{Browser, RenderRequest};
use pumper_engine_browser::BrowserEngine;

#[tokio::test]
async fn renders_example_dot_com() {
    let mut cfg = BrowserConfig::default();
    cfg.user_data_dir = std::env::temp_dir().join("pumper-browser-test-profile");
    // Chrome isn't reliably on PATH on Windows; point at the standard install
    // when present, otherwise let chromiumoxide auto-detect.
    let default_chrome =
        std::path::PathBuf::from(r"C:\Program Files\Google\Chrome\Application\chrome.exe");
    if default_chrome.exists() {
        cfg.chrome_executable = Some(default_chrome);
    }

    let engine = BrowserEngine::new(&cfg);
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
}
