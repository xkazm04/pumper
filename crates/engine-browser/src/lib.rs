//! Headless-browser engine on chromiumoxide (Chrome DevTools Protocol).
//! Chrome launches lazily on first use with a persistent user-data dir, so
//! logged-in sessions survive restarts. Run once with `headless = false` to
//! log in to a site manually; subsequent headless scrapes reuse the cookies.

use std::time::Duration;

use async_trait::async_trait;
use chromiumoxide::browser::{Browser as ChromeBrowser, BrowserConfig as ChromeConfig};
use futures::StreamExt;
use pumper_core::config::BrowserConfig;
use pumper_core::{Browser, Error, RenderRequest, RenderedPage, Result};
use tokio::sync::OnceCell;
use tracing::{info, warn};

pub struct BrowserEngine {
    cfg: BrowserConfig,
    browser: OnceCell<ChromeBrowser>,
}

impl BrowserEngine {
    pub fn new(cfg: &BrowserConfig) -> Self {
        Self { cfg: cfg.clone(), browser: OnceCell::new() }
    }

    async fn browser(&self) -> Result<&ChromeBrowser> {
        self.browser
            .get_or_try_init(|| async {
                // Chrome resolves --user-data-dir against its own working
                // directory, not ours, so a relative path (from config) fails
                // to launch (exit 21). Absolutize it against our cwd first.
                let mut user_data_dir = self.cfg.user_data_dir.clone();
                if user_data_dir.is_relative() {
                    if let Ok(cwd) = std::env::current_dir() {
                        user_data_dir = cwd.join(user_data_dir);
                    }
                }
                std::fs::create_dir_all(&user_data_dir)?;
                let mut builder = ChromeConfig::builder()
                    .user_data_dir(&user_data_dir)
                    .arg("--disable-blink-features=AutomationControlled");
                if let Some(path) = &self.cfg.chrome_executable {
                    builder = builder.chrome_executable(path);
                }
                if !self.cfg.headless {
                    builder = builder.with_head();
                }
                let config = builder.build().map_err(Error::Browser)?;

                info!("launching chrome");
                let (browser, mut handler) = ChromeBrowser::launch(config)
                    .await
                    .map_err(|e| Error::Browser(format!("launch: {e}")))?;
                tokio::spawn(async move {
                    while let Some(event) = handler.next().await {
                        if let Err(e) = event {
                            warn!("browser handler: {e}");
                        }
                    }
                    warn!("browser handler loop ended (chrome exited?)");
                });
                Ok(browser)
            })
            .await
    }
}

#[async_trait]
impl Browser for BrowserEngine {
    async fn render(&self, req: RenderRequest) -> Result<RenderedPage> {
        let browser = self.browser().await?;
        let nav_timeout = Duration::from_secs(self.cfg.nav_timeout_secs);

        let page = browser
            .new_page(req.url.as_str())
            .await
            .map_err(|e| Error::Browser(format!("new_page {}: {e}", req.url)))?;

        match tokio::time::timeout(nav_timeout, page.wait_for_navigation()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => warn!(url = %req.url, "navigation: {e}"),
            Err(_) => warn!(url = %req.url, "navigation wait timed out; capturing current DOM"),
        }

        if let Some(selector) = &req.wait_for_selector {
            let deadline = tokio::time::Instant::now() + nav_timeout;
            while page.find_element(selector.as_str()).await.is_err() {
                if tokio::time::Instant::now() >= deadline {
                    warn!(selector = %selector, "wait_for_selector timed out");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }

        let settle_ms = req.extra_wait_ms.unwrap_or(self.cfg.default_wait_ms);
        if settle_ms > 0 {
            tokio::time::sleep(Duration::from_millis(settle_ms)).await;
        }

        let evaluated = match &req.evaluate {
            Some(js) => match page.evaluate(js.as_str()).await {
                Ok(result) => result.into_value::<serde_json::Value>().ok(),
                Err(e) => {
                    warn!("evaluate failed: {e}");
                    None
                }
            },
            None => None,
        };

        let html = page
            .content()
            .await
            .map_err(|e| Error::Browser(format!("content: {e}")))?;
        let final_url = page.url().await.ok().flatten();

        if let Err(e) = page.close().await {
            warn!("page close: {e}");
        }

        Ok(RenderedPage { html, final_url, evaluated })
    }
}
