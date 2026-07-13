//! Headless-browser engine on chromiumoxide (Chrome DevTools Protocol).
//! Chrome launches lazily on first use with a persistent user-data dir, so
//! logged-in sessions survive restarts. Run once with `headless = false` to
//! log in to a site manually; subsequent headless scrapes reuse the cookies.
//!
//! ## Resilience
//!
//! The single shared Chrome instance lives behind a relaunchable holder
//! ([`BrowserEngine::acquire`]). A background task drives the CDP handler loop
//! and flips a liveness flag when Chrome's connection ends (crash or exit); the
//! next acquire sees the dead flag and relaunches, so a crash no longer wedges
//! every future render until a server restart. Concurrent renders are capped by
//! `[browser] max_concurrent_renders` (a semaphore) so N callers can't spawn N
//! unbounded tabs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chromiumoxide::browser::{Browser as ChromeBrowser, BrowserConfig as ChromeConfig};
use futures::StreamExt;
use pumper_core::config::BrowserConfig;
use pumper_core::{Browser, Error, RenderRequest, RenderedPage, Result};
use tokio::sync::{Mutex, Semaphore};
use tracing::{info, warn};

/// Whether a held Chrome instance must be relaunched before the next render.
/// Pure so it can be unit-tested without a real browser: an instance is stale
/// when its handler task has died (crash detection, `alive == false`).
fn is_stale(alive: bool) -> bool {
    !alive
}

/// A launched Chrome instance plus liveness bookkeeping.
struct LiveBrowser {
    /// Shared so concurrent renders each hold a clone and open their own tab
    /// against the same Chrome; `new_page` only needs `&self`.
    browser: Arc<ChromeBrowser>,
    /// Flipped to `false` by the handler task when Chrome's CDP connection ends
    /// (crash or clean exit). This is the crash-detection mechanism: the handler
    /// stream terminates iff the browser is gone. Checked on acquire.
    alive: Arc<AtomicBool>,
}

pub struct BrowserEngine {
    cfg: BrowserConfig,
    /// Relaunchable holder. The mutex is held only briefly (health check +
    /// Arc clone), never for a render's duration, so renders run concurrently.
    holder: Mutex<Option<LiveBrowser>>,
    /// Caps concurrent renders (tabs). `None` = unlimited.
    render_slots: Option<Arc<Semaphore>>,
}

impl BrowserEngine {
    pub fn new(cfg: &BrowserConfig) -> Self {
        let render_slots = match cfg.max_concurrent_renders {
            0 => None,
            n => Some(Arc::new(Semaphore::new(n))),
        };
        Self { cfg: cfg.clone(), holder: Mutex::new(None), render_slots }
    }

    /// Launches a fresh Chrome and spawns its handler-drain task.
    async fn launch(&self) -> Result<LiveBrowser> {
        // Chrome resolves --user-data-dir against its own working directory, not
        // ours, so a relative path (from config) fails to launch (exit 21).
        // Absolutize it against our cwd first.
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

        let alive = Arc::new(AtomicBool::new(true));
        let alive_flag = alive.clone();
        tokio::spawn(async move {
            while let Some(event) = handler.next().await {
                if let Err(e) = event {
                    warn!("browser handler: {e}");
                }
            }
            // Stream ended => CDP connection gone => Chrome exited/crashed.
            alive_flag.store(false, Ordering::Relaxed);
            warn!("browser handler loop ended (chrome exited?)");
        });

        Ok(LiveBrowser { browser: Arc::new(browser), alive })
    }

    /// Returns a handle to a live Chrome, relaunching it if the previous one
    /// died (crash detection).
    async fn acquire(&self) -> Result<Arc<ChromeBrowser>> {
        let mut holder = self.holder.lock().await;
        let stale = match holder.as_ref() {
            None => true,
            Some(live) => is_stale(live.alive.load(Ordering::Relaxed)),
        };
        if stale {
            // Drop the previous instance: `kill_on_drop` reaps its Chrome. Any
            // in-flight render still holding an Arc clone keeps its own Chrome
            // alive until it finishes, then that clone drops and reaps.
            *holder = Some(self.launch().await?);
        }
        let live = holder.as_ref().expect("holder populated above");
        Ok(live.browser.clone())
    }
}

#[async_trait]
impl Browser for BrowserEngine {
    async fn render(&self, req: RenderRequest) -> Result<RenderedPage> {
        // Cap concurrent tabs. Held for the whole render (dropped on return).
        let _permit = match &self.render_slots {
            Some(sem) => Some(
                sem.clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| Error::Browser(format!("render semaphore closed: {e}")))?,
            ),
            None => None,
        };

        let browser = self.acquire().await?;
        let nav_timeout = Duration::from_secs(self.cfg.nav_timeout_secs);

        let page = browser
            .new_page(req.url.as_str())
            .await
            .map_err(|e| Error::Browser(format!("new_page {}: {e}", req.url)))?;

        let mut nav_timed_out = false;
        match tokio::time::timeout(nav_timeout, page.wait_for_navigation()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => warn!(url = %req.url, "navigation: {e}"),
            Err(_) => {
                nav_timed_out = true;
                warn!(url = %req.url, "navigation wait timed out; capturing current DOM");
            }
        }

        let mut selector_found = None;
        if let Some(selector) = &req.wait_for_selector {
            let deadline = tokio::time::Instant::now() + nav_timeout;
            let mut found = false;
            loop {
                if page.find_element(selector.as_str()).await.is_ok() {
                    found = true;
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    warn!(selector = %selector, "wait_for_selector timed out");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            selector_found = Some(found);
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

        Ok(RenderedPage { html, final_url, evaluated, nav_timed_out, selector_found })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BrowserConfig {
        BrowserConfig::default()
    }

    #[test]
    fn semaphore_absent_when_unlimited() {
        let mut c = cfg();
        c.max_concurrent_renders = 0;
        assert!(BrowserEngine::new(&c).render_slots.is_none());
    }

    #[test]
    fn semaphore_present_and_sized_when_capped() {
        let mut c = cfg();
        c.max_concurrent_renders = 3;
        let engine = BrowserEngine::new(&c);
        let sem = engine.render_slots.expect("cap => semaphore");
        assert_eq!(sem.available_permits(), 3);
    }

    /// Crash detection: the handler task flips `alive` to false when Chrome's
    /// CDP stream ends. A dead flag must mark the holder stale so `acquire`
    /// relaunches — exactly like an empty holder. (Relaunching real Chrome in a
    /// unit test is impractical; a gated live reuse test lives in tests/render.rs.)
    #[test]
    fn dead_alive_flag_forces_relaunch() {
        assert!(!is_stale(true), "a live instance is reused");
        assert!(is_stale(false), "a dead handler task forces relaunch");
    }
}
