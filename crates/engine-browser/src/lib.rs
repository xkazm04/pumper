//! Headless-browser engine on chromiumoxide (Chrome DevTools Protocol).
//! Chrome launches lazily on first use with a persistent user-data dir, so
//! logged-in sessions survive restarts. Run once with `headless = false` to
//! log in to a site manually; subsequent headless scrapes reuse the cookies.
//!
//! ## Resilience & cost
//!
//! The single shared Chrome instance lives behind a relaunchable holder
//! ([`BrowserEngine::acquire`]). A background task drives the CDP handler loop
//! and flips a liveness flag when Chrome's connection ends (crash or exit); the
//! next acquire sees the dead flag and relaunches, so a crash no longer wedges
//! every future render until a server restart. The holder also relaunches after
//! `[browser] recycle_after_renders` renders to shed accumulated memory.
//!
//! Concurrent renders are capped by `[browser] max_concurrent_renders` (a
//! semaphore) so N callers can't spawn N unbounded tabs. When
//! `[browser] block_resources` is set, CDP request interception drops
//! images/fonts/media (never stylesheets) so scraping renders stay cheap; a
//! render can opt back in with `RenderRequest.load_all_resources`.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chromiumoxide::browser::{Browser as ChromeBrowser, BrowserConfig as ChromeConfig};
use chromiumoxide::cdp::browser_protocol::fetch::{
    ContinueRequestParams, EventRequestPaused, FailRequestParams,
};
use chromiumoxide::cdp::browser_protocol::network::{ErrorReason, ResourceType};
use futures::StreamExt;
use pumper_core::config::BrowserConfig;
use pumper_core::{Browser, Error, RenderRequest, RenderedPage, Result};
use tokio::sync::{Mutex, Semaphore};
use tracing::{info, warn};

/// Cap Chrome's V8 heap so a runaway page can't balloon the shared instance.
const JS_HEAP_CAP_MB: u32 = 512;

/// Whether a held Chrome instance must be relaunched before the next render.
/// Pure so it can be unit-tested without a real browser: an instance is stale
/// when its handler task has died (crash detection, `alive == false`) or it has
/// served its recycle quota (`recycle > 0 && renders >= recycle`).
fn is_stale(alive: bool, renders: u64, recycle: u64) -> bool {
    !alive || (recycle > 0 && renders >= recycle)
}

/// A launched Chrome instance plus liveness/recycle bookkeeping.
struct LiveBrowser {
    /// Shared so concurrent renders each hold a clone and open their own tab
    /// against the same Chrome; `new_page` only needs `&self`.
    browser: Arc<ChromeBrowser>,
    /// Flipped to `false` by the handler task when Chrome's CDP connection ends
    /// (crash or clean exit). This is the crash-detection mechanism: the handler
    /// stream terminates iff the browser is gone. Checked on acquire.
    alive: Arc<AtomicBool>,
    /// Renders served by this instance; drives periodic recycle.
    renders: u64,
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
            .arg("--disable-blink-features=AutomationControlled")
            // Avoid tiny /dev/shm in containers exhausting and crashing Chrome.
            .arg("--disable-dev-shm-usage")
            // Bound V8 heap so one heavy page can't OOM the shared instance.
            .arg(format!("--js-flags=--max-old-space-size={JS_HEAP_CAP_MB}"));
        if let Some(proxy) = &self.cfg.proxy {
            // Route the browser through the configured proxy. Falls back to
            // `[http] proxy` at config load. Chrome's --proxy-server takes no
            // in-URL auth (an authenticated proxy would prompt), so auth is
            // unsupported on the browser tier.
            builder = builder.arg(format!("--proxy-server={proxy}"));
        }
        if self.cfg.block_resources {
            // Enable the Fetch domain so per-page drainers can drop subresources.
            // (This also auto-disables Chrome's HTTP cache; cookies are separate
            // and still persist via the profile dir.)
            builder = builder.enable_request_intercept();
        }
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

        Ok(LiveBrowser { browser: Arc::new(browser), alive, renders: 0 })
    }

    /// Returns a handle to a live Chrome, relaunching it if the previous one
    /// died (crash detection) or hit the recycle threshold. Counts one render.
    async fn acquire(&self) -> Result<Arc<ChromeBrowser>> {
        let mut holder = self.holder.lock().await;
        let recycle = self.cfg.recycle_after_renders;
        let stale = match holder.as_ref() {
            None => true,
            Some(live) => is_stale(live.alive.load(Ordering::Relaxed), live.renders, recycle),
        };
        if stale {
            // Drop the previous instance: `kill_on_drop` reaps its Chrome. Any
            // in-flight render still holding an Arc clone keeps its own Chrome
            // alive until it finishes, then that clone drops and reaps.
            *holder = Some(self.launch().await?);
        }
        let live = holder.as_mut().expect("holder populated above");
        live.renders += 1;
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

        // Start blank so the interception drainer is listening before the first
        // (document) request fires; otherwise the initial navigation would pause
        // with no one to resolve it and hang.
        let page = browser
            .new_page("about:blank")
            .await
            .map_err(|e| Error::Browser(format!("new_page: {e}")))?;

        // Resource-blocking drainer. Only wired when interception is enabled at
        // launch (`block_resources`); otherwise no Fetch events ever fire.
        let blocked = Arc::new(AtomicUsize::new(0));
        let drainer = if self.cfg.block_resources {
            let block_heavy = !req.load_all_resources;
            let drain_page = page.clone();
            let counter = blocked.clone();
            let mut paused = page
                .event_listener::<EventRequestPaused>()
                .await
                .map_err(|e| Error::Browser(format!("intercept listener: {e}")))?;
            Some(tokio::spawn(async move {
                while let Some(ev) = paused.next().await {
                    let drop_it = block_heavy
                        && matches!(
                            ev.resource_type,
                            ResourceType::Image | ResourceType::Font | ResourceType::Media
                        );
                    if drop_it {
                        // Fail the request so the resource never downloads.
                        if drain_page
                            .execute(FailRequestParams::new(
                                ev.request_id.clone(),
                                ErrorReason::BlockedByClient,
                            ))
                            .await
                            .is_ok()
                        {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        // Every paused request must be resolved or it hangs.
                        let _ = drain_page
                            .execute(ContinueRequestParams::new(ev.request_id.clone()))
                            .await;
                    }
                }
            }))
        } else {
            None
        };

        if let Err(e) = page.goto(req.url.as_str()).await {
            if let Some(d) = &drainer {
                d.abort();
            }
            let _ = page.close().await;
            return Err(Error::Browser(format!("goto {}: {e}", req.url)));
        }

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

        if let Some(d) = &drainer {
            d.abort();
        }
        if let Err(e) = page.close().await {
            warn!("page close: {e}");
        }

        let blocked_resources = blocked.load(Ordering::Relaxed);
        if blocked_resources > 0 {
            info!(url = %req.url, blocked = blocked_resources, "blocked heavy subresources");
        }

        Ok(RenderedPage {
            html,
            final_url,
            evaluated,
            nav_timed_out,
            selector_found,
            blocked_resources,
        })
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
    /// unit test is impractical; a gated live crash-recovery test lives in
    /// tests/render.rs.)
    #[test]
    fn dead_alive_flag_forces_relaunch() {
        // Alive + under quota => reuse.
        assert!(!is_stale(true, 0, 200));
        assert!(!is_stale(true, 199, 200));
        // Handler task died (crash/exit) => relaunch, regardless of counts.
        assert!(is_stale(false, 0, 200));
        assert!(is_stale(false, 5, 0));
    }

    #[test]
    fn recycle_threshold_is_honored() {
        // renders < threshold => fresh; >= threshold => stale.
        assert!(!is_stale(true, 199, 200));
        assert!(is_stale(true, 200, 200));
        assert!(is_stale(true, 201, 200));
        // 0 disables recycling regardless of count.
        assert!(!is_stale(true, u64::MAX, 0));
    }
}
