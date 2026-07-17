//! Headless-browser engine on chromiumoxide (Chrome DevTools Protocol).
//! Chrome launches lazily on first use with a persistent user-data dir, so
//! logged-in sessions survive restarts. Run once with `headless = false` to
//! log in to a site manually; subsequent headless scrapes reuse the cookies.
//!
//! ## Resilience & cost
//!
//! Chrome instances live behind relaunchable holders ([`BrowserEngine::acquire`]).
//! A background task drives each CDP handler loop and flips a liveness flag when
//! Chrome's connection ends (crash or exit); the next acquire sees the dead flag
//! and relaunches, so a crash no longer wedges every future render until a
//! server restart. A holder also relaunches after
//! `[browser] recycle_after_renders` renders to shed accumulated memory.
//! Relaunches are serialized per profile by a launch gate, so a crash or recycle
//! seen by several concurrent renders triggers **one** Chrome launch (not N
//! racing the same `--user-data-dir`); the launch itself runs off the holders
//! lock under a timeout so one slow start can't stall other profiles.
//!
//! ## Session profiles
//!
//! Chromium binds `--user-data-dir` at **launch**, so one Chrome = one profile.
//! A `RenderRequest.profile` therefore selects among a **small map of holders**
//! keyed by profile name (`None` = the shared default, `[browser] user_data_dir`),
//! each with the full relaunch/recycle logic above. At most [`MAX_LIVE_PROFILES`]
//! Chromes are kept alive; the least-recently-used holder is closed (dropped,
//! which reaps its Chrome) when a new profile pushes past the cap.
//!
//! The alternative — one holder that relaunches whenever the profile changes —
//! was rejected: interleaved profiles (the normal case for a queue serving
//! several logins) would thrash Chrome on every request. The cost of the map is
//! up to `MAX_LIVE_PROFILES` resident Chromes; LRU eviction bounds it.
//!
//! Concurrent renders are capped by `[browser] max_concurrent_renders` (a
//! semaphore, shared across profiles) so N callers can't spawn N unbounded tabs.
//! When `[browser] block_resources` is set, CDP request interception drops
//! images/fonts/media (never stylesheets) so scraping renders stay cheap; a
//! render can opt back in with `RenderRequest.load_all_resources`.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
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
use pumper_core::engine::PageAction;
use pumper_core::{
    lru_touch_evict, profile_browser_dir, Browser, Error, RenderRequest, RenderedPage, Result,
};
use tokio::sync::{Mutex, Semaphore};
use tracing::{info, warn};

/// Cap Chrome's V8 heap so a runaway page can't balloon the shared instance.
const JS_HEAP_CAP_MB: u32 = 512;
/// Max Chrome instances (= session profiles, incl. the default) kept alive at
/// once. Past this, the least-recently-used one is closed on the next acquire.
/// Each Chrome costs real memory, so this stays small; a workload cycling
/// through more than this many profiles pays a relaunch per eviction.
const MAX_LIVE_PROFILES: usize = 4;
/// Holder key for the profile-less default instance (`[browser] user_data_dir`).
/// The empty string can never collide with a real profile name — those are
/// validated non-empty by `pumper_core::validate_profile_name`.
const DEFAULT_PROFILE_KEY: &str = "";
/// Hard ceiling on a single Chrome launch, kept under chromiumoxide's own ~20s
/// `launch_timeout` so a wedged launch surfaces a typed error (and releases the
/// per-key launch gate) rather than parking every waiter for the full 20s.
const LAUNCH_TIMEOUT_SECS: u64 = 15;

/// Whether a held Chrome instance must be relaunched before the next render.
/// Pure so it can be unit-tested without a real browser: an instance is stale
/// when its handler task has died (crash detection, `alive == false`) or it has
/// served its recycle quota (`recycle > 0 && renders >= recycle`).
fn is_stale(alive: bool, renders: u64, recycle: u64) -> bool {
    !alive || (recycle > 0 && renders >= recycle)
}

/// Whether captured HTML of `html_len` bytes exceeds the `cap`. Pure so the cap
/// decision is unit-testable without Chrome. `cap == 0` disables the cap; strictly
/// over the cap fails (exactly at the cap is allowed, mirroring the HTTP tier).
fn over_html_cap(html_len: u64, cap: u64) -> bool {
    cap > 0 && html_len > cap
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

/// The live Chrome instances, one per profile, with LRU ordering.
#[derive(Default)]
struct Holders {
    live: HashMap<String, LiveBrowser>,
    /// Front = least-recently-used, back = most-recent.
    order: VecDeque<String>,
    /// Per-profile launch gate: a task launching (or relaunching) Chrome for a
    /// key holds this key's lock so concurrent stale/cold acquires for the SAME
    /// profile await one launch instead of each racing a full Chrome against the
    /// shared `--user-data-dir` (whose single-instance lock they'd contend for).
    /// Entries whose only reference is this map (no in-flight launch, no waiters)
    /// are pruned opportunistically to bound the map.
    launching: HashMap<String, Arc<Mutex<()>>>,
}


pub struct BrowserEngine {
    cfg: BrowserConfig,
    /// Root of the session vault (`[fetcher] profiles_dir`); a profile renders
    /// under `<profiles_dir>/<name>/browser`.
    profiles_dir: PathBuf,
    /// Relaunchable holders keyed by profile (`""` = the default instance). The
    /// mutex is held only briefly (health check + Arc clone, plus a launch on a
    /// miss), never for a render's duration, so renders run concurrently.
    holders: Mutex<Holders>,
    /// Caps concurrent renders (tabs) across all profiles. `None` = unlimited.
    render_slots: Option<Arc<Semaphore>>,
}

impl BrowserEngine {
    pub fn new(cfg: &BrowserConfig, profiles_dir: impl Into<PathBuf>) -> Self {
        let render_slots = match cfg.max_concurrent_renders {
            0 => None,
            n => Some(Arc::new(Semaphore::new(n))),
        };
        Self {
            cfg: cfg.clone(),
            profiles_dir: profiles_dir.into(),
            holders: Mutex::new(Holders::default()),
            render_slots,
        }
    }

    /// The user-data-dir a render should run under: the profile's `browser/`
    /// dir, or the shared `[browser] user_data_dir` when profile-less. Validates
    /// the profile name (typed `Error::Profile` on anything unsafe).
    fn user_data_dir(&self, profile: Option<&str>) -> Result<PathBuf> {
        match profile {
            Some(name) => profile_browser_dir(&self.profiles_dir, name),
            None => Ok(self.cfg.user_data_dir.clone()),
        }
    }

    /// Launches a fresh Chrome bound to `user_data_dir` and spawns its
    /// handler-drain task.
    async fn launch(&self, user_data_dir: &Path) -> Result<LiveBrowser> {
        // Chrome resolves --user-data-dir against its own working directory, not
        // ours, so a relative path (from config) fails to launch (exit 21).
        // Absolutize it against our cwd first.
        let mut user_data_dir = user_data_dir.to_path_buf();
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

        info!(user_data_dir = %user_data_dir.display(), "launching chrome");
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

    /// Returns a handle to a live Chrome **bound to `profile`'s user-data-dir**
    /// (`None` = the shared default instance), relaunching it if the previous one
    /// died (crash detection) or hit the recycle threshold. Counts one render and
    /// LRU-evicts the least-recently-used other profile past [`MAX_LIVE_PROFILES`].
    async fn acquire(&self, profile: Option<&str>) -> Result<Arc<ChromeBrowser>> {
        // Validated (and the path built) before any lock is taken.
        let user_data_dir = self.user_data_dir(profile)?;
        let key = profile.unwrap_or(DEFAULT_PROFILE_KEY).to_string();
        let recycle = self.cfg.recycle_after_renders;

        // Fast path: a live, fresh holder already exists — hand it out under the
        // lock without launching anything.
        {
            let mut holders = self.holders.lock().await;
            if Self::is_fresh(&holders, &key, recycle) {
                return Ok(Self::checkout(&mut holders, &key));
            }
        }

        // Slow path. Take this profile's launch gate so concurrent stale/cold
        // acquires for the SAME key collapse onto ONE launch: the 2nd..Nth caller
        // blocks here, then finds the holder the winner installed. Other profiles
        // are unaffected (their key, their gate). Cloned out under a brief lock;
        // finished gates (map-only refs) are pruned to keep the map small.
        let launch_gate = {
            let mut holders = self.holders.lock().await;
            Self::gate_for(&mut holders, &key)
        };
        let _gate = launch_gate.lock().await;

        // Re-check under the gate: a task that raced us here may have already
        // launched a fresh holder while we waited — if so, reuse it.
        {
            let mut holders = self.holders.lock().await;
            if Self::is_fresh(&holders, &key, recycle) {
                return Ok(Self::checkout(&mut holders, &key));
            }
            // Drop the stale/dead holder BEFORE launching so its Chrome releases
            // the `--user-data-dir` single-instance lock the replacement needs;
            // in-flight renders keep the outgoing browser alive via their own
            // `Arc<ChromeBrowser>` clone from `checkout`, so this can't kill a
            // render mid-flight.
            if holders.live.remove(&key).is_some() {
                info!(profile = %key, "recycling browser profile (dropped before relaunch)");
            }
        }

        // Launch WITHOUT the holders lock (a launch can take many seconds), and
        // under an explicit timeout below chromiumoxide's own ~20s ceiling.
        let launched = match tokio::time::timeout(
            Duration::from_secs(LAUNCH_TIMEOUT_SECS),
            self.launch(&user_data_dir),
        )
        .await
        {
            Ok(res) => res?,
            Err(_) => {
                return Err(Error::Browser(format!(
                    "chrome launch timed out after {LAUNCH_TIMEOUT_SECS}s (profile '{key}')"
                )))
            }
        };

        let mut holders = self.holders.lock().await;
        holders.live.insert(key.clone(), launched);
        Ok(Self::checkout(&mut holders, &key))
    }

    /// Get-or-create the per-key launch gate, first pruning gates that only the
    /// map still references (no in-flight launch, no waiters) so the map can't
    /// grow without bound across many distinct profiles. Caller holds the lock.
    fn gate_for(holders: &mut Holders, key: &str) -> Arc<Mutex<()>> {
        holders.launching.retain(|_, g| Arc::strong_count(g) > 1);
        holders.launching.entry(key.to_string()).or_default().clone()
    }

    /// Whether `key` has a live, non-stale holder. Caller holds the holders lock.
    fn is_fresh(holders: &Holders, key: &str, recycle: u64) -> bool {
        holders
            .live
            .get(key)
            .is_some_and(|l| !is_stale(l.alive.load(Ordering::Relaxed), l.renders, recycle))
    }

    /// Bumps the LRU order + render counter for `key` (evicting the least-recently
    /// used profile past `MAX_LIVE_PROFILES`) and returns its browser handle. The
    /// caller must hold the holders lock and `key` must be populated.
    fn checkout(holders: &mut Holders, key: &str) -> Arc<ChromeBrowser> {
        for evicted in lru_touch_evict(&mut holders.order, key, MAX_LIVE_PROFILES) {
            // Closing = dropping the holder (kill_on_drop reaps its Chrome).
            if holders.live.remove(&evicted).is_some() {
                info!(profile = %evicted, "closing least-recently-used browser profile");
            }
        }
        let live = holders.live.get_mut(key).expect("holder populated above");
        live.renders += 1;
        live.browser.clone()
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

        let browser = self.acquire(req.profile.as_deref()).await?;
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
            let found = wait_for_selector(&page, selector, deadline).await;
            if !found {
                warn!(selector = %selector, "wait_for_selector timed out");
            }
            selector_found = Some(found);
        }

        let settle_ms = req.extra_wait_ms.unwrap_or(self.cfg.default_wait_ms);
        if settle_ms > 0 {
            tokio::time::sleep(Duration::from_millis(settle_ms)).await;
        }

        // Scripted actions (scroll/click/wait) drive infinite-scroll / "load more"
        // pages the one-shot render can't reach. Run after the settle and before
        // `evaluate`, under a total-time deadline of one nav timeout so a `Repeat`
        // can't run forever.
        let actions_completed = if req.actions.is_empty() {
            0
        } else {
            let deadline = tokio::time::Instant::now() + nav_timeout;
            execute_actions(&page, &req.actions, deadline).await
        };

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

        // Capture content + url, then ALWAYS release the tab and the interception
        // drainer — even if content() failed, so a failed render does not leak a
        // Chrome tab plus a background drainer task.
        let content = page.content().await;
        let final_url = page.url().await.ok().flatten();
        if let Some(d) = &drainer {
            d.abort();
        }
        if let Err(e) = page.close().await {
            warn!("page close: {e}");
        }
        let html = content.map_err(|e| Error::Browser(format!("content: {e}")))?;

        // Cap the captured HTML like the HTTP tier caps its body, so a pathological
        // JS-built DOM can't balloon memory on the expensive tier — a typed error
        // naming the cap and URL, symmetric with `Error::Http`.
        let cap = req.max_body_bytes.unwrap_or(self.cfg.max_html_bytes);
        if over_html_cap(html.len() as u64, cap) {
            return Err(Error::Browser(format!(
                "rendered HTML from {} ({} bytes) exceeds max_html_bytes cap of {cap} bytes",
                req.url,
                html.len()
            )));
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
            actions_completed,
        })
    }
}

/// Polls for `selector` until it appears or `deadline` passes. Shared by the
/// `wait_for_selector` render option and the `WaitForSelector` page action.
async fn wait_for_selector(
    page: &chromiumoxide::Page,
    selector: &str,
    deadline: tokio::time::Instant,
) -> bool {
    loop {
        if page.find_element(selector).await.is_ok() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Counts elements matching `selector` via the DOM (for the scroll-until-stable
/// loop). Selector embedded as a JSON string literal so it's safely quoted.
async fn count_matches(page: &chromiumoxide::Page, selector: &str) -> u64 {
    let sel = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".into());
    let js = format!("document.querySelectorAll({sel}).length");
    page.evaluate(js).await.ok().and_then(|r| r.into_value::<u64>().ok()).unwrap_or(0)
}

/// Runs a scripted [`PageAction`] list in order, stopping at `deadline`. Returns
/// the count of top-level actions that ran (a `Repeat` counts as one). Boxed so
/// `Repeat` can recurse into its steps. Every step is best-effort — a failed
/// click/selector is logged and skipped, never aborting the render.
fn execute_actions<'a>(
    page: &'a chromiumoxide::Page,
    actions: &'a [PageAction],
    deadline: tokio::time::Instant,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = usize> + Send + 'a>> {
    Box::pin(async move {
        let mut completed = 0usize;
        for action in actions {
            if tokio::time::Instant::now() >= deadline {
                warn!("page actions hit the time budget; capturing current DOM");
                break;
            }
            match action {
                PageAction::ScrollBottom => {
                    let _ = page.evaluate("window.scrollTo(0, document.body.scrollHeight)").await;
                }
                PageAction::ScrollBy { pixels } => {
                    let _ = page.evaluate(format!("window.scrollBy(0, {pixels})")).await;
                }
                PageAction::Click { selector } => match page.find_element(selector).await {
                    Ok(el) => {
                        if let Err(e) = el.click().await {
                            warn!(selector = %selector, "page action click failed: {e}");
                        }
                    }
                    Err(_) => warn!(selector = %selector, "page action click: selector not found"),
                },
                PageAction::Type { selector, text } => {
                    if let Ok(el) = page.find_element(selector).await {
                        let _ = el.click().await;
                        if let Err(e) = el.type_str(text).await {
                            warn!(selector = %selector, "page action type failed: {e}");
                        }
                    } else {
                        warn!(selector = %selector, "page action type: selector not found");
                    }
                }
                PageAction::WaitForSelector { selector, timeout_ms } => {
                    let d = timeout_ms
                        .map(|ms| tokio::time::Instant::now() + Duration::from_millis(ms))
                        .unwrap_or(deadline)
                        .min(deadline);
                    wait_for_selector(page, selector, d).await;
                }
                PageAction::WaitMs { ms } => {
                    tokio::time::sleep(Duration::from_millis(*ms)).await;
                }
                PageAction::Repeat { times, steps, until_selector_count_stable } => {
                    let mut last_count: Option<u64> = None;
                    for _ in 0..*times {
                        if tokio::time::Instant::now() >= deadline {
                            break;
                        }
                        execute_actions(page, steps, deadline).await;
                        // Stop early once the tracked selector's match count stops
                        // growing — "scroll until no new rows load".
                        if let Some(sel) = until_selector_count_stable {
                            let count = count_matches(page, sel).await;
                            if last_count.is_some_and(|prev| count <= prev) {
                                break;
                            }
                            last_count = Some(count);
                        }
                    }
                }
            }
            completed += 1;
        }
        completed
    })
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
        assert!(BrowserEngine::new(&c, "data/profiles").render_slots.is_none());
    }

    #[test]
    fn gate_for_reuses_same_key_and_separates_keys() {
        let mut holders = Holders::default();
        let a1 = BrowserEngine::gate_for(&mut holders, "p1");
        let a2 = BrowserEngine::gate_for(&mut holders, "p1");
        let b = BrowserEngine::gate_for(&mut holders, "p2");
        // Same key → same gate (concurrent same-profile acquires collapse).
        assert!(Arc::ptr_eq(&a1, &a2));
        // Different key → different gate (other profiles proceed independently).
        assert!(!Arc::ptr_eq(&a1, &b));
    }

    #[test]
    fn gate_for_prunes_unreferenced_gates() {
        let mut holders = Holders::default();
        // A caller currently launching for "held" keeps its gate referenced.
        let _held = BrowserEngine::gate_for(&mut holders, "held");
        // "done" has no outstanding reference (its would-be caller finished).
        BrowserEngine::gate_for(&mut holders, "done");
        assert_eq!(holders.launching.len(), 2);
        // Next get-or-create prunes map-only ("done") but keeps referenced ("held").
        let _next = BrowserEngine::gate_for(&mut holders, "new");
        assert!(holders.launching.contains_key("held"));
        assert!(!holders.launching.contains_key("done"));
        assert!(holders.launching.contains_key("new"));
    }

    #[test]
    fn semaphore_present_and_sized_when_capped() {
        let mut c = cfg();
        c.max_concurrent_renders = 3;
        let engine = BrowserEngine::new(&c, "data/profiles");
        let sem = engine.render_slots.expect("cap => semaphore");
        assert_eq!(sem.available_permits(), 3);
    }

    #[test]
    fn profile_selects_its_own_user_data_dir() {
        let mut c = cfg();
        c.user_data_dir = "data/browser-profile".into();
        let engine = BrowserEngine::new(&c, "data/profiles");
        // Profile-less renders keep using the shared [browser] user_data_dir.
        assert_eq!(
            engine.user_data_dir(None).unwrap(),
            std::path::PathBuf::from("data/browser-profile")
        );
        // A profile renders under its own Chrome user-data-dir in the vault.
        assert_eq!(
            engine.user_data_dir(Some("acme")).unwrap(),
            std::path::Path::new("data/profiles").join("acme").join("browser")
        );
        // An unsafe name is rejected before any path exists.
        let err = engine.user_data_dir(Some("../../etc")).unwrap_err();
        assert!(matches!(err, Error::Profile(_)), "got {err:?}");
    }

    #[test]
    fn holders_are_lru_bounded_by_max_live_profiles() {
        // Filling to the cap evicts nothing.
        let mut order = VecDeque::new();
        for i in 0..MAX_LIVE_PROFILES {
            assert!(lru_touch_evict(&mut order, &format!("p{i}"), MAX_LIVE_PROFILES).is_empty());
        }
        assert_eq!(order.len(), MAX_LIVE_PROFILES);
        // Touching p0 makes it most-recent, so p1 becomes the victim when a new
        // profile pushes past the cap.
        assert!(lru_touch_evict(&mut order, "p0", MAX_LIVE_PROFILES).is_empty());
        let evicted = lru_touch_evict(&mut order, "pN", MAX_LIVE_PROFILES);
        assert_eq!(evicted, vec!["p1".to_string()], "least-recently-used closed");
        assert_eq!(order.len(), MAX_LIVE_PROFILES);
        assert!(order.contains(&"p0".to_string()), "recently used kept alive");
        assert!(order.contains(&"pN".to_string()), "newest is live");
        // The key just acquired is never itself evicted.
        let mut tight = VecDeque::from(vec!["a".to_string()]);
        assert_eq!(lru_touch_evict(&mut tight, "b", 1), vec!["a".to_string()]);
        assert_eq!(tight, VecDeque::from(vec!["b".to_string()]));
    }

    #[test]
    fn default_profile_key_cannot_collide_with_a_real_profile() {
        // Real profile names are validated non-empty, so "" is exclusively ours.
        assert!(pumper_core::validate_profile_name(DEFAULT_PROFILE_KEY).is_err());
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

    #[test]
    fn html_cap_is_strict_and_disabled_by_zero() {
        assert!(!over_html_cap(100, 100), "exactly at the cap is allowed");
        assert!(!over_html_cap(99, 100));
        assert!(over_html_cap(101, 100), "strictly over fails");
        // 0 disables the cap regardless of size.
        assert!(!over_html_cap(u64::MAX, 0));
    }
}
