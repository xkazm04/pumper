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
use chromiumoxide::cdp::browser_protocol::network::{
    EnableParams, ErrorReason, EventRequestWillBeSent, EventResponseReceived,
    GetResponseBodyParams, ResourceType,
};
use futures::StreamExt;
use pumper_core::config::BrowserConfig;
use pumper_core::engine::{
    interaction_outcome, parse_transact_probe, pass_fully_succeeded, require_existing_profile,
    summarize_steps, transact_probe_js, CapturedCall, PageAction, StepOutcome,
};
use pumper_core::{
    lru_touch_evict, profile_browser_dir, Browser, Error, RenderRequest, RenderedPage, Result,
    TransactEvidence, TransactRequest,
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
/// Ceiling on giving one tab back. Cleanup, not work: a Chrome that has already
/// died never answers `Page.close`, and waiting on it would turn a crash into a
/// hang on the *cleanup* path — including inside a detached drop task nobody is
/// awaiting.
const TAB_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// Slice of the render budget a caller-requested wait may never eat, so a
/// pathological `extra_wait_ms` / `wait_ms` is *truncated* rather than turned
/// into a failed job: sleeping the budget out to the last millisecond would
/// leave nothing to capture the DOM with, and the render would die at
/// `content()` having done all the work.
const CAPTURE_RESERVE: Duration = Duration::from_secs(5);

// ── network capture (API X-ray) caps ─────────────────────────────────────────
// A `capture_network` render observes the page's own XHR/fetch traffic; every
// cap below bounds what an arbitrary page can make us buffer.

/// Max captured JSON responses returned per render.
const CAPTURE_MAX_CALLS: usize = 30;
/// Per-response body cap (bytes); over-cap bodies are dropped, never truncated
/// (a truncated body is no longer valid JSON).
const CAPTURE_MAX_BODY_BYTES: usize = 256 * 1024;
/// Total budget across all captured bodies in one render (bytes).
const CAPTURE_MAX_TOTAL_BYTES: usize = 2 * 1024 * 1024;
/// Candidate events remembered before body fetch (some won't yield a body, so
/// keep more candidates than final capture slots).
const CAPTURE_MAX_CANDIDATES: usize = 4 * CAPTURE_MAX_CALLS;

/// Whether a response MIME type is JSON (`application/json`,
/// `application/vnd.foo+json`, `text/json`, with or without parameters).
fn is_json_mime(mime: &str) -> bool {
    let essence = mime.split(';').next().unwrap_or("").trim().to_lowercase();
    essence.ends_with("/json") || essence.ends_with("+json")
}

/// Same-site check for captured calls: the API host must be the page's host or
/// a sibling/subdomain of it (`www.example.com` page ↔ `api.example.com` API).
/// Comparing after stripping a leading `www.` and accepting suffix containment
/// keeps this dependency-free (no PSL); a cross-site CDN or tracker never
/// shares the page's registrable tail this way.
fn same_site(page_host: &str, call_host: &str) -> bool {
    let a = page_host.trim_start_matches("www.").to_lowercase();
    let b = call_host.trim_start_matches("www.").to_lowercase();
    if a.is_empty() || b.is_empty() {
        return false;
    }
    a == b || b.ends_with(&format!(".{a}")) || a.ends_with(&format!(".{b}"))
}

/// Capture-budget decision: whether a body of `len` bytes may still be taken
/// given the running `total` and call `count`. Pure for unit tests.
fn capture_fits(len: usize, total: usize, count: usize) -> bool {
    count < CAPTURE_MAX_CALLS
        && len <= CAPTURE_MAX_BODY_BYTES
        && total + len <= CAPTURE_MAX_TOTAL_BYTES
}

/// One network response observed during the render, pending body retrieval.
#[derive(Debug, Clone)]
struct PendingCapture {
    request_id: chromiumoxide::cdp::browser_protocol::network::RequestId,
    url: String,
    method: String,
    status: u16,
    content_type: String,
}

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

// ── the render budget ────────────────────────────────────────────────────────
//
// `nav_timeout_secs` reads like a render budget but bounds ONE of a render's
// waits. Everything else was unbounded: `goto`, `evaluate`,
// `Network.getResponseBody`, `content()`, `page.url()`, `find_element` inside
// the selector poll — plus `extra_wait_ms` and the `wait_ms` action, raw `u64`
// milliseconds a caller supplies with no ceiling. A render holds one of only
// `max_concurrent_renders` slots for its whole life, so four such calls (or one
// half-dead Chrome that stays "alive" but never answers CDP) wedge the browser
// tier for every app on the box, with no error until the job timeout — which
// then dropped the future and leaked the tab (see `RenderScope`).
//
// One deadline now bounds the lot.

/// Deadline for a render starting at `now`, or `None` when the budget is off
/// (`render_budget_secs = 0`). Pure so the arithmetic is testable without Chrome.
fn budget_deadline(now: tokio::time::Instant, budget_secs: u64) -> Option<tokio::time::Instant> {
    (budget_secs > 0).then(|| now + Duration::from_secs(budget_secs))
}

/// Deadline for one *stage* of a render (a navigation wait, a selector wait, an
/// action list): its own cap, clamped by whatever is left of the render budget.
/// Pure.
fn stage_deadline(
    now: tokio::time::Instant,
    cap: Duration,
    budget: Option<tokio::time::Instant>,
) -> tokio::time::Instant {
    let own = now + cap;
    match budget {
        Some(end) => own.min(end),
        None => own,
    }
}

/// How long a caller-requested wait may actually sleep, and whether it was cut.
///
/// `remaining = None` means the budget is disabled and the caller gets exactly
/// what they asked for. Otherwise the wait is clamped to the remaining budget
/// **less [`CAPTURE_RESERVE`]**: the point is a truncated wait with a visible
/// signal, not a render that sleeps its whole budget away and then fails at
/// capture time. Pure.
fn clamp_wait_ms(requested_ms: u64, remaining: Option<Duration>) -> (u64, bool) {
    let Some(remaining) = remaining else {
        return (requested_ms, false);
    };
    let usable = remaining.saturating_sub(CAPTURE_RESERVE);
    let usable_ms = u64::try_from(usable.as_millis()).unwrap_or(u64::MAX);
    if requested_ms <= usable_ms {
        (requested_ms, false)
    } else {
        (usable_ms, true)
    }
}

/// The failure a render owes when its total budget runs out: it names the budget
/// and the stage, because "browser engine: timed out" leaves an operator
/// guessing which of six waits died and which knob moves it.
///
/// Stays an `Error::Browser`, i.e. **retryable**, on purpose: unlike a malformed
/// request, "this page was too slow *this time*" is a fact about a live remote
/// site, and the next attempt may well be fast.
fn budget_exhausted(stage: &str, url: &str, budget_secs: u64) -> Error {
    Error::Browser(format!(
        "render budget exhausted after {budget_secs}s while {stage} ({url}): one render may hold \
         its tab and one of the [browser] max_concurrent_renders slots for at most \
         [browser] render_budget_secs. Raise that key for genuinely slow pages, or narrow the \
         render (wait_for_selector, fewer actions, smaller extra_wait_ms) so it fits."
    ))
}

/// The single deadline that bounds one render, from the moment it owns a Chrome
/// to the moment it hands back HTML.
#[derive(Clone, Copy)]
struct RenderBudget {
    /// `[browser] render_budget_secs`, carried so failures can name it.
    secs: u64,
    /// `None` = the budget is disabled.
    deadline: Option<tokio::time::Instant>,
}

impl RenderBudget {
    /// Starts the clock. Called AFTER the Chrome handle is acquired: a render
    /// queued behind a busy semaphore, or waiting on a relaunch, must not burn
    /// budget it never got to use (the launch has its own `LAUNCH_TIMEOUT_SECS`).
    fn start(budget_secs: u64) -> Self {
        Self {
            secs: budget_secs,
            deadline: budget_deadline(tokio::time::Instant::now(), budget_secs),
        }
    }

    fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(tokio::time::Instant::now()))
    }

    /// Deadline for a stage with its own cap (`nav_timeout_secs`), clamped by
    /// the budget.
    fn stage(&self, cap: Duration) -> tokio::time::Instant {
        stage_deadline(tokio::time::Instant::now(), cap, self.deadline)
    }

    /// Awaits something the render **cannot proceed without**; running out of
    /// budget here is a typed failure naming the budget.
    async fn require<T>(
        &self,
        stage: &str,
        url: &str,
        fut: impl std::future::Future<Output = T>,
    ) -> Result<T> {
        match self.deadline {
            None => Ok(fut.await),
            Some(d) => tokio::time::timeout_at(d, fut)
                .await
                .map_err(|_| budget_exhausted(stage, url, self.secs)),
        }
    }

    /// Awaits something best-effort (an `evaluate`, a captured body, the final
    /// URL): `None` when the budget ran out, and the render carries on with what
    /// it already has rather than throwing the page away.
    async fn attempt<T>(&self, fut: impl std::future::Future<Output = T>) -> Option<T> {
        match self.deadline {
            None => Some(fut.await),
            Some(d) => tokio::time::timeout_at(d, fut).await.ok(),
        }
    }
}

// ── render cleanup (RAII) ────────────────────────────────────────────────────

/// The half of a render's cleanup that must `.await`: giving the tab back.
///
/// A trait rather than the concrete `chromiumoxide::Page` for one reason —
/// [`RenderScope`]'s entire job is what it does on **drop**, and a test can only
/// observe that against a closable it controls. Launching real Chrome to prove a
/// tab is released would put the guard's whole contract behind an `#[ignore]`.
#[async_trait]
trait Closable: Send + Sync + 'static {
    /// Best-effort by contract: a render whose Chrome already died has nothing
    /// left to close, and that is the *normal* end of a crashed render, not an
    /// incident.
    async fn close(&self);
}

/// A live Chrome tab.
struct Tab(chromiumoxide::Page);

#[async_trait]
impl Closable for Tab {
    async fn close(&self) {
        // `Page::close` takes `self` by value; `Page` is a cheap Arc handle, so
        // the clone costs nothing and leaves ours intact for the (idempotent)
        // second call that can never happen.
        match tokio::time::timeout(TAB_CLOSE_TIMEOUT, self.0.clone().close()).await {
            Ok(Ok(())) => {}
            // Expected whenever Chrome went away underneath the render: the tab
            // died with it. Logged quietly so a crash does not report twice.
            Ok(Err(e)) => tracing::debug!("page close: {e}"),
            Err(_) => tracing::debug!("page close did not answer in {TAB_CLOSE_TIMEOUT:?}"),
        }
    }
}

/// Everything ONE render must give back, released by [`Drop`] instead of by
/// remembering to release it on each exit path.
///
/// The anti-pattern this replaces: `render` closed its page and aborted its two
/// auxiliary tasks on the happy path and on the goto-error path — and on no
/// other. The worker races the app future against `DELETE /jobs/{id}` and the
/// wall-clock job timeout and `break`s out of its `select!`, which **drops** the
/// render future; a dropped future runs no cleanup path at all. Dropping a
/// `JoinHandle` *detaches* its task rather than aborting it, so every cancelled
/// or timed-out render left a Chrome tab plus one or two tasks still servicing
/// that dead tab's CDP events, invisibly, until the 200-render recycle relaunched
/// Chrome. Two of the engine's own `?` early-returns (the interception and
/// capture listener errors) leaked the same way.
///
/// Cleanup therefore lives on **no** path: it lives in `Drop`.
struct RenderScope {
    /// `None` once closed — the close-exactly-once latch. `Option::take` is the
    /// whole double-close guard, so [`Self::release`] followed by a drop (the
    /// success path) closes once, and a drop alone (cancel/timeout/`?`) also
    /// closes once.
    page: Option<Arc<dyn Closable>>,
    /// Tasks whose only reason to exist is this render's tab.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl RenderScope {
    fn new(page: Arc<dyn Closable>) -> Self {
        Self {
            page: Some(page),
            tasks: Vec::new(),
        }
    }

    /// Binds a spawned task's life to this render's tab.
    fn watch(&mut self, task: tokio::task::JoinHandle<()>) {
        self.tasks.push(task);
    }

    fn abort_tasks(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }

    /// The success path's explicit release: abort the tasks, then **await** the
    /// close at exactly the point the old code closed the page. Idempotent — the
    /// subsequent `Drop` finds nothing to do.
    async fn release(&mut self) {
        self.abort_tasks();
        if let Some(page) = self.page.take() {
            page.close().await;
        }
    }
}

impl Drop for RenderScope {
    fn drop(&mut self) {
        self.abort_tasks();
        let Some(page) = self.page.take() else {
            return; // already released on the success path
        };
        // `Drop` cannot `.await`, so the close is handed to a detached task.
        //
        // FAILURE MODE, stated because it is real: this is best-effort. If the
        // runtime is shutting down (the usual reason a render future is dropped
        // during a server drain) the spawned task may never be polled, and if
        // the drop happens with no runtime entered at all there is nowhere to
        // spawn it. In both cases the tab is not closed here — the holder's
        // crash/recycle relaunch (`[browser] recycle_after_renders`) remains the
        // backstop, exactly as it was for every leak before this guard. What the
        // guard makes unconditional is the `abort` above: no task outlives its
        // tab, ever, because that needs no runtime.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move { page.close().await });
            }
            Err(_) => warn!("render scope dropped outside a tokio runtime; tab left to recycle"),
        }
    }
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

        Ok(LiveBrowser {
            browser: Arc::new(browser),
            alive,
            renders: 0,
        })
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
        holders
            .launching
            .entry(key.to_string())
            .or_default()
            .clone()
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
        // ONE deadline for everything from here to the return. Started after the
        // Chrome handle is in hand, so queueing behind the semaphore or a
        // relaunch does not eat a render's budget.
        let budget = RenderBudget::start(self.cfg.render_budget_secs);

        // Start blank so the interception drainer is listening before the first
        // (document) request fires; otherwise the initial navigation would pause
        // with no one to resolve it and hang.
        let page = budget
            .require("opening a tab", &req.url, browser.new_page("about:blank"))
            .await?
            .map_err(|e| Error::Browser(format!("new_page: {e}")))?;
        // From here to the return, EVERY exit path — including the ones that are
        // not exits at all (a cancelled or timed-out job drops this future
        // mid-await) — releases the tab and its tasks through this guard.
        let mut scope = RenderScope::new(Arc::new(Tab(page.clone())));

        // Resource-blocking drainer. Only wired when interception is enabled at
        // launch (`block_resources`); otherwise no Fetch events ever fire.
        let blocked = Arc::new(AtomicUsize::new(0));
        if self.cfg.block_resources {
            let block_heavy = !req.load_all_resources;
            let drain_page = page.clone();
            let counter = blocked.clone();
            let mut paused = budget
                .require(
                    "attaching the interception listener",
                    &req.url,
                    page.event_listener::<EventRequestPaused>(),
                )
                .await?
                .map_err(|e| Error::Browser(format!("intercept listener: {e}")))?;
            scope.watch(tokio::spawn(async move {
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
            }));
        }

        // Network capture (API X-ray): when requested, remember same-site JSON
        // responses as they arrive; bodies are pulled AFTER settle/actions (and
        // before the tab closes), size-capped. Listeners attach before goto so
        // the page's very first XHR is observed.
        let candidates: Arc<std::sync::Mutex<Vec<PendingCapture>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        if req.capture_network {
            let page_host = url::Url::parse(&req.url)
                .ok()
                .and_then(|u| u.host_str().map(str::to_lowercase))
                .unwrap_or_default();
            // Explicitly enable the Network domain — harmless if already on.
            match budget.attempt(page.execute(EnableParams::default())).await {
                Some(Err(e)) => warn!("network capture: enable failed: {e}"),
                None => warn!("network capture: enable hit the render budget"),
                Some(Ok(_)) => {}
            }
            let mut sent = budget
                .require(
                    "attaching the capture listener",
                    &req.url,
                    page.event_listener::<EventRequestWillBeSent>(),
                )
                .await?
                .map_err(|e| Error::Browser(format!("capture listener (request): {e}")))?;
            let mut received = budget
                .require(
                    "attaching the capture listener",
                    &req.url,
                    page.event_listener::<EventResponseReceived>(),
                )
                .await?
                .map_err(|e| Error::Browser(format!("capture listener (response): {e}")))?;
            let sink = candidates.clone();
            scope.watch(tokio::spawn(async move {
                // request-id → method, from the request side of the pair.
                let mut methods: HashMap<String, String> = HashMap::new();
                loop {
                    tokio::select! {
                        ev = sent.next() => {
                            let Some(ev) = ev else { break };
                            if methods.len() < 4 * CAPTURE_MAX_CANDIDATES {
                                methods.insert(
                                    ev.request_id.inner().clone(),
                                    ev.request.method.clone(),
                                );
                            }
                        }
                        ev = received.next() => {
                            let Some(ev) = ev else { break };
                            let mime = ev.response.mime_type.clone();
                            let call_host = url::Url::parse(&ev.response.url)
                                .ok()
                                .and_then(|u| u.host_str().map(str::to_lowercase))
                                .unwrap_or_default();
                            if !is_json_mime(&mime) || !same_site(&page_host, &call_host) {
                                continue;
                            }
                            let mut sink = sink.lock().expect("capture sink poisoned");
                            if sink.len() >= CAPTURE_MAX_CANDIDATES {
                                continue;
                            }
                            sink.push(PendingCapture {
                                request_id: ev.request_id.clone(),
                                url: ev.response.url.clone(),
                                method: methods
                                    .get(ev.request_id.inner())
                                    .cloned()
                                    .unwrap_or_else(|| "GET".to_string()),
                                status: ev.response.status as u16,
                                content_type: mime,
                            });
                        }
                    }
                }
            }));
        }

        if let Err(e) = budget
            .require("navigating", &req.url, page.goto(req.url.as_str()))
            .await?
        {
            // No cleanup here on purpose: `scope` releases the tab and both
            // tasks as it drops out of this early return.
            return Err(Error::Browser(format!("goto {}: {e}", req.url)));
        }

        let mut nav_timed_out = false;
        match tokio::time::timeout_at(budget.stage(nav_timeout), page.wait_for_navigation()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => warn!(url = %req.url, "navigation: {e}"),
            Err(_) => {
                nav_timed_out = true;
                warn!(url = %req.url, "navigation wait timed out; capturing current DOM");
            }
        }

        let mut selector_found = None;
        if let Some(selector) = &req.wait_for_selector {
            let found = wait_for_selector(&page, selector, budget.stage(nav_timeout)).await;
            if !found {
                warn!(selector = %selector, "wait_for_selector timed out");
            }
            selector_found = Some(found);
        }

        // The settle wait is raw caller-supplied milliseconds (or the config
        // default), so it is clamped to the budget rather than trusted.
        let requested_settle = req.extra_wait_ms.unwrap_or(self.cfg.default_wait_ms);
        let (settle_ms, settle_truncated) = clamp_wait_ms(requested_settle, budget.remaining());
        if settle_truncated {
            warn!(
                url = %req.url, requested_ms = requested_settle, granted_ms = settle_ms,
                "settle wait truncated to fit the render budget"
            );
        }
        if settle_ms > 0 {
            tokio::time::sleep(Duration::from_millis(settle_ms)).await;
        }

        // Scripted actions (scroll/click/wait) drive infinite-scroll / "load more"
        // pages the one-shot render can't reach. Run after the settle and before
        // `evaluate`, under a total-time deadline of one nav timeout so a `Repeat`
        // can't run forever.
        let action_outcomes = if req.actions.is_empty() {
            Vec::new()
        } else {
            execute_actions(&page, &req.actions, budget.stage(nav_timeout)).await
        };
        let actions_completed = action_outcomes.len();

        let evaluated = match &req.evaluate {
            Some(js) => match budget.attempt(page.evaluate(js.as_str())).await {
                Some(Ok(result)) => result.into_value::<serde_json::Value>().ok(),
                Some(Err(e)) => {
                    warn!("evaluate failed: {e}");
                    None
                }
                None => {
                    warn!(url = %req.url, "evaluate hit the render budget; no result");
                    None
                }
            },
            None => None,
        };

        // Pull captured JSON bodies while the tab is still alive (CDP retains
        // response bodies only for the page's lifetime). Size-capped per body
        // and in total; non-parsing bodies are dropped.
        let mut network: Vec<CapturedCall> = Vec::new();
        if req.capture_network {
            let pending: Vec<PendingCapture> = candidates
                .lock()
                .expect("capture sink poisoned")
                .drain(..)
                .collect();
            let mut total = 0usize;
            for p in pending {
                if network.len() >= CAPTURE_MAX_CALLS {
                    break;
                }
                let got = match budget
                    .attempt(page.execute(GetResponseBodyParams::new(p.request_id.clone())))
                    .await
                {
                    Some(Ok(res)) => res.result,
                    Some(Err(_)) => continue, // body evicted / no body (204, redirects)
                    // Out of budget: keep the bodies already pulled and get on
                    // with capturing the DOM, which is what the caller asked for.
                    None => break,
                };
                if got.base64_encoded {
                    // A JSON body is text; base64 here means binary — skip.
                    continue;
                }
                if !capture_fits(got.body.len(), total, network.len()) {
                    continue;
                }
                let Ok(body) = serde_json::from_str::<serde_json::Value>(&got.body) else {
                    continue;
                };
                total += got.body.len();
                network.push(CapturedCall {
                    url: p.url,
                    method: p.method,
                    status: p.status,
                    content_type: p.content_type,
                    body,
                });
            }
            if !network.is_empty() {
                info!(url = %req.url, calls = network.len(), "captured network JSON responses");
            }
        }

        // Capture content + url, then release the tab and both auxiliary tasks at
        // exactly this point — the same point, in the same order, as before the
        // guard existed. Everything after this is arithmetic over values already
        // in hand, so a failure there costs nothing.
        let content = budget
            .require("capturing the DOM", &req.url, page.content())
            .await;
        let final_url = budget
            .attempt(page.url())
            .await
            .and_then(|u| u.ok())
            .flatten();
        scope.release().await;
        let html = content?.map_err(|e| Error::Browser(format!("content: {e}")))?;

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
            action_outcomes,
            network,
        })
    }

    /// Executes a declarative transact flow **dry-run only** (M06 v1): the
    /// reversible steps run through the existing render/action machinery
    /// (profile-bound Chrome, tab cap, action deadline), the live DOM values of
    /// every filled field are read via `evaluate`, and the flow STOPS at that
    /// state — `req.submit_action` is never handed to the executor; there is no
    /// code path here that could run it. The evidence bundle carries the DOM
    /// snapshot, filled-field summary, and the exact would-be action.
    async fn transact(&self, req: TransactRequest) -> Result<TransactEvidence> {
        // Defense in depth: apps validate before dispatch, and the engine
        // re-validates so a raw caller can't slip `submit: true` past the app
        // layer. Typed Error::Transact, never a silent downgrade to dry-run.
        req.validate()?;
        // A flow that ACTS must run under an identity that already exists.
        // `acquire` would otherwise `create_dir_all` a typo'd profile into a
        // fresh, logged-OUT Chrome and produce a plausible bundle of a login
        // wall. Checked BEFORE any Chrome work — nothing is launched, nothing
        // is created. "No profile" stays valid.
        if let Some(name) = &req.profile {
            let browser_dir = profile_browser_dir(&self.profiles_dir, name)?;
            require_existing_profile(name, browser_dir.is_dir())?;
        }
        let fill = req.fill_selectors();
        let submit_selector = req.submit_action.selector().map(str::to_string);
        let mut render = RenderRequest::new(&req.url);
        render.profile = req.profile.clone();
        render.wait_for_selector = req.wait_for_selector.clone();
        render.extra_wait_ms = req.extra_wait_ms;
        // The transact path caps the DOM ITSELF (truncate-and-flag below), so
        // the render's fail-closed cap is disabled for this call only. An
        // over-cap DOM must not destroy the evidence for a flow that has
        // already navigated, filled and clicked — the read-only render path
        // keeps failing closed, because there nothing has happened yet.
        render.max_body_bytes = Some(0);
        // Only the reversible steps are executed. `submit_action` is
        // deliberately NOT appended — stop-before-submit is structural.
        render.actions = req.steps.clone();
        render.evaluate = Some(transact_probe_js(&fill, submit_selector.as_deref()));
        let page = self.render(render).await?;
        let cap = req.max_body_bytes.unwrap_or(self.cfg.max_html_bytes);
        Ok(evidence_from_render(req, &fill, page, cap))
    }
}

/// Truncates `html` to at most `cap` bytes, on a UTF-8 char boundary, reporting
/// whether anything was cut. `cap == 0` disables the cap (mirrors
/// [`over_html_cap`]). Pure, so the truncate-don't-destroy contract is testable
/// without Chrome.
fn truncate_to_cap(html: String, cap: u64) -> (String, bool) {
    if !over_html_cap(html.len() as u64, cap) {
        return (html, false);
    }
    let mut end = cap as usize;
    while end > 0 && !html.is_char_boundary(end) {
        end -= 1;
    }
    let mut html = html;
    html.truncate(end);
    (html, true)
}

/// Assembles the dry-run evidence bundle from a completed render. Pure, so the
/// stop-before-submit contract (`dry_run: true`, `would_submit` untouched, no
/// screenshot claim the engine can't honor) and the honest step accounting are
/// testable without Chrome.
fn evidence_from_render(
    req: TransactRequest,
    fill_selectors: &[String],
    page: RenderedPage,
    dom_cap_bytes: u64,
) -> TransactEvidence {
    let submit_selector = req.submit_action.selector().map(str::to_string);
    let (filled_fields, submit_target) = parse_transact_probe(
        fill_selectors,
        submit_selector.as_deref(),
        page.evaluated.as_ref(),
    );
    let steps = summarize_steps(req.steps.len(), &page.action_outcomes);
    let dom_bytes = page.html.len();
    let (dom_html, dom_truncated) = truncate_to_cap(page.html, dom_cap_bytes);
    TransactEvidence {
        dry_run: true,
        idempotency_key: req.idempotency_key,
        profile: req.profile,
        url: req.url,
        final_url: page.final_url,
        steps_requested: steps.requested,
        steps_attempted: steps.attempted,
        steps_completed: steps.completed,
        step_outcomes: page.action_outcomes,
        steps_deadline_hit: steps.deadline_hit,
        wait_for_selector_found: page.selector_found,
        filled_fields,
        would_submit: req.submit_action,
        submit_target,
        dom_html,
        dom_bytes,
        dom_truncated,
        // Honest gap: the render path does not expose screenshot capture yet;
        // claiming a path here would be a lie the reviewer acts on.
        screenshot_path: None,
        nav_timed_out: page.nav_timed_out,
    }
}

/// Polls for `selector` until it appears or `deadline` passes. Shared by the
/// `wait_for_selector` render option and the `WaitForSelector` page action.
///
/// Each probe runs **under the deadline**, not merely before it: the loop used
/// to check the clock only *after* `find_element` returned, so a single CDP call
/// that never answered (a half-dead Chrome) blew the deadline by an unbounded
/// amount while the render kept its tab and its concurrency slot.
async fn wait_for_selector(
    page: &chromiumoxide::Page,
    selector: &str,
    deadline: tokio::time::Instant,
) -> bool {
    loop {
        if let Ok(Ok(_)) = tokio::time::timeout_at(deadline, page.find_element(selector)).await {
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
    page.evaluate(js)
        .await
        .ok()
        .and_then(|r| r.into_value::<u64>().ok())
        .unwrap_or(0)
}

/// Runs a scripted [`PageAction`] list in order, stopping at `deadline`.
/// Returns **one [`StepOutcome`] per top-level action the executor reached**, in
/// order (a `Repeat` counts as one, with a coarse rolled-up outcome). Boxed so
/// `Repeat` can recurse into its steps. Every step is still best-effort — a
/// failed click/selector is logged and skipped, never aborting the render — but
/// the failure is now *recorded* instead of silently counted as progress.
///
/// The anti-pattern this replaces: `completed += 1` sat outside every match arm,
/// so a flow whose three selectors all missed reported three completed steps,
/// and the evidence bundle a human approves off could not tell that run apart
/// from a clean one. `outcomes.len()` is the attempt count callers used to get;
/// `outcomes.iter().filter(is_ok)` is the honest success count.
fn execute_actions<'a>(
    page: &'a chromiumoxide::Page,
    actions: &'a [PageAction],
    deadline: tokio::time::Instant,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<StepOutcome>> + Send + 'a>> {
    Box::pin(async move {
        let mut outcomes: Vec<StepOutcome> = Vec::with_capacity(actions.len());
        for action in actions {
            if tokio::time::Instant::now() >= deadline {
                warn!("page actions hit the time budget; capturing current DOM");
                break;
            }
            // Bounded *during* the step too, not just before it: a click or a
            // `find_element` against a wedged Chrome never returns, and checking
            // the clock between steps cannot cut a step that never ends. Cut
            // steps are reported exactly like steps the deadline never reached —
            // absent from `outcomes`, so `attempted < requested` stays the
            // honest signal that the budget bit.
            let outcome =
                match tokio::time::timeout_at(deadline, run_action(page, action, deadline)).await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        warn!("a page action hit the time budget mid-step; capturing current DOM");
                        break;
                    }
                };
            outcomes.push(outcome);
        }
        outcomes
    })
}

/// Runs ONE top-level [`PageAction`], best-effort, under the shared `deadline`.
/// Split out of [`execute_actions`] so each step can be bounded as a whole.
async fn run_action(
    page: &chromiumoxide::Page,
    action: &PageAction,
    deadline: tokio::time::Instant,
) -> StepOutcome {
    match action {
        PageAction::ScrollBottom => {
            let ok = page
                .evaluate("window.scrollTo(0, document.body.scrollHeight)")
                .await
                .is_ok();
            interaction_outcome(true, ok)
        }
        PageAction::ScrollBy { pixels } => {
            let ok = page
                .evaluate(format!("window.scrollBy(0, {pixels})"))
                .await
                .is_ok();
            interaction_outcome(true, ok)
        }
        PageAction::Click { selector } => match page.find_element(selector).await {
            Ok(el) => {
                let clicked = el.click().await;
                if let Err(e) = &clicked {
                    warn!(selector = %selector, "page action click failed: {e}");
                }
                interaction_outcome(true, clicked.is_ok())
            }
            Err(_) => {
                warn!(selector = %selector, "page action click: selector not found");
                interaction_outcome(false, false)
            }
        },
        PageAction::Type { selector, text } => {
            if let Ok(el) = page.find_element(selector).await {
                let _ = el.click().await;
                let typed = el.type_str(text).await;
                if let Err(e) = &typed {
                    warn!(selector = %selector, "page action type failed: {e}");
                }
                interaction_outcome(true, typed.is_ok())
            } else {
                warn!(selector = %selector, "page action type: selector not found");
                interaction_outcome(false, false)
            }
        }
        PageAction::WaitForSelector {
            selector,
            timeout_ms,
        } => {
            let d = timeout_ms
                .map(|ms| tokio::time::Instant::now() + Duration::from_millis(ms))
                .unwrap_or(deadline)
                .min(deadline);
            // A selector that never appears is a MISS, not progress:
            // that is exactly the confirmation state a reviewer checks.
            interaction_outcome(wait_for_selector(page, selector, d).await, true)
        }
        PageAction::WaitMs { ms } => {
            // Raw caller-supplied milliseconds with no schema ceiling:
            // clamped to what is left, and the truncation is REPORTED
            // (`Partial`) rather than silently swallowed.
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let (granted, truncated) = clamp_wait_ms(*ms, Some(left));
            if granted > 0 {
                tokio::time::sleep(Duration::from_millis(granted)).await;
            }
            if truncated {
                warn!(
                    requested_ms = *ms,
                    granted_ms = granted,
                    "wait_ms truncated to fit the render budget"
                );
                StepOutcome::Partial
            } else {
                StepOutcome::Ok
            }
        }
        PageAction::Repeat {
            times,
            steps,
            until_selector_count_stable,
        } => {
            let mut last_count: Option<u64> = None;
            // Coarse by design: one outcome for the whole block. Only a
            // pass where every inner step ran AND succeeded keeps it Ok.
            let mut every_pass_clean = true;
            for _ in 0..*times {
                if tokio::time::Instant::now() >= deadline {
                    every_pass_clean = false;
                    break;
                }
                let inner = execute_actions(page, steps, deadline).await;
                if !pass_fully_succeeded(steps.len(), &inner) {
                    every_pass_clean = false;
                }
                // Stop early once the tracked selector's match count stops
                // growing — "scroll until no new rows load". This is a
                // success condition, not a failure.
                if let Some(sel) = until_selector_count_stable {
                    let count = count_matches(page, sel).await;
                    if last_count.is_some_and(|prev| count <= prev) {
                        break;
                    }
                    last_count = Some(count);
                }
            }
            if every_pass_clean {
                StepOutcome::Ok
            } else {
                StepOutcome::Partial
            }
        }
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
        assert!(BrowserEngine::new(&c, "data/profiles")
            .render_slots
            .is_none());
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
            std::path::Path::new("data/profiles")
                .join("acme")
                .join("browser")
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
        assert_eq!(
            evicted,
            vec!["p1".to_string()],
            "least-recently-used closed"
        );
        assert_eq!(order.len(), MAX_LIVE_PROFILES);
        assert!(
            order.contains(&"p0".to_string()),
            "recently used kept alive"
        );
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
    fn json_mime_detection_covers_the_real_shapes() {
        for yes in [
            "application/json",
            "application/json; charset=utf-8",
            "text/json",
            "application/vnd.api+json",
            "APPLICATION/JSON",
        ] {
            assert!(is_json_mime(yes), "{yes:?} is JSON");
        }
        for no in [
            "text/html",
            "application/javascript",
            "image/png",
            "",
            "json",
        ] {
            assert!(!is_json_mime(no), "{no:?} is not JSON");
        }
    }

    #[test]
    fn same_site_accepts_subdomains_and_rejects_cross_site() {
        assert!(same_site("example.com", "example.com"));
        assert!(same_site("www.example.com", "api.example.com"));
        assert!(same_site("example.com", "api.v2.example.com"));
        assert!(
            same_site("app.example.com", "example.com"),
            "page on subdomain, API on apex"
        );
        assert!(!same_site("example.com", "tracker.io"));
        assert!(
            !same_site("example.com", "notexample.com"),
            "suffix needs a dot boundary"
        );
        assert!(!same_site("", "example.com"));
        assert!(!same_site("example.com", ""));
    }

    #[test]
    fn capture_budget_caps_per_body_total_and_count() {
        // Under every cap => fits.
        assert!(capture_fits(1024, 0, 0));
        // Per-body cap is strict-over.
        assert!(capture_fits(CAPTURE_MAX_BODY_BYTES, 0, 0));
        assert!(!capture_fits(CAPTURE_MAX_BODY_BYTES + 1, 0, 0));
        // Total budget counts the incoming body.
        assert!(!capture_fits(1, CAPTURE_MAX_TOTAL_BYTES, 0));
        assert!(capture_fits(1, CAPTURE_MAX_TOTAL_BYTES - 1, 0));
        // Call-count ceiling.
        assert!(!capture_fits(1, 0, CAPTURE_MAX_CALLS));
    }

    #[test]
    fn html_cap_is_strict_and_disabled_by_zero() {
        assert!(!over_html_cap(100, 100), "exactly at the cap is allowed");
        assert!(!over_html_cap(99, 100));
        assert!(over_html_cap(101, 100), "strictly over fails");
        // 0 disables the cap regardless of size.
        assert!(!over_html_cap(u64::MAX, 0));
    }

    /// The dry-run evidence contract (M06 v1): `dry_run` is hard-coded true,
    /// the would-be submit action travels verbatim and UNEXECUTED into the
    /// bundle, filled fields decode from the evaluate result, and no screenshot
    /// path is claimed (the render path can't produce one yet).
    #[test]
    fn transact_evidence_is_dry_run_with_the_would_be_action_verbatim() {
        let req = flow_with_two_steps();
        let fill = req.fill_selectors();
        let ev = evidence_from_render(req, &fill, clean_page(), 0);
        assert!(ev.dry_run);
        assert_eq!(ev.idempotency_key, "signup-1");
        assert_eq!(ev.steps_completed, 2);
        assert_eq!(ev.filled_fields.len(), 1);
        assert_eq!(ev.filled_fields[0].value.as_deref(), Some("a@b.c"));
        assert!(
            matches!(&ev.would_submit, PageAction::Click { selector } if selector == "#submit"),
            "the irreversible action is reported, never executed"
        );
        assert!(
            ev.screenshot_path.is_none(),
            "no screenshot claim without capture support"
        );
        assert_eq!(ev.dom_html, "<form>...</form>");
        assert_eq!(ev.dom_bytes, "<form>...</form>".len());
        assert!(!ev.dom_truncated);
        assert_eq!(ev.profile.as_deref(), Some("portal_login"));
    }

    fn flow_with_two_steps() -> TransactRequest {
        serde_json::from_str(
            r##"{"url":"https://portal.example/signup",
                 "idempotency_key":"signup-1",
                 "profile":"portal_login",
                 "wait_for_selector":"#confirm",
                 "steps":[{"action":"type","selector":"#email","text":"a@b.c"},
                          {"action":"click","selector":"#next"}],
                 "submit_action":{"action":"click","selector":"#submit"}}"##,
        )
        .unwrap()
    }

    /// A render where both steps worked and the probe answered.
    fn clean_page() -> RenderedPage {
        RenderedPage {
            html: "<form>...</form>".into(),
            final_url: Some("https://portal.example/signup?step=confirm".into()),
            evaluated: Some(serde_json::json!({
                "fields": [{"selector": "#email", "value": "a@b.c", "found": true}],
                "submit_target": {"selector": "#submit", "found": true, "visible": true,
                                  "enabled": true, "tag": "button", "label": "Confirm"}
            })),
            selector_found: Some(true),
            actions_completed: 2,
            action_outcomes: vec![StepOutcome::Ok, StepOutcome::Ok],
            ..Default::default()
        }
    }

    /// The anti-pattern this direction exists to kill: a flow whose selectors
    /// all missed produced a bundle a reviewer could not tell from a clean run.
    #[test]
    fn failed_selectors_not_reported_as_completed_steps() {
        let req = flow_with_two_steps();
        let fill = req.fill_selectors();
        let page = RenderedPage {
            html: "<html>login wall</html>".into(),
            final_url: Some("https://portal.example/login".into()),
            evaluated: Some(serde_json::json!({
                "fields": [{"selector": "#email", "value": null, "found": false}],
                "submit_target": {"selector": "#submit", "found": false, "visible": null,
                                  "enabled": null, "tag": null, "label": null}
            })),
            selector_found: Some(false),
            actions_completed: 2,
            action_outcomes: vec![StepOutcome::SelectorMissing, StepOutcome::SelectorMissing],
            ..Default::default()
        };
        let bad = evidence_from_render(req, &fill, page, 0);
        let good = evidence_from_render(flow_with_two_steps(), &fill, clean_page(), 0);

        // Both asked for and attempted two steps; only one COMPLETED any.
        assert_eq!((bad.steps_requested, bad.steps_attempted), (2, 2));
        assert_eq!(bad.steps_completed, 0, "nothing succeeded");
        assert_eq!(good.steps_completed, 2);
        assert!(!bad.steps_deadline_hit, "the list ran, it just failed");
        assert_eq!(
            bad.step_outcomes,
            vec![StepOutcome::SelectorMissing; 2],
            "per-step outcomes carry WHY"
        );
        // The confirmation state and the submit target tell the runs apart...
        assert_eq!(bad.wait_for_selector_found, Some(false));
        assert_eq!(good.wait_for_selector_found, Some(true));
        assert_eq!(bad.submit_target.as_ref().unwrap().found, Some(false));
        assert_eq!(good.submit_target.as_ref().unwrap().found, Some(true));
        // ...while `would_submit` alone cannot: it is identical in both.
        assert_eq!(
            serde_json::to_value(&bad.would_submit).unwrap(),
            serde_json::to_value(&good.would_submit).unwrap()
        );
    }

    #[test]
    fn deadline_cut_steps_are_visible_as_attempted_below_requested() {
        let req = flow_with_two_steps();
        let fill = req.fill_selectors();
        let page = RenderedPage {
            html: "<form>...</form>".into(),
            actions_completed: 1,
            action_outcomes: vec![StepOutcome::Ok],
            ..Default::default()
        };
        let ev = evidence_from_render(req, &fill, page, 0);
        assert_eq!((ev.steps_requested, ev.steps_attempted), (2, 1));
        assert!(ev.steps_deadline_hit, "step 2 never ran");
        // The probe never answered, so the target is "unknown", not "missing".
        assert_eq!(ev.submit_target.expect("selector exists").found, None);
    }

    /// The anti-pattern: an over-cap DOM failed the whole job AFTER the flow had
    /// already navigated, filled and clicked — destroying every trace of what a
    /// live page was just made to do. The transact path truncates and flags.
    #[test]
    fn over_cap_dom_truncated_not_evidence_destroyed() {
        let req = flow_with_two_steps();
        let fill = req.fill_selectors();
        let mut page = clean_page();
        page.html = "abcdefghij".repeat(10); // 100 bytes
        let ev = evidence_from_render(req, &fill, page, 40);
        assert!(ev.dom_truncated);
        assert_eq!(ev.dom_bytes, 100, "the CAPTURED size is reported in full");
        assert_eq!(ev.dom_html.len(), 40, "the stored snapshot is the prefix");
        // The rest of the bundle survived intact — that is the whole point.
        assert_eq!(ev.steps_completed, 2);
        assert_eq!(ev.submit_target.expect("assessed").found, Some(true));
    }

    /// The anti-pattern, at the engine seam: a typo'd profile went straight to
    /// `create_dir_all` inside `acquire`, silently birthing an empty, logged-OUT
    /// Chrome profile — the flow then ran against a login wall. The refusal must
    /// happen BEFORE any Chrome is launched or any directory is created, so this
    /// test needs no browser at all.
    #[tokio::test]
    async fn missing_profile_not_silently_created_by_a_flow() {
        // A vault root we own by name and clear first, so "the dir is not there
        // afterwards" is a statement about THIS run.
        let vault = std::env::temp_dir().join("pumper-transact-missing-profile-vault");
        let _ = std::fs::remove_dir_all(&vault);
        let engine = BrowserEngine::new(&cfg(), &vault);

        let err = engine
            .transact(flow_with_two_steps()) // profile: "portal_login"
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Transact(_)), "got {err:?}");
        assert!(
            err.to_string().contains("portal_login"),
            "the error names the profile: {err}"
        );
        assert!(
            !vault.join("portal_login").exists(),
            "the refusal must not create the profile it refused — and it must \
             happen before any Chrome is launched, which is why this test needs none"
        );
    }

    // ── the render budget ────────────────────────────────────────────────────

    /// THE anti-pattern: `extra_wait_ms` and the `wait_ms` action are raw `u64`
    /// milliseconds with no clamp and no schema `maximum`, so one caller could
    /// park a render — and one of only four concurrency slots — for 49 days.
    /// Four of them wedge the browser tier for every app on the box.
    #[test]
    fn pathological_wait_not_allowed_to_outlive_the_render_budget() {
        let budget = Duration::from_secs(180);
        let (granted, truncated) = clamp_wait_ms(u64::MAX, Some(budget));
        assert!(truncated, "a 49-day wait must be reported as cut");
        assert_eq!(
            granted,
            (budget - CAPTURE_RESERVE).as_millis() as u64,
            "a clamped wait leaves CAPTURE_RESERVE to actually capture the DOM"
        );
        assert!(
            Duration::from_millis(granted) < budget,
            "the wait alone can never consume the whole budget"
        );
    }

    #[test]
    fn a_wait_that_fits_is_untouched_and_a_disabled_budget_clamps_nothing() {
        // Comfortably inside the budget: exactly what was asked for.
        assert_eq!(
            clamp_wait_ms(1_000, Some(Duration::from_secs(180))),
            (1_000, false)
        );
        // Right at the reserve boundary: still not truncated.
        assert_eq!(
            clamp_wait_ms(5_000, Some(Duration::from_secs(10))),
            (5_000, false)
        );
        assert_eq!(
            clamp_wait_ms(5_001, Some(Duration::from_secs(10))),
            (5_000, true)
        );
        // Budget spent: the wait is skipped entirely rather than the render dying.
        assert_eq!(clamp_wait_ms(30_000, Some(Duration::ZERO)), (0, true));
        // `render_budget_secs = 0` disables the budget: no clamp at all.
        assert_eq!(clamp_wait_ms(u64::MAX, None), (u64::MAX, false));
    }

    /// A stage's own cap and the render budget are both ceilings; the binding
    /// one wins. Before this, `nav_timeout_secs` was applied from *now* at each
    /// of three stages, so three stages could each take a full nav timeout.
    #[test]
    fn a_stage_never_outlives_the_budget_that_contains_it() {
        let now = tokio::time::Instant::now();
        let budget = budget_deadline(now, 180).expect("a positive budget exists");
        // Early in the render the stage cap binds...
        assert_eq!(
            stage_deadline(now, Duration::from_secs(30), Some(budget)),
            now + Duration::from_secs(30)
        );
        // ...late in it, the budget does.
        let late = now + Duration::from_secs(170);
        assert_eq!(
            stage_deadline(late, Duration::from_secs(30), Some(budget)),
            budget
        );
        // Disabled budget: the stage keeps exactly its own cap.
        assert_eq!(
            stage_deadline(late, Duration::from_secs(30), None),
            late + Duration::from_secs(30)
        );
        assert!(budget_deadline(now, 0).is_none(), "0 disables the budget");
    }

    /// A budget failure an operator can act on: it names the budget, the key
    /// that moves it, the stage that ran out and the URL — not a bare "browser
    /// engine: deadline has elapsed".
    #[test]
    fn budget_exhaustion_names_the_budget_not_a_generic_timeout() {
        let err = budget_exhausted("capturing the DOM", "https://slow.example/x", 180);
        let msg = err.to_string();
        assert!(msg.contains("render_budget_secs"), "{msg}");
        assert!(msg.contains("180s"), "{msg}");
        assert!(msg.contains("capturing the DOM"), "{msg}");
        assert!(msg.contains("https://slow.example/x"), "{msg}");
        assert!(
            !err.is_terminal_for_job(),
            "a slow page may be fast next attempt — budget exhaustion stays retryable"
        );
    }

    /// The budget must bound a wait that would otherwise never end. Uses tokio's
    /// paused clock, so this proves the wiring in zero real milliseconds.
    #[tokio::test(start_paused = true)]
    async fn a_never_ending_await_is_cut_by_the_budget_not_left_to_wedge_the_tier() {
        let budget = RenderBudget::start(180);
        let forever = async {
            tokio::time::sleep(Duration::from_secs(86_400)).await;
        };
        let err = budget
            .require("navigating", "https://wedged.example/", forever)
            .await
            .expect_err("an unbounded await must not outlive the budget");
        assert!(err.to_string().contains("render budget exhausted"), "{err}");

        // Best-effort waits report the same fact as `None` instead of failing.
        let budget = RenderBudget::start(180);
        assert!(budget
            .attempt(async {
                tokio::time::sleep(Duration::from_secs(86_400)).await;
            })
            .await
            .is_none());
    }

    /// The escape hatch stays honest: `render_budget_secs = 0` disables the
    /// budget, and then nothing is bounded by it (this is the only way to get
    /// the old, unbounded behaviour back).
    #[tokio::test(start_paused = true)]
    async fn a_disabled_budget_bounds_nothing() {
        let budget = RenderBudget::start(0);
        assert!(budget.remaining().is_none());
        assert_eq!(
            budget
                .require("navigating", "https://x.example/", async {
                    tokio::time::sleep(Duration::from_secs(86_400)).await;
                    7u8
                })
                .await
                .expect("no budget, no cut"),
            7
        );
    }

    // ── render cleanup (RAII) ────────────────────────────────────────────────

    /// A closable that reports every close, so "exactly once" is a fact rather
    /// than an inspection of the code.
    struct RecordingTab(tokio::sync::mpsc::UnboundedSender<&'static str>);

    #[async_trait]
    impl Closable for RecordingTab {
        async fn close(&self) {
            let _ = self.0.send("tab-closed");
        }
    }

    /// Sends when the task's future is **dropped**, which the runtime does only
    /// when the task is aborted or finishes. A detached (merely forgotten) task
    /// parked on an await keeps its future alive, so silence here IS the leak.
    ///
    /// Constructed outside the `async` block and moved in, so the signal does
    /// not depend on the task having been polled first — the sequence a
    /// cancelled render produces (spawn, then abort before the next poll) is the
    /// one that must be provable.
    struct SignalOnDrop(tokio::sync::mpsc::UnboundedSender<&'static str>);

    impl Drop for SignalOnDrop {
        fn drop(&mut self) {
            let _ = self.0.send("task-dropped");
        }
    }

    /// A task that never finishes on its own, exactly like the CDP drainer and
    /// the capture loop: they end when their event stream ends (i.e. when the
    /// tab dies) or when someone aborts them.
    fn parked_task(
        tx: tokio::sync::mpsc::UnboundedSender<&'static str>,
    ) -> tokio::task::JoinHandle<()> {
        let signal = SignalOnDrop(tx);
        tokio::spawn(async move {
            let _signal = signal;
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        })
    }

    /// THE leak this guard exists to kill. Every job cancel (`DELETE
    /// /jobs/{id}`) and every job timeout lands mid-render: the worker `break`s
    /// out of its `select!`, **dropping** the render future. A dropped future
    /// runs none of the cleanup that used to live on the success and goto-error
    /// paths — and dropping a `JoinHandle` *detaches* its task instead of
    /// aborting it — so the tab and its one or two CDP tasks survived the render
    /// that owned them, invisibly, until Chrome was recycled 200 renders later.
    #[tokio::test]
    async fn dropped_render_not_left_as_a_zombie_tab_with_detached_tasks() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        {
            let mut scope = RenderScope::new(Arc::new(RecordingTab(tx.clone())));
            scope.watch(parked_task(tx.clone()));
            scope.watch(parked_task(tx.clone()));
            // Let both tasks actually start servicing their "tab", the state a
            // real render is in when the worker's select! fires.
            tokio::task::yield_now().await;
            // Nothing is released explicitly: this models the worker dropping
            // the pinned future mid-render.
        }

        // Both tasks were aborted (they unpark and drop their locals), and the
        // tab was closed by the detached drop task.
        let mut seen: Vec<&'static str> = Vec::new();
        for _ in 0..3 {
            seen.push(rx.recv().await.expect("cleanup must report"));
        }
        seen.sort_unstable();
        assert_eq!(
            seen,
            vec!["tab-closed", "task-dropped", "task-dropped"],
            "a dropped render must abort BOTH tasks and close its tab"
        );

        // ...and exactly once. Give any stray second close a chance to arrive.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            rx.try_recv().is_err(),
            "cleanup ran twice; the close-once latch is not latching"
        );
    }

    /// The mirror risk: the success path still closes at its own point, so the
    /// guard must not close a second time when it drops a moment later. (Chrome
    /// answers a second `Page.close` with an error, which the old code would
    /// have logged as a scary-looking failure on every single render.)
    #[tokio::test]
    async fn released_render_not_closed_again_when_the_scope_drops() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        {
            let mut scope = RenderScope::new(Arc::new(RecordingTab(tx.clone())));
            scope.watch(parked_task(tx.clone()));
            tokio::task::yield_now().await;
            scope.release().await;
            let mut seen = vec![
                rx.recv().await.expect("release closes"),
                rx.recv().await.expect("release aborts too"),
            ];
            seen.sort_unstable();
            assert_eq!(seen, vec!["tab-closed", "task-dropped"]);
        }
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(rx.try_recv().is_err(), "the tab was closed twice");
    }

    #[test]
    fn truncate_to_cap_respects_char_boundaries_and_a_zero_cap() {
        // Under/at the cap: untouched.
        assert_eq!(truncate_to_cap("abc".into(), 10), ("abc".into(), false));
        assert_eq!(truncate_to_cap("abc".into(), 3), ("abc".into(), false));
        // 0 disables the cap (mirrors `over_html_cap`).
        assert_eq!(truncate_to_cap("abc".into(), 0), ("abc".into(), false));
        // Over the cap: cut, flagged.
        assert_eq!(truncate_to_cap("abcdef".into(), 3), ("abc".into(), true));
        // A cut that lands mid-codepoint walks back to a boundary rather than
        // panicking (String::truncate would).
        let (out, cut) = truncate_to_cap("aé".into(), 2); // 'é' is 2 bytes
        assert!(cut);
        assert_eq!(out, "a");
        let (out, cut) = truncate_to_cap("é".into(), 1);
        assert!(cut);
        assert_eq!(out, "", "no valid prefix => empty, still flagged");
    }
}
