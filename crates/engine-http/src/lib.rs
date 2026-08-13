//! Traditional HTTP scraping engine: reqwest with a cookie jar, browser-like
//! User-Agent, and retries with exponential backoff. Fronted by a
//! content-addressed TTL cache and a per-domain politeness governor.
//!
//! ## Clients, proxies and session profiles
//!
//! reqwest binds both its proxy and its cookie jar at **client-build** time, so
//! a request that overrides either needs its own client. One bounded LRU pool
//! ([`ClientPool`]) serves both dimensions: it is keyed by the
//! `(proxy, profile)` pair the client was built with.
//!
//! A `profile` (session vault, phase 1) swaps reqwest's in-memory jar — which
//! dies with the process — for a [`ProfileJar`]: a serializable jar loaded from
//! and written back to `<profiles_dir>/<name>/cookies.json`, so a logged-in
//! session survives a restart. See `docs/features/fetching.md`.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cookie_store::{CookieStore, RawCookie};
use pumper_core::config::HttpConfig;
use pumper_core::{
    profile_cookies_path, require_safe_profile_name, Error, Governor, HttpCache, HttpClient,
    HttpMethod, HttpRequest, HttpResponse, Result,
};
use reqwest::header::HeaderValue;
use tracing::{debug, warn};

/// Base of the exponential retry backoff (attempt 1 waits this, then doubles).
const RETRY_BASE_MS: u64 = 500;
/// Retry jitter is up to this fraction of the (post-max) delay, spread with a
/// deterministic hash — no `rand` dependency (mirrors the governor's approach).
const RETRY_JITTER_FRAC: f64 = 0.25;
/// Max distinct pooled clients (LRU). A client is built per `(proxy, profile)`
/// pair, so this bounds the combined fan-out of per-request proxy overrides and
/// session profiles. Cost: up to this many idle keep-alive pools may linger.
/// Evicting a client never loses cookies — the profile's [`ProfileJar`] is owned
/// by the engine's jar map, not by the client.
const MAX_POOLED_CLIENTS: usize = 8;
/// Debounce for writing a profile's cookie jar back to disk. Cookies set by a
/// response are flushed at most this long afterwards (trailing-edge: the last
/// response in a burst is always written). Crash-loss window: a hard kill within
/// this window of a Set-Cookie loses that cookie on disk (it was still applied
/// in-process). One write per profile per window bounds the write rate under a
/// profiled crawl.
const COOKIE_FLUSH_DEBOUNCE: Duration = Duration::from_secs(1);
/// The smallest slice of an end-to-end budget worth starting an attempt with.
/// Below this a request cannot realistically connect, transfer and be read, so
/// sleeping the last of the budget to reach it only delays the same failure by
/// the length of the sleep.
const MIN_ATTEMPT_BUDGET: Duration = Duration::from_secs(1);

/// A persistent, serializable cookie jar for one named profile. Installed as
/// reqwest's `cookie_provider`, so reqwest reads/writes it exactly like its own
/// in-memory jar — but it is loaded from disk on first use and written back
/// (atomically: tmp file + rename) on a trailing debounce after responses.
///
/// Persisted with cookie_store's JSON format **including session (non-persistent)
/// cookies** — a login that sets only a session cookie is the whole point of the
/// vault — while expired cookies are dropped at load time.
pub(crate) struct ProfileJar {
    name: String,
    path: PathBuf,
    /// std `Mutex`: reqwest's `CookieStore` trait methods are sync and the
    /// critical sections (match cookies / store Set-Cookie / serialize) never
    /// await.
    store: Mutex<CookieStore>,
    /// Set when a response may have changed the jar; cleared by the flusher.
    dirty: AtomicBool,
    /// Whether a flusher task is currently armed (at most one per jar).
    flushing: AtomicBool,
}

impl ProfileJar {
    /// Loads `<profiles_dir>/<name>/cookies.json`. A missing file starts an
    /// empty jar **with a warning**; an unreadable/corrupt one is warned about
    /// and also starts empty (a bad jar must not wedge fetches).
    ///
    /// It deliberately does **not** create the profile directory. It used to,
    /// before the open — so a typo'd `profile: "acme_portl"` *materialised*
    /// `data/profiles/acme_portl/` and the typo then appeared in
    /// `GET /profiles` as a real, indistinguishable profile. The directory is
    /// created by the first [`ProfileJar::save`] that actually has a cookie to
    /// write, which is the first moment the profile is real.
    fn load(name: &str, path: PathBuf) -> Result<Self> {
        let store = match std::fs::File::open(&path) {
            Ok(file) => cookie_store::serde::json::load(BufReader::new(file)).unwrap_or_else(|e| {
                warn!(profile = %name, "cookie jar {} unreadable ({e}); starting empty", path.display());
                CookieStore::default()
            }),
            // NotFound covers both a missing jar and a missing profile dir.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // The signal this seam had none of. A fetch under a profile with
                // no stored session is not an error — it is how a login is
                // established on this tier — but it is also exactly what a typo
                // looks like, and it used to be completely silent.
                warn!(
                    profile = %name,
                    jar = %path.display(),
                    "session profile has no stored cookies — requests under it go out \
                     ANONYMOUS until a response sets one (a mistyped profile name looks \
                     exactly like this)"
                );
                CookieStore::default()
            }
            Err(e) => {
                return Err(Error::Profile(format!("opening {}: {e}", path.display())));
            }
        };
        Ok(Self {
            name: name.to_string(),
            path,
            store: Mutex::new(store),
            dirty: AtomicBool::new(false),
            flushing: AtomicBool::new(false),
        })
    }

    /// How many cookies the in-memory jar currently holds (expired and session
    /// ones included — the persisted set).
    fn cookie_count(&self) -> usize {
        self.store
            .lock()
            .expect("cookie jar mutex poisoned")
            .iter_any()
            .count()
    }

    /// Whether the jar would send nothing: a profiled request made while this is
    /// true goes out **anonymous**, whatever the profile is called.
    fn is_empty(&self) -> bool {
        self.cookie_count() == 0
    }

    /// Serializes the jar and replaces the file atomically (write tmp + rename),
    /// so a crash mid-write can never leave a truncated jar behind. Creates the
    /// profile directory on the first write that actually has something to
    /// persist — see [`ProfileJar::load`].
    fn save(&self) -> Result<()> {
        match save_decision(self.cookie_count(), self.path.exists()) {
            SaveDecision::Write => {}
            SaveDecision::NothingToPersist => {
                debug!(profile = %self.name, "cookie jar is empty; nothing to write");
                return Ok(());
            }
            SaveDecision::WouldClobber => {
                warn!(
                    profile = %self.name,
                    jar = %self.path.display(),
                    "refusing to overwrite a stored cookie jar with an empty one — \
                     the in-memory jar holds no cookies, so this write could only \
                     destroy the session on disk"
                );
                return Ok(());
            }
        }
        let mut buf: Vec<u8> = Vec::new();
        {
            let store = self.store.lock().expect("cookie jar mutex poisoned");
            // `_incl_expired_and_nonpersistent` keeps **session** cookies, which
            // is exactly what a login profile needs; `load` drops expired ones.
            cookie_store::serde::json::save_incl_expired_and_nonpersistent(&store, &mut buf)
                .map_err(|e| {
                    Error::Profile(format!("serializing jar for profile '{}': {e}", self.name))
                })?;
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, &self.path)?;
        debug!(profile = %self.name, "cookie jar saved");
        Ok(())
    }

    /// Marks the jar dirty after a response and arms the (single) flusher task.
    fn touch(self: &Arc<Self>) {
        self.dirty.store(true, Ordering::SeqCst);
        if self.flushing.swap(true, Ordering::SeqCst) {
            return; // a flusher is already armed; it will pick this up.
        }
        let jar = self.clone();
        tokio::spawn(jar.flush_loop());
    }

    /// Write-behind loop: sleeps the debounce, writes if dirty, and retires once
    /// the jar is clean. The re-arm check closes the race where a `touch` lands
    /// between the clean observation and retiring the flag.
    ///
    /// A **failed** write re-arms `dirty` so the next cycle tries again. It used
    /// to clear the flag *before* saving and drop the error on the floor, so one
    /// transient failure — a Windows sharing violation while an antivirus or a
    /// backup holds the file, the exact case `Error::is_terminal_for_job`
    /// documents as the reason `Error::Profile` stays retryable — silently threw
    /// the login away: the user stayed logged in for the life of the process and
    /// was logged out by the restart, with one WARN as the only evidence.
    async fn flush_loop(self: Arc<Self>) {
        let mut consecutive_failures: u32 = 0;
        loop {
            tokio::time::sleep(COOKIE_FLUSH_DEBOUNCE).await;
            if self.dirty.swap(false, Ordering::SeqCst) {
                match self.save() {
                    Ok(()) => consecutive_failures = 0,
                    Err(e) => {
                        consecutive_failures += 1;
                        if should_retry_save(consecutive_failures) {
                            // The write did NOT happen, so the jar is still
                            // dirty. Put the flag back rather than pretending.
                            self.dirty.store(true, Ordering::SeqCst);
                            warn!(
                                profile = %self.name,
                                attempt = consecutive_failures,
                                "saving cookie jar failed ({e}); retrying on the next flush"
                            );
                        } else {
                            warn!(
                                profile = %self.name,
                                attempts = consecutive_failures,
                                "saving cookie jar failed ({e}) and will not be retried — \
                                 cookies set in this process will NOT survive a restart"
                            );
                        }
                    }
                }
                continue;
            }
            self.flushing.store(false, Ordering::SeqCst);
            if self.dirty.load(Ordering::SeqCst) && !self.flushing.swap(true, Ordering::SeqCst) {
                continue; // a touch raced in and saw `flushing`; keep going.
            }
            return;
        }
    }
}

impl reqwest::cookie::CookieStore for ProfileJar {
    fn set_cookies(
        &self,
        cookie_headers: &mut dyn Iterator<Item = &HeaderValue>,
        url: &reqwest::Url,
    ) {
        let cookies = cookie_headers.filter_map(|value| {
            std::str::from_utf8(value.as_bytes())
                .ok()
                .and_then(|raw| RawCookie::parse(raw.to_owned()).ok())
        });
        let mut store = self.store.lock().expect("cookie jar mutex poisoned");
        store.store_response_cookies(cookies, url);
    }

    fn cookies(&self, url: &reqwest::Url) -> Option<HeaderValue> {
        let store = self.store.lock().expect("cookie jar mutex poisoned");
        let header = store
            .get_request_values(url)
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");
        if header.is_empty() {
            return None;
        }
        HeaderValue::from_str(&header).ok()
    }
}

/// How many consecutive failed jar writes are retried before the flusher gives
/// up. Bounded so a permanently unwritable path (a read-only volume, a deleted
/// `profiles_dir`) cannot turn into a warn-per-second forever; five debounce
/// cycles is ~5 s, comfortably past the sharing-violation window this exists for.
const MAX_SAVE_RETRIES: u32 = 5;

/// Whether a failed jar write should stay pending for another debounce cycle.
fn should_retry_save(consecutive_failures: u32) -> bool {
    consecutive_failures < MAX_SAVE_RETRIES
}

/// What [`ProfileJar::save`] should do with the jar it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SaveDecision {
    /// Persist it.
    Write,
    /// Empty jar, nothing on disk: writing would only create a profile
    /// directory for a session that does not exist — which is how a typo'd
    /// profile name used to appear in `GET /profiles` as a real profile.
    NothingToPersist,
    /// Empty jar, a real one on disk: writing would **destroy** it.
    WouldClobber,
}

/// The save gate. Refuses to replace a stored jar with an empty in-memory one.
///
/// The sequence it defends against: the server starts while `cookies.json` is
/// missing (so the in-memory jar is empty), an operator restores the file from
/// backup, and the next profiled response `touch`es the jar — whose cached
/// `Arc` never re-reads disk — so the debounced flush renames an empty jar over
/// the restored session and logs `cookie jar saved`.
///
/// The cost of the rule, stated honestly: a genuine **logout** (the site expires
/// its own cookie, emptying the jar) no longer erases the stored jar, so a dead
/// cookie survives on disk until the next login overwrites it. That is the
/// cheaper failure — the site rejects a dead cookie and the profile re-logs in,
/// whereas a clobbered session has no recovery at all.
fn save_decision(cookies_in_memory: usize, jar_on_disk: bool) -> SaveDecision {
    match (cookies_in_memory, jar_on_disk) {
        (0, true) => SaveDecision::WouldClobber,
        (0, false) => SaveDecision::NothingToPersist,
        _ => SaveDecision::Write,
    }
}

/// Pool key: a client is uniquely determined by what it was **built** with — its
/// proxy and its cookie jar (profile). The unit separator can appear in neither,
/// so the two fields can never collide.
fn pool_key(proxy: Option<&str>, profile: Option<&str>) -> String {
    format!("{}\u{1f}{}", proxy.unwrap_or(""), profile.unwrap_or(""))
}

/// Small LRU pool of clients keyed by [`pool_key`]. Guarded by a std `Mutex`:
/// the critical section (lookup / build / insert) is fully sync — building a
/// reqwest client does not await — so no async lock is needed.
struct ClientPool {
    clients: HashMap<String, reqwest::Client>,
    /// Front = least-recently-used, back = most-recent. Bounded by MAX_POOLED_CLIENTS.
    order: VecDeque<String>,
}

impl ClientPool {
    fn new() -> Self {
        Self {
            clients: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// LRU lookup: returns a cached client for `key`, touching it as most-recent.
    fn get(&mut self, key: &str) -> Option<reqwest::Client> {
        let client = self.clients.get(key).cloned()?;
        pumper_core::lru_touch(&mut self.order, key);
        Some(client)
    }

    /// Insert a freshly built client as most-recent, evicting the least-recently
    /// used entries until the pool is within `cap`.
    fn insert(&mut self, key: &str, client: reqwest::Client, cap: usize) {
        self.clients.insert(key.to_string(), client);
        for evict in pumper_core::lru_touch_evict(&mut self.order, key, cap) {
            self.clients.remove(&evict);
        }
    }
}

pub struct HttpEngine {
    /// Client for profile-less requests with no per-request proxy override
    /// (carries `[http] proxy` when configured, and reqwest's in-memory jar).
    client: reqwest::Client,
    /// Kept to rebuild pooled clients on demand.
    cfg: HttpConfig,
    /// Root of the session vault (`[fetcher] profiles_dir`).
    profiles_dir: PathBuf,
    governor: Arc<Governor>,
    cache: Arc<HttpCache>,
    /// LRU pool of clients keyed by `(proxy, profile)`.
    clients: Mutex<ClientPool>,
    /// One jar per profile, keyed by name. Deliberately **not** LRU-evicted: a
    /// jar holds the live copy of a profile's cookies, so dropping it when its
    /// client is evicted could lose cookies set since the last flush. Jars are
    /// a few KB each and only exist for profiles this process actually used.
    jars: Mutex<HashMap<String, Arc<ProfileJar>>>,
}

/// Builds a reqwest client mirroring the base settings, optionally proxied and
/// optionally bound to a profile's persistent cookie jar.
fn build_client(
    cfg: &HttpConfig,
    proxy: Option<&str>,
    jar: Option<Arc<ProfileJar>>,
) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .user_agent(&cfg.user_agent)
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .gzip(true)
        .brotli(true)
        .redirect(reqwest::redirect::Policy::limited(cfg.redirect_limit));
    builder = match jar {
        // Profiled: a persistent jar, shared by every client of this profile.
        Some(jar) => builder.cookie_provider(jar),
        // Default: reqwest's own in-memory jar (dies with the process).
        None => builder.cookie_store(true),
    };
    if let Some(url) = proxy {
        // `Proxy::all` covers http/https/socks5 and honors `user:pass@` auth in
        // the URL. socks5 support comes from reqwest's `socks` feature.
        let p = reqwest::Proxy::all(url)
            .map_err(|e| Error::Http(format!("invalid proxy '{url}': {e}")))?;
        builder = builder.proxy(p);
    }
    builder.build().map_err(|e| Error::Http(e.to_string()))
}

impl HttpEngine {
    pub fn new(
        cfg: &HttpConfig,
        governor: Arc<Governor>,
        cache: Arc<HttpCache>,
        profiles_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        let client = build_client(cfg, cfg.proxy.as_deref(), None)?;
        Ok(Self {
            client,
            cfg: cfg.clone(),
            profiles_dir: profiles_dir.into(),
            governor,
            cache,
            clients: Mutex::new(ClientPool::new()),
            jars: Mutex::new(HashMap::new()),
        })
    }

    /// The persistent jar for `name`, loading it from disk (and creating the
    /// profile dir) on first use. Validates the name — a bad one is a typed
    /// [`Error::BadRequest`] and never touches the filesystem.
    fn jar_for(&self, name: &str) -> Result<Arc<ProfileJar>> {
        // The name check runs BEFORE the cache lookup, and as the terminal
        // `Error::BadRequest` rather than the retryable `Error::Profile`.
        //
        // The anti-pattern, third seam: a profile name is frozen into the job
        // row at enqueue, so a typo'd one re-refuses identically on every
        // attempt — the ladder buys nothing and bills for it. `render` and
        // `transact` were retyped this round; this seam was left pinned as a
        // known gap in the conformance battery, and this closes the class.
        // Before the cache lookup because a cached entry must not be able to
        // launder a name this rule would reject.
        require_safe_profile_name(name)?;
        let mut jars = self.jars.lock().expect("jar map mutex poisoned");
        if let Some(jar) = jars.get(name) {
            return Ok(jar.clone());
        }
        let path = profile_cookies_path(&self.profiles_dir, name)?;
        let jar = Arc::new(ProfileJar::load(name, path)?);
        debug!(profile = %name, "opened session profile");
        jars.insert(name.to_string(), jar.clone());
        Ok(jar)
    }

    /// Selects the client for a request. The base client serves the common case
    /// (no profile, no proxy override beyond the configured one); anything that
    /// changes what the client is *built* with — a proxy override, a profile, or
    /// both — comes from the LRU pool keyed by that pair. Returns the profile's
    /// jar alongside so the caller can flush it after a response.
    fn client_for(&self, req: &HttpRequest) -> Result<(reqwest::Client, Option<Arc<ProfileJar>>)> {
        // Effective proxy: the per-request override, else the configured one.
        let proxy = req.proxy.as_deref().or(self.cfg.proxy.as_deref());
        let jar = match req.profile.as_deref() {
            Some(name) => Some(self.jar_for(name)?),
            None => {
                // No profile: if the effective proxy is the configured one, the
                // base client already carries it — reuse it (and its jar) rather
                // than pooling a duplicate.
                if proxy == self.cfg.proxy.as_deref() {
                    return Ok((self.client.clone(), None));
                }
                None
            }
        };
        let key = pool_key(proxy, req.profile.as_deref());
        let mut pool = self.clients.lock().expect("client pool mutex poisoned");
        if let Some(existing) = pool.get(&key) {
            return Ok((existing, jar));
        }
        let client = build_client(&self.cfg, proxy, jar.clone())?;
        pool.insert(&key, client.clone(), MAX_POOLED_CLIENTS);
        Ok((client, jar))
    }

    /// Builds one attempt. `attempt_timeout` is always explicit — it is the
    /// per-attempt timeout ([`HttpRequest::timeout_secs`] else `[http]
    /// timeout_secs`) already clamped to what is left of the fetch's end-to-end
    /// budget, so no single attempt can outlive the deadline.
    fn build(
        &self,
        client: &reqwest::Client,
        req: &HttpRequest,
        attempt_timeout: Duration,
    ) -> reqwest::RequestBuilder {
        let mut builder = match req.method {
            HttpMethod::Get => client.get(&req.url),
            HttpMethod::Post => client.post(&req.url),
        };
        builder = builder.timeout(attempt_timeout);
        for (key, value) in &req.headers {
            builder = builder.header(key, value);
        }
        // Conditional GET validators for incremental recrawl. Explicit headers in
        // `req.headers` win (inserted first, above) — these only add the standard
        // revalidation headers when the caller supplied a stored validator.
        if let Some(etag) = &req.etag {
            builder = builder.header("if-none-match", etag);
        }
        if let Some(since) = &req.if_modified_since {
            builder = builder.header("if-modified-since", since);
        }
        if let Some(body) = &req.body {
            builder = builder.body(body.clone());
        }
        builder
    }

    /// Only idempotent, bodyless GETs are cacheable — and never a **profiled**
    /// request: the shared `http_cache` is keyed by method+url+body only, so
    /// caching a logged-in body would serve it to anonymous callers (and vice
    /// versa). Profiled fetches always hit the network.
    fn cacheable(req: &HttpRequest) -> bool {
        req.method == HttpMethod::Get
            && req.body.is_none()
            && !req.no_cache
            && req.profile.is_none()
    }

    async fn send(&self, req: &HttpRequest) -> Result<HttpResponse> {
        let host = reqwest::Url::parse(&req.url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned));
        let (client, jar) = self.client_for(req)?;
        let retries = self.cfg.retries;
        let cap = req.max_body_bytes.unwrap_or(self.cfg.max_body_bytes);

        // ONE clock for the whole fetch, started before the first attempt: the
        // retry loop used to bound only each attempt, so `timeout_secs` was
        // multiplied by `retries + 1` and then had every backoff sleep added on
        // top. See `HttpConfig::total_budget_secs`.
        let per_attempt = per_attempt_timeout(req.timeout_secs, self.cfg.timeout_secs);
        let budget = fetch_budget(self.cfg.total_budget_secs, per_attempt);
        let started = Instant::now();
        let deadline = budget.map(|b| started + b);

        let mut last_error = String::new();
        // Retry-After from the previous retryable response, so the next sleep can
        // honor the server's requested delay instead of a blind doubling.
        let mut last_retry_after: Option<Duration> = None;
        for attempt in 0..=retries {
            if attempt > 0 {
                let seed = jitter_seed(&req.url, attempt);
                let delay = retry_delay(attempt, last_retry_after, RETRY_BASE_MS, seed);
                let Some(delay) = capped_retry_sleep(delay, remaining(deadline)) else {
                    return Err(budget_exhausted(
                        &req.url,
                        started.elapsed(),
                        attempt,
                        budget,
                        &last_error,
                    ));
                };
                debug!(url = %req.url, attempt, "retrying in {delay:?} ({last_error})");
                tokio::time::sleep(delay).await;
            }
            // Politeness spacing is applied per attempt so retries also wait.
            // Deliberately NOT bounded by the budget: shortening a governor wait
            // would trade politeness for latency, which is not this deadline's
            // job. A wait that eats the budget is caught by the check below.
            if let Some(host) = &host {
                self.governor.acquire(host).await;
            }
            let Some(attempt_timeout) = attempt_timeout(per_attempt, remaining(deadline)) else {
                return Err(budget_exhausted(
                    &req.url,
                    started.elapsed(),
                    attempt,
                    budget,
                    &last_error,
                ));
            };
            // Captured BEFORE the request goes out: a login response's own
            // Set-Cookie is applied to the jar by reqwest during `send`, which
            // would otherwise mask the fact that THIS request carried nothing.
            let sent_anonymous = jar.as_ref().is_some_and(|j| j.is_empty());
            match self.build(&client, req, attempt_timeout).send().await {
                Ok(response) => {
                    // reqwest has already applied any Set-Cookie (including on
                    // redirect hops) to the profile's jar by now — schedule the
                    // debounced write-back, whatever the status.
                    if let Some(jar) = &jar {
                        jar.touch();
                    }
                    let status = response.status().as_u16();
                    // Adaptive politeness: rate-limit/overload responses teach
                    // the governor a longer per-host spacing; only a genuinely
                    // healthy (2xx) response decays a learned penalty. A 4xx
                    // (e.g. 404/403) is NOT health — it must not reward the host
                    // with faster spacing — and other 5xx are left neutral.
                    let ra = retry_after(&response);
                    if let Some(host) = &host {
                        if matches!(status, 429 | 503) {
                            self.governor.penalize(host, ra).await;
                        } else if (200..300).contains(&status) {
                            self.governor.reward(host).await;
                        }
                    }
                    if self.cfg.retryable_statuses.contains(&status) && attempt < retries {
                        warn!(url = %req.url, status, "retryable status");
                        last_error = format!("status {status}");
                        last_retry_after = ra;
                        continue;
                    }
                    let final_url = response.url().to_string();
                    let mut headers = response
                        .headers()
                        .iter()
                        .map(|(k, v)| {
                            (
                                k.to_string(),
                                String::from_utf8_lossy(v.as_bytes()).into_owned(),
                            )
                        })
                        .collect::<HashMap<_, _>>();
                    // "This fetch named a login and ran anonymously" is the one
                    // fact about a profiled body that no consumer could see: an
                    // empty jar fetches the login wall with a 200, which clears
                    // `min_content_chars` and is stored as a real revision.
                    // Written both ways round, so any value from the wire is
                    // dropped and the marker cannot be forged by an origin.
                    pumper_core::engine::mark_anonymous_profile(
                        &mut headers,
                        req.profile.as_deref(),
                        sent_anonymous,
                    );
                    // Charset from the Content-Type header (e.g. `charset=windows-1250`),
                    // captured before the response is consumed by the streamed reader.
                    let header_charset = response
                        .headers()
                        .get(reqwest::header::CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .and_then(charset_from_content_type)
                        .map(str::to_owned);
                    // Non-2xx bodies are returned, not raised — scrapers often
                    // want to inspect 404/403 pages; apps decide via is_success().
                    // Streamed with a hard size cap so one huge/hostile body can't
                    // balloon memory (over-limit => a typed error naming cap + URL).
                    let body = read_body_capped(response, cap, &req.url, header_charset.as_deref())
                        .await?;
                    return Ok(HttpResponse {
                        status,
                        headers,
                        body,
                        final_url,
                        cache_hit: false,
                    });
                }
                Err(e) => {
                    // Statuses have always been classified here; transport
                    // failures were not, so an unparseable URL or an `ftp://`
                    // scheme burned the whole ladder AND three governor slots
                    // before failing with a retryable error the worker then
                    // re-queued. Classify at the source, like the status arm.
                    if let Some(terminal) = deterministic_transport_error(&req.url, &e) {
                        return Err(terminal);
                    }
                    last_error = e.to_string();
                    warn!(url = %req.url, error = %last_error, "request error");
                }
            }
        }
        Err(Error::Http(format!(
            "{} failed after {} attempts: {last_error}",
            req.url,
            retries + 1
        )))
    }
}

/// Reads a response body in streamed chunks, aborting the instant the cumulative
/// size would exceed `cap`. Returns a typed error naming the cap and URL on
/// overflow. Decodes to a `String` honouring the source charset (`header_charset`
/// from the Content-Type, else an HTML `<meta charset>` sniff, else a BOM, else
/// UTF-8) — so a windows-1250 Czech page is not mangled into U+FFFD replacement
/// characters the way a blind UTF-8 decode does.
async fn read_body_capped(
    response: reqwest::Response,
    cap: u64,
    url: &str,
    header_charset: Option<&str>,
) -> Result<String> {
    let buf = read_bytes_capped(response, cap, url).await?;
    Ok(decode_body(&buf, header_charset))
}

/// The raw-bytes half of [`read_body_capped`]: chunked read with the same hard
/// size cap, no decoding. Shared by the text path and [`HttpEngine::fetch_bytes`].
async fn read_bytes_capped(
    mut response: reqwest::Response,
    cap: u64,
    url: &str,
) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| Error::Http(e.to_string()))?
    {
        if would_exceed_cap(buf.len() as u64, chunk.len() as u64, cap) {
            return Err(Error::Http(format!(
                "response body from {url} exceeds max_body_bytes cap of {cap} bytes"
            )));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// The `charset` token of a `Content-Type` value (`text/html; charset=windows-1250`
/// → `windows-1250`). Case/space tolerant; strips surrounding quotes. `None` when
/// absent.
fn charset_from_content_type(content_type: &str) -> Option<&str> {
    content_type.split(';').skip(1).find_map(|param| {
        let (k, v) = param.split_once('=')?;
        k.trim()
            .eq_ignore_ascii_case("charset")
            .then(|| v.trim().trim_matches(['"', '\'']))
            .filter(|s| !s.is_empty())
    })
}

/// A `<meta charset=…>` or `<meta http-equiv="Content-Type" content="…charset=…">`
/// label sniffed from the first 1 KiB of an HTML body — the fallback when the
/// transport header declared no charset. Scans a bounded prefix so a hostile
/// document can't make this expensive.
fn charset_from_meta(head: &[u8]) -> Option<String> {
    let prefix = &head[..head.len().min(1024)];
    // Latin-1 view is safe for sniffing ASCII meta syntax out of any byte soup.
    let text = prefix
        .iter()
        .map(|&b| b as char)
        .collect::<String>()
        .to_ascii_lowercase();
    let at = text.find("charset")?;
    let after = &text[at + "charset".len()..];
    let after = after.trim_start().strip_prefix('=')?.trim_start();
    // A quoted value (`charset="windows-1250"`) — drop the opening quote so the
    // delimiter scan below stops at the CLOSING quote, not the opening one.
    let after = after.strip_prefix(['"', '\'']).unwrap_or(after);
    let end = after
        .find(['"', '\'', ' ', ';', '/', '>'])
        .unwrap_or(after.len());
    let label = after[..end].trim();
    (!label.is_empty()).then(|| label.to_string())
}

/// Decodes raw body bytes to a `String`, resolving the encoding in priority
/// order: explicit `header_charset` → HTML `<meta charset>` → BOM → UTF-8. An
/// unrecognized label falls through to the next source rather than erroring, and
/// the final UTF-8 decode is lossy (never fails), preserving the old contract
/// that a body is always returned.
fn decode_body(buf: &[u8], header_charset: Option<&str>) -> String {
    let encoding = header_charset
        .and_then(|c| encoding_rs::Encoding::for_label(c.as_bytes()))
        .or_else(|| {
            charset_from_meta(buf).and_then(|c| encoding_rs::Encoding::for_label(c.as_bytes()))
        });
    match encoding {
        Some(enc) => enc.decode(buf).0.into_owned(),
        // No declared charset: encoding_rs still honours a UTF-8/UTF-16 BOM here,
        // and otherwise decodes as UTF-8 lossily — matching the prior behaviour.
        None => encoding_rs::Encoding::for_bom(buf)
            .map(|(enc, _)| enc.decode(buf).0.into_owned())
            .unwrap_or_else(|| String::from_utf8_lossy(buf).into_owned()),
    }
}

/// Whether appending `chunk_len` bytes to a `current_len`-byte buffer would
/// exceed `cap`. Split out for unit testing the streaming cap decision without a
/// live server (the cap check `read_body_capped` performs per chunk).
fn would_exceed_cap(current_len: u64, chunk_len: u64, cap: u64) -> bool {
    current_len + chunk_len > cap
}

/// Reqwest's typed failure predicates, lifted into a plain value.
///
/// The rule below is written against **this**, not against `reqwest::Error`,
/// for one concrete reason: `reqwest::Error` has no public constructor, so a
/// classifier that takes one can only ever be exercised through a live socket —
/// and the cases that matter most here (a TLS mismatch, an NXDOMAIN) are
/// precisely the ones a loopback test cannot produce. Splitting the rule out
/// makes every combination testable, and makes the *decision* reviewable in one
/// place instead of inferred from a `match` on someone else's enum.
///
/// Deliberately **not** message substrings: `Error::plugin`'s doc records what
/// that costs (rewording a message silently reclassified every row it produced).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TransportPredicates {
    /// `is_builder` — the request could not be *constructed*: an unparseable
    /// URL, a URL that is not a valid URI, or a scheme reqwest does not speak
    /// (`ftp://`, `file://`, `mailto:`). Also a redirect *to* such a scheme.
    builder: bool,
    /// `is_connect` — the connection could not be established. Covers DNS
    /// resolution failure, connection refused/reset, and **TLS handshake
    /// failures** (they happen inside the connector).
    connect: bool,
    /// `is_timeout` — a deadline elapsed (ours, or an OS-level one).
    timeout: bool,
    /// `is_redirect` — the redirect policy stopped the request (limit exceeded).
    redirect: bool,
    /// `is_body` — the request or response body failed mid-stream.
    body: bool,
    /// `is_decode` — the response body could not be decoded (bad gzip/brotli).
    decode: bool,
    /// `is_request` — a generic send failure that is none of the above.
    request: bool,
}

impl TransportPredicates {
    fn of(e: &reqwest::Error) -> Self {
        Self {
            builder: e.is_builder(),
            connect: e.is_connect(),
            timeout: e.is_timeout(),
            redirect: e.is_redirect(),
            body: e.is_body(),
            decode: e.is_decode(),
            request: e.is_request(),
        }
    }
}

/// Whether a transport failure is **deterministic**: a pure function of the
/// request, so every retry re-derives the identical refusal.
///
/// Exactly one class qualifies, and the ones left out matter more than the one
/// in — over-classifying turns a recoverable blip into a failed job:
///
/// - **`builder` → deterministic.** The request was never sent. An unparseable
///   URL and an unsupported scheme are facts about the string, and a job's URL
///   is frozen at enqueue. Nothing about attempt 4 makes `ftp://` speakable.
/// - **`connect` → retryable, and this is the judgement call.** It bundles
///   three things that look permanent and are not. **DNS**: an NXDOMAIN from a
///   resolver that is itself down, mid-failover, or rate-limiting is
///   indistinguishable here from a domain that does not exist, and the first is
///   common on a box that just booted. **TLS**: a certificate mismatch is
///   usually permanent, but the textbook exception is a captive portal — the
///   one situation where the *next* attempt genuinely succeeds — and reqwest
///   gives no predicate that separates "wrong hostname" from "intercepted".
///   **Refused/reset**: a restarting service. Left retryable on purpose.
/// - **`redirect` → retryable.** A redirect loop is usually a *session* fact
///   (an expired cookie bouncing every request to a login), not a property of
///   the URL, so a later attempt with a warmed jar can break it.
/// - **`timeout` / `body` / `decode` / `request` → retryable.** All transient
///   by construction; the end-to-end budget (`total_budget_secs`) is what
///   bounds their cost, not this classifier.
fn transport_is_deterministic(p: TransportPredicates) -> bool {
    p.builder
}

/// Maps a deterministic transport failure to the terminal [`Error::BadRequest`],
/// or `None` when the failure may legitimately be retried.
///
/// `BadRequest` rather than a widened `is_terminal_for_job`: the variant already
/// means "client-supplied input the server understood and rejected", is already
/// terminal, and is already the 400 this is at the request boundary — the same
/// lever `require_safe_profile_name` took for a typo'd profile. Widening
/// `Error::Http` would have swept up every connect blip and body error with it.
fn deterministic_transport_error(url: &str, e: &reqwest::Error) -> Option<Error> {
    transport_is_deterministic(TransportPredicates::of(e)).then(|| {
        Error::BadRequest(format!(
            "{url} cannot be requested at all: {e}. The URL is unparseable or its scheme is not \
             http(s) — a pure function of the request, so no retry can change it."
        ))
    })
}

/// The timeout one *attempt* gets: the per-request override else the
/// client-global `[http] timeout_secs`.
fn per_attempt_timeout(req_timeout_secs: Option<u64>, cfg_timeout_secs: u64) -> Duration {
    Duration::from_secs(req_timeout_secs.unwrap_or(cfg_timeout_secs))
}

/// The end-to-end budget for one `send()`: `[http] total_budget_secs`, raised to
/// at least one full per-attempt timeout. `None` = no deadline (`0` disables).
///
/// The raise is what keeps the budget from becoming a *shorter* timeout than the
/// caller asked for: `app-cms-fee-schedule` and `mpsv-vpm` widen
/// `HttpRequest.timeout_secs` for very large downloads, and a global budget
/// below that would cut the single attempt they need. The budget bounds the
/// *multiplication* of attempts, never the length of one.
fn fetch_budget(total_budget_secs: u64, per_attempt: Duration) -> Option<Duration> {
    (total_budget_secs > 0).then(|| Duration::from_secs(total_budget_secs).max(per_attempt))
}

/// What is left of the budget right now; `None` when there is no deadline.
fn remaining(deadline: Option<Instant>) -> Option<Duration> {
    deadline.map(|d| d.saturating_duration_since(Instant::now()))
}

/// The sleep a retry may actually take. `None` means **stop now**: the budget
/// cannot fit this sleep plus an attempt worth starting.
///
/// The anti-pattern this guards: `retry_delay` honours a server `Retry-After` up
/// to 600 s, so a rate-limited host could park a fetch for ten minutes at a time
/// and then be handed the remains of a budget it had already spent. Truncating
/// the sleep instead would be worse than failing — it would retry *earlier* than
/// the server asked, which is the one thing politeness must never do.
fn capped_retry_sleep(delay: Duration, remaining: Option<Duration>) -> Option<Duration> {
    let Some(left) = remaining else {
        return Some(delay); // no deadline configured
    };
    (delay + MIN_ATTEMPT_BUDGET <= left).then_some(delay)
}

/// The timeout one attempt may use: the per-attempt timeout, clamped to what is
/// left of the end-to-end budget so no attempt can outlive the deadline. `None`
/// means the budget is spent and no attempt should be started.
fn attempt_timeout(per_attempt: Duration, remaining: Option<Duration>) -> Option<Duration> {
    let Some(left) = remaining else {
        return Some(per_attempt); // no deadline configured
    };
    (left >= MIN_ATTEMPT_BUDGET).then(|| per_attempt.min(left))
}

/// The end-to-end budget failure: names the URL, the wall clock actually spent,
/// how many attempts that bought and the knob that stopped it — so an operator
/// can tell "the origin was slow" (a per-attempt `timeout_secs` error, raised by
/// reqwest) from "we gave up on our own clock" without reading a log.
///
/// Stays **retryable** (`Error::Http`), like `[browser] render_budget_secs`
/// exhaustion: "this host was slow *this time*" is a fact about a live site, not
/// a pure function of the request, so a job may legitimately try again later.
fn budget_exhausted(
    url: &str,
    elapsed: Duration,
    attempts: u32,
    budget: Option<Duration>,
    last_error: &str,
) -> Error {
    let budget_secs = budget.map(|b| b.as_secs()).unwrap_or_default();
    let why = if last_error.is_empty() {
        "no attempt completed".to_string()
    } else {
        format!("last error: {last_error}")
    };
    Error::Http(format!(
        "{url} exhausted its end-to-end fetch budget ([http] total_budget_secs = {budget_secs}s) \
         after {:.1}s and {attempts} attempt(s) — {why}",
        elapsed.as_secs_f64()
    ))
}

/// Deterministic per-retry jitter seed from the URL and attempt number — same
/// URL+attempt always jitters identically (reproducible), distinct URLs spread.
fn jitter_seed(url: &str, attempt: u32) -> u64 {
    let mut h = DefaultHasher::new();
    url.hash(&mut h);
    attempt.hash(&mut h);
    h.finish()
}

/// Retry sleep policy (pure, deterministic for testing): the larger of the
/// exponential backoff (`base_ms * 2^(attempt-1)`) and any server `Retry-After`,
/// plus hash-based jitter up to `RETRY_JITTER_FRAC` of that floor. `attempt` is
/// 1-based (the first retry). No `rand` dependency — jitter is derived from
/// `seed` via the shared `pumper_core::lcg_fraction`, exactly like the governor.
fn retry_delay(attempt: u32, retry_after: Option<Duration>, base_ms: u64, seed: u64) -> Duration {
    let exp = attempt.saturating_sub(1).min(20); // cap the shift; 2^20 ms ≈ 17min
    let backoff = Duration::from_millis(base_ms.saturating_mul(2u64.saturating_pow(exp)));
    let floor = backoff.max(retry_after.unwrap_or(Duration::ZERO));
    // Deterministic scramble of the seed -> fraction in [0,1) (shared with the
    // governor's pacing jitter).
    let frac = pumper_core::lcg_fraction(seed);
    floor + floor.mul_f64(RETRY_JITTER_FRAC * frac)
}

/// Parses a `Retry-After` header. Both RFC 7231 forms are honored: delta
/// -seconds (`Retry-After: 120`) and an HTTP-date (`Retry-After: Wed, 21 Oct
/// 2025 07:28:00 GMT`), the latter converted to a delay from now. Clamped to
/// 10 minutes; a past/malformed date yields `None` (falls back to doubling).
fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    let raw = response.headers().get("retry-after")?.to_str().ok()?.trim();
    retry_after_value(raw, chrono::Utc::now())
}

/// Header-value parsing split out for testing (the `now` reference makes the
/// HTTP-date form deterministic).
fn retry_after_value(raw: &str, now: chrono::DateTime<chrono::Utc>) -> Option<Duration> {
    const MAX_SECS: u64 = 600;
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(Duration::from_secs(secs.min(MAX_SECS)));
    }
    let when = parse_http_date(raw)?;
    let secs = when.signed_duration_since(now).num_seconds();
    if secs <= 0 {
        return Some(Duration::ZERO);
    }
    Some(Duration::from_secs((secs as u64).min(MAX_SECS)))
}

/// Parses an HTTP-date. The RFC 7231-mandated IMF-fixdate form ("Sun, 06 Nov
/// 1994 08:49:37 GMT") is tried first; a numeric-offset RFC 2822 date is
/// accepted as a fallback.
fn parse_http_date(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(raw, "%a, %d %b %Y %H:%M:%S GMT") {
        return Some(chrono::DateTime::from_naive_utc_and_offset(
            naive,
            chrono::Utc,
        ));
    }
    chrono::DateTime::parse_from_rfc2822(raw)
        .ok()
        .map(|d| d.with_timezone(&chrono::Utc))
}

#[async_trait]
impl HttpClient for HttpEngine {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let cache_key = Self::cacheable(&req).then(|| HttpCache::key(&req));
        let ttl = req
            .ttl_override
            .map(Duration::from_secs)
            .unwrap_or_else(|| self.cache.default_ttl());

        if let Some(key) = &cache_key {
            // ttl_override caps read staleness too, not just storage TTL: a reader
            // asking for <=N-second-old content must not be handed a longer-lived
            // entry written by another caller.
            let max_age = req.ttl_override.map(Duration::from_secs);
            if let Some(hit) = self.cache.get(key, max_age).await? {
                debug!(url = %req.url, "cache hit");
                return Ok(hit);
            }

            // Expired-but-maybe-still-valid: revalidate with the stored ETag /
            // Last-Modified instead of re-downloading the whole body — a 304 is a
            // few hundred bytes where the body can be megabytes (the `watch`/poll
            // workload's common case). Only when the CALLER isn't already running
            // its own conditional GET (the crawler's revisit mode owns that path
            // and wants the raw 304 passed through).
            if req.etag.is_none() && req.if_modified_since.is_none() {
                if let Some(stale) = self.cache.get_stale(key).await? {
                    if stale.etag.is_some() || stale.last_modified.is_some() {
                        let mut cond = req.clone();
                        cond.etag = stale.etag;
                        cond.if_modified_since = stale.last_modified;
                        let resp = self.send(&cond).await?;
                        if resp.status == 304 {
                            // Still valid: extend the entry's life (no body rewrite)
                            // and serve the stored body as a cache hit.
                            self.cache.refresh(key, ttl).await?;
                            debug!(url = %req.url, "cache revalidated (304)");
                            return Ok(HttpResponse {
                                cache_hit: true,
                                ..stale.response
                            });
                        }
                        // Changed: log the labeled observation (feeds the
                        // change-cadence estimator; the 304 counterpart is
                        // recorded inside `refresh`), store, return fresh body.
                        // Only a real 2xx body is a "changed" observation — an
                        // origin error is evidence of neither outcome.
                        if resp.is_success() {
                            self.cache.record_revalidation(key, true).await;
                        }
                        self.cache.put(key, &req.url, &resp, ttl).await?;
                        return Ok(resp);
                    }
                }
            }
        }

        let response = self.send(&req).await?;

        // A 304 Not Modified is a revalidation signal, not content — its (empty)
        // body must never overwrite a cached full response. Pass the status
        // through untouched so conditional-GET callers can act on it.
        if let Some(key) = &cache_key {
            if response.status == 304 {
                return Ok(response);
            }
            self.cache.put(key, &req.url, &response, ttl).await?;
        }
        Ok(response)
    }

    /// Binary fetch — the deliberately minimal **engine-traits#2-LITE** seam
    /// (first paying customer: the CMS RVU ZIP in `app-cms-fee-schedule`).
    ///
    /// Contract, and what is deliberately NOT here:
    /// - **Hard size cap**: the existing `max_body_bytes` machinery
    ///   (`req.max_body_bytes` else `[http] max_body_bytes`), enforced per chunk
    ///   while reading; over-cap is a typed error naming cap + URL.
    /// - **No streaming**: the body is buffered in memory — callers size their
    ///   cap accordingly. The full streaming/spill-to-artifact binary-body
    ///   design (engine-traits#2) stays deferred.
    /// - **Cache bypass**: the response cache stores charset-decoded text
    ///   bodies, so binary fetches never read or write it.
    /// - **Governor applies**: per-host politeness spacing + 429/503 penalties
    ///   are identical to `fetch` — a ZIP download is not a license to hammer.
    /// - **No retries, 2xx only**: a single attempt; a non-2xx status is an
    ///   error (binary callers want the file, not an error page's bytes).
    ///   Profiles/proxies work via the same client pool as `fetch`.
    async fn fetch_bytes(&self, req: HttpRequest) -> Result<Vec<u8>> {
        let host = reqwest::Url::parse(&req.url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned));
        let (client, jar) = self.client_for(&req)?;
        let cap = req.max_body_bytes.unwrap_or(self.cfg.max_body_bytes);
        // One attempt, so the end-to-end bound IS the attempt timeout — and the
        // budget is never below one full attempt, which is why a 188 MB feed
        // that widens `timeout_secs` is not cut short here.
        let per_attempt = per_attempt_timeout(req.timeout_secs, self.cfg.timeout_secs);
        if let Some(host) = &host {
            self.governor.acquire(host).await;
        }
        let response = self
            .build(&client, &req, per_attempt)
            .send()
            .await
            // Same classification as `send`: this method makes a single attempt,
            // so there is no ladder to save here — but the JOB's ladder is real,
            // and a URL that cannot be requested at all must not ride it.
            .map_err(|e| {
                deterministic_transport_error(&req.url, &e)
                    .unwrap_or_else(|| Error::Http(e.to_string()))
            })?;
        if let Some(jar) = &jar {
            jar.touch();
        }
        let status = response.status().as_u16();
        // Teach the governor exactly like `send` does.
        if let Some(host) = &host {
            if matches!(status, 429 | 503) {
                self.governor.penalize(host, retry_after(&response)).await;
            } else if (200..300).contains(&status) {
                self.governor.reward(host).await;
            }
        }
        if !(200..300).contains(&status) {
            return Err(Error::Http(format!(
                "{} returned status {status} (fetch_bytes requires a 2xx body)",
                req.url
            )));
        }
        read_bytes_capped(response, cap, &req.url).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_cache_makes_request_uncacheable() {
        // A plain GET is cacheable; setting no_cache skips both cache read and
        // write (the same gate governs the get() and put() paths in fetch()).
        let mut req = HttpRequest::get("https://example.com/");
        assert!(HttpEngine::cacheable(&req), "plain GET should be cacheable");
        req.no_cache = true;
        assert!(
            !HttpEngine::cacheable(&req),
            "no_cache must bypass the cache"
        );
    }

    #[test]
    fn ttl_override_does_not_affect_cacheability() {
        // ttl_override shapes storage freshness, not whether a request is cached.
        let mut req = HttpRequest::get("https://example.com/");
        req.ttl_override = Some(30);
        assert!(HttpEngine::cacheable(&req));
    }

    #[test]
    fn retry_after_delta_seconds() {
        let now = chrono::Utc::now();
        assert_eq!(
            retry_after_value("120", now),
            Some(Duration::from_secs(120))
        );
        // Clamped to 10 minutes.
        assert_eq!(
            retry_after_value("99999", now),
            Some(Duration::from_secs(600))
        );
    }

    #[test]
    fn retry_after_http_date() {
        // IMF-fixdate ("... GMT") is the form real rate limiters emit. chrono
        // validates the weekday against the date, so use the RFC 7231 example
        // (06 Nov 1994 is a Sunday).
        let now = chrono::NaiveDate::from_ymd_opt(1994, 11, 6)
            .unwrap()
            .and_hms_opt(8, 49, 37)
            .unwrap()
            .and_utc();
        // 90 seconds in the future.
        let later = "Sun, 06 Nov 1994 08:51:07 GMT";
        assert_eq!(retry_after_value(later, now), Some(Duration::from_secs(90)));
        // A date in the past yields a zero (immediate) delay, not None.
        let past = "Sun, 06 Nov 1994 08:48:00 GMT";
        assert_eq!(retry_after_value(past, now), Some(Duration::ZERO));
    }

    #[test]
    fn retry_after_malformed_is_none() {
        let now = chrono::Utc::now();
        assert_eq!(retry_after_value("not-a-date", now), None);
    }

    #[test]
    fn body_cap_decision() {
        // Under cap: fits.
        assert!(!would_exceed_cap(0, 100, 100));
        assert!(!would_exceed_cap(50, 50, 100));
        // Exactly at cap is allowed; one over trips.
        assert!(would_exceed_cap(50, 51, 100));
        assert!(would_exceed_cap(100, 1, 100));
        // A single oversized first chunk trips immediately.
        assert!(would_exceed_cap(0, 101, 100));
    }

    #[test]
    fn charset_parsed_from_content_type() {
        assert_eq!(
            charset_from_content_type("text/html; charset=windows-1250"),
            Some("windows-1250")
        );
        // Case- and space-insensitive, quote-stripping.
        assert_eq!(
            charset_from_content_type("text/html;  CharSet = \"UTF-8\""),
            Some("UTF-8")
        );
        // No charset param → None (falls through to meta/BOM/UTF-8).
        assert_eq!(charset_from_content_type("text/html"), None);
        assert_eq!(charset_from_content_type("application/json"), None);
    }

    #[test]
    fn charset_sniffed_from_meta_tag() {
        let html = br#"<!doctype html><html><head><meta charset="windows-1250"><title>x"#;
        assert_eq!(charset_from_meta(html).as_deref(), Some("windows-1250"));
        let legacy = br#"<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-2">"#;
        assert_eq!(charset_from_meta(legacy).as_deref(), Some("iso-8859-2"));
        assert_eq!(charset_from_meta(b"<html><head><title>no charset"), None);
    }

    #[test]
    fn decode_body_honours_windows_1250() {
        // "Řehoř" — the Czech letters Ř/ř are 0xD8/0xF8 in windows-1250, bytes that
        // are NOT valid UTF-8 and would each become U+FFFD under a blind decode.
        let cp1250: &[u8] = &[0xD8, 0x65, 0x68, 0x6F, 0xF8];
        // Header-declared charset wins.
        assert_eq!(decode_body(cp1250, Some("windows-1250")), "Řehoř");
        // The old lossy path would have mangled it — prove the fix changed behaviour.
        assert_ne!(String::from_utf8_lossy(cp1250), "Řehoř");
        assert!(String::from_utf8_lossy(cp1250).contains('\u{FFFD}'));
    }

    #[test]
    fn decode_body_falls_back_to_meta_then_utf8() {
        // No header charset, but the HTML declares windows-1250 in a meta tag.
        let mut body = br#"<meta charset="windows-1250">"#.to_vec();
        body.extend_from_slice(&[0xC8, 0x65, 0x73, 0x6B, 0x6F]); // "Česko" (È=0xC8)
        assert!(decode_body(&body, None).ends_with("Česko"));
        // Plain UTF-8 with no declaration decodes cleanly.
        assert_eq!(decode_body("čau".as_bytes(), None), "čau");
        // An unknown label falls through to UTF-8 rather than erroring.
        assert_eq!(decode_body("hi".as_bytes(), Some("x-bogus-charset")), "hi");
    }

    #[test]
    fn retry_delay_backoff_doubles_per_attempt() {
        // Zero jitter (seed chosen so frac≈0 is not guaranteed) — instead assert
        // the delay is within [floor, floor*1.25]. floor = base * 2^(attempt-1).
        let base = 500;
        for (attempt, floor_ms) in [(1u32, 500u64), (2, 1000), (3, 2000), (4, 4000)] {
            let d = retry_delay(attempt, None, base, jitter_seed("https://x/", attempt));
            let floor = Duration::from_millis(floor_ms);
            assert!(d >= floor, "attempt {attempt}: {d:?} < floor {floor:?}");
            assert!(
                d <= floor.mul_f64(1.0 + RETRY_JITTER_FRAC),
                "attempt {attempt}: {d:?} exceeds floor+jitter"
            );
        }
    }

    #[test]
    fn retry_delay_honors_retry_after_over_backoff() {
        // Attempt 1 backoff floor is 500ms; a 5s Retry-After must win.
        let d = retry_delay(1, Some(Duration::from_secs(5)), 500, 12345);
        assert!(
            d >= Duration::from_secs(5),
            "Retry-After should dominate: {d:?}"
        );
        assert!(d <= Duration::from_millis(5000).mul_f64(1.0 + RETRY_JITTER_FRAC));
        // When backoff exceeds a tiny Retry-After, backoff wins.
        let d2 = retry_delay(4, Some(Duration::from_millis(10)), 500, 12345);
        assert!(d2 >= Duration::from_millis(4000));
    }

    #[test]
    fn retry_delay_is_deterministic_for_same_inputs() {
        let a = retry_delay(2, None, 500, 999);
        let b = retry_delay(2, None, 500, 999);
        assert_eq!(a, b, "same seed/inputs must yield identical delay");
    }

    /// The anti-pattern the budget exists for: `timeout_secs` bounded one
    /// ATTEMPT, and the retry loop multiplied it. The budget must never be
    /// shorter than a single attempt, or turning it on silently shortens the
    /// per-request timeout callers deliberately widened for large downloads.
    #[test]
    fn a_fetch_budget_is_never_shorter_than_one_attempt() {
        // The shipped shape: 300 s budget over a 30 s attempt.
        assert_eq!(
            fetch_budget(300, Duration::from_secs(30)),
            Some(Duration::from_secs(300))
        );
        // A caller who widened `HttpRequest.timeout_secs` past the budget (the
        // 188 MB feed) still gets one complete attempt, not a truncated one.
        assert_eq!(
            fetch_budget(300, Duration::from_secs(600)),
            Some(Duration::from_secs(600))
        );
        // `0` disables the deadline entirely (the pre-2026-08 behaviour).
        assert_eq!(fetch_budget(0, Duration::from_secs(30)), None);
    }

    /// A `429 Retry-After: 600` used to buy three sleeps of up to 750 s each on
    /// top of four attempts — ~37.5 minutes for ONE fetch, past
    /// `[worker] job_timeout_secs`. The sleep must be refused, never truncated:
    /// retrying earlier than the server asked is the one thing politeness may
    /// not do.
    #[test]
    fn a_retry_sleep_is_refused_not_truncated_when_the_budget_cannot_hold_it() {
        let ten_min = Duration::from_secs(600);
        // No deadline: unchanged behaviour, the full sleep is taken.
        assert_eq!(capped_retry_sleep(ten_min, None), Some(ten_min));
        // Budget cannot hold the sleep AND an attempt after it -> stop now.
        assert_eq!(
            capped_retry_sleep(ten_min, Some(Duration::from_secs(60))),
            None
        );
        // Exactly enough for the sleep but nothing left to fetch with -> stop.
        assert_eq!(capped_retry_sleep(ten_min, Some(ten_min)), None);
        // Room for the sleep plus a usable attempt -> the sleep is unchanged
        // (never shortened, so the server's Retry-After is still honoured).
        assert_eq!(
            capped_retry_sleep(ten_min, Some(Duration::from_secs(700))),
            Some(ten_min)
        );
    }

    /// No attempt may be started that cannot finish inside the deadline: an
    /// attempt is either given the remaining budget as its timeout, or refused.
    #[test]
    fn an_attempt_never_outlives_the_remaining_budget() {
        let thirty = Duration::from_secs(30);
        // No deadline: the per-attempt timeout is used as-is.
        assert_eq!(attempt_timeout(thirty, None), Some(thirty));
        // Plenty left: the per-attempt timeout still wins (the budget is a
        // ceiling on the fetch, not a shorter timeout for a healthy attempt).
        assert_eq!(
            attempt_timeout(thirty, Some(Duration::from_secs(200))),
            Some(thirty)
        );
        // Less left than one attempt: clamped to the remainder, so the attempt
        // ends ON the deadline rather than 30 s past it.
        assert_eq!(
            attempt_timeout(thirty, Some(Duration::from_secs(4))),
            Some(Duration::from_secs(4))
        );
        // Nothing usable left: refuse rather than fire a doomed request.
        assert_eq!(
            attempt_timeout(thirty, Some(Duration::from_millis(10))),
            None
        );
        assert_eq!(attempt_timeout(thirty, Some(Duration::ZERO)), None);
    }

    /// The operator has to be able to tell "the origin was slow" from "we gave
    /// up on our own clock" — which means the knob, the URL, the wall clock and
    /// the attempt count all travel in the failure.
    #[test]
    fn the_budget_failure_names_the_knob_the_url_and_the_clock() {
        let err = budget_exhausted(
            "https://slow.test/feed",
            Duration::from_secs_f64(302.4),
            2,
            Some(Duration::from_secs(300)),
            "status 429",
        );
        let shown = err.to_string();
        for needle in [
            "https://slow.test/feed",
            "total_budget_secs = 300s",
            "302.4s",
            "2 attempt(s)",
            "status 429",
        ] {
            assert!(shown.contains(needle), "{needle:?} missing from {shown}");
        }
        // Retryable: a slow site is a fact about the site, not about the request.
        assert!(!err.is_terminal_for_job());
        // A budget spent before any attempt reported anything still reads.
        let none = budget_exhausted("https://x/", Duration::from_secs(1), 0, None, "");
        assert!(none.to_string().contains("no attempt completed"));
    }

    /// THE transport classification. Statuses were classified at this seam and
    /// transport failures were not, so `ftp://x` and `not a url` each burned
    /// `retries + 1` attempts, three governor slots and then a *retryable*
    /// error the worker re-queued — the whole ladder again, per job attempt.
    ///
    /// Enumerated over every predicate reqwest exposes, because the failure
    /// mode of a fix like this is OVER-classification: one transient class
    /// wrongly marked deterministic turns a recoverable blip into a failed job.
    #[test]
    fn only_an_unsendable_request_is_deterministic_not_every_transport_error() {
        let only = |set: fn(&mut TransportPredicates)| {
            let mut p = TransportPredicates::default();
            set(&mut p);
            p
        };
        // The one deterministic class: the request was never sent.
        assert!(transport_is_deterministic(only(|p| p.builder = true)));
        // Everything else stays retryable — each for a reason recorded on
        // `transport_is_deterministic`, and each a real recoverable case.
        for (name, p) in [
            // NXDOMAIN from a resolver that is itself down; a captive portal
            // failing the TLS handshake; a service mid-restart.
            ("connect", only(|p| p.connect = true)),
            ("timeout", only(|p| p.timeout = true)),
            // A login bounce that a warmed cookie jar breaks next time.
            ("redirect", only(|p| p.redirect = true)),
            ("body", only(|p| p.body = true)),
            ("decode", only(|p| p.decode = true)),
            ("request", only(|p| p.request = true)),
            ("nothing at all", TransportPredicates::default()),
        ] {
            assert!(
                !transport_is_deterministic(p),
                "{name} must stay retryable: marking a transient class terminal \
                 turns a recoverable blip into a failed job"
            );
        }
        // A builder error is still deterministic when it arrives alongside
        // another predicate (reqwest's `is_timeout`/`is_connect` walk the source
        // chain, so they are not mutually exclusive with a kind check).
        assert!(transport_is_deterministic(TransportPredicates {
            builder: true,
            connect: true,
            ..Default::default()
        }));
    }

    /// The class has to reach the *worker*, not just this crate: a deterministic
    /// refusal that comes back retryable is re-queued and re-runs the whole
    /// ladder on every job attempt.
    #[test]
    fn a_deterministic_transport_refusal_is_terminal_for_the_job() {
        // A real reqwest builder error — the only way to get one is to make
        // reqwest build it, since `reqwest::Error` has no public constructor.
        let client = reqwest::Client::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for url in ["ftp://example.test/file", "::definitely not a url::"] {
            let e = rt
                .block_on(client.get(url).send())
                .expect_err("reqwest must refuse this before any socket");
            assert!(
                e.is_builder(),
                "{url}: expected a builder error, got {e:?} — the classification \
                 rests on this predicate"
            );
            let mapped = deterministic_transport_error(url, &e).expect("must classify");
            assert!(mapped.is_terminal_for_job(), "{url}: {mapped}");
            assert!(matches!(mapped, Error::BadRequest(_)), "{mapped:?}");
            assert!(mapped.to_string().contains(url));
        }
    }

    #[test]
    fn per_attempt_timeout_prefers_the_request_override() {
        assert_eq!(per_attempt_timeout(Some(600), 30), Duration::from_secs(600));
        assert_eq!(per_attempt_timeout(None, 30), Duration::from_secs(30));
    }

    #[test]
    fn jitter_seed_varies_by_url_and_attempt() {
        assert_ne!(jitter_seed("https://a/", 1), jitter_seed("https://b/", 1));
        assert_ne!(jitter_seed("https://a/", 1), jitter_seed("https://a/", 2));
        assert_eq!(jitter_seed("https://a/", 1), jitter_seed("https://a/", 1));
    }

    #[test]
    fn proxy_client_reused_when_matching_configured_proxy() {
        // A per-request proxy equal to the configured [http] proxy reuses the
        // base client rather than pooling a duplicate (no live network needed —
        // build_client just constructs a client).
        let cfg = HttpConfig {
            proxy: Some("http://gw:8080".into()),
            ..Default::default()
        };
        // build_client must accept a valid proxy URL.
        assert!(build_client(&cfg, cfg.proxy.as_deref(), None).is_ok());
    }

    #[test]
    fn build_client_rejects_invalid_proxy() {
        let cfg = HttpConfig::default();
        // A syntactically invalid proxy URL surfaces a typed Http error.
        let err = build_client(&cfg, Some("::not a url::"), None).unwrap_err();
        assert!(matches!(err, Error::Http(_)));
    }

    #[test]
    fn client_pool_is_lru_bounded() {
        // Dummy clients (no network) exercise the pool's LRU + eviction directly.
        let mut pool = ClientPool::new();
        for i in 0..MAX_POOLED_CLIENTS {
            pool.insert(&format!("p{i}"), reqwest::Client::new(), MAX_POOLED_CLIENTS);
        }
        assert_eq!(pool.clients.len(), MAX_POOLED_CLIENTS);
        // Touch p0 so it's most-recent; p1 becomes the LRU victim.
        assert!(pool.get("p0").is_some());
        // Insert one over cap -> evicts the least-recently-used (p1), keeps p0.
        pool.insert("pN", reqwest::Client::new(), MAX_POOLED_CLIENTS);
        assert_eq!(pool.clients.len(), MAX_POOLED_CLIENTS);
        assert!(pool.get("p0").is_some(), "recently-touched entry retained");
        assert!(!pool.clients.contains_key("p1"), "LRU entry evicted");
        assert!(pool.get("pN").is_some(), "newest entry present");
    }

    #[test]
    fn pool_key_separates_proxy_and_profile_dimensions() {
        // The same proxy under two profiles => two clients; the same profile
        // behind two proxies => two clients; and no cross-field collision.
        assert_ne!(
            pool_key(Some("http://gw"), None),
            pool_key(Some("http://gw"), Some("a"))
        );
        assert_ne!(
            pool_key(None, Some("a")),
            pool_key(Some("http://gw"), Some("a"))
        );
        assert_ne!(pool_key(Some("a"), Some("b")), pool_key(Some("ab"), None));
        assert_ne!(pool_key(None, None), pool_key(None, Some("a")));
        // Stable for the same pair (a pooled client is actually reused).
        assert_eq!(
            pool_key(Some("p"), Some("a")),
            pool_key(Some("p"), Some("a"))
        );
    }

    #[test]
    fn profiled_requests_never_touch_the_shared_cache() {
        // The http_cache key ignores `profile`, so a logged-in body must never be
        // cached (it would be served to anonymous callers).
        let mut req = HttpRequest::get("https://example.com/");
        assert!(HttpEngine::cacheable(&req));
        req.profile = Some("acme".into());
        assert!(
            !HttpEngine::cacheable(&req),
            "profiled fetches must bypass the cache"
        );
    }

    /// The jar round-trips through disk: a cookie stored from a response is
    /// written to `cookies.json` and comes back on the next process (a fresh
    /// `ProfileJar::load` of the same path), which is the whole point of the
    /// vault. Uses the reqwest `CookieStore` trait exactly like reqwest does.
    #[test]
    fn cookie_jar_round_trips_through_disk() {
        use reqwest::cookie::CookieStore as _;

        let dir = std::env::temp_dir().join(format!(
            "pumper-jar-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = profile_cookies_path(&dir, "acme").expect("valid name");
        let url: reqwest::Url = "https://example.com/app".parse().unwrap();

        let jar = ProfileJar::load("acme", path.clone()).expect("fresh jar");
        // A session cookie (no Expires/Max-Age) — the login case.
        let set = HeaderValue::from_static("sid=secret-123; Path=/");
        jar.set_cookies(&mut [&set].into_iter(), &url);
        assert_eq!(
            jar.cookies(&url).unwrap().to_str().unwrap(),
            "sid=secret-123",
            "the live jar replays the cookie"
        );
        jar.save().expect("jar saves");
        assert!(path.exists(), "cookies.json written at {}", path.display());

        // A second process: load the same file, cookie must still be there.
        let reloaded = ProfileJar::load("acme", path.clone()).expect("reload");
        assert_eq!(
            reloaded.cookies(&url).unwrap().to_str().unwrap(),
            "sid=secret-123",
            "session cookie survived the round-trip"
        );
        // Cookies are scoped to their origin — another host gets nothing.
        let other: reqwest::Url = "https://other.test/".parse().unwrap();
        assert!(reloaded.cookies(&other).is_none());

        // A separate profile has a separate jar (no cross-profile bleed).
        let other_path = profile_cookies_path(&dir, "beta").expect("valid name");
        let beta = ProfileJar::load("beta", other_path).expect("fresh jar");
        assert!(
            beta.cookies(&url).is_none(),
            "profiles do not share cookies"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The anti-pattern: **an empty jar clobbering a real one**. `jar_for`
    /// returns a cached `Arc` without ever re-reading disk, and `save` renamed
    /// over the path unconditionally — so a server started while `cookies.json`
    /// was missing would, on the next profiled response, overwrite an operator's
    /// restored backup with an empty jar and log `cookie jar saved`.
    #[test]
    fn an_empty_jar_never_overwrites_a_stored_one() {
        // The destructive case: nothing in memory, a real jar on disk.
        assert_eq!(save_decision(0, true), SaveDecision::WouldClobber);
        // Nothing in memory, nothing on disk: writing would only create a
        // profile directory for a session that does not exist — which is how a
        // typo'd profile name used to appear in `GET /profiles`.
        assert_eq!(save_decision(0, false), SaveDecision::NothingToPersist);
        // A real jar is written, whether or not one is already there.
        assert_eq!(save_decision(1, false), SaveDecision::Write);
        assert_eq!(save_decision(7, true), SaveDecision::Write);
    }

    /// The anti-pattern: **a failed write silently dropped**. The flusher used
    /// to clear `dirty` BEFORE saving, so one transient failure (a Windows
    /// sharing violation — the exact case that keeps `Error::Profile` retryable)
    /// left the flag `false` and the cookie was never written: logged in for the
    /// life of the process, logged out by the restart.
    #[test]
    fn a_failed_jar_save_is_retried_a_bounded_number_of_times() {
        assert!(should_retry_save(1), "the first failure must be retried");
        assert!(should_retry_save(MAX_SAVE_RETRIES - 1));
        // ...but a permanently unwritable path must not become a warn-per-second
        // forever.
        assert!(!should_retry_save(MAX_SAVE_RETRIES));
        assert!(!should_retry_save(MAX_SAVE_RETRIES + 100));
        // Long enough to ride out a sharing violation at a 1 s debounce: the
        // window this exists for is seconds, not one cycle.
        assert!(
            (COOKIE_FLUSH_DEBOUNCE * MAX_SAVE_RETRIES) >= Duration::from_secs(3),
            "{MAX_SAVE_RETRIES} retries at a {COOKIE_FLUSH_DEBOUNCE:?} debounce is \
             too short a window for a transient file lock"
        );
    }

    #[test]
    fn jar_load_rejects_an_unsafe_profile_name() {
        // Validation happens before any path is built (typed Profile error).
        let err =
            profile_cookies_path(std::path::Path::new("data/profiles"), "../etc").unwrap_err();
        assert!(matches!(err, Error::Profile(_)));
    }
}
