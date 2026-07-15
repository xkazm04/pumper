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
use std::time::Duration;

use async_trait::async_trait;
use cookie_store::{CookieStore, RawCookie};
use pumper_core::config::HttpConfig;
use pumper_core::{
    profile_cookies_path, Error, Governor, HttpCache, HttpClient, HttpMethod, HttpRequest,
    HttpResponse, Result,
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
    /// Loads `<profiles_dir>/<name>/cookies.json`, creating the profile dir on
    /// first use. A missing file starts an empty jar; an unreadable/corrupt one
    /// is warned about and also starts empty (a bad jar must not wedge fetches).
    fn load(name: &str, path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let store = match std::fs::File::open(&path) {
            Ok(file) => cookie_store::serde::json::load(BufReader::new(file)).unwrap_or_else(|e| {
                warn!(profile = %name, "cookie jar {} unreadable ({e}); starting empty", path.display());
                CookieStore::default()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => CookieStore::default(),
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

    /// Serializes the jar and replaces the file atomically (write tmp + rename),
    /// so a crash mid-write can never leave a truncated jar behind.
    fn save(&self) -> Result<()> {
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
    async fn flush_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(COOKIE_FLUSH_DEBOUNCE).await;
            if self.dirty.swap(false, Ordering::SeqCst) {
                if let Err(e) = self.save() {
                    warn!(profile = %self.name, "saving cookie jar: {e}");
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
        Self { clients: HashMap::new(), order: VecDeque::new() }
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
    /// [`Error::Profile`] and never touches the filesystem.
    fn jar_for(&self, name: &str) -> Result<Arc<ProfileJar>> {
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

    fn build(&self, client: &reqwest::Client, req: &HttpRequest) -> reqwest::RequestBuilder {
        let mut builder = match req.method {
            HttpMethod::Get => client.get(&req.url),
            HttpMethod::Post => client.post(&req.url),
        };
        if let Some(secs) = req.timeout_secs {
            // Per-attempt override of the client-global timeout.
            builder = builder.timeout(Duration::from_secs(secs));
        }
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

        let mut last_error = String::new();
        // Retry-After from the previous retryable response, so the next sleep can
        // honor the server's requested delay instead of a blind doubling.
        let mut last_retry_after: Option<Duration> = None;
        for attempt in 0..=retries {
            if attempt > 0 {
                let seed = jitter_seed(&req.url, attempt);
                let delay = retry_delay(attempt, last_retry_after, RETRY_BASE_MS, seed);
                debug!(url = %req.url, attempt, "retrying in {delay:?} ({last_error})");
                tokio::time::sleep(delay).await;
            }
            // Politeness spacing is applied per attempt so retries also wait.
            if let Some(host) = &host {
                self.governor.acquire(host).await;
            }
            match self.build(&client, req).send().await {
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
                    let headers = response
                        .headers()
                        .iter()
                        .map(|(k, v)| {
                            (k.to_string(), String::from_utf8_lossy(v.as_bytes()).into_owned())
                        })
                        .collect::<HashMap<_, _>>();
                    // Non-2xx bodies are returned, not raised — scrapers often
                    // want to inspect 404/403 pages; apps decide via is_success().
                    // Streamed with a hard size cap so one huge/hostile body can't
                    // balloon memory (over-limit => a typed error naming cap + URL).
                    let body = read_body_capped(response, cap, &req.url).await?;
                    return Ok(HttpResponse { status, headers, body, final_url, cache_hit: false });
                }
                Err(e) => {
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
/// overflow. Decoded lossily as UTF-8 (matches the prior `.text()` fallback for
/// non-UTF-8 bytes; charset-from-header detection is not performed).
async fn read_body_capped(mut response: reqwest::Response, cap: u64, url: &str) -> Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| Error::Http(e.to_string()))? {
        if would_exceed_cap(buf.len() as u64, chunk.len() as u64, cap) {
            return Err(Error::Http(format!(
                "response body from {url} exceeds max_body_bytes cap of {cap} bytes"
            )));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Whether appending `chunk_len` bytes to a `current_len`-byte buffer would
/// exceed `cap`. Split out for unit testing the streaming cap decision without a
/// live server (the cap check `read_body_capped` performs per chunk).
fn would_exceed_cap(current_len: u64, chunk_len: u64, cap: u64) -> bool {
    current_len + chunk_len > cap
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
/// `seed` exactly like the governor.
fn retry_delay(attempt: u32, retry_after: Option<Duration>, base_ms: u64, seed: u64) -> Duration {
    let exp = attempt.saturating_sub(1).min(20); // cap the shift; 2^20 ms ≈ 17min
    let backoff = Duration::from_millis(base_ms.saturating_mul(2u64.saturating_pow(exp)));
    let floor = backoff.max(retry_after.unwrap_or(Duration::ZERO));
    // Deterministic LCG scramble of the seed -> fraction in [0,1).
    let scrambled = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let frac = (scrambled >> 33) as f64 / (1u64 << 31) as f64;
    floor + floor.mul_f64(RETRY_JITTER_FRAC * frac.min(1.0))
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
        return Some(chrono::DateTime::from_naive_utc_and_offset(naive, chrono::Utc));
    }
    chrono::DateTime::parse_from_rfc2822(raw)
        .ok()
        .map(|d| d.with_timezone(&chrono::Utc))
}

#[async_trait]
impl HttpClient for HttpEngine {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let cache_key = Self::cacheable(&req).then(|| HttpCache::key(&req));
        if let Some(key) = &cache_key {
            // ttl_override caps read staleness too, not just storage TTL: a reader
            // asking for <=N-second-old content must not be handed a longer-lived
            // entry written by another caller.
            let max_age = req.ttl_override.map(Duration::from_secs);
            if let Some(hit) = self.cache.get(key, max_age).await? {
                debug!(url = %req.url, "cache hit");
                return Ok(hit);
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
            let ttl = req
                .ttl_override
                .map(Duration::from_secs)
                .unwrap_or_else(|| self.cache.default_ttl());
            self.cache.put(key, &req.url, &response, ttl).await?;
        }
        Ok(response)
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
        assert!(!HttpEngine::cacheable(&req), "no_cache must bypass the cache");
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
        assert_eq!(retry_after_value("120", now), Some(Duration::from_secs(120)));
        // Clamped to 10 minutes.
        assert_eq!(retry_after_value("99999", now), Some(Duration::from_secs(600)));
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
        assert!(d >= Duration::from_secs(5), "Retry-After should dominate: {d:?}");
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
        let mut cfg = HttpConfig::default();
        cfg.proxy = Some("http://gw:8080".into());
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
        assert!(pool.clients.get("p1").is_none(), "LRU entry evicted");
        assert!(pool.get("pN").is_some(), "newest entry present");
    }

    #[test]
    fn pool_key_separates_proxy_and_profile_dimensions() {
        // The same proxy under two profiles => two clients; the same profile
        // behind two proxies => two clients; and no cross-field collision.
        assert_ne!(pool_key(Some("http://gw"), None), pool_key(Some("http://gw"), Some("a")));
        assert_ne!(pool_key(None, Some("a")), pool_key(Some("http://gw"), Some("a")));
        assert_ne!(pool_key(Some("a"), Some("b")), pool_key(Some("ab"), None));
        assert_ne!(pool_key(None, None), pool_key(None, Some("a")));
        // Stable for the same pair (a pooled client is actually reused).
        assert_eq!(pool_key(Some("p"), Some("a")), pool_key(Some("p"), Some("a")));
    }

    #[test]
    fn profiled_requests_never_touch_the_shared_cache() {
        // The http_cache key ignores `profile`, so a logged-in body must never be
        // cached (it would be served to anonymous callers).
        let mut req = HttpRequest::get("https://example.com/");
        assert!(HttpEngine::cacheable(&req));
        req.profile = Some("acme".into());
        assert!(!HttpEngine::cacheable(&req), "profiled fetches must bypass the cache");
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
        assert!(beta.cookies(&url).is_none(), "profiles do not share cookies");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn jar_load_rejects_an_unsafe_profile_name() {
        // Validation happens before any path is built (typed Profile error).
        let err = profile_cookies_path(std::path::Path::new("data/profiles"), "../etc")
            .unwrap_err();
        assert!(matches!(err, Error::Profile(_)));
    }
}
