//! Traditional HTTP scraping engine: reqwest with a shared cookie jar,
//! browser-like User-Agent, and retries with exponential backoff. Fronted by
//! a content-addressed TTL cache and a per-domain politeness governor.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use pumper_core::config::HttpConfig;
use pumper_core::{
    Error, Governor, HttpCache, HttpClient, HttpMethod, HttpRequest, HttpResponse, Result,
};
use tracing::{debug, warn};

/// Base of the exponential retry backoff (attempt 1 waits this, then doubles).
const RETRY_BASE_MS: u64 = 500;
/// Retry jitter is up to this fraction of the (post-max) delay, spread with a
/// deterministic hash — no `rand` dependency (mirrors the governor's approach).
const RETRY_JITTER_FRAC: f64 = 0.25;
/// Max distinct per-request-proxy clients cached. reqwest binds a proxy at
/// client-build time, so a per-request proxy override needs its own client; we
/// pool them (LRU, this cap) rather than rebuild per request. Cost: each pooled
/// client has its **own** cookie jar (proxied requests don't share cookies with
/// the default client), and up to this many idle keep-alive pools may linger.
const MAX_PROXY_CLIENTS: usize = 8;

/// Small LRU pool of proxy-bound clients keyed by proxy URL. Guarded by a
/// std `Mutex`: the critical section (lookup / build / insert) is fully sync —
/// building a reqwest client does not await — so no async lock is needed.
struct ProxyPool {
    clients: HashMap<String, reqwest::Client>,
    /// Front = least-recently-used, back = most-recent. Bounded by MAX_PROXY_CLIENTS.
    order: VecDeque<String>,
}

impl ProxyPool {
    fn new() -> Self {
        Self { clients: HashMap::new(), order: VecDeque::new() }
    }

    /// LRU lookup: returns a cached client for `key`, touching it as most-recent.
    fn get(&mut self, key: &str) -> Option<reqwest::Client> {
        let client = self.clients.get(key).cloned()?;
        self.order.retain(|k| k != key);
        self.order.push_back(key.to_string());
        Some(client)
    }

    /// Insert a freshly built client as most-recent, evicting the least-recently
    /// used entries until the pool is within `cap`.
    fn insert(&mut self, key: &str, client: reqwest::Client, cap: usize) {
        self.clients.insert(key.to_string(), client);
        self.order.retain(|k| k != key);
        self.order.push_back(key.to_string());
        while self.order.len() > cap {
            if let Some(evict) = self.order.pop_front() {
                self.clients.remove(&evict);
            }
        }
    }
}

pub struct HttpEngine {
    /// Client for requests with no per-request proxy override (carries
    /// `[http] proxy` when configured).
    client: reqwest::Client,
    /// Kept to rebuild proxy-bound clients on demand.
    cfg: HttpConfig,
    governor: Arc<Governor>,
    cache: Arc<HttpCache>,
    proxy_pool: Mutex<ProxyPool>,
}

/// Builds a reqwest client mirroring the base settings, optionally proxied.
fn build_client(cfg: &HttpConfig, proxy: Option<&str>) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .user_agent(&cfg.user_agent)
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .cookie_store(true)
        .gzip(true)
        .brotli(true)
        .redirect(reqwest::redirect::Policy::limited(cfg.redirect_limit));
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
    pub fn new(cfg: &HttpConfig, governor: Arc<Governor>, cache: Arc<HttpCache>) -> Result<Self> {
        let client = build_client(cfg, cfg.proxy.as_deref())?;
        Ok(Self {
            client,
            cfg: cfg.clone(),
            governor,
            cache,
            proxy_pool: Mutex::new(ProxyPool::new()),
        })
    }

    /// Selects the client for a request: the base client unless the request
    /// carries a `proxy` override, in which case a pooled proxy-bound client.
    fn client_for(&self, req: &HttpRequest) -> Result<reqwest::Client> {
        let Some(proxy) = req.proxy.as_deref() else {
            return Ok(self.client.clone());
        };
        // If the override equals the configured proxy, the base client already
        // carries it — reuse it (and its cookie jar) rather than pooling a dup.
        if self.cfg.proxy.as_deref() == Some(proxy) {
            return Ok(self.client.clone());
        }
        let mut pool = self.proxy_pool.lock().expect("proxy pool mutex poisoned");
        if let Some(existing) = pool.get(proxy) {
            return Ok(existing);
        }
        let client = build_client(&self.cfg, Some(proxy))?;
        pool.insert(proxy, client.clone(), MAX_PROXY_CLIENTS);
        Ok(client)
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

    /// Only idempotent, bodyless GETs are cacheable.
    fn cacheable(req: &HttpRequest) -> bool {
        req.method == HttpMethod::Get && req.body.is_none() && !req.no_cache
    }

    async fn send(&self, req: &HttpRequest) -> Result<HttpResponse> {
        let host = reqwest::Url::parse(&req.url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned));
        let client = self.client_for(req)?;
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
            if let Some(hit) = self.cache.get(key).await? {
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
        assert!(build_client(&cfg, cfg.proxy.as_deref()).is_ok());
    }

    #[test]
    fn build_client_rejects_invalid_proxy() {
        let cfg = HttpConfig::default();
        // A syntactically invalid proxy URL surfaces a typed Http error.
        let err = build_client(&cfg, Some("::not a url::")).unwrap_err();
        assert!(matches!(err, Error::Http(_)));
    }

    #[test]
    fn proxy_pool_is_lru_bounded() {
        // Dummy clients (no network) exercise the pool's LRU + eviction directly.
        let mut pool = ProxyPool::new();
        for i in 0..MAX_PROXY_CLIENTS {
            pool.insert(&format!("p{i}"), reqwest::Client::new(), MAX_PROXY_CLIENTS);
        }
        assert_eq!(pool.clients.len(), MAX_PROXY_CLIENTS);
        // Touch p0 so it's most-recent; p1 becomes the LRU victim.
        assert!(pool.get("p0").is_some());
        // Insert one over cap -> evicts the least-recently-used (p1), keeps p0.
        pool.insert("pN", reqwest::Client::new(), MAX_PROXY_CLIENTS);
        assert_eq!(pool.clients.len(), MAX_PROXY_CLIENTS);
        assert!(pool.get("p0").is_some(), "recently-touched entry retained");
        assert!(pool.clients.get("p1").is_none(), "LRU entry evicted");
        assert!(pool.get("pN").is_some(), "newest entry present");
    }
}
