//! Traditional HTTP scraping engine: reqwest with a shared cookie jar,
//! browser-like User-Agent, and retries with exponential backoff. Fronted by
//! a content-addressed TTL cache and a per-domain politeness governor.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pumper_core::config::HttpConfig;
use pumper_core::{
    Error, Governor, HttpCache, HttpClient, HttpMethod, HttpRequest, HttpResponse, Result,
};
use tracing::{debug, warn};

const RETRYABLE_STATUS: [u16; 4] = [429, 502, 503, 504];

pub struct HttpEngine {
    client: reqwest::Client,
    retries: u32,
    governor: Arc<Governor>,
    cache: Arc<HttpCache>,
}

impl HttpEngine {
    pub fn new(cfg: &HttpConfig, governor: Arc<Governor>, cache: Arc<HttpCache>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(&cfg.user_agent)
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .cookie_store(true)
            .gzip(true)
            .brotli(true)
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .map_err(|e| Error::Http(e.to_string()))?;
        Ok(Self { client, retries: cfg.retries, governor, cache })
    }

    fn build(&self, req: &HttpRequest) -> reqwest::RequestBuilder {
        let mut builder = match req.method {
            HttpMethod::Get => self.client.get(&req.url),
            HttpMethod::Post => self.client.post(&req.url),
        };
        for (key, value) in &req.headers {
            builder = builder.header(key, value);
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

        let mut last_error = String::new();
        for attempt in 0..=self.retries {
            if attempt > 0 {
                let backoff = Duration::from_millis(500 * 2u64.pow(attempt - 1));
                debug!(url = %req.url, attempt, "retrying in {backoff:?} ({last_error})");
                tokio::time::sleep(backoff).await;
            }
            // Politeness spacing is applied per attempt so retries also wait.
            if let Some(host) = &host {
                self.governor.acquire(host).await;
            }
            match self.build(req).send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    // Adaptive politeness: rate-limit/overload responses teach
                    // the governor a longer per-host spacing; only a genuinely
                    // healthy (2xx) response decays a learned penalty. A 4xx
                    // (e.g. 404/403) is NOT health — it must not reward the host
                    // with faster spacing — and other 5xx are left neutral.
                    if let Some(host) = &host {
                        if matches!(status, 429 | 503) {
                            self.governor.penalize(host, retry_after(&response)).await;
                        } else if (200..300).contains(&status) {
                            self.governor.reward(host).await;
                        }
                    }
                    if RETRYABLE_STATUS.contains(&status) && attempt < self.retries {
                        warn!(url = %req.url, status, "retryable status");
                        last_error = format!("status {status}");
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
                    let body = response.text().await.map_err(|e| Error::Http(e.to_string()))?;
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
            self.retries + 1
        )))
    }
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

        if let Some(key) = &cache_key {
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
}
