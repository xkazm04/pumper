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
                    // the governor a longer per-host spacing; anything else
                    // (even a 404) decays a learned penalty back down.
                    if let Some(host) = &host {
                        if matches!(status, 429 | 503) {
                            self.governor.penalize(host, retry_after(&response)).await;
                        } else {
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
                    return Ok(HttpResponse { status, headers, body, final_url });
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

/// Seconds-form `Retry-After` header (the HTTP-date form is rare on rate
/// limiters and simply falls back to the doubling policy).
fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get("retry-after")?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|secs| Duration::from_secs(secs.min(600)))
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
}
