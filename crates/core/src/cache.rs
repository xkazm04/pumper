//! Content-addressed HTTP response cache with per-entry TTL, backed by SQLite.
//! Keyed by (method, url, body) so identical fetches — from re-runs, tiered
//! escalation, or several apps hitting the same endpoint — are served from
//! disk instead of the network.

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

use crate::config::CacheConfig;
use crate::engine::{HttpRequest, HttpResponse, ResearchOutput, ResearchRequest};
use crate::Result;

pub struct HttpCache {
    pool: SqlitePool,
    enabled: bool,
    default_ttl: Duration,
}

impl HttpCache {
    pub fn new(pool: SqlitePool, cfg: &CacheConfig) -> Self {
        Self {
            pool,
            enabled: cfg.enabled,
            default_ttl: Duration::from_secs(cfg.ttl_secs),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn default_ttl(&self) -> Duration {
        self.default_ttl
    }

    /// Stable cache key for a request. Covers every input that varies the
    /// response: method, url, body, **request headers** (content negotiation via
    /// `Accept`/`Accept-Language`, etc.) and **proxy** (geo-variant egress).
    /// Headers are sorted first — `HashMap` iteration order is nondeterministic
    /// and would otherwise scatter the key for identical requests across runs.
    pub fn key(req: &HttpRequest) -> String {
        let mut hasher = Sha256::new();
        hasher.update(format!("{:?}", req.method).as_bytes());
        hasher.update([0]);
        hasher.update(req.url.as_bytes());
        hasher.update([0]);
        if let Some(body) = &req.body {
            hasher.update(body.as_bytes());
        }
        hasher.update([0]);
        let mut headers: Vec<(&String, &String)> = req.headers.iter().collect();
        headers.sort();
        for (k, v) in headers {
            hasher.update(k.as_bytes());
            hasher.update([1]);
            hasher.update(v.as_bytes());
            hasher.update([0]);
        }
        hasher.update([0]);
        if let Some(proxy) = &req.proxy {
            hasher.update(proxy.as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    /// Returns a live (non-expired) cached response, if any. `max_age` caps read
    /// staleness: an entry created more than `max_age` ago is treated as a miss
    /// even if its stored TTL has not expired — so a short-TTL reader is never
    /// served a long-TTL writer's stale body (the two-watches-on-one-endpoint
    /// case). `None` means "any live entry".
    pub async fn get(&self, key: &str, max_age: Option<Duration>) -> Result<Option<HttpResponse>> {
        if !self.enabled {
            return Ok(None);
        }
        let now = Utc::now();
        let min_created = max_age
            .and_then(|d| chrono::Duration::from_std(d).ok())
            .map(|d| ts(now - d));
        let row: Option<(i64, String, String, String)> = sqlx::query_as(
            "SELECT status, headers, body, final_url FROM http_cache \
             WHERE key = ?1 AND expires_at > ?2 AND (?3 IS NULL OR created_at > ?3)",
        )
        .bind(key)
        .bind(ts(now))
        .bind(min_created)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(status, headers, body, final_url)| HttpResponse {
            status: status as u16,
            headers: serde_json::from_str(&headers).unwrap_or_default(),
            body,
            final_url,
            cache_hit: true,
        }))
    }

    /// Stores a response under `key`. Only 2xx responses are cached.
    pub async fn put(&self, key: &str, url: &str, resp: &HttpResponse, ttl: Duration) -> Result<()> {
        if !self.enabled || !resp.is_success() {
            return Ok(());
        }
        let now = Utc::now();
        let expires = now + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::hours(1));
        let headers = serde_json::to_string(&resp.headers)?;
        sqlx::query(
            "INSERT INTO http_cache (key, url, status, headers, body, final_url, created_at, expires_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT(key) DO UPDATE SET status = excluded.status, headers = excluded.headers, \
             body = excluded.body, final_url = excluded.final_url, created_at = excluded.created_at, \
             expires_at = excluded.expires_at",
        )
        .bind(key)
        .bind(url)
        .bind(resp.status as i64)
        .bind(headers)
        .bind(&resp.body)
        .bind(&resp.final_url)
        .bind(ts(now))
        .bind(ts(expires))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Deletes expired entries; returns the number removed.
    pub async fn purge_expired(&self) -> Result<u64> {
        let result = sqlx::query("DELETE FROM http_cache WHERE expires_at <= ?1")
            .bind(ts(Utc::now()))
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(url: &str) -> HttpRequest {
        serde_json::from_value(serde_json::json!({ "url": url })).unwrap()
    }

    #[test]
    fn cache_key_varies_on_headers_and_proxy_and_is_stable() {
        let base = req("https://x.test/a");
        let k = HttpCache::key(&base);
        // Stable across identical requests (and across HashMap orderings).
        assert_eq!(k, HttpCache::key(&req("https://x.test/a")));
        // Content-negotiation headers change the response → change the identity.
        let mut with_lang = base.clone();
        with_lang.headers.insert("Accept-Language".into(), "cs".into());
        assert_ne!(k, HttpCache::key(&with_lang));
        // Proxy (geo-variant egress) changes the identity.
        let mut with_proxy = base.clone();
        with_proxy.proxy = Some("http://eu.proxy:8080".into());
        assert_ne!(k, HttpCache::key(&with_proxy));
    }
}

/// Cost-aware cache for Claude research runs. Research spends real money, so
/// identical requests within the TTL are served from disk. Keyed by every
/// answer-shaping field of the request; `resume_session` requests bypass the
/// cache entirely (they are stateful by design). TTL 0 disables.
pub struct ResearchCache {
    pool: SqlitePool,
    ttl: Duration,
}

impl ResearchCache {
    pub fn new(pool: SqlitePool, ttl_secs: u64) -> Self {
        Self { pool, ttl: Duration::from_secs(ttl_secs) }
    }

    pub fn enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    /// Stable cache key over the fields that shape the answer.
    pub fn key(req: &ResearchRequest) -> String {
        let mut hasher = Sha256::new();
        for part in [
            req.prompt.as_str(),
            req.append_system_prompt.as_deref().unwrap_or(""),
            req.role.as_deref().unwrap_or(""),
            req.model.as_deref().unwrap_or(""),
            req.effort.as_deref().unwrap_or(""),
        ] {
            hasher.update(part.as_bytes());
            hasher.update([0]);
        }
        hasher.update(req.max_turns.map(|t| t.to_string()).unwrap_or_default().as_bytes());
        hasher.update([0]);
        if let Some(schema) = &req.json_schema {
            hasher.update(schema.to_string().as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    /// Fresh cached output, if any. The returned `cost_usd` is the ORIGINAL
    /// run's spend (what the hit saved), not this run's.
    pub async fn get(&self, key: &str) -> Result<Option<ResearchOutput>> {
        if !self.enabled() {
            return Ok(None);
        }
        let row: Option<(String, Option<String>, Option<f64>)> = sqlx::query_as(
            "SELECT text, json, cost_usd FROM research_cache WHERE key = ?1 AND expires_at > ?2",
        )
        .bind(key)
        .bind(ts(Utc::now()))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(text, json, cost_usd)| ResearchOutput {
            text,
            json: json.as_deref().and_then(|s| serde_json::from_str(s).ok()),
            cost_usd,
            duration_ms: None,
            num_turns: None,
            session_id: None,
        }))
    }

    pub async fn put(&self, key: &str, out: &ResearchOutput) -> Result<()> {
        if !self.enabled() || out.text.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let expires =
            now + chrono::Duration::from_std(self.ttl).unwrap_or(chrono::Duration::hours(24));
        sqlx::query(
            "INSERT INTO research_cache (key, text, json, cost_usd, created_at, expires_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(key) DO UPDATE SET text = excluded.text, json = excluded.json, \
             cost_usd = excluded.cost_usd, created_at = excluded.created_at, \
             expires_at = excluded.expires_at",
        )
        .bind(key)
        .bind(&out.text)
        .bind(out.json.as_ref().map(|j| j.to_string()))
        .bind(out.cost_usd)
        .bind(ts(now))
        .bind(ts(expires))
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
