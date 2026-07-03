//! Content-addressed HTTP response cache with per-entry TTL, backed by SQLite.
//! Keyed by (method, url, body) so identical fetches — from re-runs, tiered
//! escalation, or several apps hitting the same endpoint — are served from
//! disk instead of the network.

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

use crate::config::CacheConfig;
use crate::engine::{HttpRequest, HttpResponse};
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

    /// Stable cache key for a request.
    pub fn key(req: &HttpRequest) -> String {
        let mut hasher = Sha256::new();
        hasher.update(format!("{:?}", req.method).as_bytes());
        hasher.update([0]);
        hasher.update(req.url.as_bytes());
        hasher.update([0]);
        if let Some(body) = &req.body {
            hasher.update(body.as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    /// Returns a live (non-expired) cached response, if any.
    pub async fn get(&self, key: &str) -> Result<Option<HttpResponse>> {
        if !self.enabled {
            return Ok(None);
        }
        let row: Option<(i64, String, String, String)> = sqlx::query_as(
            "SELECT status, headers, body, final_url FROM http_cache \
             WHERE key = ?1 AND expires_at > ?2",
        )
        .bind(key)
        .bind(ts(Utc::now()))
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(status, headers, body, final_url)| HttpResponse {
            status: status as u16,
            headers: serde_json::from_str(&headers).unwrap_or_default(),
            body,
            final_url,
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
