//! Content-addressed HTTP response cache with per-entry TTL, backed by SQLite.
//! Keyed by (method, url, body) so identical fetches — from re-runs, tiered
//! escalation, or several apps hitting the same endpoint — are served from
//! disk instead of the network.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

use crate::config::CacheConfig;
use crate::engine::{HttpRequest, HttpResponse, ResearchOutput, ResearchRequest};
use crate::Result;

/// A cached entry returned by [`HttpCache::get_stale`]: the stored response plus
/// the revalidation validators pulled from its headers.
pub struct StaleEntry {
    pub response: HttpResponse,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

/// Rows one [`HttpCache::evict_over_cap`] pass may delete. The janitor shares
/// its pool with live fetches, so a cache that is wildly over its cap converges
/// over a few hourly passes instead of holding one enormous write transaction.
const EVICT_MAX_PER_PASS: i64 = 5_000;

pub struct HttpCache {
    pool: SqlitePool,
    enabled: bool,
    default_ttl: Duration,
    /// `[cache] max_rows`: hard ceiling on stored entries (`0` = unbounded).
    max_rows: u64,
}

impl HttpCache {
    pub fn new(pool: SqlitePool, cfg: &CacheConfig) -> Self {
        Self {
            pool,
            enabled: cfg.enabled,
            default_ttl: Duration::from_secs(cfg.ttl_secs),
            max_rows: cfg.max_rows,
        }
    }

    /// The configured row ceiling (`0` = unbounded) — the janitor reads it here
    /// rather than re-deriving it from config, so the store and its bound can
    /// never disagree.
    pub fn max_rows(&self) -> u64 {
        self.max_rows
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

    /// Returns a cached entry **regardless of expiry** plus its stored
    /// revalidation validators (`ETag` / `Last-Modified`, read case-insensitively
    /// out of the response headers). Used after [`get`] misses to turn an
    /// expired-but-maybe-still-valid entry into a cheap conditional GET instead of
    /// a full re-download. `None` when nothing is stored under `key`.
    pub async fn get_stale(&self, key: &str) -> Result<Option<StaleEntry>> {
        if !self.enabled {
            return Ok(None);
        }
        let row: Option<(i64, String, String, String)> = sqlx::query_as(
            "SELECT status, headers, body, final_url FROM http_cache WHERE key = ?1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        let Some((status, headers_json, body, final_url)) = row else {
            return Ok(None);
        };
        let headers: HashMap<String, String> =
            serde_json::from_str(&headers_json).unwrap_or_default();
        let find = |name: &str| {
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        let etag = find("etag");
        let last_modified = find("last-modified");
        Ok(Some(StaleEntry {
            response: HttpResponse {
                status: status as u16,
                headers,
                body,
                final_url,
                cache_hit: true,
            },
            etag,
            last_modified,
        }))
    }

    /// Extends a still-valid entry's life without rewriting its body — called on a
    /// `304 Not Modified` revalidation. Moves `created_at` forward too, so the
    /// `max_age` read-staleness cap keeps measuring from the last *confirmed* fetch.
    ///
    /// A refresh IS a revalidation observation (the origin confirmed "unchanged"),
    /// so it also appends a `changed = 0` row to the revalidation log — the one
    /// place every 304 path (demand-side engine revalidate + background refresher)
    /// already flows through. The changed-body counterpart is recorded explicitly
    /// by those callers via [`record_revalidation`](Self::record_revalidation).
    pub async fn refresh(&self, key: &str, ttl: Duration) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let now = Utc::now();
        let expires = now + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::hours(1));
        sqlx::query("UPDATE http_cache SET expires_at = ?2, created_at = ?3 WHERE key = ?1")
            .bind(key)
            .bind(ts(expires))
            .bind(ts(now))
            .execute(&self.pool)
            .await?;
        self.record_revalidation(key, false).await;
        Ok(())
    }

    /// Appends one revalidation observation for `key`: `changed = true` when the
    /// conditional GET came back with a new body, `false` on a 304. Best-effort
    /// telemetry — a failed insert warns and never fails the fetch path.
    pub async fn record_revalidation(&self, key: &str, changed: bool) {
        if !self.enabled {
            return;
        }
        let result =
            sqlx::query("INSERT INTO revalidations (key, checked_at, changed) VALUES (?1, ?2, ?3)")
                .bind(key)
                .bind(ts(Utc::now()))
                .bind(changed as i64)
                .execute(&self.pool)
                .await;
        if let Err(e) = result {
            tracing::warn!(key = %key, "revalidation log insert failed: {e}");
        }
    }

    /// Drops revalidation observations older than `retention_days` (indexed on
    /// `checked_at`). Returns rows removed. Called by the refresher tick so the
    /// append-only log stays bounded.
    pub async fn prune_revalidations(&self, retention_days: u32) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::days(retention_days.max(1) as i64);
        let result = sqlx::query("DELETE FROM revalidations WHERE checked_at < ?1")
            .bind(ts(cutoff))
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Per-key freshness model over the revalidation log, joined against the
    /// entries still present in `http_cache` (an evicted/never-stored key has
    /// nothing to refresh). Keys with the shortest time-to-predicted-change come
    /// first. `limit` caps the returned keys; the underlying row read is bounded
    /// by [`FRESHNESS_MAX_ROWS`].
    pub async fn freshness(&self, now: DateTime<Utc>, limit: usize) -> Result<Vec<KeyFreshness>> {
        // Bounded, ordered read: fold rows per key in one pass.
        let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
            "SELECT r.key, h.url, r.checked_at, r.changed FROM revalidations r \
             JOIN http_cache h ON h.key = r.key \
             ORDER BY r.key, r.checked_at LIMIT ?1",
        )
        .bind(FRESHNESS_MAX_ROWS)
        .fetch_all(&self.pool)
        .await?;

        // Fallback prior for keys with fewer than two observed changes: the
        // configured default TTL (the operator's own staleness guess).
        let prior_secs = self.default_ttl.as_secs_f64().max(60.0);
        let mut out: Vec<KeyFreshness> = Vec::new();
        let mut cur: Option<FreshnessFold> = None;
        for (key, url, checked_at, changed) in rows {
            let t = chrono::DateTime::parse_from_rfc3339(&checked_at)
                .map(|d| d.with_timezone(&Utc))
                .ok();
            let Some(t) = t else { continue };
            if cur.as_ref().is_none_or(|f| f.key != key) {
                if let Some(f) = cur.take() {
                    out.push(f.finish(now, prior_secs));
                }
                cur = Some(FreshnessFold::new(key, url));
            }
            if let Some(f) = cur.as_mut() {
                f.observe(t, changed != 0);
            }
        }
        if let Some(f) = cur.take() {
            out.push(f.finish(now, prior_secs));
        }
        // Most-urgent first: smallest predicted time-to-change at the top.
        out.sort_by(|a, b| {
            a.due_in_secs
                .partial_cmp(&b.due_in_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out.truncate(limit);
        Ok(out)
    }

    /// Stores a response under `key`. Only 2xx responses are cached.
    pub async fn put(
        &self,
        key: &str,
        url: &str,
        resp: &HttpResponse,
        ttl: Duration,
    ) -> Result<()> {
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

    /// Enforces the `[cache] max_rows` ceiling, evicting **oldest-confirmed
    /// first**. Returns rows removed; `0` rows for an unbounded (`max_rows = 0`)
    /// or under-cap store.
    ///
    /// The anti-pattern this exists to defend
    /// (`http_cache_row_cap_evicts_oldest_not_freshest`): expiry was the only
    /// bound this table had, and [`refresh`](Self::refresh) pushes `expires_at`
    /// out on every 304 — so a continuously-revalidated entry never expired and
    /// `pumper.db` grew monotonically on an unattended box.
    ///
    /// Age is measured on `created_at`, which `refresh` also moves forward, so
    /// eviction picks the entries whose bodies were confirmed *least recently*
    /// — the refresher keeps what it works on alive rather than fighting the
    /// janitor for it. Bounded at [`EVICT_MAX_PER_PASS`] deletions per call
    /// (the count is served from the `expires_at` index; the eviction itself is
    /// a `LIMIT`ed subselect, never a whole-table rewrite).
    pub async fn evict_over_cap(&self) -> Result<u64> {
        if self.max_rows == 0 {
            return Ok(0);
        }
        let max_rows = self.max_rows.min(i64::MAX as u64) as i64;
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM http_cache")
            .fetch_one(&self.pool)
            .await?;
        let over = rows - max_rows;
        if over <= 0 {
            return Ok(0);
        }
        let result = sqlx::query(
            "DELETE FROM http_cache WHERE key IN ( \
               SELECT key FROM http_cache ORDER BY created_at ASC, key ASC LIMIT ?1)",
        )
        .bind(over.min(EVICT_MAX_PER_PASS))
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

// ── revalidation freshness model (M02) ──────────────────────────────────────

/// Cap on revalidation rows folded per freshness pass — bounds the query even
/// on a long-lived mirror with a large log.
const FRESHNESS_MAX_ROWS: i64 = 50_000;

/// EWMA smoothing factor for observed inter-change gaps. 0.3 weighs the newest
/// gap enough to track a cadence shift within a few observations without letting
/// one outlier (a flash edit, a long weekend) whipsaw the estimate.
const EWMA_ALPHA: f64 = 0.3;

/// One key's learned freshness state, derived from its revalidation history.
#[derive(Debug, Clone, serde::Serialize)]
pub struct KeyFreshness {
    pub key: String,
    pub url: String,
    /// Total revalidations observed (both outcomes).
    pub checks: u64,
    /// Revalidations that found a changed body.
    pub changes: u64,
    pub last_checked_at: DateTime<Utc>,
    /// Last time a change was observed (`None` = never seen changed).
    pub last_change_at: Option<DateTime<Utc>>,
    /// EWMA of observed inter-change gaps (seconds). `None` until two changes
    /// have been observed — the prior (default TTL) fills in for prediction.
    pub interval_secs: Option<f64>,
    /// Predicted next-change time: `last_change_at` (else last check) + the
    /// estimated interval (learned EWMA, else the configured-TTL prior).
    pub predicted_next_change: DateTime<Utc>,
    /// Seconds until (negative = past) the predicted next change.
    pub due_in_secs: f64,
}

/// Streaming fold of one key's ordered revalidation rows into a [`KeyFreshness`].
struct FreshnessFold {
    key: String,
    url: String,
    checks: u64,
    changes: u64,
    last_checked_at: Option<DateTime<Utc>>,
    last_change_at: Option<DateTime<Utc>>,
    ewma_secs: Option<f64>,
}

impl FreshnessFold {
    fn new(key: String, url: String) -> Self {
        Self {
            key,
            url,
            checks: 0,
            changes: 0,
            last_checked_at: None,
            last_change_at: None,
            ewma_secs: None,
        }
    }

    fn observe(&mut self, at: DateTime<Utc>, changed: bool) {
        self.checks += 1;
        self.last_checked_at = Some(at);
        if changed {
            self.changes += 1;
            if let Some(prev) = self.last_change_at {
                let gap = (at - prev).num_seconds().max(1) as f64;
                self.ewma_secs = Some(ewma_update(self.ewma_secs, gap));
            }
            self.last_change_at = Some(at);
        }
    }

    fn finish(self, now: DateTime<Utc>, prior_secs: f64) -> KeyFreshness {
        let last_checked_at = self.last_checked_at.unwrap_or(now);
        let interval = self.ewma_secs.unwrap_or(prior_secs).max(1.0);
        // Predict from the last observed change; a never-changed key predicts
        // from its last check (the best "unchanged since" anchor we have).
        let anchor = self.last_change_at.unwrap_or(last_checked_at);
        let predicted = anchor + chrono::Duration::seconds(interval as i64);
        KeyFreshness {
            key: self.key,
            url: self.url,
            checks: self.checks,
            changes: self.changes,
            last_checked_at,
            last_change_at: self.last_change_at,
            interval_secs: self.ewma_secs,
            predicted_next_change: predicted,
            due_in_secs: (predicted - now).num_milliseconds() as f64 / 1000.0,
        }
    }
}

/// One EWMA step over inter-change gaps: `None` prior seeds with the first
/// observed gap; afterwards `alpha·gap + (1-alpha)·prev`.
pub fn ewma_update(prev: Option<f64>, gap_secs: f64) -> f64 {
    match prev {
        None => gap_secs,
        Some(p) => EWMA_ALPHA * gap_secs + (1.0 - EWMA_ALPHA) * p,
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
        Self {
            pool,
            ttl: Duration::from_secs(ttl_secs),
        }
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
        hasher.update(
            req.max_turns
                .map(|t| t.to_string())
                .unwrap_or_default()
                .as_bytes(),
        );
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

    /// Deletes entries past their TTL; returns the number removed.
    ///
    /// Research answers are the most expensive bytes in the store (they cost
    /// real money to produce) and were also the only cache with **no purge path
    /// at all** — an expired row was unreadable via [`get`](Self::get) yet kept
    /// its full text and JSON on disk forever. Same shape and same janitor
    /// cadence as `HttpCache::purge_expired`, and indexed on `expires_at`.
    pub async fn purge_expired(&self) -> Result<u64> {
        let result = sqlx::query("DELETE FROM research_cache WHERE expires_at <= ?1")
            .bind(ts(Utc::now()))
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(url: &str) -> HttpRequest {
        serde_json::from_value(serde_json::json!({ "url": url })).unwrap()
    }

    use chrono::TimeZone;

    #[test]
    fn ewma_seeds_with_first_gap_then_smooths() {
        assert_eq!(ewma_update(None, 100.0), 100.0);
        // alpha 0.3: 0.3*200 + 0.7*100 = 130.
        let second = ewma_update(Some(100.0), 200.0);
        assert!((second - 130.0).abs() < 1e-9, "{second}");
        // An outlier moves the estimate but doesn't replace it.
        let third = ewma_update(Some(130.0), 10_000.0);
        assert!(third > 130.0 && third < 10_000.0);
    }

    #[test]
    fn freshness_fold_learns_interval_and_predicts_next_change() {
        let t = |h: u32| Utc.with_ymd_and_hms(2026, 7, 30, h, 0, 0).unwrap();
        let mut f = FreshnessFold::new("k".into(), "https://x.test/a".into());
        // Changes at 00:00 and 02:00 (gap 7200s), unchanged checks between.
        f.observe(t(0), true);
        f.observe(t(1), false);
        f.observe(t(2), true);
        f.observe(t(3), false);
        let now = t(3);
        let out = f.finish(now, 999_999.0);
        assert_eq!(out.checks, 4);
        assert_eq!(out.changes, 2);
        assert_eq!(out.last_change_at, Some(t(2)));
        // One observed gap: EWMA seeds at 7200s — the prior is NOT used.
        assert_eq!(out.interval_secs, Some(7200.0));
        assert_eq!(out.predicted_next_change, t(4));
        assert!(
            (out.due_in_secs - 3600.0).abs() < 1.0,
            "{}",
            out.due_in_secs
        );
    }

    #[test]
    fn freshness_fold_never_changed_key_uses_prior_from_last_check() {
        let t0 = Utc.with_ymd_and_hms(2026, 7, 30, 0, 0, 0).unwrap();
        let mut f = FreshnessFold::new("k".into(), "https://x.test/a".into());
        f.observe(t0, false);
        let out = f.finish(t0, 3600.0);
        assert_eq!(out.changes, 0);
        assert_eq!(out.interval_secs, None, "no learned interval yet");
        // Prior anchors on the last check: predicted = t0 + 3600s.
        assert_eq!(out.predicted_next_change, t0 + chrono::Duration::hours(1));
    }

    #[test]
    fn cache_key_varies_on_headers_and_proxy_and_is_stable() {
        let base = req("https://x.test/a");
        let k = HttpCache::key(&base);
        // Stable across identical requests (and across HashMap orderings).
        assert_eq!(k, HttpCache::key(&req("https://x.test/a")));
        // Content-negotiation headers change the response → change the identity.
        let mut with_lang = base.clone();
        with_lang
            .headers
            .insert("Accept-Language".into(), "cs".into());
        assert_ne!(k, HttpCache::key(&with_lang));
        // Proxy (geo-variant egress) changes the identity.
        let mut with_proxy = base.clone();
        with_proxy.proxy = Some("http://eu.proxy:8080".into());
        assert_ne!(k, HttpCache::key(&with_proxy));
    }
}
