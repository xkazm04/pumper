//! Host profiles: self-learning tier-router memory (v2). The tiered fetcher
//! escalates http → browser → claude per request, but it forgot everything
//! between requests: a JS-heavy host paid the doomed HTTP attempt (plus
//! politeness spacing) on every single fetch. This store remembers, per host,
//! how often the HTTP tier failed or came back thin; after `STRIKE_LIMIT`
//! consecutive strikes the metered `AppContext::fetch` starts that host at the
//! browser tier. One HTTP win clears the record.
//!
//! v2 adds two things:
//! - **Aging** — strikes (and the browser pin) decay after
//!   `[fetcher] host_memory_ttl_secs`, so a host that failed a month ago gets a
//!   fresh crack at the cheap tier instead of staying pinned until a lucky win.
//! - **Penalty persistence** — the governor's learned per-host politeness
//!   penalty is written behind into the same row so it survives a restart, and
//!   the whole learned state is inspectable via `GET /hosts`.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use sqlx::SqlitePool;

use crate::Result;

/// Consecutive HTTP-tier losses before a host prefers the browser tier.
const STRIKE_LIMIT: i64 = 3;

/// Sentinel cutoff used when aging is disabled (`ttl_secs == 0`): every real
/// RFC-3339 timestamp sorts after it, so nothing is ever considered stale.
const NEVER_STALE: &str = "0000-01-01T00:00:00.000000Z";

/// One host's learned state — the row behind `GET /hosts`.
#[derive(Debug, Clone, Serialize)]
pub struct HostProfile {
    pub host: String,
    /// Learned starting tier (`Some("browser")`) or `None` for the default
    /// cheap-first path. Reflects aging: a lapsed pin reads back as `None`.
    pub preferred_tier: Option<String>,
    pub http_strikes: i64,
    /// Learned politeness penalty in ms (the last persisted snapshot; the live
    /// value from the governor is merged in by the API handler).
    pub penalty_ms: i64,
    /// Last time the tier memory (strikes/pin) changed.
    pub updated_at: String,
    /// Last time the penalty snapshot was written, if ever.
    pub penalty_updated_at: Option<String>,
}

pub struct TierMemory {
    pool: SqlitePool,
    /// Strike/pin aging horizon in seconds; `0` disables aging.
    ttl_secs: u64,
}

impl TierMemory {
    pub fn new(pool: SqlitePool, ttl_secs: u64) -> Self {
        Self { pool, ttl_secs }
    }

    /// The cutoff timestamp: rows whose `updated_at` is strictly older are
    /// stale. When aging is disabled, returns a sentinel nothing is older than.
    fn stale_cutoff(&self) -> String {
        if self.ttl_secs == 0 {
            return NEVER_STALE.to_string();
        }
        let cutoff = Utc::now() - chrono::Duration::seconds(self.ttl_secs as i64);
        ts(cutoff)
    }

    /// The learned starting tier for a host (`Some("browser")` or None). A pin
    /// whose strikes have aged past the TTL reads back as `None` — the host is
    /// given a fresh chance at the cheap HTTP tier.
    pub async fn preferred(&self, host: &str) -> Result<Option<String>> {
        let preferred: Option<Option<String>> = sqlx::query_scalar(
            "SELECT preferred FROM tier_memory WHERE host = ?1 AND updated_at >= ?2",
        )
        .bind(host.to_lowercase())
        .bind(self.stale_cutoff())
        .fetch_optional(&self.pool)
        .await?;
        Ok(preferred.flatten())
    }

    /// Records one tiered-fetch outcome. An HTTP win resets the host; an HTTP
    /// loss (the http tier failed/thin while a higher tier won) adds a strike,
    /// flipping `preferred` to 'browser' at the limit. Stale strikes (older than
    /// the TTL) reset to a single fresh strike rather than accumulating — an
    /// aged-out host must earn a fresh pin, not re-pin on one loss.
    pub async fn record(&self, host: &str, winner: &str, http_lost: bool) -> Result<()> {
        let host = host.to_lowercase();
        let now = ts(Utc::now());
        if winner == "http" {
            sqlx::query(
                "INSERT INTO tier_memory (host, http_strikes, preferred, updated_at) \
                 VALUES (?1, 0, NULL, ?2) \
                 ON CONFLICT(host) DO UPDATE SET http_strikes = 0, preferred = NULL, \
                 updated_at = excluded.updated_at",
            )
            .bind(&host)
            .bind(&now)
            .execute(&self.pool)
            .await?;
        } else if http_lost {
            let cutoff = self.stale_cutoff();
            sqlx::query(
                "INSERT INTO tier_memory (host, http_strikes, preferred, updated_at) \
                 VALUES (?1, 1, NULL, ?2) \
                 ON CONFLICT(host) DO UPDATE SET \
                   http_strikes = CASE WHEN updated_at < ?4 THEN 1 ELSE http_strikes + 1 END, \
                   preferred = CASE \
                     WHEN updated_at < ?4 THEN NULL \
                     WHEN http_strikes + 1 >= ?3 THEN 'browser' ELSE preferred END, \
                   updated_at = excluded.updated_at",
            )
            .bind(&host)
            .bind(&now)
            .bind(STRIKE_LIMIT)
            .bind(&cutoff)
            .execute(&self.pool)
            .await?;
        }
        // A browser/claude win without an http attempt (skipped or explicit
        // strategy) teaches nothing about the http tier: no write.
        Ok(())
    }

    /// One host's full profile, or `None` if unknown. Unlike `preferred`, this
    /// does not hide an aged-out pin — diagnostics show the raw stored state
    /// (aging is applied by callers that route on it).
    pub async fn get(&self, host: &str) -> Result<Option<HostProfile>> {
        let row: Option<ProfileRow> = sqlx::query_as(
            "SELECT host, preferred, http_strikes, penalty_ms, updated_at, penalty_updated_at \
             FROM tier_memory WHERE host = ?1",
        )
        .bind(host.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(HostProfile::from))
    }

    /// A page of host profiles, most-recently-active first, keyset-paged by
    /// `(updated_at, host)`. `after` is the previous page's last
    /// `(updated_at, host)` pair.
    pub async fn list_page(
        &self,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<HostProfile>> {
        let (after_ts, after_host) = match after {
            Some((t, h)) => (Some(t), Some(h)),
            None => (None, None),
        };
        let rows: Vec<ProfileRow> = sqlx::query_as(
            "SELECT host, preferred, http_strikes, penalty_ms, updated_at, penalty_updated_at \
             FROM tier_memory \
             WHERE (?1 IS NULL) OR (updated_at < ?1) OR (updated_at = ?1 AND host > ?2) \
             ORDER BY updated_at DESC, host ASC \
             LIMIT ?3",
        )
        .bind(after_ts)
        .bind(after_host)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(HostProfile::from).collect())
    }

    /// Forgets a host: drops its tier memory row (strikes + pin + persisted
    /// penalty snapshot). Returns whether a row existed. The caller also clears
    /// the live in-memory governor penalty.
    pub async fn forget(&self, host: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM tier_memory WHERE host = ?1")
            .bind(host.to_lowercase())
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Write-behind snapshot of the governor's learned penalties. Upserts each
    /// `(host, penalty_ms)` without touching `updated_at` (strike aging) or the
    /// strike/pin columns — a penalty-only host gets a fresh row.
    pub async fn save_penalties(&self, penalties: &[(String, u64)]) -> Result<()> {
        if penalties.is_empty() {
            return Ok(());
        }
        let now = ts(Utc::now());
        let mut tx = self.pool.begin().await?;
        for (host, penalty_ms) in penalties {
            sqlx::query(
                "INSERT INTO tier_memory (host, http_strikes, preferred, updated_at, penalty_ms, penalty_updated_at) \
                 VALUES (?1, 0, NULL, ?2, ?3, ?2) \
                 ON CONFLICT(host) DO UPDATE SET \
                   penalty_ms = excluded.penalty_ms, penalty_updated_at = excluded.penalty_updated_at",
            )
            .bind(host.to_lowercase())
            .bind(&now)
            .bind(*penalty_ms as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Restores persisted penalties on boot: every host with a non-zero learned
    /// penalty, to be seeded back into the in-memory governor.
    pub async fn load_penalties(&self) -> Result<Vec<(String, u64)>> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT host, penalty_ms FROM tier_memory WHERE penalty_ms > 0")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|(h, ms)| (h, ms.max(0) as u64)).collect())
    }
}

#[derive(sqlx::FromRow)]
struct ProfileRow {
    host: String,
    preferred: Option<String>,
    http_strikes: i64,
    penalty_ms: i64,
    updated_at: String,
    penalty_updated_at: Option<String>,
}

impl From<ProfileRow> for HostProfile {
    fn from(r: ProfileRow) -> Self {
        HostProfile {
            host: r.host,
            preferred_tier: r.preferred,
            http_strikes: r.http_strikes,
            penalty_ms: r.penalty_ms,
            updated_at: r.updated_at,
            penalty_updated_at: r.penalty_updated_at,
        }
    }
}

/// Fixed-width RFC 3339 UTC micros — lexicographic order == chronological.
fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}
