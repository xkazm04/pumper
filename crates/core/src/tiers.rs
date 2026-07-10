//! Self-learning tier router memory. The tiered fetcher escalates
//! http → browser → claude per request, but it forgot everything between
//! requests: a JS-heavy host paid the doomed HTTP attempt (plus politeness
//! spacing) on every single fetch. This store remembers, per host, how often
//! the HTTP tier failed or came back thin; after `STRIKE_LIMIT` consecutive
//! strikes the metered `AppContext::fetch` starts that host at the browser
//! tier. One HTTP win clears the record, so hosts that recover fall back to
//! the cheap path.

use chrono::{SecondsFormat, Utc};
use sqlx::SqlitePool;

use crate::Result;

/// Consecutive HTTP-tier losses before a host prefers the browser tier.
const STRIKE_LIMIT: i64 = 3;

pub struct TierMemory {
    pool: SqlitePool,
}

impl TierMemory {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// The learned starting tier for a host (`Some("browser")` or None).
    pub async fn preferred(&self, host: &str) -> Result<Option<String>> {
        let preferred: Option<Option<String>> =
            sqlx::query_scalar("SELECT preferred FROM tier_memory WHERE host = ?1")
                .bind(host.to_lowercase())
                .fetch_optional(&self.pool)
                .await?;
        Ok(preferred.flatten())
    }

    /// Records one tiered-fetch outcome. An HTTP win resets the host; an HTTP
    /// loss (the trail shows the http tier failed/thin while a higher tier
    /// won) adds a strike, flipping `preferred` to 'browser' at the limit.
    pub async fn record(&self, host: &str, winner: &str, http_lost: bool) -> Result<()> {
        let host = host.to_lowercase();
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
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
            sqlx::query(
                "INSERT INTO tier_memory (host, http_strikes, preferred, updated_at) \
                 VALUES (?1, 1, NULL, ?2) \
                 ON CONFLICT(host) DO UPDATE SET \
                   http_strikes = http_strikes + 1, \
                   preferred = CASE WHEN http_strikes + 1 >= ?3 THEN 'browser' ELSE preferred END, \
                   updated_at = excluded.updated_at",
            )
            .bind(&host)
            .bind(&now)
            .bind(STRIKE_LIMIT)
            .execute(&self.pool)
            .await?;
        }
        // A browser/claude win without an http attempt (skipped or explicit
        // strategy) teaches nothing about the http tier: no write.
        Ok(())
    }
}
