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
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::Result;

/// Consecutive HTTP-tier losses before a host prefers the browser tier.
const STRIKE_LIMIT: i64 = 3;

/// Ceiling on an *imported* politeness penalty (host-weather, M01). Well below
/// the governor's own 300s learned cap: remote intel is a prior, not truth — a
/// peer (or a poisoned bundle) must not be able to park a host behind minutes
/// of spacing that only local 429s should earn.
pub const WEATHER_IMPORT_PENALTY_CAP_MS: u64 = 60_000;

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
    /// Tiered-fetch outcomes recorded locally for this host (0033) — the
    /// evidence weight behind host-weather export floors and import merges.
    pub observations: i64,
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
                "INSERT INTO tier_memory (host, http_strikes, preferred, updated_at, observations) \
                 VALUES (?1, 0, NULL, ?2, 1) \
                 ON CONFLICT(host) DO UPDATE SET http_strikes = 0, preferred = NULL, \
                 updated_at = excluded.updated_at, observations = observations + 1",
            )
            .bind(&host)
            .bind(&now)
            .execute(&self.pool)
            .await?;
        } else if http_lost {
            let cutoff = self.stale_cutoff();
            sqlx::query(
                "INSERT INTO tier_memory (host, http_strikes, preferred, updated_at, observations) \
                 VALUES (?1, 1, NULL, ?2, 1) \
                 ON CONFLICT(host) DO UPDATE SET \
                   http_strikes = CASE WHEN updated_at < ?4 THEN 1 ELSE http_strikes + 1 END, \
                   preferred = CASE \
                     WHEN updated_at < ?4 THEN NULL \
                     WHEN http_strikes + 1 >= ?3 THEN 'browser' ELSE preferred END, \
                   updated_at = excluded.updated_at, observations = observations + 1",
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
            "SELECT host, preferred, http_strikes, penalty_ms, observations, \
             updated_at, penalty_updated_at \
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
            "SELECT host, preferred, http_strikes, penalty_ms, observations, \
             updated_at, penalty_updated_at \
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
        Ok(rows
            .into_iter()
            .map(|(h, ms)| (h, ms.max(0) as u64))
            .collect())
    }
}

#[derive(sqlx::FromRow)]
struct ProfileRow {
    host: String,
    preferred: Option<String>,
    http_strikes: i64,
    penalty_ms: i64,
    observations: i64,
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
            observations: r.observations,
            updated_at: r.updated_at,
            penalty_updated_at: r.penalty_updated_at,
        }
    }
}

// ---------------------------------------------------------------------------
// Host weather (M01 v1): export/import of the learned per-host state
// ---------------------------------------------------------------------------

/// One host row of a host-weather bundle — the wire shape both `GET
/// /host-weather/export` emits and `POST /host-weather/import` accepts.
///
/// `challenge_fingerprints` is part of the v1 schema but always empty on
/// export: pumper does not (yet) persist per-host challenge fingerprints —
/// only the transient reason string of a blocked fetch. The field is carried
/// so the bundle schema does not need a version bump when they land.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherEntry {
    pub host: String,
    /// `Some("browser")` for a pinned host, `None` for cheap-first.
    #[serde(default)]
    pub preferred_tier: Option<String>,
    #[serde(default)]
    pub http_strikes: i64,
    /// Learned politeness penalty in ms (live governor value at export time).
    #[serde(default)]
    pub penalty_ms: i64,
    /// Evidence weight: tiered-fetch outcomes the exporter observed locally.
    #[serde(default)]
    pub observations: i64,
    /// Reserved in v1 — see the type docs.
    #[serde(default)]
    pub challenge_fingerprints: Vec<String>,
    /// Last tier-outcome change at the exporter, if known.
    #[serde(default)]
    pub updated_at: Option<String>,
}

/// The conservative merge decision for one imported [`WeatherEntry`] against
/// the local state — computed by [`plan_weather_import`] (pure, so precedence
/// is unit-testable) and applied by `TierMemory::apply_weather` + the
/// governor. Every field is a *raise*: an import can add caution, never remove
/// locally-earned knowledge.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WeatherPlan {
    pub host: String,
    /// Adopt the remote browser pin (local strikes rise to the pin threshold).
    pub adopt_pin: bool,
    /// Raise local strikes to this value. Always below [`STRIKE_LIMIT`]:
    /// imported strikes alone can never cross the pin threshold — a pin only
    /// arrives explicitly via `adopt_pin`, which is count-gated.
    pub raise_strikes: Option<i64>,
    /// Raise the live politeness penalty to this many ms. Already capped at
    /// [`WEATHER_IMPORT_PENALTY_CAP_MS`].
    pub raise_penalty_ms: Option<u64>,
    /// Human-readable reasons for what was kept, capped, or ignored.
    pub notes: Vec<String>,
}

impl WeatherPlan {
    /// Nothing to write — the local state already dominates the remote entry.
    pub fn is_noop(&self) -> bool {
        !self.adopt_pin && self.raise_strikes.is_none() && self.raise_penalty_ms.is_none()
    }
}

/// Plans the conservative merge of one remote entry into the local state.
///
/// Precedence rules (in order):
/// 1. **A local pin is never downgraded.** No remote entry — pinned or not,
///    however well-observed — ever clears a locally-earned browser pin.
/// 2. **A remote pin is adopted only when strictly better-observed**: the
///    local host is unpinned AND `remote.observations > local.observations`.
///    Count-weighted: equal evidence keeps the local verdict.
/// 3. **Strikes only rise, and never past the pin threshold** — imported
///    strikes are capped at `STRIKE_LIMIT - 1`, so strike intel alone cannot
///    pin a host without local confirmation.
/// 4. **Penalties only rise, capped at [`WEATHER_IMPORT_PENALTY_CAP_MS`]** —
///    compared against the max of the live governor value and the persisted
///    snapshot, so an import never shortens locally-earned spacing.
pub fn plan_weather_import(
    local: Option<&HostProfile>,
    live_penalty_ms: u64,
    remote: &WeatherEntry,
) -> WeatherPlan {
    let host = remote.host.to_lowercase();
    let mut plan = WeatherPlan {
        host,
        adopt_pin: false,
        raise_strikes: None,
        raise_penalty_ms: None,
        notes: Vec::new(),
    };
    let local_obs = local.map(|l| l.observations).unwrap_or(0);
    let local_strikes = local.map(|l| l.http_strikes).unwrap_or(0);
    let local_pinned = local.is_some_and(|l| l.preferred_tier.is_some());
    let remote_pinned = remote.preferred_tier.as_deref() == Some("browser");

    // Rules 1 + 2: pin precedence.
    if local_pinned {
        plan.notes
            .push("local pin kept: imports never downgrade a local pin".into());
    } else if remote_pinned {
        if remote.observations > local_obs {
            plan.adopt_pin = true;
            plan.notes.push(format!(
                "remote pin adopted: better-observed ({} remote vs {local_obs} local observations)",
                remote.observations
            ));
        } else {
            plan.notes.push(format!(
                "remote pin ignored: not better-observed ({} remote vs {local_obs} local observations)",
                remote.observations
            ));
        }
    }

    // Rule 3: strikes only rise, and imported strikes never reach the pin
    // threshold on their own. Irrelevant when a pin is being adopted (which
    // sets strikes to the threshold) or the host is already pinned.
    if !plan.adopt_pin && !local_pinned {
        let capped = remote.http_strikes.clamp(0, STRIKE_LIMIT - 1);
        if capped < remote.http_strikes {
            plan.notes.push(format!(
                "remote strikes capped at {capped} (pin threshold needs local confirmation)"
            ));
        }
        if capped > local_strikes {
            plan.raise_strikes = Some(capped);
        }
    }

    // Rule 4: penalty severity cap + never-lower.
    let local_penalty = live_penalty_ms.max(local.map(|l| l.penalty_ms.max(0) as u64).unwrap_or(0));
    let capped = (remote.penalty_ms.max(0) as u64).min(WEATHER_IMPORT_PENALTY_CAP_MS);
    if capped < remote.penalty_ms.max(0) as u64 {
        plan.notes.push(format!(
            "remote penalty capped at {WEATHER_IMPORT_PENALTY_CAP_MS}ms (import severity cap)"
        ));
    }
    if capped > local_penalty {
        plan.raise_penalty_ms = Some(capped);
    } else if capped > 0 {
        plan.notes
            .push("remote penalty ignored: local penalty is already as strict".into());
    }

    plan
}

impl TierMemory {
    /// The host-weather export set: every profile with at least
    /// `min_observations` recorded outcomes. The floor keeps thin/noisy hosts
    /// (including penalty-only snapshot rows, which never accrue observations)
    /// from travelling between deployments.
    pub async fn export_weather(&self, min_observations: i64) -> Result<Vec<HostProfile>> {
        let rows: Vec<ProfileRow> = sqlx::query_as(
            "SELECT host, preferred, http_strikes, penalty_ms, observations, \
             updated_at, penalty_updated_at \
             FROM tier_memory WHERE observations >= ?1 ORDER BY host ASC",
        )
        .bind(min_observations)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(HostProfile::from).collect())
    }

    /// Applies the tier-memory half of a [`WeatherPlan`] (pin/strikes). The
    /// penalty half goes through the live governor + `save_penalties`, both
    /// driven by the API handler. `observations` is deliberately untouched —
    /// it counts *local* evidence only, so imported intel stays overridable
    /// the moment local observations disagree.
    pub async fn apply_weather(&self, plan: &WeatherPlan) -> Result<()> {
        let now = ts(Utc::now());
        if plan.adopt_pin {
            sqlx::query(
                "INSERT INTO tier_memory (host, http_strikes, preferred, updated_at) \
                 VALUES (?1, ?2, 'browser', ?3) \
                 ON CONFLICT(host) DO UPDATE SET \
                   http_strikes = MAX(http_strikes, ?2), preferred = 'browser', \
                   updated_at = excluded.updated_at",
            )
            .bind(&plan.host)
            .bind(STRIKE_LIMIT)
            .bind(&now)
            .execute(&self.pool)
            .await?;
        } else if let Some(strikes) = plan.raise_strikes {
            sqlx::query(
                "INSERT INTO tier_memory (host, http_strikes, preferred, updated_at) \
                 VALUES (?1, ?2, NULL, ?3) \
                 ON CONFLICT(host) DO UPDATE SET \
                   http_strikes = MAX(http_strikes, ?2), \
                   updated_at = excluded.updated_at",
            )
            .bind(&plan.host)
            .bind(strikes)
            .bind(&now)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod weather_tests {
    use super::*;

    fn profile(host: &str, pin: Option<&str>, strikes: i64, penalty_ms: i64, obs: i64) -> HostProfile {
        HostProfile {
            host: host.into(),
            preferred_tier: pin.map(str::to_string),
            http_strikes: strikes,
            penalty_ms,
            observations: obs,
            updated_at: "2026-07-31T00:00:00.000000Z".into(),
            penalty_updated_at: None,
        }
    }

    fn entry(host: &str, pin: Option<&str>, strikes: i64, penalty_ms: i64, obs: i64) -> WeatherEntry {
        WeatherEntry {
            host: host.into(),
            preferred_tier: pin.map(str::to_string),
            http_strikes: strikes,
            penalty_ms,
            observations: obs,
            challenge_fingerprints: Vec::new(),
            updated_at: None,
        }
    }

    #[test]
    fn remote_pin_adopted_only_when_strictly_better_observed() {
        // Unknown host: remote pin with any observations (>0) wins.
        let plan = plan_weather_import(None, 0, &entry("a.com", Some("browser"), 3, 0, 5));
        assert!(plan.adopt_pin);
        // Equal evidence keeps the local verdict (count-weighted, strict >).
        let local = profile("a.com", None, 0, 0, 5);
        let plan = plan_weather_import(Some(&local), 0, &entry("a.com", Some("browser"), 3, 0, 5));
        assert!(!plan.adopt_pin, "equal observations must not adopt");
        // Strictly better-observed remote pin wins over an unpinned local.
        let plan = plan_weather_import(Some(&local), 0, &entry("a.com", Some("browser"), 3, 0, 6));
        assert!(plan.adopt_pin);
    }

    #[test]
    fn local_pin_is_never_downgraded() {
        let local = profile("a.com", Some("browser"), 3, 0, 2);
        // A remote entry without a pin, however well-observed, changes nothing.
        let plan = plan_weather_import(Some(&local), 0, &entry("a.com", None, 0, 0, 1000));
        assert!(plan.is_noop(), "unpinned remote must not touch a local pin: {plan:?}");
        // A remote pin over an existing local pin is also a no-op (nothing to raise).
        let plan =
            plan_weather_import(Some(&local), 0, &entry("a.com", Some("browser"), 3, 0, 1000));
        assert!(!plan.adopt_pin && plan.raise_strikes.is_none());
    }

    #[test]
    fn imported_strikes_are_capped_below_the_pin_threshold_and_never_lowered() {
        // Remote 99 strikes without an adopted pin cap at STRIKE_LIMIT - 1.
        let plan = plan_weather_import(None, 0, &entry("a.com", None, 99, 0, 1));
        assert_eq!(plan.raise_strikes, Some(STRIKE_LIMIT - 1));
        // Never lowered: local already has more strikes than the capped import.
        let local = profile("a.com", None, 2, 0, 9);
        let plan = plan_weather_import(Some(&local), 0, &entry("a.com", None, 1, 0, 1));
        assert_eq!(plan.raise_strikes, None);
        // Negative remote strikes are treated as zero, not written.
        let plan = plan_weather_import(None, 0, &entry("a.com", None, -5, 0, 1));
        assert!(plan.is_noop());
    }

    #[test]
    fn imported_penalty_is_capped_and_never_lowered() {
        // Severity cap: 10 minutes remote arrives as the import cap.
        let plan = plan_weather_import(None, 0, &entry("a.com", None, 0, 600_000, 1));
        assert_eq!(plan.raise_penalty_ms, Some(WEATHER_IMPORT_PENALTY_CAP_MS));
        // Never lowers a live local penalty…
        let plan = plan_weather_import(None, 5_000, &entry("a.com", None, 0, 2_000, 1));
        assert_eq!(plan.raise_penalty_ms, None);
        // …or a persisted local snapshot.
        let local = profile("a.com", None, 0, 8_000, 1);
        let plan = plan_weather_import(Some(&local), 0, &entry("a.com", None, 0, 2_000, 1));
        assert_eq!(plan.raise_penalty_ms, None);
        // A genuinely stricter remote penalty raises.
        let plan = plan_weather_import(Some(&local), 0, &entry("a.com", None, 0, 30_000, 1));
        assert_eq!(plan.raise_penalty_ms, Some(30_000));
    }

    #[test]
    fn dominated_remote_entry_is_a_noop_and_hosts_are_lowercased() {
        let local = profile("a.com", Some("browser"), 3, 60_000, 50);
        let plan = plan_weather_import(
            Some(&local),
            60_000,
            &entry("A.COM", Some("browser"), 3, 60_000, 10),
        );
        assert!(plan.is_noop());
        assert_eq!(plan.host, "a.com");
    }
}

/// Fixed-width RFC 3339 UTC micros — lexicographic order == chronological.
fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}
