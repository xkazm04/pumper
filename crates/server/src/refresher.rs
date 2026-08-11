//! Background cache refresher (M02 self-refreshing mirror). Piggybacked on the
//! scheduler tick (like the DLQ drain and the stuck-job reaper): each tick it
//! asks the cache's freshness model for the keys whose predicted next change is
//! within `[refresher] horizon_secs`, and conditionally revalidates them —
//! turning the HTTP cache from a reactive TTL store into a mirror that is warm
//! *just before* each URL's learned change cadence says it will move.
//!
//! Politeness is structural, not best-effort:
//!  - every request is admitted through `Governor::try_acquire` — a
//!    non-blocking, idle-slot-only claim. A host with a busy or penalized slot
//!    is simply skipped this tick; background traffic can never queue ahead of
//!    (or behind) live jobs.
//!  - hard `per_host_per_tick` and `global_per_tick` budgets bound the total.
//!  - the actual fetch still runs through the normal HTTP engine, so the
//!    per-host spacing, penalties, retries, and body caps all apply.
//!
//! Outcomes flow through the exact seams the demand path uses: a `304` calls
//! `HttpCache::refresh` (which extends TTL and logs the unchanged observation),
//! a changed body logs `record_revalidation(_, true)` + `put`. No new event
//! source is invented — a changed body reaches datasets/triggers only when a
//! job later reads it, exactly as before.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::Utc;
use pumper_core::{HttpRequest, KeyFreshness};
use tracing::{debug, warn};

use crate::state::AppState;

/// Overlap guard: a slow tick (each revalidation pays real network latency and
/// one governor spacing interval) must not stack a second concurrent pass.
static RUNNING: AtomicBool = AtomicBool::new(false);

/// How many freshness candidates to pull per tick — a small multiple of the
/// global budget so host-budget/idle-slot skips still leave enough to fill it.
const CANDIDATE_OVERSAMPLE: usize = 4;

/// Spawns one refresher pass if none is already running. Called from the
/// scheduler tick; returns immediately (the pass runs as its own task so the
/// scheduler's cron reconcile is never delayed by network latency).
pub fn tick(state: &AppState) {
    if !state.config.refresher.enabled {
        return;
    }
    // Nothing new STARTS during shutdown. A pass is pure speculative outbound
    // traffic whose only product is a warmer cache, so beginning one while the
    // process is stopping spends a host's politeness budget on a mirror that is
    // about to be closed.
    if state.shutdown.is_cancelled() {
        return;
    }
    if RUNNING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        debug!("cache refresher: previous pass still running; skipping tick");
        return;
    }
    let state = state.clone();
    tokio::spawn(async move {
        let shutdown = state.shutdown.clone();
        // Cancellation-aware: a bare spawn here escaped the shutdown token
        // entirely, so a pass' revalidations could still be on the wire after
        // the drain returned — a network call outliving the process that made
        // it, with nothing left to read the answer. Abandoning mid-pass is safe:
        // each revalidation commits (or doesn't) on its own, and the freshness
        // model re-selects whatever this pass didn't reach on the next tick.
        let result = tokio::select! {
            _ = shutdown.cancelled() => {
                debug!("cache refresher: shutdown signalled; abandoning the pass");
                Ok(())
            }
            result = run_pass(&state) => result,
        };
        RUNNING.store(false, Ordering::Release);
        if let Err(e) = result {
            warn!("cache refresher pass failed: {e}");
        }
    });
}

async fn run_pass(state: &AppState) -> anyhow::Result<()> {
    let cfg = &state.config.refresher;
    // The revalidation log is bounded by the always-on store janitor
    // (`main::store_janitor`, `[refresher] retention_days`), NOT here: the
    // demand path appends to it whether or not this pass ever runs, so pruning
    // from inside the pass left the log unbounded on the shipping default
    // (`enabled = false`) and re-pruned it every scheduler tick when it was on.

    let now = Utc::now();
    let horizon = cfg.horizon_secs as f64;
    let candidates = state
        .cache
        .freshness(
            now,
            cfg.global_per_tick.saturating_mul(CANDIDATE_OVERSAMPLE),
        )
        .await?;

    let mut refreshed = 0usize;
    let mut per_host: HashMap<String, usize> = HashMap::new();
    for key in candidates {
        if refreshed >= cfg.global_per_tick {
            break;
        }
        // Near-due only: predicted change within the horizon (or already past).
        if key.due_in_secs > horizon {
            break; // sorted ascending — everything after is further out
        }
        let Some(host) = host_of(&key.url) else {
            continue;
        };
        let used = per_host.entry(host.clone()).or_insert(0);
        if *used >= cfg.per_host_per_tick {
            continue;
        }
        // Idle-slot admission: claim the host's governor slot only if it is
        // free RIGHT NOW; a busy/penalized host waits for a later tick.
        if !state.governor.try_acquire(&host) {
            continue;
        }
        *used += 1;
        refreshed += 1;
        revalidate_one(state, &key).await;
    }
    if refreshed > 0 {
        debug!(refreshed, "cache refresher: pass complete");
    }
    Ok(())
}

/// Conditionally revalidates one cached entry, mirroring the demand-path
/// engine revalidate: 304 → `refresh` (TTL extend + unchanged observation),
/// changed 2xx → changed observation + `put`. Best-effort; failures warn.
async fn revalidate_one(state: &AppState, key: &KeyFreshness) {
    let stale = match state.cache.get_stale(&key.key).await {
        Ok(Some(stale)) => stale,
        Ok(None) => return, // entry evicted since the model was built
        Err(e) => {
            warn!(url = %key.url, "cache refresher: stale read failed: {e}");
            return;
        }
    };
    let mut req = HttpRequest::get(&key.url);
    // Bypass the cache read path (we ARE the revalidation) and carry the stored
    // validators so an unchanged origin answers with a cheap 304.
    req.no_cache = true;
    req.etag = stale.etag;
    req.if_modified_since = stale.last_modified;
    let ttl = Duration::from_secs(state.config.cache.ttl_secs);
    match state.engines.http.fetch(req).await {
        Ok(resp) if resp.status == 304 => {
            // refresh() records the unchanged observation itself.
            if let Err(e) = state.cache.refresh(&key.key, ttl).await {
                warn!(url = %key.url, "cache refresher: refresh failed: {e}");
            }
        }
        Ok(resp) if resp.is_success() => {
            state.cache.record_revalidation(&key.key, true).await;
            if let Err(e) = state.cache.put(&key.key, &key.url, &resp, ttl).await {
                warn!(url = %key.url, "cache refresher: put failed: {e}");
            }
        }
        Ok(resp) => {
            // A non-2xx/-304 (origin error, bot-wall) is not an observation of
            // either outcome — leave the model alone, log at debug.
            debug!(url = %key.url, status = resp.status, "cache refresher: revalidate skipped");
        }
        Err(e) => debug!(url = %key.url, "cache refresher: fetch failed: {e}"),
    }
}

/// Lowercased host of an http(s) URL, for the per-host budget + governor key
/// (and the freshness route's per-host rollup). Hand-rolled — the server crate
/// doesn't depend on the `url` crate: strips scheme, `userinfo@`, port, and
/// path/query/fragment.
pub(crate) fn host_of(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme.split(['/', '?', '#']).next()?;
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = authority.split(':').next()?;
    (!host.is_empty()).then(|| host.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::host_of;

    #[test]
    fn host_of_strips_scheme_userinfo_port_and_path() {
        assert_eq!(
            host_of("https://User:pw@Ex.COM:8443/a?b#c"),
            Some("ex.com".into())
        );
        assert_eq!(host_of("http://x.test/path"), Some("x.test".into()));
        assert_eq!(host_of("https:///no-host"), None);
    }
}
