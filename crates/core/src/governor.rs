//! Per-domain politeness governor. A token-bucket-of-capacity-one per host:
//! requests to the same host are spaced by a minimum interval so a tiered
//! escalation (http -> browser -> claude) or a burst of jobs never hammers a
//! single origin. Hosts are independent, so unrelated targets never wait on
//! each other.
//!
//! The HTTP tier acquires here from inside `HttpEngine::send` (so raw-HTTP
//! callers like the crawler are governed too); the browser tier acquires from
//! `Fetcher::fetch`, which also feeds `penalize`/`reward` from the browser
//! render's verdict. Both tiers share one `Governor` instance, so an
//! http -> browser escalation to the same host stays coherently spaced.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::time::Instant;

use crate::config::GovernorConfig;

/// Default adaptive-penalty bounds (overridable via `[governor]` config).
pub const DEFAULT_PENALTY_BASE_SECS: u64 = 1;
pub const DEFAULT_PENALTY_CAP_SECS: u64 = 300;
pub const DEFAULT_PENALTY_FLOOR_MS: u64 = 100;

/// Idle-host eviction bounds. When the per-host map grows past `MAX_HOSTS`, a
/// sweep drops entries untouched for `IDLE_TTL` (keeping any still serving out
/// a penalty). Bounds memory for long-lived servers hitting many origins.
const MAX_HOSTS: usize = 4096;
const IDLE_TTL: Duration = Duration::from_secs(3600);
/// Amortize the (relatively pricey) size check across many acquires.
const EVICT_CHECK_EVERY: u64 = 1024;

/// Per-host politeness state. Held in one sharded map ([`DashMap`]) so distinct
/// hosts never contend on a single global lock the way two `Mutex<HashMap>`s did.
struct HostState {
    /// Next free slot; acquire() claims one and advances it.
    next_slot: Instant,
    /// Learned extra spacing: doubled on 429/503 (respecting Retry-After),
    /// halved on success, zeroed below the floor.
    penalty: Duration,
    /// Last time this host was touched — drives idle eviction.
    last_seen: Instant,
}

pub struct Governor {
    enabled: bool,
    default_interval: Duration,
    per_domain: HashMap<String, Duration>,
    max_jitter: Duration,
    /// Sharded per-host slot + penalty state. Distinct hosts hit different
    /// shards, so unrelated targets never serialize on each other.
    hosts: DashMap<String, HostState>,
    /// Cheap deterministic jitter source (no rng dependency, resume-safe).
    tick: AtomicU64,
    /// Acquire counter; gates the amortized idle-eviction sweep.
    ops: AtomicU64,
    /// Learned-penalty bounds (from `[governor]` config).
    penalty_base: Duration,
    penalty_cap: Duration,
    penalty_floor: Duration,
}

impl Governor {
    pub fn new(cfg: &GovernorConfig) -> Self {
        let interval = |rps: f64| {
            if rps <= 0.0 {
                Duration::ZERO
            } else {
                Duration::from_secs_f64(1.0 / rps)
            }
        };
        let per_domain = cfg
            .per_domain
            .iter()
            .map(|(host, rps)| (host.to_lowercase(), interval(*rps)))
            .collect();
        Self {
            enabled: cfg.enabled,
            default_interval: interval(cfg.default_rps),
            per_domain,
            max_jitter: Duration::from_millis(cfg.jitter_ms),
            hosts: DashMap::new(),
            tick: AtomicU64::new(0),
            ops: AtomicU64::new(0),
            penalty_base: Duration::from_secs(cfg.penalty_base_secs),
            penalty_cap: Duration::from_secs(cfg.penalty_cap_secs),
            penalty_floor: Duration::from_millis(cfg.penalty_floor_ms),
        }
    }

    /// Blocks until this caller's slot for `host` is due. Returns immediately
    /// when disabled or when the host's rate is unlimited (and unpenalized).
    pub async fn acquire(&self, host: &str) {
        if !self.enabled {
            return;
        }
        let host = host.to_lowercase();
        let base = self.per_domain.get(&host).copied().unwrap_or(self.default_interval);

        // Fast path: unlimited + unpenalized + no jitter needs no spacing and no
        // state churn. Only a live penalty on this host forces the slow path.
        if base.is_zero() && self.max_jitter.is_zero() {
            let penalized = self.hosts.get(&host).map(|s| !s.penalty.is_zero()).unwrap_or(false);
            if !penalized {
                return;
            }
        }

        let slot = {
            let now = Instant::now();
            let mut entry = self.hosts.entry(host).or_insert_with(|| HostState {
                next_slot: now,
                penalty: Duration::ZERO,
                last_seen: now,
            });
            entry.last_seen = now;
            let interval = base + entry.penalty;
            let start = entry.next_slot.max(now);
            entry.next_slot = start + interval;
            start
        };

        self.maybe_evict();

        let wake = slot + self.jitter();
        let now = Instant::now();
        if wake > now {
            tokio::time::sleep_until(wake).await;
        }
    }

    /// Records a rate-limit response (429/503) for `host`: the learned extra
    /// spacing doubles (starting at 1s), honors a larger server `Retry-After`,
    /// caps at 5 minutes — and the host's next slot is pushed out so already
    /// -queued peers back off too.
    pub async fn penalize(&self, host: &str, retry_after: Option<Duration>) {
        if !self.enabled {
            return;
        }
        let host = host.to_lowercase();
        let now = Instant::now();
        let mut entry = self.hosts.entry(host.clone()).or_insert_with(|| HostState {
            next_slot: now,
            penalty: Duration::ZERO,
            last_seen: now,
        });
        entry.last_seen = now;
        let doubled = if entry.penalty.is_zero() {
            self.penalty_base
        } else {
            entry.penalty.saturating_mul(2)
        };
        let next = doubled.max(retry_after.unwrap_or(Duration::ZERO)).min(self.penalty_cap);
        entry.penalty = next;
        entry.next_slot = entry.next_slot.max(now + next);
        drop(entry);
        tracing::warn!(host = %host, penalty_secs = next.as_secs_f64(), "rate-limited; backing off");
    }

    /// Records a healthy response for `host`: the learned penalty halves and is
    /// zeroed below the floor — recovery without a config change. A no-op for a
    /// host with no learned penalty (never inserts state).
    pub async fn reward(&self, host: &str) {
        let host = host.to_lowercase();
        if let Some(mut entry) = self.hosts.get_mut(&host) {
            if entry.penalty.is_zero() {
                return;
            }
            entry.last_seen = Instant::now();
            entry.penalty /= 2;
            if entry.penalty < self.penalty_floor {
                entry.penalty = Duration::ZERO;
            }
        }
    }

    /// The current learned extra spacing for a host (observability + tests).
    pub async fn penalty(&self, host: &str) -> Duration {
        self.hosts
            .get(&host.to_lowercase())
            .map(|s| s.penalty)
            .unwrap_or(Duration::ZERO)
    }

    /// Snapshot of every host currently carrying a non-zero learned penalty, as
    /// `(host, penalty)`. Feeds the write-behind persistence so penalties
    /// survive a restart. Cheap: a lock-free `DashMap` walk of penalized hosts.
    pub fn snapshot_penalties(&self) -> Vec<(String, Duration)> {
        self.hosts
            .iter()
            .filter(|e| !e.value().penalty.is_zero())
            .map(|e| (e.key().clone(), e.value().penalty))
            .collect()
    }

    /// Seeds a host's learned penalty from persisted state on boot. A no-op for
    /// a zero penalty (nothing to restore). Does not push the next slot out —
    /// spacing resumes naturally on the next acquire.
    pub fn restore_penalty(&self, host: &str, penalty: Duration) {
        if penalty.is_zero() {
            return;
        }
        let now = Instant::now();
        self.hosts
            .entry(host.to_lowercase())
            .or_insert_with(|| HostState { next_slot: now, penalty: Duration::ZERO, last_seen: now })
            .penalty = penalty;
    }

    /// Forgets a host's live politeness state (learned penalty + slot). Returns
    /// whether any state existed. Used by `DELETE /hosts/{host}/memory` to reset
    /// the learned penalty alongside the tier memory.
    pub fn clear(&self, host: &str) -> bool {
        self.hosts.remove(&host.to_lowercase()).is_some()
    }

    /// Amortized idle-host eviction: rarely checked, sweeps only when the map
    /// has grown past the cap, and keeps hosts touched recently or still under
    /// a penalty. Holds no entry ref (avoids a shard self-deadlock).
    fn maybe_evict(&self) {
        if self.ops.fetch_add(1, Ordering::Relaxed) % EVICT_CHECK_EVERY != 0 {
            return;
        }
        if self.hosts.len() <= MAX_HOSTS {
            return;
        }
        let now = Instant::now();
        self.hosts.retain(|_, s| {
            now.saturating_duration_since(s.last_seen) < IDLE_TTL || !s.penalty.is_zero()
        });
    }

    fn jitter(&self) -> Duration {
        if self.max_jitter.is_zero() {
            return Duration::ZERO;
        }
        // Deterministic LCG step — spreads requests without pulling in `rand`.
        let n = self.tick.fetch_add(1, Ordering::Relaxed);
        self.max_jitter.mul_f64(crate::jitter::lcg_fraction(n))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::config::GovernorConfig;

    fn governor() -> Governor {
        Governor::new(&GovernorConfig::default())
    }

    /// Distinct hosts never serialize on each other, yet each host's own
    /// spacing still holds — the whole point of the per-host sharded map.
    #[tokio::test]
    async fn distinct_hosts_run_in_parallel_but_each_host_spaces() {
        // 200ms per-host spacing, no jitter for a deterministic lower bound.
        let cfg = GovernorConfig {
            enabled: true,
            default_rps: 5.0,
            jitter_ms: 0,
            per_domain: HashMap::new(),
            ..GovernorConfig::default()
        };
        let g = Arc::new(Governor::new(&cfg));

        let start = Instant::now();
        let mut tasks = Vec::new();
        for i in 0..32 {
            let g = g.clone();
            tasks.push(tokio::spawn(async move {
                let host = format!("h{i}.example");
                // 3 acquires => 2 spacing gaps of 200ms each (~400ms) per host.
                for _ in 0..3 {
                    g.acquire(&host).await;
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let elapsed = start.elapsed();

        // Lower bound: same-host spacing was enforced (two 200ms gaps).
        assert!(elapsed >= Duration::from_millis(350), "per-host spacing lost: {elapsed:?}");
        // Upper bound: 32 hosts ran concurrently, not serialized into ~12.8s.
        assert!(elapsed < Duration::from_secs(3), "distinct hosts serialized: {elapsed:?}");
    }

    #[tokio::test]
    async fn penalize_doubles_honors_retry_after_and_caps() {
        let base = Duration::from_secs(DEFAULT_PENALTY_BASE_SECS);
        let cap = Duration::from_secs(DEFAULT_PENALTY_CAP_SECS);
        let g = governor();
        assert_eq!(g.penalty("X.com").await, Duration::ZERO);
        g.penalize("x.com", None).await;
        assert_eq!(g.penalty("x.com").await, base);
        g.penalize("x.com", None).await;
        assert_eq!(g.penalty("X.COM").await, base * 2, "case-insensitive host");
        // A larger server Retry-After wins over doubling.
        g.penalize("x.com", Some(Duration::from_secs(60))).await;
        assert_eq!(g.penalty("x.com").await, Duration::from_secs(60));
        // Growth is capped.
        for _ in 0..10 {
            g.penalize("x.com", None).await;
        }
        assert_eq!(g.penalty("x.com").await, cap);
        // Other hosts are unaffected.
        assert_eq!(g.penalty("y.com").await, Duration::ZERO);
    }

    #[tokio::test]
    async fn reward_decays_and_removes_penalty() {
        let g = governor();
        g.penalize("x.com", Some(Duration::from_secs(4))).await;
        g.reward("x.com").await;
        assert_eq!(g.penalty("x.com").await, Duration::from_secs(2));
        for _ in 0..8 {
            g.reward("x.com").await;
        }
        assert_eq!(g.penalty("x.com").await, Duration::ZERO, "dropped below the floor");
        // Reward on an unpenalized host is a no-op.
        g.reward("y.com").await;
        assert_eq!(g.penalty("y.com").await, Duration::ZERO);
    }

    #[tokio::test]
    async fn snapshot_restore_and_clear_round_trip() {
        let g = governor();
        g.penalize("a.com", None).await; // base penalty
        g.penalize("b.com", Some(Duration::from_secs(5))).await;

        // snapshot_penalties captures only penalized hosts.
        let mut snap = g.snapshot_penalties();
        snap.sort_by(|x, y| x.0.cmp(&y.0));
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].0, "a.com");
        assert_eq!(snap[1], ("b.com".to_string(), Duration::from_secs(5)));

        // A fresh governor (simulated restart) restores the penalties.
        let g2 = governor();
        assert_eq!(g2.penalty("b.com").await, Duration::ZERO);
        for (host, penalty) in &snap {
            g2.restore_penalty(host, *penalty);
        }
        assert_eq!(g2.penalty("b.com").await, Duration::from_secs(5));
        // A zero restore is a no-op (never inserts state).
        g2.restore_penalty("c.com", Duration::ZERO);
        assert!(!g2.clear("c.com"), "zero restore must not create state");

        // clear() forgets a host and reports whether state existed.
        assert!(g2.clear("b.com"));
        assert_eq!(g2.penalty("b.com").await, Duration::ZERO);
        assert!(!g2.clear("b.com"), "second clear is a no-op");
    }
}
