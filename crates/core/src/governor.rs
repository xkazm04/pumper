//! Per-domain politeness governor. A token-bucket-of-capacity-one per host:
//! requests to the same host are spaced by a minimum interval so a tiered
//! escalation (http -> browser -> claude) or a burst of jobs never hammers a
//! single origin. Hosts are independent, so unrelated targets never wait on
//! each other.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::config::GovernorConfig;

/// Adaptive penalty bounds, learned from rate-limit responses.
const PENALTY_BASE: Duration = Duration::from_secs(1);
const PENALTY_CAP: Duration = Duration::from_secs(300);
const PENALTY_FLOOR: Duration = Duration::from_millis(100);

pub struct Governor {
    enabled: bool,
    default_interval: Duration,
    per_domain: HashMap<String, Duration>,
    max_jitter: Duration,
    /// Next free slot per host; acquire() claims one and advances it.
    next_slot: Mutex<HashMap<String, Instant>>,
    /// Learned extra spacing per host: doubled on 429/503 (respecting
    /// Retry-After), halved on success, dropped below the floor.
    penalties: Mutex<HashMap<String, Duration>>,
    /// Cheap deterministic jitter source (no rng dependency, resume-safe).
    tick: AtomicU64,
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
            next_slot: Mutex::new(HashMap::new()),
            penalties: Mutex::new(HashMap::new()),
            tick: AtomicU64::new(0),
        }
    }

    /// Blocks until this caller's slot for `host` is due. Returns immediately
    /// when disabled or when the host's rate is unlimited (and unpenalized).
    pub async fn acquire(&self, host: &str) {
        if !self.enabled {
            return;
        }
        let host = host.to_lowercase();
        let penalty = { self.penalties.lock().await.get(&host).copied().unwrap_or(Duration::ZERO) };
        let interval = self
            .per_domain
            .get(&host)
            .copied()
            .unwrap_or(self.default_interval)
            + penalty;
        if interval.is_zero() && self.max_jitter.is_zero() {
            return;
        }

        let slot = {
            let mut slots = self.next_slot.lock().await;
            let now = Instant::now();
            let entry = slots.entry(host).or_insert(now);
            let start = (*entry).max(now);
            *entry = start + interval;
            start
        };

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
        let next = {
            let mut penalties = self.penalties.lock().await;
            let current = penalties.get(&host).copied().unwrap_or(Duration::ZERO);
            let doubled = if current.is_zero() { PENALTY_BASE } else { current.saturating_mul(2) };
            let next = doubled.max(retry_after.unwrap_or(Duration::ZERO)).min(PENALTY_CAP);
            penalties.insert(host.clone(), next);
            next
        };
        {
            let mut slots = self.next_slot.lock().await;
            let now = Instant::now();
            let entry = slots.entry(host.clone()).or_insert(now);
            *entry = (*entry).max(now + next);
        }
        tracing::warn!(host = %host, penalty_secs = next.as_secs_f64(), "rate-limited; backing off");
    }

    /// Records a healthy response for `host`: the learned penalty halves and
    /// is dropped entirely below the floor — recovery without a config change.
    pub async fn reward(&self, host: &str) {
        let host = host.to_lowercase();
        let mut penalties = self.penalties.lock().await;
        if let Some(current) = penalties.get_mut(&host) {
            *current /= 2;
            if *current < PENALTY_FLOOR {
                penalties.remove(&host);
            }
        }
    }

    /// The current learned extra spacing for a host (observability + tests).
    pub async fn penalty(&self, host: &str) -> Duration {
        self.penalties
            .lock()
            .await
            .get(&host.to_lowercase())
            .copied()
            .unwrap_or(Duration::ZERO)
    }

    fn jitter(&self) -> Duration {
        if self.max_jitter.is_zero() {
            return Duration::ZERO;
        }
        // Deterministic LCG step — spreads requests without pulling in `rand`.
        let n = self.tick.fetch_add(1, Ordering::Relaxed);
        let scrambled = n.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let frac = (scrambled >> 33) as f64 / (1u64 << 31) as f64;
        self.max_jitter.mul_f64(frac.min(1.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GovernorConfig;

    fn governor() -> Governor {
        Governor::new(&GovernorConfig::default())
    }

    #[tokio::test]
    async fn penalize_doubles_honors_retry_after_and_caps() {
        let g = governor();
        assert_eq!(g.penalty("X.com").await, Duration::ZERO);
        g.penalize("x.com", None).await;
        assert_eq!(g.penalty("x.com").await, PENALTY_BASE);
        g.penalize("x.com", None).await;
        assert_eq!(g.penalty("X.COM").await, PENALTY_BASE * 2, "case-insensitive host");
        // A larger server Retry-After wins over doubling.
        g.penalize("x.com", Some(Duration::from_secs(60))).await;
        assert_eq!(g.penalty("x.com").await, Duration::from_secs(60));
        // Growth is capped.
        for _ in 0..10 {
            g.penalize("x.com", None).await;
        }
        assert_eq!(g.penalty("x.com").await, PENALTY_CAP);
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
}
