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

pub struct Governor {
    enabled: bool,
    default_interval: Duration,
    per_domain: HashMap<String, Duration>,
    max_jitter: Duration,
    /// Next free slot per host; acquire() claims one and advances it.
    next_slot: Mutex<HashMap<String, Instant>>,
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
            tick: AtomicU64::new(0),
        }
    }

    /// Blocks until this caller's slot for `host` is due. Returns immediately
    /// when disabled or when the host's rate is unlimited.
    pub async fn acquire(&self, host: &str) {
        if !self.enabled {
            return;
        }
        let host = host.to_lowercase();
        let interval = self
            .per_domain
            .get(&host)
            .copied()
            .unwrap_or(self.default_interval);
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
