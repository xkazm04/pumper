//! LRU bookkeeping over a `VecDeque` of keys, shared by the engines that hold a
//! bounded pool of expensive handles — pooled HTTP clients (keyed by proxy +
//! profile) and live Chrome instances (keyed by profile).
//!
//! Both hand-rolled the same retain / push_back / evict-past-cap dance, with the
//! caps and the "never evict the key you just touched" guard drifting
//! independently. One implementation, one set of tests.

use std::collections::VecDeque;

/// Marks `key` as most-recently-used, without evicting anything.
pub fn lru_touch(order: &mut VecDeque<String>, key: &str) {
    order.retain(|k| k != key);
    order.push_back(key.to_string());
}

/// Marks `key` most-recently-used, then evicts least-recently-used keys until at
/// most `cap` remain (`cap` is floored at 1, so the key just touched is never
/// itself evicted). Returns the evicted keys so the caller can drop the matching
/// entries from its own map.
pub fn lru_touch_evict(order: &mut VecDeque<String>, key: &str, cap: usize) -> Vec<String> {
    lru_touch(order, key);
    let mut evicted = Vec::new();
    while order.len() > cap.max(1) {
        if let Some(old) = order.pop_front() {
            evicted.push(old);
        }
    }
    evicted
}

#[cfg(test)]
mod tests {
    use super::{lru_touch, lru_touch_evict};
    use std::collections::VecDeque;

    #[test]
    fn touch_moves_key_to_most_recent_without_evicting() {
        let mut order = VecDeque::new();
        lru_touch(&mut order, "a");
        lru_touch(&mut order, "b");
        lru_touch(&mut order, "a"); // re-touch: moves to the back, no duplicate
        assert_eq!(order, VecDeque::from(vec!["b".to_string(), "a".to_string()]));
    }

    #[test]
    fn evicts_least_recently_used_past_cap() {
        let mut order = VecDeque::new();
        // Filling to the cap evicts nothing.
        for k in ["a", "b", "c"] {
            assert!(lru_touch_evict(&mut order, k, 3).is_empty());
        }
        // Touching `a` makes it most-recent, so `b` is the victim when `d` pushes
        // past the cap.
        assert!(lru_touch_evict(&mut order, "a", 3).is_empty());
        assert_eq!(lru_touch_evict(&mut order, "d", 3), vec!["b".to_string()]);
        assert_eq!(order.len(), 3);
        assert!(order.contains(&"a".to_string()), "recently used is kept");
        assert!(order.contains(&"d".to_string()), "newest is kept");
    }

    #[test]
    fn key_just_touched_is_never_evicted() {
        // cap 1: the incumbent goes, the new key stays.
        let mut order = VecDeque::from(vec!["a".to_string()]);
        assert_eq!(lru_touch_evict(&mut order, "b", 1), vec!["a".to_string()]);
        assert_eq!(order, VecDeque::from(vec!["b".to_string()]));
        // cap 0 is floored to 1 rather than evicting everything.
        let mut zero = VecDeque::new();
        assert!(lru_touch_evict(&mut zero, "x", 0).is_empty());
        assert_eq!(zero, VecDeque::from(vec!["x".to_string()]));
    }
}
