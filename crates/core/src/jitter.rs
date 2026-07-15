//! Deterministic, dependency-free jitter.
//!
//! Timing jitter here is derived from a caller-supplied seed rather than `rand`,
//! so a run stays reproducible (and resume-safe). The governor's pacing jitter
//! and the HTTP retry backoff both need the same scramble — they previously
//! carried a copy of the constants each, and the HTTP one's doc already conceded
//! it worked "exactly like the governor".

/// Scrambles `seed` into a deterministic fraction in `[0, 1)`.
///
/// An LCG step (the Numerical-Recipes multiplier/increment) whose high bits are
/// taken, so nearby seeds — a counter tick, or an attempt number — still spread.
pub fn lcg_fraction(seed: u64) -> f64 {
    let scrambled = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    ((scrambled >> 33) as f64 / (1u64 << 31) as f64).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::lcg_fraction;

    #[test]
    fn fraction_is_in_range_deterministic_and_spread() {
        // Always a usable fraction.
        for seed in [0u64, 1, 2, 7, 12345, u64::MAX] {
            let f = lcg_fraction(seed);
            assert!((0.0..=1.0).contains(&f), "seed {seed} gave {f}");
        }
        // Deterministic: the same seed always jitters identically.
        assert_eq!(lcg_fraction(42), lcg_fraction(42));
        // Consecutive seeds (a governor tick) do not collide.
        assert_ne!(lcg_fraction(1), lcg_fraction(2));
        // Spread: successive ticks cover the range rather than clustering.
        let mut low = 0;
        let mut high = 0;
        for n in 0..100u64 {
            if lcg_fraction(n) < 0.5 {
                low += 1;
            } else {
                high += 1;
            }
        }
        assert!(low > 20 && high > 20, "poor spread: {low} low / {high} high");
    }
}
