//! SimHash near-duplicate detection. Produces a 64-bit fingerprint where
//! *similar* documents get *similar* fingerprints (small Hamming distance),
//! unlike a normal content hash where one byte flips everything. Lets the
//! dataset store detect near-duplicate pages — not just exact changes — with no
//! external service. Pure Rust and deterministic (version-stable FNV-1a hash).

use serde_json::Value;

/// 64-bit SimHash of the token stream in `text`.
pub fn simhash(text: &str) -> u64 {
    let mut bits = [0i32; 64];
    let mut seen = false;
    for token in tokenize(text) {
        seen = true;
        let h = hash_token(&token);
        for (i, bit) in bits.iter_mut().enumerate() {
            if (h >> i) & 1 == 1 {
                *bit += 1;
            } else {
                *bit -= 1;
            }
        }
    }
    if !seen {
        return 0;
    }
    let mut out = 0u64;
    for (i, &b) in bits.iter().enumerate() {
        if b > 0 {
            out |= 1 << i;
        }
    }
    out
}

/// SimHash over the textual content of a JSON value (concatenated string and
/// number leaves — field names and JSON punctuation are ignored).
pub fn simhash_value(value: &Value) -> u64 {
    let mut text = String::new();
    collect_text(value, &mut text);
    simhash(&text)
}

/// Number of differing bits — the near-duplicate distance metric.
pub fn hamming(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

fn collect_text(value: &Value, out: &mut String) {
    match value {
        Value::String(s) => {
            out.push_str(s);
            out.push(' ');
        }
        Value::Number(n) => {
            out.push_str(&n.to_string());
            out.push(' ');
        }
        Value::Array(a) => a.iter().for_each(|v| collect_text(v, out)),
        Value::Object(m) => m.values().for_each(|v| collect_text(v, out)),
        _ => {}
    }
}

fn tokenize(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| s.len() >= 2)
        .map(str::to_lowercase)
}

fn hash_token(token: &str) -> u64 {
    // FNV-1a: a fixed, version-stable hash. `DefaultHasher` (SipHash) has no
    // documented cross-version output stability, so persisted simhashes would
    // silently drift after a toolchain upgrade and defeat dedup against records
    // stored under the old hash. (One-time reindex when adopting this.)
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in token.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // splitmix64 finalizer — FNV-1a alone has weak avalanche (low bits barely
    // mix), which skews the per-bit SimHash votes and inflates near-dup distance.
    // This gives ~half-the-bits-flip diffusion, restoring SimHash separation.
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^= hash >> 31;
    hash
}

#[cfg(test)]
mod tests {
    use super::{hamming, simhash};

    #[test]
    fn near_duplicates_are_close() {
        let a = simhash("The quick brown fox jumps over the lazy dog in the yard");
        // One word changed → should stay within a small Hamming radius.
        let b = simhash("The quick brown fox jumps over the lazy cat in the yard");
        assert!(hamming(a, b) <= 6, "near-dup distance too large: {}", hamming(a, b));
    }

    #[test]
    fn different_texts_are_far() {
        let a = simhash("annual budget report for the finance department fiscal year");
        let b = simhash("photographs of tropical birds migrating across the ocean at dawn");
        assert!(hamming(a, b) >= 18, "unrelated distance too small: {}", hamming(a, b));
    }

    #[test]
    fn identical_is_zero_distance() {
        let a = simhash("same content here");
        let b = simhash("same content here");
        assert_eq!(hamming(a, b), 0);
    }
}
