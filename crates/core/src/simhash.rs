//! SimHash near-duplicate detection. Produces a 64-bit fingerprint where
//! *similar* documents get *similar* fingerprints (small Hamming distance),
//! unlike a normal content hash where one byte flips everything. Lets the
//! dataset store detect near-duplicate pages — not just exact changes — with no
//! external service. Pure Rust and deterministic (fixed-key SipHash).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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
    let mut hasher = DefaultHasher::new();
    token.hash(&mut hasher);
    hasher.finish()
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
