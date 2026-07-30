//! Per-field distributional sketches and the statistics that read them.
//!
//! A sketch is a fixed-size summary of what one field produced across one run's
//! documents: how often it matched, how long its values were, what kinds of
//! characters they held, how many distinct values there were, and a k-minhash of
//! the value multiset. Fixed-size matters — the baseline is a rolling window of
//! these, so the storage cost per source is bounded by
//! `fields x window_runs`, not by corpus size.
//!
//! Two families of statistic read them, because the two kinds of signal have
//! different natural variance:
//!
//! - **Rates** (miss, error, coercion failure) are proportions with a known
//!   sampling distribution, so they are compared by [`wilson`] interval
//!   separation. This is what makes a 3-document run incapable of tripping
//!   anything — its interval is enormous — with no special case for small runs.
//! - **Distributions** (lengths, character classes, distinctness) are compared
//!   by [`robust_z`] against the median and MAD of the baseline window. Median
//!   and MAD rather than mean and sigma because one bad run inflates sigma
//!   enough to hide the next five.

use std::collections::HashSet;

use serde_json::Value;

use crate::extract::{CoercionStatus, DocReport, FieldStatus};

/// Hash functions in the k-minhash — 64 keeps the Jaccard estimate's standard
/// error near 1/sqrt(64) = 12%, enough to see a value set turn over, and the
/// sketch a flat 512 bytes.
pub const MINHASH_K: usize = 64;

/// Log2 length buckets. Bucket `i` holds values of `2^i .. 2^(i+1)` characters,
/// saturating at 15 (32k+), which covers every real extracted field.
pub const LEN_BUCKETS: usize = 16;

/// Character classes profiled per field: digit, alphabetic, whitespace, other.
pub const CLS: usize = 4;

/// One field's summary over one run's documents.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldSketch {
    /// Documents this field was evaluated on.
    pub n: u32,
    pub matched: u32,
    pub empty: u32,
    pub error: u32,
    /// `each` containers that matched but held no items — present, not missing.
    pub container_empty: u32,
    pub coerced: u32,
    pub coercion_failed: u32,
    pub len_sum: f64,
    pub len_sumsq: f64,
    pub len_hist: [u16; LEN_BUCKETS],
    /// Character-class fractions of the concatenated values; sums to 1 when the
    /// field produced any characters at all.
    pub cls: [f32; CLS],
    /// Distinct values / non-empty values. The single highest-precision
    /// silent-rebind signal: a per-record field that becomes constant across a
    /// cohort has no benign explanation.
    pub distinct_ratio: f32,
    pub minhash: [u64; MINHASH_K],
}

impl Default for FieldSketch {
    fn default() -> Self {
        Self {
            n: 0,
            matched: 0,
            empty: 0,
            error: 0,
            container_empty: 0,
            coerced: 0,
            coercion_failed: 0,
            len_sum: 0.0,
            len_sumsq: 0.0,
            len_hist: [0; LEN_BUCKETS],
            cls: [0.0; CLS],
            distinct_ratio: 0.0,
            minhash: [u64::MAX; MINHASH_K],
        }
    }
}

impl FieldSketch {
    /// Documents where the selector found nothing (`empty` or `error`).
    /// `container_empty` is deliberately excluded: the listing was there.
    pub fn misses(&self) -> u32 {
        self.empty + self.error
    }

    /// Fraction of documents where the selector found nothing.
    pub fn miss_rate(&self) -> f64 {
        rate(self.misses(), self.n)
    }

    /// Fraction of the documents that had something to coerce whose transform
    /// chain then produced nothing — the wrong-element rate, orthogonal to the
    /// miss rate because its denominator is matched documents only.
    pub fn coercion_failure_rate(&self) -> f64 {
        rate(self.coercion_failed, self.coerced + self.coercion_failed)
    }

    /// Mean value length, or 0 with no values.
    pub fn mean_len(&self) -> f64 {
        let values = self.len_hist.iter().map(|&c| c as f64).sum::<f64>();
        if values <= 0.0 {
            0.0
        } else {
            self.len_sum / values
        }
    }

    /// The length histogram as a probability distribution (all zeros if the
    /// field produced no values).
    pub fn len_distribution(&self) -> [f64; LEN_BUCKETS] {
        let total: f64 = self.len_hist.iter().map(|&c| c as f64).sum();
        let mut out = [0.0; LEN_BUCKETS];
        if total > 0.0 {
            for (o, &c) in out.iter_mut().zip(self.len_hist.iter()) {
                *o = c as f64 / total;
            }
        }
        out
    }
}

fn rate(part: u32, whole: u32) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64
    }
}

/// Accumulates a [`FieldSketch`] one document at a time. Kept separate from the
/// sketch because distinctness needs a set that does not survive into storage.
#[derive(Debug, Default)]
pub struct SketchBuilder {
    sketch: FieldSketch,
    cls_counts: [u64; CLS],
    values: u32,
    distinct: HashSet<u64>,
}

impl SketchBuilder {
    pub fn new() -> Self {
        Self {
            sketch: FieldSketch::default(),
            ..Default::default()
        }
    }

    /// Folds one document's outcome for this field.
    pub fn push(&mut self, status: &FieldStatus, coercion: Option<CoercionStatus>, value: &Value) {
        self.sketch.n += 1;
        match status {
            FieldStatus::Matched => self.sketch.matched += 1,
            FieldStatus::Empty => self.sketch.empty += 1,
            FieldStatus::ContainerEmpty => self.sketch.container_empty += 1,
            FieldStatus::Error { .. } => self.sketch.error += 1,
        }
        // Coercion is only counted where the selector actually matched. A field
        // that found nothing had nothing to coerce, and folding those documents
        // into the denominator would make the coercion rate a diluted copy of the
        // miss rate instead of an orthogonal signal.
        if matches!(status, FieldStatus::Matched) {
            match coercion {
                Some(CoercionStatus::Coerced) => self.sketch.coerced += 1,
                Some(CoercionStatus::CoercionFailed) => self.sketch.coercion_failed += 1,
                Some(CoercionStatus::NoTransforms) | None => {}
            }
        }
        let text = value_text(value);
        if text.is_empty() {
            return;
        }
        self.values += 1;
        let len = text.chars().count();
        self.sketch.len_sum += len as f64;
        self.sketch.len_sumsq += (len as f64) * (len as f64);
        self.sketch.len_hist[len_bucket(len)] += 1;
        for ch in text.chars() {
            let class = if ch.is_ascii_digit() {
                0
            } else if ch.is_alphabetic() {
                1
            } else if ch.is_whitespace() {
                2
            } else {
                3
            };
            self.cls_counts[class] += 1;
        }
        let h = hash64(text.as_bytes());
        self.distinct.insert(h);
        for (i, slot) in self.sketch.minhash.iter_mut().enumerate() {
            *slot = (*slot).min(mix(h ^ mix(i as u64 + 0x9e37_79b9_7f4a_7c15)));
        }
    }

    /// Finishes the sketch: normalizes the character-class profile and computes
    /// the distinct ratio.
    pub fn finish(mut self) -> FieldSketch {
        let total: u64 = self.cls_counts.iter().sum();
        if total > 0 {
            for (out, &count) in self.sketch.cls.iter_mut().zip(self.cls_counts.iter()) {
                *out = (count as f64 / total as f64) as f32;
            }
        }
        self.sketch.distinct_ratio = if self.values == 0 {
            0.0
        } else {
            self.distinct.len() as f32 / self.values as f32
        };
        self.sketch
    }
}

/// Builds one sketch per field from a run's `(values, report)` pairs.
///
/// Fields are taken from the reports, not from the values, so a field whose rule
/// errored on every document still gets a sketch — the miss is the signal.
pub fn sketch_run<'a>(
    docs: impl IntoIterator<Item = (&'a Value, &'a DocReport)>,
) -> std::collections::BTreeMap<String, FieldSketch> {
    let mut builders: std::collections::BTreeMap<String, SketchBuilder> = Default::default();
    for (values, report) in docs {
        for (field, status) in &report.fields {
            let value = values.get(field).unwrap_or(&Value::Null);
            builders.entry(field.clone()).or_default().push(
                status,
                report.coercion.get(field).copied(),
                value,
            );
        }
    }
    builders.into_iter().map(|(k, b)| (k, b.finish())).collect()
}

/// The comparable text of an extracted value. Arrays and objects render as
/// compact JSON so a field that returns structure still has a length, a
/// character profile and an identity — a repeating `each` block that collapses
/// to the same object on every page is exactly the rebind we want to catch.
pub fn value_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn len_bucket(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    (usize::BITS - 1 - len.leading_zeros()).min(LEN_BUCKETS as u32 - 1) as usize
}

/// FNV-1a + splitmix64 finalizer — the same version-stable construction the
/// SimHash tokenizer uses, for the same reason: these hashes are persisted, and
/// `DefaultHasher` has no documented cross-version output stability.
fn hash64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix(hash)
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Estimated Jaccard similarity of two value multisets from their minhashes.
/// `None` when either sketch saw no values, where the answer is undefined rather
/// than 1.0 (two empty sets agreeing on nothing is not agreement).
pub fn jaccard(a: &[u64; MINHASH_K], b: &[u64; MINHASH_K]) -> Option<f64> {
    if a.iter().all(|&h| h == u64::MAX) || b.iter().all(|&h| h == u64::MAX) {
        return None;
    }
    let agree = a.iter().zip(b.iter()).filter(|(x, y)| x == y).count();
    Some(agree as f64 / MINHASH_K as f64)
}

/// Wilson score interval for a proportion at ~95% confidence.
///
/// Used instead of a raw rate comparison so run size is accounted for
/// structurally: 1 miss in 3 documents and 300 in 900 are both 33%, but only the
/// second one's interval is narrow enough to separate from a 5% baseline.
///
/// Note what this does *not* do. Separation is a statement about evidence, and
/// three misses out of three against a 5% baseline genuinely is evidence
/// (p < 10^-3), so it separates. Wilson alone therefore does not make small runs
/// inert — the cohort floor does, and the two are complementary rather than
/// redundant.
pub fn wilson(successes: u32, n: u32) -> (f64, f64) {
    if n == 0 {
        return (0.0, 1.0);
    }
    const Z: f64 = 1.96;
    let n = n as f64;
    let p = successes as f64 / n;
    let z2 = Z * Z;
    let denom = 1.0 + z2 / n;
    let centre = p + z2 / (2.0 * n);
    let margin = Z * ((p * (1.0 - p) / n) + z2 / (4.0 * n * n)).sqrt();
    (
        ((centre - margin) / denom).max(0.0),
        ((centre + margin) / denom).min(1.0),
    )
}

/// True when `run`'s rate is separated *upward* from `baseline`'s — the run's
/// lower bound above the baseline's upper bound. Separation, not a raw
/// difference, is what a small run cannot fake.
pub fn rate_rose(run: (u32, u32), baseline: (u32, u32)) -> bool {
    let (run_lo, _) = wilson(run.0, run.1);
    let (_, base_hi) = wilson(baseline.0, baseline.1);
    run_lo > base_hi
}

/// Median of a sample (`None` when empty). Copies, so the caller's slice is
/// untouched.
pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    })
}

/// Modified z-score against a baseline sample (Iglewicz–Hoaglin):
/// `0.6745 * (x - median) / MAD`. `None` when the baseline is too short to say
/// anything (fewer than [`MIN_BASELINE_RUNS`] samples).
///
/// Two fallbacks matter more than the headline formula:
/// - MAD of zero with non-zero spread (a skewed sample where over half the
///   values are identical) falls back to the mean absolute deviation.
/// - A baseline that has *never* varied — `distinct_ratio` of exactly 1.0 on
///   twenty consecutive runs is the common case — has no scale at all. Returning
///   `None` there would disable the signal exactly where it is cleanest, so any
///   departure beyond `tol` reads as infinitely significant.
pub fn robust_z(x: f64, baseline: &[f64], tol: f64) -> Option<f64> {
    if baseline.len() < MIN_BASELINE_RUNS {
        return None;
    }
    let med = median(baseline)?;
    let deviations: Vec<f64> = baseline.iter().map(|v| (v - med).abs()).collect();
    let mad = median(&deviations).unwrap_or(0.0);
    if mad > 1e-12 {
        return Some(0.6745 * (x - med) / mad);
    }
    let mean_ad = deviations.iter().sum::<f64>() / deviations.len() as f64;
    if mean_ad > 1e-12 {
        return Some((x - med) / (1.253_314 * mean_ad));
    }
    let delta = x - med;
    Some(if delta.abs() <= tol {
        0.0
    } else if delta > 0.0 {
        f64::INFINITY
    } else {
        f64::NEG_INFINITY
    })
}

/// Baseline runs needed before a distributional comparison is attempted. Below
/// three, a median and a MAD describe the noise rather than the source.
pub const MIN_BASELINE_RUNS: usize = 3;

/// Total variation distance between two distributions over the same support:
/// half the L1 distance, in `[0,1]`.
pub fn total_variation(a: &[f64], b: &[f64]) -> f64 {
    let sum: f64 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum();
    (sum / 2.0).clamp(0.0, 1.0)
}

/// Element-wise median of several distributions — the baseline's typical shape,
/// robust to one odd run.
pub fn median_distribution<const N: usize>(samples: &[[f64; N]]) -> [f64; N] {
    let mut out = [0.0; N];
    for (i, slot) in out.iter_mut().enumerate() {
        let column: Vec<f64> = samples.iter().map(|s| s[i]).collect();
        *slot = median(&column).unwrap_or(0.0);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::{CoercionStatus, FieldStatus};
    use serde_json::json;

    fn push_values(b: &mut SketchBuilder, values: &[&str]) {
        for v in values {
            b.push(
                &FieldStatus::Matched,
                Some(CoercionStatus::NoTransforms),
                &json!(v),
            );
        }
    }

    #[test]
    fn distinct_ratio_separates_a_per_record_field_from_a_constant_one() {
        let mut per_record = SketchBuilder::new();
        push_values(&mut per_record, &["$10", "$20", "$30", "$40"]);
        assert_eq!(per_record.finish().distinct_ratio, 1.0);

        // The silent rebind: the selector now matches a site-wide banner.
        let mut rebound = SketchBuilder::new();
        push_values(&mut rebound, &["Free shipping"; 4]);
        assert_eq!(rebound.finish().distinct_ratio, 0.25);
    }

    #[test]
    fn character_profile_moves_when_a_price_field_becomes_prose() {
        let mut digits = SketchBuilder::new();
        push_values(&mut digits, &["1099", "2599", "3999"]);
        let digits = digits.finish();
        assert!(digits.cls[0] > 0.9, "digit fraction {:?}", digits.cls);

        let mut prose = SketchBuilder::new();
        push_values(&mut prose, &["Add to cart", "Add to cart", "Out of stock"]);
        let prose = prose.finish();
        assert!(prose.cls[1] > 0.6, "alpha fraction {:?}", prose.cls);
        // The two profiles are far apart under the same metric the detector uses.
        let a: Vec<f64> = digits.cls.iter().map(|&f| f as f64).collect();
        let b: Vec<f64> = prose.cls.iter().map(|&f| f as f64).collect();
        assert!(total_variation(&a, &b) > 0.5);
    }

    #[test]
    fn misses_exclude_a_container_that_matched_but_held_nothing() {
        let mut b = SketchBuilder::new();
        b.push(&FieldStatus::Empty, None, &Value::Null);
        b.push(&FieldStatus::ContainerEmpty, None, &json!([]));
        b.push(
            &FieldStatus::Error { detail: "x".into() },
            None,
            &Value::Null,
        );
        b.push(&FieldStatus::Matched, None, &json!("v"));
        let s = b.finish();
        assert_eq!(s.n, 4);
        assert_eq!(s.misses(), 2, "empty + error, never container_empty");
        assert_eq!(s.container_empty, 1);
        assert_eq!(s.miss_rate(), 0.5);
    }

    #[test]
    fn coercion_failure_rate_is_over_matched_docs_not_all_docs() {
        let mut b = SketchBuilder::new();
        b.push(
            &FieldStatus::Matched,
            Some(CoercionStatus::CoercionFailed),
            &Value::Null,
        );
        b.push(
            &FieldStatus::Matched,
            Some(CoercionStatus::Coerced),
            &json!(10),
        );
        b.push(
            &FieldStatus::Empty,
            Some(CoercionStatus::Coerced),
            &Value::Null,
        );
        let s = b.finish();
        // 1 of the 2 documents that had something to coerce.
        assert!((s.coercion_failure_rate() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn wilson_separation_needs_evidence_not_just_a_big_ratio() {
        // One miss out of 3 is a 33% rate against a 5% baseline — a dramatic
        // ratio, and no evidence at all. The interval swallows it.
        assert!(
            !rate_rose((1, 3), (15, 300)),
            "a 1-of-3 miss must not separate"
        );
        // The same rate over 60 documents is real evidence.
        assert!(rate_rose((20, 60), (15, 300)));
        // A rate that did not actually rise never separates, at any size.
        assert!(!rate_rose((15, 300), (15, 300)));
        assert!(!rate_rose((5, 300), (15, 300)));
        // And a rate that rose to certainty separates even on a tiny run — three
        // misses out of three under a 5% baseline is p < 10^-3, so calling that
        // "not evidence" would be the wrong answer. Small runs are made inert by
        // the cohort floor, not by pretending this interval is wider than it is.
        assert!(rate_rose((3, 3), (15, 300)));
    }

    #[test]
    fn robust_z_flags_an_outlier_and_tolerates_a_noisy_source() {
        // A source that varies between 0.10 and 0.14: 0.12 is normal, 0.9 is not.
        let baseline = [0.10, 0.12, 0.11, 0.14, 0.12, 0.13];
        assert!(robust_z(0.12, &baseline, 0.02).unwrap().abs() < 3.5);
        assert!(robust_z(0.90, &baseline, 0.02).unwrap() > 3.5);
        // One earlier bad run does not blow up the scale enough to hide the next.
        let poisoned = [0.10, 0.12, 0.95, 0.14, 0.12, 0.13];
        assert!(
            robust_z(0.90, &poisoned, 0.02).unwrap() > 3.5,
            "median/MAD must survive one outlier in the window"
        );
    }

    #[test]
    fn robust_z_treats_a_never_varying_baseline_as_zero_scale() {
        // distinct_ratio == 1.0 on every healthy run is the common case; a drop
        // to 0.03 has to fire even though the sample has no variance to scale by.
        let flat = [1.0, 1.0, 1.0, 1.0];
        assert_eq!(robust_z(1.0, &flat, 0.02), Some(0.0));
        assert_eq!(robust_z(0.03, &flat, 0.02), Some(f64::NEG_INFINITY));
        // Float noise inside the tolerance is not an outlier.
        assert_eq!(robust_z(0.995, &flat, 0.02), Some(0.0));
        // Too short a baseline says nothing rather than guessing.
        assert_eq!(robust_z(0.03, &[1.0, 1.0], 0.02), None);
    }

    #[test]
    fn minhash_tracks_value_set_turnover() {
        let mut a = SketchBuilder::new();
        push_values(&mut a, &["a", "b", "c", "d", "e", "f", "g", "h"]);
        let a = a.finish();

        let mut same = SketchBuilder::new();
        push_values(&mut same, &["a", "b", "c", "d", "e", "f", "g", "h"]);
        assert_eq!(jaccard(&a.minhash, &same.finish().minhash), Some(1.0));

        let mut disjoint = SketchBuilder::new();
        push_values(&mut disjoint, &["q", "r", "s", "t", "u", "v", "w", "x"]);
        assert!(jaccard(&a.minhash, &disjoint.finish().minhash).unwrap() < 0.3);

        // An empty set has no defined similarity to anything.
        assert_eq!(jaccard(&a.minhash, &FieldSketch::default().minhash), None);
    }

    #[test]
    fn length_histogram_buckets_by_magnitude() {
        let mut b = SketchBuilder::new();
        push_values(&mut b, &["ab", "abcd", "abcdefgh"]); // 2, 4, 8 chars
        let s = b.finish();
        assert_eq!(s.len_hist[1], 1);
        assert_eq!(s.len_hist[2], 1);
        assert_eq!(s.len_hist[3], 1);
        assert!((s.mean_len() - 14.0 / 3.0).abs() < 1e-9);
        let dist = s.len_distribution();
        assert!((dist.iter().sum::<f64>() - 1.0).abs() < 1e-9);
    }
}
