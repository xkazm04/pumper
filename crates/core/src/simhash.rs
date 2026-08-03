//! SimHash near-duplicate detection. Produces a 64-bit fingerprint where
//! *similar* documents get *similar* fingerprints (small Hamming distance),
//! unlike a normal content hash where one byte flips everything. Lets the
//! dataset store detect near-duplicate pages — not just exact changes — with no
//! external service. Pure Rust and deterministic (version-stable FNV-1a hash).
//!
//! Two fingerprints live here, and the asymmetry between them is what the
//! extraction-health detector runs on:
//!
//! - [`simhash`] over text is **structure-blind** — it moves when the words move.
//! - [`dom_simhash`] over markup shape is **text-blind** — it moves when the DOM
//!   moves.
//!
//! Extraction is structure-bound, so "text still, markup moved, output moved" is
//! the redesign-broke-the-extractor signature, and "text moved, markup still" is
//! a healthy source reporting new content. Neither fingerprint can tell those
//! apart alone.

use scraper::Html;
use serde_json::Value;

/// 64-bit SimHash of the token stream in `text`.
pub fn simhash(text: &str) -> u64 {
    simhash_tokens(tokenize(text).map(|t| hash_token(&t)))
}

/// SimHash over the document's *shape*: the pre-order sequence of
/// `(tag, sorted class tokens, id-presence)` triples, text nodes ignored. Class
/// tokens that look like build hashes (`btn-1a2b3c4d`) fold to a placeholder, so
/// a webpack/Tailwind rebuild that changes nothing visible does not read as a
/// redesign — the false positive that would otherwise fire on every deploy.
///
/// Frequency-weighted like the text fingerprint: 50 product cards with the same
/// classes cast 50 votes, so renaming that class moves the fingerprint hard while
/// inserting one wrapper barely moves it.
pub fn dom_simhash(html: &Html) -> u64 {
    let mut buf = String::new();
    simhash_tokens(html.tree.nodes().filter_map(|node| {
        let el = node.value().as_element()?;
        buf.clear();
        buf.push_str(el.name());
        buf.push('|');
        // Classes in a stable order: markup that reorders a class list is the
        // same shape, and `scraper` preserves source order.
        let mut classes: Vec<&str> = el.classes().collect();
        classes.sort_unstable();
        for class in classes {
            match build_hash_stem(class) {
                // Keep the stable stem, drop the churning digest: `card-1a2b3c4d`
                // and `card-5e6f7a8b` are the same class, `tile-5e6f7a8b` is not.
                Some(stem) => {
                    buf.push_str(stem);
                    buf.push_str("-#");
                }
                None => buf.push_str(class),
            }
            buf.push('.');
        }
        buf.push('|');
        // Id *presence*, not the id itself: per-item ids (`item-4712`) would make
        // every page structurally unique and the fingerprint useless.
        if el.id().is_some() {
            buf.push('#');
        }
        Some(hash_token(&buf))
    }))
}

/// [`dom_simhash`] for a caller holding only the raw document. Parses the HTML;
/// prefer the `&Html` form when a parsed tree is already at hand.
pub fn dom_simhash_str(doc: &str) -> u64 {
    dom_simhash(&Html::parse_document(doc))
}

/// The stable stem of a class token that ends in a build digest — `name` +
/// separator + ≥6 hex digits (`css-1a2b3c4d` → `css`, `header_9f8e7d6c` →
/// `header`) — or `None` for a hand-written name.
///
/// The design folded the *whole* token to one placeholder; that also erases the
/// stem, so renaming `card-1a2b3c4d` to `tile-5e6f7a8b` became invisible — real
/// signal thrown away to suppress noise. Keeping the stem suppresses exactly the
/// digest churn and nothing else. Deliberately narrow: `text-gray-500` and
/// `col-md-6` are hand-written and must keep their identity.
pub(crate) fn build_hash_stem(class: &str) -> Option<&str> {
    let sep = class.rfind(['-', '_'])?;
    let (stem, digest) = (&class[..sep], &class[sep + 1..]);
    let looks_generated = !stem.is_empty()
        && stem.chars().all(|c| c.is_ascii_alphabetic())
        && digest.len() >= 6
        && digest.chars().all(|c| c.is_ascii_hexdigit())
        // An all-alphabetic "digest" like `header-feedbed` is a word, not a hash.
        && digest.chars().any(|c| c.is_ascii_digit());
    looks_generated.then_some(stem)
}

/// Folds pre-hashed tokens into a 64-bit fingerprint by per-bit majority vote.
/// Shared by the text and DOM fingerprints so the two are comparable in the same
/// Hamming space (and a change to the fold can't silently apply to only one).
fn simhash_tokens(tokens: impl Iterator<Item = u64>) -> u64 {
    let mut bits = [0i32; 64];
    let mut seen = false;
    for h in tokens {
        seen = true;
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

/// Normalized fingerprint drift in `[0,1]`: Hamming distance over the 64 bits.
/// The scale every drift threshold in the health detector is expressed in.
pub fn drift(a: u64, b: u64) -> f64 {
    hamming(a, b) as f64 / 64.0
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

/// Banded SimHash index — candidate lookup instead of an O(n) scan per query,
/// with **the same Hamming-distance decision** a linear scan would make.
///
/// Pigeonhole: two 64-bit hashes within Hamming distance `d` differ in at most
/// `d` bits, so across `b = d + 1` contiguous bit-bands at least one band is
/// bit-identical. Every entry is bucketed by all `b` of its band values; a query
/// gathers candidates from its own band buckets and verifies the true Hamming
/// distance. No false negatives (a true near-dup always shares a band) and no
/// false positives (every candidate is verified).
///
/// This is the one banded implementation in the tree. It grew inside the crawler
/// (`crawl::SimHashIndex`, which now wraps it) for streaming "have I seen
/// something like this?"; the dataset store's `duplicate_pairs` uses the same
/// buckets to enumerate pairs. Forking a second copy would let the two drift on
/// the band arithmetic, which is exactly where an off-by-one silently becomes a
/// false negative.
///
/// `T` is a caller payload carried alongside each hash (a record key, a row
/// index, or `()` when only the boolean answer is wanted).
#[derive(Debug, Clone)]
pub struct BandedIndex<T> {
    distance: u32,
    /// Per-band `(shift, mask)` extracting that band's value from a hash. EMPTY
    /// when banding is not selective at this distance — see [`band_widths`].
    segs: Vec<(u32, u64)>,
    /// Per-band bucket: band value -> indices into `entries`.
    buckets: Vec<std::collections::HashMap<u64, Vec<usize>>>,
    entries: Vec<(u64, T)>,
}

/// Narrowest band that still filters. The pigeonhole guarantee forces `d + 1`
/// bands over 64 bits, so a band is `64 / (d + 1)` bits wide and holds roughly
/// `n / 2^width` entries — at `d = 3` that is 16 bits (a handful of candidates
/// out of 50k), at `d = 8` it is 7 bits and every query touches thousands.
/// Past this width the buckets stop discriminating and the bookkeeping is pure
/// overhead on top of the same Hamming verification.
const MIN_BAND_BITS: u32 = 10;

/// Band `(shift, mask)` pairs for `distance`, or an EMPTY vec when bands that
/// narrow would not filter.
///
/// Returning nothing is deliberate: it makes "banding does not pay here" a
/// property of the index rather than a second algorithm at the call site. The
/// caller's code path, the candidate verification and the answer are identical
/// either way — only the candidate *generation* degrades to a walk.
fn band_widths(distance: u32) -> Vec<(u32, u64)> {
    // b = d + 1 bands guarantee a shared band for any pair within distance d.
    let bands = (distance + 1).clamp(1, 64) as usize;
    if (64 / bands as u32) < MIN_BAND_BITS {
        return Vec::new();
    }
    let base = 64 / bands;
    let rem = 64 % bands;
    let mut segs = Vec::with_capacity(bands);
    let mut shift = 0u32;
    for i in 0..bands {
        let width = base + if i < rem { 1 } else { 0 };
        let mask = if width == 64 {
            u64::MAX
        } else {
            (1u64 << width) - 1
        };
        segs.push((shift, mask));
        shift += width as u32;
    }
    segs
}

impl<T> BandedIndex<T> {
    /// An index answering "within `distance` bits". Any distance is valid,
    /// including 0 (exact match) and distances too large for banding to help.
    pub fn new(distance: u32) -> Self {
        let segs = band_widths(distance);
        let buckets = vec![std::collections::HashMap::new(); segs.len()];
        Self {
            distance,
            segs,
            buckets,
            entries: Vec::new(),
        }
    }

    /// Whether candidate lookup is bucket-backed at this distance (false = the
    /// index verifies against a plain walk; same answers, linear candidates).
    pub fn is_banded(&self) -> bool {
        !self.segs.is_empty()
    }

    pub fn distance(&self) -> u32 {
        self.distance
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Adds `hash` under every one of its band values.
    pub fn insert(&mut self, hash: u64, payload: T) {
        let slot = self.entries.len();
        for (i, (shift, mask)) in self.segs.iter().enumerate() {
            let band = (hash >> shift) & mask;
            self.buckets[i].entry(band).or_default().push(slot);
        }
        self.entries.push((hash, payload));
    }

    /// True when some already-inserted entry is within `distance` of `hash` —
    /// identical to `entries.iter().any(|(h, _)| hamming(*h, hash) <= d)`, which
    /// is what it replaces. Short-circuits, so it never materializes candidates.
    pub fn is_near_dup(&self, hash: u64) -> bool {
        if !self.is_banded() {
            return self
                .entries
                .iter()
                .any(|(h, _)| hamming(*h, hash) <= self.distance);
        }
        for (i, (shift, mask)) in self.segs.iter().enumerate() {
            let band = (hash >> shift) & mask;
            if let Some(slots) = self.buckets[i].get(&band) {
                if slots
                    .iter()
                    .any(|&s| hamming(self.entries[s].0, hash) <= self.distance)
                {
                    return true;
                }
            }
        }
        false
    }

    /// Visits every entry whose slot is **after** `after` and whose hash is
    /// within `distance` of `hash`, in ascending slot order, until `visit`
    /// returns `false`.
    ///
    /// Three properties the shape buys, all of which a "collect the buckets,
    /// sort, dedup, filter" version loses:
    ///
    /// - **Ascending, deduped, no allocation.** Bucket lists are already in
    ///   insertion order, so this is a k-way merge across the `d + 1` of them
    ///   (`d + 1 <= 64`, so the linear min-scan beats a heap). A pair sharing
    ///   several bands is still visited once.
    /// - **`after` is a skip, not a filter.** Each bucket is binary-searched to
    ///   the first slot past `after`, so an all-pairs walk costs
    ///   `sum(n - i) = n²/2` in the *degenerate* case where one bucket holds
    ///   everything — never *worse* than the linear scan it replaces. Skewed
    ///   corpora (shared boilerplate collapsing a band) are the realistic case
    ///   and this is what stops them being a regression.
    /// - **Early exit.** A capped caller stops paying the moment its budget is
    ///   met, instead of materializing every candidate first.
    pub fn for_each_neighbor_after<F>(&self, hash: u64, after: usize, mut visit: F)
    where
        F: FnMut(usize, &T) -> bool,
    {
        if !self.is_banded() {
            // Indexed loop, not an iterator chain: this is the hot inner loop of
            // an n²/2 walk and the chain costs ~1.5x in an unoptimized build,
            // which is what the tests and the local binary run.
            for slot in (after + 1)..self.entries.len() {
                let (candidate, payload) = &self.entries[slot];
                if hamming(*candidate, hash) <= self.distance && !visit(slot, payload) {
                    return;
                }
            }
            return;
        }
        let mut cursors: Vec<&[usize]> = Vec::with_capacity(self.segs.len());
        for (i, (shift, mask)) in self.segs.iter().enumerate() {
            let band = (hash >> shift) & mask;
            if let Some(bucket) = self.buckets[i].get(&band) {
                let start = bucket.partition_point(|&slot| slot <= after);
                if start < bucket.len() {
                    cursors.push(&bucket[start..]);
                }
            }
        }
        loop {
            let mut next = usize::MAX;
            for cursor in &cursors {
                if let Some(&slot) = cursor.first() {
                    next = next.min(slot);
                }
            }
            if next == usize::MAX {
                return;
            }
            for cursor in cursors.iter_mut() {
                while cursor.first() == Some(&next) {
                    *cursor = &cursor[1..];
                }
            }
            let (candidate, payload) = &self.entries[next];
            if hamming(*candidate, hash) <= self.distance && !visit(next, payload) {
                return;
            }
        }
    }

    /// Every entry within `distance` of `hash`, as `(slot, &payload)` in
    /// insertion order. Convenience over
    /// [`for_each_neighbor_after`](Self::for_each_neighbor_after) for callers
    /// that want the whole (bounded) neighbourhood.
    pub fn neighbors(&self, hash: u64) -> Vec<(usize, &T)> {
        let mut out = Vec::new();
        if !self.is_banded() {
            return self
                .entries
                .iter()
                .enumerate()
                .filter(|(_, (h, _))| hamming(*h, hash) <= self.distance)
                .map(|(slot, (_, payload))| (slot, payload))
                .collect();
        }
        // `after` is exclusive and slots start at 0, so there is no "before the
        // first slot" index — walk every band from its head instead.
        for (i, (shift, mask)) in self.segs.iter().enumerate() {
            let band = (hash >> shift) & mask;
            if let Some(bucket) = self.buckets[i].get(&band) {
                out.extend_from_slice(bucket);
            }
        }
        out.sort_unstable();
        out.dedup();
        out.retain(|&s| hamming(self.entries[s].0, hash) <= self.distance);
        out.into_iter().map(|s| (s, &self.entries[s].1)).collect()
    }

    /// The hashes held, in insertion order.
    pub fn hashes(&self) -> Vec<u64> {
        self.entries.iter().map(|(h, _)| *h).collect()
    }
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
    use super::{build_hash_stem, dom_simhash_str, drift, hamming, simhash, BandedIndex};

    /// A list page with `n` cards using the given class names — the shape a real
    /// redesign changes.
    fn page(card: &str, price: &str, texts: &[&str]) -> String {
        let mut html = String::from("<html><body><div id=\"main\">");
        for (i, text) in texts.iter().enumerate() {
            html.push_str(&format!(
                "<div class=\"{card}\"><h3 class=\"t\">{text}</h3>\
                 <span class=\"{price}\">${i}9.99</span></div>"
            ));
        }
        html.push_str("</div></body></html>");
        html
    }

    const ITEMS: &[&str] = &[
        "Widget",
        "Gadget",
        "Doohickey",
        "Sprocket",
        "Flange",
        "Gizmo",
    ];

    #[test]
    fn dom_fingerprint_moves_on_a_class_rename_and_not_on_a_text_change() {
        let before = dom_simhash_str(&page("card", "price", ITEMS));
        // Negative control: same markup, entirely different words. The DOM
        // fingerprint MUST NOT move — this is the "genuine content change" case
        // that must never read as a broken extractor.
        let text_only = dom_simhash_str(&page(
            "card",
            "price",
            &["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta"],
        ));
        assert_eq!(before, text_only, "dom fingerprint must be text-blind");

        // The redesign: class names changed, words identical.
        let renamed = dom_simhash_str(&page("product-tile", "amount", ITEMS));
        assert!(
            drift(before, renamed) >= 0.20,
            "a class rename must move the dom fingerprint (drift {})",
            drift(before, renamed)
        );

        // And the text fingerprint sees exactly the opposite pair.
        let t_before = simhash(&page("card", "price", ITEMS));
        let t_renamed = simhash(&page("product-tile", "amount", ITEMS));
        assert!(
            drift(t_before, t_renamed) < 0.20,
            "a class rename must barely move the text fingerprint"
        );
    }

    #[test]
    fn dom_fingerprint_ignores_build_hash_class_churn() {
        // A webpack/Tailwind rebuild: same markup, regenerated hash suffixes.
        let a = dom_simhash_str(&page("card-1a2b3c4d", "price-9f8e7d6c", ITEMS));
        let b = dom_simhash_str(&page("card-5e6f7a8b", "price-0c1d2e3f", ITEMS));
        assert_eq!(a, b, "build-hash churn must fold to the same fingerprint");
        // But a real rename of the same element still moves it.
        let renamed = dom_simhash_str(&page("tile-5e6f7a8b", "cost-0c1d2e3f", ITEMS));
        assert!(drift(a, renamed) >= 0.15, "drift {}", drift(a, renamed));
    }

    #[test]
    fn build_hash_folding_keeps_the_stem_and_spares_hand_written_names() {
        assert_eq!(build_hash_stem("css-1a2b3c4d"), Some("css"));
        assert_eq!(build_hash_stem("header_9f8e7d6c"), Some("header"));
        // Hand-written utility classes must keep their identity.
        assert_eq!(build_hash_stem("text-gray-500"), None);
        assert_eq!(build_hash_stem("col-md-6"), None);
        assert_eq!(build_hash_stem("card"), None);
        // Short suffixes are not digests, and an all-letter suffix is a word.
        assert_eq!(build_hash_stem("btn-abc"), None);
        assert_eq!(build_hash_stem("header-feedbed"), None);
    }

    #[test]
    fn a_textless_document_still_has_a_dom_fingerprint() {
        // The empty sentinel means "no tokens at all". A document with structure
        // but no words has no text fingerprint and a real DOM one — the whole point
        // of keeping the two separate.
        let markup = "<html><body><div class=\"a\"><span class=\"b\"></span></div></body></html>";
        assert_eq!(simhash(""), 0);
        assert_ne!(dom_simhash_str(markup), 0);
        // html5ever synthesizes html/head/body for any input, so even "" has shape.
        assert_ne!(dom_simhash_str(""), 0);
    }

    #[test]
    fn near_duplicates_are_close() {
        let a = simhash("The quick brown fox jumps over the lazy dog in the yard");
        // One word changed → should stay within a small Hamming radius.
        let b = simhash("The quick brown fox jumps over the lazy cat in the yard");
        assert!(
            hamming(a, b) <= 6,
            "near-dup distance too large: {}",
            hamming(a, b)
        );
    }

    #[test]
    fn different_texts_are_far() {
        let a = simhash("annual budget report for the finance department fiscal year");
        let b = simhash("photographs of tropical birds migrating across the ocean at dawn");
        assert!(
            hamming(a, b) >= 18,
            "unrelated distance too small: {}",
            hamming(a, b)
        );
    }

    /// A spread of hashes: a base, controlled neighbours at several Hamming
    /// distances, exact duplicates, and unrelated values.
    fn index_fixture() -> Vec<u64> {
        let base: u64 = 0xa5c3_9f10_7e2b_4d68;
        let flip = |bits: &[u32]| bits.iter().fold(base, |h, b| h ^ (1u64 << b));
        vec![
            base,
            flip(&[3]),
            0x1122_3344_5566_7788,
            flip(&[9, 41]),
            base,
            flip(&[0, 1, 2, 3, 4, 5, 6, 7, 8]),
            !base,
            flip(&[62, 63]),
            0,
            flip(&[11, 27, 43, 59]),
        ]
    }

    #[test]
    fn neighbor_walk_matches_the_linear_scan_across_both_banding_regimes() {
        // Distances 0..=25 straddle MIN_BAND_BITS: small ones bucket, large ones
        // fall back to a walk. The ANSWER must not depend on which — a regime
        // that quietly drops pairs is a false negative nobody notices, so the
        // sweep asserts the exact slot list against the definition.
        let hashes = index_fixture();
        for distance in 0..=25u32 {
            let mut index: BandedIndex<usize> = BandedIndex::new(distance);
            for (i, h) in hashes.iter().enumerate() {
                index.insert(*h, i);
            }
            for (i, query) in hashes.iter().enumerate() {
                for after in [0usize, i, hashes.len() - 1] {
                    let mut got = Vec::new();
                    index.for_each_neighbor_after(*query, after, |slot, &payload| {
                        assert_eq!(slot, payload, "payload must ride with its slot");
                        got.push(slot);
                        true
                    });
                    let want: Vec<usize> = (after + 1..hashes.len())
                        .filter(|&j| hamming(hashes[j], *query) <= distance)
                        .collect();
                    assert_eq!(
                        got,
                        want,
                        "distance {distance}, query {i}, after {after}: banded={}",
                        index.is_banded()
                    );
                }
            }
        }
    }

    #[test]
    fn a_neighbor_walk_stops_when_the_visitor_says_stop() {
        // The early exit is what lets a capped caller (duplicate_pairs) avoid
        // materializing an unbounded candidate set.
        let mut index: BandedIndex<usize> = BandedIndex::new(64);
        for i in 0..50usize {
            index.insert(0xdead_beef_0000_0000, i);
        }
        let mut seen = 0;
        index.for_each_neighbor_after(0xdead_beef_0000_0000, 0, |_, _| {
            seen += 1;
            seen < 3
        });
        assert_eq!(seen, 3, "must stop at the visitor's word, not walk all 49");
    }

    #[test]
    fn banding_turns_itself_off_rather_than_bucketing_into_noise() {
        // Bands are 64/(d+1) bits wide; past a point they stop discriminating
        // and the bookkeeping is pure overhead on the same verification. The
        // index must degrade to a walk on its own — a call site that had to know
        // would be a second algorithm to keep in sync.
        assert!(BandedIndex::<()>::new(0).is_banded());
        assert!(BandedIndex::<()>::new(5).is_banded(), "64/6 = 10 bits");
        assert!(!BandedIndex::<()>::new(6).is_banded(), "64/7 = 9 bits");
        assert!(!BandedIndex::<()>::new(20).is_banded());
    }

    #[test]
    fn identical_is_zero_distance() {
        let a = simhash("same content here");
        let b = simhash("same content here");
        assert_eq!(hamming(a, b), 0);
    }
}
