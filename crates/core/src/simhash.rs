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
    use super::{build_hash_stem, dom_simhash_str, drift, hamming, simhash};

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

    #[test]
    fn identical_is_zero_distance() {
        let a = simhash("same content here");
        let b = simhash("same content here");
        assert_eq!(hamming(a, b), 0);
    }
}
