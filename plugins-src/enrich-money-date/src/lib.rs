//! Reference index-time ENRICHER plugin for Pumper (N11).
//!
//! It reproduces the two entity kinds the search index has always shipped —
//! `amount` (largest US-dollar figure carrying an explicit currency marker) and
//! `event_date` (earliest *upcoming* deadline-like date) — as an installable
//! `.wasm`, so the enricher hook is proven by a plugin that does exactly what
//! the built-in pass does. Point `[search] enrichers` at
//! `["plugin:enrich-money-date"]` (INSTEAD of `"builtin"`) and the index gets
//! the same two fields from a module you can edit and reinstall without
//! rebuilding the server, or list it after `"builtin"` to add kinds of your own
//! in a copy of this crate.
//!
//! Host ABI (the core-module one every Pumper plugin speaks):
//!   alloc(len) -> ptr        reserve `len` bytes in linear memory
//!   enrich(ptr, len) -> u64  input is a `{"doc": <text>, "params": {...}}`
//!                            envelope; output is packed `(ptr << 32) | len`
//!   describe() -> u64        self-describing manifest (`kind: enricher`)
//!
//! Output contract: `{"entities": {kind: scalar|array}}`. A kind the document
//! does not carry is ABSENT, never null and never zero — the host stores what it
//! is given, and a zero `amount` would match every `amount_gte` filter.
//!
//! ## `params.now` is not optional
//!
//! A wasm32 guest has NO clock. "Is this deadline still upcoming" therefore
//! cannot be answered inside the sandbox at all, so the host puts the
//! document's own timestamp (its `indexed_at`) in `params.now` and this plugin
//! judges against that. Without it every date rule would either be disabled or,
//! worse, silently pick an epoch and call every 2019 deadline upcoming — which
//! is why a missing `params.now` yields NO `event_date` rather than a guess.

use regex::Regex;
use serde_json::{json, Value};

/// Amounts above one trillion dollars are extraction noise (concatenated
/// digits, ids), dropped rather than emitted.
const MAX_AMOUNT_DOLLARS: f64 = 1_000_000_000_000.0;
/// A deadline further out than 10 years is far more likely a parse artifact.
const MAX_HORIZON_SECS: i64 = 10 * 365 * 24 * 3600;
/// A deadline "today" must not vanish partway through the day.
const TODAY_GRACE_SECS: i64 = 86_400;
/// How far back (bytes) from a date a deadline keyword must appear.
const KEYWORD_WINDOW: usize = 120;

// ---- host ABI ---------------------------------------------------------------

/// Reserve `len` bytes and hand the host a pointer to write the input into.
#[no_mangle]
pub extern "C" fn alloc(len: u32) -> u32 {
    let mut buf: Vec<u8> = Vec::with_capacity(len as usize);
    let ptr = buf.as_mut_ptr() as u32;
    std::mem::forget(buf); // freed when the whole store is torn down after the call
    ptr
}

/// Packs an output JSON string into the `(ptr << 32) | len` return convention.
fn emit(out: String) -> u64 {
    let bytes = out.into_bytes();
    let out_ptr = bytes.as_ptr() as u32;
    let out_len = bytes.len() as u32;
    std::mem::forget(bytes);
    ((out_ptr as u64) << 32) | out_len as u64
}

fn read_input<'a>(ptr: u32, len: u32) -> &'a str {
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    std::str::from_utf8(input).unwrap_or("")
}

/// The enricher entry point.
#[no_mangle]
pub extern "C" fn enrich(ptr: u32, len: u32) -> u64 {
    let envelope: Value = serde_json::from_str(read_input(ptr, len)).unwrap_or(Value::Null);
    let doc = envelope.get("doc").and_then(Value::as_str).unwrap_or("");
    let now = envelope
        .get("params")
        .and_then(|p| p.get("now"))
        .and_then(Value::as_i64);
    emit(json!({ "entities": entities(doc, now) }).to_string())
}

/// Self-describing manifest for `GET /plugins?kind=enricher`.
#[no_mangle]
pub extern "C" fn describe() -> u64 {
    emit(
        json!({
            "version": "0.1.0",
            "kind": "enricher",
            "description": "Index-time entity enricher: `amount` (largest figure carrying an explicit $/USD marker, whole dollars) and `event_date` (earliest UPCOMING date preceded by a deadline keyword, unix seconds at UTC midnight). No match = no entity; an ambiguous European decimal (`$1.234,56`) is dropped rather than reinterpreted.",
            "params_schema": {
                "now": "number — the document's own unix timestamp, supplied by the search host. A wasm guest has no clock, so WITHOUT this no `event_date` is emitted at all (never a guessed one).",
            },
            "output_schema": {
                "entities": "object — {amount?: number, event_date?: number}; a kind the document does not carry is absent, never null and never 0",
            },
        })
        .to_string(),
    )
}

// ---- the rules ---------------------------------------------------------------

/// Both entity kinds for one document's text, as the output envelope's
/// `entities` object. Pure, so the crate's own `cargo test` (which `just
/// plugins-test` runs on the HOST target) covers every rule without a sandbox.
pub fn entities(text: &str, now: Option<i64>) -> Value {
    let lowered = text.to_ascii_lowercase();
    let mut out = serde_json::Map::new();
    if let Some(amount) = max_amount_dollars(&lowered) {
        out.insert("amount".into(), json!(amount));
    }
    // No clock, no deadline: an "upcoming" judgement needs something to be
    // upcoming OF.
    if let Some(now) = now {
        if let Some(ts) = earliest_upcoming_deadline(&lowered, now) {
            out.insert("event_date".into(), json!(ts));
        }
    }
    Value::Object(out)
}

fn money_re() -> Regex {
    Regex::new(
        r"(?:\$|\busd\s?)\s*([0-9][0-9,]{0,17})(\.[0-9]{1,4})?\s*(thousand|million|billion|mm|k|m|b)?\b",
    )
    .expect("money regex")
}

fn date_iso_re() -> Regex {
    Regex::new(
        r"\b(20[0-9]{2})-([01]?[0-9])-([0-3]?[0-9])(?:t[0-9]{2}:[0-9]{2}(?::[0-9]{2})?(?:\.[0-9]+)?(?:z|[+-][0-9]{2}:?[0-9]{2})?)?\b",
    )
    .expect("iso re")
}

fn date_us_re() -> Regex {
    Regex::new(r"\b([01]?[0-9])/([0-3]?[0-9])/(20[0-9]{2})\b").expect("us date re")
}

fn date_name_re() -> Regex {
    Regex::new(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+([0-9]{1,2})(?:st|nd|rd|th)?,?\s+(20[0-9]{2})\b",
    )
    .expect("name date re")
}

fn deadline_keyword_re() -> Regex {
    Regex::new(r"\b(deadline|due|clos|expir|apply|submit|submission|respond|end[_\s-]?date)")
        .expect("keyword re")
}

/// True when the text right after a money match continues the number with
/// another separator+digit — the European `1.234,56` shape. `$1.234,56` read
/// US-style is **$1**, a 1000x lie, so such a candidate is dropped rather than
/// reinterpreted: guessing which convention a document uses is exactly the
/// inference this plugin refuses to make.
fn is_ambiguous_decimal_tail(rest: &str) -> bool {
    let mut chars = rest.chars();
    matches!(chars.next(), Some(',') | Some('.'))
        && chars.next().is_some_and(|c| c.is_ascii_digit())
}

/// Largest dollar amount carrying an explicit currency marker, whole dollars.
pub fn max_amount_dollars(lowered: &str) -> Option<u64> {
    let re = money_re();
    let mut best: Option<u64> = None;
    for cap in re.captures_iter(lowered) {
        let whole = cap.get(0).expect("whole match");
        if is_ambiguous_decimal_tail(&lowered[whole.end()..]) {
            continue;
        }
        let digits: String = cap[1].chars().filter(|c| *c != ',').collect();
        let Ok(int_part) = digits.parse::<f64>() else {
            continue;
        };
        let frac: f64 = cap
            .get(2)
            .and_then(|m| m.as_str().parse::<f64>().ok())
            .unwrap_or(0.0);
        let scale = match cap.get(3).map(|m| m.as_str()) {
            Some("k") | Some("thousand") => 1_000.0,
            Some("m") | Some("mm") | Some("million") => 1_000_000.0,
            Some("b") | Some("billion") => 1_000_000_000.0,
            _ => 1.0,
        };
        let dollars = (int_part + frac) * scale;
        if !dollars.is_finite() || dollars < 1.0 || dollars > MAX_AMOUNT_DOLLARS {
            continue;
        }
        let dollars = dollars as u64;
        best = Some(best.map_or(dollars, |b: u64| b.max(dollars)));
    }
    best
}

/// The lookback a deadline keyword must appear in: up to [`KEYWORD_WINDOW`]
/// bytes before `start`, snapped FORWARD to a char boundary. `start - 120` lands
/// mid-codepoint whenever a multi-byte character straddles it, and slicing a
/// `str` there panics — inside a sandbox that is a trap, and the host then
/// indexes the document with no entities at all.
fn keyword_window(lowered: &str, start: usize) -> &str {
    let mut lo = start.saturating_sub(KEYWORD_WINDOW);
    while lo < start && !lowered.is_char_boundary(lo) {
        lo += 1;
    }
    &lowered[lo..start]
}

/// Days from 1970-01-01 to a proleptic-Gregorian y/m/d (Howard Hinnant's
/// `days_from_civil`). Hand-rolled because a date library that can do this
/// pulls a clock this target does not have; the arithmetic is exact and the
/// tests pin it against known timestamps.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Whether y/m/d is a real calendar date (month 13 and Feb 30 are not).
fn is_valid_date(y: i64, m: i64, d: i64) -> bool {
    if !(1..=12).contains(&m) || d < 1 {
        return false;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let last = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if leap => 29,
        _ => 28,
    };
    d <= last
}

/// Earliest upcoming deadline-like date, unix seconds at UTC midnight. A date
/// counts only when a deadline keyword appears within the preceding
/// [`KEYWORD_WINDOW`] bytes — a bare publication date is not a deadline.
pub fn earliest_upcoming_deadline(lowered: &str, now: i64) -> Option<i64> {
    let mut best: Option<i64> = None;
    let keyword = deadline_keyword_re();
    let mut consider = |start: usize, y: i64, m: i64, d: i64| {
        if !keyword.is_match(keyword_window(lowered, start)) || !is_valid_date(y, m, d) {
            return;
        }
        let ts = days_from_civil(y, m, d) * 86_400;
        if ts < now - TODAY_GRACE_SECS || ts > now + MAX_HORIZON_SECS {
            return;
        }
        best = Some(best.map_or(ts, |b: i64| b.min(ts)));
    };

    for cap in date_iso_re().captures_iter(lowered) {
        let s = cap.get(0).expect("match").start();
        if let (Ok(y), Ok(m), Ok(d)) = (cap[1].parse(), cap[2].parse(), cap[3].parse()) {
            consider(s, y, m, d);
        }
    }
    for cap in date_us_re().captures_iter(lowered) {
        let s = cap.get(0).expect("match").start();
        if let (Ok(m), Ok(d), Ok(y)) = (cap[1].parse(), cap[2].parse(), cap[3].parse()) {
            consider(s, y, m, d);
        }
    }
    for cap in date_name_re().captures_iter(lowered) {
        let s = cap.get(0).expect("match").start();
        let month = match &cap[1] {
            "jan" => 1,
            "feb" => 2,
            "mar" => 3,
            "apr" => 4,
            "may" => 5,
            "jun" => 6,
            "jul" => 7,
            "aug" => 8,
            "sep" => 9,
            "oct" => 10,
            "nov" => 11,
            "dec" => 12,
            _ => continue,
        };
        if let (Ok(d), Ok(y)) = (cap[2].parse(), cap[3].parse()) {
            consider(s, y, month, d);
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    // 2026-01-01T00:00:00Z — a fixed "now", so every date test is deterministic.
    const NOW: i64 = 1_767_225_600;

    /// The hand-rolled calendar has to be exactly right, or every `event_date`
    /// is off by a day (or a century) in a way no other test would notice.
    #[test]
    fn days_from_civil_matches_known_timestamps() {
        assert_eq!(days_from_civil(1970, 1, 1) * 86_400, 0);
        assert_eq!(days_from_civil(2026, 1, 1) * 86_400, NOW);
        assert_eq!(days_from_civil(2026, 3, 1) * 86_400, 1_772_323_200);
        assert_eq!(days_from_civil(2024, 2, 29) * 86_400, 1_709_164_800);
        assert!(is_valid_date(2024, 2, 29));
        assert!(!is_valid_date(2026, 2, 29), "2026 is not a leap year");
        assert!(!is_valid_date(2026, 13, 1));
    }

    /// The doctrine this plugin exists to reproduce: a value is never guessed.
    /// A bare number is not money, and an ambiguously formatted one is dropped
    /// rather than read 1000x wrong.
    #[test]
    fn an_unmarked_or_ambiguous_amount_yields_no_entity() {
        assert_eq!(max_amount_dollars("population 1,234,567 in 2026"), None);
        assert_eq!(max_amount_dollars("5 million people"), None);
        assert_eq!(max_amount_dollars("id $12345678901234567"), None);
        // European grouping: $1.234,56 is NOT $1.
        assert_eq!(max_amount_dollars("cena $1.234,56 celkem"), None);
        // ...while the marked, unambiguous ones are read.
        assert_eq!(max_amount_dollars("award of $1,234,567 total"), Some(1_234_567));
        assert_eq!(max_amount_dollars("up to $1.5 million available"), Some(1_500_000));
        assert_eq!(max_amount_dollars("min $5,000 and max $250,000"), Some(250_000));
        assert_eq!(max_amount_dollars("usd 5,000 per year"), Some(5_000));
    }

    /// A date is only a DEADLINE when a deadline word precedes it, and only
    /// when it is still upcoming. A publication date is neither.
    #[test]
    fn a_bare_or_past_date_is_not_a_deadline() {
        assert_eq!(
            earliest_upcoming_deadline("published 2026-03-01 by the agency", NOW),
            None
        );
        assert_eq!(
            earliest_upcoming_deadline("applications close 2019-03-01", NOW),
            None,
            "a past deadline is not upcoming"
        );
        assert_eq!(
            earliest_upcoming_deadline("applications close 2026-03-01", NOW),
            Some(1_772_323_200)
        );
        // Earliest upcoming wins, across formats.
        assert_eq!(
            earliest_upcoming_deadline(
                "deadline march 15, 2026 and a second due 3/1/2026 window",
                NOW
            ),
            Some(1_772_323_200)
        );
    }

    /// A multi-byte character straddling the keyword window used to be a panic
    /// — inside the sandbox, a trap, which costs the whole document's entities.
    #[test]
    fn a_non_ascii_body_extracts_instead_of_trapping() {
        let text = format!("{} uzávěrka due 2026-03-01", "č".repeat(80));
        let out = entities(&text.to_ascii_lowercase(), Some(NOW));
        assert_eq!(out["event_date"], json!(1_772_323_200));
    }

    /// The output contract: absent, never null and never zero. A zero `amount`
    /// would match every `amount_gte` filter the index can express.
    #[test]
    fn a_document_with_nothing_to_extract_emits_no_keys() {
        let out = entities("a page about nothing in particular", Some(NOW));
        assert_eq!(out, json!({}));
        // And with no clock, the date rule sits out rather than guessing one.
        let out = entities("applications close 2026-03-01", None);
        assert_eq!(out, json!({}), "no params.now means no event_date");
        let out = entities("award of $250,000; applications close 2026-03-01", Some(NOW));
        assert_eq!(out, json!({"amount": 250_000, "event_date": 1_772_323_200}));
    }
}
