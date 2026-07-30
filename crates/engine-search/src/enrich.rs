//! Index-time entity enrichment (M14 "entity-typed index"): conservative,
//! regex-only extraction of money amounts and deadline-like dates from a
//! document's title+body, feeding the `amount` / `event_date` fast fields.
//!
//! Doctrine: **no match = no field**. Every rule requires an explicit marker
//! (a currency symbol for money, a deadline-ish keyword near the date) and
//! validated components — a value is never guessed, defaulted, or inferred.
//! Org/geo extraction is deliberately out of scope (regex cannot do it
//! honestly; NER is not available here).

use std::sync::LazyLock;

use chrono::NaiveDate;
use regex::Regex;

/// Amounts above this (one trillion dollars) are treated as extraction noise
/// (concatenated digits, ids) and dropped rather than indexed.
const MAX_AMOUNT_DOLLARS: u64 = 1_000_000_000_000;

/// Deadlines are only considered "upcoming" within this horizon (10 years) —
/// anything further out is far more likely a parse artifact than a real date.
const MAX_HORIZON_SECS: i64 = 10 * 365 * 24 * 3600;

/// A date whose UTC midnight is up to this long before `now` still counts as
/// upcoming — a deadline "today" must not vanish partway through the day.
const TODAY_GRACE_SECS: i64 = 86_400;

/// How far back (bytes) from a date match a deadline keyword must appear.
const KEYWORD_WINDOW: usize = 120;

/// Money: requires an explicit `$` or `usd` marker, digits (commas allowed),
/// optional decimals, optional scale suffix (k/thousand/m/mm/million/b/billion).
/// Runs over ASCII-lowercased text.
static MONEY_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?:\$|\busd\s?)\s*([0-9][0-9,]{0,17})(\.[0-9]{1,4})?\s*(thousand|million|billion|mm|k|m|b)?\b",
    )
    .expect("money regex")
});

/// ISO `YYYY-MM-DD` (the shape stored JSON fields overwhelmingly use).
static DATE_ISO_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(20[0-9]{2})-([01]?[0-9])-([0-3]?[0-9])\b").expect("iso re"));

/// US `M/D/YYYY`.
static DATE_US_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b([01]?[0-9])/([0-3]?[0-9])/(20[0-9]{2})\b").expect("us date re")
});

/// `month D[, ] YYYY` with full or abbreviated month names (lowercased text).
static DATE_NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+([0-9]{1,2})(?:st|nd|rd|th)?,?\s+(20[0-9]{2})\b",
    )
    .expect("name date re")
});

/// Deadline-ish keyword that must precede a date for it to count. Leading `\b`
/// keeps "residue"/"overdue" from matching "due"; the open tail lets `clos`
/// cover close/closes/closing/close_date (underscore is a word char, so a
/// trailing `\b` would reject the common `close_date` JSON key).
static DEADLINE_KEYWORD_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(deadline|due|clos|expir|apply|submit|submission|respond|end[_\s-]?date)")
        .expect("keyword re")
});

/// Largest dollar amount mentioned with an explicit currency marker, in **whole
/// dollars** (fractional cents truncated after scale multipliers apply, so
/// `$1.5 million` → 1_500_000). `None` when no marked amount is present or
/// every candidate is implausible (> $1T).
pub fn max_amount_dollars(text: &str) -> Option<u64> {
    let lowered = text.to_ascii_lowercase();
    let mut best: Option<u64> = None;
    for cap in MONEY_RE.captures_iter(&lowered) {
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
        if !dollars.is_finite() || dollars < 1.0 || dollars > MAX_AMOUNT_DOLLARS as f64 {
            continue; // implausible — drop, never index a guess
        }
        let dollars = dollars as u64;
        best = Some(best.map_or(dollars, |b| b.max(dollars)));
    }
    best
}

/// Earliest **upcoming** deadline-like date as unix seconds (UTC midnight).
/// A date only qualifies when a deadline keyword appears within the preceding
/// [`KEYWORD_WINDOW`] bytes — a bare date (publication date, historical
/// reference) is NOT a deadline and yields nothing. "Upcoming" = within
/// [`now - TODAY_GRACE_SECS`, `now + MAX_HORIZON_SECS`].
pub fn earliest_upcoming_deadline(text: &str, now: i64) -> Option<i64> {
    let lowered = text.to_ascii_lowercase();
    let mut best: Option<i64> = None;
    let mut consider = |start: usize, y: i32, m: u32, d: u32| {
        let window = &lowered[start.saturating_sub(KEYWORD_WINDOW)..start];
        if !DEADLINE_KEYWORD_RE.is_match(window) {
            return;
        }
        // Invalid calendar components (month 13, Feb 30) parse to None — dropped.
        let Some(date) = NaiveDate::from_ymd_opt(y, m, d) else {
            return;
        };
        let ts = date.and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp();
        if ts < now - TODAY_GRACE_SECS || ts > now + MAX_HORIZON_SECS {
            return;
        }
        best = Some(best.map_or(ts, |b| b.min(ts)));
    };

    for cap in DATE_ISO_RE.captures_iter(&lowered) {
        let s = cap.get(0).unwrap().start();
        if let (Ok(y), Ok(m), Ok(d)) = (cap[1].parse(), cap[2].parse(), cap[3].parse()) {
            consider(s, y, m, d);
        }
    }
    for cap in DATE_US_RE.captures_iter(&lowered) {
        let s = cap.get(0).unwrap().start();
        if let (Ok(m), Ok(d), Ok(y)) = (cap[1].parse(), cap[2].parse(), cap[3].parse()) {
            consider(s, y, m, d);
        }
    }
    for cap in DATE_NAME_RE.captures_iter(&lowered) {
        let s = cap.get(0).unwrap().start();
        let month = match &cap[1] {
            "jan" => 1, "feb" => 2, "mar" => 3, "apr" => 4, "may" => 5, "jun" => 6,
            "jul" => 7, "aug" => 8, "sep" => 9, "oct" => 10, "nov" => 11, "dec" => 12,
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

    // 2026-01-01T00:00:00Z — a fixed "now" so tests are deterministic.
    const NOW: i64 = 1_767_225_600;

    #[test]
    fn money_plain_commas_and_decimals() {
        assert_eq!(max_amount_dollars("award of $1,234,567 total"), Some(1_234_567));
        assert_eq!(max_amount_dollars("fee: $99.99"), Some(99));
        assert_eq!(max_amount_dollars("USD 5,000 per year"), Some(5_000));
    }

    #[test]
    fn money_scale_suffixes() {
        assert_eq!(max_amount_dollars("up to $1.5 million available"), Some(1_500_000));
        assert_eq!(max_amount_dollars("budget $3M"), Some(3_000_000));
        assert_eq!(max_amount_dollars("$2b program"), Some(2_000_000_000));
        assert_eq!(max_amount_dollars("$40k stipend"), Some(40_000));
    }

    #[test]
    fn money_takes_max_of_multiple() {
        assert_eq!(
            max_amount_dollars("min $5,000 and max $250,000 per award"),
            Some(250_000)
        );
    }

    #[test]
    fn money_requires_currency_marker() {
        // Bare numbers — even huge ones — are not money.
        assert_eq!(max_amount_dollars("population 1,234,567 in 2026"), None);
        assert_eq!(max_amount_dollars("5 million people"), None);
    }

    #[test]
    fn money_rejects_implausible() {
        // > $1T is extraction noise, not indexed.
        assert_eq!(max_amount_dollars("id $12345678901234567"), None);
        assert_eq!(max_amount_dollars(""), None);
    }

    #[test]
    fn money_suffix_needs_boundary() {
        // "m" followed by letters is a word, not a multiplier.
        assert_eq!(max_amount_dollars("$5 miles of road"), Some(5));
        assert_eq!(max_amount_dollars("$5 max"), Some(5));
    }

    #[test]
    fn deadline_iso_with_keyword() {
        let t = r#"{"title":"Grant","close_date":"2026-09-01","posted":"2020-01-01"}"#;
        // 2026-09-01T00:00:00Z
        assert_eq!(earliest_upcoming_deadline(t, NOW), Some(1_788_220_800));
    }

    #[test]
    fn deadline_requires_keyword_near_date() {
        // A bare date with no deadline-ish keyword nearby is NOT a deadline.
        assert_eq!(earliest_upcoming_deadline("published 2026-09-01 report", NOW), None);
    }

    #[test]
    fn deadline_past_dates_excluded() {
        assert_eq!(
            earliest_upcoming_deadline("deadline was 2020-03-01", NOW),
            None
        );
    }

    #[test]
    fn deadline_earliest_upcoming_wins() {
        let t = "applications due September 15, 2026; final deadline 12/1/2026";
        // Sep 15 2026 midnight UTC.
        assert_eq!(earliest_upcoming_deadline(t, NOW), Some(1_789_430_400));
    }

    #[test]
    fn deadline_invalid_calendar_dropped() {
        assert_eq!(earliest_upcoming_deadline("due 2026-02-30", NOW), None);
        assert_eq!(earliest_upcoming_deadline("due 2026-13-01", NOW), None);
    }

    #[test]
    fn deadline_keyword_boundary_honest() {
        // "residue"/"overdue" must not activate the "due" keyword.
        assert_eq!(
            earliest_upcoming_deadline("chemical residue 2026-09-01 sample", NOW),
            None
        );
    }

    #[test]
    fn deadline_far_future_excluded() {
        assert_eq!(earliest_upcoming_deadline("due 2039-01-01", NOW), None);
    }
}
