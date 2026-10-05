//! `GET /grants/closing-soon` must answer on the canonical deadline instant.
//!
//! Every case here is written as a *disagreement check*: it computes what the
//! urgency surface says and what `grants_common`'s canonical door
//! ([`grants_common::deadline_end_utc`]) says, and asserts they agree. That
//! framing is the point — the defect was never a wrong number in isolation, it
//! was two surfaces of one product answering "is this grant still claimable?"
//! with two different rules, ~12 hours out of step every UTC day, in the
//! direction that hides claimable money.
//!
//! Before this file, the route's answer was spelled with the three expressions
//! named `route_today_*` below: a lexicographic compare on the printed
//! `$.close_date` and a `NaiveDate` subtraction. They are kept here, verbatim,
//! as the anti-pattern each case forbids — so a future change that quietly
//! reverts to a date compare fails on the same assertion that caught it first.

use chrono::{DateTime, NaiveDate, Utc};

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

// ---------------------------------------------------------------------------
// The anti-pattern, preserved. These are the route's OLD expressions, copied
// verbatim from crates/server/src/routes/query.rs before this change. Nothing
// calls them in production any more; they exist so every case can state the
// disagreement it forbids rather than merely asserting a new number.
// ---------------------------------------------------------------------------

/// The old window: `$.close_date >= today` AND `$.close_date <= today + days`,
/// both lexicographic string compares on the printed date.
fn route_today_in_window(close_date: Option<&str>, now: DateTime<Utc>, days: i64) -> bool {
    let today = now.date_naive().to_string();
    let until = (now.date_naive() + chrono::Duration::days(days)).to_string();
    close_date.is_some_and(|d| d >= today.as_str() && d <= until.as_str())
}

/// The old `days_left`: a date subtraction against `Utc::now().date_naive()`,
/// `None` for anything that is not exactly `YYYY-MM-DD`.
fn route_today_days_left(close_date: Option<&str>, now: DateTime<Utc>) -> Option<i64> {
    close_date
        .and_then(|c| NaiveDate::parse_from_str(c, "%Y-%m-%d").ok())
        .map(|close| (close - now.date_naive()).num_days())
}

/// The canonical membership question, as the corpus sweep asks it.
fn canonically_claimable(
    close_date: Option<&str>,
    close_at: Option<&str>,
    now: DateTime<Utc>,
) -> bool {
    grants_common::deadline_end_utc(close_date, close_at).is_some_and(|end| now <= end)
}

/// Whether the window the route asks SQL for admits this printed date.
fn window_admits(close_date: Option<&str>, now: DateTime<Utc>, days: i64) -> bool {
    let (lower, upper) = grants_common::closing_window_bounds(now, days);
    close_date.is_some_and(|d| d >= lower.as_str() && d <= upper.as_str())
}

// ---------------------------------------------------------------------------
// Case 1 — the headline. The last-day grant, in the anywhere-on-Earth tail.
// ---------------------------------------------------------------------------

#[test]
fn a_grant_open_in_unified_is_not_missing_from_the_urgency_view() {
    let now = at("2026-10-05T03:00:00Z");
    let (close_date, close_at) = (Some("2026-10-04"), None);

    // The corpus says it is open: `2026-10-04` lapses at `2026-10-05T12:00Z`.
    assert!(
        canonically_claimable(close_date, close_at, now),
        "precondition: the sweep considers this row open at 03:00Z"
    );
    // The anti-pattern: the date compare said it did not exist.
    assert!(
        !route_today_in_window(close_date, now, 14),
        "this case is only interesting because the OLD bound excluded the row"
    );
    // The fix: the canonical bound admits it.
    assert!(
        window_admits(close_date, now, 14),
        "the canonical window must agree with the corpus"
    );

    let c = grants_common::deadline_countdown(close_date, close_at, now).unwrap();
    assert_eq!(c.closes_at, at("2026-10-05T12:00:00Z"));
    assert_eq!(c.hours_left, 9);
    assert_eq!(c.days_left, 0, "printed yesterday, claimable today");
    assert!(!c.lapsed);
}

// ---------------------------------------------------------------------------
// Case 2 — GUARD (declared green-before). Past the anywhere-on-Earth end, the
// canonical window drops the row exactly as the old bound did, and count and
// page see the one predicate.
// ---------------------------------------------------------------------------

#[test]
fn past_the_anywhere_on_earth_end_both_bounds_agree_the_row_is_gone() {
    let now = at("2026-10-05T13:00:00Z");
    let (close_date, close_at) = (Some("2026-10-04"), None);

    assert!(!canonically_claimable(close_date, close_at, now));
    assert!(!route_today_in_window(close_date, now, 14));
    assert!(
        !window_admits(close_date, now, 14),
        "after midday the lower bound is today, so the row leaves count AND page together"
    );
    // One predicate: whatever the bounds are, they are a single pair, so a
    // COUNT and a page built from them cannot disagree.
    let (lower, _) = grants_common::closing_window_bounds(now, 14);
    assert_eq!(lower, "2026-10-05");
    let (morning_lower, _) = grants_common::closing_window_bounds(at("2026-10-05T11:59:59Z"), 14);
    assert_eq!(
        morning_lower, "2026-10-04",
        "the twelve-hour boundary, from below"
    );
}

// ---------------------------------------------------------------------------
// Case 3 — a zoned deadline is shown to the hour, not rounded to a day.
// ---------------------------------------------------------------------------

#[test]
fn a_zoned_deadline_is_not_rounded_to_a_printed_day() {
    let now = at("2026-10-05T23:00:00Z");
    let (close_date, close_at) = (Some("2026-10-06"), Some("2026-10-06T17:00:00Z"));

    // The anti-pattern: "1 day left" for a deadline eighteen hours away, and no
    // instant anywhere in the response.
    assert_eq!(route_today_days_left(close_date, now), Some(1));

    let c = grants_common::deadline_countdown(close_date, close_at, now).unwrap();
    assert_eq!(
        c.closes_at,
        at("2026-10-06T17:00:00Z"),
        "the zoned instant the source published, NOT 2026-10-07T12:00:00Z"
    );
    assert_eq!(c.hours_left, 18);
    assert_eq!(c.days_left, 0, "eighteen hours is not a day");
    assert!(!c.lapsed);
}

// ---------------------------------------------------------------------------
// Case 4 — a row carrying a usable instant and an unusable printed date is
// judged by the instant.
// ---------------------------------------------------------------------------

#[test]
fn a_row_is_judged_by_its_instant_even_when_the_printed_date_is_not_iso() {
    let now = at("2026-10-05T23:00:00Z");
    let (close_date, close_at) = (Some("06/10/2026"), Some("2026-10-06T17:00:00Z"));

    // The anti-pattern: `days_left: null`, because the only thing the row was
    // asked for was a `%Y-%m-%d` parse.
    assert_eq!(route_today_days_left(close_date, now), None);

    let c = grants_common::deadline_countdown(close_date, close_at, now).unwrap();
    assert_eq!(c.closes_at, at("2026-10-06T17:00:00Z"));
    assert_eq!(c.hours_left, 18);
    assert_eq!(c.days_left, 0);

    // And the canonical door is tolerant where the route's parse was not: a
    // US-format date alone still yields a countdown instead of a null.
    let us = grants_common::deadline_countdown(Some("10/06/2026"), None, now).unwrap();
    assert_eq!(us.closes_at, at("2026-10-07T12:00:00Z"));
    assert_eq!(
        route_today_days_left(Some("10/06/2026"), now),
        None,
        "the old expression could not read this row at all"
    );

    // KNOWN GAP, asserted so it cannot be mistaken for a fix: the SQL window is
    // still indexed on the printed `$.close_date`, so a row whose printed date
    // is not canonical is not *selected* by it. Row shaping is instant-true;
    // row selection needs a disjunctive store filter this change does not add.
    assert!(
        !window_admits(Some("06/10/2026"), now, 14),
        "a non-canonical printed date is invisible to a text bound — documented gap"
    );
}

// ---------------------------------------------------------------------------
// Case 5 — a provably-lapsed row still stamped `open`: the sweep runs once per
// UTC cycle, so this state is normal and was unreported.
// ---------------------------------------------------------------------------

#[test]
fn a_lapsed_row_is_marked_not_dropped() {
    let now = at("2026-10-05T03:00:00Z");
    let (close_date, close_at) = (Some("2026-10-04"), Some("2026-10-04T23:00:00Z"));

    // Inside the window the canonical bound selects...
    assert!(window_admits(close_date, now, 14));
    // ...but its real deadline passed four hours ago.
    assert!(!canonically_claimable(close_date, close_at, now));

    let c = grants_common::deadline_countdown(close_date, close_at, now).unwrap();
    assert_eq!(c.closes_at, at("2026-10-04T23:00:00Z"));
    assert!(c.lapsed, "the one fact the operator could not get before");
    assert_eq!(c.hours_left, 0, "a spent countdown is 0, never negative");
    assert_eq!(c.days_left, 0);
}

// ---------------------------------------------------------------------------
// Case 6 — GUARD (declared green-before). A grant well inside its deadline is
// unaffected by the change, at either side of the twelve-hour boundary.
// ---------------------------------------------------------------------------

#[test]
fn a_grant_well_inside_its_deadline_is_unchanged() {
    for now in [at("2026-10-05T03:00:00Z"), at("2026-10-05T13:00:00Z")] {
        let close_date = Some("2026-11-20");
        assert!(route_today_in_window(close_date, now, 60));
        assert!(window_admits(close_date, now, 60));
        let c = grants_common::deadline_countdown(close_date, None, now).unwrap();
        assert_eq!(
            Some(c.days_left),
            route_today_days_left(close_date, now),
            "a far-off printed deadline keeps the days_left it always had"
        );
        assert_eq!(c.days_left, 46);
        assert!(!c.lapsed);
    }
}

// ---------------------------------------------------------------------------
// Case 7 — one door. The countdown is computed in exactly one place, so the
// per-source digest and the cross-source route cannot drift again.
// ---------------------------------------------------------------------------

#[test]
fn the_countdown_has_one_definition_for_date_only_rows() {
    let now = at("2026-08-13T09:00:00Z");
    // grants.gov publishes `MM/DD/YYYY` with no timezone, so every row it feeds
    // the digest takes the date-only arm. The digest's own number is the
    // calendar distance floored at 0; `deadline_countdown` must reproduce it
    // exactly, which is what makes it safe for the digest to call this instead.
    for (raw, expect) in [
        ("08/16/2026", 3),
        ("08/12/2026", 0),
        ("2026-08-13", 0),
        ("08/27/2026", 14),
    ] {
        let c = grants_common::deadline_countdown(Some(raw), None, now).unwrap();
        let calendar = (grants_common::parse_date(raw).unwrap() - now.date_naive())
            .num_days()
            .max(0);
        assert_eq!(c.days_left, expect, "{raw}");
        assert_eq!(
            c.days_left, calendar,
            "{raw}: the digest's definition, preserved"
        );
    }
    // An unknown deadline is never a lapsed one — same rule as the door itself.
    assert!(grants_common::deadline_countdown(Some("Deadline—see website"), None, now).is_none());
    assert!(grants_common::deadline_countdown(None, None, now).is_none());
}
