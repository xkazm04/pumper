//! **One collapse verdict**, for every app that scrapes a bounded thing.
//!
//! The defining failure mode of a scraper is not an error: it is a *success*
//! that carried almost nothing. Fetch returns 200, the parse returns `Ok`, two
//! rows of an expected three hundred come out, and the job is green.
//!
//! Eight apps in this repo found that out independently and each built a private
//! floor for it — a roster ratio, a minimum row count, a parsed/seen share — in
//! four different vocabularies, inside four crates nobody else can reach. Worse,
//! only *some* of them turned the floor into an outcome: five apps built the
//! coverage number and exactly one consulted it before writing. A run that
//! collapsed to 1 row of an expected 5 was a GREEN job carrying a string in
//! `warnings[]` that nothing reads.
//!
//! This module is the judgment, once, where every app layer can reach it. An app
//! declares what it *expected* ([`Expectation`]) and how harshly a shortfall
//! should be treated ([`Disposition`]); [`judge`] returns one [`Verdict`] with
//! three states:
//!
//! | state | meaning | what the app does |
//! | --- | --- | --- |
//! | [`Completeness::Ok`] | the run got what it asked for | write normally, tombstone allowed |
//! | [`Completeness::Suspect`] | materially short, but plausibly real | write, but **never tombstone** — a full-snapshot write downgrades to an upsert and says why |
//! | [`Completeness::Collapsed`] | so short it cannot be the source | `Err` — the job is RED, nothing is written |
//!
//! `Suspect` is the state the registry technique `shape-change-detection` calls
//! for and that this repo had nowhere to put: "let *suspect* be a real state —
//! reconciliation-blocking, human-visible — distinct from both success and hard
//! failure", and "surface a run outcome of `collapsed`, not `succeeded(0)`".
//!
//! # Why `core` and not a fifth `*-common`
//!
//! The call sites span two unrelated app families (the US agentic trades apps
//! and the Czech open-data apps) plus the census apps. Apps may depend only on
//! `core` — never on another app crate — so `core` is the only place both
//! families already reach. The interface is three constructors and one function;
//! it is priced well below what it serves.
//!
//! # The carve-outs are data, not exceptions
//!
//! A floor that judges the runs it exists to allow is a floor that gets deleted.
//! Both escape hatches are first-class:
//!
//! * [`Expectation::unbounded`] — this run did not ask for the whole thing
//!   (`maxRecords`, a trimmed mirror behind a `url` override), so there is no
//!   shortfall to measure.
//! * [`Disposition::allow_shrink`] — an operator who knows the roster really
//!   shrank forces [`Completeness::Ok`].

use std::collections::HashSet;

use serde_json::{json, Value};

use crate::{Error, Result};

/// How complete one run was, as an outcome rather than a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completeness {
    /// The run covered what it asked for. Destructive writes are allowed.
    Ok,
    /// Materially short of the expectation, but within the range a real source
    /// can produce. Written, never tombstoned, always visible.
    Suspect,
    /// So far short that the only plausible explanations are a truncated
    /// download, a partial publication, or a schema change. Not a result.
    Collapsed,
}

impl Completeness {
    /// The wire word an operator reads in a result's `completeness.verdict`.
    pub fn as_str(self) -> &'static str {
        match self {
            Completeness::Ok => "ok",
            Completeness::Suspect => "suspect",
            Completeness::Collapsed => "collapsed",
        }
    }

    /// Whether this verdict has earned the right to mark absent keys removed.
    /// Only [`Completeness::Ok`] has: a short run's "absent" keys are the ones
    /// it failed to read, not the ones the source retired.
    pub fn may_tombstone(self) -> bool {
        matches!(self, Completeness::Ok)
    }
}

/// What a run asked the source for, and what it got.
///
/// Three shapes cover every floor in this repo. They differ only in how
/// `expected` and `covered` are derived; the judging is identical, which is the
/// whole point of the module.
#[derive(Debug, Clone)]
pub struct Expectation {
    unit: &'static str,
    expected: usize,
    covered: usize,
    missing: Option<Vec<String>>,
    bounded: bool,
}

impl Expectation {
    /// A **named roster**: `roster` is the full expected list, `present` what
    /// this run actually produced. The missing names are kept, so the verdict
    /// can say *which* members vanished rather than only how many.
    pub fn roster(unit: &'static str, roster: &[&str], present: &HashSet<String>) -> Self {
        let missing: Vec<String> = roster
            .iter()
            .filter(|m| !present.contains(**m))
            .map(|m| (*m).to_string())
            .collect();
        Self {
            unit,
            expected: roster.len(),
            covered: roster.len().saturating_sub(missing.len()),
            missing: Some(missing),
            bounded: true,
        }
    }

    /// A roster whose members are **not a fixed list of names** — "trades that
    /// came back with at least one priced job". `missing` is honest-Null rather
    /// than `[]`, which would read as "nothing missing".
    pub fn counts(unit: &'static str, expected: usize, covered: usize) -> Self {
        Self {
            unit,
            expected,
            covered: covered.min(expected),
            missing: None,
            bounded: true,
        }
    }

    /// A **minimum row count**: `floor` rows is the least this source can
    /// plausibly publish, `observed` is what arrived. Expressed as a ratio
    /// against the floor so one judge serves it, which is why
    /// [`Disposition::fatal`] (floor 1.0) is the disposition that matches: at or
    /// above the floor the ratio is exactly 1.0.
    pub fn min_rows(unit: &'static str, floor: usize, observed: usize) -> Self {
        Self::counts(unit, floor, observed)
    }

    /// A **parsed / seen share**: of the `seen` blocks the document published,
    /// `parsed` of them were read. A document with nothing in it is complete —
    /// nothing was published, so nothing was lost; that case belongs to the
    /// app's own empty guard.
    pub fn parsed_ratio(unit: &'static str, seen: usize, parsed: usize) -> Self {
        Self::counts(unit, seen, parsed)
    }

    /// **This run did not ask for the whole thing.** A `maxRecords` run, or one
    /// pointed at a trimmed mirror by a `url` override, is deliberately short:
    /// there is no shortfall to measure and the verdict is always
    /// [`Completeness::Ok`].
    pub fn unbounded(unit: &'static str) -> Self {
        Self {
            unit,
            expected: 0,
            covered: 0,
            missing: None,
            bounded: false,
        }
    }

    pub fn unit(&self) -> &'static str {
        self.unit
    }
    pub fn expected(&self) -> usize {
        self.expected
    }
    pub fn covered(&self) -> usize {
        self.covered
    }
    /// The roster members this run did not return, or `&[]` for a count-only
    /// expectation (distinguish with [`Expectation::missing_named`]).
    pub fn missing(&self) -> &[String] {
        self.missing.as_deref().unwrap_or(&[])
    }
    /// Whether this expectation knows the *names* of what is missing.
    pub fn missing_named(&self) -> bool {
        self.missing.is_some()
    }
    /// Whether a shortfall is measurable at all — `false` for
    /// [`Expectation::unbounded`].
    pub fn is_bounded(&self) -> bool {
        self.bounded
    }

    /// Covered / expected. An empty expectation is complete (1.0): nothing was
    /// asked for, so nothing is missing.
    pub fn ratio(&self) -> f64 {
        if self.expected == 0 {
            return 1.0;
        }
        self.covered as f64 / self.expected as f64
    }
}

/// How harshly THIS app treats a shortfall.
///
/// Two thresholds, not one, because "short" and "collapsed" are different
/// claims about the source: a 90%-of-roster run is a source that lost a member;
/// a 20%-of-roster run is not that source at all.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Disposition {
    /// Below this ratio the run is [`Completeness::Suspect`].
    pub suspect_below: f64,
    /// Below this ratio it is [`Completeness::Collapsed`] and the job fails.
    /// `0.0` means "this app never fails on a shortfall, it only suppresses
    /// removals" — nothing is below zero.
    pub collapse_below: f64,
    /// Operator override: the roster really did shrink, judge nothing.
    pub allow_shrink: bool,
}

impl Disposition {
    /// Both thresholds, explicitly. `collapse_below` must not exceed
    /// `suspect_below` — a run cannot be collapsed without also being short —
    /// and is clamped if it does.
    pub fn new(suspect_below: f64, collapse_below: f64) -> Self {
        Self {
            suspect_below,
            collapse_below: collapse_below.min(suspect_below),
            allow_shrink: false,
        }
    }

    /// **Report and suppress, never fail.** A shortfall below `suspect_below`
    /// downgrades a full-snapshot write to an upsert and is reported; no
    /// shortfall, however severe, turns the job red. This is what the apps that
    /// own a destructive write do, because refusing to write at all is a worse
    /// answer than refusing to tombstone.
    pub fn suppress_only(suspect_below: f64) -> Self {
        Self::new(suspect_below, 0.0)
    }

    /// **Any shortfall is fatal.** One threshold: at or above `floor` the run is
    /// `Ok`, below it the job is red. The disposition for a source whose floor
    /// is already set far below its real size, so that reaching it at all means
    /// the document is broken.
    pub fn fatal(floor: f64) -> Self {
        Self::new(floor, floor)
    }

    /// Set the operator's shrink override. Deliberately NOT spelled `force`:
    /// `force` is how an operator ordinarily re-runs a vintage-gated app, so
    /// hanging the hatch on it would disable the floor on exactly the runs it
    /// exists for.
    pub fn allow_shrink(mut self, yes: bool) -> Self {
        self.allow_shrink = yes;
        self
    }
}

/// The judged outcome of one run: the state, the numbers behind it, and the one
/// sentence an operator reads.
#[derive(Debug, Clone)]
pub struct Verdict {
    state: Completeness,
    unit: &'static str,
    expected: usize,
    covered: usize,
    missing: Option<Vec<String>>,
    suspect_below: f64,
    collapse_below: f64,
    bounded: bool,
}

impl Verdict {
    pub fn state(&self) -> Completeness {
        self.state
    }
    pub fn is_ok(&self) -> bool {
        self.state == Completeness::Ok
    }
    pub fn is_suspect(&self) -> bool {
        self.state == Completeness::Suspect
    }
    pub fn is_collapsed(&self) -> bool {
        self.state == Completeness::Collapsed
    }
    /// Whether this run earned the right to run removal detection. See
    /// [`Completeness::may_tombstone`].
    pub fn may_tombstone(&self) -> bool {
        self.state.may_tombstone()
    }
    pub fn unit(&self) -> &'static str {
        self.unit
    }
    pub fn expected(&self) -> usize {
        self.expected
    }
    pub fn covered(&self) -> usize {
        self.covered
    }
    pub fn missing(&self) -> &[String] {
        self.missing.as_deref().unwrap_or(&[])
    }
    pub fn ratio(&self) -> f64 {
        if self.expected == 0 {
            return 1.0;
        }
        self.covered as f64 / self.expected as f64
    }

    /// The one-line summary of a non-`Ok` verdict, or `None` when the run was
    /// complete. The state word is in the text on purpose: a line scraped out of
    /// a log should still say which of the three outcomes it reports.
    pub fn message(&self) -> Option<String> {
        if self.is_ok() {
            return None;
        }
        Some(format!(
            "completeness {}: {} of {} {} ({:.0}% < {:.0}% floor)",
            self.state.as_str(),
            self.covered,
            self.expected,
            self.unit,
            self.ratio() * 100.0,
            self.suspect_below * 100.0,
        ))
    }

    /// **The gate.** `Err(Error::App)` when the run collapsed, so an app turns
    /// the verdict into a red job with one `?`. `Ok(())` for `Ok` and `Suspect`
    /// alike — `Suspect` is a state the job survives.
    pub fn guard(&self) -> Result<()> {
        if self.is_collapsed() {
            return Err(Error::App(self.message().unwrap_or_else(|| {
                format!("completeness collapsed: {} short", self.unit)
            })));
        }
        Ok(())
    }

    /// The shared `completeness` block every judged app reports. One shape, so
    /// an operator reads eight apps the same way.
    pub fn to_json(&self) -> Value {
        json!({
            "verdict": self.state.as_str(),
            "unit": self.unit,
            "covered": self.covered,
            "expected": self.expected,
            // 3 dp: enough to read, short of float noise in a stored result.
            "ratio": (self.ratio() * 1000.0).round() / 1000.0,
            "suspect_below": self.suspect_below,
            "collapse_below": self.collapse_below,
            "bounded": self.bounded,
            "missing": match &self.missing {
                Some(m) => json!(m),
                None => Value::Null,
            },
            "message": match self.message() {
                Some(m) => json!(m),
                None => Value::Null,
            },
        })
    }
}

/// **The judgment.** One expectation plus one disposition is one verdict.
///
/// An unbounded expectation and an operator shrink override both short-circuit
/// to [`Completeness::Ok`] *before* any ratio is compared, so a deliberately
/// truncated run can never be reported as a collapsed one.
pub fn judge(expectation: &Expectation, disposition: Disposition) -> Verdict {
    let ratio = expectation.ratio();
    let state = if disposition.allow_shrink || !expectation.bounded {
        Completeness::Ok
    } else if ratio < disposition.collapse_below {
        Completeness::Collapsed
    } else if ratio < disposition.suspect_below {
        Completeness::Suspect
    } else {
        Completeness::Ok
    };
    Verdict {
        state,
        unit: expectation.unit,
        expected: expectation.expected,
        covered: expectation.covered,
        missing: expectation.missing.clone(),
        suspect_below: disposition.suspect_below,
        collapse_below: disposition.collapse_below,
        bounded: expectation.bounded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn present(codes: &[&str]) -> HashSet<String> {
        codes.iter().map(|c| c.to_string()).collect()
    }

    const ROSTER: [&str; 5] = ["AL", "AK", "AZ", "AR", "CA"];
    /// The agentic-trades family's disposition: short at 90%, collapsed at 50%.
    fn trades() -> Disposition {
        Disposition::new(0.9, 0.5)
    }

    #[test]
    fn a_one_of_five_run_is_not_a_green_job() {
        let exp = Expectation::roster("priced trades", &ROSTER, &present(&["AL"]));
        let v = judge(&exp, trades());
        assert_eq!(v.state(), Completeness::Collapsed);
        assert!(v.guard().is_err(), "a collapsed run must fail its job");
        let msg = v.message().expect("collapsed carries a message");
        assert!(msg.contains("1 of 5"), "{msg}");
        assert!(msg.contains("collapsed"), "{msg}");
        assert!(!v.may_tombstone());
    }

    #[test]
    fn a_four_of_five_run_is_suspect_not_collapsed() {
        let exp = Expectation::roster(
            "priced trades",
            &ROSTER,
            &present(&["AL", "AK", "AZ", "AR"]),
        );
        let v = judge(&exp, trades());
        assert_eq!(v.state(), Completeness::Suspect);
        assert!(v.guard().is_ok(), "suspect is a state the job survives");
        assert!(!v.may_tombstone(), "a short run must not tombstone");
        assert_eq!(v.missing(), ["CA"]);
    }

    #[test]
    fn a_complete_roster_is_ok_and_may_tombstone() {
        let exp = Expectation::roster("states", &ROSTER, &present(&ROSTER));
        let v = judge(&exp, trades());
        assert_eq!(v.state(), Completeness::Ok);
        assert!(v.may_tombstone());
        assert!(v.message().is_none());
        assert_eq!(v.to_json()["missing"], json!([]));
    }

    #[test]
    fn a_short_snapshot_still_may_not_tombstone_but_never_fails() {
        // state-tax's disposition: 30 of 51 suppresses removals; it does not
        // turn a paid agentic run red.
        let exp = Expectation::counts("states", 51, 30);
        let v = judge(&exp, Disposition::suppress_only(0.9));
        assert_eq!(v.state(), Completeness::Suspect);
        assert!(!v.may_tombstone());
        assert!(v.guard().is_ok());
        // Even a 1-of-51 run only suppresses under this disposition.
        let v = judge(
            &Expectation::counts("states", 51, 1),
            Disposition::suppress_only(0.9),
        );
        assert_eq!(v.state(), Completeness::Suspect);
        assert!(v.guard().is_ok());
    }

    #[test]
    fn a_row_floor_collapses_one_row_below_and_passes_at_it() {
        let d = Disposition::fatal(1.0);
        let below = judge(&Expectation::min_rows("rows", 50, 49), d);
        assert_eq!(below.state(), Completeness::Collapsed);
        assert!(below.guard().is_err());
        let at = judge(&Expectation::min_rows("rows", 50, 50), d);
        assert_eq!(at.state(), Completeness::Ok);
        // Far above the floor is still exactly Ok — the floor catches collapse,
        // it does not police the source's size.
        let above = judge(&Expectation::min_rows("rows", 50, 5_000), d);
        assert_eq!(above.state(), Completeness::Ok);
        assert_eq!(above.covered(), 50);
    }

    #[test]
    fn a_truncated_feed_is_not_a_collapsed_one() {
        let d = Disposition::fatal(1.0);
        // 999 postings of a 1 000 floor on the DEFAULT full-width feed: collapsed.
        let full = judge(&Expectation::min_rows("postings", 1_000, 999), d);
        assert_eq!(full.state(), Completeness::Collapsed);
        // The SAME 999 postings asked for on purpose: nothing to judge.
        let truncated = judge(&Expectation::unbounded("postings"), d);
        assert_eq!(truncated.state(), Completeness::Ok);
        assert!(truncated.guard().is_ok());
        assert_eq!(truncated.to_json()["bounded"], json!(false));
    }

    #[test]
    fn a_lossy_parse_is_suspect_and_a_clean_one_tombstones() {
        let d = Disposition::suppress_only(1.0);
        let lossy = judge(&Expectation::parsed_ratio("<dump> blocks", 3, 2), d);
        assert_eq!(lossy.state(), Completeness::Suspect);
        assert!(!lossy.may_tombstone());
        let clean = judge(&Expectation::parsed_ratio("<dump> blocks", 3, 3), d);
        assert_eq!(clean.state(), Completeness::Ok);
        assert!(clean.may_tombstone());
    }

    #[test]
    fn allow_shrink_forces_ok_without_touching_the_numbers() {
        let exp = Expectation::roster("states", &ROSTER, &present(&["AL"]));
        let v = judge(&exp, trades().allow_shrink(true));
        assert_eq!(v.state(), Completeness::Ok);
        assert!(v.may_tombstone());
        // The shortfall is still reported honestly — only the verdict moved.
        assert_eq!(v.covered(), 1);
        assert_eq!(v.expected(), 5);
        assert_eq!(v.missing(), ["AK", "AZ", "AR", "CA"]);
    }

    #[test]
    fn an_empty_expectation_is_complete_not_collapsed() {
        let v = judge(&Expectation::counts("trades", 0, 0), trades());
        assert_eq!(v.state(), Completeness::Ok);
        assert_eq!(v.ratio(), 1.0);
    }

    #[test]
    fn a_collapse_floor_above_the_suspect_floor_is_clamped() {
        // A nonsense disposition must not make a run collapsed while it is not
        // even short: collapse implies suspect.
        let d = Disposition::new(0.5, 0.9);
        assert_eq!(d.collapse_below, 0.5);
        let v = judge(&Expectation::counts("rows", 10, 6), d);
        assert_eq!(v.state(), Completeness::Ok);
    }

    #[test]
    fn the_json_block_names_the_verdict_and_the_floors() {
        let v = judge(
            &Expectation::roster("trades", &ROSTER, &present(&["AL"])),
            trades(),
        );
        let j = v.to_json();
        assert_eq!(j["verdict"], "collapsed");
        assert_eq!(j["unit"], "trades");
        assert_eq!(j["covered"], 1);
        assert_eq!(j["expected"], 5);
        assert_eq!(j["ratio"], 0.2);
        assert_eq!(j["suspect_below"], 0.9);
        assert_eq!(j["collapse_below"], 0.5);
        assert_eq!(j["bounded"], true);
        assert!(j["message"].as_str().unwrap().contains("collapsed"));
    }

    #[test]
    fn a_count_only_expectation_reports_null_missing_not_empty() {
        let v = judge(&Expectation::counts("priced trades", 5, 1), trades());
        assert_eq!(v.to_json()["missing"], Value::Null);
        assert!(v.missing().is_empty());
    }
}
