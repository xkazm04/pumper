//! The HN test source: 40 hand-trimmed Hacker News item pages and the RuleSet
//! `docs/features/resilient-extraction.md` ("HN test source") schedules against
//! the live site.
//!
//! Fixtures (`tests/fixtures/hn/<id>.html`): the `fatitem` story table exactly
//! as `news.ycombinator.com/item?id=8863` served it on 2026-09-22, rebuilt for
//! the first 40 linked stories (ids 1..) with each story's values from the HN
//! API, comments and navigation trimmed. `manifest.json` holds what each page
//! must extract to. The live check is `#[ignore]`d in the server crate
//! (`e2e::hn_source`) — a normal test never touches the network.

use pumper_core::extract::extract_one_with_report_at;
use pumper_core::{FieldStatus, RuleSet};
use serde_json::Value;

const RULESET: &str = include_str!("fixtures/hn/ruleset.json");
const MANIFEST: &str = include_str!("fixtures/hn/manifest.json");

fn fixture(id: u64) -> String {
    let path = format!("{}/tests/fixtures/hn/{id}.html", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn ruleset() -> RuleSet {
    serde_json::from_str(RULESET).expect("the HN ruleset parses")
}

#[test]
fn there_are_forty_hn_fixtures() {
    let manifest: Vec<Value> = serde_json::from_str(MANIFEST).unwrap();
    assert_eq!(manifest.len(), 40);
}

#[test]
fn the_hn_ruleset_extracts_all_five_fields_from_every_fixture() {
    let compiled = ruleset().compile().expect("the HN ruleset compiles");
    let manifest: Vec<Value> = serde_json::from_str(MANIFEST).unwrap();
    for entry in &manifest {
        let id = entry["id"].as_u64().unwrap();
        let page = entry["page"].as_str().unwrap();
        let (values, report) = extract_one_with_report_at(&compiled, &fixture(id), Some(page));
        for field in ["title", "url", "by", "time", "points"] {
            assert_eq!(
                report.fields.get(field),
                Some(&FieldStatus::Matched),
                "item {id}: `{field}` did not match"
            );
        }
        assert_eq!(values, entry["expected"], "item {id}");
    }
}

/// A template element the selectors must NOT fall back onto: the page title
/// carries the story title too, and `points` must be an integer, not the
/// "57 points" label.
#[test]
fn points_are_integers_and_the_title_is_the_story_link_not_the_page_title() {
    let compiled = ruleset().compile().unwrap();
    let (values, _) = extract_one_with_report_at(
        &compiled,
        &fixture(1),
        Some("https://news.ycombinator.com/item?id=1"),
    );
    assert!(values["points"].is_i64(), "{values}");
    assert!(!values["title"].as_str().unwrap().contains("Hacker News"));
}
