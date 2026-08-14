//! `run()`-level proof of the destructive case: **a `state-tax` run that returns
//! 30 of 51 jurisdictions must not tombstone the other 21.**
//!
//! `state-tax` is the one app in the trades family that writes a full snapshot
//! through `sync_many_with_provenance`, so before the completeness floor every
//! previously-live state absent from a short answer was marked removed — and the
//! job still reported SUCCESS with no `removed` count at all. Core's own doc
//! names the hole: `detect_removed` "already refuses an *empty* batch; a partial
//! batch is the case that guard does not cover".
//!
//! Every test here is named after the anti-pattern it defends.

use std::sync::Arc;

use app_state_tax::StateTax;
use pumper_core::testing::{
    engines_with, research_output, Dead, ScriptedResearcher, TempStore, TestContext,
};
use pumper_core::{Datasets, ScrapeApp};
use serde_json::{json, Value};

/// The 50 states + DC in the order the app enumerates them.
const ROSTER: [&str; 51] = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
];

const YEAR: &str = "2025";

/// One agent answer covering `states` — the shape `tax_schema()` pins.
fn answer(states: &[&str]) -> String {
    let entries: Vec<Value> = states
        .iter()
        .map(|s| {
            json!({
                "state": s,
                "state_name": format!("State of {s}"),
                "income_tax_type": "flat",
                "top_marginal_rate": 5.0,
                "top_bracket_threshold": 0,
                "notes": "scripted",
            })
        })
        .collect();
    json!({
        "year": YEAR,
        "federal": {
            "self_employment_tax_rate": 15.3,
            "qbi_deduction_pct": 20.0,
            "standard_deduction_single": 15000,
            "section_179_limit": 1250000,
            "top_marginal_rate": 37.0,
        },
        "states": entries,
    })
    .to_string()
}

/// Seed the store with a COMPLETE 51-state snapshot plus the federal record, so
/// the next run has something to destroy.
async fn seed_full_roster(datasets: &Datasets) {
    let mut items: Vec<(String, Value)> = ROSTER
        .iter()
        .map(|s| {
            (
                format!("state:{s}"),
                json!({
                    "level": "state",
                    "state": s,
                    "state_name": format!("State of {s}"),
                    "income_tax_type": "flat",
                    "top_marginal_rate": 5.0,
                    "year": YEAR,
                }),
            )
        })
        .collect();
    items.push((
        "federal:US".to_string(),
        json!({ "level": "federal", "state": "US", "year": YEAR }),
    ));
    datasets
        .upsert_many("state-tax", "tax", &items)
        .await
        .expect("seed");
}

/// Live (non-tombstoned) `state:` keys currently in the store.
async fn live_states(datasets: &Datasets) -> Vec<String> {
    let mut keys: Vec<String> = datasets
        .list("state-tax", "tax", 500)
        .await
        .expect("list")
        .into_iter()
        .filter(|r| r.removed_at.is_none() && r.key.starts_with("state:"))
        .map(|r| r.key)
        .collect();
    keys.sort();
    keys
}

/// Drive one `run()` against a scripted answer covering `states`.
async fn run_with(store: &TempStore, states: &[&str], params: Value) -> Value {
    let claude = Arc::new(ScriptedResearcher::new().always_text(answer(states)));
    let ctx = TestContext::new(&store.storage, "state-tax")
        .params(params)
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), claude))
        .build();
    StateTax.run(ctx).await.expect("run")
}

/// **The direction's headline case.** 30 of 51 states came back; the other 21
/// were live in the store. Against pre-floor code `sync_many_with_provenance`
/// tombstones all 21 and the job is green — this test fails there and passes
/// with the completeness floor in place.
#[tokio::test]
async fn a_thirty_of_fiftyone_run_does_not_tombstone_the_other_twentyone() {
    let store = TempStore::new("state-tax-partial").await;
    let datasets = store.datasets();
    seed_full_roster(&datasets).await;
    assert_eq!(live_states(&datasets).await.len(), 51, "seeded roster");

    let short: Vec<&str> = ROSTER[..30].to_vec();
    // `force` bypasses the *vintage* gate only — it must not disable the floor.
    let out = run_with(&store, &short, json!({ "year": YEAR, "force": true })).await;

    assert_eq!(
        live_states(&datasets).await.len(),
        51,
        "a short run must not delete the 21 states it failed to return"
    );
    assert_eq!(out["coverage"]["covered"], 30);
    assert_eq!(out["coverage"]["expected"], 51);
    assert_eq!(out["coverage"]["short"], true);
    assert_eq!(out["removed"], 0, "nothing was tombstoned");
    assert!(
        out["removals_suppressed"].is_string(),
        "a suppressed removal is visible as such, not silently absent: {out}"
    );
    let warnings = out["warnings"].as_array().expect("warnings[]");
    assert!(
        warnings.len() >= 2,
        "short coverage AND the suppressed removal are both surfaced: {warnings:?}"
    );
    assert!(warnings
        .iter()
        .any(|w| w.as_str().is_some_and(|s| s.contains("coverage short"))));
}

/// The floor must not become "never delete anything": a run that clears it keeps
/// full-snapshot semantics, so a jurisdiction that genuinely dropped out is
/// tombstoned and *counted* in the result.
#[tokio::test]
async fn a_complete_run_still_tombstones_and_reports_the_removal() {
    let store = TempStore::new("state-tax-complete").await;
    let datasets = store.datasets();
    seed_full_roster(&datasets).await;

    // 50 of 51 = 98%, comfortably over the floor. WY is the one that vanished.
    let all_but_wy: Vec<&str> = ROSTER.iter().copied().filter(|s| *s != "WY").collect();
    let out = run_with(&store, &all_but_wy, json!({ "year": YEAR, "force": true })).await;

    assert_eq!(out["coverage"]["short"], false);
    assert!(out["removals_suppressed"].is_null());
    assert_eq!(out["removed"], 1, "WY dropped out of a complete snapshot");
    let live = live_states(&datasets).await;
    assert_eq!(live.len(), 50);
    assert!(!live.contains(&"state:WY".to_string()));
}

/// The escape hatch, and the reason it is not `force`: `force: true` is the
/// ordinary way to re-run this vintage-gated app, so hanging the hatch on it
/// would switch the floor off on exactly the runs it protects. `allow_shrink`
/// is the explicit opt-in.
#[tokio::test]
async fn allow_shrink_lets_a_short_run_delete_but_force_alone_does_not() {
    let store = TempStore::new("state-tax-shrink").await;
    let datasets = store.datasets();
    seed_full_roster(&datasets).await;

    let short: Vec<&str> = ROSTER[..30].to_vec();
    let out = run_with(
        &store,
        &short,
        json!({ "year": YEAR, "force": true, "allow_shrink": true }),
    )
    .await;

    assert_eq!(out["coverage"]["short"], true, "still reported as short");
    assert!(
        out["removals_suppressed"].is_null(),
        "the operator authorised the shrink"
    );
    assert_eq!(out["removed"], 21);
    assert_eq!(live_states(&datasets).await.len(), 30);
}

/// The tombstone leak at the join: `Datasets::list` returns removed records by
/// design, so before the consumer-side filter a state that `state-tax` had just
/// tombstoned still produced a live `<ST>:<trade>` row in
/// `trades/operator_economics` and its rate still entered `median_state_rate`.
#[tokio::test]
async fn a_tombstoned_state_does_not_reappear_as_a_live_joined_row() {
    let store = TempStore::new("state-tax-join").await;
    let datasets = store.datasets();
    seed_full_roster(&datasets).await;

    // Authorised shrink to 30 states — the other 21 are now tombstones that
    // `list` still hands back.
    let short: Vec<&str> = ROSTER[..30].to_vec();
    run_with(
        &store,
        &short,
        json!({ "year": YEAR, "force": true, "allow_shrink": true }),
    )
    .await;

    let joined = datasets
        .list("trades", "operator_economics", 1000)
        .await
        .expect("joined rows");
    let dead_prefixes: Vec<&str> = ROSTER[30..].to_vec();
    for r in &joined {
        if r.removed_at.is_some() {
            continue;
        }
        let Some((st, _)) = r.key.split_once(':') else {
            continue;
        };
        assert!(
            !dead_prefixes.contains(&st),
            "{} was tombstoned in state-tax/tax but came back live in the join",
            r.key
        );
    }
    assert!(
        joined.iter().any(|r| r.key.starts_with("CA:")),
        "the join still produced rows for the states that survived"
    );
}

/// Keeps the harness honest: [`research_output`] is what the scripted answer is
/// built from, and a malformed script would make every assertion above vacuous.
#[test]
fn the_scripted_answer_parses_as_the_tax_schema_shape() {
    let out = research_output(answer(&ROSTER));
    let json = out.json.expect("scripted answer is JSON");
    assert_eq!(json["states"].as_array().expect("states").len(), 51);
    assert!(json["federal"]["qbi_deduction_pct"].is_number());
}
