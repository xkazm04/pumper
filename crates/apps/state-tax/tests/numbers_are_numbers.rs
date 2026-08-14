//! `run()`-level proof that a **quoted** figure from the model is stored as a
//! JSON number and reaches the join.
//!
//! `validate::num` deliberately accepts `"13.3"` so a model that quotes its
//! numbers still validates — but the app then stored `s.clone()`, the raw model
//! JSON. So `"13.3"` passed `require_rate` as 13.3 and landed in the store as the
//! **string** `"13.3"`, and `sync_operator_economics`'s
//! `.and_then(Value::as_f64)` dropped that state out of `median_state_rate`
//! without a word. The catalog `ranges` contract cannot see it either: ranges are
//! checked only when a field is present *and numeric*.
//!
//! Every test here is named after the anti-pattern it defends.

use std::sync::Arc;

use app_state_tax::StateTax;
use pumper_core::testing::{engines_with, Dead, ScriptedResearcher, TempStore, TestContext};
use pumper_core::{Datasets, ScrapeApp};
use serde_json::{json, Value};

const YEAR: &str = "2025";

/// An answer whose every number is QUOTED, exactly as a model periodically
/// returns them — commas, dollar signs and all.
fn quoted_answer() -> String {
    json!({
        "year": YEAR,
        "federal": {
            "self_employment_tax_rate": "15.3",
            "qbi_deduction_pct": "20",
            "standard_deduction_single": "$15,000",
            "section_179_limit": "1,250,000",
            "top_marginal_rate": "37.0",
        },
        "states": [
            { "state": "CA", "income_tax_type": "graduated", "top_marginal_rate": "13.3", "top_bracket_threshold": "$1,000,000" },
            { "state": "TX", "income_tax_type": "none", "top_marginal_rate": "0", "top_bracket_threshold": "0" },
            { "state": "PA", "income_tax_type": "flat", "top_marginal_rate": "3.07", "top_bracket_threshold": "0" },
        ],
    })
    .to_string()
}

async fn run_answer(store: &TempStore, answer: String) -> Value {
    let claude = Arc::new(ScriptedResearcher::new().always_text(answer));
    let ctx = TestContext::new(&store.storage, "state-tax")
        .params(json!({ "year": YEAR }))
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), claude))
        .build();
    StateTax.run(ctx).await.expect("run")
}

async fn stored(datasets: &Datasets, key: &str) -> Value {
    datasets
        .get("state-tax", "tax", key)
        .await
        .expect("get")
        .unwrap_or_else(|| panic!("{key} was not stored"))
        .data
}

/// The headline: a quoted rate must round-trip as a NUMBER and must reach
/// `illustrative_state_top_marginal_rate_median`. Against pre-fix code the
/// stored value is the string `"13.3"` and the median is `null`.
#[tokio::test]
async fn a_quoted_rate_round_trips_as_a_number_and_reaches_the_median() {
    let store = TempStore::new("state-tax-quoted").await;
    let datasets = store.datasets();
    run_answer(&store, quoted_answer()).await;

    let ca = stored(&datasets, "state:CA").await;
    assert!(
        ca["top_marginal_rate"].is_number(),
        "stored raw: {}",
        ca["top_marginal_rate"]
    );
    assert_eq!(ca["top_marginal_rate"].as_f64(), Some(13.3));
    // Dollar magnitudes too — a quoted "$1,000,000" drops out of a consumer the
    // same way a quoted rate does.
    assert_eq!(ca["top_bracket_threshold"].as_f64(), Some(1_000_000.0));

    let fed = stored(&datasets, "federal:US").await;
    assert_eq!(fed["self_employment_tax_rate"].as_f64(), Some(15.3));
    assert_eq!(fed["section_179_limit"].as_f64(), Some(1_250_000.0));

    // ...and the join sees them. Median of {0, 3.07, 13.3} = 3.07.
    let us_row = datasets
        .list("trades", "operator_economics", 200)
        .await
        .expect("joined")
        .into_iter()
        .find(|r| r.key.starts_with("US:"))
        .expect("a national roll-up row");
    assert_eq!(
        us_row.data["tax"]["illustrative_state_top_marginal_rate_median"].as_f64(),
        Some(3.07),
        "every state's rate must be readable as f64: {}",
        us_row.data["tax"]
    );
}

/// A state's real rate reaches its own joined row as a number, not a string —
/// the per-state half of the same defect.
#[tokio::test]
async fn a_quoted_rate_reaches_the_per_state_joined_row_as_a_number() {
    let store = TempStore::new("state-tax-quoted-row").await;
    let datasets = store.datasets();
    run_answer(&store, quoted_answer()).await;

    let ca_row = datasets
        .list("trades", "operator_economics", 500)
        .await
        .expect("joined")
        .into_iter()
        .find(|r| r.key.starts_with("CA:"))
        .expect("a CA row");
    assert_eq!(
        ca_row.data["tax"]["state"]["top_marginal_rate"].as_f64(),
        Some(13.3)
    );
}

/// `income_tax_type` is a closed vocabulary the prompt declares and nothing
/// enforced: "Progressive" and "N/A" both stored and flowed into
/// `state_tax_context`. Drift normalizes; junk rejects the record.
#[tokio::test]
async fn an_unclassifiable_income_tax_type_rejects_the_record_and_drift_normalizes() {
    let store = TempStore::new("state-tax-vocab").await;
    let datasets = store.datasets();
    let answer = json!({
        "year": YEAR,
        "federal": { "self_employment_tax_rate": 15.3, "qbi_deduction_pct": 20.0, "top_marginal_rate": 37.0 },
        "states": [
            { "state": "NY", "income_tax_type": "Progressive", "top_marginal_rate": 10.9 },
            { "state": "FL", "income_tax_type": "No state income tax", "top_marginal_rate": 0.0 },
            { "state": "CO", "income_tax_type": "Single rate", "top_marginal_rate": 4.4 },
            { "state": "ZZ", "income_tax_type": "banana", "top_marginal_rate": 5.0 },
        ],
    })
    .to_string();
    let out = run_answer(&store, answer).await;

    assert_eq!(
        stored(&datasets, "state:NY").await["income_tax_type"],
        "graduated"
    );
    assert_eq!(
        stored(&datasets, "state:FL").await["income_tax_type"],
        "none"
    );
    assert_eq!(
        stored(&datasets, "state:CO").await["income_tax_type"],
        "flat"
    );
    assert!(
        datasets
            .get("state-tax", "tax", "state:ZZ")
            .await
            .expect("get")
            .is_none(),
        "an unclassifiable income_tax_type rejects the record rather than storing junk"
    );
    let reasons = out["rejected"]
        .as_array()
        .expect("rejected[]")
        .iter()
        .find(|r| r["key"] == "state:ZZ")
        .expect("ZZ is reported as rejected")["reasons"]
        .to_string();
    assert!(reasons.contains("income_tax_type"), "{reasons}");
}

/// The unit is percentage points (`trades_common::validate::RATE_UNIT`). A
/// fraction-shaped rate is a 100x error in stored market data, so it rejects
/// rather than storing quietly.
#[tokio::test]
async fn a_fraction_shaped_rate_is_rejected_not_stored_as_a_percentage() {
    let store = TempStore::new("state-tax-unit").await;
    let datasets = store.datasets();
    let answer = json!({
        "year": YEAR,
        "federal": { "self_employment_tax_rate": 15.3, "qbi_deduction_pct": 20.0, "top_marginal_rate": 37.0 },
        "states": [
            { "state": "CA", "income_tax_type": "graduated", "top_marginal_rate": 0.133 },
            { "state": "TX", "income_tax_type": "none", "top_marginal_rate": 0.0 },
        ],
    })
    .to_string();
    let out = run_answer(&store, answer).await;

    assert!(
        datasets
            .get("state-tax", "tax", "state:CA")
            .await
            .expect("get")
            .is_none(),
        "0.133 is a fraction, not 13.3%"
    );
    // 0 is still a legitimate answer for a no-income-tax state.
    assert!(datasets
        .get("state-tax", "tax", "state:TX")
        .await
        .expect("get")
        .is_some());
    assert_eq!(out["rejected_count"], 1);
}
