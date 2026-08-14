//! `run()`-level proof that `trades/operator_economics` — the product of the
//! whole trades family — is **reachable** by the mechanisms that already exist,
//! and is written as the derived dataset it is.
//!
//! Before this: zero apps declared `index_datasets`, so `run_indexed_apps` never
//! widened past the job's own app and every `trades/*` revision was invisible to
//! watches, dataset triggers, `enforce_contracts`, search docs and DataHub
//! lineage. And the write was a raw `ctx.datasets.upsert_many` — no `Provenance`,
//! no `job_id`, no trust stamp, no `@q` diversion, `DerivedPaths::NONE`.
//!
//! Every test here is named after the anti-pattern it defends.

use std::sync::Arc;

use app_state_tax::StateTax;
use pumper_core::testing::{engines_with, Dead, ScriptedResearcher, TempStore, TestContext};
use pumper_core::ScrapeApp;
use serde_json::{json, Value};
use trades_common::unified;

const YEAR: &str = "2025";

fn answer(states: &[(&str, f64)]) -> String {
    let entries: Vec<Value> = states
        .iter()
        .map(|(s, rate)| {
            json!({
                "state": s,
                "state_name": format!("State of {s}"),
                "income_tax_type": "flat",
                "top_marginal_rate": rate,
                "top_bracket_threshold": 0,
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

async fn run_with(store: &TempStore, states: &[(&str, f64)]) -> Value {
    let claude = Arc::new(ScriptedResearcher::new().always_text(answer(states)));
    let ctx = TestContext::new(&store.storage, "state-tax")
        .params(json!({ "year": YEAR, "force": true, "allow_shrink": true }))
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), claude))
        .build();
    StateTax.run(ctx).await.expect("run")
}

const THREE: [(&str, f64); 3] = [("CA", 13.3), ("TX", 0.0), ("PA", 3.07)];

/// The declaration has to reach the RESULT — `run_indexed_apps` reads it from
/// there, not from the manifest.
#[tokio::test]
async fn the_index_declaration_reaches_the_result_naming_both_trades_datasets() {
    let store = TempStore::new("trades-index").await;
    let out = run_with(&store, &THREE).await;

    assert_eq!(out["index_datasets"], unified::product_index_datasets());
    let specs = out["index_datasets"].as_array().expect("specs");
    assert!(specs.iter().all(|s| s["app"] == "trades"));
    let datasets: Vec<&str> = specs.iter().filter_map(|s| s["dataset"].as_str()).collect();
    assert_eq!(datasets, ["operator_economics", "compliance"]);
}

/// The most-derived dataset in the family carried the LEAST provenance: a raw
/// `ctx.datasets` write stamps nothing, not even the producing job.
#[tokio::test]
async fn every_joined_row_carries_a_job_id_and_names_its_inputs() {
    let store = TempStore::new("trades-prov").await;
    run_with(&store, &THREE).await;
    let datasets = store.datasets();

    let rows = datasets
        .list("trades", "operator_economics", 500)
        .await
        .expect("joined rows");
    assert!(!rows.is_empty(), "the join produced rows");
    for r in &rows {
        let revs = datasets
            .history("trades", "operator_economics", &r.key, 1)
            .await
            .expect("history");
        let prov = &revs.first().expect("a revision").provenance;
        assert!(
            prov.job_id.is_some(),
            "{} carries no job_id — the raw write path stamps nothing",
            r.key
        );
        let url = prov.source_url.as_deref().unwrap_or_default();
        assert!(
            url.starts_with("derived://trades/operator_economics?inputs="),
            "provenance must name the join's inputs, got {url:?}"
        );
        assert!(url.contains("state-tax/tax"), "{url}");
        // A joined row has no archived body and no RuleSet — it must not claim
        // to be replayable.
        assert!(prov.artifact_sha.is_none());
        assert!(prov.rules_hash.is_none());
    }
}

/// Idempotence: re-deriving the identical join must read `unchanged`, not
/// churn the change feed of the one dataset a consumer would watch.
#[tokio::test]
async fn a_byte_identical_rederivation_reads_unchanged() {
    let store = TempStore::new("trades-idem").await;
    let first = run_with(&store, &THREE).await;
    assert!(first["unified"]["new"].as_u64().unwrap_or(0) > 0);

    let second = run_with(&store, &THREE).await;
    assert_eq!(
        second["unified"]["changed"], 0,
        "a re-derivation of identical inputs is not a change: {}",
        second["unified"]
    );
    assert_eq!(second["unified"]["new"], 0);
    assert!(second["unified"]["unchanged"].as_u64().unwrap_or(0) > 0);
    assert_eq!(second["unified"]["join_complete"], true);
    assert_eq!(second["unified"]["dataset"], "trades/operator_economics");
    assert_eq!(second["unified"]["inputs_truncated"], json!([]));
}

/// A change to ONE state's rate touches that state's rows and nothing else.
#[tokio::test]
async fn a_one_state_change_does_not_mark_every_other_state_changed() {
    let store = TempStore::new("trades-scope").await;
    run_with(&store, &THREE).await;

    // CA 13.3 -> 12.0. The median of {0, 3.07, 12.0} is still 3.07, so the
    // national roll-up rows are genuinely unchanged too.
    let moved = [("CA", 12.0), ("TX", 0.0), ("PA", 3.07)];
    let out = run_with(&store, &moved).await;
    // 5 trades × (1 US roll-up + 3 state rows) = 20 rows.
    assert_eq!(
        out["unified"]["changed"], 5,
        "only the five CA rows moved: {}",
        out["unified"]
    );
    assert_eq!(out["unified"]["unchanged"], 15, "{}", out["unified"]);
}

/// **The `DerivedPaths` payoff.** `tax.federal` is the same national block
/// replicated onto every per-state row, so a federal-constants refresh used to
/// mark all ~255 per-state rows `changed` for one national fact. It is now
/// announced exactly once, on the `US:<trade>` roll-ups.
#[tokio::test]
async fn a_national_fact_is_announced_on_the_rollups_not_on_every_state_row() {
    let store = TempStore::new("trades-national").await;
    run_with(&store, &THREE).await;

    let claude = Arc::new(ScriptedResearcher::new().always_text({
        let mut a: Value = serde_json::from_str(&answer(&THREE)).expect("json");
        a["federal"]["self_employment_tax_rate"] = json!(16.0);
        a.to_string()
    }));
    let ctx = TestContext::new(&store.storage, "state-tax")
        .params(json!({ "year": YEAR, "force": true, "allow_shrink": true }))
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), claude))
        .build();
    let out = StateTax.run(ctx).await.expect("run");

    assert_eq!(
        out["unified"]["changed"], 5,
        "one federal fact, five roll-up rows — not 15 per-state rows too: {}",
        out["unified"]
    );
    assert_eq!(out["unified"]["unchanged"], 15, "{}", out["unified"]);

    // The per-state rows still STORE the new federal block — `DerivedPaths`
    // narrows the change-detection hash and nothing else.
    let ca = store
        .datasets()
        .get("trades", "operator_economics", "CA:Plumbing")
        .await
        .expect("get")
        .expect("CA row");
    assert_eq!(ca.data["tax"]["federal"]["self_employment_tax_rate"], 16.0);
}
