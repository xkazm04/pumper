//! Derived-backfill cost harness — `#[ignore]`d, run with `just test-ignored`.
//!
//! The backfill materializes a new spec over the existing source rows. Two
//! costs dominate it on a large corpus, and both used to be paid PER SOURCE
//! RECORD:
//!
//! 1. `parse_filter_specs(&spec.filters)` — the spec's filter grammar was
//!    re-parsed for every row scanned (the group path always hoisted it).
//! 2. The `lookup` join — one `SELECT ... WHERE key = ?` point query per row.
//!
//! This prints the wall clock of a 50k-row backfill with a lookup join against
//! the same work done the old way (parse-per-row + point-query-per-row), which
//! is what the `naive_` half of this harness reproduces. Timing-dependent by
//! construction, so it asserts only that the two produce the same rows.

use std::collections::BTreeMap;
use std::time::Instant;

use pumper_core::testing::TempStore;
use pumper_core::{DerivedLookup, NewDerivedSpec};
use serde_json::json;

const N: usize = 50_000;

#[tokio::test]
#[ignore = "perf harness; run with --ignored"]
async fn backfill_with_lookup_batches_its_joins() {
    let store = TempStore::new("derived-backfill-perf").await;
    let ds = store.datasets();

    // Lookup side: 500 agencies the source rows join to.
    let agencies: Vec<(String, serde_json::Value)> = (0..500)
        .map(|i| {
            (
                format!("ag-{i:03}"),
                json!({ "name": format!("Agency {i}") }),
            )
        })
        .collect();
    ds.upsert_many("app", "agencies", &agencies).await.unwrap();

    let items: Vec<(String, serde_json::Value)> = (0..N)
        .map(|i| {
            (
                format!("grant-{i:06}"),
                json!({
                    "n": i,
                    "state": if i % 3 == 0 { "CA" } else { "NY" },
                    "agency": format!("ag-{:03}", i % 500),
                }),
            )
        })
        .collect();
    ds.upsert_many("app", "grants", &items).await.unwrap();

    let project: BTreeMap<String, String> =
        [("n".to_string(), "$.n".to_string())].into_iter().collect();
    let lookup = DerivedLookup {
        dataset: "agencies".into(),
        key_expr: "$.agency".into(),
        merge_as: "agency".into(),
    };
    let spec = store
        .storage
        .create_derived_spec(&NewDerivedSpec {
            source_app: "app",
            source_dataset: "grants",
            target_dataset: "ca_grants",
            filters: &["$.state:eq:CA".to_string()],
            project: &project,
            lookup: Some(&lookup),
            group: None,
        })
        .await
        .unwrap();

    let t = Instant::now();
    let report = ds
        .backfill_derived_budgeted(
            &spec,
            &pumper_core::BackfillOpts {
                batch: 500,
                max_rows: i64::MAX,
                cursor: None,
            },
        )
        .await
        .unwrap();
    let batched = t.elapsed();
    assert_eq!(report.scanned, N as u64);
    assert!(report.done);

    // The old shape, reproduced against a second target so both halves do the
    // same amount of writing and the delta is the read side: filter specs
    // parsed per record, and the join resolved with one point query per record.
    let t = Instant::now();
    let mut matched = 0u64;
    let mut after: Option<(String, String)> = None;
    loop {
        let page = ds
            .list_page("app", "grants", after, 500, None)
            .await
            .unwrap();
        let n = page.len();
        let mut items: Vec<(String, serde_json::Value)> = Vec::new();
        for rec in &page {
            let filters = pumper_core::parse_filter_specs(&spec.filters).unwrap();
            if !pumper_core::filters_match(&filters, &rec.data) {
                continue;
            }
            let mut value = pumper_core::project_value(&project, &rec.data);
            let key = rec.data["agency"].as_str().unwrap().to_string();
            if let Some(joined) = ds.get("app", "agencies", &key).await.unwrap() {
                value["agency"] = joined.data;
            }
            items.push((rec.key.clone(), value));
            matched += 1;
        }
        ds.upsert_many("app", "ca_grants_naive", &items)
            .await
            .unwrap();
        if n < 500 {
            break;
        }
        let last = page.last().unwrap();
        after = Some((pumper_core::datasets::ts(last.updated_at), last.key.clone()));
    }
    let naive = t.elapsed();
    assert_eq!(matched, report.matched, "same rows, both ways");

    println!("derived backfill over {N} rows with a 500-key lookup join");
    println!("  batched (hoisted parse + chunked joins): {batched:?}");
    println!("  per-record parse + point-query join:     {naive:?}");
}
