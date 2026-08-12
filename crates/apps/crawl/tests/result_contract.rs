//! What the crawl result CLAIMS must be what the run did — proved by driving
//! the real `Crawl::run()` against a real store and an in-memory site.
//!
//! Four things it used to get wrong:
//!
//! 1. `frontier_dropped` and `skipped_host_budget` — computed and documented in
//!    core as the fields that keep a capped crawl honest — were emitted by
//!    nobody, so a truncated crawl was byte-identical to a complete one;
//! 2. nothing said *whether* a crawl was truncated, only two raw counters whose
//!    relationship was undocumented;
//! 3. `edges_written` discarded the store's `UpsertSummary` and counted the rows
//!    it handed over, so no-op upserts were reported as writes;
//! 4. the manifest's `output_shape` promised `pages` / `skipped` / `unchanged`
//!    (keys no run has ever emitted) and omitted every field added by the last
//!    four milestones.

mod common;

use app_crawl::Crawl;
use common::{crawl_ctx, result_keys, StubSite};
use pumper_core::testing::TempStore;
use pumper_core::ScrapeApp;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;

/// A four-page site: the root links to three children, none of which link on.
fn small_site() -> Arc<StubSite> {
    Arc::new(
        StubSite::new()
            .page(
                "https://example.com/",
                &[
                    "https://example.com/a",
                    "https://example.com/b",
                    "https://example.com/c",
                ],
            )
            .page("https://example.com/a", &[])
            .page("https://example.com/b", &[])
            .page("https://example.com/c", &[]),
    )
}

fn base_params() -> Value {
    json!({
        "seeds": ["https://example.com/"],
        "max_pages": 50,
        "max_depth": 2,
        "concurrency": 1,
        // Off so the fixture's page set is exactly what gets crawled: the
        // filler text makes fingerprints differ, but a test asserting on page
        // counts should not also be a near-dup test.
        "dedup_distance": 0,
        "respect_robots": false,
    })
}

#[tokio::test]
async fn a_complete_crawl_says_so_instead_of_leaving_the_caller_to_guess() {
    let store = TempStore::new("crawl-coverage-complete").await;
    let out = Crawl
        .run(crawl_ctx(&store, small_site(), base_params()))
        .await
        .unwrap();

    assert_eq!(out["crawled"], 4, "{out}");
    assert_eq!(out["kept"], 4, "{out}");
    // The two counters core computes for exactly this purpose, finally emitted.
    assert_eq!(out["frontier_dropped"], 0, "{out}");
    assert_eq!(out["skipped_host_budget"], 0, "{out}");
    // ...plus the verdict, so a caller does not have to know that two zeros
    // mean "this crawl saw the whole discovered graph".
    assert_eq!(out["coverage_complete"], true, "{out}");
    assert!(
        out.get("warnings").is_none(),
        "a complete crawl must not warn, or the flag is noise: {out}"
    );
}

#[tokio::test]
async fn a_host_budget_truncation_is_reported_not_silently_dropped() {
    // THE REFUTED BEHAVIOR: `max_pages_per_host` dumps the host's entire
    // remaining backlog and core counts it — and the result said nothing, so a
    // caller treated a one-page slice of a four-page site as the whole site.
    let store = TempStore::new("crawl-coverage-host-budget").await;
    let mut params = base_params();
    params["max_pages_per_host"] = json!(1);
    let out = Crawl
        .run(crawl_ctx(&store, small_site(), params))
        .await
        .unwrap();

    assert_eq!(
        out["crawled"], 1,
        "one page, then the host left the rotation"
    );
    assert_eq!(
        out["skipped_host_budget"], 3,
        "the three queued children were dumped: {out}"
    );
    assert_eq!(out["coverage_complete"], false, "{out}");
    let warning = out["warnings"][0]
        .as_str()
        .unwrap_or_else(|| panic!("a truncated crawl carries a warning: {out}"));
    assert!(warning.contains("PARTIAL"), "{warning}");
    assert!(warning.contains("max_pages_per_host"), "{warning}");
    assert!(warning.contains('3'), "{warning}");
}

#[tokio::test]
async fn edges_written_is_the_store_summary_not_the_row_count() {
    // THE REFUTED BEHAVIOR: the `UpsertSummary` was discarded and
    // `edge_rows.len()` added on `Ok(_)`, so a no-op upsert counted as a write —
    // while `pages`, two hundred lines up the same file, always used the summary.
    //
    // The no-op case is pinned at the sink level (`an_unchanged_edge_upsert_is_
    // not_counted_as_a_write` in lib.rs) because an edge record embeds the
    // producing `job_id`: two crawls are two jobs, so the store genuinely
    // rewrites every edge. What this level proves is that the counters now
    // partition the batch — written + unchanged is what was offered, and the
    // totals come from the store rather than from `edge_rows.len()`.
    let store = TempStore::new("crawl-edges-written").await;
    let out = Crawl
        .run(crawl_ctx(&store, small_site(), base_params()))
        .await
        .unwrap();

    assert_eq!(out["edges_written"], 3, "three fresh edges: {out}");
    assert_eq!(out["edges_unchanged"], 0, "{out}");
    assert_eq!(out["edges_deduped"], 0, "{out}");
    assert_eq!(out["edges_dropped_out_degree"], 0, "{out}");

    // ...and the dataset agrees, which `edge_rows.len()` could not guarantee.
    let edges = store.datasets().list("crawl", "edges", 100).await.unwrap();
    assert_eq!(edges.len(), 3, "{out}");
}

/// The manifest's `output_shape` is the contract a consumer codes against, and
/// it drifted through four milestones because nothing compared it to a real run.
/// An EXPECTED-diff in **both** directions: no promised key may be absent, and
/// no returned key may be unpromised.
#[tokio::test]
async fn output_shape_names_exactly_the_keys_a_real_run_returns() {
    let shape = Crawl
        .manifest()
        .output_shape
        .expect("crawl declares an output shape");
    for invented in ["{pages,", " skipped,", " unchanged,"] {
        assert!(
            !shape.contains(invented),
            "output_shape still promises `{invented}`, which no run emits: {shape}"
        );
    }

    let store = TempStore::new("crawl-output-shape").await;
    let out = Crawl
        .run(crawl_ctx(&store, small_site(), base_params()))
        .await
        .unwrap();

    let promised: BTreeSet<String> = app_crawl::output_shape_keys()
        .into_iter()
        .map(String::from)
        .collect();
    let returned: BTreeSet<String> = result_keys(&out).into_iter().collect();

    let missing: Vec<&String> = promised.difference(&returned).collect();
    assert!(
        missing.is_empty(),
        "output_shape promises keys the result does not carry: {missing:?}"
    );
    let unpromised: Vec<&String> = returned.difference(&promised).collect();
    assert!(
        unpromised.is_empty(),
        "the result carries keys output_shape never named (add them to \
         OUTPUT_SHAPE and to docs/features/crawling.md): {unpromised:?}"
    );
}

#[tokio::test]
async fn a_truncated_run_adds_warnings_on_top_of_the_declared_shape() {
    // `warnings` is deliberately conditional, so it lives in the shape's prose
    // rather than its key block — this pins that it is the ONLY extra key.
    let store = TempStore::new("crawl-shape-warnings").await;
    let mut params = base_params();
    params["max_pages_per_host"] = json!(1);
    let out = Crawl
        .run(crawl_ctx(&store, small_site(), params))
        .await
        .unwrap();

    let promised: BTreeSet<String> = app_crawl::output_shape_keys()
        .into_iter()
        .map(String::from)
        .collect();
    let extra: Vec<String> = result_keys(&out)
        .into_iter()
        .filter(|k| !promised.contains(k))
        .collect();
    assert_eq!(extra, vec!["warnings".to_string()], "{out}");
}

#[tokio::test]
async fn a_revisit_with_no_known_pages_still_returns_the_whole_shape() {
    // The degenerate path (`mode: revisit` over an empty `pages` dataset) makes
    // no fetches at all — the result builder must still be complete, because a
    // consumer reading `coverage_complete` should not have to null-check it.
    let store = TempStore::new("crawl-revisit-empty").await;
    let site = Arc::new(StubSite::new());
    let out = Crawl
        .run(crawl_ctx(
            &store,
            site.clone(),
            json!({ "mode": "revisit", "max_pages": 10 }),
        ))
        .await
        .unwrap();

    assert!(
        site.fetched().is_empty(),
        "nothing to revisit, nothing fetched"
    );
    assert_eq!(out["revisit"], true, "{out}");
    assert_eq!(out["coverage_complete"], true, "{out}");
    let promised: BTreeSet<String> = app_crawl::output_shape_keys()
        .into_iter()
        .map(String::from)
        .collect();
    let returned: BTreeSet<String> = result_keys(&out).into_iter().collect();
    assert_eq!(promised, returned, "{out}");
}
