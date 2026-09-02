//! `mode: "graph"` at the `run()` level (N27): the whole-corpus consumers the
//! `edges` dataset never had, driven through the real app against a real store.
//!
//! What this level proves that the module's unit tests cannot:
//!
//! 1. a graph run reads the edges a REAL crawl wrote and turns them into
//!    `page_rank` records keyed by URL;
//! 2. re-running over an unchanged graph reports `unchanged` rather than
//!    rewriting every record — the `DerivedPaths` declaration on `run_at`;
//! 3. a hub that loses 40% of its out-links produces exactly one
//!    `structure_changes` record, the signal neither simhash nor the health
//!    detector carries (edges are upsert-only: an absent edge is not removed);
//! 4. the passes are checkpointed one per iteration, and a restored checkpoint
//!    resumes rather than restarting;
//! 5. `graph_output_shape_keys()` names exactly what a graph run returns.

mod common;

use app_crawl::graph::{self, PAGE_RANK_DATASET, STRUCTURE_CHANGES_DATASET};
use app_crawl::Crawl;
use common::{crawl_ctx, graph_ctx, graph_ctx_resumable, result_keys, StubSite};
use pumper_core::testing::{RecordingCheckpoints, TempStore};
use pumper_core::ScrapeApp;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;

/// A hub page linking to five leaves, each of which links back to the hub.
fn hub_site(hub_links: &[&str]) -> Arc<StubSite> {
    let mut site = StubSite::new().page("https://example.com/", hub_links);
    for leaf in ["a", "b", "c", "d", "e"] {
        site = site.page(
            &format!("https://example.com/{leaf}"),
            &["https://example.com/"],
        );
    }
    Arc::new(site)
}

fn all_leaves() -> Vec<&'static str> {
    vec![
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
        "https://example.com/d",
        "https://example.com/e",
    ]
}

fn crawl_params() -> Value {
    json!({
        "seeds": ["https://example.com/"],
        "max_pages": 50,
        "max_depth": 2,
        "concurrency": 1,
        "dedup_distance": 0,
        "respect_robots": false,
    })
}

/// Crawls the site so the `edges` dataset exists, then answers with the store.
async fn crawled(store: &TempStore, site: Arc<StubSite>) {
    Crawl
        .run(crawl_ctx(store, site, crawl_params()))
        .await
        .expect("the crawl runs");
}

async fn run_graph(store: &TempStore, params: Value) -> Value {
    Crawl
        .run(graph_ctx(store, params))
        .await
        .expect("the graph run succeeds")
}

#[tokio::test]
async fn a_graph_run_ranks_the_whole_corpus_the_crawl_persisted() {
    // THE REFUTED BEHAVIOR: the crawl streamed a complete link graph into
    // `edges` and NOTHING read it — ranking was a within-run top-10 that froze
    // at 200k tracked edges, and the feature doc said a whole-corpus ranking
    // "would have to be computed from the `edges` dataset". This is that
    // computation.
    let store = TempStore::new("graph-ranks-corpus").await;
    crawled(&store, hub_site(&all_leaves())).await;

    let out = run_graph(&store, json!({"mode": "graph"})).await;
    assert_eq!(out["mode"], "graph", "{out}");
    assert_eq!(out["nodes"], 6, "the hub plus five leaves: {out}");
    assert_eq!(out["edges_scanned"], 10, "5 out + 5 back: {out}");
    assert_eq!(out["graph_complete"], true, "{out}");
    assert_eq!(out["iterations"], graph::DEFAULT_ITERATIONS, "{out}");
    assert_eq!(out["resumed"], false, "{out}");
    assert_eq!(out["page_rank_new"], 6, "{out}");
    assert_eq!(
        out["structure_baseline"], false,
        "the first graph run has nothing to compare against: {out}"
    );
    assert_eq!(out["structure_changes_written"], 0, "{out}");

    // The dataset is keyed by URL and the hub — linked from every leaf —
    // outranks each of them.
    let ranked = store
        .datasets()
        .list("crawl", PAGE_RANK_DATASET, 100)
        .await
        .unwrap();
    assert_eq!(ranked.len(), 6);
    let by_url: std::collections::HashMap<String, Value> = ranked
        .into_iter()
        .map(|r| (r.key.clone(), r.data.clone()))
        .collect();
    let hub = &by_url["https://example.com/"];
    assert_eq!(hub["in_degree"], 5, "{hub}");
    assert_eq!(hub["out_degree"], 5, "{hub}");
    let hub_rank = hub["rank"].as_f64().unwrap();
    let leaf_rank = by_url["https://example.com/a"]["rank"].as_f64().unwrap();
    assert!(
        hub_rank > leaf_rank,
        "the hub must outrank a leaf: {hub_rank} vs {leaf_rank}"
    );
    let sum: f64 = by_url.values().map(|v| v["rank"].as_f64().unwrap()).sum();
    assert!((sum - 1.0).abs() < 1e-6, "ranks sum to {sum}, not 1");
}

#[tokio::test]
async fn an_unchanged_graph_reports_unchanged_instead_of_rewriting_every_record() {
    // `run_at` moves on every run by construction. Without declaring it a
    // DERIVED path, a nightly graph job would mark all six records `changed`
    // every night — and every watch, trigger and webhook on `page_rank` would
    // fire on a corpus that did not move.
    let store = TempStore::new("graph-unchanged").await;
    crawled(&store, hub_site(&all_leaves())).await;

    let first = run_graph(&store, json!({"mode": "graph"})).await;
    assert_eq!(first["page_rank_new"], 6, "{first}");

    let second = run_graph(&store, json!({"mode": "graph"})).await;
    assert_eq!(second["page_rank_new"], 0, "{second}");
    assert_eq!(
        second["page_rank_changed"], 0,
        "an unchanged graph must not rewrite its ranking: {second}"
    );
    assert_eq!(second["page_rank_unchanged"], 6, "{second}");
    assert_eq!(
        second["structure_baseline"], true,
        "the second run HAS a previous rollup to compare against: {second}"
    );
    assert_eq!(second["structure_changes_written"], 0, "{second}");
}

#[tokio::test]
async fn a_hub_losing_forty_percent_of_its_out_links_writes_one_structure_change() {
    // Edges are upsert-only — "an edge absent this run is NOT removed" — so a
    // hub that drops two of its five out-links looks *unchanged* to every other
    // detector: simhash sees one different page, the health detector sees
    // nothing, and the `edges` dataset still carries the vanished rows. The
    // graph rollup is where the site map itself is diffed.
    let store = TempStore::new("graph-structure-change").await;
    crawled(&store, hub_site(&all_leaves())).await;
    run_graph(&store, json!({"mode": "graph"})).await;

    // The site restructures: the hub keeps three of its five links.
    let shrunk = &all_leaves()[..3];
    crawled(&store, hub_site(shrunk)).await;

    let out = run_graph(&store, json!({"mode": "graph"})).await;
    assert_eq!(
        out["structure_changes_written"], 1,
        "exactly one hub shrank: {out}"
    );

    let changes = store
        .datasets()
        .list("crawl", STRUCTURE_CHANGES_DATASET, 100)
        .await
        .unwrap();
    assert_eq!(changes.len(), 1, "{changes:?}");
    let rec = &changes[0].data;
    assert_eq!(rec["url"], "https://example.com/", "{rec}");
    assert_eq!(rec["previous_out_degree"], 5, "{rec}");
    assert_eq!(rec["out_degree"], 3, "{rec}");
    assert_eq!(rec["links_removed"], 2, "{rec}");
    assert_eq!(rec["dropped_fraction"], 0.4, "{rec}");
    assert_ne!(rec["previous_digest"], rec["digest"], "{rec}");
}

#[tokio::test]
async fn a_re_pointed_hub_of_the_same_size_is_not_reported_as_drift() {
    // Precision over recall: the signal is "the site map SHRANK", not "an
    // anchor moved". A hub that swaps one target for another has the same
    // out-degree and must not manufacture a drift record — or the dataset
    // becomes a change log and nobody reads it.
    let store = TempStore::new("graph-structure-repoint").await;
    crawled(&store, hub_site(&all_leaves())).await;
    run_graph(&store, json!({"mode": "graph"})).await;

    let repointed = [
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
        "https://example.com/d",
        "https://example.com/z",
    ];
    crawled(&store, hub_site(&repointed)).await;

    let out = run_graph(&store, json!({"mode": "graph"})).await;
    assert_eq!(out["structure_changes_written"], 0, "{out}");
    assert!(
        store
            .datasets()
            .list("crawl", STRUCTURE_CHANGES_DATASET, 100)
            .await
            .unwrap()
            .is_empty(),
        "a same-size re-point is not a structural loss"
    );
}

#[tokio::test]
async fn the_passes_are_checkpointed_one_per_iteration_and_resume() {
    let store = TempStore::new("graph-checkpoint").await;
    crawled(&store, hub_site(&all_leaves())).await;

    let sink = Arc::new(RecordingCheckpoints::new());
    let out = Crawl
        .run(graph_ctx_resumable(
            &store,
            json!({"mode": "graph", "iterations": 4}),
            sink.clone(),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(out["iterations"], 4, "{out}");
    assert_eq!(
        sink.save_count(),
        4,
        "one checkpoint per PASS is the whole resumability claim"
    );
    let last = sink.last_state().expect("a final state");
    assert_eq!(last["iteration"], 4, "{last}");
    assert_eq!(last["ranks"].as_object().unwrap().len(), 6, "{last}");

    // Resumed at pass 4 of 4: nothing left to iterate, and the run says it
    // resumed rather than silently redoing the work.
    let resume_sink = Arc::new(RecordingCheckpoints::new());
    let resumed = Crawl
        .run(graph_ctx_resumable(
            &store,
            json!({"mode": "graph", "iterations": 4}),
            resume_sink.clone(),
            Some(last),
        ))
        .await
        .unwrap();
    assert_eq!(resumed["resumed"], true, "{resumed}");
    assert_eq!(resumed["iterations"], 4, "{resumed}");
    assert_eq!(
        resume_sink.save_count(),
        0,
        "a fully-iterated checkpoint pays for no further passes"
    );
}

#[tokio::test]
async fn an_empty_corpus_graph_run_is_a_clean_no_op_not_a_crash() {
    let store = TempStore::new("graph-empty").await;
    let out = run_graph(&store, json!({"mode": "graph"})).await;
    assert_eq!(out["nodes"], 0, "{out}");
    assert_eq!(out["edges_scanned"], 0, "{out}");
    assert_eq!(out["iterations"], 0, "{out}");
    assert_eq!(out["page_rank_new"], 0, "{out}");
    assert_eq!(out["graph_complete"], true, "{out}");
    // ...and the shape is still complete, so a consumer never null-checks it.
    let promised: BTreeSet<String> = app_crawl::graph_output_shape_keys()
        .into_iter()
        .map(String::from)
        .collect();
    assert_eq!(promised, result_keys(&out).into_iter().collect(), "{out}");
}

#[tokio::test]
async fn graph_output_shape_names_exactly_the_keys_a_real_graph_run_returns() {
    // The same two-way EXPECTED diff the crawl shape gets: `mode: "graph"`
    // returns a whole different result, and a shape nobody diffs is a shape
    // that drifts.
    let store = TempStore::new("graph-output-shape").await;
    crawled(&store, hub_site(&all_leaves())).await;
    let out = run_graph(&store, json!({"mode": "graph"})).await;

    let promised: BTreeSet<String> = app_crawl::graph_output_shape_keys()
        .into_iter()
        .map(String::from)
        .collect();
    let returned: BTreeSet<String> = result_keys(&out).into_iter().collect();
    let missing: Vec<&String> = promised.difference(&returned).collect();
    assert!(
        missing.is_empty(),
        "GRAPH_OUTPUT_SHAPE promises keys the result does not carry: {missing:?}"
    );
    let unpromised: Vec<&String> = returned.difference(&promised).collect();
    assert!(
        unpromised.is_empty(),
        "the graph result carries keys GRAPH_OUTPUT_SHAPE never named (add them there \
         and to docs/features/crawling.md): {unpromised:?}"
    );
    // And the manifest's `mode` enum admits it, or nobody can call it.
    let schema = Crawl.manifest().params_schema.expect("a params schema");
    let modes = schema["properties"]["mode"]["enum"].clone();
    assert_eq!(modes, json!(["revisit", "graph"]), "{modes}");
}
