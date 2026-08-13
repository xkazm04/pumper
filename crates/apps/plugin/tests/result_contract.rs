//! What a write-mode run **reports**, held against what the manifest promises.
//!
//! THE ANTI-PATTERN THESE DEFEND: `AppManifest.output_shape` declared
//! `{ran, errors, dataset, new, changed, unchanged}` and **no mode emitted
//! `errors` or `dataset`** — three result builders written by hand, each drifting
//! from the manifest independently. An agent reading `GET /apps` (or the MCP tool
//! definitions, which serve the same string) was told to expect fields that never
//! arrived. Meanwhile the `records` echo was every output, unbounded, into the
//! `jobs.result` column / the terminal SSE event / the result webhook / one
//! Tantivy doc per element; and the no-keys sweep silently truncated at 10,000.

mod common;

use common::{ctx_with, seed_page, seed_pages, source_params, Answer, StubPlugins};
use pumper_core::config::ResilienceConfig;
use pumper_core::error::PluginFailure;
use pumper_core::resilience::{Resilience, SourceState};
use pumper_core::testing::{TempStore, TestContext};
use pumper_core::ScrapeApp;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use app_plugin::Plugin;

/// The keys every write mode must carry, whatever mode it is. Kept as one list
/// so a fourth write mode cannot quietly omit half of them — the EXPECTED-diff
/// idiom, applied to a result contract instead of a call-site inventory.
const WRITE_MODE_KEYS: [&str; 9] = [
    "mode",
    "plugin",
    "dataset",
    "ran",
    "errors",
    "errors_by_class",
    "plugin_reported_errors",
    "new",
    "changed",
];

fn assert_write_mode_contract(out: &Value) {
    let missing: Vec<&str> = WRITE_MODE_KEYS
        .iter()
        .copied()
        .filter(|k| out.get(k).is_none())
        .collect();
    assert!(
        missing.is_empty(),
        "the {} mode result omits manifest keys {missing:?}: {out}",
        out["mode"]
    );
}

/// Every write mode agrees with `output_shape` — including the two keys the
/// manifest promised and none of them emitted.
#[tokio::test]
async fn all_three_write_modes_emit_the_keys_the_manifest_declares() {
    let store = TempStore::new("plugin-contract-modes").await;
    seed_pages(&store, 2, "<h1>Hi</h1>").await;

    // urls
    let out = common::run_urls_mode(
        &store,
        json!({ "plugin": "title", "urls": ["http://a/", "http://b/"], "dataset": "plugin_out" }),
        StubPlugins::echoing(),
    )
    .await;
    assert_eq!(out["mode"], "urls");
    assert_write_mode_contract(&out);
    assert_eq!(out["dataset"], "plugin_out", "{out}");
    assert_eq!(out["ran"], 2, "{out}");
    assert_eq!(out["records_total"], 2, "{out}");

    // source
    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(json!({})),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["mode"], "source");
    assert_write_mode_contract(&out);
    assert_eq!(out["dataset"], "plugin_out", "{out}");

    // backfill
    let crawl_job = Uuid::new_v4().to_string();
    let dir = store.path().join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("v1.html"), b"<h1>v1</h1>")
        .await
        .unwrap();
    store
        .datasets()
        .upsert_many(
            "crawl",
            "page_versions",
            &[(
                "http://p#1".into(),
                json!({"url": "http://p", "revision": 1, "artifact_path": "v1.html",
                       "job_id": crawl_job, "fetched_at": "2026-01-05T00:00:00+00:00"}),
            )],
        )
        .await
        .unwrap();
    let out = Plugin
        .run(ctx_with(
            &store,
            json!({
                "plugin": "title",
                "source": { "app": "crawl", "dataset": "pages", "backfill": true },
                "dataset": "title_history"
            }),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["mode"], "backfill");
    assert_write_mode_contract(&out);
    assert_eq!(out["dataset"], "title_history", "{out}");
    assert!(
        out.get("records").is_none(),
        "backfill never echoes records: {out}"
    );
}

/// A diverted run says where its rows really went. Before this, a quarantined
/// source's run looked identical to a normal one and the reader went looking in
/// the wrong table.
#[tokio::test]
async fn a_quarantined_run_names_the_shadow_dataset_it_was_diverted_to() {
    let store = TempStore::new("plugin-contract-quarantine").await;
    seed_pages(&store, 2, "<h1>Hi</h1>").await;
    let health = Arc::new(Resilience::new(
        store.storage.pool(),
        &ResilienceConfig {
            enforce: true,
            ..ResilienceConfig::default()
        },
    ));
    let store_h = health.store().expect("resilience store");
    store_h.ensure_source("plugin", "plugin_out").await.unwrap();
    store_h
        .set_state_manual("plugin/plugin_out", SourceState::Quarantined, "test")
        .await
        .unwrap();

    let mut ctx = TestContext::new(&store.storage, "plugin")
        .params(source_params(json!({})))
        .health(Arc::clone(&health))
        .artifacts_dir(store.path().join("plugin").join("job"))
        .build();
    ctx.plugins = StubPlugins::echoing();
    let out = Plugin.run(ctx).await.unwrap();

    assert_eq!(out["dataset"], "plugin_out@q", "{out}");
    assert!(store
        .datasets()
        .get("plugin", "plugin_out@q", "http://p0")
        .await
        .unwrap()
        .is_some());
    assert!(store
        .datasets()
        .get("plugin", "plugin_out", "http://p0")
        .await
        .unwrap()
        .is_none());
}

/// The echo is a SAMPLE. The dataset is the record of truth; the result is what
/// gets stored forever in a `jobs` row and re-sent on every webhook replay.
#[tokio::test]
async fn the_records_echo_is_a_bounded_prefix_not_the_whole_corpus() {
    let store = TempStore::new("plugin-contract-echo").await;
    seed_pages(&store, 5, "<h1>Hi</h1>").await;

    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(json!({ "records_echo": 2 })),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["loaded"], 5, "{out}");
    assert_eq!(out["records"].as_array().unwrap().len(), 2, "{out}");
    assert_eq!(out["records_total"], 5, "the honest total travels with it");
    assert_eq!(out["records_truncated"], true, "{out}");
    assert_eq!(out["new"], 5, "all five were still WRITTEN: {out}");

    // Under the bound, nothing is claimed to be missing.
    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(json!({})),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["records"].as_array().unwrap().len(), 5, "{out}");
    assert_eq!(out["records_truncated"], false, "{out}");

    // `0` = counts only, for a caller that reads from the dataset.
    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(json!({ "records_echo": 0 })),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["records"].as_array().unwrap().len(), 0, "{out}");
    assert_eq!(out["records_total"], 5, "{out}");
    assert_eq!(out["records_truncated"], true, "{out}");
}

/// THE ANTI-PATTERN: a capped sweep reported `requested: <the cap>` — a number
/// indistinguishable from a dataset that really holds that many rows. Judged on
/// the page the store returned, before the removed/gone filter.
#[tokio::test]
async fn a_capped_sweep_says_so_instead_of_looking_like_a_complete_run() {
    let store = TempStore::new("plugin-contract-sweep").await;
    seed_pages(&store, 5, "<h1>Hi</h1>").await;

    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(json!({ "source": { "app": "crawl", "dataset": "pages", "limit": 3 } })),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["limit"], 3, "{out}");
    assert_eq!(out["requested"], 3, "{out}");
    assert_eq!(out["truncated"], true, "the cap decided where it stopped");

    // The whole dataset fits: not truncated, and `limit` still reported so the
    // reader never has to guess which cap was in force.
    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(json!({})),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["limit"], 10_000, "{out}");
    assert_eq!(out["truncated"], false, "{out}");

    // A caller-named key set has no cap applied to it, so it is never truncated.
    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(
                json!({ "source": { "app": "crawl", "dataset": "pages", "keys": ["http://p0"] } }),
            ),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["requested"], 1, "{out}");
    assert_eq!(out["truncated"], false, "{out}");
}

/// What `/economics` will make of this result, pinned — the part a reader of the
/// code cannot see.
///
/// **A correction to the premise this was written from:** adding a `dataset`
/// field does NOT re-attribute the yield. `extract_yields` keys a `YieldEntry`
/// on the JSON *path* where it finds `new`/`changed` (documented on
/// `YieldEntry::dataset`), so a root-level summary is `""` whatever fields sit
/// next to it — the extractor, which has shipped a root `dataset` field since
/// r12, is attributed the same way. Re-keying would mean nesting the summary
/// under a dataset-named object, and `walk_yields` keeps walking below a match,
/// so a run would then report its counts TWICE. What is pinned here is
/// therefore: exactly one entry, the right counts, and no double-count.
#[tokio::test]
async fn the_run_yields_exactly_one_summary_and_never_double_counts_it() {
    let store = TempStore::new("plugin-contract-yield").await;
    seed_pages(&store, 3, "<h1>Hi</h1>").await;
    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(json!({})),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();

    let yields = pumper_core::extract_yields(&out);
    assert_eq!(
        yields.len(),
        1,
        "one run, one yield entry — the nested `source`/`cost` blocks carry no \
         counts and the `records` array is never descended: {yields:?}"
    );
    assert_eq!(yields[0].dataset, "", "the ROOT path, not the dataset name");
    assert_eq!(yields[0].new, Some(3));
    assert_eq!(yields[0].changed, Some(0));
    assert_eq!(yields[0].unchanged, Some(0));
    // …while the result itself does name the dataset, which is what a human or
    // agent reading `GET /jobs/{id}` needs.
    assert_eq!(out["dataset"], "plugin_out", "{out}");
}

/// THE UNPAIRING THIS FORBIDS: bounding the `records` echo without declaring
/// `index_datasets` silently shrinks search coverage to the first N outputs of
/// every run — the worker mints one document per element of the echo, and that
/// was this app's only per-record coverage. The two must ship together (the
/// extractor learned this in r12; this app forked away before it).
///
/// The spec's exact shape is what the worker parses (`spec.app` / `spec.dataset`
/// in `dataset_search_docs`) and its mere presence is what makes
/// `echo_indexing_delegated` skip the echo, so the first N records are not also
/// indexed under a second, divergent `<app>:<url>` id that nothing would ever
/// update or delete.
#[tokio::test]
async fn every_write_mode_delegates_indexing_to_the_dataset_not_the_bounded_echo() {
    let store = TempStore::new("plugin-contract-index").await;
    seed_pages(&store, 5, "<h1>Hi</h1>").await;

    // urls — into its own dataset, so the spec is proved to track the dataset
    // actually requested rather than a constant, and so the change-feed
    // assertion below is unambiguously about the source-mode run.
    let out = common::run_urls_mode(
        &store,
        json!({ "plugin": "title", "urls": ["http://a/"], "dataset": "urls_out" }),
        StubPlugins::echoing(),
    )
    .await;
    assert_eq!(
        out["index_datasets"],
        json!([{ "app": "plugin", "dataset": "urls_out" }]),
        "{out}"
    );

    // source, with the echo capped well below the corpus
    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(json!({ "records_echo": 1 })),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["records"].as_array().unwrap().len(), 1, "{out}");
    assert_eq!(
        out["index_datasets"],
        json!([{ "app": "plugin", "dataset": "plugin_out" }]),
        "{out}"
    );

    // What `dataset_search_docs` will find in the change feed it reads: one
    // indexable revision per record WRITTEN — five, not the one echoed.
    let revs = store
        .datasets()
        .changes_since("plugin", Some("plugin_out"), None, 1000, None)
        .await
        .unwrap();
    assert_eq!(
        revs.len(),
        5,
        "the change feed carries every written record"
    );
    assert!(
        revs.iter()
            .all(|r| r.data.is_some() && r.change != "removed"),
        "every revision carries the snapshot the indexer needs"
    );

    // backfill — it echoes NO records, so before the declaration its output had
    // no per-record search coverage at all, only one whole-result document.
    let crawl_job = Uuid::new_v4().to_string();
    let dir = store.path().join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("v1.html"), b"<h1>v1</h1>")
        .await
        .unwrap();
    store
        .datasets()
        .upsert_many(
            "crawl",
            "page_versions",
            &[(
                "http://p#1".into(),
                json!({"url": "http://p", "revision": 1, "artifact_path": "v1.html",
                       "job_id": crawl_job, "fetched_at": "2026-01-05T00:00:00+00:00"}),
            )],
        )
        .await
        .unwrap();
    let out = Plugin
        .run(ctx_with(
            &store,
            json!({
                "plugin": "title",
                "source": { "app": "crawl", "dataset": "pages", "backfill": true },
                "dataset": "title_history"
            }),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert!(out.get("records").is_none(), "backfill never echoes: {out}");
    assert_eq!(
        out["index_datasets"],
        json!([{ "app": "plugin", "dataset": "title_history" }]),
        "…and therefore needs the delegation more than anyone: {out}"
    );
}

/// A quarantined source must not offer its rows to the index that saved-search
/// alerts fire from. Withheld by the PRODUCER: the worker's own gate reads the
/// health of the spec's pair, and `("plugin", "plugin_out@q")` is a pair no
/// `observe_extraction` ever judges, so it would always read Healthy.
#[tokio::test]
async fn a_quarantined_run_withholds_the_index_declaration_it_would_otherwise_make() {
    let store = TempStore::new("plugin-contract-index-q").await;
    seed_pages(&store, 2, "<h1>Hi</h1>").await;
    let health = Arc::new(Resilience::new(
        store.storage.pool(),
        &ResilienceConfig {
            enforce: true,
            ..ResilienceConfig::default()
        },
    ));
    let store_h = health.store().expect("resilience store");
    store_h.ensure_source("plugin", "plugin_out").await.unwrap();
    store_h
        .set_state_manual("plugin/plugin_out", SourceState::Quarantined, "test")
        .await
        .unwrap();

    let mut ctx = TestContext::new(&store.storage, "plugin")
        .params(source_params(json!({})))
        .health(Arc::clone(&health))
        .artifacts_dir(store.path().join("plugin").join("job"))
        .build();
    ctx.plugins = StubPlugins::echoing();
    let out = Plugin.run(ctx).await.unwrap();

    assert_eq!(out["dataset"], "plugin_out@q", "{out}");
    assert!(
        out.get("index_datasets").is_none(),
        "a quarantined source must not offer its rows to the index: {out}"
    );
}

/// The two counts that decide what becomes data, exercised through a real run:
/// a returned-but-empty-ish output is still a run, and only real extractions
/// reach the dataset.
#[tokio::test]
async fn ran_counts_calls_that_returned_while_only_extractions_become_records() {
    let store = TempStore::new("plugin-contract-ran").await;
    seed_page(&store, "http://ok", "a.html", "<h1>one</h1>").await;
    seed_page(&store, "http://bad", "b.html", "<h1>POISON</h1>").await;
    let plugins = StubPlugins::new(&["title"], Answer::FailIf("POISON", PluginFailure::Trap));

    let out = Plugin
        .run(ctx_with(&store, source_params(json!({})), plugins))
        .await
        .unwrap();
    assert_eq!(out["loaded"], 2, "{out}");
    assert_eq!(out["ran"], 1, "one call returned: {out}");
    assert_eq!(out["errors"], 1, "{out}");
    assert_eq!(out["new"], 1, "{out}");
    assert_eq!(
        out["records_total"], 2,
        "the echo covers BOTH outcomes: {out}"
    );

    let stored = store
        .datasets()
        .list("plugin", "plugin_out", 100)
        .await
        .unwrap();
    assert_eq!(stored.len(), 1, "the trapped page is not a record");
    assert_eq!(stored[0].key, "http://ok");
}
