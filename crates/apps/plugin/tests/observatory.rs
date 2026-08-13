//! The observatory's headline promise, held to a **behavioural** test.
//!
//! `observatory.rs` sells the feature as "change detection + triggers on that
//! dataset surface extraction rot for free". It was structurally incapable of
//! it: every row embedded `run_at`, `avg_elapsed_ms`, the fuel/memory figures,
//! `drift_score` and `prev_run_at`, and was written through a plain
//! `upsert_many` — while change detection hashes the whole canonical value. So
//! **every row was `changed` on every run**, `unchanged` was structurally always
//! 0, a watch on `plugin/observatory` fired on 100% of its rows every run, and
//! the drift signal the feature exists to raise was buried in universal noise.
//! (`lib.rs` documents that exact anti-pattern as the reason cost lives on the
//! job result. This file committed it anyway.)
//!
//! "The fields are marked derived" is not evidence. What is evidence is a second
//! run over an unchanged corpus reporting its rows unchanged, which is what
//! these tests assert.

mod common;

use common::{ctx_with, seed_page, seed_site_pages, Answer, StubPlugins};
use pumper_core::testing::TempStore;
use pumper_core::ScrapeApp;
use serde_json::{json, Value};

use app_plugin::Plugin;

/// THE criterion. Two runs, same corpus, same plugin behaviour → the second run
/// reports the rows unchanged.
#[tokio::test]
async fn a_rerun_over_an_unchanged_corpus_reports_unchanged_not_every_row_changed() {
    let store = TempStore::new("observatory-quiet-rerun").await;
    seed_site_pages(&store, 6, "<h1>Hi</h1>").await;

    let first = Plugin
        .run(ctx_with(
            &store,
            json!({ "observatory": true }),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(first["rows"], 1, "one (plugin, site) row: {first}");
    assert_eq!(first["new"], 1, "{first}");

    let second = Plugin
        .run(ctx_with(
            &store,
            json!({ "observatory": true }),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(
        second["changed"], 0,
        "a re-run over an unchanged corpus is not news: {second}"
    );
    assert_eq!(second["unchanged"], 1, "{second}");
    assert_eq!(second["new"], 0, "{second}");

    // The row still carries the CURRENT measurements — this is a
    // change-detection seam, not a projection — and no second revision was
    // appended for the derived-only movement.
    let row = store
        .datasets()
        .get("plugin", "observatory", "title|site.test")
        .await
        .unwrap()
        .expect("the row exists under the historic, unsuffixed key");
    assert!(row.data["run_at"].is_string(), "{}", row.data);
    assert_eq!(row.data["drift_score"], 0.0, "{}", row.data);
    assert_eq!(
        store
            .datasets()
            .history("plugin", "observatory", "title|site.test", 10)
            .await
            .unwrap()
            .len(),
        1,
        "no second revision for a measurement-only movement"
    );
}

/// The other half of the seam: a REAL behaviour change must still fire. Deriving
/// the telemetry is only safe if the findings stay in the identity.
#[tokio::test]
async fn a_real_behaviour_change_still_marks_the_row_changed() {
    let store = TempStore::new("observatory-real-change").await;
    seed_site_pages(&store, 6, "<h1>Hi</h1>").await;

    Plugin
        .run(ctx_with(
            &store,
            json!({ "observatory": true }),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();

    // Same corpus, same key — but the plugin now traps on every page.
    let broken = StubPlugins::new(
        &["title"],
        Answer::Always(pumper_core::error::PluginFailure::Trap),
    );
    let out = Plugin
        .run(ctx_with(&store, json!({ "observatory": true }), broken))
        .await
        .unwrap();
    assert_eq!(out["changed"], 1, "extraction rot must surface: {out}");
    let row = store
        .datasets()
        .get("plugin", "observatory", "title|site.test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.data["rates"]["trap"], 1.0, "{}", row.data);
    assert!(
        row.data["drift_score"].as_f64().unwrap() > 0.9,
        "the drift score is the point: {}",
        row.data
    );
}

/// THE anti-pattern: every plugin was replayed with `params: null`, though
/// `plugin_params` is this app's flagship feature. A module that only produces
/// output under a configuration was `Empty` at every site forever — and because
/// the rate never *rose*, `empty_rate_rising` never flagged it while the row
/// read `low_confidence: false` and looked authoritative.
#[tokio::test]
async fn a_configured_plugin_is_replayed_with_its_params_not_with_null() {
    let store = TempStore::new("observatory-params").await;
    seed_site_pages(&store, 6, "<h1>Hi</h1>").await;

    // Unconfigured: the module genuinely produces nothing.
    let bare = StubPlugins::new(&["title"], Answer::OnlyWithTag);
    let out = Plugin
        .run(ctx_with(
            &store,
            json!({ "observatory": true }),
            bare.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(
        out["rows"], 1,
        "control: the unconfigured audit still writes a row: {out}"
    );
    assert!(bare.seen_params().iter().all(Value::is_null));
    let row = store
        .datasets()
        .get("plugin", "observatory", "title|site.test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.data["rates"]["empty"], 1.0, "{}", row.data);

    // Configured through the job-level envelope: the module works, and the
    // audit says so.
    let configured = StubPlugins::new(&["title"], Answer::OnlyWithTag);
    let out = Plugin
        .run(ctx_with(
            &store,
            json!({ "observatory": { "plugins": ["title"] }, "plugin_params": { "tag": "h2" } }),
            configured.clone(),
        ))
        .await
        .unwrap();
    assert!(
        configured
            .seen_params()
            .iter()
            .all(|p| p == &json!({ "tag": "h2" })),
        "every replay must carry the configuration: {:?}",
        configured.seen_params()
    );
    assert_eq!(out["rows"], 1, "{out}");

    // …and it is a DIFFERENT row, so the configured audit did not overwrite the
    // unconfigured one's drift history.
    let rows = store
        .datasets()
        .list("plugin", "observatory", 100)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "two configurations, two rows: {rows:?}");
    let configured_row = rows
        .iter()
        .find(|r| r.key != "title|site.test")
        .expect("the configured row is keyed apart");
    assert!(
        configured_row.key.starts_with("title@") && configured_row.key.ends_with("|site.test"),
        "{}",
        configured_row.key
    );
    assert_eq!(
        configured_row.data["rates"]["ok"], 1.0,
        "{}",
        configured_row.data
    );
    assert_eq!(configured_row.data["params"], json!({ "tag": "h2" }));
}

/// THE anti-pattern: a zero-byte stored artifact short-circuited without
/// calling the plugin and was bucketed `Empty` — the plugin's bucket. A crawl
/// that stored empty bodies therefore inflated the site's empty rate, could trip
/// `empty_rate_rising`, and inflated `drift_score`: a false positive on the very
/// canary this feature exists to raise, attributed to the plugin rather than the
/// corpus. It also counted in `pages_replayed`, though nothing was replayed.
#[tokio::test]
async fn an_empty_stored_body_is_reported_as_a_corpus_fact_not_as_a_plugin_miss() {
    let store = TempStore::new("observatory-empty-artifact").await;
    seed_page(&store, "http://s/a", "a.html", "<h1>real</h1>").await;
    seed_page(&store, "http://s/b", "b.html", "").await;

    let out = Plugin
        .run(ctx_with(
            &store,
            json!({ "observatory": true }),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();

    assert_eq!(out["pages_replayed"], 1, "one body actually ran: {out}");
    assert_eq!(out["pages_empty"], 1, "{out}");
    assert_eq!(out["pages_unreadable"], 0, "{out}");

    let row = store
        .datasets()
        .get("plugin", "observatory", "title|s")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.data["sampled"], 2,
        "both pages were sampled: {}",
        row.data
    );
    assert_eq!(row.data["classified"], 1, "only one reached the plugin");
    assert_eq!(row.data["empty_artifacts"], 1, "{}", row.data);
    assert_eq!(
        row.data["rates"]["empty"], 0.0,
        "the corpus's empty body is not the plugin's empty rate: {}",
        row.data
    );
    assert_eq!(row.data["rates"]["ok"], 1.0, "{}", row.data);
}
