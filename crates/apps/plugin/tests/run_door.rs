//! The run door, and what a run that produced nothing reports.
//!
//! THE ANTI-PATTERN THESE DEFEND: a plugin job that failed 100% of the time
//! reported SUCCEEDED. The door was `ctx.require_str("plugin")?` — a type check
//! and nothing more — so a typo, an uninstalled build, or
//! `[plugins] enabled = false` produced one `{"error": ..}` per URL, `ran: 0`,
//! zero dataset writes and `Ok(..)`: a green job on `GET /jobs`, a `succeeded`
//! SSE event, a fired result webhook, and an empty dataset. Observatory mode in
//! this same app validated correctly the whole time.

mod common;

use common::{ctx_with, ctx_without_plugins, seed_pages, source_params, Answer, StubPlugins};
use pumper_core::error::PluginFailure;
use pumper_core::testing::TempStore;
use pumper_core::ScrapeApp;
use serde_json::json;

use app_plugin::Plugin;

/// The door refuses a plugin the host cannot execute — and refuses it BEFORE
/// the fan-out, so the run costs nothing. The engines are `Dead`: any fetch is a
/// panic, which is how "no fetch happened" is proved rather than asserted.
#[tokio::test]
async fn an_unknown_plugin_is_refused_before_any_fetch_not_reported_as_a_succeeded_run() {
    let store = TempStore::new("plugin-door-unknown").await;
    let plugins = StubPlugins::new(&["title"], Answer::Echo);
    let ctx = ctx_with(
        &store,
        json!({ "plugin": "titel", "urls": ["http://a/", "http://b/"] }),
        plugins.clone(),
    );

    let err = Plugin
        .run(ctx)
        .await
        .expect_err("an unloadable plugin must not report a succeeded run");
    let msg = err.to_string();
    assert!(msg.contains("titel"), "{msg}");
    assert!(msg.contains("GET /plugins"), "{msg}");
    assert!(msg.contains("title"), "and name what IS runnable: {msg}");
    assert!(
        err.is_terminal_for_job(),
        "a missing module cannot appear between attempts — the ladder buys \
         three identical refusals: {msg}"
    );
    assert_eq!(plugins.calls(), 0, "the module was never reached");
}

/// `[plugins] enabled = false` is the case the old e2e fixture ran under: the
/// context's default `NoPlugins` answers every call `Disabled`. The refusal must
/// name the config flag, not send an operator to read an empty plugin list.
#[tokio::test]
async fn a_disabled_plugin_subsystem_is_refused_at_the_door_not_once_per_document() {
    let store = TempStore::new("plugin-door-disabled").await;
    let ctx = ctx_without_plugins(&store, json!({ "plugin": "noop", "urls": ["http://a/"] }));

    let msg = Plugin
        .run(ctx)
        .await
        .expect_err("a disabled subsystem must not report a succeeded run")
        .to_string();
    assert!(msg.contains("no plugins are loaded"), "{msg}");
    assert!(msg.contains("[plugins] enabled"), "{msg}");
}

/// The door guards source mode too — before the dataset read, not after it.
#[tokio::test]
async fn source_mode_is_refused_at_the_same_door_as_urls_mode() {
    let store = TempStore::new("plugin-door-source").await;
    seed_pages(&store, 3, "<h1>Hi</h1>").await;
    let plugins = StubPlugins::new(&["title"], Answer::Echo);
    let ctx = ctx_with(
        &store,
        source_params(json!({ "plugin": "gone" })),
        plugins.clone(),
    );

    let err = Plugin.run(ctx).await.expect_err("refused");
    assert!(err.is_terminal_for_job());
    assert_eq!(plugins.calls(), 0);
    // Nothing was written, and now nothing claims otherwise.
    assert!(store
        .datasets()
        .list("plugin", "plugin_out", 100)
        .await
        .unwrap()
        .is_empty());
}

/// Observatory mode takes no `plugin` param at all, so the door must not fire
/// on it — it has always run its own (correct) validation.
#[tokio::test]
async fn observatory_mode_is_not_refused_by_the_urls_mode_door() {
    let store = TempStore::new("plugin-door-observatory").await;
    seed_pages(&store, 2, "<h1>Hi</h1>").await;
    let plugins = StubPlugins::new(&["title"], Answer::Echo);
    let out = Plugin
        .run(ctx_with(&store, json!({ "observatory": true }), plugins))
        .await
        .expect("observatory mode audits a plugin LIST, with no `plugin` param");
    assert_eq!(out["mode"], "observatory", "{out}");
}

/// THE headline anti-pattern: every document failed, so the run wrote nothing —
/// and said `Ok`. It now fails, and the failure names the classes.
#[tokio::test]
async fn a_run_where_every_document_failed_fails_the_job_instead_of_succeeding_empty() {
    let store = TempStore::new("plugin-total-failure").await;
    seed_pages(&store, 3, "<h1>Hi</h1>").await;
    let plugins = StubPlugins::new(&["title"], Answer::Always(PluginFailure::Trap));

    let err = Plugin
        .run(ctx_with(&store, source_params(json!({})), plugins.clone()))
        .await
        .expect_err("a 100%-failed run is not a success");
    let msg = err.to_string();
    assert!(msg.contains("all 3 documents failed"), "{msg}");
    assert!(msg.contains("trap=3"), "the classes are named: {msg}");
    assert!(
        !err.is_terminal_for_job(),
        "a trapping plugin may be a transient corpus, so the retries stay"
    );
    assert_eq!(
        plugins.calls(),
        3,
        "the module really did run on every page"
    );
}

/// Partial failure is the common case and must stay a success — failing it would
/// throw away a 2-of-3 run. The classes still travel on the result.
#[tokio::test]
async fn a_partial_failure_succeeds_and_reports_the_failures_by_class() {
    let store = TempStore::new("plugin-partial-failure").await;
    common::seed_page(&store, "http://ok1", "a.html", "<h1>one</h1>").await;
    common::seed_page(&store, "http://bad", "b.html", "<h1>POISON</h1>").await;
    common::seed_page(&store, "http://ok2", "c.html", "<h1>two</h1>").await;
    let plugins = StubPlugins::new(
        &["title"],
        Answer::FailIf("POISON", PluginFailure::MalformedOutput),
    );

    let out = Plugin
        .run(ctx_with(&store, source_params(json!({})), plugins))
        .await
        .expect("two good documents out of three is a successful run");
    assert_eq!(out["loaded"], 3, "{out}");
    assert_eq!(out["ran"], 2, "{out}");
    assert_eq!(out["errors"], 1, "{out}");
    assert_eq!(
        out["errors_by_class"],
        json!({ "malformed_output": 1 }),
        "{out}"
    );
    assert_eq!(out["new"], 2, "{out}");
    // The class rides the echoed record too, so a reader never has to parse prose.
    let records = out["records"].as_array().unwrap();
    let failed = records
        .iter()
        .find(|r| r.get("error").is_some())
        .expect("the failure is echoed");
    assert_eq!(failed["error_class"], "malformed_output", "{failed}");
}

/// A plugin's own `{"error": "no <title> found"}` output is the module saying it
/// could not extract — data, not a host failure. It must not fail the run, must
/// not be counted as an error class, and must be visible as its own count.
#[tokio::test]
async fn a_plugins_own_error_output_does_not_fail_the_run_as_if_the_host_had() {
    let store = TempStore::new("plugin-self-reported").await;
    seed_pages(&store, 2, "<h1>Hi</h1>").await;
    let plugins = StubPlugins::new(&["title"], Answer::SelfReportedError);

    let out = Plugin
        .run(ctx_with(&store, source_params(json!({})), plugins))
        .await
        .expect("the module ran on every page — it just found nothing");
    assert_eq!(out["ran"], 2, "{out}");
    assert_eq!(out["errors"], 0, "{out}");
    assert_eq!(out["errors_by_class"], json!({}), "{out}");
    assert_eq!(out["plugin_reported_errors"], 2, "{out}");
    // Still not written: a record that is nothing but an error message is not a
    // fact about the page.
    assert_eq!(out["new"], 0, "{out}");
}

/// A run with nothing to do is not a failed run. An empty source dataset is a
/// legitimate quiet outcome, and failing it would make every idle scheduled job
/// red.
#[tokio::test]
async fn an_empty_source_is_a_quiet_success_not_a_total_failure() {
    let store = TempStore::new("plugin-empty-source").await;
    let plugins = StubPlugins::echoing();
    let out = Plugin
        .run(ctx_with(&store, source_params(json!({})), plugins))
        .await
        .expect("nothing to run is not a failure");
    assert_eq!(out["requested"], 0, "{out}");
    assert_eq!(out["ran"], 0, "{out}");
    assert_eq!(out["errors"], 0, "{out}");
}

/// The happy path, so the failure tests above have a control: a loaded plugin
/// over stored bodies writes records and reports zero errors.
#[tokio::test]
async fn a_loaded_plugin_over_stored_bodies_writes_records_and_reports_no_errors() {
    let store = TempStore::new("plugin-happy").await;
    seed_pages(&store, 3, "<h1>Hi</h1>").await;
    let out = Plugin
        .run(ctx_with(
            &store,
            source_params(json!({})),
            StubPlugins::echoing(),
        ))
        .await
        .unwrap();
    assert_eq!(out["ran"], 3, "{out}");
    assert_eq!(out["errors"], 0, "{out}");
    assert_eq!(out["new"], 3, "{out}");
    let stored = store
        .datasets()
        .list("plugin", "plugin_out", 100)
        .await
        .unwrap();
    assert_eq!(stored.len(), 3);
    assert!(stored.iter().all(|r| r.data["_url"].is_string()));
}
