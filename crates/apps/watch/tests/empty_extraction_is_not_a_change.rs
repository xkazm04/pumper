//! `run()`-level proof that **an empty extraction is not a page change.**
//!
//! `watch` is the fleet's Visualping: its entire product is "tell me when this
//! page changed". Before the guard it fingerprinted whatever the ladder returned
//! with no emptiness check, so an interstitial, a transient render failure or an
//! empty 200 stored `{chars: 0, content_sha256: e3b0c442…, excerpt: ""}` as a
//! **changed** revision — every subscribed webhook fired "the entire page
//! vanished", and the next healthy run fired a second, equally false alarm.
//!
//! The acceptance is the ABSENT ALERT — no `pages` record, no revision — not the
//! returned `Err`. Every test is named after the anti-pattern it defends.

use std::collections::HashMap;
use std::sync::Arc;

use app_watch::Watch;
use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
use pumper_core::{Datasets, HttpClient, HttpRequest, HttpResponse, Result, ScrapeApp};
use serde_json::{json, Value};

const URL: &str = "https://example.com/releases";

/// Serves one canned body at any URL.
struct StubPage(String);

#[async_trait]
impl HttpClient for StubPage {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::from([("content-type".into(), "text/html".into())]),
            body: self.0.clone(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// One `run()` against a canned body. `strategy: "http"` pins the ladder to the
/// stub tier — the browser and researcher stay [`Dead`], so an escalation would
/// panic rather than silently rescue the empty body under test.
async fn run_page(store: &TempStore, body: &str) -> Result<Value> {
    let ctx = TestContext::new(&store.storage, "watch")
        .params(json!({ "url": URL, "strategy": "http" }))
        .engines(engines_with(
            Arc::new(StubPage(body.to_string())),
            Arc::new(Dead),
            Arc::new(Dead),
        ))
        .build();
    Watch.run(ctx).await
}

async fn revisions(datasets: &Datasets) -> Vec<String> {
    datasets
        .history("watch", "pages", URL, 50)
        .await
        .expect("history")
        .into_iter()
        .map(|r| r.change)
        .collect()
}

const REAL_PAGE: &str =
    "<html><body><h1>Release Notes</h1><p>v2.1 shipped today.</p></body></html>";

/// **The direction's headline case.** A healthy run records the page; the next
/// fetch comes back empty. Against the unguarded code the empty body upserts as
/// `changed` and every webhook fires — here nothing is written at all.
#[tokio::test]
async fn an_empty_extraction_does_not_append_a_changed_revision() {
    let store = TempStore::new("watch-empty").await;
    let datasets = store.datasets();

    let first = run_page(&store, REAL_PAGE).await.expect("healthy run");
    assert_eq!(first["change"], "new");
    assert_eq!(revisions(&datasets).await, ["new"], "the page was recorded");

    // The ladder hands back an empty 200 — an interstitial, a JS-only shell, a
    // transient render failure. Escalation is best-effort, so this happens.
    let err = run_page(&store, "   \n\t  \n")
        .await
        .expect_err("an empty extraction is a failed run, not an empty success");

    assert_eq!(
        revisions(&datasets).await,
        ["new"],
        "no revision was appended, so no alert fired: {err}"
    );
    let rec = datasets
        .get("watch", "pages", URL)
        .await
        .expect("get")
        .expect("the previous fingerprint survives");
    assert_eq!(
        rec.data["chars"], first["chars"],
        "the stored record still holds the real page, not a 0-char one"
    );
    assert!(
        rec.data["content_sha256"].as_str().is_some_and(
            |h| h != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ),
        "the empty-body hash never reached the store"
    );
}

/// The first-ever run against an empty page must not create a phantom record
/// either — otherwise the *next* healthy run reports a "change" that is really
/// just the failure being repaired.
#[tokio::test]
async fn a_first_run_on_an_empty_page_writes_no_record_at_all() {
    let store = TempStore::new("watch-empty-first").await;
    let datasets = store.datasets();

    run_page(&store, "")
        .await
        .expect_err("an empty first fetch fails the run");

    assert!(
        datasets
            .list("watch", "pages", 10)
            .await
            .expect("list")
            .is_empty(),
        "no phantom record"
    );
    assert!(revisions(&datasets).await.is_empty(), "and no revision");

    // ...and the page arriving for real afterwards is a `new` page, not a change.
    let out = run_page(&store, REAL_PAGE).await.expect("healthy run");
    assert_eq!(out["change"], "new");
}

/// A site that really does serve a near-empty page must still be diagnosable:
/// the refusal names the URL, the engine and the status, as `readable` does.
#[tokio::test]
async fn the_refusal_names_the_url_engine_and_status() {
    let store = TempStore::new("watch-empty-msg").await;
    let msg = run_page(&store, "<html><body></body></html>")
        .await
        .expect_err("empty document")
        .to_string();

    assert!(msg.contains(URL), "names the URL: {msg}");
    assert!(msg.contains("http"), "names the engine: {msg}");
    assert!(msg.contains("200"), "names the status: {msg}");
}

/// The guard must not fire on a page that is merely SHORT — the defect was zero
/// being treated as a measurement, and the fix is a guard, not a length policy.
#[tokio::test]
async fn a_short_but_real_page_is_still_watched() {
    let store = TempStore::new("watch-short").await;
    let out = run_page(&store, "<html><body>ok</body></html>")
        .await
        .expect("two characters are content");
    assert_eq!(out["change"], "new");
    assert_eq!(out["chars"], 2);
}
