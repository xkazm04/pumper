//! `run()`-level proof that **a cosmetic change does not wake anyone.**
//!
//! `watch` is the fleet's Visualping, and before the shared judge it had no
//! materiality stage at all: `upsert_with_provenance` returned the store's
//! exact-hash `ChangeKind` and that went straight into the result and into
//! every subscribed webhook. Any moved byte — a reordered list, a reflowed
//! paragraph, a rotating ad slot — appended a revision and fired an alert.
//!
//! The acceptance is the ABSENT REVISION, not the returned verdict: no
//! revision is what "no watch fired" actually means in this store. Every test
//! is named after the anti-pattern it defends.

use std::collections::HashMap;
use std::sync::Arc;

use app_watch::Watch;
use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
use pumper_core::{Datasets, HttpClient, HttpRequest, HttpResponse, Result, ScrapeApp};
use serde_json::{json, Value};

const URL: &str = "https://example.com/releases";

/// The page as first seen.
const PAGE: &str = "<html><body><h1>Release Notes</h1><ul>\
    <li>v2.1 shipped today and the payments endpoint now accepts idempotency keys</li>\
    <li>the dashboard shows per-connector latency for every workspace</li>\
    </ul></body></html>";

/// **The same page, reordered and reflowed.** Not one word differs; the markdown
/// text — and therefore the sha256 the store hashes — does. This is the body
/// that used to fire a webhook.
const PAGE_REORDERED: &str = "<html><body>\n  <h1>Release   Notes</h1>\n  <ul>\n\
    <li>the dashboard shows per-connector latency for every workspace</li>\n\
    <li>v2.1 shipped today and the payments endpoint now accepts idempotency keys</li>\n\
    </ul>\n</body></html>";

/// A real edit: the release note is replaced with a different announcement.
const PAGE_REWRITTEN: &str = "<html><body><h1>Maintenance Window</h1><ul>\
    <li>scheduled downtime on Sunday between 02:00 and 04:00 UTC</li>\
    <li>we are migrating the primary cluster; billing is unaffected</li>\
    </ul></body></html>";

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

/// **The headline case.** Run #2 moves bytes and nothing else. The revision
/// count must not move, because a revision is what a subscribed watch fires on.
#[tokio::test]
async fn a_reordered_page_appends_no_revision_so_no_watch_fires() {
    let store = TempStore::new("watch-cosmetic").await;
    let datasets = store.datasets();

    let first = run_page(&store, PAGE).await.expect("first run");
    assert_eq!(first["change"], "new");
    assert_eq!(revisions(&datasets).await, ["new"]);
    let first_sha = datasets
        .get("watch", "pages", URL)
        .await
        .expect("get")
        .expect("record")
        .data["content_sha256"]
        .clone();

    let second = run_page(&store, PAGE_REORDERED).await.expect("second run");
    assert_eq!(
        second["materiality"]["significance"], "cosmetic",
        "a reorder is not news: {}",
        second["materiality"]
    );
    assert_eq!(
        revisions(&datasets).await,
        ["new"],
        "the cosmetic run appended a revision — every subscribed watch just fired"
    );

    // ...and the record body still refreshed, so readers are not stuck on the
    // stale text. Withholding the revision is not withholding the write.
    let rec = datasets
        .get("watch", "pages", URL)
        .await
        .expect("get")
        .expect("record");
    assert_ne!(
        rec.data["content_sha256"], first_sha,
        "the stored body must track the live page"
    );
}

/// The guard that keeps the suppression honest: a real edit must still fire.
/// A monitor that suppresses everything is worse than one that fires on bytes.
#[tokio::test]
async fn a_real_edit_still_appends_its_revision() {
    let store = TempStore::new("watch-material").await;
    let datasets = store.datasets();

    run_page(&store, PAGE).await.expect("first run");
    let second = run_page(&store, PAGE_REWRITTEN).await.expect("second run");

    assert_eq!(second["change"], "changed");
    assert_ne!(
        second["materiality"]["significance"], "cosmetic",
        "a replaced page is news: {}",
        second["materiality"]
    );
    assert_eq!(
        revisions(&datasets).await,
        ["changed", "new"],
        "a material change must still reach every subscriber"
    );
}

/// The verdict is stored, not only returned: `watch --preview` and any dataset
/// consumer must be able to read why a run did or did not alert.
#[tokio::test]
async fn the_verdict_is_readable_from_the_record_not_only_the_job_result() {
    let store = TempStore::new("watch-verdict-readable").await;
    let datasets = store.datasets();

    run_page(&store, PAGE).await.expect("first run");
    run_page(&store, PAGE_REORDERED).await.expect("second run");

    let rec = datasets
        .get("watch", "pages", URL)
        .await
        .expect("get")
        .expect("record");
    assert_eq!(rec.data["materiality"]["significance"], "cosmetic");
    assert!(
        rec.data["materiality"]["reason"]
            .as_str()
            .is_some_and(|r| !r.is_empty()),
        "the stored verdict carries its reason"
    );
}

/// The transition guard. Adding the fingerprint and the verdict to the record
/// must NOT re-hash every stored page into a `changed` revision on the first
/// run after deploy — that would fire every watch in the fleet exactly once,
/// which is the alarm this app exists to prevent.
#[tokio::test]
async fn adopting_the_judge_does_not_re_fire_every_existing_page() {
    let store = TempStore::new("watch-transition").await;
    let datasets = store.datasets();

    // A record written the old way: no `body_simhash`, no `materiality`.
    datasets
        .upsert(
            "watch",
            "pages",
            URL,
            &json!({
                "url": URL,
                "title": "Release Notes",
                "chars": 1,
                "content_sha256": "deadbeef",
                "excerpt": "x",
            }),
        )
        .await
        .expect("legacy record");
    assert_eq!(revisions(&datasets).await, ["new"]);

    // The same body it was written from would be `unchanged`; a *different*
    // body is a real change — either way, exactly one more revision at most,
    // never a fleet-wide re-fire of untouched pages.
    let out = run_page(&store, PAGE).await.expect("first judged run");
    assert!(
        out["materiality"]["significance"].is_string(),
        "the run reports a verdict even with no prior fingerprint"
    );
    assert!(
        revisions(&datasets).await.len() <= 2,
        "the adoption write re-hashed the record into extra revisions"
    );
}
