//! `run()`-level proof that **a preview writes nothing.**
//!
//! `watch` is the fleet's alarm clock, and until now the only way to find out
//! what it would report — and how often it has been reporting it — was to
//! `POST /schedules`, `POST /watches` and wait to be woken. Setting the monitor
//! up WAS the experiment.
//!
//! The registry's `dry-run-preview` technique is the shape, and its central
//! rule is the one these tests exist to hold: **a preview that runs its own
//! evaluator is a gate watching a proxy.** So the acceptance here is never
//! "the preview returned a plausible answer". It is two things, asserted
//! separately because they are different claims:
//!
//! 1. **The store is untouched.** No record, no revision, no moved
//!    `content_sha256`, no `page.md` on disk. A preview legitimately *fetches*
//!    — it must, to judge a live body — so what is asserted is the WRITE half.
//! 2. **The numbers are the run's numbers.** Seeded identically, the preview's
//!    `would_change`/`diff` equal the real run's `change`/`diff`. A preview
//!    whose answer differs from the run it previews is worthless, and that
//!    equality is what proves the preview went down the production path
//!    (`judge_page` → `page_record` → `pumper_core::diff_values`) rather than
//!    a second one written to agree with it.
//!
//! Every test is named after the anti-pattern it defends.

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
/// panic rather than silently rescue the body under test.
async fn run_with(store: &TempStore, body: &str, preview: bool) -> Result<Value> {
    let ctx = TestContext::new(&store.storage, "watch")
        .params(json!({ "url": URL, "strategy": "http", "preview": preview }))
        .engines(engines_with(
            Arc::new(StubPage(body.to_string())),
            Arc::new(Dead),
            Arc::new(Dead),
        ))
        .build();
    Watch.run(ctx).await
}

async fn real_run(store: &TempStore, body: &str) -> Value {
    run_with(store, body, false).await.expect("real run")
}

async fn preview(store: &TempStore, body: &str) -> Value {
    run_with(store, body, true).await.expect("preview run")
}

async fn revisions(datasets: &Datasets) -> Vec<String> {
    datasets
        .history("watch", "pages", URL, 100)
        .await
        .expect("history")
        .into_iter()
        .map(|r| r.change)
        .collect()
}

async fn stored_sha(datasets: &Datasets) -> Option<String> {
    datasets
        .get("watch", "pages", URL)
        .await
        .expect("get")
        .and_then(|rec| rec.data["content_sha256"].as_str().map(String::from))
}

/// Where `save_artifact("page.md", …)` lands for a `TestContext`-built job.
fn artifact(store: &TempStore) -> std::path::PathBuf {
    store
        .path()
        .join("artifacts")
        .join("watch")
        .join("job")
        .join("page.md")
}

const V1: &str = "<html><body><h1>Release Notes</h1><p>v2.1 shipped today, with \
                  faster imports and a rewritten scheduler.</p></body></html>";
const V2: &str = "<html><body><h1>Release Notes</h1><p>v2.2 withdraws the import \
                  rewrite and restores the previous queue semantics entirely.</p></body></html>";

// ---------------------------------------------------------------------------
// Case 1 — a first sighting
// ---------------------------------------------------------------------------

/// The headline case. Previewing a URL nobody has ever watched must answer
/// "this would be new" and leave the dataset exactly as empty as it found it —
/// the defect being that today the only way to learn the answer is to create
/// the record whose creation is the alert.
#[tokio::test]
async fn a_preview_of_an_unwatched_url_creates_no_record() {
    let store = TempStore::new("watch-preview-new").await;
    let datasets = store.datasets();

    let out = preview(&store, V1).await;

    assert_eq!(out["would_change"], "new");
    assert_eq!(out["preview"], true);
    assert_eq!(out["writes"]["record_written"], false);
    assert_eq!(out["writes"]["revision_appended"], false);
    assert_eq!(out["writes"]["artifact_saved"], false);
    assert!(
        out.get("change").is_none(),
        "a preview must not report `change` — that key means a write happened: {out}"
    );

    assert!(
        datasets
            .get("watch", "pages", URL)
            .await
            .expect("get")
            .is_none(),
        "the preview wrote a record"
    );
    assert!(
        datasets
            .list("watch", "pages", 10)
            .await
            .expect("list")
            .is_empty(),
        "the preview wrote a record under some other key"
    );
    assert!(
        revisions(&datasets).await.is_empty(),
        "the preview appended a revision"
    );
    assert!(
        !artifact(&store).exists(),
        "the preview left page.md on disk — an artifact is a side effect, so that is a run"
    );
}

// ---------------------------------------------------------------------------
// Case 2 — a byte-identical page
// ---------------------------------------------------------------------------

/// Zero revisions appended means zero watch firings, and that is the claim an
/// operator is actually buying. Asserted as the revision list before and after,
/// not as "the call returned unchanged".
#[tokio::test]
async fn a_preview_of_an_identical_page_appends_no_revision() {
    let store = TempStore::new("watch-preview-same").await;
    let datasets = store.datasets();

    real_run(&store, V1).await;
    let before = revisions(&datasets).await;
    let sha_before = stored_sha(&datasets).await;
    assert_eq!(before, ["new"]);

    let out = preview(&store, V1).await;

    assert_eq!(out["would_change"], "unchanged");
    assert_eq!(out["writes"]["revision_appended"], false);
    assert_eq!(
        revisions(&datasets).await,
        before,
        "the revision history moved, so something subscribed would have fired"
    );
    assert_eq!(
        stored_sha(&datasets).await,
        sha_before,
        "the stored body moved"
    );
}

// ---------------------------------------------------------------------------
// Case 3 — a page that really changed
// ---------------------------------------------------------------------------

/// A preview over a genuinely changed page reports the change AND the field
/// diff, while the stored fingerprint still holds the OLD body. That pairing is
/// the whole product: you see what the alarm would say without the baseline
/// moving under you.
#[tokio::test]
async fn a_preview_of_a_changed_page_shows_the_diff_without_moving_the_baseline() {
    let store = TempStore::new("watch-preview-changed").await;
    let datasets = store.datasets();

    let first = real_run(&store, V1).await;
    let old_sha = stored_sha(&datasets).await.expect("a stored fingerprint");
    let before = revisions(&datasets).await;

    let out = preview(&store, V2).await;

    assert_eq!(out["would_change"], "changed", "{out}");
    assert!(
        out["diff"]["content_sha256"]["from"].as_str() == Some(old_sha.as_str()),
        "the field diff must be present and anchored on the STORED body: {}",
        out["diff"]
    );
    assert_eq!(
        stored_sha(&datasets).await.as_deref(),
        Some(old_sha.as_str()),
        "the stored content_sha256 moved — the preview replaced the baseline"
    );
    assert_eq!(
        revisions(&datasets).await,
        before,
        "the preview appended a revision"
    );
    assert_ne!(
        out["chars"], first["chars"],
        "sanity: the two fixtures really are different pages"
    );
}

// ---------------------------------------------------------------------------
// The equivalence — the preview is not a second evaluator
// ---------------------------------------------------------------------------

/// **The anti-proxy assertion.** Two stores seeded identically; one is
/// previewed, the other really run. The verdict and the diff must agree
/// exactly. If the preview ever grows its own judge, its own narrowing or its
/// own diff function, this is what goes red.
#[tokio::test]
async fn a_preview_reports_what_the_run_would_report_not_its_own_opinion() {
    for (tag, body) in [("same", V1), ("moved", V2)] {
        let previewed = TempStore::new(&format!("watch-preview-eq-p-{tag}")).await;
        let executed = TempStore::new(&format!("watch-preview-eq-r-{tag}")).await;
        real_run(&previewed, V1).await;
        real_run(&executed, V1).await;

        let p = preview(&previewed, body).await;
        let r = real_run(&executed, body).await;

        assert_eq!(
            p["would_change"], r["change"],
            "[{tag}] the preview disagrees with the run it previews"
        );
        assert_eq!(
            p["diff"], r["diff"],
            "[{tag}] the preview's diff is not the run's diff"
        );
        assert_eq!(
            p["materiality"], r["materiality"],
            "[{tag}] the preview judged the change with something other than the run's judge"
        );
        assert_eq!(p["chars"], r["chars"], "[{tag}]");
    }
}

// ---------------------------------------------------------------------------
// Case 4 — the noise rate
// ---------------------------------------------------------------------------

/// Eight successive bodies of a genuinely churning status page.
const CHURN: [&str; 8] = [
    "<html><body><h1>Status</h1><p>All regional queues nominal; delivery latency within budget.</p></body></html>",
    "<html><body><h1>Status</h1><p>Investigating elevated error rates on the payments webhook fan-out.</p></body></html>",
    "<html><body><h1>Status</h1><p>Identified: a stuck migration on the EU shard is holding the write lock.</p></body></html>",
    "<html><body><h1>Status</h1><p>Monitoring recovery after the lock was released; backlog draining.</p></body></html>",
    "<html><body><h1>Status</h1><p>Resolved. A postmortem will be published within five business days.</p></body></html>",
    "<html><body><h1>Status</h1><p>Scheduled maintenance tonight: search reindex, read-only for ten minutes.</p></body></html>",
    "<html><body><h1>Status</h1><p>Maintenance complete. Search is serving the rebuilt index everywhere.</p></body></html>",
    "<html><body><h1>Status</h1><p>Degraded: third-party geocoding provider is timing out on bulk lookups.</p></body></html>",
];

/// The number an operator wants before subscribing: this URL changed on 7 of
/// its 8 recorded runs, so a nightly schedule is a nightly page.
#[tokio::test]
async fn a_churning_url_reports_its_own_noise_rate() {
    let store = TempStore::new("watch-preview-noisy").await;
    let datasets = store.datasets();

    // Eight recorded runs: one `new`, then seven material changes. The bodies
    // are genuinely different documents, not one with a rotating digit — a
    // one-token edit is *cosmetic* to the judge and would append no revision,
    // which is the whole point of the judge the sibling card landed.
    for body in CHURN {
        real_run(&store, body).await;
    }
    let before = revisions(&datasets).await;
    assert_eq!(
        before.len(),
        8,
        "fixture: expected 8 revisions, got {before:?}"
    );
    assert_eq!(
        before.iter().filter(|c| *c == "changed").count(),
        7,
        "fixture: expected 7 of them changed, got {before:?}"
    );

    let out = preview(&store, V1).await;
    let noise = &out["noise"];

    assert_eq!(noise["runs"], 8, "{noise}");
    assert_eq!(noise["changed"], 7, "{noise}");
    assert_eq!(noise["rate"], 0.875, "{noise}");
    assert_eq!(noise["verdict"], "noisy", "{noise}");
    assert!(noise["last_changed_at"].is_string(), "{noise}");

    assert_eq!(
        revisions(&datasets).await,
        before,
        "reading the history wrote to it"
    );
}

/// **Never a confident rate off one sample.** A URL with a thin history reports
/// that its history is thin — the failure mode of the naive version is a
/// brand-new monitor reporting a 100% change rate off its single `new`
/// revision and being dismissed as noisy.
#[tokio::test]
async fn a_thin_history_reports_unproven_rather_than_a_hundred_percent() {
    let store = TempStore::new("watch-preview-unproven").await;
    let datasets = store.datasets();

    real_run(&store, V1).await;
    let out = preview(&store, V1).await;

    assert_eq!(out["noise"]["runs"], 1);
    assert_eq!(out["noise"]["verdict"], "unproven", "{}", out["noise"]);
    assert!(
        out["noise"]["rate"].is_null(),
        "one revision is not a rate: {}",
        out["noise"]
    );
    assert!(revisions(&datasets).await.len() == 1);
}

/// And the other end of the scale: a page that has not moved since it was first
/// recorded is `quiet`, which is the answer that makes a monitor worth having.
#[tokio::test]
async fn a_settled_url_reports_quiet_not_merely_not_noisy() {
    let store = TempStore::new("watch-preview-quiet").await;
    let datasets = store.datasets();

    real_run(&store, V1).await;
    real_run(&store, V2).await;
    // Three more identical runs append nothing, so the history stays at 2 — the
    // honest denominator is RECORDED revisions, which is what `runs` counts.
    for _ in 0..3 {
        real_run(&store, V2).await;
    }
    let before = revisions(&datasets).await;

    let out = preview(&store, V2).await;
    assert_eq!(out["noise"]["runs"], before.len());
    assert_eq!(out["noise"]["changed"], 1);
    assert_eq!(out["would_change"], "unchanged");
    assert_eq!(revisions(&datasets).await, before);
}

// ---------------------------------------------------------------------------
// Case 6 — the refusal a preview must not soften
// ---------------------------------------------------------------------------

/// A preview must never report "looks fine" where the real run errors. The
/// empty-extraction refusal is the one `watch` rule that exists to stop a false
/// alert, and a preview that skipped it would be a gate watching a proxy — it
/// would bless a monitor whose every scheduled run fails.
#[tokio::test]
async fn a_preview_hits_the_same_empty_extraction_refusal_a_run_does() {
    let store = TempStore::new("watch-preview-empty").await;
    let datasets = store.datasets();

    let err = run_with(&store, "   \n\t  \n", true)
        .await
        .expect_err("a preview over an empty extraction fails, it does not report 'unchanged'");
    let msg = err.to_string();
    assert!(msg.contains("refusing to"), "the same refusal: {msg}");
    assert!(msg.contains(URL), "names the URL: {msg}");

    assert!(
        datasets
            .list("watch", "pages", 10)
            .await
            .expect("list")
            .is_empty(),
        "and still nothing written"
    );
    assert!(!artifact(&store).exists());
}

// ---------------------------------------------------------------------------
// The guard: absent `preview`, nothing about today's run changed
// ---------------------------------------------------------------------------

/// `preview` defaults false, so a call that does not mention it is byte-for-byte
/// the run this app has always done: it writes, it reports `change`, and it
/// saves its artifact.
#[tokio::test]
async fn a_run_without_the_param_still_writes_exactly_as_before() {
    let store = TempStore::new("watch-preview-default").await;
    let datasets = store.datasets();

    let ctx = TestContext::new(&store.storage, "watch")
        .params(json!({ "url": URL, "strategy": "http" }))
        .engines(engines_with(
            Arc::new(StubPage(V1.to_string())),
            Arc::new(Dead),
            Arc::new(Dead),
        ))
        .build();
    let out = Watch.run(ctx).await.expect("run");

    assert_eq!(out["change"], "new");
    assert!(
        out.get("would_change").is_none(),
        "no preview block on a real run"
    );
    assert!(out.get("noise").is_none(), "no preview block on a real run");
    assert_eq!(revisions(&datasets).await, ["new"]);
    assert!(
        artifact(&store).exists(),
        "the real run still saves page.md"
    );
}
