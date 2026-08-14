//! `run()`-level proof of the destructive case: **an index whose blocks partly
//! fail to parse must not tombstone the dumps it failed to read.**
//!
//! `smlouvy-dump-watch` writes a FULL SNAPSHOT of the Ministry's dump index, so
//! before the completeness floor a feed publishing 5 dumps of which 3 parsed
//! marked the other 2 removed — and the job reported success with numbers
//! indistinguishable from a clean run, because `dumps_in_index` was the *parsed*
//! count. Core's own doc names the hole: `detect_removed` "already refuses an
//! *empty* batch; a partial batch is the case that guard does not cover".
//!
//! It now also covers the **second, orthogonal** way a batch turns out to be a
//! subset of the published index: the `year_from` window. That one is a
//! *request-scoping* measure, not a document-fidelity one, so the parsed-share
//! floor structurally cannot see it — a 120-of-120 parse read through a window
//! has `share() == 1.0` and used to tombstone every dump outside it.
//!
//! Every test here is named after the anti-pattern it defends.

use std::collections::HashMap;
use std::sync::Arc;

use app_smlouvy_dump_watch::SmlouvyDumpWatch;
use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
use pumper_core::{Datasets, HttpClient, HttpRequest, HttpResponse, Result, ScrapeApp};
use serde_json::{json, Value};

const INDEX_URL: &str = "https://data.smlouvy.gov.cz/index.xml";

/// Serves one canned index document at [`INDEX_URL`].
struct StubIndex(String);

#[async_trait]
impl HttpClient for StubIndex {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: self.0.clone(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// One well-formed `<dump>` block for month `m` of year `y`.
fn block_in(y: u32, m: u32) -> String {
    format!(
        "<dump><mesic>{m}</mesic><rok>{y}</rok>\
         <hashDumpu algoritmus=\"sha1\">{:040}</hashDumpu>\
         <velikostDumpu>{}</velikostDumpu>\
         <casGenerovani>{y}-{m:02}-01T00:11:51+02:00</casGenerovani>\
         <odkaz>https://data.smlouvy.gov.cz/dump_{y}_{m:02}.xml</odkaz></dump>",
        y * 100 + m,
        1_000_000 + m as u64,
    )
}

/// One well-formed `<dump>` block for month `m` of 2025.
fn block(m: u32) -> String {
    block_in(2025, m)
}

/// A block the parser must skip: no `<odkaz>`.
fn block_without_url(m: u32) -> String {
    format!("<dump><mesic>{m}</mesic><rok>2025</rok></dump>")
}

fn index_of(blocks: Vec<String>) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <index xmlns=\"http://portal.gov.cz/rejstriky/ISRS/1.2/\">{}</index>",
        blocks.concat()
    )
}

fn dump_url_in(y: u32, m: u32) -> String {
    format!("https://data.smlouvy.gov.cz/dump_{y}_{m:02}.xml")
}

fn dump_url(m: u32) -> String {
    dump_url_in(2025, m)
}

/// Drive one `run()` against a canned index document, with extra params merged
/// over `index_url`.
async fn run_index_with(store: &TempStore, xml: String, extra: Value) -> Value {
    let mut params = json!({ "index_url": INDEX_URL });
    for (k, v) in extra.as_object().expect("params object") {
        params[k] = v.clone();
    }
    let ctx = TestContext::new(&store.storage, "smlouvy-dump-watch")
        .params(params)
        .engines(engines_with(
            Arc::new(StubIndex(xml)),
            Arc::new(Dead),
            Arc::new(Dead),
        ))
        .build();
    SmlouvyDumpWatch.run(ctx).await.expect("run")
}

/// Drive one `run()` against a canned index document.
async fn run_index(store: &TempStore, xml: String) -> Value {
    run_index_with(store, xml, json!({})).await
}

/// Live (non-tombstoned) dump keys currently in the store, sorted.
async fn live_dumps(datasets: &Datasets) -> Vec<String> {
    let mut keys: Vec<String> = datasets
        .list("smlouvy-dump-watch", "dumps", 500)
        .await
        .expect("list")
        .into_iter()
        .filter(|r| r.removed_at.is_none())
        .map(|r| r.key)
        .collect();
    keys.sort();
    keys
}

/// **The direction's headline case.** A clean 5-dump index is harvested, then the
/// Ministry publishes the same 5 months with 2 blocks garbled. Against pre-floor
/// code `sync_many_with_provenance` tombstones those 2 and the job is green —
/// this test fails there and passes with the completeness floor in place.
#[tokio::test]
async fn a_partial_parse_does_not_tombstone_the_dumps_it_failed_to_read() {
    let store = TempStore::new("smlouvy-partial").await;
    let datasets = store.datasets();

    let clean = run_index(&store, index_of((1..=5).map(block).collect())).await;
    assert_eq!(clean["dumps_in_index"], 5);
    assert_eq!(clean["dumps_parsed"], 5);
    assert_eq!(live_dumps(&datasets).await.len(), 5, "seeded index");

    // Same five months published; two blocks lost their <odkaz>.
    let garbled = index_of(vec![
        block(1),
        block(2),
        block(3),
        block_without_url(4),
        block_without_url(5),
    ]);
    let out = run_index(&store, garbled).await;

    assert_eq!(
        live_dumps(&datasets).await.len(),
        5,
        "a 3-of-5 parse must not delete the 2 dumps it failed to read"
    );
    assert_eq!(out["removed"], 0, "nothing was tombstoned");
    assert_eq!(out["dumps_in_index"], 5, "five blocks were SEEN");
    assert_eq!(out["dumps_parsed"], 3, "three of them parsed");
    assert_eq!(out["parse"]["skipped"], 2);
    assert_eq!(out["parse"]["skipped_missing_url"], 2);
    assert_eq!(out["parse"]["partial"], true);
    assert!(
        out["removals_suppressed"].is_string(),
        "a suppressed removal is visible as such, not silently absent: {out}"
    );
    let warnings = out["warnings"].as_array().expect("warnings[]");
    assert_eq!(
        warnings.len(),
        2,
        "the lossy parse AND the suppressed removal are both surfaced: {warnings:?}"
    );
    assert!(warnings.iter().any(|w| w
        .as_str()
        .is_some_and(|s| s.contains("partial index parse"))));
}

/// The seam the old `dumps_in_index` hid: a 3-of-5 run and a 3-of-3 run must not
/// emit the same numbers. Before the split, both said `dumps_in_index: 3`.
#[tokio::test]
async fn a_three_of_five_run_is_not_reported_as_a_three_of_three_run() {
    let store = TempStore::new("smlouvy-distinguishable").await;

    let partial = run_index(
        &store,
        index_of(vec![
            block(1),
            block(2),
            block(3),
            block_without_url(4),
            block_without_url(5),
        ]),
    )
    .await;
    let complete = run_index(&store, index_of((1..=3).map(block).collect())).await;

    assert_ne!(
        (&partial["dumps_in_index"], &partial["dumps_parsed"]),
        (&complete["dumps_in_index"], &complete["dumps_parsed"]),
    );
    assert_eq!(partial["parse"]["share"], 0.6);
    assert_eq!(complete["parse"]["share"], 1.0);
    assert!(complete["removals_suppressed"].is_null());
    assert_eq!(
        complete["warnings"].as_array().expect("warnings[]").len(),
        0,
        "a clean run warns about nothing"
    );
}

/// The floor must not become "never delete anything": a feed that genuinely
/// shrank — every block still parsing — keeps full-snapshot semantics, so the
/// retired dump is tombstoned and *counted*.
#[tokio::test]
async fn a_shrinking_but_clean_index_still_tombstones() {
    let store = TempStore::new("smlouvy-shrink").await;
    let datasets = store.datasets();

    run_index(&store, index_of((1..=5).map(block).collect())).await;
    assert_eq!(live_dumps(&datasets).await.len(), 5);

    // The Ministry retires the two oldest months. All three remaining blocks
    // parse, so this is a real shrink, not a garbled feed.
    let out = run_index(&store, index_of((3..=5).map(block).collect())).await;

    assert_eq!(out["parse"]["partial"], false);
    assert!(out["removals_suppressed"].is_null());
    assert_eq!(out["removed"], 2, "a clean shrink still tombstones");
    let live = live_dumps(&datasets).await;
    assert_eq!(live.len(), 3);
    assert!(!live.contains(&dump_url(1)));
    assert!(live.contains(&dump_url(5)));
}

// ---------------------------------------------------------------------------
// The window floor: a per-run SCOPE must not mutate a global SNAPSHOT
// ---------------------------------------------------------------------------

/// A five-dump index spanning two years: 2023-01, 2023-02, 2025-01..03.
fn two_year_index() -> String {
    index_of(vec![
        block_in(2023, 1),
        block_in(2023, 2),
        block_in(2025, 1),
        block_in(2025, 2),
        block_in(2025, 3),
    ])
}

/// **THE anti-pattern this direction exists for.** `year_from` is a per-run
/// SCOPE parameter, and it was being applied to a global SNAPSHOT write: the
/// narrowing happened after the parse, the batch handed to
/// `sync_many_with_provenance` held only the in-window dumps, and
/// `detect_removed` tombstoned every dump outside it. On the real feed that is
/// ~96 of ~120 dumps deleted by a run that read the document perfectly.
///
/// The parsed-share floor shipped for the garbled-feed case cannot cover this,
/// by construction: this parse is 5-of-5, `share() == 1.0`, `partial == false`.
#[tokio::test]
async fn a_year_window_does_not_tombstone_the_dumps_outside_it() {
    let store = TempStore::new("smlouvy-window").await;
    let datasets = store.datasets();

    // Seed the shared dataset the way the scheduled daily run does: no window.
    let seeded = run_index(&store, two_year_index()).await;
    assert_eq!(seeded["dumps_tracked"], 5);
    assert_eq!(live_dumps(&datasets).await.len(), 5, "seeded index");

    // A consumer asks for 2025 onward. Same document, read perfectly.
    let out = run_index_with(&store, two_year_index(), json!({ "year_from": 2025 })).await;

    assert_eq!(
        out["parse"]["partial"], false,
        "the document parsed completely — the parse floor is NOT what has to \
         catch this: {out}"
    );
    assert_eq!(out["dumps_parsed"], 5, "five blocks parsed");
    assert_eq!(out["dumps_tracked"], 3, "three of them are in the window");
    assert_eq!(
        out["removed"], 0,
        "a request-scoped run tombstoned dumps that are still live upstream"
    );
    assert_eq!(
        live_dumps(&datasets).await.len(),
        5,
        "the two 2023 dumps were deleted by a run that merely scoped them out"
    );
    let reason = out["removals_suppressed"]
        .as_str()
        .expect("a suppressed removal is visible as such, not silently absent");
    assert!(
        reason.contains("year_from=2025"),
        "the reason must name the window, so an operator can tell it apart from \
         a partial parse: {reason}"
    );
}

/// The counter-test, and the failure mode that would be worse than the bug:
/// the guard must not turn `dumps` into an append-only index. A run **without**
/// a window still tombstones a month the Ministry genuinely retired — and so
/// does a window that happens to exclude nothing, because that batch IS the
/// full index.
#[tokio::test]
async fn removal_still_works_without_a_window_and_with_a_vacuous_one() {
    let store = TempStore::new("smlouvy-window-counter").await;
    let datasets = store.datasets();

    run_index(&store, two_year_index()).await;
    assert_eq!(live_dumps(&datasets).await.len(), 5);

    // A window that excludes nothing: every dump is 2023 or later.
    let vacuous = run_index_with(&store, two_year_index(), json!({ "year_from": 2016 })).await;
    assert_eq!(vacuous["dumps_tracked"], 5);
    assert!(
        vacuous["removals_suppressed"].is_null(),
        "a window that drops no dump must keep full-snapshot semantics, or \
         setting year_from at all would silently make the app append-only: \
         {vacuous}"
    );

    // The Ministry retires 2023-01. Unwindowed run: a real removal.
    let shrunk = index_of(vec![
        block_in(2023, 2),
        block_in(2025, 1),
        block_in(2025, 2),
        block_in(2025, 3),
    ]);
    let out = run_index(&store, shrunk).await;
    assert!(out["removals_suppressed"].is_null());
    assert_eq!(
        out["removed"], 1,
        "a genuinely vanished dump is still removed"
    );
    let live = live_dumps(&datasets).await;
    assert_eq!(live.len(), 4);
    assert!(!live.contains(&dump_url_in(2023, 1)));
}

/// **The user cost, reproduced end to end.** `dumps` is ONE shared dataset, and
/// two consumers with different `year_from` values is a supported configuration.
/// Before the window floor the pair alternated: the windowed run tombstoned the
/// out-of-window dumps, the next unwindowed run resurrected all of them — and
/// every resurrection lands in `fresh_dumps`, which this app's manifest tells a
/// dataset trigger to fan out as a targeted re-download of ~100 MB files. The
/// third run's `fresh_dumps` being empty is the whole point: nothing changed
/// upstream, so nothing may be re-downloaded.
#[tokio::test]
async fn alternating_windows_do_not_flip_the_shared_dataset() {
    let store = TempStore::new("smlouvy-window-flip").await;
    let datasets = store.datasets();

    run_index(&store, two_year_index()).await;
    run_index_with(&store, two_year_index(), json!({ "year_from": 2025 })).await;
    let back = run_index(&store, two_year_index()).await;

    assert_eq!(
        back["fresh_dumps"].as_array().expect("fresh_dumps[]").len(),
        0,
        "the unwindowed run RESURRECTED dumps the windowed run had tombstoned, \
         and every resurrection is a ~100 MB re-download a dataset trigger will \
         fan out: {back}"
    );
    assert_eq!(back["new"], 0, "nothing upstream is actually new: {back}");
    assert_eq!(live_dumps(&datasets).await.len(), 5);
}

/// An index that parses to nothing is still a hard failure, not an empty
/// snapshot — the pre-existing guard must survive the floor.
#[tokio::test]
async fn an_index_that_parses_to_nothing_is_an_error_not_an_empty_snapshot() {
    let store = TempStore::new("smlouvy-empty").await;
    let ctx = TestContext::new(&store.storage, "smlouvy-dump-watch")
        .params(json!({ "index_url": INDEX_URL }))
        .engines(engines_with(
            Arc::new(StubIndex(index_of(vec![
                block_without_url(1),
                block_without_url(2),
            ]))),
            Arc::new(Dead),
            Arc::new(Dead),
        ))
        .build();
    let err = SmlouvyDumpWatch
        .run(ctx)
        .await
        .expect_err("a feed that parses to nothing fails the run");
    let msg = err.to_string();
    assert!(
        msg.contains("2 <dump> blocks seen"),
        "the error says what it saw, not just that it got nothing: {msg}"
    );
}
