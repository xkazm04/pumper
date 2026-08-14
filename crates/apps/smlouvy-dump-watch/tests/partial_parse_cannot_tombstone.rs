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

/// One well-formed `<dump>` block for month `m` of 2025.
fn block(m: u32) -> String {
    format!(
        "<dump><mesic>{m}</mesic><rok>2025</rok>\
         <hashDumpu algoritmus=\"sha1\">{m:040}</hashDumpu>\
         <velikostDumpu>{}</velikostDumpu>\
         <casGenerovani>2025-{m:02}-01T00:11:51+02:00</casGenerovani>\
         <odkaz>https://data.smlouvy.gov.cz/dump_2025_{m:02}.xml</odkaz></dump>",
        1_000_000 + m as u64,
    )
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

fn dump_url(m: u32) -> String {
    format!("https://data.smlouvy.gov.cz/dump_2025_{m:02}.xml")
}

/// Drive one `run()` against a canned index document.
async fn run_index(store: &TempStore, xml: String) -> Value {
    let ctx = TestContext::new(&store.storage, "smlouvy-dump-watch")
        .params(json!({ "index_url": INDEX_URL }))
        .engines(engines_with(
            Arc::new(StubIndex(xml)),
            Arc::new(Dead),
            Arc::new(Dead),
        ))
        .build();
    SmlouvyDumpWatch.run(ctx).await.expect("run")
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
