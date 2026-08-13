//! Everything a crawl *learns* about the hosts it touches used to be committed
//! only after `crawl(...)` returned.
//!
//! The per-host tallies live in process memory; the drain that turns them into
//! `web-reliability` observations, cost-ledger events and tier-router learning
//! all sat after the `?`. The worker pins `app.run(ctx)` in a select and **drops**
//! it on cancel or timeout, so a reaped job, a shutdown drain, or a propagating
//! fetch error skipped the entire loop — and the durable resume state carries no
//! tallies, so a resumed attempt could not recover them either. A six-hour crawl
//! reaped at 95% left the reliability index and the cost ledger looking like the
//! run never happened.
//!
//! **What this file covers, and what it does not.** The interruption guarantee
//! itself — abandon the future mid-flight and the store already knows — is
//! pinned inside the crate, against `crawl_flushing_telemetry` (the exact
//! function `run()` drives) at millisecond cadence. It cannot be pinned here:
//! the flush interval is two minutes, and a paused tokio clock is not usable
//! because auto-advance fires the SQLite pool's acquire timeout, so
//! `TempStore::new` fails before the test starts. What IS pinned here is the
//! whole path through the real `Crawl::run()`: the wiring reaches the store, a
//! completed crawl records exactly one run, and a host with no `robots.txt`
//! stops being recorded as a host serving dead pages.

mod common;

use std::sync::Arc;

use app_crawl::Crawl;
use common::{crawl_ctx, StubSite};
use pumper_core::costs::CostLedger;
use pumper_core::testing::TempStore;
use pumper_core::tiers::TierMemory;
use pumper_core::ScrapeApp;
use serde_json::{json, Value};

const OBS: &str = "host_observations";
const INDEX: &str = "host_index";
const APP: &str = "web-reliability";

fn today() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

async fn observation(store: &TempStore, host: &str) -> Option<Value> {
    let key = format!("{host}@{}", today());
    store
        .datasets()
        .get(APP, OBS, &key)
        .await
        .unwrap()
        .map(|r| r.data)
}

async fn index(store: &TempStore, host: &str) -> Option<Value> {
    store
        .datasets()
        .get(APP, INDEX, host)
        .await
        .unwrap()
        .map(|r| r.data)
}

/// `n` same-host pages in one chain, so a crawl of them takes `n` fetches.
fn chain_site(n: usize) -> StubSite {
    let mut site = StubSite::new();
    for i in 0..n {
        let url = format!("https://example.com/p{i}");
        let next = format!("https://example.com/p{}", i + 1);
        site = if i + 1 < n {
            site.page(&url, &[next.as_str()])
        } else {
            site.page(&url, &[])
        };
    }
    site
}

fn chain_params(n: usize) -> Value {
    json!({
        "seeds": ["https://example.com/p0"],
        "max_pages": n,
        "max_depth": n,
        "concurrency": 1,
        "dedup_distance": 0,
        "respect_robots": false,
    })
}

#[tokio::test]
async fn a_completed_crawl_commits_one_run_through_all_three_seams() {
    // The whole flush path, wired through the real `run()`: the reliability
    // index, the cost ledger and the tier router all learn from the crawl, and
    // a run is ONE run — `low_confidence` keys off `observations`, so a run
    // counted per flush would manufacture confidence in its own numbers.
    let store = TempStore::new("crawl-telemetry-completed").await;
    let site = Arc::new(chain_site(20));
    let ctx = crawl_ctx(&store, site.clone(), chain_params(20));
    let job_id = ctx.job_id;
    let out = Crawl.run(ctx).await.unwrap();
    assert_eq!(out["crawled"], 20, "{out}");
    assert_eq!(
        out["reliability_hosts"], 1,
        "one host, however many flushes"
    );

    let obs = observation(&store, "example.com")
        .await
        .expect("observation");
    assert_eq!(obs["crawl"]["runs"], 1, "{obs}");
    assert_eq!(obs["crawl"]["runs_complete"], 1, "{obs}");
    assert_eq!(obs["crawl"]["partial"], false, "the run finished: {obs}");
    assert_eq!(
        obs["crawl"]["fetches"], 20,
        "every fetch, counted once: {obs}"
    );

    let idx = index(&store, "example.com").await.expect("index record");
    assert_eq!(idx["observations"], 1, "{idx}");
    assert_eq!(idx["scrapeability"]["low_confidence"], true, "{idx}");
    assert_eq!(idx["scrapeability"]["partial_runs"], false, "{idx}");

    assert!(
        !CostLedger::new(store.storage.pool())
            .job_events(job_id)
            .await
            .unwrap()
            .is_empty(),
        "the crawl's fetches must reach the cost ledger"
    );
    assert!(
        TierMemory::new(store.storage.pool(), 0)
            .get("example.com")
            .await
            .unwrap()
            .is_some(),
        "the crawl's fetches must reach the tier router"
    );
}

#[tokio::test]
async fn a_host_with_no_robots_txt_is_not_recorded_as_serving_dead_pages() {
    // THE REFUTED BEHAVIOR: the metering client wraps the client core hands to
    // `robots_for`. A host with no robots.txt answers 404 → classified `gone` →
    // folded into the index. EVERY crawl fabricated a gone-page observation for
    // EVERY host lacking a robots.txt, so the index was wrong in a consistent
    // direction, not merely sparse.
    let store = TempStore::new("crawl-robots-probe").await;
    let site = Arc::new(
        StubSite::new()
            .page("https://example.com/a", &["https://example.com/b"])
            .page("https://example.com/b", &[]),
    );
    let out = Crawl
        .run(crawl_ctx(
            &store,
            site.clone(),
            json!({
                "seeds": ["https://example.com/a"],
                "max_pages": 10,
                "max_depth": 2,
                "concurrency": 1,
                "dedup_distance": 0,
                // The whole point: robots IS fetched, and there is none.
                "respect_robots": true,
            }),
        ))
        .await
        .unwrap();
    assert!(
        site.fetched()
            .iter()
            .any(|u| u == "https://example.com/robots.txt"),
        "fixture is wrong: the crawl must actually probe robots.txt"
    );
    assert_eq!(out["crawled"], 2, "{out}");

    let obs = observation(&store, "example.com")
        .await
        .expect("observation");
    assert_eq!(
        obs["crawl"]["gone"], 0,
        "a missing robots.txt is not a dead page: {obs}"
    );
    assert_eq!(obs["crawl"]["fetches"], 2, "pages only: {obs}");
    assert_eq!(
        obs["crawl"]["probes"], 1,
        "the probe is counted, just not scored: {obs}"
    );

    let idx = index(&store, "example.com").await.expect("index record");
    let components = &idx["scrapeability"]["components"];
    assert_eq!(components["availability"], 1.0, "{idx}");
    assert_eq!(components["fetch_ok"], 1.0, "{idx}");
}

#[tokio::test]
async fn a_host_that_really_serves_dead_pages_still_scores_for_it() {
    // The control arm: excluding probes must not have hidden the real signal.
    let store = TempStore::new("crawl-real-gone").await;
    let site = Arc::new(
        // `/missing` is unregistered, so the stub answers 404 for it.
        StubSite::new().page("https://example.com/a", &["https://example.com/missing"]),
    );
    Crawl
        .run(crawl_ctx(
            &store,
            site,
            json!({
                "seeds": ["https://example.com/a"],
                "max_pages": 10,
                "max_depth": 2,
                "concurrency": 1,
                "dedup_distance": 0,
                "respect_robots": true,
            }),
        ))
        .await
        .unwrap();

    let obs = observation(&store, "example.com")
        .await
        .expect("observation");
    assert_eq!(
        obs["crawl"]["gone"], 1,
        "a real 404 page still counts: {obs}"
    );
    assert_eq!(obs["crawl"]["probes"], 1, "{obs}");
    let idx = index(&store, "example.com").await.expect("index record");
    assert_eq!(
        idx["scrapeability"]["components"]["availability"], 0.5,
        "one of two page fetches was gone: {idx}"
    );
}
