//! The opt-in importance term on the revisit frontier (N27), driven through the
//! real app.
//!
//! The frontier used to spend `revisit_budget` on learned change cadence alone,
//! so a rarely-changing hub and a leaf nobody links to got identical treatment.
//! `importance_weight` multiplies each seed's due-score by its whole-corpus
//! PageRank — and **`0` must be today's behaviour, byte for byte**, which is the
//! pin the first test is.

mod common;

use app_crawl::Crawl;
use common::{crawl_ctx, graph_ctx, StubSite};
use pumper_core::testing::TempStore;
use pumper_core::ScrapeApp;
use serde_json::{json, Value};
use std::sync::Arc;

const HUB: &str = "https://example.com/hub";
const LEAVES: [&str; 5] = [
    "https://example.com/a",
    "https://example.com/b",
    "https://example.com/c",
    "https://example.com/d",
    "https://example.com/e",
];

/// A hub every leaf links back to, so PageRank ranks the hub far above the
/// leaves — while the learned cadence ranks all six identically (nothing has
/// been seen to change yet).
fn hub_site() -> Arc<StubSite> {
    let mut site = StubSite::new().page(HUB, &LEAVES);
    for leaf in LEAVES {
        site = site.page(leaf, &[HUB]);
    }
    Arc::new(site)
}

async fn seeded_store(name: &str) -> (TempStore, Arc<StubSite>) {
    let store = TempStore::new(name).await;
    let site = hub_site();
    Crawl
        .run(crawl_ctx(
            &store,
            site.clone(),
            json!({
                "seeds": [HUB],
                "max_pages": 50,
                "max_depth": 2,
                "concurrency": 1,
                "dedup_distance": 0,
                "respect_robots": false,
            }),
        ))
        .await
        .expect("the seeding crawl runs");
    age_pages(&store, 3).await;
    (store, site)
}

/// Backdates every `pages` record's due clock by `days`.
///
/// A page fingerprinted a millisecond ago has a due-score of **exactly 0** —
/// `1 - exp(0)` — and no weighting can order a vector of zeros. That is correct
/// behaviour (nothing is due, so importance has nothing to spend on) and it is
/// also not the case anyone runs a revisit in, so the fixture ages the corpus
/// to where a real sentinel sweep finds it: every page equally due, differing
/// only in importance.
async fn age_pages(store: &TempStore, days: i64) {
    let now = chrono::Utc::now().timestamp();
    let records = store.datasets().list("crawl", "pages", 1000).await.unwrap();
    let aged: Vec<(String, Value)> = records
        .into_iter()
        .map(|r| {
            let mut data = r.data.clone();
            data["cadence"]["last_checked_at"] = json!(now - days * 86_400);
            (r.key, data)
        })
        .collect();
    store
        .datasets()
        .upsert_many("crawl", "pages", &aged)
        .await
        .unwrap();
}

/// The one URL a `revisit_budget: 1` run actually fetched (robots is off and
/// `discover` defaults false, so the revisit fetches exactly its budget).
fn only_fetch(site: &StubSite, before: usize) -> String {
    let fetched: Vec<String> = site.fetched().into_iter().skip(before).collect();
    assert_eq!(fetched.len(), 1, "one seed, one fetch: {fetched:?}");
    fetched[0].clone()
}

async fn revisit(store: &TempStore, site: Arc<StubSite>, weight: Option<f64>) -> Value {
    let mut params = json!({
        "mode": "revisit",
        "max_pages": 10,
        "concurrency": 1,
        "dedup_distance": 0,
        "respect_robots": false,
        "revisit_budget": 1,
    });
    if let Some(w) = weight {
        params["importance_weight"] = json!(w);
    }
    Crawl
        .run(crawl_ctx(store, site, params))
        .await
        .expect("the revisit runs")
}

#[tokio::test]
async fn an_unweighted_revisit_orders_exactly_as_it_did_before_the_term_existed() {
    // THE PIN. `importance_weight` defaults to 0, and at 0 the seed list must
    // reach core untouched: no `page_rank` read, no reordering, no truncation
    // in the app — the frontier is core's cadence-only ranking, whose tie-break
    // is the URL. Every seed here is cold (due-score 1.0 for all six), so the
    // budget of one goes to the alphabetically first URL, hub or not.
    let (store, site) = seeded_store("revisit-importance-pin").await;
    // A ranking EXISTS — this is not passing because there was nothing to read.
    let ranked = Crawl
        .run(graph_ctx(&store, json!({"mode": "graph"})))
        .await
        .unwrap();
    assert_eq!(ranked["nodes"], 6, "{ranked}");

    let before = site.fetched().len();
    let out = revisit(&store, site.clone(), None).await;
    assert_eq!(out["importance_weight"], 0.0, "{out}");
    assert_eq!(
        out["importance_skipped"], 0,
        "an unweighted run does not select in the app at all: {out}"
    );
    assert_eq!(
        only_fetch(&site, before),
        LEAVES[0],
        "cadence-only ordering breaks its ties by URL, and /a sorts first"
    );
    // Core, not the app, spent the budget — its own counter says so.
    assert_eq!(out["skipped_not_due"], 5, "{out}");

    // Passing the parameter EXPLICITLY as 0 is the same run — over the same
    // corpus state, so the revisit that just happened is rolled back off the
    // due clock first.
    age_pages(&store, 3).await;
    let before = site.fetched().len();
    let zero = revisit(&store, site.clone(), Some(0.0)).await;
    assert_eq!(zero["importance_skipped"], 0, "{zero}");
    assert_eq!(only_fetch(&site, before), LEAVES[0]);
}

#[tokio::test]
async fn a_weighted_revisit_spends_its_budget_on_the_page_the_corpus_points_at() {
    // Same corpus, same cadence (all cold, all equally due), same budget — the
    // only difference is that importance is now part of the score, and the hub
    // five pages link to wins instead of whichever URL sorts first.
    let (store, site) = seeded_store("revisit-importance-weighted").await;
    Crawl
        .run(graph_ctx(&store, json!({"mode": "graph"})))
        .await
        .unwrap();

    let before = site.fetched().len();
    let out = revisit(&store, site.clone(), Some(2.0)).await;
    assert_eq!(out["importance_weight"], 2.0, "{out}");
    assert_eq!(
        out["importance_skipped"], 5,
        "the app spent the budget and says how many seeds it ranked out: {out}"
    );
    assert_eq!(
        only_fetch(&site, before),
        HUB,
        "the budget must follow importance, not the alphabet"
    );
}

#[tokio::test]
async fn a_weighted_revisit_without_a_ranking_falls_back_to_cadence_instead_of_inventing_one() {
    // Honest absence: `importance_weight` is set but no `graph` run has ever
    // written `page_rank`, so no page has an importance. Every multiplier is
    // 1.0 and the order collapses to due-score — it does NOT fabricate a
    // ranking, and it does not fail the run.
    let (store, site) = seeded_store("revisit-importance-no-ranking").await;
    let before = site.fetched().len();
    let out = revisit(&store, site.clone(), Some(2.0)).await;
    assert_eq!(out["importance_weight"], 2.0, "{out}");
    assert_eq!(out["importance_skipped"], 5, "{out}");
    assert_eq!(
        only_fetch(&site, before),
        LEAVES[0],
        "with no ranking the weighted order is the cadence order"
    );
}
