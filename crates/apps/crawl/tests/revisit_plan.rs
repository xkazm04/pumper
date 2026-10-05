//! Revisit **planning**: what the budget would fetch, shown before anything is
//! fetched — and the two skip reasons told apart.
//!
//! Two defects these cases pin, both of them contract rather than accident:
//!
//! 1. `revisit_budget` and `min_due_score` were free-form operator inputs with
//!    nothing anywhere that showed the due-score distribution they are compared
//!    against. The only feedback was running the crawl — spending real fetches —
//!    and reading one number afterwards.
//! 2. That one number, `skipped_not_due`, was incremented by BOTH "scored below
//!    `min_due_score`" and "ranked past `revisit_budget`". Those call for
//!    opposite tuning actions (lower the threshold vs. raise the budget) and the
//!    result could not tell them apart.
//!
//! The structural half matters more than the reporting half: preview and
//! execution both call `pumper_core::plan_revisit`, so they cannot diverge. A
//! preview that runs its own evaluator is a gate watching a proxy, and
//! `plan_matches_what_the_real_run_actually_selects` is the case that would
//! catch it.

mod common;

use app_crawl::Crawl;
use common::{crawl_ctx, StubSite};
use pumper_core::testing::TempStore;
use pumper_core::{Error, ScrapeApp};
use serde_json::{json, Value};
use std::sync::Arc;

/// Five live `pages` records on one host with deliberately spread due scores:
///
/// | url | last checked | interval | due_score |
/// | --- | --- | --- | --- |
/// | `/p0` | never | — | `1.0` |
/// | `/p1` | 1 interval ago | 3600 | `~0.632` |
/// | `/p2` | ½ interval ago | 3600 | `~0.393` |
/// | `/p3` | ¼ interval ago | 3600 | `~0.221` |
/// | `/p4` | 1/10 interval ago | 3600 | `~0.095` |
///
/// Exactly one seed (`/p0`) scores `>= 0.9`, and every seed scores `> 0`, so
/// `min_due_score: 0` selects all five.
const SEED_INTERVAL: f64 = 3600.0;

fn now_epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `1 - exp(-elapsed/interval)` — the closed form of `pumper_core::due_score`,
/// written out here so the expectation is independent of the implementation.
fn expected_due_score(elapsed: f64) -> f64 {
    1.0 - (-elapsed / SEED_INTERVAL).exp()
}

/// Elapsed seconds since last check, per seed index (index 0 = never checked).
const ELAPSED: [f64; 5] = [0.0, 3600.0, 1800.0, 900.0, 360.0];

fn seed_url(i: usize) -> String {
    format!("https://example.com/p{i}")
}

/// Plants the five records above into `crawl/pages` and returns a site that
/// serves all five, so the same corpus drives both a plan and a real run.
async fn plant_corpus(store: &TempStore) -> Arc<StubSite> {
    let now = now_epoch();
    let mut site = StubSite::new();
    for i in 0..5 {
        let url = seed_url(i);
        site = site.page(&url, &[]);
        let cadence = if i == 0 {
            // Never checked: due_score 1.0, no baseline to discount it.
            json!({
                "checks": 0,
                "changes": 0.0,
                "last_checked_at": Value::Null,
                "last_change_at": Value::Null,
                "interval_secs": Value::Null,
            })
        } else {
            let checked = now - ELAPSED[i] as i64;
            json!({
                "checks": 3,
                "changes": 2.0,
                "last_checked_at": checked,
                "last_change_at": checked,
                "interval_secs": SEED_INTERVAL,
            })
        };
        store
            .datasets()
            .upsert(
                "crawl",
                "pages",
                &url,
                &json!({ "url": url, "simhash": 0, "cadence": cadence }),
            )
            .await
            .expect("plant page record");
    }
    Arc::new(site)
}

fn plan_params(extra: Value) -> Value {
    let mut params = json!({
        "mode": "revisit",
        "plan": true,
        "respect_robots": false,
        "min_due_score": 0,
    });
    if let (Value::Object(base), Value::Object(more)) = (&mut params, extra) {
        for (k, v) in more {
            base.insert(k, v);
        }
    }
    params
}

fn run_params(extra: Value) -> Value {
    let mut params = json!({
        "mode": "revisit",
        "respect_robots": false,
        "dedup_distance": 0,
        "min_due_score": 0,
    });
    if let (Value::Object(base), Value::Object(more)) = (&mut params, extra) {
        for (k, v) in more {
            base.insert(k, v);
        }
    }
    params
}

/// Every planted key still has exactly its planting revision: a plan writes
/// nothing. "No writes" is the first property of a dry run, and the failure mode
/// worth pinning is the preview that quietly advances cadence bookkeeping.
async fn assert_no_new_revisions(store: &TempStore) {
    for i in 0..5 {
        let url = seed_url(i);
        let history = store
            .datasets()
            .history("crawl", "pages", &url, 10)
            .await
            .expect("history");
        assert_eq!(
            history.len(),
            1,
            "a plan must not write a `pages` revision, but {url} has {} \
             (1 = the planting upsert)",
            history.len()
        );
    }
}

#[tokio::test]
async fn a_plan_reports_what_would_be_fetched_and_fetches_nothing() {
    let store = TempStore::new("crawl-plan-nothing-fetched").await;
    let site = plant_corpus(&store).await;

    let out = Crawl
        .run(crawl_ctx(&store, site.clone(), plan_params(json!({}))))
        .await
        .unwrap();

    assert_eq!(out["plan"], true, "{out}");
    assert_eq!(out["revisit"], true, "{out}");
    assert_eq!(out["seeds_known"], 5, "{out}");
    assert_eq!(out["would_fetch"], 5, "{out}");
    assert_eq!(out["skipped_not_due"], 0, "{out}");
    assert_eq!(out["skipped_over_budget"], 0, "{out}");
    assert_eq!(
        out["by_host"]["example.com"], 5,
        "the per-host would-fetch breakdown: {out}"
    );

    // THE POINT: the operator learned the selection without paying for it.
    assert!(
        site.fetched().is_empty(),
        "a plan fetched {:?}",
        site.fetched()
    );
    assert_no_new_revisions(&store).await;
}

#[tokio::test]
async fn a_budget_shows_its_cutoff_instead_of_only_a_skip_count() {
    let store = TempStore::new("crawl-plan-budget").await;
    let site = plant_corpus(&store).await;

    let out = Crawl
        .run(crawl_ctx(
            &store,
            site.clone(),
            plan_params(json!({ "revisit_budget": 2 })),
        ))
        .await
        .unwrap();

    assert_eq!(out["would_fetch"], 2, "{out}");
    assert_eq!(out["skipped_over_budget"], 3, "{out}");
    assert_eq!(
        out["skipped_not_due"], 0,
        "nothing was below the threshold — the budget is what bit: {out}"
    );

    // The cutoff is the score of the LAST seed the budget still affords, i.e.
    // the 2nd-highest here. That number is the whole point: it says where to put
    // `min_due_score`, or how much budget the next band would cost.
    let cutoff = out["cutoff_due_score"]
        .as_f64()
        .unwrap_or_else(|| panic!("a budgeted plan names its cutoff: {out}"));
    let expected = expected_due_score(ELAPSED[1]);
    assert!(
        (cutoff - expected).abs() < 1e-3,
        "cutoff {cutoff} should be the 2nd-highest seed's due score {expected}: {out}"
    );

    assert!(site.fetched().is_empty(), "{:?}", site.fetched());
    assert_no_new_revisions(&store).await;
}

#[tokio::test]
async fn a_threshold_skip_is_reported_as_a_threshold_skip() {
    let store = TempStore::new("crawl-plan-threshold").await;
    let site = plant_corpus(&store).await;

    let out = Crawl
        .run(crawl_ctx(
            &store,
            site.clone(),
            plan_params(json!({ "min_due_score": 0.9 })),
        ))
        .await
        .unwrap();

    assert_eq!(out["would_fetch"], 1, "only /p0 scores >= 0.9: {out}");
    assert_eq!(out["skipped_not_due"], 4, "{out}");
    assert_eq!(
        out["skipped_over_budget"], 0,
        "no budget was set — nothing was skipped for budget: {out}"
    );
    assert!(site.fetched().is_empty(), "{:?}", site.fetched());
    assert_no_new_revisions(&store).await;
}

#[tokio::test]
async fn plan_matches_what_the_real_run_actually_selects() {
    // THE CASE THAT MAKES THE PREVIEW REAL. A preview running its own simplified
    // evaluator is a gate watching a proxy: it stays green while the thing it
    // claims to describe drifts. Both paths call `pumper_core::plan_revisit`, so
    // this cannot be made to pass by a parallel re-implementation that happens to
    // agree today.
    let store = TempStore::new("crawl-plan-parity").await;
    let site = plant_corpus(&store).await;

    let plan = Crawl
        .run(crawl_ctx(
            &store,
            site.clone(),
            plan_params(json!({ "revisit_budget": 2, "min_due_score": 0.2 })),
        ))
        .await
        .unwrap();

    let real = Crawl
        .run(crawl_ctx(
            &store,
            site.clone(),
            run_params(json!({ "revisit_budget": 2, "min_due_score": 0.2 })),
        ))
        .await
        .unwrap();

    assert_eq!(
        real["revisited"], plan["would_fetch"],
        "the plan promised {} fetches, the run made {}: plan={plan} real={real}",
        plan["would_fetch"], real["revisited"]
    );
    assert_eq!(
        real["skipped_not_due"], plan["skipped_not_due"],
        "plan={plan} real={real}"
    );
    assert_eq!(
        real["skipped_over_budget"], plan["skipped_over_budget"],
        "plan={plan} real={real}"
    );
    // ...and the plan really did describe a selection, not an empty one.
    assert_eq!(plan["would_fetch"], 2, "{plan}");
    assert_eq!(plan["skipped_over_budget"], 2, "{plan}");
    assert_eq!(plan["skipped_not_due"], 1, "/p4 scores ~0.095: {plan}");
    // The run fetched exactly the two URLs the plan afforded.
    assert_eq!(
        site.fetched().len(),
        2,
        "only the real run fetched, and only its budget: {:?}",
        site.fetched()
    );
}

#[tokio::test]
async fn a_budget_skip_is_not_reported_as_not_due() {
    // THE REFUTED BEHAVIOR: core incremented `skipped_not_due` for a seed below
    // `min_due_score` AND for a seed ranked past `revisit_budget`, so this run
    // used to report `skipped_not_due: 3` with a threshold of 0 — "nothing was
    // due" and "the budget ran out" were one number, and they call for opposite
    // tuning actions.
    let store = TempStore::new("crawl-run-budget-split").await;
    let site = plant_corpus(&store).await;

    let out = Crawl
        .run(crawl_ctx(
            &store,
            site.clone(),
            run_params(json!({ "revisit_budget": 2 })),
        ))
        .await
        .unwrap();

    assert_eq!(out["revisited"], 2, "{out}");
    assert_eq!(
        out["skipped_not_due"], 0,
        "min_due_score was 0 — no seed was below it: {out}"
    );
    assert_eq!(
        out["skipped_over_budget"], 3,
        "three seeds lost to the budget, and the result now says so: {out}"
    );
}

#[tokio::test]
async fn plan_outside_revisit_mode_fails_instead_of_crawling_for_real() {
    // A `plan: true` that silently performed a full fetching crawl would be the
    // worst outcome of the whole feature: the operator asked what WOULD happen
    // and paid for it happening.
    let store = TempStore::new("crawl-plan-wrong-mode").await;
    let site = Arc::new(StubSite::new().page("https://example.com/", &[]));

    let err = Crawl
        .run(crawl_ctx(
            &store,
            site.clone(),
            json!({
                "seeds": ["https://example.com/"],
                "plan": true,
                "respect_robots": false,
            }),
        ))
        .await
        .expect_err("plan outside revisit mode must fail");

    match &err {
        Error::App(msg) => {
            assert!(
                msg.contains("plan") && msg.contains("revisit"),
                "the error must name the constraint: {msg}"
            );
        }
        other => panic!("expected Error::App, got {other:?}"),
    }
    assert!(
        site.fetched().is_empty(),
        "a refused plan must not have crawled: {:?}",
        site.fetched()
    );
}
