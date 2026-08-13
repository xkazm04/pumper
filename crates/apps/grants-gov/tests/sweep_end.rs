//! `run()`-level proof that every way the Search2 walk can stop is a distinct,
//! visible outcome — and that only the arm which proves coverage reads as a
//! complete corpus.
//!
//! **Why this file exists at all.** The crate's inline `ScriptedGrantsGov`
//! answers ONE fixed page regardless of `startRecordNum`, which is structurally
//! why every pagination-shaped bug in this app survived: no test could ever
//! reach page 2. `PagedGrantsGov` below serves a scripted *sequence* keyed by
//! the offset the app actually requests, so a short page, a mid-sweep drop, a
//! `maxPages` stop, a renamed `hitCount` and a `hitCount:0` answer are all
//! reachable from `run()`.
//!
//! Every test here is named after the anti-pattern it defends.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use app_grants_gov::GrantsGov;
use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
use pumper_core::{HttpRequest, HttpResponse, Result, ScrapeApp};
use serde_json::{json, Value};

/// One scripted Search2 page: what `data.oppHits` holds at that offset.
struct Page {
    /// Opportunity ids this page serves, in order.
    ids: Vec<String>,
    /// Replace `data.oppHits` with a renamed key, i.e. the array vanished.
    drop_hits: bool,
}

impl Page {
    fn of(n: usize, from: usize) -> Page {
        Page {
            ids: (from..from + n)
                .map(|i| format!("{}", 100_000 + i))
                .collect(),
            drop_hits: false,
        }
    }
    fn dropped() -> Page {
        Page {
            ids: Vec::new(),
            drop_hits: true,
        }
    }
}

/// A Search2 endpoint that serves a scripted page sequence, keyed by the
/// `startRecordNum` the app asks for — the thing the inline scripted client
/// cannot do. fetchOpportunity is answered with a minimal valid detail so the
/// secondary stage never colours the listing assertions.
struct PagedGrantsGov {
    /// `data.hitCount` on page 1, exactly as the server would render it.
    hit_count: Value,
    /// Page size the script assumes; offsets are `i * rows`.
    rows: usize,
    pages: Vec<Page>,
    /// Offsets requested, in order — proof of how far the walk actually got.
    requested: Mutex<Vec<u64>>,
}

impl PagedGrantsGov {
    fn new(hit_count: Value, rows: usize, pages: Vec<Page>) -> Arc<PagedGrantsGov> {
        Arc::new(PagedGrantsGov {
            hit_count,
            rows,
            pages,
            requested: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl pumper_core::HttpClient for PagedGrantsGov {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let body = if req.url.contains("search2") {
            let sent: Value = serde_json::from_str(req.body.as_deref().unwrap_or("{}")).unwrap();
            let start = sent["startRecordNum"].as_u64().unwrap_or(0);
            self.requested.lock().unwrap().push(start);
            let idx = (start as usize) / self.rows;
            let page = self.pages.get(idx);
            let mut data = serde_json::Map::new();
            // Only page 1 is ever read for the total, but a real server sends it
            // on every page.
            data.insert("hitCount".into(), self.hit_count.clone());
            match page {
                Some(p) if p.drop_hits => {
                    data.insert("oppResults".into(), json!([]));
                }
                Some(p) => {
                    data.insert(
                        "oppHits".into(),
                        Value::Array(p.ids.iter().map(|id| hit(id)).collect()),
                    );
                }
                None => {
                    data.insert("oppHits".into(), json!([]));
                }
            }
            json!({ "errorcode": 0, "msg": "ok", "data": Value::Object(data) }).to_string()
        } else {
            json!({
                "errorcode": 0,
                "data": { "id": 1, "synopsis": { "awardCeiling": "none" } }
            })
            .to_string()
        };
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body,
            final_url: req.url,
            cache_hit: false,
        })
    }
}

fn hit(id: &str) -> Value {
    json!({
        "id": id,
        "number": format!("TEST-{id}"),
        "title": format!("Opportunity {id}"),
        "agency": "HHS",
        "oppStatus": "posted",
        "closeDate": "09/30/2099",
    })
}

async fn run_with(client: Arc<PagedGrantsGov>, params: Value) -> Value {
    let store = TempStore::new("grants-gov-sweep").await;
    run_on(&store, client, params).await
}

async fn run_on(store: &TempStore, client: Arc<PagedGrantsGov>, params: Value) -> Value {
    let engines = engines_with(client, Arc::new(Dead), Arc::new(Dead));
    let ctx = TestContext::new(&store.storage, "grants-gov")
        .params(params)
        .engines(engines)
        .build();
    GrantsGov
        .run(ctx)
        .await
        .expect("the listing sync completes")
}

fn warnings(out: &Value) -> Vec<String> {
    out["warnings"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|w| w.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn has_warning(out: &Value, needle: &str) -> bool {
    warnings(out).iter().any(|w| w.contains(needle))
}

/// The base case the other tests are read against: the walk really did cover
/// the corpus, so `sweep: complete`, no coverage warning, and the pagination
/// actually happened (three offsets requested).
#[tokio::test]
async fn a_proven_walk_reports_complete_and_pages_through_the_whole_corpus() {
    let client = PagedGrantsGov::new(
        json!(25),
        10,
        vec![Page::of(10, 0), Page::of(10, 10), Page::of(5, 20)],
    );
    let out = run_with(
        client.clone(),
        json!({ "rows": 10, "maxPages": 25, "harvestDetails": false }),
    )
    .await;
    assert_eq!(out["sweep"], json!("complete"));
    assert_eq!(out["truncated"], json!(false));
    assert_eq!(out["fetched"], json!(25));
    assert_eq!(out["pages"], json!(3));
    assert_eq!(*client.requested.lock().unwrap(), vec![0, 10, 20]);
    assert!(!has_warning(&out, "coverage"), "{:?}", warnings(&out));
}

/// THE bug: `hitCount` renamed reads 0 through `unwrap_or(0)`, `start >= 0`
/// broke the walk after page 1, `truncated` was false and the drift guard —
/// gated on `hit_count > 0` — never fired. The corpus capped at one page,
/// green, indefinitely.
#[tokio::test]
async fn a_renamed_hit_count_does_not_cap_the_corpus_at_one_silent_page() {
    let client = PagedGrantsGov::new(
        // The server renamed `hitCount`; the app reads absent → 0.
        Value::Null,
        10,
        vec![Page::of(10, 0), Page::of(10, 10), Page::of(4, 20)],
    );
    let out = run_with(
        client.clone(),
        json!({ "rows": 10, "maxPages": 25, "harvestDetails": false }),
    )
    .await;
    // The walk no longer stops at page 1 — it runs to the short page.
    assert_eq!(*client.requested.lock().unwrap(), vec![0, 10, 20]);
    assert_eq!(out["fetched"], json!(24));
    // …and it is never called a complete corpus.
    assert_eq!(out["sweep"], json!("unknown_total"));
    assert_eq!(out["truncated"], json!(true));
    assert!(
        has_warning(&out, "coverage unproven"),
        "{:?}",
        warnings(&out)
    );
}

/// A rate-limited or partially-served page 2 (HTTP 200, fewer hits than asked)
/// used to end the walk at 1,100 of 1,366 records with `truncated: false`, no
/// warning, and the drift guard silent because `hits` was non-empty.
#[tokio::test]
async fn a_short_page_is_reported_as_truncation_not_as_the_end_of_the_corpus() {
    let client = PagedGrantsGov::new(
        json!(37),
        10,
        vec![Page::of(10, 0), Page::of(2, 10), Page::of(10, 20)],
    );
    let out = run_with(
        client.clone(),
        json!({ "rows": 10, "maxPages": 25, "harvestDetails": false }),
    )
    .await;
    assert_eq!(out["sweep"], json!("short_page"));
    assert_eq!(out["truncated"], json!(true));
    assert_eq!(out["fetched"], json!(12), "12 of the 37 the server reports");
    assert_eq!(out["hitCount"], json!(37));
    assert!(has_warning(&out, "TRUNCATED page"), "{:?}", warnings(&out));
    // The walk stopped there rather than paging on into a broken upstream.
    assert_eq!(*client.requested.lock().unwrap(), vec![0, 10]);
}

/// The one arm the old `truncated` flag did cover — pinned so the rewrite that
/// generalized it did not lose it.
#[tokio::test]
async fn a_max_pages_stop_stays_visible_as_a_capped_sweep() {
    let client = PagedGrantsGov::new(
        json!(1000),
        10,
        vec![Page::of(10, 0), Page::of(10, 10), Page::of(10, 20)],
    );
    let out = run_with(
        client.clone(),
        json!({ "rows": 10, "maxPages": 2, "harvestDetails": false }),
    )
    .await;
    assert_eq!(out["sweep"], json!("capped"));
    assert_eq!(out["truncated"], json!(true));
    assert_eq!(out["pages"], json!(2));
    assert!(has_warning(&out, "maxPages=2"), "{:?}", warnings(&out));
}

/// Mid-sweep `oppHits` drift on page ≥ 2: `unwrap_or_default()` emptied the
/// array, `got = 0 < rows` broke the walk, and `truncated` was false because
/// page 1 had landed. The aggregate guard could not see it — it only ever
/// looked at whether the WHOLE run parsed zero hits.
#[tokio::test]
async fn a_mid_sweep_opp_hits_rename_fails_loudly_instead_of_ending_the_walk() {
    let store = TempStore::new("grants-gov-sweep-middrift").await;
    let client = PagedGrantsGov::new(
        json!(30),
        10,
        vec![Page::of(10, 0), Page::dropped(), Page::of(10, 20)],
    );
    let engines = engines_with(client, Arc::new(Dead), Arc::new(Dead));
    let ctx = TestContext::new(&store.storage, "grants-gov")
        .params(json!({ "rows": 10, "maxPages": 25, "harvestDetails": false }))
        .engines(engines)
        .build();
    let err = GrantsGov
        .run(ctx)
        .await
        .expect_err("a page inside the corpus that parses zero hits is drift");
    let msg = err.to_string();
    assert!(msg.contains("schema drift"), "{msg}");
    assert!(msg.contains("page 2"), "{msg}");
}

/// Query-grammar drift answering `{hitCount: 0, oppHits: []}` over a non-empty
/// stored corpus produced `{fetched: 0, new: 0, changed: 0, warnings: []}` — a
/// perfect run in which nothing was swept and every unified row went stale
/// forever. The count must come from the STORE, never from the same response.
#[tokio::test]
async fn an_empty_listing_over_a_stored_corpus_is_drift_not_a_clean_sweep() {
    let store = TempStore::new("grants-gov-sweep-empty").await;

    // A healthy run first, so there is a corpus to contradict.
    let healthy = PagedGrantsGov::new(json!(3), 10, vec![Page::of(3, 0)]);
    let first = run_on(
        &store,
        healthy,
        json!({ "rows": 10, "maxPages": 25, "harvestDetails": false }),
    )
    .await;
    assert_eq!(first["new"], json!(3));
    assert_eq!(first["sweep"], json!("complete"));

    // Now the grammar drifts: a syntactically valid query that matches nothing.
    let drifted = PagedGrantsGov::new(json!(0), 10, vec![]);
    let engines = engines_with(drifted, Arc::new(Dead), Arc::new(Dead));
    let ctx = TestContext::new(&store.storage, "grants-gov")
        .params(json!({ "rows": 10, "maxPages": 25, "harvestDetails": false }))
        .engines(engines)
        .build();
    let err = GrantsGov
        .run(ctx)
        .await
        .expect_err("hitCount:0 over a stored corpus is drift");
    let msg = err.to_string();
    assert!(msg.contains("hitCount:0"), "{msg}");
    assert!(msg.contains("3 opportunities are already stored"), "{msg}");
}

/// The counter-test that keeps the guard above usable: a NARROWED pull
/// (`keyword`/`eligibilities`) may legitimately match nothing while the corpus
/// is full. Failing those would break the manifest's own targeted-pull example.
#[tokio::test]
async fn a_narrowed_pull_matching_nothing_is_not_drift() {
    let store = TempStore::new("grants-gov-sweep-narrow").await;
    let healthy = PagedGrantsGov::new(json!(3), 10, vec![Page::of(3, 0)]);
    run_on(
        &store,
        healthy,
        json!({ "rows": 10, "maxPages": 25, "harvestDetails": false }),
    )
    .await;

    let empty = PagedGrantsGov::new(json!(0), 10, vec![]);
    let out = run_on(
        &store,
        empty,
        json!({ "keyword": "no such programme", "eligibilities": "12",
                "rows": 10, "maxPages": 25, "harvestDetails": false }),
    )
    .await;
    assert_eq!(out["fetched"], json!(0));
    // An honestly-empty result set IS a fully-swept one.
    assert_eq!(out["sweep"], json!("complete"));
    assert_eq!(out["truncated"], json!(false));
}
