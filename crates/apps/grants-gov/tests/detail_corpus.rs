//! `grants/opportunity_details` is the only source of federal award amounts in
//! the product, and it is genuinely watchable (`grants` is a registered virtual
//! namespace with grants-gov as a publisher, so `POST /watches
//! {app:"grants", dataset:"opportunity_details"}` is accepted and rides
//! `load_run_changes` → `notify_watches` / `fire_dataset_triggers`).
//!
//! It could never report `unchanged`: `detail_record` stamps
//! `harvested_at: ts(Utc::now())` into every record and change detection hashed
//! the whole value, so a re-harvest of a **byte-identical** `fetchOpportunity`
//! body wrote a new revision and read `changed`. Every notification the dataset
//! would ever send was news about our own clock.
//!
//! These tests pin both halves of the fix — the quiet one AND the loud one —
//! because "stop detecting changes" would satisfy the first alone.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use app_grants_gov::GrantsGov;
use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
use pumper_core::{Datasets, HttpRequest, HttpResponse, Result, ScrapeApp};
use serde_json::{json, Value};

/// What the two endpoints answer right now. Mutating it between runs is how a
/// test says "the LISTING moved" independently of "the DETAIL moved".
#[derive(Clone)]
struct Script {
    /// Listing title — moving it makes the sync report the opportunity CHANGED,
    /// which is the only thing that puts it in the detail-harvest delta.
    listing_title: String,
    /// A real fact about the opportunity, inside the detail body.
    award_ceiling: Value,
}

struct Scripted(Arc<Mutex<Script>>);

#[async_trait]
impl pumper_core::HttpClient for Scripted {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let script = self.0.lock().unwrap().clone();
        let body = if req.url.contains("search2") {
            json!({
                "errorcode": 0,
                "data": {
                    "hitCount": 1,
                    "oppHits": [{
                        "id": "1", "number": "TEST-1",
                        "title": script.listing_title,
                        "agency": "HHS", "oppStatus": "posted",
                        "closeDate": "09/30/2099"
                    }]
                }
            })
        } else {
            // Note the detail carries its OWN title, so a listing-title move
            // does not leak into the stored detail record: run 2's detail body
            // is byte-identical to run 1's.
            json!({
                "errorcode": 0,
                "data": {
                    "id": 1,
                    "opportunityNumber": "TEST-1",
                    "opportunityTitle": "Detail Title",
                    "agencyName": "Health and Human Services",
                    "synopsis": {
                        "awardCeiling": script.award_ceiling,
                        "responseDate": "09/30/2099"
                    }
                }
            })
        };
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: body.to_string(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

const DETAILS_APP: &str = "grants";
const DETAILS_DATASET: &str = "opportunity_details";

async fn run(store: &TempStore, script: &Arc<Mutex<Script>>) -> Value {
    let engines = engines_with(
        Arc::new(Scripted(script.clone())),
        Arc::new(Dead),
        Arc::new(Dead),
    );
    let ctx = TestContext::new(&store.storage, "grants-gov")
        .params(json!({ "rows": 10, "maxPages": 5, "harvestDetails": true }))
        .engines(engines)
        .build();
    GrantsGov.run(ctx).await.expect("run")
}

async fn revisions(ds: &Datasets) -> usize {
    ds.history(DETAILS_APP, DETAILS_DATASET, "1", 10)
        .await
        .unwrap()
        .len()
}

async fn harvested_at(ds: &Datasets) -> String {
    ds.get(DETAILS_APP, DETAILS_DATASET, "1")
        .await
        .unwrap()
        .expect("the detail record is stored")
        .data["harvested_at"]
        .as_str()
        .expect("harvested_at is still stored and readable")
        .to_string()
}

#[tokio::test]
async fn re_harvesting_an_identical_detail_body_is_unchanged_not_a_new_revision() {
    let store = TempStore::new("grants-gov-detail-derived").await;
    let ds = store.datasets();
    let script = Arc::new(Mutex::new(Script {
        listing_title: "Rural Health v1".into(),
        award_ceiling: json!("750000"),
    }));

    let first = run(&store, &script).await;
    assert_eq!(first["new"], json!(1));
    assert_eq!(first["details"]["harvested"], json!(1));
    assert_eq!(revisions(&ds).await, 1, "the first harvest is a new record");
    let stamp_one = harvested_at(&ds).await;

    // The LISTING moves (so the sync reports `changed` and the opportunity
    // enters the detail delta), but the detail body is byte-identical.
    script.lock().unwrap().listing_title = "Rural Health v2".into();
    let second = run(&store, &script).await;
    assert_eq!(second["changed"], json!(1), "the listing really did move");
    assert_eq!(second["details"]["harvested"], json!(1), "it re-harvested");
    assert_eq!(
        revisions(&ds).await,
        1,
        "an identical fetchOpportunity body is not news: no second revision"
    );

    // …and the fix is a CHANGE-DETECTION seam, not a projection: the stored
    // record still carries a fresh `harvested_at`, so "when did we last touch
    // this" is readable even though it stopped firing change detection.
    let stamp_two = harvested_at(&ds).await;
    assert_ne!(
        stamp_one, stamp_two,
        "the derived field is still stored and still refreshed"
    );
}

#[tokio::test]
async fn a_real_detail_change_is_still_reported_as_changed() {
    // The counter-test. Without it, "stop detecting changes" would pass the
    // test above — and this dataset is the ONLY source of federal award
    // amounts, so silencing it would be far worse than the churn it replaced.
    let store = TempStore::new("grants-gov-detail-real-change").await;
    let ds = store.datasets();
    let script = Arc::new(Mutex::new(Script {
        listing_title: "Rural Health v1".into(),
        award_ceiling: json!("750000"),
    }));

    run(&store, &script).await;
    assert_eq!(revisions(&ds).await, 1);

    {
        let mut s = script.lock().unwrap();
        s.listing_title = "Rural Health v2".into();
        // The agency raised the ceiling — a real fact about the opportunity.
        s.award_ceiling = json!("900000");
    }
    let second = run(&store, &script).await;
    assert_eq!(second["changed"], json!(1));
    assert_eq!(
        revisions(&ds).await,
        2,
        "a genuine award-amount move must still append a revision"
    );

    let stored = ds
        .get(DETAILS_APP, DETAILS_DATASET, "1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.data["requirements"]["award_ceiling"],
        json!(900_000.0)
    );
}

/// The money join reads the shared detail corpus with `list_filtered`
/// (`removed_at IS NULL`); the plain `Datasets::list` it replaced returned
/// tombstoned records too, so a retired detail kept overlaying its award
/// amounts onto a live unified row.
#[tokio::test]
async fn a_tombstoned_detail_record_no_longer_joins_its_money_onto_a_live_row() {
    let store = TempStore::new("grants-gov-detail-tombstone").await;
    let ds = store.datasets();
    let script = Arc::new(Mutex::new(Script {
        listing_title: "Rural Health v1".into(),
        award_ceiling: json!("750000"),
    }));

    let first = run(&store, &script).await;
    assert_eq!(first["amountsFilled"], json!(1));
    assert_eq!(first["detailCorpus"]["read"], json!(1));
    assert_eq!(first["detailCorpus"]["truncated"], json!(false));

    ds.tombstone_keys(DETAILS_APP, DETAILS_DATASET, &["1".to_string()])
        .await
        .unwrap();

    // Re-run: the listing is unchanged, so no re-harvest happens and the only
    // detail record there is is a tombstone.
    let second = run(&store, &script).await;
    assert_eq!(second["detailCorpus"]["read"], json!(0), "{second}");
    assert_eq!(second["amountsFilled"], json!(0), "{second}");
}
