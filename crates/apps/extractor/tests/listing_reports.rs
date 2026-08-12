//! End-to-end guard for per-inner-field listing reports.
//!
//! A whole real extractor run over stored bodies whose listing has rotted: the
//! `each` container still matches, every card still yields an object, and the
//! document-level status is still `Matched` — so before `DocReport::each` the
//! job result said the run was perfect. This asserts the run now names the dead
//! inner field in `worst_fields`, through the DEFAULT health-enabled path (the
//! fused extract+fingerprint parse), which is the one that actually ships.

use std::path::Path;

use app_extractor::Extractor;
use pumper_core::testing::{TempStore, TestContext};
use pumper_core::{AppContext, ScrapeApp};
use serde_json::{json, Value};
use uuid::Uuid;

fn ctx_with(root: &Path, store: &TempStore, params: Value) -> AppContext {
    TestContext::new(&store.storage, "extractor")
        .params(params)
        .artifacts_dir(root.join("extractor").join("job"))
        .build()
}

/// Three product cards. `price_class` is the class the price span actually
/// carries — flip it to simulate the site dropping the class the rules bind to.
/// The badge is on the first card only: a legitimately sparse field.
fn listing_page(price_class: &str) -> String {
    let cards: String = ["A", "B", "C"]
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let badge = if i == 0 {
                "<i class=\"badge\">new</i>"
            } else {
                ""
            };
            format!(
                "<div class=\"card\"><h3>{name}</h3>\
                 <span class=\"{price_class}\">${i}0</span>{badge}</div>"
            )
        })
        .collect();
    format!("<html><body><div id=\"listing\">{cards}</div></body></html>")
}

#[tokio::test]
async fn a_rotted_listing_field_is_named_in_worst_fields() {
    let store = TempStore::new("extract-listing-rot").await;
    let root = store.path().to_path_buf();
    let datasets = store.datasets();

    let crawl_job = Uuid::new_v4().to_string();
    let crawl_dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&crawl_dir).await.unwrap();
    // The stored body: the price span is now `.amount`, not `.price`.
    tokio::fs::write(
        crawl_dir.join("page.html"),
        listing_page("amount").as_bytes(),
    )
    .await
    .unwrap();
    datasets
        .upsert_many(
            "crawl",
            "pages",
            &[(
                "http://shop/list".into(),
                json!({"url":"http://shop/list","artifact_path":"page.html","job_id":crawl_job}),
            )],
        )
        .await
        .unwrap();

    let params = json!({
        "source": {"app":"crawl","dataset":"pages"},
        "rules": {
            "products": {
                "type": "each", "selector": ".card", "container": "#listing",
                "fields": {
                    "name":  {"type":"css","selector":"h3"},
                    "price": {"type":"css","selector":".price"},
                    "badge": {"type":"css","selector":".badge"}
                }
            }
        }
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();

    // THE REFUTED BEHAVIOR: one document, one field, and it matched — so the
    // document-scoped tallies still report a flawless run.
    assert_eq!(out["fields_matched"], 1, "{out}");
    assert_eq!(out["fields_total"], 1, "{out}");
    assert_eq!(out["records"][0]["products"][0]["price"], Value::Null);

    // ...and `worst_fields` is no longer empty: it names the dead inner field.
    let worst = out["worst_fields"].as_array().expect("worst_fields array");
    let price = worst
        .iter()
        .find(|w| w["field"] == "products.price")
        .unwrap_or_else(|| panic!("products.price must be reported: {out}"));
    assert_eq!(price["scope"], "item");
    assert_eq!(price["items"], 3);
    assert_eq!(price["misses"], 3);
    assert_eq!(price["miss_rate"], 1.0);
    assert_eq!(price["dead"], true);

    // The sparse badge is reported as missing on 2 of 3 cards, but NOT dead.
    let badge = worst
        .iter()
        .find(|w| w["field"] == "products.badge")
        .unwrap_or_else(|| panic!("products.badge must be reported: {out}"));
    assert_eq!(badge["misses"], 2);
    assert_eq!(badge["dead"], false);
    // A healthy inner field never enters the list.
    assert!(worst.iter().all(|w| w["field"] != "products.name"), "{out}");
    // The dead field sorts ahead of the sparse one (most misses first).
    assert_eq!(worst[0]["field"], "products.price");
}

#[tokio::test]
async fn a_healthy_listing_reports_no_worst_fields() {
    let store = TempStore::new("extract-listing-ok").await;
    let root = store.path().to_path_buf();
    let datasets = store.datasets();

    let crawl_job = Uuid::new_v4().to_string();
    let crawl_dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&crawl_dir).await.unwrap();
    tokio::fs::write(
        crawl_dir.join("page.html"),
        listing_page("price").as_bytes(),
    )
    .await
    .unwrap();
    datasets
        .upsert_many(
            "crawl",
            "pages",
            &[(
                "http://shop/list".into(),
                json!({"url":"http://shop/list","artifact_path":"page.html","job_id":crawl_job}),
            )],
        )
        .await
        .unwrap();

    let params = json!({
        "source": {"app":"crawl","dataset":"pages"},
        "rules": {
            "products": {
                "type": "each", "selector": ".card", "container": "#listing",
                "fields": {
                    "name":  {"type":"css","selector":"h3"},
                    "price": {"type":"css","selector":".price"}
                }
            }
        }
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    assert_eq!(out["records"][0]["products"][0]["price"], "$00");
    assert_eq!(
        out["worst_fields"].as_array().map(Vec::len),
        Some(0),
        "a working listing must stay silent: {out}"
    );
}
