//! End-to-end guard: an extracted `href` leaves a real extractor run absolute.
//!
//! A listing scrape's whole output is links, and a relative link is a value that
//! means nothing once it leaves the page it came from — every downstream
//! consumer (crawl seeding, watch targets, peer mirrors, external clients) had
//! to re-derive a base the extractor already knew. These run the REAL app over
//! stored bodies, through the DEFAULT health-enabled path, because that path
//! (the fused extract+fingerprint parse) is the one that ships and the one that
//! carries no per-document URL of its own.

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

/// A listing whose links are written the four ways real markup writes them.
const LISTING: &str = r#"<html><body>
    <link rel="canonical" href="?page=1">
    <div id="listing">
      <div class="card"><a href="/item/1">one</a></div>
      <div class="card"><a href="../item/2">two</a></div>
      <div class="card"><a href="//cdn.shop.test/item/3">three</a></div>
      <div class="card"><a href="https://other.test/item/4">four</a></div>
    </div>
  </body></html>"#;

fn link_rules() -> Value {
    json!({
        "canonical": {"type": "css", "selector": "link", "attr": "href",
                      "transforms": [{"op": "url_absolute"}]},
        "products": {
            "type": "each", "selector": ".card", "container": "#listing",
            "fields": {
                "url": {"type": "css", "selector": "a", "attr": "href",
                        "transforms": [{"op": "url_absolute"}]}
            }
        }
    })
}

/// Stores one crawl page under `key`, returns the ready store + root.
async fn store_page(name: &str, key: &str, body: &str) -> (TempStore, std::path::PathBuf) {
    let store = TempStore::new(name).await;
    let root = store.path().to_path_buf();
    let crawl_job = Uuid::new_v4().to_string();
    let dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("page.html"), body.as_bytes())
        .await
        .unwrap();
    store
        .datasets()
        .upsert_many(
            "crawl",
            "pages",
            &[(
                key.to_string(),
                json!({"url": key, "artifact_path": "page.html", "job_id": crawl_job}),
            )],
        )
        .await
        .unwrap();
    (store, root)
}

#[tokio::test]
async fn extracted_hrefs_come_out_absolute_not_relative() {
    let (store, root) = store_page(
        "extract-url-absolute",
        "https://shop.test/cat/page",
        LISTING,
    )
    .await;

    let params = json!({
        "source": {"app": "crawl", "dataset": "pages"},
        "rules": link_rules(),
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();

    let urls: Vec<&str> = out["records"][0]["products"]
        .as_array()
        .unwrap_or_else(|| panic!("products array: {out}"))
        .iter()
        .map(|p| p["url"].as_str().unwrap())
        .collect();
    assert_eq!(
        urls,
        vec![
            "https://shop.test/item/1",
            "https://shop.test/item/2",
            "https://cdn.shop.test/item/3",
            "https://other.test/item/4",
        ],
        "{out}"
    );
    assert_eq!(
        out["records"][0]["canonical"],
        json!("https://shop.test/cat/page?page=1")
    );
    assert_eq!(out["base_url_missing"], 0, "{out}");

    // The health verdict is still produced: taking the base-carrying extraction
    // path must not cost the run its resilience signals.
    assert!(!out["health"].is_null(), "health verdict lost: {out}");
}

#[tokio::test]
async fn a_source_without_a_url_reports_the_missing_base_instead_of_lying() {
    // A source dataset keyed by id, not link: there IS no base, so the links
    // stay exactly as the page wrote them AND the run says so. The failure this
    // forbids is a `url` column that is relative on some runs and absolute on
    // others with nothing marking which.
    let (store, root) = store_page("extract-url-no-base", "sku-4471", LISTING).await;

    let params = json!({
        "source": {"app": "crawl", "dataset": "pages"},
        "rules": link_rules(),
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();

    assert_eq!(out["records"][0]["products"][0]["url"], json!("/item/1"));
    assert_eq!(
        out["records"][0]["products"][3]["url"],
        json!("https://other.test/item/4"),
        "an already-absolute link needs no base"
    );
    assert_eq!(out["base_url_missing"], 1, "{out}");
    // A no-op is not a miss: the selector still bound on every card.
    assert_eq!(out["fields_matched"], out["fields_total"], "{out}");
}

#[tokio::test]
async fn a_ruleset_without_url_absolute_never_reports_a_missing_base() {
    // The flag must stay silent for the rule sets that never asked — otherwise
    // it is noise, and noise is how honesty signals get ignored.
    let (store, root) = store_page("extract-url-unasked", "sku-9", LISTING).await;
    let params = json!({
        "source": {"app": "crawl", "dataset": "pages"},
        "rules": {"first": {"type": "css", "selector": ".card a", "attr": "href"}},
    });
    let out = Extractor
        .run(ctx_with(&root, &store, params))
        .await
        .unwrap();
    assert_eq!(out["base_url_missing"], 0, "{out}");
    assert_eq!(out["records"][0]["first"], json!("/item/1"));
}
