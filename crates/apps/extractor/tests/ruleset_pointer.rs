//! Stage 2 of the repair plane, through the REAL extractor: a source enrolled
//! on the ruleset pointer (`rules_ref`), its pinned bodies, and the per-row
//! replayable stamps an enrolled URL-mode run writes — plus the proof that a
//! source that was never enrolled runs exactly as before.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use app_extractor::Extractor;
use async_trait::async_trait;
use pumper_core::datasets::{body_sha256, canonical_rules_hash, pinned_body_path, rules_hash};
use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
use pumper_core::{
    AppContext, Datasets, HealthStore, HttpClient, HttpRequest, HttpResponse, Result, RuleSet,
    ScrapeApp,
};
use serde_json::{json, Value};

const SOURCE: &str = "extractor/d";

/// Serves a mutable map of `url -> body`; anything else is a 404.
#[derive(Default)]
struct Site {
    pages: Mutex<HashMap<String, String>>,
}

impl Site {
    fn with(pages: &[(&str, &str)]) -> Arc<Self> {
        let site = Arc::new(Self::default());
        site.set(pages);
        site
    }

    fn set(&self, pages: &[(&str, &str)]) {
        let mut map = self.pages.lock().unwrap();
        map.clear();
        for (u, b) in pages {
            map.insert((*u).to_string(), (*b).to_string());
        }
    }
}

#[async_trait]
impl HttpClient for Site {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let body = self.pages.lock().unwrap().get(&req.url).cloned();
        Ok(HttpResponse {
            status: if body.is_some() { 200 } else { 404 },
            headers: HashMap::new(),
            body: body.unwrap_or_default(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

fn rules() -> Value {
    json!({
        "title": {"type": "css", "selector": "h1"},
        "link": {"type": "css", "selector": "a", "attr": "href",
                 "transforms": [{"op": "url_absolute"}]}
    })
}

fn page(title: &str) -> String {
    format!("<html><body><h1>{title}</h1><a href=\"/item/{title}\">x</a></body></html>")
}

fn ctx(store: &TempStore, site: &Arc<Site>, params: Value) -> AppContext {
    TestContext::new(&store.storage, "extractor")
        .params(params)
        .engines(engines_with(site.clone(), Arc::new(Dead), Arc::new(Dead)))
        .build()
}

fn url_params(rules_key: &str, rules_val: Value, urls: &[&str]) -> Value {
    json!({ rules_key: rules_val, "urls": urls, "dataset": "d", "records_echo": 0 })
}

/// Enrols `extractor/d` on `rules()` exactly as `POST /sources/{id}/rules/import`
/// does: canonical registration, then the first head.
async fn enrol(store: &TempStore) -> String {
    HealthStore::new(store.storage.pool(), 1)
        .ensure_source("extractor", "d")
        .await
        .unwrap();
    let ds = store.datasets();
    let parsed: RuleSet = serde_json::from_value(rules()).unwrap();
    let hash = ds.register_ruleset(&parsed).await.unwrap();
    ds.import_ruleset(SOURCE, &hash).await.unwrap();
    hash
}

/// Every record of `extractor/d` as `(key, stored JSON text)`, key-ordered.
async fn record_data(store: &TempStore) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = store
        .datasets()
        .list("extractor", "d", 1000)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.key, r.data.to_string()))
        .collect();
    rows.sort();
    rows
}

async fn pins(store: &TempStore) -> i64 {
    store.datasets().body_pin_count(SOURCE).await.unwrap()
}

/// Every revision of `extractor/d/<key>`, oldest first.
async fn revisions(store: &TempStore, key: &str) -> Vec<pumper_core::Revision> {
    let mut revs = store
        .datasets()
        .history("extractor", "d", key, 100)
        .await
        .unwrap();
    revs.reverse();
    revs
}

fn root(store: &TempStore) -> std::path::PathBuf {
    store.storage.artifacts_dir.clone()
}

#[tokio::test]
async fn an_imported_ruleset_by_reference_extracts_byte_identical_records() {
    let store = TempStore::new("ptr-identical").await;
    let site = Site::with(&[
        ("https://s.test/a", &page("a")),
        ("https://s.test/b", &page("b")),
    ]);
    let urls = ["https://s.test/a", "https://s.test/b"];

    let inline = Extractor
        .run(ctx(&store, &site, url_params("rules", rules(), &urls)))
        .await
        .unwrap();
    assert_eq!(inline["new"], 2, "{inline}");
    let before = record_data(&store).await;

    enrol(&store).await;
    let by_ref = Extractor
        .run(ctx(
            &store,
            &site,
            url_params("rules_ref", json!("active"), &urls),
        ))
        .await
        .unwrap();
    assert_eq!(
        (
            by_ref["new"].clone(),
            by_ref["changed"].clone(),
            by_ref["unchanged"].clone()
        ),
        (json!(0), json!(0), json!(2)),
        "switching to the pointer must not change one record: {by_ref}"
    );
    assert_eq!(record_data(&store).await, before, "records byte-identical");
    assert_eq!(
        inline["rules_hash"], by_ref["rules_hash"],
        "inline and referenced runs of one ruleset stamp one hash"
    );
}

#[tokio::test]
async fn an_enrolled_url_run_stamps_every_revision_replayable() {
    let store = TempStore::new("ptr-stamped").await;
    let head = enrol(&store).await;
    let site = Site::with(&[
        ("https://s.test/a", &page("a")),
        ("https://s.test/b", &page("b")),
    ]);
    let out = Extractor
        .run(ctx(
            &store,
            &site,
            url_params(
                "rules_ref",
                json!("active"),
                &["https://s.test/a", "https://s.test/b"],
            ),
        ))
        .await
        .unwrap();
    assert_eq!(out["rules_hash"], json!(head), "{out}");
    assert_eq!(out["enrolled"]["bodies_pinned"], 2, "{out}");

    // A changed body appends a revision, and it is stamped too.
    site.set(&[
        ("https://s.test/a", &page("a2")),
        ("https://s.test/b", &page("b")),
    ]);
    Extractor
        .run(ctx(
            &store,
            &site,
            url_params(
                "rules_ref",
                json!("active"),
                &["https://s.test/a", "https://s.test/b"],
            ),
        ))
        .await
        .unwrap();

    let mut revs = revisions(&store, "https://s.test/a").await;
    revs.extend(revisions(&store, "https://s.test/b").await);
    let changes: Vec<&str> = revs.iter().map(|r| r.change.as_str()).collect();
    assert_eq!(
        changes,
        vec!["new", "changed", "new"],
        "a:new, a:changed, b:new"
    );
    for rev in &revs {
        let p = &rev.provenance;
        assert_eq!(
            p.source_url.as_deref(),
            Some(rev.key.as_str()),
            "each row names ITS url"
        );
        assert_eq!(p.rules_hash.as_deref(), Some(head.as_str()));
        assert!(p.replayable(), "{p:?}");
        let sha = p
            .artifact_sha
            .as_deref()
            .expect("artifact_sha on every enrolled revision");
        let body =
            std::fs::read_to_string(pinned_body_path(&root(&store), "extractor", sha).unwrap())
                .expect("the cited body is on disk");
        assert_eq!(body_sha256(&body), sha);
    }
}

#[tokio::test]
async fn two_documents_with_one_body_pin_one_file_and_two_observations() {
    let store = TempStore::new("ptr-dedup").await;
    enrol(&store).await;
    let same = page("same");
    let site = Site::with(&[("https://s.test/a", &same), ("https://s.test/b", &same)]);
    let out = Extractor
        .run(ctx(
            &store,
            &site,
            url_params(
                "rules_ref",
                json!("active"),
                &["https://s.test/a", "https://s.test/b"],
            ),
        ))
        .await
        .unwrap();
    assert_eq!(out["enrolled"]["bodies_pinned"], 2, "{out}");
    assert_eq!(out["enrolled"]["bodies_written"], 1, "{out}");
    assert_eq!(pins(&store).await, 2, "observations are never deduplicated");
    let files = std::fs::read_dir(root(&store).join("extractor").join("bodies"))
        .unwrap()
        .count();
    assert_eq!(files, 1, "bytes are deduplicated by sha");
}

#[tokio::test]
async fn an_old_raw_hash_still_resolves_after_the_switch() {
    let store = TempStore::new("ptr-raw-hash").await;
    let ds = store.datasets();
    // A revision stamped before the switch pinned the raw-params hash.
    let raw = ds.register_rules(&rules()).await.unwrap();
    assert_eq!(raw, rules_hash(&rules()));

    let site = Site::with(&[("https://s.test/a", &page("a"))]);
    let out = Extractor
        .run(ctx(
            &store,
            &site,
            url_params("rules", rules(), &["https://s.test/a"]),
        ))
        .await
        .unwrap();
    let canonical = out["rules_hash"].as_str().unwrap().to_string();
    assert_ne!(canonical, raw, "the new stamp is the canonical identity");

    // The old stamp still resolves, to the SAME ruleset the new one names.
    let old = ds
        .rules_by_hash(&raw)
        .await
        .unwrap()
        .expect("old raw hash resolvable");
    assert_eq!(old, rules());
    let old: RuleSet = serde_json::from_value(old).unwrap();
    assert_eq!(canonical_rules_hash(&old), canonical);
    assert!(ds.rules_by_hash(&canonical).await.unwrap().is_some());
}

#[tokio::test]
async fn a_never_enrolled_source_runs_exactly_as_before() {
    let store = TempStore::new("ptr-untouched").await;
    let site = Site::with(&[
        ("https://s.test/a", &page("a")),
        ("https://s.test/b", &page("b")),
    ]);
    let out = Extractor
        .run(ctx(
            &store,
            &site,
            url_params("rules", rules(), &["https://s.test/a", "https://s.test/b"]),
        ))
        .await
        .unwrap();
    assert!(out.get("enrolled").is_none(), "{out}");
    // One ruleset, one identity: even a never-enrolled run stamps the
    // canonical hash (Director decision) — only the stamp moved, not the values.
    let parsed: RuleSet = serde_json::from_value(rules()).unwrap();
    assert_eq!(out["rules_hash"], json!(canonical_rules_hash(&parsed)));
    assert_eq!(pins(&store).await, 0);
    assert!(!root(&store).join("extractor").join("bodies").exists());
    for key in ["https://s.test/a", "https://s.test/b"] {
        let revs = revisions(&store, key).await;
        assert_eq!(revs.len(), 1);
        let p = &revs[0].provenance;
        assert_eq!(
            (p.source_url.as_deref(), p.artifact_sha.as_deref()),
            (None, None),
            "the batch stamp, unchanged: a multi-URL batch names no URL and no body"
        );
    }
}

#[tokio::test]
async fn rules_and_rules_ref_together_are_refused_not_ranked() {
    let store = TempStore::new("ptr-both").await;
    let site = Site::with(&[("https://s.test/a", &page("a"))]);
    let err = Extractor
        .run(ctx(
            &store,
            &site,
            json!({"rules": rules(), "rules_ref": "active", "urls": ["https://s.test/a"]}),
        ))
        .await
        .expect_err("two rule inputs, one job");
    assert!(err.to_string().contains("mutually exclusive"), "{err}");

    // A reference that does not resolve fails the job with a named error.
    let err = Extractor
        .run(ctx(
            &store,
            &site,
            json!({"rules_ref": "active", "urls": ["https://s.test/a"], "dataset": "d"}),
        ))
        .await
        .expect_err("nothing to resolve");
    let msg = err.to_string();
    assert!(
        msg.contains("rules_ref 'active'") && msg.contains(SOURCE),
        "{msg}"
    );
    let written = store
        .datasets()
        .record_count("extractor", "d")
        .await
        .unwrap();
    assert_eq!(
        written, 0,
        "a job that could not resolve its rules writes nothing"
    );
}

fn admits(params: &Value) -> bool {
    let schema = Extractor.manifest().params_schema.expect("params schema");
    jsonschema::validator_for(&schema)
        .expect("schema compiles")
        .is_valid(params)
}

#[test]
fn the_door_admits_rules_ref_and_refuses_it_beside_rules() {
    let urls = json!(["https://s.test/a"]);
    let source = json!({"app": "crawl", "dataset": "pages"});
    assert!(admits(&json!({"rules_ref": "active", "urls": urls})));
    assert!(admits(&json!({"rules_ref": "0123abcd", "source": source})));
    assert!(!admits(
        &json!({"rules": rules(), "rules_ref": "active", "urls": urls})
    ));
    assert!(!admits(
        &json!({"rules_ref": "active", "replay": {"rules": rules()}})
    ));
    assert!(!admits(&json!({"rules_ref": "active"})), "no input list");
    assert!(
        !admits(&json!({"rules_ref": "", "urls": urls})),
        "empty reference"
    );
}

#[tokio::test]
async fn replay_resolves_rules_refs_over_the_pinned_corpus() {
    let store = TempStore::new("ptr-replay").await;
    enrol(&store).await;
    let site = Site::with(&[
        ("https://s.test/a", &page("a")),
        ("https://s.test/b", &page("b")),
    ]);
    Extractor
        .run(ctx(
            &store,
            &site,
            url_params(
                "rules_ref",
                json!("active"),
                &["https://s.test/a", "https://s.test/b"],
            ),
        ))
        .await
        .unwrap();

    let out = Extractor
        .run(ctx(
            &store,
            &site,
            json!({"replay": {
                "rules": {"rules_ref": "active"},
                "baseline_rules": {"rules_ref": "active"},
                "against": {"source": SOURCE}
            }}),
        ))
        .await
        .unwrap();
    assert_eq!(out["docs"], 2, "{out}");
    assert_eq!(out["baseline"], true);
    assert_eq!(
        out["regressed_urls"], 0,
        "a ruleset replayed against itself: {out}"
    );
    let link = out["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["field"] == "link")
        .expect("link row");
    assert_eq!(link["match_rate"], 1.0);

    // The shared dry-run entrance agrees with the replay mode.
    let ds: Datasets = store.datasets();
    let parsed: RuleSet = serde_json::from_value(rules()).unwrap();
    let dry = app_extractor::replay_pinned_bodies(
        &ds,
        &root(&store),
        SOURCE,
        &parsed,
        Some(&parsed),
        500,
    )
    .await
    .unwrap();
    assert_eq!(dry["docs"], 2);
    assert_eq!(dry["regressed_urls"], 0);

    // A reference without any source to resolve in is refused by name.
    let err = Extractor
        .run(ctx(
            &store,
            &site,
            json!({"replay": {"rules": {"rules_ref": "active"}}}),
        ))
        .await
        .expect_err("no source");
    assert!(err.to_string().contains("needs a source"), "{err}");
}
