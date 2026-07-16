//! End-to-end test for the extractor's `source` mode (the crawl → extract seam).
//! Builds a real temp-dir SQLite + `Datasets`, seeds `pages`-style records with
//! `artifact_path` + `job_id`, writes a body to the origin job's artifacts dir,
//! then runs the app and asserts it extracts from the stored body (never
//! fetching) and reports missing/unreadable artifacts per key.

use std::sync::Arc;

use app_extractor::Extractor;
use async_trait::async_trait;
use pumper_core::config::{FetcherConfig, StorageConfig};
use pumper_core::{
    AppContext, Browser, CostLedger, Datasets, EngineSet, Fetcher, HttpClient, HttpRequest,
    HttpResponse, NoPlugins, NoProgress, RenderRequest, RenderedPage, ResearchCache, ResearchOutput,
    ResearchRequest, Researcher, Result, ScrapeApp, SpentTotal, Storage, TierMemory,
};
use serde_json::{json, Value};
use uuid::Uuid;

// Source mode never fetches, so the engines are stubs that must never be called.
struct DeadHttp;
#[async_trait]
impl HttpClient for DeadHttp {
    async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
        panic!("source mode must not fetch over HTTP")
    }
}
struct DeadBrowser;
#[async_trait]
impl Browser for DeadBrowser {
    async fn render(&self, _req: RenderRequest) -> Result<RenderedPage> {
        panic!("source mode must not render")
    }
}
struct DeadResearcher;
#[async_trait]
impl Researcher for DeadResearcher {
    async fn research(&self, _req: ResearchRequest) -> Result<ResearchOutput> {
        panic!("source mode must not research")
    }
}

async fn ctx_with(root: std::path::PathBuf, storage: &Storage, params: Value) -> AppContext {
    let pool = storage.pool();
    let engines = Arc::new(EngineSet {
        http: Arc::new(DeadHttp),
        browser: Arc::new(DeadBrowser),
        claude: Arc::new(DeadResearcher),
        fetch: Fetcher::new(
            Arc::new(DeadHttp),
            Arc::new(DeadBrowser),
            Arc::new(DeadResearcher),
            &FetcherConfig::default(),
        ),
    });
    let job_id = Uuid::new_v4();
    AppContext {
        job_id,
        app: "extractor".into(),
        params,
        engines,
        datasets: Arc::new(Datasets::new(pool.clone())),
        costs: Arc::new(CostLedger::new(pool.clone())),
        budget_usd: None,
        spent_usd: Arc::new(SpentTotal::default()),
        research_cache: Arc::new(ResearchCache::new(pool.clone(), 3600)),
        tiers: Arc::new(TierMemory::new(pool.clone(), 3600)),
        plugins: Arc::new(NoPlugins),
        progress: Arc::new(NoProgress),
        // Must be `<root>/extractor/<job_id>` so the app resolves the shared
        // artifacts root two levels up.
        artifacts_dir: root.join("extractor").join(job_id.to_string()),
    }
}

#[tokio::test]
async fn source_mode_extracts_stored_bodies_and_reports_missing() {
    let root = std::env::temp_dir().join(format!("pumper-extract-{}", Uuid::new_v4()));
    let cfg = StorageConfig {
        database_path: root.join("pumper.db"),
        artifacts_dir: root.join("artifacts-unused"),
    };
    let storage = Storage::connect(&cfg).await.expect("connect + migrate");
    let pool = storage.pool();
    let datasets = Datasets::new(pool.clone());

    // The origin crawl job wrote one body to disk under its per-job dir.
    let crawl_job = Uuid::new_v4().to_string();
    let crawl_dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&crawl_dir).await.unwrap();
    tokio::fs::write(crawl_dir.join("page-0001.html"), b"<html><h1>Hello World</h1></html>")
        .await
        .unwrap();

    // Seed pages: (a) present body, (b) body path points at a missing file,
    // (c) record has no artifact_path. Key = canonical URL, as the crawl writes.
    datasets
        .upsert_many(
            "crawl",
            "pages",
            &[
                (
                    "http://a".into(),
                    json!({"url":"http://a","artifact_path":"page-0001.html","job_id":crawl_job}),
                ),
                (
                    "http://b".into(),
                    json!({"url":"http://b","artifact_path":"page-9999.html","job_id":crawl_job}),
                ),
                ("http://c".into(), json!({"url":"http://c","job_id":crawl_job})),
            ],
        )
        .await
        .unwrap();

    // Explicit keys, including one (`http://d`) with no record at all.
    let params = json!({
        "source": {"app":"crawl","dataset":"pages",
                   "keys":["http://a","http://b","http://c","http://d"]},
        "rules": {"headline": {"type":"css","selector":"h1"}}
    });
    let out = Extractor.run(ctx_with(root.clone(), &storage, params).await).await.unwrap();

    assert_eq!(out["mode"], "source");
    assert_eq!(out["requested"], 4);
    assert_eq!(out["loaded"], 1, "only http://a has a readable body: {out}");
    assert_eq!(out["missing"], 3, "b unreadable, c no artifact_path, d no record: {out}");
    // The one loaded doc extracted from the STORED body (engines would panic).
    assert_eq!(out["records"][0]["headline"], "Hello World");
    assert_eq!(out["records"][0]["_url"], "http://a");
    assert_eq!(out["new"], 1);
    assert_eq!(out["fields_matched"], 1);
    assert_eq!(out["fields_total"], 1);
    // Missing reasons are attributed per key.
    let missing: Vec<String> =
        out["missing_keys"].as_array().unwrap().iter().map(|m| m["key"].as_str().unwrap().into()).collect();
    assert!(missing.contains(&"http://b".to_string()));
    assert!(missing.contains(&"http://c".to_string()));
    assert!(missing.contains(&"http://d".to_string()));

    // The extracted fields landed in the `extracted` dataset under the extractor app.
    let stored = datasets.get("extractor", "extracted", "http://a").await.unwrap().unwrap();
    assert_eq!(stored.data["headline"], "Hello World");
}

#[tokio::test]
async fn source_mode_without_keys_sweeps_live_records() {
    let root = std::env::temp_dir().join(format!("pumper-extract-{}", Uuid::new_v4()));
    let cfg = StorageConfig {
        database_path: root.join("pumper.db"),
        artifacts_dir: root.join("artifacts-unused"),
    };
    let storage = Storage::connect(&cfg).await.expect("connect + migrate");
    let pool = storage.pool();
    let datasets = Datasets::new(pool.clone());

    let crawl_job = Uuid::new_v4().to_string();
    let crawl_dir = root.join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&crawl_dir).await.unwrap();
    tokio::fs::write(crawl_dir.join("p.html"), b"<h1>Only</h1>").await.unwrap();

    datasets
        .upsert_many(
            "crawl",
            "pages",
            &[(
                "http://only".into(),
                json!({"url":"http://only","artifact_path":"p.html","job_id":crawl_job}),
            )],
        )
        .await
        .unwrap();

    // No keys, no trigger → sweep all live records.
    let params = json!({
        "source": {"app":"crawl","dataset":"pages"},
        "rules": {"h": {"type":"css","selector":"h1"}}
    });
    let out = Extractor.run(ctx_with(root, &storage, params).await).await.unwrap();
    assert_eq!(out["requested"], 1);
    assert_eq!(out["loaded"], 1);
    assert_eq!(out["records"][0]["h"], "Only");
}
