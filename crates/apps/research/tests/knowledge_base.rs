//! N25: a research run is a knowledge-base write, not just an answer.
//!
//! These are the gates the card asks for, driven end to end through `run()`
//! with the scripted researcher and a scripted HTTP engine — no `claude`
//! subprocess, no network:
//!
//! 1. a structured run produces N findings + M sources, each provenance-stamped;
//! 2. a second run on the same topic UPDATES those keys instead of duplicating
//!    them (the resume/follow-up path, where the query text differs);
//! 3. `snapshot_sources` archives each citation, stamps `artifact_sha`, and the
//!    fetch lands on the job's cost ledger as spend;
//! 4. the per-run source cap bites and SAYS so.

use std::collections::HashMap;
use std::sync::Arc;

use app_research::Research;
use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
use pumper_core::{
    AppContext, Browser, Error, HttpClient, HttpRequest, HttpResponse, RenderRequest, RenderedPage,
    Researcher, ScrapeApp,
};
use serde_json::{json, Value};

/// A site that serves one HTML body per URL and counts what was asked for. A
/// URL it does not know 404s — a snapshot of a dead citation is a real case.
struct StubSite {
    pages: HashMap<String, String>,
    hits: std::sync::Mutex<Vec<String>>,
}

impl StubSite {
    fn new(pages: &[(&str, &str)]) -> Self {
        Self {
            pages: pages
                .iter()
                .map(|(u, b)| ((*u).to_string(), (*b).to_string()))
                .collect(),
            hits: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn hits(&self) -> Vec<String> {
        self.hits.lock().expect("stub site lock").clone()
    }
}

#[async_trait]
impl HttpClient for StubSite {
    async fn fetch(&self, req: HttpRequest) -> pumper_core::Result<HttpResponse> {
        self.hits
            .lock()
            .expect("stub site lock")
            .push(req.url.clone());
        match self.pages.get(&req.url) {
            Some(body) => Ok(HttpResponse {
                status: 200,
                headers: HashMap::from([("content-type".into(), "text/html".into())]),
                body: body.clone(),
                final_url: req.url.clone(),
                cache_hit: false,
            }),
            None => Ok(HttpResponse {
                status: 404,
                headers: HashMap::new(),
                body: "<html><body>not found</body></html>".into(),
                final_url: req.url,
                cache_hit: false,
            }),
        }
    }
}

/// A browser that is present but broken. `Dead` PANICS on a render, which is
/// right for a write-path test but wrong here: the tiered fetcher legitimately
/// climbs to the browser when the HTTP body is thin, and what this test wants to
/// observe is what the app does when the whole ladder gives up on a citation.
struct BrokenBrowser;

#[async_trait]
impl Browser for BrokenBrowser {
    async fn render(&self, _: RenderRequest) -> pumper_core::Result<RenderedPage> {
        Err(Error::Browser("no chrome in this test".into()))
    }
}

const ONE: &str = "https://a.example/one";
const TWO: &str = "https://b.example/two";

/// A report citing both sources, as the agent would return it.
fn report_json(findings: &[&str]) -> String {
    json!({
        "summary": "Two thresholds moved.",
        "key_findings": findings,
        "sources": [
            {"url": ONE, "title": "One"},
            {"url": TWO, "title": "Two"}
        ]
    })
    .to_string()
}

/// A page whose extracted text clears `[fetcher] min_content_chars` (250).
fn one_page() -> String {
    format!(
        "<html><body><h1>One</h1><p>The threshold is 2M CZK. {}</p></body></html>",
        "Registration is compulsory past it. ".repeat(10)
    )
}

fn scripted(text: String) -> Arc<dyn Researcher> {
    Arc::new(pumper_core::testing::ScriptedResearcher::new().always_text(text))
}

fn ctx(store: &TempStore, params: Value, researcher: Arc<dyn Researcher>) -> AppContext {
    TestContext::new(&store.storage, "research")
        .params(params)
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), researcher))
        .build()
}

fn ctx_with_site(
    store: &TempStore,
    params: Value,
    researcher: Arc<dyn Researcher>,
    site: Arc<StubSite>,
) -> AppContext {
    TestContext::new(&store.storage, "research")
        .params(params)
        .engines(engines_with(site, Arc::new(BrokenBrowser), researcher))
        .build()
}

#[tokio::test]
async fn a_structured_run_writes_provenance_stamped_findings_and_sources() {
    let store = TempStore::new("research-kb-writes").await;
    let researcher = scripted(report_json(&["VAT threshold is 2M CZK", "It rose in 2023"]));
    let out = Research::default()
        .run(ctx(
            &store,
            json!({"query": "Czech VAT thresholds"}),
            researcher,
        ))
        .await
        .expect("a shaped reply completes the run");

    assert_eq!(out["structured"], json!(true), "{out:#}");
    assert_eq!(out["topic"], json!("Czech VAT thresholds"));
    assert_eq!(
        out["datasets"]["findings"],
        json!(["czech-vat-thresholds#0", "czech-vat-thresholds#1"]),
        "{out:#}"
    );
    assert_eq!(out["datasets"]["sources"], json!([ONE, TWO]));
    assert_eq!(out["datasets"]["findings_new"], json!(2));
    assert_eq!(out["datasets"]["sources_new"], json!(2));
    assert_eq!(out["datasets"]["error"], Value::Null);
    // The two datasets are declared for indexing, so search and saved-search
    // alerts see the corpus the run just wrote.
    assert_eq!(
        out["index_datasets"],
        json!([
            {"app": "research", "dataset": "findings"},
            {"app": "research", "dataset": "sources"}
        ])
    );

    let datasets = store.datasets();
    // Findings: real records, and every revision names the producing job.
    let rec = datasets
        .get("research", "findings", "czech-vat-thresholds#0")
        .await
        .unwrap()
        .expect("finding #0 was written");
    assert_eq!(rec.data["finding"], json!("VAT threshold is 2M CZK"));
    assert_eq!(rec.data["topic"], json!("Czech VAT thresholds"));
    assert_eq!(rec.data["sources"], json!([ONE, TWO]));

    let rev = datasets
        .history("research", "findings", "czech-vat-thresholds#0", 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("one revision for a new record");
    assert!(
        rev.provenance.job_id.is_some(),
        "the producing job is known"
    );

    // Sources: keyed by URL, stamped with the URL they are about, and NOT
    // claiming an archived body nobody fetched.
    let rev = datasets
        .history("research", "sources", ONE, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("one revision per cited source");
    assert_eq!(rev.provenance.source_url.as_deref(), Some(ONE));
    assert_eq!(rev.provenance.artifact_sha, None);
    assert!(rev.provenance.job_id.is_some());

    // Nothing was fetched and nothing was proposed: both side effects are off.
    assert_eq!(
        out["snapshots"],
        json!({"attempted": 0, "saved": 0, "failed": 0})
    );
    assert_eq!(out["watch_requests"], json!([]));
    assert_eq!(out["sources_truncated"], json!(false));
}

#[tokio::test]
async fn a_second_run_on_the_same_topic_updates_the_keys_instead_of_duplicating_them() {
    // The follow-up path: a different QUESTION against the same body of
    // knowledge. Without `topic` the run would fork a second copy of the
    // findings under its own slug, which is exactly the failure this pins.
    let store = TempStore::new("research-kb-update").await;
    let first = Research::default()
        .run(ctx(
            &store,
            json!({"query": "Czech VAT thresholds"}),
            scripted(report_json(&["threshold is 2M CZK", "it rose in 2023"])),
        ))
        .await
        .unwrap();
    assert_eq!(first["datasets"]["findings_new"], json!(2));

    let second = Research::default()
        .run(ctx(
            &store,
            json!({
                "query": "Source https://a.example/one changed: the number moved. Update the findings.",
                "topic": "Czech VAT thresholds",
                "session_id": "sess-1"
            }),
            scripted(report_json(&["threshold is 2.5M CZK", "it rose in 2023"])),
        ))
        .await
        .unwrap();

    assert_eq!(second["resumed"], json!(true));
    // Same keys, not new ones.
    assert_eq!(
        second["datasets"]["findings"],
        first["datasets"]["findings"]
    );
    assert_eq!(second["datasets"]["findings_new"], json!(0), "{second:#}");
    assert_eq!(second["datasets"]["findings_changed"], json!(1));
    // The unchanged finding is not churned, and the sources are unchanged too.
    assert_eq!(second["datasets"]["sources_new"], json!(0));
    assert_eq!(second["datasets"]["sources_changed"], json!(0));

    // The store holds ONE record per key across both runs.
    let page = store
        .datasets()
        .list("research", "findings", 100)
        .await
        .unwrap();
    assert_eq!(page.len(), 2, "two runs must not make four findings");
    let updated = store
        .datasets()
        .get("research", "findings", "czech-vat-thresholds#0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.data["finding"], json!("threshold is 2.5M CZK"));
}

#[tokio::test]
async fn snapshot_sources_archives_the_citation_stamps_its_sha_and_spends_on_the_job() {
    let store = TempStore::new("research-kb-snapshot").await;
    // ONE is a real page (comfortably past the fetcher's 250-char content
    // floor, so the HTTP tier wins outright); TWO is a 200 that renders to
    // nothing, which is how a dead citation actually presents.
    let site = Arc::new(StubSite::new(&[
        (ONE, &one_page()),
        (TWO, "<html><body>   </body></html>"),
    ]));
    let out = Research::default()
        .run(ctx_with_site(
            &store,
            json!({"query": "Czech VAT thresholds", "snapshot_sources": true}),
            scripted(report_json(&["threshold is 2M CZK"])),
            site.clone(),
        ))
        .await
        .unwrap();

    // Both citations were attempted; the live one saved, the dead one failed —
    // and a dead citation never fails the paid-for run.
    assert_eq!(out["snapshots"]["attempted"], json!(2), "{out:#}");
    assert_eq!(out["snapshots"]["saved"], json!(1));
    assert_eq!(out["snapshots"]["failed"], json!(1));
    assert_eq!(site.hits().len(), 2);

    let datasets = store.datasets();
    let rec = datasets
        .get("research", "sources", ONE)
        .await
        .unwrap()
        .unwrap();
    let artifact = rec.data["snapshot"]["artifact"]
        .as_str()
        .expect("the saved snapshot names its artifact");
    let sha = rec.data["snapshot"]["sha256"].as_str().unwrap().to_string();

    // The stamp on the revision is the same hash, so the citation is
    // re-derivable from the archived body rather than from a promise.
    let rev = datasets
        .history("research", "sources", ONE, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(rev.provenance.artifact_sha.as_deref(), Some(sha.as_str()));

    // …and the artifact really is on disk with that hash.
    let job_id = rev.provenance.job_id.clone().expect("job id");
    let path = store
        .storage
        .artifacts_dir
        .join("research")
        .join("job")
        .join(artifact);
    let body = tokio::fs::read(&path)
        .await
        .unwrap_or_else(|e| panic!("snapshot artifact {path:?} must exist: {e}"));
    assert_eq!(sha256_hex(&body), sha, "the stamped sha verifies");
    assert!(String::from_utf8_lossy(&body).contains("2M CZK"));

    // The dead citation records WHY, and claims no archive.
    let dead = datasets
        .get("research", "sources", TWO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dead.data["snapshot"], Value::Null);
    assert!(
        dead.data["snapshot_error"].as_str().is_some(),
        "{:#}",
        dead.data
    );
    let dead_rev = datasets
        .history("research", "sources", TWO, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(dead_rev.provenance.artifact_sha, None);

    // The snapshot fetches are metered on the job, not free-riding outside the
    // ledger the way the agent's own page reads used to.
    let events = pumper_core::CostLedger::new(store.storage.pool())
        .job_events(job_id.parse().expect("provenance job id is a uuid"))
        .await
        .unwrap();
    assert!(
        events.iter().any(|e| e.url.as_deref() == Some(ONE)),
        "the snapshot fetch is metered on the job that made it: {events:#?}"
    );
}

#[tokio::test]
async fn the_source_cap_bites_and_says_so_instead_of_silently_dropping() {
    let store = TempStore::new("research-kb-cap").await;
    let out = Research::default()
        .run(ctx(
            &store,
            json!({
                "query": "Czech VAT thresholds",
                "watch_sources": true,
                "max_watched_sources": 1
            }),
            scripted(report_json(&["threshold is 2M CZK"])),
        ))
        .await
        .unwrap();

    assert_eq!(out["sources_truncated"], json!(true), "{out:#}");
    assert_eq!(out["datasets"]["sources"], json!([ONE]));
    assert_eq!(
        out["watch_requests"],
        json!([{ "app": "watch", "cron": "0 0 7 * * *", "params": { "url": ONE } }]),
        "one ready-to-POST /schedules body per capped source"
    );
    // The cap governs every per-source action, so the second URL has no record
    // either — one meaning for `sources_truncated`.
    assert!(store
        .datasets()
        .get("research", "sources", TWO)
        .await
        .unwrap()
        .is_none());
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}
