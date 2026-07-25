//! Integration test for extraction health against a real temp-dir SQLite with
//! the full migration chain.
//!
//! The unit tests in `resilience::detect` cover each signal in isolation; this
//! covers the parts only a database can answer: that a baseline accumulates from
//! `ok` runs and no others, that a source climbs the hysteresis ladder over
//! several runs and stops where it should, that fingerprints make the next run's
//! drift comparison possible, and that enforcement gates the write paths — trust
//! stamps, the shadow dataset, and the removal downgrade that keeps a degrading
//! source from tombstoning its own dataset.

use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::config::{FetcherConfig, GovernorConfig, ResilienceConfig, StorageConfig};
use pumper_core::extract::{CoercionStatus, DocReport, FieldStatus};
use pumper_core::resilience::store::Resilience;
use pumper_core::{
    doc_signals, AppContext, Browser, CostLedger, Datasets, EngineSet, FetchHealth, Fetcher,
    Governor, HttpClient, HttpRequest, HttpResponse, NoPlugins, NoProgress, ObservedDoc,
    RenderRequest, RenderedPage, ResearchCache, ResearchOutput, ResearchRequest, Researcher,
    Result as CoreResult, RunReport, RunVerdict, SourceState, SpentTotal, Storage, TierMemory,
};
use serde_json::{json, Value};
use uuid::Uuid;

async fn fresh_db(tag: &str) -> (Storage, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("pumper-{tag}-{}", Uuid::new_v4()));
    let cfg = StorageConfig {
        database_path: dir.join("pumper.db"),
        artifacts_dir: dir.join("artifacts"),
        ..StorageConfig::default()
    };
    let storage = Storage::connect(&cfg).await.expect("connect + migrate");
    (storage, dir)
}

/// Detection on, thresholds scaled down so a test cohort is a real cohort.
fn cfg() -> ResilienceConfig {
    ResilienceConfig {
        min_cohort_docs: 10,
        window_runs: 10,
        sketch_retention_runs: 30,
        // Mining needs a sample; the default 500-record support would never be met
        // by a test dataset.
        invariant_min_support: 10,
        ..ResilienceConfig::default()
    }
}

/// One product page. `price_class` is the class the price sits under, so a
/// "redesign" is a single argument change.
fn page(i: usize, price_class: &str, price: &str, words: &str) -> String {
    format!(
        "<html><body><div id=\"main\"><div class=\"card\">\
         <h1 class=\"title\">Product {i} {words}</h1>\
         <span class=\"{price_class}\">{price}</span>\
         <p class=\"desc\">A {words} description of product number {i} for sale.</p>\
         </div></div></body></html>"
    )
}

/// Extracts `price` and `title` from a document the way a real rule set would,
/// building the `(values, report)` pair the detector consumes.
fn extract(doc: &str, price_selector: &str) -> (Value, DocReport) {
    let html = scraper::Html::parse_document(doc);
    let pick = |selector: &str| -> Option<String> {
        let sel = scraper::Selector::parse(selector).unwrap();
        html.select(&sel).next().map(|el| el.text().collect::<String>().trim().to_string())
    };
    let title = pick(".title");
    let price = pick(price_selector);
    let mut report = DocReport::default();
    report.fields.insert(
        "title".into(),
        title.as_ref().map_or(FieldStatus::Empty, |_| FieldStatus::Matched),
    );
    report.fields.insert(
        "price".into(),
        price.as_ref().map_or(FieldStatus::Empty, |_| FieldStatus::Matched),
    );
    report.coercion.insert("title".into(), CoercionStatus::NoTransforms);
    report.coercion.insert("price".into(), CoercionStatus::NoTransforms);
    (json!({ "title": title, "price": price }), report)
}

/// A cohort of `n` documents, extracted with `price_selector` against pages whose
/// price lives under `price_class`.
fn cohort(n: usize, price_class: &str, price_selector: &str, words: &str) -> Vec<ObservedDoc> {
    (0..n)
        .map(|i| {
            let doc = page(i, price_class, &format!("${}.{:02}", 10 + i, (i * 7) % 100), words);
            let (values, report) = extract(&doc, price_selector);
            ObservedDoc {
                key: format!("http://shop.example/p/{i}"),
                signals: doc_signals(&doc, &values),
                values,
                report,
            }
        })
        .collect()
}

async fn observe(
    health: &Resilience,
    docs: &[ObservedDoc],
) -> pumper_core::SourceVerdict {
    health
        .observe(
            "extractor",
            &RunReport {
                job_id: Uuid::new_v4(),
                dataset: "products",
                docs,
                fetch: FetchHealth { attempted: docs.len() as u32, ok: docs.len() as u32 },
                build_id: Some("test".into()),
            },
        )
        .await
        .expect("observe")
        .expect("detection is enabled")
}

#[tokio::test]
async fn a_redesign_walks_the_source_down_the_ladder_and_a_fix_walks_it_back() {
    let (storage, dir) = fresh_db("resilience-ladder").await;
    let health = Resilience::new(storage.pool(), &cfg());

    // Three healthy runs build the baseline. The first can never trip — there is
    // nothing it stopped doing.
    for run in 0..3 {
        let v = observe(&health, &cohort(30, "price", ".price", "sturdy")).await;
        assert_eq!(v.verdict, RunVerdict::Ok, "healthy run {run}: {:?}", v.reasons);
        assert_eq!(v.state, SourceState::Healthy);
        assert!(v.statistical_coverage);
    }
    // Run 2 onwards has a previous run to compare against.
    let baseline_drift = observe(&health, &cohort(30, "price", ".price", "sturdy")).await;
    assert!(baseline_drift.drift.is_some(), "fingerprints must make drift computable");
    let d = baseline_drift.drift.unwrap();
    assert_eq!(d.compared, 30);
    assert!(d.text < 0.05 && d.dom < 0.05, "a repeat of the same pages must not drift: {d:?}");

    // The redesign: `.price` becomes `.amount`, the words are untouched. The old
    // selector now matches nothing.
    let broken = || cohort(30, "amount", ".price", "sturdy");

    let first = observe(&health, &broken()).await;
    assert_eq!(first.verdict, RunVerdict::Broken, "{:?}", first.reasons);
    // One tripped run reaches `suspect` and no further: suspect changes nothing
    // downstream, which is what makes it safe to enter on one bad run.
    assert_eq!(first.state, SourceState::Suspect);

    let second = observe(&health, &broken()).await;
    assert_eq!(second.state, SourceState::Degraded, "two of the last three trips it");

    let third = observe(&health, &broken()).await;
    assert_eq!(third.state, SourceState::Quarantined, "three consecutive earns quarantine");

    // The broken runs must NOT have entered the baseline they were judged against.
    let store = health.store().unwrap();
    let baseline = store.baseline("extractor/products", 10).await.unwrap();
    assert_eq!(baseline.runs("price"), 4, "only the four ok runs are baseline material");
    assert!(
        baseline.pooled_misses("price").0 == 0,
        "a broken run leaked into the baseline: {:?}",
        baseline.pooled_misses("price")
    );

    // Quarantine is terminal without an operator: a clean run does not release it.
    let fixed = observe(&health, &cohort(30, "amount", ".amount", "sturdy")).await;
    assert_eq!(fixed.verdict, RunVerdict::Ok);
    assert_eq!(fixed.state, SourceState::Quarantined, "quarantine must not self-release");

    // The operator releases it, and only then does the ladder resume.
    store
        .set_state_manual("extractor/products", SourceState::Healthy, "selector fixed")
        .await
        .unwrap();
    let after = observe(&health, &cohort(30, "amount", ".amount", "sturdy")).await;
    assert_eq!(after.state, SourceState::Healthy);

    // Every run is on the record with the tests that produced it.
    let runs = store.runs("extractor/products", 50).await.unwrap();
    assert_eq!(runs.len(), 9);
    assert!(runs.iter().all(|r| r.reasons.is_some()), "a verdict must explain itself");
    assert!(runs.iter().all(|r| r.build_id.as_deref() == Some("test")));

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_content_change_is_not_a_break() {
    let (storage, dir) = fresh_db("resilience-content").await;
    let health = Resilience::new(storage.pool(), &cfg());

    for _ in 0..4 {
        observe(&health, &cohort(30, "price", ".price", "sturdy")).await;
    }
    // Same markup, same selectors, completely different words and prices — the
    // negative control the whole design turns on.
    let changed = cohort(30, "price", ".price", "refurbished lightweight aluminium");
    let v = observe(&health, &changed).await;
    let d = v.drift.expect("comparable keys");
    assert!(d.text > 0.05, "the words really did move: {d:?}");
    assert!(d.dom < 0.05, "the markup really did not: {d:?}");
    assert_eq!(v.verdict, RunVerdict::Ok, "a content change must not read as a break: {:?}", v.reasons);
    assert_eq!(v.state, SourceState::Healthy);

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_bot_wall_run_changes_nothing_at_all() {
    let (storage, dir) = fresh_db("resilience-gate").await;
    let health = Resilience::new(storage.pool(), &cfg());
    for _ in 0..3 {
        observe(&health, &cohort(30, "price", ".price", "sturdy")).await;
    }
    let store = health.store().unwrap();
    let before = store.fingerprints("extractor/products", &["http://shop.example/p/0".into()])
        .await
        .unwrap();

    // Everything broke, but only 3 of 30 fetches actually delivered.
    let v = health
        .observe(
            "extractor",
            &RunReport {
                job_id: Uuid::new_v4(),
                dataset: "products",
                docs: &cohort(30, "amount", ".price", "bot wall challenge page"),
                fetch: FetchHealth { attempted: 30, ok: 3 },
                build_id: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v.verdict, RunVerdict::Inconclusive);
    assert_eq!(v.state, SourceState::Healthy, "the state must not move");
    assert_eq!(v.score, 0.0);

    // And the fingerprints must not have been rewritten: comparing tomorrow's
    // real page against today's bot wall would read as a redesign.
    let after = store
        .fingerprints("extractor/products", &["http://shop.example/p/0".into()])
        .await
        .unwrap();
    assert_eq!(before, after, "an unjudged run must not become the next run's reference");

    // The run is still recorded — soak mode's whole value is the record.
    let runs = store.runs("extractor/products", 10).await.unwrap();
    assert_eq!(runs[0].verdict, "inconclusive");
    assert!((runs[0].fetch_ok_rate - 0.1).abs() < 1e-9);

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn invariants_are_mined_from_live_records_and_then_checked() {
    let (storage, dir) = fresh_db("resilience-invariants").await;
    let datasets = Datasets::new(storage.pool());
    let health = Resilience::new(storage.pool(), &cfg());

    // A dataset with a rigid shape: every price is `$NN.NN`.
    let items: Vec<(String, Value)> = (0..30)
        .map(|i| {
            (
                format!("http://shop.example/p/{i}"),
                json!({ "title": format!("Product {i}"), "price": format!("${}.{:02}", 10 + i, i) }),
            )
        })
        .collect();
    datasets.upsert_many("extractor", "products", &items).await.unwrap();

    // A healthy run triggers mining (none have ever been mined for this source).
    observe(&health, &cohort(30, "price", ".price", "sturdy")).await;
    let store = health.store().unwrap();
    let mined = store.invariants("extractor/products").await.unwrap();
    assert!(!mined.is_empty(), "a rigid dataset must yield invariants");
    let price_kinds: Vec<&str> = mined
        .iter()
        .filter(|i| i.field == "price")
        .map(|i| i.kind.name())
        .collect();
    assert!(price_kinds.contains(&"regex"), "mined for price: {price_kinds:?}");
    assert!(price_kinds.contains(&"nonnull"), "mined for price: {price_kinds:?}");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

// ---- enforcement: what a degrading source must never do --------------------

/// Engines that must never be called — these tests only exercise the write paths.
struct Dead;
#[async_trait]
impl HttpClient for Dead {
    async fn fetch(&self, _: HttpRequest) -> CoreResult<HttpResponse> {
        panic!("no fetching in a write-path test")
    }
}
#[async_trait]
impl Browser for Dead {
    async fn render(&self, _: RenderRequest) -> CoreResult<RenderedPage> {
        panic!("no rendering in a write-path test")
    }
}
#[async_trait]
impl Researcher for Dead {
    async fn research(&self, _: ResearchRequest) -> CoreResult<ResearchOutput> {
        panic!("no research in a write-path test")
    }
}

fn ctx(storage: &Storage, health: Arc<Resilience>) -> AppContext {
    let pool = storage.pool();
    AppContext {
        job_id: Uuid::new_v4(),
        app: "extractor".into(),
        params: json!({}),
        engines: Arc::new(EngineSet {
            http: Arc::new(Dead),
            browser: Arc::new(Dead),
            claude: Arc::new(Dead),
            fetch: Fetcher::new(
                Arc::new(Dead),
                Arc::new(Dead),
                Arc::new(Dead),
                Arc::new(Governor::new(&GovernorConfig::default())),
                &FetcherConfig::default(),
            ),
        }),
        datasets: Arc::new(Datasets::new(pool.clone())),
        costs: Arc::new(CostLedger::new(pool.clone())),
        budget_usd: None,
        spent_usd: Arc::new(SpentTotal::default()),
        research_cache: Arc::new(ResearchCache::new(pool.clone(), 0)),
        tiers: Arc::new(TierMemory::new(pool.clone(), 0)),
        health,
        plugins: Arc::new(NoPlugins),
        progress: Arc::new(NoProgress),
        artifacts_dir: storage.artifacts_dir.join("extractor").join("job"),
    }
}

fn items(keys: &[&str]) -> Vec<(String, Value)> {
    keys.iter().map(|k| (k.to_string(), json!({ "id": k, "price": 10 }))).collect()
}

#[tokio::test]
async fn a_degrading_source_cannot_tombstone_its_own_dataset() {
    let (storage, dir) = fresh_db("resilience-tombstone").await;
    let enforcing = ResilienceConfig { enforce: true, ..cfg() };
    let health = Arc::new(Resilience::new(storage.pool(), &enforcing));
    let ctx = ctx(&storage, health.clone());
    let datasets = Datasets::new(storage.pool());

    // A healthy full snapshot of three keys.
    ctx.sync_many("products", &items(&["a", "b", "c"])).await.unwrap();

    // Still healthy: a genuine disappearance IS detected. Without this half the
    // test would pass on a system that never tombstones anything.
    let summary = ctx.sync_many("products", &items(&["a", "b"])).await.unwrap();
    assert_eq!(summary.removed, vec!["c".to_string()], "a healthy source must still detect removals");

    // The source degrades.
    health
        .store()
        .unwrap()
        .ensure_source("extractor", "products")
        .await
        .unwrap();
    health
        .store()
        .unwrap()
        .set_state_manual("extractor/products", SourceState::Degraded, "test")
        .await
        .unwrap();

    // A half-broken run returns a SHORT but non-empty batch. The empty-batch guard
    // does not cover this, and tombstoning `b` here would be silent data loss.
    let summary = ctx.sync_many("products", &items(&["a"])).await.unwrap();
    assert!(summary.removed.is_empty(), "a degrading source must not tombstone: {summary:?}");
    let b = datasets.get("extractor", "products", "b").await.unwrap().unwrap();
    assert!(b.removed_at.is_none(), "key b was tombstoned by a short broken batch");

    // And what it did write is stamped as not-stood-behind.
    let a = datasets.get("extractor", "products", "a").await.unwrap().unwrap();
    assert_eq!(a.trust, "provisional");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_quarantined_source_writes_to_the_shadow_dataset() {
    let (storage, dir) = fresh_db("resilience-quarantine").await;
    let health = Arc::new(Resilience::new(
        storage.pool(),
        &ResilienceConfig { enforce: true, ..cfg() },
    ));
    let ctx = ctx(&storage, health.clone());
    let datasets = Datasets::new(storage.pool());

    ctx.upsert_many("products", &items(&["a"])).await.unwrap();
    let store = health.store().unwrap();
    store.ensure_source("extractor", "products").await.unwrap();
    store
        .set_state_manual("extractor/products", SourceState::Quarantined, "test")
        .await
        .unwrap();

    ctx.upsert_many("products", &items(&["b"])).await.unwrap();

    // The live dataset is untouched by the quarantined run...
    assert!(datasets.get("extractor", "products", "b").await.unwrap().is_none());
    // ...and the write landed in the shadow dataset, which is an ordinary dataset
    // so every existing tool already works on it.
    let shadow = datasets.get("extractor", "products@q", "b").await.unwrap().unwrap();
    assert_eq!(shadow.trust, "quarantined");
    // The pre-quarantine record keeps its stamp.
    assert_eq!(datasets.get("extractor", "products", "a").await.unwrap().unwrap().trust, "stable");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn soak_mode_records_the_verdict_and_gates_nothing() {
    let (storage, dir) = fresh_db("resilience-soak").await;
    // The shipping default: detection on, enforcement off.
    let health = Arc::new(Resilience::new(storage.pool(), &cfg()));
    assert!(health.enabled() && !health.enforcing());
    let ctx = ctx(&storage, health.clone());
    let datasets = Datasets::new(storage.pool());

    ctx.sync_many("products", &items(&["a", "b", "c"])).await.unwrap();
    let store = health.store().unwrap();
    store.ensure_source("extractor", "products").await.unwrap();
    store
        .set_state_manual("extractor/products", SourceState::Quarantined, "test")
        .await
        .unwrap();

    // Quarantined, but enforcement is off: writes stay in the live dataset,
    // carry no stamp, and removal detection still runs. Nothing is gated.
    let summary = ctx.sync_many("products", &items(&["a"])).await.unwrap();
    assert_eq!(summary.removed.len(), 2, "soak mode must not suppress removals");
    assert!(datasets.get("extractor", "products@q", "a").await.unwrap().is_none());
    assert_eq!(datasets.get("extractor", "products", "a").await.unwrap().unwrap().trust, "stable");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn the_change_feed_holds_back_provisional_revisions_by_default() {
    let (storage, dir) = fresh_db("resilience-feed").await;
    let health = Arc::new(Resilience::new(
        storage.pool(),
        &ResilienceConfig { enforce: true, ..cfg() },
    ));
    let ctx = ctx(&storage, health.clone());
    let datasets = Datasets::new(storage.pool());

    ctx.upsert_many("products", &items(&["trusted"])).await.unwrap();
    let store = health.store().unwrap();
    store.ensure_source("extractor", "products").await.unwrap();
    store
        .set_state_manual("extractor/products", SourceState::Degraded, "test")
        .await
        .unwrap();
    ctx.upsert_many("products", &items(&["doubtful"])).await.unwrap();

    // Default: only what we stand behind. The pre-degradation revision has no
    // stamp at all and must still be included — that is the NULL-means-stable
    // equivalence, and without it every historical revision would vanish.
    let stable = datasets
        .changes_page("extractor", Some("products"), None, None, 100, Some("stable"))
        .await
        .unwrap();
    let keys: Vec<&str> = stable.items.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(keys, vec!["trusted"], "provisional revision leaked into the default feed");

    // A consumer that wants everything can always ask, and each revision says
    // which era it came from.
    let all = datasets
        .changes_page("extractor", Some("products"), None, None, 100, None)
        .await
        .unwrap();
    assert_eq!(all.items.len(), 2);
    let doubtful = all.items.iter().find(|r| r.key == "doubtful").unwrap();
    assert_eq!(doubtful.trust, "provisional");

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn retention_keeps_the_baseline_window_and_prunes_behind_it() {
    let (storage, dir) = fresh_db("resilience-prune").await;
    let health = Resilience::new(
        storage.pool(),
        &ResilienceConfig { window_runs: 3, sketch_retention_runs: 3, ..cfg() },
    );
    for _ in 0..8 {
        observe(&health, &cohort(12, "price", ".price", "sturdy")).await;
    }
    let store = health.store().unwrap();
    assert_eq!(store.runs("extractor/products", 50).await.unwrap().len(), 8);

    let pruned = store.prune(3).await.unwrap();
    assert!(pruned > 0, "8 runs kept to 3 must prune something");
    let left = store.runs("extractor/products", 50).await.unwrap();
    assert_eq!(left.len(), 3, "the newest three runs survive");
    // And the baseline still reads: retention must never prune what the detector
    // is about to compare against.
    let baseline = store.baseline("extractor/products", 3).await.unwrap();
    assert_eq!(baseline.runs("price"), 3);

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}
