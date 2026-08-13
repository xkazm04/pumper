//! The fetch chokepoint, proved through the two apps that fan out over a
//! caller-supplied `urls` list: `extractor` and `plugin`.
//!
//! Both take a `strategy` param that accepts `auto_with_research`, so a raw
//! `engines.fetch` there was a direct line from job params to Claude spend that
//! skipped the per-job budget clamp, the cost ledger, the learned tier router
//! and the VCR cassette. `crates/core/tests/fetch_chokepoint.rs` pins the
//! *structure* (no raw call site survives); this file pins the *behaviour* the
//! structure buys.
//!
//! Lives in the server crate because that is the only crate that may depend on
//! app crates (apps depend on `core` and nothing else).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pumper_core::testing::{
    dead_engines, engines_with, Dead, ScriptedResearcher, TempStore, TestContext,
};
use pumper_core::{
    Browser, Cassette, HttpClient, HttpRequest, HttpResponse, Recorder, RenderRequest,
    RenderedPage, Result, ScrapeApp, Vcr,
};
use serde_json::{json, Value};

/// An HTTP engine that answers every request with the same body and counts
/// calls. `thin` bodies (under the fetcher's 250-char floor) are what make an
/// escalating strategy climb to the next tier.
struct CannedHttp {
    body: String,
    calls: AtomicUsize,
}

impl CannedHttp {
    fn new(body: &str) -> Arc<Self> {
        Arc::new(Self {
            body: body.to_string(),
            calls: AtomicUsize::new(0),
        })
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl HttpClient for CannedHttp {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(HttpResponse {
            status: 200,
            headers: Default::default(),
            body: self.body.clone(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// A browser that renders one canned (deliberately thin) document — the tier
/// between "http was thin" and "escalate to Claude".
struct CannedBrowser(String);

#[async_trait::async_trait]
impl Browser for CannedBrowser {
    async fn render(&self, _: RenderRequest) -> Result<RenderedPage> {
        Ok(RenderedPage {
            html: self.0.clone(),
            ..Default::default()
        })
    }
}

/// Extractor params over `urls` with one CSS rule.
fn extractor_params(urls: &[&str], strategy: &str) -> Value {
    json!({
        "urls": urls,
        "strategy": strategy,
        "rules": {"headline": {"type": "css", "selector": "h1"}},
    })
}

/// Plugin params over `urls`. The module named here must be one
/// [`EchoPlugins`] loads: the plugin app now refuses an unloadable name at the
/// door, *before any fetch*, which would make these fetch assertions vacuous.
fn plugin_params(urls: &[&str], strategy: &str) -> Value {
    json!({ "plugin": "echo", "urls": urls, "strategy": strategy })
}

/// A plugin host that loads one module and echoes the document back.
///
/// THE ANTI-PATTERN THIS REPLACES: these two tests ran the plugin app against
/// the context builder's default `NoPlugins`, where **every** call fails
/// `plugins_disabled`. Both then `unwrap()`ed a run in which every single
/// document failed and asserted only on the cost ledger — so they passed green
/// on a total-failure run for their whole life, and were the proof that nothing
/// guarded the app's run door. A host that actually runs keeps the metering
/// invariants meaningful (they are about the fetch that happens *before* the
/// module) while letting the run reach a real result.
struct EchoPlugins;

#[async_trait::async_trait]
impl pumper_core::Plugins for EchoPlugins {
    async fn run(&self, name: &str, input: &str, _params: &Value) -> Result<Value> {
        assert_eq!(name, "echo", "the app must call the plugin it was given");
        Ok(json!({ "doc": input }))
    }
    fn list(&self) -> Vec<String> {
        vec!["echo".to_string()]
    }
    async fn reload(&self) -> Result<usize> {
        Ok(1)
    }
}

// ── Criterion: budget exhaustion downgrades, it does not spend ───────────────

/// The anti-pattern: `strategy: "auto_with_research"` on a job whose budget is
/// already spent (or forced to $0 by a DataHub `cost:pause`) used to reach the
/// Claude tier anyway, because the app drove the raw fetcher and the raw fetcher
/// has never known what a job budget is.
///
/// The control arm matters as much as the assertion: without it a green test
/// could just mean the fixture never escalated at all.
#[tokio::test]
async fn exhausted_budget_downgrades_extractor_instead_of_spending() {
    let thin = "<html><h1>T</h1></html>";

    // Control: budget headroom exists → the ladder really does reach Claude.
    let store = TempStore::new("chokepoint-extractor-control").await;
    let claude = Arc::new(ScriptedResearcher::new().always_text("x".repeat(400)));
    let ctx = TestContext::new(&store.storage, "extractor")
        .params(extractor_params(&["http://a/"], "auto_with_research"))
        .engines(engines_with(
            CannedHttp::new(thin),
            Arc::new(CannedBrowser(thin.into())),
            claude.clone(),
        ))
        .budget_usd(1.00)
        .build();
    app_extractor::Extractor.run(ctx).await.unwrap();
    assert_eq!(
        claude.call_count(),
        1,
        "fixture is wrong: with budget, this ladder must reach the Claude tier"
    );

    // The real assertion: no headroom → the Claude tier is never reached.
    let store = TempStore::new("chokepoint-extractor-broke").await;
    let claude = Arc::new(ScriptedResearcher::new().always_text("x".repeat(400)));
    let ctx = TestContext::new(&store.storage, "extractor")
        .params(extractor_params(&["http://a/"], "auto_with_research"))
        .engines(engines_with(
            CannedHttp::new(thin),
            Arc::new(CannedBrowser(thin.into())),
            claude.clone(),
        ))
        .budget_usd(0.10)
        .build();
    // Burn the whole budget through the ledger the chokepoint consults.
    ctx.meter("claude", None, 0.10, Some("prior call")).await;
    let job = ctx.job_id;
    let costs = ctx.costs.clone();
    let out = app_extractor::Extractor.run(ctx).await.unwrap();

    assert_eq!(
        claude.call_count(),
        0,
        "a budget-exhausted job reached the Claude tier through extractor: {out}"
    );
    let events = costs.job_events(job).await.unwrap();
    let downgrade = events
        .iter()
        .find(|e| e.url.as_deref() == Some("http://a/"))
        .expect("the fetch must be metered");
    assert!(
        downgrade
            .detail
            .as_deref()
            .is_some_and(|d| d.contains("claude tier skipped")),
        "the soft downgrade must be recorded on the cost event, not silent: {:?}",
        downgrade.detail
    );
    assert_eq!(
        downgrade.engine, "browser",
        "the downgrade is to the free tiers, not a hard failure"
    );
}

/// Same invariant through `plugin`, which has its own fan-out (and its own
/// positional zip, so it must not reorder either).
#[tokio::test]
async fn exhausted_budget_downgrades_plugin_instead_of_spending() {
    let thin = "<html><h1>T</h1></html>";
    let store = TempStore::new("chokepoint-plugin-broke").await;
    let claude = Arc::new(ScriptedResearcher::new().always_text("x".repeat(400)));
    let mut ctx = TestContext::new(&store.storage, "plugin")
        .params(plugin_params(
            &["http://a/", "http://b/"],
            "auto_with_research",
        ))
        .engines(engines_with(
            CannedHttp::new(thin),
            Arc::new(CannedBrowser(thin.into())),
            claude.clone(),
        ))
        .budget_usd(0.10)
        .build();
    ctx.plugins = Arc::new(EchoPlugins);
    ctx.meter("claude", None, 0.10, Some("prior call")).await;
    let job = ctx.job_id;
    let costs = ctx.costs.clone();
    let out = app_plugin::Plugin.run(ctx).await.unwrap();

    // The fixture has to have RUN, or the metering assertions below are about a
    // run in which nothing happened.
    assert_eq!(out["ran"], 2, "both documents must reach the module: {out}");
    assert_eq!(out["errors"], 0, "{out}");
    assert_eq!(
        claude.call_count(),
        0,
        "a budget-exhausted job reached the Claude tier through plugin: {out}"
    );
    let urls: Vec<String> = costs
        .job_events(job)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|e| e.url)
        .collect();
    assert!(
        urls.contains(&"http://a/".to_string()) && urls.contains(&"http://b/".to_string()),
        "both plugin fetches must be metered: {urls:?}"
    );
}

// ── Criterion: every fetched URL lands a cost event ──────────────────────────

/// The anti-pattern: a job's whole fan-out is invisible to `/economics` and to
/// the budget governor because the app fetched around the ledger. One event per
/// fetched URL, carrying the winning engine and the URL.
#[tokio::test]
async fn every_fanned_out_url_lands_a_cost_event() {
    let body = "<html><h1>Title</h1></html>";
    for (app, params) in [
        (
            "extractor",
            extractor_params(&["http://a/", "http://b/"], "http"),
        ),
        ("plugin", plugin_params(&["http://a/", "http://b/"], "http")),
    ] {
        let store = TempStore::new("chokepoint-meter").await;
        let mut ctx = TestContext::new(&store.storage, app)
            .params(params)
            .engines(engines_with(
                CannedHttp::new(body),
                Arc::new(Dead),
                Arc::new(Dead),
            ))
            .build();
        ctx.plugins = Arc::new(EchoPlugins);
        let job = ctx.job_id;
        let costs = ctx.costs.clone();
        match app {
            "extractor" => {
                let out = app_extractor::Extractor.run(ctx).await.unwrap();
                assert_eq!(
                    out["fetched"], 2,
                    "{app}: the run must have happened: {out}"
                );
            }
            _ => {
                let out = app_plugin::Plugin.run(ctx).await.unwrap();
                // Without this the whole loop passed on a run where every
                // document failed and no record was ever produced.
                assert_eq!(out["ran"], 2, "{app}: the run must have happened: {out}");
                assert_eq!(out["errors"], 0, "{app}: {out}");
            }
        }

        let mut events = costs.job_events(job).await.unwrap();
        events.sort_by(|a, b| a.url.cmp(&b.url));
        assert_eq!(events.len(), 2, "{app}: one cost event per fetched URL");
        assert_eq!(events[0].url.as_deref(), Some("http://a/"), "{app}");
        assert_eq!(events[1].url.as_deref(), Some("http://b/"), "{app}");
        assert!(
            events.iter().all(|e| e.engine == "http"),
            "{app}: the winning engine must be attributed, got {:?}",
            events.iter().map(|e| &e.engine).collect::<Vec<_>>()
        );
    }
}

// ── Criterion: a recorded run replays with no engines at all ─────────────────

/// The anti-pattern: `record: true` on an extractor job produced a cassette
/// with zero fetch entries (the app fetched around the recorder), so the
/// "deterministic" replay silently went back to the live network. Here the
/// replay runs against `Dead` engines — every one of which panics on contact —
/// so a single live fetch fails the test loudly.
#[tokio::test]
async fn recorded_extractor_run_replays_from_the_cassette_with_dead_engines() {
    let store = TempStore::new("chokepoint-vcr").await;
    let dir = store.path().join("extractor").join("rec");
    let http = CannedHttp::new("<html><h1>Recorded</h1></html>");

    let recorded = TestContext::new(&store.storage, "extractor")
        .params(extractor_params(&["http://a/", "http://b/"], "http"))
        .engines(engines_with(http.clone(), Arc::new(Dead), Arc::new(Dead)))
        .artifacts_dir(dir.clone())
        .vcr(Vcr::Record(Arc::new(Recorder::new(dir.clone()))))
        .build();
    let recorded_job = recorded.job_id;
    let first = app_extractor::Extractor.run(recorded).await.unwrap();
    assert_eq!(first["fetched"], 2);
    assert_eq!(http.calls(), 2, "the recording run really did fetch");

    let cassette = Cassette::load(&dir, recorded_job)
        .await
        .expect("the recorded run must have written a cassette");
    assert_eq!(cassette.len(), 2, "one entry per fetched URL");

    let replay = TestContext::new(&store.storage, "extractor")
        .params(extractor_params(&["http://a/", "http://b/"], "http"))
        .engines(dead_engines())
        .artifacts_dir(store.path().join("extractor").join("replay"))
        .vcr(Vcr::Replay(Arc::new(cassette)))
        .build();
    let job = replay.job_id;
    let costs = replay.costs.clone();
    let out = app_extractor::Extractor.run(replay).await.unwrap();

    assert_eq!(out["fetched"], 2, "replay served both URLs: {out}");
    assert_eq!(
        out["records"][0]["headline"], "Recorded",
        "replay reproduced the recorded body: {out}"
    );
    let events = costs.job_events(job).await.unwrap();
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|e| e.cost_usd == 0.0 && e.detail.as_deref() == Some("vcr_replay")),
        "a replay spends $0 and says so: {:?}",
        events.iter().map(|e| &e.detail).collect::<Vec<_>>()
    );
}
