//! The discovery caller the API X-ray shipped without (N14).
//!
//! `AppContext::xray` — the discovery heuristic, the `api_recipes` store and
//! `GET /recipes` behind it — had **zero call sites** since M05, so the table
//! was empty on every deployment by construction and the fetcher's
//! `api_recipe` tier could never fire. The extractor is the honest caller: it
//! is the one app that holds both halves of the evidence at the same moment —
//! the JSON calls the render observed, and the values it just read off that
//! page — which is exactly what the overlap heuristic scores.
//!
//! Scripted engines only: no Chrome, no network.

use std::collections::HashMap;
use std::sync::Arc;

use app_extractor::Extractor;
use async_trait::async_trait;
use pumper_core::config::{FetcherConfig, GovernorConfig, RecipesConfig};
use pumper_core::engine::{
    Browser, CapturedCall, EngineSet, HttpClient, HttpRequest, HttpResponse, RenderRequest,
    RenderedPage,
};
use pumper_core::governor::Governor;
use pumper_core::recipes::RecipeSource;
use pumper_core::testing::{Dead, TempStore, TestContext};
use pumper_core::{Fetcher, Result, ScrapeApp};
use serde_json::{json, Value};

const PAGE_URL: &str = "https://spa.test/grants";
const API_URL: &str = "https://spa.test/api/search?q=grants";

/// The shell a plain HTTP client gets — under `min_content_chars`, so the
/// ladder escalates and the render that follows is a capture candidate.
const SHELL: &str = "<html><body><div id=\"root\"></div></body></html>";

fn rendered_html() -> String {
    "<html><body><article><h1>Alpha Grant</h1>\
     <p class=\"agency\">Department of Energy</p>\
     <p class=\"amount\">50000</p>\
     <p>Applications for this programme close at the end of the quarter, and \
     the listing page is rendered client-side from a JSON endpoint.</p>\
     </article></body></html>"
        .to_string()
}

struct ShellHttp;

#[async_trait]
impl HttpClient for ShellHttp {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: SHELL.to_string(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// Returns captures only when the fetcher actually asked for them.
struct CapturingBrowser;

#[async_trait]
impl Browser for CapturingBrowser {
    async fn render(&self, req: RenderRequest) -> Result<RenderedPage> {
        Ok(RenderedPage {
            html: rendered_html(),
            network: if req.capture_network {
                vec![CapturedCall {
                    url: API_URL.into(),
                    method: "GET".into(),
                    status: 200,
                    content_type: "application/json".into(),
                    body: json!({"results": [{
                        "title": "Alpha Grant",
                        "agency": "Department of Energy",
                        "amount": "50000"
                    }]}),
                }]
            } else {
                Vec::new()
            },
            ..RenderedPage::default()
        })
    }
}

fn engines(store: &TempStore, xray: bool) -> Arc<EngineSet> {
    let http: Arc<dyn HttpClient> = Arc::new(ShellHttp);
    let browser: Arc<dyn Browser> = Arc::new(CapturingBrowser);
    let recipes: Arc<dyn RecipeSource> = Arc::new(store.storage.recipes());
    let fetch = Fetcher::new(
        http.clone(),
        browser.clone(),
        Arc::new(Dead),
        Arc::new(Governor::new(&GovernorConfig {
            default_rps: 0.0,
            ..GovernorConfig::default()
        })),
        &FetcherConfig {
            xray,
            ..FetcherConfig::default()
        },
    )
    .with_recipes(Some(recipes), &RecipesConfig::default());
    Arc::new(EngineSet::new(http, browser, Arc::new(Dead), fetch))
}

fn params() -> Value {
    json!({
        "urls": [PAGE_URL],
        "strategy": "auto",
        "dataset": "grants",
        "rules": {
            "title": {"type": "css", "selector": "h1"},
            "agency": {"type": "css", "selector": ".agency"},
            "amount": {"type": "css", "selector": ".amount"},
        },
    })
}

async fn run(store: &TempStore, xray: bool) -> Value {
    let ctx = TestContext::new(&store.storage, "extractor")
        .params(params())
        .engines(engines(store, xray))
        .artifacts_dir(store.path().join("extractor").join("job"))
        .build();
    Extractor.run(ctx).await.expect("extraction runs")
}

/// THE gap this closes: `routes/recipes.rs` used to say it in prose — *"no app
/// calls `xray` yet, so this table stays empty until a discovery caller
/// ships"*. One ordinary extractor run over a JS-heavy URL now fills it.
#[tokio::test]
async fn the_extractor_fills_the_recipe_table_the_xray_shipped_empty() {
    let store = TempStore::new("xray-caller").await;
    let out = run(&store, true).await;
    assert_eq!(
        out["fetched"], 1,
        "the render must have produced a document"
    );

    let recipes = store.storage.recipes().list(None, 10).await.unwrap();
    assert_eq!(
        recipes.len(),
        1,
        "the page's own JSON API must be discovered from the records it yielded"
    );
    assert_eq!(recipes[0]["host"], "spa.test");
    assert_eq!(
        recipes[0]["url_template"], "https://spa.test/api/search?q={q}",
        "the endpoint is stored parameterized, not as the one URL observed"
    );
    assert_eq!(
        recipes[0]["validated"], false,
        "discovery proposes; only a replay proves"
    );
}

/// The default posture: with `[fetcher] xray` off the extractor behaves exactly
/// as before — nothing is captured, so nothing is discovered.
#[tokio::test]
async fn the_extractor_discovers_nothing_while_the_xray_is_off() {
    let store = TempStore::new("xray-caller-off").await;
    let out = run(&store, false).await;
    assert_eq!(out["fetched"], 1);
    assert!(store
        .storage
        .recipes()
        .list(None, 10)
        .await
        .unwrap()
        .is_empty());
}
