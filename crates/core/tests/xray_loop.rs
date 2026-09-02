//! The API X-ray loop, end to end (N14).
//!
//! M05 shipped every piece except the one that runs them: the capture, the
//! discovery heuristic, the `api_recipes` store, `GET /recipes` and the
//! fetcher's `api_recipe` tier all existed with **no caller**, so the table was
//! empty on every deployment by construction and the tier it feeds could never
//! fire. These tests drive the whole loop through the seams a real job touches:
//!
//!   escalated render → capture → discovery → one proving replay → the
//!   `api_recipe` tier serving the next fetch → the router learning the host.
//!
//! Everything runs on scripted engines (no Chrome, no network) and the real
//! `RecipeStore`/`TierMemory` on a temp SQLite, so the SQL that promotes,
//! strikes and burns a recipe is exercised rather than mocked.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pumper_core::config::{FetcherConfig, GovernorConfig, RecipesConfig};
use pumper_core::engine::{
    Browser, CapturedCall, EngineSet, HttpClient, HttpRequest, HttpResponse, RenderRequest,
    RenderedPage,
};
use pumper_core::governor::Governor;
use pumper_core::recipes::RecipeSource;
use pumper_core::testing::{Dead, TempStore, TestContext};
use pumper_core::tiers::TierMemory;
use pumper_core::{FetchRequest, FetchStrategy, Fetcher, Result};
use serde_json::{json, Value};

const PAGE_URL: &str = "https://spa.test/grants";
const API_URL: &str = "https://spa.test/api/search?q=grants&page=1";
const HOST: &str = "spa.test";

/// The shell a JS-heavy page serves to a plain HTTP client: real markup, far
/// under `min_content_chars`, so the ladder escalates to the browser.
const SHELL: &str = "<html><body><div id=\"root\"></div></body></html>";

/// The rendered DOM, comfortably past the 250-char escalation threshold.
fn rendered_html() -> String {
    format!(
        "<html><body><ul>{}</ul></body></html>",
        "<li>Alpha Grant — Dept of Energy — 50000</li>\
         <li>Beta Grant — Dept of Labor — 75000</li>"
            .repeat(6)
    )
}

/// What the extractor read off that page — the `extracted` half of discovery.
fn extracted() -> Vec<Value> {
    vec![json!({
        "title": "Alpha Grant",
        "agency": "Dept of Energy",
        "amount": "50000",
    })]
}

/// The payload the page's own API returned: it carries the very values the
/// extractor scraped, which is what makes it discoverable.
fn api_payload() -> Value {
    json!({"results": [
        {"title": "Alpha Grant", "agency": "Dept of Energy", "amount": "50000"},
        {"title": "Beta Grant", "agency": "Dept of Labor", "amount": "75000"}
    ]})
}

/// The same endpoint after a redesign: valid JSON, no overlapping paths.
fn moved_payload() -> Value {
    json!({"data": {"rows": [{"name": "Alpha Grant"}]}})
}

/// Serves the thin shell for the page and a scripted body for the API URL,
/// counting API hits so a test can prove a replay actually happened.
struct RouteHttp {
    api_body: Value,
    api_hits: AtomicUsize,
}

impl RouteHttp {
    fn new(api_body: Value) -> Self {
        Self {
            api_body,
            api_hits: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl HttpClient for RouteHttp {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let body = if req.url == API_URL {
            self.api_hits.fetch_add(1, Ordering::Relaxed);
            serde_json::to_string(&self.api_body).unwrap()
        } else {
            SHELL.to_string()
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

/// A browser that records whether each render was asked to capture, and only
/// returns captures when it was — the engine's own contract, so a test cannot
/// pass by capturing unconditionally.
#[derive(Default)]
struct ScriptedBrowser {
    capture_flags: Mutex<Vec<bool>>,
}

#[async_trait]
impl Browser for ScriptedBrowser {
    async fn render(&self, req: RenderRequest) -> Result<RenderedPage> {
        self.capture_flags.lock().unwrap().push(req.capture_network);
        Ok(RenderedPage {
            html: rendered_html(),
            network: if req.capture_network {
                vec![
                    CapturedCall {
                        url: API_URL.into(),
                        method: "GET".into(),
                        status: 200,
                        content_type: "application/json".into(),
                        body: api_payload(),
                    },
                    // Analytics noise: captured, but nothing overlaps.
                    CapturedCall {
                        url: "https://spa.test/api/telemetry".into(),
                        method: "POST".into(),
                        status: 204,
                        content_type: "application/json".into(),
                        body: json!({"session": "zzz-1"}),
                    },
                ]
            } else {
                Vec::new()
            },
            ..RenderedPage::default()
        })
    }
}

/// An `EngineSet` whose fetcher runs the real recipe tier against the store.
fn engines(
    http: Arc<dyn HttpClient>,
    browser: Arc<dyn Browser>,
    recipes: Arc<dyn RecipeSource>,
    xray: bool,
) -> Arc<EngineSet> {
    let fetch = Fetcher::new(
        http.clone(),
        browser.clone(),
        Arc::new(Dead),
        // Politeness spacing is not what these tests are about.
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

fn auto(url: &str) -> FetchRequest {
    let mut req = FetchRequest::new(url);
    req.strategy = FetchStrategy::Auto;
    req
}

/// One recipe row as `GET /recipes` renders it.
async fn only_recipe(store: &TempStore) -> Value {
    let rows = store.storage.recipes().list(Some(HOST), 10).await.unwrap();
    assert_eq!(rows.len(), 1, "exactly one recipe should be discovered");
    rows.into_iter().next().unwrap()
}

/// THE user moment: *"my extractor renders this SPA in Chrome on every single
/// fetch, even though the page just calls a JSON endpoint."*
///
/// One escalated render is captured, the page's own API is discovered from the
/// values the extractor read, one replay proves it, and every later fetch of
/// that host is served by one governed JSON GET — with the router learning it.
#[tokio::test]
async fn an_escalated_render_is_captured_discovered_validated_and_served_by_the_recipe_tier() {
    let store = TempStore::new("xray-loop").await;
    let http = Arc::new(RouteHttp::new(api_payload()));
    let browser = Arc::new(ScriptedBrowser::default());
    let recipes: Arc<dyn RecipeSource> = Arc::new(store.storage.recipes());
    let ctx = TestContext::new(&store.storage, "xray")
        .engines(engines(http.clone(), browser.clone(), recipes, true))
        .artifacts_dir(store.path().join("xray").join("job"))
        .build();

    // 1. The escalating fetch: http serves a shell, the browser renders — and
    //    because that render is an ESCALATION, it is captured.
    let out = ctx
        .fetch(auto(PAGE_URL))
        .await
        .expect("browser tier serves");
    assert_eq!(out.engine, "browser");
    assert_eq!(
        browser.capture_flags.lock().unwrap().as_slice(),
        &[true],
        "an escalated render must ask the engine to capture"
    );
    assert_eq!(
        out.network.len(),
        2,
        "the captures must reach the app on the outcome, not die at the engine"
    );

    // 2. Discovery, exactly as the extractor calls it: the captures scored
    //    against the records that page produced.
    let page = RenderedPage {
        network: out.network.clone(),
        ..RenderedPage::default()
    };
    let (calls, stored) = ctx.xray(&page, &extracted()).await.expect("xray runs");
    assert_eq!(
        (calls, stored),
        (2, 1),
        "only the overlapping call qualifies"
    );

    let row = only_recipe(&store).await;
    assert_eq!(row["host"], HOST);
    assert_eq!(row["validated"], false, "discovery never self-validates");
    assert!(
        row["validation_reason"].is_null(),
        "a recipe nothing has replayed yet has no verdict — Null, not a made-up one"
    );

    // 3 + 4. The next fetch of that host: the recipe is replayed once through
    //        the HTTP tier, proves itself, and serves the fetch.
    let out = ctx.fetch(auto(PAGE_URL)).await.expect("recipe tier serves");
    assert_eq!(
        out.engine, "api_recipe",
        "a validated recipe must beat the page tiers"
    );
    assert_eq!(
        browser.capture_flags.lock().unwrap().len(),
        1,
        "the second fetch must not render at all"
    );
    assert!(
        out.html.is_none() && out.text.is_some(),
        "API data, not a document"
    );
    assert_eq!(http.api_hits.load(Ordering::Relaxed), 1, "one replay");

    let row = only_recipe(&store).await;
    assert_eq!(row["validated"], true);
    assert_eq!(
        row["validation_reason"], "replay matched the expected field paths",
        "the verdict is stored, so `GET /recipes` says WHY it is validated"
    );
    assert!(row["validated_at"].is_string());

    // 5. The router learned the third tier state.
    let profile = TierMemory::new(store.storage.pool(), 0)
        .get(HOST)
        .await
        .unwrap()
        .expect("the host has learned state");
    assert_eq!(profile.preferred_tier.as_deref(), Some("api_recipe"));
}

/// The negative half of the same gate: a recipe whose replay does NOT come back
/// with the data is stored with the reason it was refused, is never preferred,
/// and — the bound that did not exist before — is eventually burned instead of
/// costing one wasted governed request on every fetch of that host, forever.
#[tokio::test]
async fn an_invalid_recipe_is_stored_with_its_reason_and_never_preferred() {
    let store = TempStore::new("xray-invalid").await;
    // The endpoint moved on: still JSON, still 200, but the recipe's paths are
    // gone — the case `payload_overlaps` exists to catch.
    let http = Arc::new(RouteHttp::new(moved_payload()));
    let browser = Arc::new(ScriptedBrowser::default());
    let recipes: Arc<dyn RecipeSource> = Arc::new(store.storage.recipes());
    let ctx = TestContext::new(&store.storage, "xray")
        .engines(engines(http.clone(), browser.clone(), recipes, true))
        .artifacts_dir(store.path().join("xray").join("job"))
        .build();

    // Discover from a capture (the capture itself still carries the data).
    let page = RenderedPage {
        network: vec![CapturedCall {
            url: API_URL.into(),
            method: "GET".into(),
            status: 200,
            content_type: "application/json".into(),
            body: api_payload(),
        }],
        ..RenderedPage::default()
    };
    assert_eq!(ctx.xray(&page, &extracted()).await.unwrap().1, 1);

    // Every fetch falls through to the page tiers; the recipe never serves.
    let max_failures = RecipesConfig::default().max_failures;
    for attempt in 1..=max_failures {
        let out = ctx
            .fetch(auto(PAGE_URL))
            .await
            .expect("ladder still serves");
        assert_eq!(
            out.engine, "browser",
            "attempt {attempt}: an unproven recipe must never be terminal"
        );
        let row = only_recipe(&store).await;
        assert_eq!(row["validated"], false, "a thin replay never promotes");
        assert_eq!(
            row["validation_reason"], "payload lost the expected field paths",
            "the refusal reason is stored, not just implied by validated=false"
        );
        assert_eq!(row["consecutive_failures"], attempt);
    }
    assert_eq!(
        http.api_hits.load(Ordering::Relaxed),
        max_failures as usize,
        "the candidate is tried exactly `max_failures` times, not forever"
    );

    // Burned: neither the validated-only lookup nor the opportunistic one will
    // offer it again, so the next fetch costs no replay at all.
    let store_handle = store.storage.recipes();
    assert!(store_handle
        .best_for_host(HOST, false, max_failures)
        .await
        .unwrap()
        .is_none());
    assert!(
        store_handle
            .best_for_host(HOST, true, max_failures)
            .await
            .unwrap()
            .is_none(),
        "a burned candidate must not be replayed again"
    );
    ctx.fetch(auto(PAGE_URL))
        .await
        .expect("ladder still serves");
    assert_eq!(
        http.api_hits.load(Ordering::Relaxed),
        max_failures as usize,
        "no further replays after the burn"
    );

    // And the host is not advertised as recipe-served on `GET /hosts`.
    let profile = TierMemory::new(store.storage.pool(), 0)
        .get(HOST)
        .await
        .unwrap()
        .expect("the host has learned state");
    assert_ne!(profile.preferred_tier.as_deref(), Some("api_recipe"));
}

/// The default posture: with `[fetcher] xray` off, the ladder behaves exactly
/// as it did — no capture is requested, nothing is discovered, and the recipe
/// store is never even consulted.
#[tokio::test]
async fn nothing_is_captured_or_consulted_while_the_xray_is_off() {
    let store = TempStore::new("xray-off").await;
    let http = Arc::new(RouteHttp::new(api_payload()));
    let browser = Arc::new(ScriptedBrowser::default());
    let recipes: Arc<dyn RecipeSource> = Arc::new(store.storage.recipes());
    let ctx = TestContext::new(&store.storage, "xray")
        .engines(engines(http.clone(), browser.clone(), recipes, false))
        .artifacts_dir(store.path().join("xray").join("job"))
        .build();

    let out = ctx
        .fetch(auto(PAGE_URL))
        .await
        .expect("browser tier serves");
    assert_eq!(out.engine, "browser");
    assert_eq!(browser.capture_flags.lock().unwrap().as_slice(), &[false]);
    assert!(out.network.is_empty(), "no capture, nothing to discover");
    assert_eq!(
        http.api_hits.load(Ordering::Relaxed),
        0,
        "the recipe tier must not fire without an opt-in"
    );
    let out_json = serde_json::to_value(&out).unwrap();
    assert!(
        out_json.get("network").is_none(),
        "an empty capture list must not even appear in the serialized outcome"
    );
}
