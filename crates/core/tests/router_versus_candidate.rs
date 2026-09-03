//! The router-versus-candidate axis at the fetch ladder.
//!
//! The measurable: **engine invocations per router-caused failure**, from one
//! per tier down to 1. A failure that is pumper's own (`Error::Config` here)
//! reproduces identically on every tier, so the ladder must stop on it — while
//! a candidate failure still climbs every tier, which is the paired assertion
//! that keeps the fix from becoming a regression in the case the ladder exists
//! for.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::config::{FetcherConfig, GovernorConfig};
use pumper_core::engine::{
    Browser, HttpClient, HttpRequest, HttpResponse, RenderRequest, RenderedPage, ResearchOutput,
    ResearchRequest, Researcher,
};
use pumper_core::error::{Error, Result};
use pumper_core::fetcher::{FetchRequest, FetchStrategy, Fetcher};
use pumper_core::Governor;

const URL: &str = "https://example.test/doc";

/// An engine that fails every call with a fixed error and counts the calls.
/// Implements all three capability traits, so one struct instruments whichever
/// tier a test wires it into.
struct Counting {
    calls: AtomicUsize,
    make: fn() -> Error,
}

impl Counting {
    fn new(make: fn() -> Error) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            make,
        })
    }

    fn hit(&self) -> Error {
        self.calls.fetch_add(1, Ordering::SeqCst);
        (self.make)()
    }

    fn count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl HttpClient for Counting {
    async fn fetch(&self, _: HttpRequest) -> Result<HttpResponse> {
        Err(self.hit())
    }
}

#[async_trait]
impl Browser for Counting {
    async fn render(&self, _: RenderRequest) -> Result<RenderedPage> {
        Err(self.hit())
    }
}

#[async_trait]
impl Researcher for Counting {
    async fn research(&self, _: ResearchRequest) -> Result<ResearchOutput> {
        Err(self.hit())
    }
}

fn ladder(http: Arc<Counting>, browser: Arc<Counting>, claude: Arc<Counting>) -> Fetcher {
    Fetcher::new(
        http,
        browser,
        claude,
        Arc::new(Governor::new(&GovernorConfig::default())),
        &FetcherConfig::default(),
    )
}

fn config_error() -> Error {
    Error::config("missing [claude] api key")
}

#[tokio::test]
async fn router_failure_stops_the_ladder_at_one_engine_invocation() {
    let (http, browser, claude) = (
        Counting::new(config_error),
        Counting::new(|| Error::browser("chrome died")),
        Counting::new(|| Error::App("research failed".into())),
    );
    let fetcher = ladder(http.clone(), browser.clone(), claude.clone());

    let mut req = FetchRequest::new(URL);
    req.strategy = FetchStrategy::AutoWithResearch;
    let err = fetcher.fetch(req).await.expect_err("every tier fails");

    // The mark is one carrier — the variant itself — and it survives to the
    // caller, so the job row names the origin instead of the ladder's prose.
    assert!(
        matches!(err, Error::Config { .. }),
        "a router failure must reach the caller as itself, got: {err}"
    );
    assert_eq!(http.count(), 1, "the failing tier runs exactly once");
    assert_eq!(
        browser.count() + claude.count(),
        0,
        "no other tier is a different environment for pumper's own config"
    );
}

#[tokio::test]
async fn candidate_failure_still_climbs_every_tier() {
    let (http, browser, claude) = (
        Counting::new(|| Error::http("connection reset")),
        Counting::new(|| Error::browser("chrome died")),
        Counting::new(|| Error::App("research failed".into())),
    );
    let fetcher = ladder(http.clone(), browser.clone(), claude.clone());

    let mut req = FetchRequest::new(URL);
    req.strategy = FetchStrategy::AutoWithResearch;
    let err = fetcher.fetch(req).await.expect_err("every tier fails");

    assert!(
        err.to_string().contains("all fetch tiers exhausted"),
        "an engine failure still ends in exhaustion, got: {err}"
    );
    assert_eq!(
        (http.count(), browser.count(), claude.count()),
        (1, 1, 1),
        "each candidate is a genuinely different environment and gets its turn"
    );
}

#[tokio::test]
async fn router_failure_at_the_browser_never_un_skips_the_http_tier() {
    // Router-caused: the un-skip would overturn a correct routing decision on
    // evidence that says nothing about the http tier.
    let (http, browser) = (
        Counting::new(|| Error::http("connection reset")),
        Counting::new(config_error),
    );
    let fetcher = ladder(http.clone(), browser.clone(), Counting::new(config_error));
    let mut req = FetchRequest::new(URL);
    req.skip_http = true;
    let err = fetcher
        .fetch(req)
        .await
        .expect_err("the browser tier fails");
    assert!(matches!(err, Error::Config { .. }), "got: {err}");
    assert_eq!(browser.count(), 1);
    assert_eq!(http.count(), 0, "the router's skip stands");

    // Candidate-caused: the un-skip is exactly right, and still fires.
    let (http, browser) = (
        Counting::new(|| Error::http("connection reset")),
        Counting::new(|| Error::browser("chrome died")),
    );
    let fetcher = ladder(http.clone(), browser.clone(), Counting::new(config_error));
    let mut req = FetchRequest::new(URL);
    req.skip_http = true;
    fetcher.fetch(req).await.expect_err("both tiers fail");
    assert_eq!(browser.count(), 1);
    assert_eq!(http.count(), 1, "a dead engine un-skips the cheap tier");
}
