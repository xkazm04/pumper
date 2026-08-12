//! Shared harness for the crawl app's **`run()`-level** tests.
//!
//! Until this directory existed, nothing anywhere in the workspace constructed
//! `Crawl` and called `run()` — the only construction site is
//! `crates/server/src/registry.rs`. That is the structural reason a documented
//! result field could go missing and the manifest could drift through four
//! milestones unnoticed: every existing test drove `DatasetPageSink::emit`
//! directly, so the result builder, the param plumbing, the metering/reliability
//! flush and the revisit wiring were all uncovered.
//!
//! [`StubSite`] closes that: a fixed website served from memory, so the whole app
//! runs — frontier, robots, sink, edges, reliability flush, result builder —
//! against a real `TempStore` with zero network.

#![allow(dead_code)] // each test binary uses a different slice of this module

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
use pumper_core::{AppContext, HttpClient, HttpRequest, HttpResponse, Result};
use serde_json::Value;

/// A tiny website served entirely from memory.
///
/// Anything not registered answers `404` with an empty body — which is exactly
/// how a host with no `robots.txt` behaves, so the probe-pollution case is the
/// default rather than something a test has to stage.
#[derive(Default)]
pub struct StubSite {
    responses: HashMap<String, (u16, String)>,
    fetched: Mutex<Vec<String>>,
}

impl StubSite {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an HTML page at `url` linking to `links`.
    ///
    /// The filler text is derived from the URL so every page's SimHash differs —
    /// otherwise near-dup detection would quietly eat the site out from under a
    /// test that was asserting on page counts.
    #[must_use]
    pub fn page(self, url: &str, links: &[&str]) -> Self {
        let filler: String = (0..40)
            .map(|i| format!("{url} word{i} "))
            .collect::<String>();
        let anchors: String = links
            .iter()
            .map(|l| format!("<a href=\"{l}\">{l}</a>"))
            .collect();
        let body = format!(
            "<html><head><title>{url}</title></head><body><h1>{url}</h1>\
             <p>{filler}</p>{anchors}</body></html>"
        );
        self.raw(url, 200, &body)
    }

    /// Registers a raw response (status + body) at `url`.
    #[must_use]
    pub fn raw(mut self, url: &str, status: u16, body: &str) -> Self {
        self.responses
            .insert(url.to_string(), (status, body.to_string()));
        self
    }

    /// Every URL fetched through this site, in completion order.
    pub fn fetched(&self) -> Vec<String> {
        self.fetched
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn fetched_count(&self, url: &str) -> usize {
        self.fetched().iter().filter(|u| *u == url).count()
    }
}

#[async_trait]
impl HttpClient for StubSite {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        self.fetched
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(req.url.clone());
        let (status, body) = self
            .responses
            .get(&req.url)
            .cloned()
            .unwrap_or((404, String::new()));
        Ok(HttpResponse {
            status,
            headers: HashMap::new(),
            body,
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// An `AppContext` for the `crawl` app over `store`, fetching from `site`.
/// Browser and researcher stay [`Dead`] — a crawl that reaches them is a bug.
pub fn crawl_ctx(store: &TempStore, site: Arc<StubSite>, params: Value) -> AppContext {
    TestContext::new(&store.storage, "crawl")
        .params(params)
        .engines(engines_with(site, Arc::new(Dead), Arc::new(Dead)))
        .build()
}

/// The result's keys, sorted — the unit the manifest inventory diffs against.
pub fn result_keys(out: &Value) -> Vec<String> {
    let mut keys: Vec<String> = out
        .as_object()
        .expect("the crawl result is an object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    keys
}
