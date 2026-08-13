//! Shared harness for the plugin app's **`run()`-level** tests.
//!
//! Until this directory existed, the crate had 21 tests and **none of them ran
//! `run()`** — the only construction sites are `crates/server/src/registry.rs`
//! and one e2e fixture that drove the app against `NoPlugins` and asserted
//! nothing about the outcome. That is the structural reason a run in which every
//! single document failed could report SUCCESS for as long as the app has
//! existed: nothing anywhere executed the result builders.
//!
//! [`StubPlugins`] closes that — an in-process plugin host with a fixed
//! loaded-name list and one scripted answer, so the whole app runs (door, fetch
//! fan-out, artifact resolution, upsert, result builder) against a real
//! `TempStore` with zero wasm and zero network.

#![allow(dead_code)] // each test binary uses a different slice of this module

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::error::PluginFailure;
use pumper_core::testing::{TempStore, TestContext};
use pumper_core::{AppContext, Error, Plugins, Result};
use serde_json::{json, Value};
use uuid::Uuid;

/// What a [`StubPlugins`] call does with the body it was handed.
pub enum Answer {
    /// `{"title": <body>}` — an ordinary extraction.
    Echo,
    /// Every call fails with this class.
    Always(PluginFailure),
    /// Bodies containing the marker fail with this class; the rest echo.
    FailIf(&'static str, PluginFailure),
    /// The module runs to completion and reports it could not extract. This is
    /// the plugin's own DATA, not a host failure — the distinction the typed
    /// outcome seam exists to keep.
    SelfReportedError,
}

/// An in-process `Plugins` host: a fixed loaded-name list plus one scripted
/// answer, counting the calls that actually reached it.
///
/// The call counter is the load-bearing part for the door tests: "the run was
/// refused" and "the run was refused *before spending anything*" are different
/// claims, and only the second one is worth having.
pub struct StubPlugins {
    loaded: Vec<String>,
    answer: Answer,
    calls: AtomicUsize,
}

impl StubPlugins {
    pub fn new(loaded: &[&str], answer: Answer) -> Arc<Self> {
        Arc::new(Self {
            loaded: loaded.iter().map(|s| (*s).to_string()).collect(),
            answer,
            calls: AtomicUsize::new(0),
        })
    }

    /// A host that loads `title` and echoes the body back.
    pub fn echoing() -> Arc<Self> {
        Self::new(&["title"], Answer::Echo)
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Plugins for StubPlugins {
    async fn run(&self, name: &str, input: &str, _params: &Value) -> Result<Value> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.loaded.iter().any(|n| n == name) {
            return Err(Error::plugin(
                PluginFailure::Unknown,
                name,
                "no module of that name is loaded",
            ));
        }
        match &self.answer {
            Answer::Echo => Ok(json!({ "title": input })),
            Answer::Always(kind) => Err(Error::plugin(*kind, name, "scripted failure")),
            Answer::FailIf(marker, kind) if input.contains(marker) => {
                Err(Error::plugin(*kind, name, "scripted failure"))
            }
            Answer::FailIf(..) => Ok(json!({ "title": input })),
            Answer::SelfReportedError => Ok(json!({ "error": "no <title> found" })),
        }
    }

    fn list(&self) -> Vec<String> {
        self.loaded.clone()
    }

    async fn reload(&self) -> Result<usize> {
        Ok(self.loaded.len())
    }
}

/// A plugin-app context over `store`, with `plugins` swapped in for the
/// builder's `NoPlugins` default (the builder has no seam for it, and
/// `AppContext::plugins` is a public field).
pub fn ctx_with(store: &TempStore, params: Value, plugins: Arc<dyn Plugins>) -> AppContext {
    let mut ctx = TestContext::new(&store.storage, "plugin")
        .params(params)
        .artifacts_dir(store.path().join("plugin").join("job"))
        .build();
    ctx.plugins = plugins;
    ctx
}

/// The default context: `NoPlugins`, exactly as a deployment with
/// `[plugins] enabled = false` behaves.
pub fn ctx_without_plugins(store: &TempStore, params: Value) -> AppContext {
    TestContext::new(&store.storage, "plugin")
        .params(params)
        .artifacts_dir(store.path().join("plugin").join("job"))
        .build()
}

/// Seeds `n` crawl pages sharing one stored body, keyed `http://p{i}`.
/// Returns the crawl job id the artifacts live under.
pub async fn seed_pages(store: &TempStore, n: usize, body: &str) -> String {
    let crawl_job = Uuid::new_v4().to_string();
    let dir = store.path().join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("p.html"), body.as_bytes())
        .await
        .unwrap();
    let items: Vec<(String, Value)> = (0..n)
        .map(|i| {
            let key = format!("http://p{i}");
            (
                key.clone(),
                json!({ "url": key, "artifact_path": "p.html", "job_id": crawl_job }),
            )
        })
        .collect();
    store
        .datasets()
        .upsert_many("crawl", "pages", &items)
        .await
        .unwrap();
    crawl_job
}

/// Seeds one crawl page whose body is `body`, under key `url`.
pub async fn seed_page(store: &TempStore, url: &str, file: &str, body: &str) -> String {
    let crawl_job = Uuid::new_v4().to_string();
    let dir = store.path().join("crawl").join(&crawl_job);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join(file), body.as_bytes())
        .await
        .unwrap();
    store
        .datasets()
        .upsert_many(
            "crawl",
            "pages",
            &[(
                url.to_string(),
                json!({ "url": url, "artifact_path": file, "job_id": crawl_job }),
            )],
        )
        .await
        .unwrap();
    crawl_job
}

/// Params for a source-mode run over the seeded `crawl/pages`.
pub fn source_params(extra: Value) -> Value {
    let mut params = json!({
        "plugin": "title",
        "source": { "app": "crawl", "dataset": "pages" },
        "dataset": "plugin_out"
    });
    if let (Value::Object(map), Value::Object(extra)) = (&mut params, extra) {
        map.extend(extra);
    }
    params
}
