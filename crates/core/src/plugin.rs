//! Sandboxed plugin capability. Apps run named WebAssembly modules over
//! documents; the implementation (`engine-wasm`) executes them with a CPU-fuel
//! budget and a hard memory cap, with no ambient authority (no filesystem or
//! network unless granted). This makes it safe to run **untrusted,
//! hot-swappable** extraction/transform logic in-process — a capability Python
//! has no equivalent for (`exec`/`RestrictedPython` are escapable; real
//! isolation needs a separate process/container).
//!
//! `core` defines only the trait; the wasmtime dependency lives in `engine-wasm`
//! so the runtime stays out of the shared crate.

use async_trait::async_trait;
use serde_json::Value;

use crate::error::PluginFailure;
use crate::{Error, Result};

#[async_trait]
pub trait Plugins: Send + Sync {
    /// Runs plugin `name` over `input` with a `params` envelope, returning its
    /// JSON output. Enforces the configured fuel and memory limits; a runaway
    /// plugin traps rather than hanging the host. `params` lets one plugin be
    /// reused across jobs with different config (e.g. a different selector)
    /// instead of recompiling a module per variation; a plugin that only exports
    /// the legacy `extract` receives just the document and ignores `params`.
    async fn run(&self, name: &str, input: &str, params: &Value) -> Result<Value>;

    /// Names of currently loaded plugins.
    fn list(&self) -> Vec<String>;

    /// Whether `name` is currently loaded — i.e. whether [`run`](Plugins::run)
    /// would find a module at all, as opposed to failing with "unknown
    /// plugin". Callers whose failure semantics are FAIL-OPEN (trigger hooks)
    /// need this: a trap and a plugin that was never deployed both end as a
    /// passed-through event, and only the second one means "your build/install
    /// step never ran". The default answers from [`list`](Plugins::list);
    /// hosts with an index should override it — this sits on the per-event
    /// hot path.
    fn has(&self, name: &str) -> bool {
        self.list().iter().any(|n| n == name)
    }

    /// Per-plugin metadata for `GET /plugins`: each entry is at least
    /// `{"name": ...}`, enriched with a plugin's self-describing manifest
    /// (`{name, version, description, params_schema, output_schema}`) when it
    /// exports `describe`. Default: name-only entries from [`list`].
    fn manifests(&self) -> Vec<Value> {
        self.list()
            .into_iter()
            .map(|name| serde_json::json!({ "name": name }))
            .collect()
    }

    /// Rescans the plugin directory (hot-swap); returns the loaded count.
    async fn reload(&self) -> Result<usize>;
}

/// Fallback host used when WASM plugins are disabled.
pub struct NoPlugins;

#[async_trait]
impl Plugins for NoPlugins {
    async fn run(&self, name: &str, _input: &str, _params: &Value) -> Result<Value> {
        // `Disabled`, not `Unknown`: the name is irrelevant here — NO name would
        // resolve — and the fix is `[plugins] enabled = true`, not a build step.
        // Callers that report missing hooks use the distinction to avoid telling
        // an operator to rebuild a plugin they deliberately switched off.
        Err(Error::plugin(
            PluginFailure::Disabled,
            name,
            "the plugin subsystem is disabled ([plugins] enabled = false)",
        ))
    }
    fn list(&self) -> Vec<String> {
        Vec::new()
    }
    async fn reload(&self) -> Result<usize> {
        Ok(0)
    }
}
