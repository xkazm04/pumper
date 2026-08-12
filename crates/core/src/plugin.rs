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

/// What one plugin call **cost**, as measured by the host that ran it.
///
/// The sandbox enforced a CPU-fuel budget and a memory cap from the day it
/// existed and reported neither, so nobody could see how close a plugin ran to
/// its limits — the `plugin` app's observatory said so in its own module docs
/// and substituted wall-clock elapsed time for cost. Wall clock measures the
/// machine's load as much as the plugin's appetite; fuel is deterministic, which
/// is exactly what a "did this get more expensive?" comparison needs.
///
/// Every field is `Option` because "this host does not meter" is a different
/// fact from "this call was free", and a zeroed cost that reads as free is the
/// specific lie this type exists to avoid.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct PluginRunStats {
    /// CPU fuel consumed: the budget minus what remained when the call returned.
    pub fuel_used: Option<u64>,
    /// The budget it ran against, so "how close to the ceiling" is answerable
    /// from the same object rather than from a second config lookup.
    pub fuel_budget: Option<u64>,
    /// Linear-memory high-water in bytes. Wasm memory only grows within a store
    /// and every call gets a fresh store, so the size after the call IS this
    /// call's high-water — no sampling needed.
    pub memory_bytes: Option<usize>,
    /// The cap that memory ran against.
    pub memory_cap_bytes: Option<usize>,
}

impl PluginRunStats {
    /// A host that does not meter. Distinct from a metered zero.
    pub const fn unmetered() -> Self {
        Self {
            fuel_used: None,
            fuel_budget: None,
            memory_bytes: None,
            memory_cap_bytes: None,
        }
    }

    /// Whether this carries a real measurement — i.e. whether a consumer may use
    /// fuel as its cost signal rather than falling back to wall clock.
    pub fn is_metered(&self) -> bool {
        self.fuel_used.is_some()
    }

    /// How much of the fuel budget this call used, in `[0, 1]`. `None` when
    /// unmetered or when the budget is zero (which would make the ratio a
    /// division by nothing rather than "100% used").
    pub fn fuel_fraction(&self) -> Option<f64> {
        match (self.fuel_used, self.fuel_budget) {
            (Some(used), Some(budget)) if budget > 0 => Some(used as f64 / budget as f64),
            _ => None,
        }
    }
}

#[async_trait]
pub trait Plugins: Send + Sync {
    /// Runs plugin `name` over `input` with a `params` envelope, returning its
    /// JSON output. Enforces the configured fuel and memory limits; a runaway
    /// plugin traps rather than hanging the host. `params` lets one plugin be
    /// reused across jobs with different config (e.g. a different selector)
    /// instead of recompiling a module per variation; a plugin that only exports
    /// the legacy `extract` receives just the document and ignores `params`.
    async fn run(&self, name: &str, input: &str, params: &Value) -> Result<Value>;

    /// [`run`](Plugins::run), plus what the call cost.
    ///
    /// A separate method with a default impl rather than a widened `run` return
    /// type, deliberately: the hook path (`crates/server/src/triggers.rs`) and
    /// every stub host in the test suite want the value and nothing else, and
    /// making them all unpack a tuple they discard would be churn that buys
    /// nothing. Hosts that cannot meter — `NoPlugins`, in-process stubs — get
    /// this default and implement nothing; the wasmtime host overrides it and
    /// routes its own `run` through it.
    ///
    /// Note the bound: stats describe a call that **returned**. A call that
    /// trapped propagates the error, and the fuel it burned on the way is not
    /// carried (see `docs/features/extraction.md`).
    async fn run_metered(
        &self,
        name: &str,
        input: &str,
        params: &Value,
    ) -> Result<(Value, PluginRunStats)> {
        Ok((
            self.run(name, input, params).await?,
            PluginRunStats::unmetered(),
        ))
    }

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

#[cfg(test)]
mod tests {
    use super::{NoPlugins, PluginRunStats, Plugins};
    use crate::error::PluginFailure;

    /// The distinction the whole type exists for: a host that does not measure
    /// must not report a cost of zero, which reads as "this ran for free".
    #[test]
    fn unmetered_is_not_a_metered_zero() {
        let none = PluginRunStats::unmetered();
        assert!(!none.is_metered());
        assert_eq!(none.fuel_used, None);
        assert_eq!(none.fuel_fraction(), None);

        let free = PluginRunStats {
            fuel_used: Some(0),
            fuel_budget: Some(1_000),
            ..PluginRunStats::unmetered()
        };
        assert!(free.is_metered(), "a measured 0 IS a measurement");
        assert_eq!(free.fuel_fraction(), Some(0.0));
        assert_ne!(free, none);
    }

    #[test]
    fn fuel_fraction_reports_headroom_and_refuses_a_zero_budget() {
        let s = PluginRunStats {
            fuel_used: Some(750),
            fuel_budget: Some(1_000),
            ..PluginRunStats::unmetered()
        };
        assert_eq!(s.fuel_fraction(), Some(0.75));
        // A zero budget makes the ratio meaningless, not "100% used".
        let s = PluginRunStats {
            fuel_used: Some(0),
            fuel_budget: Some(0),
            ..PluginRunStats::unmetered()
        };
        assert_eq!(s.fuel_fraction(), None);
    }

    /// The default impl must stay honest for a host that cannot meter: same
    /// error, same value, and no invented cost.
    #[tokio::test]
    async fn the_default_metered_impl_reports_no_cost_and_keeps_the_error() {
        let err = NoPlugins
            .run_metered("anything", "doc", &serde_json::Value::Null)
            .await
            .expect_err("plugins are disabled");
        assert_eq!(err.plugin_failure(), Some(PluginFailure::Disabled));
    }
}
