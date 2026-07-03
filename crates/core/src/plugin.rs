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

use crate::{Error, Result};

#[async_trait]
pub trait Plugins: Send + Sync {
    /// Runs plugin `name` over `input`, returning its JSON output. Enforces the
    /// configured fuel and memory limits; a runaway plugin traps rather than
    /// hanging the host.
    async fn run(&self, name: &str, input: &str) -> Result<Value>;

    /// Names of currently loaded plugins.
    fn list(&self) -> Vec<String>;

    /// Rescans the plugin directory (hot-swap); returns the loaded count.
    async fn reload(&self) -> Result<usize>;
}

/// Fallback host used when WASM plugins are disabled.
pub struct NoPlugins;

#[async_trait]
impl Plugins for NoPlugins {
    async fn run(&self, name: &str, _input: &str) -> Result<Value> {
        Err(Error::App(format!("plugins are disabled; cannot run '{name}'")))
    }
    fn list(&self) -> Vec<String> {
        Vec::new()
    }
    async fn reload(&self) -> Result<usize> {
        Ok(0)
    }
}
