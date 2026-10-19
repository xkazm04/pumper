//! Capability-scoped host imports for the **core-module plugin sandbox** (N10).
//!
//! The sandbox's contract has always been "declare no imports, so you have no
//! filesystem and no network". That default does not change: a module that
//! declares nothing still gets a linker with nothing in it. What changes is that
//! a module MAY now ask, in its own `describe()` manifest, for a bounded slice
//! of the outside world — and the host builds that module's linker **from the
//! manifest it just read**.
//!
//! Three properties follow, and they are the whole security story:
//!
//! * **Fail closed at LOAD.** The linker for a plugin contains exactly the
//!   imports its manifest declared. A module that imports `pumper_http_request`
//!   without declaring `capabilities.http` finds nothing to resolve against and
//!   fails to link — a typed [`PluginFailure::MissingExport`] the trigger ledger
//!   already words as `hook_not_executable`. There is no path where an
//!   undeclared import resolves to a stub, and none where it fails later.
//! * **Two locks, two owners.** The manifest bounds what the plugin asks for;
//!   `[plugins] allow_http_hosts` bounds what this deployment will do. The
//!   decision is [`pumper_core::plugin::authorize_http`], a pure function tested
//!   without a socket.
//! * **No ambient authority even when granted.** The granted imports are
//!   performed by a [`PluginCapabilityHost`] the server supplies. With no bridge
//!   attached — every test host, and any deployment that never wired one — a
//!   declared capability is present in the linker and *traps* when called. A
//!   plugin cannot fetch its way through a `describe()` probe either: the probe
//!   store carries no [`CallCaps`] at all.
//!
//! The component-model host in [`crate::app_host`] is the same abstraction one
//! level up: there the import table is the WIT world, here it is this linker.
//! Neither is a second host for the other's modules.

use std::sync::Arc;

use pumper_core::error::PluginFailure;
use pumper_core::plugin::{
    authorize_http, PluginCapabilities, PluginCapabilityHost, PluginHttpRequest, PluginHttpResponse,
};
use pumper_core::{Error, Result};
use wasmtime::{Caller, Engine, Linker, Memory, ResourceLimiter, StoreLimits};

/// The wasm import module name every capability lands under. `env` is what a
/// bare `extern "C"` block compiles to on `wasm32-unknown-unknown`, so a plugin
/// declares the import the obvious way and nothing has to be told about a
/// custom module name.
pub const CAPABILITY_MODULE: &str = "env";

/// Host calls one plugin invocation may make.
///
/// Fuel does not bound these: a host call burns the HOST's time (an HTTP round
/// trip), not the guest's instructions, so a plugin that loops on
/// `pumper_http_request` would sit inside its fuel budget forever while holding
/// an admission permit. This ceiling is what actually bounds the loop, and it is
/// deliberately small — a *sink* posts one delivery, a connector reads a page or
/// two. A plugin that needs a hundred calls per invocation is an app (see
/// `docs/features/apps.md` §dynamic), not a hook.
pub const MAX_CAPABILITY_CALLS: u64 = 32;

/// Bound on one payload crossing the boundary in either direction. A request
/// body or a response the plugin could not have produced honestly is refused
/// rather than truncated: a half-read JSON body parsed into a delivery is a
/// silent data defect, which is the failure mode this sandbox exists to prevent.
pub const MAX_CAPABILITY_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

/// What one plugin invocation was granted, and what performs it.
///
/// Cloned out of the store before any guest re-entry (calling the guest's own
/// `alloc` from inside a host function needs the store back), which is why it is
/// an `Arc` of immutable facts rather than a borrow.
pub struct CallCaps {
    pub plugin: String,
    pub capabilities: PluginCapabilities,
    pub allow_http_hosts: Arc<Vec<String>>,
    pub bridge: Option<Arc<dyn PluginCapabilityHost>>,
    /// The runtime the async bridge is driven on. A plugin call runs on the
    /// blocking pool (see `run_admitted`), so blocking on a future here is legal
    /// — it parks a blocking thread, never a runtime worker.
    pub handle: tokio::runtime::Handle,
}

/// Store data for one core-module plugin call.
///
/// `caps` is `None` for the load-time `describe()` probe, which instantiates the
/// same module against a linker that has every import present. A probe that
/// calls one gets a typed refusal instead of a fabricated context — a plugin
/// cannot fetch its way through discovery.
pub struct PluginStore {
    pub limits: StoreLimits,
    caps: Option<Arc<CallCaps>>,
    calls: u64,
}

impl PluginStore {
    pub fn new(limits: StoreLimits, caps: Option<Arc<CallCaps>>) -> Self {
        Self {
            limits,
            caps,
            calls: 0,
        }
    }

    /// The limiter hook, in one place so both the probe and the call path set it
    /// identically.
    pub fn limiter(s: &mut PluginStore) -> &mut dyn ResourceLimiter {
        &mut s.limits as &mut dyn ResourceLimiter
    }

    /// The gate every capability import passes first. Extracted rather than
    /// repeated per import because "the ceiling is checked at every import" is
    /// this host's only bound on host-side time, and one import that forgot the
    /// check would be the hole.
    fn admit(&mut self, what: &str) -> wasmtime::Result<Arc<CallCaps>> {
        self.calls += 1;
        if self.calls > MAX_CAPABILITY_CALLS {
            return Err(trap(format!(
                "host-call ceiling reached at {what}: {} calls > {MAX_CAPABILITY_CALLS}",
                self.calls
            )));
        }
        self.caps.clone().ok_or_else(|| {
            trap(format!(
                "{what} is not available here: this instance is the load-time describe() \
                 probe, which is granted no capabilities at all"
            ))
        })
    }
}

/// Every capability refusal is a TRAP the guest cannot catch, because each one
/// means a bound this host exists to enforce was reached. A denied *request*
/// (wrong host, wrong method) is different: that comes back as data on the
/// `error` field, so a connector can report a permanent delivery failure instead
/// of dying mid-batch.
fn trap(message: impl Into<String>) -> wasmtime::Error {
    wasmtime::Error::msg(message.into())
}

/// Builds the linker for one plugin: exactly the imports `caps` declared.
///
/// This is the guard the whole model rests on. The import table IS the sandbox,
/// so a module asking for a function its manifest never declared must fail HERE,
/// at load — not at some later call, and never by being handed a stub.
pub fn plugin_linker(engine: &Engine, caps: &PluginCapabilities) -> Result<Linker<PluginStore>> {
    let mut linker: Linker<PluginStore> = Linker::new(engine);
    let wrap = |e: wasmtime::Error, what: &str| {
        Error::plugin(
            PluginFailure::Host,
            "<host>",
            format!("could not define the {what} capability import: {e}"),
        )
    };
    if caps.declares_http() {
        linker
            .func_wrap(CAPABILITY_MODULE, "pumper_http_request", http_request)
            .map_err(|e| wrap(e, "http"))?;
    }
    if caps.kv {
        linker
            .func_wrap(CAPABILITY_MODULE, "pumper_kv_get", kv_get)
            .map_err(|e| wrap(e, "kv_get"))?;
        linker
            .func_wrap(CAPABILITY_MODULE, "pumper_kv_put", kv_put)
            .map_err(|e| wrap(e, "kv_put"))?;
    }
    Ok(linker)
}

// ---- The imports ----------------------------------------------------------

/// `pumper_http_request(ptr, len) -> u64`
///
/// In: JSON `{method, url, headers, body}`. Out: JSON `{status, body}` or
/// `{error}`, packed as `(out_ptr << 32) | out_len` — the same convention the
/// plugin's own `extract_v2` returns by, so a plugin needs no second ABI idea.
///
/// A *denied* request returns `{error: ...}` rather than trapping: refusing a
/// destination is an answer the connector should report (and the delivery ladder
/// should see as permanent), whereas exceeding the call ceiling is the host
/// stopping the plugin.
fn http_request(mut caller: Caller<'_, PluginStore>, ptr: u32, len: u32) -> wasmtime::Result<u64> {
    let caps = caller.data_mut().admit("pumper_http_request")?;
    let input = read_guest(&mut caller, ptr, len, "http request")?;
    let response = perform_http(&caps, &input);
    let out = serde_json::to_vec(&response)
        .map_err(|e| trap(format!("http response is unserializable: {e}")))?;
    emit_to_guest(&mut caller, &out)
}

/// The decision + the round trip, split out of the wasm plumbing so the
/// authorize-then-perform order is readable in one screen and testable through
/// [`PluginCapabilityHost`] alone.
fn perform_http(caps: &CallCaps, input: &str) -> PluginHttpResponse {
    let req: PluginHttpRequest = match serde_json::from_str(input) {
        Ok(req) => req,
        Err(e) => return PluginHttpResponse::failed(format!("not an http request object: {e}")),
    };
    if let Err(denial) = authorize_http(
        &caps.capabilities,
        &caps.allow_http_hosts,
        &req.method,
        &req.url,
    ) {
        tracing::warn!(
            plugin = %caps.plugin, url = %req.url, method = %req.method,
            "plugin http request refused: {denial}"
        );
        return PluginHttpResponse::failed(denial.to_string());
    }
    let Some(bridge) = caps.bridge.clone() else {
        return PluginHttpResponse::failed(
            "this host has no capability bridge wired, so no plugin can reach the network \
             (the server wires one; a bare WasmPluginHost does not)",
        );
    };
    let plugin = caps.plugin.clone();
    caps.handle
        .block_on(async move { bridge.http_request(&plugin, req).await })
}

/// `pumper_kv_get(ptr, len) -> u64` — key in, value out, `0` when absent.
///
/// A packed `0` (null pointer, zero length) is the miss, and it is
/// distinguishable from an empty stored value only in that both read as "no
/// bytes"; a plugin that needs to tell them apart stores a JSON wrapper. Said
/// here rather than discovered later.
fn kv_get(mut caller: Caller<'_, PluginStore>, ptr: u32, len: u32) -> wasmtime::Result<u64> {
    let caps = caller.data_mut().admit("pumper_kv_get")?;
    let key = read_guest(&mut caller, ptr, len, "kv key")?;
    let Some(bridge) = caps.bridge.clone() else {
        return Err(trap("this host has no capability bridge wired"));
    };
    let plugin = caps.plugin.clone();
    let value = caps
        .handle
        .block_on(async move { bridge.kv_get(&plugin, &key).await });
    match value {
        Some(value) => emit_to_guest(&mut caller, value.as_bytes()),
        None => Ok(0),
    }
}

/// `pumper_kv_put(kptr, klen, vptr, vlen) -> u32` — `1` stored, `0` refused.
fn kv_put(
    mut caller: Caller<'_, PluginStore>,
    kptr: u32,
    klen: u32,
    vptr: u32,
    vlen: u32,
) -> wasmtime::Result<u32> {
    let caps = caller.data_mut().admit("pumper_kv_put")?;
    let key = read_guest(&mut caller, kptr, klen, "kv key")?;
    let value = read_guest(&mut caller, vptr, vlen, "kv value")?;
    let Some(bridge) = caps.bridge.clone() else {
        return Err(trap("this host has no capability bridge wired"));
    };
    let plugin = caps.plugin.clone();
    match caps
        .handle
        .block_on(async move { bridge.kv_put(&plugin, &key, &value).await })
    {
        Ok(()) => Ok(1),
        Err(e) => {
            tracing::warn!(plugin = %caps.plugin, "plugin kv_put failed: {e}");
            Ok(0)
        }
    }
}

// ---- Guest memory plumbing -------------------------------------------------

fn memory_of(caller: &mut Caller<'_, PluginStore>) -> wasmtime::Result<Memory> {
    caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or_else(|| trap("the module exports no 'memory', so nothing can cross the boundary"))
}

/// Reads `len` guest bytes as UTF-8, bounds-checked against the module's own
/// linear memory BEFORE allocating — a crafted pointer must not drive a giant
/// host-side allocation.
fn read_guest(
    caller: &mut Caller<'_, PluginStore>,
    ptr: u32,
    len: u32,
    what: &str,
) -> wasmtime::Result<String> {
    let memory = memory_of(caller)?;
    let (ptr, len) = (ptr as usize, len as usize);
    if len > MAX_CAPABILITY_PAYLOAD_BYTES {
        return Err(trap(format!(
            "{what} is {len} bytes, over the {MAX_CAPABILITY_PAYLOAD_BYTES}-byte capability \
             payload cap"
        )));
    }
    let size = memory.data_size(&*caller);
    if ptr.checked_add(len).is_none_or(|end| end > size) {
        return Err(trap(format!(
            "{what} range out of bounds: ptr={ptr} len={len} mem={size}"
        )));
    }
    let mut buf = vec![0u8; len];
    memory
        .read(&*caller, ptr, &mut buf)
        .map_err(|e| trap(format!("{what} unreadable: {e}")))?;
    String::from_utf8(buf).map_err(|e| trap(format!("{what} is not UTF-8: {e}")))
}

/// Hands `bytes` back to the guest through the guest's OWN `alloc`, packed as
/// `(ptr << 32) | len`.
///
/// Re-entering the guest from a host function is what makes a returning import
/// possible at all with this ABI; the alternative (a second import the guest
/// calls to collect the result) doubles the call ceiling's accounting for no
/// gain.
fn emit_to_guest(caller: &mut Caller<'_, PluginStore>, bytes: &[u8]) -> wasmtime::Result<u64> {
    if bytes.len() > MAX_CAPABILITY_PAYLOAD_BYTES {
        return Err(trap(format!(
            "capability result is {} bytes, over the {MAX_CAPABILITY_PAYLOAD_BYTES}-byte cap",
            bytes.len()
        )));
    }
    let alloc = caller
        .get_export("alloc")
        .and_then(|e| e.into_func())
        .ok_or_else(|| {
            trap("the module exports no 'alloc', so a capability result cannot be returned")
        })?
        .typed::<u32, u32>(&*caller)
        .map_err(|e| trap(format!("alloc is not alloc(u32)->u32: {e}")))?;
    let len = bytes.len() as u32;
    let ptr = alloc
        .call(&mut *caller, len)
        .map_err(|e| trap(format!("alloc({len}) trapped: {e}")))?;
    let memory = memory_of(caller)?;
    memory
        .write(&mut *caller, ptr as usize, bytes)
        .map_err(|e| {
            trap(format!(
                "alloc({len}) returned an unwritable pointer {ptr}: {e}"
            ))
        })?;
    Ok(((ptr as u64) << 32) | len as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn handle() -> tokio::runtime::Handle {
        tokio::runtime::Handle::current()
    }

    fn caps_for(manifest: serde_json::Value, allow: &[&str]) -> CallCaps {
        CallCaps {
            plugin: "sink-test".into(),
            capabilities: pumper_core::plugin::parse_capabilities(&manifest).expect("manifest"),
            allow_http_hosts: Arc::new(allow.iter().map(|s| s.to_string()).collect()),
            bridge: None,
            handle: handle(),
        }
    }

    /// A refusal must reach the plugin as DATA, not as a trap: a connector that
    /// was pointed at the wrong host should be able to report a permanent
    /// delivery failure, which is what puts the row in the DLQ instead of
    /// looking like a crashed sandbox.
    #[tokio::test]
    async fn a_denied_destination_is_an_error_field_not_a_trap() {
        let caps = caps_for(
            json!({"capabilities": {"http": {"hosts": ["api.example.com"]}}}),
            &["*"],
        );
        let out = perform_http(&caps, &json!({"url": "https://evil.example/x"}).to_string());
        assert!(out.status.is_none());
        assert!(
            out.error
                .as_deref()
                .unwrap_or_default()
                .contains("evil.example"),
            "{out:?}"
        );
    }

    /// The bridge-less host — every unit test, and any deployment that never
    /// wired one — must refuse rather than invent a response.
    #[tokio::test]
    async fn a_host_with_no_bridge_performs_nothing() {
        let caps = caps_for(
            json!({"capabilities": {"http": {"hosts": ["api.example.com"]}}}),
            &["api.example.com"],
        );
        let out = perform_http(
            &caps,
            &json!({"url": "https://api.example.com/x"}).to_string(),
        );
        assert!(out.status.is_none());
        assert!(
            out.error.as_deref().unwrap_or_default().contains("bridge"),
            "{out:?}"
        );
    }

    /// A module that reaches for the network. Its import is the ONLY thing that
    /// differs between the two halves of the test below.
    const HTTP_IMPORTING_WAT: &str = r#"(module
        (import "env" "pumper_http_request" (func $req (param i32 i32) (result i64)))
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "extract_v2") (param i32 i32) (result i64)
          (call $req (i32.const 0) (i32.const 0))))"#;

    /// **The gate the whole item rests on.** The linker IS the sandbox: a plugin
    /// that declared no capabilities gets a linker with nothing in it, so a
    /// module importing `pumper_http_request` cannot resolve and fails to LINK —
    /// at load, with a class a caller can branch on, never as a stub and never
    /// at some later call.
    #[test]
    fn a_module_importing_an_undeclared_host_function_fails_to_link() {
        let engine = Engine::default();
        let module = wasmtime::Module::new(&engine, HTTP_IMPORTING_WAT).expect("wat");

        let linker = plugin_linker(&engine, &PluginCapabilities::default()).expect("linker");
        let err = match linker.instantiate_pre(&module) {
            Ok(_) => panic!("an undeclared import must not link"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("pumper_http_request"),
            "the refusal must name the import: {err}"
        );

        // And the negative of that negative: with the capability declared, the
        // SAME module links. Without this, a linker that granted nothing at all
        // would pass the assertion above.
        let declared = pumper_core::plugin::parse_capabilities(
            &json!({"capabilities": {"http": {"hosts": ["a.example.com"]}}}),
        )
        .expect("manifest");
        plugin_linker(&engine, &declared)
            .expect("linker")
            .instantiate_pre(&module)
            .expect("a declared capability must resolve");
    }

    /// The kv half of the same property, so neither import is guarded only by
    /// the other's test.
    #[test]
    fn kv_imports_appear_only_when_kv_is_declared() {
        let engine = Engine::default();
        let wat = r#"(module
            (import "env" "pumper_kv_put" (func $put (param i32 i32 i32 i32) (result i32)))
            (memory (export "memory") 1)
            (func (export "alloc") (param i32) (result i32) (i32.const 1024)))"#;
        let module = wasmtime::Module::new(&engine, wat).expect("wat");
        // Declaring HTTP does not smuggle in kv.
        let http_only = pumper_core::plugin::parse_capabilities(
            &json!({"capabilities": {"http": {"hosts": ["a.example.com"]}}}),
        )
        .expect("manifest");
        assert!(plugin_linker(&engine, &http_only)
            .expect("linker")
            .instantiate_pre(&module)
            .is_err());
        let kv = pumper_core::plugin::parse_capabilities(&json!({"capabilities": {"kv": true}}))
            .expect("manifest");
        plugin_linker(&engine, &kv)
            .expect("linker")
            .instantiate_pre(&module)
            .expect("a declared kv capability must resolve");
    }
}
