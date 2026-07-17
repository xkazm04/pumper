//! Sandboxed WASM plugin host (implements `pumper_core::Plugins`) using
//! wasmtime. Each plugin call gets a fresh `Store` with a CPU **fuel** budget
//! (a deterministic instruction ceiling — a runaway plugin traps instead of
//! hanging the host) and a hard linear-memory cap. Plugins have no imports, so
//! no ambient authority (no filesystem/network). This is the capability Python
//! can't match: safe, in-process execution of untrusted, hot-swappable code.
//!
//! ABI a plugin must export:
//!   - `memory`                          (linear memory, default export)
//!   - `alloc(len: u32) -> u32`          reserve `len` bytes, return the pointer
//!   - `extract(ptr: u32, len: u32) -> u64`  read the input, return the output
//!                                        packed as `(out_ptr << 32) | out_len`
//! The output bytes must be UTF-8 JSON.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use pumper_core::config::PluginConfig;
use pumper_core::{Error, Plugins, Result};
use serde_json::Value;
use tokio::sync::Semaphore;
use wasmtime::{
    Config, Engine, Instance, Linker, Memory, Module, ResourceLimiter, Store, StoreLimits,
    StoreLimitsBuilder, TypedFunc,
};

pub struct WasmPluginHost {
    engine: Engine,
    dir: std::path::PathBuf,
    fuel: u64,
    max_memory: usize,
    /// Global admission gate: caps concurrent `execute` calls so aggregate wasm
    /// memory (`max_memory × permits`) and blocking-pool usage stay bounded no
    /// matter how wide the caller's fan-out is.
    sem: Arc<Semaphore>,
    modules: RwLock<HashMap<String, LoadedPlugin>>,
}

/// A compiled plugin plus its self-describing manifest (from the optional
/// `describe` export), read once at load and cached for `GET /plugins`.
#[derive(Clone)]
struct LoadedPlugin {
    module: Module,
    manifest: Option<Value>,
}

/// Resolve the concurrency cap: `0` means "one per core" via
/// [`std::thread::available_parallelism`], falling back to 4 if it's unavailable.
fn resolve_max_concurrent(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
}

impl WasmPluginHost {
    pub fn new(cfg: &PluginConfig) -> Result<Self> {
        let mut config = Config::new();
        config.consume_fuel(true); // enables the per-call instruction budget
        let engine = Engine::new(&config).map_err(|e| Error::App(format!("wasm engine: {e}")))?;
        std::fs::create_dir_all(&cfg.dir)?;
        let modules = load_dir(&engine, &cfg.dir);
        let max_concurrent = resolve_max_concurrent(cfg.max_concurrent);
        tracing::info!(
            count = modules.len(),
            dir = %cfg.dir.display(),
            max_concurrent,
            "loaded wasm plugins"
        );
        Ok(Self {
            engine,
            dir: cfg.dir.clone(),
            fuel: cfg.fuel,
            max_memory: cfg.max_memory_mb.saturating_mul(1024 * 1024),
            sem: Arc::new(Semaphore::new(max_concurrent)),
            modules: RwLock::new(modules),
        })
    }
}

#[async_trait]
impl Plugins for WasmPluginHost {
    async fn run(&self, name: &str, input: &str, params: &Value) -> Result<Value> {
        let module = self
            .modules
            .read()
            .unwrap()
            .get(name)
            .map(|p| p.module.clone())
            .ok_or_else(|| Error::App(format!("unknown plugin '{name}'")))?;
        let engine = self.engine.clone();
        let input = input.to_string();
        let params = params.clone();
        let (fuel, max_memory) = (self.fuel, self.max_memory);
        // Global admission: hold a permit for the whole execution so a wide
        // fan-out (e.g. a 200-URL plugin job) can't spin up 200 stores at once.
        // Acquired BEFORE spawn_blocking so excess callers wait here rather than
        // piling onto the blocking pool. The semaphore is never closed, so the
        // only error is impossible — map it defensively.
        let _permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Error::App(format!("plugin semaphore closed: {e}")))?;
        // Wasm execution is synchronous and CPU-bound — run it off the async
        // runtime so a busy plugin never stalls a tokio worker.
        tokio::task::spawn_blocking(move || execute(engine, module, input, params, fuel, max_memory))
            .await
            .map_err(|e| Error::App(format!("plugin task panicked: {e}")))?
    }

    fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.modules.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    fn manifests(&self) -> Vec<Value> {
        let modules = self.modules.read().unwrap();
        let mut entries: Vec<(&String, &LoadedPlugin)> = modules.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        entries
            .into_iter()
            .map(|(name, p)| match &p.manifest {
                // A plugin's own describe() output, with its name authoritative.
                Some(Value::Object(m)) => {
                    let mut m = m.clone();
                    m.insert("name".into(), Value::String(name.clone()));
                    Value::Object(m)
                }
                _ => serde_json::json!({ "name": name }),
            })
            .collect()
    }

    async fn reload(&self) -> Result<usize> {
        // load_dir is synchronous fs + a full Cranelift compile per module. Run it
        // off the async runtime — as `run` already does for the same reason — so a
        // dir of 10-20 modules (~0.2-2s of compile) doesn't park a tokio worker and
        // stall unrelated in-flight requests. Only the brief lock swap stays inline.
        let (engine, dir) = (self.engine.clone(), self.dir.clone());
        let modules = tokio::task::spawn_blocking(move || load_dir(&engine, &dir))
            .await
            .map_err(|e| Error::App(format!("plugin reload task panicked: {e}")))?;
        let count = modules.len();
        *self.modules.write().unwrap() = modules;
        tracing::info!(count, "reloaded wasm plugins");
        Ok(count)
    }
}

/// Fuel budget for the one-shot `describe()` probe at load time — generous for
/// returning a small static manifest, but bounded so a hostile module can't spin
/// the loader.
const DESCRIBE_FUEL: u64 = 10_000_000;

fn load_dir(engine: &Engine, dir: &Path) -> HashMap<String, LoadedPlugin> {
    let mut map = HashMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return map;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wasm") {
            continue;
        }
        let name = match path.file_stem().and_then(|s| s.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };
        match Module::from_file(engine, &path) {
            Ok(module) => {
                // Read the optional self-describing manifest once, best-effort —
                // a missing/failed `describe` degrades to name-only metadata.
                let manifest = describe_manifest(engine, &module);
                map.insert(name, LoadedPlugin { module, manifest });
            }
            Err(err) => tracing::warn!(path = %path.display(), "failed to compile plugin: {err}"),
        }
    }
    map
}

/// Builds a fuel-and-memory-limited store and instantiates `module` in it.
fn instantiate(
    engine: &Engine,
    module: &Module,
    fuel: u64,
    max_memory: usize,
) -> Result<(Store<StoreLimits>, Instance)> {
    // Cap every store-growable resource, not just linear memory: a module can
    // otherwise exhaust host RAM at instantiation via huge tables/instances,
    // sidestepping `memory_size` entirely. These bounds are generous for a
    // single extraction plugin (one instance, one memory, a small call table).
    let limits = StoreLimitsBuilder::new()
        .memory_size(max_memory)
        .memories(1)
        .tables(4)
        .table_elements(1_000_000)
        .instances(1)
        .build();
    let mut store = Store::new(engine, limits);
    store.limiter(|l| l as &mut dyn ResourceLimiter);
    store.set_fuel(fuel).map_err(|e| Error::App(format!("set fuel: {e}")))?;
    let linker: Linker<StoreLimits> = Linker::new(engine);
    let instance = linker
        .instantiate(&mut store, module)
        .map_err(|e| Error::App(format!("instantiate: {e}")))?;
    Ok((store, instance))
}

/// Reads and validates a plugin's packed `(out_ptr << 32 | out_len)` return,
/// returning the output bytes. Guards the guest-controlled `out_len` against the
/// module's own linear-memory size BEFORE allocating, so a crafted return can't
/// drive a giant host-side allocation and abort the process.
fn read_packed(store: &mut Store<StoreLimits>, memory: &Memory, packed: u64) -> Result<Vec<u8>> {
    let out_ptr = (packed >> 32) as usize;
    let out_len = (packed & 0xffff_ffff) as usize;
    let mem_size = memory.data_size(&*store);
    if out_ptr.checked_add(out_len).map_or(true, |end| end > mem_size) {
        return Err(Error::App(format!(
            "plugin output range out of bounds: ptr={out_ptr} len={out_len} mem={mem_size}"
        )));
    }
    let mut out = vec![0u8; out_len];
    memory
        .read(&*store, out_ptr, &mut out)
        .map_err(|e| Error::App(format!("read output: {e}")))?;
    Ok(out)
}

/// Best-effort read of a plugin's `describe() -> u64` manifest at load time.
/// Any miss (no export, trap, non-JSON) → `None`, degrading to name-only.
fn describe_manifest(engine: &Engine, module: &Module) -> Option<Value> {
    let (mut store, instance) = instantiate(engine, module, DESCRIBE_FUEL, 16 * 1024 * 1024).ok()?;
    let memory = instance.get_memory(&mut store, "memory")?;
    let describe = instance.get_typed_func::<(), u64>(&mut store, "describe").ok()?;
    let packed = describe.call(&mut store, ()).ok()?;
    let bytes = read_packed(&mut store, &memory, packed).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn execute(
    engine: Engine,
    module: Module,
    input: String,
    params: Value,
    fuel: u64,
    max_memory: usize,
) -> Result<Value> {
    let (mut store, instance) = instantiate(&engine, &module, fuel, max_memory)?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| Error::App("plugin exports no 'memory'".into()))?;
    let alloc = instance
        .get_typed_func::<u32, u32>(&mut store, "alloc")
        .map_err(|e| Error::App(format!("plugin missing alloc(u32)->u32: {e}")))?;

    // Prefer the params-aware `extract_v2` ABI (input is a `{doc, params}`
    // envelope); fall back to the legacy `extract` (raw document, params ignored)
    // so plugins built before the envelope keep working unchanged.
    let (func, input_bytes): (TypedFunc<(u32, u32), u64>, Vec<u8>) =
        match instance.get_typed_func::<(u32, u32), u64>(&mut store, "extract_v2") {
            Ok(f) => {
                let envelope = serde_json::json!({ "doc": input, "params": params }).to_string();
                (f, envelope.into_bytes())
            }
            Err(_) => {
                let f = instance
                    .get_typed_func::<(u32, u32), u64>(&mut store, "extract")
                    .map_err(|e| Error::App(format!("plugin missing extract(u32,u32)->u64: {e}")))?;
                (f, input.into_bytes())
            }
        };

    let len = input_bytes.len() as u32;
    let in_ptr = alloc
        .call(&mut store, len)
        .map_err(|e| Error::App(format!("plugin alloc trapped: {e}")))?;
    memory
        .write(&mut store, in_ptr as usize, &input_bytes)
        .map_err(|e| Error::App(format!("write input: {e}")))?;

    // On fuel exhaustion / OOM this returns a trap — the sandbox holds.
    let packed = func
        .call(&mut store, (in_ptr, len))
        .map_err(|e| Error::App(format!("plugin trapped (fuel/memory/panic): {e}")))?;

    let out = read_packed(&mut store, &memory, packed)?;
    serde_json::from_slice(&out).map_err(|e| Error::App(format!("plugin returned invalid JSON: {e}")))
}

#[cfg(test)]
mod tests {
    use super::resolve_max_concurrent;

    #[test]
    fn max_concurrent_honors_explicit_and_derives_default() {
        // Explicit value passes through untouched.
        assert_eq!(resolve_max_concurrent(8), 8);
        // 0 → one-per-core, always at least 1 (never an empty semaphore that
        // would deadlock every plugin run).
        assert!(resolve_max_concurrent(0) >= 1);
    }
}
