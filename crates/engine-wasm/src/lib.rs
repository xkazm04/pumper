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
use std::sync::RwLock;

use async_trait::async_trait;
use pumper_core::config::PluginConfig;
use pumper_core::{Error, Plugins, Result};
use serde_json::Value;
use wasmtime::{Config, Engine, Linker, Module, ResourceLimiter, Store, StoreLimits, StoreLimitsBuilder};

pub struct WasmPluginHost {
    engine: Engine,
    dir: std::path::PathBuf,
    fuel: u64,
    max_memory: usize,
    modules: RwLock<HashMap<String, Module>>,
}

impl WasmPluginHost {
    pub fn new(cfg: &PluginConfig) -> Result<Self> {
        let mut config = Config::new();
        config.consume_fuel(true); // enables the per-call instruction budget
        let engine = Engine::new(&config).map_err(|e| Error::App(format!("wasm engine: {e}")))?;
        std::fs::create_dir_all(&cfg.dir)?;
        let modules = load_dir(&engine, &cfg.dir);
        tracing::info!(
            count = modules.len(),
            dir = %cfg.dir.display(),
            "loaded wasm plugins"
        );
        Ok(Self {
            engine,
            dir: cfg.dir.clone(),
            fuel: cfg.fuel,
            max_memory: cfg.max_memory_mb.saturating_mul(1024 * 1024),
            modules: RwLock::new(modules),
        })
    }
}

#[async_trait]
impl Plugins for WasmPluginHost {
    async fn run(&self, name: &str, input: &str) -> Result<Value> {
        let module = self
            .modules
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| Error::App(format!("unknown plugin '{name}'")))?;
        let engine = self.engine.clone();
        let input = input.to_string();
        let (fuel, max_memory) = (self.fuel, self.max_memory);
        // Wasm execution is synchronous and CPU-bound — run it off the async
        // runtime so a busy plugin never stalls a tokio worker.
        tokio::task::spawn_blocking(move || execute(engine, module, input, fuel, max_memory))
            .await
            .map_err(|e| Error::App(format!("plugin task panicked: {e}")))?
    }

    fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.modules.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    async fn reload(&self) -> Result<usize> {
        let modules = load_dir(&self.engine, &self.dir);
        let count = modules.len();
        *self.modules.write().unwrap() = modules;
        tracing::info!(count, "reloaded wasm plugins");
        Ok(count)
    }
}

fn load_dir(engine: &Engine, dir: &Path) -> HashMap<String, Module> {
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
                map.insert(name, module);
            }
            Err(err) => tracing::warn!(path = %path.display(), "failed to compile plugin: {err}"),
        }
    }
    map
}

fn execute(engine: Engine, module: Module, input: String, fuel: u64, max_memory: usize) -> Result<Value> {
    let limits = StoreLimitsBuilder::new().memory_size(max_memory).build();
    let mut store = Store::new(&engine, limits);
    store.limiter(|l| l as &mut dyn ResourceLimiter);
    store
        .set_fuel(fuel)
        .map_err(|e| Error::App(format!("set fuel: {e}")))?;

    let linker: Linker<StoreLimits> = Linker::new(&engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| Error::App(format!("instantiate: {e}")))?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| Error::App("plugin exports no 'memory'".into()))?;
    let alloc = instance
        .get_typed_func::<u32, u32>(&mut store, "alloc")
        .map_err(|e| Error::App(format!("plugin missing alloc(u32)->u32: {e}")))?;
    let extract = instance
        .get_typed_func::<(u32, u32), u64>(&mut store, "extract")
        .map_err(|e| Error::App(format!("plugin missing extract(u32,u32)->u64: {e}")))?;

    let bytes = input.as_bytes();
    let len = bytes.len() as u32;
    let in_ptr = alloc
        .call(&mut store, len)
        .map_err(|e| Error::App(format!("plugin alloc trapped: {e}")))?;
    memory
        .write(&mut store, in_ptr as usize, bytes)
        .map_err(|e| Error::App(format!("write input: {e}")))?;

    // On fuel exhaustion / OOM this returns a trap — the sandbox holds.
    let packed = extract
        .call(&mut store, (in_ptr, len))
        .map_err(|e| Error::App(format!("plugin trapped (fuel/memory/panic): {e}")))?;

    let out_ptr = (packed >> 32) as usize;
    let out_len = (packed & 0xffff_ffff) as usize;
    let mut out = vec![0u8; out_len];
    memory
        .read(&store, out_ptr, &mut out)
        .map_err(|e| Error::App(format!("read output: {e}")))?;

    serde_json::from_slice(&out).map_err(|e| Error::App(format!("plugin returned invalid JSON: {e}")))
}
