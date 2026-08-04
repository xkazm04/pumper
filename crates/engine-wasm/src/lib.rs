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
//!     packed as `(out_ptr << 32) | out_len`
//!
//! The output bytes must be UTF-8 JSON.
//!
//! The host is a general UDF runtime, not just an extraction sandbox (M15
//! "WASM everywhere"): `run(name, input, params)` wraps the call in the
//! `extract_v2` `{doc, params}` envelope regardless of what `doc` holds. The
//! same ABI therefore serves extraction plugins (doc = fetched document),
//! trigger PREDICATE plugins (doc = the `_trigger` delta object, output
//! `{"pass": bool}`), and trigger TRANSFORM plugins (doc = the `_trigger`
//! object, output = the shaped object; provenance re-stamped by the caller).
//! Convention: a plugin declares its hook class in its `describe()` manifest
//! via `"kind": "extractor" | "predicate" | "transform"` so `GET
//! /plugins?kind=` can offer the right plugins per hook. Callers own their
//! failure semantics — trigger hooks fail OPEN (a trap/malformed output never
//! wedges the pipeline), extraction propagates the error.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use pumper_core::config::PluginConfig;
use pumper_core::{Error, Plugins, Result};
use serde_json::Value;
use tokio::sync::Semaphore;
use wasmtime::{
    Config, Engine, Instance, InstancePre, Linker, Memory, Module, ResourceLimiter, Store,
    StoreLimits, StoreLimitsBuilder, TypedFunc,
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

/// A compiled, **pre-instantiated** plugin plus its self-describing manifest
/// (from the optional `describe` export), read once at load and cached for
/// `GET /plugins`.
///
/// The `InstancePre` is the load-time half of instantiation — import
/// resolution and the type-checking a `Linker` would otherwise redo on every
/// call — done once per plugin lifetime. What it deliberately does NOT share
/// is the `Store`: every call still gets its own, so fuel budgets, linear
/// memory and any state a plugin leaves behind stay per-invocation. Sharing a
/// Store would trade the sandbox's isolation for the speedup.
#[derive(Clone)]
struct LoadedPlugin {
    pre: InstancePre<StoreLimits>,
    manifest: Option<Value>,
}

/// Resolve the concurrency cap: `0` means "one per core" via
/// [`std::thread::available_parallelism`], falling back to 4 if it's unavailable.
fn resolve_max_concurrent(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
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
        let pre = self
            .modules
            .read()
            .unwrap()
            .get(name)
            .map(|p| p.pre.clone())
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
        tokio::task::spawn_blocking(move || execute(engine, pre, input, params, fuel, max_memory))
            .await
            .map_err(|e| Error::App(format!("plugin task panicked: {e}")))?
    }

    fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.modules.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    /// Map lookup rather than the trait's default list-and-scan: trigger hooks
    /// ask this per event, per hook.
    fn has(&self, name: &str) -> bool {
        self.modules.read().unwrap().contains_key(name)
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
        let module = match Module::from_file(engine, &path) {
            Ok(module) => module,
            Err(err) => {
                tracing::warn!(path = %path.display(), "failed to compile plugin: {err}");
                continue;
            }
        };
        match pre_instantiate(engine, &module) {
            Ok(pre) => {
                // Read the optional self-describing manifest once, best-effort —
                // a missing/failed `describe` degrades to name-only metadata.
                let manifest = describe_manifest(engine, &pre);
                map.insert(name, LoadedPlugin { pre, manifest });
            }
            Err(err) => {
                tracing::warn!(path = %path.display(), "failed to link plugin: {err}")
            }
        }
    }
    map
}

/// Resolves a module's imports and type-checks it against the (empty) linker
/// ONCE, yielding a reusable [`InstancePre`].
///
/// Plugins declare no imports, so this cannot fail for a well-formed module —
/// but a module that *does* import something now fails at LOAD time with a
/// clear message instead of failing identically on every call forever.
fn pre_instantiate(engine: &Engine, module: &Module) -> Result<InstancePre<StoreLimits>> {
    let linker: Linker<StoreLimits> = Linker::new(engine);
    linker
        .instantiate_pre(module)
        .map_err(|e| Error::App(format!("pre-instantiate: {e}")))
}

/// Builds a fuel-and-memory-limited store and instantiates `pre` in it.
///
/// The store is per-call by design: fuel budget, linear memory and any residue
/// a previous invocation left behind must not be visible to the next one. Only
/// the *linking* work is shared, via the caller's [`InstancePre`].
fn instantiate(
    engine: &Engine,
    pre: &InstancePre<StoreLimits>,
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
    store
        .set_fuel(fuel)
        .map_err(|e| Error::App(format!("set fuel: {e}")))?;
    let instance = pre
        .instantiate(&mut store)
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
    if out_ptr
        .checked_add(out_len)
        .is_none_or(|end| end > mem_size)
    {
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
fn describe_manifest(engine: &Engine, pre: &InstancePre<StoreLimits>) -> Option<Value> {
    let (mut store, instance) = instantiate(engine, pre, DESCRIBE_FUEL, 16 * 1024 * 1024).ok()?;
    let memory = instance.get_memory(&mut store, "memory")?;
    let describe = instance
        .get_typed_func::<(), u64>(&mut store, "describe")
        .ok()?;
    let packed = describe.call(&mut store, ()).ok()?;
    let bytes = read_packed(&mut store, &memory, packed).ok()?;
    serde_json::from_slice(&bytes).ok()
}

// ---- Dynamic-app discovery (M28 v1 slice: discovery + manifest ONLY) --------

/// A dynamic-app candidate found in `[plugins] app_dir`: a `.wasm` module that
/// exports a working `describe()` returning a JSON **object** manifest. This is
/// the whole v1 contract — the module is *listed*, never *run*. Actually
/// executing a dynamic app requires the component-model host (typed WIT world,
/// async host imports for fetch/storage, budget + politeness enforcement across
/// the boundary), which is the documented next slice, deliberately not faked
/// here: there is NO execution path for these modules.
pub struct DynamicAppManifest {
    /// File stem of the module — the app's listing name (authoritative; a
    /// `name` key inside the manifest is ignored, matching plugin manifests).
    pub name: String,
    /// The parsed `describe()` output (always a JSON object).
    pub manifest: Value,
}

/// Scans `dir` for `.wasm` modules exporting `describe()` and returns their
/// manifests, sorted by name. Modules that fail to compile, lack `describe`,
/// trap, or return non-object JSON are skipped with a warning — a dynamic APP
/// (unlike an extraction plugin) must self-describe to be listable at all. A
/// missing/unreadable dir is simply empty. Each probe runs in a fresh
/// fuel-and-memory-limited store, so a hostile module can't spin discovery.
pub fn discover_dynamic_apps(dir: &Path) -> Vec<DynamicAppManifest> {
    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = match Engine::new(&config) {
        Ok(engine) => engine,
        Err(err) => {
            tracing::warn!("dynamic-app discovery: wasm engine failed: {err}");
            return Vec::new();
        }
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut apps = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wasm") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|s| s.to_str()).map(String::from) else {
            continue;
        };
        let module = match Module::from_file(&engine, &path) {
            Ok(module) => module,
            Err(err) => {
                tracing::warn!(path = %path.display(), "dynamic app failed to compile: {err}");
                continue;
            }
        };
        let pre = match pre_instantiate(&engine, &module) {
            Ok(pre) => pre,
            Err(err) => {
                tracing::warn!(path = %path.display(), "dynamic app failed to link: {err}");
                continue;
            }
        };
        match describe_manifest(&engine, &pre) {
            Some(manifest @ Value::Object(_)) => apps.push(DynamicAppManifest { name, manifest }),
            _ => tracing::warn!(
                path = %path.display(),
                "skipping dynamic app: no working describe() returning a JSON object manifest"
            ),
        }
    }
    apps.sort_by(|a, b| a.name.cmp(&b.name));
    apps
}

fn execute(
    engine: Engine,
    pre: InstancePre<StoreLimits>,
    input: String,
    params: Value,
    fuel: u64,
    max_memory: usize,
) -> Result<Value> {
    let (mut store, instance) = instantiate(&engine, &pre, fuel, max_memory)?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| Error::App("plugin exports no 'memory'".into()))?;
    let alloc = instance
        .get_typed_func::<u32, u32>(&mut store, "alloc")
        .map_err(|e| Error::App(format!("plugin missing alloc(u32)->u32: {e}")))?;

    // Prefer the params-aware `extract_v2` ABI (input is a `{doc, params}`
    // envelope); fall back to the legacy `extract` (raw document, params ignored)
    // so plugins built before the envelope keep working unchanged.
    let (func, input_bytes): (TypedFunc<(u32, u32), u64>, Vec<u8>) = match instance
        .get_typed_func::<(u32, u32), u64>(&mut store, "extract_v2")
    {
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
    serde_json::from_slice(&out)
        .map_err(|e| Error::App(format!("plugin returned invalid JSON: {e}")))
}

#[cfg(test)]
mod discovery_tests {
    use super::discover_dynamic_apps;
    use std::path::PathBuf;

    const MANIFEST_JSON: &str =
        r#"{"description":"demo dynamic app","params_schema":{"type":"object"}}"#;

    /// A module whose `describe()` returns `data` (placed at offset 16) packed
    /// as `(ptr << 32) | len`. wasmtime's default `wat` feature compiles the
    /// text form transparently, so writing it to a `.wasm` file is enough.
    fn describing_wat(data: &str) -> String {
        let escaped = data.replace('\\', "\\\\").replace('"', "\\\"");
        format!(
            "(module (memory (export \"memory\") 1) (data (i32.const 16) \"{escaped}\") \
             (func (export \"describe\") (result i64) \
               (i64.or (i64.shl (i64.const 16) (i64.const 32)) (i64.const {len}))))",
            len = data.len()
        )
    }

    fn fresh_dir(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("pumper-dynamic-apps-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn discovery_lists_only_object_describing_modules_sorted() {
        let dir = fresh_dir("mixed");
        // Two well-formed dynamic apps, written out of order to prove sorting.
        std::fs::write(dir.join("beta.wasm"), describing_wat(MANIFEST_JSON)).unwrap();
        std::fs::write(dir.join("alpha.wasm"), describing_wat(MANIFEST_JSON)).unwrap();
        // Legacy extraction-plugin shape: compiles, but no describe() → skipped.
        std::fs::write(
            dir.join("extract_only.wasm"),
            "(module (memory (export \"memory\") 1))",
        )
        .unwrap();
        // describe() returning valid JSON that is NOT an object → skipped.
        std::fs::write(dir.join("scalar.wasm"), describing_wat("42")).unwrap();
        // Non-wasm noise → ignored.
        std::fs::write(dir.join("notes.txt"), "not a module").unwrap();

        let apps = discover_dynamic_apps(&dir);
        let names: Vec<&str> = apps.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta"]);
        for app in &apps {
            assert_eq!(
                app.manifest["description"].as_str(),
                Some("demo dynamic app")
            );
            assert!(app.manifest["params_schema"].is_object());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discovery_of_missing_dir_is_empty() {
        let dir = std::env::temp_dir().join("pumper-dynamic-apps-definitely-missing");
        assert!(discover_dynamic_apps(&dir).is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal module in the plugin shape: a memory, an `alloc` and an
    /// `extract_v2` that returns a fixed byte range. Enough to instantiate.
    const FIXTURE_WAT: &str = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16) "{\"pass\":true}")
        (func (export "alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "extract_v2") (param i32 i32) (result i64)
          (i64.or (i64.shl (i64.const 16) (i64.const 32)) (i64.const 13))))"#;

    fn fixture_engine_and_module() -> (Engine, Module) {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).expect("engine");
        let module = Module::new(&engine, FIXTURE_WAT).expect("module");
        (engine, module)
    }

    /// Measures the two instantiation paths against each other, and both
    /// against the bare `Store::new` they share, so the report says which part
    /// of a plugin call the per-invocation cost actually sits in.
    ///
    /// The gate is deliberately one-sided — pre-instantiation must not be a
    /// REGRESSION — rather than "must be faster". Plugins declare no imports,
    /// so the linking this hoists out of the call path is genuinely small for
    /// them; the win is bounded, and asserting a speedup would be asserting
    /// scheduler noise. `#[ignore]`d with the other timing-dependent tests
    /// (`just test-ignored`).
    #[test]
    #[ignore = "timing-dependent microbenchmark; run with `cargo test -- --ignored`"]
    fn instance_pre_instantiation_is_never_slower_than_relinking_per_call() {
        const N: u32 = 2_000;
        let (engine, module) = fixture_engine_and_module();
        let limits = || StoreLimitsBuilder::new().memory_size(16 << 20).build();
        let per_call = |d: std::time::Duration| d.as_secs_f64() * 1e6 / N as f64;

        // The floor both paths pay: a fresh, limited, fuelled Store per call.
        // Per-call isolation is non-negotiable, so this is not reclaimable.
        let started = std::time::Instant::now();
        for _ in 0..N {
            let mut store = Store::new(&engine, limits());
            store.set_fuel(1_000_000).unwrap();
            std::hint::black_box(&mut store);
        }
        let store_only = started.elapsed();

        // BEFORE: a fresh Linker + full instantiate per call.
        let started = std::time::Instant::now();
        for _ in 0..N {
            let mut store = Store::new(&engine, limits());
            store.set_fuel(1_000_000).unwrap();
            let linker: Linker<StoreLimits> = Linker::new(&engine);
            let _ = linker
                .instantiate(&mut store, &module)
                .expect("instantiate");
        }
        let relink = started.elapsed();

        // AFTER: link once at load, then a fresh Store + InstancePre::instantiate.
        let pre = pre_instantiate(&engine, &module).expect("pre");
        let started = std::time::Instant::now();
        for _ in 0..N {
            let mut store = Store::new(&engine, limits());
            store.set_fuel(1_000_000).unwrap();
            let _ = pre.instantiate(&mut store).expect("instantiate");
        }
        let prelinked = started.elapsed();

        eprintln!(
            "instantiate x{N}: store-only {:.2}us/call | relink-per-call {:.2}us/call | \
             InstancePre {:.2}us/call",
            per_call(store_only),
            per_call(relink),
            per_call(prelinked),
        );
        assert!(
            prelinked.as_secs_f64() < relink.as_secs_f64() * 1.5,
            "pre-instantiation must never be a regression on relinking every call \
             (relink {relink:?} vs pre {prelinked:?})"
        );
    }

    #[test]
    fn max_concurrent_honors_explicit_and_derives_default() {
        // Explicit value passes through untouched.
        assert_eq!(resolve_max_concurrent(8), 8);
        // 0 → one-per-core, always at least 1 (never an empty semaphore that
        // would deadlock every plugin run).
        assert!(resolve_max_concurrent(0) >= 1);
    }
}
