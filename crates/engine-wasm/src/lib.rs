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
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use async_trait::async_trait;
use pumper_core::config::PluginConfig;
use pumper_core::error::PluginFailure;
use pumper_core::plugin::PluginRunStats;
use pumper_core::{Error, Plugins, Result};
use serde_json::Value;
use tokio::sync::Semaphore;
use wasmtime::{
    Config, Engine, Instance, InstancePre, Linker, Memory, Module, ResourceLimiter, Store,
    StoreLimits, StoreLimitsBuilder, TypedFunc,
};

/// The loaded-module index, swapped wholesale by [`Plugins::reload`].
type ModuleMap = HashMap<String, LoadedPlugin>;

pub struct WasmPluginHost {
    engine: Engine,
    dir: std::path::PathBuf,
    fuel: u64,
    max_memory: usize,
    /// Budget for the load-time `describe()` probe, derived from the same
    /// `[plugins]` config as a real call.
    probe: ProbeBudget,
    /// Global admission gate: caps concurrent `execute` calls so aggregate wasm
    /// memory (`max_memory × permits`) and blocking-pool usage stay bounded no
    /// matter how wide the caller's fan-out is.
    sem: Arc<Semaphore>,
    modules: RwLock<ModuleMap>,
    /// What each plugin has cost since it was loaded, for `GET /plugins`.
    ///
    /// Its own map rather than a field on [`LoadedPlugin`] so a reload — which
    /// replaces the index wholesale — clears it: after a hot-swap the name
    /// refers to a different binary, and carrying the old build's cost history
    /// forward under the new build's name would be the sort of quiet fiction
    /// this telemetry exists to end.
    telemetry: RwLock<HashMap<String, PluginTelemetry>>,
}

/// Per-plugin cost accumulated since the module was loaded. In-memory only:
/// this is a live gauge for "how close is this plugin running to its caps",
/// not an evidence ledger, and it deliberately resets with the process and
/// with every reload.
#[derive(Debug, Clone, Copy, Default)]
struct PluginTelemetry {
    calls: u64,
    fuel_total: u64,
    fuel_max: u64,
    fuel_last: u64,
    memory_max: usize,
    memory_last: usize,
}

impl PluginTelemetry {
    fn record(&mut self, stats: &PluginRunStats) {
        self.calls += 1;
        if let Some(fuel) = stats.fuel_used {
            self.fuel_total = self.fuel_total.saturating_add(fuel);
            self.fuel_max = self.fuel_max.max(fuel);
            self.fuel_last = fuel;
        }
        if let Some(bytes) = stats.memory_bytes {
            self.memory_max = self.memory_max.max(bytes);
            self.memory_last = bytes;
        }
    }

    /// The `GET /plugins` view. Reports the BUDGETS alongside the usage, because
    /// "18 million fuel" answers nothing on its own — "18 million of 200
    /// million" is the number an operator can act on.
    fn to_json(self, fuel_budget: u64, memory_cap: usize) -> Value {
        let avg_fuel = (self.calls > 0).then(|| self.fuel_total as f64 / self.calls as f64);
        serde_json::json!({
            "calls": self.calls,
            "fuel_last": self.fuel_last,
            "fuel_max": self.fuel_max,
            "fuel_avg": avg_fuel,
            "fuel_budget": fuel_budget,
            "memory_bytes_last": self.memory_last,
            "memory_bytes_max": self.memory_max,
            "memory_bytes_cap": memory_cap,
        })
    }
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
    /// Whether this module exports the ABI a [`Plugins::run`] call needs.
    ///
    /// Loading stays permissive on purpose — a `.wasm` in the plugin dir that
    /// only exports `describe()` is a legitimate module (that is exactly the
    /// dynamic-app shape) and must keep appearing in `GET /plugins`. But it can
    /// never gate or shape anything, and reporting it as a usable plugin was a
    /// lie the repo's own `extract_only.wasm` fixture proved: `has()` said
    /// `true` for a module with no `alloc` and no `extract`, so a trigger hook
    /// pointed at it looked deployed and silently did nothing.
    executable: bool,
}

/// Whether `module` exports the ABI [`Plugins::run`] needs: a `memory`, an
/// `alloc`, and at least one of `extract_v2` / `extract`.
///
/// Names only — the exact signatures are re-checked per call, where a
/// wrong-typed export surfaces as a `missing_export` failure. This is the cheap
/// load-time answer to "could this ever run at all", which is the question
/// `has()` and `list()` are asked.
fn exports_extract_abi(module: &Module) -> bool {
    let names: std::collections::HashSet<&str> = module.exports().map(|e| e.name()).collect();
    names.contains("memory")
        && names.contains("alloc")
        && (names.contains("extract_v2") || names.contains("extract"))
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

/// The fuel + memory budget one `describe()` manifest probe runs under.
///
/// This used to be two constants nailed into the source — 10M fuel and a bare
/// `16 * 1024 * 1024` — that no configuration could reach. Raising `[plugins]
/// fuel` for a plugin whose `describe()` legitimately needed more did nothing
/// (its manifest silently stayed absent), and the probe's 16 MiB quietly
/// contradicted a configured `max_memory_mb` of 64. A manifest read IS a plugin
/// call, so it runs under the plugin call's own configured limits.
#[derive(Debug, Clone, Copy)]
struct ProbeBudget {
    fuel: u64,
    max_memory: usize,
}

impl ProbeBudget {
    fn from_config(cfg: &PluginConfig) -> Self {
        Self {
            fuel: cfg.fuel,
            max_memory: cfg.max_memory_mb.saturating_mul(1024 * 1024),
        }
    }
}

impl WasmPluginHost {
    pub fn new(cfg: &PluginConfig) -> Result<Self> {
        let mut config = Config::new();
        config.consume_fuel(true); // enables the per-call instruction budget

        // Deliberately NOT an `Error::Plugin`: no plugin is involved yet. This is
        // the wasmtime engine itself refusing to exist, which is a startup fault.
        let engine = Engine::new(&config).map_err(|e| Error::App(format!("wasm engine: {e}")))?;
        std::fs::create_dir_all(&cfg.dir)?;
        let probe = ProbeBudget::from_config(cfg);
        let modules = load_dir(&engine, &cfg.dir, probe);
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
            probe,
            sem: Arc::new(Semaphore::new(max_concurrent)),
            modules: RwLock::new(modules),
            telemetry: RwLock::new(HashMap::new()),
        })
    }

    /// Reads the cost gauges, recovering from poisoning like the module index.
    /// Worst case for a recovered read is one stale counter on a diagnostic
    /// surface — nothing here is evidence.
    fn read_telemetry(&self) -> RwLockReadGuard<'_, HashMap<String, PluginTelemetry>> {
        match self.telemetry.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn_poisoned();
                poisoned.into_inner()
            }
        }
    }

    fn write_telemetry(&self) -> RwLockWriteGuard<'_, HashMap<String, PluginTelemetry>> {
        match self.telemetry.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn_poisoned();
                poisoned.into_inner()
            }
        }
    }

    /// Reads the module index, **recovering** from a poisoned lock rather than
    /// propagating it.
    ///
    /// The anti-pattern this replaces: five bare `.read()/.write().unwrap()`s.
    /// A `std::sync::RwLock` is poisoned *permanently* by one panic under its
    /// write guard, so a single unlucky reload turned every plugin call, every
    /// `GET /plugins`, every `has()` on the trigger hot path and every
    /// subsequent reload into a panic — for the rest of the process's life.
    ///
    /// Poisoning is a warning about the DATA, and this data cannot be left
    /// half-written: the only writer replaces the whole map in one move
    /// (`*guard = modules`), after `load_dir` has already finished building it
    /// off-lock. A recovered reader therefore sees either the complete old map
    /// or the complete new one — never a torn one. (Same reasoning as the
    /// server's `lock_advisory` carve-out; that helper is `Mutex`-only and
    /// private to the server crate, so the recovery lives here.)
    fn read_modules(&self) -> RwLockReadGuard<'_, ModuleMap> {
        match self.modules.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn_poisoned();
                poisoned.into_inner()
            }
        }
    }

    /// Write half of [`read_modules`] — same carve-out, same reasoning.
    fn write_modules(&self) -> RwLockWriteGuard<'_, ModuleMap> {
        match self.modules.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn_poisoned();
                poisoned.into_inner()
            }
        }
    }
}

fn warn_poisoned() {
    tracing::warn!(
        "recovering a poisoned wasm plugin index lock — some earlier task panicked while \
         holding it. The index is replaced wholesale, never edited in place, so what is \
         reused here is a structurally complete map; the panic itself was reported where \
         it happened"
    );
}

/// Runs `work` on the blocking pool under an admission permit that travels
/// **with the work**, not with the caller.
///
/// The anti-pattern this replaces, and the reason the gate was a lie: the
/// permit was an `OwnedSemaphorePermit` bound in the async fn's own frame while
/// the wasm ran inside `spawn_blocking`. `spawn_blocking` is uncancellable — the
/// thread runs to completion no matter what — but dropping the *future* (a
/// worker timeout, a `select!` branch losing a race, a disconnected client)
/// drops that frame, releasing the permit while the orphaned thread is still
/// burning its fuel budget inside a live `Store`. A caller that cancels N times
/// therefore admits N+1 concurrent stores, so live wasm memory could exceed the
/// `max_concurrent × max_memory` bound this gate exists to enforce, precisely
/// under the load that produces timeouts.
async fn run_admitted<T: Send + 'static>(
    sem: Arc<Semaphore>,
    plugin: &str,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T> {
    // Acquired BEFORE spawn_blocking so excess callers wait here rather than
    // piling onto the blocking pool. The semaphore is never closed, so the only
    // error is impossible — map it defensively.
    let permit = sem.acquire_owned().await.map_err(|e| {
        Error::plugin(
            PluginFailure::Host,
            plugin,
            format!("plugin admission gate closed: {e}"),
        )
    })?;
    tokio::task::spawn_blocking(move || {
        // The permit lives HERE, in the blocking closure's frame, so the slot is
        // returned when the work actually stops — never when the caller merely
        // stops waiting for it.
        let _permit = permit;
        work()
    })
    .await
    .map_err(|e| {
        Error::plugin(
            PluginFailure::Host,
            plugin,
            format!("blocking plugin task panicked: {e}"),
        )
    })
}

#[async_trait]
impl Plugins for WasmPluginHost {
    /// The value-only call, in terms of the metered one — a single execution
    /// path, so a caller that ignores the cost can never diverge in behaviour
    /// from one that reads it.
    async fn run(&self, name: &str, input: &str, params: &Value) -> Result<Value> {
        self.run_metered(name, input, params)
            .await
            .map(|(value, _)| value)
    }

    async fn run_metered(
        &self,
        name: &str,
        input: &str,
        params: &Value,
    ) -> Result<(Value, PluginRunStats)> {
        let pre = self
            .read_modules()
            .get(name)
            .map(|p| p.pre.clone())
            .ok_or_else(|| {
                Error::plugin(
                    PluginFailure::Unknown,
                    name,
                    "no module of that name is loaded — build and install it \
                     (`just plugins-install`), then POST /plugins/reload",
                )
            })?;
        let engine = self.engine.clone();
        let plugin = name.to_string();
        let input = input.to_string();
        let params = params.clone();
        let (fuel, max_memory) = (self.fuel, self.max_memory);
        // Global admission: a wide fan-out (e.g. a 200-URL plugin job) can't
        // spin up 200 stores at once. Wasm execution is synchronous and
        // CPU-bound, so it runs off the async runtime — and the permit rides
        // along with it (see `run_admitted`).
        let (value, stats) = run_admitted(self.sem.clone(), name, move || {
            execute(engine, pre, &plugin, input, params, fuel, max_memory)
        })
        .await??;
        // Gauges only — a failed call is reported through the error, and folding
        // a trap's partial burn into an average would make "how expensive is this
        // plugin" answer about failures instead of about work done.
        self.write_telemetry()
            .entry(name.to_string())
            .or_default()
            .record(&stats);
        Ok((value, stats))
    }

    /// The plugins a caller can actually RUN — `executable` only.
    ///
    /// A describe-only module stays loaded and stays visible in
    /// [`manifests`](Plugins::manifests) (that is the dynamic-app discovery
    /// shape), but handing its name back here would mean the observatory
    /// replaying a module that cannot execute and a UI offering it for a hook.
    fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .read_modules()
            .iter()
            .filter(|(_, p)| p.executable)
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        names
    }

    /// Map lookup rather than the trait's default list-and-scan: trigger hooks
    /// ask this per event, per hook.
    ///
    /// Answers **executability**, not mere presence. The trait documents this as
    /// "would `run` find a module at all", and a module without the extract ABI
    /// is one `run` can never serve — reporting it as present made a hook
    /// pointed at it look deployed while it silently did nothing.
    fn has(&self, name: &str) -> bool {
        self.read_modules().get(name).is_some_and(|p| p.executable)
    }

    /// Every loaded module, executable or not, each with an explicit
    /// `executable` flag — `GET /plugins` is the discovery surface, so hiding a
    /// module here would hide the very mistake an operator needs to see.
    fn manifests(&self) -> Vec<Value> {
        let modules = self.read_modules();
        let mut entries: Vec<(&String, &LoadedPlugin)> = modules.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let telemetry = self.read_telemetry();
        entries
            .into_iter()
            .map(|(name, p)| {
                let mut m = match &p.manifest {
                    // A plugin's own describe() output, with its name authoritative.
                    Some(Value::Object(m)) => m.clone(),
                    _ => serde_json::Map::new(),
                };
                m.insert("name".into(), Value::String(name.clone()));
                m.insert("executable".into(), Value::Bool(p.executable));
                // Present with `calls: 0` for a plugin nothing has run yet —
                // "never invoked" is an answer, and omitting the key would make
                // it indistinguishable from "this host does not measure".
                m.insert(
                    "telemetry".into(),
                    telemetry
                        .get(name)
                        .copied()
                        .unwrap_or_default()
                        .to_json(self.fuel, self.max_memory),
                );
                Value::Object(m)
            })
            .collect()
    }

    async fn reload(&self) -> Result<usize> {
        // load_dir is synchronous fs + a full Cranelift compile per module. Run it
        // off the async runtime — as `run` already does for the same reason — so a
        // dir of 10-20 modules (~0.2-2s of compile) doesn't park a tokio worker and
        // stall unrelated in-flight requests. Only the brief lock swap stays inline.
        let (engine, dir, probe) = (self.engine.clone(), self.dir.clone(), self.probe);
        let modules = tokio::task::spawn_blocking(move || load_dir(&engine, &dir, probe))
            .await
            .map_err(|e| {
                Error::plugin(
                    PluginFailure::Host,
                    "<reload>",
                    format!("plugin reload task panicked: {e}"),
                )
            })?;
        let count = modules.len();
        *self.write_modules() = modules;
        // The names now refer to freshly compiled binaries; carrying the old
        // builds' cost history forward under them would be a fiction.
        self.write_telemetry().clear();
        tracing::info!(count, "reloaded wasm plugins");
        Ok(count)
    }
}

fn load_dir(engine: &Engine, dir: &Path, probe: ProbeBudget) -> ModuleMap {
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
        let executable = exports_extract_abi(&module);
        match pre_instantiate(engine, &name, &module) {
            Ok(pre) => {
                // Read the optional self-describing manifest once, best-effort —
                // a missing/failed `describe` degrades to name-only metadata,
                // but it is REPORTED (it used to vanish into `.ok()?`).
                let manifest = match describe_manifest(engine, &pre, &name, probe) {
                    Ok(manifest) => Some(manifest),
                    Err(miss) => {
                        log_describe_miss(&path, &miss);
                        None
                    }
                };
                if !executable {
                    tracing::info!(
                        path = %path.display(),
                        "loaded a module with no extract ABI: it is listed (with \
                         executable: false) but can never serve a run() call or a \
                         trigger hook"
                    );
                }
                map.insert(
                    name,
                    LoadedPlugin {
                        pre,
                        manifest,
                        executable,
                    },
                );
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
fn pre_instantiate(
    engine: &Engine,
    plugin: &str,
    module: &Module,
) -> Result<InstancePre<StoreLimits>> {
    let linker: Linker<StoreLimits> = Linker::new(engine);
    linker.instantiate_pre(module).map_err(|e| {
        Error::plugin(
            PluginFailure::MissingExport,
            plugin,
            format!("module declares imports the sandbox grants nothing for: {e}"),
        )
    })
}

/// Builds a fuel-and-memory-limited store and instantiates `pre` in it.
///
/// The store is per-call by design: fuel budget, linear memory and any residue
/// a previous invocation left behind must not be visible to the next one. Only
/// the *linking* work is shared, via the caller's [`InstancePre`].
fn instantiate(
    engine: &Engine,
    pre: &InstancePre<StoreLimits>,
    plugin: &str,
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
    // Fuel metering is enabled on the Engine, so this only fails if the host
    // built the engine wrong — our bug, not the plugin's.
    store.set_fuel(fuel).map_err(|e| {
        Error::plugin(
            PluginFailure::Host,
            plugin,
            format!("could not set the fuel budget: {e}"),
        )
    })?;
    // Classed as a sandbox stop rather than a host error: with imports already
    // resolved at load time, what fails here is the store's resource limiter
    // refusing the module's declared memory/tables — i.e. the caps doing their
    // job, which is what the operator needs to read out of the failure.
    let instance = pre.instantiate(&mut store).map_err(|e| {
        Error::plugin(
            PluginFailure::Trap,
            plugin,
            format!("instantiation refused (memory/table limits): {e}"),
        )
    })?;
    Ok((store, instance))
}

/// Reads and validates a plugin's packed `(out_ptr << 32 | out_len)` return,
/// returning the output bytes. Guards the guest-controlled `out_len` against the
/// module's own linear-memory size BEFORE allocating, so a crafted return can't
/// drive a giant host-side allocation and abort the process.
fn read_packed(
    store: &mut Store<StoreLimits>,
    memory: &Memory,
    plugin: &str,
    packed: u64,
) -> Result<Vec<u8>> {
    let out_ptr = (packed >> 32) as usize;
    let out_len = (packed & 0xffff_ffff) as usize;
    let mem_size = memory.data_size(&*store);
    if out_ptr
        .checked_add(out_len)
        .is_none_or(|end| end > mem_size)
    {
        return Err(Error::plugin(
            PluginFailure::MalformedOutput,
            plugin,
            format!("output range out of bounds: ptr={out_ptr} len={out_len} mem={mem_size}"),
        ));
    }
    let mut out = vec![0u8; out_len];
    memory.read(&*store, out_ptr, &mut out).map_err(|e| {
        Error::plugin(
            PluginFailure::MalformedOutput,
            plugin,
            format!("output bytes unreadable: {e}"),
        )
    })?;
    Ok(out)
}

/// Why a `describe()` probe produced no manifest.
///
/// Two cases, deliberately kept apart, because they mean opposite things about
/// the module: not having a `describe` export at all is the LEGAL legacy
/// extraction-plugin shape, while having one that fails is a defect.
enum DescribeMiss {
    /// No `describe` export (or no `memory` to read a manifest out of).
    NoExport,
    /// It exports `describe` and the probe still failed: a trap, fuel
    /// exhaustion, an out-of-range return, non-JSON bytes.
    Broken(String),
}

/// The single place a failed `describe()` probe is reported, so the plugin-load
/// path and dynamic-app discovery say the same thing about the same module.
///
/// The anti-pattern this replaces: the load path swallowed every miss through
/// `.ok()?` while discovery `warn!`ed about all of them — so a genuinely broken
/// manifest was *silent* when the module was loaded as a plugin, and an
/// ordinary describe-less extraction plugin was *noisy* when the same directory
/// was scanned for dynamic apps. The level now follows the DEFECT, not the
/// caller.
fn log_describe_miss(path: &Path, miss: &DescribeMiss) {
    match miss {
        DescribeMiss::NoExport => tracing::debug!(
            path = %path.display(),
            "no describe() manifest — metadata degrades to name-only"
        ),
        DescribeMiss::Broken(why) => tracing::warn!(
            path = %path.display(),
            "describe() is exported but failed, so this module has no manifest: {why}"
        ),
    }
}

/// Best-effort read of a plugin's `describe() -> u64` manifest, under the
/// configured probe budget. The reason for a miss is returned rather than
/// dropped, so every caller can report it (see [`log_describe_miss`]).
fn describe_manifest(
    engine: &Engine,
    pre: &InstancePre<StoreLimits>,
    plugin: &str,
    budget: ProbeBudget,
) -> std::result::Result<Value, DescribeMiss> {
    let (mut store, instance) = instantiate(engine, pre, plugin, budget.fuel, budget.max_memory)
        .map_err(|e| DescribeMiss::Broken(e.to_string()))?;
    let Some(memory) = instance.get_memory(&mut store, "memory") else {
        return Err(DescribeMiss::NoExport);
    };
    let Ok(describe) = instance.get_typed_func::<(), u64>(&mut store, "describe") else {
        return Err(DescribeMiss::NoExport);
    };
    let packed = describe
        .call(&mut store, ())
        .map_err(|e| DescribeMiss::Broken(format!("describe() trapped: {e}")))?;
    let bytes = read_packed(&mut store, &memory, plugin, packed)
        .map_err(|e| DescribeMiss::Broken(e.to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| DescribeMiss::Broken(format!("describe() output is not JSON: {e}")))
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
///
/// Probes run under the DEFAULT `[plugins]` budget. Callers holding the live
/// config should use [`discover_dynamic_apps_with`] so a deployment that raised
/// `fuel`/`max_memory_mb` gets the budget it asked for here too.
pub fn discover_dynamic_apps(dir: &Path) -> Vec<DynamicAppManifest> {
    discover_dynamic_apps_with(dir, &PluginConfig::default())
}

/// [`discover_dynamic_apps`] with the probe budget taken from `cfg` — the same
/// `fuel` / `max_memory_mb` a real plugin call runs under.
pub fn discover_dynamic_apps_with(dir: &Path, cfg: &PluginConfig) -> Vec<DynamicAppManifest> {
    let budget = ProbeBudget::from_config(cfg);
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
        let pre = match pre_instantiate(&engine, &name, &module) {
            Ok(pre) => pre,
            Err(err) => {
                tracing::warn!(path = %path.display(), "dynamic app failed to link: {err}");
                continue;
            }
        };
        match describe_manifest(&engine, &pre, &name, budget) {
            Ok(manifest @ Value::Object(_)) => apps.push(DynamicAppManifest { name, manifest }),
            // A manifest that parsed but is not an object is as unusable as a
            // missing one for a dynamic app, and is a defect either way.
            Ok(other) => log_describe_miss(
                &path,
                &DescribeMiss::Broken(format!(
                    "describe() returned {other}, not a JSON object manifest"
                )),
            ),
            Err(miss) => log_describe_miss(&path, &miss),
        }
    }
    apps.sort_by(|a, b| a.name.cmp(&b.name));
    apps
}

#[allow(clippy::too_many_arguments)]
fn execute(
    engine: Engine,
    pre: InstancePre<StoreLimits>,
    plugin: &str,
    input: String,
    params: Value,
    fuel: u64,
    max_memory: usize,
) -> Result<(Value, PluginRunStats)> {
    let (mut store, instance) = instantiate(&engine, &pre, plugin, fuel, max_memory)?;
    let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
        Error::plugin(PluginFailure::MissingExport, plugin, "exports no 'memory'")
    })?;
    let alloc = instance
        .get_typed_func::<u32, u32>(&mut store, "alloc")
        .map_err(|e| {
            Error::plugin(
                PluginFailure::MissingExport,
                plugin,
                format!("missing alloc(u32)->u32: {e}"),
            )
        })?;

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
                    .map_err(|e| {
                        Error::plugin(
                            PluginFailure::MissingExport,
                            plugin,
                            format!("exports neither extract_v2 nor extract(u32,u32)->u64: {e}"),
                        )
                    })?;
                (f, input.into_bytes())
            }
        };

    let len = input_bytes.len() as u32;
    let in_ptr = alloc
        .call(&mut store, len)
        .map_err(|e| Error::plugin(PluginFailure::Trap, plugin, format!("alloc trapped: {e}")))?;
    // The guest handed back a pointer we cannot write `len` bytes to — its own
    // ABI contract, broken on the input side.
    memory
        .write(&mut store, in_ptr as usize, &input_bytes)
        .map_err(|e| {
            Error::plugin(
                PluginFailure::MalformedOutput,
                plugin,
                format!("alloc({len}) returned an unwritable pointer {in_ptr}: {e}"),
            )
        })?;

    // On fuel exhaustion / OOM this returns a trap — the sandbox holds.
    let packed = func.call(&mut store, (in_ptr, len)).map_err(|e| {
        Error::plugin(
            PluginFailure::Trap,
            plugin,
            format!("trapped (fuel/memory/panic): {e}"),
        )
    })?;

    let out = read_packed(&mut store, &memory, plugin, packed)?;
    let value: Value = serde_json::from_slice(&out).map_err(|e| {
        Error::plugin(
            PluginFailure::MalformedOutput,
            plugin,
            format!("returned invalid JSON: {e}"),
        )
    })?;
    Ok((value, measure(&mut store, &memory, fuel, max_memory)))
}

/// What the call that just finished in `store` cost.
///
/// The sandbox metered nothing it enforced: fuel was set and never read back,
/// and the memory high-water was never observed at all — so an operator could
/// not see how close a plugin ran to caps the host was already policing, and the
/// observatory substituted wall-clock elapsed time for cost by its own
/// admission.
///
/// Both numbers are exact rather than sampled. Fuel is the budget minus what
/// `set_fuel` left; linear memory only ever GROWS inside a store, and every call
/// gets a fresh store, so the size now is this call's high-water by
/// construction.
fn measure(
    store: &mut Store<StoreLimits>,
    memory: &Memory,
    fuel: u64,
    max_memory: usize,
) -> PluginRunStats {
    PluginRunStats {
        // `get_fuel` errors only when the engine has fuel metering off, which
        // this host always sets — so a miss means "we cannot say", never 0.
        fuel_used: store.get_fuel().ok().map(|left| fuel.saturating_sub(left)),
        fuel_budget: Some(fuel),
        memory_bytes: Some(memory.data_size(&*store)),
        memory_cap_bytes: Some(max_memory),
    }
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
        let pre = pre_instantiate(&engine, "fixture", &module).expect("pre");
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

    /// Blocks until told to stop, announcing when it started — a stand-in for
    /// wasm burning through a fuel budget on an uncancellable thread.
    fn blocking_work() -> (
        impl FnOnce() + Send + 'static,
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let work = move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
        };
        (work, started_rx, release_tx)
    }

    /// THE admission bug: the permit used to live in the async fn's frame while
    /// the work ran on an uncancellable `spawn_blocking` thread. A caller that
    /// stops waiting (worker timeout, lost `select!` race, dropped request)
    /// dropped that frame and returned the slot — while the orphaned thread was
    /// still holding a live `Store` with up to `max_memory` of wasm memory. The
    /// gate's whole promise, `max_concurrent × max_memory`, was therefore void
    /// exactly under the load that produces cancellations.
    ///
    /// Semaphore-saturation shaped, no sleeps: cancel a call, then try to admit
    /// another one while the first is provably still running.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cancelled_caller_cannot_admit_a_second_store_while_the_first_still_runs() {
        let sem = Arc::new(Semaphore::new(1));
        let (work, started, release) = blocking_work();
        let mut call = Box::pin(run_admitted(sem.clone(), "burner", work));

        // Drive the call far enough that the blocking work has genuinely begun,
        // then abandon it — the caller's cancellation.
        tokio::select! {
            _ = &mut call => panic!("the work cannot complete: it is blocked on release"),
            started = started => started.expect("the blocking work started"),
        }
        drop(call);

        assert!(
            sem.try_acquire().is_err(),
            "the caller went away but its store is STILL live on an uncancellable thread — \
             admitting another call here is exactly how max_concurrent × max_memory is exceeded"
        );

        // The slot comes back when the WORK stops, not when the caller does.
        release
            .send(())
            .expect("the orphaned thread is still there");
        let permit = tokio::time::timeout(std::time::Duration::from_secs(5), sem.acquire())
            .await
            .expect("the permit must be released once the work actually finishes")
            .expect("the gate is never closed");
        drop(permit);
    }

    /// The shape that was replaced, pinned so nobody reintroduces it: holding
    /// the permit in the ASYNC frame frees the slot on cancellation alone. This
    /// asserts the BUG, on a hand-rolled copy of the old code, so the assertion
    /// above is a real difference rather than a tautology.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn holding_the_permit_in_the_async_frame_is_what_broke_the_bound() {
        let sem = Arc::new(Semaphore::new(1));
        let (work, started, release) = blocking_work();
        let gate = sem.clone();
        let mut call = Box::pin(async move {
            // The old shape: permit bound here, work moved elsewhere.
            let _permit = gate.acquire_owned().await.expect("gate open");
            let _ = tokio::task::spawn_blocking(work).await;
        });
        tokio::select! {
            _ = &mut call => panic!("the work cannot complete: it is blocked on release"),
            started = started => started.expect("the blocking work started"),
        }
        drop(call);
        assert!(
            sem.try_acquire().is_ok(),
            "this is the anti-pattern: the slot is free while the thread it accounted for runs"
        );
        release.send(()).ok();
    }

    /// The permanent-500 shape, at the plugin host: one panic under the write
    /// guard poisons a `std::sync::RwLock` forever, and five bare `.unwrap()`s
    /// meant every later call, listing, `has()` and reload panicked for the rest
    /// of the process's life. The index is replaced wholesale, so recovery hands
    /// back a structurally complete map.
    #[test]
    fn a_poisoned_module_index_degrades_instead_of_killing_every_plugin_call() {
        let dir = fresh_host_dir("poison");
        std::fs::write(dir.join("here.wasm"), FIXTURE_WAT).expect("write fixture");
        let host = WasmPluginHost::new(&PluginConfig {
            dir: dir.clone(),
            ..Default::default()
        })
        .expect("host");
        assert_eq!(host.list(), vec!["here".to_string()]);

        // A holder dies mid-write, exactly as a panicking reload would.
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = host.modules.write().expect("not poisoned yet");
            panic!("holder died");
        }));
        assert!(caught.is_err(), "the holder really did unwind");
        assert!(
            host.modules.is_poisoned(),
            "and the lock really is poisoned"
        );
        assert!(
            host.modules.read().is_err(),
            "so a bare `.read().unwrap()` here would panic — the permanent-failure generator"
        );

        // Every reader still answers, with the data intact.
        for _ in 0..3 {
            assert_eq!(host.list(), vec!["here".to_string()]);
            assert!(host.has("here"));
            assert!(!host.has("gone"));
            assert_eq!(host.manifests().len(), 1);
        }
        // …and the writer can still swap the index.
        *host.write_modules() = ModuleMap::new();
        assert!(host.list().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The probe budget used to be two constants (`10_000_000` fuel, a bare
    /// 16 MiB) that no config could reach: a deployment that raised `[plugins]
    /// fuel` for a describe-heavy plugin got nothing, and the 16 MiB silently
    /// contradicted a configured 64 MiB cap.
    #[test]
    fn describe_probe_budget_follows_config_not_a_hidden_constant() {
        let budget = ProbeBudget::from_config(&PluginConfig {
            fuel: 7_777,
            max_memory_mb: 3,
            ..Default::default()
        });
        assert_eq!(budget.fuel, 7_777);
        assert_eq!(budget.max_memory, 3 * 1024 * 1024);
        assert_ne!(budget.fuel, 10_000_000, "the old hardcoded probe fuel");
        assert_ne!(budget.max_memory, 16 * 1024 * 1024, "the old hardcoded cap");
        // A default host derives the same budget a real call runs under.
        let cfg = PluginConfig::default();
        let budget = ProbeBudget::from_config(&cfg);
        assert_eq!(budget.fuel, cfg.fuel);
        assert_eq!(budget.max_memory, cfg.max_memory_mb * 1024 * 1024);
    }

    /// Every failure the CALL path can produce is classified, so consumers
    /// (observatory buckets, the trigger ledger) never have to read the prose.
    #[tokio::test]
    async fn call_failures_carry_a_typed_class_not_just_a_message() {
        let dir = fresh_host_dir("classes");
        // Legal module, but not an executable plugin: no alloc, no extract.
        std::fs::write(
            dir.join("describe_only.wasm"),
            "(module (memory (export \"memory\") 1))",
        )
        .expect("write fixture");
        // Returns bytes that are not JSON.
        std::fs::write(
            dir.join("garbage.wasm"),
            "(module (memory (export \"memory\") 1) (data (i32.const 16) \"not json\") \
             (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
             (func (export \"extract_v2\") (param i32 i32) (result i64) \
               (i64.or (i64.shl (i64.const 16) (i64.const 32)) (i64.const 8))))",
        )
        .expect("write fixture");
        // Traps immediately.
        std::fs::write(
            dir.join("boom.wasm"),
            "(module (memory (export \"memory\") 1) \
             (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
             (func (export \"extract_v2\") (param i32 i32) (result i64) (unreachable)))",
        )
        .expect("write fixture");
        let host = WasmPluginHost::new(&PluginConfig {
            dir: dir.clone(),
            ..Default::default()
        })
        .expect("host");

        for (name, expected) in [
            ("nowhere", PluginFailure::Unknown),
            ("describe_only", PluginFailure::MissingExport),
            ("boom", PluginFailure::Trap),
            ("garbage", PluginFailure::MalformedOutput),
        ] {
            let err = host
                .run(name, "<doc/>", &Value::Null)
                .await
                .expect_err("must fail");
            assert_eq!(
                err.plugin_failure(),
                Some(expected),
                "{name} misclassified: {err}"
            );
            assert!(
                err.to_string().contains(name),
                "the failure must name the module: {err}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Returns `null`, having spun a counted loop `iters` times — a plugin whose
    /// fuel appetite is a knob rather than an accident.
    fn burner_wat(iters: i32) -> String {
        format!(
            "(module (memory (export \"memory\") 1) (data (i32.const 16) \"null\") \
             (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
             (func (export \"extract_v2\") (param i32 i32) (result i64) (local $i i32) \
               (local.set $i (i32.const {iters})) \
               (block $done (loop $l \
                 (br_if $done (i32.eqz (local.get $i))) \
                 (local.set $i (i32.sub (local.get $i) (i32.const 1))) \
                 (br $l))) \
               (i64.or (i64.shl (i64.const 16) (i64.const 32)) (i64.const 4))))"
        )
    }

    /// Returns `null` after growing linear memory by `pages` — the memory
    /// high-water has to reflect what the guest actually took.
    fn growing_wat(pages: i32) -> String {
        format!(
            "(module (memory (export \"memory\") 1) (data (i32.const 16) \"null\") \
             (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
             (func (export \"extract_v2\") (param i32 i32) (result i64) \
               (drop (memory.grow (i32.const {pages}))) \
               (i64.or (i64.shl (i64.const 16) (i64.const 32)) (i64.const 4))))"
        )
    }

    /// The sandbox metered nothing it enforced: fuel was set and never read
    /// back, memory high-water never observed. An operator could not see how
    /// close a plugin ran to caps the host was already policing, and the
    /// observatory substituted wall-clock time for cost by its own admission.
    #[tokio::test]
    async fn a_call_reports_the_fuel_it_burned_against_the_budget_it_had() {
        let dir = fresh_host_dir("fuel");
        std::fs::write(dir.join("idle.wasm"), burner_wat(1)).expect("write");
        std::fs::write(dir.join("busy.wasm"), burner_wat(20_000)).expect("write");
        const BUDGET: u64 = 5_000_000;
        let host = WasmPluginHost::new(&PluginConfig {
            dir: dir.clone(),
            fuel: BUDGET,
            ..Default::default()
        })
        .expect("host");

        let (idle_out, idle) = host
            .run_metered("idle", "<doc/>", &Value::Null)
            .await
            .expect("idle runs");
        assert_eq!(idle_out, Value::Null, "the value is unaffected by metering");
        let (_, busy) = host
            .run_metered("busy", "<doc/>", &Value::Null)
            .await
            .expect("busy runs");

        let (idle_fuel, busy_fuel) = (
            idle.fuel_used.expect("the wasm host meters"),
            busy.fuel_used.expect("the wasm host meters"),
        );
        assert!(
            busy_fuel > idle_fuel,
            "a plugin that spins 20k times must cost more than one that spins once \
             ({busy_fuel} vs {idle_fuel})"
        );
        assert!(
            busy_fuel <= BUDGET,
            "a call that RETURNED cannot have spent more than its budget: {busy_fuel} > {BUDGET}"
        );
        assert!(idle_fuel > 0, "even a trivial call executes instructions");
        assert_eq!(
            busy.fuel_budget,
            Some(BUDGET),
            "the ceiling travels with it"
        );
        // …which is what makes "how close to the cap" answerable at all.
        let fraction = busy.fuel_fraction().expect("metered");
        assert!((0.0..=1.0).contains(&fraction), "{fraction}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_call_reports_the_memory_high_water_it_reached() {
        let dir = fresh_host_dir("memory");
        const PAGE: usize = 64 * 1024;
        std::fs::write(dir.join("small.wasm"), growing_wat(0)).expect("write");
        std::fs::write(dir.join("hungry.wasm"), growing_wat(4)).expect("write");
        let host = WasmPluginHost::new(&PluginConfig {
            dir: dir.clone(),
            ..Default::default()
        })
        .expect("host");

        let (_, small) = host
            .run_metered("small", "<doc/>", &Value::Null)
            .await
            .expect("small runs");
        let (_, hungry) = host
            .run_metered("hungry", "<doc/>", &Value::Null)
            .await
            .expect("hungry runs");
        assert_eq!(small.memory_bytes, Some(PAGE));
        assert_eq!(
            hungry.memory_bytes,
            Some(5 * PAGE),
            "growth inside the call must be reflected — wasm memory only grows, so \
             the size after the call IS this call's high-water"
        );
        assert_eq!(
            hungry.memory_cap_bytes,
            Some(PluginConfig::default().max_memory_mb * 1024 * 1024)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A plugin built before the params envelope exports only `extract`. It must
    /// keep running unchanged — and be metered like any other call.
    #[tokio::test]
    async fn the_legacy_extract_fallback_still_runs_and_is_metered() {
        let dir = fresh_host_dir("legacy");
        std::fs::write(
            dir.join("legacy.wasm"),
            "(module (memory (export \"memory\") 1) (data (i32.const 16) \"null\") \
             (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
             (func (export \"extract\") (param i32 i32) (result i64) \
               (i64.or (i64.shl (i64.const 16) (i64.const 32)) (i64.const 4))))",
        )
        .expect("write");
        let host = WasmPluginHost::new(&PluginConfig {
            dir: dir.clone(),
            ..Default::default()
        })
        .expect("host");
        assert!(
            host.has("legacy"),
            "the legacy ABI is still an executable one"
        );
        let (out, stats) = host
            .run_metered("legacy", "<doc/>", &serde_json::json!({"ignored": true}))
            .await
            .expect("legacy plugin runs");
        assert_eq!(out, Value::Null);
        assert!(stats.is_metered());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The read surface: `GET /plugins` has to answer "how close is this running
    /// to its caps" without a new subsystem. A plugin nothing has run reports
    /// `calls: 0` rather than being absent — "never invoked" is an answer, and
    /// silence would read as "this host does not measure".
    #[tokio::test]
    async fn manifests_expose_per_plugin_cost_and_reset_on_reload() {
        let dir = fresh_host_dir("telemetry");
        std::fs::write(dir.join("busy.wasm"), burner_wat(5_000)).expect("write");
        std::fs::write(dir.join("untouched.wasm"), burner_wat(1)).expect("write");
        let host = WasmPluginHost::new(&PluginConfig {
            dir: dir.clone(),
            ..Default::default()
        })
        .expect("host");

        let entry = |ms: &[Value], name: &str| -> Value {
            ms.iter()
                .find(|m| m["name"] == serde_json::json!(name))
                .expect("listed")
                .clone()
        };
        let before = host.manifests();
        assert_eq!(entry(&before, "busy")["telemetry"]["calls"], 0);

        for _ in 0..2 {
            host.run("busy", "<doc/>", &Value::Null).await.expect("run");
        }
        let after = host.manifests();
        let t = entry(&after, "busy")["telemetry"].clone();
        assert_eq!(t["calls"], 2);
        assert!(t["fuel_last"].as_u64().expect("fuel") > 0);
        assert_eq!(
            t["fuel_max"], t["fuel_last"],
            "identical calls, identical cost"
        );
        assert!(t["fuel_avg"].as_f64().expect("avg") > 0.0);
        assert_eq!(
            t["fuel_budget"],
            serde_json::json!(PluginConfig::default().fuel),
            "the budget rides along, or the usage number answers nothing"
        );
        assert!(t["memory_bytes_last"].as_u64().expect("memory") > 0);
        // A plugin nobody ran is honest about that rather than absent.
        assert_eq!(entry(&after, "untouched")["telemetry"]["calls"], 0);

        // A reload swaps the binaries these names refer to; carrying the old
        // build's cost forward under the new name would be a fiction.
        host.reload().await.expect("reload");
        assert_eq!(entry(&host.manifests(), "busy")["telemetry"]["calls"], 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn fresh_host_dir(tag: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("pumper-wasm-host-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("fixture dir");
        dir
    }
}
