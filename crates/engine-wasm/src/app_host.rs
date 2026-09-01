//! The **component-model host** for dynamic apps (N09): the half of this crate
//! that can actually RUN a `.wasm` app, as opposed to the core-module plugin
//! sandbox in [`crate`] which is a pure document transformer with an empty
//! linker.
//!
//! What is different here, and why it needed a second host rather than a wider
//! ABI:
//!
//! * **The guest has imports.** They are exactly the metered [`AppContext`]
//!   seams declared in `wit/pumper-app.wit` — `fetch`, `upsert-many`,
//!   `checkpoint`, `restore`, `save-artifact`, `progress`, `log` — and nothing
//!   else. A guest that reaches for anything more fails to LINK at load, with a
//!   typed [`PluginFailure::MissingExport`]; it never half-runs.
//! * **The calls are async.** A fetch takes seconds and must not pin a thread,
//!   so this host runs on the async engine (`Config::async_support`) instead of
//!   `spawn_blocking`. That is also what makes a run *cancellable*: with a fuel
//!   yield interval the guest returns to the executor periodically, so dropping
//!   the future actually stops the work (the blocking-pool plugin path cannot —
//!   see `run_admitted`'s note).
//! * **The bounds are per JOB, not per call.** One run holds one instance —
//!   and its memory cap — across every host call it awaits, so admission is
//!   what bounds live wasm memory: `max_concurrent × max_memory_mb`. Fuel is a
//!   whole-job ceiling; the wall clock is enforced around the call AND
//!   re-checked at every import; the host-call count is capped because a guest
//!   that loops on `fetch` burns host time, not fuel.
//!
//! Everything crossing the boundary is JSON text (see the WIT file's own note):
//! the Rust types already have stable serde shapes, and re-declaring them as
//! WIT records would make every field addition a breaking ABI change.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::{Duration, Instant};

use pumper_core::app::AppContext;
use pumper_core::config::WasmAppsConfig;
use pumper_core::datasets::Provenance;
use pumper_core::error::PluginFailure;
use pumper_core::Error;

/// The crate's own result type, kept under a distinct name: the bindgen-
/// generated code says `Result<T>` meaning `wasmtime::Result`, so a bare
/// `pumper_core::Result` import in this module silently retypes the whole ABI.
use pumper_core::Result as CoreResult;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Config, Engine, ResourceLimiter, Store, StoreLimits, StoreLimitsBuilder};

wasmtime::component::bindgen!({
    path: "wit",
    world: "app",
    imports: { default: async | trappable },
    exports: { default: async },
});

use pumper::app::host::Host;

/// The WIT world this host links, reported in `GET /apps` so a deployed module
/// and the host that loaded it can be checked against the same string.
pub const WORLD: &str = "pumper:app/app@0.1.0";

/// Lowercase-hex SHA-256 of a module's bytes — the identity a catalog row pins
/// (`[[source]] module_sha256`) and the `rules_hash` every record this app
/// writes is stamped with.
pub fn module_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
        out.push(char::from_digit((b & 0x0f) as u32, 16).unwrap_or('0'));
    }
    out
}

/// Whether `bytes` is a **component**, as opposed to a core module.
///
/// Both start with the same `\0asm` magic; the next four bytes are where they
/// part — a core module declares version 1 / layer 0, a component version 13 /
/// layer 1. The distinction is load-bearing rather than cosmetic: the
/// describe-only core modules M28 discovered are still legal in the same
/// directory and must keep being listed (`runnable: false`), so the loader has
/// to tell the two apart before it decides which host owns the file.
pub fn is_component(bytes: &[u8]) -> bool {
    bytes.len() >= 8 && bytes[0..4] == *b"\0asm" && bytes[6..8] == [0x01, 0x00]
}

/// One dynamic app the host has compiled, linked and read a manifest from.
#[derive(Clone)]
struct LoadedApp {
    pre: Arc<AppPre<AppStore>>,
    manifest: Value,
    sha256: String,
}

/// A component in the app dir, as reported to the registry.
pub struct DiscoveredApp {
    /// File stem — the app's name, authoritative over anything the manifest says.
    pub name: String,
    /// The parsed `describe()` output (always a JSON object).
    pub manifest: Value,
    /// SHA-256 of the module's bytes, for the catalog pin and provenance.
    pub sha256: String,
}

/// Store data for one dynamic-app run.
///
/// `ctx` is an `Option` because the load-time `describe()` probe instantiates
/// the very same component with the very same linker, and there is no job to
/// give it. A probe that calls a host import therefore gets a typed refusal
/// instead of a fabricated context — an app cannot fetch its way through
/// discovery.
pub struct AppStore {
    ctx: Option<AppContext>,
    limits: StoreLimits,
    /// Wall-clock ceiling, re-checked at every import.
    deadline: Instant,
    /// Host calls made so far, against `max_host_calls`.
    calls: u64,
    max_host_calls: u64,
    max_payload_bytes: usize,
    /// SHA-256 of the running module: the `rules_hash` on every record it
    /// writes, so the era of an app build stays identifiable after a hot swap.
    module_sha256: String,
    /// What this run cost the host, for the job result.
    stats: RunStats,
}

/// Per-run counters returned alongside the job result.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunStats {
    pub host_calls: u64,
    pub fetches: u64,
    pub records_upserted: u64,
    pub fuel_used: Option<u64>,
    pub memory_bytes: Option<usize>,
}

impl RunStats {
    pub fn to_json(self, fuel_budget: u64, memory_cap: usize) -> Value {
        serde_json::json!({
            "host_calls": self.host_calls,
            "fetches": self.fetches,
            "records_upserted": self.records_upserted,
            "fuel_used": self.fuel_used,
            "fuel_budget": fuel_budget,
            "memory_bytes": self.memory_bytes,
            "memory_bytes_cap": memory_cap,
        })
    }
}

/// Why a host import refused before doing any work. Every arm is a TRAP (the
/// guest cannot catch it and keep going), because each one means a bound this
/// host exists to enforce has been reached.
fn trap(message: impl Into<String>) -> wasmtime::Error {
    wasmtime::Error::msg(message.into())
}

impl AppStore {
    /// The gate every host import passes through first.
    ///
    /// Extracted rather than repeated per import because "the deadline is
    /// checked at every import" is the whole cancellation story of this host,
    /// and a single import that forgot the check would be the hole. Returns the
    /// context, so an import cannot accidentally use it without being admitted.
    fn admit(&mut self, what: &str) -> wasmtime::Result<&AppContext> {
        self.calls += 1;
        self.stats.host_calls = self.calls;
        if self.calls > self.max_host_calls {
            return Err(trap(format!(
                "host-call ceiling reached at {what}: {} calls > [wasm_apps] max_host_calls = {}",
                self.calls, self.max_host_calls
            )));
        }
        if Instant::now() >= self.deadline {
            return Err(trap(format!(
                "job deadline passed at {what} ([wasm_apps] max_wall_secs)"
            )));
        }
        self.ctx.as_ref().ok_or_else(|| {
            trap(format!(
                "{what} is not available here: this instance is the load-time describe() probe, \
                 which has no job, no budget and no dataset to write to"
            ))
        })
    }

    /// Bounds one payload crossing the boundary. A cap that truncated would be
    /// worse than one that refuses: a half-read body extracted into records is
    /// a silent data defect, and this host's whole point is that the guest
    /// cannot produce those.
    fn check_payload(&self, what: &str, len: usize) -> wasmtime::Result<()> {
        if len > self.max_payload_bytes {
            return Err(trap(format!(
                "{what} payload is {len} bytes, over [wasm_apps] max_payload_bytes = {}",
                self.max_payload_bytes
            )));
        }
        Ok(())
    }
}

/// Parses `items-json` into the `(key, value)` batch `upsert_many` takes.
///
/// Accepts both spellings a guest naturally produces — `[[k, v], …]` and
/// `[{"key": k, "value": v}, …]` — and refuses everything else by NAME rather
/// than by silently dropping the malformed entries, because a batch that
/// upserts 9 of 10 records and reports success is the failure mode change
/// detection can never recover from.
pub fn parse_upsert_items(items_json: &str) -> std::result::Result<Vec<(String, Value)>, String> {
    let parsed: Value =
        serde_json::from_str(items_json).map_err(|e| format!("items must be a JSON array: {e}"))?;
    let Some(array) = parsed.as_array() else {
        return Err(format!(
            "items must be a JSON array, got {}",
            kind_of(&parsed)
        ));
    };
    let mut out = Vec::with_capacity(array.len());
    for (i, item) in array.iter().enumerate() {
        match item {
            Value::Array(pair) if pair.len() == 2 => {
                let key = pair[0]
                    .as_str()
                    .ok_or_else(|| format!("items[{i}][0] must be a string key"))?;
                out.push((key.to_string(), pair[1].clone()));
            }
            Value::Object(map) => {
                let key = map
                    .get("key")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("items[{i}] has no string \"key\""))?;
                let value = map
                    .get("value")
                    .ok_or_else(|| format!("items[{i}] has no \"value\""))?;
                out.push((key.to_string(), value.clone()));
            }
            other => {
                return Err(format!(
                    "items[{i}] is {}, expected [key, value] or {{key, value}}",
                    kind_of(other)
                ))
            }
        }
    }
    Ok(out)
}

fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

impl Host for AppStore {
    async fn fetch(
        &mut self,
        request_json: String,
    ) -> wasmtime::Result<std::result::Result<String, String>> {
        self.admit("fetch")?;
        self.check_payload("fetch request", request_json.len())?;
        let req = match serde_json::from_str(&request_json) {
            Ok(req) => req,
            // A malformed request is the GUEST's error, not a host fault: it is
            // reported on the `err` arm so the app can handle it, exactly like
            // a 404 would be.
            Err(e) => return Ok(Err(format!("fetch: request is not a FetchRequest: {e}"))),
        };
        let outcome = match self.ctx.as_ref().expect("admitted above").fetch(req).await {
            Ok(outcome) => outcome,
            Err(e) => return Ok(Err(e.to_string())),
        };
        self.stats.fetches += 1;
        let body = serde_json::to_string(&outcome)
            .map_err(|e| trap(format!("fetch outcome is unserializable: {e}")))?;
        self.check_payload("fetch outcome", body.len())?;
        Ok(Ok(body))
    }

    async fn upsert_many(
        &mut self,
        dataset: String,
        items_json: String,
    ) -> wasmtime::Result<std::result::Result<String, String>> {
        self.admit("upsert-many")?;
        self.check_payload("upsert batch", items_json.len())?;
        let items = match parse_upsert_items(&items_json) {
            Ok(items) => items,
            Err(e) => return Ok(Err(format!("upsert-many: {e}"))),
        };
        // Every record carries the module's own digest as `rules_hash`: after a
        // hot swap the rows produced by the previous build stay identifiable,
        // which is the one provenance fact the runtime always knows about a
        // dynamic app (its code IS the rules).
        let prov = Provenance {
            rules_hash: Some(self.module_sha256.clone()),
            ..Provenance::default()
        };
        let count = items.len() as u64;
        let summary = match self
            .ctx
            .as_ref()
            .expect("admitted above")
            .upsert_many_with_provenance(&dataset, &items, prov)
            .await
        {
            Ok(summary) => summary,
            Err(e) => return Ok(Err(e.to_string())),
        };
        self.stats.records_upserted += count;
        let body = serde_json::to_string(&summary)
            .map_err(|e| trap(format!("upsert summary is unserializable: {e}")))?;
        Ok(Ok(body))
    }

    async fn checkpoint(&mut self, state_json: String) -> wasmtime::Result<bool> {
        self.admit("checkpoint")?;
        self.check_payload("checkpoint state", state_json.len())?;
        let Ok(state) = serde_json::from_str(&state_json) else {
            // Checkpointing is advisory on both sides: a state the host cannot
            // parse is not persisted, and the guest is told so rather than
            // trapped mid-run.
            return Ok(false);
        };
        Ok(self
            .ctx
            .as_ref()
            .expect("admitted above")
            .checkpoint(state)
            .await)
    }

    async fn restore(&mut self) -> wasmtime::Result<Option<String>> {
        self.admit("restore")?;
        Ok(self
            .ctx
            .as_ref()
            .expect("admitted above")
            .restore()
            .map(|v| v.to_string()))
    }

    async fn save_artifact(
        &mut self,
        name: String,
        bytes: Vec<u8>,
    ) -> wasmtime::Result<std::result::Result<String, String>> {
        self.admit("save-artifact")?;
        self.check_payload("artifact", bytes.len())?;
        match self
            .ctx
            .as_ref()
            .expect("admitted above")
            .save_artifact(&name, &bytes)
            .await
        {
            Ok(path) => Ok(Ok(path.display().to_string())),
            Err(e) => Ok(Err(e.to_string())),
        }
    }

    async fn progress(&mut self, snapshot_json: String) -> wasmtime::Result<()> {
        self.admit("progress")?;
        if let Ok(snapshot) = serde_json::from_str::<Value>(&snapshot_json) {
            self.ctx
                .as_ref()
                .expect("admitted above")
                .progress
                .report(snapshot);
        }
        Ok(())
    }

    async fn log(&mut self, level: String, message: String) -> wasmtime::Result<()> {
        self.admit("log")?;
        let ctx = self.ctx.as_ref().expect("admitted above");
        let app = ctx.app.clone();
        let job = ctx.job_id;
        match level.as_str() {
            "error" => tracing::error!(app = %app, job = %job, "dynamic app: {message}"),
            "warn" => tracing::warn!(app = %app, job = %job, "dynamic app: {message}"),
            "debug" => tracing::debug!(app = %app, job = %job, "dynamic app: {message}"),
            _ => tracing::info!(app = %app, job = %job, "dynamic app: {message}"),
        }
        Ok(())
    }
}

/// The component-model host: compiles, links and runs the dynamic apps found in
/// `dir`.
pub struct WasmAppHost {
    engine: Engine,
    dir: PathBuf,
    cfg: WasmAppsConfig,
    /// Live-instance admission. Held for the WHOLE run — a dynamic app holds
    /// its instance (and its memory cap) across every host call it awaits, so
    /// bounding calls instead of runs would make `max_concurrent ×
    /// max_memory_mb` stop being a real ceiling.
    sem: Arc<Semaphore>,
    apps: RwLock<HashMap<String, LoadedApp>>,
}

impl WasmAppHost {
    /// Builds the host and loads every component in `dir`.
    ///
    /// Returns `Ok(None)` when `[wasm_apps] enabled = false` — the default —
    /// so a caller wires "no runnable dynamic apps" without a second code path
    /// and without compiling anything.
    pub fn new(dir: &Path, cfg: &WasmAppsConfig) -> CoreResult<Option<Self>> {
        if !cfg.enabled {
            return Ok(None);
        }
        let mut config = Config::new();
        config.consume_fuel(true);
        config.wasm_component_model(true);
        let engine =
            Engine::new(&config).map_err(|e| Error::App(format!("wasm app engine: {e}")))?;
        let host = Self {
            engine,
            dir: dir.to_path_buf(),
            cfg: cfg.clone(),
            sem: Arc::new(Semaphore::new(resolve_max_concurrent(cfg.max_concurrent))),
            apps: RwLock::new(HashMap::new()),
        };
        let loaded = host.load_dir();
        *host.apps.write().unwrap_or_else(|p| p.into_inner()) = loaded;
        Ok(Some(host))
    }

    fn read_apps(&self) -> RwLockReadGuard<'_, HashMap<String, LoadedApp>> {
        match self.apps.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Every loaded app, sorted by name.
    pub fn discovered(&self) -> Vec<DiscoveredApp> {
        let apps = self.read_apps();
        let mut out: Vec<DiscoveredApp> = apps
            .iter()
            .map(|(name, app)| DiscoveredApp {
                name: name.clone(),
                manifest: app.manifest.clone(),
                sha256: app.sha256.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// The digest of the module currently loaded under `name`.
    pub fn sha256_of(&self, name: &str) -> Option<String> {
        self.read_apps().get(name).map(|a| a.sha256.clone())
    }

    /// The budgets in force, for `GET /apps` and the run result.
    pub fn config(&self) -> &WasmAppsConfig {
        &self.cfg
    }

    /// Compiles + links + probes every component in the app dir.
    ///
    /// Runs the async `describe()` probe on a dedicated thread with its own
    /// current-thread runtime: loading happens from synchronous startup code,
    /// and blocking a tokio worker on a fiber would be the kind of quiet
    /// runtime hazard this crate's existing host comments keep warning about.
    /// The probe touches no host import (it cannot — see [`AppStore`]), so it
    /// needs nothing from the caller's runtime.
    fn load_dir(&self) -> HashMap<String, LoadedApp> {
        let engine = self.engine.clone();
        let dir = self.dir.clone();
        let cfg = self.cfg.clone();
        let handle = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().build() {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::warn!("dynamic-app loader: no runtime: {e}");
                    return HashMap::new();
                }
            };
            rt.block_on(load_dir_async(&engine, &dir, &cfg))
        });
        handle.unwrap_or_default_on_panic()
    }

    /// Runs one dynamic app to completion under every bound this host enforces.
    pub async fn run(&self, name: &str, ctx: AppContext) -> CoreResult<(Value, RunStats)> {
        let app = self.read_apps().get(name).cloned().ok_or_else(|| {
            Error::plugin(
                PluginFailure::Unknown,
                name,
                "no dynamic app of that name is loaded — install the component into \
                 [plugins] app_dir and restart",
            )
        })?;
        // Admission BEFORE the store exists, so waiting runs hold no memory.
        let _permit = self.sem.acquire().await.map_err(|e| {
            Error::plugin(
                PluginFailure::Host,
                name,
                format!("dynamic-app admission gate closed: {e}"),
            )
        })?;
        let wall = Duration::from_secs(self.cfg.max_wall_secs.max(1));
        let params = ctx.params.to_string();
        let mut store = self.store_for(name, Some(ctx), &app.sha256, wall)?;
        let instance = app.pre.instantiate_async(&mut store).await.map_err(|e| {
            Error::plugin(
                PluginFailure::Trap,
                name,
                format!("instantiation refused (memory/table limits): {e}"),
            )
        })?;
        let called = tokio::time::timeout(wall, instance.call_run(&mut store, &params)).await;
        let stats = self.measure(&mut store);
        match called {
            Err(_elapsed) => Err(Error::plugin(
                PluginFailure::Trap,
                name,
                format!(
                    "run exceeded [wasm_apps] max_wall_secs = {}",
                    self.cfg.max_wall_secs
                ),
            )),
            Ok(Err(trap)) => Err(Error::plugin(
                PluginFailure::Trap,
                name,
                format!("trapped (fuel/memory/deadline/panic): {trap}"),
            )),
            Ok(Ok(Err(message))) => Err(Error::plugin(PluginFailure::Host, name, message)),
            Ok(Ok(Ok(body))) => {
                let value = serde_json::from_str(&body).map_err(|e| {
                    Error::plugin(
                        PluginFailure::MalformedOutput,
                        name,
                        format!("run() returned invalid JSON: {e}"),
                    )
                })?;
                Ok((value, stats))
            }
        }
    }

    fn measure(&self, store: &mut Store<AppStore>) -> RunStats {
        let mut stats = store.data().stats;
        stats.fuel_used = store
            .get_fuel()
            .ok()
            .map(|left| self.cfg.fuel_per_job.saturating_sub(left));
        stats
    }

    fn store_for(
        &self,
        name: &str,
        ctx: Option<AppContext>,
        sha256: &str,
        wall: Duration,
    ) -> CoreResult<Store<AppStore>> {
        let data = AppStore {
            ctx,
            limits: StoreLimitsBuilder::new()
                .memory_size(self.cfg.max_memory_bytes())
                .memories(1)
                .tables(4)
                .table_elements(1_000_000)
                .instances(1)
                .build(),
            deadline: Instant::now() + wall,
            calls: 0,
            max_host_calls: self.cfg.max_host_calls,
            max_payload_bytes: self.cfg.max_payload_bytes,
            module_sha256: sha256.to_string(),
            stats: RunStats::default(),
        };
        let mut store = Store::new(&self.engine, data);
        store.limiter(|s: &mut AppStore| &mut s.limits as &mut dyn ResourceLimiter);
        store.set_fuel(self.cfg.fuel_per_job).map_err(|e| {
            Error::plugin(
                PluginFailure::Host,
                name,
                format!("could not set the fuel budget: {e}"),
            )
        })?;
        if let Some(interval) = self.cfg.yield_interval_or_none() {
            store
                .fuel_async_yield_interval(Some(interval))
                .map_err(|e| {
                    Error::plugin(
                        PluginFailure::Host,
                        name,
                        format!("could not set the fuel yield interval: {e}"),
                    )
                })?;
        }
        Ok(store)
    }
}

/// Small helper so a panicking loader thread degrades to "no dynamic apps"
/// instead of poisoning startup.
trait JoinOrDefault<T> {
    fn unwrap_or_default_on_panic(self) -> T;
}

impl<T: Default> JoinOrDefault<T> for std::thread::JoinHandle<T> {
    fn unwrap_or_default_on_panic(self) -> T {
        match self.join() {
            Ok(value) => value,
            Err(_) => {
                tracing::warn!("dynamic-app loader thread panicked — no dynamic apps loaded");
                T::default()
            }
        }
    }
}

/// Builds the linker for the `pumper:app` world: the imports the WIT declares,
/// and nothing else.
pub(crate) fn app_linker(engine: &Engine) -> CoreResult<Linker<AppStore>> {
    let mut linker: Linker<AppStore> = Linker::new(engine);
    pumper::app::host::add_to_linker::<AppStore, HasSelf<AppStore>>(&mut linker, |s| s).map_err(
        |e| {
            Error::plugin(
                PluginFailure::Host,
                "<host>",
                format!("could not build the pumper:app linker: {e}"),
            )
        },
    )?;
    Ok(linker)
}

/// Compiles and links one component, returning the typed link failure a module
/// that imports something undeclared produces.
///
/// This is the guard the world exists for: the import table IS the sandbox, so
/// a guest asking for a function the host never granted must fail HERE, at
/// load, with a class a caller can branch on — not at some later call, and
/// never by being granted a stub.
pub(crate) fn link_component(
    engine: &Engine,
    name: &str,
    bytes: &[u8],
) -> CoreResult<AppPre<AppStore>> {
    let component = Component::from_binary(engine, bytes).map_err(|e| {
        Error::plugin(
            PluginFailure::MalformedOutput,
            name,
            format!("not a valid component: {e}"),
        )
    })?;
    link(engine, name, &component)
}

/// [`link_component`] over an already-compiled component.
pub(crate) fn link(
    engine: &Engine,
    name: &str,
    component: &Component,
) -> CoreResult<AppPre<AppStore>> {
    let linker = app_linker(engine)?;
    let pre = linker.instantiate_pre(component).map_err(|e| {
        Error::plugin(
            PluginFailure::MissingExport,
            name,
            format!(
                "does not link against {WORLD}: it imports something this host grants nothing \
                 for: {e}"
            ),
        )
    })?;
    // The second half of the same contract: the world's EXPORTS must be there
    // too, and a component that links but exports no `run` is not an app.
    AppPre::new(pre).map_err(|e| {
        Error::plugin(
            PluginFailure::MissingExport,
            name,
            format!("does not export the {WORLD} world (describe/run): {e}"),
        )
    })
}

async fn load_dir_async(
    engine: &Engine,
    dir: &Path,
    cfg: &WasmAppsConfig,
) -> HashMap<String, LoadedApp> {
    let mut map = HashMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return map;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wasm") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|s| s.to_str()).map(String::from) else {
            continue;
        };
        let Ok(bytes) = std::fs::read(&path) else {
            tracing::warn!(path = %path.display(), "dynamic app unreadable");
            continue;
        };
        if !is_component(&bytes) {
            // A core module here is the M28 describe-only shape: still legal,
            // still listed by the older discovery path, just not runnable.
            tracing::debug!(
                path = %path.display(),
                "not a component — left to describe-only discovery"
            );
            continue;
        }
        let sha256 = module_sha256(&bytes);
        let pre = match link_component(engine, &name, &bytes) {
            Ok(pre) => pre,
            Err(e) => {
                tracing::warn!(path = %path.display(), "dynamic app failed to link: {e}");
                continue;
            }
        };
        match probe_manifest(engine, &pre, &name, &sha256, cfg).await {
            Ok(manifest @ Value::Object(_)) => {
                map.insert(
                    name,
                    LoadedApp {
                        pre: Arc::new(pre),
                        manifest,
                        sha256,
                    },
                );
            }
            Ok(other) => tracing::warn!(
                path = %path.display(),
                "dynamic app describe() returned {other}, not a JSON object manifest"
            ),
            Err(e) => tracing::warn!(path = %path.display(), "dynamic app describe() failed: {e}"),
        }
    }
    map
}

/// Reads a component's `describe()` manifest under the same budgets a real run
/// gets — a manifest read IS a call, and a probe with its own hidden ceiling is
/// how a legitimately expensive `describe()` ends up silently manifest-less.
async fn probe_manifest(
    engine: &Engine,
    pre: &AppPre<AppStore>,
    name: &str,
    sha256: &str,
    cfg: &WasmAppsConfig,
) -> CoreResult<Value> {
    let wall = Duration::from_secs(cfg.max_wall_secs.max(1));
    let data = AppStore {
        ctx: None,
        limits: StoreLimitsBuilder::new()
            .memory_size(cfg.max_memory_bytes())
            .memories(1)
            .tables(4)
            .table_elements(1_000_000)
            .instances(1)
            .build(),
        deadline: Instant::now() + wall,
        calls: 0,
        max_host_calls: cfg.max_host_calls,
        max_payload_bytes: cfg.max_payload_bytes,
        module_sha256: sha256.to_string(),
        stats: RunStats::default(),
    };
    let mut store = Store::new(engine, data);
    store.limiter(|s: &mut AppStore| &mut s.limits as &mut dyn ResourceLimiter);
    store.set_fuel(cfg.fuel_per_job).map_err(|e| {
        Error::plugin(
            PluginFailure::Host,
            name,
            format!("could not set the probe fuel budget: {e}"),
        )
    })?;
    let instance = pre.instantiate_async(&mut store).await.map_err(|e| {
        Error::plugin(
            PluginFailure::Trap,
            name,
            format!("probe instantiation refused: {e}"),
        )
    })?;
    let json = instance.call_describe(&mut store).await.map_err(|e| {
        Error::plugin(
            PluginFailure::Trap,
            name,
            format!("describe() trapped: {e}"),
        )
    })?;
    serde_json::from_str(&json).map_err(|e| {
        Error::plugin(
            PluginFailure::MalformedOutput,
            name,
            format!("describe() output is not JSON: {e}"),
        )
    })
}

/// `0` means "one per core" — the same rule the plugin host uses, so the two
/// admission gates are configured the same way.
fn resolve_max_concurrent(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> Engine {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.wasm_component_model(true);
        Engine::new(&config).expect("engine")
    }

    /// The distinction the loader is built on: the describe-only CORE modules
    /// M28 discovers must not be handed to the component host, and a component
    /// must not be left to the core-module path. Both start with the same magic
    /// bytes, so a naive "is it wasm" check reads them as the same thing.
    #[test]
    fn a_core_module_is_not_a_component() {
        let core = b"\0asm\x01\x00\x00\x00";
        let component = b"\0asm\x0d\x00\x01\x00";
        assert!(!is_component(core));
        assert!(is_component(component));
        assert!(!is_component(b"\0asm"), "a truncated header is neither");
        assert!(!is_component(b"not wasm at all"));
    }

    /// The gate this world exists for: a guest that imports a function the host
    /// never granted fails to LINK, with a class a caller can branch on —
    /// rather than linking and trapping later, or being handed a stub.
    #[tokio::test]
    async fn a_module_importing_an_undeclared_host_function_fails_to_link() {
        let engine = engine();
        // A component importing an interface that is not in the world at all.
        let wat = r#"
            (component
              (import "pumper:app/forbidden@0.1.0" (instance
                (export "exfiltrate" (func (param "s" string)))))
            )
        "#;
        let component = Component::new(&engine, wat).expect("component wat");
        let err = match link(&engine, "sneaky", &component) {
            Ok(_) => panic!("an undeclared import must not link"),
            Err(e) => e,
        };
        assert_eq!(
            err.plugin_failure(),
            Some(PluginFailure::MissingExport),
            "{err}"
        );
        assert!(err.to_string().contains("forbidden"), "{err}");
    }

    /// And the negative of that negative: the declared world links. Without
    /// this, a linker that granted NOTHING would pass the test above.
    #[tokio::test]
    async fn the_declared_world_links() {
        let engine = engine();
        let wat = r#"
            (component
              (import "pumper:app/host@0.1.0" (instance
                (export "log" (func (param "level" string) (param "message" string)))))
            )
        "#;
        let component = Component::new(&engine, wat).expect("component wat");
        // It links (the import resolves); it exports nothing, which is a
        // separate complaint, so accept either "linked" or an export-shaped
        // failure — never an unresolved-import one.
        if let Err(e) = link(&engine, "polite", &component) {
            assert!(
                !e.to_string().contains("unknown import") && !e.to_string().contains("not found"),
                "the declared host interface must resolve: {e}"
            );
        }
    }

    /// The batch parser accepts what a guest naturally writes, and refuses the
    /// rest BY NAME — a batch that silently dropped its malformed entries would
    /// report a clean upsert of fewer records, which change detection then
    /// reads as deletions at the source.
    #[test]
    fn a_malformed_item_is_named_not_dropped() {
        let pairs = parse_upsert_items(r#"[["a", {"v": 1}], ["b", 2]]"#).expect("pairs");
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, "a");
        let objects = parse_upsert_items(r#"[{"key": "a", "value": {"v": 1}}]"#).expect("objects");
        assert_eq!(objects[0].0, "a");

        let err = parse_upsert_items(r#"[["a", 1], "oops"]"#).expect_err("must refuse");
        assert!(err.contains("items[1]"), "{err}");
        let err = parse_upsert_items(r#"[{"value": 1}]"#).expect_err("must refuse");
        assert!(err.contains("key"), "{err}");
        let err = parse_upsert_items("{}").expect_err("must refuse");
        assert!(err.contains("array"), "{err}");
    }

    /// `[wasm_apps] enabled = false` is the default, and it must mean the host
    /// does not exist — not that it exists and refuses, which would still
    /// compile modules and hold memory at boot.
    #[test]
    fn the_host_does_not_exist_until_an_operator_enables_it() {
        let cfg = WasmAppsConfig::default();
        assert!(!cfg.enabled);
        let host = WasmAppHost::new(Path::new("."), &cfg).expect("no error");
        assert!(host.is_none());
    }

    /// A module's identity is its bytes: the catalog pins this digest and every
    /// record it writes is stamped with it.
    #[test]
    fn the_module_digest_is_its_bytes_not_its_name() {
        let a = module_sha256(b"\0asm\x0d\x00\x01\x00");
        let b = module_sha256(b"\0asm\x0d\x00\x01\x00x");
        assert_ne!(a, b);
        assert_eq!(a.len(), 64);
        assert!(pumper_core::catalog::is_sha256_hex(&a));
    }
}
