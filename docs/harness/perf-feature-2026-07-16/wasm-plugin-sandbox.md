# WASM Plugin Sandbox — perf-optimizer + feature-scout scan

> Total: 3
> Critical: 0 | High: 2 | Medium: 1 | Low: 0

## 1. Bound total concurrent plugin executions — the memory cap is per-store, not global

- **Severity**: High
- **Lens**: perf-optimizer
- **Category**: resource-pooling / hot-path
- **File**: crates/engine-wasm/src/lib.rs:57-73, 114-127
- **Scenario**: The `plugin` app (`crates/apps/plugin/src/lib.rs`) does `futures::future::join_all(tasks)` over **every** URL in the job's `urls` param, and each task calls `plugins.run(...)`. A 200-URL plugin job therefore issues 200 concurrent `Plugins::run` calls. `WasmPluginHost::run` unconditionally does `tokio::task::spawn_blocking(...)` per call, and `execute` builds a fresh `Store` with `StoreLimitsBuilder::memory_size(max_memory)`. There is no semaphore anywhere between the app and the store.
- **Root cause**: `StoreLimits` bounds **one** store. Nothing bounds *how many stores exist at once*. The `max_memory_mb` guarantee is per-instance, so the host's aggregate wasm memory ceiling is `max_memory_mb × concurrent_calls` — an unbounded product. Secondarily, each in-flight call occupies a tokio blocking-pool thread (default max 512); a fleet of slow/fuel-hungry plugins can saturate that pool and starve every other `spawn_blocking` user in the process.
- **Impact**: With the default `PluginConfig { fuel: 200_000_000, max_memory_mb: 64 }` (`crates/core/src/config.rs:436-455`), 100 concurrent runs admit a 6.4 GiB wasm-memory ceiling; the blocking pool caps the true worst case at 512 × 64 MiB ≈ 32 GiB. Trade-off worth stating honestly: wasmtime's on-demand allocator reserves address space lazily, so RSS only reaches the cap for plugins that actually grow memory — the *reliable* harm is blocking-pool saturation; the memory blow-up is the tail risk. A global budget makes the documented "hard memory cap" true for the process, not just per-call.
- **Fix sketch**: Add a `tokio::sync::Semaphore` field to `WasmPluginHost` sized from config (e.g. `max_concurrent`, defaulting to `available_parallelism()`), and `let _permit = self.sem.acquire().await` in `run` before `spawn_blocking`. This is also the natural place to adopt wasmtime's pooling allocator (`PoolingAllocationConfig` on `Config` in `WasmPluginHost::new`), which enforces a real global instance/memory ceiling and reuses pre-mapped slots. Metric to watch: peak RSS and blocking-pool queue depth during a ≥100-URL plugin job.

## 2. Move plugin compilation off the async runtime in `reload`

- **Severity**: Medium
- **Lens**: perf-optimizer
- **Category**: blocking-in-async
- **File**: crates/engine-wasm/src/lib.rs:81-87, 90-112
- **Scenario**: `POST /plugins/reload` (`crates/server/src/routes.rs:2443`) awaits `state.plugins.reload()`. That `async fn` calls `load_dir(&self.engine, &self.dir)` **directly in the future's body** — `std::fs::read_dir` plus a `Module::from_file` (full Cranelift compile) per `.wasm` file — with no `spawn_blocking`.
- **Root cause**: `reload` is `async` but its whole body is synchronous CPU + filesystem work. It parks a tokio *worker* thread (not a blocking thread) for the entire compile. Note the contrast with `run`, which correctly reasons about exactly this ("run it off the async runtime so a busy plugin never stalls a tokio worker") — `reload` just never got the same treatment. `WasmPluginHost::new` calling `load_dir` is fine: startup is not on the runtime.
- **Impact**: Cranelift compiles a small extractor module in roughly 10–100 ms; a plugins dir with 10–20 modules blocks one worker for ~0.2–2 s. On a small worker pool that is a visible latency spike across unrelated in-flight HTTP requests, and it scales linearly with plugin count — precisely the direction a hot-swappable plugin story grows. Bounded because reload is admin-triggered, not per-document.
- **Fix sketch**: Mirror `run`: `let (engine, dir) = (self.engine.clone(), self.dir.clone()); let modules = tokio::task::spawn_blocking(move || load_dir(&engine, &dir)).await.map_err(...)?;` then take the `RwLock` write guard and swap. Optionally enable `Config::parallel_compilation(true)` and a module cache in `WasmPluginHost::new` so repeated process starts and reloads skip re-compiling unchanged `.wasm` files.

## 3. Give plugins a params envelope and a self-describing manifest export

- **Severity**: High
- **Lens**: feature-scout
- **Category**: feature-gap
- **File**: crates/engine-wasm/src/lib.rs:8-13, 143-159; plugins-src/title-extractor/src/lib.rs:17-35
- **Scenario**: A user wants "title-extractor, but pull `<h2>` instead of `<h1>`", or wants one `css-plugin` reused across three jobs with different selectors. Today the ABI is `extract(ptr, len) -> u64` where the input bytes are *only the document* — `execute` writes `input.as_bytes()` and nothing else, and `Plugins::run(&self, name: &str, input: &str)` has no third parameter. The `plugin` app passes only `doc`. So every behavioural variation means editing Rust, re-running the wasm toolchain, and dropping a new file in the plugins dir.
- **Root cause**: The ABI was designed around the sandbox guarantees (fuel, memory, no imports) and treats the plugin as a pure `document -> JSON` function. Config-ability was never part of it. Meanwhile the declarative side (`RuleSet`, `docs/features/extraction.md`) is fully parameterized per job — plugins are the one extraction path that is not, which undercuts the "hot-swappable without recompiling the service" promise: you don't recompile the *service*, but you do recompile the *plugin*. Relatedly, `list()` returns bare file stems (`path.file_stem()` in `load_dir`), so `GET /plugins` can't tell a caller what a plugin does, what params it takes, or what shape it returns.
- **Impact**: Plugins stay one-off artifacts instead of reusable, shareable components — the difference between "write a wasm module per site" and "publish a `json-ld` or `microdata` plugin the whole fleet configures per job." It also blocks the plugin-marketplace direction in the vision backlog (T10 Platform & marketplace plays) and any UI that would render a param form.
- **Fix sketch**: Two additive, independently shippable steps.
  (a) **Params envelope**: extend the trait to `run(&self, name: &str, input: &str, params: &Value)` and have `execute` write a JSON envelope `{"doc": <input>, "params": <params>}` instead of raw bytes; the `plugin` app forwards `ctx.params.get("plugin_params")`. Version it by keeping the raw-doc path for plugins that don't export a marker, or bump to a new `extract_v2` export and fall back to `extract` when `get_typed_func::<(u32,u32),u64>(&mut store, "extract_v2")` misses — the same `get_typed_func` probe already used for `alloc`/`extract`.
  (b) **Manifest**: define an optional `describe() -> u64` export (same packed-pointer convention) returning `{name, version, description, params_schema, output_schema}`. `load_dir` calls it once at compile time under a small fuel budget and caches the JSON next to the `Module` (making `modules` a `HashMap<String, LoadedPlugin>`); `list()` and `GET /plugins` then return real metadata, and a missing/failed `describe` degrades to today's stem-only entry. Update `plugins-src/title-extractor` to export both as the reference implementation.
