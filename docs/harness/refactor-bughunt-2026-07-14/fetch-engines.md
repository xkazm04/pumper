# Fetch Engines (HTTP / Browser / Claude) — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 1, Medium: 4, Low: 0)
> Files scanned: `crates/engine-http/src/lib.rs`, `crates/engine-browser/src/lib.rs`, `crates/engine-browser/tests/render.rs`, `crates/engine-claude/src/lib.rs` (confirmed against `crates/core/src/engine.rs` and the vendored `chromiumoxide 0.7.0` source)

## 1. Chrome launch runs while the global `holders` mutex is held — a cold start, crash-relaunch, or recycle stalls every render across every profile
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: race-condition / contention
- **File**: `crates/engine-browser/src/lib.rs:214-243` (launch at `:149-208`)
- **Scenario**: `acquire()` takes `self.holders.lock().await` (an async `tokio::Mutex`) and then, on a stale/missing holder, calls `self.launch(&user_data_dir).await?` **while still holding the lock** (lines 219-232). `ChromeBrowser::launch` blocks until it detects Chrome's DevTools WebSocket in stderr, bounded only by chromiumoxide's `launch_timeout` — **20 s by default**. Every other `render()` (any profile, including profiles whose Chrome is already live and healthy) is parked on `holders.lock()` for that whole window. A misconfigured `chrome_executable`, a container missing Chrome deps, or a slow cold start therefore freezes the entire render pool for up to 20 s per launch. Worse, a crash-looping Chrome makes `is_stale()` return true on *every* acquire, so each render relaunches under the lock — sustained global serialization plus repeated ≤20 s stalls, defeating the whole point of the per-profile holder map and the `max_concurrent_renders` semaphore.
- **Root cause**: the module doc claims the mutex "is held only briefly … plus a launch on a miss, never for a render's duration", but launching is not brief — it is the single most expensive, most failure-prone step, and it is serialized under the one lock shared by all profiles.
- **Impact**: resource-exhaustion / reliability — concurrency collapses to ~1 during any launch; a stuck or crash-looping Chrome wedges all renders for extended periods.
- **Fix sketch**: don't launch under the lock. Under the lock, decide staleness and reserve the key (e.g. insert a `tokio::sync::OnceCell`/`Shared`-future placeholder), release the lock, `await` the launch, then re-lock to store the result — so concurrent acquires for other profiles proceed and concurrent acquires for the *same* profile await one launch instead of racing. Also wrap `launch` in an explicit `tokio::time::timeout` shorter than 20 s.

## 2. Failed `page.content()` leaks the Chrome tab and the interception drainer task
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: resource-leak
- **File**: `crates/engine-browser/src/lib.rs:364-375` (drainer at `:272-310`)
- **Scenario**: after the drainer task is spawned, the render reaches `let html = page.content().await.map_err(...)?;` (lines 364-367). If `content()` errors — Chrome crashed or the renderer died during settle/evaluate, or a CDP protocol error — the `?` returns immediately, **skipping both `drainer.abort()` and `page.close().await`** (lines 370-375, which only run on the success path). The `about:blank`-plus-drainer tab is never closed, and the drainer task keeps looping on `paused.next()` forever, holding its own `drain_page = page.clone()` — which keeps the CDP target alive. On the shared, long-lived instance (recycled only after 200 renders) each such failure accumulates a leaked tab (real Chrome memory) plus a spinning tokio task, so a run of flaky/renderer-crashing pages can balloon the shared Chrome's memory long before the recycle threshold reaps it. The same skip-cleanup applies to the earlier `event_listener::<EventRequestPaused>()?` (line 277-280), which leaks the freshly opened `about:blank` page.
- **Root cause**: cleanup (`drainer.abort()` / `page.close()`) is written as straight-line code on the happy path instead of being tied to the page/drainer's scope, so any `?` between setup and cleanup bypasses it.
- **Impact**: resource-exhaustion — leaked Chrome tabs + tokio tasks on the shared instance under a realistic error path.
- **Fix sketch**: put the page and drainer behind RAII guards (a struct whose `Drop` calls `abort()` and spawns/blocks a `close()`), or wrap the body so every exit path funnels through the abort+close (e.g. an inner `async` block whose `Result` is handled after unconditional cleanup).

## 3. Real Claude spend on a timed-out or failed run never reaches metering
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure / cost-accounting
- **File**: `crates/engine-claude/src/lib.rs:139-160` (cost read only at `:176-183`)
- **Scenario**: `total_cost_usd` is extracted only on the fully-successful path (line 179), inside the returned `ResearchOutput`. Every failure path discards it: a `timeout` returns `Err(Error::Claude("timed out …"))` (lines 139-141) after the CLI may have run for minutes and spent real money; a non-zero exit returns `Err` with only truncated stderr (lines 146-153); an `is_error: true` envelope returns `Err` (line 158-160) even though that same envelope carries `total_cost_usd`. Since engines are unmetered by design and metering happens at `AppContext::research`, and the meter can only read cost from a successful `ResearchOutput`, **any run that burned to its `--max-budget-usd` and then errored, or timed out, is invisible to spend accounting** — exactly the runs most likely to be expensive.
- **Root cause**: cost is modeled as a field of the success value only; `Error::Claude` is a bare string with no structured cost, so there is no channel to report partial/failed spend upward.
- **Impact**: wrong result — under-counted spend, budget overruns that never show up in metering.
- **Fix sketch**: on the `is_error` and non-success paths, parse the envelope (when present) and surface `total_cost_usd` — either via a cost-carrying error variant or by logging/emitting the cost before returning `Err` — so the meter can charge failed/aborted runs too.

## 4. Bounded-LRU-with-eviction is reimplemented in both engine-http and engine-browser
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/engine-http/src/lib.rs:193-224` vs `crates/engine-browser/src/lib.rs:86-108`
- **Scenario**: `engine-http`'s `ClientPool` (`HashMap<String, _>` + `VecDeque<String>` order, with `order.retain(|k| k != key); order.push_back(key); while len > cap { pop_front() }`) and `engine-browser`'s `Holders` + `touch_lru` implement the identical "touch-as-MRU, evict-LRU-past-cap" algorithm over the same `HashMap`+`VecDeque` representation. Both carry their own near-duplicate unit tests (`client_pool_is_lru_bounded`, `holders_are_lru_bounded_by_max_live_profiles`). The only real difference is what the map values are (a `reqwest::Client` vs a `LiveBrowser`) and that the browser variant returns the evicted keys so the caller can run drop-side effects.
- **Root cause**: no shared bounded-cache primitive in `core`; each engine grew its own copy of the same LRU bookkeeping.
- **Impact**: wasted maintenance — eviction/ordering fixes must be made and tested twice, and the two copies can drift.
- **Fix sketch**: extract a small generic `BoundedLru<V>` (or `LruMap<K, V>`) into `pumper_core` that returns evicted entries on insert, and have both `ClientPool` and `Holders` delegate to it; keep the engine-specific drop/reap semantics at the call sites.

## 5. A transient `ProfileJar::save()` failure permanently drops the pending cookie write
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/engine-http/src/lib.rs:136-151` (save at `:105-121`)
- **Scenario**: `flush_loop` does `if self.dirty.swap(false, …) { if let Err(e) = self.save() { warn!(…) } continue; }`. The `dirty` flag is cleared **before** `save()` runs, so if `save()` fails (disk full, permission error, an interrupted `write`/`rename` on the tmp file) the flag is already `false`. The `continue` loops back to `sleep`, but on the next pass `dirty.swap(false)` sees `false` and the loop retires — the failed write is never retried. The Set-Cookie was applied in-process (so the live session keeps working) but is silently absent from `cookies.json`, so it is lost on the next restart of the profile — the exact durability guarantee the persistent vault exists to provide, defeated by a one-off I/O hiccup, with nothing but a `warn!` to show for it.
- **Root cause**: `dirty` is consumed optimistically (cleared up-front) rather than being cleared only after a confirmed successful save; failure has no re-arm path.
- **Impact**: wrong result / data-loss — a persisted login cookie can be silently lost across restart after a transient disk error.
- **Fix sketch**: on `save()` error, re-set `dirty` (`self.dirty.store(true, …)`) before `continue` so the next debounce cycle retries the write (optionally with a bounded backoff), leaving the flag clear only after a successful persist.
