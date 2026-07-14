# WASM Plugin Sandbox — refactor + bug-hunt findings

> Total: 5 findings (Critical: 1, High: 1, Medium: 1, Low: 2)
> Files scanned: `crates/engine-wasm/src/lib.rs` (host), `plugins-src/busyloop/src/lib.rs`, `plugins-src/title-extractor/src/lib.rs`. Confirmed against `crates/core/src/plugin.rs`, `crates/core/src/config.rs` (defaults), `crates/apps/plugin/src/lib.rs` (caller), `crates/server/src/worker.rs` (job timeout), `Cargo.lock` (wasmtime 46.0.1).

## 1. Guest-controlled output length drives an unbounded host-side allocation
- **Severity**: Critical
- **Lens**: bug-hunter
- **Category**: resource-exhaustion / trust-boundary
- **File**: `crates/engine-wasm/src/lib.rs:147-156`
- **Scenario**: `extract` returns a packed `u64`; the host splits it into `out_ptr = packed >> 32` and `out_len = packed & 0xffff_ffff` and then, on line 153, does `let mut out = vec![0u8; out_len];` **before** the bounds-checked `memory.read` on line 155. `out_len` is fully attacker-controlled — a malicious/buggy guest returns any value up to `0xffff_ffff` (4.29 GB) regardless of its actual 64 MB linear memory. The `StoreLimits` `memory_size` cap governs only the *guest's* linear memory; it does nothing for this *host-heap* `Vec`. The host therefore attempts a ~4.29 GB zeroed allocation. If it fails, Rust's `handle_alloc_error` **aborts the whole process** — and because this runs inside `spawn_blocking`, the abort is *not* catchable by the `JoinError` handler at line 72 (that only catches unwinding panics). If it succeeds, the host spikes 4.29 GB and stalls zeroing it. Either way one crafted plugin return takes down or freezes the entire server, killing all in-flight jobs.
- **Root cause**: The ABI trusts a guest-supplied length as the size of a host allocation, and the sandbox's memory cap is (correctly) scoped to guest linear memory but was assumed to also protect host-side buffers.
- **Impact**: crash / whole-process DoS (untrusted plugin escapes the "the sandbox holds" guarantee the comment on line 146 claims).
- **Fix sketch**: Before allocating, validate against actual guest memory: `let size = memory.data_size(&store); if out_ptr as u64 + out_len as u64 > size as u64 { return Err(...) }` and additionally clamp `out_len` to a sane maximum (e.g. `max_memory`). Only then allocate `out`.

## 2. Store limiter caps linear memory but not tables/instances — memory cap is bypassable at instantiation
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: resource-exhaustion / trust-boundary
- **File**: `crates/engine-wasm/src/lib.rs:115` (and `:122-125`)
- **Scenario**: `StoreLimitsBuilder::new().memory_size(max_memory).build()` sets **only** the linear-memory size limit. `table_elements`, `tables`, `instances`, and `memories` are left at their unlimited (`None`) defaults. Wasmtime consults the `ResourceLimiter` for the *initial* table size at instantiation, and `StoreLimits::table_growing` returns `Ok(true)` when no `table_elements` limit is set. A malicious `.wasm` dropped into the plugin dir (the explicit threat model — "untrusted, hot-swappable code") that declares e.g. `(table 500000000 funcref)` forces wasmtime to eagerly allocate a multi-GB table backing `Vec` during `linker.instantiate` (line 123), bounded only by host RAM — completely bypassing the advertised 64 MB `max_memory` cap and OOMing/aborting the host before `extract` ever runs.
- **Root cause**: The "hard memory cap" was equated with `memory_size`, but `StoreLimits` governs several independent resource classes; only one was configured.
- **Impact**: host OOM / DoS via instantiation, defeating the documented memory guarantee.
- **Fix sketch**: Set the full envelope on the builder: `.table_elements(N).tables(M).instances(1).memories(1)` alongside `.memory_size(max_memory)`, sized from config, so tables and instance counts are also bounded (and instantiation of an oversized module fails cleanly instead of allocating).

## 3. No wall-clock/epoch deadline; `spawn_blocking` is non-cancellable — cancelled jobs keep burning the full fuel budget
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: resource-exhaustion
- **File**: `crates/engine-wasm/src/lib.rs:68-73` (fuel default `200_000_000` in `crates/core/src/config.rs:445`; caller fan-out `crates/apps/plugin/src/lib.rs:69`; job timeout `crates/server/src/worker.rs:181-184`)
- **Scenario**: Fuel is the *only* CPU bound — no `epoch_interruption` and no wall-clock deadline are configured. `run` offloads execution via `tokio::task::spawn_blocking`, whose closure **cannot be cancelled**: even after the enclosing job hits its `job_timeout_secs` `select!` (worker.rs) and the caller drops the future, the blocking thread keeps executing until the full 200M-fuel budget drains. The plugin app runs `futures::future::join_all` over every URL in the caller-supplied `urls` array (plugin app line 69), so a request with N URLs against the `busyloop` plugin launches N concurrent runaway `spawn_blocking` tasks, each pinning a thread from the bounded blocking pool (default 512) for the entire fuel-drain duration regardless of job timeout. Enough concurrent runaways starve the blocking pool and stall unrelated blocking work. This contradicts the module doc's "a runaway plugin traps instead of hanging the host" (line 4) and the trait doc's "a runaway plugin traps rather than hanging the host."
- **Root cause**: Fuel bounds *instructions*, not *wall-clock*, and offloading to a non-cancellable blocking task means request/job cancellation cannot reclaim the thread; there is no independent time ceiling.
- **Impact**: blocking-pool starvation / latency DoS; timed-out jobs still consume resources to completion.
- **Fix sketch**: Add `config.epoch_interruption(true)`, set an epoch deadline (`store.set_epoch_deadline(1)`) and a background ticker (`engine.increment_epoch()`), so execution traps on a real wall-clock bound in addition to fuel; alternatively bound total in-flight plugin executions with a semaphore.

## 4. Input length truncated via `as u32`; write uses full slice while alloc/extract see the truncated length
- **Severity**: Low
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/engine-wasm/src/lib.rs:137-144`
- **Scenario**: `let len = bytes.len() as u32;` silently truncates for any input ≥ 4 GiB. The host then calls `alloc(len)` (guest reserves the *truncated* size) and passes the truncated `len` to `extract`, but `memory.write(&mut store, in_ptr, bytes)` writes the *full* `bytes` slice. The guest is told the input is shorter than what was written, so it under-reads the document; and because the write length exceeds what the guest reserved, it either lands in the guest's own heap beyond the reservation or (more likely, given the 64 MB cap) fails the wasmtime bounds check and errors. Realistic scraped documents are far below 4 GiB, so this is latent rather than routinely hit.
- **Root cause**: An unchecked `usize -> u32` narrowing cast in the ABI packing, with the subsequent write sized off the original `usize`.
- **Impact**: silent data loss / spurious "write input" errors on pathologically large inputs; no explicit guard.
- **Fix sketch**: Reject early when `bytes.len() > u32::MAX as usize` with a clear error, or cap input size to `max_memory` before `alloc`.

## 5. Byte-identical `alloc` duplicated across the two example plugin crates
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `plugins-src/busyloop/src/lib.rs:5-11` and `plugins-src/title-extractor/src/lib.rs:9-15`
- **Scenario**: Both example guests define an identical `alloc(len) -> u32` (`Vec::with_capacity` + `as_mut_ptr` + `std::mem::forget`). As more example/real plugins are added the ABI boilerplate is re-copied verbatim, and a future ABI change (e.g. the `len == 0` dangling-pointer edge) must be fixed in each copy.
- **Root cause**: No shared guest-side ABI helper crate; each plugin is a fully standalone template. (Partly intentional — these are copy-paste starter fixtures — so this is low-value cleanup, not a defect.)
- **Impact**: minor duplicated maintenance across guest crates.
- **Fix sketch**: Extract the `alloc`/packing boilerplate into a tiny `plugin-abi` helper crate (or a shared macro) that guests depend on, leaving each plugin to implement only `extract`.
