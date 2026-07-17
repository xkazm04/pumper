# Perf-Feature Scan — Wave 4: Concurrency & resource bounds (Theme D)

> 4 commits, **4 High findings** closed — the remaining OOM / fd-exhaustion /
> thundering-herd surface. All resource-safety, tightly scoped.
> Baseline preserved: build clean, tests **230 → 237** (+7 new tests, 0 regressions).
> Branch `vibeman/wave4-bounds-2026-07-17` (off master after PR #7).

## Commits

| # | Commit | Finding | What |
|---|--------|---------|------|
| 1 | `94ebd8c` | extraction-crawl #1 | bound the fetch fan-out in extractor + plugin urls-mode (`concurrency` param, default 16) |
| 2 | `79fab06` | wasm #1 | global semaphore caps concurrent plugin executions (`[plugins] max_concurrent`) |
| 3 | `1f9eab5` | live-events #1 | Arc-share `JobEvent` + byte-budget the replay ring (32 MiB) |
| 4 | `fd71b78` | fetch-engines #1 | collapse concurrent stale acquires onto one Chrome launch per profile |

## What was fixed

1. **Unbounded URL-list fan-out (extraction-crawl #1).** Both `extractor` and
   `plugin` drove every URL with a single `join_all`, so a 5000-URL / 800-host list
   opened ~5000 sockets + TLS handshakes at once — enough to exhaust file descriptors
   and hold every response body resident, degrading every other job on the box. The
   per-host governor serializes same-host requests but caps nothing globally; the
   sibling `crawl` app already exposes this knob. Added a `concurrency` param
   (default 16, clamp `>= 1`) and replaced `join_all` with a bounded stream —
   `buffer_unordered` in the extractor (results keyed by URL), `buffered`
   (order-preserving) in the plugin app (results are positionally zipped back to
   keys). Pure `parse_concurrency` unit-tested.

2. **Per-store memory cap ≠ global cap (wasm #1).** `StoreLimits.memory_size`
   bounds ONE store, but nothing bounded how many stores exist at once: a 200-URL
   plugin job issued 200 concurrent `run` calls, each building its own `Store` and
   holding a blocking-pool thread. Aggregate wasm memory was
   `max_memory_mb × concurrent_calls` (unbounded), and slow plugins could saturate
   tokio's blocking pool and starve every other `spawn_blocking` user. Added a
   `[plugins] max_concurrent` knob (`0` → one per CPU core) backing a
   `tokio::sync::Semaphore`; `run` acquires an owned permit **before**
   `spawn_blocking`, so excess callers wait at the gate rather than piling onto the
   pool. `resolve_max_concurrent` unit-tested.

3. **Deep-cloned multi-MB events pinning ~1 GB RSS (live-events #1).**
   `SeqEvent = (u64, JobEvent)` was by-value, so a multi-MB `result` was deep-cloned
   three ways per emit (ring + broadcast slot + once per receiver at `recv()`) — ~72
   copies at 70 subscribers, all under the ring `Mutex`. Changed to
   `(u64, Arc<JobEvent>)`: ring, broadcast, and every subscriber share one
   allocation; `recv()` becomes a refcount bump (consumers already take
   `&JobEvent`, covered by deref coercion). Separately, the 1024-event count cap let
   large-result bursts pin ~1 GB for the process lifetime, so the ring now also
   carries a **32 MiB byte budget** (running total, approx = serialized `result`
   length computed once at emit) and evicts past it, always keeping `>= 1` event.
   Two eviction tests added.

4. **Thundering-herd Chrome launches (fetch-engines #1).** The 2026-07-14 fix moved
   `launch()` off the holders lock but left the check-then-launch window open: when a
   holder is stale (cold start, crash, recycle boundary) every concurrent caller
   launched its **own** Chrome against the **same** `--user-data-dir` — up to 4
   full launches (~20s + ~2 GB transient V8 each) to keep one, racing Chromium's
   single-instance lock (a correctness hazard, not just waste). Added a per-profile
   **launch gate** (`Arc<Mutex<()>>` map): a stale/missing key takes the gate,
   re-checks freshness (a racer may have just launched), and only then launches — so
   the 2nd..Nth caller awaits the one launch. Other profiles keep their own gate.
   Also: drop the stale holder **before** relaunch so the outgoing Chrome frees the
   user-data-dir lock first (in-flight renders keep their `Arc` handle); wrap launch
   in a 15s timeout under chromiumoxide's ~20s ceiling; prune map-only gates so the
   map stays bounded. `is_stale` unchanged; `gate_for` unit-tested.

## New config knobs

- `[plugins] max_concurrent` (default `0` = one per CPU core) — global plugin
  execution cap.
- `extractor` / `plugin` apps: `concurrency` param (default 16) — max in-flight
  fetch (+run) tasks.

## Gate

```
cargo build --workspace   # clean
cargo test --workspace    # 237 passed / 0 failed  (was 230)
```

## Open Highs after this wave

19 of the original 36 remain (themes F caching, G query surface, H introspection,
I domain model, J extraction power, plus E grants-gov #1 deferred). Theme D
(concurrency & bounds) is now fully closed.
