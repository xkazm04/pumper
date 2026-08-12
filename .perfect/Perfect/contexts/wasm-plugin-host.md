---
name: wasm-plugin-host
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 5
last_proposed: never
cooldown_until: —
directions: []
---

## Current state (scouted 2026-08-12, r13 — full brief in scout report, digest here)
Host: engine-wasm/src/lib.rs — per-call Store (fuel 200M, mem 64MiB, StoreLimits also
caps tables/instances), InstancePre cached in RwLock<HashMap>, global semaphore
(0→cores), spawn_blocking execution, extract_v2/{doc,params} envelope with legacy
extract fallback. Consumers: trigger hooks (server/triggers.rs:250,281 predicate/
transform; has() at :230), plugin app + observatory (apps/plugin), GET /plugins +
POST /plugins/reload (routes/runtime.rs:303-342), dynamic-app discovery (listing only,
honestly non-executable). NOT consumers: extractor app, crawl, search.

Direction-grade gaps (scout, file:line in r13 scout report):
1. **Fail-open is ledger-blind for every class except missing-module**: trap/fuel/
   malformed-output/missing-export → hop fires ungated, warn!-only, NO trigger-runs row
   (triggers.rs:260-272,284-290); only plugin_missing (has()=false) is recorded
   (:647-663). And a module missing `extract*` exports still answers has()=true
   (load_dir never checks exports, lib.rs:190-224) so it gets NO row either. Dry-run
   POST /triggers/{id}/test skips report_missing_plugins entirely (routes/triggers.rs:
   423-427) → would_fire:true for an uninstalled gate.
2. **Permit leak under caller cancellation** (lib.rs:126-136): _permit held by the
   async fn, work in spawn_blocking; worker cancel/timeout drops the future → permit
   released while the thread runs to fuel exhaustion → live stores can exceed
   max_concurrent × max_memory; cancelled work burns blocking threads.
3. **One error class for everything** (Error::App strings); observatory string-matches
   "trapped"/"panicked" (apps/plugin/observatory.rs:57-74) — reworded message silently
   reclassifies rows.
4. **No fuel/memory telemetry** — fuel remaining never read post-call; docs/features/
   extraction.md:180 names it a known gap; observatory substitutes elapsed_ms.
5. Doc lie: lib.rs:26-27 "extraction propagates the error" — FALSE; plugin app swallows
   into {"error":…} records and the job SUCCEEDS (apps/plugin/lib.rs:482-484,716-718).
   Also: typo'd plugin name → job succeeds ran:0 (plugin-runner context's door).
6. Lock .unwrap() on std RwLock ×5 (lib.rs:112-179) — poisoned lock = permanent host
   kill (r10 lock_advisory precedent). describe() probe hardcodes 16MiB/10M fuel
   (:188,:298) divorced from config.
7. Minor: input copied ~3× per call (~48MiB transient for a 16MiB doc, uncounted);
   compiled-module cache reload-only (documented); no ABI version handshake beyond the
   extract_v2→extract probe; plugins-src SDK is 3 hand-rolled duplicates of the same
   unsafe alloc/emit trio.
Untested: reload(), kind filter, read_packed OOB, memory-cap breach, semaphore
saturation, NoPlugins-behind-triggers, missing-export modules.
Working copy: data/plugins has only busyloop.wasm + title.wasm — trigger-gate/delta-slim
NOT installed (fail-open path live here).

## Direction history
- (round 6, via trigger-pipeline): activate-wasm-hooks shipped 8adfc91.

## Shipped
- (via trigger-pipeline r6 — 8adfc91)

## Banked (r13, 2026-08-12)
Slate-grade scout brief banked above — NOT gated this round (pool cap 6 went to
claude-engine + cron-scheduler). Front of the r14 queue with a ready slate:
W1 hook-failure ledger honesty (every fail-open class recorded, load-time export
validation so has() is honest, dry-run reports missing plugins) · W2 sandbox
admission integrity (permit held by the blocking task, poisoned-lock tolerance,
typed error classes killing observatory string-matching) · W3 fuel/memory telemetry
(documented known gap, extraction.md:180). Context remains UNCOVERED until gated.
