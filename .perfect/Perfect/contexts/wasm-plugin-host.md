---
name: wasm-plugin-host
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 6
last_proposed: 2026-08-12
cooldown_until: r16
directions: ["[[wasm-ledger-honesty]]", "[[wasm-sandbox-admission]]", "[[wasm-fuel-telemetry]]"]
---

## Current state (r13 scout, RE-VERIFIED claim-by-claim 2026-08-12 r14 — verdicts below)
Host: engine-wasm/src/lib.rs — per-call Store (fuel 200M, mem 64MiB), InstancePre
cached in RwLock<HashMap>, global semaphore, spawn_blocking execution, extract_v2
envelope w/ legacy fallback. Consumers: trigger hooks (server/triggers.rs
apply_plugin_hooks :240-298), plugin app + observatory, GET /plugins + reload,
dynamic-app discovery.

r14 verification verdicts (engine-wasm untouched since Aug 4; claims checked live):
1. VERIFIED+SHARPER — apply_plugin_hooks takes no storage handle, structurally cannot
   write ledger rows; outcome allowlist (storage.rs:2830-2866, 15 outcomes) has NO
   trap/fuel/malformed outcome — unrepresentable, not just unwritten. WORSE: predicate
   error under on_error:skip records as predicate_veto (triggers.rs:920,:1050),
   indistinguishable from genuine pass=false. Transform failures ledger-blind in ALL
   configs.
2. VERIFIED — load_dir validates compile+imports only; fixture extract_only.wasm
   (lib.rs:467-471) proves has()=true for a module with no alloc/extract.
3. VERIFIED — test_trigger (routes/triggers.rs:340-458) never calls
   missing_hook_plugins; would_fire:true for uninstalled gate; on_error:skip branch
   returns a FABRICATED reason ("returned pass=false").
4. VERIFIED — permit in async frame, not captured by spawn_blocking closure
   (lib.rs:126-137); spawn_blocking uncancellable → cancelled caller frees slot while
   thread burns to fuel exhaustion; stores can exceed max_concurrent × max_memory.
5. VERIFIED — all errors Error::App(format!) ×17 sites; observatory.rs:57-65
   string-matches "trapped"/"panicked"; its tests assert their own literals so a
   reword reclassifies with zero test failures.
6. VERIFIED — fuel only ever set, never read back; extraction.md:180 names the gap;
   observatory substitutes avg_elapsed_ms (observatory.rs:15-17,:406-407).
7. VERIFIED — lib.rs:26-27 "extraction propagates the error" is false: plugin app
   swallows into {"error":…} and job SUCCEEDS (plugin/lib.rs:482-484,:716-718);
   typo'd plugin → ran:0 green job (plugin-runner's door — banked there).
8. VERIFIED — 5 raw RwLock unwraps (lib.rs:112,140,148,152,179); lock_advisory is
   pub(crate)-server + Mutex-only → structurally unreachable from engine-wasm;
   error.rs:183-184 carve-out says the map-replace lock QUALIFIES for recovery.
   describe probe hardcodes DESCRIBE_FUEL=10M (:188) + 16MiB (:298) off-config;
   .ok()? swallows describe failures silently on the load path, logged on discovery.
9. REFUTED as banked — e2e/trigger_plugins.rs (485 lines) drives the REAL host over
   wat fixtures: fuel-burn, trap, non-JSON, both slots, on_error:skip, plugin_missing
   rows. HONEST remaining gaps: reload() zero tests, ?kind filter, read_packed OOB,
   memory-cap breach, semaphore saturation, missing-export via has()/run(),
   permit-leak-under-cancel, poisoned-lock; both engine-wasm/tests/plugins.rs tests
   #[ignore]d on a stale manual title.wasm that plugins-install doesn't even build.
10. VERIFIED — data/plugins = busyloop.wasm + title.wasm only (manual artifacts);
    plugins-install installs trigger-gate + delta-slim only.
NEW: enabled=false ⇒ NoPlugins ⇒ plugin_missing row PER HOOK PER EVENT forever
(state.rs:289-293 + trigger-plugins.md:132-133 known gap — unbounded ledger growth);
restamp_provenance silently drops keys original lacked (:205-207, covered by e2e);
docs tabulate fail-open accurately but never state the ledger blindness.

## Direction history
- (round 6, via trigger-pipeline): activate-wasm-hooks shipped 8adfc91.
- r14 (2026-08-12, director-self-gated, SWEEP): slate of 5 → 3 ACCEPTED:
  [[wasm-ledger-honesty]] (robustness·M), [[wasm-sandbox-admission]] (robustness·M),
  [[wasm-fuel-telemetry]] (feature·M).
  REJECTED-deferred: plugin-app run-door validation (typo'd plugin ⇒ green ran:0 job)
  — it is plugin-runner's door; BANKED as that context's anchor (see
  contexts/plugin-runner.md). REJECTED outright: plugins-src SDK dedup (3 hand-rolled
  unsafe alloc/emit trios) — example-code duplication, no user moment, cosmetic churn
  per taste log.

## Shipped
- (via trigger-pipeline r6 — 8adfc91)
- r14 (2026-08-12): [[wasm-sandbox-admission]] → 068e11c (permit travels with the
  blocking work — cancelled callers can't over-admit stores; poisoned-lock
  recovery at all 5 sites; Error::Plugin + PluginFailure×6 kills observatory
  string-matching; probe budgets from config). [[wasm-ledger-honesty]] → 10fa27d
  (every hook failure class a distinct allowlisted ledger outcome; predicate_veto
  means only pass=false; has()/list() answer executability; dry-run names
  unusable plugins + incidents; plugin_missing once-per-deployment, re-armed on
  reload). [[wasm-fuel-telemetry]] → f2884e0 (fuel-used + memory high-water per
  call via run_metered; GET /plugins telemetry with budgets; observatory fuel
  cost signal; extraction.md:180 known gap closed). + Director 44f7e33 (dynamic
  discovery probes on the live budget; smoke 32→34).
  Remaining known honest gaps: trapped call's partial burn not carried;
  shipped-plugin #[ignore] e2e still need `just plugins-install`; busyloop.wasm
  still sits in data/plugins (less hazardous now — cancelled callers hold slots).
