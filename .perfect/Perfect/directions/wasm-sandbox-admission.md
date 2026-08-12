---
slug: wasm-sandbox-admission
type: perfect/direction
context: "[[wasm-plugin-host]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
The sandbox's admission bound is a lie under cancellation: the semaphore permit lives
in the async fn frame while the work runs on an uncancellable spawn_blocking thread,
so a worker timeout frees the slot while the orphaned thread burns to fuel exhaustion
— live stores can exceed max_concurrent × max_memory, and a busyloop plugin burns a
blocking thread per cancelled call (one such .wasm sits in data/plugins today). One
poisoned RwLock permanently kills every plugin call, listing, and reload. And every
host error is a formatted Error::App string that the observatory classifies by
substring — rewording a message silently reclassifies rows with zero test failures.

## Evidence
- Permit not captured: engine-wasm/src/lib.rs:126-137 (OwnedSemaphorePermit local;
  move closure captures only engine/pre/input/params/fuel/max_memory); comment at
  :121-125 claims "held for the whole execution" — true only uncancelled.
- 5 raw RwLock unwraps: lib.rs:112, :140, :148, :152, :179. lock_advisory is
  pub(crate)-server + Mutex-only (routes/error.rs:185) — unreachable here; the
  carve-out at error.rs:183-184 says a whole-map-replace lock QUALIFIES for recovery.
- One error class: Error::App(format!) ×17 in lib.rs; core Error (error.rs:93-150)
  has no plugin variant; observatory.rs:57-65 string-matches "trapped"/"panicked";
  its tests (:503-520) assert their own literals.
- Hardcoded probe budgets: DESCRIBE_FUEL=10M (lib.rs:188), 16MiB inline (:298),
  divorced from cfg.fuel/max_memory; describe failures swallowed by .ok()? on the
  load path (:298-305) but logged on discovery (:369-372) — inconsistent.

## Acceptance criteria
1. The permit travels with the blocking work (captured by the spawn_blocking
   closure): caller cancellation cannot admit a new store while an orphaned thread
   still runs. Test proves the admission bound under a cancelled caller (semaphore-
   saturation-shaped test — currently zero coverage).
2. A poisoned lock degrades, never kills: advisory recovery at all 5 sites (helper
   local to engine-wasm or hoisted to core — builder picks and says why; the
   error.rs:183-184 carve-out reasoning applies).
3. Typed plugin errors: a core error variant (or typed kind carried structurally)
   distinguishing at minimum trap/fuel-or-memory, missing-export, unknown-plugin,
   malformed-output. engine-wasm produces it; routes/error.rs client_facing mapping
   extended (inventory test updated deliberately).
4. Observatory classifies via the typed kind — rewording a message can no longer
   reclassify rows; its tests assert against the type, not literals.
5. describe/discovery probe budgets derive from config; describe-failure logging
   consistent between load and discovery paths.

## Risks / non-goals
- Risk: Error variant addition ripples into exhaustive matches — follow the r10
  error-contract conventions (client_facing table is inventory-enforced).
- Non-goal: cancelling the blocking thread itself (wasmtime epoch interruption is a
  bigger design; permit correctness is the contract here). If the builder finds epoch
  deadlines cheap and safe, propose in report — do not build unbriefed.
- Non-goal: fuel/memory telemetry (that is [[wasm-fuel-telemetry]]).

## Build record
(pending)
