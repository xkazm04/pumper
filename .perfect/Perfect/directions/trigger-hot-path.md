---
slug: trigger-hot-path
type: perfect/direction
context: "[[trigger-pipeline]]"
lens: optimization
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: b30bd47
---

## What & why
Every job completion pays two unconditional trigger queries even when zero triggers exist;
every inbound webhook re-parses every candidate trigger's filter specs from strings (specs
are immutable after create); every hook invocation builds a fresh wasmtime Store + instance
via `instantiate` on `spawn_blocking`, behind a global CPU-count semaphore shared with
extraction plugins — two hooks = two full instantiations per trigger per event, serialized.
Cache the eval set (invalidated on trigger CRUD), cache parsed filters, and use wasmtime
`InstancePre` pre-instantiation so per-hook cost drops to near-zero while keeping per-call
store isolation.

## Evidence
- `triggers.rs:374/:430` — `enabled_triggers` twice per job completion, unconditional
- `triggers.rs:492` — `parse_filters` per candidate trigger per inbound event
- `engine-wasm/src/lib.rs:344` — `instantiate` per invocation; `:118-123` global semaphore
- `storage.rs:175-220` — enqueue_dedup 2-3 queries per hop (secondary)

## Acceptance criteria
- Zero-trigger job completion performs no trigger DB queries (cached existence/eval set,
  correctly invalidated on trigger create/update/delete/enable).
- Filter specs parsed once per trigger lifetime, not per event.
- WASM hook path uses `InstancePre` (module linked once; per-call Store retained for
  isolation); measured before/after per-invocation cost reported.
- Existing hook + trigger test suites pass unchanged (no semantics change).
- Cache coherence test: CRUD on a trigger is visible to the very next firing decision.

## Risks / non-goals
- Cache invalidation across the multi-writer surface (routes + storage) — the coherence test
  is the guard. Concurrency-correctness → this brief escalates the builder tier.
- Non-goal: splitting the WASM semaphore from extraction (note it if it dominates).

## Build record
- Builder T2 (opus), wave 2 → master `b30bd47` (final gate in flight at write time).
  `Storage::trigger_generation` (Arc<AtomicU64>, shared across Storage clones — per-clone
  counter would be the exact lost-invalidation hole), bumped after-commit by all three
  mutation paths; invalidation at STORAGE layer because six e2e sites create triggers via
  Storage directly (route-level invalidation would have been a live bug).
  `TriggerEvalCache` keyed (kind, app), stamps generation sampled BEFORE the SELECT; empty
  sets cached (the zero-trigger win); filters parsed once per set; put refuses older-gen
  overwrite. Coherence e2e: CRUD visible to the NEXT firing decision.
- **Honest negative result**: InstancePre showed NO per-call win (relink 77µs vs InstancePre
  72-116µs; store-only 1.4-2.5µs — fresh-Store creation dominates, and per-call isolation
  mandates it; these plugins declare no imports so hoisted linking is tiny). Kept for the
  structural win (import failures surface once at load) with the benchmark as a REGRESSION
  gate, not a speedup claim. Semaphore left shared per non-goal.
- Refuted: fire_dataset_triggers was NOT unconditional (gated by changes.is_empty());
  there is no trigger update path (mutation surface = create/delete/enable) — which is WHY
  lifetime filter caching is sound.
- Gates: worktree full workspace 1072/0.
