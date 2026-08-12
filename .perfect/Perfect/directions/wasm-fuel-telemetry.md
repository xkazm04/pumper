---
slug: wasm-fuel-telemetry
type: perfect/direction
context: "[[wasm-plugin-host]]"
lens: feature
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
The sandbox meters nothing it enforces: fuel is set but never read back, memory
high-water is never observed, so the operator cannot see how close a plugin runs to
its caps, and the observatory substitutes elapsed_ms for cost by its own admission.
This is the feature doc's own named known gap (extraction.md:180). Surfacing per-call
fuel-used and memory high-water turns cap tuning and plugin-cost regressions from
guesswork into a read.

## Evidence
- Fuel only ever set: engine-wasm/src/lib.rs:264, :298; no get_fuel/fuel_consumed
  anywhere in the crate.
- Known-gap line: docs/features/extraction.md:180 "Plugin fuel/memory telemetry isn't
  surfaced per-run (backlog)."
- Observatory substitution admitted: apps/plugin/src/observatory.rs:15-17, computed
  :406-407, emitted :468.

## Acceptance criteria
1. Per-call fuel-used (budget − remaining) and memory high-water (linear memory size
   post-run — memory only grows) measured in execute() and surfaced through the
   Plugins trait (extended return type or a new method with a default impl — builder
   picks, says why; NoPlugins stays consistent).
2. Plugin app records/result carry the per-call cost; observatory uses fuel as the
   cost signal when available, elapsed_ms as labelled fallback — rows say which.
3. A minimal honest read surface: GET /plugins (or the observatory dataset) exposes
   last-run/cumulative telemetry. No new subsystem.
4. docs/features/extraction.md:180 known-gap line replaced with the real contract;
   trigger-plugins.md updated if the hook path surfaces anything.
5. Tests: a fuel-burning plugin reports fuel_used ≤ budget and > a trivial plugin's;
   memory growth reflected; legacy extract fallback path unchanged.

## Risks / non-goals
- Risk: Plugins trait signature change ripples to every impl/consumer (triggers.rs,
  plugin app, NoPlugins) — all inside this wave's write set; prefer the shape that
  keeps stub impls trivial.
- Non-goal: per-call telemetry for trigger hooks in the ledger rows beyond what
  [[wasm-ledger-honesty]] adds (coordinate — same lot, sequenced after it).
- Non-goal: cost accounting in job receipts (fuel is not money; CostClass unchanged).

## Build record
(pending)
