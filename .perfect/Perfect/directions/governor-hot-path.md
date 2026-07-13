---
slug: governor-hot-path
type: perfect/direction
context: "[[Tiered Fetcher & Politeness]]"
lens: optimization
status: shipped
size: S
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 1deadf9
---

## What & why
Two global mutexes serialize slot math for ALL hosts — a global choke point under the crawler's thousand-task fan-out — and per-host maps grow unbounded. Separately, `html_to_markdown` runs on every tier purely to count chars even when markdown wasn't requested, then is discarded.

## Evidence
- Global mutexes on hot path: crates/core/src/governor.rs:67, 78-85 (penalties then next_slot, sequentially)
- Unbounded per-host maps: governor.rs:27-30
- Wasted markdown conversion: crates/core/src/fetcher.rs:121, 143, 202

## Acceptance criteria
- [ ] Per-host sharded locking (e.g. DashMap or sharded Mutex) — hosts no longer serialize each other; spacing semantics unchanged.
- [ ] Idle-host eviction (age- or size-bounded) for governor state.
- [ ] Markdown computed once per tier attempt and reused in the outcome; cheap length check when `to_markdown=false`.
- [ ] Concurrency test demonstrating parallel acquire for distinct hosts; existing governor tests still green.

## Risks / non-goals
- Non-goal: changing penalty/reward semantics (that's [[fetch-tier-verdicts]]).

## Build record
(pending)
