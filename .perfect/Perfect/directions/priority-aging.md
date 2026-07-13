---
slug: priority-aging
type: perfect/direction
context: "[[Job Worker & Cron Scheduler]]"
lens: optimization
status: shipped
size: S
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 49e133c
---

## What & why
Claim order is priority DESC, created_at with no aging — a steady high-priority stream starves low-priority jobs forever. Add priority aging in the claim SQL (effective priority rises with queue wait); document the per-app fairness knob. Defaults unchanged.

## Evidence
- Claim query: crates/core/src/storage.rs:182-193 (ORDER BY priority DESC, created_at)
- default_app_concurrency=0 (unlimited): config.rs:93

## Acceptance criteria
- [ ] Aging in claim query (e.g. priority + waited_secs/coefficient), coefficient config key (repo-law defaults; 0 disables = today's exact behavior).
- [ ] Starvation test: low-priority job eventually claims under a continuous high-priority stream.
- [ ] Equal-priority FIFO preserved; docs/runtime.md updated.

## Risks / non-goals
- Non-goal: changing default_app_concurrency default.

## Build record
(pending)
