---
slug: datahub-poll-mechanics
type: perfect/direction
context: "[[datahub-bridge]]"
lens: optimization
status: shipped
size: S
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 89a9b37
---

## What & why
The governance poll issues one SERIAL GraphQL round-trip per dataset on the 60-second
client — worst case ~20 minutes for a 20-dataset poll — while `last_poll` is stamped at
tick START, so a slow poll overlaps the next and two tasks race on `paused_apps` and
re-issue disables/enqueues.

## Evidence
- `datahub.rs:954-971` (serial per-dataset reads, 60s client), `:819-825` (stamp at tick
  time → overlap)

## Acceptance criteria
- Bounded-concurrency reads (small N) with a short per-request timeout for the poll path
  (separate from the 60s write client).
- Poll COMPLETION gates the next poll (in-flight guard) — overlap impossible by
  construction, with a test.
- Worst-case poll duration bounded and logged when exceeded.

## Risks / non-goals
- Coordinates with [[datahub-governance-reversible]]'s staleness policy (same builder).
- Non-goal: caching remote state across polls.

## Build record
- Builder DH1 (opus), wave 1 → master `89a9b37` (gate in flight at write).
  POLL_CONCURRENCY=4 buffer_unordered on a dedicated 10s poll client (write client stays
  60s); worst_case_poll_secs = ceil(n/4)×10 → 50s for 20 datasets vs ~20min; PollGuard
  releases in_flight AND stamps completion on DROP (panic-safe, no wedge, no immediate
  refire); pure `poll_due` gates. e2e: hanging-GMS test proves a second tick starts zero
  reads.
- Gates: worktree 1098/0.
