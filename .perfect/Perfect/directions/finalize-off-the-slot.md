---
slug: finalize-off-the-slot
type: perfect/direction
context: "[[job-worker]]"
lens: optimization
status: shipped
size: M
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: a372209
---
## What & why
The entire finalize fan-out runs inline in the job task while it still holds a semaphore permit
(default concurrency 4): search index plus a forced `search.flush()`, watch dispatch, dataset
triggers, saved-search alerts and materialization. A slow index or a large materialization burns a
scrape slot. Separately, nothing reports where a job's wall-clock went, so a user seeing a
three-minute job cannot tell scraping from fan-out.

## Evidence
- Inline fan-out under the permit: `crates/server/src/worker.rs:458-526`.
- Forced flush inside the alert path: `crates/server/src/worker.rs:832`.
- Materialization runs unconditionally, even with no new match: `crates/server/src/worker.rs:839-844`.
- Load-bearing ordering comment: `crates/server/src/worker.rs:510-522`.
- Semaphore + default concurrency 4: `worker.rs:21`, `crates/core/src/config.rs:863`.

## Acceptance criteria
- Non-critical fan-out no longer occupies the concurrency permit, and is BOUNDED (not
  fire-and-forget): failures and drops remain visible in logs/metrics, and shutdown does not lose
  in-flight fan-out silently.
- The enforcement ordering `suppress_unhealthy → enforce_contracts → watches/triggers` is provably
  preserved (a test fails if it moves).
- Per-stage timings (run, index, hooks, alerts) exposed on the job result/terminal event.
- Throughput measured before/after with a deliberately slow index fixture.

## Risks / non-goals
Correctness before speed: the health/contract gates must never run after the hooks. Do not detach
anything whose loss would silently drop a user-visible alert.

## Build record
New bounded `crate::fanout` pool: **off-slot but never fire-and-forget** — bounded concurrency,
bounded backlog that falls back to INLINE (never drops, because a dropped unit is a silently unsent
webhook), drainable (shutdown awaits it and logs `abandoned=N` rather than exiting quietly), and
panic-caught per unit. Measured with the predecessor's harness + a slow-index fixture: 6 jobs @
concurrency 2, 40ms scrape + 300ms index → **827ms inline vs 230ms off-slot (3.6x)**, zero work lost
in either arm; the test asserts only 2x so a loaded box cannot flake it. Migration 0034 `job_stages`
records per-stage timings (NULL = never reached that stage, deliberately distinct from 0).
**Ordering guard strengthened, not weakened**: the structural inventory test now also requires each
gate/hook call to appear exactly once AND to lie inside `finalize_fanout` — because moving the
pipeline changed its task, and the new failure mode (splitting the pipeline across the permit
boundary so a webhook races its own gate) is invisible to an order-only check. New
`fanout_owns_outcome` fence re-checks (status, attempts) at unit start — the ownership the inline
version got for free from having just written the completion.
Director review: read the diff; failure/timeout/panic paths still finalize inline (small), success
path is the one moved. **Accepted semantic change**: a job now reads `succeeded` before its docs are
searchable — documented in runtime.md ("poll the terminal event, not the row"). Also fixed a
pre-existing hole: `complete()` Err used to fall through to hooks on a row still marked running.
