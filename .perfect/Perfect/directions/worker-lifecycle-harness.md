---
slug: worker-lifecycle-harness
type: perfect/direction
context: "[[job-worker]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: ddebd66
---
## What & why
Essentially all of `worker.rs` is under three weeks old (M20 contracts, M22 sinks, M23 durable
execution, M24 VCR, M25/M26 DataHub all landed into it) and it has exactly one test — an e2e over
the non-looping `run_one` seam. The real loop, the shutdown drain, the suspend→resume round trip,
the reaper, timeouts, cancellation, per-app caps and the load-bearing gate ordering are all
unguarded; the ordering invariant is defended only by a comment that says "if it moves below them
the guarantee is gone".

## Evidence
- Only test: `crates/server/src/e2e/worker_fanout.rs` (over `run_one`, `worker.rs:100-126`).
- Drain/suspend: `crates/server/src/worker.rs:144-192`, `411-428`; restore `worker.rs:199-232`.
- Reaper: `crates/server/src/worker.rs:615-640`, `crates/core/src/storage.rs:602-623`.
- Ordering comment: `crates/server/src/worker.rs:510-522`.

## Acceptance criteria
- Deterministic harness driving the real loop (`run()`), not only `run_one`.
- Suspend→resume proves the app resumes from its checkpoint and does not re-pay budget.
- Reaper, timeout, cooperative cancel and per-app caps each guarded by a test.
- An ordering test that fails if the health/contract gates move below the hooks.
- Runs under `cargo test --workspace` with no network and no real Chrome; no flaky sleeps.

## Risks / non-goals
Not a rewrite of the worker to be testable at any cost — seams added must be ones the production
path actually uses. No wall-clock-sensitive tests.

## Build record
`WorkerLoop::{start,shutdown}` spawns the REAL `worker::run` (no test-only fork) + `wait_for`/
`wait_status` condition polling. 11 new tests: suspend→resume proving reset semantics AND that
budget is not re-paid ($0.60 of $1.00 remaining), reaper (requeue / permanent-fail / disabled),
timeout, cooperative cancel, per-app cap **with a control test** (cap 3 ⇒ peak > 1, so the cap-1
assertion is not just harness serialisation), unregistered app, yielding-job lease freshness, and
gate ordering guarded twice (behavioural + structural inventory over the call sites).
**Builder refuted the brief**: `worker.rs` had THREE test files, not one (`durable.rs`,
`shutdown_drain.rs` existed) — it built on them instead of duplicating. Negative control run
(gate moved below hooks) made both ordering guards fail as designed.
Director review: read the diff; harness uses production seams only. Gates green on master.
