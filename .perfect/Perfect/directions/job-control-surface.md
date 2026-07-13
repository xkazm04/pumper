---
slug: job-control-surface
type: perfect/direction
context: "[[Job Worker & Cron Scheduler]]"
lens: feature
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 5a6258a
---

## What & why
Operators can retry exactly one failed|cancelled job at a time; a job stuck in `running` can't be touched until restart; a running job cannot be cancelled at all (cancel is queued-only). Add bulk retry, stuck-job reset, and true in-flight cancellation via per-job CancellationToken (shutdown work added the token infra).

## Evidence
- Single retry, failed|cancelled only: crates/core/src/storage.rs:341-355
- Queued-only cancel: storage.rs:249-258
- Worker spawns tasks with no handle/token map: crates/server/src/worker.rs:26-53

## Acceptance criteria
- [ ] `POST /jobs/retry` bulk (status/app filter, capped, count returned).
- [ ] `POST /jobs/{id}/reset` re-queues a running job; the orphaned task's late completion is discarded (guard on status/attempt).
- [ ] `DELETE /jobs/{id}` cancels running jobs cooperatively (per-job token; job → cancelled, not failed).
- [ ] OpenAPI annotations + EXPECTED inventory updated; live-verified.

## Risks / non-goals
- Cooperative cancel only — a blocking app future is bounded by job_timeout as today.

## Build record
(pending)
