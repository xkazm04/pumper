---
slug: stuck-job-reaper
type: perfect/direction
context: "[[Job Worker & Cron Scheduler]]"
lens: robustness
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: f04e2a8
---

## What & why
recover_stuck() runs only at startup; a job orphaned by a hung worker task stays `running` forever on a live server — no lease/heartbeat/reaper. Add heartbeat_at (migration), worker heartbeats while running, reaper on the scheduler tick re-queues stale jobs.

## Evidence
- Startup-only recovery: crates/server/src/main.rs:28-31, storage.rs:358-365
- No heartbeat column: crates/core/migrations/0001_init.sql
- 900s timeout drops future but can't kill blocking work: worker.rs:123-124

## Acceptance criteria
- [ ] heartbeat_at column (append-only migration, ts() format); worker updates it on an interval while a job runs.
- [ ] Reaper (scheduler tick) re-queues running jobs with stale heartbeat (threshold config, #[serde(default)] + Default).
- [ ] Slow-but-alive jobs never reaped (heartbeat proves liveness); attempts/backoff respected on reap.
- [ ] Unit tests + live verification; docs/features/runtime.md updated.

## Risks / non-goals
- Multi-process safety is a bonus, not required (single-process today).

## Build record
(pending)
