---
slug: job-control-event-honesty
type: perfect/direction
context: "[[job-search-api]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
Two ways the job-control surface tells watchers something false:
1. **Invisible events.** Bulk retry and queued-cancel emit `JobEvent::new(id, "", ...)` —
   an empty app string. Both the MCP live stream and app-filtered SSE match on the event's
   exact `app`, so an app-scoped watcher (the normal way to watch) NEVER sees a bulk retry
   re-queue its jobs or a cancel land. An operator dashboard filtered to `app=grants`
   shows a cancelled job as still queued.
2. **The drain-window cancel lie.** During graceful shutdown, the worker fires every
   running job's cancel token to mean SUSPEND (checkpoint + requeue, run again next boot).
   `DELETE /jobs/{id}` in that window fires the same token and answers
   `{cancelled: true, running: true}` — but the job is actually suspended and WILL
   resurrect. The user's explicit cancel is silently converted into "run later".

## Evidence
- `crates/server/src/routes/jobs.rs:297` (bulk retry), `:367` (queued cancel) — empty app.
- `crates/server/src/mcp/live.rs` `LiveFilter::keep` — `ev.app != *app` → filtered out.
- `crates/server/src/routes/jobs.rs:361-386` — cancel fires the shared token, reports
  cancelled unconditionally.
- `crates/server/src/worker.rs:142-192` — drain phase 2 fires the same tokens as suspend;
  `execute` treats cancellation during shutdown as suspend.
- `worker.rs:50-66` — token registry keyed `(job.id) -> (attempts, token)`.

## Acceptance criteria
- Bulk retry emits per-job events carrying the REAL app (storage `retry_bulk` returns
  `(id, app)` pairs or equivalent); queued-cancel's event carries the job's app. Test:
  `control_events_carry_app_not_blank`.
- A user cancel during the drain window results in the job actually CANCELLED (user
  intent outranks suspend), OR — if the builder finds cancel-during-drain structurally
  unable to win the race — the response tells the truth (`suspended`/`requeued`, not
  `cancelled: true`). Preferred: record user-cancel intent (e.g. alongside the token
  registry entry) so `execute`'s shutdown path distinguishes "operator said stop" from
  "process is draining". Test named `cancel_during_drain_cancels_not_resurrects` (or the
  honest-response variant).
- No regression in the drain path's suspend semantics for jobs nobody cancelled
  (existing drain tests stay green).
- `docs/features/http-api.md` (or the jobs section's doc home) reflects the contract.

## Risks / non-goals
- The intent flag races the drain window — the fence is the token registry's existing
  mutex; keep the check inside it.
- Non-goal: changing bulk-retry semantics, the fence, or the reaper.

## Build record
(pending)
