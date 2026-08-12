---
slug: job-control-event-honesty
type: perfect/direction
context: "[[job-search-api]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: e638efc
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
- Builder (Lot J, opus) commit `e638efc`. Director review: **keep** — (a) `retry_bulk` →
  `(Uuid, String)` pairs and `cancel` → `Option<String>` via RETURNING in the SAME
  statement (no follow-up get racing the transition it describes); `requeued_events` is
  the extracted pure builder; wire shapes unchanged. (b) Shipped the CANCEL-WINS outcome:
  pure `cancel_kind(user_requested, shutting_down)`; the door claims intent UNDER the
  job_cancels mutex immediately before firing the token — that ordering closes the
  fire/mark gap; `resolve_cancel` records a committed suspend so the microsecond-loser
  path answers `{cancelled: false, suspended: true, note}` instead of lying (pinned at
  unit level); e2e with a real 6s drain window proves cancel-wins deterministically, 5/5.
  Accepted deviation: CANCEL_INTENTS as a worker.rs module static (state.rs was out of
  set) — poison-tolerant, lock order job_cancels→intents documented, bounded by in-flight
  count via the attempt-matched cleanup. BANKED follow-up: move it onto AppState.
- Builder refutation (load-bearing): "/events SSE has an app filter" was FALSE —
  `GET /events` streams with `|_| true`, no app filter exists; the blindness was confined
  to mcp/live.rs. Fix and docs worded accordingly.
- Bonus doors-audit find → Director commit `6f6efdb`: POST /triggers had the IDENTICAL
  budget filter, worse (stored on the row, replayed into every hop). Same
  validate_budget_usd wired; e2e at the trigger door; `budget_filter_antipattern_is_extinct`
  comment-stripped whitespace-stripped scan makes the convention enforced, not remembered
  (scan failed twice on first runs — doc-comment quote, then its own needle — both fixed
  in the scan, not the assertion).
