---
slug: schedule-truth
type: perfect/direction
context: "[[automation-api]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
GET /schedules exists to answer "why isn't this firing?" and it answers wrong in the two
cases that matter. (1) The scheduler's overlap guard is existential over ALL jobs of the
schedule (queued|running) while health reads only the NEWEST job — so POST /jobs/retry
re-queuing an OLD job of the schedule wedges firing forever while health says "ok". Bulk
retry with {app} can wedge every schedule of an app at once. (2) Under misfire_policy
"skip", touch_schedule advances last_run when NO job ran — last_run + null last_job_id
contradict each other in one response, and the eaten-firings count exists only in logs.

## Evidence
- crates/core/src/storage.rs:540-548 (schedule_has_active_job, existential) vs :553-564
  (latest_job_for_schedule, newest-only) — verified live 2026-08-12.
- schedules.rs:56-59 claims "the API and the reconcile loop can't disagree" — falsified.
- scheduler.rs:123-129 — Skip arm calls touch_schedule; storage.rs:739-746 UPDATE last_run.
- storage retry paths do not clear schedule_id (migration 0012 has no cascade).

## Acceptance criteria
- One extracted predicate (named function) backs BOTH the scheduler's overlap guard and
  the API's health derivation — the two reads structurally cannot disagree; test named
  after the anti-pattern (e.g. health_and_guard_share_one_predicate).
- The retry-wedge is fixed: a manually retried older job no longer holds the schedule's
  firing forever. Options (builder verifies data shapes first, recommends, records
  reasoning): guard considers only the schedule's most recent firing; retry clears or
  rewrites schedule_id; guard keys on newest-job-active like health. A test reproduces the
  wedge (fire A fail, fire B succeed, retry A, schedule still fires) and pins the fix.
- Misfire skip stops lying: last_run advances only when a job was enqueued; skips are
  recorded distinctly (e.g. last_skipped_at + skipped_count or equivalent) and surfaced on
  GET /schedules; project_next_run stays correct after the change — builder verifies its
  reference explicitly before choosing the recording shape.
- GET /schedules e2e over real HTTP exists (today: zero — scheduler tests bypass the
  router), covering health for: ok, the wedge scenario (as the refuted case), disabled,
  invalid cron, misfire-skip.

## Risks / non-goals
- Non-goal: the full schedule_runs decision ledger (banked as the context's next anchor).
- Risk: changing overlap-guard semantics affects legitimate long-running-job holds — the
  fix must keep "don't double-fire while my job still runs" intact; tests must cover it.

## Build record
(pending)
