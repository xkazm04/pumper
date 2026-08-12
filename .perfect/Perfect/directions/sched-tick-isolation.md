---
slug: sched-tick-isolation
type: perfect/direction
context: "[[cron-scheduler]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 4788dbb
---

## What & why
The scheduler task is the process's heartbeat — cron firing, stuck-job reaping, and
the webhook DLQ drain all ride it — and it can die or starve silently. (1) One
schedule's storage error aborts the whole reconcile pass via `?`, so every
alphabetically-later schedule silently never fires while `GET /schedules` says
`ok` (bughunt 2026-07-14, still open). (2) The task is spawned UNJOINED with no
panic guard, and two sync-mutex `unwrap`/`expect` sites are reachable inline from
the tick — a single poisoning panic kills cron+reaper+DLQ forever while HTTP keeps
serving, with no log line saying so. (3) Shutdown: the tick body has zero
cancellation checks, so a SIGTERM mid-reconcile keeps enqueuing scheduled jobs into
the drain, and main never joins the scheduler. The tick loop itself has ZERO test
coverage (no SchedulerLoop harness; every test drives `reconcile` directly). The
user moment: "schedules just stopped firing last Tuesday and nothing anywhere said
why."

## Evidence
- `crates/server/src/scheduler.rs:132-135,147,191` — per-schedule `?` aborts the
  pass; caller logs once (`:60`); rows ordered by app (`storage.rs:726`).
- `crates/server/src/main.rs:192` — `tokio::spawn(scheduler::run(...))`, handle
  dropped; only the worker is joined (`main.rs:233`).
- `crates/server/src/datahub.rs:1038` + `crates/server/src/worker.rs:1233` —
  sync-mutex `unwrap`/`expect` reachable inline from the tick; the r10
  `lock_advisory` sweep covers only routes/mcp.
- `crates/server/src/scheduler.rs:56-58,82-85` — the only shutdown checks; comment
  at `:81` contradicts placement (claims "as soon as", runs after enqueues).
- `crates/server/src/harness.rs:89-115` — WorkerLoop exists (r4 ddebd66);
  no SchedulerLoop; only production ref to `scheduler::run` is main.rs:192.

## Acceptance criteria
- [ ] One failing schedule cannot starve the rest: per-schedule errors are caught,
      logged with the schedule id, counted, and the pass continues. Extract the
      per-schedule step into a named function so the isolation is testable
      (repo doctrine: bug fixes ship as extracted, tested functions). Test named
      for the anti-pattern (e.g. `one_bad_schedule_does_not_starve_the_rest`).
- [ ] The tick body cannot die silently: panics in a tick are contained (the loop
      survives and logs loudly), and the two inline sync-mutex sites stop
      unwrapping (use/extend the r10 advisory-recovery idiom — verify what
      `routes/error.rs:185-202` provides and whether it is reachable from these
      call sites; if a shared helper needs a new home, say so in the report).
- [ ] Shutdown is honored between tick phases: once the token fires, no NEW
      schedule is enqueued (check the token in the reconcile loop), and `main.rs`
      joins the scheduler task like it joins the worker so the tick finishes
      cleanly instead of being torn down mid-await between `enqueue` and
      `touch_schedule`.
- [ ] A `SchedulerLoop` harness (mirroring `WorkerLoop`) drives the REAL `run()`
      loop in e2e: proves a tick fires a due schedule, survives an injected
      per-schedule failure, and exits cleanly on the token. This is the first-ever
      coverage of the tick loop.
- [ ] docs/features/runtime.md's scheduler section tells the truth about the tick
      composition (currently names 2 piggybacked jobs; the real count is 4 plus a
      nested hourly prune) and about the boot catalog reconcile.

## Risks / non-goals
- Non-goal: making the tick concurrent / re-ordering the piggybacked tasks (the
  DLQ-drain-parks-the-tick issue is banked on webhook-delivery — do not fix here).
- Non-goal: restarting a dead scheduler from outside (containment inside the loop
  makes that unnecessary).
- Risk: catch-unwind over an async body needs care (AssertUnwindSafe over the
  future, or containment at the spawn boundary + supervised respawn — builder
  picks with reasoning).

## Build record
Builder (opus, Lot S, original r13 session) shipped `4788dbb`. Review verdict KEEP
(session -4, diff read in full): `reconcile_one` + `PassTally::absorb` — a
per-schedule error CANNOT propagate by signature; two-level panic containment with
argued `AssertUnwindSafe`; `lock_advisory` at both inline sync-mutex sites
(datahub.rs, worker.rs); shutdown checked between schedules; main joins the task
bounded by `SCHEDULER_SHUTDOWN_GRACE`. `SchedulerLoop` harness = first-ever coverage
of the real tick loop (fires a due schedule, survives an injected per-schedule
failure, exits on the token). runtime.md now tells the truth about the tick's four
piggybacked jobs + boot catalog reconcile. Wave gate: `just ci` exit 0 (session -5).
