---
slug: sched-misfire-honesty
type: perfect/direction
context: "[[cron-scheduler]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
`misfire_policy = "skip"` eats real, on-time work. `decide()` classifies the whole
pending batch from its OLDEST firing: an hourly schedule that missed 11:00 while
down and comes back at 12:00:05 sees `earliest = 11:00` → `Skip {missed: 2}` — the
legitimately-due 12:00 run is silently dropped (bughunt 2026-07-14, documented,
still open — the existing unit test pins the on-time claim only with no backlog
behind it). Worse, `grace` derives from the CONFIGURED tick, so any slow tick
(webhook drain, big reconcile) reclassifies an on-time firing as a misfire and a
skip-policy schedule eats it while the process was up the whole time. And the Skip
branch bypasses the registry/params gates the Fire branch applies, so a
skip-policy schedule pointing at an unregistered app accrues `skipped_count`
forever while its health says `unregistered_app` — two surfaces, two stories. The
user moment: "my nightly sync didn't run last night, `skipped_count` went up, and
the scheduler was healthy the whole time."

## Evidence
- `crates/server/src/scheduler.rs:442-496` — `decide()`: `earliest` classification
  (`:457-463`), Skip applies to the whole batch (`:464-477`).
- `crates/server/src/scheduler.rs:100` — `grace = schedule_tick_secs * 2` (config,
  not observed); `:768-783` — the on-time unit test uses a no-backlog reference.
- `crates/server/src/scheduler.rs:124-136` vs `:138-173` — Skip records
  unconditionally; registry/params gates live only under Fire.
- `docs/harness/refactor-bughunt-2026-07-14/job-worker-cron-scheduler.md:46-52` —
  the batch-skip bug documented as open.
- `docs/features/runtime.md:55` — "a firing detected on-time within that grace
  window always runs under both policies" — currently false in the shared-tick case.

## Acceptance criteria
- [ ] The load-bearing invariant, made true and pinned: **an on-time firing runs
      under BOTH policies even when it shares a tick with older missed firings.**
      Classification becomes per-firing, not per-batch (skip advances past the
      genuinely-missed ones AND the due on-time one still fires, in one tick).
      `decide()` stays pure; its Action vocabulary may grow (e.g. skip-then-fire) —
      builder's design, reasoning recorded.
- [ ] Slow ticks stop manufacturing misfires: "missed" must mean "a previous pass
      already had the chance to fire this and didn't" — not "older than a constant".
      Options with tradeoffs (builder picks): thread the previous pass's timestamp
      through `run()` into `decide()` (a firing due since the last pass is on-time
      by construction), or derive grace from the observed tick duration with the
      configured value as floor. The chosen rule must keep the existing
      DST/downtime tests meaningful (update them only with stated reasoning —
      a first-run test failure here is signal, not noise).
- [ ] Skip-branch gate parity: a schedule that could not fire (unregistered app,
      invalid params) does not record skip-advances either — fixing the row makes
      it fire, same contract the Fire branch already keeps (`last_run`/
      `last_skipped_at` untouched so the firing stays due).
- [ ] Fire-time enabled re-check: the tick works from a snapshot; before enqueue,
      re-verify the schedule is still enabled (governance/API disables race the
      pass — one point-read alongside the existing `latest_run` read).
- [ ] Unit tests for each: shared-tick skip+fire, slow-tick on-time preservation,
      skip-gate parity, disable race (e2e where the pure layer can't reach).
      docs/features/runtime.md's misfire prose updated to the now-true rule.

## Risks / non-goals
- Non-goal: changing `fire_once` collapse semantics (it already fires; keep it).
- Non-goal: exact `missed` counts beyond the existing MAX_MISFIRE_SCAN bound.
- Risk: `record_schedule_skip` interplay with `schedule_reference` — the skip
  advance must still move the reference exactly as today for genuinely-missed
  firings or the backlog re-scans forever (the r11 tests pin this; keep them green
  with reasoning if touched).

## Build record
(pending)
