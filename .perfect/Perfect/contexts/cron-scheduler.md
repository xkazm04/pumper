---
name: cron-scheduler
type: perfect/context
group: Job Orchestration
category: lib
opportunity: 5
last_proposed: 2026-08-12
cooldown_until: 2026-08-14 (2 rounds)
directions: ["[[sched-tick-isolation]]", "[[sched-misfire-honesty]]", "[[schedule-budget-door]]"]
alias_of_old_map: "[[job-worker-cron-scheduler]] (round-2 pass covered scheduler.rs)"
---

## Current state (scouted 2026-08-12, r13 — full brief in scout report, digest here)
Tick loop scheduler.rs:44-88 (spawned UNJOINED, main.rs:192): serial per tick =
reconcile → reap_once (+hourly trigger-ledger prune) → webhook drain_due (inline,
default-on) → refresher::tick (spawned, default-off) → datahub govern_tick (spawned,
default-off) → sleep(tick). Idle cost 4 queries/tick. r11 door-parity + schedule-truth
and r12 budget floors verified in place — overlap guard, last_run/last_skipped_at split,
params door ×3 callers, health vocabulary: all sound, do NOT re-propose.

Direction-grade gaps (scout, verified against source):
- **G1** one schedule's storage Err aborts every alphabetically-later schedule that tick
  (`?` at scheduler.rs:132/147/191; caller logs once) — still-open bughunt 2026-07-14.
- **G4 CONFIRMED bug**: skip-policy batch-drops a currently-due ON-TIME firing that
  shares a tick with an older missed one (decide classifies from `earliest`,
  scheduler.rs:457-477) — bughunt doc names it open. G3: grace = configured tick ×2, so
  a slow tick (G2) misclassifies on-time as misfire → skip eats real firings.
- **G5** no panic guard on the tick task; datahub.rs:1038 + worker.rs:1233 sync-mutex
  unwrap/expect reachable inline; poisoned → scheduler dies silently, HTTP keeps
  serving, no cron/reaper/DLQ, no log. r10 lock_advisory sweep covers routes/mcp only.
- **G10** schedules carry NO budget_usd: CreateScheduleBody has no field, fire path
  EnqueueOptions::default → None = unlimited — the last work-creator without the r12
  budget door. [economics] enforce seam inert (config.rs:86-94).
- **G12** Skip branch bypasses registry+params gates (Fire-only) → skipped_count accrues
  on a row whose health says unregistered_app.
- G2 serial tick: DLQ drain batch 20×3 attempts×15s can park cron for minutes (fix
  overlaps webhook-delivery write set — bank there). G6 shutdown mid-tick: reconcile
  keeps enqueuing after token; task torn down between enqueue and touch_schedule.
  G7 disable race vs snapshot (governance disable mid-iteration → one extra fire).
  G8 cron_cache monotonic growth. G9 list_schedules unbounded full-table read w/ params
  blobs. G11 schedule_tick_secs unvalidated. G13 boot reconcile blocking fs IO,
  un-cancellable.
- **No SchedulerLoop lifecycle harness** (worker got one r4 ddebd66); every test calls
  reconcile directly — the tick loop itself has zero coverage. refresher.rs: 1 test
  (host_of); tick/run_pass/revalidate_one untested; shutdown_bounds.rs:255-260 asserts
  the DATAHUB flag while claiming to check the refresher's (private) RUNNING static.
- Docs: runtime.md:48 names 2 piggybacks, real count 4 (+nested prune); D3/D4 misfire
  prose overstates decide(); refresher spacing double-pays (try_acquire advances
  next_slot, then engine acquire sleeps it out — pass > tick easily).

## Direction history
- (as job-worker-cron-scheduler, round 2): see [[job-worker-cron-scheduler]].
- 2026-08-12 (r13, gate: director-self-gated, autonomous): slate of 5 drafted from the
  scout + Director first-hand read of scheduler.rs/refresher.rs. ACCEPTED 3:
  [[sched-tick-isolation]] (robustness M — per-schedule error isolation, panic/poison
  containment, shutdown between phases, first-ever SchedulerLoop harness),
  [[sched-misfire-honesty]] (robustness M — G4 CONFIRMED batch-skip of on-time
  firings, G3 slow-tick misclassification, G12 skip-gate parity, G7 disable race),
  [[schedule-budget-door]] (feature M — the last work-creator without the r12 budget
  contract). REJECTED 2, reasoning:
  - sched-tick-serialization (G2, robustness M): REJECTED-deferred — the real fix
    (drain off the tick / bounded drain) lives in webhook.rs/fanout.rs, which is
    webhook-delivery's write set; banked on THAT context's note as its next anchor.
    Fixing it from this context would violate the disjoint-write-set partition for
    no urgency (drain is bounded at 20/tick).
  - scheduler-tick-telemetry (feature S — last_tick_at/tick_duration surface):
    REJECTED — thin outcome value on its own; the runtime.md truth fix rides
    sched-tick-isolation's doc criterion, and tick-duration visibility only matters
    once G2 is fixed. Banked as a seed, not an anchor.

## Shipped
- (r13 wave in flight)
- (inherited — see [[job-worker-cron-scheduler]])
