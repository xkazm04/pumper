---
slug: cron-maturity
type: perfect/direction
context: "[[Job Worker & Cron Scheduler]]"
lens: api-ux
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: c544db2
---

## What & why
Cron is UTC-only (no tz column); N missed firings collapse into one silent catch-up with no policy; scheduled jobs enqueue with max_attempts=1 so cron runs never retry transient errors. Add per-schedule timezone, misfire_policy (skip|fire_once), and max_attempts passthrough.

## Evidence
- UTC-only: crates/server/src/scheduler.rs:31,44; schedules table 0003_orchestration.sql:7-16
- max_attempts:1 hardcoded: scheduler.rs:69
- Misfire gap documented: docs/features/runtime.md:34

## Acceptance criteria
- [ ] Schedule columns timezone/misfire_policy/max_attempts via append-only migration; POST /schedules accepts + validates them (bad tz → 400).
- [ ] Scheduler evaluates cron in the schedule's tz (chrono-tz).
- [ ] Misfire policy honored across simulated downtime (skip advances last_run without firing; fire_once = today's behavior, explicit).
- [ ] runtime.md gap note replaced; OpenAPI updated.

## Risks / non-goals
- Full backfill/catch_up-N remains a non-goal.

## Build record
(pending)
