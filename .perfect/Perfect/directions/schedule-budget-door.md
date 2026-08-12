---
slug: schedule-budget-door
type: perfect/direction
context: "[[cron-scheduler]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 85348d2 + 1f05e09
---

## What & why
Schedules are the LAST work-creator without a spend ceiling. Round 12 put the
budget floor on the jobs door and the trigger door; a schedule has no `budget_usd`
field at all — `CreateScheduleBody` can't express one and the fire path builds
`EnqueueOptions { ..Default::default() }`, so every scheduled run of a Claude-tier
app executes with `budget_usd = None`, which the budget system explicitly treats
as UNLIMITED. A standing cron order for a research app is exactly the scenario a
ceiling exists for: unattended, recurring, paid. This completes the r12 arc — one
budget contract, every door. The user moment: "I put a $2 ceiling on my research
jobs, then scheduled them nightly — and the scheduled ones ran unlimited."

## Evidence
- `crates/server/src/routes/schedules.rs:122-141` — `CreateScheduleBody`: no
  `budget_usd`.
- `crates/server/src/scheduler.rs:177-183` — fire path: `EnqueueOptions { params,
  max_attempts, priority, schedule_id, ..Default::default() }` → `budget_usd: None`.
- `crates/core/src/storage.rs:24-44` — `EnqueueOptions.budget_usd` already exists
  (jobs + triggers use it); `crates/core/src/storage.rs:994,1007` — triggers
  already persist `budget_usd`; schedules table (`0003` + `0039`) has no column.
- `crates/server/src/routes/jobs.rs:47-58` — `validate_budget_usd` (r12) is the
  shared floor validator; the r12 extinction scan
  (`routes/jobs.rs:612-648`) sweeps for unvalidated `budget_usd` doors.
- `crates/core/src/config.rs:86-94` — the `[economics] enforce` seam notes this
  exact gap as inert.

## Acceptance criteria
- [ ] `schedules.budget_usd REAL NULL` via migration `0040_schedule_budget.sql`;
      `Schedule` struct + every schedules read/write path carries it (grep-complete:
      list/create/update/managed paths). Migration inventory test updated.
- [ ] `POST /schedules` and the update path accept `budget_usd` and validate it
      through the SAME `validate_budget_usd` the jobs and trigger doors use — 0,
      negative, NaN, ∞ refused with the same 422 contract. The r12 extinction scan
      must SEE this door (verify it fails when the validator is bypassed — drive
      the guard against a violating snippet like its own meta-test does).
- [ ] The fire path passes the schedule's `budget_usd` into `EnqueueOptions`, so a
      scheduled job's receipt/budget behavior is byte-identical to the same app
      enqueued at the jobs door with that budget.
- [ ] Catalog-managed schedules: `create_managed_schedule` leaves budget NULL
      (catalog has no budget vocabulary today — do NOT invent one; note it in the
      doc as a known limit of catalog-managed rows).
- [ ] e2e: schedule with budget fires → job carries the budget → budget-exhausted
      behavior matches the jobs-door path (reuse the r9 budget-terminal fixtures).
      `GET /schedules` surfaces the field. docs/features/runtime.md (schedules
      section) + docs/features/http-api.md updated where the schedule body is
      documented.

## Risks / non-goals
- Non-goal: a default ceiling for schedules that don't set one (None stays
  unlimited — changing that default is a product decision explicitly NOT taken
  here; the door just becomes expressible).
- Non-goal: catalog TOML budget vocabulary.
- Risk: OpenAPI/utoipa body schema — regen/coverage test may pin the new field;
  run the route-inventory and openapi tests.

## Build record
Two-session build. The original Lot S builder died with the first half done —
session -4 snapshotted it as `85348d2` (migration 0040, storage column +
`set_schedule_budget`, both route doors through the shared
`routes::jobs::validate_budget_usd`). Continuation builder (opus, Lot S2) completed
it as `1f05e09`. Review verdict KEEP (session -5, diff read in full): the fire path
plumbs the ceiling via extracted `firing_budget`, read off the LIVE row the step
already re-reads for `still_firable` — a ceiling set while the pass walks the table
binds THAT firing (the money-vs-params asymmetry is argued in the doc comment);
`POST /schedules/{id}/budget` registered + in the EXPECTED route inventory;
migration replay test pins schedules cols 0018/0023/0039/0040; five e2e tests incl.
mid-pass bind and catalog-reconcile clobber-proof (create re-apply, cron update,
disable/enable), 3/5 fail with the fire-path line removed (builder-verified). NULL
stays no-ceiling — no default invented. The r12 extinction scan sweeps all of
server/src so the new doors are inside its coverage. **Deviation noted:**
budget-exhaustion parity with the jobs door is covered transitively (the enqueued
job is byte-identical; the exhaustion machinery is job-side and r9-tested) rather
than re-driven e2e — accepted. Docs: runtime.md spend-ceiling section, http-api.md
schedules row, "every work-creating door now has the same floor". Smoke extended
with 3 live checks (422 floor, 404 late door, round-trip) — Director commit.
Wave gate: `just ci` exit 0 (session -5).
