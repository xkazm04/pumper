---
slug: quarantine-recovery-ladder
type: perfect/direction
context: "[[source-resilience]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 0096a79
---
## What & why
`Quarantined` is a terminal state: `next_state` returns it unconditionally regardless of how many
clean runs follow. The only exit is a human `POST /sources/{id}/state`. Meanwhile `Probation` exists
in the enum, behaves correctly in the ladder, and **nothing in the codebase ever promotes into it** —
the module itself admits automated repair "is not built". So a source that breaks at 03:00 and
self-heals at 04:00 stays quarantined — writes diverted to `@q`, removals suppressed, pushes and
indexing blocked — until a person notices. This is the single biggest reason `enforce = true` is not
adoptable today.

## Evidence
- Terminal state: `crates/core/src/resilience/detect.rs:718-721`.
- `Probation` behaves but is unreachable automatically: `detect.rs:724-730`, `mod.rs:83-85`.
- Only exit: `crates/server/src/routes/health.rs:214-241` → `store.rs:158-177`.
- What stays gated meanwhile: `app.rs:642-648` (`@q` + trust), `app.rs:613-626` (removals),
  `worker.rs:970-992` (pushes), `worker.rs:1450-1459` (index).

## Acceptance criteria
- N consecutive clean JUDGED runs promote Quarantined → Probation → Healthy; N is config-gated with
  a documented default and validated.
- A trip during Probation drops straight back to Quarantined (no slow re-climb).
- The fate of the `@q` shadow dataset on recovery is decided and documented — not left implicit.
- The ladder state-table test is extended to cover the up-path, including trip-during-probation.
- Recovery counts only runs the detector actually judged — an unjudged run must not heal a source.

## Risks / non-goals
Recovery must not be reachable through unjudged or below-cohort runs (that would turn
[[adaptive-cohort-floor]]'s hole into a self-healing exploit). Not a change to what trips a source.

## Build record
`[resilience] recovery_runs` (default 3, rejected at 0 in the validation block). Quarantined →
Probation → Healthy, each rung costing its own clean streak; a trip during Probation drops straight
back. The streak is **derived** from `source_runs` since `state_since`, never a stored column —
following the existing `recent_trips` doctrine — and the SQL counts only judged verdicts
(`ok|broken|self_inflicted`), so a `below_cohort` run provably cannot heal a source. That interlock
with [[adaptive-cohort-floor]] has its own test (`an_unjudged_run_cannot_heal_a_quarantined_source`).

**The `@q` call was taken, not deferred: leave the shadow dataset in place.** Auto-merging
quarantine-era records would launder exactly the data quarantine exists to exclude; renaming breaks
the audit trail. Release goes to `probation` (rows stamped `provisional`) rather than straight to
healthy, so a premature release is visible **in the data**, not just in a log line.

Director review: read the diff; the derived-streak design and the judged-verdict filter are both
correct. The builder made the product call I told it it could escalate — and made the right one.
