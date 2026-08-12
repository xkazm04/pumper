---
slug: job-budget-floor
type: perfect/direction
context: "[[job-search-api]]"
lens: robustness
status: accepted
size: S
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
`POST /jobs/{app}` runs `budget_usd: body.budget_usd.filter(|b| *b > 0.0)` — a caller who
sends `budget_usd: 0.0` ("spend nothing") or a negative value gets `None`, which downstream
means NO budget: unlimited spend on a paid path (Claude research, paid fetch tiers). The
most cautious possible input produces the least cautious possible behavior. Same class as
round-9's budget-exhaustion-terminal: budgets are load-bearing safety rails here.

## Evidence
- `crates/server/src/routes/jobs.rs:126` — the filter.
- Round-9 [[budget-exhaustion-terminal]] made exhaustion terminal; this closes the front
  door that lets a zero budget mean infinity.
- Check the other work-creating doors for the same coercion: schedules
  (`routes/schedules.rs`), and whether `budget_usd` is even accepted there — the answer
  must be consistent across doors.

## Acceptance criteria
- A named validation fn rejects non-positive `budget_usd` with 422 and a message telling
  the caller what a zero budget would have meant; wired at the jobs door.
- Test named `budget_zero_is_rejected_not_unlimited` (+ negative case).
- Doors audit: every surface that accepts `budget_usd` (jobs, schedules if applicable)
  applies the same rule — inventory in the builder report; fix any divergent door in-set.
- `docs/features/http-api.md` documents the constraint.

## Risks / non-goals
- Risk: a caller scripting `budget_usd: 0` as "unlimited" breaks — that caller was
  already getting the opposite of the field's plain meaning; the 422 message educates.
- Non-goal: changing budget enforcement semantics mid-run.

## Build record
(pending)
