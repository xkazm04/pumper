---
slug: budget-exhaustion-terminal
type: perfect/direction
context: "[[app-runtime]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-11
commit: f918006
---

## What & why
Budget exhaustion mid-run is deterministic, but the worker treats it as transient:
the job is retried with exponential backoff, the retry re-seeds spent_usd from the
ledger and re-exhausts instantly, burning every remaining attempt and hours of wall
clock for zero work. Separately, a DataHub `cost:pause` surfaces as "job budget of
$0.00 exhausted" — a confusing lie about what is actually a governance pause. Budgeted
jobs should fail once, fast, with an honest reason.

## Evidence
- `crates/core/src/app.rs:252–260` — require_budget returns generic Error::App.
- `crates/server/src/worker.rs:663–680` — Outcome::Finished(Err) → storage.fail, no
  classification; storage.rs:318–333 backoff retry.
- `crates/server/src/worker.rs:470–476` — retry re-seeds spend from ledger.
- `crates/server/src/datahub.rs:991–1007` — effective_budget forces Some(0.0) on
  cost:pause; the real reason is logged there but never reaches the error.
- fail_permanently already exists (used by VCR pre-run failures, worker.rs:447–455).

## Acceptance criteria
- [ ] Budget exhaustion is a typed/classifiable error (extracted named predicate or
      variant), with a test named after the anti-pattern (e.g.
      budget_exhaustion_not_retried).
- [ ] The worker routes it to the permanent-fail path; test proves a budgeted job
      exhausting mid-run fails once with remaining attempts un-burned.
- [ ] A governance-paused app's refusal says paused-by-governance, not "$0.00
      exhausted" — test.
- [ ] All other error retry semantics unchanged (worker lifecycle harness green).
- [ ] Doc-sync: runtime.md budget section documents terminal semantics.

## Risks / non-goals
- Only the require_budget refusal counts as exhaustion — do not classify transient
  ledger read errors or engine errors. The fetch path's soft downgrade stays a
  downgrade (it never errors).
- Non-goal: budget top-up / resume-with-more-budget flows.

## Build record
Shipped `f918006` (Lot A, opus, 2026-08-11). Error::BudgetExhausted +
is_terminal_for_job() (deliberately one-variant; over-classification guard lists every
other variant as retryable) → worker fail_permanently branch; budget_is_exhausted
shared by hard research refusal + soft fetch downgrade, pins None=unlimited.
Governance-pause substitution in worker::terminal_failure_reason (builder refuted my
criterion: the pause is SERVER state, core cannot know it — observable outcome
identical); is_cost_paused extracted from effective_budget. e2e: fails once with
attempts un-burned, ordinary errors still retry, terminal failure still notifies.
Review: keep.
