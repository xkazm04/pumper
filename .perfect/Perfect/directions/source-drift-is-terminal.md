---
slug: source-drift-is-terminal
type: perfect/direction
context: "[[grants-unified-layer]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## What & why

**The grants fleet's schema-drift refusals are correctly fail-closed and incorrectly classified as
retryable, so a permanent upstream rename burns three attempts plus backoff every scheduled day,
forever, and reads in the job log exactly like the source being down.**

The refusals themselves are good work and must not change. Every one is raised *before* any write,
so the stored corpus is untouched:

- `ca-grants/src/lib.rs:209-214` — `total > 0 && records.is_empty()`.
- `grants-gov/src/lib.rs:341-347` (`empty_page_is_drift`), `:363-381` (`empty_listing_is_drift`,
  gated on `whole_corpus_query` so a legitimately-empty narrowed pull is not drift).
- `cordis/src/lib.rs:244-251`, `:254-258`, `:307-312`, `:395-400`.
- `eu-sedia/src/lib.rs:204`.

Failing closed is right: grants apps are upsert-only, so a partial listing would not tombstone — but
it *would* flow into `finalize_unified` → `sweep_batch` (`grants-common:979-1002`) and `sweep_closed`
(`:1012-1062`), which flip rows to `status: "closed"`. There is a destructive path; guarding it
before the write is correct.

**The defect is the error type.** All of them are `Error::App`, and `Error::App` is not in
`is_terminal_for_job` (`crates/core/src/error.rs:368-376`, currently
`BudgetExhausted | Transact | BadRequest | ReplayMiss`). So grants.gov renaming `hitCount` produces
three identical failing attempts with backoff, every scheduled run, indefinitely — with no signal
distinguishing "the source changed its schema" from "the source was down." The operator moment: a
permanently-broken pipeline that looks like a flaky one, and a retry budget spent proving the same
thing 3× a day.

This is the same honesty argument the repo has already made twice in this file. `ReplayMiss`
(`:220-223`) and `BadRequest` (`:231-233`) each carry a `**Terminal for a job**` paragraph justifying
their classification from the *nature of the failure*: a retry re-reads identical inputs and
re-refuses. Schema drift is precisely that shape — the upstream field is renamed, the params are
frozen at enqueue, and attempt #2 re-parses the identical response and re-refuses.

## Evidence

- `crates/core/src/error.rs:368-376` — `is_terminal_for_job`, a `matches!` over four variants.
- `:220-223` (`ReplayMiss`), `:231-233` (`BadRequest`) — the established doc convention a new
  variant must follow.
- `:154` `pub enum Error` — **not** `#[non_exhaustive]`, and `is_terminal_for_job` is a `matches!`,
  so **adding a variant is purely additive**; verified before this direction was accepted.
- The eight drift refusal sites listed above.
- `crates/apps/grants-common/src/lib.rs:979-1002`, `:1012-1062` — `sweep_batch` / `sweep_closed`, the
  destructive path that makes fail-closed the right call.
- Contrast, deliberately **out of scope**: `grants-gov/src/lib.rs:487-496` documents the detail stage
  as *"NON-FATAL, LOUD"* and `detail_stage_is_broken` (`:530-533`) aborts the stage, not the job.
  That degradation is correct and must stay.

## Acceptance criteria

1. A schema-drift refusal is **terminal for the job** — one attempt, no backoff loop.
2. It is **distinguishable in the job record** from a transient source outage. An operator reading a
   failed job must be able to tell "the source renamed a field" from "the source was down" without
   reading prose.
3. The classification is justified in the same `**Terminal for a job**` doc-comment form the file
   already uses for `ReplayMiss` and `BadRequest` — reasoning from why a retry cannot succeed.
4. All eight refusal sites across the four apps adopt it. A test proves the new class is terminal,
   and an inventory-style test proves no drift site was missed (EXPECTED-diff idiom — see
   `crates/server/src/routes/mod.rs`).
5. The detail-stage degradation and every warn-only path (`sweep_warning`, `drift_warnings`,
   `DetailJoin::warning`) are **unchanged**. Only pre-write listing refusals are retyped.

## Risks / non-goals

- **Risk:** a new `Error` variant is additive but reaches the whole workspace. Verified safe before
  acceptance — `Error` is not `#[non_exhaustive]`, `is_terminal_for_job` is a `matches!`, and
  `triggers.rs`'s `hook_failure_outcome` matches on `plugin_failure()` (a different enum), not on
  `Error`. If the compiler nonetheless finds a match arm **outside your write set**, report it, do
  not fix it.
- **Risk:** making drift terminal means a genuine transient that *looks* like drift stops retrying.
  Every guard here already gates on evidence that rules that out (a non-zero declared total with
  zero parsed rows; an empty listing against a non-empty stored corpus). Do not add new drift
  detection — only retype the existing refusals.
- **Non-goal:** `crates/server/src/worker.rs`. If terminality is honored purely through
  `is_terminal_for_job`, the worker needs no edit; it is NOT in the write set. If you believe it
  does, that is a `DECISION NEEDED`.

## Build record

(filled during build)
