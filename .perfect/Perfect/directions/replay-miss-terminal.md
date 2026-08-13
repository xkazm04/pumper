---
slug: replay-miss-terminal
type: perfect/direction
context: "[[vcr-testing]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why

A VCR replay that reaches an unrecorded request fails with `Error::ReplayMiss` — and the job
is re-queued and re-runs from the top, `max_attempts` times, re-doing whatever live-free work
preceded the miss and missing again in exactly the same place. The refusal is a pure function
of an immutable cassette and immutable job params: the backoff ladder cannot change the answer.

The asymmetry is the tell. The **load**-time miss (no cassette at that path) IS terminal —
`worker.rs:551-568` calls `fail_permanently`. The **resolve**-time miss (cassette present,
request absent) is not. Same feature, same determinism, opposite handling.

`Error::BadRequest`'s own doc comment states the rationale verbatim: *"every producer is a pure
function of input that is immutable for the life of the job, so a retry re-parses the identical
text and re-refuses."* That sentence describes `ReplayMiss` exactly, and `ReplayMiss` is absent
from the list.

This is the fourth instance of a class this loop has killed once per round — r17
(`Browser::transact` default), r18 (`Error::Profile` refusals), r19 (http transport errors).

## Evidence

- `crates/core/src/error.rs:337-342` — `is_terminal_for_job` matches only
  `BudgetExhausted(_) | Transact(_) | BadRequest(_)`. No `ReplayMiss`.
- `crates/core/src/error.rs:209-214` — the `ReplayMiss` variant, doc'd "Typed so a replay MISS is
  distinguishable from an app failure", with no terminality claim either way.
- `crates/core/src/error.rs:220-222` — `BadRequest`'s rationale, which applies to `ReplayMiss`
  word for word.
- `crates/server/src/worker.rs:551-568` — the load-time miss calls `fail_permanently`; the
  resolve-time path (`crates/core/src/app.rs:288-294`, `:430-437`) returns an ordinary `Err`.

## Acceptance criteria

1. A resolve-time `ReplayMiss` fails the job **once**, with the same permanence the load-time miss
   already has. State in the diff which lever you took and why.
2. **Verify before widening**: check every construction site of `Error::ReplayMiss` and confirm
   each is genuinely deterministic-for-the-life-of-the-job. If any site is transient (e.g. an I/O
   error surfaced as a miss), do NOT widen the variant — take the other lever (classify at the
   construction site, or split the variant). Record the check in a doc comment so a future round
   cannot silently undo it.
3. A test named after the anti-pattern (`replay_miss_does_not_ride_the_retry_ladder` or similar)
   that fails against today's classification. Assert the attempt count, not just the error type.
4. `error.rs`'s existing terminal-classification test inventory covers the new variant, so a
   future variant addition is a visible decision rather than a default.
5. The `ReplayMiss` doc comment says whether it is terminal and why — the other three variants
   in that list each carry that sentence; this one should match.

## Risks / non-goals

- **Non-goal**: changing what counts as a miss, or adding any fallback to a live fetch. Replay's
  refusal semantics are correct; only its *retry* handling is wrong. ("Non-goal" here means *do
  not assume* — if you find a reason the refusal itself is wrong, raise it, don't act on it.)
- Hazard: `Error::App` must stay retryable (`error.rs:585` asserts it). Do not reclassify by
  widening anything that would capture `App`.

## Build record

(filled during build)
