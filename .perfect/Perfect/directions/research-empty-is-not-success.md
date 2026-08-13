---
slug: research-empty-is-not-success
type: perfect/direction
context: "[[agentic-research]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why
A research run that produced **nothing** completes as SUCCEEDED: the job row goes green, the result
webhook fires, a search doc is written, and the payload is `{report: "", structured: false}`. Five
of six `StopReason` values return `Ok`, and the empty case is not distinguished from the useful one.

**The Director narrowed this finding under verification, and the narrowing is the direction.** The
scout read the whole class as broken. It is not: `stop_reason: budget_exhausted` carrying real
partial text is *correct and valuable* — the user paid for those findings and should get them.
`budget_exhaustion_between_steps_stops_the_loop_and_keeps_the_partial` (`lib.rs:738-760`) asserting
success on `"partial findings, not json"` is therefore a **right** test and must keep passing. The
genuine lies are narrower and there are two:

1. **Empty is not partial.** A run whose accumulated text is empty has produced nothing at any price.
   The rest of the fleet already fails this case — plugin fails when every document failed, mpsv-*
   fail on collapsed feeds, grants-gov fails on a positive `hitCount` with zero rows. Research is
   the outlier, and `an_unshaped_reply_with_no_session_id_stops_immediately` (`lib.rs:696-713`)
   `.unwrap()`s a run whose entire output was `"not json at all"`.
2. **In-shape emptiness is stamped `completed`.** `is_report_shaped` (`lib.rs:492-496`) accepts
   `{"summary": "", "key_findings": [], "sources": []}`, so a model refusal emitted in the requested
   shape is reported as a **finished** research with `structured: true`. The three shape tests
   (`:502-532`) cover wrong types and missing keys and never cover empty ones.

**Rider — a decorative write must not cost a re-research.** `save_artifact` at `lib.rs:483` uses `?`.
A `tokio::fs` failure becomes `Error::Io`, which is retryable (`error.rs:689`). The checkpoint
already holds the finished result, so the retry takes `Plan::Done` and hits the *same* `?` at
`lib.rs:332`. Each restored attempt bumps the resume counter (`worker.rs:399`); at
`max_resume_failures = 3` the checkpoint is discarded and the next attempt **re-runs the entire
research at full price**. A JSON dump nobody reads is not worth a paid re-run.

## Evidence
- `crates/apps/research/src/lib.rs:458-485` — five of six stop reasons return `Ok`
- `crates/apps/research/src/lib.rs:492-496` — `is_report_shaped` ignores emptiness
- `crates/apps/research/src/lib.rs:502-532` — shape tests: wrong types, missing keys, never empty
- `crates/apps/research/src/lib.rs:696-713` — `.unwrap()` on a run whose output was `"not json at all"`
- `crates/apps/research/src/lib.rs:738-760` — the test that is **correct** and must keep passing
- `crates/apps/research/src/lib.rs:332, 483` — the two `?`s on the decorative artifact write
- `crates/core/src/error.rs:689` — `Error::Io` is retryable
- `crates/server/src/worker.rs:386-399` — resume counter → checkpoint discarded at the cap
- `crates/server/src/worker.rs:801-868` — the full success fan-out that fires on an empty report

## Acceptance criteria
1. A run that ends with no usable content **fails** the job with a reason naming what happened. A
   test proves it. Define "no usable content" narrowly — empty/whitespace-only accumulated text —
   and say in a doc comment why partial-but-nonempty stays a success.
2. `budget_exhaustion_between_steps_stops_the_loop_and_keeps_the_partial` **still passes unchanged.**
   If your change fails it, your definition of failure is too wide — narrow it, do not edit that test.
3. `is_report_shaped` rejects a structurally-valid but content-free report; a test covers
   `{"summary":"","key_findings":[],"sources":[]}` and the non-empty counter-case.
4. `an_unshaped_reply_with_no_session_id_stops_immediately` is updated to assert the *new* honest
   outcome, with its rename reflecting what it now pins.
5. The `report.json` artifact write is best-effort: a filesystem failure is logged and does not fail
   a job whose research already completed. Both sites (`:332`, `:483`) are covered.
6. Whatever the failure carries, the spend already made is still reported/metered — a failed run
   must not also lose the record of what it cost.

## Risks / non-goals
- **Non-goal:** changing the `StopReason` vocabulary or the chunking loop.
- **Non-goal (do not assume — raise it if you see it):** the `cost_usd`-excludes-failed-steps gap
  and the checkpoint-bool-is-discarded gap are real and banked; do not fix them here unless
  criterion 6 forces you to touch that line, in which case say so in your report.
- Risk: failing on empty could break a legitimate "the agent had nothing to say" case. That case is
  indistinguishable from a total failure from the outside, and the fleet's answer is to fail — but
  if you find a concrete counter-example in the code, report it instead of guessing.

## Build record
(filled during build)
