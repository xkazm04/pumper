---
slug: research-run-budget-is-real
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
`max_budget_usd` is sold as a **per-run** spend ceiling in three places and implemented as a
**per-CLI-call** one. The app's own param description says `"max_budget_usd": 0.0 (per-run Claude
spend ceiling)`; its first worked manifest example is titled *"Bounded web research run with a hard
spend ceiling"*; `docs/features/apps.md` repeats it. The value is written onto **every**
`ResearchRequest` (`lib.rs:410`) and the loop makes up to `MAX_STEPS = 12` of them.

The between-steps wall that would have caught this is a no-op on the shape the manifest itself
models. It reads `ctx.remaining_budget_usd()`, which returns `Ok(None)` whenever the **job** has no
`budget_usd` (`app.rs:199-200`) — so `if let Some(remaining)` never binds. Enqueue a research job
with `max_budget_usd: 0.5` and no job budget, exactly as the manifest example shows, and the
enforced ceiling is **$6.00**. The only entry path that is safe today is MCP `deep_research`, which
sets the clamped value as *both* the job ceiling and the param (`mcp/mod.rs:508-513`) — i.e. the one
caller that does not trust the app's own knob.

The same defect shape sits on turns: `state.turns_used += num_turns.unwrap_or(fallback)`
(`lib.rs:129`) trusts a per-envelope count, so `max_turns` is not a total either — it either
overshoots ~12× or over-counts and truncates the run early, and no test pins which.

The accumulator needed to fix both **already exists** — `RunState` carries `spent_usd` and
`turns_used` and both survive resume. What is missing is that they are consulted before *every*
step rather than only when `steps_done > 0`, and that each call's ceiling is clamped to the run's
remaining headroom.

## Evidence
- `crates/apps/research/src/lib.rs:36` — `const MAX_STEPS: u32 = 12`
- `crates/apps/research/src/lib.rs:218` — param doc: "(per-run Claude spend ceiling)"
- `crates/apps/research/src/lib.rs:250-256` — manifest example "hard spend ceiling", `max_budget_usd: 0.5`
- `crates/apps/research/src/lib.rs:379-386` — the wall, gated on `steps_done > 0`, `if let Some(..)`
- `crates/apps/research/src/lib.rs:410` — `request.max_budget_usd = max_budget_usd`, inside the loop
- `crates/core/src/app.rs:199-200` — `let Some(budget) = self.budget_usd else { return Ok(None) }`
- `crates/engine-claude/src/lib.rs:109-112` — `--max-budget-usd` emitted per invocation
- `crates/apps/research/src/lib.rs:129, 179-193` — `turns_used +=` per envelope
- `crates/apps/research/src/lib.rs:716-735` — `step_cap_is_recorded_…` builds a context with **no**
  `budget_usd` and asserts `call_count() == MAX_STEPS`, i.e. pins "12 metered calls, no ceiling" green

## Acceptance criteria
1. With `max_budget_usd: X` and **no** job `budget_usd`, total spend across the whole run cannot
   exceed `X`. A test proves it by scripting a researcher that reports cost and asserting the run
   stops without exceeding `X` — a test that fails against today's code.
2. The pre-step budget check runs before **every** step including the first-after-resume, not only
   when `steps_done > 0`. Restored `spent_usd` counts toward the ceiling.
3. Each per-call ceiling is clamped to the run's remaining headroom, so the last step cannot
   overshoot the total.
4. `max_turns` is enforced as a run total against `turns_used`. **Hazard, decide it in code, don't
   assume:** the CLI's `num_turns` may be per-invocation or session-cumulative, and the two readings
   break in opposite directions. Read `engine-claude:396` and the resume path, pick the reading the
   code actually produces, and leave the reasoning in a doc comment so a later round cannot silently
   undo it. If it is genuinely ambiguous, make the accumulator correct under BOTH readings
   (e.g. cap on max, not sum) and say so.
5. `docs/features/apps.md` and the param/manifest text agree with the implementation. If you keep
   the per-call semantics for the CLI flag, the *documented* contract must still be the run total.
6. `step_cap_is_recorded_when_the_agent_never_shapes_a_report` still passes (it sets no
   `max_budget_usd`, so it must be unaffected) — if your change breaks it, you changed the wrong thing.

## Risks / non-goals
- **Non-goal:** changing `MAX_STEPS`, the chunking strategy, or the engine's CLI flag semantics.
- **Non-goal (do not assume — raise it if you see it):** the job-level `budget_usd` path already
  works; do not re-plumb it.
- Risk: clamping the per-call ceiling to a tiny remainder could make a final step useless. Prefer
  stopping with `stop_reason: budget_exhausted` over issuing a call that cannot finish.

## Build record
(filled during build)
