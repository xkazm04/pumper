# Task — the research agent is never told the budget it is being cut off by

- **Opened:** 2026-09-04 by registry run `odr-2026-09-04`
- **Registry subject:** `software-engineering/llm-agent/orchestration/fleet-orchestration`
- **Techniques:** `soft-budget-under-the-hard-cap` (new), `parallel-dispatch` (amended: *when the requester cannot survive the wait, refuse instead of queueing*)
- **Mode:** `task` — the shippable half landed this run (commit `eb37835`); this is the behavioural half
- **Status:** proposed, first step not taken

## What already shipped, and why the rest is separate

`eb37835` made the loop's two bounds agree and report: `reachable_turn_budget`
derives the ceiling from `MAX_STEPS`, and the result carries
`turns_budget_reachable` + `turns_budget_clamped`. That change is measurable on
`cargo test` alone and is done.

What it did not change is the thing the registry technique is actually about.

## The gap

`crates/apps/research/src/lib.rs:587-599` builds one of three step prompts. None
of them names a number. The agent is told *"search further only where needed,
then finish"* — a **condition with no floor** — while three hard caps
(`MAX_STEPS`, `max_turns`, `max_budget_usd`) sit in the machinery and terminate
the loop without ever appearing in the agent's context.

Two consequences, both invisible in any current metric:

1. **The only elected exit is `Completed`.** Every other `StopReason` is the
   machinery cutting the loop mid-plan. On a run where the report never shapes,
   the cap firing *is* the ordinary exit path — which is precisely the state the
   cap exists to bound.
2. **The refusal never reaches the requester.** The requester here is a model
   turn: it cannot be handed a promoted slot, it will not exist when a later
   step runs, and it re-reads only its own transcript. Telling the *caller*
   (`turns_budget_clamped`) is correct and does not help the agent. The
   amendment's rule is that the refusal is addressed to the requester in the
   channel the requester reads, and carries the number.

## The change

In the resume prompt branches (`(Some(_), 0)` and `(Some(_), _)`), interpolate
the run's remaining budget from the same values the machinery enforces — never
a literal:

```
"You have <remaining> of <total> turns left across at most <steps_left> more
steps. Finish and emit the report before they run out; a truncated report is
worse than a shorter complete one."
```

Derived, per `limits-are-derived`: the numbers come from `state.turns_used`,
`max_turns`, `state.steps_done` and `MAX_STEPS`, so tuning any of them moves
the prompt too. A hand-typed number here would reintroduce the drift `eb37835`
just removed, one layer up.

## The measurable

**The cap-fired fraction**: runs ending `stop_reason != completed` over runs
that entered the loop. Read it from the existing result field — no new
instrumentation. Secondary: mean `num_turns` on completed runs (the technique
predicts it falls, because the agent stops on its own earlier).

**Falsifier.** If the cap-fired fraction does not move, the agent is not reading
the budget line and the change is prompt noise — revert it rather than tuning
the wording, and record that a mid-conversation budget statement did not bind.

## Size and gate

Three prompt branches plus a small `remaining_budget_line` helper and its unit
test: one file, ~40-60 lines. `cargo test -p app-research` covers the helper;
the cap-fired fraction needs metered runs, so the behavioural arm is an
operator-scheduled A/B over a real topic set, not a CI gate.

## Why it is a branch

The behavioural claim cannot be settled by the project's own gate, and
`eb37835` deliberately kept behaviour unchanged. Open
`direction/agent-told-its-budget` and hold it until the A/B has an arm count.
