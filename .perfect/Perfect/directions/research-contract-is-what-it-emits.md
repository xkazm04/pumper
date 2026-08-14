---
slug: research-contract-is-what-it-emits
type: perfect/direction
context: "[[agentic-research]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: df6fab1
---

## What & why
`output_shape` is the consumer contract: `GET /apps`, `GET /apps?format=tools` and the MCP manifest
all publish it verbatim (`registry.rs:130-154`). Research's declares **three keys `run()` never
emits** and **omits six it does** — the worst drift measured in this repo so far.

Declared: `{summary, key_findings, sources, session_id, cost_usd, steps, resumed_from_checkpoint,
stop_reason}`. Emitted: `{query, report, structured, resumed, resumed_from_checkpoint, steps,
cost_usd, duration_ms, num_turns, session_id, stop_reason}`.

`summary` / `key_findings` / `sources` are nested inside `report`, and only when `structured ==
true`. When it is false, `report` is a bare string — so the three declared keys are unreachable **at
any depth**. An agent that codes against the published manifest gets `undefined` on every job.

This is the same drift class `docs/features/apps.md:25` records as fixed for grants-gov, whose
remedy — `tests/result_contract.rs` deriving the emitted shape from a real run and diffing it
against the declaration — the repo has now built **three times** (grants-gov, plugin, crawl).
Research has no `tests/` directory at all.

**Riders** (all one-liners in code the builder is already in):
- `lib.rs:305-306` documents the `research` role as "Sonnet, **normal reasoning**"; `config.rs:1380-1387`
  configures it `effort: "high"` — drift on a cost-relevant knob.
- `resumed` reports the caller's `session_id` param (`:301`, `:467`) on the `Plan::Resume` path where
  that param was **discarded** in favour of the checkpoint's (`:336`).
- `duration_ms` is initialised to `0` each attempt (`:359`) while `steps` / `cost_usd` / `num_turns`
  are restored cumulatively (`:470-473`) — a resumed result mixes two grains with no marker.
- `lib.rs:377-378`'s comment describes only the fresh-run budget path and reads as if the first call
  is always unguarded on resume; it is not.

## Evidence
- `crates/apps/research/src/lib.rs:268-275` — the declaration
- `crates/apps/research/src/lib.rs:463-475` — what `run()` actually builds
- `crates/apps/research/src/lib.rs:461` — `report` is a bare `Value::String` when unstructured
- `crates/server/src/registry.rs:130-154` — `output_shape` published over `/apps` and `/mcp`
- `docs/features/apps.md:25` — the same class, recorded as fixed for grants-gov
- `crates/apps/grants-gov/tests/result_contract.rs` — the instrument, already built here
- `crates/apps/plugin/tests/result_contract.rs`, `crates/apps/crawl/tests/result_contract.rs` — twice more
- `crates/apps/research/` — glob returns exactly `Cargo.toml` + `src/lib.rs`; no `tests/`

## Acceptance criteria
1. `output_shape` matches what `run()` emits, key by key, at the top level — no phantom keys, no
   undeclared keys. Where a key's value is a nested object (`report`), the declaration says so
   honestly rather than hoisting its children.
2. A new `crates/apps/research/tests/result_contract.rs` derives the emitted shape **from a real
   `run()`** (via `TestContext`/`ScriptedResearcher`, as the three sibling contract tests do) and
   diffs it against `output_shape`. It must fail against today's declaration — run it before your
   fix and say so in your report.
3. The contract test covers **both** the structured and unstructured result shapes, since they
   differ, and the skip/resume paths if they emit a different key set.
4. The four riders above are fixed, each with the doc comment or test that keeps it fixed.
5. `docs/features/apps.md`'s research entry matches the corrected contract. **You do not edit that
   file** — see the write-set rule; report the exact replacement text in your final report.

## Risks / non-goals
- **Non-goal:** redesigning the result shape. The contract should describe what the app emits, not
  the other way round — unless a key is plainly vestigial, in which case say so rather than removing
  it unilaterally.
- **Non-goal (do not assume — raise it if you see it):** `stop_reason` has zero programmatic readers
  workspace-wide. That is banked as its own question; declaring it correctly here is enough.
- Risk: directions 1 and 2 in this same lot change the emitted keys. **Sequence this direction
  last** so the contract test pins the final shape, and note in your report if it forced a change to
  either sibling's output.

## Build record
New `crates/apps/research/tests/result_contract.rs` (272 lines) parses the `output_shape` declaration
and diffs it against shapes derived from **real runs**. **Ran before the fix: 3 of 4 failed** —
*"output_shape promises keys the structured run never emits: [key_findings, sources, summary]"* plus the
six undeclared emitted keys. `output_shape` now declares all eleven with `report: {summary, key_findings,
sources}` nested honestly and states in prose that `report` is a bare string when `structured` is false.

Tests cover the structured run, the unstructured run, the restored-finished zero-spend path, and that
every `stop_reason` the declaration lists is producible. **Side effect worth recording: this is the first
use of `TestContext::restored` in this app**, so it also closed the scout's separate finding that the
`Plan::Done`/`Plan::Resume` branches were unexecuted by any test in the workspace.

Riders shipped: `resumed_callers_session()` (a re-claim reports `resumed: false` / `resumed_from_checkpoint:
true`, and the test asserts the engine got the *checkpoint's* session, not the discarded param);
`duration_ms` moved into `RunState` so all four counters share one grain — **additive + `serde(default)`,
deliberately no `STATE_VERSION` bump, because a bump would discard live checkpoints and re-buy their
research**; the role comment corrected to Sonnet @ effort `high`.

**Director review: keep.** Verified the test genuinely derives from `run()` output rather than restating
the declaration.
