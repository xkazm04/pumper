---
slug: trigger-gate-honest-across-source-kinds
type: perfect/direction
context: "[[wasm-plugin-examples]]"
lens: robustness
status: accepted
size: S
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## What & why

**The shipped example predicate silently kills every hop on two of the three trigger kinds — and
the ledger records it as a healthy gate decision.**

`trigger-gate` reads `delta.count` with `unwrap_or(0)` (`plugins-src/trigger-gate/src/lib.rs:58`)
against a `min_count` that defaults to **1** (`:54-57`). But `count` exists only in the *dataset*
envelope (`crates/server/src/triggers.rs:116-127`); the `job` envelope (`:145-154`) and the
`external` envelope (`:586-595`) carry no `count` and no `dataset`. So `0 >= 1` is false and the
plugin returns a **well-formed `{"pass": false}`** — forever.

This is the one place in this area where the fail-open doctrine is **inverted**. The plugin does not
fail, so no safety net fires: `predicate_fail_default` never engages, no incident is raised, and the
decision ledger writes `predicate_veto` — which the docs define as *"a predicate that ran and
answered"* (`docs/features/trigger-plugins.md:111, 121-122`), i.e. a healthy gate saying no. The
docs meanwhile present trigger-gate as *the* predicate example (`:13-16`) and state that both hooks
are "attachable to any `source_kind`" (`:4-5`).

The user moment: an operator attaches the shipped, documented example gate to a job trigger, and the
edge is dead permanently while every surface reports it working normally.

## Evidence

- `plugins-src/trigger-gate/src/lib.rs:54-63` — `min_count` default 1; `count` read `unwrap_or(0)`.
- `crates/server/src/triggers.rs:116-127` — the dataset envelope, the only one with `count`.
- `:145-154` (job) and `:586-595` (external) — no `count`, no `dataset`.
- `:162-168` — `predicate_verdict` reads `{"pass": bool}`; a false is a legitimate answer.
- `:173-175`, `:394-398` — `predicate_fail_default` and its `on_error=fire, hop NOT gated` stamp:
  the safety net that cannot engage here because nothing failed.
- `docs/features/trigger-plugins.md:4-5, 13-16, 111, 121-122` — the claims this contradicts.

## Acceptance criteria (for whoever builds this)

1. A delta carrying **no `count`** and no configured `min_count` answers `pass: true` (with a reason
   in the output), not a silent veto.
2. `describe()`'s `params_schema` says what shape this predicate is for; the docs gain the caveat
   that it is a dataset-shaped predicate.
3. A test proves a job-kind and an external-kind envelope are not vetoed — which requires the
   artifact harness from [[shipped-plugins-are-verified]]. **Build them as a pair.**

## Risks / non-goals

- **Non-goal:** changing `predicate_veto`'s ledger semantics. The outcome vocabulary is right; the
  plugin's answer is wrong.
- **Risk:** flipping the default changes behavior for anyone relying on the current veto. Nobody
  can be — a permanently-dead edge is not a configuration.

## Why it was rejected in r22 (history — superseded by the r23 acceptance above)

Real and confirmed by the scout against both sides of the seam — **rejected on the 6-direction cap**,
and structurally: criterion 3 is **unassertable** until
[[shipped-plugins-are-verified]] lands the artifact harness, which is the same
enabler-blocks-the-fix relationship this round finally paid off for the checkpoint seam
([[harness-expresses-the-run]] → [[grants-detail-delta-survives-restart]]).

Banked for r23 as the junior half of that pair. Do not build it without the harness — shipping a fix
whose regression test cannot exist is exactly what r20's gate refused.

## r23 RE-VERIFICATION (Director, 2026-08-14) — CONFIRMED, every link including the crux

Re-scouted against HEAD `caf5e61`; both plugin sources last touched `12401e0`, `triggers.rs`
`10fa27d`. The crux was verified by **reading all three envelope constructors**, not by assuming:

| builder | line | `count`? | `dataset`? |
|---|---|---|---|
| `dataset_trigger_obj` | `triggers.rs:116-127` | **YES** (`:122` `revs.len()`) | **YES** (`:120`) |
| `terminal_trigger_obj` (job) | `:145-154` | **NO** | **NO** |
| `external_trigger_obj` | `:586-595` | **NO** | **NO** |

The job envelope's only numeric content is nested under `result_summary` (`:139-144`), not at the
top level where `delta.get("count")` looks; the external envelope's is entirely under `payload`
(`:592`). Neither is reachable. Adding `params.dataset` makes it **worse** — `dataset_ok` also goes
false since the key is absent.

Confirmed the fail-open net structurally cannot engage here: `predicate_fail_default` is computed at
`:356` but only consulted inside the `if let Some((outcome, why))` block at `:385`, and the
`Some(false)` arm returns at `:369` first.

**New evidence:** the gap is structurally untested — the shipped-plugin test
(`e2e/trigger_plugins.rs:646-676`) exercises `trigger-gate` only against the dataset envelope; its
fixture `delta()` (`:128-139`) hardcodes `source_kind: "dataset"`, `count: 3`, `dataset: "grants"`.

**The sharpest framing, worth putting in the commit message:** the repo already has the *inverse*
test — `triggers.rs:1937-1971`, `a_crashed_predicate_is_not_recorded_as_a_veto`, asserting *"a
crashed sandbox must never read as a gate decision."* r22 worked hard to stop crashes masquerading
as vetoes. This is the remaining case of a **veto masquerading as a decision** — same class of
dishonesty, opposite direction.
