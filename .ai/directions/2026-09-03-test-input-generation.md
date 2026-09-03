---
subject: software-engineering/test-input-generation
project: pumper
raised_by: intake intake-boa-0903
source: librarian/sources/2026-09-03-boa.md
stage: the trigger hook pipeline in crates/server/src/triggers.rs (predicate -> transform -> record) and the only harness that drives it through the real wasm host, crates/server/src/e2e/trigger_plugins.rs
size: 2 files / ~150 lines / S
status: proposed
---

## Why the scope implies it

`scope.does` names *"pluggable scrape and fetch engines"* and the plugin host runs
*"untrusted, hot-swappable code"* (`crates/engine-wasm/src/lib.rs:1-6`) through a
three-stage pipeline: a predicate plugin decides whether a hop fires, a transform plugin
reshapes the envelope, and the host records the result with provenance re-stamped
(`crates/server/src/triggers.rs:177-200`). Every input to that pipeline comes from
outside the tree - a fetched document, a trigger delta, an operator's plugin - and the
tree already tests it the way a pipeline is usually tested: end to end, through
`trigger_plugins.rs`, with inline `wat` fixtures compiled by the host.

The force the source showed is masking by stage. A crash in an early stage hides every
defect in the stages behind it on that input, so an end-to-end harness reports one
shallow finding where three were available. pumper has already met this once and paid
for it: `triggers.rs:302-306` records that a predicate whose module never loaded
"takes the same fail-open path as a predicate that passed, so a gate nobody deployed is
indistinguishable from a gate that said yes", and `missing_hook_plugins` (`:309-319`)
was added to name the unrunnable stage at error level. That fix is the technique's first
rule applied by hand to one stage; the direction is to apply it to the harness.

## What the first context contains

Two generated-input targets beside the existing e2e file, one per stage whose input
type differs from its predecessor's, triaged in pipeline order:

- **A predicate target.** Generates `_trigger` delta objects (the shapes
  `external_trigger_obj` and `fire_dataset_triggers` already build) and predicate
  outputs across the verdict grammar `predicate_verdict` accepts (`triggers.rs:179-188`:
  bare booleans, `{"pass": bool}`, and everything else), and asserts the stage's own
  oracle: a verdict is produced, or the fail-open default fires *and is recorded* as an
  incident (`HookVerdict.incidents`, `:337-364`). Its crash set is drained first.
- **A transform target.** Fed only envelopes the predicate stage accepted, generates
  transform outputs across the JSON space, and asserts the transform stage's oracle:
  output parses as JSON, host-owned keys are re-stamped from the original
  (`:197-200`), and malformed output lands as `MalformedOutput`, never as a record.
- Both run the real host under its fuel budget, so a plugin that loops is a trap and a
  finding, not a hang; time-over-budget is logged as its own class.

It must NOT absorb: fuzzing the plugin binaries themselves (they are opaque; the plugin
stage's oracle stays crash-and-budget); the fetch tier (a different pipeline with its
own governor); or the existing e2e cases, which remain the end-to-end contract and are
not replaced.

## The measurable

**Distinct transform-stage failures reachable on inputs the predicate stage rejects or
traps on: today 0 by construction (no harness can reach them), target > 0 or a
demonstrated 0.** Measured by running the transform target alone over the set of inputs
the predicate target trapped or vetoed on, and counting distinct `PluginFailure`
classes and distinct malformed-output shapes. A second number: the discard fraction of
the predicate target's generator (inputs the stage rejected as malformed), reported
beside the run - a rising fraction after a generator change is a generator that got
worse while the suite stayed green.

## What would make this wrong

**If no transform defect exists behind a predicate failure in the e2e corpus**, the
extra target finds nothing and the harness is a cost with no finding; the honest verdict
is then `deferred` with "one transform defect found by any other means" as the return
condition. **If the stages cannot fail independently** - if every predicate failure
already implies the transform never runs by contract, and that contract is itself
tested - there is no masking to remove and the middle target is dead weight. **If the
generated envelopes cannot reach the transform's real inputs** (the `_trigger` object's
shape is richer than a generator can build without the crawl archive behind it), the
target tests the validator's reject path and reports green, which is the failure the
registry subject warns about first; the check is the discard fraction above, and a
fraction near one means stop.
