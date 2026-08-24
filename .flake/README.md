# The flake register

A flaky test is not a state a test is in; it is a **process it goes through** —
detected, labelled, quarantined, fixed, released — with an owner at every step.
Teams that treat flakiness as a condition end up with two populations: tests that
block, and tests everyone ignores. The second one grows.

This directory is the second population, made loud.

## The one rule that is not negotiable

> **An agent never quarantines a test to make a build green.**

That is the build-fixing shortcut in its most respectable disguise. Adding a row
to `register.json` requires a human author, always. If you arrived here because
CI went red, the answer is not a new row.

## Files

| file | versioned | what it is |
| --- | --- | --- |
| `register.json` | yes | the standard: the quarantine register, the exemption table, the ceiling |
| `history/runs/*.json` | no | one record per recorded cargo-test run — the detection signal |
| `history/register-size.jsonl` | no | one sample per `flake:check` run, for the size **trend** |
| `history/labels.json` | no | the currently-labelled set, for CI's job summary |

Everything under `history/` is evidence, not standard. It accumulates in CI
through `actions/cache` with a rolling key, and locally through `just lanes` /
`just test-recorded`. A lost cache reads as *no history*, which the report spells
differently from *no flakes* — see "Three outcomes" below.

## The stable test identity

Detection is a query over run history, and history keyed by an unstable name
resets every time somebody tidies a file. libtest prints only the module path
(`governor::tests::foo`), which is **not unique in a workspace**: the same path
exists in the lib target and in every integration binary that declares it. Keying
on that alone silently merges unrelated tests' histories, which is worse than
having none.

    <package>::<target>::<module path>::<fn>

    pumper-core::lib::governor::tests::distinct_hosts_run_in_parallel_but_each_host_spaces
    pumper-core::test:datasets_bulk_perf::bulk_upsert_50k_cost_report
    pumper-server::bin:pumper::e2e::trigger_plugins::shipped_delta_slim_slims_the_envelope_without_losing_lineage

`<target>` is cargo's own compilation unit (`lib`, `test:<name>`, `bin:<name>`,
`bench:<name>`, `doctest`), because that is exactly the scope within which the
module path is unique. `pumper-server` is binary-only, so its whole e2e suite is
`bin:pumper` — calling it `lib` would make every server id wrong, and wrong ids
are invisible: they simply never match anything.

**Survives:** reordering; adding or removing sibling tests; moving a test between
files that resolve to the same module path; renaming the file behind `mod x`;
running on another machine or OS.

**Deliberately does not survive:** renaming the test function, renaming the
package, moving a test between targets. Each of those is a redeclaration of what
is being asserted, and carrying the old history forward would attribute one
test's flakiness to another. The register catches the fallout instead: an entry
whose id no longer resolves is an `orphan` finding, which forces a human to
re-point it rather than letting the history quietly rot.

## Detection: transitions on the same code, never a failure rate

The usable signal is **how often a test's outcome changed between consecutive
runs of the same commit**, over a window. A raw failure rate is the wrong
instrument: a consistently failing test is *broken*, not flaky, and the two need
opposite responses. Same-code is load-bearing — outcomes compared across
different trees measure the product's churn, not the test's stability.

Every figure travels with its predicate. "12% flaky" is not a finding;
"changed outcome in 12 of 100 same-commit run pairs on `master` over 14 days" is,
because it exposes the case of nine runs where the percentage means nothing.

## Labelling is not quarantining

A label is applied by the system, printed **where the test appears** (the run's
own output, via `scripts/ci/flake-record.mjs`), and removed automatically when
the history stops supporting it. There is no stored label to forget to remove —
the label *is* the query, so a test stable for the whole window stops being
described as flaky on the very next run. That reversal is the half everyone
forgets, and its absence is why registers only ever grow.

**A labelled test still blocks.** The label is information; the register is the
decision.

## Quarantine rows

Every row carries an **owner (a named person, never a team)**, an **entry date**,
an **expiry**, a **suspected cause** (`test` | `harness` | `product` — the third
is a product defect wearing a test's clothing and escalates immediately), a
**form**, and a link to the **failure evidence**. On expiry the row escalates; it
is never silently extended.

`muted` (still runs, result recorded, does not block) is preferred over
`skipped`, because a muted test keeps producing the history that will eventually
diagnose it. **libtest has no muted form** — `#[ignore]` is a skip — so both of
this repo's rows are `skipped` with a stated `formReason`, and the compensating
control is the nightly `long-lanes` CI leg, which runs the whole `--ignored` set
through the recorder. The data a mute would have produced accumulates anyway,
off the blocking path.

## The exemption table, and why it is checked both ways

This repo has 19 `#[ignore]`d tests and only **two** are flakes. The rest are
environment-gated: they need real Chrome, a built `.wasm`, live network, or a
50k-record perf corpus. Those are not flakes and must not be laundered into the
register — but they also cannot be left undeclared, or a new timing flake hides
among them.

So every `#[ignore]` in `crates/**` must appear in **exactly one** of
`quarantine` or `exempt`, and both directions are gated:

- an `#[ignore]` in neither table → `undeclared`
- a flake-reasoned `#[ignore]` with no quarantine row → `unregistered-flake`
- an `exempt` row whose own source reason says "flaky"/"timing" → `laundered`
- a row of either table naming a test the tree no longer has → `orphan`

Same shape as `crates/core/tests/fetch_chokepoint.rs`'s
`EXPECTED_RAW_ENGINE_CALLS`, and for the same reason: a reviewed inventory that
fails in both directions is the only kind that stays true.

## Ceiling

`ceiling` is a **stop-the-line** threshold, not a warning. Without one the
register absorbs every hard problem and the suite quietly stops certifying
anything. Published beside it, every run: the register's **size with its trend**
(a register growing monotonically is deletion with a slower fuse) and the **age
of the oldest entry** (more diagnostic than size — 40 entries none older than a
fortnight is a working process; 6 with one 14 months old is a broken one).

## Retries: none, on purpose

There are no automatic retries in this repo's test path, and none are being
added. A retry that hides the first failure destroys the detection signal this
whole design runs on; a retry that *records* the first failure preserves it — but
that machinery only earns its keep once there is history to protect, and there is
none yet. If retries are ever added, the first failure must be recorded and the
retry rate published with its predicate, or they are masking rather than
measuring.

## Why a wrapper and not cargo-nextest

nextest gives per-test JSON for free and was the first option considered. It was
rejected here: the `test` job is a branch-protection required check, and nextest
does not run doctests at all and runs every test in its own process. Against a
2039-test baseline that changes **what is run** on the rung the branch is
protected by — a coverage cut dressed as a tooling upgrade. The wrapper changes
nothing about what runs; it only reads the output, and it forwards cargo's exit
code byte for byte with every recording step wrapped so an instrument failure can
never become a verdict.

## Three outcomes

`just flake-check` exits **0** (checked, register honest), **2** (checked,
findings), or **3** (**could not check** — unreadable register, no ceiling, or a
source scan that found zero `#[ignore]`s in a tree that has nineteen). A 3 is not
a pass: nothing was verified. Same discipline as
`scripts/docs/check-doc-sync.mjs`, and for the same reason.

## Commands

    just flake-check          # the gate
    just flake-report         # register size + trend + oldest entry + labels
    just test-recorded        # cargo test --workspace, recorded into history
