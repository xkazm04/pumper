# Long lanes

Long lanes — the perf harnesses and the artifact suites — answer questions only
time and pressure can ask: does per-row cost grow as a table fills, does a bulk
sync starve another app's writer, does a shipped `.wasm` still export the host
ABI. They are **certifications, not gates**: they run on their own clock, judge
statistically, and their unit of value is the trend across runs rather than the
verdict of one.

Before this directory existed, `crates/core/tests/datasets_bulk_perf.rs` measured
write-lock hold time as another app experiences it — a genuinely good
measurement — and then **printed** it. No percentile, no ceiling, no schedule, no
artifact, no trend. A harness that asserts nothing can only ever report that it
ran.

## Files

| file | versioned | what it is |
| --- | --- | --- |
| `criteria.json` | yes | the standard: every lane, its command, where it can run, and its pre-declared bounds |
| `runs/*.json` | no | per-run measurement artifacts, emitted by the harness itself |
| `verdicts/*.json` | no | measurement + criteria + verdict together, one file per certification |
| `health.json` | no | the lane-health ledger: first-green, and each lane's pass-rate history |

## Measurement here, judgement there

    crates/core/tests/lane_artifact/mod.rs  ->  .lanes/runs/<lane>.json   (measure + emit)
    scripts/ci/lane-certify.mjs             ->  verdict                    (judge)

The Rust harnesses are not allowed to decide pass or fail. That split is what
makes *declared before, judged after* enforceable: a bound cannot be quietly
relaxed in the same commit that broke it, and any run's verdict is reproducible
by anyone holding the artifact and the criteria — which is why each verdict file
carries both.

Percentiles and slopes are computed by the certifier, not the harness, so raw
series travel to the artifact and a past run can be re-judged under a changed
bound without re-running an hours-long lane.

## Criteria are pre-declared, statistical, and carry their predicate

- **Percentiles, never averages.** An average hides exactly the tail the lane
  exists to see. Nearest-rank, so the bound is a value the run actually observed.
- **Ceilings with trend, not just endpoint.** Memory (or per-chunk cost) under X
  at the finish is compatible with linear growth that clears X an hour later, so
  the criterion is the **slope over the run's second half** — which is what
  distinguishes warm-up from a leak.
- **Ratios where possible.** A ratio between two halves measured in the same run
  on the same input certifies the *code*; a millisecond count certifies the
  *runner*. `derived-backfill` and `fingerprint-shared-dom` are pure ratio lanes
  and need no baseline at all.
- **Every number carries its predicate** — the window, the workload and the
  population it was measured over. A bound without one is not a finding.
- **Absolute bounds name their `basis`**: the recorded baseline, the build it
  came from, and the multiple applied. Re-baselining is its own commit with its
  own reason.

Criteria adjusted while looking at a result are commentary, not criteria.

## Load reality

A lane certifies only the traffic it generates. Every artifact carries a
`workload` block naming the corpus size, the record shape, the concurrency — and
a `shape_fidelity` field that says plainly whether the shape is REAL,
DECLARED-APPROXIMATE, or EXACT-for-the-comparison. "Holds at N" is meaningless
without the workload's description travelling beside it, and an honest lane
certifying a declared-approximate workload beats a confident lane certifying an
unexamined one.

## Four verdicts, because three are not enough

| verdict | meaning |
| --- | --- |
| `pass` | every criterion met, on this runner, for this workload |
| `fail` | a declared bound was breached |
| `cannot-see` | the lane should have run here and produced no artifact, or the artifact lacks a metric a criterion names. **Not a pass.** |
| `cannot-run` | declared unavailable on this runner (no Chrome, no live network). Its own category — never counted as a pass, never silently dropped from the report. |

`browser-render` and `archive-wayback` are permanently `cannot-run`: they need a
local browser or live `web.archive.org`, and a lane that reddens because a third
party is down reports nothing about this repo. They stay listed so the gap stays
counted rather than forgotten.

Exit codes: **2** if any lane failed, **3** if any lane could not be seen (or the
criteria file is unreadable), **0** otherwise.

## Lane health: earned green, planted red, and never green

A lane certifies nothing until it has been green on a known-good build **and red
on a known-bad one**. The second is the one nobody schedules —
`scripts/ci/lane-certify.test.mjs` plants a breach of every bound kind and
asserts the certifier fires.

The deadlier inverse hides in plain sight: **a lane that has never passed**. If
red is normal there, every failure after the first is wallpaper. So `first-green`
is an explicit tracked event and the report calls out three distinct states:

- `NO RUNS RECORDED` — unobserved. Not "never green".
- `never attempted here` — recorded, but every run was `cannot-run`.
- `NEVER GREEN` — attempted and never passed. A lane at a 100% historical failure
  rate is not flaky; it is an unbuilt lane wearing a gate's clothes, and the
  finding it reports is about the harness, not the product.

`health.json` lives in CI's cache rather than in the repo, so an evicted cache
reads as `NO RUNS RECORDED` — unobserved, which is the honest reading — and never
as a green.

## Schedule

The lanes run on a **nightly cron** (`41 5 * * *`) and on `workflow_dispatch`,
never on a per-change gate: blocking a merge on a long run destroys the merge
cadence without improving the certification, because the property being certified
is not a property of any single change.

The workflow already carried a weekly cron for the advisory database. Adding a
second one would have made every existing job run nightly too, so each cron is
routed to the jobs it is for with `github.event.schedule` — the nightly one
reaches only `long-lanes`.

The two artifact lanes also keep running in the Linux `test` job exactly as
before. The certification adds the schedule and the health record; it removes no
existing coverage.

## Flake discipline

Long lanes breed flakes, and they share the register in `.flake/` — there is not
a second one. `just lanes` runs every lane through
`scripts/ci/flake-record.mjs`, which is also what finally gives the `--ignored`
set a run history.

## Commands

    just lanes           # run every runnable lane, then certify
    just lane-certify    # judge whatever artifacts are in runs/
    just lane-health     # each lane's pass-rate history
