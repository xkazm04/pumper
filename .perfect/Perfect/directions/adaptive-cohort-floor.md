---
slug: adaptive-cohort-floor
type: perfect/direction
context: "[[source-resilience]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 107dc6b
---
## What & why
`min_cohort_docs = 30` is a fleet-wide constant. A source whose listing is legitimately smaller is
NEVER statistically judged — every distribution-based test is skipped and the run is recorded `Ok`
with `statistical_coverage: false`. But `baselines()` gates on the VERDICT, not on coverage, so those
unjudged sketches keep feeding the source's own baseline window forever: a self-referential history
that can never catch the silent rebind the detector exists to catch. Only `total_collapse` (5 docs)
backstops it. Worse, the operator surface reports such a source as healthy when it is really
unwatched.

## Evidence
- Fleet-wide floor: `crates/core/src/config.rs:371`; skip path `crates/core/src/resilience/detect.rs:269-285`.
- Verdict-only baseline gate: `crates/core/src/resilience/mod.rs:195-197`, `store.rs:213-214,746`.
- The 5-doc backstop: `detect.rs:56,342-359` (`COLLAPSE_MIN_DOCS`, `COLLAPSE_BASE_RATE`).
- Operator read that cannot distinguish the two: `crates/server/src/routes/health.rs:61-70,88-153`.

## Acceptance criteria
- Cohort adequacy is decided per source, not by one global constant.
- A run that could not be judged no longer silently enters the baseline window.
- `GET /sources` distinguishes **healthy** from **unmonitored / insufficient-evidence** — the status
  a thin source really has.
- Test: a chronically-thin source cannot accrue a baseline that makes it look monitored.
- Sources already clearing the floor keep identical behavior (existing detector tests stay green).

## Risks / non-goals
Not a loosening of detection — a thin source must not become easier to trip. Do not change the five
score weights or the trip thresholds.

## Build record
**The builder deliberately reinterpreted this direction, and was right to.** It argued an adaptive
NUMERIC floor is unimplementable under the direction's own constraints — lowering it for thin sources
makes them triable (violates the non-goal); raising it changes behavior for sources that already
clear it (violates AC5). It shipped per-source *adequacy classification* instead: `cohort_adequacy()`
returns full / shrunken (below floor but this source HAS cleared it before) / chronic (never cleared
it in the retained window), with `Baseline::peak_docs()` as the per-source evidence.

New `RunVerdict::BelowCohort` is neither judged nor baselining, so the existing `verdict = 'ok'`
filters exclude it for free — the self-referential-baseline hole closes without new filter logic.
`SourceHealth.monitored` is derived, not cached, plus an `unmonitored` count on `GET /sources`.
Test: five 6-doc runs leave `baseline.runs("price") == 0` and `monitored == false`; one 10-doc run
flips both.

Director review: the reinterpretation preserves the direction's actual value (unjudged runs stop
padding the baseline; the operator can see what is unwatched), and the builder flagged it as a
deliberate choice rather than quietly delivering something narrower. Accepted. Merged ff.
Open item it flagged: thin runs already recorded `ok` stay baseline material until retention ages
them out — there is no backfill.
