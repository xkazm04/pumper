---
slug: detector-false-positive-fixes
type: perfect/direction
context: "[[source-resilience]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 6bbaf7d
---
## What & why
Three real defects in a detector that is about to gate writes:
(a) `robust_z`'s never-varying-baseline fallback returns ±∞ for ANY departure past tolerance — a
perfectly stable templated field (the common case) trips full shape-drift on one legitimate extra
digit or an added currency symbol.
(b) The conclusive-rebind override forces the score to 1.0, and its only escape hatch is a
`ContentChanged` divergence verdict — which cannot be produced when there is no prior fingerprint to
diff against. A brand-new source is therefore maximally exposed to precisely the false positive the
guard was written to prevent.
(c) `Diagnosis::Ambiguous` is reachable and has zero test coverage.

## Evidence
- (a) `crates/core/src/resilience/sketch.rs:371-378`; consumed by `score_shape` `detect.rs:525-567`.
- (b) override `detect.rs:299-314,375-395`; `ContentChanged` requires a drift cell `detect.rs:638`;
  with no fingerprint history `explain_divergence` returns `None` `detect.rs:624`, and drift comes
  from `cohort_drift` `store.rs:769-807`.
- (c) the fallthrough `detect.rs:641`; no test in `detect.rs:1136-1192` exercises it.
- Also thin: `score_missrate`'s multi-field worst-wins branch `detect.rs:450-452` is only ever
  exercised with a single field.

## Acceptance criteria
- Each of the three has a test named after the failure it prevents (`x_not_y` style).
- A first-run source (no fingerprint history) cannot be scored 1.0 by an override whose guard is
  structurally unavailable to it.
- Never-varying baselines get a defensible floor rather than infinity, justified in a comment.
- Fixes are extracted named predicates, not inline patches (repo doctrine).
- Every existing detector test stays green — they encode intended behavior and are the regression net.

## Risks / non-goals
Do not weaken true-positive detection to buy false-positive relief: total-collapse and
distinctness-collapse must still fire on the cases their current tests cover.

## Build record
Shipped: `sketch::zero_scale_z(delta, tol)` replaces the infinite z-score (tolerance becomes the
scale, saturating at `ZERO_SCALE_Z_CAP = 25`, well above the 3.5 default `mad_z`);
`content_change_ruled_out(cfg, drift, s_shape)` requires value-domain corroboration when there is no
divergence evidence at all; and the two missing tests
(`everything_moving_at_once_is_ambiguous_not_a_redesign`,
`miss_rate_scores_the_worst_field_not_the_last_one`). No weight and no threshold was touched.

**The builder refuted two of the three premises I wrote**, and fixed the real defects underneath:
- My claim that a *brand-new* source can be scored 1.0 was WRONG — `conclusive_rebind` requires
  `baseline.distributional()`, i.e. 3+ prior `ok` runs. The genuinely exposed population is
  **key-rotating listings** ("the 30 newest items"), where `cohort_drift` returns `None` on every run
  forever, so the escape hatch is permanently unreachable. That is the population it fixed.
- My infinite-z claim was overstated for `score_shape` (which has a second `tv < SHAPE_TOL` guard and
  carries only 0.15 weight) but was real in `score_distinctness`, where `RATIO_TOL = 0.02` meant
  three duplicate values in thirty fired the term. That is what the fix actually closes.
- It also found `explain_divergence` returning `None` conflates TWO conditions — "no drift evidence"
  and "the output held still" — and that conflation is the underlying bug.

**Director-accepted risk:** this narrows one true positive. A key-rotating source whose selector
rebinds onto a SAME-SHAPED template value is no longer auto-convicted (scores ~0.19, still recorded
with diagnosis `silent_rebind`). Accepted because a rebind onto a template element normally moves the
length/char-class profile too, and the module already documents same-shaped swaps as fundamentally
undetectable. Recorded here so a future round can revisit it with real FPR/TPR data — which does not
exist yet: there is no mutation harness, so `ZERO_SCALE_Z_CAP = 25` and the `s_shape > 0`
corroboration are reasoned choices measured only against the existing test corpus.
