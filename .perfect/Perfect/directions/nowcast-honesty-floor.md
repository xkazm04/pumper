---
slug: nowcast-honesty-floor
type: perfect/direction
context: "[[czech-labor-market]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 1fa1e59
---
## What & why
`cz-labour/salary_nowcast` is the context's highest-risk derived product:
`nowcast_median = posted_median / ratio_used` with `ratio_used <= 0` as the ONLY
output-side guard — a garbage ratio surviving the window mints an arbitrarily
implausible "projected median" that persists with full numeric authority. No
catalog contract evaluates it (or salary_gap, or vacancy_lifecycle), a
1-observation row ships looking like data, and anchor staleness is stamped but
never judged. This closes the round-4 note verbatim: "the nowcast is a
projection and needs honesty guards."

## Evidence
- mpsv-vpm/src/lib.rs:1333-1385 compute_salary_nowcast; :1362-1364 the only
  guard; :68-69 SALARY_MIN/MAX exist but unused on output.
- :1290-1298 nowcast_confidence; :2587 pins low-from-1-obs still ships.
- Honesty metadata present (ratio_used, observations, dispersion, confidence,
  ispv_anchor_date, staleness_days, method) — the floor/bounds are what's
  missing, not the labels.
- Catalog: contracts exist only for role_region_agg (:371-393) + wages
  (:411-425); zero rows for the cz-labour trio. Worker contract pass:
  worker.rs:1390-1401. Virtual-app catalog-row precedent: r14 topic_stats.
- Design doc's planned backtest (docs/harness/moonshot-2026-07-30/
  economic-data.md:56) never built — NON-GOAL here (banked separately).

## Acceptance criteria
1. Output-side plausibility bound: a nowcast_median outside a sane band
   (derived from SALARY_MIN..MAX and/or a ratio-vs-anchor band) is suppressed-
   with-count or flagged — builder chooses with reasoning; the garbage-ratio
   case proven by test.
2. Thin-evidence floor: rows below an observation floor are withheld or
   payload-quarantined so a 1-obs row can no longer ship indistinguishable (in
   downstream effect) from a 6-obs one; policy recorded + test.
3. Catalog contracts exist AND demonstrably evaluate for cz-labour/
   salary_nowcast + salary_gap + vacancy_lifecycle: verify which (app, dataset)
   key the worker's contract pass uses for virtual-namespace datasets (r14
   topic_stats is the precedent to copy); a violating run records the violation
   (test). If the seam structurally can't reach virtual-app datasets, report
   precisely — don't hack it.
4. Anchor staleness judged, not just stamped: beyond a threshold (quarterly
   source — builder picks with reasoning) confidence degrades or the row is
   flagged anchor_stale; test.
5. Docs same session: apps.md mpsv-vpm row names salary_nowcast +
   vacancy_lifecycle (currently omitted).

## Risks / non-goals
- NO backtest this round (needs accumulated ISPV releases; banked as the
  context anchor).
- Suppression semantics must not silently tombstone previously-published rows
  the builder didn't intend to remove (no sync_many in play — verify).

## Build record
(to fill during build)
