# Perf-Feature Scan — Wave I: Domain data model (Theme I)

> 3 commits, **3 High findings** closed (trades #2 phase c deferred — needs new
> research). Highest product leverage: data already fetched/parsed, just discarded
> or under-keyed.
> Baseline preserved: build clean, tests **261 → 266** (+5 tests, 0 regressions).
> Branch `vibeman/wave-i-domain-2026-07-17` (off master after PR #11).

## Commits

| # | Commit | Finding | What |
|---|--------|---------|------|
| 1 | `b563dc7` | czech-mpsv #3 | derive skill-demand + education aggregates from postings already parsed |
| 2 | `205b134` | census #1 | persist saturation + join a per-capita base onto the blend |
| 3 | `1afe7b2` | trades #2 (a+b) | real per-state dimension on operator_economics + fix pricing contamination |

## What was fixed

1. **Parsed-but-dropped labour dimensions (czech-mpsv #3).** `mpsv-vpm` already
   deserialized `pozadovanaDovednost` (skills) and `minPozadovaneVzdelani`
   (education) off **every** posting but read them only inside `as_sample` (≤4 kept
   per group) and dropped the rest. Two new accumulators in the existing loop (zero
   extra fetch/parse) now emit `skill_demand` (unit-group × skill: count, share of
   group, salary distribution) and `education_agg` (unit-group × education: median +
   an honest median-vs-median premium, never a fabricated delta). Codebook ids kept
   opaque. Once persisted, `role_trends`' revision technique composes onto these for
   rising/fading skills next run.

2. **The headline metric was unqueryable (census #1).** "Rank by saturation
   (establishments per 10k), not absolute size" is the app's stated purpose, but
   `per_10k` existed only in one job's result JSON, capped at 60 rows — invisible to
   change-detection, triggers, search, exports. Now the **full** ranking persists as
   a durable `census/saturation` dataset (place, base, denominator_kind, per_10k,
   ACS vintage), and `blend_market` joins the persisted base to emit `base`,
   `denominator_kind`, and `total_market_per_10k` per naics4×state cell — the
   per-capita number the launch ranking actually wants, which didn't exist before.
   The blend reads the base (it does no ACS fetch — `census-nonemp` calls the same
   path); graceful nulls when no base is known.

3. **National-only economics with a corrupting half-built axis (trades #2, a+b).**
   The unified `operator_economics` row was `US:{trade}` with the tax set-aside
   resolved to the **median** rate across 51 jurisdictions — a Texan (0%) and a
   Californian (13.3%) got the same wrong middle number. And `summarize_pricing`
   filtered on trade alone, so two priced localities were silently averaged into one
   envelope. Fixed (pure code, no new research): `summarize_pricing` now filters on
   `locality` (refactored to `&[&Value]`, unit-tested; read cap 1000 → 50000); and
   the join emits a `US:{trade}` roll-up **plus** a `{ST}:{trade}` row per state
   carrying that state's **real** top-marginal rate (`state_tax_context`, sharing
   `federal_summary`). Wage/valuation stay the national roll-up on state rows
   (`wage_grain: "national"`); pricing is per-locality (null until priced).

## Deferred — trades #2 phase (c)

Per-state OEWS wages. Needs **new metered Claude research** (batch several states per
call, reuse `Trade::soc_code()`) and live verification of the research output — I
can't spend/verify that here. `wage_grain: "national"` on the state rows marks the
seam; when phase (c) lands it flips to `state` for the states researched. Valuation
stays national by design (per-state broker comps are too thin to be honest at that
grain — the finding says so).

## Gate

```
cargo build --workspace   # clean
cargo test --workspace    # 266 passed / 0 failed  (was 261)
```

## Open Highs after this wave

7 of the original 36 remain: engine-traits #2 (deferred, architectural), theme F
caching (3), theme C tail (2), trades #2 phase c (deferred), and E grants-gov #1
(deferred). Themes A/B/D/G/H/I/K now have zero *actionable* open Highs (I's
remainder is the research-gated phase c).
