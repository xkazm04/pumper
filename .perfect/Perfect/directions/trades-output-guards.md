---
slug: trades-output-guards
type: perfect/direction
context: "[[US Trades Wages, Tax & Valuation]]"
lens: robustness
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: d95ba60
---

## What & why
Only homewyse schema-locks output + salvages fenced JSON; trade-wages/state-tax/valuation do blind `output.json.ok_or_else` — a prose-wrapped answer fails the whole paid run (~1/3 of runs historically). Nothing validates values: negative wages, 90% tax rates, median outside [low,high] upsert silently; state-tax accepts 20/52 states. Add json_schema + salvage to all, plus range/monotonicity checks and completeness reporting.

## Evidence
- Guard only in homewyse: :94, :178-211, salvage+tests :217-314
- Blind parses: trade-wages:98, state-tax:96, valuation-multiples:91
- Weak floor: state-tax:136; docs flag: docs/features/apps.md:36

## Acceptance criteria
- [ ] json_schema on all four research requests; salvage_json shared (lift to a common helper, e.g. trades-common or core).
- [ ] Plausibility checks: monotone low≤median≤high, rates ∈ [0,100], positive wages/employment; violations rejected with per-record detail in the job result, valid records still upserted.
- [ ] state-tax completeness: expected 50+DC set enumerated in code; missing states listed in result.
- [ ] Unit tests per validator; docs/features/apps.md guard note updated.

## Risks / non-goals
- Non-goal: retry-on-invalid loops (one salvage pass only — cost discipline).

## Build record
(pending)
