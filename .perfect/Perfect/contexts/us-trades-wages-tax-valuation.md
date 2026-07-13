---
name: "US Trades Wages, Tax & Valuation"
type: perfect/context
group: "Economic & Labor Market Data Apps"
category: lib
opportunity: 8
last_proposed: 2026-07-13
cooldown_until: after-round-3
directions: ["[[trades-common-unified]]", "[[trades-meter-research]]", "[[trades-output-guards]]"]
---

## Current state (scout brief digest, 2026-07-13)

- Four `ScrapeApp` crates, each one Claude-research call → national reference dataset: `wages` (5 trades, key `US:<trade>`), `pricing` (per trade×job, key `<locality>:<trade>:<job>`), `tax` (federal + ~52 state records), `valuation` (SDE/revenue multiples per trade). No `schedule()` on any — manual-trigger only, year params pinned (2024/2025).
- **All four UNMETERED**: `ctx.engines.claude.research` direct calls (trade-wages:90, homewyse:95, state-tax:88, valuation:83) — spend unattributed, unbudgeted, uncached. Known follow-up (harness-learnings:26). Fix is mechanical → `ctx.research` (json_schema field carries over).
- **No provenance**: sources named only in prompts; no citation/url/confidence field in any schema (trade-wages hardcodes label `source="BLS OEWS (agentic)"`:121). Transcript saved as artifact only.
- **Validation**: only homewyse has json_schema lock + salvage_json fallback (:94, :178-211, :217-314); other three do blind `output.json.ok_or_else` — a prose-wrapped answer fails the whole paid run. No plausibility checks anywhere (low≤median≤high, rate ranges); state-tax accepts ≥20 of 52 states as success (:136).
- **No shared taxonomy**: 5-trade list re-typed in three prompt strings; keys embed model-echoed labels → phrasing drift creates duplicate keys, defeating change detection. No geo join between pricing `locality` and tax state codes.
- **No cross-dataset consumer**: generic dataset API only; digital-twin / exit-readiness backlog ideas are far (no join layer, no scoring code). Contrast grants-common (unified layer, wave 6).

## Direction history
- 2026-07-13: 5 proposed, 3 accepted (unified layer, metering, output guards). **REJECTED**: provenance/confidence fields (api-ux), exit-readiness endpoint (wildcard) — user passed on the consumer-facing composition/trust directions and kept the engine/data-correctness ones. Signal: prefer substrate quality over new product surfaces in this context, for now.

## Shipped
- [[trades-meter-research]] → d83edfd — all four apps metered (cost events, budget, research cache)
- [[trades-output-guards]] → d95ba60 — json_schema + shared salvage + plausibility/completeness validation
- [[trades-common-unified]] → a458c2a — canonical taxonomy + trades/operator_economics join (build record: builder scoped taxonomy to the 3 trade-keyed apps; state-tax joins via tax context only — Director ratified. Historical raw-label-keyed rows orphaned by design; next run repopulates.)
