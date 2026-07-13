---
slug: trades-meter-research
type: perfect/direction
context: "[[US Trades Wages, Tax & Valuation]]"
lens: optimization
status: shipped
size: S
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: d83edfd
---

## What & why
All four apps call `ctx.engines.claude.research` directly — real Claude spend invisible to the cost ledger, exempt from `budget_usd`, never research-cached. Migrate to `ctx.research` (the metered seam). Closes the wave-2 follow-up.

## Evidence
- trade-wages:90, homewyse-pricing:95, state-tax:88, valuation-multiples:83
- Metered seam: crates/core/src/app.rs:162-200; follow-up: harness-learnings.md:26

## Acceptance criteria
- [ ] All four apps use `ctx.research`; homewyse's `json_schema` carries over intact.
- [ ] Cost events recorded per run (verify via /jobs/{id}/costs); budget_usd honored.
- [ ] Research cache hit on identical re-run (verify with TTL default).
- [ ] harness-learnings follow-up line updated; docs/features/apps.md metering note.

## Risks / non-goals
- Cache TTL 86400 could serve day-old research on manual re-runs — acceptable for reference data; note `resume_session`/TTL-0 escape hatch.

## Build record
(pending)
