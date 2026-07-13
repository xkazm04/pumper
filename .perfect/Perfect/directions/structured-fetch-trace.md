---
slug: structured-fetch-trace
type: perfect/direction
context: "[[Tiered Fetcher & Politeness]]"
lens: api-ux
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: a2bcee2
---

## What & why
The escalation trail is free-text `Vec<String>`; pumper's own tier router string-matches `"http tier"` to detect losses, and consumers can't branch on why a fetch escalated, whether the cache hit, or what each tier cost. Replace with a typed `FetchTrace` (per-tier: tier, verdict/reason enum, http status, content chars, cache_hit, latency_ms, cost_usd) surfaced in `FetchOutcome`, cost-event detail, and job results.

## Evidence
- Free-text trail: crates/core/src/fetcher.rs:126-133
- Fragile string-match router coupling: crates/core/src/app.rs:133
- Cache hits debug!-only, invisible to callers: crates/engine-http/src/lib.rs:139
- Cost only for claude tier: fetcher.rs:87-88

## Acceptance criteria
- [ ] Typed trace struct (serde) with per-tier entries; reason is an enum, not a string.
- [ ] Tier router keys on the enum, not string prefixes.
- [ ] cache_hit and latency_ms populated for the http tier; cost_usd per tier where known.
- [ ] Human-readable trail preserved as a rendered view (backward-compatible cost-event detail).
- [ ] docs/features/fetching.md documents the trace shape.

## Risks / non-goals
- Risk: FetchOutcome shape change ripples to apps reading `escalations` — keep the old field populated.

## Build record
(pending)
