---
slug: host-profiles-api
type: perfect/direction
context: "[[Tiered Fetcher & Politeness]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 6fad704
---

## What & why
Tier memory is a blunt 3-strikes-pin-to-browser with no aging (a host that failed 3× a month ago stays pinned until a lucky http win), the governor's learned penalties evaporate on restart, and none of this learned state is visible. Extend `tier_memory` into a per-host profile (preferred tier with strike decay, persisted learned interval, last outcomes) and expose `GET /hosts` + `DELETE /hosts/{host}/memory` so operators can inspect and reset what pumper has learned. Ships the practical half of the backlog's "fleet rate governor that learns host limits from 429s" (idea f50bc37e).

## Evidence
- No strike decay, browser-only pinning: crates/core/src/tiers.rs:40-72, STRIKE_LIMIT tiers.rs:16
- Governor penalties in-memory only: crates/core/src/governor.rs:27-30, 98-131
- No visibility endpoint: crates/server/src/routes.rs router table (no /hosts)

## Acceptance criteria
- [ ] Strike decay/aging (time-based) in tier memory; migration appends to crates/core/migrations/.
- [ ] Governor's learned per-host penalty persisted and restored on boot (write-behind is fine).
- [ ] `GET /hosts` (cursor keyset pagination per repo convention) + `GET /hosts/{host}` + `DELETE /hosts/{host}/memory`.
- [ ] docs/features/fetching.md + http-api.md updated.

## Risks / non-goals
- Non-goal: predictive/learned RPS ceilings beyond the existing penalty model; cross-node federation.

## Build record
(pending)
