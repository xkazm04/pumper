---
slug: federal-award-amounts
type: perfect/direction
context: "[[grants-unified-layer]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 230a766
---
## What & why
`GET /grants?min_award=…` is fully wired and can never match a single federal record: Grants.gov
Search2 hits carry no amounts, so `normalize_grants_gov` always writes `Null` for
`award_floor`/`award_ceiling`/`total_funding`. Federal is the largest source in the corpus, so the
filter is a live, callable, permanently-empty path for most of it — a user asking for grants over
$500k silently gets a fraction of what exists. M33 (`82f9501`) added a `fetchOpportunity` detail
harvest of full announcement records, which plausibly carries the money.

## Evidence
- Always-null money: `crates/apps/grants-common/src/lib.rs:131-132,143-145`.
- The filter that cannot match: `crates/server/src/routes/query.rs:121-126` (`NumGteAny` over
  `award_ceiling` / `total_funding`).
- The detail harvest that may carry amounts: commit `82f9501` (M33), plus its same-day live
  field-name correction `e81f05c` — evidence this source's field names must be verified, not assumed.

## Acceptance criteria
- **Verify against the LIVE API** whether the detail corpus actually carries award amounts before
  building on the assumption (a previous builder had to correct a wrong field-name claim on this
  exact source — this is a standing hazard here).
- Where amounts exist, they reach `grants/unified` so `min_award` matches federal records.
- Where they genuinely do not exist, the `Null` stays honest — never a fabricated 0 — and the gap is
  documented in the feature doc rather than left for a user to discover.
- The money parser's existing null-honesty and K/M/B handling are reused, not re-implemented.
- Currency stays out of scope: EU amounts remain `Null` (no currency dimension exists in unified).

## Risks / non-goals
Do not inflate coverage by inferring amounts from prose in descriptions. Not a new fetch of the
whole federal corpus — work from what the detail harvest already stores.

## Build record
(pending)
