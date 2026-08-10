---
slug: search-surface-parity
type: perfect/direction
context: "[[search-engine]]"
lens: feature
status: shipped
size: S
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 576a3d7
---

## What & why
The M14 entity filters (amount_gte/lte, date_after/before) and M13 saved-search
materialization are entirely absent from docs/features/search.md — shipped, OpenAPI-
documented, officially nonexistent. The MCP `search` tool exposes only q/limit/app/dataset —
no fuzzy, sort, since, or entity filters — so agent consumers can't reach what HTTP has.

## Evidence
- docs/features/search.md — zero grep hits for amount/event_date/materialize
- mcp/mod.rs:172-176, 361-393 (tool params subset, facets:false)
- routes/search.rs:15-43 (the real param surface)

## Acceptance criteria
- search.md documents the real surface: entity filters, sort/fuzzy/since/offset, saved-
  search materialize + max_materialize_results, corrected recovery claims (coordinates with
  [[search-lifecycle-safety]]).
- MCP search tool gains parity params (fuzzy, sort, since, amount/date ranges) with tests;
  param mapping shared with the HTTP route where practical (no second grammar).
- Known-gaps section honest (ghost docs until [[search-ghost-doc-gc]] lands, USD-only
  amounts).

## Risks / non-goals
- Non-goal: new query capabilities; parity + documentation only.

## Build record
- Builder SE2 (opus), wave 2, verdict merge (pick pending DH2 gate). `862659f`: extracted
  `build_search_request(SearchInput)` — HTTP route and MCP tool share ONE set of defaults/
  clamps/sort vocabulary (no second grammar); MCP gains fuzzy/sort/since/offset/entity
  ranges with SEARCH_MAX_OFFSET advertised in the schema; EXPECTED-diff param inventory
  test + recording-Search-fake test proving every arg reaches the query. search.md: MCP
  parity + materialize + honest gaps (USD-only, doc-level max, unreclaimed quarantines).
- Gates: worktree 1144/0/17.
