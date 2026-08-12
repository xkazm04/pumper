---
slug: search-degraded-honesty
type: perfect/direction
context: "[[job-search-api]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 63db76f
---

## What & why
`GET /search` (and the MCP `search` tool through the same parser) answers
`200 {total: 0, hits: []}` on a DISABLED or WIPED index — indistinguishable from "no
matches". The wiped-index trap is documented in MEMORY.md as invariant #4 (schema drift
rebuilds the index EMPTY and the delta-driven refill only rolls forward; queries keep
returning 200 with fewer hits, "which looks healthy") and its signal lives only on
`/search/status`, which nothing forces a caller to consult. A human or MCP agent
searching a wiped index concludes the data doesn't exist. The honesty surface belongs on
the search response itself.

## Evidence
- `crates/server/src/routes/search.rs:119-127` — response contract `{query, total, count,
  hits, facets}`; no index-state field.
- `search.rs:169-179` — `/search/status` knows: `enabled`, `doc_count` (0 on enabled =
  wiped/never populated), docs the recovery path.
- MEMORY.md invariant #4 — the trap has already cost a debugging session.
- MCP parity: `build_search_request` shared (r7 `576a3d7`) — whatever the response gains,
  the MCP tool result must gain too.

## Acceptance criteria
- The search response carries an index-state block (e.g. `index: {enabled, doc_count}` or
  a `degraded: true` + reason when `enabled && doc_count == 0`, and when disabled) —
  additive, no existing key renamed. Named pure predicate for "degraded", tested
  (`wiped_index_says_so_not_silent_empty`).
- MCP `search` tool result carries the same signal (shared construction, not a copy).
- Disabled search: response says disabled rather than an indistinguishable empty page.
- `docs/features/search.md` documents the field and points at `search-backfill` recovery.

## Risks / non-goals
- `doc_count()` on every search adds a reader call — verify it's cheap (Tantivy segment
  metadata, no disk scan); if not, gate it behind the empty-result case (an honest signal
  is only NEEDED when the answer would otherwise look like "nothing matched").
- Non-goal: auto-heal / auto-backfill (the maintenance bins own recovery, server stopped).

## Build record
- Builder (Lot J, opus) commit `63db76f`. Director review: **keep** — named pure
  `index_degraded_reason(enabled, doc_count)` with THREE degraded states (disabled /
  wiped-or-never-populated / count-unreadable — the last deliberately NOT folded into 0,
  per the repo's never-report-unmeasured-as-zero rule); new shared `run_search` renderer
  extends r7's build_search_request sharing to the ANSWER shape, so /search and the MCP
  tool cannot diverge on honesty; doc_count verified cheap (segment metadata on the same
  searcher — carried unconditionally, not just on empty pages); MCP tool description
  teaches agents to read `index.reason` before concluding from zero hits; e2e separates
  empty-page from empty-index with scriptable Search impls + a populated control.
  Reasons name the exact recovery command. Docs (search.md, http-api.md) updated.
