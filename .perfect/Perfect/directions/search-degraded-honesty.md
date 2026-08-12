---
slug: search-degraded-honesty
type: perfect/direction
context: "[[job-search-api]]"
lens: robustness
status: accepted
size: S
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
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
(pending)
