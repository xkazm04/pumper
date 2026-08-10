---
slug: read-path-population-honesty
type: perfect/direction
context: "[[dataset-api]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: fa26d29
---

## What & why
`GET /datasets/{app}/{ds}?trust=stable&filter=…` returns quarantined rows: any `?filter=`
routes to `list_filtered` which hard-codes trust to `None`, and the default
no-cursor-no-filter path uses `list()` which has no trust argument at all. Round 5's
`list_filtered_trust` was adopted only by `/grants`, so two routes answer "stable rows in
grants/unified" differently. Separately, adding `?filter=` silently flips tombstone
inclusion (list/list_page include removed rows; list_filtered excludes) — same route, two
populations, and no filtered-including-tombstones mode exists.

## Evidence
- `routes/datasets.rs:208/:222` → `list_filtered` = `list_filtered_trust(..., None)`
  (`core/datasets.rs:1595`); `:203-210` → `list()` (no trust arg)
- `query.rs:162/:169` — /grants uses `list_filtered_trust` correctly
- Tombstones: `core:1528/:1546` include; `core:1617-1619` exclude; export inherits the split
- Engine-level single predicate exists: `TRUST_PREDICATE` `core/datasets.rs:79-103`

## Acceptance criteria
- Every `/datasets/{app}/{ds}` read path (default, cursor, filtered, export — all formats)
  honors `?trust=` via the shared trust plumbing; no path silently drops it.
- Tombstone inclusion is explicit: `?removed=include|exclude` with ONE consistent default
  across all paths (spec: default exclude, matching filtered + /grants behavior; unfiltered
  paths change to match — document the behavior change).
- HTTP-level tests pin the matrix: {default, cursor, filter} × {trust} × {removed}.
- `docs/features/datasets.md` + `http-api.md` corrected and mutually consistent.

## Risks / non-goals
- Changing the unfiltered default population (removed rows now excluded by default) is a
  consumer-visible change — docs must flag it; the changes feed remains the tombstone-aware
  surface.
- Non-goal: touching mcp/grants-common bypasses (other contexts; note them for their rounds).

## Build record
- Builder D1 (sonnet), wave 1 → master `fa26d29` (gate pending at write time). New
  `Datasets::list_records_view(app, ds, filters, after, limit, trust, include_removed)` —
  ONE function behind default/cursor/filtered/export so the paths cannot disagree; built on
  TRUST_PREDICATE/push_trust_filter, no second predicate. `?removed=include|exclude`
  (default exclude — behavior change on unfiltered paths, documented). Export now honors
  trust too. HTTP-level e2e matrix test (tower::oneshot) {default,cursor,filter} ×
  {trust} × {removed} + 400 on bad removed=. Docs reconciled — including fixing datasets.md's
  stale "json buffered (100k cap)" claim (code streams all 3 formats; http-api.md was right).
- Non-goals honestly listed: mcp tool_query_dataset and grants-common/trades-common/plugin/
  census-density/extractor internal `list_filtered` reads still trust-unaware (their
  contexts' rounds).
- Worktree gates: 1044 tests 0 failed incl. OpenAPI inventory.
