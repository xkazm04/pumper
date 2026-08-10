---
slug: search-incremental-proof
type: perfect/direction
context: "[[search-engine]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 4ca9cc4
---

## What & why
The delta-indexing path that replaced the full re-index (`367cc7b`) — changes_since read,
dedupe-to-latest-revision-per-key, removed-key deletion — has NO test at all. And
`search-backfill` skips tombstoned rows but never deletes their already-indexed docs: a
partial backfill leaves ghosts for rows tombstoned since indexing.

## Evidence
- `worker.rs:1514-1582` (dataset_search_docs — untested)
- `search-backfill.rs:64-66` (tombstone skip, no delete)

## Acceptance criteria
- Tests over `dataset_search_docs`: new/changed indexed from revision snapshot, removed
  keys deleted, latest-revision-per-key dedupe (multiple revisions in one run).
- `search-backfill` deletes index docs for tombstoned rows it encounters (extracted fn +
  test).
- A backfill e2e over a scratch index directory (build → tombstone → re-backfill → ghost
  gone).

## Risks / non-goals
- Non-goal: changing delta semantics; this direction PROVES them and fixes the backfill
  ghost only.

## Build record
- Builder SE1 (opus), wave 1 → master `4ca9cc4`. `dataset_docs_from_revisions` extracted +
  4 tests — the dedupe test names the real hazard: changes_since is newest-first, so
  without latest-revision dedupe a changed-then-removed key emits delete AND an older add,
  and the add WINS (ghost resurrection). `backfill_action` classifies Index|Purge;
  tombstoned rows now purged; e2e over a real scratch Tantivy dir.
- Honest: e2e exercises classification + real index, not the SQLite read path.
- Gates: worktree 1107/0; wave-1 integration gate green.
