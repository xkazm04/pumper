---
name: maintenance-tooling
type: perfect/context
group: Job Orchestration
category: config
opportunity: 4
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: crates/server/src/bin/{reindex,search-backfill}.rs,
scripts/docs/check-doc-sync.mjs. Both binaries MUST run with the server stopped (Tantivy
exclusive writer lock — MEMORY.md gotcha #4: a wiped index does not self-heal and
backfill is the recovery). r7 search work touched backfill's tombstone purge (4ca9cc4).
Guard-rails (running-server detection, partial-failure reporting) unswept.

## Direction history
- (via search-engine r7 for backfill internals.)
- 2026-08-12 (round 11): scouted (medium); candidate directions EXIST — banked, not slated
  (cap). NOT covered yet. Anchors (scout's call: C-1+C-2 together are the best single-session
  payoff in the thin sweep):
  1. **backfill --all ghost skip** (CONFIRMED by two SQL reads): list_all_datasets filters
     removed_at IS NULL while the --app path doesn't — a fully-tombstoned dataset is skipped by
     the documented full recovery, its ghosts answer /search forever, tool prints success.
     resolve_targets has ZERO tests.
  2. **doctor search-drift finding**: nothing ever tells the operator the index needs
     backfilling (doc_count vs live records; mirror records_without_simhash's remediation
     pattern). The wiped-index gotcha stays a human-noticing-a-zero.
  3. **silent 1M cap**: backfill list(…, 1_000_000) drops the OLDEST rows past the cap and
     still reports success; latent (real DB ~5.2k) but silent.
  4. (lesser) reindex has no server-stopped enforcement (one big tx = workspace stall);
     check-doc-sync scan-window + any-doc-satisfies holes (dev-loop only).

## Shipped
- (via search-engine r7)
