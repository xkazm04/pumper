---
slug: grants-searchable-alerts
type: perfect/direction
context: "[[US Grant Opportunities]]"
lens: feature
status: shipped
size: S
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 94940a9
---

## What & why
Each daily grants job indexes as ONE opaque blob (result shape lacks records/items arrays the worker recognizes) — /search can't find a single grant and the fully-built saved-search alert rail has nothing to match. Emit per-opportunity normalized records in a recognized shape (or index from the unified upsert path). Unlocks keyword grant search + standing funding alerts in one diff.

## Evidence
- Indexer recognizes only records/stories/items: crates/server/src/worker.rs:496-517
- Grants results have no such array: grants-gov/lib.rs:206-236 (closingSoon only), ca-grants result shape
- Alert rail idle: docs/features/search.md:17-19 (saved searches + search.matched webhooks)

## Acceptance criteria
- [ ] Per-opportunity docs reach the search index on every grants run (title/url/agency/status searchable) — choose result-shape vs unified-path indexing, justify; keep job results compact (repo law) — do NOT inline 2500 full records if avoidable (consider indexing seam instead).
- [ ] GET /search?q= finds individual grants; saved search on a keyword fires search.matched for new matches (live-verify both).
- [ ] Re-index replaces prior docs per id (no duplicates across daily runs).
- [ ] docs/features/apps.md + search.md updated.

## Risks / non-goals
- Result-size blowup if records inlined — prefer an indexing path that doesn't fatten job results.

## Build record
(pending)
