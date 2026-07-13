---
slug: crawl-pages-dataset
type: perfect/direction
context: "[[Broad Crawler]]"
lens: feature
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 4c132df
---

## What & why
Crawl output is page-NNNN.html files + one opaque search doc (worker auto-indexer explodes only records/stories/items keys — crawl's `pages` matches none). Write each kept page as a `crawl/pages` record (key = canonical URL; compact fingerprint: title, status, chars, simhash, excerpt, artifact path — body stays artifact per repo anti-pattern). Pages become searchable, change-detected, and trigger-chainable.

## Evidence
- No ctx.datasets use: crates/apps/crawl/src/lib.rs
- Single-doc indexing: crates/server/src/worker.rs:366-374
- Anti-pattern (fingerprints not bodies): harness-learnings

## Acceptance criteria
- [ ] Per-kept-page record upserted to `crawl/pages` (upsert_many — partial batches, never sync_many).
- [ ] Record: url key, title, status, content_chars, simhash, excerpt, artifact_path, depth, crawl job id.
- [ ] Search auto-index picks pages up per-record (records land through the normal dataset path or result shape adjusted).
- [ ] docs/features/crawling.md updated.

## Risks / non-goals
- Non-goal: storing bodies in records.

## Build record
(pending)
