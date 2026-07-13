---
slug: crawl-incremental-recrawl
type: perfect/direction
context: "[[Broad Crawler]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 1c3fe35
---

## What & why
The crawler always fetches from scratch; backlog #8 "crawl + diff = monitoring product" (score 18) unshipped. Add `revisit` mode: seed from stored crawl/pages records, conditional GETs (ETag/If-Modified-Since captured into records) + no_cache, report only new/changed/gone — a scheduled site-change sentinel that pairs with dataset triggers. Builds on [[crawl-pages-dataset]].

## Evidence
- No conditional-GET support: crates/core/src/engine.rs:31-39 (HttpRequest has no etag/if_modified_since)
- no_cache/ttl_override shipped round 1; change detection substrate: datasets.rs

## Acceptance criteria
- [ ] HttpRequest gains conditional-GET fields; engine sends headers, surfaces 304 cleanly.
- [ ] crawl `mode: revisit` param: seeds from crawl/pages, stores etag/last_modified, 304 ⇒ unchanged (cheap), changed pages re-fingerprinted + upserted, vanished pages flagged via removal semantics.
- [ ] Result reports {revisited, unchanged_304, changed, gone, new}.
- [ ] docs/features/crawling.md documents the sentinel recipe (schedule + dataset trigger).

## Risks / non-goals
- Non-goal: full-frontier expansion in revisit mode by default (opt-in discovery of new links).

## Build record
(pending)
