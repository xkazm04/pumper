---
slug: extract-from-stored-pages
type: perfect/direction
context: "[[Declarative Extraction Engine]]"
lens: feature
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 66b063f
---

## What & why
Extractor only accepts `params.urls` and re-fetches live; crawl's `pages` records carry artifact_path to bodies already on disk — crawl→extract double-fetches every page. Add `source: {app, dataset}` input mode reading bodies from stored artifacts, filtered by keys incl. `_trigger.keys`, completing the crawl→trigger→extract pipeline with zero re-fetching.

## Evidence
- Live-fetch only: crates/apps/extractor/src/lib.rs:29-37, 63-75
- artifact_path in pages records: crates/core/src/crawl.rs:596-612; dataset write apps/crawl/lib.rs:60-78
- Trigger keys plumbing exists: crates/server/src/triggers.rs:92-118 (capped changed keys in _trigger)

## Acceptance criteria
- [ ] Extractor accepts `source: {app, dataset, keys?}`; keys default to `_trigger.keys` when triggered; reads bodies via artifact_path (resolve against the source job's artifacts dir — state the resolution rule).
- [ ] Missing/unreadable artifacts counted + reported per key, not silent.
- [ ] Existing `urls` mode unchanged; both modes share the extract path.
- [ ] Docs: extraction.md pipeline recipe (crawl → dataset trigger → extractor with source mode).

## Risks / non-goals
- Artifact retention: bodies live in per-job artifact dirs — document that extraction must run before artifact cleanup (no retention policy exists yet).

## Build record
(pending)
