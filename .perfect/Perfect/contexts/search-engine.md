---
name: search-engine
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 7
last_proposed: 2026-08-04
cooldown_until: 2026-08 +2 rounds
directions: ["[[search-ghost-doc-gc]]", "[[search-enrich-hardening]]", "[[search-lifecycle-safety]]", "[[search-incremental-proof]]", "[[search-surface-parity]]"]
---

## Current state (scouted 2026-08-04, HEAD 8adfc91 — prefetched during provisioner gate)

Files: engine-search/src/{lib,enrich}.rs; writers = worker Stage::Index + search-backfill
bin (reindex bin is SimHash, NOT search — naming trap). Round-4 "full re-index per run"
finding is FIXED (`367cc7b` — delta indexing via changes_since, O(changes)).

**Top findings:**
1. **Unbounded ghost growth**: per-job result docs minted `{app}:{job_id}` — unique per
   run, indexed forever, deleted by NOTHING. Largest growth source; no merge policy tuning,
   no GC, no size telemetry (status = doc_count only). These docs also stamp
   `dataset = app` — fictitious dataset in facets/filters.
2. **UTF-8 panic in enrich** (`enrich.rs:108`): byte-slice `lowered[start-120..start]` need
   not land on a char boundary; non-ASCII body → panic INSIDE the writer-lock closure →
   poisons lock, whole batch dropped (recovered but lossy). All 13 tests are ASCII-only.
3. **RFC3339 timestamps invisible to event_date** (`\b` after ISO regex fails on `…-01T…`);
   USD-marker-prefix-only money; European decimals mis-parse ($1.234,56 → $1); M/D/Y
   assumed US; doc-level MAX amount (unrelated fee beats award).
4. **Schema-drift wipe races a live server**: remove_dir_all BEFORE taking the writer lock
   — new-schema binary vs running old-schema server destroys the live index (Unix) or
   fails boot (Windows). Corrupt-but-present meta.json → server won't boot (docs claim
   rebuild-empty). Wipe branch + incremental path + backfill e2e all untested.
5. Backfill skips tombstones but never DELETES — tombstoned-since-indexing docs survive.
6. Saved searches: app-scoping fix (21c838d) intact + tested; materialization guarded
   (RemovalGuard, score bucketing). Gaps: `saved_search_seen` retention re-alerts old docs
   if enabled (nothing guards); dataset-only-scoped search evaluated on every app's jobs.
7. Perf: regex enrichment (2 full-body lowercase allocs + 4 regex scans/doc) runs INSIDE
   the writer-lock critical section; delete_term before every add (upsert cost for new
   docs); one fsync per run with removals + one per saved-search flush; deep offset ranks
   offset+limit (10,020 at cap).
8. Docs drift: search.md has ZERO mention of amount/event_date/entity filters (M14
   undocumented); materialize/M13 absent; corrupt-dir claim wrong; ghost-doc gap unlisted.
9. Dead ends: serde_json unused dep; url/amount/event_date/indexed_at STORED but never read
   into hits; MCP search tool lacks fuzzy/sort/entity filters.

## Direction history
- 2026-08-04 (round 7): presented 5, **accepted 5/5 clean sweep** — ghost-doc-gc,
  enrich-hardening (confirmed panic-class bug), lifecycle-safety, incremental-proof,
  surface-parity.

## Shipped
- [[search-ghost-doc-gc]] → `dc03bd0` — `_job` snapshot sweep + `_records` namespace;
  size/segment telemetry on /search/status.
- [[search-enrich-hardening]] → `ed2c683` — non-ASCII panic dead; RFC3339 dates;
  enrichment out of the writer lock.
- [[search-incremental-proof]] → `4ca9cc4` — delta path tested; backfill purges tombstones.
- [[search-lifecycle-safety]] → `f4a7d1b` — locked wipe; corrupt dir quarantined, boot
  proceeds.
- [[search-surface-parity]] → `576a3d7` — MCP tool = full query surface via shared builder.
