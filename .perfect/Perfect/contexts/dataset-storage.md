---
name: dataset-storage
type: perfect/context
group: Core Platform
category: data
opportunity: 9
last_proposed: 2026-08-03
cooldown_until: —
directions: []
---

## Current state (scout brief, 2026-08-03 — engine level, file:line verified)

**Files (context-map drift):** map says `crates/core/src/storage/*.rs` (5 files); on disk it is
`crates/core/src/storage.rs` (2177 L, jobs/schedules/watches/triggers/saved-searches/deliveries/
checkpoints/derived CRUD) + `datasets.rs` (2784 L, records/revisions/provenance/derived-eval) +
`costs.rs` + `backup.rs` + `job.rs`. Highest migration: `0033_host_weather.sql`.

**What exists.** `upsert`→`upsert_trusted`→`upsert_stamped` (datasets.rs:269-343) atomic under
`BEGIN IMMEDIATE` (fix `573aa0c`). Bulk path `upsert_many_inner` (datasets.rs:798-928) chunks 500
rows/txn (`64efa0c`) but still 3 queries per record. `sync_many` / `sync_many_with_provenance` live
on **AppContext** (app.rs:586-630), not on `Datasets` — they carry the degrading-source
`suppresses_removals()` guard (app.rs:613-623). Change detection = SHA-256 over
`serde_json::Value::to_string()` (datasets.rs:2548), canonical only because `preserve_order` is off
workspace-wide. Trust labels normalize NULL→"stable" (datasets.rs:58-66) behind a shared
`TRUST_PREDICATE` (datasets.rs:76-82). Provenance = columns on `record_revisions`
(`job_id, source_url, artifact_sha, rules_hash`, migration `0030`); `replayable()` needs both pins
(datasets.rs:139-141); content-addressed `rules_versions` registry (datasets.rs:1357-1383).
Duplicates: `duplicate_pairs` (datasets.rs:1051-1093) in-memory **O(n²)**, cap `MAX_DUP_PAIRS=10_000`.
Derived aggregates bounded by `max_group_scan` (10k) writing `stale:true` rather than a wrong number.
Job queue: `claim_next` priority-aged (storage.rs:236-267), every terminal write fenced on
`(running, attempts)`. Cost ledger backed by an in-memory `SpentTotal` (costs.rs:52-89, `7a66236`).
Pre-migration `VACUUM INTO` backups keeping 3 (backup.rs).

**Recent churn (<3 weeks, unproven):** 16 commits on datasets.rs, 11 on storage.rs — atomicity fixes
for `upsert` and `detect_removed` (`573aa0c`, `de9f0a0` — implying real lost-revision bugs until
recently), chunked commits, simhash reindex/backfill, `prune_revisions` (`8ad5d15`), hard-delete API
(`1600d84`), resilience enforcement (`b0b98cc`), M11 derived aggregates, M12 provenance, M30 peering.

**Rough (evidence):**
- **Zero artifact GC anywhere.** Bodies at `data/artifacts/<app>/<job_id>/<name>` (app.rs:133-150).
  `docs/features/extraction.md:59`, `crawling.md:56`, `resilient-extraction.md:90` all admit it.
  Consumers that need them later: stored-pages extraction (app.rs:163-191), VCR cassettes
  (vcr.rs:351), crawl revisits write a NEW job_id copy and never reclaim the old
  (crates/apps/crawl/src/lib.rs:568), and `POST /provenance/{…}/rederive`
  (routes/provenance.rs:166 — verified real: refuses honestly when the body/hash/rules are missing).
- **Revision janitor off by default** (`revision_retention_days: 0`, config.rs:907; janitor
  main.rs:301-338 returns early). `cost_events`, `webhook_deliveries`, `job_yield`,
  `saved_search_seen` have **no prune path at all**.
- **Bulk upsert = 3 queries/record** inside each 500-row chunk (datasets.rs:868-928) → ~150k queries
  for a 50k sync, holding the DB-wide write lock (`BEGIN IMMEDIATE`); comment datasets.rs:788-791
  names this as the mechanism behind cross-app write stalls.
- **`duplicate_pairs` O(n²)** with no input-size cap; grants `link_duplicates` runs it per run over
  the whole corpus (round-3 banked seed). crawler-core already has banded SimHash (round-2 ship).
- **Partial-snapshot removal guard is one layer above the store** — any caller hand-rolling
  upsert+`detect_removed` bypasses it; the peer app does exactly that (apps/peer/src/lib.rs:38-44).
- **`clients/typescript` (@pumper/sync) is documented but absent from git** (`docs/features/
  sdk-typescript.md`, CLAUDE.md architecture) — verified: no `clients/` dir, zero tracked files.
- Possible stale table: `triggers_new` created in `0021_ingress.sql:18` while CRUD targets `triggers`.

**Tests:** core/tests/datasets.rs 21, jobs.rs 9, migrations.rs 7, checkpoints.rs 3, costs 8 inline,
backup 10 inline. Unguarded: dup-scan at scale, artifact lifecycle, cross-app write-lock contention,
unbounded-table growth.

## Direction history
- 2026-08-03: slate proposed (round 4) — see gate outcome below.

## Shipped
(none yet)
