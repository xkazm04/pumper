---
name: dataset-api
type: perfect/context
group: HTTP API
category: api
opportunity: 7
last_proposed: 2026-08-04
cooldown_until: 2026-08 +2 rounds
directions: ["[[read-path-population-honesty]]", "[[derived-trust-inheritance]]", "[[backfill-budget-and-batching]]", "[[history-keyset-honest-exports]]", "[[resurrect-pumper-sync]]"]
---

## Current state (scouted 2026-08-04, HEAD 49ca08c — prefetched during trigger-pipeline gate)

Files: `routes/{datasets,derived,provenance,query}.rs`. All routes mounted + in the OpenAPI
inventory test. Engine-level trust is now single-source (`TRUST_PREDICATE`,
`core/datasets.rs:79-103`) — the round-4 "reimplemented per route" finding is REFUTED at the
engine level, but CONFIRMED at the route level:

1. **Trust param silently dropped when `?filter=` present** (`datasets.rs:208/:222` →
   `list_filtered` = `list_filtered_trust(..., None)`); the no-cursor-no-filter path uses
   `list()` which has no trust arg at all. `/grants?trust=stable` filters; `/datasets/grants/
   unified?trust=stable&filter=…` returns quarantined rows. Round 5's `list_filtered_trust`
   adopted by `/grants` only.
2. **Tombstone population switch**: adding `?filter=` flips removed-row inclusion
   (list/list_page include; list_filtered excludes) — same route, different population.
   No filtered-including-tombstones mode exists.
3. **Latent keyset bug in `history_page`** (`core/datasets.rs:669-674`): predicate leads on
   `created_at` but `ORDER BY revision DESC` — out-of-order created_at skips/repeats rows
   across page boundaries. `changes_page` gets it right. Untested.
4. **Derived rows are unstamped and implicitly stable**: `apply_derived` →
   `upsert_many_at_depth(..., None, None, ...)` (`core:2033`, `:2105-2112`) — provisional/
   quarantined source rows derive into stable-looking derived rows; no provenance link to
   source record or spec. Untagged `lookup`-column parse failure silently degrades a spec to
   passthrough (`core:2781-2785`).
5. **`backfill_derived` runs unbounded + synchronous inside the HTTP request**
   (`derived.rs:277` → `core:2087-2122`), N+1 per record for lookup joins + per-record filter
   re-parse (`core:2050/:2061`); group path hoists correctly, row path doesn't.
6. **Export swallows mid-stream store errors** (`datasets.rs:396-399`): truncated export is
   HTTP 200 valid JSON, indistinguishable from complete.
7. Cursors: 3 formats, minted in 2 owners (route: `updated_at|key`; core: `created_at|rowid`
   and `created_at|revision`); only the route half unit-tested.
8. **`clients/typescript` deleted from HEAD in `27dba84`** but `docs/features/sdk-typescript.md`,
   `docs/features/README.md:22`, `CLAUDE.md:67` still document it as shipped. No pinned
   contract, no conformance tests.
9. N+1s: `get_provenance` 1 job query per revision (≤500); `catalog_health` 1 list per source;
   `rederive` does blocking file I/O on the async runtime (`provenance.rs:306`).
10. Filtered reads are full-partition `json_extract` scans (acknowledged `core:1580-1586`);
    closing-soon scans twice + unindexable ORDER BY.
11. Zero HTTP-level tests for all four route files (engine under them well tested) — exactly
    where the trust-drop/tombstone/cursor/export defects live. Two filter grammars (server
    `parse_filters` vs core `parse_filter_spec`), no cross-check test.

## Direction history
- 2026-08-04 (round 6): presented 5, **accepted 5/5 clean sweep** — read-path-population-
  honesty (robustness, confirmed bug), derived-trust-inheritance (robustness), backfill-
  budget-and-batching (optimization), history-keyset-honest-exports (robustness),
  resurrect-pumper-sync (wildcard). Zero rejections.

## Shipped
- [[read-path-population-honesty]] → `fa26d29` — unified list_records_view; trust honored
  everywhere; explicit removed= (default exclude).
- [[history-keyset-honest-exports]] → `2aa150d` — keyset order aligned; truncated exports
  detectable.
- [[derived-trust-inheritance]] → `e8ed5e9` — weakest_trust floor; spec provenance; corrupt
  lookup column loud.
- [[backfill-budget-and-batching]] → `1fde4ac` — budgeted/resumable; −36% measured; joins
  chunked.
- [[resurrect-pumper-sync]] → `78ff895` — SDK restored; two-sided conformance pin; mirror
  tombstone catch (removed=include).
