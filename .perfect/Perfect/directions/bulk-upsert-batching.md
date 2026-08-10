---
slug: bulk-upsert-batching
type: perfect/direction
context: "[[dataset-storage]]"
lens: optimization
status: shipped
size: M
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: 879f9ab
---
## What & why
`upsert_many_inner` issues SELECT + INSERT/UPDATE + revision-INSERT per record inside each 500-row
chunk — roughly 150k queries for a 50k-record sync (mpsv-vpm ingests ~300k postings daily). Each
chunk holds the DB-wide write lock under `BEGIN IMMEDIATE`, which the code's own comment names as
the mechanism behind cross-app write stalls. Batching the per-chunk reads and revision writes cuts
both the query count and the lock-hold time that blocks every other app's worker.

## Evidence
- `crates/core/src/datasets.rs:798-928` (`upsert_many_at_depth` / `upsert_many_inner`), chunk size
  `UPSERT_CHUNK=500` at `datasets.rs:241`.
- Write-lock contention comment: `crates/core/src/datasets.rs:788-791`.
- Prior fix reduced commits, not queries: `64efa0c perf(datasets): commit upsert_many in chunked transactions`.

## Acceptance criteria
- Queries per chunk are O(1) in chunk size (batched existing-hash read; multi-row revision insert),
  not O(n).
- Differential/property test: identical New/Changed/Unchanged/Removed verdicts vs the current
  implementation over randomized batches, including duplicate keys within a batch and null/absent
  field mixes.
- Atomicity from `573aa0c` preserved — concurrent same-key writers still cannot corrupt the chain.
- Measured before/after wall-clock AND write-lock hold time on a 50k-record sync, in the report.

## Risks / non-goals
No change to hash canonicalization or trust semantics. Respect SQLite's parameter limit when
batching (chunk the IN-list).

## Build record
A 500-row chunk now issues ~20 statements instead of ~1,500: two batched reads (hashes, next
revision numbers), one multi-row upsert, one IN-list UPDATE, one multi-row revision insert, bounded
by a conservative 900-bind-param limit (SQLite <3.32 caps at 999). **Unplanned larger win**: sha256 +
SimHash + to_string were being computed INSIDE `BEGIN IMMEDIATE`; they now run before the lock.
Measured (debug, 50k records): all-new 28.9s→12.1s, all-changed 36.8s→16.2s, lock hold per chunk
184→106ms. Competing writer during the sync: 12-15 writes with 1-3 starved on SQLITE_BUSY →
464-491 writes, **zero starved**. Differential test vs a per-record reference loop over randomized
batches (duplicate keys in-batch, null vs absent, chunk-boundary sizes) — **mutation-checked**.
Director review: read the diff; atomicity preserved (reads inside the same BEGIN IMMEDIATE), the
per-chunk shared timestamp is justified (ordered reads already tiebreak). Cherry-picked as 879f9ab.
