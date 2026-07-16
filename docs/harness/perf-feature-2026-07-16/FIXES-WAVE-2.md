# Perf-Feature Scan — Fix Wave 2: Write Amplification

> 5 commits, 5 findings closed (theme B + the deferred grants #3).
> Baseline preserved: build clean, tests **216 → 218** (0 regressions).
> Branch `vibeman/perf-feature-2026-07-16` (continues after Waves 3, 1). Not pushed.

## Theme

Every fix here is the same shape: *we rewrite far more than the change*. Batch
upserts committed one transaction per record, removals wrote two, the search index
rebuilt the whole dataset per job, the crawl re-serialized the whole frontier
thousands of times, and the grants sweep re-scanned the whole corpus per source.
All bounded today, all grow with the data — so they get worse every day deferred.

## Commits

| # | Commit | Finding | Severity | Files |
|---|---|---|---|---|
| 1 | `64efa0c` | dataset-store #1 — per-record upsert transactions | High | datasets.rs, tests |
| 2 | `de9f0a0` | dataset-store #2 — detect_removed non-atomic + 2 txn/key | High | datasets.rs, tests |
| 3 | `367cc7b` | job-worker #1 — full-dataset reindex per job | High | worker.rs |
| 4 | `6717ecc` | broad-crawler #1 — quadratic checkpoint | High | crawl.rs |
| 5 | `bee7854` | us-grants #3 (deferred from W1) — sweep scans whole corpus | Medium | grants-common |

## What was fixed

1. **`upsert_many` chunked transactions.** The most-executed write path looped
   `upsert()` per item — each its own `BEGIN IMMEDIATE`/commit/fsync and a
   database-wide write-lock acquisition, so a 5k-record ingest was 5k commits and
   5k lock grabs (the mechanism behind cross-app write stalls). Now commits in
   chunks of 500 on one held connection via the existing `upsert_in_tx` seam
   (~10 commits), keeping exact per-record semantics; a mid-chunk failure rolls
   back its chunk and propagates.

2. **`detect_removed` atomic + chunked.** Per removed key it wrote the
   `UPDATE removed_at` and the `removed` revision as **two separate autocommit
   transactions** — both a cost problem (2 commits/key) and a **durability hole**:
   a crash between them tombstoned a record with no revision, and since the next
   sync sees `removed_at` already set and the key still absent, the loop never
   revisits it — the change-feed/watch/trigger signal for that removal is lost
   *permanently*. Now the pair runs in one transaction (`remove_in_tx`, mirroring
   `upsert_in_tx`), chunked. The removed-record signal is a differentiated product
   output, not an internal detail.

3. **Index only changed records.** `dataset_search_docs` re-read and re-indexed
   the entire named dataset (`list(.., 100_000)` → a Tantivy delete+add per live
   record) on every completion — grants-gov and ca-grants both name
   `grants/unified` (~5k rows), so the whole corpus was re-indexed twice every
   morning for a handful of changes (~100–1000× amplification, growing forever).
   Now reads the dataset's revisions since the job started (scoped to the indexed
   dataset's app namespace, which differs from the running app), indexes the
   new/changed keys from their snapshots, and routes `removed` keys to
   `delete_ids` (previously left as stale hits). Cost O(changes), not O(corpus).

4. **Checkpoint by wall-clock, not page count.** `Checkpoint::save` serializes
   the whole frontier (up to 100k seen-strings) every call; firing it every 25
   kept pages made total work O(pages/25 × frontier) — a 100k-page crawl did
   ~4,000 full ~10 MB rewrites (~40 GB write amplification) and each inline save
   froze all in-flight fetches. Replaced with a 5s minimum interval; the
   unconditional final save still captures the end state, and the frontier's
   seen-set makes a resume idempotent.

5. **Sweep only candidates.** `sweep_closed` loaded the entire `grants/unified`
   corpus (mostly already-`closed` rows that can never flip) to find the
   open/forecasted past-due rows — paid once per source, now 3×/day after eu-sedia
   joined unified. Now filters `status IN {open, forecasted}` in SQL via
   `list_filtered`; the predicate is unchanged so results are identical.

## Verification

| Gate | Before Wave 2 | After Wave 2 |
|---|---|---|
| `cargo build --workspace` | clean | clean |
| `cargo test --workspace` | 216 / 0 | 218 / 0 |

New integration tests: `upsert_many` across the 500-record chunk boundary
(all-new / re-run-unchanged / change-per-side / revision chain intact);
`detect_removed` tombstone + matching `new→removed` revision chain + idempotent
re-run. (The crawl time-gate and sweep filter are covered by existing predicate
tests + build; both preserve behaviour, only what's written/read shrinks.)

## Patterns established (catalogue additions)

9. **Transaction granularity should match the caller's unit of work.** A
   correctly-atomic per-record transaction is wrong for a batch — chunk the batch
   so the commit boundary is the batch, not the row. The seam that makes one
   record atomic (`*_in_tx(conn, …)`) is exactly what a chunked batch reuses.
10. **A two-statement write that another path made atomic must be atomic
    everywhere.** `upsert` got `BEGIN IMMEDIATE`; `detect_removed` wrote the same
    two rows and didn't — same corruption, one place. When hardening one writer,
    grep for every site that writes the same row-pair.
11. **Derived indexes should be rebuilt by delta, not from scratch per event.**
    "Re-index the whole dataset each run" is O(corpus)×frequency and hides a
    dependency on a backfill path for the wiped-index case. Drive the index from
    the change feed and keep a separate reindex bin for recovery.
12. **Gate expensive periodic work by time, not by item count.** An O(state) save
    fired every N items is O(items/N × state); a wall-clock gate makes it O(state
    × duration) and decouples cost from throughput.

## What remains (per the INDEX)

- **dataset-store #3** (High, feature): no delete/retention API — the store only
  grows (unbounded revisions, GB/year per active dataset, blocks GDPR-style
  deletion). Deferred from this wave: it's a 3-part *feature* (delete API +
  `prune_revisions` + janitor tick), better as its own focused session than folded
  into a write-amp perf wave.
- **search #2** (High): the backfill/reindex bin that Wave 2 commit #3 now depends
  on for wiped-index recovery. Natural pairing with dataset-store #3.
- **broad-crawler #1 follow-ups:** delta/journal checkpoint + off-loop
  single-flight save (the time-gate removed the amplification; these remove the
  remaining per-save frontier serialize + fetch stall).
- Standalone bug (out-of-lens): crawl `artifact_name` resets to 0 on checkpoint
  resume → overwrites prior `page-NNNN.html`. Fix regardless of wave.
