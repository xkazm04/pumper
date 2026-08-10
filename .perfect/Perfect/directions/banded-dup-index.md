---
slug: banded-dup-index
type: perfect/direction
context: "[[dataset-storage]]"
lens: optimization
status: shipped
size: M
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: 51ce092
---
## What & why
`duplicate_pairs` loads every live record of a dataset and does an in-memory pairwise SimHash
Hamming compare — genuinely O(n²); `MAX_DUP_PAIRS` caps the OUTPUT, not the input. The grants
unified layer calls `link_duplicates` on every run over the whole corpus, which is the scaling cliff
banked as a round-3 seed. crawler-core already ships banded SimHash bucketing (round-2
`crawl-memory-bounds`, commit `4b085c3`) — layer on it rather than fork a second implementation.

## Evidence
- `crates/core/src/datasets.rs:1051-1093` (nested loop), cap `MAX_DUP_PAIRS=10_000` at `datasets.rs:17`.
- Existing banded implementation to reuse: crawler-core SimHash banding (`4b085c3`).
- Consumer: grants unified layer `link_duplicates`, per run, whole corpus.

## Acceptance criteria
- Candidate generation via band buckets + exact Hamming verify within buckets.
- Equivalence test: same pair set as the brute-force scan on a fixture with known near-duplicates.
- Measured scan time at ~50k records vs today, recorded in the report.
- Output cap semantics and the `MAX_DUP_PAIRS` contract unchanged; the grants path uses the new seam.

## Risks / non-goals
Do not change the SimHash token hash — that would invalidate stored fingerprints
(`reindex_simhashes` exists for that case and is out of scope).

## Build record
Lifted the crawler's band arithmetic into `simhash::BandedIndex<T>`; `crawl::SimHashIndex` is now a
thin wrapper over it (layered, not forked — its 0..20 equivalence test guards the shared code).
Output contract preserved exactly: pair ordering, enumeration order, MAX_DUP_PAIRS truncating the
walk, stable final sort. **Distance 3 (what both real callers use): 0.8s vs 23s at 50k, ~27x.**
Two design corrections the builder found and reported: (1) a naive per-query neighbors() was 2x
SLOWER than brute force on skewed corpora where a band collapses into one bucket — replaced with
binary-search-skip + k-way merge so the degenerate case never exceeds the scan it replaces;
(2) pigeonhole banding needs d+1 bands over 64 bits, so it stops discriminating past distance ~5 —
the index detects this itself and falls back to a plain walk, one code path.
Director review: honest self-benchmarking against its own first design is exactly the standard.
Open item recorded: MIN_BAND_BITS=10 is a judgement call from 50k debug measurements, not
production data. Cherry-picked as 51ce092.
