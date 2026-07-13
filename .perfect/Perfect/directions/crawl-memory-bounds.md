---
slug: crawl-memory-bounds
type: perfect/direction
context: "[[Broad Crawler]]"
lens: optimization
status: shipped
size: S
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 4b085c3
---

## What & why
kept_hashes is linear-scanned per page (O(n²) over a 100k crawl) and stats.pages accumulates one struct per page in RAM for the whole run (and is lost on resume). Replace the linear SimHash scan with a banded/prefix-bucketed index; stop accumulating pages[] in memory (stream to artifact/dataset, keep counters in the result).

## Evidence
- O(n²) scan: crates/core/src/crawl.rs:254; unbounded pages[]: crawl.rs:283
- False "constant memory" docstring: crawl.rs:1-9

## Acceptance criteria
- [ ] SimHash dedup via banded buckets (same Hamming-distance semantics; test proves equivalence on a fixture set).
- [ ] pages[] no longer held in memory; result carries counters + artifact/dataset pointer.
- [ ] Checkpoint format versioned or backward-compatible (state what happens to old checkpoints).
- [ ] Docstring claim corrected.

## Risks / non-goals
- Checkpoint compat: breaking old checkpoints is acceptable if detected and reported cleanly (fresh start), not silently wrong.

## Build record
(pending)
