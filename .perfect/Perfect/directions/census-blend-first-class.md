---
slug: census-blend-first-class
type: perfect/direction
context: "[[us-business-census]]"
lens: feature
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---
## What & why
The two product datasets of the census fleet — `census/market_blend` and
`census/saturation` — are the least observable artifacts in the whole pipeline:
no `index_datasets` (so no per-record search, no watch/trigger/saved-search can
ever fire on `app="census"`), no provenance (raw `ctx.datasets.upsert_many` →
`Provenance::default()` on every revision), no catalog contract, and the blend
silently computes over truncated inputs at the 50k read cap. Make them
first-class: the user moment is "I set a watch on `census/market_blend`, it
fires; I search saturation, it's there; I audit a blend row to its inputs."

## Evidence
- Writes: census-density/src/lib.rs:512-515 (saturation), :714-717 (market_blend)
  — raw upsert_many, no provenance, no index spec.
- No census run() result sets `index_datasets` (verified all four apps).
- Consumers gate on it: worker.rs:1902 (dataset_search_docs), :1520
  (run_indexed_apps), :1287-1306 (load_run_changes scopes hooks to indexed_apps —
  a watch on app="census" can NEVER fire today).
- Pattern: grants-common/src/lib.rs:204-209 (`index_datasets` emission).
- Stamped-write seam exists: mpsv-vpm uses `ctx.datasets.upsert_many_stamped`.
- 50k cap: BLEND_READ_LIMIT density:607; 4 of 5 reads plain `.list` (:641-645,
  :662-666, :685-689, :690-694) — at-cap read is silent (r14 topic-stats killed
  exactly this class for eu).
- Catalog precedent for virtual-app dataset contracts: r14 topic_stats row.

## Acceptance criteria
1. All four census apps' results declare `index_datasets` covering at least
   `census/market_blend` + `census/saturation` (grants-common pattern); emission
   proven by unit test per app; consumption may rest on the worker's existing
   generic tests (name them) — add e2e only if achievable without live fetches,
   else report honestly.
2. Blend + saturation writes carry real provenance (upsert_many_stamped or
   equivalent): at minimum derived-from-inputs + as-of; no more
   Provenance::default() rows (test).
3. `sync_market_blend` distinguishes complete from truncated reads: any input
   read hitting BLEND_READ_LIMIT flags the result (aggregate_truncated-style)
   instead of silently blending a partial corpus (test at the boundary).
4. Catalog contract rows for the two product datasets following the r14
   topic_stats precedent; verify the catalog inventory test accepts virtual-app
   rows — if the seam refuses them, report the gap precisely, don't hack it.
5. Docs same session: apps.md census rows name all four apps + both product
   datasets; datasets.md "source-prefixed keys" claim corrected to the real key
   shape; resilient-extraction.md:83's false census-quarantine claim corrected.

## Risks / non-goals
- No ranking function (no consumer in-repo — rejected separately), no new
  routes, no worker.rs edits (report seam gaps instead).
- Indexing volume judgment: primary datasets (establishments etc.) stay
  unindexed unless the builder argues otherwise with numbers.

## Build record
(to fill during build)
