# Perf-Feature Scan — Retention + Search-Backfill Session

> 6 code commits (+1 lockfile sync), 2 findings closed (dataset-store #3 +
> full-text-search #2 — a deliberately paired session).
> Baseline preserved: build clean, tests **218 → 220** (0 regressions).
> Branch `vibeman/perf-feature-2026-07-16` (after Waves 3, 1, 2). Not pushed.

## Why paired

Wave 2's move to incremental search indexing (index only a run's changed records)
removed the accidental full-rebuild-every-run safety net, so a wiped/late-enabled
index now needs an explicit backfill — **search #2**. And **dataset-store #3** (no
delete/retention API, unbounded revision growth) is the other half of "the store
only ever grows." They share the same seam (`SearchDoc::from_dataset_record`, the
`records`/`record_revisions` tables) and the same operator story, so they shipped
together.

## Commits

| # | Commit | Finding | What |
|---|---|---|---|
| 1 | `9d74b2b` | search #2 | `Search::doc_count` + `GET /search/status`; relocate the doc builder to `pumper_core` |
| 2 | `57af720` | search #2 | `search-backfill` bin + actionable schema-drift log |
| — | `a7eee02` | (Wave 1) | Cargo.lock sync for eu-sedia's grants-common dep |
| 3 | `1600d84` | dataset-store #3 | `delete_record` / `delete_dataset` + DELETE routes (drop search docs too) |
| 4 | `8ad5d15` | dataset-store #3 | `prune_revisions(older_than, keep_min_per_key)` |
| 5 | `61d45d0` | dataset-store #3 | retention janitor + `[storage]` config (opt-in) |

## What was fixed

**search #2 — observe and rebuild the index.**
- `Search::doc_count` (Tantivy `num_docs()`; `NoSearch` → 0) surfaced on
  `GET /search/status` → `{enabled, doc_count}`, so an emptied-but-healthy-looking
  index is finally visible (`doc_count: 0` on an enabled index = run the backfill).
- `search-backfill` bin (mirrors `bin/reindex.rs`): walks live dataset records and
  re-indexes them in commit-sized chunks through the **same**
  `SearchDoc::from_dataset_record` builder the live path uses, so ids are stable
  (`<app>:<dataset>:<key>`) and it upserts, not duplicates. Scope is required
  (`--all` | `--app` | `--app --dataset`) so a full rebuild is deliberate. The
  builder + id fn were relocated from a private worker fn into `pumper_core` so the
  live and offline paths can't drift. The schema-drift branch in
  `TantivyIndex::new` now logs an actionable message pointing at the bin instead of
  implying self-healing.
- **Verified live** against the real `data/pumper.db`: `--all` indexed 5196 records
  across 12 datasets (doc_count 4 → 5200); a scoped run and the no-scope error path
  both behave. (Side effect: this populated the user's local search index — see
  Notes.)

**dataset-store #3 — the store can shrink now.**
- `Datasets::delete_record` (record + its whole revision history, one
  `BEGIN IMMEDIATE`, returns existed) and `delete_dataset` (all records + revisions,
  returns count). Hard deletes, distinct from `detect_removed`'s tombstoning.
  Surfaced as `DELETE /datasets/{app}/{dataset}` and
  `DELETE /datasets/{app}/{dataset}/records/{key}`, both of which also drop the
  corresponding search docs so a deleted record can't linger as a hit.
- `prune_revisions(older_than, keep_min_per_key)`: trims append-only
  `record_revisions` (≈ GB/year per active dataset) while always keeping the newest
  N per record so diffs/history stay usable. Portable correlated subquery, backed
  by the `(app,dataset,key,revision)` PK.
- A retention janitor drives it every 6h, **off by default** (`[storage]
  revision_retention_days = 0`) — deleting a dataset's accrued history, the
  product's value, must be opt-in.

## Verification

| Gate | Before | After |
|---|---|---|
| `cargo build --workspace` | clean | clean |
| `cargo test --workspace` | 218 / 0 | 220 / 0 |
| OpenAPI route-coverage test | pass | pass (3 new routes registered + inventoried) |
| `search-backfill` live run | — | 5196 records indexed on real DB |

New tests: `delete_record`/`delete_dataset` remove rows + revisions and report
existed/count; `prune_revisions` keeps the newest N and respects the cutoff.

## Notes / follow-ups

- **The live smoke-test populated the local search index** (`--all`, 5196 docs).
  This is benign (searchable scraped content) but note the live worker path only
  *maintains* datasets named in `index_datasets` (today `grants/unified`), so the
  other backfilled datasets won't be kept current by normal runs. To reset: delete
  `data/search-index/` (rebuilds empty, refills per the maintained set) or
  `DELETE /datasets/{app}/{dataset}` the ones you don't want searchable.
- **search #1** (commit-per-job → background committer) and **search #3** (indexed
  timestamp for recency sort/filter) remain — #3 explicitly wants to land *with* a
  backfill, which now exists, so it's cheap next.
- **crawl artifact-overwrite** (out-of-lens) still open: `artifact_name` resets to
  0 on checkpoint resume → overwrites prior `page-NNNN.html`.

## Patterns established (catalogue additions)

13. **A derived-index optimization must ship its rebuild path.** Removing a
    full-rebuild-every-event created a silent dependency on a backfill that didn't
    exist. Pair "index by delta" with a reindex bin + a doc-count you can observe.
14. **Share the doc/id builder between the live and offline index paths.** If the
    backfill builds docs differently from the live path, a re-index silently
    duplicates or mis-deletes. One `from_record` fn in the shared crate.
15. **Destructive retention defaults to OFF.** Silently deleting a user's accrued
    data must be opt-in; the janitor returns immediately when disabled.
16. **Hard delete ≠ tombstone.** Keep both: `detect_removed` tombstones (a
    change-feed signal); `delete_*` hard-deletes (operator/GDPR). Don't conflate.
