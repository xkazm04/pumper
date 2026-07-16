# Perf-Feature Scan — search #3 (recency) + crawl artifact-overwrite bug

> 2 commits, 1 finding (full-text-search #3) + 1 out-of-lens data-integrity bug.
> Baseline preserved: build clean, tests **220 → 222** (0 regressions).
> Branch `vibeman/perf-feature-2026-07-16` (after Waves 3, 1, 2 + retention/backfill).

## Commits

| Commit | Item | What |
|---|---|---|
| `714efb8` | search #3 (Medium) | index an `indexed_at` timestamp; `sort=newest` + `since=` |
| `673a06d` | crawl bug (out-of-lens) | URL-address page artifacts so a resume can't overwrite prior bodies |

## search #3 — recency sort + date filter

The index had no time dimension, so "newest first" and "only what appeared since
X" were impossible on a corpus whose whole point is being re-scraped. Added an
`indexed_at` i64 FAST field (unix seconds) to the schema and `SearchDoc`,
populated from the record's stored timestamp (`rev.created_at` live,
`rec.updated_at` in the backfill bin, now for job-result docs). `SearchRequest`
gained `sort: SearchSort {Score|Newest}` and `since: Option<i64>`; `GET /search`
takes `?sort=newest` / `?since=`. Newest swaps `order_by_score` for
`order_by_fast_field(indexed_at, Desc)`; `since` pushes a half-open `RangeQuery`.

Being a schema change, it generalized `body_is_stored` → `schema_is_current`
(all expected fields present + body stored), so an index missing `indexed_at` is
detected as outdated and rebuilt empty. This is exactly why it landed *after* the
search-backfill bin (#2): **re-ran `search-backfill --all` against the real DB**,
which rebuilt under the new schema and re-indexed all 5196 records with timestamps
(index now holds exactly 5196 docs — the 4 stale job-result docs dropped in the
rebuild). Verified end-to-end.

Test (`engine-search/tests/recency.rs`): newest sort orders b(300),c(200),a(100)
by `indexed_at`; `since=200` keeps only ≥200; a future `since` returns nothing.

## crawl artifact-overwrite bug (out-of-lens)

Kept-page bodies were written as `page-{stats.kept:04}.html`, but `stats.kept` is
a per-run counter that restarts at 0 on a checkpoint resume — so a resumed crawl
wrote `page-0001.html` over the prior run's `page-0001.html` (a different URL's
body), while the earlier run's `pages` records still pointed their `artifact_path`
there. Stored record and on-disk file then described different URLs: silent
data-integrity corruption.

Fixed by naming the file `page-<sha256(url)[..16]>.html` — a pure function of the
(canonical, frontier-unique) URL, so it's stable across runs and revisits: each
URL owns one file, a resume writes the same page to the same name, a revisit
updates it in place. Old `page-NNNN.html` artifacts are untouched (their records
still resolve); no migration needed. This was surfaced by the broad-crawler scan
as a bug-hunter-shaped observation outside the perf/feature lenses.

## Verification

| Gate | Before | After |
|---|---|---|
| `cargo build --workspace` | clean | clean |
| `cargo test --workspace` | 220 / 0 | 222 / 0 |
| search-backfill live (new schema) | — | 5196 records re-indexed w/ timestamps |

## Patterns established (catalogue additions)

17. **Land a schema change with its rebuild path.** A search-schema addition trips
    the drift check and wipes the index; ship it *after* the backfill bin exists
    and re-run it, so the change is a clean rebuild, not silent data loss.
18. **Content/URL-address derived artifacts, never a per-run sequence.** A counter
    that resets on resume makes the same name mean different content across runs.
    Key the artifact on a stable identity (the URL) so the record and the file can
    never disagree.

## What remains (per the INDEX)

- **search #1** (High): commit-per-job → a background committer (Tantivy commits
  are fsync-heavy; a job burst pays ~200 fsyncs where ~5 would do). The last
  search finding.
- The medium/low tail across the 21 reports, and the deliberate follow-ups noted
  in the earlier wave summaries (crawl delta-journal/off-loop checkpoint save;
  grants money-enrichment once a `fetchOpportunity` response can be verified).
