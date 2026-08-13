---
slug: backfill-purges-ghosts
type: perfect/direction
context: "[[maintenance-tooling]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---
## What & why
`search-backfill --all` is the documented full rebuild — the thing an operator runs after a
wiped or drifted index (`docs/features/search.md:83`). Its own header comment says it exists to
purge tombstoned documents. **It cannot purge the ghosts it exists to purge.**

`--all` resolves targets through `Datasets::list_all_datasets()`, whose SQL is
`SELECT DISTINCT app, dataset FROM records **WHERE removed_at IS NULL**`
(`crates/core/src/datasets.rs:1854`). The `--app X` path goes through `datasets(app)`, which has
**no such filter** (`:1865`). So a dataset whose records are *all* tombstoned is invisible to
`--all` — and that is precisely the state the purge exists to repair. Its already-indexed
documents keep answering `/search` forever, and because `index.degraded` is false on a
repopulated index, nothing ever contradicts them.

The tool then prints `search backfill complete: N record(s) indexed, 0 tombstoned record(s)
purged` and exits 0.

Two smaller honesty defects live in the same 24 lines and ship with it:
- **A typo'd scope reports success.** `resolve_targets` (`search-backfill.rs:136-159`) returns
  `(app, dataset)` unvalidated. `--app grants --dataset unifed` reads 0 rows and prints the same
  cheerful completion line with exit 0. The `no datasets to backfill` guard (`:56-59`) covers
  only the `--app`/`--all` paths, never the two-flag path.
- **Silent 1M truncation.** `:66` calls `list(app, dataset, 1_000_000)`, and the query is
  `ORDER BY updated_at DESC LIMIT ?3` (`datasets.rs:1622-1631`) — past a million rows the
  **oldest** records are silently dropped from the rebuild and the summary reports the truncated
  count as success. Latent at today's scale, unbounded in principle, and it also materializes up
  to 1M parsed `Record`s in one `Vec`.

The user moment: *"We retired an app and tombstoned its records. Months later search was still
returning them. I ran the documented full rebuild twice — it said it succeeded both times."*

## Evidence (Director-verified)
- `crates/core/src/datasets.rs:1852-1860` — `list_all_datasets`, **`WHERE removed_at IS NULL`**.
- `crates/core/src/datasets.rs:1863-1871` — `datasets(app)`, no such filter. The asymmetry is the
  bug.
- `crates/server/src/bin/search-backfill.rs:136-159` — `resolve_targets`, unvalidated;
  `:56-59` the guard that misses the two-flag path; `:66` the 1M read; `:103-106` the summary.
- `crates/core/src/datasets.rs:1622-1631` — `ORDER BY updated_at DESC LIMIT ?3`.
- Coverage: `resolve_targets` has **zero tests** (grep returns the definition and one call site);
  the file's three tests cover `backfill_action` and a **hand-rolled reimplementation** of the
  loop body (`run_backfill`, `:260-275`), which by construction cannot catch a target-resolution
  bug.

## Acceptance criteria
1. `--all` reaches datasets whose records are entirely tombstoned, so the documented full rebuild
   can actually purge them. Decide deliberately whether the fix belongs in a new
   `Datasets` method or a flag on the existing one, and **do not change
   `list_all_datasets`'s behavior for its other callers** without checking them — grep first and
   say what you found.
2. A scope that matches nothing **says so and exits non-zero**, on every path including
   `--app X --dataset Y`. An operator who typos a dataset name must not get a success line.
3. The 1M ceiling is either removed (page/stream the read) or **reported honestly** when hit —
   "indexed 1,000,000 (truncated: more records exist)" is acceptable, silently indexing the
   newest million and calling it complete is not. State which you chose and why.
4. `resolve_targets` gets its **first tests** — it is the function both defects live in.
   Anti-pattern names (`a_fully_tombstoned_dataset_is_not_invisible_to_all`, `a_typod_scope_is_not_reported_as_success`).
   Confirm each fails against today's behavior first and say so.
5. The end-to-end loop gets coverage that could actually catch this class — the existing
   `run_backfill` test double reimplements the loop and is therefore blind to it. Either drive
   the real path or say plainly why you could not.
6. `docs/features/search.md` describes what `--all` really covers, and the recovery story is
   honest about the tombstone case.

## Risks / non-goals
- `crates/core/src/datasets.rs` is load-bearing for the whole fleet. Touch only what criterion 1
  needs, and check every caller of any function you change.
- No change to the search index schema, doc-id derivation, or `SearchDoc::from_dataset_record`
  (pinned by the test at `search-backfill.rs:204-215`).
- Not a rewrite of the binary's argv handling into a real CLI parser — banked separately on
  [[maintenance-tooling]] with the `reindex` guards. Fix the honesty defects, not the ergonomics.

## Build record
(to fill during build)
