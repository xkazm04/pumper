---
name: czech-procurement
type: perfect/context
group: Grants Intelligence
category: lib
opportunity: 4
last_proposed: 2026-08-14
cooldown_until: r24 (mined r22)
directions: ["[[smlouvy-year-window-is-not-a-snapshot]]", "[[smlouvy-partial-parse-cannot-tombstone]]"]
---

## Current state
Not yet scouted on the 46-map. Files: crates/apps/smlouvy-dump-watch/src/lib.rs.
Czech contract-registry (smlouvy) dump watcher. Never swept on any map. Strategic fit
note: Politicas' FollowTheMoney arc consumes public-money trails — this source feeds
that pipeline.

## Direction history
- (none)

## Shipped
- **2026-08-14 (r22) [[smlouvy-partial-parse-cannot-tombstone]] `a88af1c`** — `parse_dumps` returns
  `IndexParse` (blocks seen + skips by reason); `dumps_in_index` now means blocks SEEN with
  `dumps_parsed` beside it; below a parsed-share floor of **1.0** the write downgrades
  `sync_many_*` -> `upsert_many_*` and says so in `warnings[]` + `removals_suppressed`.
  **Observed effect:** a 30-of-51 feed can no longer tombstone the 21 dumps it merely failed to
  read, and is no longer byte-identical to a clean run. Tombstone path stays reachable — a
  shrinking-but-clean index parses 100% and still removes (`a_shrinking_but_clean_index_still_tombstones`).
  Verified destructive: at floor 0.0 (pre-fix) the regression test fails with 3 live dumps of 5.
- (none on this map)
- 2026-08-14 (r23): [[smlouvy-year-window-is-not-a-snapshot]] -> `de3af27` — `year_from` is
  a per-run **scope** mutating a global **snapshot**, and r22's parsed-share floor
  structurally cannot see it (the floor is document-fidelity; the window is request-scoping,
  and `IndexParse` is built before the filter exists). A clean 120-of-120 parse with
  `year_from: 2024` had `share: 1.0` and tombstoned the ~96 pre-2024 dumps. Worse, they came
  back: the daily unwindowed run and a windowed consumer alternated deleting and resurrecting
  them, and each resurrection lands in `fresh_dumps`, which a trigger fans out as ~10 GB of
  re-downloads. **The builder improved on the brief**: the guard keys on what the window
  ACTUALLY excludes, not on `year_from.is_some()` — so a window that excludes nothing keeps
  full-snapshot semantics and a consumer pinned at 2016 still sees a month retired.
