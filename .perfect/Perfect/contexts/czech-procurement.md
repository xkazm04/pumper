---
name: czech-procurement
type: perfect/context
group: Grants Intelligence
category: lib
opportunity: 4
last_proposed: 2026-08-14
cooldown_until: r24 (mined r22)
directions: ["[[smlouvy-partial-parse-cannot-tombstone]]"]
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
