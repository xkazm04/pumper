---
slug: coalesce-unified-finalize
type: perfect/direction
context: "[[grants-unified-layer]]"
lens: optimization
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 139b064
---
## What & why
`finalize_unified` runs once per producer job — grants-gov, ca-grants, eu-sedia — and each run does
a full live-corpus pass over `grants/unified` twice: `sweep_closed`'s two unindexed `json_extract`
scans over every open/forecasted row, and `link_duplicates`' full live key+simhash read. The work is
identical and idempotent across producers; only the last one of the day changes anything. The code
already names the dedup as future work.

## Evidence
- Three call sites, once per producer: `crates/apps/grants-common/src/lib.rs:105-129`.
- Unindexed scans: `lib.rs:525-572` (`sweep_closed`, `limit 1_000_000`), and `list_filtered`'s own
  admission of a full partition scan at `crates/core/src/datasets.rs:1558-1568`.
- Full live corpus read per run: `crates/core/src/datasets.rs:1420-1427` via `duplicate_pairs`.
- The flag: `crates/apps/grants-common/src/lib.rs:534-535`.

## Acceptance criteria
- The corpus-wide passes run once per cycle, not once per producer.
- Correctness preserved when producers run at different times, out of order, or one fails entirely —
  a skipped producer must not strand the sweep.
- Measured reduction in reads/writes per day, reported with real numbers.
- No new race between concurrent producers (two jobs finishing together must not double-sweep or
  interleave a partial link set).
- Reuses the existing schedule/trigger machinery — no parallel scheduler.

## Risks / non-goals
Do not weaken freshness: a grant that closed today must still be swept today. Not a rewrite of the
sweep predicate itself (see [[close-date-timezone-honesty]]).

## Build record
(pending)
