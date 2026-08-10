---
slug: politeness-memory-honesty
type: perfect/direction
context: "[[tiered-fetcher]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-11
commit: a69420a
---

## What & why
Learned politeness state lies across restarts. A host whose penalty decayed back to
zero (it recovered) keeps its old penalty_ms row forever and is resurrected at FULL
penalty on every server boot — recovered hosts stay throttled indefinitely. An
operator's `DELETE /hosts/{host}/memory` can be silently undone by the 60s
write-behind tick. And tier_memory rows are never reclaimed, one per host ever
touched. The learned-politeness feature should tell the truth the router acts on.

## Evidence
- `crates/core/src/governor.rs:233–239` — snapshot filters !penalty.is_zero().
- `crates/core/src/tiers.rs:191–212` — save_penalties upserts only snapshot entries;
  a zeroed host's row is never updated.
- `crates/core/src/tiers.rs:216–225` — load_penalties: WHERE penalty_ms > 0, no age
  filter → full-strength resurrection on boot.
- `crates/server/src/routes/runtime.rs:120–121` — forget() then clear(); write-behind
  tick between them re-persists the live penalty.
- `crates/server/src/state.rs:332–352` — the 60s write-behind loop.
- No GC on tier_memory anywhere (scout §6; prune_ledgers at storage.rs:1658 excludes it).

## Acceptance criteria
- [ ] A decayed-to-zero penalty is zeroed in the DB (snapshot includes zeros for
      previously-persisted hosts, or the save pass zeroes rows absent from the
      snapshot) — extracted named fn + anti-pattern test
      (e.g. zombie_penalty_not_resurrected_on_boot).
- [ ] load_penalties honors an age bound tied to the existing host-memory TTL
      (penalty_updated_at) — test.
- [ ] DELETE /hosts/{host}/memory survives the write-behind race (ordering fix or
      tombstone — builder proves it with a test, not an argument).
- [ ] Stale tier_memory rows (no pin, no strikes, no penalty, updated_at beyond TTL)
      are pruned via the existing janitor/retention machinery, bounded per pass — test.
- [ ] GET /hosts stays honest (aged-pin display semantics unchanged).
- [ ] Doc-sync: fetching.md politeness/host-memory section.

## Risks / non-goals
- Do not change decay constants or strike limits.
- apply_weather's aging-clock reset (tiers.rs:420–450) is out of scope — note it in
  the doc's known-gaps if touched territory.
- The write-behind and the janitor share the pool — keep each pass one bounded
  transaction; no full-table scans per tick.

## Build record
Shipped `a69420a` (Lot F, opus, 2026-08-11). persist_penalty_snapshot (authoritative,
zero-then-rewrite one transaction) for the write-behind; save_penalties stays additive
— builder refuted my either/or criterion: routes/host_weather.rs:231 calls it with a
PARTIAL list, so making it authoritative would have made every weather import wipe
unmentioned hosts (Director verified the import also raises the LIVE governor penalty,
so the authoritative pass preserves imported state). penalty_is_restorable ages boot
restore on the host-memory TTL; undatable rows not restored. DELETE race: order swap
(live first) + AppState::host_memory_lock across snapshot→commit — order alone leaves
the snapshot-before/commit-after window. prune_stale in the renamed always-on
store_janitor (retention_janitor is off-by-default → would have shipped dead), ≤1000
rows/pass, only rows saying nothing. Review: keep.
