---
slug: topic-stats-honesty
type: perfect/direction
context: "[[eu-grants]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
The funded-outcome stats are the product of the cordis app — and eu-sedia embeds them
verbatim into every Horizon topic as `history.stats`. But during the ~46-week corpus
walk they are partial-corpus aggregates presented as truth: "3 projects funded, mean
€2.1M" carries no as_of, no corpus size, no coverage — indistinguishable from a
complete answer. Ghost families persist forever (the rollup is a complete recompute
written with upsert_many, so a family that left the corpus keeps its stale row and
eu-sedia keeps joining it), and the 200k aggregation limit truncates silently.

## Evidence
- Stats record shape (no as_of/coverage): crates/apps/cordis/src/lib.rs:706-715.
- Rollup complete-recompute written with upsert_many (no removals): :344-357.
- AGGREGATE_LIMIT=200_000 silent, ORDER BY updated_at DESC LIMIT (no tombstone
  exclusion): :71, :344-352 + core datasets.rs:1603-1614.
- eu-sedia embeds with only {family, source}: crates/apps/eu-sedia/src/lib.rs:250-256.
- registry.rs:82 lists cordis as a grants virtual-namespace publisher — it publishes
  nothing into grants/*; publishes_into() lies to watch-placers.
- Catalog: cordis row declares only `projects`; topic_stats (the load-bearing joined
  output) has no contract/freshness row (catalog/data-sources.toml:609-643).

## Acceptance criteria
1. topic_stats records carry honest coverage: as_of + aggregated corpus size (and the
   listing total when stage 1 knows it) so a reader can tell "3 of ~23k walked" from
   "3 of 3". eu-sedia's embedded history block carries that context through.
2. The rollup writes with removal honesty (sync_many or equivalent): a family that
   left the corpus disappears; eu-sedia joining a removed family yields no history
   block. Test proves the ghost dies.
3. AGGREGATE_LIMIT gets a tripwire (corpus ≥ limit ⇒ loud signal, never silent
   truncation); the rollup read excludes tombstoned rows.
4. Rider: registry.rs cordis-as-grants-publisher fixed (it publishes nothing into
   grants/*) with the guarding test tightened or the removal reasoned in the report.
5. topic_stats gets a catalog contract row, or the omission is explicitly reasoned in
   the report and documented — builder judgment.

## Risks / non-goals
- Risk: sync_many on topic_stats changes removal semantics for a dataset consumers
  may watch — the removals are the CORRECT behavior (complete recompute), but state
  the transition in the docs/commit.
- Coordinate with [[cordis-sweep-honesty]] (same lot): coverage fields should use the
  sweep-honesty walk state where natural (e.g. corpus_swept ⇒ coverage complete).
- Non-goal: making cordis a grants/unified producer — out of scope, different design.

## Build record
(pending)
