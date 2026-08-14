---
slug: smlouvy-partial-parse-cannot-tombstone
type: perfect/direction
context: "[[czech-procurement]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## What & why

**A partially-garbled feed silently deletes live records, and the result JSON reports the same
numbers as a clean run.** This is round 21's headline defect — a partial harvest tombstoning rows
that still exist upstream — repeating in a different app, and here it is completely unguarded.

`parse_dumps` skips any `<dump>` block missing `<odkaz>` or with an unparseable `<rok>`/`<mesic>`
with a bare `continue` (`:97-106`). It counts nothing. The only guard is all-or-nothing
(`dumps.is_empty()`, `:216-221`), so a feed publishing 51 dumps of which 30 parse sails through it
and lands in `sync_many_with_provenance` (`:235`) — a **full-snapshot** write whose removal
detection tombstones the 21 keys absent from the batch.

The repo's own defence is stated in `sync_many`'s doc and **structurally cannot engage for this
app**: *"`detect_removed` already refuses an empty batch; **a partial batch is the case that guard
does not cover**"* (`crates/core/src/app.rs:642-643`). The downgrade to `upsert_many` fires only
when source health suppresses removals — and this app makes **zero** `observe_extraction` calls, so
its health is never judged, while `[resilience] enforce` defaults false and is documented
"currently inert" (`crates/core/src/config.rs:98-100`).

And it is **unobservable**. `dumps_in_index` (`:253`) is `total_parsed` (`:222`) — computed *after*
parsing. The field whose name promises "how many dumps the index contains" reports "how many we
managed to parse", so a 30-of-51 run and a 30-of-30 run emit byte-identical results. The user
moment: the Ministry ships one malformed month, a sibling product's tender radar loses eight years
of dump pointers overnight, and every number the operator can see says the run was clean.

## Evidence

- `crates/apps/smlouvy-dump-watch/src/lib.rs:97-106` — the two silent `continue`s. No counter.
- `:216-221` — the empty-only guard.
- `:222` / `:253` — `total_parsed` is post-parse, published as `dumps_in_index`.
- `:235-244` — `sync_many_with_provenance`, full-snapshot, removal-detecting.
- `crates/core/src/app.rs:630-648` — `sync_many`'s doc naming the partial batch as the uncovered
  case and the health downgrade as the mitigation.
- `grep observe_extraction crates/apps/smlouvy-dump-watch/` → **0 hits**. The mitigation is
  unreachable for this app.
- `crates/core/src/config.rs:90-100` — `enforce` default false, "currently inert".
- `:350-360` — **`skips_entries_missing_url_or_date` asserts the silent skip as correct behavior**
  ("only the complete entry is kept"). The suite is green over the production failure. Fixing the
  code means confronting this test, not deleting it.
- Second tombstoning path: `year_from` (`:223-225`) retires the excluded history. Documented as a
  NOTE at `:162-166`, but `default_params` carries only `index_url` (`:144-146`) and params
  shallow-merge, so one manual `{"year_from": 2024}` permanently tombstones 2016–2023.

## Acceptance criteria

1. `parse_dumps` returns what it **saw** as well as what it kept — block count and skip count, with
   the skip reasons distinguishable (no `odkaz` vs unparseable date). A pure function with its own
   test, per `.claude/CLAUDE.md`'s extracted-function doctrine.
2. `dumps_in_index` means what its name says: blocks **seen** in the index. The parsed count is
   reported alongside under its own key so a 30-of-51 run is distinguishable from a 30-of-30 run in
   the result JSON. Name the new key in your report — it is a consumer-visible result field.
3. **A partial parse must not tombstone.** When the parsed share falls below a floor, the write
   downgrades from `sync_many_*` to `upsert_many_*` and the run says so — loudly, in `warnings`
   and in the result. Pick and justify the floor in your report; a skipped block is a strong signal
   (the feed is small and hand-shaped), so the floor should be tight rather than generous.
4. `:350-360` is **updated, not deleted**: keeping the complete entry is still right, but the test
   must now also assert the skip was *counted*. A test that still passes unchanged means the fix
   did not reach the seam the test guards.
5. A test drives the whole partial-harvest path end to end and proves no key was tombstoned —
   that is the acceptance, not the counter.
6. `docs/features/apps.md` is **Director-owned this round**. Report your doc text; do not edit it.

## Risks / non-goals

- **Non-goal:** wiring `observe_extraction` into this app. It is the right long-term answer and it
  is a fleet-wide change (every app in the family makes zero calls); this direction closes the hole
  at the app that can actually lose data today. Bank the fleet sweep.
- **Non-goal:** the `year_from` tombstoning path. Real, separately banked — it is a deliberate
  param doing a documented thing, and conflating it with the drift case muddies both.
- **Risk:** the floor is a judgement call. Too generous and it never fires; too tight and a
  legitimately-shrinking index stops tombstoning. State your reasoning; the tombstone-suppressed
  path must stay *reachable* on a genuinely shrinking feed.

## Build record

(filled during build)
