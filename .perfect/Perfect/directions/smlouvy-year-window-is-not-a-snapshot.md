---
slug: smlouvy-year-window-is-not-a-snapshot
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

**A per-run scope parameter mutates a global snapshot: `year_from` tombstones every dump outside the
window, and the parsed-share floor shipped in r22 structurally cannot see it.**

The trace (`crates/apps/smlouvy-dump-watch/src/lib.rs`):

1. `:343-347` — `year_from` read from params.
2. `:359` — `parse_dumps(&response.body)`; the floor's only inputs are `parse.blocks_seen` and
   `parse.dumps.len()` (`IndexParse::share`, `:136-141`).
3. `:376-381` — the narrowing, applied **after** the parse, with a comment that states the split
   plainly: *"`year_from` filters what we TRACK; it never changes what the index was seen to hold,
   so the floor below is judged on the parse, not on this."*
4. `:391-394` — the snapshot batch is built from `tracked`.
5. `:401` — `removal_suppression_reason(&parse)` — computed from **`parse`**, not `tracked`.
6. `:409` — `sync_many_with_provenance("dumps", &items, prov)` — a full-snapshot write.

So a clean 120-of-120 parse with `year_from: 2024` gives `share() == 1.0` → `is_partial() == false`
→ no suppression → `sync_many` runs with ~24 keys → `detect_removed` tombstones the ~96 pre-2024
dumps. Core's health guard cannot help: it is `Healthy` by default (`[resilience] enforce` defaults
false, `config.rs:443`) and this app makes zero `observe_extraction` calls.

**The floor cannot ever cover this, by construction.** `PARSE_FLOOR` (`:104`) is a *document-fidelity*
measure — "did we read every block the feed published?" The year window is a *request-scoping*
measure — "which of the blocks we read do we want?" `IndexParse` has no field for the second and no
way to acquire one; it is built before the filter exists. The floor's own test
`a_clean_parse_of_any_size_may_still_tombstone` (`:574-584`) deliberately asserts that a clean-but-
small batch *may* tombstone — which is exactly the case `year_from` manufactures.

**The user-visible cost is worse than "rows go missing", because the rows come back.** `dumps` is one
shared dataset. The daily scheduled default run and any consumer run with `year_from: 2024` alternate
tombstoning and resurrecting ~96 rows. Every resurrection lands in `summary.fresh_keys()` →
`fresh_dumps` (`:414`, `:433`) — which this app's own docs say a dataset trigger uses to *"fan out a
targeted re-download"* of ~100 MB files. **Each flip can trigger ~10 GB of downstream re-downloads.**
Two consumers with different `year_from` values on one dataset is a supported configuration today.

The app already admits the behavior in its params schema (`:300-305`): *"NOTE: raising it tombstones
the now-excluded dumps (the sync is a full snapshot)."* A documented footgun is still a footgun when
the blast radius is 10 GB and a churn loop.

## Evidence

- `crates/apps/smlouvy-dump-watch/src/lib.rs:376-381` — the narrowing and the comment that scopes the
  floor away from it.
- `:401` vs `:391-394` — suppression judged on `parse`, batch built from `tracked`.
- `:409` — the full-snapshot `sync_many_with_provenance`.
- `:104` `PARSE_FLOOR`, `:136-141` `share`, `:146-148` `is_partial`, `:196-208`
  `removal_suppression_reason`.
- `:300-305` — the params-schema note admitting the tombstoning.
- `:414`, `:433` — `fresh_dumps`, the trigger fan-out that turns a flip into re-downloads.
- `:574-584` — `a_clean_parse_of_any_size_may_still_tombstone`, the test that makes the gap explicit.
- `crates/apps/smlouvy-dump-watch/tests/partial_parse_cannot_tombstone.rs` — **0 occurrences of
  `year_from`.** The floor's test file has never considered this path.

## Acceptance criteria

1. A run with `year_from` set **cannot** tombstone dumps outside its window. Extract the decision
   into a named predicate/transform (repo doctrine: bug fixes ship as extracted, tested functions) —
   do not bury it in the `run()` body.
2. A run **without** `year_from` still tombstones genuinely-vanished dumps exactly as it does today.
   The fix must not turn the app into upsert-only; a counter-test must prove removal still works.
3. A test in `partial_parse_cannot_tombstone.rs` covers the year-window case, named after the
   anti-pattern it defends.
4. The params-schema note (`:300-305`) and `output_shape` (`:320-331`) say what the app now does
   instead of warning about what it used to do.
5. The existing parsed-share floor and its tests are untouched in behavior — this is a second,
   orthogonal guard, not a replacement.

## Risks / non-goals

- **Design call, and it is yours to make with justification:** the two honest options are
  (a) suppress removals whenever `year_from.is_some()` — simple, slightly conservative, matches the
  existing `removal_suppression_reason` seam; or (b) make the window a first-class part of the
  snapshot's identity so each window tombstones only within itself. (a) is a much smaller change and
  fits the shipped seam; (b) is more correct but touches dataset identity. **Prefer (a) unless you
  can ship (b) cleanly** — and if you take (a), say in the commit message what it does not solve.
- **Widening `IndexParse` will not work** — it is constructed before the filter is applied. Do not
  spend the session trying.
- **Non-goal:** `crates/core`. This is an app-layer scoping bug; core is not in the write set.

## Build record

(filled during build)
