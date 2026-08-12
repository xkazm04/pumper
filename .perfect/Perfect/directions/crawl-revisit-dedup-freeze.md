---
slug: crawl-revisit-dedup-freeze
type: perfect/direction
context: "[[crawler-core]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---
## What & why
In **revisit** mode the near-duplicate gate is applied across *sibling pages of the same
run*, and it permanently freezes the records of templated sites.

Sequence for a known page whose body is within `dedup_distance` bits of another page already
fetched this run: `stats.revisited` is incremented, then the dedup branch counts
`skipped_duplicates` and returns **without touching the sink**. Its fresh `etag` /
`last_modified` are discarded, its `RevisitCadence` never advances, and its stored record —
including now-stale validators — is frozen. Next run it sends the same stale validator, gets
a full 200, is dropped again. Forever.

Cross-page content dedup is semantically wrong in a revisit at all: a sentinel recrawl
re-checks *known* pages against **their own history**, not against each other. Two different
product pages that share a template are not duplicates of one another for monitoring
purposes — that is precisely the set `REVISIT_SEED_LIMIT = 10_000` exists to sweep.

And the app ships `dedup_distance: 3` as the default in **all** modes, so this is the default
behavior, not an edge case.

The user moment: *"I set up a revisit crawl over my 400 product pages. Every run re-downloads
them, `skipped_duplicates` climbs, and the records never change — the monitor is dead and it
reports success."*

## Evidence
- Unconditional gate: `crates/core/src/crawl.rs:1004`
  (`cfg.dedup_distance > 0 && dedup_index.is_near_dup(hash)`).
- Revisit counters bumped first: `:999-1001`; the dedup early-return that skips the sink:
  `:1006-1007`.
- App default in every mode: `crates/apps/crawl/src/lib.rs:706` (`dedup_distance: 3`).
- Revisit seeds from stored `pages` with `REVISIT_SEED_LIMIT = 10_000`
  (`crates/apps/crawl/src/lib.rs:503-544`).
- The conditional-request map is the natural discriminator for "this URL is a known page
  being re-checked" (built in the revisit path, keyed by URL).
- **Why it survived:** `dedup_distance` is **0 in every end-to-end test** — `test_cfg`
  (`:1820`) and the filter test (`:2433`) are the only two settings in the file, both 0. The
  entire dedup gate through `crawl()` is unexercised.

## Acceptance criteria
1. A page being re-checked in revisit mode is never skipped as a near-duplicate **of another
   page**. Named predicate (not an inline condition) deciding whether the dedup gate applies
   to a given fetch, with the reasoning in its doc comment.
2. A revisited page's fresh validators and cadence reach the sink even when its body is a
   near-duplicate of a sibling — i.e. the record updates.
3. Fresh-crawl behavior is unchanged: cross-page dedup still suppresses duplicate NEW pages,
   proven by a test that would fail if the gate were simply removed.
4. **First end-to-end coverage of the dedup gate**: at least one `crawl()` test running with
   `dedup_distance > 0`. Both AC1 and AC3 rest on it. (This is the structural reason the bug
   shipped; a fix without it is unguarded.)
5. If the builder concludes self-vs-sibling cannot be distinguished cleanly at that point in
   the loop, STOP and return `DECISION NEEDED` with the options rather than widening the
   change into the revisit seeding path.

## Risks / non-goals
- Do not change `dedup_distance`'s default or its meaning for fresh crawls.
- Do not touch the app crate (its default is correct; the mode-blindness is core's).
- `crates/core/src/crawl.rs` ONLY.

## Build record
(to fill during build)
