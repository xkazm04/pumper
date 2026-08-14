---
slug: hackernews-snapshot-floor
type: perfect/direction
context: "[[hackernews-example]]"
lens: robustness
status: proposed
size: S
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## What & why

**`hackernews` is the last full-snapshot write in the fleet with no completeness floor.** A markup
change that drops half the parsed rows tombstones the survivors' complement, green.

`crates/apps/hackernews/src/lib.rs:123` calls `ctx.sync_many("stories", &items)` with only an
all-or-nothing empty check at `:99-106` protecting it. `sync_many` runs `detect_removed`, so every
story that failed to parse this run is marked removed — and a run that parses 15 of 30 stories
reports success with no signal that it saw half a page.

This is exactly the defect the smlouvy `PARSE_FLOOR` (r22) and the trades `COVERAGE_FLOOR` (r21)
were built for. Every other app in the fleet that can tombstone now has a floor:

| app | floor | shipped |
|---|---|---|
| smlouvy-dump-watch | `PARSE_FLOOR = 1.0` + `removal_suppression_reason` | r22 |
| trades family | `COVERAGE_FLOOR = 0.9` + `may_tombstone()` | r21 |
| cordis | `rollup_is_complete()` → `upsert_many` at the cap | earlier |
| peer | `tombstones_would_empty_the_mirror` | earlier |
| **hackernews** | **none** | — |

Surfaced by the r23 scout while refuting [[observe-extraction-is-vacuous]] — it was in no banked
claim, and it is the honest residue of that refutation.

## Evidence

- `crates/apps/hackernews/src/lib.rs:123` — `ctx.sync_many("stories", &items)`.
- `:99-106` — the all-or-nothing empty check, the only existing protection.
- `crates/apps/smlouvy-dump-watch/src/lib.rs:104`, `:136-148`, `:196-208`, `:401-410` — the pattern
  to layer on, plus `tests/partial_parse_cannot_tombstone.rs` for the test shape.
- `crates/apps/trades-common/src/lib.rs:700`, `:840-842`, `:884-918` — the sibling pattern.
- Core cannot help: `[resilience] enforce` defaults false (`crates/core/src/config.rs:443`) and
  hackernews makes zero `observe_extraction` calls — see [[observe-extraction-is-vacuous]].

## Acceptance criteria (for whoever builds this)

1. A run that parses materially fewer stories than the page published **cannot** tombstone the
   difference — the write downgrades from `sync_many` to `upsert_many`, as smlouvy and cordis do.
2. The floor is an **extracted, named function** with a test named after the anti-pattern
   (repo doctrine), not an inline `if` in `run()`.
3. A counter-test proves a genuinely-shrinking-but-clean feed still tombstones — the guard must not
   turn the app upsert-only.
4. The run reports what it saw vs what it kept, so a partial page is visible rather than merely
   survivable.

## Risks / non-goals

- **Reach is small — this is an example app.** That is exactly why it lost the r23 slate to six
  higher-reach directions, and why it is `S` rather than `M`: the pattern is shipped twice already
  and this is its third application.
- **Non-goal:** `crates/core`. App-layer floor, like its two siblings.
- HN markup is genuinely volatile, which cuts both ways: it is the app most likely to hit this, and
  the floor must be tolerant enough not to false-positive on a normal short page.

## Status

**Banked r23** as the honest replacement for the refuted `observe_extraction` fleet-wide claim.
Not slated — lost the cap to six directions with larger blast radius (money leak on cancel, a
documented config causing a 10,000-record sweep, a permanently-dead trigger edge, ~10 GB of
re-download churn, and a fleet-wide retry loop).
