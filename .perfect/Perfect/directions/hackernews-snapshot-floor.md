---
slug: hackernews-snapshot-floor
type: perfect/direction
context: "[[hackernews-example]]"
lens: robustness
status: accepted
size: S
proposed: 2026-08-14
accepted: 2026-08-14
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

---

## r24 re-verification (2026-08-14) — CONFIRMED as the last unfloored write; the SPECIFIED FLOOR IS REFUTED

**"Last one" holds.** Every `sync_many*` call site under `crates/apps/**`:

| site | floored? |
|---|---|
| `cordis/src/lib.rs:450` | yes — `rollup_is_complete` (`:448`, fn `:921`), falls back to `upsert_many` `:459`, emits `aggregate_truncated` `:495` |
| `smlouvy-dump-watch/src/lib.rs:510` | yes — `removal_suppression_reason` |
| `trades-common/src/lib.rs:893` | yes — `may_tombstone` inside `write_snapshot` |
| **`hackernews/src/lib.rs:123`** | **no** |

`crates/core/tests/removal_guard.rs:219-274` pins the allowed `detect_removed` sites, so nothing can
tombstone off-seam. hackernews is genuinely the last.

### REFUTED: the floor as this note specified it is UNBUILDABLE

The note said "parses materially fewer stories **than the page published**". **HN publishes no
total.** `parse_front_page` (`hackernews/src/lib.rs:139-198`) returns `Vec<Story>` only — the exact
signature defect smlouvy's `IndexParse` doc names at `:106-111`.

**But the denominator exists and is thrown away.** `doc.select(&row_sel)` (`:152`) enumerates every
`tr.athing`; `.filter_map` (`:154-155`) silently drops any row whose `span.titleline > a` is missing.
So the buildable floor is **parsed ÷ story-rows-served**, which is smlouvy's `blocks_seen` shape
exactly. Secondary: HN serves 30 rows/page and `pages` is clamped 1..=5 (`:73-78`), so a page
returning materially fewer than 30 `tr.athing` rows is a short page — detectable with no upstream
total. **A floor phrased "of the N stories the page claimed" cannot be built; "of the N story rows
the page served" can, and it is the same defect class.** This correction is load-bearing — build the
second phrasing.

### Two riders found in the same file, both worth more than the floor alone

**Rider 1 — `summary.removed` is dropped on the floor.** `output_shape` (`:63-67`) *promises* the
tombstoning (*"stories that fell off the listing are tombstoned"*), but the result (`:125-131`) emits
only `{count, new, changed, unchanged, stories}`. `UpsertSummary` carries `removed` and cordis reads
it (`cordis/src/lib.rs:494`). **The run that tombstones 15 of 30 stories and the run that tombstones
nothing produce byte-identical results** — so even after a floor lands, an operator cannot see
removals. Cheapest high-value fix in the sweep.

**Rider 2 — rank is used as a primary key.** `:117`
`s.id.clone().unwrap_or_else(|| format!("rank-{}", s.rank))`. Ids come from the `tr.athing` `id`
attribute (`:189`); when it is absent, the record is keyed `rank-7`. Rank is positional and changes
every run, so each run's `rank-N` record **silently overwrites a different story**, manufacturing
fake `changed` revisions and polluting the change feed — and with `pages > 1` the rank offset
(`:95`, `:188`) makes cross-page collisions avoidable only by accident. The parser test at `:252-257`
covers fully-drifted markup, but an id-less row inside *valid* markup takes the `rank-N` path, which
nothing covers.

**Reference patterns to copy** (read these, do not invent): smlouvy `:104` `PARSE_FLOOR`, `:112-122`
`IndexParse`, `:136-141` `share()` (returns 1.0 on a zero denominator), `:146` `is_partial`,
`:152-164` `to_json`, `:169-182` `warning`, `:196-208` `removal_suppression_reason` (the pure gate),
called `:493`; test file `tests/partial_parse_cannot_tombstone.rs` — note `:248`, the tombstone path
must stay reachable for a genuinely shrinking feed. trades `:700` `COVERAGE_FLOOR`, `:840-842`
`may_tombstone`, `:884-918` `write_snapshot`, plus `:825` `RESULT_FIELDS` + `:833`
`shape_declares_coverage` — the manifest-assertion trick that keeps `output_shape` honest.

**ACCEPTED r24** — it was rejected in r23 for small reach; it earns the slot now because it closes
the class fleet-wide (last of four) and the two riders are honesty defects in their own right.
Gate: director-self-gated (autonomous, Athena-dispatched).
