---
slug: hackernews-snapshot-extent-is-honest
type: perfect/direction
context: "[[hackernews-example]]"
lens: robustness
status: rejected
size: S
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## What & why

**The template app teaches the wrong thing about full-snapshot writes.**

`hackernews` calls `sync_many` (`crates/apps/hackernews/src/lib.rs:123`) over a **caller-variable
snapshot extent**: `pages` (clamped 1..=5, `:73-78`) decides how much of the world the run sees,
while `sync_many` treats every batch as the whole world. So a `pages=3` run stores 90 stories, and
the next default run (`pages` absent → 1, `:77`) syncs 30 and **tombstones 60 rows that are still
live** on HN pages 2–3.

The same via the partial path: the empty-guard is on the *total* (`:99-106`), so page 1 parsing 30
while pages 2–3 return soft-rate-limit HTML parsing to 0 is a "successful" 30-story sync that
tombstones 60. This is the smlouvy defect ([[smlouvy-partial-parse-cannot-tombstone]], shipped this
round) in the app the README points new contributors at.

Secondary: `key = id.unwrap_or("rank-{n}")` (`:117`) — if HN drops the row `id`, keys go positional,
every run "changes" every rank key, and every real id tombstones at once.

## Evidence

- `:73-78` — `pages` clamp; `:77` — the default-1 fallback.
- `:99-106` — the empty guard, on the total only.
- `:121-123` — `sync_many` over that variable extent.
- `:117` — the positional key fallback.
- `crates/server/src/routes/mod.rs:398, 412` — `CATALOG_EXEMPT` reason: "an example/template app,
  not a production pipeline"; `:35-36` — `schedule()` is commented out, so it never runs unattended.
- `:225-257` — two tests, both on parsing. `drifted_markup_parses_to_zero_stories_not_garbage`
  (`:251-257`) correctly backs the total-guard. Nothing tests `run()` or the `pages` ↔ `sync_many`
  interaction.
- Also carried from r11 and still true: this app uses the **raw-engine bypass**
  (`ctx.engines.http.fetch`, `:82-88`) rather than the metered `ctx.fetch` chokepoint, and writes
  with `sync_many` rather than `sync_many_with_provenance` — an example that teaches the bypass is
  itself a defect. `crates/core/tests/fetch_chokepoint.rs:71` inventories it with a candid
  "Reviewed, not endorsed" note.

## Acceptance criteria (for whoever builds this)

1. The snapshot extent and the write mode agree: either `pages` is fixed for the sync path, or the
   write downgrades to `upsert_many` whenever the run did not cover the same extent as the last one.
2. The partial-page case (page 1 parses, pages 2–3 do not) cannot tombstone.
3. The positional key fallback is removed or made loud.
4. Whatever it does, it does **explicitly and with a comment**, because its readers are copying it.

## Risks / non-goals

- **Non-goal:** making this a production app. Its job is to be the simplest correct example.

## Why REJECTED this round

**Real defect, lowest blast radius in the round.** The app is unscheduled (`schedule()` commented
out, `:35-36`) and catalog-exempt precisely because it is a template, so nobody's data is at risk
today — the harm is entirely pedagogical.

Rejected on the 6-direction cap. But the pedagogical harm is the reason to build it soon rather than
never: this round shipped the *same* bug class in `smlouvy` as a genuine data-loss fix, and the
example app the README sends new contributors to still teaches it. **Banked for r23 with a strong
recommendation to pair it with the r11 anchor `hackernews-teaches-current-idioms`** (raw-engine
bypass + missing provenance) and to fix all three in one pass — an example app is worth touching
once, thoroughly.
