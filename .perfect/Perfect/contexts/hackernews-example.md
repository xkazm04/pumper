---
name: hackernews-example
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 2
last_proposed: 2026-08-14
cooldown_until: —
directions: ["[[hackernews-snapshot-extent-is-honest]]"]
---

## Current state
**Scouted 2026-08-14 (round 22), as part of the six-context "thin app + its catalog row" family
brief — COVERED.** Files: crates/apps/hackernews/src/lib.rs (258 lines, 2 tests).

The r11 read ("verdict-shaped: its job is to be simple and current") was right about the *job* and
wrong about the *state*. Findings:

- **Declaration hygiene is clean** — `output_shape` (`:63-67`) matches the emitted JSON
  (`:125-131`) exactly, and even documents the tombstoning. No drift. The catalog exemption is
  documented and consistent (`routes/mod.rs:398, 412`), and `schedule()` is commented out
  (`:35-36`) so it never runs unattended.
- **One real defect: the snapshot extent is caller-variable while the write is full-snapshot.**
  `pages` (1..=5) decides how much of the world the run sees; `sync_many` (`:123`) treats every
  batch as the whole world. → [[hackernews-snapshot-extent-is-honest]]. **This is the same bug
  class r22 shipped as a genuine data-loss fix in `smlouvy`
  ([[smlouvy-partial-parse-cannot-tombstone]]) — and it is still being taught here, in the app the
  README points new contributors at.**
- Bounded at 5 pages / ~150 stories. The full `stories` vec is inlined into the job result (`:130`)
  as well as the artifact — mild friction with ONBOARDING §6.

## Direction history
- **2026-08-14 (round 22): PROPOSED — 1 direction, REJECTED on the 6-direction cap.**
  [[hackernews-snapshot-extent-is-honest]] is a real defect with the round's **lowest blast radius**
  (unscheduled, catalog-exempt, template-only), so it lost every slot contest to live data loss and
  false user-facing alerts. Banked for r23 with a recommendation to fix it **together with** the
  r11 anchor below in one thorough pass — an example app is worth touching once.
- 2026-08-12 (round 11): scouted (medium); candidates exist — banked, not slated (cap). NOT
  covered yet. Anchors:
  1. **hackernews-teaches-current-idioms** (S): the canonical template uses the raw-engine
     bypass (ctx.engines.http.fetch, lib.rs:82-88) instead of the metered ctx.fetch chokepoint,
     writes without provenance (1 of only 2 apps left — sync_many vs sync_many_with_provenance),
     and README.md:195's snippet teaches ctx.engines.browser.render. An example that teaches
     the bypass IS a defect. Pairs with the chokepoint-guard fix (see below).
  2. Cross-context find (owned by app-runtime; Director-committed r11): the fetch_chokepoint
     guard scanned line-by-line and rustfmt's chain-wrapping made 9 raw-engine sites invisible
     (6 files entirely unreviewed incl. this one). See r11 session note.
  Checked-and-current: rich manifest, CostClass::Free, zero-parse drift guard, catalog
  exemption documented.

## Shipped
- (none on this map)
