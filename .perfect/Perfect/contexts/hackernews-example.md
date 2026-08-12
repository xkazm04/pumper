---
name: hackernews-example
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 2
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: crates/apps/hackernews/src/lib.rs. The canonical
example app (README's "adding a use case" reference). Verdict-shaped: its job is to be
simple and current, not featureful.

## Direction history
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
