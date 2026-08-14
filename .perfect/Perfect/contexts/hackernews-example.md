---
name: hackernews-example
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 2
last_proposed: 2026-08-14
cooldown_until: —
directions: ["[[hackernews-snapshot-floor]]", "[[hackernews-snapshot-extent-is-honest]]"]
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
- 2026-08-14 (r23): [[hackernews-snapshot-floor]] — **new, surfaced by a scout while REFUTING a
  different claim.** `hackernews/src/lib.rs:123` is the last full-snapshot `sync_many` in the
  fleet with no completeness floor (only an all-or-nothing empty check at `:99-106`), so HN
  markup drift that halves the parsed rows tombstones the complement, green. Every sibling that
  can tombstone now has one: smlouvy `PARSE_FLOOR` (r22), trades `COVERAGE_FLOOR` (r21), cordis
  `rollup_is_complete`, peer `tombstones_would_empty_the_mirror`. Not slated — reach is one
  example app, and it lost the cap to six directions with larger blast radius. `S`, because the
  pattern is shipped twice already and this is its third application.
### r24 — [[hackernews-snapshot-floor]] `9d6efb2`
The last unfloored full-snapshot write in the fleet now has a completeness floor, so a garbled page
cannot tombstone the rows it never read. **The floor as banked was unbuildable** (HN publishes no
total); it ships against the denominator the parser was throwing away — `tr.athing` rows *served* vs
stories parsed. Riders: `removed` is emitted (the `output_shape` promise stops being a lie), and the
`rank-N` fallback key is gone — rank is positional, so each run's `rank-7` had been overwriting a
different story and manufacturing fake `changed` revisions.
**Builder improvement, recorded:** it refused to gate tombstoning on the 30-rows-per-page signal I
suggested, because a genuinely shrinking front page would then make the app permanently upsert-only.
**Known gap carried:** the denominator premise — that HN drift removes `span.titleline` while leaving
`tr.athing` — is inferred from the parser's structure and has never been observed against live HN.
