---
slug: sedia-sweep-end-honest
type: perfect/direction
context: "[[eu-grants]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## What & why

**eu-sedia caps its corpus at ONE PAGE, green, indefinitely, if `totalResults` is renamed —
and the drift guard written for exactly that schema change is disarmed by it.**

`crates/apps/eu-sedia/src/lib.rs:188-191` reads `total` with `.unwrap_or(0)`. With `total = 0`:

- the drift guard at `:201-211` is gated `total > 0 && got == 0` — **cannot fire**;
- the loop break at `:224` tests `(pages_fetched * page_size) >= total`, i.e. `100 >= 0` — **true
  after page one**;
- `truncated` at `:233` requires `pages_fetched >= max_pages` — **false**.

So a renamed field turns a 2,000-topic sweep into a 100-topic corpus that reports a clean run. This
is word-for-word the defect `ca-grants/src/lib.rs:289-295` and `grants-gov/src/lib.rs:681-688`
document having killed in their own apps. **eu-sedia is the unpatched third instance.**

A second silent-partial path exists independently: a **short-served page** exits at `:224` via
`got < page_size` and also reports `truncated: false`. cordis calls the silent version of this bug
worth *"~46 weeks of walk"* (`crates/apps/cordis/src/lib.rs:502`).

One boolean cannot distinguish *swept everything* / *hit the page cap* / *source short-paged us* /
*source never published a usable total*. That is precisely what the `SweepEnd` enum in three sibling
apps exists to express — and eu-sedia is the one grant app that **already depends on
`grants-common`** (`:291-293` calls `grants_common::normalize_eu_sedia` / `finalize_unified`).

## Evidence

- `crates/apps/eu-sedia/src/lib.rs:188-191` (`unwrap_or(0)`), `:201-211` (disarmed guard), `:224`
  (break), `:233` (`truncated`), `:295-308` (result), `:117-129` (`output_shape` publishes
  `truncated`), `:310-326` (warning).
- Reference: `crates/apps/ca-grants/src/lib.rs:296-311` (enum), `:342-374` (`walk_end`), `:378-405`
  (`sweep_warning`) — semantically byte-identical to `grants-gov/src/lib.rs:689-704`, `:737-770`,
  `:857-882`.
- `crates/apps/ca-grants/src/lib.rs:286-287` explicitly nominates `grants_common` as the enum's
  eventual home — the direction is pre-blessed by the code's own comment.
- `grants-common` has **no** `SweepEnd` today; adding one is purely additive.
- `crates/apps/grants-gov/src/lib.rs:772-779` `empty_listing_is_drift` — the corpus-relative guard
  shape to copy for the rider.

## Acceptance criteria

1. A sweep that ended for a reason other than "swept the whole corpus" is **distinguishable in the
   result** from one that did — the four cases above are separately nameable, not one boolean.
   Adopt in **eu-sedia only**.
2. The enum + its `walk_end`/`sweep_warning` land in `crates/apps/grants-common` as a **purely
   additive** public surface: **not one existing line of `ca-grants`, `grants-gov` or `cordis` may
   change.** If your change requires editing any of those three, stop and report — the ~45-reference
   lift was rejected twice and is not in scope.
3. **Rider, and it does not come for free:** re-arm the drift guard. Fixing the sweep enum does
   *not* fix `:201-211` — a renamed `totalResults` and a renamed `results` array are the same schema
   change, and the guard must survive `total = 0`. Use the corpus-relative test shape from
   `grants-gov`'s `empty_listing_is_drift`, not the `total > 0` gate.
4. A test per silent-partial path: the `total = 0` one-page case, and the short-served-page case.
   Named after the anti-pattern (`x_not_y` style), per repo doctrine.
5. `output_shape` (`:117-129`) matches what `run()` actually emits, and `docs/features/apps.md`'s
   eu-sedia row says what the new field means.
6. **Rider (one line, you are already in the file):** `grants_common::sync_unified`
   (`crates/apps/grants-common/src/lib.rs:552`) is an `upsert_many_stamped` (`:568`) wearing a
   `sync_*` name, which in this codebase is the load-bearing signal for "absentees tombstoned"
   (`crates/core/src/app.rs:648-692`, policed by `crates/core/tests/removal_guard.rs`). **Do not
   rename it** — add a doc comment saying it does not tombstone, so a future contributor does not
   "restore" removal behaviour the name implies.

## Risks / non-goals

- **Non-goal: the lift.** ca-grants (21 non-test refs) and grants-gov (24) keep their private copies.
- **Non-goal: cordis.** It does not depend on `grants-common`, its enum has 3 arms not 4, and its
  `walk_end` uses *requested* rather than *collected* arithmetic — a separate architectural decision.
- **Blast radius is coverage dishonesty, not data loss** (verified): `grants_common::sync_unified` is
  an upsert and eu-sedia's own write is `upsert_many_with_derived` (`:273-283`), so a truncated run
  cannot tombstone. Do not oversell the fix.

## Build record
