---
slug: grants-sweep-end-proven
type: perfect/direction
context: "[[us-federal-grants]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why

The federal grants sweep stops for four different reasons and calls three of them a complete
corpus. `truncated` is computed from the `maxPages` case alone:

```rust
let truncated = pages >= max_pages && start < hit_count;   // :307
if got < rows || start >= hit_count || pages >= max_pages { break; }   // :297
```

- **Short page.** A rate-limited or partially-served page 2 (HTTP 200, 100 hits instead of 1000)
  stops the walk at 1,100 of 1,366 records with `truncated: false`, no warning, and the drift
  guard silent because `hits` is non-empty.
- **`hitCount` rename.** `hit_count` reads `data.hitCount` with `unwrap_or(0)`. Rename the field
  and `hit_count = 0`, so `start >= hit_count` is `1000 >= 0` → break after page 1; the drift
  guard requires `hit_count > 0` so it never fires; `truncated` is false. The corpus caps at one
  page **indefinitely**, green every run.
- **`hitCount: 0` over a non-empty stored corpus.** Query-grammar drift answering
  `{errorcode: 0, data: {hitCount: 0, oppHits: []}}` produces
  `{fetched: 0, new: 0, changed: 0, unchanged: 0, warnings: []}` — a perfect run. Nothing is
  swept, nothing is flagged, and every unified row goes stale forever.
- **Mid-sweep `oppHits` drift on page ≥ 2.** `unwrap_or_default()` → `got = 0` → `got < rows` →
  break, `truncated: false`. The aggregate drift guard is satisfied because page 1 landed.

The comment above the `truncated` line claims this exact class is already fixed — *"a truncated
run was indistinguishable from a complete one"* — while covering one of the four arms. A contract
describing behavior the code does not have.

**This is an unfixed sibling instance**, the highest-value finding shape in this repo. Round 14's
`cordis-sweep-honesty` (`7f525e4`) shipped `SweepEnd::{Complete, Capped, ShortPage}` and
`empty_listing_is_drift` for precisely these two holes. `grep -n "SweepEnd\|ShortPage\|
empty_listing" crates/apps/grants-gov/src/lib.rs` → **0 hits**.

What a user loses: federal opportunities silently missing from `GET /grants`,
`GET /grants/closing-soon`, the search index and cross-source dedup — with a green job. Because
they are not past-due, `sweep_closed` never retires them, so they go stale rather than
disappearing, which is undetectable from outside.

## Evidence

- `crates/apps/grants-gov/src/lib.rs:297` — the four-way break.
- `crates/apps/grants-gov/src/lib.rs:307` — `truncated` from the `maxPages` arm only.
- `crates/apps/grants-gov/src/lib.rs:303-306` — the comment claiming the class is closed.
- `crates/apps/grants-gov/src/lib.rs:280` — `hitCount` via `unwrap_or(0)`.
- `crates/apps/grants-gov/src/lib.rs:286-290` — `oppHits` via `unwrap_or_default()`.
- `crates/apps/grants-gov/src/lib.rs:313-318` — the drift guard, gated on `hit_count > 0`.
- Sibling that already solved it: `crates/apps/cordis/src/lib.rs:510-568` (`SweepEnd`),
  `:586-589` (`empty_listing_is_drift`).
- `crates/apps/grants-gov/src/lib.rs:268` — `errorcode` absent read as success (rider).

## Acceptance criteria

1. Every way the walk can stop is a **named, distinguishable outcome** in the result, and only the
   arm that proves the corpus was covered reads as complete. Read `cordis`'s `SweepEnd` first and
   **layer on the existing concept rather than inventing a second vocabulary** — if you diverge
   from its shape, say why in the diff.
2. A short page, a mid-sweep empty `oppHits`, and a `maxPages` stop are each visible to a caller
   without reading logs — via the result and the `warnings[]` channel this app already has
   (`:552-567`).
3. `hitCount: 0` (or absent) while the stored corpus is non-empty is **drift, not a clean sweep**.
   Reuse cordis's `empty_listing_is_drift` reasoning; the count must come from the stored corpus,
   not from the same response being doubted. Note `Datasets::list` has **no `removed_at IS NULL`
   filter** (`crates/core/src/datasets.rs:1622-1633`) — if you count stored rows, decide
   explicitly how tombstones affect the comparison and say so.
4. `errorcode` absent or non-integer is not success (`:268`, `:850` — `"0"` as a string also
   misses `as_i64` today).
5. Tests. The crate's `ScriptedGrantsGov` (`:1411-1461`) answers **one fixed page regardless of
   `startRecordNum`**, which is why none of this was ever catchable. Extending it to serve a
   scripted page sequence IS part of this direction — pagination, short page, `maxPages`,
   `hitCount` rename, `hitCount: 0`, and mid-sweep drift each get a `run()`-level test named after
   the anti-pattern. The crate has no `tests/` dir; adding one is fine.

## Risks / non-goals

- **Non-goal**: adding a listing cursor. The full re-walk each run (`:239`) is stateless by
  design and is not the bug. (Do not assume — raise it if you disagree, don't act on it.)
- **Non-goal**: `Error::App` terminality. Deterministic drift burning the retry ladder is real and
  is **deliberately deferred this round** (its fix lives in `crates/core/src/error.rs`, which a
  sibling builder owns). Do not touch `crates/core/`.
- Hazard: making the empty-listing case an error means a genuinely empty legitimate result fails
  the job. Check whether `posted|forecasted` can legitimately return zero before choosing the
  lever, and prefer a comparison against the stored corpus over an absolute zero test.

## Build record

(filled during build)
