---
slug: grants-sweep-end-proven
type: perfect/direction
context: "[[us-federal-grants]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 12ae222
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

**Verdict: KEEP.** `12ae222`. `SweepEnd` layers on cordis's vocabulary (`Complete`/`Capped`/
`ShortPage`) rather than inventing a second one — and the two **deliberate divergences** are the
best judgment in the commit:

1. **Coverage is proven by records COLLECTED, not by page arithmetic.** cordis asks
   `page * page_size >= total`, which counts positions *requested*; the short-page bug is precisely
   a page that asked for 1000 and delivered 100, so that test reads `2000 >= 1366` and calls 1100
   records a full sweep. `walk_end` counts what actually arrived. The cost is stated rather than
   hidden: a racy upstream `hitCount` ends `ShortPage` with a warning instead of `Complete` — *"a
   false 'coverage unproven' is recoverable; a false 'corpus covered' is the failure that hides
   money."*
2. **`UnknownTotal`**, the arm cordis cannot have, because grants.gov's `hitCount` is read through
   `unwrap_or(0)`. This is the direction's headline bug — a rename makes `start >= hit_count` true
   at `1000 >= 0` — and it gets its own name because *the remedy is different*: nothing is wrong
   with the walk, the proof is missing. The `hit_count == 0 && got == 0` sub-case correctly reads
   `Complete` (self-consistent; drift is judged against the STORED corpus, never against the same
   response being doubted).

`start` is now **derived** from the page counter (`pages.saturating_mul(rows)`) rather than
accumulated, so the request offset and the arithmetic `walk_end` reasons about cannot drift apart —
a structural fix I did not ask for.

Beyond the criteria: `empty_page_is_drift` fires on **every** page, not just via the post-loop
`hits.is_empty()` check it subsumes, so a mid-sweep `oppHits` rename fails loudly instead of reading
as a short page; `empty_listing_is_drift` gates on `whole_corpus_query` because a
`keyword`/`eligibilities` pull may legitimately match nothing (**the manifest's own second example
does exactly that**); and `envelope_error` refuses an absent/null/unreadable `errorcode` on *both*
endpoints, where `as_i64().unwrap_or(0)` used to default every unrecognizable envelope to success —
with a stringified `"0"` accepted as the same integer, because both endpoints already publish
numbers as strings (`awardFloor: "55746"`), so that is a live possibility rather than a hypothetical.
