---
slug: cordis-sweep-honesty
type: perfect/direction
context: "[[eu-grants]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 7f525e4
---

## What & why
The cordis corpus walk (23k Horizon projects at 500/week ≈ 46 weeks) can be wiped by
one transient short page: `got < page_size` sets `exhausted = true`, which both
claims `corpus_swept: true` in the result AND resets the persisted cursor to page 1 —
weeks of progress gone, and the operator is told the opposite of what happened. A
query-grammar drift that returns `total: 0` takes the same path (the
`normalized == 0` guard never fires because `attempted == 0`). Separately, a
maxProjects that isn't a multiple of pageSize silently skips the truncated tail for
an entire corpus cycle because the cursor advances past ids that were never fetched.

## Evidence
- Short page ⇒ exhausted: crates/apps/cordis/src/lib.rs:251-254; cursor wrap to 1
  :360-362; corpus_swept:true :384.
- total:0 drift hole: `attempted>0` precondition on the drift guard :335-341 — an
  empty listing yields attempted==0 ⇒ success + cursor reset.
- Tail-skip: `while ids.len() < max_projects` :198, `ids.truncate(max_projects)` :256,
  cursor persisted as page-past-the-tail :360.
- Existing checkpoint/restore machinery to preserve: :391-430 (stage2_state, restore
  gated on v+stage+start_page).

## Acceptance criteria
1. `exhausted`/`corpus_swept`/cursor-wrap happen only when the walk provably reached
   the end (page arithmetic against the listing's reported total, or an explicit
   empty-tail proof). A transient short page neither claims corpus_swept nor resets
   the cursor; the result distinguishes complete/partial/short-page honestly.
   Extracted predicate + `x_not_y` tests (repo doctrine).
2. `total: 0` (or empty page 1) against a previously non-empty local corpus is drift,
   not a clean sweep: loud failure or explicit drift outcome, cursor preserved. The
   attempted==0 hole at :335 is closed with a test named for it.
3. The maxProjects/pageSize truncation tail cannot be skipped for a cycle: the
   persisted cursor accounts for consumed ids, not just page count (the truncated
   tail is revisited next run). Test with maxProjects=450, pageSize=100.
4. Existing stage-2 checkpoint/restore tests stay green; `corpus_swept` semantics
   documented where the field is named (result docs / catalog notes).

## Risks / non-goals
- Risk: cursor semantics are persisted state — a shape change must tolerate the old
  `cordis/state` row (treat unknown/legacy as start-from-current, never panic).
- Non-goal: durable eu-sedia sweeps (banked separately); chokepoint migration for the
  raw GETs (fetch-discipline is inventoried; out of scope here).

## Build record
Shipped 7f525e4 (Lot E, opus). SweepEnd{Complete|Capped|ShortPage} + extracted
predicates (reached_listing_end / walk_end / empty_first_page_is_drift /
empty_listing_is_drift / cursor_offset / cursor_record); only page arithmetic
against the listing's own total wraps the cursor. Cursor became an absolute
OFFSET (survives pageSize changes; legacy page-only rows converted, nonsense →
top, never a panic); the 450/100 tail is revisited (e2e proves the literal
case). total:0-over-stored-corpus fails loudly via count_filtered gated to the
suspicious case; stage-2 checkpoint v2 keyed on offset (v1 discarded, bounded
cost). Builder refutations: the old first-page drift guard's start_page==1 gate
exempted EVERY scheduled run after the first (generalized to arithmetic); the
short-page clause fired alone only in the pathological case. 12 new tests incl.
4 e2e walks against a scripted CORDIS + real temp store. Review: KEEP.
