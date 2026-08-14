---
slug: ca-grants-sweep-says-what-it-proved
type: perfect/direction
context: "[[us-state-grants]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## What & why

**The identical bug the sibling app already found, named and fixed — never carried across.**

r20 shipped grants-gov's sweep honesty: the Search2 walk stops for four different reasons and only
one proves the corpus was covered, so the run reports `sweep: complete | capped | short_page |
unknown_total`. The ledger records what the `unknown_total` arm was worth: *"the renamed-`hitCount`
case capped the federal corpus at one page indefinitely"*.

`ca-grants` has the same walk and none of the vocabulary. `total` is read **once, from page 1
only**, `unwrap_or(0)` (`:164`). The break is `got < limit || offset >= total || pages >= max_pages`
(`:179`). So if CKAN renames, moves or drops `result.total`:

- `total = 0` → `offset >= total` is true immediately after page 1 → **the walk stops at one page**;
- `truncated = pages >= max_pages && offset < total` (`:186`) → `1 >= 100` is false → **`truncated:
  false`**;
- the drift guard (`:192-197`) is gated on `total > 0`, so with 1000 records parsed it never fires.

Net: **the California corpus is capped at one page indefinitely while the result reports a clean,
complete, untruncated sweep.** The short-page case is the same shape — `got < limit` breaks with
`truncated: false` even when `total` says more remains, which is precisely the rate-limited page
grants-gov reports as `short_page`.

The user moment: California posts 900 grants, Pumper stores the first 100, `GET /grants` returns
them as the state's complete open set, and `sweep_closed` keeps confidently judging a corpus it has
seen a ninth of. Blast radius is bounded — the app is upsert-only (`:212`), so nothing is
tombstoned — but the un-fetched grants simply stop being refreshed, and nothing anywhere says so.

## Evidence

- `crates/apps/ca-grants/src/lib.rs:163-167` — `total` read only when `pages == 0`, `unwrap_or(0)`.
- `:179` — the three-arm break; the `offset >= total` arm fires on page 1 when `total == 0`.
- `:184-186` — `truncated` computed from the `max_pages` arm alone, so it cannot see the other two.
- `:190-197` — the drift guard, structurally blind while `total == 0`.
- `:104-115` — `limit` 1..1000 × `maxPages` 1..100.
- `:228` — `finalize_unified` runs the cross-source sweep over the corpus regardless.
- `docs/features/apps.md` § grants-gov "sweep honesty" — the four-arm vocabulary, its rationale, and
  the record that the unknown-total arm was a live indefinite cap. **Mirror it; do not invent a
  second vocabulary.**
- `crates/apps/grants-gov/src/lib.rs` — the reference implementation. Read it before writing.

## Acceptance criteria

1. The run reports **`sweep`** with the same four values and the same meanings as grants-gov:
   `complete` (records *collected* reached the server's own total — counted on records delivered,
   not offsets requested), `capped`, `short_page`, `unknown_total`. Reuse the sibling's semantics
   verbatim; a third dialect of this vocabulary would be worse than the bug.
2. `truncated` stays as the boolean projection (`sweep != "complete"`) so no existing consumer
   breaks, but it is now **computed from `sweep`**, not from the `max_pages` arm.
3. Every non-`complete` arm also lands in `warnings[]`, as grants-gov does.
4. An **absent or unreadable `result.total` no longer ends the walk.** It reads as
   `unknown_total`, the walk continues to a short page or the cap, and coverage is reported
   unproven. A `total` that is present and genuinely `0` with zero records is still the ordinary
   empty answer, not drift — pin that boundary with a test.
5. `output_shape` (`:82-92`) declares the new field. While you are there its `unified` block
   declares **three** keys where the shared `grants_common::merge_into` writes **six** — the exact
   drift grants-gov's declaration was corrected for (`{new, changed, events, dataset, trust,
   sourceState}`). Correct it in the same commit.
6. Tests cover the four arms. At minimum: a renamed `total` with a full page must **not** report
   `complete`, and must fail against today's code — run it unmodified first and say so.
7. `docs/features/apps.md` is **Director-owned this round**. Report your doc text; do not edit it.

## Risks / non-goals

- **Non-goal:** `max_row_delta_pct = 40.0` in this app's catalog row. It is inert (the app is
  upsert-only, and `removed` is populated only by `sync_many`) and the TOML comment already admits
  it. Real, banked, but it is a `catalog/` edit and a separate concern from the walk.
- **Non-goal:** a full `tests/result_contract.rs` for ca-grants. Banked — criterion 5 fixes the one
  block that is provably wrong today without standing up the machinery.
- **Non-goal:** `required_fields = ["PortalID"]` accepting `""`. That is a `Contract::evaluate`
  limitation (presence + non-null, no non-blank notion) and belongs to the catalog context.
- **Risk:** changing when the walk stops changes how much the daily job fetches. `maxPages` (100)
  still bounds it; the run must not become unbounded on an `unknown_total` feed.

## Build record

(filled during build)
