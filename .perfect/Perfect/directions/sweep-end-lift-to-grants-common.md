---
slug: sweep-end-lift-to-grants-common
type: perfect/direction
context: "[[eu-grants]]"
lens: optimization
status: rejected
size: M
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## The banked claim, and why "mechanical" is REFUTED

r22 banked: *"lifting `SweepEnd` into `grants-common` (it now exists in three apps — cordis,
grants-gov, ca-grants — with identical semantics, so the lift is mechanical)."*

Three definitions exist. **Two are identical; cordis's is not.**

| | ca-grants `:291-317` | grants-gov `:684-710` | cordis `:509-530` |
|---|---|---|---|
| variants | `Complete, Capped, ShortPage, UnknownTotal` | **identical** | **`Complete, Capped, ShortPage` — 3, no `UnknownTotal`** |
| `as_str` | 4 strings | identical | 3 |
| `walk_end` sig | `(page, limit, total, got, collected, max_pages)` `:337-369` | `(page, rows, hit_count, got, collected, max_pages)` `:732-765` — same fn, renamed params | **`(page, page_size, total, got, leftover, full)` `:547-568` — different arity semantics**, leans on `reached_listing_end()` (`:537-539`), orders `leftover → complete → full → short` |
| `sweep_warning` | `:378-405` | `:857-882` (prose differs only) | **none** — inlines a `ShortPage` warning at `:488-497` |
| depends on `grants-common` | yes | yes | **no** (`Cargo.toml`: `pumper-core, app-eu-sedia, async-trait, serde_json, url`) |

grants-gov's own doc already calls the divergence out (`:676-683`): *"**The one divergence**: cordis's
listing always publishes a usable total, so it has no equivalent of `SweepEnd::UnknownTotal`."*

All three are private, same derives, **no `Display`, no `serde`**, one associated fn. A lift into
`grants-common` would be purely **additive** — that crate (3038 lines) has nothing about sweep
endings, so no existing signature changes and no caller breaks.

**Scale:** occurrences are ca-grants 36 (21 non-test), grants-gov 35 (24), cordis 30 (17), with **19
`SweepEnd::X =>` match arms** across the three. A two-app lift moves ~45 non-test references plus two
`walk_end`s and two `sweep_warning`s.

## Why REJECTED — taste, not cap

`config.md → ## User taste` is three rounds of evidence that outcome-value work is accepted and
cosmetic churn is rejected. **A ~45-reference refactor whose stated payoff is "prevents the next
divergence" is the churn side of that line.** There is no user-visible symptom today: all three
enums work correctly in their own apps. Correctly scoped it is also not the one-session job the bank
implied — cordis needs a new cross-app dependency and a different `walk_end`, which is a separate
architectural decision, not a lift.

Note also the stale rationale it would fix, which is worth knowing but is not itself a reason to
build: `ca-grants/src/lib.rs:279-282` says the enum *"lives here rather than in `grants-common` only
because apps may not depend on apps and the shared crate was out of this change's write set."* The
first clause is **false today** — ca-grants already lists `grants-common.workspace = true`.

## The ONE outcome-value half, banked separately

**`eu-sedia` still has the collapsed boolean the enum exists to kill.**
`crates/apps/eu-sedia/src/lib.rs:230-231`:
```rust
let truncated = pages_fetched >= max_pages && (pages_fetched * page_size) < total;
```
with a warning at `:308-317`. One bool cannot distinguish "swept everything", "hit the page cap",
"the source short-paged us", and "the source never told us the total" — which is precisely the
honesty defect `SweepEnd` was invented for, in the one grant app that **already depends on
`grants-common`**.

That is a real, live honesty defect with a user-visible consequence (a truncated sweep reported the
same way as a complete one). **If a future round takes this context, build that — not the lift.**
Scope it as: *ca-grants + grants-gov → `grants-common`, then adopt in eu-sedia.* **cordis is out of
scope and is a separate decision.** The trades family has no equivalent — its `Coverage`/
`COVERAGE_FLOOR` measures roster coverage, an orthogonal axis.

---

## r24 — REJECTED A SECOND TIME (the lift), and the eu-sedia half was ACCEPTED as [[sedia-sweep-end-honest]]

Scout sizing re-measured at HEAD: **ca-grants 21 non-test refs, grants-gov 24** (= the 45 this note
recorded), cordis 17. Nothing changed, so nothing overturns the taste verdict: a 45-reference
refactor whose stated payoff is "prevents the next divergence", with no user-visible symptom today,
is the churn side of the line `config.md -> ## User taste` has drawn for four rounds.

**cordis stays rejected separately and for a stronger reason:** it does not depend on
`grants-common`, its enum has 3 arms not 4, and its `walk_end` uses *requested* arithmetic
(`cordis:550-552`) where the other two use *collected* — which is the exact bug `ca-grants:327-332`
and `grants-gov:719-726` record having replaced. Unifying it is an architectural decision, not a lift.

**What this note predicted, r24 built.** The "ONE outcome-value half" banked above became
[[sedia-sweep-end-honest]] — and re-verification made it considerably bigger than this note knew:
the `total = 0` path caps eu-sedia's corpus at **one page, green, indefinitely**, and disarms the
drift guard written for that same schema change. Scoped additive (enum lands in `grants-common`,
adopted in eu-sedia only, **zero lines changed in ca-grants/grants-gov/cordis**), it is a one-session
job — which is exactly the shape r22's log says a blast-radius-deferred direction should be rewritten
into.
