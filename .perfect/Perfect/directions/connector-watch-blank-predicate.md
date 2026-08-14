---
slug: connector-watch-blank-predicate
type: perfect/direction
context: "[[connector-api-watch]]"
lens: optimization
status: rejected
size: S
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## The banked claim — REFUTED on both halves

r22 banked: *"`connector-api-watch`'s third copy of `extracted_nothing` (one-line change to the core
function)."*

**There is no third copy, and the core function needs no change at all.**

The core fn, `crates/core/src/extract.rs:645-647`:
```rust
pub fn extracted_nothing(text: &str) -> bool {
    text.trim().is_empty()
}
```
Not in the root re-export (`crates/core/src/lib.rs:122-125`), so the path is
`pumper_core::extract::extracted_nothing`.

Every user across `crates/apps/`:
- `readable/src/lib.rs:6` + `:136` — **imports the core fn.**
- `watch/src/lib.rs:10` + `:134` — **imports the core fn.**
- `connector-api-watch/src/lib.rs:260` — `if markdown.trim().is_empty() {` — an **inline predicate**.
  `grep extracted_nothing crates/apps/connector-api-watch/` returns **0 hits**.

So: **1 definition, 2 importers, 1 inline duplicate.** A third *site*, not a third *copy*.

**"A one-line change to the core function" would be nothing** — the core fn is already
`text.trim().is_empty()`, and changing it would silently change `readable`, `watch`, and `is_blank`
(`extract.rs:655`, whose `String` arm delegates here). The real work is **two lines in one app file**:
add the `use`, rewrite `:260`. Zero behavior change. **The banked framing pointed at the wrong file.**

## The divergence that must be preserved if anyone does touch it

The predicate is identical; the *handling* is deliberately not:
- readable `:139-143` → `Err(Error::App(...))`, job fails.
- watch `:145-150` → `Err(Error::App(...))`, job fails, with a comment about false webhook alerts.
- connector-api-watch `:261-264` → pushes `{"connector": slug, "error": "empty document"}` into
  `state.errors` and `break 'connector` — **per-connector skip, the run continues.**

That divergence is **correct** for a loop over N connectors: one dead docs page must not fail the
other N. Only the predicate is shared, never the reaction.

## Why REJECTED

**Below the bar.** Two-line dedupe, zero behavior change, zero user-visible symptom — pure tidiness,
which is what `config.md → ## User taste` rejects. Worth doing only as a **rider** on other work that
already opens `connector-api-watch/src/lib.rs`; not worth a slate slot or a commit of its own.

**Two near-misses deliberately NOT folded in** (different predicates, do not unify them):
`crates/apps/census-common/src/lib.rs:336` (`status == 204 || body.trim().is_empty()` — HTTP-level
emptiness) and `crates/apps/research/src/lib.rs:681`
(`!structured && accumulated_text.trim().is_empty()` — LLM accumulator emptiness).
