---
slug: connector-watch-failures-are-not-success
type: perfect/direction
context: "[[connector-api-watch]]"
lens: robustness
status: rejected
size: S
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## What & why

Two honesty holes in the same app, on a **monthly** cadence — so the blind spot is measured in
quarters.

**(a) A failed write is read as "unchanged", and the checkpoint cements it.**
`crates/apps/connector-api-watch/src/lib.rs:290` ends the upsert with `.unwrap_or(ChangeKind::Unchanged)`.
A storage failure therefore becomes "nothing changed": the document is not stored, nothing lands in
`state.errors`, `kind != Changed` sends it down `break 'connector` (`:293-295`), and then `:341-342`
marks the connector **done and checkpoints it** — so a resumed attempt never retries it. Silent
loss, cemented durably.

**(b) A run in which every connector failed returns `Ok`.**
Failures push-and-continue (`:251-257`) and the run ends `{scanned: N, changed: 0, errors: [N items]}`
(`:345-354`) — a green monthly job. Both `plugin` and `research` explicitly fail a run that produced
nothing (`docs/features/apps.md`); this app has no such guard.

## Evidence

- `:278-295` — the `unwrap_or(ChangeKind::Unchanged)` swallow and the `break 'connector` it feeds.
- `:341-342` — the checkpoint that marks the swallowed connector done.
- `:251-257`, `:345-354` — push-and-continue into an unconditional `Ok`.
- `:516-652` + `tests/adoption.rs` — 13 tests, the best-covered app in its family, and **none** of
  them reaches either hole. `tests/adoption.rs:58-71` asserts `Ok` on an empty watch list scanning
  zero connectors, i.e. it encodes "scanning nothing is success".
- Riders found in the same pass, all real but smaller: `engine = "http"` in the catalog row
  (`catalog/data-sources.toml:780`) on the only `CostClass::Claude` app in the family (`:170`, and
  the catalog vocabulary has a `claude` value at `:22`); `max_row_delta_pct = 20.0` (`:795`) is
  **structurally inert** (the app only ever upserts; `removed` comes solely from `sync_many`) —
  the same inertness already deleted from grants-gov's row for this exact reason;
  `output_shape` (`:164-169`) omits `resumed_from_checkpoint`, which the run emits (`:350`).
- **A genuine doc-vs-code contradiction worth its own line:** `catalog/data-sources.toml:789-792`
  says `max_staleness_hours` was deliberately omitted because "a quiet month is normal and a
  staleness floor would fire false alarms" — but `cadence = "monthly"` (`:782`) →
  `cadence_secs()` = 31d (`crates/core/src/catalog.rs:86`) → expected 62d with the ×2 grace
  (`crates/server/src/routes/query.rs:364`) → the source is monitored **anyway** and flagged stale
  off `updated_at`, which moves only on a real change. A stable set of API docs going 62 days
  unchanged — the normal case the comment describes — is reported stale. There is no way to express
  the stated intent with today's catalog fields.

## Acceptance criteria (for whoever builds this)

1. A failed upsert is an error entry, not `Unchanged`, and the connector is **not** checkpointed as
   done.
2. A run in which no connector succeeded fails, naming the count and the first few reasons —
   matching `plugin`/`research`.
3. `tests/adoption.rs:58-71` is confronted, not deleted: an empty watch list may still be `Ok`, but
   the all-failed case must not be.

## Risks / non-goals

- **Non-goal:** the unbounded default sweep (`limit: 0` = all) and the full-markdown-in-record
  storage. Real bounds questions, separately banked.

## Why REJECTED this round

Real, confirmed, and cheap — **rejected on the round's 6-direction cap.** It lost its slot to three
directions with strictly larger blast radius: a partial parse that **tombstones live rows**
(`smlouvy`), an alerting app that **fires false alerts at users** (`watch`), and a paginator that
**caps a whole state's corpus at one page while reporting a complete sweep** (`ca-grants`). This
app's failure mode is *invisibility of a failure*, which is one step less severe than *data loss*
and *false positives delivered to humans*.

Banked for r23 with the staleness contradiction, which is the more interesting half: it is a defect
in the **catalog's expressiveness**, not in this app, so it belongs to a joint
[[connector-api-watch]] × [[data-pipeline-catalog]] pass.
