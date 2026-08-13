---
name: us-federal-grants
type: perfect/context
group: Grants Intelligence
category: lib
opportunity: 4
last_proposed: 2026-08-13
cooldown_until: round 22
directions: ["[[grants-sweep-end-proven]]", "[[grants-details-first-class]]", "[[grants-result-contract-true]]"]
alias_of_old_map: "[[us-grant-opportunities]] (round-3 grants pass + r5 unified work covered this file)"
---

## Current state
Scouted end to end, round 20 (`crates/apps/grants-gov/src/lib.rs`, all 1,534 lines read; every
sibling app, the shared unified layer, the catalog rows and the registry traced).

**The pipeline.** `GrantsGov::run` (`:205`), scheduled daily 09:00 UTC. A `loop` of POSTs to
`api.grants.gov/v1/api/search2` (`:242-300`) with `startRecordNum`/`rows` — **no cursor, no
checkpoint on the listing**: every run re-walks from `start=0`, which is stateless by design and
is not a bug. Page 1 is saved as a `page1.json` artifact. Keying is `id` → `number` →
positional `row-{i}` fallback (`:327-332`). Writes `grants-gov/opportunities` via
`upsert_many_with_provenance` (upsert-only — **never** `sync_many`, so no removal ever occurs).
A delta-only NOFO detail stage (`:385-463`, defaults `maxDetailsPerRun: 50`, `DETAIL_FLUSH: 25`)
harvests `fetchOpportunity` into `grants/opportunity_details`, then `grants_common` normalizes →
`enrich_with_detail_amounts` → `finalize_unified` writes `grants/unified`, `grants/events`,
`grants/duplicate_links`, `grants/recurrence_links`, `grants/maintenance`.

**Sibling-fix audit — the reason this context scored well.** Of eight fixes shipped elsewhere in
this loop, grants-gov is missing six: `SweepEnd::{Complete,Capped,ShortPage}` (r14 cordis) — 0
hits; `empty_listing_is_drift` (r14 cordis) — 0 hits; `DerivedPaths` (r14, adopted by eu-sedia and
the plugin observatory) — 0 hits; derived-aggregate coverage honesty — no equivalent; the
aggregate-limit tripwire + tombstone exclusion (r14 cordis) — absent; a catalog row for the second
dataset (r14 `cordis-topic-stats`) — absent. Two it does have or does not need: the listing cursor
(N/A, stateless walk) and the registry publisher guard (present but weak — `registry.rs:307`
asserts `shape.contains("unified")`, which passes on the prose "counts unified rows").

**Refuted, and worth recording** — the unbounded records-echo bug that r12 and r19 both shipped
fixes for is **not present here**: `closingSoon` is `.take(25)` (`:535`) and `details.errors` is
capped at 3 (`:594`). Money honesty is also genuinely good: `money_scalar` maps `"none"`/`"$0"`/
prose to Null and `overlay_amounts` is fill-only and refuses `<= 0`. The detail stage's
failure isolation is well built (counts rather than swallows, 5-consecutive abort, separate tally
from the truncated verbatim list) and is the one thing the crate's single `run()`-level test
proves.

**Test reality.** No `tests/` dir; 16 inline tests, of which **exactly one is `run()`-level**
(`:1480`). The scripted client (`:1426-1460`) answers one fixed page regardless of
`startRecordNum` — which is structurally why every pagination-shaped bug below survived. Pure-
function coverage is genuinely good (requirements honesty, attachment manifest, drift refusal,
capped delta, stage degradation).

## Direction history

**Round 20 — 3 accepted, 3 rejected** (director-self-gated, autonomous). All four severest claims
were Director-verified in source before gating and all four CONFIRMED verbatim.

- ACCEPTED [[grants-sweep-end-proven]] — the walk stops four ways and calls three of them complete;
  a `hitCount` rename caps the corpus at one page, green, forever. Unfixed sibling instance of r14's
  `cordis-sweep-honesty`.
- ACCEPTED [[grants-details-first-class]] — `harvested_at` inside the change hash makes the only
  source of federal award amounts always report `changed`; the dataset has no catalog row at all; and
  the one contract it does have (`max_row_delta_pct`) cannot fire because the app never emits a
  `removed` revision.
- ACCEPTED [[grants-result-contract-true]] — `output_shape` declares `hit_count` (emitted as
  `hitCount`) and `removed?` (structurally unemittable), omits twelve keys it does emit, the crate
  contradicts itself three times about its own default in one file, and absent `applicant_types` /
  `attachments` are fabricated as `[]` with the tests pinning it.
- REJECTED-DEFERRED **grants-drift-is-terminal** — all five app-level failure paths are
  `Error::App`, which `error.rs:585` explicitly asserts must stay retryable, so a permanent 400 or
  a schema drift re-fetches the entire 25-page corpus on every attempt of the ladder. Real, and the
  fifth instance of the terminal-error class this loop has killed. Deferred for a structural
  reason, not a value one: **the correct lever is core's error taxonomy (`crates/core/src/error.rs`),
  which the sibling lot owns this round** via [[replay-miss-terminal]] — slating both would put one
  file in two lots and break a zero-Class-B partition. `Error::BadRequest` is semantically wrong
  here (documented as client-supplied input → HTTP 400; a source's schema drift is neither).
  Banked as this context's anchor, to be built **after** `replay-miss-terminal` lands so it extends
  a freshly-reasoned taxonomy rather than racing it.
- REJECTED-DEFERRED **grants-detail-delta-survives-restart** — the crate doc (`:377-384`) says the
  checkpoint exists precisely because a restart "would silently ZERO" the harvest, but the first
  checkpoint is written only inside `if buffer.len() >= DETAIL_FLUSH` (25) and `ctx.checkpoint` is
  throttled (`app.rs:116-118`), so with the shipped defaults there is at most one throttled mid-run
  checkpoint and the only guaranteed one is *after all work is done*. The delta is never
  checkpointed before the first fetch. Deferred for the sharpest reason in this round's gate:
  **it is unassertable today.** Proving it requires a checkpoint sink that can fail or be counted,
  and `NoCheckpoints::save` returns `true` unconditionally with **zero** `CheckpointSink` doubles
  in the workspace. Shipping it now means shipping a fix whose regression test cannot exist.
  **Gated on [[vcr-testing]]'s banked harness direction — build them as a pair.**
- REJECTED **row-key-positional-fallback** (`:327-332` falls back to `row-{i}` when neither `id`
  nor `number` is a JSON *string*, and `ca-grants:264-277` handles the numeric case while this does
  not; a key shift would churn the whole corpus). Rejected as UNVERIFIED against live payloads —
  the code path is real but no observed grants.gov response has ever produced it, and the catalog
  contract already records that `id` "arrives as string OR number" and type-pins around it.
  Speculative hardening without a user moment; revisit if a run ever emits a `row-` key.

## Shipped
- (inherited, pre-46-map — see [[us-grant-opportunities]], [[grants-unified-layer]]): round 3
  close-date sweep + taxonomies (`9d18132`, `d59b307`); round 5 federal award amounts joined live
  (`230a766`).
- **Round 20 — 3/3 shipped, all KEEP.** Observed effect, one line each:
  - [[grants-sweep-end-proven]] `12ae222` — the Search2 walk's four endings are four named outcomes
    (`SweepEnd::{Complete,Capped,ShortPage,UnknownTotal}`), and only the arm that proves coverage
    reads complete. The headline bug is dead: a `hitCount` rename can no longer cap the federal
    corpus at **one page, green, indefinitely**. Coverage is proven by records COLLECTED, which is a
    deliberate divergence from cordis's position arithmetic — that test calls a rate-limited page a
    full sweep. Riders: per-page `oppHits` drift refusal, whole-corpus empty-listing drift, and an
    `errorcode` that no longer defaults to success when it is absent, null or a string.
  - [[grants-details-first-class]] `3b8d994` + `6489ec2` + `0154932` — the product's only source of
    federal award amounts stopped reporting `changed` on every byte-identical re-harvest
    (`harvested_at` declared derived), got the `[[source]]` row + contract it never had, lost a
    `max_row_delta_pct` that could not fire on any run, and its 1,000,000-row money join became
    live-only, bounded and **reportable** (`detailCorpus.{read,truncated}`). The catalog row required
    widening the registered-app guard to accept a virtual namespace — the builder found that blocker
    and **reported it instead of filing the row under `grants-gov`**, which would have passed every
    test while being a lie.
  - [[grants-result-contract-true]] `8753553` — `output_shape`, published by `GET /apps` and the MCP
    manifest, is now pinned to a REAL run in both directions by `tests/result_contract.rs`. Three
    further truths in the same surface: the harvest default is stated once (the runtime default was
    `false` while the crate said ON twice, so a hand-built params call ran a different pipeline from
    the scheduler's); absent is no longer fabricated as `[]`; and the closing-soon digest now judges
    at the same anywhere-on-Earth instant as `grants/unified` and `GET /grants/closing-soon`, closing
    a ~12-hour window where the three surfaces disagreed about the same grant.
- **Verdict on the context after r20**: the app's honesty surfaces (sweep, contract, catalog,
  result) are now guarded by tests that derive truth from a real run rather than restate it in prose.
  Banked next: `grants-detail-delta-survives-restart` — a real loss window whose regression test
  **cannot exist today**, blocked on vcr-testing's `harness-expresses-the-run`; and
  `grants-drift-is-terminal`, deferred this round only because `crates/core/src/error.rs` belonged to
  the sibling lot (build it AFTER [[replay-miss-terminal]]; note `error.rs` asserts `Error::App` must
  stay retryable, so it needs its own variant or a classification site). Cooldown: not before r22.
