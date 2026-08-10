---
slug: fetch-chokepoint
type: perfect/direction
context: "[[app-runtime]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-11
commit: 6237cc8
---

## What & why
Every fetch an app makes must pass through `AppContext::fetch` so it is metered,
budget-clamped, tier-learned, and VCR-faithful. Today the extractor and plugin apps fan
out through the raw fetcher with a param-selectable strategy including
`auto_with_research`: a job with budget $1 — or $0 from a DataHub governance pause —
can spend unbounded Claude money invisibly (never ledgered, never clamped), and a
recorded run of those apps silently hits the live network on replay. This closes the
last spend/determinism bypass the same way the research chokepoint was closed
(a1f3706 + EXPECTED inventory), which is also architect-backlog item (i).

## Evidence
- `crates/apps/extractor/src/lib.rs:875–909` — param strategy incl. auto_with_research;
  `ctx.engines.fetch.clone()` → raw `f.fetch(req)`; no meter call in the crate.
- `crates/apps/plugin/src/lib.rs:437–468` — same shape (note the FnOnce-inference
  comment at :451–458; the fan-out is lifetime-touchy).
- `crates/core/src/engine.rs:630` — `EngineSet::fetch` is pub (claude is pub(crate)).
- `crates/core/src/fetcher.rs:131` — `FetchRequest::new` defaults `max_budget_usd: None`.
- `crates/core/tests/llm_chokepoint.rs:48–51` — the research inventory; no fetch
  equivalent exists.

## Acceptance criteria
- [ ] Extractor's and plugin's URL fan-outs route through `AppContext::fetch`
      (metered, budget-clamped, tier-learned, VCR-recorded/replayed). Bounded
      concurrency preserved; plugin's positional zip order preserved.
- [ ] A raw-fetch inventory test (EXPECTED-diff idiom) pins every remaining
      `.engines.fetch` / `.engines.http` / `.engines.browser` call site in
      crates/apps + crates/core + crates/server, with a per-entry comment naming why
      it is allowed (crawl's MeteringHttpClient, transact's browser flow, raw-http
      conditional-GET apps, server-side jobless /extract/preview, extractor's archive
      backfill construction, refresher). Adding a call site fails the test.
- [ ] With zero budget headroom, `auto_with_research` through extractor/plugin cannot
      reach the Claude tier (test proves the soft downgrade applies to these apps now).
- [ ] Each fetched URL in those apps lands a cost event (engine, url) — test.
- [ ] A recorded extractor job replays from cassette with Dead engines (test).
- [ ] Doc-sync: extraction.md (+ apps.md if its text describes the fetch behavior)
      note metered + replayable fetches.

## Risks / non-goals
- Do NOT change `EngineSet::fetch` visibility if jobless server-side callers need it —
  the inventory test is the guard (repo convention: conventions are inventory tests).
- Do NOT migrate crawl (self-metered by design, structurally different fan-out).
- Borrow shape: futures can borrow `&ctx` (no spawn → no 'static bound); if inference
  fights as in plugin's comment, document the workaround chosen.

## Build record
Shipped `6237cc8` (Lot A, opus, 2026-08-11). Both fan-outs through ctx.fetch; futures
borrow &ctx (predicted inference fight never materialized). Inventory test pins 16 raw
call sites WITH COUNTS (a second bypass in a listed file also fails) incl. a
routes/remote.rs site the scout missed; scanner self-excludes by constant. e2e proves:
budget-exhausted downgrade with a funded control arm, one cost event per URL, replay
vs Dead engines at $0. Builder refutations: plugin has no dev-deps → app tests live in
server/e2e; no visibility change needed. Review: keep. Follow-up seeds: extractor
archive-backfill fetches still unmetered (inventory-allowed); fan-out budget clamp is
racy within one concurrency window (pre-existing).
