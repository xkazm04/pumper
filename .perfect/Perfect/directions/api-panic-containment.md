---
slug: api-panic-containment      type: perfect/direction
context: "[[api-surface]]"       lens: robustness
status: shipped                  size: M
proposed: 2026-08-11  accepted: 2026-08-11  shipped: 2026-08-11  commit: 4855fcd
---
## What & why
A panic anywhere on a request path drops the connection with no response — the client sees a
reset, Sentry sees nothing. Worse: six request-path sites lock std::sync::Mutexes with
unwrap/expect, and those mutexes are shared with the worker — one panicking holder ANYWHERE
poisons the lock and turns /sources, /catalog/health, /jobs/{id}/receipt, DELETE /jobs/{id}, and
POST /ingest/{id} into permanent connection-reset generators for the process lifetime. Contain
panics into clean 500 envelopes and make the locks poison-tolerant. Also clamp the one param the
docs promise is clamped and isn't.

## Evidence
- Cargo.toml:66 — tower-http features lack catch-panic; no CatchPanicLayer in routes/mod.rs
  middleware stack (:278-307).
- Poisonable request-path locks: health.rs:329, query.rs:351, receipt.rs:270 (contract_verdicts
  .expect ×3), jobs.rs:375 (job_cancels .unwrap), ingress.rs:93 (BUCKETS .unwrap). Same mutexes
  locked by the worker (worker.rs:63,120) — a worker panic poisons routes.
- health.rs:250-256 — /enforcement/preview `runs` passes through unclamped; docs claim max 1000.

## Acceptance criteria
1. A handler panic returns the standard JSON error envelope with a 500 (CatchPanicLayer or
   equivalent), and the panic is logged/Sentry-visible with the route. An e2e or router test
   proves response-not-reset.
2. The poisonable sites go through ONE extracted poison-tolerant helper (recover into_inner —
   these are simple caches/flags where the data is safe to reuse; builder documents why per site)
   with a test named after the anti-pattern (poisoned_lock_not_permanent_500 style). No new
   request-path unwrap/expect on lock results remains (sweep, not just the six).
3. /enforcement/preview clamps `runs` to the documented bound (extracted + tested, matching the
   limit clamp style beside it).
4. No behavior change for non-panic paths; middleware ordering documented (panic layer placement
   relative to Trace/Compression stated in the code where it's added).

## Risks / non-goals
- CatchPanicLayer must not swallow the worker's panic-containment semantics (it's HTTP-layer
  only — verify). contract_verdicts/job_cancels recovery-on-poison is safe BECAUSE they're
  advisory caches — if the builder finds one that isn't, say so and handle differently. Non-goals:
  request timeouts for the 6 unbounded handlers (banked — needs per-route design), tokio panic
  =abort policy changes, /metrics single-flight (banked).

## Build record
- Shipped `4855fcd` (Lot A, opus). All 4 criteria met. CatchPanicLayer INNERMOST
  (response compressed/traced like any other; panic logged inside the TraceLayer span that
  names the route) — stack extracted to `with_middleware` so the e2e drives the SHIPPED stack
  over panicking routes, not a lookalike. One `lock_advisory` helper (into_inner recovery) with
  per-site justification; all 5 sites converted; `no_route_unwraps_a_lock_result` inventory
  sweeps routes+mcp production code (test blocks excluded, non-vacuity guard). preview_runs
  clamp at the boundary. Review: keep.
- REFUTED: "six sites" → five; `runs` was NOT unbounded (core preview_fleet already clamps
  1..=1000 — boundary clamp added anyway so the promise lives where documented, said so in the
  commit).
- BANKED (out of write set, same bug class — round-11 api-surface anchors): poisonable locks in
  events.rs:171,194,241,260 (EventBus ring — EVERY SSE connect + worker publish; highest value),
  progress.rs:46-162, triggers.rs:545,554,1450; worker.rs:63,120,185,1112,1285 (read-only).
  DELIBERATE non-conversion: datahub.rs `in_flight` is NOT advisory — PollGuard::drop owns its
  lifecycle; into_inner could strand the poller and drop-on-poisoned is a double-panic path.
  Needs its own design decision.
