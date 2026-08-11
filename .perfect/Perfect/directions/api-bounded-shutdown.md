---
slug: api-bounded-shutdown       type: perfect/direction
context: "[[api-surface]]"       lens: robustness
status: shipped                  size: M
proposed: 2026-08-11  accepted: 2026-08-11  shipped: 2026-08-11  commit: c9c2c68
---
## What & why
With one dashboard tab attached to GET /events, Ctrl-C / systemctl stop never completes: axum's
graceful shutdown waits for every in-flight connection, the SSE loop only exits when the broadcast
sender drops, and the sender lives in AppState clones held by the worker/scheduler/janitors/router.
The process dies only by SIGKILL — which skips the worker drain and loses the politeness snapshot
since the last write-behind tick. Three background loops additionally escape the shutdown token
entirely. Make shutdown terminate, bounded, with state flushed.

## Evidence
- crates/server/src/main.rs:175-182 — serve(...).with_graceful_shutdown awaited BEFORE worker
  drain; no deadline.
- crates/server/src/routes/events.rs:41-57 — /events stream loops until RecvError::Closed (never
  arrives); KeepAlive keeps sockets alive. /jobs/{id}/stream self-terminates (fine).
- crates/server/src/mcp/live.rs:109 — same infinite-stream shape.
- crates/server/src/state.rs:345-354 — host-penalty write-behind: bare loop, no shutdown select,
  no final flush.
- crates/server/src/refresher.rs:56 + crates/server/src/datahub.rs:1039 — bare tokio::spawn
  passes, cancellation-unaware.

## Acceptance criteria
1. SSE streams (/events, /mcp live) end promptly when the shutdown token fires (select! in the
   stream generators or equivalent) — an attached client sees a clean stream end, not a reset.
2. The serve await is bounded: after the token fires, in-flight HTTP gets a grace window (config
   or constant — builder's call, stated) and then the process proceeds to worker drain regardless.
   Worker drain semantics (two-phase, requeue-at-deadline) are NOT weakened.
3. The write-behind loop is shutdown-aware AND performs one final persist pass on shutdown — the
   politeness state on disk after a clean stop reflects the live governor, not the last tick.
4. Refresher pass and datahub govern_tick become cancellation-aware (token-checked / tracked) so
   no network call outlives the process silently; their off-by-default status is not an excuse.
5. A test proves the shutdown path: e2e that opens a live SSE subscription, fires the token, and
   observes both stream end and run() completion within the bound (shape of the harness is the
   builder's call; shutdown_drain.rs precedent drives the worker directly).

## Risks / non-goals
- Do not break Last-Event-ID resume semantics or the replay ring. Do not add a shutdown 503
  readiness surface (banked separately). The MCP stream's protocol semantics on close: verify
  what rmcp/client expects — closing the stream is fine, corrupting a JSON-RPC frame is not.

## Build record
- Shipped `c9c2c68` (Lot A, opus). All 5 criteria met. One shared `next_or_shutdown`
  (biased select, re-exported routes→mcp so neither surface can regain the bare recv()) ends
  /events, /jobs/{id}/stream AND /mcp live on the token at frame boundaries. `await_bounded`
  bounds the serve await; HTTP_SHUTDOWN_GRACE=10s CONSTANT (builder's reasoned deviation: the
  operator knob is [worker] shutdown_drain_secs; windows run concurrently so stop = max not sum).
  Write-behind loop shutdown-aware; final flush moved to main::run AFTER worker drain (builder
  improvement over the criterion: a job finishing during the drain teaches a penalty a loop-local
  flush would miss). refresher/datahub token-checked + cancellable mid-pass. 4 unit + 7 e2e; two
  e2e were vacuous on first write (biased select never reads a pre-cancel event) and were
  restructured to read a live chunk first. Review: keep.
- REFUTED: crates/server/tests/ doesn't exist (e2e is src/e2e/, binary-only crate); no rmcp dep
  (hand-rolled JSON-RPC — asserted frame integrity directly). NOT live-verified: a real SIGINT
  to a live process (Windows can't deliver Ctrl-C to a detached process — smoke.ps1 documents
  this; the composition real-listener+serve+signal remains e2e-level only).
