---
slug: webhook-delivery-drain
type: perfect/direction
context: "[[webhook-delivery]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-10
commit: 614e7e3
---

## What & why
Every webhook delivery escapes the process's drainable lifecycle through a bare
`tokio::spawn`, so graceful shutdown exits with POSTs mid-flight and the harness can
only test deliveries by polling with 30s deadlines — the structural root of the
sink-delivery flake that has now cost four sessions. Deliveries join the FanoutPool
(the exact idiom round 7 shipped for DataHub emissions, `27c5131`): shutdown drains
them, tests get a synchronization point instead of a race, and the unlogged-fallback
path stops discarding its outcome. User moment: "I stopped the service during a burst;
every webhook either completed or is in the DLQ — none vanished."

## Evidence
- `crates/server/src/webhook.rs:285` (`spawn_logged`) and `:258` (`replay`) — bare
  `tokio::spawn`; `fanout.rs:11-24` documents the pool as the designed non-lossy home
  for exactly this; `worker.rs:213` (`drain_fanout`) awaits the pool on shutdown but
  returns 0 while N webhook POSTs are still open.
- `crates/server/src/webhook.rs:296-313` — on `create_delivery` failure the outcome of
  the fallback send is discarded (`let _ = deliver(...)`); half-fixed bug #4 from
  docs/harness/refactor-bughunt-2026-07-14/live-events-webhooks.md.
- `crates/server/src/e2e/sink_delivery.rs:56-58,88-91` and `webhook_contract.rs:58` —
  polling with raised-twice deadlines against detached spawns; the 30s budget hides the
  race, it does not remove it (b5aae81 history).

## Acceptance criteria
- [ ] `spawn_logged` and `replay` run through the FanoutPool (or an equivalent tracked
      handle set the shutdown path awaits) — after `drain`, zero webhook tasks are
      in flight. Follow the DataHub emission-lifecycle integration shape.
- [ ] Shutdown test: enqueue deliveries against a slow loopback receiver, initiate
      drain, assert all rows reach a terminal/scheduled state (`delivered`/`failed`
      with `next_retry_at`) — no `pending` row without a schedule survives a clean
      drain, and no delivery task outlives it.
- [ ] The unlogged-fallback path (`create_delivery` error) logs the delivery outcome at
      `warn` with attempts/last-error instead of discarding it (storage is down, so a
      log line is the honest ceiling — say so in the comment).
- [ ] At least one existing webhook e2e (sink_delivery or webhook_contract) replaces
      its deadline-poll with the drain synchronization point, proving the flake class
      is structurally dead — not re-tuned.
- [ ] No ordering regression: deliveries for the same watch keep today's semantics
      (none guaranteed); this direction adds lifecycle, not ordering.

## Risks / non-goals
- Non-goal: redirect policy on `webhook_client` (rejected egress-hardening direction
  covers it; noted for a future pass).
- Non-goal: per-host circuit breaker / poisoned-endpoint isolation.
- Risk: the FanoutPool is sized/shared — webhook bursts must not starve worker fan-out;
  if the shared pool is contended, a dedicated pool instance for deliveries is the
  honest shape (builder decides, states the reasoning).
- Risk: `replay` is called from the drain loop — pooling it must not serialize the
  drain batch behind slow receivers in a way that blocks the scheduler tick; the tick
  must remain fire-and-forget with respect to delivery completion.

## Build record
Dedicated 16/1024 FanoutPool for deliveries — no more bare tokio::spawn escapes; shutdown drains in dependency order; unlogged-fallback outcome now logged. Both webhook e2es dropped deadline polls: the 4-session sink-delivery flake class is structurally dead. Also fixed 4d753fc's fmt miss. Review: keep.
