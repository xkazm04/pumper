---
name: trigger-pipeline
type: perfect/context
group: Event Pipeline
category: lib
opportunity: 7
last_proposed: 2026-08-04
cooldown_until: 2026-08 +2 rounds
directions: ["[[per-dataset-trigger-hops]]", "[[trigger-decision-ledger]]", "[[ingress-replay-defense]]", "[[trigger-hot-path]]", "[[activate-wasm-hooks]]", "[[pumper-smoke-harness]]"]
---

## Current state (scouted 2026-08-04, HEAD 49ca08c)

Decision half pure (`crates/server/src/triggers.rs:1-359`), IO half `:361-593`; CRUD in
`routes/triggers.rs`; ingress in `routes/ingress.rs`; storage `core/storage.rs:852-983`,
`:1494-1504`, `enqueue_dedup :175-220`. All four firing paths LIVE and traced end-to-end
(dataset fanout `worker.rs:825-827`, saved-search view `worker.rs:1320-1324`, terminal
`worker.rs:1382`, ingress `ingress.rs:221→333`). Cycle/depth guard closed-loop verified;
idempotency check-then-insert race properly closed (`storage.rs:206-213`). Plugin-hook
provenance re-stamping is genuinely unforgeable (`triggers.rs:175-205`).

**Top findings:**
1. **CONFIRMED BUG — idempotency key omits dataset** (`triggers.rs:573`): multi-dataset runs
   fire ONE hop for a nondeterministic (`RandomState`) dataset; the rest silently dedup away.
   Also collides saved-search view hops (same `job.id`) with fanout hops. Docs claim "whole
   capped batch". Untested, unlogged (`:587` silent).
2. **No `trigger_runs` table** — phantom in context-map. Negative decisions (dedup, filter
   miss, cycle, depth, predicate veto, unregistered target) are log-only or fully silent;
   "why didn't it fire" is unanswerable. `/runs` = `jobs WHERE trigger_id` (fires only).
3. **Fail-open evaluation** (`triggers.rs:382/:435/:481/:588`): transient DB error drops the
   edge set permanently — source job terminated, nothing re-evaluates.
4. **Ingress replay window** (`ingress.rs:304/:320`): bare/GitHub scheme has no skew gate;
   without `x-pumper-delivery-id` a captured signed body replays into fresh jobs forever
   (fresh `Uuid::new_v4` each time), bounded only by the 60/min bucket.
5. **WASM hook feature is inert in deployment**: `plugins-src/{trigger-gate,delta-slim}` fully
   written+ABI-correct but never built into `data/plugins/`; every configured hook takes the
   unknown-plugin fail-open path (`engine-wasm/src/lib.rs:108`). No justfile build step.
6. Perf: filters re-parsed per event per trigger (`triggers.rs:492`); fresh Store+instance per
   hook invocation behind a global CPU-count semaphore shared with extraction
   (`engine-wasm/src/lib.rs:344`); 2 unconditional trigger queries per job completion.
7. Dry-run `POST /triggers/{id}/test` skips health/contract gates the live path applies and
   `?fire=true` bypasses idempotency (`routes/triggers.rs:341/:392`).
8. Cycle guard is per-trigger-id; two-trigger cycles caught only by max_depth (8 hops).
9. Docs drift: triggers.md omits `filters`+`plugins` body fields; ingress.md claims UUIDv5
   (actually truncated SHA-256, no version bits); export posture omits the replay gap.

Test coverage: pure half thorough (decide/filters/hooks via StubPlugins); e2e only for
dataset-fanout kind (`e2e/worker_fanout.rs`). Untested: multi-dataset enqueue, terminal e2e,
ingress handler gates, real-WASM-host hooks, dedup-race branch.

## Banked anchors
- 2026-08-12 (r14 landing, Director-observed): **the trigger door is unknown-body-field
  tolerant** — POST /triggers with a typo'd `plugins` key (e.g. `plugin_hooks`, the
  storage column name) answers 201 and creates an UNGATED trigger; the exact
  mis-deployment class the dry-run's `unusable_plugins` surface exists to catch, one
  level earlier. Candidate direction: unknown-body-field policy at the work-creator
  doors (triggers/schedules/jobs/watches), probably serde deny_unknown_fields or a
  shared 400-on-unknown-keys helper + inventory test. Found when r14's smoke check
  tripped over it (1a8b955).

## Direction history
- 2026-08-04 (round 6): presented 6 (5 context + 1 banked cross-context seed), **accepted
  6/6 clean sweep** — per-dataset-trigger-hops (robustness, confirmed bug), trigger-decision-
  ledger (feature), ingress-replay-defense (robustness), trigger-hot-path (optimization),
  activate-wasm-hooks (wildcard), pumper-smoke-harness (cross-context robustness). Zero
  rejections; engine-depth taste holds.

## Shipped
- [[per-dataset-trigger-hops]] → `48e7ade` — multi-dataset runs fire one hop per dataset;
  view hops disambiguated.
- [[trigger-decision-ledger]] → `5d99cc6` — trigger_runs real (migration 0036); skips
  observable via /runs; 404 on unknown trigger.
- [[ingress-replay-defense]] → `f908903` — body-derived event ids; replay dedups.
- [[trigger-hot-path]] → `b30bd47` — generation-stamped eval cache (zero-trigger completions
  query nothing); InstancePre honest wash, kept as regression gate.
- [[activate-wasm-hooks]] → `8adfc91` — plugins built+installed via just plugins-install;
  real-host e2e; plugin_missing loud.
- [[pumper-smoke-harness]] → `c4f3766` — just smoke; 11/11 live on final master.
