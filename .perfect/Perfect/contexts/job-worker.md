---
name: job-worker
type: perfect/context
group: Job Orchestration
category: lib
opportunity: 9
last_proposed: never
cooldown_until: —
directions: []
---

## Current state (scout brief, 2026-08-03 — CACHED, unused; next cursor candidate)

**Files:** `crates/server/src/worker.rs` (1196 L) + `crates/server/src/e2e/worker_fanout.rs`.
Table names in the context map are stale: real tables are `checkpoints` (`0022_checkpoints.sql:16`)
and `job_yield` (`0024_job_yield.sql:16`), not `job_checkpoints`/`job_yields`.

**Lifecycle.** Atomic claim with priority aging (storage.rs:236-266); every terminal write fenced on
`(status='running', attempts=N)`. Global semaphore (worker.rs:21, default 4) + per-app caps
(worker.rs:234-257, default unlimited). Timeout raced via `select!` (worker.rs:382-409). Heartbeat
only ticks while the app future yields (worker.rs:387-396 — a non-yielding wedge is invisible).
`reap_stale` routes through the normal `fail()` backoff (storage.rs:602-623). `drain()`
(worker.rs:144-192) two-phase: wait, then cancel → treated as **suspend** via `storage.reset()`
(worker.rs:411-428). Checkpoint resume is genuinely exercised — `load_restore` (worker.rs:199-232)
plus per-app `restored_*` decoders with unit tests in grants-gov, cordis, extractor, research,
mpsv-vpm, connector-api-watch, plugin, provisioner, state-licensing.

**Finalize side effects** (all inline in the per-job spawned task unless noted): search indexing
(worker.rs:458-471, warn-only), job-yield persist, watch dispatch (spawned inside webhook.rs:258),
dataset triggers (inline enqueue), saved-search alerts (worker.rs:819-894, forces a search flush),
DataHub `on_job_success` (**detached spawn**, datahub.rs:558), terminal + failure webhooks (spawned),
terminal triggers. Ordering is load-bearing and comment-documented: `suppress_unhealthy` →
`enforce_contracts` → watches/triggers (worker.rs:510-522) — untested. **No `catch_unwind`**: a panic
in `app.run()` leaves the row `running` until the reaper picks it up.

**Banked seeds — adjudicated:**
- (a) **Saved-search app scoping — CONFIRMED BUG.** `notify_saved_searches` filters
  `search.app != job.app` (worker.rs:836) but the unified layer indexes under the virtual app
  `UNIFIED_APP = "grants"` (grants-common/src/lib.rs:29, index_datasets spec at :87-90) consumed
  as-is by `dataset_search_docs` (worker.rs:1080-1097). A saved search scoped `app:"grants"` is
  skipped for every `ca-grants`/`eu-sedia` run. Unguarded by any test.
- (b) **`index_datasets` full re-index — REFUTED, already fixed** by `367cc7b` (2026-07-16):
  `dataset_search_docs` now reads `changes_since(..., job.started_at, …)` (worker.rs:1069-1129).
  Residual: a wiped Tantivy index only backfills forward; `search-backfill` is the manual recovery.

**Churn:** essentially all of worker.rs is <3 weeks old (M25/M26 DataHub, M24 VCR, M22 sinks, M20
contracts, M13 saved-search materialization, M23 durable execution, M04 yields).

**Tests:** exactly one e2e (`worker_fanout.rs`) over `run_one`. Untested: `run()`'s semaphore/per-app
loop, `drain()`, reaper end-to-end, timeout, cancel, VCR through the worker, the enforcement
ordering guarantee, DataHub spawn, and bug (a).

## Direction history
(not yet proposed)

## Shipped
(none yet)
