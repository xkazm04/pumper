---
name: api-surface                type: perfect/context
group: HTTP API                  category: api
opportunity: 6                   # 94 ops ride through it; the surface itself last served 2026-07 at 48 ops
last_proposed: 2026-08-14
directions: ["[[checkpoint-failures-metric]]", "[[api-bounded-shutdown]]", "[[api-error-contract]]", "[[api-panic-containment]]"]
supersedes: "[[http-api-routes]] (old 21-context map; its 4 shipped directions + auth rejection carry over)"
---

## Current state (scout brief digest, 2026-08-11)

routes/mod.rs composes 94 operations / 24 submodules via utoipa-axum single-source router+spec;
EXPECTED inventory test still EXACT (mod.rs:423-598); two-tier body limit (1 MiB + 8 MiB preview)
with compile-time assert; CORS same-origin by design; compression skips SSE. Route count grew
48 → 94 since July while the surface's own machinery went untouched — every recent commit is a
feature context adding routes *through* it.

**Verified rough edges (Director re-checked each at source):**
- **Shutdown hangs on one open SSE tab**: axum graceful shutdown waits for in-flight connections;
  /events loops until RecvError::Closed which never comes (state clones hold the sender)
  (main.rs:175-182, routes/events.rs:41-57); /mcp live stream same shape. No shutdown deadline.
  Three loops escape the lifecycle entirely: host-penalty write-behind (state.rs:345-354, bare
  loop), refresher pass (refresher.rs:56 bare spawn), datahub govern_tick (datahub.rs:1039).
  Worker/scheduler/janitors/pools ARE shutdown-aware (round 1 + 8 work) — the gap is the rest.
- **Error contract broken 3 ways**: error_code() lacks 403/429/503 arms → ingress deny/rate-limit
  and the 5 resilience-503 routes all report code "internal" (error.rs:25-36, ingress.rs:274,282,
  health.rs:340); From<core::Error> maps only BadRequest → everything else is a 500 whose body
  echoes raw sqlx/SQLite text, filesystem paths, upstream URLs (error.rs:45-56); Profile /
  BudgetExhausted / ReplayMiss / Transact / Http|Browser|Claude all collapse to 500.
- **No panic containment**: no CatchPanicLayer (tower-http features lack it, Cargo.toml:66);
  6 request-path std::Mutex unwrap/expect sites (health.rs:329, query.rs:351, receipt.rs:270,
  jobs.rs:375, ingress.rs:93) — one poisoning panic kills those routes for the process lifetime.
  /enforcement/preview `runs` unclamped (health.rs:250-256) despite doc claiming max 1000.
- No request timeouts anywhere (6 unbounded handlers: datahub/sync, governance/preview, doctor,
  retention/preview, enforcement/preview, fetch-proxy); /metrics = 6 full-table scans per cold
  scrape with NO single-flight (meta.rs:53-154); /openapi.json rebuilds the 94-route router per
  request (meta.rs:21); /health is a constant 200 (meta.rs:30-32); no request-id, access log at
  DEBUG only; http-api.md route table missing 14+ shipped route groups.

## Direction history
- 2026-07-13 (as http-api-routes, rounds 1): 4 shipped (pagination/errors, streaming, SSE resume +
  graceful worker shutdown, OpenAPI). **REJECTED: API-key auth — stays parked, never re-propose
  unprompted.**
- 2026-08-11 (round 10, director-self-gated autonomous): 5 drafted, 3 accepted —
  [[api-bounded-shutdown]] · [[api-error-contract]] · [[api-panic-containment]].
  **REJECTED-deferred: metrics-hot-path** (single-flight /metrics + OnceLock /openapi.json —
  real but small; lost on the pool cap to three correctness directions; same taste precedent as
  round-9 governor-hot-path reject. BANKED.)
  **REJECTED-deferred: doc-coverage test + http-api.md backfill** (route table missing 14+ groups,
  no doc↔EXPECTED test — the wave's own doc-sync fixes touched surfaces; the full backfill +
  inventory test is a clean future S. BANKED.)

## Banked seeds (re-verify at proposal time — seeds decay)
- metrics-hot-path (above): single-flight + windowless cost_events scan grows forever with default
  retention-off; /openapi.json OnceLock.
- doc-coverage test + http-api.md route-table backfill (above).
- Request timeouts / deadline budget for the 6 unbounded handlers — needs per-route design
  (datahub full_sync can legitimately run minutes); global TimeoutLayer would kill SSE/export.
- Real /health (DB probe) + shutdown-aware readiness flip; request-id + INFO access log.
- **NEW from round-10 build (Lot A sweep, out of its write set)**: poisonable request-path locks
  remain in events.rs:171,194,241,260 (EventBus ring — every SSE connect + worker publish;
  highest-value instance), progress.rs:46-162 (GET /jobs/{id} reads, worker writes),
  triggers.rs:545,554,1450 (POST /ingest path); worker.rs:63,120,185,1112,1285. The
  `lock_advisory` helper + inventory-test pattern from `4855fcd` is the template — but
  datahub.rs `in_flight` is NOT advisory (PollGuard::drop owns it; into_inner could strand the
  poller) and needs a design decision, not a mechanical swap.
- clients/typescript/src/http.ts:16 doc comment still lists the old error-code vocabulary.

## Shipped
- (as http-api-routes, 2026-07-13): 0a91f46 pagination+codes · 268d271 streaming+bounded ·
  5bdb7ae SSE resume + worker drain · 343341a OpenAPI single-source.
- 2026-08-11 (round 10): [[api-bounded-shutdown]] → `c9c2c68` (SSE surfaces end on the token
  via one biased next_or_shutdown; serve await bounded at 10s grace; write-behind final flush
  AFTER worker drain; refresher/datahub cancellation-aware; 4 unit + 7 e2e) ·
  [[api-error-contract]] → `0cfc366` (code map complete + inventory-test enforced;
  client_facing exhaustive mapping — Transact 422, BudgetExhausted 402/budget_exhausted,
  engines 502; RowNotFound stays 500 by pinned reasoning; 500 bodies redacted+logged) ·
  [[api-panic-containment]] → `4855fcd` (CatchPanicLayer innermost in the extracted
  with_middleware stack the e2e drives; lock_advisory ×5 + no-lock-unwrap inventory sweep;
  preview_runs boundary clamp). Full gate 1314/0 + live smoke 21/21. Observed effect: a stop
  with an attached dashboard terminates in bounded time with the politeness snapshot flushed;
  clients can branch on refusals; a panic is a JSON 500, not a reset that poisons routes forever.
- 2026-08-14 (r23): [[checkpoint-failures-metric]] — series confirmed absent; the "one-file
  change to meta.rs" framing REFUTED, and **the banked design would have shipped a metric that
  lies.** The `checkpoint_failures` block is stamped only on the SUCCESS path
  (`worker.rs:822`, inside `Outcome::Finished(Ok(_))`), so failed/cancelled/timed-out/panicked
  runs carry none — a `jobs.result`-derived series systematically undercounts exactly the runs
  an operator most wants counted, and shrinks under retention pruning. Re-banked as a
  process-lifetime `AtomicU64` in `JobCheckpointer::record_failure` surfaced through
  `AppState`: same file count, complete across all outcomes, zero query cost.
- 2026-08-14 (r23): SHIPPED here as a Director ratification — `1a8b48d`, `Error::SourceDrift`
  maps to **502**, not 500. See [[source-drift-is-terminal]]. Also records the invariant that
  direction's pre-flight missed: **adding a `pumper_core::Error` variant requires an arm in
  `routes/error.rs::client_facing`** (an exhaustive match with no wildcard, by design).
### r24 — [[checkpoint-failures-metric]] `c52bd45`
Checkpoint failures are counted on **every** job outcome, not only the success arm (1 of 7), and
`pumper_checkpoint_failures_total{reason}` renders on `/metrics` as a process-lifetime counter — not
a `jobs.result` scan, which would undercount exactly the runs that matter and shrink under retention
pruning. Threaded by a `.counting(..)` builder beside the existing `.announcing(..)`, so no new
dependency threading.
**The half that made it worth a slot** (and that overturned r23's "observability-only" rejection):
the shutdown-suspend arm had logged *"re-queued to resume from checkpoint"* unconditionally — false
exactly when `storage_error > 0`, with the sink holding that count in scope one line above. Now
downgraded and named. **A stale lineage deliberately does NOT downgrade it** — that means another
attempt owns the job and this task's state was supposed to lose.
