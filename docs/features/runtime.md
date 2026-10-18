# Job runtime, scheduler, budgets & costs

Pumper runs **apps** (implementations of `ScrapeApp`, registered in `crates/server/src/registry.rs`) as **jobs** on a durable SQLite queue.

**The registry carries the fleet's declaration ratchets.** An app's `manifest().output_shape` is not only documentation — `registry.rs` holds two-direction EXPECTED-diff inventories over it, so a judgment the fleet has standardised cannot be re-answered privately by the next app without failing a test **by name**:

| inventory | the declaration it pins | the roster that still owes it |
| --- | --- | --- |
| `completeness_inventory_tests` | a `completeness` block — "did this run get enough?" (`pumper_core::completeness`) | `AWAITING_THE_JUDGE` |
| `materiality_inventory_tests` | a `materiality {` block — "is this change news?" (`pumper_core::materiality`, see [datasets.md](datasets.md)) | `MONITORS_AWAITING_THE_JUDGE` |

Both fail in **both** directions — an app that newly declares and is not listed, and a listed app whose declaration vanished — and both exemption rosters only ever shrink: a listed app that adopts the judge fails the test until it is dropped from the roster, so an exemption cannot become permanent. Each is the generalised fix for the same defect found one layer apart: a judgment invented privately in four crates nobody else can reach.

## Jobs

- Enqueue: `POST /apps/{name}/jobs` with optional `{params, max_attempts, delay_secs, priority, callback_url, callback_secret, budget_usd, idempotency_key}`. An `Idempotency-Key` header (or body field) makes retries safe: a duplicate key returns the original job with `200` instead of a new `202`.
- Lifecycle: `queued → running → succeeded | failed | cancelled`, with one non-terminal detour: `running → waiting → queued` when a job parks on external input (see **Jobs that wait** below). Failures retry with exponential backoff up to `max_attempts`; the boot recovery sweep (`recover_stuck`) issues the *same* verdict to jobs orphaned by a crash — see **Recovery verdicts** below. `POST /jobs/{id}/retry` resurrects a failed/cancelled job with one extra attempt.
- **Job control surface:**
  - `POST /jobs/retry` — bulk resurrect: re-queues every job in a terminal state (`failed` default, or `cancelled`), optionally scoped to one `app`, up to `limit` (≤500), each with one extra attempt. Returns `{retried, ids}`.
  - `POST /jobs/{id}/reset` — re-queues a **running** job (e.g. one stuck on a hung task) with a fresh attempt budget (409 if not running).
  - `DELETE /jobs/{id}` — cancels a **queued** job synchronously, or signals a **running** job's per-job `CancellationToken` (registered in `AppState::job_cancels`) so the worker aborts the app future and marks it `cancelled` (not `failed`); a terminal job is 409.
  - **Every control event carries the job's real app.** Bulk retry and queued-cancel used to emit their `queued`/`cancelled` events with a **blank** app, and app-scoped watchers match the app exactly (`GET /mcp?app=…`) — so a bulk retry re-queued work invisibly to the one surface subscribed to it. `Storage::retry_bulk` now returns `(id, app)` pairs and `Storage::cancel` returns the cancelled job's app, both via `RETURNING` in the same statement, so the announce path stays at one query. Wire shapes are unchanged (`ids` is still a bare uuid array).
  - **Stale-write fence:** the worker's finish/fail/cancel writes are guarded on `(status='running', attempts)`. Because reset (and the reaper, below) re-queue the row and the next claim advances `attempts`, an orphaned task's late `complete`/`fail` matches no row and is discarded — the re-claimed attempt owns the outcome. The token registry entry is attempt-keyed so an overlapping re-claim is never clobbered.
- **Panic containment:** the worker runs each app future under `catch_unwind`. An app that panics fails its job **on the same tick**, through the normal attempt-fenced `fail()` path (fencing, backoff and retry are identical to an app-returned error), and `job.error` carries the panic payload prefixed `panicked: ` with its `file:line:col` when available — e.g. `panicked: called Option::unwrap() on a None value (at crates/apps/x/src/lib.rs:81:14)`. The prefix keeps a panic distinguishable from an app error, a `timed out after Ns` timeout, and the reaper's `lease expired (heartbeat stale)`. This cannot catch a **non-yielding** wedge (no unwind ever happens, so no branch is reached) or a hard abort — the reaper below stays the backstop for those.
- **Stuck-job reaper (heartbeat lease):** the worker stamps a `heartbeat_at` on each running job every `worker.heartbeat_secs` (default 30) — but only while the app future keeps yielding (`.await`-ing). A task wedged in a non-yielding loop stops beating; a slow-but-alive job keeps beating however long it runs. The beat is **non-blocking**: the tick (still inside the job's own loop) spawns the `heartbeat_at` write instead of awaiting it, with at most one write in flight (a tick while one is pending is skipped) — awaited in the loop, it stopped the app's in-flight queries from being polled and deadlocked on the connections they held, freezing a fetch fan-out for the 30s pool acquire timeout. The write is fenced on `status = 'running' AND attempts = <this attempt>`, so a beat landing after the job finished changes nothing. The reaper (piggybacked on the scheduler tick) re-queues any running job whose heartbeat is older than `worker.stale_after_secs` (default 120) using **failure semantics** — attempts + backoff apply, and an attempts-exhausted job fails permanently (`error: "lease expired (heartbeat stale)"`, with its callback + terminal triggers). This recovers a job hung on a live server (the 900s `job_timeout` only drops the future; it can't reach a task that never yields). Set `heartbeat_secs`/`stale_after_secs` to `0` to disable heartbeating / the reaper. The boot sweep still handles jobs orphaned across a full restart, and now applies the same verdict — see below.
- **Retention janitor:** one background loop (every 6h, `main.rs`) bounds every store that would otherwise grow forever — revision history, the artifact tree (provenance-pinned), the four append-only ledgers (`cost_events`, `webhook_deliveries`, `job_yield`, `saved_search_seen`) and extraction-health sketches. All of it is **off by default** under `[storage]`; the loop returns immediately when nothing is configured. Full key table, scoping rules and the `GET /retention/preview` dry run: [datasets.md § Retention](datasets.md).
- **Store janitor:** a second loop (every 1h, `main.rs`) bounds the stores whose growth is *derived state*, so it needs no opt-in and is always on: expired `http_cache` and `research_cache` entries, `http_cache` rows past `[cache] max_rows` (oldest-confirmed first), the `revalidations` log past `[refresher] retention_days`, and stale `tier_memory` rows past `[fetcher] host_memory_ttl_secs`. Every pass is bounded work (indexed deletes + one `LIMIT`ed eviction). Rationale per store, and why each was previously unbounded: [fetching.md § Cache growth bounds](fetching.md).

| key | default | bounds |
| --- | --- | --- |
| `[cache] max_rows` | `20000` (`0` = unbounded) | `http_cache` rows; evicts oldest-confirmed first. ~1–2 GB at typical 50–100 KB bodies |
| `[refresher] retention_days` | `30` | the `revalidations` log — applied by this loop, so it holds even with `[refresher] enabled = false` |
| `[claude] research_cache_ttl_secs` | `86400` (`0` = cache off) | how long a research answer is servable; expired rows are now deleted rather than kept forever |
| `[fetcher] host_memory_ttl_secs` | `604800` (`0` = no aging, no GC) | stale `tier_memory` rows with no pin/strikes/penalty |
- **Graceful shutdown:** on Ctrl-C / SIGTERM (on Windows: Ctrl-Break, console-close, or system-shutdown) the process cancels a shared token — the worker stops claiming new jobs, the scheduler and cache janitor stop, and `axum::serve` stops accepting. In-flight jobs are given up to `worker.shutdown_drain_secs` (default 25) to finish; whatever is still `running` at the deadline goes through the recovery verdict below (reason `drain deadline reached (shutdown)`) so it resumes cleanly on the next boot instead of being stranded. The same window then drains in-flight **fan-out** (which no longer holds a permit, so re-acquiring the permits is no longer proof the queue is idle); anything unfinished at the deadline is logged, not silently dropped.
- **A cancel during the drain is a cancel, not a deferral.** Drain phase 2 fires every in-flight job's cancel token to mean *suspend* (checkpoint + re-queue, resume next boot), and `DELETE /jobs/{id}` fires the **same** token to mean *stop*. The worker read both as suspend, so an operator's explicit cancel inside the drain window was silently converted into "run it again after the restart" while the door still answered `{cancelled: true, running: true}`. The door now claims the run for the operator — under the token-registry mutex the drain itself reads, immediately before firing — and `worker::cancel_kind` gives user intent priority over a concurrent drain: a suspend is a promise to run the work later, which is exactly what the operator asked us not to do. The one case a cancel cannot win is a run that already committed to a suspend microseconds earlier; that answers honestly with `{cancelled: false, running: true, suspended: true, note}` rather than a cancellation the queue contradicts. Jobs nobody cancelled suspend exactly as before.
- Lineage columns: `schedule_id` (fired by a cron schedule), `trigger_id` (fired by a reactive trigger — see [triggers.md](triggers.md)), `source_job_id` (which run's outcome fired this one), `principal_id` (the caller that enqueued it — see [auth.md](auth.md)), and `workflow_run_id` / `workflow_step` / `root_id` (this job is a step of a declared plan — see [workflows.md](workflows.md)). All nullable; NULL is the honest answer for work nobody declared or authenticated.
- **Querying the lineage (`Storage::JobQuery`).** The job list's predicates are one struct — `app`, `status`, `schedule_id`, `trigger_id`, `target_key`, `since`, `until` — and `Storage::list`, `list_page`, `status_counts_filtered` and `explain_job_list` all take it, so no two of them can disagree about what a filter means. Three facts worth knowing:
  - **The `WHERE` clause is assembled from only the predicates actually supplied** (`sqlx::QueryBuilder`, every value `push_bind`-ed, never interpolated — the same idiom `Datasets::list_filtered_trust` uses). This is not a style choice: the obvious alternative, one static statement with a `(?n IS NULL OR col = ?n)` clause per column, returns identical rows and **full-scans `jobs` every time**, because SQLite cannot use an index for a term it cannot prove is an equality while planning. Written that way the filter would work and the indexes 0012/0014/0041 paid for would stay exactly as unreachable as before. `explain_job_list` returns `EXPLAIN QUERY PLAN`'s `detail` rows so this is asserted, not asserted-in-prose: see `lineage_filters_ride_the_indexes_not_a_table_scan` in `crates/core/tests/jobs.rs`.
  - **The `created_at` window is half-open, `[since, until)`.** An inclusive upper bound returns the boundary row in two adjacent windows, which makes a day-at-a-time walk's counts wrong rather than merely off by one.
  - **Which index each form actually rides** (measured, not assumed): `trigger_id` → `idx_jobs_trigger` with *no* sort, the `ORDER BY created_at DESC` being that index's own tail; `schedule_id[&status]` → `idx_jobs_schedule`; `app[&since]` → `idx_jobs_app`, the window becoming a range on its `created_at` tail; `status&target_key` → `idx_jobs_status_target`. **Known gap:** migration 0041's index is `(status, target_key)` and `status` *leads*, so a bare `target_key` with no `status` scans the table. The scan is pinned by that same test, so the gap cannot be quietly mis-documented; closing it is an index re-order this surface does not own.
- `status_counts_filtered` is the scoped twin of the `/metrics` aggregate: one `GROUP BY status` over the same `WHERE`, metered as `StoreOp::JobStatusCounts`. With no predicate the two agree exactly (asserted), because two spellings of one count is how a dashboard and a triage page come to disagree about the same queue. Statuses the window does not hold are **absent**, not zero.
- Worker (`crates/server/src/worker.rs`): global concurrency cap + per-app caps (`[worker]` config); wakes instantly on enqueue. After a successful run it indexes the result into search, computes the run's revision batch once, then fires dataset watches, dataset triggers, and saved searches; `finalize()` emits the terminal SSE event, the result webhook, and terminal-job triggers. All side effects are fail-open.
- **Off-slot fan-out.** Everything in that list runs *after* the completion write, as one unit on a bounded pool (`crates/server/src/fanout.rs`) instead of on the job's own worker permit — a slow index or a large materialization no longer burns a scrape slot. Consequences to know:
  - The job reaches `succeeded` (and `GET /jobs/{id}` shows its `result`) **before** its documents are searchable and before its watches/triggers fire. Poll the terminal SSE event, not the row, if you need "the fan-out is done".
  - The order inside the unit is unchanged and still load-bearing: health gate → contract gate → watches → dataset triggers. It re-checks the `(status, attempts)` fence when it starts, so a job reset or reaped between the completion and the fan-out fires nothing.
  - `[worker] fanout_concurrency` (default 4) sizes the pool; `0` runs the fan-out inline on the job's permit (the historical behaviour). `[worker] fanout_max_queued` (default 64) is the backlog ceiling — at the ceiling a job runs its own fan-out inline (logged at `warn`). Backpressure is never a drop: an unsent webhook must not be a silent one.
  - Graceful shutdown drains the pool too, and logs `abandoned=N` if any unit is still running at the deadline.
- **App-declared schedules.** A run that called `AppContext::request_schedule(body)` (a `POST /schedules`-shaped `{app, cron, params?}`) has its requests applied right after the run returns, **only when it finished OK** — a panicked, timed-out or cancelled attempt will be retried, and a flapping job must not mint a schedule per retry. Rows are written through `Storage::upsert_managed_schedule` tagged `managed_by = "app:<the requesting app>"`; the tag is set by the runtime and cannot be supplied in the body, so the SQL ownership fence keeps an app-declared row from ever overwriting a hand-made or catalog-managed schedule with the same id. The id is derived (content-addressed over requester, target app, cron and canonical params), so re-running the same job re-syncs one row instead of creating a schedule per run, and an unreadable cron is refused at the seam rather than stored as a schedule that never fires. `[worker] max_app_schedules_per_run` (default 20, `0` = refuse all) bounds one run; past it `request_schedule` answers `false` so the app can report the cut (`research` reports `watch_requests_truncated`). Refusals and fence misses are logged with counts, never swallowed — and so is the cap itself: `ScheduleRequestOutcome` reports `created`, `rejected`, `not_owned` **and `over_cap`**, so `created + rejected + not_owned + over_cap` always equals the number of requests the run made. The surplus the cap dropped used to be counted in none of those, which mattered most at `0`, where the worker's summary line (gated on `created > 0 || not_owned > 0`) did not fire either and a run that dropped every schedule it asked for left no trace at all. First consumer: `research`'s `watch_sources` ([apps.md §research](apps.md#watched-sources-app-declared-schedules)).
- **Stage timings.** Each succeeded run stamps where its wall-clock went into `job_stages` (migration 0034): `run_ms` (the app's own `run()`), `index_ms`, `hooks_ms` (revisions + gates + watches + triggers), `alerts_ms` (saved searches, including the forced index flush and materialization) and `total_ms` (claim → end of fan-out). The same object rides the terminal SSE event as `stages`. Every field is nullable and a `null` means **unmeasured**, never zero — a job that failed before its fan-out has no stage row at all.
- **Claim order / priority aging:** the worker claims the queued job with the highest *effective* priority = `priority + waited_secs / worker.priority_aging_coefficient_secs`. Aging is a starvation guard — a low-priority job stuck behind a continuous high-priority stream escalates as it waits (default coefficient `900`s → +1 level every 15 min) instead of never running. Set the coefficient to `0` to disable aging and fall back to strict `priority DESC, created_at`. Equal-(effective-)priority claims stay FIFO (oldest `created_at` first).

### Recovery verdicts — one policy, three triggers

A `running` row whose executor is gone gets exactly one verdict, and it does
not depend on *what noticed*. Three things can notice:

| Trigger | Selection | Reason stamped |
| --- | --- | --- |
| boot sweep (`recover_stuck`, before the port binds) | every `running` row | `interrupted at boot (executor gone)` |
| lease reaper (per scheduler tick) | `running` rows whose heartbeat is older than `stale_after_secs` | `lease expired (heartbeat stale)` |
| shutdown drain deadline | every still-`running` row | `drain deadline reached (shutdown)` |

All three route each row through the same `fail()` door, so the attempt ladder,
the jittered backoff and the permanent-failure threshold apply identically: a
job with attempts left is re-queued *with backoff*, and a job whose attempt
budget is exhausted fails permanently rather than earning a free attempt by
killing the process. The reason lands in `job.error`, so "the boot sweep did
this" stays distinguishable from "the executor failed this" forever.

The boot sweep used to be a single blanket `UPDATE … WHERE status='running'` —
a different policy from the reaper's, applied to the same rows, with no reason
and no bound. It is now bounded twice (1,000 rows, 5s of wall clock) because it
runs inside boot; hitting either bound is logged as `truncated` and is safe, the
sweep being idempotent and the reaper continuing the work. It reports per class
— `requeued` / `failed` / `skipped` — where `skipped` counts rows a live worker
resolved between the scan and the verdict (the `(status, attempts)` fence), so a
sweep that verdicted fewer rows than it scanned is legible rather than silent.

## Jobs that wait (`waiting`)

A running app can say *"I need something from outside before I can continue"* — a human approval, a 2FA code, an agent's answer to a clarifying question — and park itself **without holding a worker permit**.

```rust
// inside ScrapeApp::run
let Some(answer) = ctx.restore_input().cloned() else {
    // No answer yet: checkpoint the reversible work, then park.
    return Err(ctx
        .await_input(
            json!({ "stage": "prepared", "amount": 42 }),   // the resume snapshot
            json!({ "kind": "approval", "prompt": "submit $42?" }), // what you're asking for
        )
        .await);
};
// A later attempt lands here with `answer` in hand and the checkpoint restored.
```

- **`ctx.await_input(resume_state, request)`** forces a checkpoint of `resume_state` (`checkpoint_now`, so the 5s throttle cannot eat it) and returns `Error::AwaitingInput(request)`, which the app propagates. The forced checkpoint is the contract: a park is a promise to resume *from here*, and without a landed snapshot the resumed attempt would restart from the top and re-ask a question somebody already answered. Pass `Value::Null` if there is genuinely nothing to resume from.
- **The worker's outcome arm** (above the terminal-failure arm — a park is not a failure, so `is_terminal_for_job` is never consulted) writes `status = 'waiting'` fenced on `(status='running', attempts)`, keeps the checkpoint, leaves `finished_at` NULL, **burns no attempt**, releases the permit, and publishes a **non-terminal `waiting` job event** carrying the request — so `GET /jobs/{id}/stream`, `/events` and the MCP live feed stay open.
- **Columns** (migration `0050`): `input_request` (what the job is asking for), `waiting_since`, `waiting_expires_at`, `resumed_input`. The first three ride `GET /jobs/{id}`; `resumed_input` is **never serialized**, for the same reason as `callback_secret` — the payloads it exists to carry are approvals, one-time codes and credentials, and the read API is unauthenticated on a default install.
- **`POST /jobs/{id}/resume {input}`** stores the answer and re-queues the job with `reset`'s attempt headroom (`max_attempts = MAX(max_attempts, attempts + 1)`), so a job that waited an hour for a human still has every retry it started with. `input` is optional (omitted = `null`, a fine answer to a pure "proceed?" gate). The write is guarded on `status = 'waiting'`, which makes the door **idempotent by refusal**: a second resume — a stale browser tab, an agent retry, a second approver — is `409`, never a second live lineage for one job. `404` if the job does not exist. The resumed attempt reads the answer through **`ctx.restore_input()`** and its checkpoint through `ctx.restore()`.
- **`DELETE /jobs/{id}` reaches a parked job.** A `waiting` job has no in-flight future to interrupt, so it is cancelled synchronously through the same guarded write as a `queued` one (the guard is `status IN ('queued','waiting')`). Without that it would be uncancellable: the door finds no registered token, falls through, and tells the operator the job is *already terminal*. `POST /jobs/{id}/retry` deliberately does **not** reach it — resume is the door, and retry would drop the answer on the floor.
- **The schedule slot is held.** `run_holds_slot` (the single predicate behind the cron overlap guard *and* `GET /schedules` health) counts `waiting` as active: a parked run is mid-work, so stacking the next firing on top of it would do the same work twice.
- **The reaper does not touch it.** The lease reaper selects `status = 'running'` on a stale heartbeat; a parked job has no executor and no heartbeat, so it is naturally outside that scan. Its policy is a *deadline* instead: with `[waiting] expiry_secs > 0` a park stamps `waiting_expires_at`, and a sweep on the scheduler tick fails an unanswered wait permanently with `error: "awaited input not provided before waiting_expires_at"` — through `finalize`, so the result callback, the failure firehose and terminal triggers all fire like any other permanent failure.
- **Metrics:** `pumper_jobs{status="waiting"}` — always emitted, `0` when nothing is parked (an absent series is indistinguishable from a scrape failure, and "how many jobs are blocked on a human right now" is the number this feature exists to make answerable).
- **MCP:** `wait_job` settles on a terminal status **or** on `waiting`, returning `{status: "waiting", input_request, note}`; `resume_job` answers it (gated by `[mcp] allow_enqueue`, like every other tool that lets a job spend). `list_jobs`/`GET /jobs?status=waiting` is the inbox. See [mcp.md](mcp.md).

| `[waiting]` key | default | meaning |
| --- | --- | --- |
| `expiry_secs` | `0` | How long a parked job may wait before the sweep fails it. **`0` = wait forever**, which is byte-for-byte the behaviour of a build without this feature. A deadline is opt-in on purpose: the alternative default is a timer that fails approvals nobody got to over a weekend. |
| `expire_batch` | `100` | Rows one sweep pass will fail before stopping (the sweep piggybacks the scheduler tick). |

**The first production consumer is `transact`** (N01): a flow enqueued with `submit: true` runs its dry run, stages a `pending` row in the transactions ledger, and parks with the evidence bundle as its `input_request`; `POST /transactions/{id}/approve` is what resumes it. Note the shape that door adds on top of the generic one — the resumed run does **not** treat its `input` as authority. It re-reads the ledger and refuses unless the row itself says `approved`, so a hand-posted `POST /jobs/{id}/resume` cannot release an irreversible action. An app whose resume grants real authority should copy that: the resume payload says *which* decision was made, the durable record says *that* it was. See [apps.md § transact](apps.md#transact-evidence-approval-submit). The other consumer is the `approval` test app in `crates/server/src/e2e/waiting_resume.rs`.

**Not in this slice:** `research`'s clarification loop is not ported. A deadline is per-server, not per-park: an app cannot ask for its own expiry window — which is why `transact` carries its own `[transact] approval_ttl_secs` for the *approval*, separate from `[waiting] expiry_secs` for the *park*.

## Live progress

A long-running app reports compact progress snapshots through `AppContext::progress` (a `ProgressReporter`). The runtime (`crates/server/src/progress.rs`) keeps only the **latest snapshot per in-flight job in memory** (no jobs-table write — a restart drops it, which is acceptable: the job re-queues and re-reports) and emits it as a `progress` job event through the EventBus. Each reporter **throttles** its own persist+emit to ≥ every 2s or every 50 `report` calls, so an in-loop stride never floods the bus.

- `GET /jobs/{id}` includes a `progress` field with the latest snapshot while the job runs (absent once terminal or after a restart) — the rest of the job JSON is unchanged.
- The snapshot rides `progress`-status job SSE events on `/jobs/{id}/stream` and `/events` (non-terminal, so the per-job stream stays open); monotonic ids / replay semantics are unchanged.
- The `crawl` app reports `{crawled, kept, failed, frontier, hosts}` (see [crawling.md](crawling.md)).

## Durable execution (checkpoints)

A long-running app persists its **resumable unit** through `AppContext::checkpoint(state)` (throttled — safe to call in a tight loop) or `AppContext::checkpoint_now(state)` (unthrottled, for the snapshots whose loss costs real work). The runtime's sink (`JobCheckpointer`, `crates/server/src/progress.rs`) writes the JSON blob to the `checkpoints` table keyed by job id; the next attempt of the same job gets it back as `ctx.restore()`, so a crash, a reaper re-queue, a timeout or a graceful-shutdown suspend costs a *resume* instead of a restart. The state is **advisory**: an app must tolerate any stored shape and start fresh on doubt, and a succeeded job's checkpoint is cleared (spent state).

**The failure contract.** Both calls return a `bool`, and the answers do not all mean the same thing:

| return | when | what it means |
| --- | --- | --- |
| `true` | the write landed | the state is durable |
| `true` | the save was **throttle-skipped** — a non-`force` call inside the 5s `CHECKPOINT_MIN_INTERVAL` | deliberately *not* a failure: the previous snapshot is still durable and the caller keeps its loop cheap. Never count it as a loss |
| `false` | **stale attempt lineage** — the job was reset, reaped or re-claimed mid-run, so a newer attempt owns the row (`Storage::save_checkpoint`'s `… AND attempts = ?` fence) | this task's state is stale and must never overwrite the live attempt's. Another attempt owns this job |
| `false` | **storage error** — a locked DB, a full disk, or a blob over `MAX_CHECKPOINT_BYTES` (8 MiB) | this run's durability is gone until it clears: a reap or restart resumes it from nothing |

A failed save **never fails the job** — losing persistence must not take down work that otherwise succeeded.

**Where a dropped checkpoint shows up.** The sink counts its own failed saves, by kind, and reports them on three surfaces (no schema migration, no app-side change — every app's `ctx.checkpoint(..)` call site is untouched):

- **`checkpoint_failed` job events**, emitted the **first** time each kind occurs, on `/events` and `/jobs/{id}/stream` (`{reason: "stale_lineage" | "storage_error", attempt}`). Non-terminal, like `progress`, so the per-job stream stays open. Only the first of each kind is announced: a run whose lineage went stale fails *every* later save, and one event per save would flood the bus with the same fact.
- **`checkpoint_failures` on the stored result** of a succeeded job — `{stale_lineage, storage_error, total}` — so the loss outlives the run and rides `GET /jobs/{id}`, `GET /jobs/{id}/receipt` and the terminal SSE event. The live progress snapshot could not carry this: `finalize` clears it.
- **`pumper_checkpoint_failures_total{reason}` on `/metrics`** — the process-lifetime tally on `AppState`, handed to each run's sink by `JobCheckpointer::counting(..)` and incremented **at the failure**, before any outcome exists. This is the only one of the three that is independent of how the job ended: the stored-result stamp is written in exactly one of the worker's seven outcome arms (`Finished(Ok)`), so the other six — cancel, shutdown-suspend, terminal failure, retryable failure, panic, timeout — used to count nowhere at all.

The block is **absent when every save landed** (no fabricated zeros — its presence is the signal), and a throttle-skip never contributes to it. Before this, both `false` paths were a `tracing::warn!` and nothing else: a long harvest whose checkpoints stopped landing at minute 3 resumed from nothing on its next reap or restart and redid hours of governor-paced fetching, while `GET /jobs/{id}` looked healthy the entire time.

**The shutdown-suspend log states what it can prove.** The suspend arm re-queues the job unconditionally (that part never depended on checkpoints), but *"re-queued to resume from checkpoint"* is only true if this run's saves landed. When `storage_error > 0` the line now says the job resumes from the last snapshot that did land — which may be older than the work already done, or absent entirely — and names the count; both counts also ride as structured fields (`checkpoint_storage_errors`, `checkpoint_stale_lineage`). A stale lineage does not downgrade the claim: it means another attempt owns the job, and that attempt's checkpoint is the one being resumed.

**Known gaps.** Apps do not yet *react* to a failed save (none of the 11 call sites reads the `bool`) — the failure is observable, not acted on. A run that never reaches a stored result (failed, timed out, or shutdown-suspended) still has no *per-job* record of its tally — it is counted on `/metrics` and announced on the bus, but the number is fleet-wide there, so "which job lost its state" is answerable only from the log; an app whose result is not a JSON object is logged for the same reason. The `/metrics` tally is per-process and resets on restart, so a crash loses the count with it.

**Testing the seam.** `pumper_core::testing::RecordingCheckpoints` (in the `test-support` harness) is a `CheckpointSink` that **records** every `(state, force)` pair instead of persisting it — `saves()`, `save_count()`, `last_state()` — and can be told to fail: `failing()` (every save) or `failing_from(n)` (the nth save on), which is the only way the `false` branch above is reachable from a test. Wire it with `TestContext::new(&storage, app).checkpoints(sink)`; the default is unchanged (`NoCheckpoints` — silently drops the state and reports `true`). A failed save is still *recorded*, because what an app tried to persist is the evidence of what the loss cost. This is what makes "the app checkpointed its work list **before** fetching the first item" an assertion instead of a comment.

## Scheduler

DB-backed cron (6-field, with seconds) reconciled every `schedule_tick_secs`. Apps can declare a static schedule (`ScrapeApp::schedule`, seeded idempotently); runtime CRUD via `GET/POST /schedules`, `DELETE /schedules/{id}`, `POST /schedules/{id}/enabled`, `POST /schedules/{id}/budget`, plus the read-only decision ledger `GET /schedules/{id}/runs` (see [The decision ledger](#the-decision-ledger-get-schedulesidruns)). **Overlap guard:** a schedule whose **most recent** run is still queued/running skips the tick without touching `last_run`, so exactly one catch-up run fires when it frees up. The guard and the `health` field on `GET /schedules` are the same read and the same predicate (`scheduler::latest_run` → `run_holds_slot`), so the API's answer is the scheduler's answer.

**What the tick actually is.** This loop is the process's only periodic timer, so five other jobs ride it rather than owning one of their own — in this order, after the cron reconcile:

1. the **stuck-job reaper** (above), with the **trigger-decision-ledger prune** nested inside it (hourly, not per tick — see [triggers.md](triggers.md));
2. the **webhook dead-letter drain** (`[webhooks] auto_retry`, see [events-webhooks.md](events-webhooks.md)) — runs in-line, so a slow receiver parks the tick;
3. the **cache refresher** (`[refresher]`, default OFF, see [fetching.md](fetching.md)) — spawned, idle-slot only;
4. the **DataHub governance poll** (`[datahub] govern`, default OFF, see [datahub.md](datahub.md)) — interval-gated and spawned;
5. the **scheduled-workflow pass** (see [workflows.md](workflows.md)) — a declared plan carrying a `cron` fires one run per firing, held while its newest run is still open. It reads the same `last_pass` the cron reconcile does, so the first tick after boot fires nothing.

Before the first tick the loop runs one **boot catalog reconcile**: it always plans and logs drift between `catalog/data-sources.toml` and the `schedules` table, and applies it only under `[catalog] auto_reconcile = true` (default OFF). An unreadable catalog is a warning, never a reason not to serve the rows that already exist.

Because everything above shares one task, the tick is **contained rather than fragile**: a panic anywhere inside it is caught, logged loudly (`scheduler tick PANICKED and was contained`) and the loop continues; one schedule's storage error is logged with its id, counted, and the pass continues with the remaining schedules instead of ending there (rows come back ordered by app, so a `?` mid-loop used to mean every later schedule silently stopped firing while `GET /schedules` still said `ok`). On shutdown the loop stops enqueuing between schedules, and the process **joins** the task so a tick finishes rather than being torn down between an `enqueue` and its `touch_schedule`. Immediately before an enqueue the row is re-read and re-checked for `enabled`, so a disable (from the API, a catalog reconcile, or DataHub governance) that lands *while the pass is walking the table* stops that firing rather than only the next one. The same re-read supplies the firing's `budget_usd`, so a ceiling set mid-pass binds that firing too (`params`/`priority` still come from the snapshot the pass validated — neither of those is a spend).

> The guard used to be existential over *all* of a schedule's jobs while `health` read only the newest. `POST /jobs/{id}/retry` on an **old** job of a schedule re-queues it without moving `created_at`, and nothing clears `schedule_id` — so the guard stayed satisfied forever, the schedule silently stopped firing, and `GET /schedules` still said `ok`. `POST /jobs/retry` with `{app}` could wedge every schedule of that app in one call. Keying both on the newest firing keeps "don't stack a run on top of mine" and drops the wedge.

Each schedule carries three cron-maturity fields (`schedules` table cols `timezone`, `misfire_policy`, `max_attempts`; all set at `POST /schedules` and returned by `GET /schedules`):

- **`timezone`** — IANA name (chrono-tz), e.g. `"America/New_York"`; `null` = UTC. The cron expression is evaluated in this zone, so DST transitions are honoured: a firing at a wall-clock time that doesn't exist (inside a spring-forward gap) is skipped to the next valid one. An unknown name is rejected at create time with `400 bad_request`.
- **`misfire_policy`** — how firings missed while the scheduler was down are handled once it's back. `"fire_once"` (default) runs a single catch-up job (collapsing the whole backlog into one run — the historical behaviour, now explicit); `"skip"` runs none of the missed ones and advances past them, recording them as `last_skipped_at` + `skipped_count` (below).

  **What "missed" means.** A firing is missed when *a previous reconcile pass already had the chance to fire it and didn't* — the cutoff is the previous pass's own timestamp, so a firing that came due since then is on-time by construction, however long the tick took. Only on the first pass after boot (no previous pass to measure against) does the fallback apply: more than two `schedule_tick_secs` late counts as missed. Before this the cutoff was always derived from the *configured* tick, so any slow tick — the webhook dead-letter drain runs in-line on this loop — reclassified a firing the process was up for as a misfire, and `skip` ate it.

  **Classification is per firing, not per batch.** `skip` on an hourly schedule that missed 11:00 while down and is back at 12:00:05 advances past 11:00 **and still runs the due 12:00 firing**, in one tick (`skipped_count` +1, `last_run` stamped). Previously the whole pending batch was classified from its oldest member, so both were skipped and a legitimately-due run was silently dropped. The invariant is now true in every case: **an on-time firing runs under both policies.**

  **Both policies pass the same gates.** A schedule that could not have fired anyway — unregistered app, or effective params that fail the app's schema — records *no* skip advance either (`skipped_count` and `last_skipped_at` stay put), so fixing the row makes it fire instead of leaving it having quietly advanced past its own backlog. Same for a firing the overlap guard is holding.
- **`max_attempts`** — attempt budget for jobs this schedule enqueues; `null` = server default (**3**), so scheduled runs retry transient failures with backoff exactly like a manual job (previously cron runs were hardcoded to a single attempt).

**Retired sources are skipped.** An `extractor` schedule whose `params.dataset` (default `extracted`) names a source whose **stored** health state is `retired` does not fire: the firing is recorded as a skip (`last_skipped_at`, `skipped_count` — the eaten firings), `last_run` does not move, and nothing is enqueued. This holds whether or not `[resilience] enforce` is on — it is a scheduling decision, not a data gate. The state is read every tick, so `POST /sources/{id}/state` back to any other state resumes the schedule at its next firing. The reason is named on the API: `GET /schedules` reports `health: "retired"` for such a row (it is one of the admission verdicts below), so the skip no longer has to be read out of the logs. The *verdict* is still never stored on the schedule row — migration 0039 has no skip-reason column, deliberately: it is derived on every read, so `POST /sources/{id}/state` back out of `retired` clears it in the same instant the scheduler starts firing the schedule again. What **is** stored, since migration 0047, is the *history*: each eaten firing writes one `schedule_runs` row carrying `reason: "retired"`, so a retirement skip and a misfire skip are no longer byte-identical after the fact (see [The decision ledger](#the-decision-ledger-get-schedulesidruns) below). Only extractor schedules are affected; no other app's params name its source. See [resilient-extraction.md §2.9](resilient-extraction.md).

**Spend ceiling (`budget_usd`, `schedules` col, migration 0040).** The ceiling replayed into **every** job this schedule enqueues, identical in meaning and effect to the `budget_usd` of the same app enqueued at the jobs door (see [Budgets](#budgets--the-cost-ledger)). Set at `POST /schedules`, changed later with `POST /schedules/{id}/budget` (`{"budget_usd": 2.0}`, or `null` to remove it), returned by `GET /schedules`.

- **`null` = no ceiling, and that is still the default.** No ceiling is invented for a schedule that doesn't set one — a standing order that ran unlimited yesterday runs unlimited today. What changed is that a ceiling is now *expressible*: before this the fire path built its enqueue options from `Default`, so `budget_usd` was `None` for every scheduled run and a nightly Claude-tier research schedule was the one work shape that could spend without limit while the same app enqueued by hand honoured its cap.
- **`0` is refused (422), not reinterpreted**, by the same `routes::jobs::validate_budget_usd` the jobs and trigger doors use — at both `POST /schedules` and `POST /schedules/{id}/budget`. On a schedule that decay would be worse than one bad job: the value lives on the row and is replayed forever, so a silent `0 → null` is a standing unlimited-spend order.
- **Rows that were never created over `POST /schedules` start `NULL`, and `POST /schedules/{id}/budget` is how they get a ceiling.** That covers both code-seeded rows (`ScrapeApp::schedule()`, id `static-<app>`) and catalog-managed ones: `catalog/data-sources.toml` has no budget vocabulary, deliberately — money in a file whose reconciler overwrites rows is a footgun. The reconciler's writes touch `cron`/`enabled` only, so a re-apply cannot wipe a ceiling an operator set on a managed row.

**Observability (`GET /schedules`).** Each returned schedule is enriched with computed fields so "when does this next fire?", "did the last run succeed?", and "why has this gone quiet?" are answerable over the API instead of only in server logs:
- **`next_run`** — the next firing, projected with the scheduler's own reference rule (`cron.after(max(last_run, last_skipped_at) ?? created_at)` in the schedule's timezone — one shared `scheduler::schedule_reference`), so the API can never disagree with the reconcile loop. `null` for an unparseable cron.
- **`last_job_id` / `last_status`** — the most recent job this schedule enqueued (joined on `schedule_id`), or `null` if it has never fired.
- **`last_run`** — when this schedule last **enqueued a job**, `null` until it has. It is only written by the fire path, so it can no longer contradict `last_job_id`.
- **`last_skipped_at` / `skipped_count`** (`schedules` cols, migration 0039) — when the `skip` misfire policy last advanced past missed firings without enqueuing, and the cumulative count of firings it has eaten over the schedule's life. This is what makes "my `skip` schedule quietly does nothing" visible; previously the advance was written to `last_run` (so the row claimed a run that never happened, beside a null `last_job_id`) and the count existed only in a log line.
- **`health`** — `ok` | `disabled` | `invalid_cron` | `unregistered_app` | `invalid_params` | `retired` | `overlapping`. **This is the fire path's own admission verdict, read verbatim — not a second derivation of it.** One firing gets exactly one verdict from one closed vocabulary (`scheduler::ScheduleAdmission`), produced by one `scheduler::admit()` that both the reconcile loop and this endpoint call, and the values above are its variants' names. The precedence is the vocabulary's declaration order, highest gate first:

  | `health` | what it means | the operator's move |
  | --- | --- | --- |
  | `disabled` | the row is switched off; its cron is never even parsed | `POST /schedules/{id}/enabled {true}` |
  | `invalid_cron` | the cron is unparseable or has no future firing (`next_run` is `null`). Only reachable by editing the row — the create door refuses it | fix `schedules.cron` |
  | `unregistered_app` | no app of that name is registered in this binary | deploy the app, or delete the row |
  | `invalid_params` | the row's effective params fail the app's declared schema, so the fire path refuses it (see *Params: same door, same answer* below) | fix `schedules.params` |
  | `retired` | an `extractor` schedule whose source (`extractor/<params.dataset>`) has stored health state `retired`, so every firing is eaten as a skip (see *Retired sources are skipped* above) | `POST /sources/{id}/state` out of `retired` |
  | `overlapping` | this schedule's own most recent run is still queued/running and the overlap guard is holding the next firing back — the one case where an ever-older `last_run` otherwise looks like a dead schedule. Read through the *same read and predicate* the guard uses, not a second query | let the run finish, or cancel it |
  | `ok` | nothing is stopping the next firing | — |

  Every verdict is **derived on each read, never stored**: fixing the row (or restoring the source) clears the marker in the same instant the scheduler starts firing it again, where a stored flag would have to be un-set by somebody. Two inventory tests keep the vocabulary single — one fails if this endpoint re-answers any gate for itself, the other if the declared variants and the `health` values the OpenAPI description publishes drift apart, so a seventh gate cannot land without the API learning its name.

  > Before this, the endpoint re-derived `health` in its own `if/else` chain. It knew five of the scheduler's six gates and never learned about retirement — so a schedule over a retired source answered `health: "ok"` beside a confident future `next_run` while eating a firing on every tick, and the only way to find out was the server log or the SQLite file.

### The decision ledger (`GET /schedules/{id}/runs`)

`health` and `next_run` answer *"what will this schedule do next?"*. The ledger answers the other half — *"what did it already swallow?"* — which had no answer outside the server log, and for the most common non-fire not even there.

**One row per non-idle tick verdict** (`schedule_runs`, migration 0047), negatives included. This is the schedules-shaped twin of the trigger decision ledger (`trigger_runs`, migration 0036) and deliberately so: same table shape, same fail-open writer contract, same keyset paging, same age bound off the reaper tick. An **idle** tick writes nothing — one row per schedule per tick for doing nothing is the one write rate this table must not have.

| field | what it carries |
| --- | --- |
| `outcome` | `fired` \| `skipped` \| `held` \| `refused` \| `failed` (`pumper_core::storage::SCHEDULE_OUTCOMES`) |
| `reason` | **the `health` word above, read verbatim** off the one `scheduler::admit()` verdict. `null` only when admission never ran (a storage failure inside the step), where `detail` is the answer |
| `due_at` | the firing this row is about, projected with the same reference rule `next_run` uses |
| `job_id` | the job enqueued — or, for `held`, the job still **holding the slot**, which is the one the operator has to go look at |
| `missed` | firings advanced past by `misfire_policy = "skip"`, **per occurrence**. `schedules.skipped_count` does accumulate this number (it is `skipped_count + missed`, not `+ 1`) — but as a *lifetime total with no window and no reason*, so four firings eaten in one tick and one eaten in each of four ticks are the same `4`, and so are a retirement skip and a misfire skip. The row is what separates them |
| `detail` | free text: an error message, the retired dataset, the collapsed backlog size |

The three silences it ends:

- **`held`** — the overlap guard's hold recorded *nothing at all*, and correctly so: `record_schedule_skip` stamps `last_skipped_at`, which would advance the cron reference past the very firing the guard just promised to keep due. A ledger row advances nothing, which is why the hold can finally be written down. Three held ticks are three rows naming the same `due_at` and the same blocking `job_id`, and `last_run` does not move.
- **`skipped`** — a retirement skip and a misfire skip wrote the identical two columns, so the store could not tell them apart. `reason` now does: `retired` for the gate, `ok` for the misfire policy (nothing *refused* that firing — the policy ate the backlog, and `ok` is what the one admission vocabulary calls the absence of a reason not to fire). A `skip` tick that also runs its on-time firing writes **two** rows, not one: the skip carries its `missed` and the fire carries its `job_id`, because collapsing them is exactly the flattening `skipped_count` already does wrong.
- **`failed`** — a per-schedule storage error inside the pass was `PassTally::failed += 1` and one aggregate log line, so the schedule that silently did nothing could not be named. It is now a row with the error text. (The pass itself still continues with the remaining schedules — that is unchanged.)

**`reason` is not a new vocabulary.** It is `ScheduleAdmission::health()`, the same closed list the fire path acts on and `GET /schedules` publishes. This context's whole problem was three separate authorities on "why isn't this firing?"; an inventory test fails if `scheduler.rs` spells a ledger `reason` as a string literal at all, so a fourth cannot be added by accident.

**Fail-open, and bounded at birth.** A decision that could not be recorded never holds up the firing it describes — dropping the table entirely still enqueues the job and still stamps `last_run`. The ledger is *diagnostic*, not evidence: the reaper tick prunes rows past 14 days alongside `trigger_runs`, in the same function and under the same hourly due-check, so a second ledger cannot end up the one that grows forever.

**The pass heartbeat (`scheduler: {last_pass_at, stale}`).** The one scheduler failure the loop cannot log. `reconcile` failing at `list_schedules` returns before it looks at a single schedule, so no firing gets its chance, every row keeps answering `health: "ok"` beside a confident future `next_run`, and `last_pass` — a loop-local — never advances. From outside the process that was indistinguishable from "nothing was due", and a liveness probe against a live HTTP server passes it.

A completed pass now stamps a durable heartbeat, gated by the **same** `completed` bool that advances `last_pass`, so the durable fact and the in-memory one cannot drift (an inventory test pins the pairing). `stale` is true when no pass has completed within four configured ticks (floored at 60s so a sub-minute tick cannot make the verdict flap), and **also when no pass has ever completed** — "never" is the honest answer and the worst case, not a reason to report healthy.

The heartbeat is **one upserted row**, not one row per pass. The trigger ledger shows what the append shape costs: a fact written once per process as an ordinary row paginates out of reach of the newest-first page it was meant to be read from. A fixed primary key makes the table's size independent of the tick rate and makes "when did a pass last complete?" a point read — and the row is **exempt from the prune**, because deleting a stale heartbeat would delete the evidence of the very wedge it exists to prove.

It is published from one builder on both `GET /schedules/{id}/runs` and the **cursored** `GET /schedules` envelope. The legacy bare-array mode of `GET /schedules` cannot carry it — that response is a JSON array and has nowhere to put a field.

> Known gap: an empty `decisions` page on a schedule that has genuinely never been due looks the same as one whose rows have aged out of the 14-day window. The `scheduler` block distinguishes "the loop is not running" from both, but not "it ran and the evidence expired".

**Params: same door, same answer.** A schedule's effective params are the app's `default_params` with the schedule's own `params` **shallow-merged over them** — byte-identical to what `POST /apps/{name}/jobs` does with a request body, and what `ScrapeApp::default_params` ("params used for scheduled runs") always promised. `POST /schedules` validates that merged object against the app's declared `params_schema` and answers **422** with the same JSON-pointer message the enqueue door uses, so a standing order can no longer hold params a one-off run would have been refused. The fire path re-checks (rows can predate the door, or be edited in SQL): an invalid row is **skipped without touching `last_run`** — so fixing the params makes it fire on the next tick rather than having silently eaten firings — and shows as `health: "invalid_params"` on `GET /schedules`.

> Before this, `POST /schedules` stored the body verbatim and the scheduler **replaced** the defaults wholesale instead of merging, so the same app ran with different params depending on which door created the work, and a bad schedule surfaced only as a failed job with no link back to the row.

## Budgets & the cost ledger

- Every metered engine call (`AppContext::fetch` / `AppContext::research`) writes a `cost_events` row (job, app, engine tier, url, `cost_usd` — Claude actual, free tiers 0.0, detail incl. escalation trail / `cache_hit`).
- `budget_usd` on a job is a hard ceiling: metered Claude calls abort once cumulative spend reaches it; per-call `max_budget_usd` is clamped to the remaining headroom. `AutoWithResearch` fetches degrade to free tiers instead of failing (noted in the escalation trail); explicit `ctx.research` errors loudly.
- **`budget_usd` must be > 0, and omitting it means *no* ceiling.** Those two facts are why `POST /apps/{name}/jobs` answers **422** to `budget_usd: 0` (or a negative/NaN/∞ value) instead of accepting it: the door used to filter a non-positive value away to `None`, and `None` is exactly "unlimited", so the most cautious request produced the least limited job. The refusal message names the alternatives — pass a real ceiling, or express "free tiers only" with the *app's* own zero-budget param (the `research` app's `"max_budget_usd": 0`) or over MCP, where the `[mcp] max_job_budget_usd` rail treats `0` as a real `$0` ceiling because it always sends `Some(budget)`. Reference: `routes::jobs::validate_budget_usd`.
- **Every work-creating door now has the same floor.** `POST /apps/{name}/jobs`, `POST /triggers`, `POST /schedules` and `POST /schedules/{id}/budget` all pass their `budget_usd` through that one function, so a value refused at one door cannot be stored at another. Schedules were the last one without it (see [Scheduler](#scheduler)).
- **Exhausting the budget is terminal, not retryable.** The refusal is a typed error (`Error::BudgetExhausted`), and the worker sends it straight to the permanent-fail path: the job goes to `failed` on the attempt that hit the ceiling, with its remaining `max_attempts` visibly un-burned (`attempts: 1 / max_attempts: 5`). Retrying could never have helped — a retry re-seeds the job's spend from the cost ledger (the money really was spent) and re-refuses on its first metered call, so the backoff ladder only delayed the same answer. Terminal budget failures still fire the normal terminal fan-out (callback, `failed` event, terminal triggers). **Only** the ceiling refusal is classified this way; engine, storage and ordinary app errors keep their full retry/backoff semantics. There is no top-up-and-resume flow: raise `budget_usd` and enqueue again.
- **Schema drift is terminal too, and no longer looks like an outage.** The grants fleet refuses to write when an upstream listing response no longer parses — a renamed field, a changed envelope — and that refusal is now `Error::SourceDrift` rather than a generic `Error::App`. It is a pure function of one already-fetched response, and the request came from params frozen at enqueue, so attempt #2 asks the identical question, gets an identically shaped answer, and re-refuses in the identical place: a permanent upstream rename used to burn three attempts plus backoff on **every scheduled run, every day, indefinitely**. A source that is *down* is a different failure (`Error::Http`) and keeps its full retry ladder; the two used to be indistinguishable `Error::App`s in the job log. At the HTTP boundary it maps to **502**, not 500 — a source changing its schema is somebody else's failure, and this server refusing to write a corrupt corpus is it working correctly. The boundary is deliberately narrow: only **pre-write listing** refusals are terminal. Warn-only drift signals stay warnings, and a per-item degradation that aborts a *stage* rather than the job (grants-gov's detail stage) stays retryable — a partial harvest is a fact about one attempt, not about the contract.
- **A governance pause is reported as a pause.** A DataHub `cost:pause` tag forces the job's budget to `$0` (see [datahub.md](datahub.md)), so the same refusal fires — but the stored `error` says the tag on `<app>`'s datasets is holding its paid work and that nothing was spent, instead of the misleading "job budget of $0.00 exhausted". A paused job's `GET /jobs/{id}/costs` total is `$0`.
- APIs: `GET /jobs/{id}/costs` (events, totals, certainty, cost-per-fresh-record), `GET /costs?app=&since=&certainty=` (app×engine rollup), `pumper_cost_usd{app,engine}` gauges on `/metrics`.
- Research cache: identical `ResearchRequest`s within `claude.research_cache_ttl_secs` (default 24h, 0 disables) are served from disk at zero cost, logged as `cache_hit (saved ~$X)` events. `resume_session` requests bypass.

### Was it billed, unmetered, or genuinely free?

`cost_usd = 0.0` used to carry four different facts and the only thing separating them was a *prefix of the free-text `detail` column*, which no endpoint selected, grouped or filtered on. A job that burned to its budget ceiling and was killed answered `GET /jobs/{id}/costs` with `{"total_usd": 0.0, "calls": 1}` — byte-identical to a job that served one free cache hit — and `cost_per_fresh_record_usd` then divided that zero by the record count and published a confident `$0.00` ROI that `GET /economics` recommends budgets from.

Every `cost_events` row now carries a typed **`spend_certainty`** (migration 0045), a closed four-member vocabulary owned by `pumper_core::error::SpendCertainty`:

| `spend_certainty` | Means | Written for |
| --- | --- | --- |
| `billed` | `cost_usd` **is** the bill, as the engine reported it. A reported `$0.00` is billed — it is a *known* amount. | a priced answer; any failure whose envelope still reported `total_cost_usd` (`failed_spend (<class>)`) |
| `unmetered` | A paid call happened and `cost_usd` is **not** its bill. The `0.0` is a floor, never a price. | `Timeout` (killed mid-generation), `Unparseable` and `PromptUndelivered` (exit 0 — the model answered), `CliError` with no amount, and a success whose envelope omitted `total_cost_usd` (`cost_unreported`) |
| `unknown` | A child existed and may have spent, and we cannot say whether it was billed **at all**. | `WaitFailed`, `PipeFailure`, `Interrupted`, `NonZeroExit` with no envelope |
| `free` | No money moved. | cache hits, VCR replays, the http/browser tiers |

**`unmetered` and `unknown` are deliberately not the same row.** Calling a failed `wait()` syscall or a broken pipe "unmetered" asserts *a paid call vanished* about a run that may never have reached the model — the exact claim the `WaitFailed`/`PipeFailure` classes were split out of `Timeout` to stop making. `ClaudeFailure::unreported_certainty()` decides it member by member, exhaustively, so a new failure class cannot inherit a neighbour's answer.

What it buys on the surfaces:

- `GET /jobs/{id}/costs` adds `billed_usd`, `unmetered_calls`, `unknown_calls` and `total_certainty` (`exact` | `at_least`). `total_usd` is kept and is the same number as `billed_usd`.
- `cost_per_fresh_record_usd` is **`null`** for any job holding an uncertain call, with `cost_per_fresh_record_unavailable` naming which (`"unmetered_calls"` | `"unknown_calls"`). A price derived from a floor is not a price.
- `GET /costs?certainty=unmetered` keeps only the (app, engine) groups holding at least one such call, with the group's whole picture; every group also reports `unmetered_calls` / `unknown_calls`.
- `GET /economics` reports `unmetered_calls`, `unknown_calls` and `cost_certainty` per app-window, and a recommendation built from a floor says so in its `reason`.
- `GET /jobs/{id}/receipt` reports the same four fields in its `cost` block, from the same rollup — `billed_usd`, `unmetered_calls`, `unknown_calls`, `total_certainty` — and `total_usd` keeps its exact prior value, so no consumer breaks. When the total is a floor, `unknown[]` gains one line naming how many calls are unmetered and how many unknown, saying the run cost *at least* that much. An **exact** total gets no entry: a footnote on every receipt is a footnote on none.
- `GET /metrics` emits `pumper_cost_uncertain_calls{app,engine}` beside `pumper_cost_usd{app,engine}` — `unmetered + unknown` for the group, with an **explicit zero** when the group is clean. Without it a month of unmetered failures reads as a flat, reassuring spend line, and an alert on `pumper_cost_usd` is unfalsifiable.

**One summer, in core.** `pumper_core::costs::roll_up` (per event) and `CostSummary` (per `(app, engine)` group) are the only code allowed to turn `cost_events.cost_usd` into a published number; `routes::jobs::certainty_fields` is the only code that emits the four fields. The rule used to live in a `pub(crate)` helper inside one route module, so the second surface in the same crate — the receipt — wrote its own sum and dropped the certainty. An inventory test over the whole server crate (`cost_sum_inventory`, the EXPECTED-diff idiom) enumerates every remaining `cost_usd` fold and fails the build on a new hand-rolled one.

**The budget ceiling charges the calls it could not price.** It used to be `budget - billed`, which read a Claude run killed mid-generation as free money: that run records `cost_usd = 0.0` with `spend_certainty = 'unmetered'`, so it contributed exactly `$0.00` to its own ceiling. The Claude tier is both the only tier that spends real money and the only tier that can produce such a row, so the cap was blind precisely where its target lives — an app that repeatedly timed out Claude mid-generation could run forever under a `$1` budget.

The ceiling now charges an unpriceable call the **mean of the job's own priced calls** (`pumper_core::costs::impute_uncertain`), and the estimate lives in the evaluation only — no stored row is ever rewritten. A `$1.00` job with four billed calls summing `$0.40` and two `unmetered` calls has `$0.40` of headroom, not `$0.60`: `$0.40` billed plus `$0.20` imputed at its own `$0.10` mean. The window is the job's own ledger, never a fleet average — a cap is per job, and borrowing another job's prices to spend this job's money is a number with no owner. A `free` row is **not** a price sample and stays out of the denominator, or a job doing a hundred free http fetches beside two unreadable Claude bills would charge its blind spots a third of a cent and re-open the hole from inside the fix.

**A job with unreadable bills and no readable one is REFUSED, not admitted.** There is nothing to take a mean of, so there is no honest number — and the old answer was to present the platform's blindness as the job's full headroom. The next metered call gets `Error::BudgetExhausted` (terminal, like any ceiling refusal) whose message says *unpriceable*, names how many calls could not be priced, and tells the operator the fix is to find out why the bill is unreadable — raising `budget_usd` cannot help and would only authorise more unmeasurable spend. An `AutoWithResearch` **fetch** degrades to the free tiers instead, with the same distinction in its escalation trail. An **unbudgeted** job in the identical state still runs: there is no ceiling to be blind about. This is the registry's own degenerate case (`llm-observability/usage-limit-governance/cost-evidence-and-imputation`) — *a cap that cannot be measured is not a cap*.

**What the surfaces say.** `GET /jobs/{id}/costs` keeps `budget_remaining_usd` (now charged) and `budget_remaining_certainty` (still `at_most` whenever a floor went into it) and adds:

| key | means |
| --- | --- |
| `budget_imputed_usd` | how much of the charge is an **estimate**. An estimate must announce itself; a headroom that silently folds in imputed dollars is a number nobody can audit |
| `unpriceable` | the job has calls we could not price and none we could, so its spend has no actionable bound. `budget_remaining_usd` and `budget_imputed_usd` are both `null` here — not `0.0`, which is a measurement |
| `breached` | charged spend actually *reached* the ceiling. A different fact from `unpriceable`: a blind job has not been shown to have overspent anything |
| `budget_imputation` | the policy in force, echoed so a surprising number can be traced to a config key |

The report and the enforcement point call one function (`pumper_core::costs::budget_verdict`, three cases: `Unlimited` / `Headroom` / `Unpriceable`), so `GET /jobs/{id}/costs` cannot tell an operator there is money left while the worker is refusing the next call. A unit test walks both policies over a table of spends and asserts the two agree.

**The knob, which is the rollback.** `[costs] budget_imputation = "window_mean"` (default) | `"off"`. `off` is the pre-change ceiling exactly: `$0.60` remaining on the job above, and the unpriceable job admitted against its `$0` total. It restores the old behaviour **without a deploy**, and `crates/core/tests/budget_imputation.rs` proves both policies against one table so "byte-identical to before" stays a test rather than a claim. An unknown value is a startup error naming the vocabulary — a typo must not quietly un-enforce a cap.

`CostLedger::job_total` is untouched and still the plain `SUM(cost_usd)` for the callers that want exactly that. The ceiling reads `job_spend()` instead — the same dollars plus the two counts it was missing, in one query — and `SpentTotal`, the in-memory mirror on the pre-flight path, carries those counts too, so a **retried** job counts its earlier attempts' unreadable bills as well as their dollars. The pre-flight path still issues no query at all.

**Back-fill.** 0045 derives `spend_certainty` for existing rows from their `detail` prefixes (`failed_spend (…)` → `billed`, `unmetered_timeout`/`cost_unreported` → `unmetered`, `failed_spend_unreported (…)` → `unmetered` or `unknown` by class, everything else by engine and amount). It is deterministic and replaying it is a no-op.

**Known gaps.** The retention pruner ignores the column: it prunes `cost_events` by age alone, so pruning an `unmetered` row destroys the only evidence that an unpriced paid call happened — and it now also destroys the evidence the ceiling imputes from, which turns a historical `at_least` aggregate back into a confident `exact` one. The imputation mean is taken over the job's **whole** ledger rather than a recency window, so a long job whose prices drift charges its late blind spots at its early rate; the fix is a windowed mean, and the mean is deliberately the simplest estimator that cannot read blindness as zero.

## Record & replay (VCR)

Two job params make a run reproducible. `record: true` persists every fetch and research call that goes through `AppContext` into `data/artifacts/<app>/<job_id>/cassette.ndjson`. `replay_of: "<job_id>"` re-runs the app serving those recorded responses instead: no network, no politeness delay, no tier learning, `$0` spend (each seam records a `vcr_replay` cost event at `0.0`), and a request that was never recorded is a typed **replay miss** — replay never silently falls through to a live fetch. The two are mutually exclusive (a replay's cassette *is* the record); a contradictory pair, a `replay_of` whose cassette is missing, or a `replay_of` against an app that cannot be replayed at all (see below) fails the job permanently before it runs.

**A replay miss is terminal, not retryable.** A miss is a pure function of an immutable cassette and params frozen at enqueue, so the job goes straight to `failed` on the attempt that missed, with its remaining `max_attempts` visibly un-burned (`attempts: 1 / max_attempts: 5`) — exactly like an exhausted budget. Retrying could only re-read the same file, re-run every live-free step that preceded the miss, and miss in the identical place. Previously only the *load*-time miss (no cassette at that path) was permanent while the *resolve*-time miss (cassette present, request absent) rode the whole backoff ladder.

**Which attempt's recording survives.** A job can run more than once — retry, reaper re-queue, graceful-shutdown suspend — and all its attempts share one cassette path. The rule: *the cassette records the job's work, and work survives exactly when a durable checkpoint does.*

- An attempt that starts **fresh** (no checkpoint restored) discards the earlier cassette before it runs. Otherwise a failed attempt's partial recording shadowed the successful attempt's complete one (entries load first-wins), and the replay reproduced the run that FAILED while reporting itself deterministic.
- An attempt that **resumes** from a checkpoint appends, because it will not re-fetch what the earlier attempt recorded. The shutdown-suspend re-queue does not even burn an attempt: a suspended recording **resumes, it does not restart**.

The whole-cassette size cap (128 MiB; 4 MiB per entry) is seeded from the file actually on disk, so it binds on real bytes across attempts rather than resetting each time. An over-cap entry is written as an identity-only marker carrying `recorded_truncated` — replaying it is an honest miss, never a silent gap. Recording is best-effort telemetry: a write failure warn-logs and never fails the job. Cassettes are exempt from artifact retention unless `[storage] artifact_retention_include_cassettes` is set.

**The cassette is verified at load, not trusted.** It is plain NDJSON on disk, deliberately retention-exempt (i.e. designed to outlive releases) and editable by anything on the box, so the loader checks the one property replay sells — *the bytes you get back are the bytes that ran recorded*:

- **Format version.** Every entry carries `v` (currently `1`). A version this build does not understand fails the load with a message naming it, instead of degrading to per-line skips and an all-miss replay. Entries written before `v` existed read as `v: 1`, so **every cassette already on disk keeps loading unchanged**.
- **Identity is recomputed, not believed.** A `GET` entry's `req_hash` is re-derived from its own method+url; an entry filed under a hash it does not hash to would be served for a request it is not a recording of, and the replayed outcome would report the URL the entry names. Either defect **fails the whole load** — a cassette that lies about one identity has no claim to be trusted about the other 4,999. (A `RESEARCH` entry's key is the canonical request key, which is deliberately not stored, so its identity is unverifiable; what is still checked there is that `req_hash` is a well-formed sha256 digest. Known gap.)
- **Torn lines are counted, not silently dropped.** A crash mid-write leaves an unparseable final line. The entries that landed are real work, so the load still succeeds — but the count reaches the operator on the `vcr replay` log line (`entries=N unreadable=M`, plus a `warn!` when `M > 0`) and on the stored result as `vcr_cassette_unreadable_lines`. Previously a 5,000-entry cassette with one readable line was a *successful* load followed by a storm of misses indistinguishable from "the run never fetched that".

**Not every app can be replayed, and the ones that can't are refused.** The cassette is written and read at ONE seam, `AppContext::fetch` / `AppContext::research`. A whole class of apps reaches an engine outside it — `engines.http` / `engines.browser` — for reviewed, deliberate reasons: a JSON API that needs a POST body or a byte response (`grants-gov`, `eu-sedia`, the `census-*` family, `cordis`, `ca-grants`, `cms-fee-schedule`, `mpsv-*`, `peer`, `smlouvy-dump-watch`, `hackernews`), the crawler owning its own frontier (`crawl`), and a transact flow that is a browser *session* rather than a fetch (`transact`). That traffic is invisible to the cassette in **both** directions: none of it is recorded, and on replay nothing would stop it running live. Replay capability is therefore declared per app in `REPLAY_BYPASS_APPS` (`crates/core/src/vcr.rs`), the one place it is decided, and graded:

- **`unreplayable`** — the app's work *is* raw engine driving, so a replay would reproduce none of it. `replay_of` **fails the job permanently before it runs**, naming the app and the reason (`app "transact" cannot be replayed: …`). Previously the only thing standing in the way was an accident: such an app records *nothing*, so its cassette file never exists and the load failed with `has no cassette … — was it enqueued with record: true?`, blaming the operator for something the app makes impossible. That accident evaporates the moment a cassette does exist at that path (the app grows one `ctx.fetch` call, a file is copied in), and then the job ran completely live with `vcr_replay_of` stamped on its result.
- **`partial`** — the app mixes both (today: `extractor`, whose `urls`/dataset modes go through the chokepoint while its `archive` mode reads Wayback raw). The replay runs, and the result says how far the claim goes.
- **`full`** — everything else: the whole run comes off the cassette.
- `record: true` on a non-`full` app still records what goes through the chokepoint, and warn-logs that the cassette cannot cover the whole run.

**A self-hosted research run has a second bypass, and it is not in that list.** With `[claude] self_hosted_tools` on (see [mcp.md](mcp.md#the-self-hosted-agent-loop-claude-self_hosted_tools)) the Claude subprocess fetches through pumper's own `fetch` MCP tool. Those fetches ARE governed, cached and metered — that is the whole point of the item — but they run in a context this tool builds for the calling job, and that context has VCR `Off`: recording into a cassette the worker's task already owns would interleave two writers, and replay would have to resolve against a cassette the MCP call never opened. So the model's own fetches are **not recorded**, and a replay of such a run would let them go live. `REPLAY_BYPASS_APPS` grades an app by the code it runs, and cannot see this — it is a config-driven bypass, not an app-driven one. Do not treat `[vcr] replay` as an egress guarantee while `self_hosted_tools` is on.

**Replay provenance on the stored result.** A replayed job's result carries `vcr_replay_of: "<job_id>"`, plus `vcr_replay_fidelity: "full" | "partial"` — written on **every** replay, including clean ones, so that a missing marker means "older pumper", not "nothing was wrong". A `partial` replay also carries `vcr_replay_bypass` with the reason. `vcr_cassette_unreadable_lines` appears when the cassette had torn lines.

**Other limits.** A browser render replays as its final response equivalent — the recorded post-render HTML — not by re-running JS or page actions. The per-call inventory of raw-engine sites (which is what the grades above are derived from) lives in `crates/core/tests/fetch_chokepoint.rs`.

## Job receipt

`GET /jobs/{id}/receipt` — one read-only document answering "what did this job cost me and what did it actually change?", joining the surfaces that previously had to be joined by hand. Blocks: `job` (status, attempts, lineage, `wall_ms`), `stages` (the per-stage timings above), `cost` (total, calls, per-engine, the job's `budget_usd`), `yield` (what the result reported, per dataset), `changes` (what the revision log records, per app/dataset/change kind), `verdicts` (extraction-health verdicts *this run* produced, plus its in-memory contract verdicts), `artifacts` (file names + byte sizes read from `data/artifacts/<app>/<job_id>/`), `deliveries` (webhook deliveries keyed to this job), `trigger_hops` (the jobs this run's outcome enqueued) and `coverage` (what it wrote vs what it declared — see below).

**Honest nulls, and an `unknown[]` list that names every gap in the caller's words** — nothing is inferred, averaged or back-filled. The structural gaps it reports:
- `stages: null` for a job that predates migration 0034, failed before its fan-out, or hasn't finished — and the message says which.
- `wall_ms: null` while a job is unfinished (never "now − started_at", which would grow on every refresh).
- `changes` counts revisions by the `job_id` provenance stamp (migration 0030), so a write path that doesn't stamp one is invisible rather than approximated from a time window.
- Watch (`dataset.changed`) and saved-search (`search.matched`) deliveries are logged against the watch/search id, so **no** delivery can be attributed to the run that caused it; the receipt says so instead of listing a plausible subset.
- `trigger_hops` reads `jobs.source_job_id` (migration 0035); hops enqueued before it are not recoverable.

Every join is an index seek on the job id — a receipt is a per-job audit view, not a metrics query, and it must not slow down as the corpus grows.

### `coverage` — what the run wrote vs what it declared

A run declares the namespaces it puts records under in its result's `index_datasets`; the worker turns that into `run_indexed_apps(job.app, index_specs)` and scopes the hook batch (`load_run_changes`) and the saved-search pass by it. A namespace the run writes but never declares is therefore reachable by **no** watch, dataset trigger or saved search scoped to it — silently, with the job green. That class has bitten twice (`ca-grants` writing `grants/unified`, `peer` writing `peer_<origin>/<dataset>`), and both times a human found it after shipping.

The receipt now diffs the two populations, which both already existed and were never compared:

```json
"coverage": {
  "declared": ["mpsv-vpm", "cz-labour"],
  "declared_datasets": [{ "app": "cz-labour", "dataset": "salary_gap" }],
  "undeclared": [{ "app": "cz-labour", "dataset": "skill_demand" }],
  "complete": true
}
```

- `declared` — `run_indexed_apps(job.app, index_specs)`: the job's own app first, then the virtual apps its result names.
- `declared_datasets` — the exact `index_datasets` specs. A declared **app** does not launder an undeclared **dataset** inside it: per-record search documents are minted per spec, so an unnamed dataset is unsearchable even in a namespace that is otherwise reachable.
- `undeclared` — every `(app, dataset)` the run's revisions carry that is **outside the job's own app** and not named by a spec. Deduplicated and sorted. The worker logs one `WARN` per entry at the end of a successful run, naming the job, the app and the namespace.
- `complete` — whether the diff may be trusted at all.

A write under the job's **own** app is never reported: an app owns its namespace and writes plenty of internal datasets there (`mpsv-vpm` alone deliberately excludes eight from indexing). Flagging those would make the signal fire on every run, and a signal that is always on is not a signal.

**Known gaps.**
- The observed set comes from the `job_id` provenance stamp (migration 0030), the same source as `changes`. A write path that does not stamp a job id is invisible here.
- When the receipt cannot see the run's writes it says so rather than reporting a clean bill: `complete: false` plus a named reason in `unknown[]`. Two causes, never collapsed — the job is not `succeeded` (its writes are not final), or no revision carries its stamp at all (“wrote nothing” and “wrote unstamped” are one picture from here). **An empty `undeclared` with `complete: false` is not a pass.**
- The check is a report, not a gate: it runs after the fan-out, a store error is warned and dropped, and nothing here can change a job outcome.


## AppContext (what a running app gets)

`job_id`, `app`, `params`, `engines`, `datasets`, `costs`, `budget_usd`, `research_cache`, `tiers`, `plugins`, `progress` (throttled live-progress seam — see [Live progress](#live-progress)), `checkpoints`/`restored` (durable-execution seam — see [Durable execution](#durable-execution-checkpoints)), `resumed_input` (the answer a `POST /jobs/{id}/resume` supplied — see [Jobs that wait](#jobs-that-wait-waiting)), `health` (extraction-health judge — see below), `artifacts_dir` + helpers: `fetch` (metered, budget-governed, tier-routed), `research` (metered, cached), `upsert`/`upsert_many`/`sync_many`, `observe_extraction`, `save_artifact`, `checkpoint`/`checkpoint_now`/`restore`, `await_input`/`restore_input`, `require_str`, `remaining_budget_usd`.

**Health-gated writes.** `upsert`/`upsert_many`/`sync_many` consult the source's extraction-health state before writing: a quarantined source's writes are redirected to the shadow dataset `<dataset>@q` (an ordinary dataset, so every existing tool works on it) and stamped with a trust marker, and `sync_many` silently **downgrades to `upsert_many`** when the state suppresses removals — a half-broken run returns a short-but-nonempty batch, and removal detection would then tombstone every key missing from it. The check lives **in the store**: `Datasets::detect_removed` demands a `RemovalGuard`, and the only public way to mint one is `RemovalGuard::for_source_state(state)`, which yields `None` for a degrading source. So an app that hand-rolls `upsert_many` + removal detection cannot skip it either (it previously could, and one did). A caller that already knows which specific records disappeared uses `Datasets::tombstone_keys` instead — removal by name, nothing inferred. `observe_extraction(dataset, docs, fetch)` records the run's verdict and must be called **before** the upserts it is meant to gate. All of this is inert for a source nothing has armed, which is every source under the shipping default (`[resilience] enforce = false` with an empty `enforce_sources` — verdicts are computed and stored, nothing is gated, and nothing is even read on the write path).

**Arming is per source.** `[resilience] enforce = true` arms the whole fleet at once; `enforce_sources = ["<app>/<dataset>", ...]` arms exactly the ids it names while `enforce = false`, mirroring `auto_retire` — which has been per-source opt-in since it shipped, over the *more* destructive consequence. An empty list arms nothing (an unset allowlist means none, never all), and `enforce = true` keeps its old meaning exactly, so no existing deployment's behaviour moves: a list can widen arming from nothing, never narrow it from all, and a list set beside `enforce = true` is reported as redundant rather than honoured as a restriction. `enabled = false` remains the absolute off and fronts both keys. The single predicate is `resilience::detect::enforcement_armed`; the gate lives inside `enforced_state`, so every consumer became per-source without a call site moving. `GET /sources` carries `armed` per row plus `enforcing_any`/`enforce_sources`, and `GET /enforcement/preview` splits its rollup into `consequences.armed` (happening) and `consequences.soaking` (hypothetical). Full surface: [resilient-extraction.md](resilient-extraction.md).

## Config

`config.toml` (or `$PUMPER_CONFIG`; both resolved **relative to the process CWD**), `#[serde(default)]` throughout — sections: `server, worker, storage, http, browser, claude, fetcher, governor, cache, plugins, search, triggers, webhooks, resilience, datahub, provisioner, waiting`. New fields need both the serde default and the manual `Default` impl. `[fetcher]` holds the tiered-fetch and host-memory knobs (see [fetching.md](fetching.md)); `[resilience]` the extraction-health detector (see [resilient-extraction.md](resilient-extraction.md)); `[datahub]` the metadata emitter (see [datahub.md](datahub.md)). `[webhooks]` holds `failure_url`/`failure_secret` — the optional global `job.failed` firehose (see [events-webhooks.md](events-webhooks.md)). `[census]` holds the sub-state launch atlas's scope rail and its metered pricing driver — `atlas_states_k` (10), `atlas_top_n` (25), `atlas_metros` (5), `metro_pricing` (**false**), `metro_pricing_cron`, `metro_pricing_budget_usd`; the driver ships OFF, so with no `[census]` section the atlas is a pure read-model that plans metro pricing and buys none of it (see [apps.md](apps.md) "census/atlas"). `[provisioner] proposal_max_age_secs` (default 30 days, `0` disables) is the window `GET /provisioner/proposals` uses to lazily flag a still-`planned` proposal `expired` — see [apps.md](apps.md) "provisioner: proposal lifecycle" and [http-api.md](http-api.md#provisioner-proposal-lifecycle-provisionerproposals).

### `[recipes]` — learned API-recipe replay, and its two strike rungs

| key | default | what it does |
| --- | --- | --- |
| `enabled` | `false` | consider recipes on every fetch. A single fetch opts in without it via `FetchRequest.use_recipes`. |
| `auto_validate` | `false` | also try an *unvalidated* recipe opportunistically, and promote it when a successful replay still overlaps its expected field paths. |
| `max_failures` | `3` | consecutive failed/thin replays after which a **validated** recipe is un-validated (back to candidate, counter reset). **Must be >= 1** — `validate()` rejects `0`, because a validated recipe needs at least one consecutive failure before it is demoted. |
| `retire_after` | `10` | the rung **below** `max_failures`: consecutive strikes after which a **candidate** is *retired* — stamped `retired_at` and dropped out of selection entirely (migration 0046). `0` disables the rung and restores pre-0046 selection exactly. |

`retire_after` is the one key in this section that ships **on**, and the
asymmetry is deliberate. Before it existed the strike counter had exactly one
consumer and it was guarded on `validated`, so once a recipe was unvalidated
that branch was unreachable for that row forever: the counter was write-only and
selection never read it, which made a candidate that had failed ten thousand
consecutive times still the first thing tried for its host — a live outbound
replay ahead of the archive tier on every fetch. The rung only ever takes things
*out* of selection, and with `[recipes]` off nothing is replayed and nothing
strikes, so a default install pays nothing for it while an opted-in one is
protected from an immortal candidate. Unlike `max_failures`, `0` is legal here:
it is the documented escape hatch, not a misconfiguration.

The threshold rides on the `RecipeStore` handle (wired in `state.rs` and
`worker.rs`) rather than being threaded through `RecipeSource::record_failure`,
because the `Fetcher` holds the source behind `dyn` and compiles without the
`storage` feature — so the trait keeps its signature and the concrete store
carries the policy. Full lifecycle: [fetching.md § The recipe lifecycle](fetching.md#the-recipe-lifecycle-candidate--validated--retired).

## Process startup & error reporting

`main` (`crates/server/src/main.rs`) is **synchronous**: it loads `.env`, initializes error reporting, installs the tracing subscriber, and only then builds the tokio runtime and enters `run()`. The Sentry transport owns a background thread that must not be born inside a runtime about to be torn down, which is what forces that order.

**Error reporting is optional and off by default.** `SENTRY_DSN` unset, empty, or whitespace-only ⇒ reporting is disabled silently and the process boots identically to before; a malformed DSN ⇒ one `warn!` at boot, then reporting off — a bad DSN never stops the service. `SENTRY_ENVIRONMENT` tags events (default `local`), and `PUMPER_BUILD_ID` doubles as the Sentry release so a release and an extraction-health run row name the same build. stdout logging is unchanged either way — the Sentry layer is composed *beside* the `fmt` layer, so `error!` becomes an event and `warn!`/`info!` become breadcrumbs. `traces_sample_rate = 0.0`, `send_default_pii = false`. Full surface: [observability.md](observability.md).

## Metrics

`GET /metrics` (Prometheus text, cached ~5s): `pumper_jobs{status}` gauges (including `status="waiting"` — jobs parked on external input, holding no permit), `pumper_job_failures_total{app}` (permanently-failed jobs per app — **DB-derived** from the current `failed` row count, so it resets/decreases if failed jobs are retried or purged rather than being a strictly monotonic process counter), `pumper_job_duration_seconds` + `pumper_job_queue_wait_seconds` summaries (`_sum`/`_count`/`_max`), `pumper_cost_usd{app,engine}`, `pumper_apps`, `pumper_schedules{enabled}`.

Webhook delivery health, all four read in one aggregate pass so they describe the same instant, and all DB-derived like `pumper_job_failures_total` (the retention sweep can lower the `_total` series): `pumper_webhook_deliveries{status="pending|delivered|failed|dead"}`, `pumper_webhook_oldest_undelivered_seconds` (oldest `pending`+`failed` row; `0` when none, `dead` excluded so the gauge can clear — **the series to alert on**), `pumper_webhook_delivery_attempts_total`, `pumper_webhook_deliveries_succeeded_total`. See [events-webhooks.md § Metrics](events-webhooks.md#metrics).

`pumper_worker_claim_failures_total` — job-claim attempts that failed (an unreachable or erroring store). **Process-lifetime, reset on restart**, always emitted, `0` on a healthy process. It exists because the claim loop's failure LOG is deliberately rate-limited: the loop polls every `poll_interval_secs` and `error!` is the level wired to remote telemetry, so an unreachable store used to ship one event every two seconds — one fact, 1,800 times an hour. The loop now reports the first failure of an outage, one "still failing, N consecutive" every 5 minutes thereafter, and the recovery with the outage's true length; the ones in between are `debug!`. This counter is what keeps that limiter from being a blindfold, and it is deliberately *local*: a telemetry channel that is itself down cannot be where its own outage is counted.

`pumper_checkpoint_failures_total{reason="stale_lineage|storage_error"}` — checkpoint saves that did not land. **Process-lifetime, reset on restart** (like `pumper_remote_egress_fetches`, and unlike the DB-derived series above), because the alternative — scanning `jobs.result` for the `checkpoint_failures` stamp — would undercount exactly the runs that matter (see below) and shrink under retention pruning. Both reasons are always emitted, at `0` when nothing failed. `storage_error` is the durability alarm: that run has no state of its own. `stale_lineage` is routine during a reap (another attempt owns the job) and only alarming in a steady state.

The store's own telemetry — per-`(op, table, phase)` durations, slow counts with
their published slow line, busy waits, rows touched, p95s with their window,
queue ages, size on disk (main / freelist / `-wal` sidecar), maintenance passes
by outcome, and the activity gauge — is listed in
[observability.md § The store's report on itself](observability.md#the-stores-report-on-itself).
It is a **partial census by design** and says so in its own `# HELP` text: seven
instrumented statement families, with schedules, watches, triggers, deliveries,
saved searches, the caches and the search index explicitly unmeasured.

## Boot: the schema post-condition

The migration chain (`crates/core/migrations/`, `0001_init.sql` …) is replayed on
every `Storage::connect`. It used to be replayed and then *believed*: the
migrator ran, `connect` returned `Ok`, and nothing read back the schema it had
produced. Boot now asserts it, in two halves, and **refuses** a store this
binary cannot read (`Error::Schema`).

**Half one, before the chain runs — is the store ahead of this binary?**
`pumper_core::schema::classify` compares `_sqlx_migrations` against the embedded
chain **in both directions**. `pending` (versions this binary carries that the
store lacks) is ordinary forward migration. `ahead` (versions the store has that
this binary does not) is a **downgrade**, and this repo ships **zero down
migrations** — so running an older binary is the only way back, and it is the
path that previously classified identically to "in sync". The five verdicts are
`fresh`, `current`, `pending`, `ahead`, `diverged` (both directions at once).

*What the operator sees:* boot fails with a message naming the ahead versions,
the version this build's chain ends at, and three remedies — run the newer
binary, restore the pre-migration snapshot written beside the database
(`<database>.bak-premigrate-*`, see "Budgets" → the backup ritual is automatic),
or set **`[storage] allow_schema_ahead = true`**. That key defaults to `false`:
it is a safety ceiling, not a preference. With it on, the same store boots, the
ahead versions are logged at `warn` on **every** start, and `GET
/datasets/doctor` keeps reporting the drift as a finding.

**Half two, after the chain runs — did it produce the inventory it promises?**
`pumper_core::schema::EXPECTED_TABLES` is the single manifest of which tables the
chain must leave behind (33 today). It used to live in `crates/core/tests/
migrations.rs`, where the boot path could not reach it; it now lives in the crate
and is read by **both** the CI replay test and every real boot. A missing table
refuses the boot naming the table — this is drift *below* the ledger (an
interrupted migration, a hand-restored partial copy, a table dropped out of
band), which no version comparison can see. An **unexpected** table is logged at
`info` and never fatal: an operator's own scratch table is not a fault.

*What the operator does:* restore the `*.bak-premigrate-*` snapshot beside the
database and re-run so the chain replays, or move the database aside and let it
rebuild from 0001. Do not create the table by hand — the chain is the only
authority for its shape.

**A migration that creates a table without adding it to `EXPECTED_TABLES` now
fails the test AND refuses a real boot.** Add the name in the same change as the
migration.

The pre-migration snapshot decision reads the same `classify`: a store that is
`ahead` takes no snapshot (there is nothing this binary is about to change) but
is logged as `SkipSchemaAhead`, not `SkipUpToDate` — the two used to be the same
arm, which is how a downgrade rode in as a no-op.

## Elastic executor plane (`[executors]`)

**Default OFF.** With no `[executors]` section a node behaves exactly as it always has: one process owns the SQLite file and runs every job. Turning the plane on lets *additional* pumper processes — started as `pumper --executor --coordinator <url>` — dial in, claim whole jobs, run them with their own engines, and report the results back. Concurrency and geography then scale by starting a process, not by the coordinator's core count.

**Only `execute` moves.** The queue, the attempt ladder, the heartbeat lease, the health/contract gates and the whole post-run fan-out stay on the coordinator. An executor's job ends with one call — *here is the result for `(job, attempt)`* — and the coordinator then indexes, gates, fires watches/triggers/saved searches, emits the terminal event and delivers the callback, through the identical `finalize_fanout` a local run uses.

### What an executor may run: result-only apps

An app is executor-eligible when it declares `ScrapeApp::executor() -> true` **and** is result-only: everything it produces comes back in the job result and the artifact tree. Today that is exactly one shipped app, `readable`.

The reason is honest rather than temporary: an executor's `AppContext` carries a **refusing** `datasets` handle (an empty in-memory database with no schema), because there is no RPC dataset client in this slice. The alternative that "works" — pointing `datasets` at the executor's own scratch store — is the one that must never ship, since the app would report success while its records existed only on a machine nobody collects from. A server-side inventory test (`crate::executors::executor_eligible_apps_are_result_only`) reads each eligible app's own source and fails if it reaches `ctx.upsert*` / `ctx.sync_many` / `ctx.datasets` / `ctx.register_rules` / `ctx.observe_extraction`, so the declaration is a checked claim, not a comment.

Everything else in the executor's context is real and local: engines (HTTP/browser/Claude/plugins), the cost ledger, the HTTP cache, learned host weather, the recipe store and the research cache all live in the executor's own `[storage] database_path`. They are process-local **derived** state — losing them costs a cache miss, never a record.

### The doors (coordinator side)

All six answer `404` unless `[executors] enabled` **and** a non-blank `secret`; there is no "configured but open" state. The secret arrives in `x-pumper-executor-secret` and is compared as SHA-256 digests, exactly like the remote fetch fabric's — a wrong secret is `401`. Under `[auth] mode = "keys"` they additionally require the `admin` scope, for free: `auth::required_scope` classifies any unknown mutating path as `Admin` and `GET /executors` as `Read`. Two credentials in two headers, so "wrong secret" and "wrong key" stay two distinguishable `401`s.

| route | body | answer |
| --- | --- | --- |
| `POST /executors/claim` | `{executor_id, capabilities?}` | `200 {job_id, app, params, attempt, budget_usd, restored, resumed_input}` or `204` |
| `POST /jobs/{id}/heartbeat` | `{executor_id, attempt}` | `{owned: true}` / `409` |
| `POST /jobs/{id}/checkpoint` | `{executor_id, attempt, state}` | `{saved}` / `409` |
| `POST /jobs/{id}/progress` | `{executor_id, attempt, state}` | `{reported}` / `409` |
| `POST /jobs/{id}/finish` | `{executor_id, attempt, result \| error, run_ms?}` | `{outcome: "succeeded"\|"queued"\|"failed"\|"stale"}` / `409` |
| `GET /executors` | — | `{executors: [...], eligible_apps: [...]}` |

- **The claim is a long poll**, held up to `[executors] claim_wait_secs` (default 30) against an empty queue and woken instantly by an enqueue (the same `Notify` the local worker waits on). The poll is the clock *and* the backpressure boundary: an executor asks only when it has a free slot, so the coordinator never models remote capacity. Note the shared wake-up is `notify_one`, so one enqueue wakes one waiter — a local worker and a waiting executor race for it, and the loser falls back to its own timer. That costs latency on one job, never correctness: the claim itself is a single atomic `UPDATE … RETURNING`.
- **`capabilities` can only narrow.** The coordinator intersects it with its own eligible set, so naming an app there never widens what an executor may claim. An empty list means "whatever you'll give me".
- **Per-app caps go cluster-wide** for this path: the blocked set is computed from `SELECT app, COUNT(*) FROM jobs WHERE status='running' AND executor_id IS NOT NULL`, not from the worker's in-process `HashMap` — that map counts one process's jobs, so with N executors a cap of 2 admitted 2×N. The **local** claim path is unchanged and still uses the in-memory map.
- **`GET /executors`** reports one of three states per executor — `busy` (polled recently, running something), `idle` (polled recently, running nothing: healthy on an empty queue), `offline` (no poll within `[executors] offline_after_secs`, default 120). It is a *report*: an executor going offline does nothing to any job by itself.

### The fence, and why executor loss is cheap

Every executor-driven write is refused unless the row still says `status='running' AND attempts = ? AND executor_id = ?`. The attempt half is the fence the local worker has always run under; the executor half makes a *reaped* process's late write refusable rather than merely unlucky.

So the failure story needs no new machinery:

1. an executor dies mid-run — its job stops being heartbeated;
2. the coordinator's existing reaper re-queues the lease past `[worker] stale_after_secs`, with failure semantics (attempts, backoff, permanent failure when exhausted);
3. the next claim advances `attempts` and hands the new attempt the checkpoint the dead executor pushed, through the same poisoned-checkpoint escape a local resume gets;
4. if the corpse comes back and reports anyway, the fence answers `409` and the report is discarded.

The heartbeat's answer is also the executor's **stop signal**: a `409` means "you no longer own this job", and the executor abandons the run rather than spending on a result that will be refused. A transport *failure* is deliberately not treated as loss of ownership — the lease is still live for `stale_after_secs`, and throwing away an almost-finished run over one dropped packet is worse than waiting.

### Executor mode

```bash
pumper --executor --coordinator http://coordinator:8088
```

`--coordinator` overrides `[executors] coordinator_url`; with neither set the process refuses to start rather than polling nothing forever while looking healthy. `[executors] secret` is required in this mode — it is the credential the executor presents. `[executors] executor_id` names the process (unset = `<host>-<pid>`, which is honest but changes across restarts; set it for a fleet whose history you want to read). `[executors] capabilities` narrows what it offers to run; `poll_interval_secs` (default 2) is the pause after a `204` or a transport failure.

An executor binds no port, serves no API, runs no scheduler, no janitors and no maintenance pass; `[search]`, `[mcp]` and the durable event log are forced off in this mode so a second index or MCP endpoint cannot appear beside the coordinator's. Progress is throttled executor-side (≥2s) and checkpoints (≥5s, `force` bypasses) exactly as the in-process seams are, so a chatty app cannot turn a tight loop into a DoS against the coordinator's event bus. A finish is retried three times with backoff, because the run is already paid for and losing its result to one dropped connection is the most expensive failure this process has; after that the lease simply goes stale and the reaper re-queues the job.

### Where a job ran

`jobs.executor_id` (migration 0056) is `NULL` for every locally-claimed job — the honest value, and every job on a single-process install. It rides `GET /jobs/{id}` and `receipt.job.executor_id`, and `/metrics` carries `pumper_executors{state="busy|idle|offline"}` whenever the plane is enabled (absent when it is not: a series that is always zero on the installs that never run an executor is noise).

### Config

| key | default | side | meaning |
| --- | --- | --- | --- |
| `enabled` | `false` | coordinator | serve the executor doors at all (paired with `secret` by `Config::validate`) |
| `secret` | `""` | both | the shared plane secret, compared as digests |
| `claim_wait_secs` | `30` | coordinator | long-poll ceiling; must be > 0 when enabled |
| `offline_after_secs` | `120` | coordinator | when `GET /executors` calls an executor offline (report only) |
| `coordinator_url` | `""` | executor | who to dial (`--coordinator` wins) |
| `executor_id` | `""` | executor | stable identity (unset ⇒ `<host>-<pid>`) |
| `capabilities` | `[]` | executor | apps offered; always intersected with the coordinator's eligible set |
| `poll_interval_secs` | `2` | executor | pause after an empty claim or a failure |

### Known gaps (deliberately out of the v1 slice)

- **No RPC `Datasets` client**, so only result-only apps run remotely. This is the one change that unlocks every app.
- **VCR is coordinator-side state** (cassettes live beside the recorded job's artifacts), so a remote run neither records nor replays; `record: true` on an executor-claimed job is silently a no-op run.
- **`DELETE /jobs/{id}` cannot stop a running remote job.** The cancel-token registry is in-process, so a running executor job has no token and the door answers 409. The executor *does* stop on a lost fence (reset the job and it abandons), but a direct cancel arm on the heartbeat is not built.
- **No TLS/mTLS of its own** — put the coordinator behind a reverse proxy if the plane crosses a network you do not control. The secret is a bearer credential in a header, exactly like the remote fabric's.
- **No autoscaling and no executor-side VCR.** Capacity is "how many processes you started".
- **Spend is metered on the executor's own ledger**, not folded back into the coordinator's `cost_events` — so `GET /jobs/{id}/costs` for a remotely-executed job is empty and its receipt says so through the existing "no ledger rows" note.

## Known gaps

- No auth on the HTTP API (deliberate local power mode; API-key auth is a parked product decision).
