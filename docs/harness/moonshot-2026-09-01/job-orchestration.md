# Job Orchestration — moonshot scout report (2026-09-01)

Scout: read-only subagent over the group's contexts; cards in the scan-sweep §4.10 form. Deck ids (N-numbers) are in [INDEX.md](INDEX.md).

## JO1 — Jobs that wait: a `waiting` lifecycle state for human/agent-in-the-loop work

- deck item **N02**
- lens: `innovation-catalyst` · size: **XL** · gate: **contract** · effort 7 / impact 9 / risk 6
- contexts: job-worker, cron-scheduler
- extends: M23 durable execution (checkpoint suspend/resume) + M06 transact v1 slice + M29 MCP wait_job

### Summary
Today a job can only run, finish, fail, be cancelled or be suspended *for shutdown*. Nothing lets a running app say "I need something from outside before I can continue" — a human approval, a 2FA code, an agent's answer to a clarifying question — and park itself without burning a worker permit. The suspend machinery M23 shipped (checkpoint + re-queue with attempts headroom) is exactly the park primitive; it is just only reachable from the drain. Promote it to an app-callable seam: `ctx.await_input(request)` checkpoints, releases the slot, moves the job to a new `waiting` status carrying the request, and `POST /jobs/{id}/resume {input}` (plus an MCP `resume_job` tool) re-queues it with the input inside `ctx.restore()`.

### Description
- The lifecycle has no such state: `JobStatus` is `Queued|Running|Succeeded|Failed|Cancelled` (`crates/core/src/job.rs:8-14`). A grep for `awaiting_input`/`Waiting` over `crates/server/src` and `crates/core/src` finds nothing.
- The park primitive already exists and is proven: the worker's shutdown-suspend arm fires the cancel token, the app's last forced checkpoint survives, and `Storage::reset` re-queues with `max_attempts = MAX(max_attempts, attempts+1)` so the suspend burns no attempt (`crates/server/src/worker.rs:878-903`, `crates/core/src/storage.rs:580-594`). `CancelKind` already distinguishes *stop* from *park* (`worker.rs:382-400`); a third kind, `AwaitInput`, is the whole new enum arm.
- `checkpoint_now(state)` (`force = true`) bypasses the 5s throttle precisely for "snapshots whose loss costs real work" (`crates/server/src/progress.rs:94, 303-319`) — that is the quiesce step of the registry's park sequence (fleet-orchestration/hibernation-and-resume: quiesce → capture resume contract → release the slot).
- An app is already asking for it. Transact v1 stops before the irreversible submit and names the missing piece verbatim: "live submission requires the human-approval design (pending-approval transactions + `POST /transactions/{id}/approve` …) — the documented next slice" (`crates/apps/transact/src/lib.rs:1-16`, the `submit` schema at `:108-111`, and the `next_slice` key it stamps on every result at `:245-247`). A generic `waiting` state makes that slice a ten-line change in transact instead of a bespoke transactions table.
- Agents already have half the loop: MCP exposes `enqueue_job` and `wait_job` (`crates/server/src/mcp/mod.rs:242, 298`) but a job has no way to talk *back* to the agent mid-run; `wait_job` can only return a terminal status. With `waiting`, `wait_job` returns `{status: "waiting", input_request}` and the agent answers with `resume_job`.
- The overlap guard and `GET /schedules` health both read the newest run's status through one predicate `run_holds_slot` (`crates/server/src/scheduler.rs:697-699`) — a `waiting` run must hold the slot (it is not done), which is a one-line, already-tested extension.
- The reaper must not reap a waiting job (no heartbeat, no executor) — it selects on `status='running'` only (`worker.rs:1357`), so `waiting` is naturally outside its scan; a separate `waiting_expires_at` policy replaces it.

### Flow
- Migration: `jobs.status` gains `waiting`; new columns `input_request` (JSON), `waiting_since`, `waiting_expires_at`; `resumed_input` stored beside the checkpoint blob (`checkpoints` table already keyed by job id).
- `AppContext::await_input(request: Value) -> Error::AwaitingInput` — forces a checkpoint, then returns a typed error the worker recognises (same pattern as `Error::BudgetExhausted` routing at `worker.rs:1003-1020`): new outcome arm sets `waiting` instead of `failed`, releases the permit, publishes a `waiting` job event (non-terminal, like `progress`, so `/jobs/{id}/stream` stays open).
- `POST /jobs/{id}/resume {input}` (409 unless `waiting`) → stores input, re-queues via the `reset` semantics (attempt headroom, not a burned attempt) → `load_restore` hands back `{checkpoint, input}`; `ctx.restore_input()` reads it.
- MCP: `wait_job` surfaces the request; add `resume_job`; `list_jobs?status=waiting` for inboxes.
- Expiry: a waiting job past `waiting_expires_at` fails permanently with a distinguishable error (`awaited input not provided`) through `finalize`, so callbacks/terminal triggers fire.
- Port transact: `submit: true` becomes allowed; the app awaits approval with the evidence bundle as the request; on resume it executes `submit_action`, deduped on the `idempotency_key` it already records.
- Second consumer: `research` awaits a clarification when the plan is ambiguous (Claude `-p` subprocess result parsing already exists).

### Expected impact
Operators and agents notice: transact becomes a real actuator with a human gate; agents get a bidirectional job protocol instead of fire-and-poll. Measured by: number of waiting→resumed transitions per week, median wait time, and zero worker permits held by parked jobs (`pumper_jobs{status="waiting"}`). What could break: every consumer that treats non-terminal as "running" (`GET /schedules` health, receipt `wall_ms`, SDK polling) needs the new state added; a mis-scoped resume (`attempts` fence) could resume a lineage that is no longer live — reuse the `(status, attempts)` fence.

### Evaluation
Claim: user - a running job can park for external input at zero slot cost and resume from the exact step, without a bespoke table per app
Before: 0 apps can pause for input; transact hard-refuses `submit: true` at the door and stamps `next_slice: not yet implemented` on every result (`crates/apps/transact/src/lib.rs:245-247`); `JobStatus` has 5 variants
After: `waiting` status + `POST /jobs/{id}/resume`; transact live submit behind approval; MCP `resume_job`; permits held by parked jobs = 0
Method: probe - read the suspend arm, `Storage::reset`, `CancelKind`, `JobStatus`, transact's declared next slice, MCP tool list
Result: unmeasurable (moonshot) — instrument: count of `waiting` transitions and `waiting`-status gauge on `/metrics`; e2e: park → resume → succeed with attempts unchanged
Gate: contract (new job status + columns; SDK and every status consumer must learn it)

### Evidence

```
crates/core/src/job.rs:8-14 (JobStatus: Queued|Running|Succeeded|Failed|Cancelled — no waiting)
crates/server/src/worker.rs:382-400 (CancelKind::{User, ShutdownSuspend} — the park/stop split already exists)
crates/server/src/worker.rs:878-903 (suspend arm: reset + checkpoint intact + `queued` event)
crates/core/src/storage.rs:580-594 (reset: max_attempts = MAX(max_attempts, attempts+1) — a park burns no attempt)
crates/server/src/progress.rs:94,303-319 (force=true bypasses the 5s throttle — the quiesce write)
crates/apps/transact/src/lib.rs:1-16,108-111,245-247 ("live submission requires the human-approval design … not yet implemented")
crates/server/src/mcp/mod.rs:242,298 (wait_job / enqueue_job exist; no way for a job to ask the caller anything)
crates/server/src/scheduler.rs:697-699 (run_holds_slot — single predicate to extend)
crates/server/src/worker.rs:1357 (reaper selects running rows only)
```

## JO2 — Preemptive, deadline-aware scheduling: suspend-to-checkpoint as a scheduler move

- deck item **N21**
- lens: `moonshot-architect` · size: **L** · gate: **contract** · effort 6 / impact 8 / risk 6
- contexts: job-worker, cron-scheduler
- extends: M23 durable execution (the checkpoint suspend is only wired to the shutdown drain) + priority aging

### Summary
The worker has every piece of a preemptive scheduler except the decision: a cooperative suspend that parks a job at its checkpoint with no attempt burned, a priority-aging claim order, per-app caps, and 10 checkpointing apps. But once a slot is taken it is taken until the job ends — a 4-hour crawl holds a permit while a priority-10 agent request queues behind it, and nothing has a due time. Add `deadline_at` to jobs, an earliest-deadline term to the claim order, and let the worker *preempt*: when a claimable job outranks a running one by more than a threshold and the running one has a landed checkpoint, fire its token with `CancelKind::Preempt`, re-queue it (reset semantics), and hand the slot over. Weighted fair-share per app replaces the binary cap.

### Description
- Claim order is a single SQL `ORDER BY (priority + waited/aging) DESC, created_at` (`crates/core/src/storage.rs:403-416`); no `deadline_at` column exists (grep over `crates/core/src` and `crates/server/src` finds none). Per-app fairness is a hard cap (`worker.rs:518-540`, `WorkerConfig.default_app_concurrency`/`app_concurrency` at `crates/core/src/config.rs:1119-1123`) — an app at its cap is *excluded* from the claim, never weighted.
- The suspend path is generic but only the drain calls it: `drain()` collects every registered token and cancels it (`worker.rs:284-297`); `execute` resolves the token to `ShutdownSuspend` only when `state.shutdown.is_cancelled()` (`worker.rs:870-871`, `cancel_kind` at `:394-400`), then `reset` re-queues with headroom (`storage.rs:580-594`) and the checkpoint survives (`worker.rs:882-884`). Everything a preemption needs — token registry keyed by attempt (`worker.rs:67-72`), stale-lineage fence on checkpoint writes (`progress.rs:320-332`), poisoned-blob escape (`worker.rs:482-515`) — is already there.
- Which jobs are safely preemptible is knowable: 17 `ctx.checkpoint*` call sites across 10 app crates (connector-api-watch, cordis, crawl, extractor, grants-gov, mpsv-vpm, plugin, provisioner, research, state-licensing); the sink counts landed saves per run (`progress.rs:268-274`), so "has a fresh checkpoint" is a live boolean, not a guess. Apps without one are simply never preempted.
- The scheduler is a natural deadline source: a cron firing has a semantic due time (its next firing); `EnqueueOptions` built at `scheduler.rs:400-411` can carry `deadline_at = next firing`, so a run that has not started by its successor's due time is the one to escalate — replacing today's silent `Held` outcome (`scheduler.rs:381-384`) with a scheduling signal.
- Registry subject background-jobs (job-progress-and-cancellation) frames cancellation as a first-class lifecycle signal; the preempt kind is the third meaning of the same token.

### Flow
- Migration: `jobs.deadline_at` (nullable), `jobs.preempt_count`; `EnqueueOptions.deadline_at`; door validation (`POST /apps/{name}/jobs`, triggers, schedules) — reuse the `validate_budget_usd` one-door idiom.
- Claim order: effective priority + `deadline_urgency = max(0, (aging_window - (deadline_at - now)) / aging_window) * weight` — one expression edit in `claim_next`; `0` weight = today's behaviour.
- Preemption decision (pure, tested): `should_preempt(candidate, running: &[RunningSummary], threshold, has_checkpoint) -> Option<victim>` evaluated in the claim loop when the semaphore is exhausted (today it just blocks at `worker.rs:30-34`); victim chosen by lowest effective priority with a landed checkpoint and `run_ms` above a floor (never thrash a job that just started).
- `CancelKind::Preempt`: same arm as `ShutdownSuspend` (reset + `queued` event) plus `preempt_count += 1` and a bounded backoff so a job cannot be preempted forever (cap N, then it becomes non-preemptible).
- Weighted fair-share: `[worker] app_weight` map; `blocked_apps` becomes a share computation over `running` counts; cap semantics retained as the ceiling.
- Surfaces: `GET /jobs?status=queued` gains `deadline_at`/`urgency`; receipt gains `preemptions`; `pumper_jobs_preempted_total{app}`.

### Expected impact
Agents (MCP `enqueue_job` with `deadline_at`) and operators notice p95 queue-wait for high-priority work drop from "whenever the crawl ends" to seconds, with the crawl resuming from its frontier checkpoint. Measured by `pumper_job_queue_wait_seconds` split by priority band before/after, and preempted-job total run time overhead (resume cost). What could break: apps whose checkpoint is stale-but-landed redo work on every preemption (bounded by the cap and the run_ms floor); a preempt racing a user cancel must keep the existing intent-priority rule (`worker.rs:394-400`).

### Evaluation
Claim: performance - high-priority/deadline work claims a slot within one poll interval even when every permit is held by long jobs
Before: claim blocks on the semaphore (`worker.rs:30-34`); a permit is held until the job ends; no deadline column; the scheduler's `Held` outcome is the only signal a firing is late
After: `deadline_at` in the claim order; preemption of checkpointed jobs; queue-wait for priority>=N bounded by poll interval + suspend grace
Method: probe - read claim_next SQL, the drain/suspend/reset chain, CancelKind, the checkpoint call-site inventory
Result: unmeasurable (moonshot) — instrument: `pumper_job_queue_wait_seconds` by priority band and `pumper_jobs_preempted_total`; e2e: two-permit worker, long checkpointing job + urgent job → urgent claims within 1 poll
Gate: contract (new job column and enqueue field; SDK/OpenAPI change)

### Evidence

```
crates/core/src/storage.rs:403-416 (claim order: priority + aging only; no deadline)
crates/server/src/worker.rs:30-34 (claim loop blocks on the semaphore; a held permit is never reclaimed)
crates/server/src/worker.rs:284-297 (drain fires every token — the only preemptor today)
crates/server/src/worker.rs:394-400,870-871 (cancel_kind: suspend only when shutting down)
crates/server/src/worker.rs:878-903 + crates/core/src/storage.rs:580-594 (suspend = reset with attempt headroom, checkpoint kept)
crates/server/src/worker.rs:518-540 (blocked_apps: binary per-app cap, no weights)
crates/server/src/scheduler.rs:381-384,400-411 (Held outcome; EnqueueOptions built without a due time)
crates/server/src/progress.rs:268-274 (per-run landed/failed checkpoint tally = live 'preemptible' signal)
10 app crates with ctx.checkpoint* (17 call sites): connector-api-watch, cordis, crawl, extractor, grants-gov, mpsv-vpm, plugin, provisioner, research, state-licensing
```

## JO3 — Maintenance as system jobs: backfill, reindex, retention and doctor run on the queue, online

- deck item **N22**
- lens: `feature-scout` · size: **L** · gate: **contract** · effort 5 / impact 7 / risk 4
- contexts: maintenance-tooling, job-worker, cron-scheduler
- extends: M23 durable execution + the quiet-window maintenance gate (maintenance.rs) + M13/M16 search index derived-artifact work

### Summary
The two maintenance binaries must run with the server *stopped* because the server holds Tantivy's exclusive writer (`search-backfill.rs:19-20`, `reindex.rs:13-14`), and the server's own periodic work is scattered across four piggybacked loops on the scheduler tick plus three spawned janitors. Every one of those is job-shaped: paged, resumable, cancellable, worth a receipt. Make them jobs. A reserved `_system` app family (`search-backfill`, `simhash-reindex`, `retention`, `doctor`, `vacuum`) runs inside the worker with progress, checkpoints, cancel, schedules and receipts — and because the running process *is* the index writer, the "stop the server" constraint disappears. `just reindex` becomes `POST /apps/_system.search-backfill/jobs`.

### Description
- The constraint is documented in both binaries and in `docs/features/search.md:119`: "recovery is the manual `search-backfill` bin, not an automatic rebuild" — even though `GET /datasets/doctor` now *detects* `search_index_empty`. Detection without an in-process remedy is the gap.
- `backfill_dataset` is already the shape of a checkpointable job: a keyset-paged loop over `list_page` with an opaque cursor (`search-backfill.rs:107-156`, `backfill_cursor` at `:147-151`), 500-row chunks (`:40`), per-dataset report. `ctx.checkpoint({app, dataset, after})` every page turns it into a resumable, reap-safe run with `progress` snapshots for free (`progress.rs:342-365`).
- The scheduler tick already hosts four non-cron jobs by piggyback — reaper, DLQ drain, cache refresher, DataHub govern (`scheduler.rs:139-158`) — with a written justification that "this loop is the process's only periodic timer" (`docs/features/runtime.md:106-111`). The refresher shows the cost: its own `RUNNING` overlap guard, its own shutdown select, its own spawn (`refresher.rs:33-83`) — every one of those is a thing the worker already provides to jobs (overlap guard = schedule slot, shutdown = drain suspend, spawn = permit).
- Quiet-window gating exists and is reusable: `maintenance.rs:1-9` gates WAL checkpoint/ANALYZE on the live activity gauge with a stale/harm ladder. A system job would enter the same gauge (`worker.rs:79`) — so the rule "maintenance must not take the writer lock under a live scrape" can be expressed as "a `_system` job is admitted only when the gauge reads zero, or when its harm bound trips" (the ladder, moved from a timer to the claim door).
- The registry is a static list (`registry.rs:10-41`); `_system` apps would be registered there like any app but hidden from `GET /apps` listings and refused over MCP `enqueue_job` (the `[mcp] allow_enqueue` rail at `mcp/mod.rs:130` already exists as the fence).
- Receipt/yield land automatically: `record_job_yield`/`record_job_stages` (`worker.rs:959-970, 1292-1298`) would give each maintenance run a cost/yield row — today `search-backfill` prints one line to stdout and exits.

### Flow
- `crates/apps/system/` crate: `ScrapeApp` impls for `search-backfill` (port `backfill_dataset` verbatim, checkpoint per page), `simhash-reindex` (port `reindex_simhashes` into pages), `retention` (wrap the existing retention sweep), `doctor` (wrap the doctor report, store as result).
- Registry: register under reserved names `_system.*`; `GET /apps` hides them unless `?include=system`; MCP `enqueue_job` refuses them; the `apps depend only on core` rule holds because the search index is reached through `AppContext` — add `ctx.search` (index/delete_ids/flush) as the one new seam, mirroring how `datasets` is exposed.
- Claim-door admission: `_system` jobs are claimable only when the activity gauge is zero or the job carries `force: true` (the harm rung) — a pure `system_job_admissible(gauge, force, stale_since)` predicate with tests.
- Schedules: seed `static-_system.doctor` daily; the `search_index_empty` doctor finding *enqueues* a `search-backfill --all` (self-healing, mirrors `disk-check`'s prune-then-measure).
- Bins become thin clients: `reindex`/`search-backfill` `POST` to a running server if one answers, else fall back to today's offline path (keeps the stopped-server story for a corrupt index).
- justfile recipes updated in the same change.

### Expected impact
Operators notice: no more "stop the server to repair search"; a wiped index heals itself; every maintenance run has a job id, progress, cancel and receipt. Measured by: time-to-searchable after an index wipe (hours/days → one job), and the number of standalone maintenance paths (2 bins + 4 piggybacks → 0 bins required). What could break: a backfill job and a live run's `index()` both hold the writer — Tantivy serialises through the one `TantivyIndex` handle already, so this is a throughput hit, not a lock conflict; deferral policy must be visible or maintenance silently never runs (the `maintenance.rs` deferral-is-an-outcome rule applies).

### Evaluation
Claim: resilience - derived-store repair is online, resumable and observable, and never requires stopping the service
Before: 2 binaries that require the server stopped; a search wipe is repaired only by hand (search.md:119); 0 maintenance runs have a job id or receipt
After: `_system.*` jobs with checkpoints/progress/receipt; doctor finding → automatic backfill enqueue; bins optional
Method: probe - read both bins, the scheduler piggyback block, the refresher's hand-rolled lifecycle, maintenance.rs gate, worker gauge/yield/stage hooks
Result: unmeasurable (moonshot) — instrument: `pumper_jobs{app="_system.*"}` and a `search_index_empty` finding-to-clear latency
Gate: contract (reserved app namespace visible in `jobs.app`, receipts, events; `just` recipes change)

### Evidence

```
crates/server/src/bin/search-backfill.rs:19-20 ("Run with the server STOPPED — Tantivy holds an exclusive writer lock")
crates/server/src/bin/reindex.rs:13-14 (same constraint)
crates/server/src/bin/search-backfill.rs:107-156 (keyset-paged loop with cursor — already checkpoint-shaped)
docs/features/search.md:119 ("recovery is the manual search-backfill bin, not an automatic rebuild")
crates/server/src/scheduler.rs:139-158 (four non-cron jobs piggybacked on the tick)
crates/server/src/refresher.rs:33-83 (hand-rolled overlap guard, shutdown select and spawn — what the worker gives jobs for free)
crates/server/src/maintenance.rs:1-9 (quiet-window gate on the activity gauge)
crates/server/src/worker.rs:79,959-970,1292-1298 (activity gauge entry; yield + stage recording every job gets)
crates/server/src/registry.rs:10-41 (static app list — where `_system.*` registers)
crates/server/src/mcp/mod.rs:130 ([mcp] allow_enqueue rail to fence system apps from agents)
```

## JO4 — Workflow runs: declared multi-step plans with join barriers, templating and one receipt

- deck item **N03**
- lens: `integration-planner` · size: **XL** · gate: **contract** · effort 8 / impact 8 / risk 6
- contexts: job-worker, cron-scheduler
- extends: M11 derived datasets on trigger DAGs + M25 lineage emission + M29 MCP; builds where triggers.md declares its non-goals

### Summary
Triggers chain jobs one edge at a time and explicitly refuse to be a workflow engine: "Non-goals (by design): fan-in/join barriers, `${…}` param templating, per-record fan-out, named pipeline grouping/UI" (`docs/features/triggers.md:87`). That is the right call for *standing* reactive edges — but it leaves no way to submit a *plan*: crawl these 3 seeds in parallel → when all three finish, extract → research the top 20 → publish. Today an agent drives that with six `enqueue_job`/`wait_job` round-trips and its own bookkeeping, and a cron cannot express it at all. Add a `workflow_runs` primitive: a declared DAG of steps, each an ordinary job, with join barriers evaluated in `finalize`, `${steps.crawl.result.pages}` templating, a single budget envelope, one receipt, and a single MCP call.

### Description
- The lineage already exists to hang it on: every enqueue can carry `source_job_id` (`crates/core/src/storage.rs:91, 329-335`), the receipt renders `trigger_hops` from it (`docs/features/runtime.md:199`), and DataHub lineage emission is shipped (M25). What is missing is the *forward* declaration — a run id that steps belong to, and a barrier.
- The seam for barriers is the terminal fan-out: `finalize_with_stages` publishes the terminal event, dispatches the callback, and fires terminal triggers (`crates/server/src/worker.rs:1985-2028`). A `workflow::on_step_terminal(job)` call beside `fire_terminal_triggers` (`:2027`) evaluates "are all predecessors of any pending step terminal?" and enqueues the next steps — the same place, the same fail-open discipline, one ordering test in the existing inventory idiom.
- Budgets compose: each job carries `budget_usd` and the worker seeds spend from the ledger (`worker.rs:770-776`); a workflow envelope is `sum(step spend) <= run budget`, enforced at each step's enqueue (`effective_budget` at `worker.rs:798` already layers a governance override on the job's own cap).
- Suspend/resume semantics come free: steps are jobs, so a shutdown parks them at checkpoints (`worker.rs:878-903`); the run itself is durable rows, so a boot needs no recovery beyond re-evaluating barriers once.
- Cron can own a workflow: schedules enqueue through `EnqueueOptions` (`scheduler.rs:400-412`); a schedule pointing at a workflow definition instead of an app fires a run — the overlap guard (`latest_run`, `scheduler.rs:702-713`) extends to "newest run still open".
- Agent fit: MCP already has `enqueue_job`/`wait_job` (`mcp/mod.rs:242, 298`); `run_workflow` + `wait_workflow` collapses N calls into 2 and gives the agent one receipt (registry fleet-orchestration: parallel-dispatch → result-harvest — one harvest surface for a fanned-out batch).

### Flow
- Tables: `workflow_defs(id, name, spec_json, managed_by)`, `workflow_runs(id, def_id, status, budget_usd, spent_usd, started_at, finished_at, result)`, `workflow_steps(run_id, step, job_id, depends_on[], status)`.
- Spec: `{steps: {name: {app, params (templated), depends_on: [..], budget_usd?, priority?}}, budget_usd?, on_failure: fail_fast|continue}`; validation at the door reuses `validate_app_params` per step (`scheduler.rs:891-899`) so a bad step is a 422, not a failed run.
- Pure core: `ready_steps(spec, step_states) -> Vec<step>` and `render_params(template, completed_results)`; both unit-tested (join, diamond, fail-fast cascade, template miss = refusal).
- Worker hook: `on_step_terminal` in `finalize_with_stages`; enqueues ready steps with `source_job_id`, `workflow_run_id` in `EnqueueOptions`; marks the run terminal when no step is pending; emits `workflow` events on the bus.
- API/MCP: `POST /workflows` (def), `POST /workflows/{id}/runs`, `GET /workflows/runs/{id}` (per-step status, joined receipt), `DELETE …/runs/{id}` (cancels open steps via the existing cancel door); MCP `run_workflow`/`wait_workflow`.
- Schedules: `POST /schedules {workflow: id}`; scheduler fires a run; overlap guard on newest run.
- Catalog: optional `[[workflow]]` in `data-sources.toml` reconciled like schedules (M19 seam).

### Expected impact
Agents and operators notice: a crawl→extract→research pipeline is one call and one receipt; cron can own multi-step pipelines; failures cascade with a named policy instead of half-run chains. Measured by: MCP round-trips per pipeline (6+ → 2), and the fraction of multi-hop `trigger_hops` chains that become declared runs. What could break: a barrier evaluated in a fan-out that was discarded by the staleness fence (`fanout_owns_outcome`, `worker.rs:1169-1171`) — the hook must live in `finalize`, which every terminal path reaches, and a boot re-evaluation must be idempotent.

### Evaluation
Claim: user - a multi-step, fan-in pipeline is submitted, budgeted, scheduled and receipted as one durable unit
Before: 0 join primitives (triggers.md:87 non-goals); an agent needs N enqueue + N wait calls; no run-level budget or receipt
After: `workflow_runs` with barriers, templating, envelope budget, one receipt; 2 MCP calls per pipeline
Method: probe - read triggers non-goals, source_job_id lineage, finalize fan-out order, EnqueueOptions on the scheduler path, MCP tool list
Result: unmeasurable (moonshot) — instrument: workflow run count, steps per run, MCP calls per completed pipeline
Gate: contract (three new tables, new enqueue field, new API/MCP surface)

### Evidence

```
docs/features/triggers.md:87 ("Non-goals (by design): Fan-in/join barriers, ${…} param templating, per-record fan-out, named pipeline grouping/UI, backfill on create")
crates/core/src/storage.rs:91,329-335 (source_job_id lineage on enqueue)
crates/server/src/worker.rs:1985-2028 (finalize_with_stages: terminal event → callback → failure firehose → fire_terminal_triggers — the barrier seam)
crates/server/src/worker.rs:770-776,798 (spend seeded from ledger; effective_budget layering — envelope budget hangs here)
crates/server/src/scheduler.rs:400-412,702-713 (EnqueueOptions on the fire path; latest_run overlap guard)
crates/server/src/scheduler.rs:891-899 (validate_schedule_params — per-step door validation to reuse)
crates/server/src/mcp/mod.rs:242,298 (wait_job / enqueue_job — the N-round-trip agent loop today)
docs/features/runtime.md:199 (receipt trigger_hops from jobs.source_job_id)
```

## JO5 — Elastic executor plane: outbound worker nodes draining the same job queue

- deck item **N18**
- lens: `business-strategist` · size: **XL** · gate: **policy** · effort 9 / impact 7 / risk 8
- contexts: job-worker, cron-scheduler
- extends: M17 distributed fetch fabric (fetch-proxy only) + M23 checkpoints + the heartbeat lease

### Summary
M17 shipped a *fetch* fabric: a coordinator ships one `HttpRequest` to a peer's `POST /fetch-proxy` and gets bytes back (`crates/engine-remote/src/lib.rs:1-6`). The job itself — the browser session, the Claude subprocess, the WASM plugin, the multi-hour crawl — still runs only on the one process that owns the SQLite file. The next order of magnitude is an executor plane: additional pumper processes that *dial out* to the coordinator, long-poll `POST /executors/claim`, run whole jobs with their own engines, stream heartbeats/progress/checkpoints back, and finalize through the coordinator's fan-out. The queue, the fan-out and every gate stay on the coordinator; only `execute` moves. The registry's outbound-compute-plane technique is the topology: executors need no ingress, the poll is the clock and the backpressure boundary.

### Description
- The single-process assumption is concrete: `claim_next` is one `UPDATE … RETURNING` with no executor identity column (`crates/core/src/storage.rs:411-416`); the cancel token registry is an in-process map (`worker.rs:67-72`); the per-app running counts are an `Arc<Mutex<HashMap>>` (`worker.rs:23`).
- The lease primitives a multi-executor design needs already exist and are exercised: heartbeat stamped per `heartbeat_secs` on `(job, attempt)` (`worker.rs:840-862`), the reaper re-queues a lease older than `stale_after_secs` with failure semantics (`worker.rs:1348-1377`), and every finish/fail/checkpoint write is fenced on `(status='running', attempts)` (`docs/features/runtime.md:14`; `progress.rs:320-332`). A remote executor that dies is handled by the reaper *today*, unchanged — the fence is what makes a late write from a dead executor harmless.
- Checkpoints make executor loss cheap: 10 apps checkpoint (17 call sites), so a re-claim on another node resumes rather than restarts (`worker.rs:482-515`).
- The post-run fan-out is already off the execute path and re-checks ownership from the row (`finalize_fanout`, `worker.rs:1185-1201`, `fanout_owns_outcome` at `:1169-1171`) — so an executor's job ends with one call: "here is the result for `(job, attempt)`"; the coordinator runs index/hooks/triggers/DataHub exactly as now.
- What an executor must reach locally: `datasets` (upserts), `costs` (ledger), `health`, `recipes`, `plugins`, `research_cache`, `tiers` — all handed via `AppContext` (`worker.rs:789-812`). This is the hard part and the honest risk: either the executor gets a thin RPC `Datasets` client (batched `upsert_many` over the wire — the peering app already ships a revision-feed replication path, M30), or the executor is limited to apps whose writes are result-only (readable, research, transact, plugin) in v1.
- Auth/transport already exist for the fabric: `REMOTE_SECRET_HEADER` and `[remote] nodes` (`engine-remote/src/lib.rs:19-25`, `crates/core/src/config.rs`); a `[executor] coordinator_url` mirrors it in the other direction.
- Business framing: it is the seam that makes "pumper on the laptop, workers in a cheap VPS with a different IP and a real Chrome" a product line, and it is the fleet substrate (registry: fleet-orchestration/durable-fleet-state, outbound-compute-plane) an agent swarm would depend on.

### Flow
- v1 scope: executors run only apps whose `AppContext` needs are result-only + fetch + research (declare with a capability flag on the manifest, `registry.rs:153-177` `tool_definition` shape).
- Coordinator: `POST /executors/claim` (long-poll, executor id + capabilities → job or 204), `POST /jobs/{id}/heartbeat`, `POST /jobs/{id}/checkpoint`, `POST /jobs/{id}/progress`, `POST /jobs/{id}/finish {attempt, result|error}`; `claim_next` gains `executor_id` and a capability filter; `job_cancels` gains a remote arm (cancel = next heartbeat response says stop).
- Executor binary mode: `pumper --executor` builds engines only, loops claim→execute→report using the same `execute` body with `AppContext` backed by RPC clients for `costs`/`datasets` (v1: result-only apps, so `datasets` is a refusing stub).
- Scheduling: coordinator's `blocked_apps`/caps become cluster-wide from the DB (`running` counts per executor), replacing the in-memory map.
- v2: RPC `Datasets` client (batched, idempotent by revision key) unlocking every app.
- Observability: `GET /executors` (last poll, running jobs, capabilities), `pumper_executors{state}`, receipts stamp `executor_id`.

### Expected impact
Operators notice: concurrency and geography scale by starting a process, not by the coordinator's core count; a Chrome-heavy or Claude-heavy job runs where those are cheap. Measured by cluster job throughput vs single-process, and the share of jobs executed remotely. What could break: split-brain on a partitioned executor still writing — mitigated only by the `(status, attempts)` fence plus reaper; `datasets` RPC latency for chatty apps; secret handling across nodes (policy gate: a shared secret over the network, as the fabric already does).

### Evaluation
Claim: performance - job execution capacity scales horizontally across processes/hosts while the queue, gates and fan-out stay single-writer
Before: 1 executor (the coordinator process); `claim_next` has no executor identity; M17 proxies fetches only
After: N outbound executors draining the queue; throughput bounded by executors not by one host; executor loss = a reap + checkpoint resume
Method: probe - read claim_next, the heartbeat/reaper/fence chain, finalize_fanout ownership check, AppContext construction, engine-remote's scope note
Result: unmeasurable (moonshot) — instrument: jobs/hour by executor_id, p95 claim latency under N executors, re-claims after executor loss
Gate: policy (network-shared secret, cross-host data movement) and contract (executor_id on jobs, new claim/report API)

### Evidence

```
crates/engine-remote/src/lib.rs:1-6 (M17 = coordinator-side fetch proxy only: ships one HttpRequest to a peer's POST /fetch-proxy)
crates/core/src/storage.rs:411-416 (claim_next: single UPDATE…RETURNING, no executor identity)
crates/server/src/worker.rs:23,67-72 (per-app running counts and cancel tokens are in-process maps)
crates/server/src/worker.rs:840-862,1348-1377 (heartbeat lease + reaper — the executor-loss recovery already exists)
crates/server/src/progress.rs:320-332 + docs/features/runtime.md:14 ((status, attempts) fence on every late write)
crates/server/src/worker.rs:482-515 (checkpoint restore on re-claim)
crates/server/src/worker.rs:1169-1171,1185-1201 (fan-out re-checks ownership from the row — finalize is already executor-agnostic)
crates/server/src/worker.rs:789-812 (AppContext: the local handles an executor would need over RPC)
crates/engine-remote/src/lib.rs:19-25 (REMOTE_SECRET_HEADER wire contract to mirror)
```

