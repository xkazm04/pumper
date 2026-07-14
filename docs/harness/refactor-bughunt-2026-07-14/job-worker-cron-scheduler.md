# Job Worker & Cron Scheduler — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 0, Medium: 4, Low: 1)
> Files scanned: `crates/server/src/worker.rs`, `crates/server/src/scheduler.rs` (confirmed against `crates/core/src/storage.rs`, `crates/core/src/config.rs`, `crates/server/src/main.rs`)

## 1. Search index is written before the completion ownership fence — a superseded attempt can clobber live FTS docs with stale content
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: race-condition / write-ordering
- **File**: `crates/server/src/worker.rs:231-246`
- **Scenario**: A job is reaped or `reset` mid-run and re-claimed. The re-claim keeps the **same** `job.id`, so both the original (stale) task and the new (live) task build search docs whose ids are `{app}:{url}` or `{app}:{job_id}:{i}` — identical between the two attempts. The `Outcome::Finished(Ok)` arm calls `state.search.index(docs).await` (line 233) **before** the fenced `complete()` (line 236). If the stale task's `app.run()` future finishes *after* the live attempt has already indexed and completed, the stale task still runs `index(...)` with its older result (overwriting the same doc ids), then hits `complete()` → `Ok(false)` and returns. Its result is correctly discarded from `jobs`, but its **stale content is already in FTS**.
- **Root cause**: Every terminal `jobs` write is `(status, attempts)`-fenced, but `search.index()` is an unfenced side effect executed ahead of that fence. The reaper never cancels the superseded task's token (worker.rs:305-330), so the loser of the completion race still runs to the end and writes search docs.
- **Impact**: wrong/stale search results for records lacking a stable `url` (self-corrects only on the next successful run of that job).
- **Fix sketch**: Move `search.index(docs).await` to run only after `complete()` returns `Ok(true)`, so a discarded (stale) run indexes nothing.

## 2. Reaper staleness threshold vs. heartbeat interval is documented-but-unenforced — a misconfig silently reaps live jobs and double-runs them
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case / config-safety
- **File**: `crates/server/src/worker.rs:305-330` (reads `stale_after_secs`); contract in `crates/core/src/config.rs:123-127`
- **Scenario**: `stale_after_secs`'s doc says "Must exceed `heartbeat_secs`," but nothing validates it (no `validate()` in config.rs; main.rs never checks). If an operator sets `stale_after_secs <= heartbeat_secs` (e.g. 20 vs 30), a perfectly healthy job — beating every 30s — always has `heartbeat_at` older than the 20s cutoff, so `reap_stale` re-queues it on every reaper tick. The reaper does **not** fire the running task's `CancellationToken`, so the original task keeps executing `app.run()` (full scrape + dataset writes) while the re-queued attempt runs concurrently.
- **Root cause**: A safety invariant is expressed only as prose. The reaper trusts the config to keep it away from the heartbeat window; there is no floor (`stale_after_secs = max(stale_after_secs, heartbeat_secs + tick)`), and no cancellation of the reaped task's in-flight future.
- **Impact**: continuous duplicate execution of scheduled/queued jobs — duplicate scrape side effects, wasted metered-Claude budget, dataset write churn — with no error surfaced.
- **Fix sketch**: Validate at startup (reject or clamp `stale_after_secs` below `heartbeat_secs`, warn loudly), and/or have `reap_once` fire the job's registered cancel token when it re-queues so the losing task stops early.

## 3. Non-atomic enqueue + touch_schedule allows a duplicate scheduled run
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: toctou / atomicity
- **File**: `crates/server/src/scheduler.rs:99-124`
- **Scenario**: In the `Fire` arm the scheduler does two independent writes: `enqueue(...)` then `touch_schedule(id, now)`. The overlap guard (`schedule_has_active_job`, line 99) only suppresses a re-fire *while the previous run is still queued/running*; once it reaches a terminal state the guard is clear and only `last_run` records that the firing was handled. If `touch_schedule` fails (its `?` at line 120 propagates) — or the process crashes between the two writes — but the enqueued job then **completes before the next reconcile tick**, the next tick re-derives the same firing from the un-advanced `last_run`, the guard sees no active job, and it enqueues the firing a second time.
- **Root cause**: `enqueue` and `touch_schedule` are separate, non-transactional statements; the "did this firing already fire?" fact lives only in `last_run`, which can lag the actual enqueue.
- **Impact**: double-run of a scheduled scrape on a `touch_schedule` failure or an ill-timed crash.
- **Fix sketch**: Persist `last_run`/`schedule_id` atomically with the enqueue (single transaction), or advance `last_run` first and roll it back on enqueue failure, so the firing is never both enqueued and still "due."

## 4. `reconcile` aborts the whole tick on any single schedule's DB error, starving every later schedule
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: silent-failure / error-handling
- **File**: `crates/server/src/scheduler.rs:66-127`
- **Scenario**: The per-schedule loop uses `?` on `schedule_has_active_job` (line 99) and `touch_schedule` (lines 89 and 120). Schedules are iterated in `ORDER BY app` (storage.rs `list_schedules`). A DB error on schedule *N* returns `Err` from `reconcile`, which the caller only logs ("scheduler reconcile failed"); schedules *N+1…* are never evaluated this tick. If one schedule reliably trips an error, every alphabetically-later schedule is skipped on every tick and effectively never fires.
- **Root cause**: One fallible unit of work (all schedules) instead of per-schedule isolation; a single row's failure is fatal to the batch.
- **Impact**: reliability failure — later schedules silently stop firing while an earlier one is unhealthy.
- **Fix sketch**: Wrap each schedule's body in its own result and `warn!` + `continue` on error (as the invalid-cron and enqueue-error paths already do), so one bad schedule can't starve the rest.

## 5. `misfire_policy = skip` drops a currently-due on-time firing when it shares a tick with an older missed firing
- **Severity**: Low
- **Lens**: bug-hunter
- **Category**: edge-case
- **File**: `crates/server/src/scheduler.rs:184-191`
- **Scenario**: `decide` computes `missed` from the **oldest** pending firing (`earliest`): `now_tz.signed_duration_since(earliest) > grace`. When the scheduler was down across a schedule boundary, a single reconcile can enumerate both a long-past firing and a firing that is due *right now* (well within `grace`). Because `missed` is true (driven by the old one) and the policy is `skip`, the entire batch — including the fresh, on-time firing — is skipped and `last_run` jumps to `now`. Example (hourly, last_run 10:00, back at 12:00:05): firings 11:00 + 12:00 → `earliest = 11:00` → `missed` → `Skip{missed:2}`; the on-time 12:00 run is dropped.
- **Root cause**: The miss classification is batch-level (oldest firing) but the action applies to the whole batch, conflating a genuinely-missed catch-up with a legitimately-due current firing.
- **Impact**: a schedule silently skips one due run immediately after any downtime that crosses a firing boundary.
- **Fix sketch**: Under `skip`, advance past firings older than `grace` but still `Fire` if the **newest** enumerated firing is within `grace` (i.e. split "missed backlog" from "current due firing").
