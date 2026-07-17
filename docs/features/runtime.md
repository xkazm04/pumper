# Job runtime, scheduler, budgets & costs

Pumper runs **apps** (implementations of `ScrapeApp`, registered in `crates/server/src/registry.rs`) as **jobs** on a durable SQLite queue.

## Jobs

- Enqueue: `POST /apps/{name}/jobs` with optional `{params, max_attempts, delay_secs, priority, callback_url, callback_secret, budget_usd, idempotency_key}`. An `Idempotency-Key` header (or body field) makes retries safe: a duplicate key returns the original job with `200` instead of a new `202`.
- Lifecycle: `queued → running → succeeded | failed | cancelled`. Failures retry with exponential backoff up to `max_attempts`; `recover_stuck` re-queues jobs orphaned by a crash. `POST /jobs/{id}/retry` resurrects a failed/cancelled job with one extra attempt.
- **Job control surface:**
  - `POST /jobs/retry` — bulk resurrect: re-queues every job in a terminal state (`failed` default, or `cancelled`), optionally scoped to one `app`, up to `limit` (≤500), each with one extra attempt. Returns `{retried, ids}`.
  - `POST /jobs/{id}/reset` — re-queues a **running** job (e.g. one stuck on a hung task) with a fresh attempt budget (409 if not running).
  - `DELETE /jobs/{id}` — cancels a **queued** job synchronously, or signals a **running** job's per-job `CancellationToken` (registered in `AppState::job_cancels`) so the worker aborts the app future and marks it `cancelled` (not `failed`); a terminal job is 409.
  - **Stale-write fence:** the worker's finish/fail/cancel writes are guarded on `(status='running', attempts)`. Because reset (and the reaper, below) re-queue the row and the next claim advances `attempts`, an orphaned task's late `complete`/`fail` matches no row and is discarded — the re-claimed attempt owns the outcome. The token registry entry is attempt-keyed so an overlapping re-claim is never clobbered.
- **Stuck-job reaper (heartbeat lease):** the worker stamps a `heartbeat_at` on each running job every `worker.heartbeat_secs` (default 30) — but only while the app future keeps yielding (`.await`-ing). A task wedged in a non-yielding loop stops beating; a slow-but-alive job keeps beating however long it runs. The reaper (piggybacked on the scheduler tick) re-queues any running job whose heartbeat is older than `worker.stale_after_secs` (default 120) using **failure semantics** — attempts + backoff apply, and an attempts-exhausted job fails permanently (`error: "lease expired (heartbeat stale)"`, with its callback + terminal triggers). This recovers a job hung on a live server (the 900s `job_timeout` only drops the future; it can't reach a task that never yields). Set `heartbeat_secs`/`stale_after_secs` to `0` to disable heartbeating / the reaper. `recover_stuck` still handles jobs orphaned across a full restart.
- **Graceful shutdown:** on Ctrl-C / SIGTERM (on Windows: Ctrl-Break, console-close, or system-shutdown) the process cancels a shared token — the worker stops claiming new jobs, the scheduler and cache janitor stop, and `axum::serve` stops accepting. In-flight jobs are given up to `worker.shutdown_drain_secs` (default 25) to finish; whatever is still `running` at the deadline is re-queued (same effect as `recover_stuck`) so it resumes cleanly on the next boot instead of being stranded.
- Lineage columns: `schedule_id` (fired by a cron schedule), `trigger_id` (fired by a reactive trigger — see [triggers.md](triggers.md)).
- Worker (`crates/server/src/worker.rs`): global concurrency cap + per-app caps (`[worker]` config); wakes instantly on enqueue. After a successful run it indexes the result into search, computes the run's revision batch once, then fires dataset watches, dataset triggers, and saved searches; `finalize()` emits the terminal SSE event, the result webhook, and terminal-job triggers. All side effects are fail-open.
- **Claim order / priority aging:** the worker claims the queued job with the highest *effective* priority = `priority + waited_secs / worker.priority_aging_coefficient_secs`. Aging is a starvation guard — a low-priority job stuck behind a continuous high-priority stream escalates as it waits (default coefficient `900`s → +1 level every 15 min) instead of never running. Set the coefficient to `0` to disable aging and fall back to strict `priority DESC, created_at`. Equal-(effective-)priority claims stay FIFO (oldest `created_at` first).

## Live progress

A long-running app reports compact progress snapshots through `AppContext::progress` (a `ProgressReporter`). The runtime (`crates/server/src/progress.rs`) keeps only the **latest snapshot per in-flight job in memory** (no jobs-table write — a restart drops it, which is acceptable: the job re-queues and re-reports) and emits it as a `progress` job event through the EventBus. Each reporter **throttles** its own persist+emit to ≥ every 2s or every 50 `report` calls, so an in-loop stride never floods the bus.

- `GET /jobs/{id}` includes a `progress` field with the latest snapshot while the job runs (absent once terminal or after a restart) — the rest of the job JSON is unchanged.
- The snapshot rides `progress`-status job SSE events on `/jobs/{id}/stream` and `/events` (non-terminal, so the per-job stream stays open); monotonic ids / replay semantics are unchanged.
- The `crawl` app reports `{crawled, kept, failed, frontier, hosts}` (see [crawling.md](crawling.md)).

## Scheduler

DB-backed cron (6-field, with seconds) reconciled every `schedule_tick_secs`. Apps can declare a static schedule (`ScrapeApp::schedule`, seeded idempotently); runtime CRUD via `GET/POST /schedules`, `DELETE /schedules/{id}`, `POST /schedules/{id}/enabled`. **Overlap guard:** a schedule whose previous job is still queued/running skips the tick without touching `last_run`, so exactly one catch-up run fires when it frees up.

Each schedule carries three cron-maturity fields (`schedules` table cols `timezone`, `misfire_policy`, `max_attempts`; all set at `POST /schedules` and returned by `GET /schedules`):

- **`timezone`** — IANA name (chrono-tz), e.g. `"America/New_York"`; `null` = UTC. The cron expression is evaluated in this zone, so DST transitions are honoured: a firing at a wall-clock time that doesn't exist (inside a spring-forward gap) is skipped to the next valid one. An unknown name is rejected at create time with `400 bad_request`.
- **`misfire_policy`** — how firings missed while the scheduler was down are handled once it's back. `"fire_once"` (default) runs a single catch-up job (collapsing the whole backlog into one run — the historical behaviour, now explicit); `"skip"` runs none and just advances `last_run` past the missed firings (the count is logged). A firing more than two `schedule_tick_secs` late is treated as missed; a firing detected on-time within that grace window always runs under both policies.
- **`max_attempts`** — attempt budget for jobs this schedule enqueues; `null` = server default (**3**), so scheduled runs retry transient failures with backoff exactly like a manual job (previously cron runs were hardcoded to a single attempt).

**Observability (`GET /schedules`).** Each returned schedule is enriched with computed fields so "when does this next fire?", "did the last run succeed?", and "why has this gone quiet?" are answerable over the API instead of only in server logs:
- **`next_run`** — the next firing, projected with the scheduler's own reference rule (`cron.after(last_run ?? created_at)` in the schedule's timezone), so the API can never disagree with the reconcile loop. `null` for an unparseable cron.
- **`last_job_id` / `last_status`** — the most recent job this schedule enqueued (joined on `schedule_id`), or `null` if it has never fired.
- **`health`** — `ok` | `disabled` | `invalid_cron` | `unregistered_app` | `overlapping`, derived from the same conditions the scheduler checks each tick. `overlapping` means the previous run is still queued/running and the overlap guard is holding the next firing back — the one case where an ever-older `last_run` otherwise looks like a dead schedule.

## Budgets & the cost ledger

- Every metered engine call (`AppContext::fetch` / `AppContext::research`) writes a `cost_events` row (job, app, engine tier, url, `cost_usd` — Claude actual, free tiers 0.0, detail incl. escalation trail / `cache_hit`).
- `budget_usd` on a job is a hard ceiling: metered Claude calls abort once cumulative spend reaches it; per-call `max_budget_usd` is clamped to the remaining headroom. `AutoWithResearch` fetches degrade to free tiers instead of failing (noted in the escalation trail); explicit `ctx.research` errors loudly.
- APIs: `GET /jobs/{id}/costs` (events, total, cost-per-fresh-record), `GET /costs?app=&since=` (app×engine rollup), `pumper_cost_usd{app,engine}` gauges on `/metrics`.
- Research cache: identical `ResearchRequest`s within `claude.research_cache_ttl_secs` (default 24h, 0 disables) are served from disk at zero cost, logged as `cache_hit (saved ~$X)` events. `resume_session` requests bypass.

## AppContext (what a running app gets)

`job_id`, `app`, `params`, `engines`, `datasets`, `costs`, `budget_usd`, `research_cache`, `tiers`, `plugins`, `progress` (throttled live-progress seam — see [Live progress](#live-progress)), `artifacts_dir` + helpers: `fetch` (metered, budget-governed, tier-routed), `research` (metered, cached), `upsert`/`upsert_many`/`sync_many`, `save_artifact`, `require_str`, `remaining_budget_usd`.

## Config

`config.toml` (or `$PUMPER_CONFIG`), `#[serde(default)]` throughout — sections: `server, worker, storage, http, browser, claude, governor, cache, plugins, search, triggers, webhooks`. New fields need both the serde default and the manual `Default` impl. `[webhooks]` holds `failure_url`/`failure_secret` — the optional global `job.failed` firehose (see [events-webhooks.md](events-webhooks.md)).

## Metrics

`GET /metrics` (Prometheus text, cached ~5s): `pumper_jobs{status}` gauges, `pumper_job_failures_total{app}` (permanently-failed jobs per app — **DB-derived** from the current `failed` row count, so it resets/decreases if failed jobs are retried or purged rather than being a strictly monotonic process counter), `pumper_job_duration_seconds` + `pumper_job_queue_wait_seconds` summaries (`_sum`/`_count`/`_max`), `pumper_cost_usd{app,engine}`, `pumper_apps`, `pumper_schedules{enabled}`.

## Known gaps

- No auth on the HTTP API (deliberate local power mode; API-key auth is a parked product decision).
