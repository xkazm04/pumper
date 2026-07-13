# Job runtime, scheduler, budgets & costs

Pumper runs **apps** (implementations of `ScrapeApp`, registered in `crates/server/src/registry.rs`) as **jobs** on a durable SQLite queue.

## Jobs

- Enqueue: `POST /apps/{name}/jobs` with optional `{params, max_attempts, delay_secs, priority, callback_url, callback_secret, budget_usd, idempotency_key}`. An `Idempotency-Key` header (or body field) makes retries safe: a duplicate key returns the original job with `200` instead of a new `202`.
- Lifecycle: `queued → running → succeeded | failed | cancelled`. Failures retry with exponential backoff up to `max_attempts`; `recover_stuck` re-queues jobs orphaned by a crash. `POST /jobs/{id}/retry` resurrects a failed/cancelled job with one extra attempt. `DELETE /jobs/{id}` cancels a queued job.
- **Graceful shutdown:** on Ctrl-C / SIGTERM (on Windows: Ctrl-Break, console-close, or system-shutdown) the process cancels a shared token — the worker stops claiming new jobs, the scheduler and cache janitor stop, and `axum::serve` stops accepting. In-flight jobs are given up to `worker.shutdown_drain_secs` (default 25) to finish; whatever is still `running` at the deadline is re-queued (same effect as `recover_stuck`) so it resumes cleanly on the next boot instead of being stranded.
- Lineage columns: `schedule_id` (fired by a cron schedule), `trigger_id` (fired by a reactive trigger — see [triggers.md](triggers.md)).
- Worker (`crates/server/src/worker.rs`): global concurrency cap + per-app caps (`[worker]` config); wakes instantly on enqueue. After a successful run it indexes the result into search, computes the run's revision batch once, then fires dataset watches, dataset triggers, and saved searches; `finalize()` emits the terminal SSE event, the result webhook, and terminal-job triggers. All side effects are fail-open.
- **Claim order / priority aging:** the worker claims the queued job with the highest *effective* priority = `priority + waited_secs / worker.priority_aging_coefficient_secs`. Aging is a starvation guard — a low-priority job stuck behind a continuous high-priority stream escalates as it waits (default coefficient `900`s → +1 level every 15 min) instead of never running. Set the coefficient to `0` to disable aging and fall back to strict `priority DESC, created_at`. Equal-(effective-)priority claims stay FIFO (oldest `created_at` first).

## Scheduler

DB-backed cron (6-field, with seconds) reconciled every `schedule_tick_secs`. Apps can declare a static schedule (`ScrapeApp::schedule`, seeded idempotently); runtime CRUD via `GET/POST /schedules`, `DELETE /schedules/{id}`, `POST /schedules/{id}/enabled`. **Overlap guard:** a schedule whose previous job is still queued/running skips the tick without touching `last_run`, so exactly one catch-up run fires when it frees up.

## Budgets & the cost ledger

- Every metered engine call (`AppContext::fetch` / `AppContext::research`) writes a `cost_events` row (job, app, engine tier, url, `cost_usd` — Claude actual, free tiers 0.0, detail incl. escalation trail / `cache_hit`).
- `budget_usd` on a job is a hard ceiling: metered Claude calls abort once cumulative spend reaches it; per-call `max_budget_usd` is clamped to the remaining headroom. `AutoWithResearch` fetches degrade to free tiers instead of failing (noted in the escalation trail); explicit `ctx.research` errors loudly.
- APIs: `GET /jobs/{id}/costs` (events, total, cost-per-fresh-record), `GET /costs?app=&since=` (app×engine rollup), `pumper_cost_usd{app,engine}` gauges on `/metrics`.
- Research cache: identical `ResearchRequest`s within `claude.research_cache_ttl_secs` (default 24h, 0 disables) are served from disk at zero cost, logged as `cache_hit (saved ~$X)` events. `resume_session` requests bypass.

## AppContext (what a running app gets)

`job_id`, `app`, `params`, `engines`, `datasets`, `costs`, `budget_usd`, `research_cache`, `tiers`, `plugins`, `artifacts_dir` + helpers: `fetch` (metered, budget-governed, tier-routed), `research` (metered, cached), `upsert`/`upsert_many`/`sync_many`, `save_artifact`, `require_str`, `remaining_budget_usd`.

## Config

`config.toml` (or `$PUMPER_CONFIG`), `#[serde(default)]` throughout — sections: `server, worker, storage, http, browser, claude, governor, cache, plugins, search, triggers`. New fields need both the serde default and the manual `Default` impl.

## Known gaps

- No auth on the HTTP API (deliberate local power mode; API-key auth is a parked product decision).
- Schedule misfire/catch-up policy beyond the single-run overlap guard is not implemented.
