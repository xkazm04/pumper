---
name: "Job Worker & Cron Scheduler"
type: perfect/context
group: "Job Server & API"
category: lib
opportunity: 7
last_proposed: 2026-07-13
cooldown_until: —
directions: ["[[job-control-surface]]", "[[stuck-job-reaper]]", "[[cron-maturity]]", "[[priority-aging]]", "[[job-failed-webhooks]]"]
---

## Current state (scout brief digest, 2026-07-13 — FRESH, reuse next round, do not re-scout)

- Worker: global Semaphore(4), per-job tokio::spawn, race-safe atomic claim (storage.rs:182-193, priority DESC + FIFO). Per-app concurrency cap machinery exists but **default 0 = unlimited** (config.rs:93) — one app can starve all slots; counter in-memory only (not multi-process safe).
- Timeout 900s drops the future (cooperative only); **no way to cancel a running job** (cancel is queued-only, storage.rs:249-258).
- Retry: exp backoff 10·2^attempts cap 3600s; **scheduled jobs enqueue with max_attempts=1** → cron runs never auto-retry (scheduler.rs:69). POST /jobs/{id}/retry covers failed|cancelled only, +1 attempt, no bulk, can't reset stuck running.
- recover_stuck() only at startup (main.rs:28-31); **no periodic reaper/lease/heartbeat** — a hung task's job stays `running` forever.
- Scheduler: fixed 15s tick, 6-field cron, UTC-only (no tz column), overlap guard via schedule_has_active_job (0012). **Misfire = exactly one catch-up run**, no backfill/policy (documented gap runtime.md:34). No adaptive cadence.
- Observability: pumper_jobs{status} gauges only; **no duration/queue-wait metrics, no DLQ**, failed jobs sit in the same table with no alerting; side effects (watches/triggers/search-index/webhooks) fail-open with warn logs only.
- No graceful shutdown (covered by accepted direction [[sse-resume-graceful-shutdown]] in HTTP API context — coordinate, don't duplicate).

## Direction history
- 2026-07-13 (round 2): 5 proposed, **5 accepted** (job control surface, stuck-job reaper, cron maturity, priority aging, job.failed webhooks).

## Shipped
- [[priority-aging]] → 49e133c — effective priority = priority + waited/coeff (default 900s/level, 0 disables), deterministic starvation tests
- [[job-control-surface]] → 5a6258a — POST /jobs/retry bulk, POST /jobs/{id}/reset, DELETE cancels running jobs; attempt-fenced complete/fail writes discard orphaned-task results (live-verified). Minor accepted race: stale run's search docs indexed pre-fence, overwritten by live attempt's re-index.
- [[stuck-job-reaper]] → f04e2a8 — heartbeat_at (migration 0017), heartbeat only while app future yields (wedge detector), reaper on scheduler tick via fail() semantics (live-verified reap + non-reap of alive job)
- [[cron-maturity]] → c544db2 — migration 0018 (timezone/misfire_policy/max_attempts), cron evaluated in schedule tz (cron 0.12 native, DST-gap tests), misfire fire_once|skip (live: collapsed=179→1 job vs missed=180→0), scheduled default max_attempts 3
- [[job-failed-webhooks]] → 041055b — [webhooks] failure_url global subscription via dispatch_event (logged/signed/replayable, live-verified HMAC delivery), pumper_job_failures_total{app} DB-derived; callback_url path untouched (already sends job.terminal)
Context COMPLETE: 5/5 shipped.
