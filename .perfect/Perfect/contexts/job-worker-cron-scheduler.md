---
name: "Job Worker & Cron Scheduler"
type: perfect/context
group: "Job Server & API"
category: lib
opportunity: 7
last_proposed: never
cooldown_until: —
directions: []
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
(never proposed — next round's cursor)

## Shipped
(none)
