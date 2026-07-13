---
slug: job-failed-webhooks
type: perfect/direction
context: "[[Job Worker & Cron Scheduler]]"
lens: wildcard
status: shipped
size: S
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 041055b
---

## What & why
A permanently-failed job produces a log line and a row — no webhook, no metric distinguishing exhausted-attempts, so failures are discovered by polling. Emit `job.failed` through webhook::dispatch_event (logged, signed, replayable) on permanent failure + pumper_job_failures_total{app} metric.

## Evidence
- fail_permanently silent: crates/core/src/storage.rs:236-246; worker fail path worker.rs:157-169
- All-webhooks-via-dispatch_event convention: harness-learnings (wave 5)
- No failure metric: routes.rs /metrics families

## Acceptance criteria
- [ ] job.failed event only on permanent failure (not retryable requeues); payload: job id, app, error, attempts.
- [ ] Flows through webhook::dispatch_event with delivery logging/replay; needs a subscription mechanism consistent with existing watch/callback patterns (builder states the chosen wiring; job callback_url also notified if set — check existing callback semantics first).
- [ ] pumper_job_failures_total{app} counter on /metrics.
- [ ] docs/features/events-webhooks.md updated.

## Risks / non-goals
- Non-goal: DLQ table/UI; retention policies.

## Build record
(pending)
