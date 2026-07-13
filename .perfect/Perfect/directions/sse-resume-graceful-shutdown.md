---
slug: sse-resume-graceful-shutdown
type: perfect/direction
context: "[[HTTP API & Routes]]"
lens: robustness
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 5bdb7ae
---

## What & why
Both SSE streams silently drop events on broadcast lag and can't resume (no Event::id, Last-Event-ID ignored) — deferred T7 item. And there is no graceful shutdown: SIGTERM kills in-flight jobs mid-run, leaving them `running` until next boot's recover_stuck. Don't lose events; don't lose jobs.

## Evidence
- Lag drops + no ids: routes.rs:118-166 (continue on lag :124,:160; no .id() :168-173); channel cap 512 state.rs:83
- No shutdown: main.rs:41-48 (axum::serve without with_graceful_shutdown; worker/scheduler/janitor not shutdown-aware)
- Deferred: harness-learnings.md:29 (SSE Last-Event-ID)

## Acceptance criteria
- [ ] Monotonic event ids on all SSE events; bounded in-memory replay ring; Last-Event-ID replays the gap or signals `reset` when too old.
- [ ] Signal handling: stop claiming new jobs, drain running jobs with a deadline, then exit; axum graceful shutdown wired.
- [ ] Jobs still running at deadline are re-queued (not left `running`).
- [ ] docs/features/events-webhooks.md + runtime.md updated (remove gap notes).

## Risks / non-goals
- Non-goal: durable/persisted event log; replay ring is in-memory best-effort.

## Build record
(pending)
