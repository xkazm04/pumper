---
slug: crawl-live-progress
type: perfect/direction
context: "[[Broad Crawler]]"
lens: api-ux
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 78ad7da
---

## What & why
A 100k-page crawl is a black box until completion: crawl() returns stats only at the end, AppContext has no progress seam, only mid-run signal is polling the checkpoint file. Add a progress reporter to AppContext; surface as `progress` on GET /jobs/{id} and `progress`-kind job SSE events via the round-1 EventBus.

## Evidence
- Stats at completion only: crates/core/src/crawl.rs:302
- No progress channel: crates/core/src/app.rs:19-37
- EventBus exists: crates/server/src/events.rs (round 1)

## Acceptance criteria
- [ ] AppContext progress seam (throttled; apps opt in — crawl wired first).
- [ ] GET /jobs/{id} exposes latest progress snapshot; SSE emits progress events (ids/replay consistent with EventBus).
- [ ] Crawl reports {crawled, kept, failed, frontier, hosts} periodically.
- [ ] OpenAPI + docs (runtime.md/crawling.md/events-webhooks.md as mapped) updated.

## Risks / non-goals
- Keep progress compact (repo law: compact job results); no per-page progress spam.

## Build record
(pending)
