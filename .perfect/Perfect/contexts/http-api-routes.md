---
name: "HTTP API & Routes"
type: perfect/context
group: "Job Server & API"
category: api
opportunity: 7
last_proposed: 2026-07-13
cooldown_until: after-round-3
directions: ["[[api-pagination-errors]]", "[[api-streaming-bounded]]", "[[sse-resume-graceful-shutdown]]", "[[openapi-spec]]"]
---

## Current state (scout brief digest, 2026-07-13)

- ~40 routes (routes.rs:18-66). Error type: uniform `{"error": string}`; **every core error collapses to 500** (routes.rs:76-80) — storage not-found surfaces as 500.
- **Pagination**: only /jobs and /datasets records follow the cursor convention; /schedules /watches /triggers /apps /plugins /searches are unbounded full-table; changes/history clamp (1000/500) with NO cursor → silent truncation past clamp; deliveries/runs limit-only. Envelope keys drift ({apps}, {watches}, {count,deliveries}, bare arrays…).
- **Auth: none** (parked decision, http-api.md:3); CORS permissive; no rate limiting, no body-size limits, no request-id.
- **OpenAPI: absent.** **SSE resume: absent** — both handlers ignore Last-Event-ID, set no Event::id, and silently drop on broadcast lag (routes.rs:124,160; channel cap 512 state.rs:83).
- **Graceful shutdown: none** — `axum::serve` without with_graceful_shutdown (main.rs:48); worker/scheduler/janitor loops not shutdown-aware; recovery is startup-time recover_stuck only.
- Heavy inline work: buffered JSON export loads ≤100k rows in memory then **silently truncates** (routes.rs:535-542); dataset_duplicates does synchronous pairwise SimHash unbounded by dataset size (:626-632); /metrics runs 3+ aggregate queries per scrape (:89-115).
- Status quirks: cancel/retry conflate not-found with wrong-state as 409 (:335-356). Job params accepted unvalidated; callback_url not validated (:239).

## Direction history
- 2026-07-13: 5 proposed, 4 accepted (pagination/errors, streaming/bounded, SSE+shutdown, OpenAPI — pool expanded past 10 by owner choice). **REJECTED**: API-key auth — the parked product decision stays parked; do not re-propose until the user raises deployment/exposure.

## Shipped
- [[api-pagination-errors]] → 0a91f46 — cursors on 7 more endpoints (dual-mode), error `code` field, 404/409 distinguished
- [[api-streaming-bounded]] → 268d271 — streamed JSON export (no 100k truncation), 413-capped dup scan, duration/queue-wait metrics + 5s cache
- [[sse-resume-graceful-shutdown]] → 5bdb7ae — EventBus replay ring, Last-Event-ID resume, reset event, SIGTERM drain + requeue-at-deadline (worker.shutdown_drain_secs)
- [[openapi-spec]] → 343341a — OpenAPI 3.1, utoipa-axum single-source router+spec, exact-coverage test (48 ops; Director added /hosts during merge)
