# Vision Scan Fix Wave 4 — API Surface Hardening

> 6 commits, 6 ideas closed + 3 duplicates absorbed (theme T7: integration-surface reliability).
> Baseline preserved: build clean → build clean; tests 37 → 37, 0 failed.

## Commits

| # | Commit | Idea | Title |
|---|---|---|---|
| 1 | `28d07ae` | f44d36dc | Persistent webhook delivery log with replay (absorbs c83b4d3e DLQ) |
| 2 | `717361d` | 0b61ffc5 | Idempotency-Key on job enqueue |
| 3 | `75d82c1` | ead7219e | Manual retry/requeue for failed jobs |
| 4 | `d0ea814` | 83d77c80 | Streaming CSV/NDJSON export (absorbs dbc5909f, 05a0c752; Parquet deferred) |
| 5 | `c7eb1a4` | 12694c51 | Cursor pagination for jobs + records |
| 6 | `5390534` | cc245097 | Prevent overlapping runs of the same schedule |

## What was built

- **Delivery log / DLQ** (`webhook_deliveries`, migration 0010): every outbound webhook (job callbacks + watch events) logged with body/attempts/status via one shared retry loop. `GET /webhooks/deliveries?status=failed` = dead-letter view; `POST .../{id}/replay` re-sends with the source's current secret.
- **Idempotent enqueue** (migration 0011): `Idempotency-Key` header/body field + partial unique index; replays return the original job (200 vs 202); insert-race loser re-selects the winner.
- **Job retry**: `POST /jobs/{id}/retry` clears failed/cancelled state, grants one attempt, wakes worker, emits `queued` event; 409 otherwise.
- **Streaming export**: `?format=csv|ndjson` streams keyset-paged 1000-row batches with content-disposition; `json` keeps legacy buffered shape. `Datasets::list_page` keyset method added.
- **Cursor pagination**: `cursor=` on `GET /jobs` and `GET /datasets/{app}/{ds}` switches to `{items, next_cursor}` (keyset `ts|id` cursors); absent = legacy bare array. `Storage::list_page` added.
- **Schedule overlap guard** (migration 0012): jobs record `schedule_id`; scheduler skips a tick while a queued/running job from the same schedule exists, without touching `last_run` (one catch-up run, no stacking).

## Patterns established

10. **One logged delivery loop for all webhook kinds** — job callbacks and change events share `deliver()` + `webhook_deliveries`; new event kinds get logging/replay for free.
11. **Cursor-presence switches response shape** — adding pagination to a bare-array endpoint without breaking existing consumers: `cursor=` (even empty) opts into `{items, next_cursor}`.
12. **Skip-without-touch for schedule overlap** — leaving `last_run` untouched on an overlap skip turns the guard into a natural one-run catch-up policy.

## Deferred with reasons

- API-key auth (23152939) — auth surface; needs an explicit product decision (localhost-power-mode is deliberate).
- OpenAPI/Swagger (6c051cfa), SSE Last-Event-ID (17a74710), misfire policy (e2ae6ccb), hot-reload config (e4b95bbe) — lower value, left pending.

## What remains (INDEX themes)

T4 search activation, T9 domain data products, T10 platform plays.
