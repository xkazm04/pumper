# HTTP API

Axum server (default port 8088, `[server]` config). **Local power mode: no auth, permissive CORS** — any localhost app may call it (API-key auth is a parked decision).

**Canonical machine-readable surface: `GET /openapi.json`** — a generated OpenAPI 3.1 document covering every route below, with typed request bodies and query params (response bodies are described inline; the ad-hoc JSON envelopes are documented in prose per endpoint). The spec and the router are generated from the same source (`utoipa` `#[utoipa::path]` annotations + `OpenApiRouter`), so a route cannot be added without appearing in the spec; a path-coverage test fails CI if the two ever diverge. Use it for client codegen and CLI agents; the table below is the human summary.

**Errors:** `{"error": "<message>", "code": "<code>"}` with the matching HTTP status. `code` is a stable machine-readable string derived from the status — branch on it instead of the human message: `bad_request` (400, validation), `not_found` (404), `conflict` (409, wrong state), `too_large` (413), `internal` (500). Not-found, wrong-state, and bad-input are raised explicitly by handlers; unexpected engine/storage failures are `internal`/500.

| Area | Routes |
| --- | --- |
| Health/metrics | `GET /health` · `GET /metrics` (Prometheus text: jobs by status, apps, schedules, `pumper_cost_usd{app,engine}`, `pumper_job_duration_seconds` + `pumper_job_queue_wait_seconds` summaries with `_sum`/`_count`/`_max`; body cached ~5s so scrape bursts don't re-run the aggregates) |
| Apps | `GET /apps` · `POST /apps/{name}/jobs` (enqueue; `Idempotency-Key` header supported) · `GET /apps/{name}/datasets` |
| Jobs | `GET /jobs?app=&status=&limit=&cursor=` (cursor ⇒ `{items,next_cursor}`) · `GET /jobs/{id}` · `DELETE /jobs/{id}` (cancel queued; 404 no job, 409 wrong state) · `POST /jobs/{id}/retry` (404 no job, 409 wrong state) · `GET /jobs/{id}/stream` (SSE) · `GET /jobs/{id}/costs` |
| Costs | `GET /costs?app=&since=` |
| Schedules | `GET /schedules?limit=&cursor=` · `POST /schedules` · `DELETE /schedules/{id}` · `POST /schedules/{id}/enabled` |
| Datasets | `GET /datasets/{app}/{ds}?limit=&cursor=` · `GET .../export?format=json\|ndjson\|csv` (all stream; see below) · `GET .../duplicates?distance=` (413 above 10k records) · `GET .../changes?since=&limit=&cursor=` · `GET .../history?key=&limit=&cursor=` |
| Watches | `GET /watches?app=&limit=&cursor=` · `POST /watches` · `DELETE /watches/{id}` · `POST /watches/{id}/enabled` |
| Webhook deliveries | `GET /webhooks/deliveries?status=&limit=&cursor=` · `GET /webhooks/deliveries/{id}` · `POST /webhooks/deliveries/{id}/replay` |
| Triggers | `GET /triggers?app=&limit=&cursor=` · `POST /triggers` · `DELETE /triggers/{id}` · `POST /triggers/{id}/enabled` · `POST /triggers/{id}/test?fire=` · `GET /triggers/{id}/runs` |
| Search | `GET /search?q=&limit=&app=&dataset=&fuzzy=` · `DELETE /search/docs` · `DELETE /search/datasets/{app}/{ds}` |
| Saved searches | `GET /searches?limit=&cursor=` · `POST /searches` · `DELETE /searches/{id}` · `POST /searches/{id}/enabled` |
| Events | `GET /events` (SSE all jobs; monotonic ids + `Last-Event-ID` resume — see [events-webhooks.md](events-webhooks.md)) |
| Hosts | `GET /hosts?limit=&cursor=` (learned tier memory + politeness per host) · `GET /hosts/{host}` (404 unknown) · `DELETE /hosts/{host}/memory` (reset strikes+pin+penalty; 404 unknown) |
| Plugins | `GET /plugins` · `POST /plugins/reload` |
| Meta | `GET /openapi.json` (OpenAPI 3.1 spec for all routes) |

Conventions: enable/disable is always `POST …/{id}/enabled {"enabled": bool}`; every list endpoint is dual-mode — without `cursor=` it returns its legacy shape (bare array or `{watches|triggers|searches|changes|revisions|deliveries: [...]}`, unbounded except where a legacy `limit` already applied), and with `cursor=` present (even empty, for page 1) it returns `{items, next_cursor}` and pages by keyset. Cursors are opaque `<stored-ts>|<tiebreak>` tokens (`next_cursor` is `null` on the last page); pass the previous response's `next_cursor` back as `cursor=`. The `changes`/`history` feeds page the full revision set — the legacy no-cursor shapes still clamp at 1000/500 rows, but `cursor=` reaches everything past that. Details of each area live in the sibling feature docs.

## Dataset export & scan limits

`GET /datasets/{app}/{ds}/export` streams in all three formats — constant memory, no row cap, no truncation — by walking the dataset in keyset-paged batches:
- `format=json` (default): a single streamed JSON **array** `[{record},…]` (`content-type: application/json`). This is a bare array, not the former `{app,dataset,count,records}` envelope — the count can't be known before streaming.
- `format=ndjson`: one JSON object per line (`application/x-ndjson`).
- `format=csv`: RFC-4180 rows under a fixed `key,first_seen,last_seen,updated_at,removed_at,data` header (`text/csv`).

All three send `content-disposition: attachment; filename="{ds}.{ext}"`.

`GET /datasets/{app}/{ds}/duplicates` runs an in-memory O(n²) pairwise SimHash sweep, so it is bounded: datasets over **10,000 records** return `413 too_large` (the message carries the actual count and the cap) rather than pinning a core. Narrow the dataset or run the scan offline.

## Host profiles (`/hosts`)

Diagnostics over the tiered fetcher's learned per-host state (see [fetching.md](fetching.md)). Each host object: `host`, `preferred_tier` (`"browser"` when pinned, else `null`), `http_strikes`, `penalty_ms` (the **live** governor politeness penalty in ms — the stored snapshot is only for boot restore), `updated_at` (last tier-outcome change), `penalty_updated_at` (last penalty snapshot, or `null`).

- `GET /hosts` — dual-mode list, most-recently-active first: no `cursor=` ⇒ `{hosts: [...]}`; `cursor=` present ⇒ `{items, next_cursor}` keyset-paged by `<updated_at>|<host>`.
- `GET /hosts/{host}` — one host's profile; `404 not_found` when the host has no learned state. A host with only a live (not-yet-snapshotted) penalty is still returned.
- `DELETE /hosts/{host}/memory` — resets the host: drops strikes + browser pin + persisted penalty **and** clears the live governor penalty; `{host, reset: true}` on success, `404 not_found` when unknown.

## Known gaps

No bundled Swagger/Scalar UI — the raw spec is served at `/openapi.json`; point any external viewer at it.
