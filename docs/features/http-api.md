# HTTP API

Axum server (default port 8088, `[server]` config). **Local power mode: no auth, permissive CORS** — any localhost app may call it (API-key auth is a parked decision). Errors: `{"error": "..."}` with proper status.

| Area | Routes |
| --- | --- |
| Health/metrics | `GET /health` · `GET /metrics` (Prometheus text: jobs by status, apps, schedules, `pumper_cost_usd{app,engine}`) |
| Apps | `GET /apps` · `POST /apps/{name}/jobs` (enqueue; `Idempotency-Key` header supported) · `GET /apps/{name}/datasets` |
| Jobs | `GET /jobs?app=&status=&limit=&cursor=` (cursor ⇒ `{items,next_cursor}`) · `GET /jobs/{id}` · `DELETE /jobs/{id}` (cancel queued) · `POST /jobs/{id}/retry` · `GET /jobs/{id}/stream` (SSE) · `GET /jobs/{id}/costs` |
| Costs | `GET /costs?app=&since=` |
| Schedules | `GET/POST /schedules` · `DELETE /schedules/{id}` · `POST /schedules/{id}/enabled` |
| Datasets | `GET /datasets/{app}/{ds}?limit=&cursor=` · `GET .../export?format=json\|ndjson\|csv` · `GET .../duplicates?distance=` · `GET .../changes?since=&limit=` · `GET .../history?key=` |
| Watches | `GET/POST /watches` · `DELETE /watches/{id}` · `POST /watches/{id}/enabled` |
| Webhook deliveries | `GET /webhooks/deliveries?status=` · `GET /webhooks/deliveries/{id}` · `POST /webhooks/deliveries/{id}/replay` |
| Triggers | `GET/POST /triggers` · `DELETE /triggers/{id}` · `POST /triggers/{id}/enabled` · `POST /triggers/{id}/test?fire=` · `GET /triggers/{id}/runs` |
| Search | `GET /search?q=&limit=&app=&dataset=&fuzzy=` · `DELETE /search/docs` · `DELETE /search/datasets/{app}/{ds}` |
| Saved searches | `GET/POST /searches` · `DELETE /searches/{id}` · `POST /searches/{id}/enabled` |
| Events | `GET /events` (SSE all jobs) |
| Plugins | `GET /plugins` · `POST /plugins/reload` |

Conventions: enable/disable is always `POST …/{id}/enabled {"enabled": bool}`; list endpoints keep legacy bare-array shapes unless `cursor=` is present; details of each area live in the sibling feature docs.

## Known gaps

No OpenAPI spec/Swagger UI (backlog). No pagination on schedules/watches/triggers lists (small tables).
