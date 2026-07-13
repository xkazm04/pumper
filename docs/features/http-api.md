# HTTP API

Axum server (default port 8088, `[server]` config). **Local power mode: no auth, permissive CORS** — any localhost app may call it (API-key auth is a parked decision).

**Canonical machine-readable surface: `GET /openapi.json`** — a generated OpenAPI 3.1 document covering every route below, with typed request bodies and query params (response bodies are described inline; the ad-hoc JSON envelopes are documented in prose per endpoint). The spec and the router are generated from the same source (`utoipa` `#[utoipa::path]` annotations + `OpenApiRouter`), so a route cannot be added without appearing in the spec; a path-coverage test fails CI if the two ever diverge. Use it for client codegen and CLI agents; the table below is the human summary.

**Errors:** `{"error": "<message>", "code": "<code>"}` with the matching HTTP status. `code` is a stable machine-readable string derived from the status — branch on it instead of the human message: `bad_request` (400, validation), `not_found` (404), `conflict` (409, wrong state), `too_large` (413), `internal` (500). Not-found, wrong-state, and bad-input are raised explicitly by handlers; unexpected engine/storage failures are `internal`/500.

| Area | Routes |
| --- | --- |
| Health/metrics | `GET /health` · `GET /metrics` (Prometheus text: jobs by status, `pumper_job_failures_total{app}` (DB-derived permanent-failure count per app), apps, schedules, `pumper_cost_usd{app,engine}`, `pumper_job_duration_seconds` + `pumper_job_queue_wait_seconds` summaries with `_sum`/`_count`/`_max`; body cached ~5s so scrape bursts don't re-run the aggregates) |
| Apps | `GET /apps` · `POST /apps/{name}/jobs` (enqueue; `Idempotency-Key` header supported) · `GET /apps/{name}/datasets` |
| Jobs | `GET /jobs?app=&status=&limit=&cursor=` (cursor ⇒ `{items,next_cursor}`) · `GET /jobs/{id}` (adds a `progress` field with the latest live snapshot while running) · `DELETE /jobs/{id}` (cancel: queued synchronously, or a `running` job via its cancellation token — response adds `running:true`; 404 no job, 409 already terminal) · `POST /jobs/{id}/retry` (404 no job, 409 wrong state) · `POST /jobs/retry` bulk (body `{status=failed\|cancelled, app?, limit≤500}` ⇒ `{retried,ids}`; 400 bad status) · `POST /jobs/{id}/reset` (re-queue a `running` job; 404 no job, 409 not running) · `GET /jobs/{id}/stream` (SSE) · `GET /jobs/{id}/costs` |
| Costs | `GET /costs?app=&since=` |
| Schedules | `GET /schedules?limit=&cursor=` · `POST /schedules` (`{app, cron, params?, priority?, timezone?, misfire_policy?, max_attempts?}` — `timezone` IANA/chrono-tz default UTC, `misfire_policy` `fire_once`\|`skip` default `fire_once`, `max_attempts` default server 3; unknown `timezone`/`misfire_policy` → 400) · `DELETE /schedules/{id}` · `POST /schedules/{id}/enabled` |
| Datasets | `GET /datasets/{app}/{ds}?limit=&cursor=` · `GET .../export?format=json\|ndjson\|csv` (all stream; see below) · `GET .../duplicates?distance=` (413 above 10k records) · `GET .../changes?since=&limit=&cursor=` · `GET .../history?key=&limit=&cursor=` |
| Watches | `GET /watches?app=&limit=&cursor=` · `POST /watches` · `DELETE /watches/{id}` · `POST /watches/{id}/enabled` |
| Webhook deliveries | `GET /webhooks/deliveries?status=&limit=&cursor=` · `GET /webhooks/deliveries/{id}` · `POST /webhooks/deliveries/{id}/replay` |
| Triggers | `GET /triggers?app=&limit=&cursor=` · `POST /triggers` · `DELETE /triggers/{id}` · `POST /triggers/{id}/enabled` · `POST /triggers/{id}/test?fire=` · `GET /triggers/{id}/runs` |
| Search | `GET /search?q=&limit=&app=&dataset=&fuzzy=` · `DELETE /search/docs` · `DELETE /search/datasets/{app}/{ds}` |
| Saved searches | `GET /searches?limit=&cursor=` · `POST /searches` · `DELETE /searches/{id}` · `POST /searches/{id}/enabled` |
| Events | `GET /events` (SSE all jobs; monotonic ids + `Last-Event-ID` resume — see [events-webhooks.md](events-webhooks.md)) |
| Hosts | `GET /hosts?limit=&cursor=` (learned tier memory + politeness per host) · `GET /hosts/{host}` (404 unknown) · `DELETE /hosts/{host}/memory` (reset strikes+pin+penalty; 404 unknown) |
| Profiles | `GET /profiles` (session vault: named login profiles; see below) |
| Plugins | `GET /plugins` · `POST /plugins/reload` |
| Extraction | `POST /extract/preview` (dry-run a RuleSet against one document; see below) |
| Grants | `GET /grants?status=&agency=&source=&closing_before=&closing_after=&min_award=&limit=&cursor=` · `GET /grants/closing-soon?days=` (see below) |
| Meta | `GET /openapi.json` (OpenAPI 3.1 spec for all routes) |

Conventions: enable/disable is always `POST …/{id}/enabled {"enabled": bool}`; every list endpoint is dual-mode — without `cursor=` it returns its legacy shape (bare array or `{watches|triggers|searches|changes|revisions|deliveries: [...]}`, unbounded except where a legacy `limit` already applied), and with `cursor=` present (even empty, for page 1) it returns `{items, next_cursor}` and pages by keyset. Cursors are opaque `<stored-ts>|<tiebreak>` tokens (`next_cursor` is `null` on the last page); pass the previous response's `next_cursor` back as `cursor=`. The `changes`/`history` feeds page the full revision set — the legacy no-cursor shapes still clamp at 1000/500 rows, but `cursor=` reaches everything past that. Details of each area live in the sibling feature docs.

## Dataset export & scan limits

`GET /datasets/{app}/{ds}/export` streams in all three formats — constant memory, no row cap, no truncation — by walking the dataset in keyset-paged batches:
- `format=json` (default): a single streamed JSON **array** `[{record},…]` (`content-type: application/json`). This is a bare array, not the former `{app,dataset,count,records}` envelope — the count can't be known before streaming.
- `format=ndjson`: one JSON object per line (`application/x-ndjson`).
- `format=csv`: RFC-4180 rows under a fixed `key,first_seen,last_seen,updated_at,removed_at,data` header (`text/csv`).

All three send `content-disposition: attachment; filename="{ds}.{ext}"`.

`GET /datasets/{app}/{ds}/duplicates` runs an in-memory O(n²) pairwise SimHash sweep, so it is bounded: datasets over **10,000 records** return `413 too_large` (the message carries the actual count and the cap) rather than pinning a core. Narrow the dataset or run the scan offline.

## RuleSet preview (`POST /extract/preview`)

Dry-run a declarative `RuleSet` against one document without enqueuing a job — the authoring loop for selectors. Body `{rules, html}` **or** `{rules, url}` (exactly one of `html`/`url`; both or neither → `400 bad_request`). `rules` is a bare `{field: rule}` map (same shape apps take).

Rules compile **field-by-field**, so a bad set returns `400 bad_request` with a per-field `fields: [{field, error}]` list naming **every** bad field (deserialize errors like an unknown rule `type`, and compile errors like a bad CSS selector / regex / XPath) — not just the first. A non-object `rules` is `400`.

`url` mode fetches through the **HTTP tier only** (no browser, never the paid Claude tier), bounded by a 15s timeout (exceeded → `400`) and an 8 MiB body cap (over → `413 too_large`); a non-`http(s)` url or fetch failure is `400`. Success (`200`) returns `{values, report, fields_matched, fields_total}` — extracted values plus the per-field match report (each field `matched`|`empty`|`error`; see [extraction.md](extraction.md)). No job, dataset write, or cost is incurred. Full detail in [extraction.md](extraction.md).

## Grants query surface (`/grants`)

A filtered read view over **`grants/unified`** — the cross-source corpus that `grants-gov` and `ca-grants` both normalize into (schema in [apps.md](apps.md)). Without it the corpus is reachable only through the generic dataset API, so consumers have to export everything and filter client-side. Both routes read **live records only** (a tombstoned `removed_at` row never appears).

### `GET /grants`

Every filter is optional and **ANDed**; with none set it lists the whole live corpus. A blank param (`?status=`) means *unset*, not "match the empty string", so a UI that always serializes its filter form still works.

| Param | Semantics |
| --- | --- |
| `status` | Exact match on the normalized status: `open` \| `forecasted` \| `closed`. |
| `source` | Exact match on the source app: `grants-gov` \| `ca-grants`. |
| `agency` | **Case-insensitive substring** of the agency name (`agency=health` matches "National Institutes of Health"). `%`/`_` are literal, not wildcards. |
| `closing_before` / `closing_after` | `close_date` on or before / on or after this date. `close_date` is canonical `YYYY-MM-DD`, so the comparison is lexicographic. **Records with no close date are excluded whenever either filter is set** — a forecasted grant with no deadline is not "closing before" anything. A non-`YYYY-MM-DD` value is `400 bad_request`. |
| `min_award` | Keeps records whose **`award_ceiling` >= v OR `total_funding` >= v**. Sources report grant size inconsistently (a per-award ceiling vs. a program total), so matching either keeps the funder's largest published number in play. A record with both fields null never matches — and since grants.gov's Search2 API publishes **no money at all**, `min_award` currently filters the federal corpus out entirely, leaving only `ca-grants`. That is upstream reality, not a bug. |

Dual-mode per the cursor convention: without `cursor=` ⇒ `{grants: [Record]}` capped at `limit` (default 50, **max 500**); with `cursor=` present (even empty) ⇒ `{items, next_cursor}`, keyset-paged by `<updated_at>|<key>` — the filters survive pagination, so walking the cursor recovers the complete filtered set past the 500 cap. Records are the standard dataset shape (`key`, `data`, `first_seen`, `last_seen`, `updated_at`, `removed_at`), newest-updated first.

### `GET /grants/closing-soon?days=`

Live **open** grants whose `close_date` falls within `days` of today, **soonest first**. `days` defaults to **14** and is clamped to **1..=365**. Returns `{days, count, grants}`, where each grant is its unified `data` object plus `key` and `days_left` (0 = closes today). `count` is the full window total; `grants` is **capped at 200**. Sorting is by `close_date` rather than the store's `updated_at` order, so the window is read up to an internal bound of 1000 rows before it is sorted and truncated.

This is **cross-source** — the pre-existing `closingSoon` digest in the grants-gov job artifact is federal-only and computed from raw API hits, so it never sees CA grants. It is **computed on read**, not materialized as a dataset: membership changes with the *calendar*, not with the data, so a snapshotted list would go stale between syncs even when nothing upstream changed. The corpus is small enough that a read view costs nothing to keep correct.

**Performance stance:** both routes filter with SQLite `json_extract` over the `data` column, i.e. a full scan of the `(app, dataset)` partition with no index on the filtered fields. That is the right trade at current scale (the unified corpus is in the low thousands) and it keeps the record store free of any coupling to an app's record shape — new filters need no migration. If the corpus grows to where the scan hurts, the escape hatch is a generated column over the hot field plus an index on it; the query builder would not have to change.

## Host profiles (`/hosts`)

Diagnostics over the tiered fetcher's learned per-host state (see [fetching.md](fetching.md)). Each host object: `host`, `preferred_tier` (`"browser"` when pinned, else `null`), `http_strikes`, `penalty_ms` (the **live** governor politeness penalty in ms — the stored snapshot is only for boot restore), `updated_at` (last tier-outcome change), `penalty_updated_at` (last penalty snapshot, or `null`).

- `GET /hosts` — dual-mode list, most-recently-active first: no `cursor=` ⇒ `{hosts: [...]}`; `cursor=` present ⇒ `{items, next_cursor}` keyset-paged by `<updated_at>|<host>`.
- `GET /hosts/{host}` — one host's profile; `404 not_found` when the host has no learned state. A host with only a live (not-yet-snapshotted) penalty is still returned.
- `DELETE /hosts/{host}/memory` — resets the host: drops strikes + browser pin + persisted penalty **and** clears the live governor penalty; `{host, reset: true}` on success, `404 not_found` when unknown.

## Session profiles (`/profiles`)

Read-only view of the session vault — the named login profiles a fetch can run under (`profile` on `FetchRequest`/`HttpRequest`/`RenderRequest`; full semantics in [fetching.md](fetching.md)).

- `GET /profiles` — `{profiles: [{name, has_cookies, has_browser_dir, last_used}]}`, alphabetical by `name`. `has_cookies` = a persistent HTTP jar (`cookies.json`) exists; `has_browser_dir` = a Chrome user-data-dir (`browser/`) exists; `last_used` = newest mtime across the profile dir and those two artifacts (RFC 3339, `null` if unreadable). An absent vault dir returns an **empty list, not an error** — it is created by the first profiled fetch. Entries whose names aren't valid profiles (or aren't directories) are ignored.

Profiles are created implicitly by the first fetch that names them; there is **no create/delete API** in phase 1 (delete = remove the directory under `[fetcher] profiles_dir`, default `data/profiles`). A request naming an invalid profile fails with a typed profile error (`500 internal` at the API boundary — names are validated in the engines, not at the route).

## Known gaps

No bundled Swagger/Scalar UI — the raw spec is served at `/openapi.json`; point any external viewer at it.
