# HTTP API

Axum server (default port 8088, `[server]` config). **Local power mode: no auth** — any process that can reach the loopback listener may call it (API-key auth is a parked decision). CORS is **off by default**; a browser UI opts in by listing its exact origin in `[server] cors_allowed_origins` (see [../deployment.md](../deployment.md#cors)). Every route carries a request-body ceiling — see [Request body limits](#request-body-limits).

**Canonical machine-readable surface: `GET /openapi.json`** — a generated OpenAPI 3.1 document covering every route below, with typed request bodies and query params (response bodies are described inline; the ad-hoc JSON envelopes are documented in prose per endpoint). The spec and the router are generated from the same source (`utoipa` `#[utoipa::path]` annotations + `OpenApiRouter`), so a route cannot be added without appearing in the spec; a path-coverage test fails CI if the two ever diverge. Use it for client codegen and CLI agents; the table below is the human summary.

**Errors:** `{"error": "<message>", "code": "<code>"}` with the matching HTTP status. `code` is a stable machine-readable string — branch on it instead of the human message. The complete map, which an inventory test diffs against the statuses the handlers actually emit (so a new status cannot ship without a code):

| status | `code` | meaning |
| --- | --- | --- |
| 400 | `bad_request` | validation — a malformed query, filter, rule, id, or an unusable `profile` |
| 401 | `unauthorized` | missing/wrong signature on `POST /ingest/{id}` |
| 402 | `budget_exhausted` | the job's `budget_usd` ceiling is already reached. **Deterministic** — retrying re-reads the same ledger and refuses again |
| 403 | `forbidden` | the ingress source exists but is disabled |
| 404 | `not_found` | no such job/dataset/record/source |
| 409 | `conflict` | wrong state for the operation (a terminal job, a disabled subsystem, a cassette that cannot serve a replay) |
| 413 | `too_large` | body over a documented per-route ceiling |
| 422 | `unprocessable` | understood and deliberately refused (e.g. a transact flow this slice will not run) |
| 429 | `rate_limited` | per-source ingress rate limit — back off and re-send |
| 500 | `internal` | an unexpected failure in this service |
| 502 | `bad_gateway` | an upstream/engine failure (HTTP, browser, Claude) |
| 503 | `unavailable` | the subsystem is switched off in config — e.g. the five source-health routes with `[resilience] enabled = false` |

**5xx bodies are deliberately generic** (`internal error`, `upstream engine failure`) and do not vary with the cause: raw SQLite/sqlx text, filesystem paths under the data dir, and upstream URLs used to reach the client verbatim. That detail is logged server-side at `error` (and reaches Sentry when configured) against the same status — branch on `code`, read the server log for the cause. The two 4xx cases whose messages are built from server-side paths (`profile`, replay-miss) are likewise fixed strings that name the *parameter* at fault rather than the path.

**A panicking handler is contained**, not dropped: it answers `500 internal` in this same envelope (a `CatchPanicLayer` sitting closest to the handlers, inside the trace span so the panic is logged with its method and URI). Previously the connection was reset with no status and no body — indistinguishable from the process having died — and nothing was logged. The panic's own text stays server-side. This is the HTTP layer only; the worker's separate containment (a panicking app fails its job through the normal attempt-fenced path) is unchanged.

The one 413 that is **not** in this envelope is the request-body ceiling below — it is refused by the extractor before any handler runs, so it comes back as axum's own plain-text rejection with status `413`. Branch on the status, not the body, if you need to catch both.

## Request body limits

Every route carries a body ceiling (`DefaultBodyLimit`, installed in `crates/server/src/routes/mod.rs`). An over-limit body is **rejected with `413 Payload Too Large`, never truncated** — the handler does not run and no partial request is acted on.

| Scope | Limit | Constant |
| --- | --- | --- |
| Every route (global default) | **1 MiB** | `BODY_LIMIT_BYTES` |
| `POST /extract/preview` | **8 MiB** | `PREVIEW_BODY_LIMIT_BYTES` |

The global 1 MiB is sized from what the POST surface actually accepts — all hand-authored JSON config (job `params`, a cron string, a webhook URL + secret, a trigger definition, a saved search, a source-state flip). The widest, `POST /jobs/retry`, carries only `{app, status, limit}`. Legitimate bodies are kilobytes, so the ceiling is headroom, not a quota.

`POST /extract/preview` is the only route that takes a **document** rather than config, so it carries a scoped 8 MiB override — deliberately equal to the 8 MiB budget the same endpoint already enforces on its `url`-fetch path, so a page cannot preview through `url` and 413 through `html`. The override is scoped by construction (a separate sub-router) and pinned by an EXPECTED-diff inventory test: adding a route to the larger ceiling is a visible, deliberate edit, and raising the *global* limit for a new document route is the anti-pattern that test exists to prevent. The override is still a ceiling — past 8 MiB, preview 413s too.

| Area | Routes |
| --- | --- |
| Health/metrics | `GET /health` · `GET /metrics` (Prometheus text: jobs by status, `pumper_job_failures_total{app}` (DB-derived permanent-failure count per app), apps, schedules, `pumper_cost_usd{app,engine}`, `pumper_job_duration_seconds` + `pumper_job_queue_wait_seconds` summaries with `_sum`/`_count`/`_max`; body cached ~5s so scrape bursts don't re-run the aggregates) |
| Apps | `GET /apps` · `POST /apps/{name}/jobs` (enqueue; `Idempotency-Key` header supported; `budget_usd` must be **> 0** — omitting it means *no* ceiling, so `0`/negative is a 422 rather than a silent "unlimited", see [runtime.md § Budgets](runtime.md#budgets--the-cost-ledger)) · `GET /apps/{name}/datasets` |
| Jobs | `GET /jobs?app=&status=&limit=&cursor=` (cursor ⇒ `{items,next_cursor}`) · `GET /jobs/{id}` (adds a `progress` field with the latest live snapshot while running) · `DELETE /jobs/{id}` (cancel: queued synchronously, or a `running` job via its cancellation token — response adds `running:true`; 404 no job, 409 already terminal) · `POST /jobs/{id}/retry` (404 no job, 409 wrong state) · `POST /jobs/retry` bulk (body `{status=failed\|cancelled, app?, limit≤500}` ⇒ `{retried,ids}`; 400 bad status) · `POST /jobs/{id}/reset` (re-queue a `running` job; 404 no job, 409 not running) · `GET /jobs/{id}/stream` (SSE) · `GET /jobs/{id}/costs` · `GET /jobs/{id}/receipt` (one run's cost + stage timings + what it changed; 404 no job) |
| Costs | `GET /costs?app=&since=` |
| Schedules | `GET /schedules?limit=&cursor=` (each row enriched with `next_run`, `last_job_id`/`last_status`, `last_skipped_at`/`skipped_count` and `health`; `last_run` is set only when a job was actually enqueued) · `POST /schedules` (`{app, cron, params?, priority?, timezone?, misfire_policy?, max_attempts?}` — `timezone` IANA/chrono-tz default UTC, `misfire_policy` `fire_once`\|`skip` default `fire_once`, `max_attempts` default server 3; unknown `timezone`/`misfire_policy` → 400; `params` shallow-merge over the app's `default_params` and the **merged** object is schema-validated → 422 with JSON-pointer paths, exactly like the enqueue door) · `DELETE /schedules/{id}` · `POST /schedules/{id}/enabled` |
| Datasets | `GET /datasets/{app}/{ds}?limit=&cursor=&filter=&trust=&removed=` (`trust` defaults `all`, `removed` defaults `exclude` — see below) · `GET .../export?format=json\|ndjson\|csv&filter=&trust=&removed=` (all stream; see below) · `GET .../duplicates?distance=` (413 above 10k records) · `GET .../changes?since=&limit=&cursor=&trust=` (defaults to `trust=stable`) · `GET .../history?key=&limit=&cursor=` |
| Watches | `GET /watches?app=&limit=&cursor=` (rows enriched with `last_delivery`, explicit `null` when never fired; unknown `app` → 400 naming the accepted namespaces) · `POST /watches` (`app` is the **namespace records land under** — registered apps plus the virtual/observed ones; unknown → 404, an `(app, dataset)` pair that could never fire → 400 naming where those records land) · `DELETE /watches/{id}` · `POST /watches/{id}/enabled` · `GET /watches/{id}/deliveries?status=&limit=&cursor=` (that watch's own delivery log; unknown id → 404). See [events-webhooks.md § Watchable namespaces](events-webhooks.md#watch-namespaces) |
| Webhook deliveries | `GET /webhooks/deliveries?status=&limit=&cursor=` · `GET /webhooks/deliveries/{id}` · `POST /webhooks/deliveries/{id}/replay` |
| Triggers | `GET /triggers?app=&limit=&cursor=` (filters `source_app`; unknown value → 400 — accepted set is the watch namespaces plus ingress source ids, `*`, and whatever is already stored) · `POST /triggers` · `DELETE /triggers/{id}` · `POST /triggers/{id}/enabled` · `POST /triggers/{id}/test?fire=` (with `fire=true`, resolved params that fail the target app's schema → 422; the live fire path records the same refusal as a `bad_params` decision) · `GET /triggers/{id}/runs` |
| Search | `GET /search?q=&limit=&app=&dataset=&fuzzy=` (the response carries an `index` block — `{enabled, doc_count, degraded, reason}`; `degraded: true` means an empty `hits` list is **not** evidence the records are missing, see [search.md § Query surface](search.md#query-surface)) · `DELETE /search/docs` · `DELETE /search/datasets/{app}/{ds}` |
| Derived datasets | `GET /derived?app=` · `POST /derived` (`{source_app, source_dataset, target_dataset, filters?, project?, lookup?, group_by?, aggregates?}`) · `GET /derived/{id}` · `DELETE /derived/{id}` · `POST /derived/{id}/enabled` · `POST /derived/{id}/backfill` (`{batch?, max_rows?, cursor?}` → `{scanned, matched, new, changed, unchanged, done, cursor?}`; **budgeted** — `done: false` means call again with the returned `cursor`, and an aggregate spec whose source exceeds `max_rows` answers 400 rather than writing a partial total). See [datasets.md](datasets.md#derived-datasets-derived) |
| Saved searches | `GET /searches?limit=&cursor=` · `POST /searches` · `DELETE /searches/{id}` · `POST /searches/{id}/enabled` |
| Events | `GET /events` (SSE all jobs; monotonic ids + `Last-Event-ID` resume — see [events-webhooks.md](events-webhooks.md)) |
| Hosts | `GET /hosts?limit=&cursor=` (learned tier memory + politeness per host) · `GET /hosts/{host}` (404 unknown) · `DELETE /hosts/{host}/memory` (reset strikes+pin+penalty; 404 unknown) |
| Profiles | `GET /profiles` (session vault: named login profiles; see below) |
| Plugins | `GET /plugins` · `POST /plugins/reload` |
| Extraction | `POST /extract/preview` (dry-run a RuleSet against one document; see below) |
| Grants | `GET /grants?status=&agency=&source=&closing_before=&closing_after=&min_award=&trust=&limit=&cursor=` · `GET /grants/closing-soon?days=` (see below) |
| Catalog | `GET /catalog/sources?market=&status=&category=` (the machine-readable data-source catalog) · `GET /catalog/health` (per-source freshness monitor; see below) |
| Source health | `GET /sources?state=&app=&limit=` · `GET /sources/{id}` (`id` = `<app>/<dataset>`) · `GET /sources/{id}/runs?limit=` · `POST /sources/{id}/state` (`{state, reason?}` — manual override; the only way out of quarantine). All `503` when `[resilience] enabled = false`. See below. |
| Provisioner proposals | `GET /provisioner/proposals?limit=&cursor=` (backlog of what `provisioner` compiled; see below) · `POST /provisioner/proposals/{key}/validate` (re-checks against a FRESH fetch) · `POST /provisioner/proposals/{key}/promote` (renders the paste-ready TOML fragment; writes nothing to the catalog file) |
| Store integrity | `GET /datasets/doctor?skip_artifacts=` (**read-only** audit; `findings` empty on a healthy store, each with its remediation — see [datasets.md § `datasets doctor`](datasets.md). Full scans; on-demand only) |
| Retention | `GET /retention/preview?days=` (**read-only dry run**: reclaimable artifact bytes per app, split reclaimable/pinned/cassette, plus ledger row counts and the configured windows — deletes nothing. See [datasets.md § Retention](datasets.md)) |
| Meta | `GET /openapi.json` (OpenAPI 3.1 spec for all routes) |

## Shutdown behaviour (what a client sees on Ctrl-C / `systemctl stop`)

Every SSE surface — `GET /events`, `GET /jobs/{id}/stream`, and the MCP live stream `GET /mcp` — **ends cleanly** when the process starts shutting down: the response body finishes normally at an event boundary, so a client sees an ordinary end-of-stream (and a complete final frame), never a connection reset or a truncated JSON-RPC message. `Last-Event-ID` resume is unaffected: reconnect against the restarted process with the last id you saw and the replay ring serves the gap, or answers `reset` if it is already too old.

In-flight non-streaming requests get a **10-second grace window** measured from the shutdown signal; anything still open after that is abandoned so the job drain and the host-politeness snapshot still run. A long `GET /datasets/{app}/{ds}/export` is therefore the one request that can be cut off by a stop — it ends without its clean terminator, which client libraries surface as a transfer error (see [Dataset export](#dataset-export--scan-limits)), never as a short 200. The grace window is a constant, not a config key; the knob operators tune is `[worker] shutdown_drain_secs` (how long an in-flight **job** gets), and the two windows run concurrently, so total stop time is the larger of them rather than their sum.

Conventions: enable/disable is always `POST …/{id}/enabled {"enabled": bool}`; every list endpoint is dual-mode — without `cursor=` it returns its legacy shape (bare array or `{watches|triggers|searches|changes|revisions|deliveries: [...]}`, unbounded except where a legacy `limit` already applied), and with `cursor=` present (even empty, for page 1) it returns `{items, next_cursor}` and pages by keyset. Cursors are opaque `<stored-ts>|<tiebreak>` tokens (`next_cursor` is `null` on the last page); pass the previous response's `next_cursor` back as `cursor=`. The `changes`/`history` feeds page the full revision set — the legacy no-cursor shapes still clamp at 1000/500 rows, but `cursor=` reaches everything past that. Details of each area live in the sibling feature docs.

## Dataset export & scan limits

`GET /datasets/{app}/{ds}/export` streams in all three formats — constant memory, no row cap — by walking the dataset in keyset-paged batches:
- `format=json` (default): a single streamed JSON **array** `[{record},…]` (`content-type: application/json`). This is a bare array, not the former `{app,dataset,count,records}` envelope — the count can't be known before streaming.
- `format=ndjson`: one JSON object per line (`application/x-ndjson`).
- `format=csv`: RFC-4180 rows under a fixed `key,first_seen,last_seen,updated_at,removed_at,data` header (`text/csv`).

All three send `content-disposition: attachment; filename="{ds}.{ext}"`. `trust=` (default `all`) and `removed=` (default `exclude`) apply to export exactly as they do to `GET /datasets/{app}/{ds}` (below) — an export is a complete copy *by default*, but an explicit `trust=`/`removed=` now actually narrows it instead of being silently ignored.

**Truncation is never silent.** If a mid-stream read from the store fails, the HTTP response ends without its clean terminator — for `json`, no closing `]` is written, so the body is not parseable JSON; for every format, the connection ends without the well-formed end of a chunked transfer, which HTTP client libraries surface as a transfer error rather than a 200 with a plausible-looking short body. The failure is logged at `error`. Separately, if an individual record fails to serialize (never observed in practice — a stored record's `data` is already a validated JSON value — but not assumed impossible), that row is skipped and counted, and the count is logged at `error` once the export otherwise completes; it is not silently dropped from the row total with no trace.

**Generic `filter` predicate.** Both `GET /datasets/{app}/{ds}` and `.../export` take a repeatable `filter` query param, all ANDed and pushed into SQL (so a filtered read/export never deserializes rows SQLite can skip, and a filtered export streams only matching rows instead of the whole corpus). Grammar per param — `<path>:<op>:<value>` where `path` is a JSON path (`$.state`) and `op` is one of:
- `eq` — exact text match · `contains` — case-insensitive substring · `gte`/`lte` — text (lexicographic) `>=`/`<=` · `numgte` — numeric `>=` on **any** of `path`'s comma-separated fields (an OR).

The value keeps any `:` after the op (so timestamps/URLs pass through). Example: `?filter=$.state:eq:CA&filter=$.employees:numgte:50`. A malformed spec (missing op/value, non-`$.` path, unknown op, non-numeric `numgte`) returns `400 bad_request`. This is the same `JsonFilter` engine the `/grants` route exposes with typed params — the `filter` grammar generalizes it to every app's datasets.

**`removed=include|exclude`, default `exclude`.** Tombstoned records (`removed_at` set) are left out of every read shape — default, cursor-paged, filtered, and export — unless `removed=include` is passed. This is a **behavior change**: previously the unfiltered page and its cursor form always included removed records, `?filter=` silently switched to excluding them, and `/export`'s formats disagreed with each other too. `trust=` (see [datasets.md § Trust](datasets.md#trust)) is likewise now honored identically across all four shapes — presence of `filter=` no longer changes what either param means.

`GET /datasets/{app}/{ds}/duplicates` runs an in-memory SimHash sweep (banded candidate lookup, exact-Hamming verified), so it is bounded: datasets over **10,000 records** return `413 too_large` (the message carries the actual count and the cap) rather than pinning a core. Narrow the dataset or run the scan offline. Banding only filters at small distances — above `distance=5` the scan degrades to the pairwise walk, which the 10k cap keeps bounded.

## RuleSet preview (`POST /extract/preview`)

Dry-run a declarative `RuleSet` against one document without enqueuing a job — the authoring loop for selectors. Body `{rules, html}` **or** `{rules, url}` (exactly one of `html`/`url`; both or neither → `400 bad_request`). `rules` is a bare `{field: rule}` map (same shape apps take).

Rules compile **field-by-field**, so a bad set returns `400 bad_request` with a per-field `fields: [{field, error}]` list naming **every** bad field (deserialize errors like an unknown rule `type`, and compile errors like a bad CSS selector / regex / XPath) — not just the first. A non-object `rules` is `400`.

This route carries the larger **8 MiB** request-body ceiling (see [Request body limits](#request-body-limits)) because `html` is a whole web page; a request body past 8 MiB is `413` before the handler runs.

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
| `min_award` | Keeps records whose **`award_ceiling` >= v OR `total_funding` >= v**. Sources report grant size inconsistently (a per-award ceiling vs. a program total), so matching either keeps the funder's largest published number in play. A record with both fields null never matches. Grants.gov's **Search2** API publishes no money at all (live-verified 2026-08-04: an `oppHits[]` entry carries only `id, number, title, agencyCode, agency, openDate, closeDate, oppStatus, docType, cfdaList`), so federal amounts do not come from the listing — they are joined in from the **`fetchOpportunity` detail corpus** (`grants/opportunity_details`), whose `synopsis` block does carry `awardFloor` / `awardCeiling` / `estimatedFunding`. A federal record therefore matches `min_award` **iff its detail record has been harvested and the agency actually published a figure**; the agency's literal `"none"` stays `Null` and never becomes a matching `0`. Coverage is the detail corpus, which the harvest fills incrementally (`harvestDetails`, **on by default since 2026-08-04**, see [apps.md](apps.md)) — an un-harvested or figure-less federal opportunity is honestly invisible to this filter. Because the harvest is delta-only, coverage grows **forward** from that date rather than retroactively: opportunities that never change are never re-fetched, so a corpus backfill remains a non-goal and federal `min_award` recall climbs day by day. |
| `trust` | `all` (default) \| `stable` \| `provisional` \| `quarantined` — the same vocabulary as `/datasets` and `/changes`, and `stable` keeps the `NULL`-means-stable equivalence. `grants/unified` is written by three independent sources, and **each run's contribution is stamped with that source's own extraction health**: a `degraded` source's rows land in the canonical dataset stamped `provisional`, a `quarantined` source's are diverted out to `grants/unified@q` and never appear here at all. `trust=stable` is how a consumer asks for only the rows we stand behind. Default is `all` because every returned record carries its own `trust` field, so the corpus view stays complete and the consumer decides. |

Dual-mode per the cursor convention: without `cursor=` ⇒ `{grants: [Record]}` capped at `limit` (default 50, **max 500**); with `cursor=` present (even empty) ⇒ `{items, next_cursor}`, keyset-paged by `<updated_at>|<key>` — the filters survive pagination, so walking the cursor recovers the complete filtered set past the 500 cap. Records are the standard dataset shape (`key`, `data`, `first_seen`, `last_seen`, `updated_at`, `removed_at`), newest-updated first.

### `GET /grants/closing-soon?days=`

Live **open** grants whose `close_date` falls within `days` of today, **soonest first**. `days` defaults to **14** and is clamped to **1..=365**. Returns `{days, count, grants}`, where each grant is its unified `data` object plus `key` and `days_left` (0 = closes today). `count` is the full window total; `grants` is **capped at 200**. Sorting is by `close_date` rather than the store's `updated_at` order, so the window is read up to an internal bound of 1000 rows before it is sorted and truncated.

This is **cross-source** — the pre-existing `closingSoon` digest in the grants-gov job artifact is federal-only and computed from raw API hits, so it never sees CA grants. It is **computed on read**, not materialized as a dataset: membership changes with the *calendar*, not with the data, so a snapshotted list would go stale between syncs even when nothing upstream changed. The corpus is small enough that a read view costs nothing to keep correct.

**Performance stance:** both routes filter with SQLite `json_extract` over the `data` column, i.e. a full scan of the `(app, dataset)` partition with no index on the filtered fields. That is the right trade at current scale (the unified corpus is in the low thousands) and it keeps the record store free of any coupling to an app's record shape — new filters need no migration. If the corpus grows to where the scan hurts, the escape hatch is a generated column over the hot field plus an index on it; the query builder would not have to change.

## Data-source catalog (`/catalog/sources`)

`GET /catalog/sources` serves the parsed `catalog/data-sources.toml` — the machine-readable list of every data pipeline this service scrapes (id, app, market, name, url, category, engine, access, cadence, cron, status, confidence, dataset, notes). Optional `?market=`/`?status=`/`?category=` filters narrow it (e.g. `?market=eu&status=live`). Returns `{count, sources: [Source]}`. This lets a downstream app query "which markets are launch-grade" over HTTP instead of scraping a TOML out of a sibling repo.

The catalog is no longer inert prose: a server-crate test cross-checks it against the live app registry — every `status = "live"` entry must name a registered app whose `schedule()` **equals** the entry's `cron` (both directions), and every registered in-scope source app must appear (a documented exempt-list covers generic tooling/engines, the `hackernews` example, and sibling-product consumers like the Ledgerline trades / Counterbill apps, mirroring the `census-*` precedent). So the catalog can't silently drift from what the apps actually do.

`GET /catalog/health` turns the catalog's `cadence`/`dataset`/`status` fields into a **freshness monitor** — the "how fresh" question the catalog previously asserted but couldn't verify. For each `status = "live"` source that declares a `dataset` and a cadence with a freshness expectation, it reads the newest `updated_at` in that dataset (scoped to the source's `app`) and reports `{last_write_at, age_secs, stale}` — `stale` when the write is older than the cadence window (`daily`→24h … `annual`→366d) times a 2× grace (tolerating one missed run). A live source that has never written its dataset is stale by definition; sources with no dataset or a no-expectation cadence (`on-demand`/`one-time`) are returned with `monitored:false`. Top-level `{checked, stale}` counts. So a silently-broken pipeline (jobs succeeding but writing nothing, or a source quietly delisted) becomes visible instead of leaving the catalog asserting `live`/`confidence:5` forever.

## Source health (`/sources`)

The other half of source liveness. `/catalog/health` answers *did this source run recently*; `/sources` answers *was what it produced right*. Neither subsumes the other, so each response links to the other. Full design and signal reference: [resilient-extraction.md](resilient-extraction.md).

- `GET /sources?state=&app=&limit=` — `{enabled, enforcing, count, sources}`, worst degradation score first. **`enforcing: false` means verdicts are recorded and nothing is gated** — the shipping default.
- `GET /sources/{id}` — one source in full: state, the last 10 runs with the tests behind each verdict, this run's per-field sketch next to its baseline (`miss_rate`, `coercion_failure_rate`, `distinct_ratio`, `mean_len` vs baseline medians), the mined invariants, and `statistical_coverage` (`false` = the source never reaches the cohort floor, so only the assumption-free rules watch it). `id` is `<app>/<dataset>`, e.g. `/sources/extractor/products`.
- `GET /sources/{id}/runs?limit=` — verdict history. Each run carries `reasons`: every test that ran with its value and threshold, so a verdict explains itself without re-running anything.
- `POST /sources/{id}/state` `{state, reason?}` — manual override, and the **only** way out of `quarantined`. Quarantine is deliberately terminal without an operator: a stuck source is an acceptable outcome, a source that silently un-quarantines itself and resumes pushing garbage downstream is not. An unrecognized state is `400`.

All four return `503` when `[resilience] enabled = false` — a health question asked of a disabled detector has no honest answer, and an empty list would read as "everything is fine".

## Provisioner proposal lifecycle (`/provisioner/proposals`)

The `provisioner` app (see [apps.md](apps.md)) compiles a prompt into a proposal record in `provisioner/proposals` and stops — it never writes `catalog/data-sources.toml` and never creates a schedule. This surface is the human-facing lifecycle **over** those records; it does not relax that invariant. `POST .../promote` still only ever *returns* a TOML fragment — the human still completes [ONBOARDING.md](../../ONBOARDING.md) Path B (write the app crate, register it, hand-paste the `[[source]]` entry).

Every proposal record carries a lifecycle `status`, distinct from its frozen compile-time `verdict`/`accepted`:

```
planned -> validated | failed -> promoted
```

`planned` is where every proposal starts, whatever its compile-time verdict — a rejected draft is still emitted (the misses are the useful part) and still starts `planned`; `may_promote` is what actually blocks promoting one that never demonstrated it binds anything, not the status value alone.

- **`GET /provisioner/proposals?limit=&cursor=`** — dual-mode list (bare array, or `{items, next_cursor}` with `cursor=`), most-recently-touched first. Each summary: `key, prompt, status, expired, verdict, accepted, catalog_confidence, engine, url, intended_dataset, age_secs`. `expired` is computed **at read time** (never stamped onto the record) against `[provisioner] proposal_max_age_secs` (default 30 days, `0` disables) — and only ever `true` for a still-`planned` proposal; one already `validated`/`promoted` had its review, and a `failed` one has its own loud signal.
- **`POST /provisioner/proposals/{key}/validate`** — re-runs the proposal's drafted `RuleSet` against a **freshly fetched** sample of its primary URL (same `Auto`/`to_markdown`/`use_recipes` shape as the original compile's sampling stage, but deliberately never satisfied by an archive snapshot — validation exists to catch drift the compile could not have seen). Sets `status` to `validated` or `failed` and stores a `validation: {checked_at, sample, dry_run, accepted}` block (same shapes as the compile-time `samples[]`/`sample_stats`). `404` unknown key; `400` when the stored `rule_set` no longer parses, the `catalog_row` has no `url`, the fetch fails, or it yields no sampleable body.
- **`POST /provisioner/proposals/{key}/promote`** — server-renders the paste-ready `[[source]]` TOML fragment from the stored `catalog_row` (the same renderer the compile itself used, so the two can never drift) and marks `status: "promoted"`. `404` unknown key; `409` when the proposal's best available evidence says its rule set does not bind (a `failed` re-validation, or a never-validated `planned` proposal whose compile-time `accepted` was `false`) — promoting it would hand a reviewer a fragment for a draft already known not to work; `400` when the stored `catalog_row` no longer parses.

Every status transition is an ordinary dataset upsert, so it lands as a new revision — `GET /datasets/provisioner/proposals/history?key=<key>` shows the full planned → validated → promoted (or → failed) trail, and the generic dataset surface (`GET /datasets/provisioner/proposals`) still reaches the full record (compiled `rule_set`, `samples`, `sample_stats`) this list intentionally leaves out.

## Host profiles (`/hosts`)

Diagnostics over the tiered fetcher's learned per-host state (see [fetching.md](fetching.md)). Each host object: `host`, `preferred_tier` (`"browser"` when pinned, else `null`), `http_strikes`, `penalty_ms` (the **live** governor politeness penalty in ms — the stored snapshot is only for boot restore), `updated_at` (last tier-outcome change), `penalty_updated_at` (last penalty snapshot, or `null`).

- `GET /hosts` — dual-mode list, most-recently-active first: no `cursor=` ⇒ `{hosts: [...]}`; `cursor=` present ⇒ `{items, next_cursor}` keyset-paged by `<updated_at>|<host>`.
- `GET /hosts/{host}` — one host's profile; `404 not_found` when the host has no learned state. A host with only a live (not-yet-snapshotted) penalty is still returned.
- `DELETE /hosts/{host}/memory` — resets the host: drops strikes + browser pin + persisted penalty **and** clears the live governor penalty; `{host, reset: true}` on success, `404 not_found` when unknown.

## Session profiles (`/profiles`)

Read-only view of the session vault — the named login profiles a fetch can run under (`profile` on `FetchRequest`/`HttpRequest`/`RenderRequest`; full semantics in [fetching.md](fetching.md)).

- `GET /profiles` — `{profiles: [{name, has_cookies, has_browser_dir, last_used}]}`, alphabetical by `name`. `has_cookies` = a persistent HTTP jar (`cookies.json`) exists; `has_browser_dir` = a Chrome user-data-dir (`browser/`) exists; `last_used` = newest mtime across the profile dir and those two artifacts (RFC 3339, `null` if unreadable). An absent vault dir returns an **empty list, not an error** — it is created by the first profiled fetch. Entries whose names aren't valid profiles (or aren't directories) are ignored.

Profiles are created implicitly by the first fetch that names them; there is **no create/delete API** in phase 1 (delete = remove the directory under `[fetcher] profiles_dir`, default `data/profiles`). A request naming an invalid profile fails with a typed profile error, surfaced as `400 bad_request` at the API boundary (names are validated in the engines, not at the route, but the *cause* is the caller's parameter — it was previously reported as `500 internal`, which told the caller to file a bug about their own typo). The message names the `profile` parameter rather than the profile directory it failed to open.

## Smoke verification (`just smoke`)

Every other check in this repo (`just test`, `just lint`) verifies code paths in isolation, never the shipped binary answering over real HTTP. `scripts/smoke.ps1` (PowerShell 7, `just smoke`) closes that gap: it builds/locates `pumper` (reusing an existing debug build — set `CARGO_TARGET_DIR` to point it at one), boots it against a scratch `config.toml` (isolated DB/artifacts/search-index dir under the OS temp folder, port `18099` so it never collides with a `just run` on 8088), polls `GET /health` for readiness, drives one real job end-to-end through `POST /apps/hackernews/jobs` → `GET /jobs/{id}`, then curls `GET /health`, `GET /datasets/doctor`, `GET /retention/preview`, `GET /enforcement/preview`, `GET /openapi.json`, and the driven job's `GET /jobs/{id}/receipt`, asserting `200` + a sane JSON shape on each. It always tears down (kills the server process, deletes the scratch dir) in a `finally` block, and prints a PASS/FAIL/SKIP line per check — a network-unreachable job run is `SKIP`, not `FAIL`, since the point is proving the server works, not the network. Exits non-zero on any `FAIL`.

`hackernews` is the driven app because it's the only registered app whose `run()` needs no API key, browser profile, or paid engine — see `scripts/smoke.ps1`'s header comment for why it's excluded from `catalog/data-sources.toml` (example/template app) yet is exactly the right smoke-test candidate.

## Known gaps

No bundled Swagger/Scalar UI — the raw spec is served at `/openapi.json`; point any external viewer at it.
