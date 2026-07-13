# Tiered fetching & engines

One `Fetcher` escalates across three engines by cost: **http → browser → claude**, climbing only when the result looks insufficient — too little content (`[fetcher] min_content_chars`, default 250; per-request `min_content_chars` overrides) **or a bot-wall / challenge page**.

## FetchRequest / FetchOutcome

`FetchRequest`: `url`, `strategy` (`http | browser | auto | auto_with_research`), `wait_for_selector`, `min_content_chars`, `research_prompt`, `max_budget_usd` (Claude tier ceiling), `skip_http` (set by the tier router), `to_markdown`, `no_cache` (bypass the HTTP cache — always hit the network), `ttl_override` (per-fetch cache TTL in seconds; caps staleness without a full bypass). `FetchOutcome`: winning `engine`, status, html/markdown/text, `escalations` trail (one line per tier rejection + router/budget notes), structured `trace` (see below), `cost_usd` (Claude tier actual).

Always prefer the metered **`AppContext::fetch`** over `ctx.engines.fetch` — it adds cost attribution, budget governance, and tier routing.

### Structured fetch trace

`FetchOutcome.trace` is a typed, serde-serializable list — **one entry per attempted tier, including the winner** — so consumers branch on *why* a fetch escalated (or the cache/latency/cost of each tier) instead of string-matching the `escalations` prose. The human-readable `escalations` lines are still populated (kept alongside, not replaced), and cost-event `detail` still embeds them.

Each `TierTrace` entry:

| field | type | notes |
| --- | --- | --- |
| `tier` | `http \| browser \| claude` | matches the winning `engine` string |
| `verdict` | enum: `ok \| thin \| blocked \| error \| skipped_by_router` | `ok` = this tier produced the result; `thin`/`blocked`/`error` = escalated; `skipped_by_router` = never attempted (learned `skip_http`, or Claude dropped because the job budget is spent) |
| `http_status` | `u16?` | http tier only; omitted elsewhere |
| `content_chars` | `usize?` | extracted-text length when measured (escalation decisions + the Claude answer); omitted when not counted |
| `cache_hit` | `bool?` | http tier only: served from the `http_cache` vs the network |
| `latency_ms` | `u64` | wall-clock time for this tier; `0` for a `skipped_by_router` entry |
| `cost_usd` | `f64?` | Claude tier only |
| `detail` | `string?` | short reason (challenge marker, error text, skip cause); omitted when the tier + verdict already say everything (e.g. a thin http tier) |

Optional fields (`http_status`, `content_chars`, `cache_hit`, `cost_usd`, `detail`) are omitted from JSON when absent; `tier`, `verdict`, and `latency_ms` are always present. The learned tier router keys on the http tier's **`verdict`** (`thin`/`blocked`/`error` = an HTTP loss) rather than the trail wording.

## Engines

- **http** (`engine-http`): reqwest + cookie jar, retries w/ backoff, fronted by the content-addressed TTL `http_cache` (GET-only; `HttpRequest.no_cache` bypasses) and the governor. **Conditional GET:** `HttpRequest.etag` / `HttpRequest.if_modified_since` (serde-defaulted) are sent as `If-None-Match` / `If-Modified-Since` (explicit `headers` still win); a `304 Not Modified` is passed through with its status intact and is **never** written to the cache over the prior full response (powers the crawler's revisit mode — [crawling.md](crawling.md)).

#### HTTP request controls (body cap, timeout, retry policy)

- **Body size cap.** The response body is read in streamed chunks and aborted the instant the cumulative size would exceed the cap — one huge/hostile URL can't balloon memory. Over-limit yields a typed `Error::Http` naming the cap and URL. Cap = `HttpRequest.max_body_bytes` (per-request `Option<u64>`) else `[http] max_body_bytes` (default **16 MiB** — comfortably above the largest real pages we fetch, e.g. SEDIA clean-text / census blobs in the low single-digit MiB). Bodies are decoded lossily as UTF-8 (charset-from-header detection is not performed).
- **Per-request timeout.** `HttpRequest.timeout_secs` (`Option<u64>`) overrides the client-global `[http] timeout_secs` for that request, applied per attempt.
- **Retry policy.** Retryable statuses are configurable via `[http] retryable_statuses` (default `[429, 502, 503, 504]`); the redirect-follow limit is `[http] redirect_limit` (default 10). The retry sleep is `max(exponential backoff, server Retry-After) + jitter`: backoff is `500ms · 2^(attempt-1)`, a `Retry-After` (both delta-seconds and HTTP-date forms) on the prior response raises the floor, and up to 25% deterministic hash-based jitter (seeded from URL+attempt, no `rand` dep) de-syncs retry bursts. The governor still learns from `Retry-After` on 429/503 as before.

- **browser** (`engine-browser`): headless Chrome render (chromiumoxide/CDP), `wait_for_selector`. One shared Chrome instance behind a relaunchable holder — details below.

#### Browser engine: resilience, concurrency & cheap renders

A single Chrome instance is shared across renders (persistent `[browser] user_data_dir`, so logins/cookies survive restarts). It is managed by a relaunchable holder:

- **Relaunch on crash.** A background task drives the CDP handler loop and flips a liveness flag when Chrome's connection ends (crash or exit). The next render's acquire sees the dead flag and relaunches — a crash no longer wedges every future render until a server restart.
- **Periodic recycle.** After `[browser] recycle_after_renders` renders (default 200; `0` disables) the holder relaunches on the next acquire to shed accumulated memory / leaked tabs. Crash-relaunch stays active regardless.
- **Render concurrency cap.** `[browser] max_concurrent_renders` (default 4; `0` = unlimited) is a semaphore bounding simultaneous tabs, so N concurrent callers can't spawn N unbounded tabs.
- **Resource blocking.** `[browser] block_resources` (default true) enables CDP request interception that drops **images, fonts, and media** (never stylesheets — CSS can matter for layout and selector waits) so scraping renders download only what the DOM needs. Enabling interception also disables Chrome's HTTP cache (cookies persist separately via the profile). Per-request `RenderRequest.load_all_resources` (serde-default `false`) opts a single render back into loading everything. When `block_resources` is false, interception is not wired at all (zero overhead).
- **Memory guards.** Launch args include `--disable-dev-shm-usage` (avoid tiny `/dev/shm` crashing Chrome) and `--js-flags=--max-old-space-size=512` (cap the V8 heap at 512 MB).

**`RenderRequest`** fields: `url`, `wait_for_selector`, `extra_wait_ms` (settle time; falls back to `[browser] default_wait_ms`), `evaluate` (JS expression; JSON result lands in `RenderedPage.evaluated`), `load_all_resources`.

**`RenderedPage`** fields: `html`, `final_url`, `evaluated`, plus honest wait/cost signals — `nav_timed_out: bool` (the navigation-wait deadline elapsed and the DOM was captured mid-load, so HTML may be partial), `selector_found: Option<bool>` (`Some(true)`/`Some(false)` for a requested `wait_for_selector` that did/didn't appear before the deadline; `None` when none was requested), `blocked_resources: usize` (count of subresources dropped by interception this render). All three are serde-defaulted.

Config keys (`[browser]`): `chrome_executable`, `headless` (true), `user_data_dir` (`data/browser-profile`), `default_wait_ms` (1000), `nav_timeout_secs` (30), `max_concurrent_renders` (4), `block_resources` (true), `recycle_after_renders` (200).

### Honest tier verdicts (bot-wall detection)

A tier no longer passes purely on char count. On escalating strategies (`auto`, `auto_with_research`) the HTTP tier escalates instead of returning content when the response is a bot-wall: a challenge/block **status** (403/429/503) or a conservative **challenge-page marker** in the body's leading window (Cloudflare "checking your browser" / "just a moment" / `cf-browser-verification`, "enable javascript", captcha, "verify you are human", "ddos protection by"). The browser tier applies the same marker heuristic before handing off to Claude (it has no HTTP status). Blocked tiers add a `... blocked: <reason>` line to the `escalations` trail. The explicit `http` / `browser` strategies still return the body as-is for the caller to inspect.
- **claude** (`engine-claude`): Claude Code CLI as a research engine — roles from `[claude.roles]` (model/effort/budget presets), `json_schema` constrained output, `resume_session`, reports `total_cost_usd`. Cached via the research cache (see [runtime.md](runtime.md)).

## Politeness governor (adaptive)

Per-host token bucket: configured spacing (`[governor] default_rps`, `per_domain`, jitter) **plus a learned penalty**: a 429/503 doubles the host's extra spacing and pushes the host's next slot out; only a genuinely healthy **2xx** response halves it (a 4xx like 404/403 is not health and no longer rewards faster spacing; other 5xx stay neutral). Penalty bounds are configurable — `[governor] penalty_base_secs` (default 1), `penalty_cap_secs` (300), `penalty_floor_ms` (100, below which a decaying penalty is dropped). Both `Retry-After` forms are honored: delta-seconds and an HTTP-date (converted to a delay from now); a larger `Retry-After` wins over doubling. State is held in one sharded map keyed by host, so distinct hosts never contend; idle hosts are evicted once the map outgrows its cap. Learned penalties are **persisted** (see host profiles below) so they survive a restart.

## Self-learning tier router (host profiles)

`tier_memory` table (a.k.a. host profiles): an HTTP-tier loss (the http tier's structured `verdict` is `thin`/`blocked`/`error` while a higher tier won) adds a strike per host; **3 consecutive strikes** flip the host to start at the browser tier (`skip_http`, noted in the trail). One HTTP win resets. Explicit `Http` strategy always overrides. Learning happens at the `AppContext::fetch` seam — engines stay stateless.

**Aging (v2).** Strikes and the browser pin decay after `[fetcher] host_memory_ttl_secs` (default 7 days; `0` disables aging). A host whose last strike is older than the TTL reads back as unpinned, so it gets a fresh crack at the cheap HTTP tier instead of staying pinned until a lucky win — and a single fresh loss after aging out does **not** immediately re-pin (stale strikes reset to one). Aging is applied lazily on read via the `updated_at` timestamp; no sweep job.

**Penalty persistence (v2).** The governor's learned per-host penalty is written behind into the host-profile row (`penalty_ms`, `penalty_updated_at`) every `[fetcher] host_penalty_persist_secs` (default 60s; `0` disables) and restored into the in-memory governor on boot. The snapshot deliberately never touches `updated_at`, so persisting a penalty doesn't reset strike aging. Only the last non-zero penalty is kept; it re-decays on the next healthy response.

Learned host state is inspectable and resettable via the `/hosts` API (see [http-api.md](http-api.md)): `GET /hosts` (paginated), `GET /hosts/{host}`, `DELETE /hosts/{host}/memory` (clears strikes, pin, and the live + persisted penalty). The `penalty_ms` reported by those endpoints is the **live** governor value (the row's stored snapshot is only for boot restore).

Config keys (`[fetcher]`): `min_content_chars` (250), `host_memory_ttl_secs` (604800), `host_penalty_persist_secs` (60).

## Known gaps

- No proxy pool / stealth tier (backlog moonshots).
- Aging is time-based only; there is no success-rate / half-life model of host reliability.
