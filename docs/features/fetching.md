# Tiered fetching & engines

One `Fetcher` escalates across three engines by cost: **http → browser → claude**, climbing only when the result looks insufficient — too little content (`[fetcher] min_content_chars`, default 250; per-request `min_content_chars` overrides) **or a bot-wall / challenge page**.

## FetchRequest / FetchOutcome

`FetchRequest`: `url`, `strategy` (`http | browser | auto | auto_with_research`), `wait_for_selector`, `min_content_chars`, `research_prompt`, `max_budget_usd` (Claude tier ceiling), `skip_http` (set by the tier router), `to_markdown`, `no_cache` (bypass the HTTP cache — always hit the network), `ttl_override` (per-fetch cache TTL in seconds; caps staleness without a full bypass), `profile` (named login profile, threaded to both tiers — see [Session vault](#session-vault-named-login-profiles)). `FetchOutcome`: winning `engine`, status, html/markdown/text, `escalations` trail (one line per tier rejection + router/budget notes), structured `trace` (see below), `cost_usd` (Claude tier actual).

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
- **Cache revalidation.** When a cacheable GET misses because its entry **expired** (but the caller isn't running its own conditional GET), the engine reads the stale entry's stored `ETag` / `Last-Modified` and re-sends as a conditional GET instead of re-downloading the whole body. A `304` **refreshes** the entry's TTL in place (no body rewrite; `created_at` moves forward so the `max_age` read-staleness cap still measures from the last confirmed fetch) and serves the stored body as a `cache_hit`; a `200` stores and returns the changed body. This turns the `watch`/poll workload's common "unchanged page past its TTL" case from a full body transfer + parse into a few-hundred-byte round trip. The caller-owns-the-validator path (crawler revisit) is untouched — it still gets the raw `304`.

#### HTTP request controls (body cap, timeout, retry policy)

- **Body size cap.** The response body is read in streamed chunks and aborted the instant the cumulative size would exceed the cap — one huge/hostile URL can't balloon memory. Over-limit yields a typed `Error::Http` naming the cap and URL. Cap = `HttpRequest.max_body_bytes` (per-request `Option<u64>`) else `[http] max_body_bytes` (default **16 MiB** — comfortably above the largest real pages we fetch, e.g. SEDIA clean-text / census blobs in the low single-digit MiB). Bodies are decoded lossily as UTF-8 (charset-from-header detection is not performed).
- **Per-request timeout.** `HttpRequest.timeout_secs` (`Option<u64>`) overrides the client-global `[http] timeout_secs` for that request, applied per attempt.
- **Retry policy.** Retryable statuses are configurable via `[http] retryable_statuses` (default `[429, 502, 503, 504]`); the redirect-follow limit is `[http] redirect_limit` (default 10). The retry sleep is `max(exponential backoff, server Retry-After) + jitter`: backoff is `500ms · 2^(attempt-1)`, a `Retry-After` (both delta-seconds and HTTP-date forms) on the prior response raises the floor, and up to 25% deterministic hash-based jitter (seeded from URL+attempt, no `rand` dep) de-syncs retry bursts. The governor still learns from `Retry-After` on 429/503 as before.

#### Proxy support

- **HTTP tier.** `[http] proxy` (`Option<String>`) routes all HTTP requests through an `http`/`https`/`socks5` proxy, applied at client-build time via `reqwest::Proxy::all`. Auth in the URL (`http://user:pass@host:port`) is honored; socks5 support comes from reqwest's `socks` feature. Per-request `HttpRequest.proxy` overrides it. Because reqwest binds a proxy at client-build time, a per-request override is served from a small **bounded client pool** keyed by proxy URL (LRU, ≤8 cached clients, oldest evicted). Costs: each pooled client carries its **own cookie jar** (proxied requests don't share cookies with the default client), and up to 8 idle keep-alive pools may linger. An override equal to the configured `[http] proxy` reuses the base client (no duplicate). An invalid proxy URL surfaces a typed `Error::Http`.
- **Browser tier.** `[browser] proxy` (`Option<String>`) is passed to Chrome as `--proxy-server`. When unset it **falls back to `[http] proxy`** at config load (`Config::normalize`), so a single `[http] proxy` knob usually serves both engines; an explicit `[browser] proxy` wins. Note: Chrome's `--proxy-server` does not accept `user:pass@` auth in the URL (an authenticated proxy prompts interactively), so browser-tier proxy auth is unsupported.

- **browser** (`engine-browser`): headless Chrome render (chromiumoxide/CDP), `wait_for_selector`. One shared Chrome instance behind a relaunchable holder — details below.

#### Browser engine: resilience, concurrency & cheap renders

A single Chrome instance is shared across renders (persistent `[browser] user_data_dir`, so logins/cookies survive restarts). It is managed by a relaunchable holder:

- **Relaunch on crash.** A background task drives the CDP handler loop and flips a liveness flag when Chrome's connection ends (crash or exit). The next render's acquire sees the dead flag and relaunches — a crash no longer wedges every future render until a server restart.
- **Periodic recycle.** After `[browser] recycle_after_renders` renders (default 200; `0` disables) the holder relaunches on the next acquire to shed accumulated memory / leaked tabs. Crash-relaunch stays active regardless.
- **Coalesced relaunch.** A crash/recycle/cold-start seen by several concurrent renders launches Chrome **once**, not once per caller: relaunches are serialized per profile by a launch gate, so the 2nd..Nth caller awaits the winner's launch instead of racing its own Chrome against the same `--user-data-dir` (Chromium enforces a single-instance lock there). The stale holder is dropped *before* the relaunch so the outgoing Chrome frees that lock first (in-flight renders keep their own handle), and the launch runs off the holders lock under a timeout (15s) so one slow start can't stall other profiles.
- **Render concurrency cap.** `[browser] max_concurrent_renders` (default 4; `0` = unlimited) is a semaphore bounding simultaneous tabs, so N concurrent callers can't spawn N unbounded tabs.
- **Resource blocking.** `[browser] block_resources` (default true) enables CDP request interception that drops **images, fonts, and media** (never stylesheets — CSS can matter for layout and selector waits) so scraping renders download only what the DOM needs. Enabling interception also disables Chrome's HTTP cache (cookies persist separately via the profile). Per-request `RenderRequest.load_all_resources` (serde-default `false`) opts a single render back into loading everything. When `block_resources` is false, interception is not wired at all (zero overhead).
- **Memory guards.** Launch args include `--disable-dev-shm-usage` (avoid tiny `/dev/shm` crashing Chrome) and `--js-flags=--max-old-space-size=512` (cap the V8 heap at 512 MB).

**`RenderRequest`** fields: `url`, `wait_for_selector`, `actions` (scripted page interactions — see below), `extra_wait_ms` (settle time; falls back to `[browser] default_wait_ms`), `evaluate` (JS expression; JSON result lands in `RenderedPage.evaluated`), `load_all_resources`, `profile` (session vault — see below).

- **Scripted page actions.** `actions` (also on `FetchRequest`, serde-default empty = one-shot render) drives the pages the browser tier exists for but a single render can't reach — infinite-scroll, "load more" buttons, lazy-loaded listings. Run in order **after the settle wait and before `evaluate`**, under a total-time budget of one `nav_timeout_secs` so a loop can't run forever. Action types (`{"action": …}`): `scroll_bottom`, `scroll_by {pixels}`, `click {selector}`, `type {selector, text}`, `wait_for_selector {selector, timeout_ms?}`, `wait_ms {ms}`, and `repeat {times, steps[], until_selector_count_stable?}` — the scroll-until-exhausted loop, which stops early once the tracked selector's match count stops growing. Each step is best-effort (a missing selector is logged and skipped, never aborting the render). `RenderedPage.actions_completed` reports how many top-level actions ran, so a truncated listing is visible rather than silent.

**`RenderedPage`** fields: `html`, `final_url`, `evaluated`, plus honest wait/cost signals — `nav_timed_out: bool` (the navigation-wait deadline elapsed and the DOM was captured mid-load, so HTML may be partial), `selector_found: Option<bool>` (`Some(true)`/`Some(false)` for a requested `wait_for_selector` that did/didn't appear before the deadline; `None` when none was requested), `blocked_resources: usize` (count of subresources dropped by interception this render). All three are serde-defaulted.

Config keys (`[browser]`): `chrome_executable`, `headless` (true), `user_data_dir` (`data/browser-profile`), `default_wait_ms` (1000), `nav_timeout_secs` (30), `max_concurrent_renders` (4), `block_resources` (true), `recycle_after_renders` (200), `proxy` (none; falls back to `[http] proxy`).

## Session vault: named login profiles

A **profile** is a named, persistent identity a fetch runs under. Without one, HTTP cookies live in reqwest's in-memory jar and **die with the process**, and the browser has exactly one unnamed profile (`[browser] user_data_dir`) — so there is no way to hold several logins, or to pick one per request. A profile gives both tiers a persistent, isolated session.

Set `profile: "<name>"` on `FetchRequest` (threaded to **both** tiers), or directly on `HttpRequest` / `RenderRequest`. All three are serde-defaulted: **`None` = exactly the previous behavior.**

**On-disk layout** — created on first use, under `[fetcher] profiles_dir` (default `data/profiles`):

```
data/profiles/<name>/cookies.json   persistent HTTP cookie jar   (http tier)
data/profiles/<name>/browser/       Chrome user-data-dir         (browser tier)
```

Names are validated **path-safe**: 1–64 chars of ASCII letters, digits, `-`, `_`. Anything else (separators, `.`/`..`, spaces, non-ASCII) is a typed `Error::Profile` raised *before* any path is built, so a name can never escape `profiles_dir`.

**HTTP tier.** A profiled request is served by a client whose `cookie_provider` is that profile's jar — loaded from `cookies.json` on first use and written back **atomically** (tmp + rename). Write-back is a trailing-edge debounce: a response marks the jar dirty and a single flusher task writes it ≤1s later (so the last response of a burst is always persisted, while a profiled crawl writes at most once per second per profile). **Crash-loss window: a hard kill within ~1s of a `Set-Cookie` loses that cookie on disk** (it was still applied in-process). The jar keeps **session** cookies (no `Expires`/`Max-Age`) — that is the whole point of a login vault — and drops expired ones at load. A corrupt jar is warned about and starts empty rather than wedging fetches.

Clients are still pooled, not duplicated: the existing bounded LRU pool's key is generalized from `proxy` to the **`(proxy, profile)`** pair a client is *built* with (≤8 clients, oldest evicted). Evicting a client never loses cookies — jars are owned by the engine's jar map, keyed by name and not evicted.

**Profiled requests bypass the shared `http_cache`.** Its key is method+url+body only, so caching a logged-in body would serve it to anonymous callers (and vice versa). Profiled fetches always hit the network.

**Browser tier.** Chromium binds `--user-data-dir` at launch, so one Chrome = one profile. A profiled render therefore selects among a **small map of relaunchable holders keyed by profile** (`None` = the shared default instance), each with the full crash-relaunch + recycle logic. At most **4 Chromes** are live at once; the least-recently-used holder is closed (dropped, which reaps its Chrome) when a new profile pushes past the cap. The alternative — one holder relaunching on every profile switch — was rejected because interleaved profiles (the normal case for a queue serving several logins) would thrash Chrome on every request; the cost of the map is up to 4 resident Chromes, bounded by the LRU. The render-concurrency semaphore is shared across profiles.

Existing profiles are listable via `GET /profiles` (name, `has_cookies`, `has_browser_dir`, `last_used`) — see [http-api.md](http-api.md). Profiles are created implicitly by the first fetch that names them; there is no create/delete API.

**Phase 1 scope.** The vault stores *session state* only. There is **no credential management and no encryption at rest**: `cookies.json` and the Chrome profile dir are plaintext on disk, exactly as readable as any other file in `data/`. Logging in is still manual (e.g. run once with `[browser] headless = false` under a profile, or drive a login POST on the HTTP tier); nothing logs in for you.

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

Config keys (`[fetcher]`): `min_content_chars` (250), `host_memory_ttl_secs` (604800), `host_penalty_persist_secs` (60), `profiles_dir` (`data/profiles` — root of the session vault).

## Known gaps

- Single static proxy per tier (`[http] proxy` / `[browser] proxy`, per-request override on the HTTP tier). No proxy **pool / rotation** and no stealth tier (backlog moonshots). Browser-tier proxy auth (`user:pass@`) is unsupported (Chrome `--proxy-server` limitation).
- Aging is time-based only; there is no success-rate / half-life model of host reliability.
- **Session vault (phase 1):** session state only — no credential management, no encryption at rest, no login automation. No create/delete/import API for profiles (they appear when first used; delete = remove the directory). Profiled fetches never use the response cache, and cookies set within ~1s of a hard kill aren't on disk. The HTTP jar and the browser profile are separate stores — a login in one is not visible to the other.
