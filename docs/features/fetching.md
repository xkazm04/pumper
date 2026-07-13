# Tiered fetching & engines

One `Fetcher` escalates across three engines by cost: **http → browser → claude**, climbing only when the result looks insufficient (`min_content_chars`, default 250).

## FetchRequest / FetchOutcome

`FetchRequest`: `url`, `strategy` (`http | browser | auto | auto_with_research`), `wait_for_selector`, `min_content_chars`, `research_prompt`, `max_budget_usd` (Claude tier ceiling), `skip_http` (set by the tier router), `to_markdown`, `no_cache` (bypass the HTTP cache — always hit the network), `ttl_override` (per-fetch cache TTL in seconds; caps staleness without a full bypass). `FetchOutcome`: winning `engine`, status, html/markdown/text, `escalations` trail (one line per tier rejection + router/budget notes), `cost_usd` (Claude tier actual).

Always prefer the metered **`AppContext::fetch`** over `ctx.engines.fetch` — it adds cost attribution, budget governance, and tier routing.

## Engines

- **http** (`engine-http`): reqwest + cookie jar, retries w/ backoff (`RETRYABLE_STATUS` 429/502/503/504), fronted by the content-addressed TTL `http_cache` (GET-only; `HttpRequest.no_cache` bypasses) and the governor.
- **browser** (`engine-browser`): headless render, `wait_for_selector`.
- **claude** (`engine-claude`): Claude Code CLI as a research engine — roles from `[claude.roles]` (model/effort/budget presets), `json_schema` constrained output, `resume_session`, reports `total_cost_usd`. Cached via the research cache (see [runtime.md](runtime.md)).

## Politeness governor (adaptive)

Per-host token bucket: configured spacing (`[governor] default_rps`, `per_domain`, jitter) **plus a learned penalty**: a 429/503 doubles the host's extra spacing (1s base, honors a larger `Retry-After`, 5-min cap) and pushes the host's next slot out; any healthy response halves it (dropped below 100ms floor). State is held in one sharded map keyed by host, so distinct hosts never contend; idle hosts are evicted once the map outgrows its cap. In-memory — resets on restart by design.

## Self-learning tier router

`tier_memory` table (persistent): an HTTP-tier loss (trail shows http failed/thin while a higher tier won) adds a strike per host; **3 consecutive strikes** flip the host to start at the browser tier (`skip_http`, noted in the trail). One HTTP win resets. Explicit `Http` strategy always overrides. Learning happens at the `AppContext::fetch` seam — engines stay stateless.

## Known gaps

- No proxy pool / stealth tier (backlog moonshots).
