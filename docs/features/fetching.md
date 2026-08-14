# Tiered fetching & engines

One `Fetcher` escalates across three engines by cost: **http → browser → claude**, climbing only when the result looks insufficient — too little content (`[fetcher] min_content_chars`, default 250; per-request `min_content_chars` overrides) **or a bot-wall / challenge page**.

## FetchRequest / FetchOutcome

`FetchRequest`: `url`, `strategy` (`http | browser | auto | auto_with_research`), `wait_for_selector`, `min_content_chars`, `research_prompt`, `max_budget_usd` (Claude tier ceiling), `skip_http` (set by the tier router), `to_markdown`, `no_cache` (bypass the HTTP cache — always hit the network), `ttl_override` (per-fetch cache TTL in seconds; caps staleness without a full bypass), `archive_max_age` (opt into the archive tier — see below), `profile` (named login profile, threaded to both tiers — see [Session vault](#session-vault-named-login-profiles)). `FetchOutcome`: winning `engine`, status, html/markdown/text, `escalations` trail (one line per tier rejection + router/budget notes), structured `trace` (see below), `cost_usd` (Claude tier actual), `snapshot` (archive provenance — see below).

Always prefer the metered **`AppContext::fetch`** over `ctx.engines.fetch` — it adds cost attribution, budget governance, and tier routing.

### Failure semantics: a dead engine degrades the ladder, it doesn't collapse it

- **Every tier traces and climbs on an engine error.** Archive, api_recipe, http, browser and claude all record a `TierTrace` with verdict `error` and let the ladder continue; when nothing is left, the fetch fails with `all fetch tiers exhausted for <url> (attempted: <tiers>): <trail>`, which names each tier that ran (once, in trace order) plus every tier's own reason. The Claude tier used to propagate its raw engine error instead, so the last tier's failure erased the trail of what the cheaper tiers had found.
- **A browser engine failure un-skips the http tier.** On `auto` / `auto_with_research`, if the browser attempt fails with an **engine error** *and* the http tier was skipped before it (`skip_http`), the fetcher retries http at that point rather than declaring exhaustion. Without it, a Chrome outage failed every escalating fetch outright — and hardest on the hosts the learned router had pinned to the browser, i.e. exactly where traffic is concentrated. The retry appears in the trace in real chronological order (browser `error`, then the http entry) with an `http tier un-skipped: …` trail line, and it goes through the governor like any HTTP request. An http **win** there records an http *win* for the tier router, so an engine outage never deepens the pin that caused it.
- **Only engine errors.** A browser `blocked` (bot-wall) or `thin` verdict is an observation about the page — and that host was pinned to the browser precisely because http kept losing on it — so those keep climbing to the Claude tier instead of re-fetching. The explicit `browser` strategy keeps its fail-fast (the caller asked for a JS render; a static body is not one), and a fetch whose http tier already ran is never re-fetched.
- **`skip_http` provenance is not tracked.** Nothing in `FetchRequest` records whether the router or the caller set it, so both get the fallback. That is the safe direction: the alternative is a caller-pinned host losing its whole ladder to an unrelated engine outage, and the cost of being wrong is one extra governed HTTP request on a fetch that was about to fail anyway.

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

## Tier zero: the archive tier (`archive_max_age`)

Opt-in and default-OFF (`[archive] enabled`). When a request sets `archive_max_age` (seconds) **and** an archive engine is wired, a stored **Wayback** snapshot is tried *before* any live tier: zero load on the target site, zero politeness budget, zero ban risk. Archive coverage is patchy, so the tier is strictly opportunistic — a miss, a snapshot older than the window, a thin body, an archived challenge page, or an engine error always falls through to the live ladder, never fails the fetch. The `browser` strategy is excluded (the caller asked for a JS render; an archived static body is not one).

### Archive provenance: `FetchOutcome.snapshot`

The tier trades **freshness for availability**, so the trade has to be visible. A winning archive fetch carries:

```json
{ "engine": "archive",
  "snapshot": { "via": "archive", "captured_at": "2019-03-11T09:15:00+00:00" } }
```

| field | type | notes |
| --- | --- | --- |
| `via` | `string` | which store served the body (`archive` today). A free string, not an enum — a second snapshot source must not need a core type change to be legible |
| `captured_at` | `string?` | the snapshot's capture time, RFC 3339 UTC, exactly as the serving engine reported it. Omitted when the engine marked provenance without one |

`snapshot` is **absent on every live tier** (http, browser, claude, api_recipe), so `snapshot != null` is a sound test for "this body came out of a store". Branch on it rather than on `engine == "archive"`: the engine string names the *tier*, while `captured_at` is the variable the tier actually trades.

Three properties worth knowing:

- **An archive win always carries provenance.** The engine marks the response with `x-pumper-fetched-via` / `x-pumper-snapshot-ts`, and the fetcher lifts those into `snapshot`; if an engine forgets them, the fetcher still stamps `via: "archive"` with no `captured_at` rather than letting an archive win report itself as live.
- **A live origin cannot forge it.** Those headers are read *only* inside the archive branch. An origin that returns `x-pumper-fetched-via: archive` on a live fetch gets `snapshot: null` — provenance anyone could forge would be worse than none.
- **It reaches the receipt and the trace, not just the return value.** The winning `TierTrace.detail` reads `served from archive snapshot captured <ts>`, and `AppContext::fetch` puts the same line **first** in the fetch's `cost_events.detail`, ahead of the escalation trail. Recorded VCR cassettes carry it too (through the entry's header map), so a replayed archive fetch does not come back looking live.

Historical **backfill** over a date range is a different surface — the extractor app's `source.archive` mode, which enumerates CDX captures and tags records `_fetched_via: "wayback"`. See [extraction.md](extraction.md).

## Engine capability contract

The capability traits (`HttpClient`, `Browser`, `Researcher`) carry **default-bodied** methods, so an engine that does not implement one still compiles. Two rules keep that from becoming a silent hole, both enforced by the cross-engine conformance battery in `crates/server/src/e2e/engine_conformance.rs` (it lives there because `crates/core` depends on no engine crate, so only the server can see every implementor at once):

- **A deterministic transport failure fails the job ONCE.** Statuses were always classified at the HTTP engine's retry seam (a 404 returns on attempt 1); transport failures were not, so an unparseable URL, an unsupported scheme (`ftp:`, `mailto:` off a crawl frontier) burned `retries + 1` attempts with full backoff and three governor slots, then failed with a *retryable* `Error::Http` that the worker re-queued — the whole ladder again on every job attempt. A failure reqwest classes `is_builder` (the request could not be **constructed**) is now an `Error::BadRequest`: terminal, HTTP **400**, one attempt. Everything else stays retryable, deliberately: `is_connect` bundles DNS (an NXDOMAIN from a resolver that is itself down), TLS (a captive portal failing the handshake) and a service mid-restart; a redirect-limit overflow is usually a *session* fact (an expired cookie bouncing every request to a login) that a warmed jar breaks. Classification is on reqwest's typed predicates, never on message text.
- **A capability refusal fails the job ONCE.** `Browser::transact`'s default returns `Error::Transact` — **terminal** (`Error::is_terminal_for_job`), surfacing as HTTP **422** rather than 502. Which engine is wired is fixed for the life of a job, so a retry can only reach the identical refusal; it used to be an `Error::Browser`, i.e. retryable, and a transact job on a deployment without a browser engine burned its whole backoff ladder producing the same sentence four times. A failure *during* a flow is still an `Error::Browser` and still retryable — only the pre-flight refusal is terminal.
- **Binary capability is declared, not discovered.** `HttpClient::fetch_bytes` (raw bytes, no charset decoding, no response cache) is implemented by:

| engine | `fetch_bytes` | |
| --- | --- | --- |
| `engine-http` `HttpEngine` | **yes** | the engine that talks to the network |
| `engine-remote` `RemoteEngine` | **yes**, served locally | `/fetch-proxy` carries a `String` body, so a binary fetch cannot travel it. It forwards to the local stack rather than dropping a capability the engine it wraps has |
| `engine-archive` `ArchiveEngine` | **no**, deliberately | "the bytes of a snapshot" is unspecified (which capture? what does a freshness window mean for an immutable artifact?). The refusal names the archive and points at `list_snapshots` + `snapshot_url` |

Apps that need binary bodies (`cms-fee-schedule` downloads a release ZIP) call `ctx.engines.http.fetch_bytes(...)`, which relies on `EngineSet.http` being the binary-capable engine. That wiring is now pinned by a test rather than by luck.

## Engines

- **http** (`engine-http`): reqwest + cookie jar, retries w/ backoff, fronted by the content-addressed TTL `http_cache` (GET-only; `HttpRequest.no_cache` bypasses) and the governor. **Conditional GET:** `HttpRequest.etag` / `HttpRequest.if_modified_since` (serde-defaulted) are sent as `If-None-Match` / `If-Modified-Since` (explicit `headers` still win); a `304 Not Modified` is passed through with its status intact and is **never** written to the cache over the prior full response (powers the crawler's revisit mode — [crawling.md](crawling.md)).
- **Cache revalidation.** When a cacheable GET misses because its entry **expired** (but the caller isn't running its own conditional GET), the engine reads the stale entry's stored `ETag` / `Last-Modified` and re-sends as a conditional GET instead of re-downloading the whole body. A `304` **refreshes** the entry's TTL in place (no body rewrite; `created_at` moves forward so the `max_age` read-staleness cap still measures from the last confirmed fetch) and serves the stored body as a `cache_hit`; a `200` stores and returns the changed body. This turns the `watch`/poll workload's common "unchanged page past its TTL" case from a full body transfer + parse into a few-hundred-byte round trip. The caller-owns-the-validator path (crawler revisit) is untouched — it still gets the raw `304`.

#### Cache growth bounds (always-on janitor)

An hourly janitor (`main::store_janitor`) bounds the caches with **no opt-in** — unlike `[storage]` retention, which deletes accrued value and therefore ships off. Nothing it removes is data an operator would miss:

| store | bound | key |
| --- | --- | --- |
| `http_cache` | expired entries, **plus** the oldest-confirmed rows past a row ceiling | `[cache] ttl_secs`, `[cache] max_rows` (default 20 000; `0` = unbounded) |
| `research_cache` | expired entries | `[claude] research_cache_ttl_secs` |
| `revalidations` | observations older than the retention window | `[refresher] retention_days` (default 30) |
| `tier_memory` | rows with no pin/strikes/penalty past the TTL | `[fetcher] host_memory_ttl_secs` |

Why each existed unbounded: a **continuously-revalidated** `http_cache` entry keeps pushing its own `expires_at` (and `created_at`) forward, so expiry alone never reclaimed it — the row cap is the backstop, and because it measures age on `created_at` it evicts what has been confirmed *least* recently, so it does not fight the refresher for the entries the refresher is keeping warm. `research_cache` had **no purge path at all** (an expired answer was unreadable yet still on disk). The `revalidations` log is appended by the *demand* path on every conditional GET, but its only pruner used to sit inside the refresher pass — unreachable at the shipping `[refresher] enabled = false`, which is why that knob is now applied by the janitor regardless of `enabled`.

Each pass is bounded work: indexed deletes plus one `LIMIT`ed eviction (≤5 000 rows per pass, so a wildly over-cap store converges over a few passes instead of holding one enormous write transaction).

#### HTTP request controls (body cap, timeout, retry policy)

- **Body size cap.** The response body is read in streamed chunks and aborted the instant the cumulative size would exceed the cap — one huge/hostile URL can't balloon memory. Over-limit yields a typed `Error::Http` naming the cap and URL. Cap = `HttpRequest.max_body_bytes` (per-request `Option<u64>`) else `[http] max_body_bytes` (default **16 MiB** — comfortably above the largest real pages we fetch, e.g. SEDIA clean-text / census blobs in the low single-digit MiB).
- **Charset decoding.** Bodies are decoded to text honouring the source charset (`encoding_rs`), resolved in priority order: the `Content-Type` header's `charset=` token → an HTML `<meta charset>` / `http-equiv` sniff of the first 1 KiB → a UTF-8/UTF-16 BOM → UTF-8. A legacy-encoded page (e.g. a **windows-1250** Czech government site like psp.cz) decodes to correct text instead of U+FFFD replacement soup; an unrecognized label falls through to the next source, and the final UTF-8 decode is lossy so a body is always returned. (Before 2026-07: bodies were blindly lossy-UTF-8, which mangled every non-UTF-8 page.)
- **A zero cap is refused at boot.** `[http] max_body_bytes = 0` used to parse fine and then reject **every** non-empty body — a total scraping outage whose only symptom was a per-URL `exceeds max_body_bytes cap of 0 bytes`. `Config::validate` now refuses it (as it already did for the `[remote]` and `[ingress]` twins). It is refused rather than reinterpreted because one tier down `[browser] max_html_bytes = 0` means the **opposite** (no cap); there is no disable value for the HTTP tier — set a large number.
- **Per-attempt timeout.** `HttpRequest.timeout_secs` (`Option<u64>`) overrides the client-global `[http] timeout_secs` for that request. It bounds **one attempt** (connect + redirects + reading the body), not the whole fetch.
- **One deadline bounds a whole fetch.** `[http] total_budget_secs` (default **300**; `0` disables) caps the *total* wall clock of one `fetch()` / `fetch_bytes()` — every attempt, every retry sleep and every governor wait together. `timeout_secs` bounds an attempt and the retry loop multiplies it: with the shipped `retries = 3` a black-holed host cost `4 × timeout_secs` plus three backoff sleeps (~127 s), and a host answering `429 Retry-After: 600` cost **~37.5 minutes**, past `[worker] job_timeout_secs` (900 s) — so one hostile URL consumed a whole job's budget and killed every other unit of work it had queued. Three rules make the bound real: the deadline is computed once before the first attempt; a retry sleep the budget cannot hold (plus a second of usable attempt after it) **fails the fetch instead of being truncated** — retrying earlier than a server's `Retry-After` would trade a wall-clock bug for a politeness one; and each attempt's timeout is clamped to the remaining budget so no attempt can end past the deadline. The budget is a separate key rather than a redefinition of `timeout_secs` because redefining it would silently shorten the first attempt for every existing deployment; and it is always raised to at least one full per-attempt timeout, so a caller that widens `HttpRequest.timeout_secs` for a huge download still gets one complete attempt. The default sits above the worst *benign* ladder the shipped defaults can produce, so no fetch that works today starts failing. Exhaustion is an `Error::Http` naming `total_budget_secs`, the elapsed wall clock, the attempt count and the URL, and stays **retryable** — "this host was slow *this time*" is a fact about a live site. (Same shape as `[browser] render_budget_secs`, and the reason `[remote] timeout_secs` had to enforce its own end-to-end deadline before this existed.)
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
- **One deadline bounds a whole render.** `[browser] render_budget_secs` (default **180**; `0` disables) caps the *total* wall clock of one render — navigation, selector wait, settle, scripted actions, `evaluate`, network-body pulls and DOM capture together — and every CDP call inside it. `nav_timeout_secs` bounds only the navigation wait; before the budget, `goto`, `evaluate`, `Network.getResponseBody`, `content()`, `page.url()` and the `find_element` inside the selector poll had **no timeout at all**, so a half-dead Chrome (alive enough that the liveness flag stays true, wedged enough not to answer CDP) held one of the four `max_concurrent_renders` slots indefinitely with no error until the job timeout. The budget is deliberately not derived from `nav_timeout_secs`: that key is per-navigation patience, and multiplying it would silently multiply the tier's worst case for anyone who raised it for one slow site. The clock starts once the render owns a Chrome, so queueing behind a busy semaphore costs a render nothing (a launch is separately capped at 15s). Exhaustion is an `Error::Browser` naming `render_budget_secs`, the stage that ran out and the URL — and stays **retryable**, because "this page was slow *this time*" is a fact about a live site.
- **Caller waits are clamped, not rejected.** `extra_wait_ms` and the `wait_ms` action are raw milliseconds with no schema ceiling. Each is truncated to what is left of the budget minus a 5s reserve for capturing the DOM, so a pathological wait shortens the wait instead of failing the job (and never sleeps the budget away and then dies at capture time). A truncated `wait_ms` reports `partial` in `RenderedPage.action_outcomes` (and so in the transact bundle's `outcomes[]`); a truncated settle wait is logged with the requested and granted values.
- **Every render gives its tab back — including a cancelled one.** The tab and the one or two background tasks servicing its CDP events are owned by an RAII scope guard, so they are released when the render future is **dropped** as well as when it returns. That matters because `DELETE /jobs/{id}` and the wall-clock `[worker] job_timeout_secs` both drop a running render mid-flight: cleanup that lived on the return paths never ran for them, and a dropped `JoinHandle` detaches its task rather than aborting it, so each cancelled render used to leave a zombie tab plus its tasks behind until the next recycle. Task aborts are unconditional; the tab close is dispatched from `Drop` as a detached best-effort task (a `Drop` cannot await), so during runtime shutdown the close may not land — the crash/recycle relaunch is still the backstop there.

**`RenderRequest`** fields: `url`, `wait_for_selector`, `actions` (scripted page interactions — see below), `extra_wait_ms` (settle time; falls back to `[browser] default_wait_ms`), `evaluate` (JS expression; JSON result lands in `RenderedPage.evaluated`), `load_all_resources`, `profile` (session vault — see below).

- **Scripted page actions.** `actions` (also on `FetchRequest`, serde-default empty = one-shot render) drives the pages the browser tier exists for but a single render can't reach — infinite-scroll, "load more" buttons, lazy-loaded listings. Run in order **after the settle wait and before `evaluate`**, under a total-time budget of one `nav_timeout_secs` (itself clamped by whatever is left of `render_budget_secs`) so a loop can't run forever — and each individual step is bounded by that same deadline, so one wedged click cannot outlive it. Action types (`{"action": …}`): `scroll_bottom`, `scroll_by {pixels}`, `click {selector}`, `type {selector, text}`, `wait_for_selector {selector, timeout_ms?}`, `wait_ms {ms}`, and `repeat {times, steps[], until_selector_count_stable?}` — the scroll-until-exhausted loop, which stops early once the tracked selector's match count stops growing. Each step is best-effort (a missing selector is logged and skipped, never aborting the render). `RenderedPage.actions_completed` reports how many top-level actions ran, so a truncated listing is visible rather than silent.

**`RenderedPage`** fields: `html`, `final_url`, `evaluated`, plus honest wait/cost signals — `nav_timed_out: bool` (the navigation-wait deadline elapsed and the DOM was captured mid-load, so HTML may be partial), `selector_found: Option<bool>` (`Some(true)`/`Some(false)` for a requested `wait_for_selector` that did/didn't appear before the deadline; `None` when none was requested), `blocked_resources: usize` (count of subresources dropped by interception this render). All three are serde-defaulted.

Config keys (`[browser]`): `chrome_executable`, `headless` (true), `user_data_dir` (`data/browser-profile`), `default_wait_ms` (1000), `nav_timeout_secs` (30), `render_budget_secs` (180; `0` disables), `max_concurrent_renders` (4), `block_resources` (true), `recycle_after_renders` (200), `max_html_bytes` (16 MiB, mirroring `[http] max_body_bytes`; `0` disables; `RenderRequest.max_body_bytes` overrides per render), `proxy` (none; falls back to `[http] proxy`).

## Remote fetch fabric (`[remote]`, default OFF)

One switch drives both sides. **Serving:** with `[remote] enabled` and a `secret`, this node exposes `POST /fetch-proxy` — a peer POSTs a serialized `HttpRequest`, the node runs it through its own local stack (HTTP engine, governor, cache, caps) and returns the `HttpResponse` envelope (`{status, headers, body, final_url, cache_hit}`) as JSON. **Dispatching:** with `nodes` also non-empty, the tiered fetcher's **live-HTTP tier** routes through `RemoteEngine` instead of the plain local engine, so a fetch egresses from a peer's IP/geography.

Config keys (`[remote]`): `enabled` (false), `nodes` (`[]` — empty means serve-only), `secret` (**required** when enabled; `Config::validate` refuses an enabled fabric without one, since an unauthenticated `/fetch-proxy` is an open proxy), `timeout_secs` (60), `node_cooldown_secs` (60), `max_body_bytes` (16 MiB), `allow_private_targets` (false).

### Failover: local is the last resort, not the second

The round-robin cursor picks the **starting** node; a failure — transport error, non-2xx proxy status, unparseable envelope, over-cap body, deadline — moves to the **next eligible peer**, and only when those are exhausted does the fetch fall back to the local engine.

That ordering is the whole point. Previously one node was tried once and any error fell straight to local, so with one dead peer out of three a deterministic **third of all egress left from exactly the IP the fabric was deployed to stop using** — silently, since the fetch then succeeded. On a host that blocks the coordinator, that leaked third comes back thin/blocked, feeds the learned tier router three strikes, and pins the whole host to the **browser** tier for every future fetch. A dead peer therefore used to escalate an entire host to a costlier engine.

Three bounds keep failover from becoming its own outage:

- **Cooldown.** A failed peer is skipped for `[remote] node_cooldown_secs` (default 60; `0` disables), so the next N fetches don't each re-discover the same dead node and pay a full timeout to do it. A success clears it immediately — recovery does not wait out a penalty.
- **Attempt budget.** At most **3** distinct peers per fetch, whatever the cluster size, so a total outage costs a bounded 3 × `timeout_secs` rather than N × it.
- **`timeout_secs` is genuinely end to end.** It is enforced as a deadline around the whole node attempt. It has always been *documented* that way, but it was handed to the HTTP engine, which applies a request timeout **per retry attempt** inside `for attempt in 0..=retries` — so a black-holed node cost `[http] retries + 1` full budgets. The HTTP engine now bounds itself too (`[http] total_budget_secs`), but the fabric keeps its own deadline: it is the tighter of the two and it covers the envelope decode, not just the transport.

Relatedly, a failed proxied fetch is answered **422, not 502**. `502` is in the shipped `[http] retryable_statuses`, and the coordinator POSTs its proxy call through its own `HttpEngine` — so a *deterministic* peer-side failure was retried four times, each paying the peer's whole fetch time with exponential backoff between, before the coordinator's failover ladder even started. The fabric owns its own retry ladder; a second one underneath it only multiplies. (Same reasoning that made a `transact` capability refusal a 422 rather than a retryable 502 — see the capability contract above.)

### Egress attribution: which node actually served it

The fabric's whole product claim is "this fetch left from a different IP/geography", and nothing in the product could confirm it: `engine` reads `http` whether the body came off this machine or a peer in another country, and total fabric observability was **one coordinator-side `warn!` on the failure path**. Success was silent — so a misconfigured secret, which makes every peer answer `401` and every fetch fall back to local, produced a log line that read identically whether one fetch or a million had leaked.

Three surfaces now answer it, all of them ones that already existed:

| surface | what it says |
| --- | --- |
| **Fetch trace** | the http tier's `detail` reads `egress via remote node <node>` — on the winning entry *and* on a losing one, since "the http tier came back blocked" reads very differently once you know whose IP it came back blocked at |
| **Job receipt** | `cost.egress` = `[{node, calls}]`, which peer nodes this run's fetches left from. Empty when nothing went through a peer (always, with `[remote]` off) |
| **Serving node logs** | each node logs the target URL, status, byte count and duration of every fetch it made **on a peer's behalf**, at the level the fetch stack already logs fetches at. A node whose IP gets banned can reconstruct what it fetched for others; before, the serving side logged nothing at all |

**On `/metrics`.** `pumper_remote_egress_fetches{served_by="peer"|"local_fallback"}` answers "is the whole cluster silently falling back?" as a single scraped number — `local_fallback` is the operational one, because every fetch counted there left from the coordinator's own IP, the address the fabric was deployed to stop using. A misconfigured secret makes every peer answer 401 and every fetch fall back, which shows up here as a `local_fallback` line climbing while `peer` stays flat. Both series are emitted even when the fabric is off (both read 0): an absent series and a zero series are different answers. Counters are process-lifetime and reset on restart, like the rest of the endpoint's counters. The read is a `state.engines.fetch` *field access* — a counter read, not a fetch — so it carries a reviewed row in `crates/core/tests/fetch_chokepoint.rs`'s `EXPECTED_RAW_ENGINE_CALLS` rather than an exemption. Per-job (`cost.egress` on the receipt) and per-fetch (the tier trace) answers remain.

The carrier is a reserved response header, `x-pumper-remote-node`, following `x-pumper-fetched-via` exactly: the header map is the only channel that survives an engine boundary, and it is read **only where the fabric is wired**, so an origin that echoes the header on an ordinary live fetch cannot forge "a peer served this". The coordinator overwrites any value that arrives from the wire — the namespace is reserved and only it may write it.

### The envelope is bound to the request

**Nothing used to bind a peer's answer to the question.** The coordinator deserialized whatever the peer sent, and the tiered fetcher minted the outcome with the *requested* URL and the peer's body — so a buggy or hostile node could return arbitrary content for any URL and have it stored, indexed and attributed with no detectable trace. One node quietly serving a cached copy of the wrong page was indistinguishable from the site changing.

The serving node now echoes the URL it was **asked** for in `x-pumper-remote-target`, and the coordinator refuses any envelope that does not match — falling back like any other node failure. The echo is deliberately *not* `final_url`, which is where the fetch ended and legitimately differs after a redirect. A missing echo is a mismatch too: an unverifiable envelope fails closed, or the binding would be opt-in for the peer being checked. The coordinator strips the echo before returning, so it never reaches a consumer.

`max_body_bytes` is enforced on the **decoded** inner body as well as on the transport. The transport cap is deliberately `2× max_body_bytes + 64 KiB` because JSON escaping inflates the envelope, but that is encoding headroom, not a raised limit — nothing used to re-check the body after decoding, so a peer whose own cap had drifted upward could hand the coordinator twice its stated cap and be paid for twice (wire, then decoded string).

**Read [deployment.md § Remote fetch fabric](../deployment.md) before enabling it.** A peer must be reachable at a routable address, so every node has to bind off loopback — which exposes every *other* route on that node, all unauthenticated by design. The secret protects `/fetch-proxy` alone; the real control is network-level (firewall / VPN / authenticating reverse proxy).

### What a peer is not allowed to change

The substitution is only honest while it changes *where* a fetch leaves from and nothing else. Three refusals enforce that:

| refusal | where | why |
| --- | --- | --- |
| **Binary bodies stay local** (`fetch_bytes`) | coordinator | the envelope's `body` is a `String`; forwarded to the local stack rather than dropping the capability (see the capability contract above) |
| **Profiled fetches stay local** (`must_serve_locally`) | coordinator | a profile is a cookie jar on the **coordinator's** disk and nothing replicates it. Dispatched, the peer opens a jar it does not have and answers `200` with the logged-out or login-wall page. (`engine-http` now warns and marks such a response — see the session-vault rules above — but the refusal stands: a marked login wall is still not the data the caller asked for) |
| **Unheld profiles are refused** (`422`) | serving | defence-in-depth for the same bug against an older or hostile coordinator: a node asked for a profile it has no jar for refuses instead of fetching anonymously. The coordinator treats it like any node failure and falls back — one extra fetch, which is the correct price for not storing a login wall as data |

### Target policy (serving side)

`/fetch-proxy` is the only route that turns a caller-supplied string into an arbitrary outbound request. It refuses (`403`) targets that are:

- **loopback** in any WHATWG spelling — `127.0.0.1`, `127.1`, `2130706433`, `0x7f.0.0.1`, `[::1]`, `[::ffff:127.0.0.1]`, `localhost`, `*.localhost`;
- **link-local** `169.254.0.0/16` (this is where the cloud metadata service lives) and `fe80::/10`;
- **private** RFC 1918, **unique-local** `fc00::/7`, **CGNAT** `100.64.0.0/10`, `0.0.0.0/8`, broadcast, multicast;
- any scheme other than `http`/`https` (`file:`, `gopher:`, …).

`[remote] allow_private_targets = true` relaxes the **address** ranges for a cluster that deliberately scrapes its own LAN. It does not relax the scheme check. The guard reads the **target** URL, not the node address — loopback *nodes* keep working, which is what the round-trip e2e pins.

**Known limit:** the predicate is pure (no DNS), so it blocks address literals and the `localhost` family by name; a **hostname that resolves into** a private range is not caught. Closing that needs resolve-then-pin inside the HTTP engine and still races DNS rebinding.

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

Three rules keep a named login from silently stopping being one:

- **An anonymous profiled fetch is visible, not silent.** A jar that is absent or empty when the request goes out is not an error — it is how a login is established on this tier — but it is also exactly what a typo looks like, and it used to be completely undetectable: a mistyped `profile: "acme_portl"` fetched the login wall with a `200`, cleared `min_content_chars`, recorded `TierVerdict::Ok` and was stored as a real dataset revision. The engine now logs a WARN when it opens a profile with no stored cookies, and marks the response with the reserved `x-pumper-anonymous-profile` header. The tiered fetcher lifts that into the escalation trail (so it reaches the job's `cost_events.detail` and the receipt) and into the HTTP tier's `TierTrace.detail` — **including on the winning entry**, since the winning fetch is the one about to be stored. Written both ways round on every profiled response and read **only when the caller asked for a profile**, so an origin cannot forge it (the same rule `x-pumper-fetched-via` and `x-pumper-remote-node` follow). `fetch_bytes` returns raw bytes and has no header map, so it carries the WARN only.
- **A profile appears on disk when it becomes real, not when it is typed.** Directory creation moved from jar *load* to the first *save* that actually has a cookie to write. Previously `create_dir_all` ran before the open, so a typo materialised `data/profiles/acme_portl/` and `GET /profiles` reported it as a real profile, indistinguishable from one not logged in yet.
- **A failed jar write is retried, and an empty jar never overwrites a real one.** The flusher used to clear its dirty flag *before* saving, so one transient failure (a Windows sharing violation while a backup or antivirus holds the file) silently threw the login away — logged in for the life of the process, logged out by the restart. The flag now survives an error and the write is retried on the next debounce, up to 5 times before a final loud WARN. And a save whose in-memory jar holds **no** cookies is refused when a jar exists on disk: `jar_for` caches its `Arc` without re-reading disk, so a server started while `cookies.json` was missing would otherwise overwrite an operator's restored backup with an empty jar and log `cookie jar saved`. Cost of the rule, stated honestly: a genuine logout no longer erases the stored jar, so a dead cookie survives on disk until the next login overwrites it — the cheaper failure, since the site rejects a dead cookie whereas a clobbered session has no recovery.

Clients are still pooled, not duplicated: the existing bounded LRU pool's key is generalized from `proxy` to the **`(proxy, profile)`** pair a client is *built* with (≤8 clients, oldest evicted). Evicting a client never loses cookies — jars are owned by the engine's jar map, keyed by name and not evicted.

**Profiled requests bypass the shared `http_cache`.** Its key is method+url+body only, so caching a logged-in body would serve it to anonymous callers (and vice versa). Profiled fetches always hit the network.

**Browser tier.** Chromium binds `--user-data-dir` at launch, so one Chrome = one profile. A profiled render therefore selects among a **small map of relaunchable holders keyed by profile** (`None` = the shared default instance), each with the full crash-relaunch + recycle logic. At most **4 Chromes** are live at once; the least-recently-used holder is closed (dropped, which reaps its Chrome) when a new profile pushes past the cap. The alternative — one holder relaunching on every profile switch — was rejected because interleaved profiles (the normal case for a queue serving several logins) would thrash Chrome on every request; the cost of the map is up to 4 resident Chromes, bounded by the LRU. The render-concurrency semaphore is shared across profiles.

Existing profiles are listable via `GET /profiles` (name, `has_cookies`, `has_browser_dir`, `last_used`) — see [http-api.md](http-api.md). Profiles are created implicitly by the first fetch that names them; there is no create/delete API.

**Phase 1 scope.** The vault stores *session state* only. There is **no credential management and no encryption at rest**: `cookies.json` and the Chrome profile dir are plaintext on disk, exactly as readable as any other file in `data/`. Logging in is still manual (e.g. run once with `[browser] headless = false` under a profile, or drive a login POST on the HTTP tier); nothing logs in for you.

### Honest tier verdicts (bot-wall detection)

A tier no longer passes purely on char count. On escalating strategies (`auto`, `auto_with_research`) the HTTP tier escalates instead of returning content when the response is a bot-wall: a challenge/block **status** (403/429/503) or a conservative **challenge-page marker** in the body's leading window (Cloudflare "checking your browser" / "just a moment" / `cf-browser-verification`, "enable javascript", captcha, "verify you are human", "ddos protection by"). The browser tier applies the same marker heuristic before handing off to Claude (it has no HTTP status). Blocked tiers add a `... blocked: <reason>` line to the `escalations` trail. The explicit `http` / `browser` strategies still return the body as-is for the caller to inspect.
- **claude** (`engine-claude`): Claude Code CLI as a research engine — roles from `[claude.roles]` (model/effort/budget presets), `json_schema` constrained output, `resume_session`, reports `total_cost_usd`. Cached via the research cache (see [runtime.md](runtime.md)).

### The Claude subprocess

**A timeout _or a cancel_ kills the whole process tree, not just the shim.** The prompt is piped over stdin; everything else travels as CLI arguments. On Windows the configured `binary` (default `claude`, an npm `.cmd`/`.ps1` shim that `CreateProcess` cannot launch) is spawned through `cmd.exe /C`, so the direct child is `cmd.exe` and the CLI is its *grandchild*. Terminating the direct child — which is all `kill_on_drop` does — left that grandchild running its full agentic loop, spending real money nothing would ever meter. Every engine timeout now runs `taskkill /PID <shim pid> /T /F` **before** the child handle is released (the handle is what stops Windows from recycling the pid), then kills and reaps the direct child; the error says `timed out after Ns …; process tree killed` so an operator can tell a clean kill from a failed one (`process tree kill FAILED (…) — a spawned process may still be running`). On POSIX there is no shim: the child *is* the CLI, and killing it stops the spend at the source. Pointing `[claude] binary` at a real `.exe` also takes the direct path on Windows.

**The timeout was only half of it.** A cancelled job does not time out: `DELETE /jobs/{id}` and shutdown-suspend make the worker `break` out of its `select!`, which **drops** the research future — and a dropped future runs none of the paths above, so the tree kill was unreachable on the single most ordinary operator action there is. `kill_on_drop` was all that remained, and it terminates the *shim*. So the API answered `cancelled`, the job row said `cancelled`, and the grandchild kept running its agentic loop and kept spending. Cleanup therefore lives on no path at all — it lives in `Drop`: a `RunScope` owning the child, the pid and the three pipe tasks, mirroring the browser engine's `RenderScope`. Its drop aborts the tasks unconditionally (dropping a `JoinHandle` *detaches* rather than aborts, so all three leaked with every cancel too) and hands the tree kill to a detached task carrying the child handle with it. A successful run disarms it and pays for no `taskkill`. Best-effort in one narrow case, stated rather than hidden: a runtime *already shutting down* may never poll that task, leaving `kill_on_drop` as the backstop.

One deadline (`[claude] timeout_secs`, default 600; per-request `timeout_secs` overrides) covers both waiting for exit and draining stdout, so a leaked process holding the output pipe open cannot park a call past its own timeout.

**A failed call still spent money, and the ledger says so.** The CLI reports `total_cost_usd` in the *same* envelope it reports a failure in, so the runs that cost the most were exactly the ones whose spend used to vanish. Every failure now carries a structured spend out of the engine, and the metered seams (`ctx.research`, `ctx.fetch`) write it to `cost_events` **before** propagating the error — which is what lets the per-job `budget_usd` clamp see money burned by a run that then failed. The `detail` column distinguishes the cases:

| `detail` | when | `cost_usd` |
| --- | --- | --- |
| `failed_spend (is_error)` | `is_error` envelope that reported a cost | what the CLI reported |
| `failed_spend (nonzero_exit)` | non-zero exit whose stdout still held an envelope | what the CLI reported |
| `failed_spend_unreported (<class>)` | the run happened but its cost is unreadable | `0` — unknown, not free |
| `unmetered_timeout` | killed by the deadline; no envelope exists | `0` — a paid call vanished |
| `cost_unreported` | a **successful** envelope with no `total_cost_usd` | `0` — unknown, not free |

A failure that never started a process (a bad `binary`) writes no row at all — inventing one would be its own lie. Tier-3 has different plumbing (the fetcher drives the researcher itself and `ctx.fetch` meters the *outcome*, which does not exist when the ladder fails), so the paid tier's spend rides out on the `all fetch tiers exhausted …` error and is metered from there; the message is identical either way.

**Structured answers are cacheable.** Under `--json-schema` the CLI may return `result` as an object rather than a string. That used to become empty `text`, which the research cache refuses to store — so the call re-paid the model on every repeat, silently. A non-string `result` now falls back to the validated `structured_output` (then the raw value), serialized.

#### What may cross the cmd.exe shim

Only the Windows shim path re-parses (`[claude] binary` pointing at a real `.exe`, and every POSIX spawn, deliver argv byte-exact and are not restricted). On that path cmd.exe parses the command line a second time, and the following were **measured** through a real round-trip rather than assumed:

| in the value | what cmd.exe does |
| --- | --- |
| `&` | truncates the value and runs the remainder as a **second command** |
| `\|`, `>` | hijacks the invocation into a pipe/redirect — the CLI never runs |
| `^` | **silently eaten** (`a ^ b` arrives as `a  b`) |
| `%` | expands: `%TEMP%` arrived as a path, `%PATH%` inlined the whole variable into the value |
| `<` | survives or breaks depending on cmd's quote state at that point |
| newline / CR | truncates the value / dropped silently |
| `"` `{}` `[]` `\` `,` `:` `;` `!` `()` non-ASCII | **survive byte-exact** — a real JSON schema round-trips intact |

Values holding a character from the first six rows are now **refused before anything spawns**, with an error naming the offending flag — never sanitised, because a silently-altered schema still costs a full run and constrains the answer to something the app never asked for. The whole rendered command line is also budgeted at 8000 characters (cmd.exe's own ceiling is 8191, where it truncates); an oversized `json_schema` is refused rather than cut in half. A refusal is classed `Spawn`: nothing ran, so nothing was spent and no ledger row is written.

`append_system_prompt` is exempt because it no longer travels on the command line at all — the engine writes it to a scratch file and passes `--append-system-prompt-file`, so operator prose is free to contain `R&D`, `100%` or `>`. The file is written under the working directory below and deleted when the run ends.

#### Roles and models are validated at the engine door

- An **unknown `role`** is now an error naming the roles that *are* configured, instead of resolving to `None` and falling through to the config defaults. A typo'd `role` in a `POST /jobs` body used to succeed while quietly buying a different model at a different effort. A request with **no** `role` is unaffected and still takes the config defaults. Note that `[claude.roles]` in a config file *replaces* the default map rather than merging with it — if you define your own roles, keep the names your apps pass (the shipped apps all send `research` or `compose`).
- **`model`** is a free string from the job body all the way to `--model`, so it must match `[A-Za-z0-9._:-]{1,128}` (a conservative pattern, deliberately not a catalogue of known ids — new models ship too often). Anything else is refused before spawning.

#### The subprocess gets its own working directory

The CLI used to inherit the server's CWD. When the server was started from a directory that is itself a Claude Code project — which in development is the norm — every research call discovered that project's `CLAUDE.md`, skills and hooks and paid for them: measured in this repo, **35k cached input tokens** loaded for a one-word prompt, plus the repo's `Stop` hook firing on each call, on questions that had nothing to do with the repo.

The subprocess now runs in `<storage root>/claude-cwd` (the parent of `[storage] database_path`, created on demand), which is also where the system-prompt scratch file goes. This is **derived, not configurable** — a key of its own could only disagree with the storage layout.

Two honest limits:

- `CLAUDE.md` discovery walks *upward*. If your storage root sits inside a Claude Code project (the default `data/pumper.db` does, when you run from a checkout), the parent project is still found. The escape hatch is the existing `[storage] database_path`: point it somewhere outside any such project and the context is genuinely empty. There is no new config key for this.
- `[claude] bare` (default `false`, unchanged) remains the complete lever: it skips hooks, plugin sync, auto-memory and `CLAUDE.md` auto-discovery outright, regardless of where the process runs. Set it when you want the subprocess to carry nothing but your prompt.

## Politeness governor (adaptive)

Per-host token bucket: configured spacing (`[governor] default_rps`, `per_domain`, jitter) **plus a learned penalty**: a 429/503 doubles the host's extra spacing and pushes the host's next slot out; only a genuinely healthy **2xx** response halves it (a 4xx like 404/403 is not health and no longer rewards faster spacing; other 5xx stay neutral). Penalty bounds are configurable — `[governor] penalty_base_secs` (default 1), `penalty_cap_secs` (300), `penalty_floor_ms` (100, below which a decaying penalty is dropped). Both `Retry-After` forms are honored: delta-seconds and an HTTP-date (converted to a delay from now); a larger `Retry-After` wins over doubling. State is held in one sharded map keyed by host, so distinct hosts never contend; idle hosts are evicted once the map outgrows its cap. Learned penalties are **persisted** (see host profiles below) so they survive a restart.

## Self-learning tier router (host profiles)

`tier_memory` table (a.k.a. host profiles): an HTTP-tier loss (the http tier's structured `verdict` is `thin`/`blocked`/`error` while a higher tier won) adds a strike per host; **3 consecutive strikes** flip the host to start at the browser tier (`skip_http`, noted in the trail). One HTTP win resets. Explicit `Http` strategy always overrides. Learning happens at the `AppContext::fetch` seam — engines stay stateless.

**Aging (v2).** Strikes and the browser pin decay after `[fetcher] host_memory_ttl_secs` (default 7 days; `0` disables aging). A host whose last strike is older than the TTL reads back as unpinned, so it gets a fresh crack at the cheap HTTP tier instead of staying pinned until a lucky win — and a single fresh loss after aging out does **not** immediately re-pin (stale strikes reset to one). Aging is applied lazily on read via the `updated_at` timestamp; no sweep job.

**Penalty persistence (v2).** The governor's learned per-host penalty is written behind into the host-profile row (`penalty_ms`, `penalty_updated_at`) every `[fetcher] host_penalty_persist_secs` (default 60s; `0` disables) and restored into the in-memory governor on boot. The snapshot deliberately never touches `updated_at`, so persisting a penalty doesn't reset strike aging.

**The persisted snapshot is authoritative, not additive.** Each pass writes the *complete* set of hosts the governor currently penalizes, so a host whose penalty decayed back to zero is **zeroed in the store** by the same pass. (Before: only penalized hosts were upserted, so a recovered host kept its last non-zero `penalty_ms` forever and was restored at full penalty on every boot — recovered hosts stayed throttled until someone reset them by hand.) The host-weather import path stays additive: it raises the hosts it merged and never touches the rest.

**A restored penalty ages like a strike.** Boot restore skips any snapshot older than `[fetcher] host_memory_ttl_secs` (measured on `penalty_updated_at`), and an undated row is never restored. With aging disabled (`0`) every dated snapshot is restored, as before.

**Stale rows are reclaimed.** The always-on hourly janitor deletes `tier_memory` rows that say nothing left — no pin, no strikes, no penalty, and both `updated_at` and `penalty_updated_at` past the TTL — up to 1 000 per pass. A row that still carries a pin, strikes, or a penalty is never touched however old, so `GET /hosts` reports exactly what it did before (an aged pin is a routing decision applied on read, not a display one). Note that reclaiming a row also drops that host's `observations` counter, i.e. its host-weather export evidence: the host has been silent for a whole TTL, so it would not have travelled anyway. With aging disabled the GC is a no-op.

Learned host state is inspectable and resettable via the `/hosts` API (see [http-api.md](http-api.md)): `GET /hosts` (paginated), `GET /hosts/{host}`, `DELETE /hosts/{host}/memory` (clears strikes, pin, and the live + persisted penalty). The `penalty_ms` reported by those endpoints is the **live** governor value (the row's stored snapshot is only for boot restore). The reset clears the **live** governor penalty first and the row second, and takes the same lock the write-behind pass holds across its snapshot→commit — so a background tick can no longer land mid-reset and re-create the row that was just deleted.

Config keys (`[fetcher]`): `min_content_chars` (250), `host_memory_ttl_secs` (604800), `host_penalty_persist_secs` (60), `profiles_dir` (`data/profiles` — root of the session vault).

## Known gaps

- Single static proxy per tier (`[http] proxy` / `[browser] proxy`, per-request override on the HTTP tier). No proxy **pool / rotation** and no stealth tier (backlog moonshots). Browser-tier proxy auth (`user:pass@`) is unsupported (Chrome `--proxy-server` limitation).
- Aging is time-based only; there is no success-rate / half-life model of host reliability.
- **Archive provenance stops at `FetchOutcome`.** It reaches the outcome, the trace, the job's cost events and the VCR cassette, but nothing stamps it onto a *dataset revision* — `Provenance` (job id, source URL, artifact sha, rules hash) has no snapshot field, so an app that upserts a record extracted from a snapshot must carry the fact itself (the extractor's backfill mode does, as `_fetched_via`). The **remote/peer tier** used to have the same gap at every layer; it now reaches the trace, the cost events and the job receipt (see "Egress attribution" above), but stops in the same place — no *dataset revision* records which node fetched it, and the fact travels as a trail marker rather than a typed `served_by` field on `FetchOutcome`. Making it typed means updating the literal `FetchOutcome` / `TierTrace` constructions in `crates/core/src/app.rs`, `crates/core/src/vcr.rs` and `crates/apps/provisioner`, and teaching `fetch_cost_detail` to render it the way it renders `SnapshotProvenance::note()`.
- **Session vault (phase 1):** session state only — no credential management, no encryption at rest, no login automation. No create/delete/import API for profiles (they appear on the first *successful cookie write*; delete = remove the directory). Profiled fetches never use the response cache, and cookies set within ~1s of a hard kill aren't on disk. The HTTP jar and the browser profile are separate stores — a login in one is not visible to the other. An anonymous profiled fetch is *marked* rather than refused (refusing would break the tier's own login flow), so a consumer that ignores `FetchOutcome.trace` can still store a login wall as data. There is no flush-on-shutdown hook, so the ~1s debounce window applies to a clean stop as well as a crash, and the jar's temp file is a fixed path per profile — two pumper processes sharing one `profiles_dir` can collide on it.
