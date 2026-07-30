# Moonshot Scan — Scraping Runtime Core (2026-07-30)

> Total: 6 moonshots across 3 contexts.

## Context: Tiered Fetcher & Politeness

### 1. Host Weather Network — federated host-intelligence exchange
- **Tier**: 1
- **Feasibility**: high
- **Horizon**: months
- **Files**: crates/core/src/tiers.rs, governor.rs, fetcher.rs
- **What it is**: Every pumper instance painstakingly learns per-host truth — tier pins (`tier_memory`), politeness penalties (`snapshot_penalties`/`restore_penalty`), challenge fingerprints — and then keeps it to itself. Make that learned state a signed, exportable/importable bundle: a "host weather map" that deployments exchange (org-internal sync, or an opt-in community feed). A fresh instance boots already knowing which of 50k hosts need the browser tier, what spacing each origin tolerates, and which challenge walls to expect.
- **Why it's a moonshot**: The cold-start tax disappears fleet-wide: N deployments learn as one organism. It also becomes pumper's network-effect moat — the shared host intelligence corpus is something no single-instance scraper can replicate, the way GreyNoise/Shodan turned observation into a defensible dataset.
- **Differentiation**: Prior #33 "Per-domain escalation memory that learns the winning tier" and the T10 "Fleet rate governor" learn *within one instance* (and shipped as tiers.rs v2). Prior federation ideas (#272 federated crawl fleet, #232 event mesh) federate *execution*. No prior idea federates the *learned knowledge* itself across deployments.
- **Path**:
  1. `GET /hosts/export` — serialize all `HostProfile` rows + live governor penalties into a versioned JSON bundle (the queries in `TierMemory::list_page`/`load_penalties` already exist).
  2. `POST /hosts/import` with a merge policy: newer-wins per host, penalties merged via max, strikes never imported past the pin threshold without local confirmation.
  3. Sign bundles (ed25519) + record provenance (source instance, exported_at) per row so bad intel is attributable and revocable.
  4. Periodic pull-sync against configured peer URLs (reuse the scheduler); decay imported state faster than locally-observed state (the aging TTL machinery in `stale_cutoff` generalizes).
  5. Optional: a public community feed repo of anonymized host profiles as the ecosystem play.
- **Risks**:
  - Poisoned intel (a peer pinning a healthy host to the expensive tier) — mitigated by provenance + faster decay of imported rows.
  - Host behavior varies by egress IP/geo, so imported profiles are priors, not truth; must stay overridable by one local observation.

### 2. Self-refreshing mirror — learned change-cadence drives proactive revalidation
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: months
- **Files**: crates/core/src/cache.rs, governor.rs, fetcher.rs
- **What it is**: Today the cache is reactive: a fetch pays network latency whenever the TTL guess expired. Instead, learn each cached URL's *empirical* change cadence — every revalidation already yields a labeled observation (304 = unchanged, new body = changed; `refresh()` and `get_stale()` are the exact seams) — and have a background loop revalidate each URL just before its predicted next change, using idle per-host governor capacity. The corpus pumper tracks becomes a bounded-staleness mirror served at disk speed; app-facing fetches almost never wait on the network.
- **Why it's a moonshot**: Flips the platform from "fetch on demand" to "already have it, provably fresh". Latency for the 15 domain apps collapses to cache-read time, change detection becomes near-real-time (feeding the shipped trigger DAGs far sooner than cron), and per-URL predicted-vs-actual freshness is a sellable SLA number instead of a TTL config.
- **Differentiation**: Prior #32 "Freshness SLA tiers built on the TTL cache" sells *statically configured* TTL classes; #167 stale-while-revalidate and the shipped ETag revalidation are still *demand-triggered*. No prior idea learns a per-URL change-frequency model or revalidates proactively.
- **Path**:
  1. Add `revalidations (key, checked_at, changed)` next to `http_cache`; record the outcome inside the existing ETag-revalidate path (already live per memory of PR-merged work).
  2. Estimate per-key inter-change interval (EWMA of observed change gaps; simhash distance from `crate::simhash` grades *how much* changed).
  3. Background refresher task: pick keys whose predicted-change time is near, `acquire()` the host's governor slot only if it's free (a `try_acquire` variant — new, small), conditional-GET, `refresh()` or `put()`.
  4. Cap by per-host and global request budgets; expose `GET /cache/freshness` (predicted staleness per key/host).
  5. Wire changed-body detections into the existing dataset-delta trigger pipeline.
- **Risks**:
  - Background traffic must never crowd out live jobs or offend origins — strictly idle-slot, strictly governed.
  - Sparse observation history for rarely-fetched keys → start with the static TTL as the prior, learn on top.

## Context: App & Job Model

### 1. Agent-native pumper — the app registry becomes an MCP tool server
- **Tier**: 1
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/core/src/app.rs (ScrapeApp trait, Requirement), job.rs
- **What it is**: `ScrapeApp` already *is* a tool manifest — `name()`, `description()`, `default_params()`, `requires()` with a resolved `ready` flag. Expose the whole runtime as a Model Context Protocol server: every registered app becomes a callable tool (`run_czech_mpsv`, `crawl_site`, …), plus generic tools for dataset query (`?filter=` surface), full-text search, and job status/await. Any MCP client — Claude Desktop, Claude Code, other agent frameworks — can then drive pumper as its data-acquisition arm.
- **Why it's a moonshot**: Pumper stops being a server you integrate and becomes infrastructure agents pick up ambiently — the "scraping tool" of the agent ecosystem. Distribution shifts from selling an API to being auto-discovered by every agent runtime; each of the 15 domain apps instantly gains an agent-facing UI for free, and new apps ship as new tools with zero client work.
- **Differentiation**: Prior #134 "App marketplace: registry becomes a distribution channel" targets *human* distribution of app packages; #228 "Typed capability graph auto-composes app pipelines" composes apps *internally*. No prior idea exposes the registry as an agent-protocol (MCP) surface to external LLM clients.
- **Path**:
  1. New `pumper-mcp` bin crate: stdio MCP server that lists tools by walking the registry (name/description/default_params → JSON-schema-ish tool defs; `requires()` gates tool visibility).
  2. Tool call = enqueue job via the existing storage/enqueue path with `budget_usd` from a per-client cap; long-poll or job-event subscribe for completion; return the job result JSON.
  3. Add read tools: `query_dataset` (existing filter surface), `search_corpus`, `list_changes` (delta feed).
  4. Auth + spend ceilings per MCP client identity (reuse the cost ledger; every tool call is a metered job already).
  5. Publish the server config snippet; optionally an HTTP/SSE MCP transport on the axum server itself.
- **Risks**:
  - An agent hammering enqueue needs quotas — per-client rate + budget caps from day one.
  - Long-running jobs vs tool-call timeouts → return job handles + an `await_job` tool rather than blocking.

### 2. The information economist — closed-loop budget allocation by marginal value per dollar
- **Tier**: 2
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/core/src/app.rs (AppContext::meter, budget_usd, spent_usd), job.rs, costs.rs
- **What it is**: Every job already carries a `budget_usd` ceiling, every engine call is metered to the cost ledger, and every upsert returns new/changed/unchanged counts. Close the loop: a planner that computes marginal information value per dollar for each app/source (new+changed records, downstream trigger fires, search hits per $ spent), then *sets* the budgets — allocating a single org-level monthly budget across schedules, boosting high-yield sources, starving zero-yield ones, and choosing when the Claude tier is worth it at all.
- **Why it's a moonshot**: Today a human guesses budgets per job; at 15+ apps and growing, nobody re-tunes them. An autonomous allocator turns a fixed spend into maximum information yield — the difference between a metered platform and a self-optimizing one, and the direct enabler of "give pumper $200/month and it decides everything else".
- **Differentiation**: Prior #1 "Cost ledger: meter every fetch tier" is measurement (shipped); #92 "Cross-engine cost intelligence and ROI dashboard" is *visibility*; #135 "Paid priority lanes" is monetization. None of them *acts*: no prior idea closes the loop from yield metrics back into budget/schedule decisions.
- **Path**:
  1. Persist per-job yield alongside cost: extend the job-result convention so `UpsertSummary` totals land in a `job_yield` table (worker-side, no app changes).
  2. `GET /economics` — $/new-record and $/changed-record per app/dataset over trailing windows (join cost ledger × yield).
  3. Advisory mode: planner computes recommended `budget_usd` + schedule cadence per app; surface as a report.
  4. Enforcement mode (opt-in): scheduler reads planner outputs for scheduled runs' budgets; global monthly ceiling with proportional-yield allocation.
  5. Special-case the Claude tier: per-app "was the escalation worth it" score from `TierTrace.cost_usd` vs records produced.
- **Risks**:
  - Value ≠ record counts for every app (a rare grant record may be worth 10k HN rows) — needs per-app value weights, human-settable.
  - Feedback instability (starving a source hides its yield) — keep an exploration floor per source.

## Context: Engine Capability Traits

### 1. API X-ray — the browser tier discovers the JSON API behind the page
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/core/src/engine.rs (RenderRequest/RenderedPage, Browser trait)
- **What it is**: A JS-heavy page is almost always a thin client over a JSON API — and the browser tier already executes those XHR/fetch calls, then throws that knowledge away and hands back rendered HTML. Add network capture to the render capability: `RenderedPage` gains a structured log of same-origin API responses (URL, method, params, JSON body). A discovery pass then correlates extracted fields with captured API payloads and emits an *API recipe* — after which the host is fetched by the HTTP tier hitting the real API directly: structured JSON, no rendering, no HTML parsing, no selector rot.
- **Why it's a moonshot**: It converts the expensive tier into a one-time reverse-engineer. Per-fetch cost on JS-heavy hosts drops ~100x (one HTTP call vs a Chrome render), data quality jumps (typed JSON vs scraped text), and extraction rules stop breaking on redesigns because the API contract outlives the DOM. "Point pumper at any SPA and get its API" is a category-defining capability.
- **Differentiation**: Nearest prior is #115 "Recipe learning: auto-downtier browser scrapes to HTTP", which replays the *same HTML document* at a cheaper tier. This abandons the document entirely by discovering the *underlying data API* from observed network traffic — a different mechanism and a qualitatively bigger prize (structured data + parameterizable endpoints). No prior idea touches network capture.
- **Path**:
  1. Extend `RenderedPage` with `network: Vec<CapturedCall>` (url, method, status, content-type, JSON body) behind a `RenderRequest.capture_network: bool` — chromiumoxide already intercepts requests for resource blocking, so the CDP seam exists in engine-browser.
  2. Filter to JSON responses; store captures as job artifacts (`save_artifact`) for inspection.
  3. Discovery heuristic: score captured payloads by overlap with the page's extracted fields; emit a candidate `ApiRecipe {url_template, params, json_paths}` per host into a new table.
  4. Teach the tiered fetcher a pre-HTTP "api" branch: when a validated recipe exists for the host/path pattern, fetch the API URL instead (a natural extension of the `skip_http` router seam in AppContext::fetch).
  5. Continuous validation: recipe fetch thin/failed → fall back to browser + re-discover (the existing strike machinery generalizes).
- **Risks**:
  - Auth/CSRF-bound APIs need the session profile's cookies threaded through (the `profile` field already reaches both tiers) and some will still resist.
  - Hitting private APIs directly is more detectable/less polite than page loads on some hosts — keep recipes per-host opt-in and governed.

### 2. Transact — a first-class capability for acting on the web, not just reading it
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: quarters
- **Files**: crates/core/src/engine.rs (PageAction, Browser trait, session profiles), plugin.rs
- **What it is**: The primitives for *doing things* already exist scattered: `PageAction::Click/Type` drive pages before capture, session-vault profiles hold logged-in identities, and the domain apps discover actionable objects (grants to apply for, tenders, listings). Promote action to a capability: a `Transact` trait executing declarative multi-step flows (fill form → review → submit) with the safety rails reads never needed — mandatory dry-run mode, idempotency keys, human confirmation gates for irreversible steps, and an evidence bundle (screenshot + DOM + response) as a signed receipt per transaction.
- **Why it's a moonshot**: It changes what pumper *is*: from a data platform to a closed-loop web agent platform. The grants apps stop at "here are matching opportunities"; with Transact the loop closes — discover, draft, and *file*. Every read-only dataset becomes the front half of an automation product, a market (RPA/agentic workflows) far larger than scraping.
- **Differentiation**: No prior idea touches the write path: the 2026-07-10 backlog is read-only throughout — #192 "Claude computer-use solves login and CAPTCHA walls" gets *through* barriers to read, #139 session vault stores identity. PageActions (shipped) are pre-capture read plumbing. Executing transactions with receipts, dry-runs and confirmation gates is untouched ground.
- **Path**:
  1. Define `Transact` in engine.rs: `execute(TransactionRequest{url, profile, steps: Vec<PageAction>, assertions, dry_run, idempotency_key}) -> TransactionReceipt` — steps reuse the existing `PageAction` enum verbatim.
  2. Implement in engine-browser: run steps on the profile-bound Chrome; `dry_run` stops before the final submit-marked step and returns the filled-form screenshot for review.
  3. Receipts: screenshot + final DOM + confirmation text saved via the artifacts seam; a `transactions` table keyed by idempotency_key blocks double-submission.
  4. Confirmation gate: transactions enqueue as `pending_approval` jobs; `POST /transactions/{id}/approve` releases them (the job model's status machinery extends naturally).
  5. Pilot on one benign flow (e.g. newsletter/alert signup for a monitored grants portal) before anything consequential.
- **Risks**:
  - Acting under a user's identity is high-stakes: a wrong submit is not a bad row — hence dry-run-by-default, per-app allowlists of transactable domains, and human gates as non-optional architecture.
  - Legal/ToS exposure differs from reading; needs explicit per-target operator consent recorded with the receipt.
