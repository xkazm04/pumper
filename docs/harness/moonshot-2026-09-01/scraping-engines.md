# Scraping Engines — moonshot scout report (2026-09-01)

Scout: read-only subagent over the group's contexts; cards in the scan-sweep §4.10 form. Deck ids (N-numbers) are in [INDEX.md](INDEX.md).

## SE1 — API X-ray closes its own loop: auto-capture, auto-discover, auto-validate, router-learned

- deck item **N14**
- lens: `feature-scout` · size: **L** · gate: **contract** · effort 5 / impact 8 / risk 4
- contexts: tiered-fetcher, browser-engine, http-engine
- extends: M05 (API X-ray) — v1 shipped the capture, the discovery heuristic, the store, GET /recipes and the fetcher's api_recipe tier, but NO caller: the table is empty by construction

### Summary
M05 shipped every piece of the API X-ray except the one that runs it. `AppContext::xray` exists (crates/core/src/app.rs:506-529) and the fetcher already tries recipes ahead of every tier (crates/core/src/fetcher.rs:520-524), but the read route says it plainly: "no app calls `xray` yet, so this table stays empty until a discovery caller ships" (crates/server/src/routes/recipes.rs:10). No app sets `RenderRequest.capture_network` (grep over crates/apps: zero hits; only `readable` exposes `use_recipes`, crates/apps/readable/src/lib.rs:65), and `[recipes] enabled` defaults off (crates/core/src/config.rs:438-456). Make the loop autonomous: capture on every escalated render, discover against what the job extracted, auto-validate on the next fetch, and let the tier router learn `api_recipe` as a host's preferred tier — so a JS-heavy host is rendered in Chrome once and served as one governed JSON GET forever after.

### Description
Today the ladder is api_recipe → archive → http → browser → claude (fetcher.rs:147-163), and the router only ever learns one thing about a host: pin it to the browser after 3 http strikes (crates/core/src/tiers.rs:24, 141-156, `preferred` is only ever `'browser'`). That is precisely the host population where the browser tier is most expensive and where the X-ray is most likely to succeed — the page rendered because it called a JSON API, and the capture code is already there (crates/engine-browser/src/lib.rs:737-807, bodies pulled at 880-924, caps at 100-108).

The v2 wiring, all at existing seams:
1. **Capture by default on escalation.** In `Fetcher::fetch`'s browser branch (fetcher.rs:640-643) set `render.capture_network = true` whenever the render is an *escalation* (`skip_http` or an http loss already traced) and `[recipes] enabled`. Cost is bounded by the existing caps (30 calls / 256 KiB / 2 MiB, lib.rs:100-105).
2. **Discover at the chokepoint, not in apps.** `AppContext::fetch` (app.rs:380-420) cannot see extracted values, but the extractor and plugin apps can: add `AppContext::xray_after_extract(outcome, &records)` and call it from the extractor's per-URL path — one caller ships, the table fills. `discover_recipes` needs ≥3 matched values at ≥25% overlap (crates/core/src/recipes.rs:42-45), which is exactly what a rule-set extraction yields.
3. **Auto-validate as the default.** `[recipes] auto_validate` already promotes an unvalidated recipe on the first overlapping replay (fetcher.rs:1050-1053); flip it on with `enabled`, and cap unvalidated tries per host per day so a bad candidate cannot burn a governor slot every fetch.
4. **Teach the router a third state.** `tier_memory.preferred` gains `'api_recipe'` (tiers.rs:65-83): a validated recipe win records it, `max_failures` strikes (fetcher.rs:1081-1091) clear it. `GET /hosts` then shows which hosts are being served without a page fetch at all.
5. **Provenance.** The api_recipe outcome carries `text` = JSON with `html: None` (fetcher.rs:1064-1078); stamp `TierTrace.detail` with the recipe id (already done, 1062) AND surface `served_by: api_recipe` in the job receipt the way egress is (fetcher.rs:215-229), so an operator can see the browser tier's render count fall.

Registry subject: `model-routing` (tiered-fetcher) — the tier router is a routing policy learning from outcomes; this adds a cheaper tier to the learned set.

### Flow
- Add `xray_after_extract` to `AppContext`; call it from `crates/apps/extractor` after each page's records exist (records are the `extracted` argument `discover_recipes` wants).
- In `Fetcher::fetch`, set `capture_network` on escalated renders when recipes are enabled; thread `page.network` back on `FetchOutcome` (new optional field, serde-skipped when empty).
- Extend `tier_memory.preferred` to accept `'api_recipe'`; record on api_recipe wins; clear on un-validation. Migration adds a per-host `unvalidated_tries_today` guard.
- Flip `[recipes] enabled` + `auto_validate` defaults to true behind a config-doc note; keep `use_recipes` per-request.
- Add `pumper_recipe_replays_total{outcome}` to `/metrics` next to the egress counters (fetcher.rs:276-301 pattern).
- Docs: fetching.md tier ladder + extraction.md X-ray section; feature-doc-map unchanged.

### Expected impact
Operators running the extractor against JS-heavy listings see browser renders per host drop to ~1 and per-fetch latency drop from a Chrome render (seconds, one of 4 `max_concurrent_renders` slots) to one governed GET; the `pumper_recipe_replays_total{outcome="ok"}` counter and `GET /hosts` `preferred_tier: api_recipe` rows are the measurement. What could break: a recipe replaying a paginated endpoint with the *observed* page param returns page 1 forever — `replay_url` refuses unfilled placeholders but not stale filled ones (recipes.rs:81-97), so the `payload_overlaps` check must stay the acceptance bar and the strike ladder must stay short.

### Evaluation
Claim: performance - JS-heavy hosts served as one JSON GET instead of a Chrome render after the first escalation
Before: `api_recipes` row count is 0 on every deployment by construction (routes/recipes.rs:10 states no caller exists; grep `capture_network` in crates/apps = 0 hits); every router-pinned host pays a full render per fetch (fetcher.rs:626-655, tiers.rs:145-148)
After: for hosts whose page calls a same-site JSON API, `preferred_tier: api_recipe` in `GET /hosts` and a falling `pumper_browser_renders` share; instrument = the new replay counter split by outcome plus `TierTrace` engine distribution over a week of extractor jobs
Method: probe - grep for callers of `xray`/`capture_network`, read of the recipe tier and router code paths
Result: unmeasurable (needs the counter and a corpus of JS-heavy targets; the observatory's stored pages cannot replay network calls)
Gate: contract - `tier_memory.preferred` vocabulary and a new optional `FetchOutcome` field; `[recipes]` default flip is a config-visible change

### Evidence

```
crates/server/src/routes/recipes.rs:4-10  "Recipes are written by the discovery pass over `capture_network` renders (`AppContext::xray`) ... no app calls `xray` yet, so this table stays empty until a discovery caller ships."
crates/core/src/app.rs:506-529  pub async fn xray(&self, page: &RenderedPage, extracted: &[Value]) -> Result<(usize, usize)>  — defined, zero callers (grep `.xray(` across crates: none)
grep -rn capture_network crates/apps → 0 hits; crates/apps/readable/src/lib.rs:65 exposes only `use_recipes`
crates/core/src/config.rs:438-456  RecipesConfig default `enabled = false`
crates/core/src/fetcher.rs:520-524  recipe tier gated on `req.use_recipes || self.recipes_enabled`; :1021-1031 validated-only unless auto_validate; :1050-1053 record_success(validate)
crates/core/src/tiers.rs:24 STRIKE_LIMIT=3; :141-156 `preferred` only ever set to 'browser'
crates/engine-browser/src/lib.rs:100-108 capture caps; :737-807 capture listeners; :880-924 body pull
crates/core/src/recipes.rs:42-45 MIN_OVERLAP_SCORE/MIN_MATCHED_VALUES; :81-97 replay_url; :240-296 discover_recipes
```

## SE2 — Self-hosted agent loop: the Claude research tier drives pumper's own engines over MCP

- deck item **N15**
- lens: `moonshot-architect` · size: **XL** · gate: **policy** · effort 8 / impact 9 / risk 6
- contexts: claude-engine, tiered-fetcher, http-engine, archive-engine, browser-engine
- extends: M43/M29 (MCP gateway, pumper as MCP server) — v1 exposes pumper to external agents; nothing points pumper's OWN agent tier at it, so the paid tier still fetches the web with the CLI's built-in tools, outside every governor, cache, profile, archive and ledger the rest of the ladder runs under

### Summary
The Claude tier is the only tier whose web access pumper does not own. `ClaudeEngine::command` passes `--allowedTools WebSearch,WebFetch` from config (crates/engine-claude/src/lib.rs:119-122; default at crates/core/src/config.rs:1542), and the fetcher's tier-3 prompt is literally "Fetch {url} and extract its main textual content" (crates/core/src/fetcher.rs:776-782) — so the most expensive tier re-fetches the same URL the http and browser tiers already tried, from the same IP, with no politeness spacing, no cookie profile, no archive fallback, no http_cache, no VCR cassette, and no host-memory learning. Meanwhile `/mcp` already serves `search`, `query_dataset`, `list_apps`, `wait_job` and (gated) `fetch_readable` (docs/features/mcp.md § Tools). Point the subprocess at its own host: per-run `.mcp.json` → `http://127.0.0.1:<port>/mcp`, a synchronous `fetch` tool that executes `AppContext::fetch` *inside the calling job's budget and ledger*, and `--allowedTools mcp__pumper__*` instead of the CLI's own network tools. The research agent then stands on the whole substrate — datasets, search, archive, profiles, recipes — and every byte it pulls is metered and governed like every other tier.

### Description
What exists and what is missing, by file:
- `EngineSet.claude` is deliberately private so every model call goes through `AppContext::research` (crates/core/src/engine.rs:1243-1252; app.rs:430-484 meters, caches, clamps budget). That chokepoint governs *model* spend, but the *fetches the model makes* are invisible: `crates/core/tests/fetch_chokepoint.rs` pins every raw-engine call site in Rust, yet the CLI's `WebFetch` is a network path no test can see.
- The subprocess env is cleared to an allowlist (lib.rs:171-174) and it runs in an isolated cwd (lib.rs:80-94; fetching.md § subprocess working directory), which is exactly the shape a per-run `.mcp.json` + `--strict-mcp-config` needs: pumper writes the config file the way it already writes `--append-system-prompt-file` scratch files (lib.rs:140-145, `ScratchFile`).
- `/mcp` is stateless streamable-HTTP with `allow_enqueue` as a second switch (mcp.md § Enabling) — but its only fetch is `fetch_readable`, which *enqueues a job* and returns an id. An agent mid-turn needs a synchronous answer: a `fetch` tool that runs the tiered fetcher directly under a **job-scoped token** (issued when the research job spawns, carried in `.mcp.json` headers, expiring with the run) so the fetch is metered against `ctx.job_id`'s `cost_events`, honours `budget_usd`, and appears in the receipt as a tier-3 sub-fetch.
- Research already resumes sessions and checkpoints turns (crates/apps/research/src/lib.rs:5-16, 42-90); MCP tool calls are just turns, so durable execution carries over unchanged.
- Payoff beyond hygiene: the agent can `query_dataset` and `search` what pumper already holds (search.md § MCP `search` tool), consult `GET /hosts` weather before hammering a host, use the archive tier for historical questions (`archive_max_age`), and run under a session-vault `profile` — none of which `WebFetch` can do. `deep_research` becomes research over *your* corpus plus the live web, not the CLI's view of the web.

Registry subjects: `prompt-assembly` and `model-routing` (claude-engine); `usage-limit-governance` (llm-observability) — the point is that spend and egress become one ledger.

### Flow
- Add `POST /mcp` tool `fetch {url, strategy?, profile?, archive_max_age?, to_markdown}` executing `Fetcher::fetch` synchronously, bounded by `[http] total_budget_secs`/`render_budget_secs`; attribute to the job named by the bearer token; refuse without a token when `allow_enqueue` is false.
- Job-scoped tokens: minted in `AppContext` for research jobs, stored in memory with expiry, validated by the MCP route; loopback-only by default.
- `ClaudeEngine::command`: when `[claude] use_pumper_mcp = true`, write `.mcp.json` (ScratchFile) with the loopback URL + token header, pass `--mcp-config <file> --strict-mcp-config`, and rewrite `allowed_tools` to `mcp__pumper__*` (keep `WebSearch` optional — search has no pumper equivalent).
- Meter sub-fetches into the calling job's `cost_events` with `detail: "claude_subfetch"`; extend the receipt's `tiers` block and the VCR cassette so replayed research does not go live (runtime.md:179 bypass list stays honest).
- Extend `crates/core/tests/fetch_chokepoint.rs` with a test that the research prompt path no longer advertises `WebFetch` when the switch is on.
- Docs: fetching.md (claude tier), mcp.md (fetch tool + job tokens), runtime.md (replay).

### Expected impact
Every deep-research job's web traffic becomes visible in the receipt and `/metrics` (today: zero rows), obeys the governor's per-host penalties and the tier router, and can read the datasets already scraped — measured as `cost_events` rows with `detail=claude_subfetch` per research job and the disappearance of ungoverned egress during research runs (compare target-host access logs before/after in the smoke harness). What could break: a synchronous fetch tool holds an MCP request open for up to a render budget; token leakage would let any local process fetch on a job's budget, so tokens must be single-job, loopback-bound and short-lived.

### Evaluation
Claim: resilience - the paid tier's fetches join the governed, metered, cached ladder instead of bypassing it
Before: `allowed_tools` default `["WebSearch","WebFetch"]` (config.rs:1542); tier-3 prompt asks the CLI to fetch the URL itself (fetcher.rs:776-782); no `--mcp-config` anywhere in engine-claude (grep: 0); research-job receipts carry model cost only — sub-fetch count is 0 by construction
After: research receipts list N `claude_subfetch` cost events with tier traces; instrument = `cost_events` per research job + the egress counters; a VCR cassette of a research run replays without network
Method: probe - read of ClaudeEngine::command argv assembly, the MCP tool table, and the research app's checkpoint loop
Result: unmeasurable (needs the fetch tool and one recorded research run to count sub-fetches)
Gate: policy - loopback MCP + job-scoped tokens are a security boundary; `[mcp] enabled` must be on for the research tier to work in this mode, which is a deployment decision

### Evidence

```
crates/engine-claude/src/lib.rs:100-134  argv assembly: `-p --output-format json`, `--model`, `--effort`, `--max-budget-usd`, `--bare`, `--dangerously-skip-permissions`, `--allowedTools <cfg.allowed_tools>`, `--max-turns`, `--resume`, `--json-schema` — no `--mcp-config`
crates/core/src/config.rs:1542  allowed_tools: vec!["WebSearch".into(), "WebFetch".into()]
crates/core/src/fetcher.rs:776-782  default tier-3 prompt: "Fetch {url} and extract its main textual content as clean Markdown"
crates/engine-claude/src/lib.rs:171-174  cmd.env_clear(); cmd.envs(allowed_env(CLAUDE_EXTRA_ENV))  — subprocess sees only an allowlist
crates/engine-claude/src/lib.rs:140-145, 190-221  ScratchFile pattern for --append-system-prompt-file (reusable for .mcp.json)
crates/core/src/engine.rs:1243-1252  EngineSet.claude private; app.rs:430-484 research chokepoint (cache, budget clamp, ledger)
docs/features/mcp.md § Tools  search / query_dataset / list_apps / wait_job / enqueue_job / fetch_readable (enqueue, async) / deep_research
crates/apps/research/src/lib.rs:5-16, 42-90  chunked turns + session resume + checkpoint state
docs/features/runtime.md:179  REPLAY_BYPASS_APPS — traffic outside the chokepoint is invisible to VCR in both directions
```

## SE3 — Runnable dynamic apps: component-model host with governed fetch/storage imports

- merged into **N09** (WASM apps v2: hot-loadable ScrapeApps with AppContext host imports)
- lens: `innovation-catalyst` · size: **XL** · gate: **contract** · effort 9 / impact 9 / risk 7
- contexts: wasm-plugin-host, tiered-fetcher, http-engine
- extends: M28 (dynamic apps as hot-loadable WASM) — v1 is discovery + listing ONLY; every dynamic app is `runnable: false` and the code names the missing slice verbatim

### Summary
The registry already lists `.wasm` apps from `[plugins] app_dir` and rejects every enqueue with one constant: "running one requires the component-model host (typed WIT world, async host imports for fetch/storage, fuel + wall-clock + spend budgets across the boundary) — the next slice" (crates/server/src/registry.rs:186-192; engine-wasm mirrors it at crates/engine-wasm/src/lib.rs:701-706). The current host is a core-module sandbox with an *empty* linker (lib.rs:545 `Linker::new(engine)`; "Plugins have no imports", lib.rs:4) whose work runs on an uncancellable blocking thread (lib.rs:304-334; trigger-plugins.md § Sandbox limits). Build the slice: a WIT world `pumper:app` whose imports are exactly the metered `AppContext` seams — `fetch` (tiered, governed, budgeted), `upsert_many`, `checkpoint`, `research` — so a scraping use case becomes a file dropped into a data dir instead of the four-step crate contract (CLAUDE.md § Architecture), and the provisioner's proposals (which today emit `app = ""` and stop, docs/features/apps.md:148-150) finally have somewhere to land.

### Description
Why the substrate makes this cheap now:
- The **budget and politeness boundary already exists as Rust APIs**: `AppContext::fetch` (crates/core/src/app.rs:380-420) meters cost, teaches the router, records VCR; `require_budget`/`clamp_to_headroom` (app.rs:460-462) bound spend; `HttpEngine::send` acquires the governor per attempt (crates/engine-http/src/lib.rs:586-588). A host import that *calls these functions* inherits every guard for free — the guest cannot reach the network except through the same chokepoint `fetch_chokepoint.rs` pins for Rust apps.
- **Manifests are already the contract**: `describe()` returns `params_schema`, description, schedule (registry.rs:224-240); `GET /apps?format=tools` and the MCP `list_apps` tool render them (mcp.md § Tools). A runnable dynamic app is the same manifest plus a `run` export.
- **Admission is already solved for sync work** (`run_admitted`, lib.rs:304-334: the permit travels with the blocking closure). Async host imports need the wasmtime `async` engine config instead — fuel is still deterministic (`consume_fuel`, lib.rs:194) and can yield every N units so a long-running guest cannot pin a runtime thread; wall-clock stays `[worker] job_timeout_secs`.
- **Telemetry and failure classes carry over**: `PluginFailure::{Trap,MalformedOutput,MissingExport,Unknown,Host}` (extraction.md § Failure classes) and the per-plugin fuel gauges (lib.rs:75-115) become per-app gauges; the observatory (extraction.md § Observatory mode) can replay a dynamic app's extraction half against stored pages exactly as it replays plugins today.

Scope discipline: v2 imports are `fetch`, `upsert_many`/`sync_many`, `save_artifact`, `checkpoint`/`restore`, `log`, `research` (budget-clamped). No raw sockets, no filesystem, no clock beyond `now()`. Static apps always win a name clash (registry.rs:198-218) — keep that rule.

Registry subjects: `companion-runtime` (wasm-plugin-host) and `usage-limit-governance` — the entire card is "same budgets across a trust boundary".

### Flow
- Define `wit/pumper-app.wit`: world `app` exporting `describe`, `run(params: string) -> result<string, string>` and importing the seams above with typed results (`fetch-outcome` mirrors `FetchOutcome`'s serde shape).
- Engine: second wasmtime `Engine` built with `async_support(true)` + `consume_fuel` + `epoch_interruption` for the app host; keep the existing core-module host untouched for extract/predicate/transform plugins.
- Host impl: a `HostCtx { ctx: AppContext }` per run; each import awaits the corresponding `AppContext` method; errors map to the guest as `result<_, string>` with the typed class preserved in the job error.
- Registry: `dynamic_entry` gains `runnable: true` when the module links against the world; worker dispatch adds a `DynamicApp: ScrapeApp` adapter so the queue, scheduler, receipts, triggers and SSE need no changes.
- Provisioner: emit a `plugins-src/app-template` build (extractor rule set compiled into a guest) as the proposal's `promote` output, closing M44's "stops at a proposal" gap.
- CI: `just plugins-verify` extended to build one sample dynamic app and run it through the e2e worker test.
- Docs: extraction.md (plugin kinds), apps.md (dynamic apps), ONBOARDING.md §5 ("Path C").

### Expected impact
A new data source stops being a Rust crate + registry line + release; it is a `.wasm` in `data/apps/` plus a catalog row, hot-loaded via `POST /plugins/reload`. Measured as time-to-first-record for a new source (today: crate scaffold + compile + restart; after: drop file + enqueue) and as the count of runnable dynamic apps in `GET /apps`. What could break: an async guest that awaits a fetch holds an instance (and its memory cap) across the await — `max_concurrent × max_memory_mb` must be enforced on *live instances*, not calls, or the bound documented in trigger-plugins.md § Sandbox limits stops being real.

### Evaluation
Claim: user - a scraping use case ships as a hot-loadable artifact with the same budgets and politeness as a compiled app
Before: `GET /apps` lists dynamic apps with `runnable: false` and `requires: ["host:component-model"]` (registry.rs:224-240); enqueue is rejected with DYNAMIC_NOT_RUNNABLE_REASON; the linker has zero imports (lib.rs:545)
After: a sample dynamic app runs end to end through the worker with `cost_events` and `TierTrace`s identical in shape to a Rust app's; instrument = the e2e worker suite plus `GET /apps` runnable count
Method: probe - read of registry dynamic slice, wasm host linker/admission/fuel code, and the AppContext chokepoints an import would call
Result: unmeasurable (no execution path exists today; the first measurable artifact is the sample app passing the worker e2e)
Gate: contract - a WIT world is a public ABI that must be versioned from day one; the manifest gains a `world` field

### Evidence

```
crates/server/src/registry.rs:186-192  DYNAMIC_NOT_RUNNABLE_REASON: "running one requires the component-model host (typed WIT world, async host imports for fetch/storage, fuel + wall-clock + spend budgets across the boundary) — the next slice"
crates/engine-wasm/src/lib.rs:701-706  "there is NO execution path for these modules"
crates/engine-wasm/src/lib.rs:4  "Plugins have no imports, so no ambient authority"; :545 `let linker: Linker<StoreLimits> = Linker::new(engine);`
crates/engine-wasm/src/lib.rs:194 consume_fuel(true); :304-334 run_admitted (permit travels with the blocking work); :560-601 per-call Store with StoreLimits
crates/core/src/app.rs:380-420 AppContext::fetch meters, learns tier, records VCR; :460-462 budget clamp
crates/engine-http/src/lib.rs:586-588 governor.acquire per attempt inside the engine
docs/features/apps.md:148-150  provisioner emits `app = ""` and no code; every step that makes a source real is a human edit
CLAUDE.md § Architecture  adding an app is a four-step contract (crate → impl → registry.rs → catalog)
```

## SE4 — Fabric v2: browser-capable satellite nodes, profile affinity and a cluster governor

- deck item **N17**
- lens: `integration-planner` · size: **XL** · gate: **contract** · effort 8 / impact 7 / risk 6
- contexts: remote-engine, browser-engine, tiered-fetcher, http-engine
- extends: M17 (distributed fetch fabric) + M01 (host weather) — v1 proxies the live-HTTP tier only, keeps profiled fetches local, and states that cluster-wide politeness is deliberately out of scope

### Summary
The fabric substitutes egress for exactly one tier. `RemoteEngine` implements `HttpClient` and is wired into the HTTP position only (crates/core/src/fetcher.rs:413-420, 504-506); the browser tier always renders on the coordinator (fetcher.rs:640-655 `self.browser.render`); there is no `RenderRequest` anywhere in `crates/engine-remote` (grep: 0). But the learned router pins hosts to the browser precisely when http keeps losing — blocked, rate-limited, JS-walled hosts (crates/core/src/tiers.rs:141-156) — so the hosts that most need a different IP are served by the one tier the fabric cannot move. Two more v1 refusals compound it: profiled fetches must stay local because the cookie jar lives on the coordinator's disk (crates/engine-remote/src/lib.rs:135-140), and the coordinator's governor never sees a proxied target at all (lib.rs:7-15, 89-91: "cluster-wide governor state is deliberately OUT of this v1"). v2: a `/render-proxy` twin of `/fetch-proxy`, node capability advertisement, profile→node affinity with jar/profile-dir sync, and observations riding back in the envelope so the coordinator's governor becomes the shared brain M17's own design step 4 described.

### Description
Build on what is there:
- **Envelope discipline is proven.** `ProxyResponse` mirrors `HttpResponse` field-for-field (lib.rs:265-287); the target-echo binding (`REMOTE_TARGET_HEADER`, fetcher.rs:206-213) and the node attribution header (fetcher.rs:196-204) are the exact pattern a render envelope needs: `RenderedPage` is already serde (`network`, `action_outcomes`, `nav_timed_out`… crates/engine-browser/src/lib.rs:967-977).
- **Failover and cooldown are generic** (lib.rs:383-431: cursor, cooldown, `MAX_NODE_ATTEMPTS`, end-to-end deadline) — lift them into a `NodeSet` shared by `RemoteEngine` and a new `RemoteBrowser: Browser`, and add `GET /fetch-proxy/capabilities` so a node declares `{http, browser, profiles: [...]}`; a render is only routed to browser-capable nodes.
- **Profile affinity.** `must_serve_locally` exists because nothing replicates jars (lib.rs:112-140). Pin a profile to one node (`[remote] profile_affinity = {acme = "http://node-b"}`) and let the serving node own the jar; the coordinator's `x-pumper-anonymous-profile` marker (fetching.md § three rules) already makes a logged-out peer visible, so the honesty rails hold. Browser profiles (a Chrome `user-data-dir`, lib.rs:19-31) pin the same way — a node with the profile is the only one that renders it.
- **Cluster governor.** The serving node already knows the target's status and `Retry-After` (it learned them in its own governor, lib.rs:7-10). Return `{host, status, retry_after_ms, penalty_ms}` in the envelope; the coordinator applies `Governor::raise_penalty` (crates/core/src/governor.rs:259-286, built for M01 imports: raise-only, capped) so an escalation to the local browser tier or a fallback to local egress starts from the cluster's knowledge, not zero. The host-weather bundle shape (`WeatherEntry`, tiers.rs:383-402) is the wire format; nodes push it on a cadence instead of an operator exporting/importing by hand (routes/host_weather.rs:82, 165 are the only movers today).
- Receipts already carry `cost.egress = [{node, calls}]` (fetching.md § Egress attribution); extend with `renders`.

Registry subjects: `device-pairing` and `pipeline-dag` (remote-engine) — pairing = capability advertisement + secret; the DAG is the ladder spanning nodes.

### Flow
- Extract `NodeSet` (nodes, cursor, cooldown, attempt budget, deadline) from `RemoteEngine`; add `capabilities` probe with a cached TTL.
- Serving side: `POST /render-proxy` runs `Browser::render` locally under the same secret + target policy (`blocked_target`, lib.rs:164-194), echoes the target, stamps `x-pumper-remote-node`; body cap = `max_html_bytes`.
- `RemoteBrowser: Browser` in engine-remote; `Fetcher::with_remote_browser`; browser tier attribution via the same `remote_egress` reader (fetcher.rs:262-267).
- Profile affinity map in `[remote]`; `must_serve_locally` becomes "must serve on the profile's node"; `GET /profiles` reports `node`.
- Envelope gains `observations: [{host, status, retry_after_ms, penalty_ms}]`; coordinator feeds `raise_penalty` + `TierMemory::save_penalties` (tiers.rs:225-236).
- Node-to-coordinator weather push: nodes POST their `export_weather` bundle to the coordinator's existing import route on `[remote] weather_push_secs`.
- Docs: fetching.md § Remote fetch fabric, deployment.md.

### Expected impact
Operators with one banned coordinator IP get browser renders and logged-in sessions from peer IPs, and the `pumper_remote_egress_fetches{served_by}` split gains a `renders` series; router pins stop concentrating unproxied traffic on the hosts most likely to ban. What could break: a render envelope is MBs of HTML per call across the wire, so `[remote] max_body_bytes` and the render budget must both bound it; profile affinity makes a node a single point of failure for that login — the fallback must be *refuse*, never *anonymous*.

### Evaluation
Claim: resilience - the tiers that get blocked can egress from peers; the cluster learns politeness once
Before: engine-remote proxies HttpRequest only (no RenderRequest in the crate); every browser render egresses from the coordinator (fetcher.rs:655); profiled fetches forced local (lib.rs:135-140); coordinator governor blind to proxied targets (lib.rs:7-15)
After: `cost.egress` on a receipt lists renders by node; `GET /hosts` penalties on the coordinator reflect peer-observed 429s; instrument = the egress metric split by tier and a two-node e2e (extend crates/server/src/e2e for the fabric) asserting a pinned host's render left via a peer
Method: probe - grep for RenderRequest in engine-remote, read of the ladder's browser branch and the remote module's stated v1 limits
Result: unmeasurable (no browser proxy exists; first measurement is the two-node e2e)
Gate: contract - a second proxy envelope and a `capabilities` document are wire contracts between versions of the binary

### Evidence

```
crates/core/src/fetcher.rs:413-420  `remote: Option<Arc<dyn HttpClient>>` — "the live-HTTP tier routes through this client"; :504-506 live_http(); :640-655 browser tier calls `self.browser.render(render)` unconditionally
grep -rn RenderRequest crates/engine-remote → 0 hits
crates/engine-remote/src/lib.rs:7-15  "It is not polite in the coordinator's own governor, which never sees the target at all"; :89-91 "Cluster-wide governor state is deliberately OUT of this v1"
crates/engine-remote/src/lib.rs:135-140  must_serve_locally: profiled fetches stay on the coordinator
crates/core/src/tiers.rs:141-156  3 http strikes → preferred='browser' (the unproxied tier)
crates/core/src/governor.rs:259-286  raise_penalty — raise-only, capped, built for imported intel
crates/core/src/tiers.rs:383-402  WeatherEntry wire shape; crates/server/src/routes/host_weather.rs:82,165 export/import are the only movers
docs/features/fetching.md:329  browser-tier proxy auth unsupported (Chrome --proxy-server limitation) — a browser node sidesteps it
crates/engine-remote/src/lib.rs:265-287  ProxyResponse ↔ HttpResponse mirror; :383-431 cooldown/attempt/deadline machinery
```

## SE5 — Index-time enrichment as a plugin hook: typed entity fields without schema wipes

- deck item **N11**
- lens: `innovation-catalyst` · size: **L** · gate: **contract** · effort 6 / impact 7 / risk 4
- contexts: search-engine, wasm-plugin-host
- extends: M14 (entity-typed index) + M15 (WASM everywhere) — M14 shipped two hard-coded regex fields; M15 shipped predicate/transform slots and declares no index-time slot

### Summary
Entity enrichment is frozen at exactly two fields, and adding a third is a destructive event. `enrich.rs` extracts `amount` (USD only) and `event_date` with regexes and says "Org/geo extraction is deliberately out of scope (regex cannot do it honestly; NER is not available here)" (crates/engine-search/src/enrich.rs:8-9); `build_schema` is "the single authority" and "adding a field here IS the schema-version bump — an index built before it … is wiped empty on open, and must be rebuilt via search-backfill" (crates/engine-search/src/lib.rs:145-150, 350-365). The trigger plugin host explicitly has "only `predicate` and `transform` slots" (docs/features/trigger-plugins.md § Known gaps). Meanwhile every doc already passes through `enrich_docs`, a pure, lock-free pre-index stage on its own blocking task (lib.rs:395-409) — a natural hook. Make enrichment a third plugin kind (`enricher`) writing into one tantivy JSON object field with per-key fast values, so a deployment can add `currency`, `org`, `region`, `naics`, `czech_ico` — or an app-specific field — by installing a `.wasm`, never by wiping the corpus.

### Description
- **Hook point exists.** `TantivyIndex::index` enriches before taking the writer lock (lib.rs:384 note in search.md; code at lib.rs:395-409). An `enricher` plugin call per doc is the same `Plugins::run(name, doc_text, params)` envelope every hook already uses (crates/engine-wasm/src/lib.rs:17-27: "The host is a general UDF runtime … `describe()` `kind`"), under the same admission gate and fuel telemetry (lib.rs:304-334, 75-115). Fail-open like trigger hooks: a trapping enricher yields no field, matching the "no match = no field" doctrine (enrich.rs:5-9).
- **Schema stability.** tantivy's JSON object field type stores arbitrary `{key: value}` with typed fast columns per key; one `entities` JSON field added *once* replaces per-entity schema bumps. `GET /search` gains generic `entity.<key>:<op>:<value>` filters mirroring the dataset `?filter=` grammar (datasets `$.path:op:value`, mcp.md § query_dataset), so `search.md`'s two hard-coded params become one grammar. Keep `amount`/`event_date` as built-in enrichers for compatibility.
- **Fixes named gaps.** search.md § Known gaps: "`amount` is US-dollar only", "both entity fields are document-level, not per-item". An enricher can emit arrays (`amounts: [...]`) and currencies; the Czech apps (`mpsv-*`, `smlouvy`) get CZK/IČO fields the regex never will.
- **Observatory covers it.** The plugin observatory replays plugins over stored pages and scores drift (extraction.md § Observatory mode); an enricher is replayable the same way, so entity-extraction rot becomes measurable per site.
- **Optional LLM NER** stays behind the research chokepoint: an `enricher` may be `kind: "research"` with a budget cap, calling `AppContext::research` with a `json_schema` (crates/core/src/engine.rs:1145-1147) — only if an operator opts a dataset in.

Registry subject: `search` (search-engine) — the golden path is structured predicates over unstructured text; this makes the predicate set open rather than compiled-in.

### Flow
- Add `entities` JSON field to `build_schema` (one last schema bump, documented) with fast columns enabled.
- `SearchDoc` gains `entities: Map<String, Value>`; `enrich_docs` runs built-ins then configured enrichers from `[search] enrichers = ["..."]`, merging keys (built-ins win on collision).
- Enricher ABI = extract_v2 envelope; output contract `{"entities": {key: scalar|array}}`; `kind: "enricher"` in `describe()`; `GET /plugins?kind=enricher`.
- Query: parse `entity=<key>:<op>:<value>` (repeatable) into tantivy JSON-path range/term queries; expose on MCP `search` through the shared `build_search_request` (search.md § The MCP search tool).
- `search-backfill` re-enriches; observatory gains `enricher` replay class.
- Docs: search.md (fields, grammar), trigger-plugins.md (kinds), extraction.md (plugin kinds).

### Expected impact
Users filter search by any entity a plugin can name (currency-aware amounts, organisations, regions, per-item values) and operators add fields without a corpus wipe; measured as fields-without-schema-bump (today 0) and query coverage on non-USD datasets (today `amount_gte` matches 0 CZK documents by construction, enrich.rs MONEY_RE requires `$`/`usd`). What could break: per-doc plugin calls on the index path add latency to job completion — bound with fuel presets and batch the calls; a JSON field cannot be sorted as cheaply as a dedicated fast column, so keep `amount`/`event_date` native.

### Evaluation
Claim: quality - open-ended typed filters over the corpus; schema stability for enrichment
Before: 2 entity fields, USD-only, document-level (search.md § Known gaps); adding a field wipes the index (lib.rs:145-150, 350-365); enrichment hook slots = 0 (trigger-plugins.md § Known gaps)
After: N enrichers installed without a rebuild; instrument = `GET /plugins?kind=enricher` count and per-enricher fuel telemetry plus `/search` hit counts for `entity=currency:eq:czk`
Method: probe - read of build_schema/schema_is_current, enrich_docs, enrich.rs doctrine, and the wasm host's kind convention
Result: unmeasurable (needs the field and one enricher; the smoke harness could then count hits on a seeded CZK dataset)
Gate: contract - one schema bump (index wipe + backfill, already the documented path) and a new query grammar on `/search` and the MCP tool

### Evidence

```
crates/engine-search/src/enrich.rs:5-9  "Doctrine: no match = no field … Org/geo extraction is deliberately out of scope (regex cannot do it honestly; NER is not available here)"
crates/engine-search/src/enrich.rs:33-38  MONEY_RE requires `\$` or `usd`
crates/engine-search/src/lib.rs:145-150  "adding a field here IS the schema-version bump — an index built before it fails the equality check below, is wiped empty on open"; :350-365 SchemaDrift → drain_dir + recreate
crates/engine-search/src/lib.rs:395-409  enrich_docs: pure, lock-free, own blocking task — the hook point
docs/features/trigger-plugins.md § Known gaps  "Only `predicate` and `transform` slots exist"
crates/engine-wasm/src/lib.rs:17-27  host is a general UDF runtime; `describe()` kind convention; :304-334 admission; :75-115 telemetry
docs/features/search.md § Known gaps  amount USD-only; entity fields document-level
docs/features/extraction.md § Observatory mode  differential replay of plugins over stored pages
```

## SE6 — Transact v2: approval ledger and live submit under the same evidence contract

- merged into **N01** (Transact v2: approval-gated live actions with a transactions ledger and agent tools)
- lens: `business-strategist` · size: **XL** · gate: **irreversible** · effort 8 / impact 8 / risk 9
- contexts: browser-engine, http-engine
- extends: M06 (Transact) — v1 is structurally dry-run only; the docs name the missing slice: "pending-approval transactions + an explicit approve endpoint + dedup on idempotency_key"

### Summary
`transact` runs every reversible step, probes the submit target, and stops: "`submit_action` is deliberately NOT appended — stop-before-submit is structural" (crates/engine-browser/src/lib.rs:980-1020, 1013-1016), `submit: true` is a 422 at enqueue (docs/features/apps.md:54), and the doc states what live submission needs (apps.md:44). The evidence bundle is already written to be falsifiable — `submit_target {found, visible, enabled}`, honest step outcomes, the profile the flow ran as (apps.md:64-70; lib.rs:1044-1082) — which is exactly the artifact an approval should bind to. v2 adds the ledger: a `transactions` dataset with `pending_approval → approved → submitted | rejected | expired`, an approve endpoint that must quote the bundle's hash, a second engine call that replays the flow and executes `submit_action` only when the re-probed page matches the approved evidence, and post-submit evidence. Same profile, same idempotency key, one submission per key — ever.

### Description
- **Engine seam.** `Browser::transact` (crates/core/src/engine.rs:1213-1234) gains a sibling `execute(req, approval: ApprovalToken)` whose executor appends `submit_action` to `render.actions` only after a fresh probe (`transact_probe_js`, engine.rs:963) agrees with the approved `submit_target` and `filled_fields`. The action machinery already reports `Ok/Partial/Missed` per step (lib.rs:1131-1279), so a drifted page yields a `submit_blocked: probe_mismatch` outcome, not a click.
- **Ledger.** Follow the provisioner lifecycle pattern: every transition is an `upsert_stamped` revision on a record keyed by `idempotency_key` (apps.md:158-166), so watches, triggers, webhooks and the SDK see `transaction.approved`/`submitted` as ordinary dataset deltas; the two-switch MCP posture (`enabled` vs `allow_enqueue`, mcp.md § Enabling) becomes `[transact] allow_live = false` by default plus per-approval operator identity.
- **Idempotency is real, not advisory.** `idempotency_key` is required and non-blank today (apps.md:46) but guards nothing; v2 makes the `submitted` revision the lock: a second approve on a submitted key is a 409; a crashed executor re-claims the job (M23 checkpoints, runtime.md § Durable execution) and finds the key already `submitted` → no replay. `transact-retry-safety` (shipped) already stops retries of the dry run; the live path must be one attempt, terminal on any error.
- **Evidence after.** Post-submit DOM (truncate-and-flag, lib.rs:1027-1038), final URL, and the confirmation selector outcome land in the same bundle shape; screenshots stay out (README still-open, excluded).
- **Profile boundary.** Live actions run only under an existing profile (`require_existing_profile`, lib.rs:997-1000) and never through the fabric (`must_serve_locally`, crates/engine-remote/src/lib.rs:135-140) — an approval names *who* acts.

Registry subject: `browser-credential-boundary` (browser-engine) — the whole card is keeping the credential, the approval and the action on one identity.

### Flow
- Dataset `transact/transactions` keyed by `idempotency_key`; state machine + `bundle_sha256` + `approved_by` + `expires_at` ([transact] approval_ttl_secs).
- `POST /transactions/{key}/approve {bundle_sha256}` and `/reject`; refuse on hash mismatch, expiry, or non-pending state; `[transact] allow_live` gates the route's existence (like `allow_enqueue`).
- `Browser::execute` in core (default: `Error::Transact` refusal, same terminal shape as `unsupported_transact`, engine.rs:1205-1209); engine-browser implements probe-then-submit.
- The `transact` app gains `mode: "execute"` reachable only from the approve route (trigger-fired or user enqueue of `execute` is refused), one attempt, no retries.
- Receipt + webhook event `transaction.submitted` with the post-submit bundle path.
- Docs: apps.md § transact, http-api.md, events-webhooks.md.

### Expected impact
The product moves from "prepare a submission for a human" to "a human approves, pumper submits under the approved identity and proves what it did" — form filings, portal submissions, renewals — with a ledger any agent (MCP) can read but never advance without the approve route. Measured as approved→submitted conversions and `submit_blocked: probe_mismatch` counts (drift caught before a click). What could break: a page that changes between approval and execution is the entire risk surface; the probe-match rule must be strict (selector found + enabled + same label + same filled values) and any mismatch must end as `blocked`, never as a retry.

### Evaluation
Claim: user - governed live actions on the web with an auditable approval trail
Before: 0 live submissions possible (no code path: lib.rs:1013-1016; `submit: true` = 422, apps.md:54); `idempotency_key` required but enforces nothing
After: one submission per key, each with pre- and post-submit bundles in the revision history; instrument = the transactions dataset's state counts and a fixture-site e2e (the transact app already has an in-process fake engine, crates/apps/transact/src/lib.rs:310) asserting probe-mismatch blocks the click
Method: probe - read of the engine's transact path, the evidence assembler, the app doc's stated missing slice, and the provisioner lifecycle pattern
Result: unmeasurable (irreversible actions cannot be trialled against real targets; the fixture e2e is the only honest instrument)
Gate: irreversible - a submit cannot be undone; default-off, operator-gated, single-attempt by construction

### Evidence

```
crates/engine-browser/src/lib.rs:980-1020  transact(): "the flow STOPS at that state — `req.submit_action` is never handed to the executor; there is no code path here that could run it"; :1013-1016 submit_action deliberately NOT appended
docs/features/apps.md:44  "live submission needs the human-approval slice: pending-approval transactions + an explicit approve endpoint + dedup on `idempotency_key`"; :54 submit:true is a 422; :84 Known gaps: no live submit
crates/engine-browser/src/lib.rs:1044-1082  evidence_from_render: dry_run: true, would_submit verbatim, submit_target probe, honest step counts
crates/core/src/engine.rs:1213-1234  Browser::transact default = terminal Error::Transact; :963 transact_probe_js
docs/features/apps.md:158-166  provisioner lifecycle: planned → validated | failed → promoted as upsert_stamped revisions (the ledger pattern)
docs/features/mcp.md § Enabling  two-switch posture (`enabled`, `allow_enqueue`) to copy for `allow_live`
crates/engine-browser/src/lib.rs:997-1000 require_existing_profile before any Chrome work; crates/engine-remote/src/lib.rs:135-140 profiled work never leaves the coordinator
```

