# Moonshot Scan — Content & Research Apps (2026-07-30)

> Total: 4 moonshots across 2 contexts.

## Context: Extraction, Crawl & API Watch

### 1. The Web Reliability Index — a longitudinal machine-readability observatory
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: months
- **Files**: crates/apps/crawl/src/lib.rs, crates/apps/extractor/src/lib.rs
- **What it is**: Every crawl already tallies per-host outcomes (`MeteringHttpClient` — fetch counts, bot-wall/transport losses fed to `learn_tier`), and every extraction already produces structured health verdicts (`observe()` → state transitions, scores, diagnoses) plus `FetchHealth` ok-rates and `worst_fields` drift signals. Today that telemetry is consumed once (router learning, one job result) and discarded. This moonshot persists it into a durable, per-host time-series dataset — bot-wall behavior by tier, robots posture, markup-drift frequency, conditional-GET support (etag/last_modified already stored per page), gone-rates — and aggregates it into a queryable "scrapeability index" of every host the platform has ever touched.
- **Why it's a moonshot**: It turns exhaust into a category-defining data product: an SSL-Labs/Down-Detector for machine readability. No scraping vendor publishes longitudinal "how does this host treat automated clients" intelligence; agents, scrapers, and data teams would query it before ever sending a request. Internally it makes every new crawl 10x cheaper to plan (pick tier, cadence, and politeness from evidence, not trial). Externally it is a subscription feed that grows automatically with normal platform use — zero marginal collection cost.
- **Differentiation**: Nearest priors are "Tamper-evident provenance ledger for every fetch" (per-fetch integrity attestation, not aggregate host intelligence) and "Sell API-change intelligence as a subscription feed" (content diffs of watched API docs, not fetch-layer behavior). No prior idea touches aggregating per-host tier/health/drift telemetry across all crawls into a longitudinal index.
- **Path**:
  1. In `Crawl::run`'s existing tally flush (the O(hosts) loop that already calls `ctx.meter`/`ctx.learn_tier`), also `upsert_many` one record per host per run into a `host_reliability` dataset — fields it already holds: fetches, http_lost, plus stats.failed_by_host/skipped_botwall/robots_fetch_failures from `CrawlStats`.
  2. In `extractor::observe`, persist each non-None verdict (source_id, state, previous_state, score, diagnosis, fetch ok-rate, worst_fields) into the same dataset family instead of only logging the transition.
  3. Add a rollup job (scheduled app, reusing the trigger-DAG machinery) that folds runs into per-host aggregates: bot-wall probability by tier, drift events/month, 304-support ratio, mean time-to-markup-break.
  4. Expose it through the already-shipped generic `?filter=` surface on /datasets + a `GET /reliability/{host}` convenience route.
  5. Package as a delta-feed product (webhooks already exist with DLQ) — "your watched hosts' readability changed".
- **Risks**:
  - Index quality is bounded by crawl diversity — a few dozen hosts is an internal tool, not a product; needs a deliberate breadth-crawl program to seed coverage.
  - Publishing per-host bot-wall intel could be seen as adversarial by target sites; needs a policy line (aggregate stats, no evasion recipes).

### 2. Retroactive schema evolution — the crawl archive as a private Common Crawl
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/apps/crawl/src/lib.rs, crates/apps/extractor/src/lib.rs, crates/apps/plugin/src/lib.rs
- **What it is**: The crawler already streams full page bodies to per-job artifact dirs and fingerprints every page (simhash, content_chars, etag, revisit/gone lifecycle) into the `pages` dataset; extractor and plugin already have a `source` mode that re-reads those stored bodies with zero re-fetch. But only the *latest* body per URL is reachable — each revisit's artifact overwrites the URL's meaning, and history is lost. This moonshot makes the archive append-only and versioned (snapshot rows keyed `url@revision`, artifact per revision, dedup via the existing simhash so unchanged revisits cost nothing), then adds a backfill engine: run ANY new rule set or WASM plugin across the entire historical corpus, emitting time-series datasets ("price of X over 14 months", "when did this page first mention Y").
- **Why it's a moonshot**: It changes what the product *is*: from "a scraper that knows the present" to "a queryable archive of everything it has ever seen, with retroactive ETL." Every future extraction idea instantly gains months of history it never had to collect — schema mistakes stop being permanent data loss. That is the Common Crawl / GDELT capability at private scale, and it compounds: the archive gets more valuable every day the crawler merely runs.
- **Differentiation**: Nearest prior is "Wayback-style time machine for watched API docs" — a viewing/business feature scoped to the `connector_docs` watch list. This is platform-wide, and its core is not *viewing* history but *re-computing over* it: retroactive rule/plugin execution producing longitudinal datasets. No prior idea proposes versioned body retention or historical backfill extraction.
- **Path**:
  1. In `DatasetPageSink::emit`, stop overwriting: when a page comes back `changed`, also upsert a `page_versions` record keyed `{url}#{revision}` carrying the artifact path + simhash + fetched-at (the sink already receives everything needed; unchanged revisits skip, so storage grows only with real change).
  2. Make crawl artifact filenames revision-scoped (the 2026-07-16 URL-address artifact fix already made them URL-stable; add a revision suffix on change).
  3. Extend extractor/plugin `source` mode with `{"as_of": <ts>}` / `{"versions": "all"}` — resolve keys through `page_versions` instead of `pages`; the existing `read_source_artifact` path then works unchanged.
  4. Backfill runner: a job that fans a compiled rule set over all versions of matching URLs (bounded by the existing SOURCE_LIST_LIMIT batching), writing records tagged `_url` + `_observed_at` so downstream datasets are naturally time-series.
  5. Add a retention policy knob (the dataset-store prune API from the 2026-07-16 retention work is the natural home) so the archive is capped by design, not by accident.
- **Risks**:
  - Storage growth on churn-heavy hosts; mitigated by simhash-gated retention (keep only substantive revisions) and per-source retention caps.
  - `_observed_at`-tagged records need a dataset-key convention (`key@ts`) or triggers/change-detection will treat historical backfill as churn — design this before the first backfill run.

## Context: Web Research & Readable Content

### 1. Pumper as the web-access layer for the agent economy (MCP gateway)
- **Tier**: 1
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/apps/readable/src/lib.rs, crates/apps/research/src/lib.rs, crates/apps/hackernews/src/lib.rs
- **What it is**: `readable` already turns any URL into clean Markdown through the tiered fetcher (http → browser → Claude escalation, wait_for_selector, min_content_chars, artifact discipline); `research` wraps budgeted, cached, schema-guarded agentic research; search/datasets/`?filter=` expose everything collected. This moonshot exposes that stack as an MCP server (stdio + SSE on the existing axum :8088): tools like `fetch_readable(url)`, `search_corpus(query, facets)`, `deep_research(query, budget)`, `watch(url)` — so ANY external agent (Claude Desktop/Code, other frameworks) uses pumper as its web-access and memory backend, inheriting the politeness governor, cost ledger, budget ceilings, ETag cache, and provenance for free.
- **Why it's a moonshot**: Positioning shift from "app platform with 15 scrapers" to "infrastructure every agent stack needs" — the fetch layer of the agent economy. Every capability already shipped (bot-wall escalation, budget governor, DLQ webhooks, recency search) becomes a differentiator versus naive `fetch()` tools that get bot-walled and have no memory. Distribution is 10x: MCP listing puts pumper in front of every Claude user without building any UI, and each connected agent's usage grows the corpus (which feeds the archive and reliability moonshots).
- **Differentiation**: Nearest priors are "Position readable as a paid any-URL-to-Markdown API" (a REST endpoint product — same capability, HTTP-shaped, human-integrated) and "Metered research API with cost-plus credit billing" (billing wrapper). Neither proposes the agent-protocol surface: MCP tool semantics, session-scoped budgets per connected agent, or pumper-as-agent-memory. No prior idea mentions MCP or agent-facing integration at all.
- **Path**:
  1. Add an `mcp` crate exposing 3 tools that are thin adapters over existing seams: `fetch_readable` → the exact `FetchRequest{to_markdown:true}` path in readable, `search_corpus` → the search API, `run_app` → job enqueue; serve over stdio first (no auth question), test from Claude Code.
  2. Add `deep_research` mapping onto `ResearchRequest` with a hard `max_budget_usd` per MCP session and the existing json_schema guardrail.
  3. SSE transport on :8088 behind the existing API-key lifecycle (list/revoke shipped in lighttrack-style form here via app registry) with per-key budget ceilings metered through the cost ledger.
  4. Return provenance with every tool result (engine, escalations, fetched_at, artifact ref) so downstream agents can cite.
  5. Publish the server manifest + a quickstart; watch which tools external agents actually call to prioritize the next tool.
- **Risks**:
  - Open fetch-for-hire is an SSRF/abuse surface — needs URL allow/deny policy and per-key quotas from day one.
  - MCP spec churn; mitigate by keeping the crate a thin adapter over stable internal seams.

### 2. Speak a data source into existence — research-compiled, catalog-provisioned pipelines
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: quarters
- **Files**: crates/apps/research/src/lib.rs (compiler engine), crates/apps/readable/src/lib.rs (page sampling)
- **What it is**: Today `research` answers questions; provisioning a new data source is expert work (write rules, pick cadence, edit the load-bearing `data-sources.toml`, wire triggers). This moonshot makes the research agent a *compiler*: from one sentence — "track Czech senior Rust salaries weekly" — it researches which sites carry the data, samples them via the readable/tiered-fetch path, drafts a declarative rule set, proposes seeds/cadence/budget, **dry-runs the extraction and iterates until the health detector's verdict is clean**, then emits a complete provisioned source: catalog entry + crawl config + rule set + trigger DAG + schedule, gated behind one human approve.
- **Why it's a moonshot**: It collapses the platform's steepest cost — source onboarding — from expert-days to a sentence, which changes who can use pumper (analysts, not scraper engineers) and how many sources it can carry (hundreds, not fifteen). Combined with the existing health monitor and repair signals, sources become cattle: cheap to mint, automatically watched, re-compilable when they drift. The catalog drift-gate shipped in Wave H means generated definitions are load-bearing and test-enforced from birth.
- **Differentiation**: "Auto-detect extraction schema from sample page" (prior, extraction ctx) generates *rules from one page* — no research, no source discovery, no lifecycle. "Chained recipes: research feeds crawl and extraction" (prior) chains *runtime jobs*, not provisioning. "Goal-directed semantic crawler" steers a crawl per-run. None compiles NL into a durable, catalog-registered, health-verified source definition — the provisioning-lifecycle target and the verify-loop against extraction health verdicts are new ground.
- **Path**:
  1. Add a `provision` mode to the research app: a `ResearchRequest` whose json_schema is a SourceSpec (sites[], seeds[], ruleset draft, cadence, expected_fields) — the schema-guardrail + `salvage_json` + shape-check pattern in research/lib.rs is exactly the needed scaffold; step 1 is one new prompt + schema.
  2. Verification loop: run the drafted rules through the extractor in urls-mode on 5 sample pages; feed `worst_fields` + FetchHealth back into a resumed session (`session_id` resume already shipped) for up to N repair turns under one `max_budget_usd`.
  3. Emit the artifact bundle: `data-sources.toml` fragment + app params JSON + trigger definition, saved via `save_artifact` for human review.
  4. Approval endpoint that applies the fragment to the catalog (drift-gate test keeps it honest) and registers schedule + trigger DAG.
  5. Later: auto re-compile hook — when the health monitor degrades a compiled source, re-enter the loop with the failing verdict as context.
- **Risks**:
  - LLM-drafted CSS/regex rules against JS-heavy sites will fail often; the dry-run gate must be a hard block, and browser-strategy sampling costs real money per compile.
  - A one-sentence path to scheduled crawling invites ToS/robots trouble at scale — provisioning must inherit robots-respect defaults and per-source budget caps, with human approval kept mandatory.
