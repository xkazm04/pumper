# Moonshot Scan — Scraping Engines (2026-07-30)

> Total: 6 moonshots across 3 contexts.

## Context: Full-Text Search Index

### 1. Queries as datasets: materialized search views that feed the trigger DAG
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/engine-search/src/lib.rs
- **What it is**: Promote a search query (`q` + app/dataset/since filters + sort) to a first-class *virtual dataset*: the result set is materialized into the dataset store on a cadence (or on index commit, via the background committer's flush points), gets change detection like any other dataset, and therefore emits deltas into the shipped reactive trigger DAGs, webhooks, `/export`, and `?filter=` surfaces. A query stops being a read and becomes a composable data *producer* — "EU grants mentioning hydrogen, newest-first" becomes a dataset other pipelines consume.
- **Why it's a moonshot**: It closes the loop between the two strongest shipped subsystems (tantivy search and trigger DAGs). Every downstream capability — triggers, webhooks, export filters, domain apps — instantly composes over arbitrary full-text semantics without new code. That is the platform inflection from "scraper with search" to "streaming query engine over the web".
- **Differentiation**: Nearest prior is #35 "Saved searches as standing watch alerts" — that ends at a notification. This makes query results durable, versioned, delta-tracked datasets that the whole DAG/export/filter machinery operates on; no prior idea connects search output to the dataset store or trigger pipelines.
- **Path**: 1) Add a `materialize` flag to the existing saved-search model that writes hits (id, title, url, score, snippet) as records into a `search/view-<name>` dataset via the existing store API. 2) Run it from the saved-search runner (already calls `flush()` for visibility). 3) Let simhash change detection produce deltas for free. 4) Register the view dataset in the catalog so `/catalog/health` monitors its freshness. 5) Wire trigger DAGs to subscribe to view deltas; document the pattern.
- **Risks**:
  - Feedback loops (a view over a dataset the view itself writes) — need a "views never index into search" rule.
  - Score volatility can churn deltas; materialize stable fields only or bucket scores.

### 2. Entity-typed index: money, dates, organizations and places as queryable fast fields
- **Tier**: 2
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/engine-search/src/lib.rs
- **What it is**: An index-time enrichment stage that extracts typed entities from `title`/`body` — currency amounts, deadlines/dates, org names, geo (state/county, already central to the trades/census apps) — into additional tantivy FAST/INDEXED fields, exactly as `indexed_at` was added (the schema-versioning + rebuild machinery in `schema_is_current` already handles migrations). Queries gain structured predicates over unstructured scraped text: "grants over $1M with a deadline in the next 60 days in Texas".
- **Why it's a moonshot**: It turns a BM25 text index into a queryable knowledge layer across all ~15 domain apps at once — the grants, labour-market and trades verticals each get amount/date/geo filtering without per-app code. Cross-dataset questions ("everything money-related touching county X this month") become one query.
- **Differentiation**: Prior #21 "Answer engine: NL questions" is LLM Q&A; #177 "Hybrid BM25 + embedding" is semantic similarity. Neither adds deterministic *typed structured fields*; no prior idea touches entity extraction or range-queryable enrichment. The shipped `since`/facets work proves the fast-field pattern this generalizes.
- **Path**: 1) Add optional `amount_max: i64`, `deadline: i64`, `geo: STRING` fields to the schema + `SCHEMA_FIELDS` (rebuild path already warns + backfill bin exists). 2) Write a pure Rust extractor (regex + chrono date parsing) applied in `index()`; start with money + dates only. 3) Extend `SearchRequest` with `amount_gte`/`deadline_before` mapped to `RangeQuery` clauses like `since`. 4) Run `search-backfill --all` to enrich the corpus. 5) Add geo via a static state/county gazetteer (census app already has the tables). 6) Expose in `/search` API + facets.
- **Risks**:
  - Extraction precision on messy HTML text; mitigate by storing provenance offsets and keeping fields optional.
  - Schema churn forces full index rebuilds — batch field additions into one version.

## Context: WASM Plugin Sandbox

### 1. WASM everywhere: one UDF runtime for every platform hook
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/engine-wasm/src/lib.rs
- **What it is**: Generalize the sandbox beyond extraction: the same fuel-metered, memory-capped, zero-ambient-authority host becomes the platform's universal user-defined-function layer. Trigger-DAG predicate nodes ("fire only if delta matches this logic"), dataset delta transforms, webhook payload shaping, and record scoring/dedup hooks all accept a named plugin. The host already has everything required — `extract_v2` params envelope, `describe()` manifests, the admission semaphore, hot reload — it is just only *called* from the extraction path today.
- **Why it's a moonshot**: It converts pumper from "configurable scraper" into a programmable data platform — users inject arbitrary safe logic at any point in the pipeline without forking the Rust codebase or waiting on releases. That's the same leap AWS made with Lambda: the integration points become the product.
- **Differentiation**: Nearest priors are #223 "Chain plugins into a transform pipeline" (chaining within extraction only) and #256 "Governed host capabilities" (giving plugins more power). Neither moves plugins to *other subsystems' hooks*; no prior idea touches trigger predicates, webhook shaping, or store-side transforms.
- **Path**: 1) Extract a `run_udf(name, json_input, params) -> Value` helper in `pumper_core::Plugins` semantics (the `run` method already is exactly this — step 1 is documentation + a manifest `kind` field, e.g. `"kind": "predicate"|"transform"|"extractor"`). 2) Add a `plugin_predicate` node type to the trigger DAG that calls `Plugins::run` with the delta as input and interprets `{"pass": bool}`. 3) Add `transform_plugin` to webhook config — payload passes through the plugin before delivery. 4) Filter `GET /plugins` by `kind` so UIs offer the right plugins per hook. 5) Per-hook fuel presets (predicates get a tiny budget; the `DESCRIBE_FUEL` pattern generalizes).
- **Risks**:
  - Hot-path latency: a predicate on every delta must be cheap — enforce small fuel budgets and cache instantiation (pair with prior precompile idea, which remains complementary).
  - ABI sprawl; keep the single `extract_v2` envelope and vary only the JSON contract per kind.

### 2. Corpus-scale extraction observatory: continuous differential testing against the stored web
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/engine-wasm/src/lib.rs, plugins-src/title-extractor/src/lib.rs
- **What it is**: Use the dataset store's raw fetched pages (plus the HTTP cache and crawl artifacts) as an ever-growing, *real* test corpus for every plugin. A background job replays each plugin against N sampled stored pages per dataset, records per-page outcome (ok / trap / empty / schema-invalid JSON), fuel consumed, and output-shape stats, and diffs against the previous run — producing a drift score per plugin per site. A rising empty-rate on a site the plugin used to extract from is surfaced days before anyone notices missing data.
- **Why it's a moonshot**: Extraction silently rotting is the number-one operational failure mode of every scraping platform. Making rot *measurable and continuous* — with zero new fetches, purely against already-stored bytes — is a 10x reliability jump and the prerequisite for any self-healing story.
- **Differentiation**: Prior #116 "Deterministic golden-output regression harness" is fixed hand-picked goldens; #117 "Self-healing extractors" is the *repair* act; #43 "per-run plugin execution metrics" is live-run telemetry. None proposes continuous corpus-scale replay with drift scoring as the detection layer — the piece all three silently assume exists.
- **Path**: 1) Add a `plugin-audit` bin: sample K records per dataset from the store, call `Plugins::run` on each (the admission semaphore already bounds fan-out), classify outcomes. 2) Persist results as a `plugin_audit` dataset (self-hosting: the observatory's history is itself queryable/searchable). 3) Compute drift = delta in ok-rate / output-field cardinality vs prior audit. 4) Schedule via the existing cron scheduler; surface worst drifters on `/catalog/health`. 5) Emit a webhook/trigger event when drift crosses a threshold — which is exactly where a future self-healing (prior #117) plugs in.
- **Risks**:
  - Stored pages age; weight samples by `indexed_at` recency so drift reflects the current site, not history.
  - CPU cost of mass replay — cap via fuel budgets and off-peak scheduling.

## Context: Fetch Engines (HTTP / Browser / Claude)

### 1. Distributed fetch fabric: satellite fetch-only nodes with a cluster-wide governor
- **Tier**: 1
- **Feasibility**: low
- **Horizon**: quarters
- **Files**: crates/engine-http/src/lib.rs, crates/engine-browser/src/lib.rs
- **What it is**: A `pumper --fetch-node` mode: the same Rust binary, stripped to the fetch tier (HTTP engine + optionally one Chrome), deployed on cheap VPSes/residential endpoints in different geographies. The coordinator's tiered fetcher gains a "remote" dispatch: requests are routed to a node (by geo, IP diversity, or load), bodies stream back, and the politeness governor's learned per-host penalties (`penalize`/`reward` already in the HTTP engine) become *cluster-wide* state so N nodes never collectively hammer one host.
- **Why it's a moonshot**: It breaks the single-IP, single-machine ceiling that bounds every current workload — geo-locked content, per-IP rate walls, and raw throughput all fall at once. Pumper becomes a scraping *fabric*, and the satellite binary is a distributable artifact (the door to a hosted/marketplace offering). The governor-as-shared-brain is the differentiated part: distributed politeness, not distributed rudeness.
- **Differentiation**: Nearest priors are #222 "Rotating proxy pool" (same box, dumb egress) and #225 "Autonomous research swarm" (Claude agents, not fetch infra). No prior idea proposes multi-node deployment of pumper itself or shared governor state.
- **Path**: 1) Feature-gate a minimal server exposing exactly one endpoint, `POST /fetch` (serialize the existing `HttpRequest`/`HttpResponse` types — they're already plain data). 2) Add a `RemoteEngine: HttpClient` impl in the coordinator that forwards to a configured node list (TOML catalog entry per node, health-checked like `/catalog/health`). 3) Route by simple policy first: explicit `node` on the request, else round-robin. 4) Sync governor penalties: nodes report 429/503 observations back in the fetch response; the coordinator's governor stays the single brain and picks the node + spacing. 5) Later: session-profile pinning (a profile's cookie jar lives on one node) and browser-capable nodes.
- **Risks**:
  - Auth/transport security between coordinator and nodes (mTLS or shared token) is mandatory before any deployment.
  - Cookie-jar/profile locality adds real complexity — defer profiled fetches to a pinned-node phase.

### 2. Tier-zero archive engine: the Wayback Machine and Common Crawl as a free fetch tier
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/engine-http/src/lib.rs, crates/engine-claude/src/lib.rs
- **What it is**: A new engine below HTTP in the ladder: for a requested URL, query the Wayback CDX API (and optionally the Common Crawl index) for a stored snapshot; if one exists within the caller's freshness window, serve *that* body — zero load on the target site, zero politeness budget, zero ban risk. It also unlocks a capability no live fetch can offer: **historical backfill** — fetch the same URL *as of* 2019, 2022, 2024 and feed the change-detection pipeline with years of data on day one.
- **Why it's a moonshot**: Every dataset's history normally starts the day you start scraping. This starts it a decade earlier, for free, and simultaneously makes the polite path the default path (archives absorb the load). For the grants/labour/trades verticals, instant multi-year time series is a step-change in product value.
- **Differentiation**: No prior idea touches web archives at all. Prior #78 "Time-travel index: versioned docs" versions *our own* future crawls; this imports the *world's* past. It also composes with the shipped ETag revalidation and TTL cache rather than replacing them.
- **Path**: 1) New `engine-archive` crate implementing `HttpClient`: hit `web.archive.org/cdx/search/cdx?url=...&limit=1&sort=reverse` then fetch the snapshot body (`/web/<ts>id_/<url>` for the raw, unrewritten payload) — reusing the existing capped/charset-aware body reader. 2) Add `archive_max_age` to `HttpRequest`; the tiered fetcher tries archive before live HTTP when set. 3) Mark responses with a `fetched_via: archive` + snapshot timestamp header so provenance is explicit in stored records. 4) Add a `backfill` job type: enumerate CDX snapshots for a URL across a date range and run each through the app's normal extraction into timestamped records. 5) Politeness for archive.org itself via the existing governor (it rate-limits too).
- **Risks**:
  - Archive coverage is patchy for niche/government pages — always fall through to live fetch; treat archive as opportunistic.
  - Historical page markup differs from today's; extraction rules may need per-era tolerance (the plugin-audit observatory above would quantify this).
