# Moonshot Scan — Job Server & API (2026-07-30)

> Total: 12 moonshots across 6 contexts.

## Context: Configuration & Data Source Catalog

### 1. Catalog as control plane: GitOps reconciler materializes the TOML into running infrastructure
- **Tier**: 1
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: catalog/data-sources.toml, catalog/README.md, config.toml, crates/server/src/scheduler.rs
- **What it is**: Today the catalog is load-bearing but passive — `GET /catalog/sources` and `/catalog/health` read it, and a drift-gate *test* catches cron mismatches, but a human still creates schedules/watches by hand. Add a boot-time (and `POST /catalog/reconcile`) reconciler that treats each `[[source]]` row with `status = "live"` as desired state: ensure a schedule exists with exactly that cron/app/params, disable schedules for rows flipped to `blocked`, and report every diff as a reconciliation plan. Editing one TOML file (or merging a PR touching it) *is* deploying a pipeline.
- **Why it's a moonshot**: Terraform-for-scrapers. The whole fleet becomes declarative, reviewable, versioned in git, and reproducible on a fresh machine from one file — the operational model flips from "server with hand-configured state" to "infrastructure from code", which is what makes a 15-app machine scale to 150 sources.
- **Differentiation**: Nearest priors are #146 "Catalog drift check API" (read-only detection) and #194 "Planned-source autopilot" (Claude *builds connectors* for planned rows). Neither makes the catalog the authoritative writer of live schedules/watches; the shipped catalog-health work (Wave H) is monitoring only.
- **Path**: 1) Extend `pumper_core::catalog::Source` diffing against `storage.list_schedules()` into a `ReconcilePlan` struct (create/update/disable/orphan). 2) Add dry-run `GET /catalog/reconcile` returning the plan. 3) Add `POST /catalog/reconcile` applying it (schedules tagged `managed_by = catalog` so hand-made schedules are never touched). 4) Run a dry-run pass at boot, log drift loudly. 5) Optional `[catalog] auto_reconcile = true` to apply at boot. 6) Retire the drift-gate test into an assertion that the plan is empty.
- **Risks**:
  - A bad TOML edit can mass-disable pipelines — mitigate with the managed-tag scoping and a plan-size guardrail requiring `force`.
  - Two sources of truth during migration; the managed tag plus loud orphan reporting keeps the cutover honest.

### 2. Declarative data contracts per source, enforced at publish time
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: months
- **Files**: catalog/data-sources.toml, config.toml ([resilience]), crates/server/src/worker.rs (suppress_unhealthy)
- **What it is**: Let a catalog row carry an explicit contract block — `[source.contract]` with required fields, type/range expectations, allowed row-count delta per run, max staleness — and enforce it in the worker at the same choke point where `suppress_unhealthy` already gates pushes. The existing resilience system infers degradation statistically; contracts add the *declared*, human/LLM-authored floor (Great Expectations built into the catalog), and contract verdicts feed `/catalog/health` and `/sources`.
- **Why it's a moonshot**: It upgrades Pumper from "scraper that detects when it breaks" to "data product with guaranteed shape" — the precondition for anyone downstream (or any paying consumer) trusting a dataset without reading the extractor. Contracts + provenance is the standard bar for selling data.
- **Differentiation**: #176 "Downstream quality feedback loop updates confidence" is consumer-feedback adjusting a score; #61 "freshness SLA page" is display. No prior idea puts producer-side declared expectations in the catalog schema or gates publication on them; resilience (shipped) is purely inferred, not declared.
- **Path**: 1) Extend the catalog TOML schema + `catalog/README.md` with an optional `contract` table (fields, types, `min_rows`, `max_removed_pct`, `max_staleness_hours`). 2) Evaluate contracts in `worker.rs` over the run's `by_dataset` revisions (cheap — data already in hand). 3) Record verdicts alongside resilience run verdicts; surface on `GET /catalog/health`. 4) Wire violations into the same suppress-pushes/skip-index gates, behind `[resilience] enforce`. 5) Ship starter contracts for the 3 highest-confidence sources (grants-gov, mpsv-vpm, census).
- **Risks**:
  - Over-strict contracts create false quarantines on legitimately volatile sources — start in soak mode like resilience did.

## Context: Live Events & Webhooks

### 1. Inbound event ingress: external webhooks become trigger-DAG inputs
- **Tier**: 1
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/server/src/events.rs, webhook.rs, triggers.rs, routes/triggers.rs
- **What it is**: Pumper only *emits* events today; every pipeline starts from cron or a manual enqueue. Add `POST /ingest/{source}` — an HMAC-verified inbound webhook endpoint (reusing the exact signature scheme `webhook.rs` already implements for outbound) that stamps the payload onto the EventBus as an `external` event kind and lets triggers match on it. GitHub push → re-crawl docs; a grants.gov subscription email relay → immediate sync; a partner system's "new client" → run a research job.
- **Why it's a moonshot**: It converts Pumper from a polling scraper into a real-time automation hub — the reactive trigger DAG (shipped) currently has only internal stimuli; opening ingress multiplies what the DAG can react to by everything on the internet that can POST. That is the difference between "scheduler" and "event-driven platform".
- **Differentiation**: The shipped reactive DAGs and prior #34/#250/#91 are all about *outbound/internal* events (dataset deltas, terminal jobs, subscriptions). No prior idea proposes an inbound event surface at all.
- **Path**: 1) Add `IngressSource` storage (id, name, secret, enabled) + CRUD routes. 2) `POST /ingest/{id}` verifies `x-pumper-signature` with the stored secret (reuse `sign()` logic inverted), size-caps the body, emits an `external` JobEvent-like event. 3) Extend trigger match rules with `on: external` + source filter + JSON-path predicates (the `?filter=` parser already exists). 4) Replay ring makes ingress events visible on `/events` for free. 5) Docs + a GitHub-webhook worked example.
- **Risks**:
  - This is the first non-localhost-trust write surface — require per-source secrets, keep it disabled by default, and rate-limit per source.

### 2. Sink connectors: dataset deltas delivered to systems, not just URLs (reverse-ETL via WASM sinks)
- **Tier**: 2
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/server/src/webhook.rs, worker.rs (notify_watches), config.toml ([plugins])
- **What it is**: Generalize the delivery side of watches from "POST JSON at a URL" to typed sinks: a `sink` field on a watch selecting a connector — builtin (file/NDJSON append, Postgres upsert, S3 object, Slack message) or a WASM plugin implementing a `sink` interface in the existing plugin host (fuel/memory-capped, hot-swappable via `/plugins/reload`). The DLQ, backoff drain, and delivery log wrap every sink uniformly.
- **Why it's a moonshot**: Extract → detect change → *land it where it's used* closes the last mile: Pumper becomes an end-to-end EL(T) platform rather than a source that every consumer must write receiver glue for. WASM sinks mean the connector ecosystem grows without recompiling the server — the same trick that made extraction extensible.
- **Differentiation**: #246 "Human-channel notifications: Slack/email digest sink" is one hardcoded channel; #232 "federated event mesh" is pumper-to-pumper. Neither proposes a general sink-connector *architecture*, and nothing prior extends the WASM plugin host beyond extraction.
- **Path**: 1) Introduce a `Sink` trait behind the existing `deliver()` seam in webhook.rs (http-post becomes the first impl). 2) Add builtin `file` sink (append NDJSON per dataset) — immediately useful, zero deps. 3) Route delivery outcomes through the existing log/DLQ unchanged. 4) Define a `sink` WASM interface (envelope in, ack/error out) in the plugin host next to `extract_v2`. 5) Postgres/S3 builtins behind feature flags. 6) Watch API gains `sink` + `sink_config`.
- **Risks**:
  - Credential handling for external systems (Postgres/S3) enters scope for the first time — keep secrets in config/env, never in the DB rows.
  - WASM sinks doing network I/O need host-function design (WASI sockets vs host-mediated HTTP); start host-mediated.

## Context: Job Worker & Cron Scheduler

### 1. Durable execution: checkpoint/resume seam so any job survives crash, restart, and reap
- **Tier**: 1
- **Feasibility**: high
- **Horizon**: months
- **Files**: crates/server/src/worker.rs (execute, drain, reap_once), progress.rs
- **What it is**: The crawl app privately checkpoints; every other app restarts from zero when reaped, timed out at the wall clock, or drained at shutdown. Promote checkpointing into `AppContext`: `ctx.checkpoint(state_json)` persists a per-attempt-lineage blob, `ctx.restore()` hands it back on re-claim; `drain()` and the reaper stop being "requeue and pray" and become "suspend and resume". Long Claude research jobs, multi-page bulk syncs, and 100k-page crawls all become resumable by adding two calls.
- **Why it's a moonshot**: This is Temporal-style durable execution inside a single binary — jobs measured in hours become safe on a laptop that sleeps, and `job_timeout_secs` can shrink because a timeout costs a resume, not a restart. It changes what class of work Pumper can be trusted with, from tasks to *workflows*.
- **Differentiation**: #113 "Self-healing jobs: LLM-diagnosed engine fallback" heals *failures*; #270 "Elastic worker fleet" distributes claims; #99 manual retry. None gives mid-job state durability — no prior idea touches checkpoint/resume, and the crawl artifact-resume work (shipped) was app-private, not a platform seam.
- **Path**: 1) Add `checkpoints` storage keyed by job id (blob + attempt written by + updated_at), with a size cap. 2) Add `checkpoint`/`restore` to `AppContext` (write-through, throttled like `progress.rs` does). 3) On claim, populate `restore` from the latest blob; clear on `complete`. 4) Port app_crawl to the seam (deleting its bespoke path proves the API). 5) Teach `drain()` to signal cooperative suspend via the existing cancel token before the deadline, so shutdown checkpoints instead of abandoning. 6) Port app_research (the expensive one).
- **Risks**:
  - Stale-checkpoint correctness across the reap/reset races the worker already guards — reuse the attempts-lineage guard (`complete(job.id, job.attempts, ..)`) for checkpoint writes.
  - Apps must treat restored state as advisory; a poisoned checkpoint needs a "start fresh after N resume failures" escape.

### 2. VCR mode: record/replay job runs for deterministic re-execution and extractor time-travel
- **Tier**: 2
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/server/src/worker.rs (execute, AppContext wiring), config.toml ([cache])
- **What it is**: A `record: true` enqueue flag makes the engines layer persist every fetch (URL, headers, body, tier used) into the job's `artifacts_dir` as a cassette; a `replay_of: <job_id>` flag runs any app entirely from a cassette — no network, no politeness delays, no spend. Changed a RuleSet? Re-run last Tuesday's grants sync against the exact bytes it saw and diff the datasets. Resilience false-positive? Replay the flagged run under a debugger.
- **Why it's a moonshot**: Deterministic replay is the foundation nobody in scraping has: extractor changes become regression-testable against real historical pages, Claude-tier prompts become benchmarkable at zero marginal cost, and every production incident is reproducible forever. It turns "scraping is inherently flaky to develop" into a solved problem.
- **Differentiation**: No prior idea touches recording or deterministic replay. #274 "App self-test / dry-run" (registry) runs against live or synthetic input; the shipped HTTP cache is TTL/freshness-oriented, not a per-job immutable capture.
- **Path**: 1) Wrap `state.engines` handed into `AppContext` with a recording decorator writing request/response pairs to `artifacts_dir/cassette/` (content-addressed, keyed by canonical request hash). 2) Add the `record` flag to EnqueueOptions + routes. 3) Add a replaying decorator that serves from a named cassette and errors (or optionally passes through) on miss. 4) `POST /jobs/{id}/replay` route enqueues with `replay_of`. 5) Diff helper: compare replay-run revisions vs original via the existing changes feed. 6) e2e: record a fixture run, mutate a rule, assert the diff.
- **Risks**:
  - Cassette size for bulk feeds (mpsv ~188 MB) — gzip + opt-in per job, with a size ceiling.
  - Non-engine nondeterminism (timestamps, Claude sampling) means replay is byte-deterministic for http/browser tiers only; scope claims accordingly.

## Context: DataHub Metadata Emitter

### 1. Emit the whole pipeline topology: schedules, jobs, and trigger DAGs as DataHub DataFlow/DataJob lineage
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/server/src/datahub.rs, scheduler.rs, triggers.rs
- **What it is**: Today the emitter pushes dataset entities, inferred schema, table-level lineage, and freshness operations. Extend it to model *process*: each schedule becomes a `dataFlow`, each app run a `dataJob` with input/output dataset edges, and — the payoff — each reactive trigger becomes a lineage edge between the datasets/jobs it connects, so the shipped trigger DAGs render as an actual visual DAG in DataHub. Add column-level lineage where declarative RuleSets make field provenance mechanically known (rule field → dataset column).
- **Why it's a moonshot**: The catalog answers "what do we scrape"; this answers "how data flows" — the full observable topology of an autonomous scraping organism in an industry-standard UI. Pumper becomes legible to data teams on day one, which is the adoption wedge for every enterprise conversation.
- **Differentiation**: No prior idea in the 2026-07-10 backlog touches DataHub or lineage at all (datahub.rs postdates that scan) — this whole context is virgin ground; the moonshot is chosen for maximum leverage of the shipped trigger-DAG work.
- **Path**: 1) Add `dataflow_urn`/`datajob_urn` builders next to `dataset_urn` in datahub.rs. 2) On `on_job_success`, emit a dataJob entity with `dataJobInputOutput` from the run's touched datasets (already computed for watches/triggers). 3) On trigger create/update, emit the trigger as a dataFlow linking cause→effect datasets. 4) Emit schedule entities with cron in customProperties from `list_schedules` during `datahub_sync`. 5) Column-level: map RuleSet field names to `fineGrainedLineage` for extractor-app datasets.
- **Risks**:
  - Aspect-model fidelity over the plain OpenAPI surface (no SDK) needs verification per entity type — the existing envelope pattern derisks it.

### 2. Close the loop: DataHub governance actions drive Pumper behavior
- **Tier**: 3
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/server/src/datahub.rs, scheduler.rs, routes/mod.rs (datahub_status/datahub_sync)
- **What it is**: Make the shadow bidirectional: a periodic (scheduler-piggybacked, like the DLQ drain) pull of DataHub state for Pumper-platform URNs — deprecation flags, tags, assertion results — mapped to actions: a dataset deprecated in DataHub disables its schedules; a `cost:pause` tag pauses Claude-tier jobs for that source; failed freshness assertions enqueue an immediate sync job. DataHub becomes a governance console operating the scraper fleet, not just observing it.
- **Why it's a moonshot**: One-way metadata emitters are commodity; a scraper that *obeys* the catalog is a new category — data governance with an actuator. For any org already living in DataHub, Pumper plugs into their existing control surface with zero new UI built.
- **Differentiation**: No prior idea touches DataHub or any inbound governance channel; #61 (SLA page) and #176 (confidence feedback) are Pumper-internal displays/scores with no external control plane.
- **Path**: 1) Add a read client for GMS entity fetch (same reqwest client, GET side of the OpenAPI surface). 2) Poll deprecation + tags for known dataset URNs on a slow tick; cache etags. 3) Map deprecation → `set_schedule_enabled(false)` for catalog-linked schedules (reuse the catalog↔schedule linkage from the reconciler moonshot, or `schedule.app` matching). 4) Map assertion failures → enqueue with existing EnqueueOptions. 5) Guard everything behind `[datahub] govern = false` default-off; log every action taken as an event on the bus.
- **Risks**:
  - Remote-state-driven disablement on an unattended box is a footgun — default-off, action allowlist, and every action reversible + evented.

## Context: App Registry

### 1. Agent-ready registry: every app exports a typed, evaluated tool manifest (MCP-compatible)
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/server/src/registry.rs, routes/jobs.rs (list_apps/enqueue_job)
- **What it is**: Extend `ScrapeApp` with a rich manifest: JSON Schema for params, 2–3 worked example invocations, expected-output shape, cost class (free/metered/Claude), and a smoke fixture. `GET /apps` serves manifests in a form directly consumable as agent tool definitions, and enqueue validates params against the schema server-side. The registry stops being a list of names and becomes a self-describing, machine-operable tool catalog — the substrate the HTTP-API MCP moonshot serves verbatim.
- **Why it's a moonshot**: The number of Pumper operators today = humans who read the source. Typed, exemplified, validated manifests make the operator population "any LLM agent", which multiplies usage of the same 21 apps without writing another line of connector code — and schema validation kills the silent wrong-params class of failure permanently.
- **Differentiation**: #181 "Per-app parameter JSON Schema for discovery" (feature, discovery only) is the seed; this is qualitatively bigger — schemas + examples + cost class + fixtures + server-side enforcement + agent-consumable format. #274 self-test overlaps only the fixture corner.
- **Path**: 1) Add `fn manifest(&self) -> AppManifest` to `ScrapeApp` with a derive-friendly default (name + default_params only) so all 21 apps compile immediately. 2) Fill rich manifests for the 5 most-used apps (extractor, crawl, research, grants-gov, plugin). 3) Validate enqueue params against the schema (jsonschema crate), 422 with pointer paths on mismatch. 4) Serve `GET /apps?format=tools` emitting MCP tool-definition JSON. 5) CI test: every manifest example must pass its own schema.
- **Risks**:
  - Schema drift vs actual param handling — the example-passes-schema test plus using schemas in e2e enqueues keeps them honest.

### 2. Dynamic apps: full ScrapeApp implementations as hot-loadable WASM components
- **Tier**: 1
- **Feasibility**: low
- **Horizon**: quarters
- **Files**: crates/server/src/registry.rs, config.toml ([plugins]), crates/server/src/state.rs
- **What it is**: Today adding an app means a new crate, Cargo.toml edits, one registry line, and a recompile; the WASM host runs only leaf extractors. Define the `ScrapeApp` contract as a WASM component-model world (run(params) with host imports for fetch/upsert/progress/cost — the AppContext surface), so a complete domain app compiles to a `.wasm` dropped in a directory and appears in the registry at runtime, sandboxed under the existing fuel/memory caps. The 21-line static vec becomes `static_apps() + dynamic_apps(dir)`.
- **Why it's a moonshot**: This is the enabling technology under every "marketplace" dream: apps become artifacts you can distribute, version, and roll back *because they are files*, written in any language that targets WASM, safely runnable because they are sandboxed. It converts Pumper from a program with plugins into a platform with an ecosystem.
- **Differentiation**: #229 "Runtime app marketplace: install, version, rollback" assumes distributable apps exist but proposes the store, not the runtime; #174 "Plugin connectors as first-class registry apps" promotes today's *extract-only* plugins into the listing. Neither proposes the component-model host that makes full apps dynamic — this is the missing foundation both depend on.
- **Path**: 1) Specify the world (WIT): `run(params) -> result` importing `fetch`, `upsert`, `report-progress`, `record-cost` — a strict subset of AppContext. 2) Build a host adapter in the server implementing those imports over the real engines/datasets (the AppContext fields already exist as Arc'd services in state.rs). 3) Wrap loaded components in a `WasmScrapeApp: ScrapeApp` adapter; extend `registry::apps()` with a directory scan. 4) Port one real app (app_hackernews — smallest) to WASM as the proof. 5) Reuse `/plugins/reload` semantics for hot-swap; manifest (moonshot #1) comes from the component's exported `describe`.
- **Risks**:
  - Component-model async host calls + budget/politeness enforcement across the boundary is genuinely hard; the fuel model must extend to wall-clock and spend, not just instructions.
  - Two app ABIs to maintain during the long transition — keep the WIT world minimal and versioned from day one.

## Context: HTTP API & Routes

### 1. Pumper as an MCP server: datasets, search, jobs, and live events as native agent tools
- **Tier**: 1
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/server/src/routes/mod.rs, main.rs, routes/events.rs, routes/search.rs
- **What it is**: Mount an MCP endpoint (streamable-HTTP, rmcp crate) beside the REST router exposing: tools (`enqueue_job`, `search`, `query_dataset` with the shipped `?filter=` grammar, `list_apps`), resources (datasets, catalog, job results), and subscriptions bridged from the EventBus replay ring (job/progress/dataset-changed events push to connected agents). The utoipa single-source router means tool schemas can be *generated* from the same annotations the OpenAPI doc comes from — one source of truth for both surfaces.
- **Why it's a moonshot**: Every Claude/agent session on the machine (and any remote agent runtime) gains a live, queryable, *actuatable* web-data layer with zero glue code. In an agent-native 2026, "the data platform agents can natively drive" is a category, and Pumper's local-first + budgeted + politeness-governed design is exactly the safe substrate agents need.
- **Differentiation**: No prior idea in the 276 mentions MCP or any agent protocol. #137 "Ask-the-data NL endpoint" builds NL understanding *into* Pumper; this instead hands typed tools to the intelligence that already exists outside — the inverse and much cheaper bet.
- **Path**: 1) Add rmcp with streamable-HTTP transport, mounted at `/mcp` on the existing axum Router in main.rs. 2) Hand-write the first 4 tools as thin wrappers over the existing handlers' logic (state is already `AppState` clones). 3) Bridge `EventBus::subscribe` + replay into MCP notifications for subscribed clients. 4) Expose catalog + app manifests as MCP resources. 5) Generate tool JSON Schemas from utoipa DTOs to prevent drift. 6) Ship a `.mcp.json` snippet in docs; dogfood from Claude Code against the live server.
- **Risks**:
  - Agents can enqueue spend (Claude-tier jobs) — enforce per-session budget caps and default the MCP surface to read-mostly with enqueue behind a config flag.

### 2. Dataset peering: resumable revision-feed replication turns single nodes into a data mesh
- **Tier**: 2
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/server/src/routes/datasets.rs (dataset_changes), routes/mod.rs, state.rs
- **What it is**: A `peer` subscribes to another Pumper's datasets over the existing revision feed: `GET /datasets/{app}/{ds}/changes` already exposes ordered change history, so a puller with a persisted cursor + the ETag/compression layers (shipped) can maintain an exact, incrementally-synced replica — including tombstones — and index it locally. Config: `[[peer]] url, datasets, interval`. One laptop scrapes; the office box, the cloud VM, and a teammate's machine all *have the data*, each with local search, triggers, and watches firing on replicated deltas.
- **Why it's a moonshot**: It breaks the single-node ceiling without building distributed systems: scrape-once/consume-everywhere, cheap read replicas, offline-capable mirrors, and org topologies (edge scrapers → aggregator) — the data-mesh outcome with only HTTP pull and a cursor. Replicated deltas feeding local trigger DAGs means downstream automation no longer requires living on the scraping box.
- **Differentiation**: #232 "Federated event mesh" (events context) federates *live events* — transient fan-out, no state transfer, no catch-up. This replicates *dataset state* with resume and tombstones, a different primitive; no prior idea proposes replication or any multi-node data topology.
- **Path**: 1) Verify/extend the changes feed for strict replication semantics: stable ordering key + `since` cursor + tombstone inclusion (most exists from the retention/backfill work). 2) Add a `peers` config block + a puller task (scheduler-piggybacked like the DLQ drain) writing into the local dataset store under the source app namespace with an `origin` marker. 3) Persist per-peer cursors; ETag + gzip make idle polls ~free. 4) Local search indexing of replicated revisions reuses the exact `dataset_search_docs` delta path. 5) Fire local watches/triggers on replicated changes behind a per-peer opt-in. 6) `GET /peers` status route (lag, last sync, cursor).
- **Risks**:
  - Write-origin discipline: replicas must be read-only for peered datasets or upsert loops corrupt both sides — enforce origin markers at the store layer.
  - Auth story for non-localhost pulls (prior #273 API-key auth becomes a prerequisite, not part of this scope).
