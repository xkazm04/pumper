# Moonshot Scan — pumper, 2026-09-01 (generation 2)

Second-generation moonshot round, run as `/scan-sweep moonshot-architect`: strategy **develop**,
ad-hoc lens `moonshot-architect` plus the develop deep tier (`feature-scout`, `innovation-catalyst`,
`integration-planner`, `business-strategist`), **L/XL only**, all 8 context-map groups, one read-only
scout per group (`_SCAN-BRIEF.md`). The first generation (M01–M44, [../moonshot-2026-07-30/INDEX.md](../moonshot-2026-07-30/INDEX.md))
is fully shipped; every card here either builds on a shipped seam's own "next slice" note or is new.

## Totals

| | |
| --- | --- |
| scout cards returned | 44 (8 scouts) |
| deck items after merge | 37 (20 XL, 17 L) |
| built this round | 0 — every item is `unmeasurable` by construction (§5: L never builds; the benefit is architectural), so all go to the deck |
| rejected (`not-better`) | 0 |
| decisions | triaged in-terminal 2026-09-01: **23 accept / 14 reject** (operator, 9 waves). Rejected items are a durable no — do not re-propose. |

## Convergence signals (independent multi-scout agreement)

Corroboration = number of independent group scouts whose cards land on the same spine (head + merged
+ related). It was the strongest ranking signal in both prior moonshot rounds and is the first sort key here.

| Spine | Scouts | Items |
| --- | --- | --- |
| **Act on the web** — transact v2 approval ledger + a generic `waiting` job state | 4 (SE, CR, CP, JO) | N01, N02 |
| **Federation** — mesh identity/sync, fabric v2, executor plane, artifact peering | 4 (HA, SE, JO, CR) | N16–N19 |
| **Pipelines** — workflow runs with fan-in, root id, param binding | 3 (JO, EP, HA) | N03, N04 |
| **Time** — as-of reads/diffs over revisions, longitudinal panels | 3 (CP, HA, MD) | N06, N07 |
| **WASM v2** — runnable apps, capability-scoped host imports, enricher hook | 3 (CP, SE, EP) | N09–N11 |
| **Self-healing sources** — closed-loop repair, compiled sources go live, X-ray loop | 3 (CP, CR, SE) | N12–N14 |
| **Analytics** — SQL/DuckDB/Parquet plane | 2 (CP, HA) | N08 |
| **Identity** — principals, scopes, spend attribution (named as a prerequisite by N01, N16, MCP) | 1 (HA) + 2 dependents | N20 |

## Full deck (ranked: corroboration, then impact)

Decision column: operator triage of 2026-09-01. Rejected (14): N06 N07 N08 N13 N17 N19 N21 N22 N26 N28 N30 N32 N34 N37.

| # | Title | Group | Contexts | Size | Gate | Corr. | Impact | Effort | Risk | Extends | Decision |
|---|---|---|---|---|---|---:|---:|---:|---:|---|---|
| N09 | WASM apps v2: hot-loadable ScrapeApps with AppContext host imports | Core Platform | engine-contracts, app-runtime, vcr-testing, data-pipeline-catalog, wasm-plugin-host, tiered-fetcher, http-engine | XL | contract | 4 | 9 | 9 | 6 | M28 v1 | **accept** |
| N01 | Transact v2: approval-gated live actions with a transactions ledger and agent tools | Core Platform | engine-contracts, app-runtime, dataset-storage, browser-engine, http-engine, browser-transact, agentic-research | XL | irreversible | 4 | 9 | 8 | 8 | M06 Transact v1 | **accept** |
| N16 | Pumper Mesh: signed node identity, scheduled peer sync of datasets, host weather and recipes | HTTP API | job-search-api, dataset-api, automation-api | XL | contract | 4 | 8 | 8 | 6 | M01 host-weather v1 | **accept** |
| N06 | Dataset time machine: as-of reads, interval diffs and lifespan analytics over revisions | Core Platform | dataset-storage, app-runtime, dataset-api, automation-api | L | contract | 3 | 8 | 6 | 3 | M12 | reject |
| N03 | Workflow runs: declared multi-step plans with join barriers, templating and one receipt | Job Orchestration | job-worker, cron-scheduler, trigger-pipeline, datahub-bridge, webhook-delivery, automation-api, job-search-api | XL | contract | 3 | 8 | 8 | 6 | M11 derived datasets on trigger DAGs + M | **accept** |
| N17 | Fabric v2: browser-capable satellite nodes, profile affinity and a cluster governor | Scraping Engines | remote-engine, browser-engine, tiered-fetcher, http-engine | XL | contract | 3 | 7 | 8 | 6 | M17 | reject |
| N18 | Elastic executor plane: outbound worker nodes draining the same job queue | Job Orchestration | job-worker, cron-scheduler | XL | policy | 3 | 7 | 9 | 8 | M17 distributed fetch fabric | **accept** |
| N13 | Compiled sources go live: promote a proposal straight into a scheduled, self-repairing pipeline | Content & Research Apps | source-provisioner, declarative-extractor, web-crawler, page-monitor | XL | policy | 2 | 10 | 8 | 6 | M44 | reject |
| N12 | Self-healing extraction: closed-loop repair over the source's own history | Core Platform | source-resilience, extraction-core, dataset-storage, app-runtime, vcr-testing | XL | policy | 2 | 10 | 9 | 7 | M09 | **accept** |
| N05 | Durable event log with cursor subscriptions: one outbox for every event kind | Event Pipeline | webhook-delivery, trigger-pipeline, datahub-bridge | XL | contract | 2 | 9 | 8 | 5 | M21 | **accept** |
| N07 | Longitudinal panel layer: stock datasets keep every vintage as a time series | Market Data | czech-labor-market, us-business-census, trades-pricing, trades-operator-economics | XL | contract | 2 | 9 | 8 | 5 | M12 | reject |
| N08 | Analytical plane: SQL over datasets and revisions with cross-app derived joins | Core Platform | dataset-storage, engine-contracts, extraction-core, dataset-api, job-search-api, automation-api | XL | policy | 2 | 9 | 8 | 5 | M11 derived datasets | reject |
| N02 | Jobs that wait: a `waiting` lifecycle state for human/agent-in-the-loop work | Job Orchestration | job-worker, cron-scheduler | XL | contract | 2 | 9 | 7 | 6 | M23 durable execution | **accept** |
| N20 | Identity & tenancy plane: scoped API keys, per-principal budgets, cost attribution | HTTP API | api-surface, automation-api, dataset-api, job-search-api | XL | policy | 2 | 9 | 8 | 6 | ingress per-source HMAC secrets | **accept** |
| N23 | Consumer plane v2: typed response schemas, generated SDKs and CLI, live dataset stream | HTTP API | api-surface, dataset-api, job-search-api | L | contract | 2 | 8 | 6 | 3 | openapi-spec + M29 MCP + @pumper/sync | **accept** |
| N19 | Artifact peering: mirror the crawl archive, not just records, so any node can extract, replay and audit another node's corpus | Content & Research Apps | dataset-peering, declarative-extractor, plugin-runner, web-crawler | XL | contract | 2 | 8 | 8 | 6 | M30 dataset peering | reject |
| N10 | Capability-scoped host imports: WASM sinks and connectors as third-party plugins | Event Pipeline | wasm-plugin-examples, webhook-delivery, trigger-pipeline | XL | policy | 2 | 8 | 9 | 7 | M15 | **accept** |
| N04 | Param binding and per-record fan-out: triggers an agent can author over MCP | Event Pipeline | trigger-pipeline, wasm-plugin-examples, webhook-delivery | L | contract | 2 | 7 | 6 | 4 | M21 | **accept** |
| N11 | Index-time enrichment as a plugin hook: typed entity fields without schema wipes | Scraping Engines | search-engine, wasm-plugin-host | L | contract | 2 | 7 | 6 | 4 | M14 | **accept** |
| N32 | Portal-grants: a recipe-driven state/national grant portal family (NY, TX, IL, OH, AU) | Grants Intelligence | us-state-grants, grants-unified-layer | XL | policy | 2 | 7 | 9 | 7 | M19 | reject |
| N30 | Awards layer: grants/awards + funder-recipient graph from USAspending and CORDIS | Grants Intelligence | eu-grants, us-federal-grants, grants-unified-layer | XL | contract | 1 | 9 | 9 | 5 | M31 | reject |
| N15 | Self-hosted agent loop: the Claude research tier drives pumper's own engines over MCP | Scraping Engines | claude-engine, tiered-fetcher, http-engine, archive-engine, browser-engine | XL | policy | 1 | 9 | 8 | 6 | M43/M29 | **accept** |
| N28 | NOFO document corpus: fetch attachments, extract text, type the requirements | Grants Intelligence | us-federal-grants, grants-unified-layer, eu-grants | XL | contract | 1 | 9 | 8 | 6 | M33 | reject |
| N29 | Program registry: the funding program, not the posting, as the unit of intelligence | Grants Intelligence | grants-unified-layer, us-federal-grants, us-state-grants, eu-grants | L | contract | 1 | 8 | 6 | 3 | M34 | **accept** |
| N33 | State x trade market profile: trades economics joined to census density, one row, one MCP tool | Market Data | trades-operator-economics, trades-pricing, us-business-census | L | contract | 1 | 8 | 5 | 3 | M35 | **accept** |
| N14 | API X-ray closes its own loop: auto-capture, auto-discover, auto-validate, router-learned | Scraping Engines | tiered-fetcher, browser-engine, http-engine | L | contract | 1 | 8 | 5 | 4 | M05 | **accept** |
| N26 | Explained change: typed, LLM-summarised change events for any watched URL or dataset | Content & Research Apps | page-monitor, connector-api-watch, plugin-runner, web-crawler | L | policy | 1 | 8 | 5 | 4 | page-monitor | reject |
| N31 | Applicant fit engine: profile-driven eligibility matching that fires as an event | Grants Intelligence | grants-unified-layer, us-federal-grants, us-state-grants, eu-grants | L | contract | 1 | 8 | 6 | 4 | M13 | **accept** |
| N34 | Employer hiring scorecard: per-IČO time-to-close, repost rate and pay positioning from the survival ledger | Market Data | czech-labor-market | L | policy | 1 | 8 | 5 | 4 | M37 | reject |
| N25 | Research as a living knowledge base: findings and cited sources become watched, re-derivable datasets | Content & Research Apps | agentic-research, page-monitor, connector-api-watch, declarative-extractor | XL | policy | 1 | 8 | 7 | 5 | M23 | **accept** |
| N21 | Preemptive, deadline-aware scheduling: suspend-to-checkpoint as a scheduler move | Job Orchestration | job-worker, cron-scheduler | L | contract | 1 | 8 | 6 | 6 | M23 durable execution | reject |
| N27 | Corpus graph intelligence: PageRank, importance-weighted revisits and structural drift from the persisted link graph | Content & Research Apps | web-crawler, plugin-runner, declarative-extractor | L | none | 1 | 7 | 5 | 3 | M08 | **accept** |
| N36 | Codebook-resolved skills demand: MPSV číselníky turn opaque URIs into a readable, ESCO-linkable product | Market Data | czech-labor-market | L | none | 1 | 7 | 4 | 3 | shipped `skill_demand` / `education_agg` | **accept** |
| N37 | Localized Medicare price oracle: GPCI join gives a dollar price per HCPCS x locality per release | Market Data | trades-pricing | L | contract | 1 | 7 | 4 | 3 | M32 | reject |
| N22 | Maintenance as system jobs: backfill, reindex, retention and doctor run on the queue, online | Job Orchestration | maintenance-tooling, job-worker, cron-scheduler | L | contract | 1 | 7 | 5 | 4 | M23 durable execution + the quiet-window | reject |
| N35 | Sub-state launch atlas: county/metro-grain blend, saturation and pricing | Market Data | us-business-census, trades-pricing | L | contract | 1 | 7 | 6 | 5 | M39/M40 | **accept** |
| N24 | Vendor-neutral lineage and quality push: OpenLineage runs plus assertions from Pumper's own verdicts | Event Pipeline | datahub-bridge, trigger-pipeline, webhook-delivery | L | none | 1 | 6 | 6 | 3 | M25 | **accept** |

Per-group reports with the full §4.10 card bodies and `file:line` evidence:
- [Scraping Engines](scraping-engines.md)
- [Core Platform](core-platform.md)
- [HTTP API](http-api.md)
- [Job Orchestration](job-orchestration.md)
- [Event Pipeline](event-pipeline.md)
- [Content & Research Apps](content-research-apps.md)
- [Grants Intelligence](grants-intelligence.md)
- [Market Data](market-data.md)

Merged cards (same spine, different scout — kept in the group reports, folded into one deck item):
- N01 ← SE6 "Transact v2: approval ledger and live submit under the same evidence contract", CR2 "Transact v2: approval-gated live submission with a transactions ledger and agent-facing approvals"
- N03 ← EP2 "Pipeline runs: root correlation id, fan-in barriers, end-to-end SLA across trigger hops", HA6 "Workflows: declarative multi-step pipelines with fan-in joins and run-level receipts"
- N06 ← HA4 "Bitemporal dataset reads: as-of snapshots and interval diffs over the revision store"
- N08 ← HA2 "Analytical query plane: read-only SQL/DuckDB over datasets, Parquet export, typed columns"
- N09 ← SE3 "Runnable dynamic apps: component-model host with governed fetch/storage imports"

## Scout notes (what was read in full, hypotheses discarded)

- **Grants Intelligence**: Read in full (non-test code): crates/apps/grants-common/src/lib.rs (all 3387 lines incl. tests), grants-gov/src/lib.rs 1-1552, eu-sedia/src/lib.rs 1-645, cordis/src/lib.rs 1-1059, ca-grants/src/lib.rs 1-464, smlouvy-dump-watch/src/lib.rs 1-541; plus docs/features/apps.md grants rows, search.md, events-webhooks.md, triggers.md, mcp.md, routes/query.rs headers, catalog/data-sources.toml grants + planned entries, moonshot INDEX + funding-grants.md, dirs.txt, core engine.rs fetch_bytes. Hypotheses traced and discarded: (a) a Czech public-money awards layer from Registr smluv dumps + CEDR - smlouvy-dump-watch is index-only by explicit design (lib.rs:16-18) and fetch_bytes is in-memory-capped (engine.rs:1187), so ~100 MB dumps need the still-open streaming download item; not proposed. (b) Adding a `currency` field to unified alone - M-sized, folded into the awards card. (c) Cross-source multi-stage deadline modelling (SEDIA cutoffs) - already handled by sedia_deadline (grants-common:535-558). (d) Grants as MCP tools - M03/M29 shipped and grants-gov already ships tool manifests (mcp.md:81). (e) Per-agency extension-rate rollup alone - M-sized; folded into the program registry card.

## How to resume

The 23 accepted items are the generation-2 build queue. Next step is a DESIGN-BATCH pass in the 2026-07-30 campaign's shape (file-scope partition, v1 slices, orchestrator protocol), ordered by corroboration: the act-on-the-web pair (N01+N02) and identity (N20) first since N01, N16 and MCP approvals all name N20 as a prerequisite; then WASM v2 (N09, N10, N11), pipelines (N03, N04, N05), federation (N16, N18), self-healing (N12, N14, N15), consumers (N23, N24), and the domain products (N25, N27, N29, N31, N33, N35, N36).
