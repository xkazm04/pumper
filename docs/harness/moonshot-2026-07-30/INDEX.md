# Moonshot Scan — pumper, 2026-07-30

> Lens: moonshot-architect (Pipeline B, scan+triage only). 7 group-level subagents over 22 contexts, 2 moonshots per context = **44 moonshots**, verified two ways (headers 44 / tier-bullets 44).
> HEAD at scan: `56376a9`. Differentiation mandated against the 2026-07-10 vision scan (276 ideas) + everything shipped in the July perf-feature campaign; every finding carries an explicit **Differentiation** field naming its nearest prior idea.
> Axes: **Tier** (1 = category-defining 10x · 2 = 3-5x multiplier · 3 = directional) × **Feasibility** × **Horizon**.

## Totals

| | Tier 1 | Tier 2 | Tier 3 | Total |
|---|---:|---:|---:|---:|
| Across 22 contexts | 23 | 20 | 1 | **44** |

Feasibility: 19 high · 23 medium · 2 low. Horizon: 13 weeks · 24 months · 7 quarters.

## Convergence signals (independent multi-scanner agreement)

1. **Agent-native pumper (MCP)** — proposed independently by 3 scanners: M03 (App & Job Model), M29 (HTTP API), M43 (Web Research), with M27 (App Registry manifests) as the substrate. Strongest single program in the scan: Tier 1, high feasibility, weeks.
2. **The archived web as a time machine** — M42 (versioned append-only crawl archive + historical backfill) is the substrate; M10 (extraction time machine / CI-for-scrapers) and M16 (corpus-scale plugin observatory) are consumers; M18 (Wayback/Common-Crawl tier-zero engine) extends history before pumper existed.
3. **Learned change-cadence** — M02 (cache self-refreshing mirror) and M07 (crawl frontier scheduler) are the same learning signal applied at two layers.

## Full backlog

| ID | Context | Moonshot | Tier | Feas | Horizon |
|---|---|---|---|---|---|
| M01 | Tiered Fetcher & Politeness | Host Weather Network — federated host-intelligence exchange | 1 | high | months |
| M02 | Tiered Fetcher & Politeness | Self-refreshing mirror — learned change-cadence revalidation | 2 | high | months |
| M03 | App & Job Model | Agent-native pumper — registry as MCP tool server | 1 | high | weeks |
| M04 | App & Job Model | Information economist — closed-loop budget allocation | 2 | med | months |
| M05 | Engine Capability Traits | API X-ray — browser tier discovers the JSON API behind the page | 1 | med | months |
| M06 | Engine Capability Traits | Transact — first-class capability for acting on the web | 1 | med | quarters |
| M07 | Broad Crawler | Learned change-cadence crawl scheduler | 1 | high | months |
| M08 | Broad Crawler | Persist the link graph — queryable site atlas | 2 | high | weeks |
| M09 | Declarative Extraction | Zero-shot wrapper induction (no LLM) from simhash clusters | 1 | med | quarters |
| M10 | Declarative Extraction | Extraction time machine — replay rule edits against archive | 2 | high | weeks |
| M11 | Dataset Store | Derived datasets — incremental dataflow on trigger DAGs | 1 | med | quarters |
| M12 | Dataset Store | Reproducible records — provenance ledger + re-derive | 2 | high | months |
| M13 | Full-Text Search | Queries as datasets — materialized search views feed triggers | 1 | med | months |
| M14 | Full-Text Search | Entity-typed index — money/dates/orgs/places as fast fields | 2 | med | months |
| M15 | WASM Plugin Sandbox | WASM everywhere — one UDF runtime for every platform hook | 1 | med | months |
| M16 | WASM Plugin Sandbox | Corpus-scale extraction observatory — continuous drift testing | 2 | high | weeks |
| M17 | Fetch Engines | Distributed fetch fabric — satellite nodes, cluster governor | 1 | low | quarters |
| M18 | Fetch Engines | Tier-zero archive engine — Wayback/Common Crawl as free tier | 2 | high | weeks |
| M19 | Config & Catalog | Catalog as control plane — GitOps reconciler | 1 | high | weeks |
| M20 | Config & Catalog | Declarative data contracts enforced at publish time | 2 | high | months |
| M21 | Live Events & Webhooks | Inbound event ingress — external webhooks as trigger inputs | 1 | high | weeks |
| M22 | Live Events & Webhooks | Sink connectors — reverse-ETL via WASM sinks | 2 | med | months |
| M23 | Job Worker & Scheduler | Durable execution — checkpoint/resume for any job | 1 | high | months |
| M24 | Job Worker & Scheduler | VCR mode — record/replay job runs | 2 | med | months |
| M25 | DataHub Emitter | Emit pipeline topology — trigger DAGs as DataHub lineage | 2 | high | weeks |
| M26 | DataHub Emitter | Close the loop — DataHub governance drives pumper | 3 | med | months |
| M27 | App Registry | Agent-ready registry — typed evaluated tool manifests | 2 | high | weeks |
| M28 | App Registry | Dynamic apps — full ScrapeApps as hot-loadable WASM | 1 | low | quarters |
| M29 | HTTP API & Routes | Pumper as an MCP server (rmcp beside REST, utoipa-derived) | 1 | high | weeks |
| M30 | HTTP API & Routes | Dataset peering — revision-feed replication, data mesh | 2 | med | months |
| M31 | EU & Regulatory Funding | Win-intelligence — CORDIS outcomes joined onto SEDIA topics | 1 | med | months |
| M32 | EU & Regulatory Funding | Medicare price oracle — own the RVU parse + price diffs | 2 | med | months |
| M33 | US Grant Opportunities | NOFO document intelligence — full announcement corpus | 1 | med | quarters |
| M34 | US Grant Opportunities | Amendment radar — typed grant lifecycle events | 2 | high | weeks |
| M35 | US Trades Wages/Tax/Val | Taxonomy-as-data — self-expanding trade coverage | 1 | med | months |
| M36 | US Trades Wages/Tax/Val | Compliance layer — per-state licensing/bonding/insurance | 2 | high | weeks |
| M37 | Czech Labour (MPSV) | Vacancy survival ledger — time-to-fill from snapshot diffs | 1 | med | months |
| M38 | Czech Labour (MPSV) | Salary nowcast — project ISPV forward with posted drift | 2 | med | months |
| M39 | Census Density | Succession-wave engine — NES-D owner age → acquisition map | 1 | med | months |
| M40 | Census Density | Formation-velocity radar — weekly BFS new-business apps | 2 | high | weeks |
| M41 | Extraction/Crawl/API Watch | Web Reliability Index — longitudinal scrapeability observatory | 2 | high | months |
| M42 | Extraction/Crawl/API Watch | Retroactive schema evolution — versioned archive + backfill | 1 | med | months |
| M43 | Web Research & Readable | MCP gateway — web-access layer for the agent economy | 1 | high | weeks |
| M44 | Web Research & Readable | Speak a data source into existence — research as compiler | 1 | med | quarters |

## Reports

runtime-core.md · extraction-storage.md · scraping-engines.md · server-api.md · funding-grants.md · economic-data.md · content-research.md (full entries with Differentiation, Path, Risks per finding).

## How this scan was run

7 parallel general-purpose subagents (one per context group), each seeded with `_SCAN-BRIEF.md` + a mandatory grep of `vision-scan-2026-07-10/INDEX.md` for its contexts' prior ideas + the shipped-since list. ~59 files read across agents. Scan is read-only; triage and any conversion to implementation are separate decisions.
