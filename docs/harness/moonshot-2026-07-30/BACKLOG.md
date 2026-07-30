# Moonshot Backlog — accepted at triage 2026-07-30

> 14 items (19 M-ids of 44) accepted via multi-select triage. Each item is a **Pipeline-A-sized program**, not a fix — convert one per session (design on strongest model + deep thinking, per user preference). Full entries with Path/Risks/Differentiation in the per-group reports; index in INDEX.md.

## The program head

1. **M29 — Pumper as an MCP server** *(merges M03 registry-as-tools + M43 research/readable gateway; substrate M27 typed tool manifests)* — T1/high/weeks. 3-of-7-scanner convergence. rmcp beside the REST router; tools = enqueue/search/query_dataset/fetch_readable/deep_research; EventBus subscriptions; utoipa-derived schemas.

## Platform & control plane

2. **M19 — Catalog as GitOps control plane** — T1/high/weeks. Reconciler materializes data-sources.toml into schedules; TOML PR = pipeline deploy.
3. **M21 — Inbound event ingress** — T1/high/weeks. HMAC POST /ingest/{source} → EventBus `external` events → trigger DAGs.
4. **M23 — Durable execution** — T1/high/months. ctx.checkpoint()/restore(); reap/drain become suspend/resume.

## Data substrate

5. **M42 — Versioned crawl archive + retroactive backfill** — T1/med/months. Append-only url@revision artifacts; run any new rule/plugin over history. (Unlocks deferred M10 replay-CI + M16 observatory later.)
6. **M18 — Tier-zero archive engine (Wayback/Common Crawl)** — T2/high/weeks. Archived snapshots as a free fetch tier + historical backfill.
7. **M11 — Derived datasets (incremental dataflow)** — T1/med/quarters. Datasets declared as transforms of datasets, recomputed on deltas via trigger DAGs.

## Fetch & runtime intelligence

8. **M05 — API X-ray** — T1/med/months. Browser tier captures XHR; emits API recipes; HTTP tier then hits the real JSON API.
9. **M07+M02 — Change-cadence learning** — T1-2/high/months. Per-URL change-rate estimation drives crawl frontier priority AND proactive cache revalidation.
10. **M04 — Information economist** — T2/med/months. Closed-loop budget allocation by marginal information value per dollar.

## Domain data products

11. **M34 — Amendment radar** — T2/high/weeks. Typed grant lifecycle events (deadline_extended, award_raised…) from semantic diffs → trigger mesh.
12. **M39+M40 — Succession-wave + formation-velocity** — T1-2/med-high. NES-D owner-age succession index + weekly BFS competition velocity, joined onto the density blend.
13. **M31 — CORDIS win-intelligence** — T1/med/months. Funded-outcome history joined onto every open SEDIA topic via topic-identifier lineage grammar.
14. **M37 — Vacancy survival ledger** — T1/med/months. Daily MPSV per-posting lifecycle diffing → time-to-fill/survival/repost analytics (irreproducible-later moat).

## Not selected (deferred, no hard rejection)

M01 M06 M08 M09 M10 M12 M13 M14 M15 M16 M17 M20 M22 M24 M25 M26 M28 M30 M32 M33 M35 M36 M38 M41 M44 — remain in INDEX.md; ledgered as deferred in the scan vault.
