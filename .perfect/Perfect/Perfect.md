---
type: perfect/home
repo: pumper
updated: 2026-07-13
pool: 0
pool_target: 10
shipped_total: 22
cursor: "Fetch Engines (HTTP / Browser / Claude)"
last_session: "[[sessions/2026-07-13]]"
---

# Perfect — pumper

**Mission**: make pumper the best possible scraping/data-product service — API ergonomics, dataset quality, runtime robustness, and cost efficiency — one gated, shipped direction at a time.

**State**: pool **0/10** · phase: **Propose (round 3)** · cursor: **Fetch Engines (HTTP / Browser / Claude)** — FRESH scout brief + Director direction-seeds cached in its context note (do not re-scout). Rounds 1+2: **22/22 accepted directions shipped**, zero failed/dropped. On cooldown after round 2: Worker/Scheduler, Broad Crawler (plus round-1 contexts entering their final cooldown round).

## Queue (opportunity-ranked, 2026-07-13 init scoring)

Score = consumer reach × headroom (post waves 1–9) × strategic fit. Refined per-context at proposal time.

| # | Context | Group | Opp | Notes |
|---:|---|---|---:|---|
| 1 | Tiered Fetcher & Politeness | Scraping Runtime Core | 8 | every app benefits; `no_cache` follow-up open; self-learning tier routing unshipped |
| 2 | US Trades Wages, Tax & Valuation | Economic & Labor | 8 | unmetered Claude spend (follow-up); digital-twin / exit-readiness ideas unshipped |
| 3 | HTTP API & Routes | Job Server & API | 7 | T7 tail: auth, OpenAPI, SSE Last-Event-ID all deferred |
| 4 | Job Worker & Cron Scheduler | Job Server & API | 7 | T8: manual retry/requeue, misfire handling, adaptive cadence unshipped |
| 5 | Broad Crawler | Data Extraction & Storage | 7 | T6 maturity: sitemap discovery, crawl-delay, per-host tuning unshipped |
| 6 | Fetch Engines (HTTP/Browser/Claude) | Scraping Engines | 7 | learned rate governor, session management headroom |
| 7 | Declarative Extraction Engine | Data Extraction & Storage | 7 | T5 LLM-assisted / self-healing extraction remains |
| 8 | US Grant Opportunities | Public Funding | 6 | unified layer shipped (w6); agency behavior intel remains |
| 9 | Full-Text Search Index | Scraping Engines | 6 | fundamentals closed (w5); deferred: hybrid semantic, autocomplete, answer layer |
| 10 | Web Research & Readable Content | Content & Research | 6 | T3 provenance/citations unshipped; research digest |
| 11 | App & Job Model | Scraping Runtime Core | 6 | metering exists; migration of agentic apps incomplete |
| 12 | Engine Capability Traits | Scraping Runtime Core | 5 | schema-locked extraction, SDK crate ideas |
| 13 | Configuration & Data Source Catalog | Job Server & API | 5 | source-scout drafting catalog entries |
| 14 | WASM Plugin Sandbox | Scraping Engines | 5 | plugin manifest/versioning, polyglot SDK |
| 15 | Extraction, Crawl & API Watch | Content & Research | 5 | watch app shipped (w1); self-maintaining connectors moonshot |
| 16 | EU & Regulatory Funding Watchers | Public Funding | 5 | SEDIA clean-text shipped (w9); reopen prediction remains |
| 17 | Live Events & Webhooks | Job Server & API | 4 | mature after w4–5 (logged, signed, replayable) |
| 18 | Dataset Store & Change Detection | Data Extraction & Storage | 4 | heavily served w1–7 (revisions, removed_at, triggers) |
| 19 | Czech Labour Market (MPSV) | Economic & Labor | 4 | served w6+w9 (role_trends, salary gap) |
| 20 | US Trades Business Density | Economic & Labor | 4 | census blend shipped w9 |
| 21 | App Registry | Job Server & API | 3 | hot-reload deferred by choice; thin surface |

## Accepted pool — round 2 (5/10)

1. [[job-control-surface]] — Worker/Scheduler · feature · M
2. [[stuck-job-reaper]] — Worker/Scheduler · robustness · M
3. [[cron-maturity]] — Worker/Scheduler · api-ux · M
4. [[priority-aging]] — Worker/Scheduler · optimization · S
5. [[job-failed-webhooks]] — Worker/Scheduler · wildcard · S
6. [[crawl-pages-dataset]] — Broad Crawler · feature · M
7. [[crawl-live-progress]] — Broad Crawler · api-ux · M
8. [[crawl-honest-errors]] — Broad Crawler · robustness · M
9. [[crawl-memory-bounds]] — Broad Crawler · optimization · S
10. [[crawl-incremental-recrawl]] — Broad Crawler · wildcard · M

## Round-1 pool (all shipped)

1. [[fetch-no-cache-ttl]] — Tiered Fetcher · feature · S
2. [[structured-fetch-trace]] — Tiered Fetcher · api-ux · M
3. [[governor-hot-path]] — Tiered Fetcher · optimization · S
4. [[fetch-tier-verdicts]] — Tiered Fetcher · robustness · M
5. [[host-profiles-api]] — Tiered Fetcher · wildcard · M
6. [[trades-common-unified]] — US Trades · feature · M
7. [[trades-meter-research]] — US Trades · optimization · S
8. [[trades-output-guards]] — US Trades · robustness · M
9. [[api-pagination-errors]] — HTTP API · api-ux · M
10. [[api-streaming-bounded]] — HTTP API · optimization · M
11. [[sse-resume-graceful-shutdown]] — HTTP API · robustness · M
12. [[openapi-spec]] — HTTP API · wildcard · M

## Shipped ledger

- 2026-07-13 · US Trades: d83edfd (metering), d95ba60 (output guards), a458c2a (trades-common unified) — gates green on master.
- 2026-07-13 · HTTP API wave 1: 0a91f46 (pagination + error codes, live-server verified), 268d271 (streamed JSON export, bounded dup scan, job-timing metrics) — gates green on master.
- 2026-07-13 · Tiered Fetcher wave 1: d6236d4 (no_cache + ttl_override, watch app live bodies), 1deadf9 (governor DashMap sharding + eviction, markdown once), 11ca817 (bot-wall verdicts, 2xx-only reward, Retry-After dates, [fetcher]/[governor] config) — gates green on master.
- 2026-07-13 · Tiered Fetcher wave 2: a2bcee2 (typed TierTrace, router keys on verdict enum — also fixed latent skip-note-counted-as-strike bug), 6fad704 (tier-memory aging + persisted penalties + /hosts API, migration 0016, live-verified restart restore) — gates green. Fetcher context COMPLETE: 5/5 shipped.
- 2026-07-13 · HTTP API wave 2: 5bdb7ae (EventBus monotonic ids + Last-Event-ID replay ring + graceful shutdown drain w/ requeue-at-deadline, verified live), 343341a (OpenAPI 3.1 at /openapi.json, router+spec single-source via utoipa-axum, coverage test; Director integrated F2's /hosts routes into the spec during merge) — merged-server smoke test green. HTTP API context COMPLETE: 4/4 shipped. **Round 1 total: 12/12.**
- 2026-07-13 · Broad Crawler wave 1 (round 2): 4c132df (crawl/pages dataset via PageSink), 525ed8a (honest errors + bot-wall skipping), 4b085c3 (banded SimHash, no per-page RAM, versioned checkpoint) — gates green, 37 core tests.
- 2026-07-13 · Worker wave 1 (round 2): 49e133c (priority aging), 5a6258a (bulk retry / reset / cancel-running, attempt-fenced writes, live-verified), f04e2a8 (heartbeat reaper, migration 0017, live-verified) — gates green, 51 core + 11 server tests.
- 2026-07-13 · Crawler wave 2 (round 2): 78ad7da (live progress seam + SSE), 1c3fe35 (incremental recrawl / sentinel mode, live-E2E-verified) — gates green, 55 core + 13 server tests. Crawler context COMPLETE: 5/5.
- 2026-07-13 · Worker wave 2 (round 2): c544db2 (cron tz + misfire policy + scheduled retries, migration 0018, live-verified misfire counts), 041055b (job.failed webhooks + failure metric, live-verified HMAC delivery) — gates green, 55 core + 19 server tests. Worker context COMPLETE: 5/5. **Round 2 total: 10/10. Cumulative: 22/22.**
