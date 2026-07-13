---
type: perfect/home
repo: pumper
updated: 2026-07-13
pool: 0
pool_target: 10
shipped_total: 35
cursor: "Dataset Store & Change Detection"
last_session: "[[sessions/2026-07-13]]"
---

# Perfect — pumper

**Mission**: make pumper the best possible scraping/data-product service — API ergonomics, dataset quality, runtime robustness, and cost efficiency — one gated, shipped direction at a time.

**State**: pool **0/10** · phase: **Propose (round 4)** · cursor: **Dataset Store & Change Detection** (no cached brief — scout fresh). Rounds 1–3: **35/35 accepted directions shipped**, zero failed/dropped. On cooldown: Fetch Engines, Extraction, Grants (round 3); Worker/Scheduler, Broad Crawler (round 2). Round-1 contexts (Tiered Fetcher, US Trades, HTTP API) are OFF cooldown and eligible again.

**Strong round-4 seeds already banked** (from round-3 builder findings, no scout needed):
- Saved-search app-scoping bug: worker scopes alerts by JOB app, but `index_datasets` docs carry the virtual app (`grants`) — alerts scoped by app are silently skipped. Worker-side fix. (Job Server context.)
- `index_datasets` re-indexes the FULL dataset every run — needs incremental indexing before large datasets adopt the seam. (Job Server / Search.)
- grants: sweep_closed O(n) + link_duplicates O(n²) run on every run of both apps over the whole corpus — real scaling cliff. (Grants, after cooldown.)
- No artifact retention/GC policy anywhere — bodies accumulate in per-job dirs forever; source-mode extraction depends on them. (Dataset Store / Runtime.)

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

## Accepted pool — round 3 (5/10)

1. [[browser-resilience]] — Fetch Engines · robustness · M
2. [[browser-cheap-renders]] — Fetch Engines · optimization · M
3. [[proxy-support]] — Fetch Engines · feature · M
4. [[http-request-controls]] — Fetch Engines · api-ux · M
5. [[session-vault]] — Fetch Engines · wildcard · M
6. [[extract-from-stored-pages]] — Extraction · feature · M
7. [[ruleset-preview-endpoint]] — Extraction · api-ux · M
8. [[extraction-quality-signal]] — Extraction · robustness · M
9. [[markdown-tables-tonumber]] — Extraction · optimization · S
10. [[grants-searchable-alerts]] — Grants · feature · S
11. [[grants-lifecycle-honesty]] — Grants · robustness · M
12. [[grants-query-surface]] — Grants · api-ux · M
13. [[grants-schema-enrichment]] — Grants · optimization · M

(Round-1 and round-2 pools: all shipped — see ledger.)

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
- 2026-07-13 · **Round 3 (13/13)** — Fetch Engines 5/5: a57ee1c (browser relaunch/semaphore/honest waits), 8d3eda5 (resource blocking + recycle, live-proven), 709e84b (body cap + timeout + Retry-After retries), 9d2044f (proxy http/https/socks5), 50e03ba (session vault + cache-bypass correctness catch). Extraction 4/4: 70221c1 (per-field quality signal), ebe5f89 (markdown tables + number parsing), 66b063f (stored-pages source mode, no-double-fetch proven), 387a509 (POST /extract/preview). Grants 4/4: 94940a9 (per-record search via generic index_datasets seam + live search.matched webhook), 9d18132 (close-date sweep + drift guard), d59b307 (taxonomies + real money parsing — builder corrected the scout's guessed CA columns against the live API), c526d9f (GET /grants filters + closing-soon, verified vs SQL over 1,988 live records). Merged-server smoke test green (49 OpenAPI paths). **Cumulative: 35/35.**
