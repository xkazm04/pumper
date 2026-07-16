# Perf-Optimizer + Feature-Scout Scan — pumper, 2026-07-16

> Dual-lens audit: every context scanned by one subagent wearing both the
> **perf-optimizer** ⚡ and **feature-scout** 🔍 hats, 3 findings per context.
> 21 parallel subagent runs across all 21 contexts / 7 groups.
> Baseline at scan time: `cargo build --workspace` clean, `cargo test --workspace` 192/0,
> master at `8753dd3` (PR #4 merged).

---

## Totals

| | Critical | High | Medium | Low | **Total** |
|---|---:|---:|---:|---:|---:|
| Across 21 contexts | 1 | 36 | 25 | 1 | **63** |
| Share | 1.6% | 57.1% | 39.7% | 1.6% | 100% |

| Lens | Count | Share |
|---|---:|---:|
| feature-scout 🔍 | 32 | 50.8% |
| perf-optimizer ⚡ | 31 | 49.2% |

Verified two ways: `^> Total:` header sum = **63**; `^- **Severity**:` bullet count = **63**. ✅

The severity curve is deliberately High-heavy and Critical-light. This is the *fourth*
campaign on pumper — the 2026-07-14 dual-lens scan already closed all 4 criticals and
19 highs, so the remaining defect surface is mostly "works correctly, costs too much" and
"capability that was never built", not "broken". Agents were explicitly instructed not to
inflate; one returned a deliberate **negative result** (app-registry #3: all app structs
are ZSTs, runtime cost nil, not worth gating).

---

## Per-context breakdown

Sorted by criticals desc, then by highs desc.

| # | Context | Group | C | H | M | L | Total | Report |
|---|---|---|---:|---:|---:|---:|---:|---|
| 1 | EU & Regulatory Funding Watchers | Public Funding | 1 | 1 | 1 | 0 | 3 | [eu-regulatory-funding-watchers.md](eu-regulatory-funding-watchers.md) |
| 2 | Dataset Store & Change Detection | Data Extraction | 0 | 3 | 0 | 0 | 3 | [dataset-store-change-detection.md](dataset-store-change-detection.md) |
| 3 | Broad Crawler | Data Extraction | 0 | 2 | 1 | 0 | 3 | [broad-crawler.md](broad-crawler.md) |
| 4 | Czech Labour Market (MPSV) | Economic & Labor | 0 | 2 | 1 | 0 | 3 | [czech-labour-market-mpsv.md](czech-labour-market-mpsv.md) |
| 5 | Declarative Extraction Engine | Data Extraction | 0 | 2 | 1 | 0 | 3 | [declarative-extraction-engine.md](declarative-extraction-engine.md) |
| 6 | Engine Capability Traits | Runtime Core | 0 | 2 | 1 | 0 | 3 | [engine-capability-traits.md](engine-capability-traits.md) |
| 7 | Fetch Engines (HTTP/Browser/Claude) | Scraping Engines | 0 | 2 | 1 | 0 | 3 | [fetch-engines-http-browser-claude.md](fetch-engines-http-browser-claude.md) |
| 8 | Full-Text Search Index | Scraping Engines | 0 | 2 | 1 | 0 | 3 | [full-text-search-index.md](full-text-search-index.md) |
| 9 | HTTP API & Routes | Job Server & API | 0 | 2 | 1 | 0 | 3 | [http-api-routes.md](http-api-routes.md) |
| 10 | Job Worker & Cron Scheduler | Job Server & API | 0 | 2 | 1 | 0 | 3 | [job-worker-cron-scheduler.md](job-worker-cron-scheduler.md) |
| 11 | Live Events & Webhooks | Job Server & API | 0 | 2 | 1 | 0 | 3 | [live-events-webhooks.md](live-events-webhooks.md) |
| 12 | Tiered Fetcher & Politeness | Runtime Core | 0 | 2 | 1 | 0 | 3 | [tiered-fetcher-politeness.md](tiered-fetcher-politeness.md) |
| 13 | US Grant Opportunities | Public Funding | 0 | 2 | 1 | 0 | 3 | [us-grant-opportunities.md](us-grant-opportunities.md) |
| 14 | US Trades Wages, Tax & Valuation | Economic & Labor | 0 | 2 | 1 | 0 | 3 | [us-trades-wages-tax-valuation.md](us-trades-wages-tax-valuation.md) |
| 15 | WASM Plugin Sandbox | Scraping Engines | 0 | 2 | 1 | 0 | 3 | [wasm-plugin-sandbox.md](wasm-plugin-sandbox.md) |
| 16 | App & Job Model | Runtime Core | 0 | 1 | 2 | 0 | 3 | [app-job-model.md](app-job-model.md) |
| 17 | Configuration & Data Source Catalog | Job Server & API | 0 | 1 | 2 | 0 | 3 | [configuration-data-source-catalog.md](configuration-data-source-catalog.md) |
| 18 | Extraction, Crawl & API Watch | Content & Research | 0 | 1 | 2 | 0 | 3 | [extraction-crawl-api-watch.md](extraction-crawl-api-watch.md) |
| 19 | US Trades Business Density (Census) | Economic & Labor | 0 | 1 | 2 | 0 | 3 | [us-trades-business-density-census.md](us-trades-business-density-census.md) |
| 20 | Web Research & Readable Content | Content & Research | 0 | 1 | 2 | 0 | 3 | [web-research-readable-content.md](web-research-readable-content.md) |
| 21 | App Registry | Job Server & API | 0 | 1 | 1 | 1 | 3 | [app-registry.md](app-registry.md) |

---

## The one Critical

**eu-sedia is not wired into `grants/unified`** — `eu-regulatory-funding-watchers.md` #1.
`crates/apps/eu-sedia/Cargo.toml` has no `grants-common` dependency, no `finalize_unified`
call exists, and no `normalize_eu_sedia` exists in grants-common. Confirmed by three
independent signals. The entire pan-EU corpus is therefore invisible to `GET /grants`,
cross-source dedup, `sweep_closed`, and per-opportunity search indexing. This was carried
as a known open item from the 2026-07-14 campaign and is now verified as still true.

Two silent-corruption traps a naive fix walks straight into (both discovered during the
field-mapping work, both worth more than the finding itself):
- SEDIA statuses are **numeric codes** (`31094502`); `norm_status` passes unknowns through,
  so a naive mapping writes `"31094502"` into `status` and breaks every `?status=open`
  filter *and* the sweep predicate.
- `budgetOverview` is **EUR** and unified has **no currency dimension** — map money to
  `Null` rather than filing euros as dollars in `min_award`.

---

## Triage themes

Clustered on Category + mechanism across all 63. Each is a coherent wave — one mental
model, fixes that compound.

| Theme | ~Count | Why this is a wave, not scattered fixes |
|---|---:|---|
| **A. Metering & politeness blind spots** | 4 | The platform's own controls silently don't apply on its highest-volume paths. Same root shape: a guard wired at the wrong seam. |
| **B. Write amplification** | 5 | Every one is "we rewrite the whole corpus to record a handful of changes". Fixing them shares the revisions-intersection technique. |
| **C. Hot-path waste** | 7 | Work computed then discarded. Each is a local, well-pinned change with a measurable before/after. |
| **D. Concurrency & resource bounds** | 6 | Caps that exist per-unit but not globally; unbounded fan-out. All need the same semaphore/budget reasoning. |
| **E. Grants coverage & truth** | 5 | The `/grants` product surface is incomplete or lying. Contains the only Critical. |
| **F. Caching & research cost** | 5 | Dollars and latency on the agentic path + revalidation. Shares the "what do we already hold?" question. |
| **G. Query & API surface gaps** | 6 | Capabilities that exist internally but aren't reachable; one is ordering-correctness, not just ergonomics. |
| **H. Introspection & operability** | 7 | The system can't tell you what it will do, what it needs, or why it isn't running. |
| **I. Domain data model** | 7 | Data already fetched/parsed but discarded or under-keyed. Highest product leverage per line changed. |
| **J. Extraction & crawl power features** | 6 | The capability ceiling of the tooling apps. |
| **K. Store lifecycle & tail** | 3 | Retention, config override, honest build-cost note. |

---

## Suggested wave split

Ordered by **truth first, then cost, then capability**. Rationale: a wave that makes the
product *lie less* outranks one that makes it *cost less*, and both outrank new surface.

| Wave | Theme | Findings | Why here |
|---|---|---:|---|
| **1** | Grants coverage & truth (E) | 5 | Contains the Critical. `/grants` is the most product-shaped surface pumper has, and today it silently omits the EU corpus and has permanently-null federal money fields. |
| **2** | Write amplification (B) | 5 | Biggest measurable cost win, and the full-reindex one grows with the corpus forever — it gets worse every day it's deferred. |
| **3** | Metering & politeness integrity (A) | 4 | Correctness of the platform's own controls. The governor gap is a ban risk on exactly the hosts already hostile to us. |
| **4** | Concurrency & resource bounds (D) | 6 | Unbounded fan-out is the remaining OOM/thundering-herd surface. |
| **5** | Hot-path waste (C) | 7 | Pure cost, low risk, each independently pinnable by a test. |
| **6** | Caching & research cost (F) | 5 | Dollars on the agentic tier + the ETag revalidation win for `watch` workloads. |
| **7** | Query & API surface (G) | 6 | `closing-soon` mis-ordering is a correctness bug hiding in an ergonomics theme — consider promoting it into Wave 1. |
| **8** | Introspection & operability (H) | 7 | Makes silent failure visible. `default_params` replace-vs-merge is a real mis-configuration bug. |
| **9** | Domain data model (I) | 7 | Highest product leverage: data already fetched, just discarded. |
| **10** | Extraction & crawl power (J) | 6 | Raises the tooling ceiling; the repeating-container rule is the standout. |
| **11** | Store lifecycle & tail (K) | 3 | Retention API + the honest negatives. |

**Recommended start: Wave 1.** It closes the Critical, and the two corruption traps mean
it needs a careful hand rather than a mechanical one — best done while the scan context is warm.

**Note on Wave 7 #1** (`closing-soon` sorts by the wrong column): this is arguably a
Wave 1 item. It pulls 1000 rows ordered by `updated_at DESC` then sorts by `days_left`
in memory, so past 1000 matches (routine at `?days=365`) "soonest first" is an arbitrary
slice — a grant closing tomorrow can be silently omitted, and `count` saturates at 1000
with no truncation signal. It is a truth bug wearing a perf costume.

---

## Cross-cutting observations

Three things surfaced that are bigger than any single finding:

1. **The "wrong seam" pattern recurs.** Governor wired inside `HttpEngine` instead of at
   the tier seam; `salvage_json` living in `trades-common` is why `research` never got it;
   `JsonFilter` reachable only via a hardcoded `/grants` route. Each time a capability was
   built one layer too low, everything above it silently missed out. Worth a proactive grep
   in future audits.
2. **`HttpResponse.body: String` is a structural ceiling.** It makes PDFs/ZIPs unfetchable
   (`cms-fee-schedule` says so in its own comment), forces mpsv-vpm to hold ~188 MB in RAM,
   and blocks streaming. Named independently by 3 agents. This is the single highest-leverage
   architectural change available and deserves its own design pass, not a wave slot.
3. **Prior-campaign docs were partly stale.** OpenAPI is fully shipped (utoipa + a
   path-coverage test) despite being carried as "deferred". `catalog/data-sources.toml` is
   inert and has drifted from the registry. Verify-before-fix keeps paying.

## Out-of-lens finding worth a bug-hunter pass

The broad-crawler agent surfaced something outside both lenses and correctly declined to
file it as perf/feature: `artifact_name` is `format!("page-{:04}.html", stats.kept)` and
`stats.kept` **restarts at 0 on a checkpoint resume** — so a resumed crawl overwrites the
prior run's `page-0001.html`…, leaving earlier `pages` records' `artifact_path` pointing at
bodies for different URLs. That's data-integrity corruption. Recommend a targeted fix
regardless of which wave runs.

---

## How this scan was run

- **Scanners**: `perf-optimizer` (⚡ `src/lib/prompts/registry/agents/perf-optimizer.ts`) +
  `feature-scout` (🔍 `feature-scout.ts`), both from the Vibeman prompt registry, fused into
  a single dual-lens prompt per context.
- **Date**: 2026-07-16. **Scope**: all 21 contexts / 7 groups, full-stack.
- **Method**: 21 parallel `general-purpose` subagents, one per context, dispatched in 3 waves.
  Each read its context's files in full plus whatever it needed to *verify* claims, wrote one
  report, and replied with terse stats only. The orchestrator never read the reports during
  scanning (context discipline).
- **Findings target**: exactly 3 per context, lens balance left to the agent's judgement
  based on what the context actually needed. Result: 32/31 feature/perf — near-even without
  being forced.
- **Anti-duplication**: every subagent was given the shipped-work list (`docs/features/`,
  both prior INDEXes, the deliberate non-dedups, and the deferred product decisions) and
  instructed that a finding with a false premise is worse than no finding. Multiple agents
  reported explicitly declining to file known-open or already-shipped items.
- **Verification**: header sum (63) == bullet count (63). ✅
- **Baseline**: build clean, 192/0 tests, master `8753dd3`.
