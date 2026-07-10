# Vision Scan Fix Wave 2 — Cost & Budget Spine

> 5 commits, 5 ideas closed + 2 duplicates absorbed (theme T2: cost & budget governance).
> Baseline preserved: build clean → build clean; tests 31 → 31, 0 failed.

## Commits

| # | Commit | Idea | Title |
|---|---|---|---|
| 1 | `23e77af` | 339c6e24 | Cost ledger: meter every fetch tier |
| 2 | `5a6739c` | aa627187 | Per-job cost and yield ledger API |
| 3 | `149f509` | 921ad081 | Metered-run cost budget ceiling with abort (absorbs c0b0740a budget-aware scheduler) |
| 4 | `1abc3b0` | 49ff07df | Budget-governed escalation to the Claude tier |
| 5 | `a3270c7` | 35bff360 | Cost-aware caching for the Claude research engine (absorbs 9a692cbb ROI dashboard → /costs + metrics) |

## What was built (one spine, bottom-up)

1. **Cost ledger substrate** (`cost_events`, migration 0007; `costs.rs`): every metered engine call attributed to its job — engine tier, url, cost_usd (Claude actual, free tiers 0.0), detail (escalation trail / cache_hit). New metered `AppContext::fetch` / `AppContext::research` wrappers; `Fetcher` now plumbs `cost_usd` + `max_budget_usd` through `FetchOutcome`/`FetchRequest` instead of discarding them. watch/readable/research apps switched as exemplars.
2. **Cost & yield API**: `GET /jobs/{id}/costs` (events, total, cost-per-fresh-record from result new/changed), `GET /costs?app=&since=` (app×engine rollup), `pumper_cost_usd{app,engine}` gauges on /metrics.
3. **Job budget ceiling** (`jobs.budget_usd`, migration 0008): enqueue accepts `budget_usd`; metered Claude calls abort once cumulative job spend reaches it, and per-call `max_budget_usd` is clamped to remaining headroom.
4. **Budget-governed escalation**: AutoWithResearch fetches downgrade to free tiers (with escalation note) when budget is gone — degrade, don't fail; explicit `ctx.research` still hard-aborts.
5. **Research cache** (`research_cache`, migration 0009; TTL `claude.research_cache_ttl_secs`, default 24h, 0 disables): identical research requests within TTL served from disk; hits recorded as zero-cost `cache_hit` events noting saved USD; `resume_session` bypasses.

## Patterns established

4. **Meter at the AppContext seam, not the engine** — engines stay storage-free; the job-scoped wrapper owns attribution, budget, and cache. Apps opt in by calling `ctx.fetch`/`ctx.research` instead of `ctx.engines.*`.
5. **Degrade-vs-abort split for budget exhaustion** — implicit spend (fetch escalation) downgrades gracefully; explicit spend (research call) errors loudly. Matches caller intent.
6. **Cache hits are ledger events too** — recording `cache_hit (saved ~$X)` at cost 0 keeps ROI honest and makes the cache's value visible in /costs.

## What remains (INDEX themes)

T4 search activation, T6 crawler maturity, T7 API surface hardening, T5 AI-assisted extraction, T9 domain data products, T10 platform plays.

## Follow-ups from this wave

- Remaining apps still call `ctx.engines.*` directly (unmetered) — migrate opportunistically; agentic apps (state-tax, trade-wages, valuation-multiples, homewyse) are the highest-value targets since they spend Claude money.
- `research_cache` has no purge job; `HttpCache::purge_expired` pattern could be extended (tiny).
- Budget check-then-spend is not transactional across concurrent metered calls within one job — fine for sequential app code, revisit if apps parallelize Claude calls.
