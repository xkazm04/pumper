# Perf-Feature Scan — Medium Tail, Batch 4: readiness, benchmarks & freshest-first crawl

> 4 commits, 4 findings closed (1 High-ish infra, 3 Medium/perf). Closes the actionable
> tail: everything remaining is either deliberately deferred (needs live verification /
> a dedicated session) or a documented non-change.
> Baseline preserved: build clean, tests **230 → 230** (0 regressions).
> Branch `vibeman/tail2-2026-07-17` (off master after PR #6).

## Commits

| # | Commit | Finding | What |
|---|--------|---------|------|
| 1 | `50eb79a` | config-catalog #2 | per-request timeout for the bulk MPSV feed; restore the 30s fleet default in `config.toml` |
| 2 | `8b8569a` | census #3 | derive employer avg-wage + avg-establishment-size benchmarks from the EMP/PAYANN we already fetch |
| 3 | `1588cee` | app-registry #2 | apps declare `requires()` preconditions; `/apps` + `/metrics` report readiness, not just existence |
| 4 | `9d47dc5` | broad-crawler #3 | seed sitemap URLs newest-first by `<lastmod>` so a clipped sitemap keeps the freshest URLs |

## What was fixed

1. **Bulk-feed timeout scoping (config-catalog #2).** The MPSV bulk feed needs a long
   read window (188 MB body), but the global `[http] timeout_secs` had been widened to
   300s to accommodate it — which slowed failure detection for *every* fetch in the
   fleet. Now the timeout is set **per-request** on the one bulk call
   (`req.timeout_secs = Some(300)` in `mpsv-vpm`) and the global default is back to a
   snappy 30s. Fast apps fail fast again; only the one call that needs patience gets it.

2. **Employer benchmarks from data already on hand (census #3).** `census-density`
   already fetches `EMP` (employment) and `PAYANN` (annual payroll) per trade × state
   but only surfaced counts. It now derives **average annual wage** (`PAYANN*1000 / EMP`)
   and **average establishment size** (`EMP / ESTAB`) per record, plus national blended
   averages in the trade summaries — a revenue/scale benchmark for Ledgerline's target
   trades at zero extra API cost. **Money-truth guard:** when a cell is
   disclosure-suppressed the input is `None`, so the derived field is JSON `null`, never
   a fabricated `$0`.

3. **Readiness, not just existence (app-registry #2).** Apps that need external
   credentials (both Census apps → `CENSUS_API_KEY`) now return them via a new
   `ScrapeApp::requires() -> &[Requirement]` (default `&[]`). `GET /apps` emits
   `requires` + a computed `ready` bool; `/metrics` splits
   `pumper_apps{ready="true"|"false"}`. A scheduled run against an unconfigured app now
   shows up as *not ready* in the registry/metrics instead of silently failing at fetch
   time. `Requirement::Env(&'static str)` is the only variant today; the enum leaves room
   for future precondition kinds.

4. **Freshest-first sitemap seeding (broad-crawler #3, PRIORITIZE half).**
   `parse_sitemap_locs` → `parse_sitemap_entries`: each entry now carries its `<lastmod>`
   (block-scoped regex over `<url>`/`<sitemap>` elements; bare-`<loc>` fallback kept).
   `seed_from_sitemaps` sorts entries newest-`lastmod`-first (missing `lastmod` sorts
   last) before applying the 2000-URL budget, so when a large sitemap is clipped the URLs
   that reach the frontier are the recently-changed ones.

## Deliberately NOT changed / deferred (tail is now closed)

- **broad-crawler #3, SKIP half** (skip URLs whose `<lastmod>` predates our last crawl):
  **deferred.** Needs a wall-clock `last_crawled` record field, a `trust_lastmod` config
  knob (many origins lie in `<lastmod>`), and revisit-mode sitemap fetching. Larger and
  caveated — not a tail commit.
- **trades #3** (vintage-keyed unified rows): **deferred.** Multi-file re-key of the
  product's unified join output; orphans existing rows and needs live verification of the
  downstream consumers. Deserves a dedicated session.
- **config-catalog #3** (freshness monitor): **deferred.** Depends on the still-unbuilt
  catalog loader (config-catalog #1); no loader, nothing to monitor.
- **grants-gov #1** (money enrichment): **deferred.** Needs a live `fetchOpportunity`
  response shape to verify the upstream contract — don't guess it.
- **app-registry #3** (compile-time app feature-gating): **documented non-change.** The
  report itself concludes the runtime cost is negligible (20 ZST `Arc`s built once at
  boot); the only cost is compile time / image size, and gating trades that for a feature
  matrix to maintain. Superseded by the runtime-app-marketplace idea (#229). Not worth
  doing ahead of that decision.

## Gate

```
cargo build --workspace   # clean
cargo test --workspace    # 230 passed / 0 failed
```
