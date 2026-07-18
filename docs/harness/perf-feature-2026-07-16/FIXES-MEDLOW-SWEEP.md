# Perf-Feature Scan — Medium/Low sweep (final)

> A subagent inventory confirmed the med/low backlog was essentially closed: of
> **25 Medium + 1 Low**, 21 Mediums + the Low were already shipped (med-tail
> batches 1–4) or a documented non-change (`app-registry #3`). This sweep closes
> the 2 remaining *actionable* Mediums; the other 2 stay deferred with reason.
> Baseline preserved: build clean (0 warnings), tests **268 → 269** (0 regressions).
> Branch `vibeman/medlow-sweep-2026-07-18` (off master after PR #14).

## Commits

| Commit | Finding | What |
|--------|---------|------|
| `27696bf` | config-catalog #3 | catalog freshness monitor — `GET /catalog/health` |
| `5024cd5` | eu-regulatory #3 | self-baseline the CMS release watcher (drop the hand-edited literal) |

## What was fixed

1. **The catalog couldn't answer freshness about itself (config-catalog #3).**
   Each `[[source]]` carried `status`/`cadence`/`dataset` but nothing joined them,
   so a silently-broken pipeline left the catalog asserting `live`/`confidence:5`
   forever. Its stated deferral blocker — the catalog loader — **shipped in Wave H**
   (`59e7b6b`), which unblocked this. Added `Source::cadence_secs()` (daily→24h …
   annual→366d; `on-demand`/`one-time` → no expectation) and `GET /catalog/health`:
   for each live source with a dataset + a freshness-bearing cadence, it reads the
   newest `updated_at` in that dataset (scoped to the source's `app`) and reports
   `{last_write_at, age_secs, stale}` — stale past the cadence window × a 2× grace
   (one missed run). Never-written live sources are stale by definition; no-dataset
   / no-expectation sources return `monitored:false`.

2. **A permanently-lit staleness alarm (eu-regulatory #3).** `cms-fee-schedule`'s
   `known_release: "RVU26A"` was a literal in `default_params`, only read and
   compared, never written back — and a scheduled run takes `default_params`, so
   once CMS shipped RVU26B the watcher reported `is_newer_than_known: true` forever.
   Now it **self-baselines**: reads the release it stored last run (before this
   run's upsert) as the implicit baseline (precedence: explicit `known_release`
   param > stored > none), so the alarm clears itself once a release is seen.
   Reports `baseline` + `baseline_source` (`param`/`stored`/`none`) to distinguish a
   real gap from a cold start, and adds a structured `ingest {release, zip_url,
   source_url}` so a `dataset` trigger can drive ingest instead of a human reading
   the prose hint. The clfs/asp generalization stays an honest hard error (finding
   part d).

## Still deferred (2 Mediums — genuine, not oversights)

- **trades #3** (`us-trades-wages-tax-valuation.md`): key the trades datasets by
  vintage for year-over-year. A cross-file **data-model re-key** that orphans
  existing rows and needs downstream-consumer verification — related to the
  deferred `trades #2 phase c`. Not a sweep-safe edit.
- **broad-crawler #3 SKIP-half**: skip URLs whose sitemap `<lastmod>` predates the
  last crawl. Needs a new wall-clock `last_crawled` record field, a `trust_lastmod`
  config knob (many origins lie in `<lastmod>`), and revisit-mode sitemap fetching
  — larger, caveated, and needs live verification. (The PRIORITIZE half shipped in
  med-tail 4.)

## Gate

```
cargo build --workspace   # clean, 0 warnings
cargo test --workspace    # 269 passed / 0 failed  (was 268)
```

## Campaign status after this sweep

The scan's actionable backlog is now **fully closed**. What remains across all
severities is deliberately gated: engine-traits #2 (architectural binary body),
trades #2 phase c + trades #3 (data-model / live research), grants-gov #1 (live
upstream shape), broad-crawler #3 SKIP-half (new record fields + live verify).
