# Perf-Feature Scan — Wave H: Introspection & operability (Theme H)

> 4 commits, **4 High findings** closed — the theme makes silent failure visible.
> Baseline preserved: build clean, tests **242 → 253** (+11 tests, 0 regressions).
> Branch `vibeman/wave-h-introspection-2026-07-17` (off master after PR #9).

## Commits

| # | Commit | Finding | What |
|---|--------|---------|------|
| 1 | `3c98771` | app-registry #1 | publish `default_params` on `GET /apps`; shallow-merge them on enqueue (fixes a silent replace-not-merge wrong-data bug) |
| 2 | `00d35c4` | job-worker #3 | make schedules observable: `next_run`, `last_job_id`/`last_status`, `health` |
| 3 | `ebf0954` | live-events #2 | auto-drain the webhook dead-letter queue with exponential backoff |
| 4 | `59e7b6b` | config-catalog #1 | make `data-sources.toml` load-bearing: loader + `GET /catalog/sources` + a drift gate |

## What was fixed

1. **Param-opaque apps + a silent mis-config bug (app-registry #1).**
   `ScrapeApp::default_params()` was never emitted over HTTP, and `enqueue_job`
   did `body.params.unwrap_or_else(default_params)` — a wholesale **replace**, so a
   POST setting one key silently dropped every other default and ran a different
   config than its scheduled twin. Now `GET /apps` includes `default_params`, and
   an object `params` body **shallow-merges** over the defaults (`merge_params`);
   a non-object body still replaces (can't merge key-wise). Unit-tested.

2. **Silently-wedged schedules (job-worker #3).** Every reason a schedule stops
   firing (invalid cron, unregistered app, overlap guard) went only to a server
   log line, so a dead schedule showed an ever-older `last_run` and looked
   healthy. `GET /schedules` now enriches each row with `next_run`
   (`scheduler::project_next_run`, reusing the reconcile loop's exact reference
   rule so the API can't disagree with the scheduler), `last_job_id`/`last_status`
   (new `Storage::latest_job_for_schedule`), and a `health` reason
   (`ok`/`disabled`/`invalid_cron`/`unregistered_app`/`overlapping`) derived from
   the same checks the scheduler makes. `project_next_run` unit-tested.

3. **Permanent silent event loss on a brief receiver outage (live-events #2).**
   A receiver down longer than the ~6s in-process retry loop burned all 3 attempts
   and landed the delivery in the DLQ, lost forever unless a human replayed it. A
   background drain (piggybacked on the scheduler tick, gated on
   `[webhooks] auto_retry`, default on) now re-sends due `failed` deliveries with
   exponential backoff (30s → 1m → 5m → 30m → 2h + jitter), marking a row `dead`
   past the cap so the DLQ view stays meaningful. Migration `0019` adds
   `retry_count` + `next_retry_at`; storage gained `fail_delivery` (schedule or
   dead), `due_deliveries`, and an atomic `begin_delivery_retry` claim; the
   secret-resolution match was factored into a shared `resolve_secret` the manual
   replay route reuses so the two can't drift. DLQ lifecycle integration-tested.

4. **An inert, drifted catalog (config-catalog #1).** `catalog/data-sources.toml`
   was declared "the single source of truth" but no code read it, so it drifted:
   two live apps (`ca-grants`, `eu-sedia`) showed `cron = ""` while actually
   scheduled. Added `pumper_core::catalog` (`Source`/`Catalog`/`load()` mirroring
   `Config::load`), `GET /catalog/sources?market=&status=&category=`, and a
   server-crate **drift gate**: every `status = "live"` entry must name a
   registered app whose `schedule()` equals its `cron` (both directions), and every
   in-scope registered source app must appear (documented exempt-list for generic
   tooling, the `hackernews` example, and sibling-product Ledgerline/Counterbill
   consumers — mirroring the `census-*` precedent). Fixed the two drifted crons the
   gate caught.

## New config / API surface

- `GET /apps` → adds `default_params`. `POST /apps/{name}/jobs` `params` now merges.
- `GET /schedules` → adds `next_run`, `last_job_id`, `last_status`, `health`.
- `[webhooks] auto_retry` (default true) — DLQ auto-drain toggle.
- `GET /catalog/sources` — the machine-readable data-source catalog.

## Deferred

- config-catalog #1 part 3 (auto-generate the README snapshot table from the TOML)
  — the drift gate already blocks the substantive registry/cron drift; the README
  table is cosmetic.

## Gate

```
cargo build --workspace   # clean
cargo test --workspace    # 253 passed / 0 failed  (was 242)
```

## Open Highs after this wave

14 of the original 36 remain (themes F caching, I domain model, J extraction
power, C tail, plus E grants-gov #1 deferred). Themes A/B/D/G/H/K now have zero
open Highs.
