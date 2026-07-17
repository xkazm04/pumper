# Perf-Feature Scan — Wave F: Caching & research cost (Theme F)

> 3 commits, **3 High findings** closed — dollars + latency on the agentic tier
> and the poll/revalidation path. The last fully-actionable theme.
> Baseline preserved: build clean, tests **266 → 267** (+3 tests, 0 regressions).
> Branch `vibeman/wave-f-caching-2026-07-17` (off master after PR #12).

## Commits

| # | Commit | Finding | What |
|---|--------|---------|------|
| 1 | `0bd718a` | web-research #1 | expose `session_id` to resume a run instead of re-researching from scratch |
| 2 | `d548f21` | trades-wages #1 | gate the metered research call on domain freshness (vintage / age) |
| 3 | `a312750` | tiered-fetcher #2 | revalidate an expired cache entry with its stored ETag, not a full re-download |

## What was fixed

1. **Every follow-up cost a full agentic run (web-research #1).** The `research`
   app already returned `session_id`, and core's `ResearchRequest.resume_session`
   + `AppContext::research` already handled resumes — but the app read no param to
   feed it back, so the one seam built for multi-step research had no front door.
   Now a `session_id` param drives `resume_session`; a resumed turn uses a compact
   follow-up prompt (the agent already holds the topic + sources in session) while
   pinning the SAME JSON shape. Also exposed `max_budget_usd`; result carries
   `resumed`.

2. **No-op refreshes re-paid the priciest path (trades-wages #1).** The four
   agentic trades apps went straight to `research_json` with no check of what they
   held, so a Wednesday re-run of Monday's `state-tax year=2025` re-paid a 30-turn
   web-searching Claude run to reproduce constants frozen when the IRS published
   them — a daily schedule spending ~365× what annual facts warrant. Added
   domain-freshness short-circuits (in the apps, not the core cache): `state-tax`
   and `trade-wages` gate on **vintage** (`vintage_held` — stored `year` ==
   requested `year`, frozen facts); `homewyse-pricing` and `valuation-multiples`
   gate on **record age** (`fresh_by_age` / `fresh_by_age_where`, `max_age_days`
   default 90, since those figures drift) with homewyse scoped to the requested
   locality. `force: true` bypasses. A gated run returns `{skipped, cost_usd: 0}`.

3. **Expired-but-unchanged pages re-downloaded in full (tiered-fetcher #2).**
   `HttpCache::get` treated an expired row as a plain miss, so the engine ran a
   full unconditional GET even for an unchanged page — despite the row's headers
   already holding the origin's ETag/Last-Modified and the engine already knowing
   how to do a conditional GET. Added `HttpCache::get_stale` (returns an entry
   regardless of expiry + its validators) and `refresh` (extends TTL with no body
   rewrite, moving `created_at` forward so the `max_age` cap still measures from the
   last confirmed fetch). `HttpEngine::fetch` now, after a miss and only when the
   caller isn't running its own conditional GET, re-sends conditionally: a `304`
   refreshes + serves the stored body as a `cache_hit`; a `200` stores the change.
   The `watch`/poll workload's common case drops from body-transfer + parse to a
   few-hundred-byte round trip. The crawler's raw-304 revisit contract is untouched.

## New surface

- `research` app: `session_id` (resume), `max_budget_usd` params.
- trades apps: `force`, `max_age_days` params + a `{skipped, cost_usd: 0}` result.
- `HttpCache::get_stale` / `refresh` + `StaleEntry` (re-exported from `pumper_core`).

## Gate

```
cargo build --workspace   # clean
cargo test --workspace    # 267 passed / 0 failed  (was 266)
```

## Open Highs after this wave

5 of the original 36 remain. **Theme C tail (2) is still fully actionable** —
czech-mpsv #1 (bulk-read the n+1 store round-trips) and engine-traits #1 (facet
opt-out on search). The other 3 are gated: engine-traits #2 (binary/streaming
body — architectural), trades #2 phase c (needs live research), grants-gov #1
(needs live upstream shape). Themes A/B/D/F/G/H/I/K now have zero open Highs.
