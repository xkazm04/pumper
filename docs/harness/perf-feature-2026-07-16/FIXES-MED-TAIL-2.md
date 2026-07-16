# Perf-Feature Scan — Medium Tail, Batch 2: server API & observability

> 4 commits, 4 Medium findings closed (server surface: compression, search paging,
> webhook idempotency, scheduler cost).
> Baseline preserved: build clean, tests **226 → 227** (0 regressions).
> Branch `vibeman/perf-feature-2026-07-16`.

## Commits

| # | Commit | Finding | What |
|---|---|---|---|
| 1 | `0f4dde9` | http-api-routes #3 | gzip/br response compression (skips SSE, stays streaming) |
| 2 | `86fa7d9` | engine-capability-traits #3 | search `offset` paging + a true match `total` |
| 3 | `1f57c78` | live-events-webhooks #3 | stable delivery-id + timestamp headers, signed |
| 4 | `ddee6eb` | job-worker #2 | scheduler misfire enumeration → O(1) + cron cache |

## What was fixed

1. **Response compression.** The router had no `CompressionLayer` and the workspace
   didn't even compile the tower-http compression features. Record JSON is highly
   repetitive (~5–10× gzip). Added `compression-gzip`/`-br` + the layer; its default
   predicate skips `text/event-stream` (SSE keeps incremental KeepAlive) and it wraps
   the body stream (export stays constant-memory).

2. **Search paging.** Search was the one list surface with no page 2 and a
   `count` that was actually the page size (reported `20` whether 20 or 50k matched).
   Added `SearchRequest.offset` + `SearchResponse.total` (exact match count via a
   `Count` collector in the same `MultiCollector` pass), `GET /search?offset=`
   (clamped), and a real `total` field. Also hardened the test harness's `unique_dir`
   against a pre-existing same-nanosecond collision this batch's extra parallel test
   exposed.

3. **Webhook idempotency + replay window.** Deliveries carried only event + a
   body-only HMAC — no idempotency key, no replay window, so a captured signed body
   re-POSTed as authentic forever and consumers had to dedup blind. Added
   `x-pumper-delivery-id` (stable across retries and `/replay`) + `x-pumper-timestamp`
   (per attempt), and moved the signature base to
   `HMAC(secret, "{ts}.{delivery_id}." ++ body)`. Documented the contract.

4. **Scheduler O(1) tick.** `decide` walked the entire missed-firing backlog (up to
   10k firings) every tick to compute a count only the misfire-skip path needs — and
   the overlap guard keeps a blocked schedule "due" for many ticks, so a per-minute
   schedule behind a 6-hour run re-enumerated ~360 firings *per tick*, delaying
   unrelated schedules. Now the earliest firing is one iterator step; the Fire path
   bounds enumeration to 64 (diagnostic `collapsed`, O(1)); parsed crons are cached
   across ticks.

## Verification

| Gate | Before | After |
|---|---|---|
| `cargo build --workspace` | clean | clean |
| `cargo test --workspace` | 226 / 0 | 227 / 0 |
| OpenAPI route-coverage | pass | pass |

New test: search offset pages return distinct windows and `total` is the match
count across pages. Compression/webhook/scheduler covered by existing tests + the
route-coverage test; the two named scheduler tests pass unchanged.

## Patterns established (catalogue additions)

19. **Report the total, not the page size.** A `count` fed from `hits.len()` lies as
    a denominator. Compute the real total (a `Count` collector rides the same search
    pass) and expose `offset` for page 2.
20. **A webhook needs a stable idempotency key + a signed timestamp.** Body-only
    HMAC with no id/ts gives receivers no dedup and an unbounded replay window. Sign
    `"{ts}.{id}." ++ body`; keep the id stable across retries/replays.
21. **Compute the count only the branch that needs it uses.** The scheduler walked
    the whole backlog for a number one arm ignored. Find the cheap discriminator
    (the earliest firing) first, and bound the diagnostic enumeration.

## Remaining Medium/Low tail (open, ~11)

app-registry #2/#3(Low), broad-crawler #3, config-catalog #2/#3, czech #2,
declarative-extraction #3, eu-funding #3, extraction-crawl-api-watch #3, census #3,
trades #3, web-research #2/#3. Plus the larger open **Highs** the themed waves
didn't reach and the two deliberate deferrals (crawl delta-journal; grants
money-enrichment — needs a live `fetchOpportunity` response).
