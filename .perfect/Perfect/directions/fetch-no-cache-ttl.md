---
slug: fetch-no-cache-ttl
type: perfect/direction
context: "[[Tiered Fetcher & Politeness]]"
lens: feature
status: shipped
size: S
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: d6236d4
---

## What & why
Monitors exist to detect change, yet every tiered fetch can be served from an up-to-1h-stale cache. Expose cache bypass and per-request TTL on the tiered `FetchRequest` so the `watch` app (and any monitor) sees live bodies. Closes the #1 open follow-up in harness-learnings.

## Evidence
- `HttpRequest.no_cache` exists and is honored: crates/core/src/engine.rs:31-43, crates/engine-http/src/lib.rs:55
- `FetchRequest` never exposes it: crates/core/src/fetcher.rs:33-58; http tier builds `HttpRequest::get` (no_cache=false hardcoded): fetcher.rs:119
- `put` hardcodes global TTL though signature takes explicit ttl: crates/engine-http/src/lib.rs:148, cache.rs:75
- Impact on watch app: crates/apps/watch/src/lib.rs:59

## Acceptance criteria
- [ ] `FetchRequest` gains `no_cache: bool` and `ttl_override: Option<u64>` (serde-defaulted), threaded to the HTTP tier's `HttpRequest` and cache `put`.
- [ ] `watch` app requests fresh bodies (no_cache or short TTL via params).
- [ ] Unit tests: bypass skips cache read AND write behaves per ttl_override.
- [ ] docs/features/fetching.md + harness-learnings gap notes updated.

## Risks / non-goals
- Non-goal: per-domain TTL policy; conditional GET (ETag/If-Modified-Since) is a future direction.

## Build record
(pending)
