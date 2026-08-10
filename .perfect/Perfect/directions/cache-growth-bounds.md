---
slug: cache-growth-bounds
type: perfect/direction
context: "[[tiered-fetcher]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-11
commit: ca4bbe9
---

## What & why
Three stores grow without bound on a default local-first deployment: `revalidations`
is appended on every demand-path revalidation but its only pruner lives inside the
refresher pass, which is unreachable at the shipping `[refresher] enabled = false`;
`http_cache` has no size/row cap and a continuously-refreshed entry never expires (the
hourly janitor deletes expired rows only); `research_cache` has no purge path at all.
An unattended box's pumper.db grows monotonically forever. This continues the
retention arc (rounds 4–7) into the cache layer.

## Evidence
- `crates/engine-http/src/lib.rs:688` + `crates/core/src/cache.rs:172` — revalidation
  writers on the always-on demand path.
- `crates/server/src/refresher.rs:45–47, 68` — the only prune_revalidations call,
  behind the enabled gate; config.toml:78–79 default false.
- `crates/core/src/cache.rs:291` + `crates/server/src/main.rs:275–288` — hourly
  purge_expired, expired-only; refresh() extends expires_at indefinitely (cache.rs:160).
- ResearchCache (cache.rs:408–498) — no purge/prune method exists.
- `crates/core/src/storage.rs:1658` — prune_ledgers covers cost_events,
  webhook_deliveries, job_yield, saved_search_seen; none of the three above.

## Acceptance criteria
- [ ] prune_revalidations runs from the always-on janitor path regardless of
      [refresher] — extracted + test (revalidations_pruned_without_refresher).
- [ ] research_cache expired entries are purged on the same janitor cadence — test.
- [ ] http_cache gets a real optional bound — `[cache]` max_rows and/or max_bytes
      (default generous but finite; document the default's rationale) with
      oldest-first eviction in the janitor pass; test proves eviction triggers and
      fresh entries survive.
- [ ] Janitor passes stay bounded (batched deletes; no unbounded full-table work per
      tick beyond the existing purge query shape).
- [ ] Config keys in config.rs defaults + config.toml comments; doc-sync: fetching.md
      cache section + runtime.md config table.

## Risks / non-goals
- Non-goal: LRU exactness, per-URL invalidation API, freshness() truncation fix
  (recorded in the context note).
- Eviction must not fight the refresher when it IS enabled (evict by
  created_at/last-refresh age, not by expires_at alone).
- SQLite: keep deletes batched (LIMIT) — the pool is shared with the job queue.

## Build record
Shipped `ca4bbe9` (Lot F, opus, 2026-08-11). All three stores bounded via the always-on
hourly store_janitor: prune_revalidations moved out of the refresher pass (kept the one
[refresher] retention_days knob, documented as always-applied); ResearchCache::
purge_expired (the most expensive bytes had NO purge); [cache] max_rows default 20 000
(≈1–2 GB at real page sizes, arithmetic in the config comment), oldest-created_at-first
≤5000/pass — created_at is what refresh() moves forward, so the janitor keeps exactly
what the refresher keeps warm (proved by a_refreshed_entry_outlives_an_older_untouched_
one). Builder refutations: max_bytes rejected (SUM(LENGTH(body)) reads every body per
tick); new CacheConfig field broke exhaustive literals in core/tests/cache.rs → minimal
2-line write-set exception, verified clean before commit. Review: keep.
