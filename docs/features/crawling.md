# Broad crawler

High-concurrency frontier crawler (`crawl()` in core; exposed as the `crawl` app). Bounded, deduplicated URL frontier feeding up to 256 concurrent fetch tasks; page bodies stream to the job's artifacts dir; SimHash drops near-duplicate pages.

## CrawlConfig (app params)

`seeds` (required), `max_pages` (50), `max_depth` (2), `concurrency` (16), `same_domain` (true), `dedup_distance` (3, 0 disables), `respect_robots` (true), `include_patterns` / `exclude_patterns` (regex; include = any-must-match, exclude drops after; **seeds exempt**), `sitemap_seeds` (false), `checkpoint` (name string → resumable frontier).

## Behaviors

- **Canonicalization**: discovered links + seeds are normalized before the frontier — fragment stripped, tracking params dropped (`utm_*`, `gclid`, `fbclid`, …), query pairs sorted, trailing slash trimmed. Kills `?utm_source=` duplicate crawling.
- **robots.txt**: Disallow-prefix matching (star group), **Crawl-delay honored** via a per-host next-allowed gate (delayed URLs rotate to the back, rotation-capped; loop sleeps when everything is delayed; delays capped 30s), `Sitemap:` directives parsed. A robots fetch that fails at the **transport layer** fails open to allow-all but is counted (`robots_fetch_failures`); a non-2xx (e.g. 404 "no robots") is a legitimate allow-all and not counted.
- **Honest errors + bot-wall awareness**: transport-layer fetch failures are counted (`failed`, plus `failed_by_host` — top-20 offenders) instead of being silently dropped. A response classified as a bot-wall/challenge — status 403/429/503, or a Cloudflare/JS-gate/CAPTCHA marker on a 200 (shared `fetcher::http_bot_wall`) — is **not** kept and counts as `skipped_botwall`. Page-body writes, output-dir creation, and checkpoint saves that fail are warn-logged; repeated checkpoint-save failures surface as `checkpoint_errors`.
- **Sitemap seeding** (`sitemap_seeds=true`): expands seeds from each seed host's declared sitemaps (fallback `/sitemap.xml`), sitemap-index followed one level; caps 10 maps/host, 2000 URLs total; filters apply.
- **Resumable checkpoint**: frontier state (queue + seen-set + kept SimHash fingerprints) persisted as versioned JSON every 25 kept pages (write-then-rename) and at end; loaded before seeding. App param `checkpoint: "name"` stores it at `data/artifacts/crawl/checkpoints/<name>.json` so a later job resumes. `resumed` reports restoration. The file carries a `version` field: an incompatible (older/corrupt) checkpoint is **discarded for a clean fresh start** — never a silently-wrong partial resume — and reported as `checkpoint_reset`.
- **Near-dup detection (banded SimHash index)**: kept-page fingerprints are indexed in a banded/bucketed SimHash index (b = distance+1 bit-bands; pigeonhole guarantees a shared band for any pair within the distance), giving candidate lookup instead of an O(n) linear scan per page — identical Hamming-distance decisions, far less work over a large crawl.
- **Bounded memory**: page bodies stream to disk (never held); per-page metadata streams to the `pages` dataset (never accumulated in the result); only the frontier seen-set (capped at 100k) and the kept-page SimHash fingerprints (8 bytes each) grow with the crawl.

## `pages` dataset (per-page records)

Every **kept** page is upserted into the crawl app's `pages` dataset as it is crawled (streamed in batches of 50 via a sink seam — `upsert_many`, **partial-batch semantics, never `sync_many`**: a crawl is a partial view, so absent URLs are never marked removed). Record **key = canonical URL**; the value is a compact fingerprint, never the body:

`url, title` (extracted from `<title>`), `status, content_chars` (visible-text char count, script/style excluded), `simhash, excerpt` (first ~300 text chars), `artifact_path` (the `page-NNNN.html` basename under the job's artifacts dir), `depth, job_id`.

This makes crawled pages queryable/diffable and lets **dataset triggers + watches fire per-page** through the normal dataset-change path (`fire_dataset_triggers` / watch notifications run off the run's revisions). Note: the full-text **search indexer** is result-key based (`records`/`stories`/`items`), so dataset records are not auto-indexed into search — that path is unchanged here.

## Result stats

`crawled, kept, skipped_duplicates, skipped_robots, skipped_filtered, sitemap_seeded, failed, failed_by_host{}, skipped_botwall, robots_fetch_failures, checkpoint_errors, resumed, checkpoint_reset, hosts, frontier_remaining`, plus the `pages` dataset pointer + write outcome `pages_dataset, pages_new, pages_changed, pages_unchanged`. Per-page detail is queried from the `pages` dataset, not returned inline (memory-bounded).

## Known gaps

- Crawl-delay gates dispatch; same-host in-flight fetches dispatched earlier can still cluster (the engine-level governor softens this). Frontier capped at 100k seen URLs. No JS rendering in the crawl loop (http engine only).
- Dataset records are not fed to the full-text search index (indexer explodes result keys, not dataset rows).
