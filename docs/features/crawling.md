# Broad crawler

High-concurrency frontier crawler (`crawl()` in core; exposed as the `crawl` app). Bounded, deduplicated URL frontier feeding up to 256 concurrent fetch tasks; page bodies stream to the job's artifacts dir; SimHash drops near-duplicate pages.

## CrawlConfig (app params)

`seeds` (required unless `mode:"revisit"`), `max_pages` (50), `max_depth` (2), `concurrency` (16), `same_domain` (true), `dedup_distance` (3, 0 disables), `respect_robots` (true), `include_patterns` / `exclude_patterns` (regex; include = any-must-match, exclude drops after; **seeds exempt**), `sitemap_seeds` (false), `checkpoint` (name string → resumable frontier), `mode` (`"revisit"` → incremental recrawl, see below), `discover` (false; revisit-only link-following opt-in).

## Behaviors

- **Canonicalization**: discovered links + seeds are normalized before the frontier — fragment stripped, tracking params dropped (`utm_*`, `gclid`, `fbclid`, …), query pairs sorted, trailing slash trimmed. Kills `?utm_source=` duplicate crawling.
- **robots.txt**: Disallow-prefix matching (star group), **Crawl-delay honored** via a per-host next-allowed gate (delayed URLs rotate to the back, rotation-capped; loop sleeps when everything is delayed; delays capped 30s), `Sitemap:` directives parsed. A robots fetch that fails at the **transport layer** fails open to allow-all but is counted (`robots_fetch_failures`); a non-2xx (e.g. 404 "no robots") is a legitimate allow-all and not counted.
- **Honest errors + bot-wall awareness**: transport-layer fetch failures are counted (`failed`, plus `failed_by_host` — top-20 offenders) instead of being silently dropped. A response classified as a bot-wall/challenge — status 403/429/503, or a Cloudflare/JS-gate/CAPTCHA marker on a 200 (shared `fetcher::http_bot_wall`) — is **not** kept and counts as `skipped_botwall`. Page-body writes, output-dir creation, and checkpoint saves that fail are warn-logged; repeated checkpoint-save failures surface as `checkpoint_errors`.
- **Sitemap seeding** (`sitemap_seeds=true`): expands seeds from each seed host's declared sitemaps (fallback `/sitemap.xml`), sitemap-index followed one level; caps 10 maps/host, 2000 URLs total; filters apply.
- **Resumable checkpoint**: frontier state (queue + seen-set + kept SimHash fingerprints) persisted as versioned JSON every 25 kept pages (write-then-rename) and at end; loaded before seeding. App param `checkpoint: "name"` stores it at `data/artifacts/crawl/checkpoints/<name>.json` so a later job resumes. `resumed` reports restoration. The file carries a `version` field: an incompatible (older/corrupt) checkpoint is **discarded for a clean fresh start** — never a silently-wrong partial resume — and reported as `checkpoint_reset`.
- **Near-dup detection (banded SimHash index)**: kept-page fingerprints are indexed in a banded/bucketed SimHash index (b = distance+1 bit-bands; pigeonhole guarantees a shared band for any pair within the distance), giving candidate lookup instead of an O(n) linear scan per page — identical Hamming-distance decisions, far less work over a large crawl.
- **Bounded memory**: page bodies stream to disk (never held); per-page metadata streams to the `pages` dataset (never accumulated in the result); only the frontier seen-set (capped at 100k) and the kept-page SimHash fingerprints (8 bytes each) grow with the crawl.
- **Live progress**: every 20 crawled pages (and once at the end) the crawl reports a `{crawled, kept, failed, frontier, hosts}` snapshot through the runtime progress seam (`ProgressFn`). The runtime throttles persist+emit to ≥ every 2s; the latest snapshot shows on `GET /jobs/{id}` and as `progress` SSE events, so a 100k-page crawl is observable mid-run instead of only at completion. See [runtime.md § Live progress](runtime.md#live-progress).

## `pages` dataset (per-page records)

Every **kept** page is upserted into the crawl app's `pages` dataset as it is crawled (streamed in batches of 50 via a sink seam — `upsert_many`, **partial-batch semantics, never `sync_many`**: a crawl is a partial view, so absent URLs are never marked removed). Record **key = canonical URL**; the value is a compact fingerprint, never the body:

`url, title` (extracted from `<title>`), `status, content_chars` (visible-text char count, script/style excluded), `simhash, excerpt` (first ~300 text chars), `artifact_path` (the `page-NNNN.html` basename under the job's artifacts dir), `depth, job_id`, and `etag` / `last_modified` (response validators captured from every fetch, so a later revisit can send conditional GETs). A revisit that finds a page gone rewrites its record to `{url, status, gone: true, job_id}` (see below).

This makes crawled pages queryable/diffable and lets **dataset triggers + watches fire per-page** through the normal dataset-change path (`fire_dataset_triggers` / watch notifications run off the run's revisions). Note: the full-text **search indexer** is result-key based (`records`/`stories`/`items`), so dataset records are not auto-indexed into search — that path is unchanged here.

## Incremental recrawl — site-change sentinel (`mode: "revisit"`)

Instead of crawling from scratch, a **revisit** run seeds the frontier from the existing live `pages` records (up to 10,000 per run, via a read-side `PageSource` seam mirroring the `PageSink`) and re-checks each with a **conditional GET** using the stored `etag` / `last_modified`:

- **`304 Not Modified`** → counted `unchanged_304`, cheap: the body is never downloaded or re-fingerprinted.
- **changed body (`200`)** → re-fingerprinted, body re-written, record upserted (a `changed` revision) with the fresh validators.
- **`404` / `410`** → the record is flagged **`gone: true`** via an explicit per-key upsert. This is a deliberate choice over `sync_many` snapshot-removal: a revisit is a *partial* view (bounded seed set), so blanket "absent ⇒ removed" would be wrong. The gone upsert is a normal `changed` revision, so dataset triggers/watches fire on it. Already-gone and already-removed records are skipped as seeds so a sentinel doesn't keep re-probing dead URLs.

Revisit does **not** follow links (no frontier expansion) unless `discover: true`. Conditional requests set `no_cache` so they revalidate against the origin instead of being served from the local TTL cache; a `304` passes through the http engine untouched and is never cached over the prior full response.

**Sentinel recipe:** schedule a revisit crawl (`POST /schedules {app:"crawl", cron, params:{mode:"revisit"}}`) after an initial full crawl has populated `pages`; add a dataset **watch** or **trigger** on the crawl app's `pages` dataset (`on_change: "changed"`) to get a webhook / chained job whenever a monitored page's content changes or goes gone. The `changed`/`gone` counts in the result summarize each sweep.

## Crawl → extract pipeline (source mode)

The crawl writes every kept page's body to disk and records `artifact_path` + `job_id` in `pages`. The [`extractor`](extraction.md) app can read those stored bodies directly instead of re-fetching — a **crawl → dataset trigger → extractor** pipeline with no double-fetch:

1. **Crawl** a site (`POST /jobs {app:"crawl", params:{seeds:[..]}}`). Kept pages stream into the `pages` dataset, each with its body at `data/artifacts/crawl/<job_id>/page-NNNN.html`.
2. **Trigger**: create a dataset trigger on `crawl`'s `pages` (`on_change:"any"` or `"changed"`) targeting the `extractor` app, with a params template that names the source and the rule set:
   ```json
   {"app":"extractor","params":{"source":{"app":"crawl","dataset":"pages"},
     "rules":{"headline":{"type":"css","selector":"h1"}}}}
   ```
   At fire time the runtime merges `_trigger` (with the capped changed `keys`) over the template; the extractor reads `_trigger.keys` and processes exactly the pages that just changed, resolving each body against `data/artifacts/crawl/<job_id>/<artifact_path>`.
3. **Extract**: extracted fields upsert into the extractor's own `extracted` dataset (override with `dataset`), with the per-field quality report (`fields_matched`/`worst_fields`) and any `missing_keys` for bodies no longer on disk.

Run it manually the same way — omit the trigger and pass `source.keys` (or nothing, to sweep all live `pages`).

**Artifact-retention caveat**: source mode reads bodies from the **origin crawl job's** per-job artifacts dir. There is **no retention/GC policy** — bodies persist until manually deleted; once a body is gone, its key surfaces in the extractor's `missing_keys` rather than as a silent null. A revisit crawl writes fresh bodies under a **new** `job_id` and updates the record's `job_id`, so extraction always follows the latest stored body.

## Result stats

`crawled, kept, skipped_duplicates, skipped_robots, skipped_filtered, sitemap_seeded, failed, failed_by_host{}, skipped_botwall, robots_fetch_failures, checkpoint_errors, resumed, checkpoint_reset, hosts, frontier_remaining`, plus the `pages` dataset pointer + write outcome `pages_dataset, pages_new, pages_changed, pages_unchanged`. Revisit mode adds `revisit, revisited, unchanged_304, changed, gone, new` (`changed`/`new` mirror the live `pages_changed`/`pages_new`). Per-page detail is queried from the `pages` dataset, not returned inline (memory-bounded).

## Known gaps

- Crawl-delay gates dispatch; same-host in-flight fetches dispatched earlier can still cluster (the engine-level governor softens this). Frontier capped at 100k seen URLs. No JS rendering in the crawl loop (http engine only).
- Revisit seeds are capped at 10k live `pages` records per run and `max_pages` still caps re-fingerprinted (changed/new) pages, so a very large monitored set is swept across multiple runs, not all at once. Conditional-GET support depends on the origin sending `ETag`/`Last-Modified`; origins that send neither are always re-fetched in full (still diffed by simhash, just not cheaply).
- Dataset records are not fed to the full-text search index (indexer explodes result keys, not dataset rows).
