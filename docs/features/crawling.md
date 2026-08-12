# Broad crawler

High-concurrency frontier crawler (`crawl()` in core; exposed as the `crawl` app). Bounded, deduplicated URL frontier feeding up to 64 concurrent fetch tasks (the app's `concurrency` schema maximum; core itself clamps at 256); page bodies stream to the job's artifacts dir; SimHash drops near-duplicate pages.

## CrawlConfig (app params)

`seeds` (required unless `mode:"revisit"`), `max_pages` (50), `max_depth` (2), `concurrency` (16, max 64), `max_pages_per_host` (null = unlimited; per-host page cap for host-fair multi-seed crawls), `same_domain` (true), `dedup_distance` (3, 0 disables), `respect_robots` (true), `include_patterns` / `exclude_patterns` (regex; include = any-must-match, exclude drops after; **seeds exempt**), `sitemap_seeds` (false), `mode` (`"revisit"` → incremental recrawl, see below), `discover` (false; revisit-only link-following opt-in), `revisit_budget` (null = every seed; revisit-only cap on how many known pages are fetched this run, spent on the highest due-score URLs), `min_due_score` (0; revisit-only, skips seeds whose probability-changed-since-last-check falls below it — skipped seeds are counted in `skipped_not_due`).

There is **no `checkpoint` param**. Resume is automatic and per job — see *Durable resume* below.

## Behaviors

- **Host-fair frontier**: the frontier buckets URLs per host and hands them out **round-robin**, so one large seed can't consume the whole `max_pages` budget and starve the other seeds (a plain FIFO would). A polite (crawl-delayed) host rotating to the back no longer sits behind a fast host's entire backlog. `max_pages_per_host` caps how many pages a single host yields; URLs skipped once a host hits its cap are counted in `skipped_host_budget` (honest truncation, like `frontier_dropped`). The checkpoint's on-disk shape is unchanged (a flat `(url, depth)` queue; host buckets are rederived on load), so existing checkpoints resume without a reset — but the per-host page count is not persisted, so the budget restarts on resume.
- **Canonicalization**: discovered links + seeds are normalized before the frontier — fragment stripped, tracking params dropped (`utm_*`, `gclid`, `fbclid`, …), query pairs sorted, trailing slash trimmed. Kills `?utm_source=` duplicate crawling.
- **robots.txt**: Disallow-prefix matching (star group), **Crawl-delay honored** via a per-host next-allowed gate (delayed URLs rotate to the back, rotation-capped; loop sleeps when everything is delayed; delays capped 30s), `Sitemap:` directives parsed. A robots fetch that fails at the **transport layer** fails open to allow-all but is counted (`robots_fetch_failures`); a non-2xx (e.g. 404 "no robots") is a legitimate allow-all and not counted.
- **Honest errors + bot-wall awareness**: transport-layer fetch failures are counted (`failed`, plus `failed_by_host` — top-20 offenders) instead of being silently dropped. A response classified as a bot-wall/challenge — status 403/429/503, or a Cloudflare/JS-gate/CAPTCHA marker on a 200 (shared `fetcher::http_bot_wall`) — is **not** kept and counts as `skipped_botwall`. Page-body writes, output-dir creation, and checkpoint saves that fail are warn-logged; repeated checkpoint-save failures surface as `checkpoint_errors`.
- **Sitemap seeding** (`sitemap_seeds=true`): expands seeds from each seed host's declared sitemaps (fallback `/sitemap.xml`), sitemap-index followed one level; caps 10 maps/host, 2000 URLs total; filters apply. Each sitemap entry's `<lastmod>` is captured and entries are **seeded newest-first** (entries with no `<lastmod>` sort last), so when the 2000-URL budget clips a large sitemap the freshest URLs are the ones that make it into the frontier.
- **Durable resume (checkpoint)**: frontier state (queue + seen-set + kept SimHash fingerprints) streams through the **platform's job checkpoint seam** (`AppContext::checkpoints` → the `checkpoints` table in SQLite), not an app-private file. Intermediate saves are gated by a **5-second wall clock**, not a page count: each save serializes the whole frontier, so firing every N pages made total checkpoint work O(pages/N × frontier) — a 100k-page crawl did thousands of full rewrites. A final, unthrottled save on exit captures the true end state. The state comes back as `resume_state` when the **same job** is re-claimed (crash, reap, or shutdown-suspend), so an interrupted crawl continues instead of restarting; `resumed` reports restoration. There is no `checkpoint: "name"` param and no `checkpoints/<name>.json` file — the seam owns persistence, lineage-guarding and the poisoned-blob escape. An incompatible (older/corrupt) checkpoint is **discarded for a clean fresh start** — never a silently-wrong partial resume — and reported as `checkpoint_reset`; saves that fail to persist surface as `checkpoint_errors`.
- **Near-dup detection (banded SimHash index)**: kept-page fingerprints are indexed in a banded/bucketed SimHash index (b = distance+1 bit-bands; pigeonhole guarantees a shared band for any pair within the distance), giving candidate lookup instead of an O(n) linear scan per page — identical Hamming-distance decisions, far less work over a large crawl.
- **Bounded memory**: page bodies stream to disk (never held); per-page metadata streams to the `pages` dataset (never accumulated in the result); only the frontier seen-set (capped at 100k) and the kept-page SimHash fingerprints (8 bytes each) grow with the crawl.
- **Live progress**: every 20 crawled pages (and once at the end) the crawl reports a `{crawled, kept, failed, frontier, hosts}` snapshot through the runtime progress seam (`ProgressFn`). The runtime throttles persist+emit to ≥ every 2s; the latest snapshot shows on `GET /jobs/{id}` and as `progress` SSE events, so a 100k-page crawl is observable mid-run instead of only at completion. See [runtime.md § Live progress](runtime.md#live-progress).

## `pages` dataset (per-page records)

Every **kept** page is upserted into the crawl app's `pages` dataset as it is crawled (streamed in batches of 50 via a sink seam — `upsert_many`, **partial-batch semantics, never `sync_many`**: a crawl is a partial view, so absent URLs are never marked removed). Record **key = canonical URL**; the value is a compact fingerprint, never the body:

`url, title` (extracted from `<title>`), `status, content_chars` (visible-text char count, script/style excluded), `simhash, excerpt` (first ~300 text chars), `artifact_path` (the `page-NNNN.html` basename under the job's artifacts dir), `depth, job_id`, and `etag` / `last_modified` (response validators captured from every fetch, so a later revisit can send conditional GETs). A revisit that finds a page gone rewrites its record to `{url, status, gone: true, job_id}` (see below).

This makes crawled pages queryable/diffable and lets **dataset triggers + watches fire per-page** through the normal dataset-change path (`fire_dataset_triggers` / watch notifications run off the run's revisions). Note: `pages` is **not** indexed into full-text search. The result-key indexer (`records`/`stories`/`items`) doesn't see dataset rows, and the per-record path is opt-in — an app has to name the dataset in its result's `index_datasets` (see [search.md](search.md)), which the crawl app does not. Making crawled pages searchable would mean emitting `index_datasets: [{app: "crawl", dataset: "pages"}]`, or a one-off `search-backfill --app crawl --dataset pages`.

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

**Artifact retention**: source mode reads bodies from the **origin crawl job's** per-job artifacts dir. Retention is **off by default** (`[storage] artifact_retention_days = 0`); when enabled, bodies past the window are reclaimed *unless a replayable revision pins them* (see [datasets.md § Retention](datasets.md)). Once a body is gone — reclaimed or manually deleted — its key surfaces in the extractor's `missing_keys` rather than as a silent null. A revisit crawl writes fresh bodies under a **new** `job_id` and updates the record's `job_id`, so extraction always follows the latest stored body; the abandoned older copy is exactly what retention reclaims. `GET /retention/preview` shows what would be reclaimed, per app, without deleting.

> **Caveat (measured, 2026-08-12):** the pin only fires for a revision carrying **both** `artifact_sha` and `rules_hash`, and no production write path in the workspace stamps both today — the crawl stamps `artifact_sha` only (it runs no RuleSet, so claiming replayability would be a fabrication) and the extractor stamps `rules_hash` only. So the pin is effectively inert. Retention is also **off by default**, so nothing is being reclaimed; the two facts cancel out today, but enabling `artifact_retention_days` would reclaim bodies the pin was meant to protect.

## Result stats

Crawl tallies: `crawled, kept, skipped_duplicates, skipped_robots, skipped_filtered, sitemap_seeded, failed, failed_by_host{}, skipped_botwall, robots_fetch_failures, checkpoint_errors, resumed, checkpoint_reset, hosts, frontier_remaining`.

**Coverage honesty** — `frontier_dropped` (discovered URLs refused at the 100k frontier cap), `skipped_host_budget` (queued URLs dumped when their host reached `max_pages_per_host`), and `coverage_complete` (`true` only when both are 0). A crawl with `coverage_complete: false` additionally carries a `warnings` array naming what was cut, so a partial corpus is never mistaken for a whole site.

`pages` dataset pointer + write outcome: `pages_dataset, pages_new, pages_changed, pages_unchanged`, plus `versions_archived` (changed revisions copied into `page_versions`) and `reliability_hosts` (hosts folded into the Web Reliability Index this run).

Link graph: `edges_dataset, edges_written` (rows the store actually wrote — new + changed, **not** the row count offered), `edges_unchanged` (no-op upserts), `edges_dropped_out_degree`, `edges_deduped`, `top_linked`.

Revisit mode adds `revisit, revisited, unchanged_304, changed, gone, new` (`changed`/`new` mirror the live `pages_changed`/`pages_new`), plus the learned-cadence frontier's `skipped_not_due` (seeds not fetched because they scored below `min_due_score` or ranked past `revisit_budget`) and `cadence_updates` (304 cadence-counter merges written).

Per-page detail is queried from the `pages` dataset, not returned inline (memory-bounded). The app's `output_shape` on `GET /apps` lists every always-present key, and an inventory test (`crates/apps/crawl/tests/result_contract.rs`) fails if the two ever drift apart.

## Known gaps

- Crawl-delay gates dispatch; same-host in-flight fetches dispatched earlier can still cluster (the engine-level governor softens this). Frontier capped at 100k seen URLs. No JS rendering in the crawl loop (http engine only).
- Revisit seeds are capped at 10k live `pages` records per run and `max_pages` still caps re-fingerprinted (changed/new) pages, so a very large monitored set is swept across multiple runs, not all at once. Conditional-GET support depends on the origin sending `ETag`/`Last-Modified`; origins that send neither are always re-fetched in full (still diffed by simhash, just not cheaply).
- The `pages` dataset is not fed to the full-text search index: the crawl app doesn't emit `index_datasets`, and the result-key indexer explodes result arrays, not dataset rows.
