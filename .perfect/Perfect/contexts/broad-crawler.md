---
name: "Broad Crawler"
type: perfect/context
group: "Data Extraction & Storage"
category: lib
opportunity: 7
last_proposed: 2026-07-13
cooldown_until: —
directions: ["[[crawl-pages-dataset]]", "[[crawl-live-progress]]", "[[crawl-honest-errors]]", "[[crawl-memory-bounds]]", "[[crawl-incremental-recrawl]]"]
---

## Current state (scout brief digest, 2026-07-13)

- Single-file crawler crawl.rs (614 lines): FIFO frontier (VecDeque + seen HashSet, cap 100k on SEEN — after 100k ever-queued, nothing new enters), URL canonicalization (tracking params stripped), include/exclude regex, FuturesUnordered up to 256 concurrent, checkpoint every 25 kept pages (atomic write-rename, resume works).
- **T6 backlog is STALE**: crawl-delay honored (crawl.rs:466-468, 219-229) and sitemap discovery shipped (crawl.rs:501-553, caps 10 sitemaps/host, 2000 seeds). Do not propose these.
- **Bypasses the tiered fetcher**: uses ctx.engines.http raw (crawl/lib.rs:79) — no bot-wall detection (Cloudflare 200-challenge stored as a KEPT page), no_cache/ttl_override unused, tier memory unused. Governor politeness does apply (transitively via HttpEngine) — plus the crawler's own crawl-delay gate, uncoordinated.
- **Silent errors everywhere**: fetch failure → `.ok()?` → invisible (crawl.rs:347, 247-249); robots fetch failure → allow_all silently (:427); checkpoint/output write errors swallowed (`let _ =` :263, 335-337). Failures appear in NO stat.
- **Unbounded metadata**: kept_hashes Vec<u64> linear-scanned per page = O(n²) (:254); stats.pages one struct per page in memory all run (:283); "constant memory" docstring claim false for metadata.
- **Output dead-ends**: page-NNNN.html files + stats JSON; no dataset records; worker's auto-search-index explodes only records/stories/items keys — crawl's `pages` matches none, so the whole result indexes as ONE opaque search doc (worker.rs:366-374). Nothing feeds extraction automatically.
- **No live progress**: stats only at completion; checkpoint file is the only mid-run signal; AppContext has no progress channel.
- Robots: only `*` group, Disallow-prefix only (no Allow:), no product UA token.

## Direction history
- 2026-07-13 (round 2): 5 proposed, **5 accepted**. Challenge gate discarded stale T6 backlog items (sitemap + crawl-delay already shipped in code).

## Shipped
- [[crawl-pages-dataset]] → 4c132df — PageSink seam in core, DatasetPageSink in app, batches of 50, key = canonical URL. KNOWN GAP (builder-flagged, Director ratified): worker's search indexer still ignores dataset records — per-page SEARCH needs a worker.rs change (candidate direction for a future Dataset Store or Job Server round).
- [[crawl-honest-errors]] → 525ed8a — CrawlFetch enum (Page/Failed/BotWall), failed/failed_by_host(top-20)/skipped_botwall/robots_fetch_failures/checkpoint_errors; reuses fetcher::http_bot_wall (widened pub(crate)).
- [[crawl-memory-bounds]] → 4b085c3 — SimHashIndex banded (d+1 pigeonhole, equivalence-tested), pages[] removed from RAM, checkpoint versioned (v1, old checkpoints → clean fresh start reported as checkpoint_reset).
- [[crawl-live-progress]] → 78ad7da — ProgressReporter seam in AppContext, in-memory ProgressStore (justified: hot path, restart-loss OK), throttled ≥2s/50-call SSE `progress` events, GET /jobs/{id} progress field.
- [[crawl-incremental-recrawl]] → 1c3fe35 — HttpRequest etag/if_modified_since, 304 pass-through (never cached over full body), revisit mode via PageSource seam, gone-flag on 404/410 (explicit per-key, never sync_many), live-E2E-verified (change + gone detection against a real server). Known gaps: revisit seed cap 10k/run; ETag path unit-tested only (python http.server sends Last-Modified only).
Context COMPLETE: 5/5 shipped.
