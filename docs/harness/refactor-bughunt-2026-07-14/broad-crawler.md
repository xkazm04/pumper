# Broad Crawler — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 2, Medium: 3, Low: 0)
> Files scanned: `crates/core/src/crawl.rs` (full), `crates/apps/crawl/src/lib.rs` (app wiring, to confirm output-dir / checkpoint / revisit seams)

## 1. Near-duplicate pages' outbound links are never followed — silent coverage loss
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure / edge-case (crawl coverage)
- **File**: `crates/core/src/crawl.rs:580-640`
- **Scenario**: The dedup decision and link-following are nested so links are only enqueued for KEPT pages. When a fetched page is judged a SimHash near-duplicate (`duplicate` at 580-582), the loop increments `skipped_duplicates` and does nothing else — `fetched.links` (already extracted in `parse_page`) are discarded. Concrete repro: a paginated archive/faceted-nav where page 2..N are near-duplicates of page 1 in layout/text (SimHash within `dedup_distance`) but each links to *distinct* detail pages. Page 1 is kept and its links followed; pages 2..N are dropped as dups, so every detail URL reachable only from pages 2..N is never enqueued. The crawl silently under-covers the site with no signal in stats.
- **Root cause**: Design assumption that a near-duplicate page carries no useful navigation. Link extraction is coupled to the "keep" branch (link enqueue at 631-638 lives inside the `else`/kept block) instead of being applied to every successfully fetched, in-depth page.
- **Impact**: wrong result — incomplete crawl; entire subtrees can be missed on exactly the sites (pagination, listings) where near-dup pages are common.
- **Fix sketch**: Move the depth/filter/`frontier.push` link-following out of the `else` (kept) branch so links are followed for any fetched non-error page within the depth budget; keep the SimHash decision governing only whether the page is *stored* (artifact + sink), not whether its links are expanded.

## 2. robots.txt fetch is awaited inside the scheduling loop, stalling all in-flight fetches on every new host
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: concurrency / throughput
- **File**: `crates/core/src/crawl.rs:489-531` (await at 494-495; in-flight only polled at 529)
- **Scenario**: The frontier "top-up" `while` loop calls `robots_for(...).await` synchronously to fetch `robots.txt` for a host not yet in the cache. This runs on the crawl's single driving task; the `FuturesUnordered` pool (`in_flight`) is only polled at `in_flight.next().await` (529). So while the loop awaits a robots.txt round-trip, none of the concurrent in-flight fetches make progress. In a broad crawl (`same_domain: false`, the crawler's stated purpose) every newly-encountered host triggers one blocking robots fetch in the hot loop; against a slow or dead host the whole in-flight pool sits idle for the full HTTP timeout before the loop resumes. Thousands of hosts ⇒ thousands of pipeline freezes.
- **Root cause**: robots resolution is inlined into the sequential scheduler rather than performed as part of the concurrent per-URL fetch (or prefetched off the critical path). The "high connection concurrency" design is defeated at each cache miss.
- **Impact**: reliability/throughput failure under plausible conditions — a single unresponsive host's robots.txt can freeze the entire concurrent crawler; aggregate multi-host crawls run far below the configured concurrency.
- **Fix sketch**: Resolve robots inside `fetch_one` (per-fetch task) or pre-warm robots for a host via a spawned task while other fetches continue; at minimum, do the robots fetch through the `in_flight` pool rather than blocking the scheduler loop.

## 3. robots.txt matching ignores `Allow` and `*`/`$` wildcard patterns — mis-honors robots
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case (protocol correctness)
- **File**: `crates/core/src/crawl.rs:1015-1049`
- **Scenario**: `RobotRules::parse` only collects `Disallow` prefixes for the `*` group and `allowed()` does a literal `path.starts_with(d)` (1048). Two real gaps: (a) `Allow:` directives are never parsed, so a common `Disallow: /` + `Allow: /public/` site is fully blocked (over-block → coverage loss); (b) wildcard rules like `Disallow: /*.pdf$` or `Disallow: /*?sort=` are matched literally by `starts_with`, so they never match any real path and the crawler fetches URLs the site explicitly disallowed (under-block → politeness violation, fetching intended-blocked content).
- **Root cause**: "Minimal robots" implementation models only literal Disallow-prefixes; the modern robots spec's `Allow` precedence and `*`/`$` wildcards aren't handled.
- **Impact**: wrong result both ways — over-blocks Allow-listed paths (missed pages) and under-blocks wildcard Disallows (fetches content the origin asked crawlers to skip).
- **Fix sketch**: Parse `Allow` lines and apply longest-match precedence per the spec; translate `*`/`$` into a matcher (or a compiled regex) instead of `starts_with`. If full spec support is out of scope, at least document the limitation and honor `Allow`.

## 4. Frontier seen-cap (100k) silently drops discovered URLs with no counter
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure / bounded-growth observability
- **File**: `crates/core/src/crawl.rs:274-280` (cap), `31` (`MAX_FRONTIER`), `225-262` (`CrawlStats` — no field for this)
- **Scenario**: `Frontier::push` returns early when `seen.len() >= MAX_FRONTIER` (100_000). The `seen` set never shrinks (popped URLs stay to prevent re-enqueue), so once 100k *distinct* URLs have ever been seen, every further discovered link is dropped. Repro: crawl a site with >100k reachable URLs and a high `max_pages`; after 100k URLs enter `seen`, newly discovered links vanish. Unlike `skipped_robots` / `skipped_filtered` (which are counted), these drops increment nothing — the result gives no hint the crawl was truncated by the cap.
- **Root cause**: The cap is intentional (bounded memory, per module doc) but there is no telemetry for hitting it; the drop path is a bare `return`.
- **Impact**: wrong result made invisible — a large-site crawl is silently truncated; operators can't distinguish "site fully covered" from "hit the frontier ceiling."
- **Fix sketch**: Add a `frontier_capped` (or `skipped_frontier_full`) counter incremented in the early-return branch and surface it in `CrawlStats` / the app result, so cap saturation is observable.

## 5. Crawl-delay wait re-parses and re-churns the entire frontier every 200ms
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: performance (busy-poll churn)
- **File**: `crates/core/src/crawl.rs:485-528` (rotation loop + fixed sleep at 526; `host_of` re-parse at 491)
- **Scenario**: When a host has a `Crawl-delay` and its window hasn't elapsed, popped URLs are `requeue`d and `rotations` climbs until `rotations > frontier.len()`, breaking the top-up. With `in_flight` empty (single delayed host — the common `same_domain` case), the loop then sleeps a *fixed* 200ms (comment says "wait out the shortest window" but the constant ignores the actual delay) and repeats. Each 200ms pass pops+requeues up to `frontier.len()` URLs, and each pop re-runs `Url::parse` via `host_of` (491) plus a robots-cache lookup and `next_allowed` check. For a 50k-URL single-host crawl with `Crawl-delay: 10`, that's ~50k URL re-parses every 200ms for the full 10s window — hundreds of thousands of wasted parses per second, scaling with frontier size.
- **Root cause**: The delay is enforced by re-scanning the whole frontier on a fixed poll interval rather than sleeping until the earliest `next_allowed` and/or tracking a per-host delayed queue.
- **Impact**: wasted CPU that scales with frontier size during every crawl-delay window; a large polite crawl burns a core doing nothing but re-parsing URLs.
- **Fix sketch**: Sleep until `min(next_allowed) - now` (clamped) instead of a fixed 200ms, and/or precompute each entry's host once (store host/depth in the frontier tuple) so the rotation pass doesn't re-`Url::parse` every URL each poll.

---

Notes on things checked and deliberately NOT reported (verify-before-flag):
- Suspected resume artifact-filename collision (`page-{kept:04}.html` restarts at 0001 after a checkpoint resume): NOT a bug — `crates/apps/crawl/src/lib.rs:243` passes the *per-job* `ctx.artifacts_dir` as `output_dir`, so a resumed run writes into a fresh job directory; no on-disk body is overwritten. (`stats.kept`/`max_pages` being per-session rather than cumulative on resume is a minor semantic, not corruption.)
- SimHash banded index vs linear scan: pigeonhole `b = d+1` banding is correct and covered by `simhash_index_matches_linear_scan`; no false-negative dedup bug.
- Checkpoint write uses write-tmp-then-rename (atomic) and version-gates incompatible files to a clean reset — no corruption path.
- The fetch loop is single-task orchestration (no `tokio::spawn`, no shared mutable state across futures) — no data race in the classic sense.
