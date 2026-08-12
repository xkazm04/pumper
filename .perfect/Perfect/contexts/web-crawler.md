---
name: web-crawler
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 6
last_proposed: never
cooldown_until: —
directions: []
alias_of_old_map: "[[broad-crawler]] (round-2 pass covered the app side)"
---

## Current state (r15 prefetch scout 2026-08-12 — slate-grade brief BANKED, re-verify at proposal)
App = crates/apps/crawl/{lib,link_graph,reliability}.rs over core crawl.rs/simhash.rs.
Fetch: raw ctx.engines.http pinned in chokepoint (:90) with MeteringHttpClient
self-metering (O(hosts) flush of cost + tier signals, lib.rs:801-823); governor
applies inside engine-http so the crawler IS governed; budget_usd accepted at the
doors but inert (CostClass::Free — door enforces what the app can't honor).
Direction-grade gaps ranked (top 6 of 10):
1. APP: run() result DROPS stats.frontier_dropped + skipped_host_budget (core
   computes both honestly, crawl.rs:469,473,1128-1129; lib.rs:841-890 never copies
   them; crawling.md:60 still claims skipped_host_budget is there) — 100k-cap/host-
   budget truncation invisible to the API caller. LIVE REGRESSION.
2. CORE: hard kill between wall-clock checkpoint save (crawl.rs:1076-1083) and the
   next 50-page sink flush (PAGE_SINK_STRIDE, :39,1039-1064) permanently strands up
   to 49 fetched pages — seen+fingerprinted in the checkpoint, never written to
   pages, and revisit seeds only from pages rows (lib.rs:503-544). Unreachable
   forever without a fresh crawl.
3. APP: reliability index flushed once at end-of-run (fresh tallies per run(),
   lib.rs:777-829) — any interrupted attempt discards ALL its fetch telemetry;
   undermines the longitudinal-observatory premise for exactly the long crawls it
   targets.
4. CORE: page_versions bodies can never be retention-Pinned (replayable() needs
   rules_hash which crawl never sets — datasets.rs:192-194,1987-2017 ×
   lib.rs:213-214,308-309); the versioned archive is unprotected the moment
   artifact_retention_days > 0; undocumented (unlike the disclosed pages analog).
5. APP: run() zero coverage (no unit or e2e constructs AppContext + Crawl.run());
   highest-volume path, thinnest integration coverage.
6. APP: EdgeGraph seen/in_degree unbounded in-memory (link_graph.rs:42-53) — no
   MAX_FRONTIER analog; breaks the module's own bounded-memory invariant.
Also: crawling.md actively FALSE on the checkpoint param (M23 rewrite, :7,:16) and
omits M07/M08/M41/M42 entirely (~1/3 of surface); edges + web-reliability/* have
ZERO consumers workspace-wide (write-only telemetry); robots cache per-run only.
Consumers that ARE real: extractor + plugin read pages/page_versions incl. backfill.

## Direction history
- (as broad-crawler, round 2): 5/5 shipped — see [[broad-crawler]].
- (never proposed on the 46-map; r15 banked the brief above — front of r16 with
  crawler-core as the natural sibling, but note gaps 2+4 are CORE-side: a two-lot
  wave would need the core/app boundary as the lot split.)

## Shipped
- (inherited — see [[broad-crawler]])
