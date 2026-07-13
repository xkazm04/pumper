---
slug: crawl-honest-errors
type: perfect/direction
context: "[[Broad Crawler]]"
lens: robustness
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 525ed8a
---

## What & why
Failed fetches vanish (.ok()? — no log, no counter, no stat), robots failures silently allow-all, checkpoint write errors swallowed, and a Cloudflare 200-challenge page is stored as a KEPT page. Count/expose failures, warn on write errors, and reuse round-1's challenge_marker to classify wall pages as skipped_botwall.

## Evidence
- Silent fetch failures: crates/core/src/crawl.rs:347, 247-249
- Silent robots allow_all: crawl.rs:427; swallowed writes: crawl.rs:263, 335-337
- challenge_marker shipped round 1: crates/core/src/fetcher.rs (reuse, don't re-implement)

## Acceptance criteria
- [ ] Stats gain failed (total + per-host map, capped) and skipped_botwall; robots-fetch-failure counter.
- [ ] Bot-wall pages (status 403/429/503 or challenge marker) not kept, counted.
- [ ] Checkpoint/output write errors warn-logged; repeated checkpoint failures surface in the result.
- [ ] Unit tests; docs/features/crawling.md updated (incl. "constant memory" claim honesty — coordinate with [[crawl-memory-bounds]]).

## Risks / non-goals
- Non-goal: escalating crawl fetches to the browser tier (wrong cost model for broad crawls).

## Build record
(pending)
