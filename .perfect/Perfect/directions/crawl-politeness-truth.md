---
slug: crawl-politeness-truth
type: perfect/direction
context: "[[crawler-core]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-13
commit: 10aa549
---
## What & why
Two politeness controls report compliance they did not deliver. Politeness is the one class
of bug in a scraper where the cost lands on *someone else's* server, so "we said we obeyed
and didn't" is the worst failure shape available here — and both defaults are on.

- **robots.txt is only ever fetched over `https`.** The seed's own scheme is ignored and the
  cache key is the bare host. For an `http`-only origin the fetch fails at the transport
  layer, which **fails open to allow-all**, increments `robots_fetch_failures`, and lets the
  crawl take every `Disallow:` path. The run then reports `respect_robots: true` and
  `skipped_robots: 0`. The counter exists, but nothing connects "N robots fetches failed" to
  "N hosts were crawled without compliance" — and a bare failure count reads as noise, not as
  a compliance breach.
- **Per-host page budgets reset on every resume.** `Frontier::taken` is not in the
  `Checkpoint` struct, acknowledged in a private comment. A crawl with
  `max_pages_per_host: 100` that is reaped and re-claimed four times fetches up to 500 pages
  from one host. `max_pages_per_host` is documented as host-fairness, but its real job on a
  long crawl is politeness, and durable execution silently multiplies it by the retry count.

The user moment: *"I set `respect_robots: true`, the run said it skipped nothing, and the
site owner emailed me about the paths I was told not to crawl."*

## Evidence
- `crates/core/src/crawl.rs:1480` — `format!("https://{host}/robots.txt")`, scheme hardcoded;
  cache key from `host_of` strips the port (`:1470`).
- Fail-open path: `:1487-1491` (transport failure → allow-all + `robots_fetch_failures += 1`).
  A non-2xx is correctly treated as a legitimate allow-all and NOT counted — that part is
  right and must stay.
- `Checkpoint` struct `:1196-1205` has no `taken`; restore doc admits it at `:653` ("per-host
  `taken` counts are not persisted, so the per-host budget restarts for the resumed run").
- Budget enforcement + refund that the persistence would protect: `:598-604`, `:629-635`.
- Version guard that makes a format change safe: `:1216` (`CHECKPOINT_VERSION`).

## Acceptance criteria
1. robots.txt is fetched over the **scheme the crawl is actually using for that host** (and
   the cache key distinguishes schemes if both are reachable). A pure, named function decides
   the robots URL from a page URL; test covers http, https, and a port.
2. A robots fetch that fails at the transport layer still fails open — that is the right
   default and is not up for change — but the run can no longer *look compliant*: the result
   distinguishes hosts crawled under **verified** robots rules from hosts crawled under a
   **failed-open assumption**, by host, in a machine-readable field. Naming the hosts is the
   point; a bare count is what exists today and is not sufficient.
3. `max_pages_per_host` survives resume: per-host taken counts are persisted in the
   checkpoint behind a `CHECKPOINT_VERSION` bump (the existing guard discards older blobs
   cleanly — do NOT write a migration). Test: budget consumed, checkpoint saved, restored,
   and the host does not get a fresh allowance.
4. Tests are named after the anti-pattern they defend (`x_not_y` style), e.g.
   "an_http_only_host_is_not_crawled_under_a_failed_open_robots_assumption_silently".
5. Do not add a new config key unless AC1-3 genuinely require one; if one is needed, it gets
   a schema entry in the app — report that as a Class-C item rather than editing the app.

## Risks / non-goals
- No UA-specific robots group support (documented scope choice, still out of scope).
- No change to the Disallow/Allow longest-match semantics — the scout verified those are
  correct.
- `crates/core/src/crawl.rs` ONLY.

## Build record
(to fill during build)
