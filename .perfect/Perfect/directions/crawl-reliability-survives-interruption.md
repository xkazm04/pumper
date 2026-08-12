---
slug: crawl-reliability-survives-interruption
type: perfect/direction
context: "[[web-crawler]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---
## What & why
Everything a crawl *learns* is held in process memory and committed only after `crawl()`
returns. An interrupted run therefore contributes **nothing** — and long, interruptible
crawls are exactly what this app is for.

The per-host tallies live in an in-process `Arc<Mutex<HashMap<String, HostTally>>>`. The
drain that turns them into (a) `web-reliability` observations, (b) cost-ledger events via
`ctx.meter`, and (c) tier-router learning via `ctx.learn_tier` all sit *after* the `?` on
`crawl(...)`. Any early return — a reaped job, a shutdown drain, a fetch error propagating —
skips the entire loop. The durable resume state carries only `queue`/`seen`/`kept_hashes`, so
a resumed attempt cannot recover the previous attempt's tallies either.

This directly undermines the module's own premise: the Web Reliability Index is a
**longitudinal** observatory of which hosts bot-wall, rate-limit, or go dark, and the runs
whose telemetry is worth the most — the multi-hour ones — are precisely the ones most likely
to be interrupted and therefore to contribute zero.

Two adjacent defects in the same files ship with it:
- **Robots/sitemap probes are metered as page fetches.** The metering client wraps the
  client handed to `crawl()`, so `robots_for`'s fetch flows through `HostTally::record`. A
  host with no `robots.txt` returns 404 → classified `gone` → folded into the reliability
  index. **Every crawl fabricates a "gone page" observation for every host lacking a
  robots.txt.** The index is not just sparse, it is wrong in a consistent direction.
- **A concurrent fold can lose a run's counters silently.** The read-modify-write per host is
  documented as able to "lose one fold"; two crawls touching the same host the same day drop
  one run's numbers, and the published `scrapeability.score` / `observations` say nothing —
  while `low_confidence` keys off the very count that was undercounted.

The user moment: *"My 6-hour crawl got reaped at 95%. The pages it wrote are there, but the
host reliability index and the cost ledger show the run never happened."*

## Evidence
- Tallies in memory only: `crates/apps/crawl/src/lib.rs:777-781`; `crawl(...).await?` at
  `:783-795`; the drain loop entered only afterwards at `:801`, `:804-823`
  (`ctx.meter` `:807`, `ctx.learn_tier` `:808`, reliability write `:824-829`).
- One-shot read-modify-write: `crates/apps/crawl/src/reliability.rs:297-357`; documented
  lost-update at `:290-295`.
- Resume state carries no tallies: `crates/core/src/crawl.rs:1198-1205`.
- The interruption path this hits in production: `crates/server/src/worker.rs:194-203`
  (shutdown drain).
- Probe pollution: metering wrapper `crates/apps/crawl/src/lib.rs:778-794` wraps the client
  that `robots_for` (`crates/core/src/crawl.rs:1481`) also uses; 404 → `gone` classification
  `crates/apps/crawl/src/lib.rs:64` → `CrawlHostObs { gone }` `:819`.

## Acceptance criteria
1. Host telemetry is committed **during** the crawl, not only at the end, so an interrupted
   run keeps what it had already learned. The flush cadence is the builder's call (a page
   stride, a wall clock, or riding the existing progress seam) but it must be stated and
   justified in a comment, and it must not turn a per-host map into a per-page write.
2. The same guarantee covers the cost ledger and tier-router learning, or the direction
   explains — with evidence — why one of them must stay end-of-run. "I only did the easy one"
   is a fail; "meter is idempotent-safe to repeat but learn_tier is not, so here is what I
   did" is a pass.
3. Robots/sitemap probes no longer enter the reliability tallies. A named predicate decides
   what counts as a *page* fetch, tested against a 404-robots host — the anti-pattern being
   "a host with no robots.txt looks like a host serving dead pages".
4. A partial/interrupted contribution is **distinguishable** from a complete one in the
   stored observation, so a consumer can weight it — the same honesty rule the rest of the
   fleet now follows for partial aggregates.
5. The known lost-update on concurrent folds is either fixed or surfaced on the record
   (builder's judgment, reasoning recorded). Do not leave it both unfixed and undisclosed.
6. Covered by tests at the app level; if a full `Crawl::run()` drive is infeasible without
   live fetches, cover the flush unit and say plainly what you could not verify end to end.

## Risks / non-goals
- Do NOT edit `crates/core/src/crawl.rs` — a sibling builder owns that entire file this wave.
  If the clean fix genuinely needs a core seam, return `DECISION NEEDED` instead.
- Do not touch `docs/features/crawling.md` — [[crawl-truncation-visible]] owns it this wave.
  Report any doc line you need changed and the Director will apply it.
- No new datasets; `web-reliability` keeps its current shape and namespace.

## Build record
(to fill during build)
