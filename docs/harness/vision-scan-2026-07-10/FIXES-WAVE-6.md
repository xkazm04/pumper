# Vision Scan Fix Wave 6 — Domain Data Products

> 3 commits, 3 ideas closed + 2 duplicates absorbed (theme T9: grants intelligence + Czech labour trending).
> Baseline preserved: build clean → build clean; tests 37 → 40 (+3, 0 failed).

## Commits

| # | Commit | Idea | Title |
|---|---|---|---|
| 1 | `5214253` | 804037e7 | Canonical unified grant schema across sources (absorbs 2e82222c award extraction) |
| 2 | `b46e038` | d7dd9f0c | Cross-source grant duplicate collapse via SimHash |
| 3 | trending | c5cd98fe | Trending vs fading roles from daily change detection (absorbs a72fc621) |

## What was built

- **`grants-common` crate** (new, shared by grants-gov + ca-grants): every opportunity is normalized into the cross-source `grants/unified` dataset (keyed `<source>:<source_id>`) — canonical status vocabulary (open/forecasted/closed), ISO dates, parsed money fields (CKAN `$5,000,000` strings tolerated; grants.gov Search2 has no amounts → null). Defensive multi-candidate field lookup; unmappable rows skipped, never fabricated. 3 unit tests.
- **Duplicate links**: after each unified sync, SimHash pairs (Hamming ≤ 3) whose keys come from *different* sources are linked into `grants/duplicate_links` — the same-grant-on-two-portals signal; same-source pairs skipped. `crossSourceDups` reported in both apps' results.
- **`role_trends` dataset** (mpsv-vpm): national (czisco × orgType) posting-count trajectories computed from `role_region_agg`'s revision history (Wave 1 substrate, last 10 changed days): delta, pct change, rising/falling/flat/new; top-15 movers inline in the job result. The docstring promised this signal since day one.

## Patterns established

16. **Virtual app namespace for cross-source datasets** — `ctx.datasets.upsert(app="grants", ...)` lets several source apps feed one canonical dataset without new infrastructure; keys carry the source prefix.
17. **Normalize defensively at the seam** — multi-candidate field lookup + skip-don't-fabricate keeps schema drift from breaking runs; the raw record stays in the source dataset for re-normalization.
18. **Derive trends from revisions, not snapshots** — the revision history IS the time series; no separate daily-snapshot table needed.

## What remains (INDEX themes)

T9 tail (census blended view/YoY, salary-gap API, ARES enrichment, SEDIA clean-text, wage bands), T10 platform moonshots, and the deferred T4/T5/T7 tails. ~240 pending ideas stay in the backlog for future sessions.
