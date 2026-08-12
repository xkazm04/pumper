---
name: us-business-census
type: perfect/context
group: Market Data
category: lib
opportunity: 6
last_proposed: never
cooldown_until: —
directions: []
---

## Current state (scouted 2026-08-12, r14 prefetch — full brief in r14 scout report, digest here)
Four apps + census-common feed the Ledgerline launch-ranking surface. The "model" is
`blend_market` (census-density/src/lib.rs:746-945) → `census/market_blend` +
`census/saturation`; NO scoring/ranking function exists in-repo — ranking is downstream.
All four apps re-derive the blend after their own upserts, each swallowing errors into
`{"skipped":…}`. Fetches: all 9 sites raw `ctx.engines.http`, DELIBERATELY inventoried
in fetch_chokepoint.rs:53-56 ("JSON APIs") → no metering/VCR/tier-router; no census run
is replayable. No pagination anywhere; 500 not in retryable_statuses; API key is part
of the HTTP cache key.

Direction-grade gaps (r14 scout, strongest first):
1. **Blend + saturation invisible to every downstream mechanism**: written via raw
   `ctx.datasets.upsert_many` (density:512-515,714-717), no census app emits
   index_datasets → no watches, no triggers, no contracts, no search, no lineage for
   the two product datasets. grants-common solves exactly this (grants-common:206).
2. **Raw upsert also bypasses trust/health ladder** (write_target @q diversion skipped)
   — quarantined density still publishes canonical blend rows.
3. **density hard-fails whole run on HTTP 204** (204 passes is_success then trips
   non-JSON error); other 3 apps guard it. Bughunt 2026-07-14 finding #3 STILL OPEN.
4. **Empty/mistyped NAICS = green run that scraped nothing** (filter_map(as_str), JSON
   numbers vanish); registry naics_prefixes truncates but never validates → 4-digit
   grain double-counts into blend (chars().take(4)). Bughunt #4 STILL OPEN.
5. **Vintage staleness w/ weekly freshness illusion**: hardcoded CBP 2022 / NES 2021 /
   NES-D 2021; bfs re-blends weekly so market_blend updated_at churns while market data
   is 4y old. density + nonemp have NO schedule() at all (density:78-79 commented out).
6. **No vintage in record keys** → re-run of older year rewrites history backwards as
   "changed"; saturation keyed {place} only so geo=county/state + denominator changes
   commingle; no two vintages coexist (bfs formations DOES carry period — the model).
7. **succession_receipts fabricates $0** on suppressed receipts (nonemp:368 unwrap_or(0)
   → density:827-829 Some(0) → :892-895 emits 0) directly contradicting field doc
   :763-765 + module header :874-876. Known-unfixed test documents it (nonemp:473-484).
8. **BFS velocity: last-12-entries window with no contiguity/staleness check**; accel
   annualizes ×4 explicitly-NSA data (seasonal artifact). as_of_period never compared
   to now.
9. **Zero suppression/coverage telemetry**; ACS base<=0 silently drops places
   (density:466-471, no counter); per-10k metrics mix both/employer_only/solo_only
   cells silently.
10. **Re-blend is O(corpus)×4 apps** (5 reads × 50k limit per run); 4 of 5 reads use
    datasets.list (no removed_at filter in SQL, recency-window cliff); run-change feed
    caps 1000 revisions/app → nationwide county run (~12.5k revisions) >90% never
    reach watches/contracts.
Tests: pure-fn coverage decent; run() untested everywhere; build_url/for_clause/
fetch_denominator/sync_market_blend zero; the ONLY drift guard (nesd QDESC :349-355)
untested; no census e2e (43 e2e files, none census); no VCR possible (raw fetch).
Docs: NO feature doc owns this context; apps.md:30 names only 2 of 4 apps;
datasets.md:48 lists market_blend but not saturation, claims source-prefixed keys
(false); resilient-extraction.md:83 claims census can be quarantined (false — no
observe_extraction). Catalog: bfs + nesd cataloged (nesd URL hardcodes 2021 vintage);
density + nonemp CATALOG_EXEMPT → the four launch-ranking datasets have ZERO contract
(no schema/row-delta/staleness floor).

## Direction history
- (none — never proposed. r14 prefetch scout banked above; front-of-queue candidate
  for r15 with a ready slate: B1 blend/saturation through write_target + index_datasets
  (kills gaps 1+2) · B2 suppression honesty (gaps 7+9, Null-not-zero + counts) ·
  B3 vintage keys + 204/naics door fixes (gaps 3,4,6) · B4 feature doc + catalog
  contracts (docs section). Re-verify seeds at proposal time — decay rule.)

## Shipped
- (none on the 46-map)
