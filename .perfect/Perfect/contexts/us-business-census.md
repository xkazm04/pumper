---
name: us-business-census
type: perfect/context
group: Market Data
category: lib
opportunity: 6
last_proposed: 2026-08-12
cooldown_until: 2026-08-14 (2 rounds)
directions: ["[[census-blend-first-class]]", "[[census-suppression-honesty]]", "[[census-vintage-truth]]"]
---

## Current state (r14 prefetch scout 2026-08-12, RE-VERIFIED r15 2026-08-12 — 9/10 claims held)
Four apps + census-common feed the Ledgerline launch-ranking surface; `blend_market`
(census-density:746-945) → `census/market_blend` + `census/saturation`; no in-repo
consumer of either (verified — one incidental test fixture only). Re-verification
deltas vs the banked brief:
- Claim 4A REFUTED: the "empty/mistyped NAICS = green run" job-params path is DEAD —
  params_schema (`minItems:1`, string items) + the r11/r12 shared validator door 422s
  it on every work-creator path (jobs.rs:139-149, mcp/mod.rs:659-678). The
  filter_map(as_str) remains as latent-only. Part B (registry mixed-grain
  double-count via naics_prefixes truncation) CONFIRMED — fix must be census-side;
  trades-common is shared.
- Claim 2 SHARPER: primary datasets DO route through write_target
  (upsert_many_with_derived → app.rs:605); the real gap is NO census app ever calls
  observe_extraction, so the resilience ladder is structurally inert for the whole
  fleet regardless of enforce; and enforce=false makes @q globally inert anyway
  (store.rs:723-728, config.rs:412,414). resilient-extraction.md:83's census claim
  is false today.
- Claim A count corrected: 5 pinned raw-HTTP sites (not 9), fetch_chokepoint.rs:53-56.
- NEW: derived writes carry Provenance::default() (no audit trail on the two product
  datasets); no census app uses sync_many → dropped sectors/trades linger forever
  un-tombstoned; blend degrades pairwise-only (writes whole corpus with Nulls when
  3 of 4 apps never ran — honest Nulls, no degradation signal).
Confirmed intact: 204 hard-fail density-only (:268-288, bughunt #3); suppressed
receipts → $0 fabrication chain (nonemp:368 → density:827-829 → :892-895, flagged
test :472-484); no vintage in keys, saturation keyed {place} only; velocity
positional windows + ×4 NSA; zero suppression/coverage counters; 4/5 blend reads
plain .list at 50k cap, silent at-cap; run() untested ×4, no census e2e, QDESC
guard untested; apps.md names 2/4 apps, datasets.md key claim false, density+nonemp
CATALOG_EXEMPT (routes/mod.rs:406-408).

## Direction history
- 2026-08-12 (r15, director-self-gated): proposed 3, ACCEPTED 3 —
  [[census-blend-first-class]] (feature · M), [[census-suppression-honesty]]
  (robustness · M), [[census-vintage-truth]] (robustness · M).
  REJECTED-deferred (banked): **observe-extraction adoption** (scout's #1) — the
  right shape is a FLEET-WIDE adoption sweep on source-resilience (one convention +
  inventory test, like the fetch-chokepoint model), not per-app dribble; soak-mode
  standalone value is thin until enforce flips. Banked on source-resilience as its
  next anchor. **census-catalog-contracts beyond the two product datasets** — D1
  carries market_blend+saturation rows (topic_stats precedent); primary-dataset
  contracts follow once the virtual-app pattern is proven twice.
  REJECTED outright: **re-blend O(corpus) compute optimization** — no volume
  consumer, r9 governor-hot-path precedent; the honesty half (silent 50k cap) is IN
  D1. **In-repo ranking function** (wildcard) — product invention with zero
  consumer; fails the concrete-user-moment test. Not slated (too thin): 500-not-
  retryable, API-key-in-cache-key (cache-miss-on-rotation only).

## Shipped
- **r15 (2026-08-12) — 3/3 shipped, landed in master `e131bae` (ff).** Full gate 1602/0.
  - [[census-blend-first-class]] → `3989fba` — the two products became reachable at all:
    `index_datasets` on all four apps (before this, `run_indexed_apps` meant no watch, trigger
    or saved search on app `census` could EVER fire), real `derived_provenance` instead of
    `Provenance::default()`, and a capped input read now flags `blend_complete: false` instead
    of blending a window as if it were the corpus.
  - [[census-suppression-honesty]] → `dd5e0e1` — the $0-succession-receipt fabrication chain
    is dead end to end (the repo's own flagged-not-fixed test flipped and renamed after the
    anti-pattern), one shared `is_empty_answer` guard across the fleet pinned by inventory
    test, and suppression is counted per trade and per run instead of absorbed.
  - [[census-vintage-truth]] → `8a9c038` — a backwards re-run is refused before it writes
    (per-dataset watermark, pure `vintage_verdict`, `allow_vintage_rewind` escape hatch),
    saturation keys carry their grain, blend rows name every input's vintage, mixed-grain
    NAICS stops double-counting, and velocity windows became calendar windows.
  - Director `e131bae` — the employer-side twin of the suppression fabrication (national
    benchmarks divided across mismatched place sets). Found by reading the diff, not by a
    failing test.
- **Seam gaps reported by this wave (Class C, not fixed here — banked):** the catalog coverage
  test refuses a virtual-namespace row while both consumers key on exactly that pair (the same
  wall czech-labor-market hit — this is now TWICE, so the server-side fix has earned its slot);
  no census app calls `observe_extraction`, so the whole resilience ladder is inert for this
  fleet; `census/saturation` legacy rows have no reachable tombstone path.
