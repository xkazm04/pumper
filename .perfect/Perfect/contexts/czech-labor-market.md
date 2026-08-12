---
name: czech-labor-market
type: perfect/context
group: Market Data
category: lib
opportunity: 5
last_proposed: 2026-08-12
cooldown_until: 2026-08-14 (2 rounds)
directions: ["[[nowcast-honesty-floor]]", "[[mpsv-feed-drift-honesty]]", "[[labour-datasets-visible]]"]
---

## Current state (fresh engine-depth scout 2026-08-12)
mpsv-ispv (212 lines, quarterly ISPV wages, key `czIsco|sfera`) + mpsv-vpm (3055
lines, daily ~300k-posting vacancy register → 12 datasets incl. the cz-labour
virtual trio salary_gap/salary_nowcast/vacancy_lifecycle via upsert_many_stamped).
Both raw ctx.engines.http, pinned in fetch_chokepoint (188MB feed, 300s per-req
timeout, only ARES phase checkpointed — a reap mid-fetch restarts from zero).
Nowcast (vpm:1333-1385): ratio_carry with GOOD label honesty (ratio_used,
observations, dispersion, confidence, ispv_anchor_date, staleness_days, method) but
`ratio_used <= 0` is the ONLY output guard — no plausibility band, no obs floor
(1-obs "low" rows ship, :2587), staleness stamped never judged, and NO catalog
contract for any of the trio (only role_region_agg + wages have contracts). The
planned backtest (economic-data.md:56) was never built.
Confirmed-unfixed bughunt findings (2026-07-14): #2 region_agg silently drops
czisco-less postings (:411-414 gate before :432-451 roll-up) — regional
distributions biased; #3 missing/renamed `polozky` = clean stored:0 success
(ispv:92-96, vpm:1918-1922 serde(default)), doubly invisible since upsert_many
never tombstones. Fixed since bughunt (with regression tests): typMzdy salary gate,
wage_num Czech number forms.
No observe_extraction, no index_datasets (12 datasets invisible to search/watch/
alert; zero production consumers found), no sync_many. run() zero coverage both
apps (~49 pure-fn tests, no e2e). apps.md:29 stale — omits salary_nowcast +
vacancy_lifecycle (shipped in 98dbf1b/efa1968 without doc edits). Mixed key grain
(raw CZ-ISCO in role_region_agg/region_agg vs 4-digit unit group in the trio +
skill/education) undocumented as a join hazard. Schedules: ispv quarterly, vpm
daily, both CostClass::Free. Encoding + retry handled at the engine layer.

## Direction history
- 2026-08-12 (r15, director-self-gated): proposed 3, ACCEPTED 3 —
  [[nowcast-honesty-floor]] (robustness · M), [[mpsv-feed-drift-honesty]]
  (robustness · M), [[labour-datasets-visible]] (feature · M).
  REJECTED-deferred (banked as the context ANCHOR): **nowcast backtest** — the
  design doc's planned release-over-release error validation; needs accumulated
  ISPV releases + its own guarded design; propose when the context comes off
  cooldown with data in hand. **sync_many/tombstoning migration** — real
  (drifted-empty runs never tombstone) but its blast radius is the whole 12-dataset
  write surface; bank until the drift-honesty guards prove the shape.
  REJECTED outright: **observe-extraction adoption** — same fleet-wide-sweep
  reasoning as us-business-census (banked on source-resilience, not here).
  **mpsv-vpm-prirustky delta feed** (catalog :429-442, planned, app="") — new
  source integration, bigger than one session, no steer asking for it.

## Shipped
- (pre-46-map: role_trends, salary gap — w6+w9 old map. r15 wave in flight.)
