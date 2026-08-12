---
name: tiered-fetcher
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 7
last_proposed: 2026-08-10
cooldown_until: round-11
directions: ["[[politeness-memory-honesty]]", "[[browser-down-ladder]]", "[[cache-growth-bounds]]"]
---

> Supersedes [[tiered-fetcher-politeness]] (old 21-context map; rounds 1–3 served it
> 5/5). Since then: archive tier (5e12d1d), API-recipe replay tier (8cf2da0),
> change-cadence cache model + refresher (b0d1186), host weather (dccdb68), remote
> fetch fabric (22b398b), browser-tier governance (11288e6).

## Current state (scout 2026-08-10, Director-verified highlights)

fetcher.rs 1760 LOC / governor.rs 503 / tiers.rs 572 / cache.rs 574 / recipes.rs 758.
Ladder: api_recipe → archive → http → browser → claude, entered via AppContext::fetch.
[archive]/[recipes]/[remote]/[refresher] all default OFF in config.toml.

**Verified defects (Director read the code):**
- **Zombie penalties:** snapshot_penalties filters non-zero (governor.rs:233–239),
  save_penalties upserts only what's in the snapshot (tiers.rs:191–212), load_penalties
  has no age filter (tiers.rs:216–225) → a host whose penalty decayed to zero is
  resurrected at FULL penalty on every boot. DELETE /hosts/{host}/memory (runtime.rs:
  120–121, forget→clear) races the 60s write-behind. No GC on tier_memory rows.
- **Browser-down collapse:** browser engine error under Auto → trace_tier_error
  fall-through with nothing after it (fetcher.rs:669–677 → 717–721 exhaustion); a
  tier-memory browser pin sets skip_http (app.rs:285–305) so the WORKING http tier was
  never tried. Claude tier error is fatal `?` (fetcher.rs:693), inconsistent with every
  other tier's trace-and-fall-through.
- **Unbounded growth:** revalidations written on every demand-path revalidation
  (engine-http/lib.rs:688, cache.rs:172) but pruned only inside the refresher pass
  (refresher.rs:68) which is unreachable at the default [refresher] enabled=false;
  http_cache purge_expired (hourly, main.rs:275–288) removes expired rows only — a
  continuously-refreshed entry never expires, no size/row cap; ResearchCache has no
  purge method at all. prune_ledgers (storage.rs:1658) covers none of the three.

**Noted, not slated (evidence bank):**
- **BANKED ANCHOR — recipe-discovery-wiring:** xray (app.rs:465) has zero callers →
  api_recipes has no writer → GET /recipes reads an empty table and the replay tier
  no-ops even when opted in (provisioner hardcodes use_recipes=true against nothing).
  The real feature — browser render captures network → recipe learned → next fetch is
  one direct API GET — is one caller away (natural: provisioner sample stage or
  readable). Deferred round 9 (design-heavy; e2e needs live Chrome; cap reached).
  "Delete M05" rejected: the replay half is fully wired.
- Stale doc comment routes/recipes.rs:8 ("deliberately not wired yet" — false since
  8cf2da0): Director-commit-sized.
- freshness() 50k LIMIT truncates alphabetically-first silently (cache.rs:308).
- apply_weather resets the aging clock on import (tiers.rs:420–450).
- Governor acquire: no timeout/cancellation/queue bound; penalized hosts exempt from
  eviction (governor.rs:310–312). Archive tier: profile not threaded; CDX freshness
  answerable from 1h-old cached index. Challenge markers: known-accepted FPs
  ("captcha", "enable javascript"); no corpus to validate changes against.
- WeatherEntry.challenge_fingerprints always empty on export (reserved schema).

## Direction history
- 2026-08-12 (round 11, banked WITHOUT a proposal pass — E2 builder finding during
  [[url-absolutize]]): **FetchOutcome discards final_url.** The HTTP engine records the
  post-redirect URL (engine-http/src/lib.rs:425) but every FetchOutcome construction site
  sets `url: req.url` (fetcher.rs:638, 841, 1015), so url_absolute resolves a
  redirect-crossing page against the PRE-redirect origin, and any consumer of outcome.url
  gets the requested, not the landed, URL. Fix = carry final_url through FetchOutcome +
  use it as the extraction base; S/M, joins recipe-discovery-wiring as this context's
  anchors.
- 2026-08-10 (round 9, director-self-gated): ACCEPTED [[politeness-memory-honesty]]
  (robustness M), [[browser-down-ladder]] (robustness M), [[cache-growth-bounds]]
  (robustness/optimization M). REJECTED-DEFERRED recipe-discovery-wiring (feature) —
  banked as this context's next anchor, see above. REJECTED governor-hot-path
  (optimization) — the acquire path is sleep-dominated by design (politeness delay
  dominates); the per-call lowercase alloc and 60s DashMap walk are noise; no user
  moment. REJECTED quality-signal-expansion (robustness) — marker-list changes cannot
  be validated without a labeled corpus; FP tradeoff documented-accepted since round 1.

## Shipped
- 2026-08-11 · [[politeness-memory-honesty]] → `a69420a` — zombie penalties dead
  (authoritative snapshot + aged restore + locked reset + tier_memory GC in the new
  always-on store_janitor).
- 2026-08-11 · [[browser-down-ladder]] → `65f893e` — dead Chrome un-skips the working
  http tier on pinned hosts; claude tier traces-and-exhausts like every other tier.
- 2026-08-11 · [[cache-growth-bounds]] → `ca4bbe9` — revalidations/research_cache/
  http_cache all bounded on default deployments ([cache] max_rows=20000).
- Rounds 1–3 (as [[tiered-fetcher-politeness]]): fetch-no-cache-ttl,
  structured-fetch-trace, governor-hot-path (sharding), fetch-tier-verdicts,
  host-profiles-api — all shipped, see old note.
