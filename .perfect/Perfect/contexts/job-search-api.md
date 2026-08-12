---
name: job-search-api
type: perfect/context
group: HTTP API
category: api
opportunity: 6
last_proposed: 2026-08-12
cooldown_until: 2026-08-14 (2 rounds)
directions: ["[[job-budget-floor]]", "[[job-control-event-honesty]]", "[[search-degraded-honesty]]"]
alias_of_old_map: "[[http-api-routes]] (round-1 pass covered jobs pagination; search/recipes/host_weather/remote post-date it)"
---

## Current state
Not yet scouted on the 46-map. Files: crates/server/src/routes/{jobs,search,host_weather,
recipes,remote}.rs. jobs.rs got r1 pagination (0a91f46) + r10 error contract; search.rs
got MCP parity via shared build_search_request (576a3d7, r7). host_weather.rs,
recipes.rs, remote.rs never swept on any map.

## Current state addendum (2026-08-12 — very-thorough scout brief BANKED, slate NOT drafted:
## round-11 cap reached by extraction-core + automation-api. NOT yet covered; strong round-12
## cursor candidate — re-verify anchors first, decay rule.)
Pre-verified anchors, strongest first:
1. **budget_usd inversion**: jobs.rs:125 filter(>0.0) turns a 0.0/negative budget into NO budget
   — "spend nothing" becomes "spend without limit" on a paid path.
2. **Empty-app events**: bulk retry (jobs.rs:296) + queued-cancel (jobs.rs:366) emit
   JobEvent::new(id, "", ...) — mcp/live.rs:46 filters by exact app, so app-scoped watchers
   never see them.
3. **Cancel honesty**: DELETE /jobs/{id} in the shutdown drain window reports cancelled:true
   while the worker suspends+requeues (jobs.rs:372-378 vs worker.rs:180-189); token lookup
   discards the attempt key it was registered under (jobs.rs:372 vs worker.rs:54-66).
4. **host-weather import**: non-atomic apply (partial governor raises never persisted on
   mid-loop error), empty-bundle 400 documented but unreachable, MAX_IMPORT_ENTRIES dead (1MiB
   body limit fires first), unstable DefaultHasher node_id, unbounded export (no LIMIT).
5. **recipes.rs**: can only return [] (xray has zero callers), zero tests, no cursor, private
   default_limit shadows the shared one, validated/score unfilterable.
6. **Cross-route**: extractor rejections bypass the {error,code} envelope (no rejection hooks);
   /search returns 200 empty on wiped/disabled index with no indexed-state field; lenient
   parse_cursor on 9 surfaces vs strict on 1; three page-size default families.

## Direction history
- (as http-api-routes r1; via search-engine r7 for search.rs.)
- 2026-08-12 (round 11): scouted, brief banked, no slate (cap). No cooldown.
- 2026-08-12 (round 12, director-self-gated, SWEEP — banked anchors re-verified inline;
  jobs.rs lines shifted by r11's 6c3f91a but all three jobs anchors held; host_weather +
  recipes anchors confirmed; the "token lookup discards attempt key" sub-claim is WEAK —
  registry holds only the current attempt's token per id, discarding is harmless; dropped
  from the direction). Slate of 5, ACCEPTED 3:
  - ACCEPTED [[job-budget-floor]] (robustness S) — budget_usd 0/negative silently means
    UNLIMITED on a paid path (jobs.rs:126).
  - ACCEPTED [[job-control-event-honesty]] (robustness M) — bulk-retry/cancel events
    carry empty app (invisible to app-scoped watchers) + drain-window cancel answers
    cancelled:true while the job suspends and resurrects.
  - ACCEPTED [[search-degraded-honesty]] (robustness S) — wiped/disabled index answers
    200 empty, indistinguishable from no-matches; MEMORY.md invariant #4 says this trap
    already cost a session.
  - REJECTED-deferred host-weather-import-atomicity (robustness M) — CONFIRMED real:
    apply loop at host_weather.rs:218-232 is non-atomic (mid-loop `?` → 500 with partial
    in-memory raises and save_penalties never reached), empty-bundle 400 documented but
    unreachable, MAX_IMPORT_ENTRIES dead behind the 1MiB body limit, DefaultHasher
    node_id unstable across toolchains. Lost the cap-6 tiebreak on reach: manual, rare-
    path operator surface vs three every-consumer surfaces. BANKED as this context's
    next anchor — propose first next visit.
  - REJECTED recipes-surface-honesty — GET /recipes can only return [] (xray has zero
    callers) and the route doc says so honestly; route polish on a dead surface is
    cosmetic churn. The REAL fix is discovery wiring, banked on [[tiered-fetcher]] since
    r9 — do it there, not here.

## Shipped
- (inherited via those contexts)
- 2026-08-12 (r12): [[job-budget-floor]] → `acba9f4` (+ Director `6f6efdb`: the SAME bug
  at the trigger door — stored + replayed into every hop — plus the
  budget_filter_antipattern_is_extinct convention scan; builder caught NaN/∞ beyond the
  brief) · [[job-control-event-honesty]] → `e638efc` (control events carry the real app
  via RETURNING; a user cancel outranks the drain — claim-under-mutex-before-fire;
  the microsecond-loser answers suspended:true, never a lie; refuted the brief's
  "/events has an app filter" claim) · [[search-degraded-honesty]] → `63db76f` (index
  block on every search answer via shared run_search; 3 degraded states; MCP parity).
