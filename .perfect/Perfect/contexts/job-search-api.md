---
name: job-search-api
type: perfect/context
group: HTTP API
category: api
opportunity: 6
last_proposed: never
cooldown_until: —
directions: []
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

## Shipped
- (inherited via those contexts)
