# Batch 3 Design — Core Substrate (2026-07-30)

> Branch: `vibeman/moonshot-exec-2026-07-30` (after Batch 2). Items: M42 (versioned archive + backfill), M11 (derived datasets), M05 (API X-ray), M07+M02 (change-cadence learning).
> Two sub-waves: **3a** = M42 + M05 + M11 (disjoint files), **3b** = M07+M02 alone (overlaps crawl.rs/cache.rs/fetcher.rs with 3a). Shared rules = DESIGN-BATCH-1.md §Shared rules. Agents no-git; orchestrator gates + commits.
> Migration numbers: continue after Batch 2's highest (check crates/core/migrations/ before creating; reserve in reply if unused).

## Sub-wave 3a

### K — M42 Versioned crawl archive + retroactive backfill
Follow content-research.md §Extraction-2 Path. Owns: `crates/core/src/crawl.rs` (DatasetPageSink section ONLY — sub-wave 3b will edit revisit/frontier sections LATER; keep your diff tight), `crates/apps/crawl/**`, `crates/apps/extractor/**`, `crates/apps/plugin/**`.
- Step 1: on `changed`, also upsert `page_versions` record keyed `{url}#{revision}` (artifact path, simhash, fetched_at); unchanged revisits skip.
- Step 2: revision-suffixed artifact filenames on change (URL-hash naming shipped 2026-07-16 — extend, don't replace).
- Step 3: extractor/plugin `source` mode gains `as_of`/`versions:"all"` resolved through page_versions; `read_source_artifact` unchanged.
- Step 4: backfill runner as a MODE of the existing extractor/plugin apps (param `backfill:true` + url pattern), NOT a new job kind — fan over versions bounded by SOURCE_LIST_LIMIT batching; records tagged `_url` + `_observed_at`. **Key convention decided now: derived record keys are `{natural_key}@{observed_at_date}`** so change detection treats backfill rows as distinct keys, not churn.
- Step 5: retention via the existing prune API — document the knob, no new janitor.
- Note: M23 (Batch 2) ported app_crawl to ctx.checkpoint — build on whatever is on disk.

### M — M05 API X-ray
Follow runtime-core.md §Traits-1 Path. Owns: `crates/engine-browser/**`, `crates/core/src/engine.rs` (RenderRequest/RenderedPage additive), artifact capture, recipe storage (new table via migration + storage methods appended with `// ── api recipes ──` marker), and the discovery pass.
- Steps 1–3 are the deliverable: `capture_network: bool` on RenderRequest (chromiumoxide CDP network events; JSON same-origin responses only, size-capped), `RenderedPage.network: Vec<CapturedCall>`, captures saved via save_artifact, discovery heuristic scoring payload-field overlap → `ApiRecipe {host, url_template, params, json_paths, validated:false}` rows.
- Step 4 (fetcher api-branch) is OPTIONAL in this batch — if you take it, compose with the existing pre-HTTP archive tier (M18): order = api-recipe → archive → live HTTP; if the composition is risky, ship recipes as data + a `GET /recipes` route and leave the fetch branch as a documented seam. Say which you chose.
- Auth-bound APIs: thread the existing `profile` cookies; per-host opt-in (`capture_network` is per-request, recipes marked validated only after a successful replay).

### N — M11 Derived datasets (v1: filter/project/lookup)
Follow extraction-storage.md §Store-1 Path. Owns: `crates/core/src/datasets.rs`, storage derived-spec methods (append `// ── derived ──`), migration, NEW routes for spec CRUD + backfill trigger (register at the routes site), core config additive.
- `DerivedSpec {id, source(app,dataset), filter: Vec<JsonFilter>, project: field map, lookup: Option<{dataset, key_expr, merge_as}>, enabled}` stored in a `derived` table. v1 = filter+project(+single-key lookup). NO group-by aggregates (v2, out of scope). Depth cap + per-spec kill-switch from day one; cycle detection reusing the trigger DAG guard's approach.
- Hook: after `upsert_many` computes UpsertSummary, feed fresh keys through matching enabled specs, upsert into the derived dataset in the same flow (change detection dedups no-ops). Derived upserts must not recursively trigger unbounded cascades — respect the depth cap.
- Backfill: `POST /derived/{id}/backfill` materializes over existing source rows (bounded batches).
- Tests: spec matching, projection, lookup merge, cycle rejection, depth cap.

## Sub-wave 3b (after 3a commits)

### L — M07+M02 Change-cadence learning (crawl frontier + cache mirror)
Follow extraction-storage.md §Crawler-1 and runtime-core.md §Fetcher-2 Paths. Owns: `crates/core/src/crawl.rs` (revisit/frontier sections), `cache.rs`, `governor.rs` (try_acquire), `fetcher.rs`, server wiring for the background refresher task, migration for `revalidations` if a table is the right shape (an in-cache-db table beside http_cache is fine).
- M07: RevisitSeed gains last_change_at/interval read from existing pages revisions; `due_score(now)` estimator (host-level prior for cold-start; simhash-distance-graded change, not raw inequality); sort/filter before frontier.push; per-URL (checks, changes, last_change_at) counters ON CrawlPageRecord (no new table); `revisit_budget` + `min_due_score` in CrawlConfig; `skipped_not_due` in CrawlStats.
- M02: `revalidations (key, checked_at, changed)` recorded in the ETag-revalidate path; EWMA inter-change estimator; background refresher task (server-side, scheduler-piggybacked like the DLQ drain) revalidating near-due keys ONLY via governor `try_acquire` (new, non-blocking) — strictly idle-slot; per-host + global budget caps; `GET /cache/freshness`; changed bodies flow into the existing dataset-delta trigger path only where a dataset write already occurs (do NOT invent a new event source).
- `[refresher] enabled=false` default-OFF.

## Orchestrator protocol
3a: dispatch K, M, N parallel → gate+commit each → 3b: dispatch L → gate+commit → full sweep + FIXES-BATCH-3.md + ledgers → final campaign report.
