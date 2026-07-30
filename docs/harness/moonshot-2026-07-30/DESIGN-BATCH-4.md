# Batch 4 Design — Open Seams (2026-07-30)

> Branch: `vibeman/moonshot-seams-2026-07-30` off merged master `f9a7bc9` (PR #16). Baseline: tests 586/0.
> Five items = the open seams left by batches 1–3, all file-disjoint → ONE wave of 5 parallel agents. Shared rules = DESIGN-BATCH-1.md §Shared rules (no git; orchestrator gates + commits per item). Each seam is documented in code and in FIXES-BATCH-1/2/3.md — read the seam's doc comment before building.

## File-scope partition (HARD boundaries)

| Agent | Item | Owns | Must not touch |
|---|---|---|---|
| P | Research checkpoint port | `crates/apps/research/**` | worker.rs, core app.rs (seam already exists) |
| Q | MCP live surface | `crates/server/src/mcp/**`, its registration lines, `docs/features/mcp.md` | jobs/routes internals, events.rs internals (read-only subscribe) |
| R | API-recipe fetch branch | `crates/core/src/fetcher.rs`, `crates/core/src/recipes.rs`, core app.rs fetch seam if needed | engine-browser, datasets.rs |
| S | Wayback historical backfill | `crates/engine-archive/**`, `crates/apps/extractor/**` | core fetcher.rs, crawl app |
| T | Derived datasets v2 aggregates | `crates/core/src/datasets.rs`, `crates/server/src/routes/derived.rs`, derived storage section | recipes, fetcher |

Shared-file rule: config.rs/config.toml additive edits allowed (distinct sections); migrations — check crates/core/migrations/ live state; 0028+ free, claim in your reply. Inventory test updates travel with your migration.

## Item specs

### P — app_research checkpoint port (M23 seam)
The CheckpointSink seam shipped in `c6efbd3` (ctx.checkpoint/restore, throttled, attempts-lineage-guarded; crawl is the reference port). Port the research app — the expensive, long-running one: checkpoint after each completed agentic turn / budget-consuming step (session id, spent budget, partial findings, cursor), restore on re-claim and resume the session via the shipped `resume_session` path instead of restarting the research from zero. Poisoned-restore: start fresh (the seam's escape already counts failures). Tests: state round-trip, resume-vs-fresh decision, restored-budget accounting (never double-spend).

### Q — MCP live surface (M29 seams)
1. **Notifications**: implement the streamable-HTTP SSE half — `GET /mcp` opens an SSE stream (per MCP spec) delivering `notifications/*` JSON-RPC messages bridged from `EventBus::subscribe` + replay ring (job status, progress, dataset-changed). Per-connection filter params (app/kind). Bounded buffering — a slow consumer drops with a warning, never blocks the bus.
2. **Research tools**: `fetch_readable {url}` and `deep_research {query, budget_usd}` tools that enqueue the readable/research apps (gated by the existing `[mcp] allow_enqueue` + budget clamp) and return the job id; plus a `wait_job {job_id, timeout_secs}` tool that awaits terminal status via the event stream (bounded timeout ≤ a config cap).
Update docs/features/mcp.md. Tests: SSE handshake + a bridged event, tool wiring, wait_job timeout.

### R — API-recipe fetch branch (M05 seam)
The seam is documented in recipes.rs: ordering **api-recipe → archive → live HTTP**. Implement: when a VALIDATED recipe matches the request's host+path, fetch the recipe's API URL via the HTTP engine instead (structured JSON body returned; TierTrace entry `api_recipe`); validation loop — an unvalidated recipe is tried at most opportunistically (config `[recipes] auto_validate=false` default-OFF: when ON, a successful replay whose payload still overlaps expected fields marks validated via set_validated); thin/failed recipe fetch → strike (existing strike/penalty machinery pattern) → fall through to archive/live; N consecutive failures un-validate. Per-host opt-in stays: recipes only apply when the FetchRequest sets `use_recipes: true` OR config `[recipes] enabled=true` (default-OFF). Tests: branch ordering, fallback, strike/un-validate, opt-in gating.

### S — Wayback historical backfill (M18 seam, aligned with M42 conventions)
The seam is doc-commented on `cdx_query_url(to=…)`. Implement: engine-archive gains CDX **range enumeration** (`list_snapshots(url, from, to, max)` — dedup by digest, honest truncation flag). Extractor app gains an archive-backfill source mode (`source: {archive: {url|url_pattern, from, to, max_snapshots}}`): fetch each snapshot's raw body via the engine (governor-covered), run the ruleset, upsert records keyed `{natural_key}@{snapshot_date}` tagged `_url` + `_observed_at` + `_fetched_via: "wayback"` — the exact M42 backfill key convention so the two histories compose. Budget/politeness: per-run snapshot cap, archive.org through the governor as shipped. Tests: CDX range parsing + dedup, key/tag conventions, cap honesty.

### T — Derived datasets v2 aggregates (M11 seam)
Add `group_by` specs: `{group_by: [field paths], aggregates: {out: count | sum($.path)}}` (count + sum only, v2 scope). Incremental maintenance: on fresh source keys, recompute ONLY affected groups — for each touched group, re-scan the source rows of that group (bounded via the existing filtered list; if a group exceeds `max_group_scan` rows, mark the group row `stale:true` instead of wrong). Removals/changes: same affected-group recompute (exact, since recompute is from source truth, not deltas). Derived group rows keyed by joined group values. Backfill covers aggregates. Cycle/depth rules unchanged. Tests: count/sum correctness on add/change/remove, affected-group-only recompute, oversized-group staleness marking, backfill.

## Orchestrator protocol
Dispatch P–T parallel → per-return: gate (`cargo check --workspace` + targeted tests) → commit per item → final full sweep → FIXES-BATCH-4.md → vault (ledger Notes + run note) → report, then merge decision.
