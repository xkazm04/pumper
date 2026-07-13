# pumper — harness learnings

## Structural facts (Perfect round 1, 2026-07-13)
- **2026-07-13** — `/perfect` loop state lives in `.perfect/Perfect/` (queue, directions, session notes); skill at `.claude/skills/perfect/skill.md`. Round 1 shipped 12 directions across fetcher/trades/api contexts.
- **2026-07-13** — `FetchOutcome.trace` is the machine-readable escalation signal (`TierTrace` per tier, `TierVerdict` enum); `escalations` strings are a rendered view — never string-match them for logic (the tier router keys on the enum).
- **2026-07-13** — OpenAPI: routes register through `openapi_router()` (utoipa-axum `OpenApiRouter`) in `crates/server/src/routes.rs` — a new route needs a `#[utoipa::path]` annotation AND a line in the coverage test's `EXPECTED` inventory, or `cargo test -p pumper-server` fails.
- **2026-07-13** — SSE events flow through `EventBus` (`crates/server/src/events.rs`): monotonic ids, 1024-event replay ring, `Last-Event-ID` resume, `reset` event on evicted gaps. Emit via the bus, never raw broadcast.
- **2026-07-13** — Graceful shutdown: shared `CancellationToken` in AppState; worker drains up to `[worker] shutdown_drain_secs` (default 25) then re-queues survivors. New background loops must select on the token.
- **2026-07-13** — Host profiles: `tier_memory` (migration 0016) carries strikes + browser pin (aging via `[fetcher] host_memory_ttl_secs`, default 7d) and write-behind governor penalty snapshots; inspect/reset via `GET/DELETE /hosts*`.
- **2026-07-13** — `trades-common` crate = canonical trade taxonomy (`taxonomy::Trade`) + `trades/operator_economics` unified dataset + shared `salvage_json`/`validate` for the four agentic trades apps (all metered via `ctx.research` now).

## Structural facts (Perfect round 2, 2026-07-13)
- **2026-07-13** — Job writes are attempt-fenced: `complete`/`fail`/`fail_permanently`/`cancel_running` guard on `(status='running', attempts)` — always pass the attempt you claimed with; a discarded write (false/None) means the job was reset/reaped and the live attempt owns it. Reset (`POST /jobs/{id}/reset`), bulk retry (`POST /jobs/retry`), and cancel-running (per-job CancellationToken in `AppState::job_cancels`) live in worker.rs/storage.rs.
- **2026-07-13** — Heartbeat lease: `jobs.heartbeat_at` (migration 0017), beaten only while the app future yields (a wedged future stops beating); reaper piggybacks the scheduler tick (`[worker] heartbeat_secs`/`stale_after_secs`, 0 disables). Keep any long CPU-bound app work yielding or it will be reaped.
- **2026-07-13** — Claim query uses priority aging: effective = priority + waited_secs/`[worker] priority_aging_coefficient_secs` (default 900; 0 = legacy strict order).
- **2026-07-13** — Schedules carry `timezone` (IANA, NULL=UTC), `misfire_policy` (`fire_once`|`skip`), `max_attempts` (NULL → server default 3) — migration 0018; cron 0.12 evaluates natively in the schedule's tz (DST gaps skip). Scheduled jobs now retry like manual ones.
- **2026-07-13** — `job.failed` webhook (permanent failures only) fires from `finalize()` to `[webhooks] failure_url` via `dispatch_event`; `pumper_job_failures_total{app}` on /metrics is DB-derived (not monotonic).
- **2026-07-13** — Crawler: kept pages stream to the `crawl/pages` dataset through the `PageSink` seam (app-side `DatasetPageSink`, batches of 50, upsert_many); `mode:"revisit"` re-checks stored pages with conditional GETs (`HttpRequest.etag`/`if_modified_since`, 304 pass-through) and flags `gone` on 404/410 — never sync_many. Bot-wall pages are counted (`skipped_botwall`), not kept. SimHash dedup is banded (d+1 bands); checkpoints are versioned (v1) — old ones reset cleanly.
- **2026-07-13** — Job progress: `AppContext.progress` (ProgressReporter, throttled ≥2s/50 calls in `crates/server/src/progress.rs`) → in-memory ProgressStore + `progress` SSE job events; surfaced on `GET /jobs/{id}`. In-flight only — lost on restart by design.
- **2026-07-13** — Parallel-builder caution: the shared CARGO_TARGET_DIR is NOT safe under two concurrent agent sessions editing the same crates (stale rlib linkage) — use per-session target dirs when running parallel worktree builds.

## Structural facts
- **2026-07-10** — Change detection substrate lives in `crates/core/src/datasets.rs` (`records` table, sha256 hash + simhash); as of wave 1 it also has `record_revisions` (field-level diff history) and `removed_at` (disappearance signal). Migrations in `crates/core/migrations/` via `sqlx::migrate!`.
- **2026-07-10** — Apps are `ScrapeApp` trait impls under `crates/apps/*`; integration = workspace dep + `crates/server/Cargo.toml` + one line in `crates/server/src/registry.rs`. `crates/apps/*` is a workspace-member glob, but the dep entry in root `Cargo.toml [workspace.dependencies]` is still required.
- **2026-07-10** — Webhook delivery contract (HMAC-SHA256 `x-pumper-signature`, 3 retries, fire-and-forget spawn) is in `crates/server/src/webhook.rs`; job callbacks and dataset watches share it.
- **2026-07-13** — `FetchRequest` exposes `no_cache` (bypass the HTTP cache) and `ttl_override` (per-fetch cache TTL secs); both thread to the HTTP tier's `HttpRequest` and the cache `put`. The `watch` app bypasses the cache by default (or caps staleness via a `cache_ttl_secs` param).
- **2026-07-10** — sqlx here uses runtime queries (`sqlx::query`), not compile-time macros — no offline cache/DATABASE_URL needed to build.

## Conventions enforced
- Timestamps stored as fixed-width RFC 3339 UTC micros (`ts()` helpers) so lexicographic SQL comparison = chronological order. Follow this for any new TEXT timestamp column.
- Record keys are stable external ids (opportunity id, URL); revision numbers are per-key starting at 1.
- Job results are JSON `Value`s; large payloads go to `ctx.save_artifact`, results stay compact.

## Anti-patterns to avoid
- Don't diff raw page bodies in dataset records — store compact fingerprints (title/chars/hash/excerpt), full content as artifact (wave 1 `watch` app decision).
- Don't use `sync_many` for filtered/partial scrapes — it marks absent keys removed; only full snapshots.

## Structural facts (Wave 2 additions)
- **2026-07-10** — Cost metering seam is `AppContext::fetch`/`::research` (crates/core/src/app.rs), NOT the engines; `cost_events`/`research_cache` tables (migrations 0007/0009), `jobs.budget_usd` (0008). Apps calling `ctx.engines.*` directly are unmetered and budget-exempt.
- **2026-07-10** — `ClaudeConfig` gained `research_cache_ttl_secs` (default 86400; 0 disables) — config structs use `#[serde(default)]` + manual `Default` impls, so new fields need both.

## Open follow-ups (from Waves 1–2, 2026-07-10)
- Line-level text diff for watch-app excerpts.
- ~~Migrate remaining apps to metered `ctx.fetch`/`ctx.research` (agentic apps first: state-tax, trade-wages, valuation-multiples, homewyse — they spend Claude money unmetered).~~ **Done 2026-07-13** — all four trades apps now call `ctx.research` (cost-attributed, budget-governed, research-cached). Remaining unmetered `ctx.fetch` callers only.
- `research_cache` purge job (mirror `HttpCache::purge_expired`).
- Vibeman-side bug observed during scans: `/api/ideas/claude` sometimes returns a different group's prompt than the requested `groupId` (agents self-corrected via `/api/contexts?groupId=`); also idea `category` rejects values outside functionality/performance/maintenance/ui/code_quality/user_benefit.
- Remaining INDEX themes: T9 domain products, T10 platform (T4 search fundamentals closed in wave 5; deferred T4 tail: answer-engine RAG, hybrid semantic [3 dup ideas], multilingual, LTR, autocomplete. T7 deferred: API-key auth [product decision], OpenAPI, SSE Last-Event-ID, misfire, hot-reload. T5 LLM-assisted extraction items remain).

## Structural facts (Wave 7 / moonshot additions)
- **2026-07-10** — Reactive pipelines = `triggers` table (migration 0014): edges (source event → target app), NO pipeline container; the DAG is the edge set. Two kinds: `dataset` (change-feed, `on_change` filter incl. `fresh`) and `job` (terminal, `on_status`). Eval runs in worker hooks (`crates/server/src/triggers.rs::fire_*`), fail-open. Contract: target reads `params._trigger` (compact — capped keys + counts + `source_job_id`; fetch full data by id); provenance `chain`/`depth` rides in `_trigger` (cycle guard + `[triggers] max_depth`, default 8); idempotency key `trig:{trigger}:{source_job}` = at-most-once per source run. Lineage via `jobs.trigger_id` + `GET /triggers/{id}/runs`. Design doc: vision-scan-2026-07-10/DESIGN-reactive-pipelines.md (fan-in barriers + templating are explicit non-goals).

## Structural facts (Wave 6 additions)
- **2026-07-10** — Cross-source grant layer: `grants-common` crate normalizes grants-gov + ca-grants into the virtual `grants` app namespace (`unified` + `duplicate_links` datasets, keys `<source>:<id>`). New grant sources should implement a `normalize_*` there and call `sync_unified` + `link_duplicates`.
- **2026-07-10** — mpsv-vpm `role_trends` derives from `role_region_agg` revision history — deleting revisions breaks the trend window.

## Structural facts (Wave 5 additions)
- **2026-07-10** — `Search::query` takes `SearchRequest` and returns `SearchResponse` (hits+facets); trait also has `delete_ids`/`delete_dataset`. Tantivy body field is STORED (snippet support); old indexes auto-rebuild empty on open. Saved searches (`saved_searches`/`saved_search_seen`, migration 0013) alert via the logged webhook path with exactly-once claim_unseen dedup.
- **2026-07-10** — Any new webhook event kind should go through `webhook::dispatch_event` (logged, signed, replayable) — never hand-roll a reqwest send.

## Structural facts (Wave 4 additions)
- **2026-07-10** — All outbound webhooks flow through `crates/server/src/webhook.rs::deliver` and are logged to `webhook_deliveries` (migration 0010); replay via `POST /webhooks/deliveries/{id}/replay`. Jobs table now also carries `idempotency_key` (0011, partial unique idx) and `schedule_id` (0012, overlap guard).
- **2026-07-10** — Pagination convention: `cursor=` param (even empty) switches list endpoints to `{items, next_cursor}`; cursors are `<stored-ts>|<id-or-key>` keysets. Follow this for any new list endpoint.

## Structural facts (Wave 3 additions)
- **2026-07-10** — Extraction rules: `RuleSet.fields` maps to `FieldRule {rule, transforms}` (serde-flattened; old plain-rule JSON still parses). Rule types: css/regex/json/xpath/const. XPath via `skyscraper` crate (pure Rust, HTML-native; heavy grammar crate, ~1min cold-build cost).
- **2026-07-10** — Crawler checkpoints live at `data/artifacts/<app>/checkpoints/<name>.json` (beside per-job dirs, not inside them) so cross-job resume works.
