# pumper — harness learnings

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
