# pumper — harness learnings

## Structural facts
- **2026-07-10** — Change detection substrate lives in `crates/core/src/datasets.rs` (`records` table, sha256 hash + simhash); as of wave 1 it also has `record_revisions` (field-level diff history) and `removed_at` (disappearance signal). Migrations in `crates/core/migrations/` via `sqlx::migrate!`.
- **2026-07-10** — Apps are `ScrapeApp` trait impls under `crates/apps/*`; integration = workspace dep + `crates/server/Cargo.toml` + one line in `crates/server/src/registry.rs`. `crates/apps/*` is a workspace-member glob, but the dep entry in root `Cargo.toml [workspace.dependencies]` is still required.
- **2026-07-10** — Webhook delivery contract (HMAC-SHA256 `x-pumper-signature`, 3 retries, fire-and-forget spawn) is in `crates/server/src/webhook.rs`; job callbacks and dataset watches share it.
- **2026-07-10** — `FetchRequest` (tiered fetcher) has NO cache-bypass flag; HTTP cache TTL governs freshness. Monitors that need live bodies await a `no_cache` addition.
- **2026-07-10** — sqlx here uses runtime queries (`sqlx::query`), not compile-time macros — no offline cache/DATABASE_URL needed to build.

## Conventions enforced
- Timestamps stored as fixed-width RFC 3339 UTC micros (`ts()` helpers) so lexicographic SQL comparison = chronological order. Follow this for any new TEXT timestamp column.
- Record keys are stable external ids (opportunity id, URL); revision numbers are per-key starting at 1.
- Job results are JSON `Value`s; large payloads go to `ctx.save_artifact`, results stay compact.

## Anti-patterns to avoid
- Don't diff raw page bodies in dataset records — store compact fingerprints (title/chars/hash/excerpt), full content as artifact (wave 1 `watch` app decision).
- Don't use `sync_many` for filtered/partial scrapes — it marks absent keys removed; only full snapshots.

## Open follow-ups (from Wave 1, 2026-07-10)
- `FetchRequest.no_cache` flag for monitors (watch app currently sees TTL-cached bodies).
- Line-level text diff for watch-app excerpts.
- Vibeman-side bug observed during scans: `/api/ideas/claude` sometimes returns a different group's prompt than the requested `groupId` (agents self-corrected via `/api/contexts?groupId=`); also idea `category` rejects values outside functionality/performance/maintenance/ui/code_quality/user_benefit.
- Remaining INDEX themes: T2 cost ledger (top-scored), T4 search, T6 crawler, T7 API hardening, T5 AI extraction, T9 domain products, T10 platform.
