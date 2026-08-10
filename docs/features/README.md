# Features

These documents describe the **implemented** product surface of pumper — a local-first Rust scraping/data platform. They are written for users, developers, and automation/CLI agents that need a stable reference. Future-looking ideas live in the Vibeman backlog, not here.

## Platform

| Area | Doc | Implementation roots |
| --- | --- | --- |
| Job runtime, scheduler, budgets & costs | [runtime.md](runtime.md) | `crates/core/src/{app,job,storage,costs,config}.rs`, `crates/server/src/{worker,scheduler,state}.rs` |
| Dataset store & change intelligence | [datasets.md](datasets.md) | `crates/core/src/{datasets,simhash}.rs`, `crates/core/migrations/` |
| Tiered fetching & engines | [fetching.md](fetching.md) | `crates/core/src/{fetcher,engine,governor,cache,tiers}.rs`, `crates/engine-{http,browser,claude}/` |
| Broad crawler | [crawling.md](crawling.md) | `crates/core/src/crawl.rs`, `crates/apps/crawl/` |
| Declarative extraction & WASM plugins | [extraction.md](extraction.md) | `crates/core/src/{extract,markdown,plugin}.rs`, `crates/engine-wasm/`, `crates/apps/{extractor,plugin}/`, `plugins-src/` |
| Extraction health (degradation detection) | [resilient-extraction.md](resilient-extraction.md) | `crates/core/src/resilience/`, migration 0020 |
| Full-text search & saved searches | [search.md](search.md) | `crates/core/src/search.rs`, `crates/engine-search/` |
| Events & webhooks | [events-webhooks.md](events-webhooks.md) | `crates/server/src/{webhook,events}.rs`; delivery-log routes live in `routes/triggers.rs`, the `pumper_webhook_*` gauges in `routes/meta.rs` |
| Reactive pipelines (triggers) | [triggers.md](triggers.md) | `crates/server/src/triggers.rs`, migration 0014 |
| Trigger plugins (sandboxed WASM hooks) | [trigger-plugins.md](trigger-plugins.md) | `crates/server/src/triggers.rs`, `crates/engine-wasm/`, `plugins-src/{trigger-gate,delta-slim}/` |
| HTTP API | [http-api.md](http-api.md) | `crates/server/src/routes/` |
| Observability (logging, error reporting, metrics) | [observability.md](observability.md) | `crates/server/src/main.rs`, `crates/server/src/routes/health.rs` |
| Data-source catalog | [catalog.md](catalog.md) | `catalog/`, `crates/core/src/catalog.rs` |
| DataHub metadata emitter | [datahub.md](datahub.md) | `crates/server/src/datahub.rs` |
| TypeScript consumer SDK (`@pumper/sync`) | [sdk-typescript.md](sdk-typescript.md) | `clients/typescript/` |
| Dataset peering (mirror another node) | [peering.md](peering.md) | `crates/apps/peer/`, consuming `crates/server/src/routes/datasets.rs` § changes feed |
| App fleet & domain datasets | [apps.md](apps.md) | `crates/apps/*` |

## Operations

| Area | Doc | Implementation roots |
| --- | --- | --- |
| Build artifact, local-first run, persistent state, env vars, **auth posture** | [../deployment.md](../deployment.md) | `crates/server/src/main.rs`, `crates/core/src/config.rs`, `config.toml`, `justfile`, `.github/workflows/ci.yml` |

## Maintenance notes

- Feature docs should name: what the feature does, the API/params surface, the data model (tables/datasets), and known gaps. State defaults and caps explicitly.
- `scripts/docs/feature-doc-map.json` maps source globs to these docs; a Stop hook reminds every Claude CLI session to update the coupled doc when it changes mapped source. Add a map entry when adding a feature area.
- Two mapped areas are currently **inert**: `catalog/` and `plugins-src/` are in the hook script's `SKIP_PATTERNS` (`scripts/docs/check-doc-sync.mjs`), so edits there never reach the map lookup. The entries exist so the coupling is recorded; removing those two skip patterns would activate them.
- Deep design rationale belongs in `docs/harness/` (e.g. `vision-scan-2026-07-10/DESIGN-reactive-pipelines.md`); keep these docs descriptive and current. `resilient-extraction.md` is the one exception — it is a design document that has been partly implemented, and it carries per-section **Not built** markers plus a "what ships today" table at the top so a reader can never mistake an unbuilt section for shipped surface.
