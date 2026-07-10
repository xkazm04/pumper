# Features

These documents describe the **implemented** product surface of pumper — a local-first Rust scraping/data platform. They are written for users, developers, and automation/CLI agents that need a stable reference. Future-looking ideas live in the Vibeman backlog, not here.

## Platform

| Area | Doc | Implementation roots |
| --- | --- | --- |
| Job runtime, scheduler, budgets & costs | [runtime.md](runtime.md) | `crates/core/src/{app,job,storage,costs,config}.rs`, `crates/server/src/{worker,scheduler,state}.rs` |
| Dataset store & change intelligence | [datasets.md](datasets.md) | `crates/core/src/{datasets,simhash}.rs`, `crates/core/migrations/` |
| Tiered fetching & engines | [fetching.md](fetching.md) | `crates/core/src/{fetcher,engine,governor,cache,tiers}.rs`, `crates/engine-{http,browser,claude}/` |
| Broad crawler | [crawling.md](crawling.md) | `crates/core/src/crawl.rs`, `crates/apps/crawl/` |
| Declarative extraction & WASM plugins | [extraction.md](extraction.md) | `crates/core/src/{extract,markdown,plugin}.rs`, `crates/engine-wasm/`, `crates/apps/{extractor,plugin}/` |
| Full-text search & saved searches | [search.md](search.md) | `crates/core/src/search.rs`, `crates/engine-search/` |
| Events & webhooks | [events-webhooks.md](events-webhooks.md) | `crates/server/src/{webhook,events}.rs` |
| Reactive pipelines (triggers) | [triggers.md](triggers.md) | `crates/server/src/triggers.rs`, migration 0014 |
| HTTP API | [http-api.md](http-api.md) | `crates/server/src/routes.rs` |
| App fleet & domain datasets | [apps.md](apps.md) | `crates/apps/*` |

## Maintenance notes

- Feature docs should name: what the feature does, the API/params surface, the data model (tables/datasets), and known gaps. State defaults and caps explicitly.
- `scripts/docs/feature-doc-map.json` maps source globs to these docs; a Stop hook reminds every Claude CLI session to update the coupled doc when it changes mapped source. Add a map entry when adding a feature area.
- Deep design rationale belongs in `docs/harness/` (e.g. `vision-scan-2026-07-10/DESIGN-reactive-pipelines.md`); keep these docs descriptive and current.
