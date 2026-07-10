# App fleet & domain datasets

Apps are `ScrapeApp` implementations under `crates/apps/*`, registered in `crates/server/src/registry.rs` (adding one: crate + workspace dep + server dep + one registry line). `GET /apps` lists name/description/schedule; each description documents its params.

## Generic apps

| App | What it does |
| --- | --- |
| `readable` | Any URL → clean Markdown via the tiered fetcher |
| `watch` | Visualping-style URL monitor: fingerprint (title/chars/sha256/600-char excerpt) into `pages` keyed by URL; result carries ChangeKind + field diff. Compose with `/schedules` + `/watches` |
| `crawl` | Broad crawler (see [crawling.md](crawling.md)) |
| `extractor` | Fetch a URL + apply a params-supplied extraction RuleSet |
| `plugin` | Run a named WASM extractor plugin over a fetched page |
| `research` | Agentic web research via the Claude engine (roles: research/compose); JSON report |
| `hackernews` | HN stories into a change-detected dataset |
| `connector-api-watch` | Watches Anthropic API docs pages: diff + summarize + alert |

## Domain apps & their cross-source datasets

| App(s) | Datasets produced |
| --- | --- |
| `grants-gov` (daily 09:00), `ca-grants` (daily 09:30) | own `opportunities` + **`grants/unified`** (canonical schema via `grants-common`: normalized status/dates/money, keys `<source>:<id>`) + **`grants/duplicate_links`** (cross-source SimHash pairs). grants-gov also emits a `closingSoon` digest (`digestDays`, default 14) |
| `eu-sedia` | EU SEDIA calls; records enriched with `description_text` (clean plain text beside raw HTML) |
| `cms-fee-schedule` | CMS fee-schedule release watcher |
| `mpsv-vpm` (daily) | CZ vacancies → `role_region_agg` (czisco×kraj×org salary cells), `region_agg`, `vacancy_samples`, `freshness`, **`role_trends`** (rising/falling from revision history), **`cz-labour/salary_gap`** (posted vs ISPV official, isco4×sphere), **`employers`** (ARES registry enrichment per IČO, capped 50 lookups/run) |
| `mpsv-ispv` | Official CZ salary statistics (quarterly `wages`) |
| `census-density`, `census-nonemp` | CBP employer + NES solo aggregates → **`census/market_blend`** (naics4×state: total market, solo_share, coverage; both apps re-derive) |
| `homewyse-pricing`, `state-tax`, `trade-wages`, `valuation-multiples` | Agentic (Claude-engine) US trades reference datasets |

## Conventions for new apps

Params defensively parsed from `ctx.params` with documented defaults; stable record keys; `upsert_many` for partial batches vs `sync_many` for full snapshots (removal detection); metered `ctx.fetch`/`ctx.research`; big payloads to artifacts; compact result JSON with new/changed counts (feeds cost-per-fresh-record and trigger summaries).

## Known gaps

Agentic apps other than homewyse lack `json_schema` output guards (backlog). Census YoY trend layer awaits multi-vintage accumulation.
