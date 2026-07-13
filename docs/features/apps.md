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
| `grants-gov` (daily 09:00), `ca-grants` (daily 09:30) | own `opportunities` + **`grants/unified`** (canonical schema via `grants-common`, keys `<source>:<id>`: normalized status/dates, honest money — CA `EstAmounts` range → `award_floor`/`award_ceiling`, `EstAvailFunds` → `total_funding`, with K/M/B-suffix + range + thousands parsing — plus `categories[]`, `eligibilities[]` (CA `Categories`/`ApplicantType`, "; "-split), and `aln` (grants.gov `cfdaList`; CA has none). Adding these fields means the first run after deploy re-writes every unified row as `changed`) + **`grants/duplicate_links`** (cross-source SimHash pairs). grants-gov also emits a `closingSoon` digest (`digestDays`, default 14). Each run names `grants/unified` in its result `index_datasets`, so every opportunity is an individual full-text search doc (title/agency/status/url) that saved searches can alert on — see [search.md](search.md).<br>**Lifecycle:** both sources are upsert-only (partial views), so a closed/delisted grant just stops appearing — it is never a `removed_at`. After each sync, `grants-common::sweep_closed` flips every live unified row whose status is `open`/`forecasted` and whose `close_date` is before today to `closed` via the normal upsert path (so the transition is a recorded `changed` revision); the run reports `swept`. `removed_at` stays reserved for true full-snapshot sources. **Drift honesty:** a positive server `hitCount`/`total` with zero parsed rows now **fails** the job (was a silent `fetched:0` success); a >50% null-`title` rate across normalized opportunities adds a `warnings` array to the result. One shared date parser (`grants-common::parse_date`, formats `MM/DD/YYYY`, ISO date, ISO/space datetime) backs normalization, the sweep, and the digest.<br>**Query surface:** `grants/unified` is not just an export — `GET /grants` filters it in SQL by `status`/`agency`/`source`/`close_date` range/`min_award`, and `GET /grants/closing-soon?days=` is the **cross-source** closing-soon view (the job's own `closingSoon` digest is federal-only and built from raw grants.gov hits, so it never sees CA). Both are computed on read from the live corpus — no extra dataset to keep in sync. Note the money filter's reach: grants.gov's Search2 API publishes no award amounts, so `award_floor`/`award_ceiling`/`total_funding` are always null there and `min_award` narrows to `ca-grants` alone. See [http-api.md](http-api.md) |
| `eu-sedia` | EU SEDIA calls; records enriched with `description_text` (clean plain text beside raw HTML) |
| `cms-fee-schedule` | CMS fee-schedule release watcher |
| `mpsv-vpm` (daily) | CZ vacancies → `role_region_agg` (czisco×kraj×org salary cells), `region_agg`, `vacancy_samples`, `freshness`, **`role_trends`** (rising/falling from revision history), **`cz-labour/salary_gap`** (posted vs ISPV official, isco4×sphere), **`employers`** (ARES registry enrichment per IČO, capped 50 lookups/run) |
| `mpsv-ispv` | Official CZ salary statistics (quarterly `wages`) |
| `census-density`, `census-nonemp` | CBP employer + NES solo aggregates → **`census/market_blend`** (naics4×state: total market, solo_share, coverage; both apps re-derive) |
| `homewyse-pricing`, `state-tax`, `trade-wages`, `valuation-multiples` | Agentic (Claude-engine) US trades reference datasets (`pricing`, `tax`, `wages`, `valuation`) + the joined **`trades/operator_economics`** (via `trades-common`, see below). All four call the metered `ctx.research` seam: each run records a `claude` cost event, is refused/clamped against the job's `budget_usd`, and serves an identical re-run from the research cache at zero cost. Cache escape hatch: config `[claude].research_cache_ttl_secs = 0` disables it (default 86400s / 24h); `resume_session` requests also bypass it |

### `trades-common`: shared taxonomy + unified operator-economics layer

`trades-common` (a library crate, like `grants-common` — not a registered app) gives the four trades apps one canonical trade vocabulary and one cross-source dataset:

- **Canonical taxonomy** (`taxonomy::Trade`): the five trades (Plumbing, Electrical, HVAC, Landscaping, Pool service) each with a stable label + BLS SOC code. `taxonomy::prompt_list()` is the single source of the trade list used in every app prompt (was re-typed in three prompt strings). `taxonomy::canonicalize(raw)` normalizes a model-returned name ("plumber", "HVAC/R", "lawn care") to its canonical label so phrasing drift can't mint duplicate record keys. The three trade-keyed apps (`trade-wages`, `valuation-multiples`, `homewyse-pricing`) key records on the canonical label and store it in the record's `trade` field; `state-tax` is state-keyed and has no trade dimension. **One-time key normalization:** records written before this change used the model's raw label in the key (`US:<raw-label>`); after it, keys are `US:<canonical-label>`. Historical rows under a differently-phrased key are orphaned (not migrated) — the next run re-populates the canonical key.
- **Unknown labels** are never dropped: the raw string is kept and surfaced in the job result's `unknown_trades` array.
- **Unified dataset `trades/operator_economics`** (virtual `trades` app namespace, key `US:<trade>`): one row per canonical trade joining wage band (trade-wages), pricing summary (homewyse low/median/high envelope + jobs priced), federal + illustrative-state tax context (state-tax federal constants + median state top-marginal rate), and valuation multiples. Rebuilt by `unified::sync_operator_economics`, which each of the four apps calls at the end of its run (mirrors `grants-common`'s `sync_unified`); it is an `upsert_many` join, never a full-snapshot sync. Job results carry `unified: {new, changed}` counts.

## Conventions for new apps

Params defensively parsed from `ctx.params` with documented defaults; stable record keys; `upsert_many` for partial batches vs `sync_many` for full snapshots (removal detection); metered `ctx.fetch`/`ctx.research`; big payloads to artifacts; compact result JSON with new/changed counts (feeds cost-per-fresh-record and trigger summaries).

## Output guards (agentic trades apps)

All four trades apps (`trade-wages`, `homewyse-pricing`, `state-tax`, `valuation-multiples`) share output guards via the `trades-common` crate:

- **Structured output**: every research request carries a `json_schema` (`claude --json-schema`), so the CLI validates the answer's shape.
- **Salvage**: `trades_common::salvage_json` recovers a fenced or prose-wrapped object from raw text in ONE pass (no metered re-run) when the engine couldn't parse `output.json`.
- **Plausibility** (`trades_common::validate`): bands must be monotone (low ≤ median ≤ high — wage entry/median/experienced hourly+annual, pricing low/median/high, SDE low/median/high), rates ∈ [0,100] (state top marginal, federal SE/QBI/top rates), magnitudes positive (wages, employment, prices, multiples). A record that violates any check is **rejected with per-record reasons** in the result (`rejected` array + `rejected_count`); valid siblings still upsert.
- **Completeness** (`state-tax`): the 50 states + DC are enumerated in code (`US_JURISDICTIONS`); the result reports `states_covered`, `states_expected`, and a `missing_states` list.

## Known gaps

Census YoY trend layer awaits multi-vintage accumulation.
