# Batch 6 Design — Data Truth & Contracts (2026-07-30)

> Branch: `vibeman/moonshot-batch6-2026-07-30` off merged master `f450383` (PR #25). Baseline: tests 674/0.
> Five deferred moonshots (batches 6–9 will drain the remaining 20; this is the data-integrity themed set): M20, M12, M14, M41, M36. Shared rules = DESIGN-BATCH-1.md §Shared rules (no git; orchestrator gates + commits per item). Each agent reads its FULL finding entry (file+heading below) and follows its Path — this doc adds only coordination.

## File-scope partition (HARD boundaries)

| Agent | Item | Finding | Owns |
|---|---|---|---|
| A6 | M20 declarative data contracts | server-api.md §"### 2. Declarative data contracts" | `crates/core/src/catalog.rs` (contract block), worker publish-gate section in `crates/server/src/worker.rs`, catalog health/sources surfacing, `catalog/data-sources.toml` (contract blocks for 2–3 pilot sources), catalog README |
| B6 | M12 reproducible records / provenance | extraction-storage.md §"### 2. Reproducible records" | `crates/core/src/datasets.rs` (provenance stamp section), storage provenance methods (`// ── provenance ──`), migration 0030, NEW `routes/provenance.rs` + registration |
| C6 | M14 entity-typed index | scraping-engines.md §"### 2. Entity-typed index" | `crates/engine-search/**`, `crates/core/src/search.rs` (additive request fields), search route param additions |
| D6 | M41 web reliability index | content-research.md §"### 1. The Web Reliability Index" | `crates/apps/extractor/**` + `crates/apps/crawl/**` (telemetry persist), NEW shared helpers in whichever of the two crates fits (no core edits unless unavoidable — say so) |
| E6 | M36 compliance layer (state-licensing) | economic-data.md §"### 2. Cost-to-operate compliance layer" | NEW `crates/apps/state-licensing/**` + ALL shared registration files (registry.rs, server+root Cargo.toml, catalog row) + `crates/apps/trades-common` (additive only) + operator_economics join in `crates/apps/state-tax` or wherever the compliance block belongs — read how trades#2 emits per-state rows first |

Migrations: 0030 = B6. Others: avoid; if unavoidable claim 0031+ in reply + inventory test. Config additive sections allowed.

## Per-item guardrails (beyond the finding's Path)

- **A6 M20**: contract = `[source.contract]` TOML block (required_fields, types/ranges, max_row_delta_pct, max_staleness_hours). Enforce at the worker seam where `suppress_unhealthy` already gates pushes — verdict `pass|warn|block` with `[contracts] enforce=false` default-OFF (warn-only). Verdicts surface in `/catalog/health` + `/sources`. Pilot contracts for grants-gov + census-bfs rows (use the real dataset shapes — they were live-verified today). Drift-gate must stay green.
- **B6 M12**: stamp every revision with {job_id, source_url?, artifact_hash?, rules_hash?} where the write path knows them (many won't — honest-Null, never fabricate). `GET /provenance/{app}/{dataset}/{key}` returns the chain. `re-derive` action: only when artifact+rules are BOTH known — replays through the stored ruleset and reports byte-identical or diff; read-only otherwise (409 with reason). Size-bounded.
- **C6 M14**: index-time enrichment extracting money amounts + dates (deadlines) into tantivy FAST/INDEXED fields via the shipped schema-version machinery (`schema_is_current` → bump wipes index; SAY SO in reply — operator must rebuild via search-backfill like the indexed_at change). Query params: `amount_gte/lte`, `date_before/after`. Org/geo extraction OUT of scope v1 (regex-only money/date is honest; NER is not available). Extraction must be conservative — no match = no field, never a guessed value.
- **D6 M41**: persist per-host per-run telemetry the platform already computes (fetch outcomes, bot-wall/transport losses, markup-drift/health verdicts, conditional-GET support, gone-rates) into a `web-reliability/host_observations` dataset (append per run, keyed `{host}@{date}`) + a `host_index` aggregate (rolling scrapeability score). Consume ONLY existing telemetry — no new probing, no extra fetches.
- **E6 M36**: clone state-tax's proven one-call/51-jurisdiction agentic pattern; datasets `trades/compliance` keyed `{ST}:{trade}`; join a `compliance` block onto existing `{ST}:{trade}` operator_economics rows (read trades#2's shape first); validate guards like siblings; freshness-gate via trades-common `vintage_held`/`fresh_by_age` (this is a metered Claude app — gate BEFORE research like the other trades apps); catalog row + registration (you own the shared files this batch).

## Orchestrator protocol
Dispatch A6–E6 parallel → gate+commit per item → full sweep → FIXES-BATCH-6.md → ledgers (deferred→Fixed) → merge → Batch 7 (M15, M22, M24, M08, M32), Batch 8 (M35, M38, M09, M25+M26), Batch 9 (M01, M06, M17, M28, M30).
