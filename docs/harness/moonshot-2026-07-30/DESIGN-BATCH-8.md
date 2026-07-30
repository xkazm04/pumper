# Batch 8 Design — Domain & Intelligence (2026-07-31)

> Branch: `vibeman/moonshot-batch8-2026-07-31` off merged master `5c75077` (PR #27). Baseline: tests 772/0.
> Items: M35, M38, M09, M25+M26 (pair, one agent). Shared rules = DESIGN-BATCH-1.md §Shared rules. Agents read their FULL finding entries and follow the Path; this doc = coordination + trims.

## File-scope partition (HARD boundaries)

| Agent | Item | Finding | Owns |
|---|---|---|---|
| A8 | M35 taxonomy-as-data | economic-data.md §"### 1. Taxonomy-as-data" | `crates/apps/trades-common/**` (taxonomy registry core), the 4 trades research apps' + 2 census apps' taxonomy consumption (read-only where possible; additive param plumbing), NEW dataset `trades/taxonomy`, proposer mode |
| B8 | M38 salary nowcast | economic-data.md §"### 2. Salary nowcast" | `crates/apps/mpsv-vpm/**` + `crates/apps/mpsv-ispv/**` |
| C8 | M09 zero-shot wrapper induction | extraction-storage.md §"### 1. Zero-shot wrapper induction" | NEW `crates/core/src/induce.rs` (or a module in extract.rs's orbit), extractor app `induce` mode (`crates/apps/extractor/**`) |
| D8 | M25+M26 DataHub pair | server-api.md §DataHub "### 1." + "### 2." | `crates/server/src/datahub.rs` + its config/scheduler piggyback |

Migrations: none expected; claim 0033+ in reply if unavoidable (+ inventory test). Config additive allowed.

## Guardrails / trims

- **A8 M35**: v1 = the REGISTRY + consumption: `trades/taxonomy` dataset (trade → canonical label, SOC, NAICS list, aliases, enabled, source:"seed"|"proposed"|"approved") seeded from the current compile-time 5-trade enum (enum stays as fallback — never break existing callers); trades-common exposes `taxonomy()` reading the dataset with the enum as fallback; the research apps + census apps consume it for their trade/NAICS lists (additive — default behavior identical when the dataset is absent). Proposer = a MODE on the app you judge best-fitting (`propose_trade: "roofing"` → one metered research call maps label→SOC/NAICS/aliases → upserts source:"proposed", enabled:false — a human flips enabled). NO auto-enable, NO auto-expansion of scheduled runs.
- **B8 M38**: nowcast v1 = deterministic, no ML: per CZ-ISCO unit group, learn the posted-vs-ISPV ratio from the existing salary_gap dataset (median of last N observations), apply to current posted medians → `cz-labour/salary_nowcast` {isco4: nowcast_median, ratio_used, observations, confidence: high|med|low by observation count + dispersion, staleness of ISPV anchor}. Absent history ⇒ no row (never extrapolate from nothing). Document that this is a ratio-carry nowcast, not a model.
- **C8 M09**: v1 scope = single-page-set induction, no clustering: given `induce: {urls|url_pattern, min_support 0.6}` the extractor mode loads stored bodies (latest artifacts), finds repeating container candidates (repeated tag+class paths with ≥3 instances), scores field slots (text varies across instances, structure fixed), emits a CANDIDATE RuleSet (Rule::Each shape) + per-field support stats as job result + artifact. Human reviews; optionally chains to M10 replay for validation (mention in result). dom_simhash CLUSTERING deferred (say so). No LLM. Pure-Rust heuristics over scraper/ego-tree.
- **D8 M25+M26**: M25 = emit schedules as dataFlow, runs as dataJob with in/out dataset edges, triggers as lineage edges (the shipped trigger DAG renders in DataHub); column-level lineage only where declarative RuleSets make it mechanical — else skip honestly. M26 = pull loop (scheduler-piggybacked like DLQ drain, `[datahub] govern=false` default-OFF): deprecation flag → disable managed schedules (reuse M19's managed_by discipline — NEVER touch untagged), `cost:pause` tag → pause Claude-tier for that source, failed freshness assertion → enqueue immediate sync. All actions logged loudly + surfaced in the result/health. DataHub absent/unreachable = clean no-op (existing emitter's posture).

## Orchestrator protocol
Dispatch A8–D8 parallel → gate+commit per item → FIXES-BATCH-8.md → ledgers → merge → Batch 9 (M01, M06, M17, M28, M30 — the big bets, expect trims).
