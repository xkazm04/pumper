# Feature Propagation Plan — per-client adoption, no big-bang (2026-07-31)

> Branch: `vibeman/adoption-2026-07-31` off master `582d20f`. Baseline: tests 862/0.
> The 44-moonshot campaign shipped **platform seams**; most apps have not adopted them. This plan propagates capability **per client group**, one Opus agent per group, each responsible for applying AND testing its own apps. Nothing is enabled globally: every adoption is opt-in per app, and the default-OFF config flags stay OFF.

## Measured adoption gap (probed 2026-07-31)

| Seam (shipped in) | Adopters today | Gap |
|---|---|---|
| Provenance stamps `upsert_with_provenance` (M12) | **0 apps** | ledger exists, nothing populates source_url / artifact_sha / rules_hash |
| Data contracts `[source.contract]` (M20) | 2 of 10 live sources | grants-gov, census-bfs only |
| Rich `AppManifest` (M27) | 9 of 28 apps | the other 19 serve name+defaults only → weak for agents/MCP |
| Checkpoints `ctx.checkpoint/restore` (M23) | crawl, research | every other long job restarts from zero on reap |
| Archive tier `archive_max_age` (M18) | **0 apps** (only `None` initializers) | free politeness + history unused |
| API recipes `use_recipes` (M05) | **0 apps** | discovery runs, recipes never consumed |
| VCR (M24) | automatic via AppContext | crawl bypasses (raw engine) — documented |

## Sequencing principle

Adoption waves are ordered by **blast radius, lowest first**: metadata (manifests) → observability (provenance) → guardrails (contracts) → behavior changes (checkpoints, archive, recipes). Within a wave, each client group is independent, so a bad adoption is contained to one domain and revertible as one commit.

## Client groups (disjoint crate ownership — safe parallel)

| Agent | Client group | Crates owned |
|---|---|---|
| G1 | **Funding & grants** | `grants-gov`, `ca-grants`, `eu-sedia`, `cordis`, `cms-fee-schedule`, `grants-common` |
| G2 | **Czech labour market** | `mpsv-vpm`, `mpsv-ispv`, `smlouvy-dump-watch` |
| G3 | **US trades & census** | `trade-wages`, `homewyse-pricing`, `state-tax`, `valuation-multiples`, `state-licensing`, `census-density`, `census-nonemp`, `census-nesd`, `census-bfs`, `trades-common`, `census-common` |
| G4 | **Content & research** | `readable`, `research`, `hackernews`, `connector-api-watch`, `watch`, `extractor`, `plugin` |
| G5 | **Crawl & platform apps** | `crawl`, `peer`, `provisioner`, `transact` |

**Shared files nobody edits**: `catalog/data-sources.toml`, `crates/server/**`, `crates/core/**`, `config.toml`, `Cargo.toml`. Contract blocks are **proposed in the reply as TOML**; the orchestrator applies them centrally (avoids 5-way conflict on one file). If an agent believes a core change is required, it reports the need instead of making it.

## Per-group task (same shape for every agent)

For **each app you own**, decide adoption per seam using the app's real behavior — *this is a judgement task, not a checklist to max out*. Skipping a seam with a one-line reason is a valid, expected outcome.

1. **Rich manifest (M27)** — adopt for every app that lacks one: params JSON Schema (match the code's actual param reads), 2 worked examples, output_shape, cost class. The registry test enforces examples-validate-against-schema, and scheduled apps' `default_params` must satisfy their own schema.
2. **Provenance (M12)** — adopt where the app *knows* the provenance: switch upserts to `upsert_with_provenance` passing `source_url` (the fetched URL for that record) and, where a RuleSet produced the record, `rules_hash`. **Never fabricate**: if a record is an aggregate of many URLs, pass None rather than a misleading single URL. Say what you passed and what you left Null.
3. **Contracts (M20)** — for each app with a **live** catalog row, propose a `[source.contract]` block grounded in the dataset's REAL shape (read the upsert code; use the live-verified shapes where the campaign verified them). Required fields must be ones that are genuinely always present; ranges only where a violation would mean corrupt data; `max_row_delta_pct` sized to the source's real churn. A too-strict contract is worse than none — it will block good publishes once `enforce` is turned on.
4. **Checkpoints (M23)** — adopt only for jobs that are genuinely long or paged (multi-page syncs, large feeds, per-item detail harvests). Checkpoint the resumable unit (page cursor, processed-key set, per-item progress) and restore on re-claim. Skip short single-call apps and say so.
5. **Archive tier (M18)** — adopt `archive_max_age` only where a stale-but-cheap body is genuinely acceptable (backfill paths, low-volatility documents). **Do not** set it on anything whose value is freshness (open-call feeds, daily labour feeds). Expect most apps to skip this.
6. **Recipes (M05)** — set `use_recipes: true` only on apps that fetch JS-heavy hosts where a discovered API recipe would apply. Most apps hit JSON APIs already and should skip.

**Rules**: no `cargo fmt` on the workspace; only your crates; run `cargo check -p <yours>` and `cargo test -p <yours>`; no git. Behavior must be identical when the new paths are inactive (no contract enforcement is on by default; provenance is additive metadata).

## Reply contract (per agent)

Per app: a one-line adoption verdict per seam (`adopted: <how>` or `skipped: <why>`), then: files touched, test count added, proposed `[source.contract]` TOML blocks (verbatim, ready to paste), and anything that needs a core/server change you did NOT make.

## Orchestrator protocol

Dispatch G1–G5 (Opus) in parallel → per return: `cargo check --workspace` + targeted tests → commit that group → apply all proposed contract blocks centrally → full sweep → `ADOPTION-REPORT.md` → merge. Contracts stay warn-only (`[contracts] enforce=false`) in this PR; flipping enforcement is a separate, deliberate step after the warn logs come back clean from real runs.
