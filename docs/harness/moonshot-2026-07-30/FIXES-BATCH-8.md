# Moonshot Batch 8 — Domain & Intelligence (2026-07-31)

> 7 commits on `vibeman/moonshot-batch8-2026-07-31` (off merged master `5c75077` / PR #27). One wave of 4 agents (M25+M26 paired).
> Baseline preserved: tests 772/0 → **808/0** (+36). No new migrations.

## Commits

| Commit | Item | Summary |
|---|---|---|
| `efa1968` | M38 | Salary nowcast — ratio-carry of the ISPV anchor via salary_gap history, confidence-graded, method-stamped |
| `7b8e2e7` | M09 | Zero-shot wrapper induction — pure-Rust RuleSet mining (containers + field slots), read-only, chains to M10 replay |
| `8dc2b67` | M35 | Taxonomy-as-data — trades/taxonomy registry consumed by 7 apps with proven enum fallback; proposer mode (enabled:false, human flips) |
| `b383477` | M25+M26 | DataHub topology lineage (flows/jobs/trigger DAG/fine-grained where mechanical) + govern=false pull loop (managed-only disables, cost:pause, assertion-triggered syncs) |
| `4d2ee8b` | — | integration |

## Operator notes

- Taxonomy: new trades enter via proposer (`propose_trade`) → human sets `enabled:true` in the trades/taxonomy record. Census/research apps expand automatically on next run once enabled.
- Nowcast rows appear only after salary_gap history accumulates (≥3 observations for med confidence).
- DataHub governance stays OFF until `[datahub] govern=true`; emit_flows is ON (harmless no-op without a DataHub).

## Remaining

Batch 9 (final): M01 host weather, M06 Transact, M17 distributed fetch fabric, M28 dynamic WASM apps, M30 dataset peering — the quarters-horizon architecture bets; v1 scope trims expected. Gated: M04 enforcement.
