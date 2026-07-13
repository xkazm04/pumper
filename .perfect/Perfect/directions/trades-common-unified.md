---
slug: trades-common-unified
type: perfect/direction
context: "[[US Trades Wages, Tax & Valuation]]"
lens: feature
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: a458c2a
---

## What & why
No shared taxonomy: the 5-trade list is re-typed in three prompt strings, keys embed model-echoed labels (phrasing drift → duplicate keys, defeated change detection), and no consumer joins the four datasets. Build `trades-common` (mirroring wave-6 `grants-common`): canonical trade enum + SOC mapping used by all four apps' prompts/keys/normalization, producing a joined `trades/operator_economics` dataset (per trade: wage band + typical job pricing + tax context + valuation multiple). Phase-1 slice of the digital-twin moonshot.

## Evidence
- Re-typed trade lists: crates/apps/trade-wages/src/lib.rs:71, homewyse-pricing:76, valuation-multiples:67
- Model-echoed keys: trade-wages:122, homewyse:131
- No cross-dataset consumer (scout grep); precedent: grants-common (docs/features/apps.md:22, harness-learnings:35)

## Acceptance criteria
- [ ] `trades-common` crate: canonical trade enum (label + SOC code), used by all four apps for prompts and record keys (normalize model output to canonical labels).
- [ ] `trades/operator_economics` unified dataset upserted after source syncs, keyed `US:<trade>`.
- [ ] Workspace/registry wiring per repo law; migration-free (datasets are rows).
- [ ] docs/features/apps.md documents the unified layer.

## Risks / non-goals
- Key normalization changes existing record keys — one-time re-key; note in docs.
- Non-goal: per-locality joins (pricing locality vs tax state remains unaligned for now).

## Build record
(pending)
