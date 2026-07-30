# Moonshot Batch 6 — Data Truth & Contracts (2026-07-30)

> 7 commits on `vibeman/moonshot-batch6-2026-07-30` (off merged master `f450383` / PR #25). One wave of 5 agents.
> Baseline preserved: tests 674/0 → **724/0** (+50; one unrelated flake in an intermediate run, clean on re-run). Migration 0030 consumed.

## Commits

| Commit | Item | Summary |
|---|---|---|
| `f0d6a35` | M41 | Web reliability index — host_observations + weighted scrapeability host_index from telemetry runs already compute, zero new fetches |
| `dea9223` | M14 | Entity-typed index — conservative money/deadline extraction into FAST fields, amount/date query predicates; **schema bump: operator must rebuild via search-backfill** |
| `c009b85` | M36 | State-licensing compliance app — {ST}:{trade} requirement/bond/insurance, joined onto operator_economics; freshness-gated metered research |
| `f043dcc` | M12 | Provenance ledger — job/source/artifact/rules stamps (0030), GET /provenance chain + read-only re-derive (reproduced/diverged) |
| `6ba5d30` | M20 | Declarative data contracts — [source.contract] evaluated at the publish seam, pass/warn/block, enforce default-OFF, pilots on live-verified shapes |
| `0d0b1f0` + docs | — | integration + this doc |

## Operator notes

- **Search index schema changed again** (amount/event_date fields) — rebuild once: `cargo run -p pumper-server --bin search-backfill -- --all` (server stopped).
- Contracts are warn-only until `[contracts] enforce=true`.
- state-licensing is a metered Claude app (annual cadence, freshness-gated; `force:true` bypasses).

## Remaining backlog after batch 6

Batch 7: M15 WASM-everywhere, M22 sink connectors, M24 VCR replay, M08 link graph, M32 Medicare oracle.
Batch 8: M35 taxonomy-as-data, M38 salary nowcast, M09 wrapper induction, M25+M26 DataHub pair.
Batch 9: M01 host weather, M06 Transact, M17 fetch fabric, M28 dynamic WASM apps, M30 peering.
Still gated: M04 enforcement (live yield history).
