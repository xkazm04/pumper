# Moonshot Batch 9 — Architecture Bets (FINAL, 2026-07-31)

> 7 commits on `vibeman/moonshot-batch9-2026-07-31` (off merged master `f4d4009` / PR #28). One wave of 5 agents.
> Baseline preserved: tests 808/0 → **862/0** (+54). Migration 0033 consumed.
> Every item is a deliberate v1 slice; the unbuilt remainder is documented, never faked.

## Commits

| Commit | Item | v1 slice shipped | Explicitly NOT built |
|---|---|---|---|
| `dccdb68` | M01 | Host-weather bundle export + conservative import (dry-run default, never-downgrade, raise-only capped) | federation service, auto-sync, challenge fingerprints (reserved, unpersisted) |
| `33180ec` | M06 | Transact flows executed to the confirmation step; structural stop-before-submit + typed rejection; evidence bundle | live submission (needs human-approval design), screenshots |
| `52fd982` | M17 | engine-remote + /fetch-proxy (shared secret, clamped caps, round-robin + local fallback), default-OFF | cluster-wide governor state (composes with M01 later) |
| `10bd894` | M28 | Dynamic .wasm manifest discovery in GET /apps, `runnable:false` + typed enqueue rejection | execution (component-model host) — deliberately no partial run path |
| `ff17aa0` | M30 | Peer puller over the verified changes-feed contract, cursor/ETag resume, tombstones, namespaced upserts | `[[peer]]` server-config auto-pull loop |
| `22b398b` | — | integration: peer registration, config, error codes, design doc | |

## Campaign complete

**All 44 moonshots from the 2026-07-30 scan are resolved**: 44 shipped across batches 1–9 (19 accepted at triage + 25 promoted afterwards), plus the live-contract verification pass. Tests 422/0 → 862/0 across the campaign.

Still deliberately gated: **M04 enforcement mode** (needs weeks of live yield history before an allocator should touch budgets).

## Operator checklist (carried forward from all batches)

1. `search-backfill --all` rebuild (entity fields changed the index schema, batch 6).
2. Watch first live runs of assumed-contract paths: cms-fee-schedule ZIP/CSV layout (batch 7).
3. Default-OFF flags to flip when wanted: `[archive]`, `[mcp]`(+allow_enqueue), `[ingress]`, `[refresher]`, `[recipes]`, `[contracts] enforce`, `[datahub] govern`, `[remote]`, `[economics] enforce`.
4. Trades taxonomy: new trades arrive via `propose_trade` → flip `enabled` by hand.
