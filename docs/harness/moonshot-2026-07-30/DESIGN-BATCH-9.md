# Batch 9 Design — Architecture Bets, v1 slices (2026-07-31, FINAL BATCH)

> Branch: `vibeman/moonshot-batch9-2026-07-31` off merged master `f4d4009` (PR #28). Baseline: tests 808/0.
> Items: M01, M06, M17, M28, M30 — the quarters-horizon bets. **Every one is deliberately sliced to a v1 that is real, safe, and default-OFF**; the remaining architecture stays documented, not half-built. Shared rules = DESIGN-BATCH-1.md §Shared rules. Read your FULL finding entry first.

## File-scope partition (HARD boundaries)

| Agent | Item | Finding | Owns |
|---|---|---|---|
| A9 | M01 host weather (export/import) | runtime-core.md §"### 1. Host Weather Network" | `crates/core/src/governor.rs`, `crates/core/src/tiers.rs` (tier_memory export/import), NEW routes `host-weather` + registration |
| B9 | M06 Transact (dry-run only) | runtime-core.md §"### 2. Transact" | `crates/core/src/engine.rs` (Transact types, additive), `crates/engine-browser/**` (flow execution), NEW `crates/apps/transact/**` + registration files (you own registry/Cargo/catalog-exempt this batch) |
| C9 | M17 fetch fabric (remote client) | scraping-engines.md §"### 1. Distributed fetch fabric" | NEW `crates/engine-remote/**`, `crates/core/src/fetcher.rs` remote branch, server engine wiring + `[remote]` config |
| D9 | M28 dynamic WASM apps (manifest discovery) | server-api.md §"### 2. Dynamic apps" | `crates/engine-wasm/**`, `crates/server/src/registry.rs` (dynamic listing), `[plugins] app_dir` config |
| E9 | M30 dataset peering (puller) | server-api.md §"### 2. Dataset peering" | NEW `crates/apps/peer/**` + its registration + `[[peer]]` config shape |

Migrations: claim 0033+ in reply if needed (+ inventory test).

## v1 slices (do NOT exceed these)

- **A9 M01**: `GET /host-weather/export` → signed-ish bundle (schema version, generated_at, node id, entries: host → tier pin, penalty state, challenge fingerprints, observation counts) with `?min_observations=` floor so thin/noisy hosts don't travel; `POST /host-weather/import` merges CONSERVATIVELY: never downgrade a locally-observed pin on fewer remote observations, cap imported penalty severity, count-weighted merge, dry-run `?apply=false` default. NO federation service, NO auto-sync. Tests: merge precedence, dry-run purity, floor.
- **B9 M06**: `Transact` = declarative multi-step flow (navigate → fill → click → wait → capture evidence) executed by the browser engine, **dry-run ONLY in v1**: every flow runs to the final confirmation step and STOPS before the irreversible submit, emitting an evidence bundle (screenshot path if the engine supports it, DOM snapshot, filled-field summary, the exact action it would have performed). `submit: true` is REJECTED with a typed error explaining that live submission needs a human-approval design (documented as the next slice). Idempotency keys + session profiles threaded now so the seam is right. App `transact` (CostClass per browser use), catalog-exempt (on-demand).
- **C9 M17**: `engine-remote` implements the HTTP client trait by POSTing the request to a peer node's `/fetch-proxy` endpoint (NEW route: takes a serialized HttpRequest, runs it through the LOCAL fetch stack incl. governor, returns the response) with shared-secret auth + size caps. `[remote] enabled=false, nodes=[]` — when enabled, the fetcher may route to a node by simple round-robin; failures fall back to local. Cluster-wide governor state is OUT (that's M01's bundle, later). Tests: proxy round-trip (local), auth rejection, fallback on node error.
- **D9 M28**: discovery + listing only: `[plugins] app_dir` scanned for `.wasm` modules exporting `describe()`; each becomes a **read-only registry entry** visible in `GET /apps` (name/description/params-schema from the manifest, `dynamic: true`, `runnable: false`) with a clear reason string. Actually RUNNING dynamic apps needs the component-model host work — document that as the next slice and DO NOT fake it (no partial run path). Tests: discovery, manifest parse, runnable:false invariant.
- **E9 M30**: `peer` app pulling from another pumper: params `{url, datasets:[...], cursor_state_key}` → for each dataset, `GET /datasets/{app}/{ds}/changes?since=<cursor>` (verify the real route/param name in routes first), upsert received records locally under a configurable namespace (default `peer_{app}`), persist cursor in a `peer/state` record, honor ETag/compression already shipped, cap per run. Tombstones applied if the changes feed carries them; otherwise noted honestly. Tests: cursor advance, namespace mapping, cap, resume.

## Orchestrator protocol
Dispatch A9–E9 parallel → gate+commit per item → FIXES-BATCH-9.md → ledgers → merge → **final campaign close-out** (Scan.md, lens memory, config improvement log, memory files).
