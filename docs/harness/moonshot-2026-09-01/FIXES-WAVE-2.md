# Wave 2 — Act, Orchestrate, Federate (2026-09-02)

Five builders, five worktrees, all five items merged into `master`. Each builder reported the five
cargo gates green in its worktree; the coordinator re-ran check/clippy/tests on `master` after each
merge and the full gate set after the last (§Verification).

## Merges (in landing order)

| Item | Branch | Builder commits | Notes |
| --- | --- | --- | --- |
| N15 Self-hosted agent loop (+ `api_recipe` pin carry-forward) | `moonshot/n15-agent-loop` | 00f66a7, f6fcb32, f054d48, 2cc1abe | no migration; clean merge |
| N03 Workflow runs (+ principal stamping, `GET /costs?principal=`, `/economics.by_principal`) | `moonshot/n03-workflows` | 34e309d … e471e9f (6) | migration **0046**; 4 conflicts (MCP tool table/test, mcp.md, doc map) — resolved keeping `wait_workflow` then `fetch` last; a shell-mangled comment fixed in `c1b4ae0` |
| N16 Pumper Mesh | `moonshot/n16-mesh` | 7 commits | no migration; MEMORY.md invariant renumbered 7→9; `fetching.md` keeps N15's closed-gap wording plus N16's mesh-recipe bullet |
| N01 Transact v2 | `moonshot/n01-transact` | d0ca92e … 4716afb (6) | migration renumbered 0046→**0047**; six conflicts; `config.rs` rebuilt from master + N01's three additions (field, `allow_approve`, `TransactConfig` block) rather than by hunk |
| N10 WASM sinks & connectors | `moonshot/n10-wasm-sinks` | 13 commits | migration renumbered 0047→**0048**; one `main.rs` conflict |

Master migrations now end at **0048**.

## What master can do now (all default OFF unless stated)

- `[claude] self_hosted_tools = true`: the research subprocess fetches through this node's own
  `fetch` MCP tool under a per-job token; spend lands on the job; `receipt.cost.self_hosted_fetches`.
  The learned `api_recipe` pin now steers `AppContext::fetch`.
- Workflows: `POST /workflows` + `/runs`, fan-in `all_of` barriers evaluated exactly once in
  `finalize`, envelope budget, one rolled-up receipt, `run_workflow`/`wait_workflow` over MCP,
  cron-driven runs. Inert until a workflow is declared. `jobs.principal_id` and
  `cost_events.principal_id` are stamped at the enqueue door.
- `[[peer]]` mesh: ed25519 node identity (`GET /node`), signed weather/recipes bundles, scheduled
  pulls of datasets + weather + recipes, `GET /datasets/{app}/{ds}/manifest` digest + ghost
  reconcile, `GET /mesh`, `pumper_mesh_*` metrics.
- `[transact] allow_live = true`: `submit: true` stages a `pending` transaction and parks the job
  (N02's `waiting`); `POST /transactions/{id}/approve` (admin scope) resumes it into
  `Browser::commit`, the only code path that submits; stale-evidence refusal; one idempotency key
  submits at most once. `[mcp] allow_approve` gates the agent-facing approval tool.
- `[plugins] allow_http_hosts = [...]`: core-module plugins with a declared capability manifest get
  `pumper_http_request` / `pumper_kv_*` imports (two-lock: manifest AND operator allow-list);
  `sink = "plugin:<name>"` on watches; `post_enqueue` trigger slot; `sink-postgrest` reference
  connector built and ABI-verified.

## Findings worth keeping

- **A real inventory-scanner bug** (N03): the work-creating-door scan matched `.enqueue_dedup(`
  but not `.enqueue_dedup_as(`, so the primary enqueue door vanished from the inventory the moment
  it was attributed. Fixed and strengthened.
- **`#[cfg(test)]` placement disables door inventories** (N15) — recorded as MEMORY.md invariant 7.
- **The timing test `e2e::fanout_offslot::a_slow_index_no_longer_holds_a_scrape_permit`** failed
  once on `master` during the post-N16 run while two builders were compiling (it demands a 2×
  ratio) and passed in isolation (2.26s). It is load-sensitive, not broken; candidate for the flake
  register if it recurs.

## Carry-forward seams (not built by any builder)

1. **Mesh wire format lives in `crates/apps/peer/src/{envelope,mesh}.rs`** and is consumed by the
   server (core was outside H's scope). Move to `crates/core/src/mesh.rs` unchanged; never
   re-implement server-side (MEMORY.md invariant 9).
2. **`Storage::upsert_managed_schedule(id, app, cron, params, enabled, tag)`** — N16 does its
   `managed_by='peer'` schedule upsert with raw `sqlx` in `scheduler.rs` (and moved `sqlx` from
   dev- to real dependency of the server; N10 needed the same). The storage-layer writer is the
   clean fix.
3. **`transaction.pending` / `transaction.submitted` webhooks** are not dispatched (stage and commit
   happen inside the app; `dispatch_event` is server-side) — same class as wave 1's
   `source.repair_promoted`. → N05 durable event log (wave 3, builder K).
4. **N10 fuel/latency benchmark not done** — bounds declared and enforced, never observed.
5. **N01 not exercised against real Chrome** — `Browser::commit`'s two-pass behaviour is proven
   only against the scripted engine; the Chrome tests stay `#[ignore]`.
6. **`[[peer]]` → schedules reconcile has no automated test** (needs a server boot with peer
   config); the pure plan has six.
7. **Doc-sync hook standalone** still exits 3 (cannot check) / blocks on stdin; docs were updated
   by hand in-commit throughout.

## Verification on master

After the fifth merge (N10), on `master`, each in its own invocation:

| gate | result |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo check --workspace` | exit 0 |
| `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| `cargo test --workspace` | exit 0, no FAILED/panicked |

Not run: `just ci` and `scripts/smoke.ps1` (owed before the campaign closes, with wave 1's).
