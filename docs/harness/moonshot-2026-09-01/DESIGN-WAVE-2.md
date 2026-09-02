# Wave 2 Design — Act, Orchestrate, Federate (2026-09-01)

Five builders off `master` after wave 1 is merged. Everything in [DESIGN-WAVE-1.md](DESIGN-WAVE-1.md)
§Shared rules and §Shared surfaces applies unchanged. Wave-1 seams these items build on: N20's
principal/scope/audit layer, N02's `waiting` state + `await_input`/`resume`, N09's component host +
`DynamicApp`. Read the wave-1 branches' commits (`git log master --since=2026-09-01`) before designing
against those seams — the merged code, not the card, is the contract.

## File-scope partition (HARD boundaries)

| Builder | Item | Owns | Must not touch |
| --- | --- | --- | --- |
| F | **N01** Transact v2 | `crates/core/src/engine.rs` (Transact* types + `Browser::transact` split into stage/commit), `crates/engine-browser/src/**` (transact paths only), `crates/apps/transact/**`, `crates/server/src/routes/transactions.rs` (new), `crates/server/src/mcp/mod.rs` (two tools, behind `[mcp] allow_approve`), migration, `docs/features/apps.md` §transact, `docs/features/mcp.md` | `worker.rs`, `triggers.rs`, `webhook.rs`, `engine-wasm` |
| G | **N03** Workflow runs | `crates/server/src/workflow.rs` (new), `crates/server/src/routes/workflows.rs` (new), `crates/server/src/worker.rs` (ONE hook call in `finalize_with_stages` beside `fire_terminal_triggers`, nothing else), `crates/core/src/storage.rs` (workflow tables + `EnqueueOptions.workflow_run_id/step` only), `crates/server/src/scheduler.rs` (`target: workflow` on a schedule), `crates/server/src/mcp/mod.rs` (`run_workflow`/`wait_workflow`), migration, `docs/features/workflows.md` (new) + map entry, `docs/features/triggers.md` cross-link | `triggers.rs` beyond calling `decide()`, `webhook.rs`, `events.rs`, `engine-*` |
| H | **N16** Pumper Mesh | `crates/server/src/node.rs` (new: identity), `crates/server/src/routes/host_weather.rs`, `crates/server/src/routes/recipes.rs` (export/import), `crates/server/src/routes/mesh.rs` (new), `crates/apps/peer/**`, `crates/core/src/config.rs` (`[[peer]]`), `crates/server/src/scheduler.rs` (peer pulls as ordinary schedules — additive), `crates/server/src/routes/datasets.rs` (`/manifest` digest route only), `docs/features/peering.md` → `mesh.md` rename + map entry, `docs/features/fetching.md` cross-links | `worker.rs`, `mcp/`, `engine-remote` (fabric is N17, rejected) |
| I | **N10** WASM sinks & connectors | `crates/engine-wasm/src/**` (host imports + capability manifest), `crates/core/src/plugin.rs` (capabilities on `describe()`), `crates/server/src/webhook.rs` (`plugin:<name>` sink branch), `crates/server/src/triggers.rs` (`post_enqueue` slot only), `crates/server/src/routes/plugins.rs` (capability listing), `plugins-src/sink-file-json/` or `sink-postgrest/` (one reference connector), `docs/features/trigger-plugins.md`, `docs/features/events-webhooks.md` | `worker.rs`, `registry.rs` (N09 owns), `mcp/` |
| J | **N15** Self-hosted agent loop | `crates/engine-claude/src/**` (`--mcp-config`, tool allow-list), `crates/core/src/config.rs` (`[claude] self_hosted_tools`), `crates/core/src/fetcher.rs` (tier-3 prompt text only), `crates/server/src/mcp/mod.rs` (a `fetch` tool scoped by a job token — additive; coordinate with F/G who also add tools: append yours at the END of the tool table), `crates/server/src/mcp/jobtoken.rs` (new), `docs/features/mcp.md`, `docs/features/fetching.md` §tier 3 | `worker.rs`, `engine-browser`, `apps/research` beyond reading |

`crates/server/src/mcp/mod.rs` is touched by F, G and J: each appends its tools at the end of the
tool list and its match arms at the end of the dispatch; no reordering, no shared helpers edited.

## Item specs (v1 slices — do NOT exceed)

### F — N01 Transact v2 (XL, irreversible) — cards CP2 / CR2 / SE6

**v1 slice.** `transactions` table (id, idempotency_key UNIQUE, app, job_id, profile, state
`pending|approved|submitted|rejected|expired`, evidence_sha, approved_by (principal id from N20),
approved_at, submitted_at, receipt_path). State machine as a pure function with tests
(`approved_with_stale_evidence_not_submitted`, `duplicate_key_not_resubmitted`, `expired_not_approvable`).
Engine: `Browser::transact` becomes `stage` (today's behaviour + returns a `stage_id` and the DOM hash
of `submit_target` + `filled_fields`) and `commit(stage_id, evidence_sha)` which re-runs the flow to
the confirmation state, refuses if the live DOM hash differs from the reviewed evidence, performs
`submit_action`, waits for `confirm_selector`, captures the post-submit DOM as an artifact, and returns
a receipt. App: `submit: true` no longer 422s — the dry-run job writes a `pending` row and the result
carries `transaction_id`; the approve path uses **N02's `waiting` state**: the job parks with the
evidence bundle as `input_request`, `POST /transactions/{id}/approve` (admin scope under N20) resumes it
with a one-shot approval token as the input, and the resumed run commits. `POST /transactions/{id}/reject`,
`GET /transactions?state=`, `GET /transactions/{id}`. `[transact] allow_live = false` default (approve
returns 409 when off). Per-profile daily cap. `transaction.pending|submitted` webhooks via
`dispatch_event`. MCP: `list_pending_transactions`, `approve_transaction` behind `[mcp] allow_approve = false`.
**Out of v1:** WASM policy predicates for auto-approval, screenshots, session-handle resume (v1
rebuilds deterministically; say so in the doc).
**Gate to prove:** an e2e with the scripted browser: dry-run → pending → approve → commit exactly
once → second approve on the same key is a no-op; stale-evidence refusal.

### G — N03 Workflow runs (XL, contract) — cards JO4 / EP2 / HA6

**v1 slice.** Tables `workflow_defs(id, name, spec_json)`, `workflow_runs(id, def_id, status,
budget_usd, spent_usd, started_at, finished_at, root_id)`, `workflow_steps(run_id, step, job_id,
depends_on JSON, status)`. Spec: `{steps: {name: {app, params (template `{{steps.X.result.path}}`),
after: {all_of: [...]}, budget_usd?, priority?}}, budget_usd?, on_failure: fail_fast|continue}`;
validated at create per step through `validate_app_params` (422 with pointer paths). Pure core:
`ready_steps(spec, states) -> Vec<step>` and `render_params(template, results) -> Result` with tests
(join, diamond, fail-fast cascade, template miss refused, join fires exactly once). Worker hook:
`workflow::on_step_terminal(job)` beside `fire_terminal_triggers` in `finalize_with_stages`,
idempotent per (run, step). Every step job carries `workflow_run_id`, `step`, and `root_id`
(= run id) in `EnqueueOptions`; envelope budget enforced at each step's enqueue (`sum(step budgets)
<= run budget`). Routes: `POST /workflows`, `POST /workflows/{id}/runs` (idempotency key),
`GET /workflows/{id}/runs`, `GET /workflow-runs/{run_id}` (step matrix + rolled-up receipt: cost from
`cost_events` by job set, yield from `job_yield`), `DELETE /workflow-runs/{run_id}` (cancels open steps
through the existing cancel door). Schedules may target a workflow. MCP `run_workflow`/`wait_workflow`.
`workflow.*` events on the bus. DataHub: out of v1 (N24 wave 3 may add the instance).
**Out of v1:** `any_of`, `foreach`, retry-from-step, catalog `[[workflow]]`, SLA/deadline sweeper.
**Gate to prove:** e2e of a 3-step diamond (two sources, one join): join runs exactly once, receipt
sums both upstream costs, cancel mid-run leaves no orphan step.

### H — N16 Pumper Mesh (XL, contract) — card HA3

**v1 slice.** Node identity: ed25519 keypair generated at first boot under `data/node.key`,
`GET /node` returns the public key + fingerprint; `node_id` becomes the fingerprint (keep the old
hash as `legacy_id` for one release). Signed envelopes `{schema, node_id, generated_at, payload, sig}`
for weather bundles (schema `pumper.host-weather/2`, import accepts /1 unsigned only when the peer
row says `allow_unsigned: true`) and a new recipes bundle (`GET /recipes/export`, `POST /recipes/import`
dry-run by default, same shape as weather). `[[peer]] url, public_key, pull = ["datasets:<app>/<ds>",
"weather", "recipes"], every = "15m", allow_unsigned = false, max_penalty_secs = 60` — the scheduler
reconciles `[[peer]]` rows into ordinary schedules (`peer` app for datasets; new tiny `mesh-pull` job
kind or the `peer` app with a `stream` param for weather/recipes — pick one and say why). Reconcile:
`GET /datasets/{app}/{ds}/manifest` = live key count + rolling hash over keys; the peer app compares
and, on mismatch, runs a bounded key diff and tombstones ghosts (`ghosts_removed` in the report).
`GET /mesh` status (peers, last pull per stream, lag, signature failures); `pumper_mesh_*` series
emitted at zero. Peer pulls carry a scope-limited principal from N20 when `[auth] mode = keys`.
**Out of v1:** push federation (N05 wave 3), artifact mirroring (N19 rejected), per-peer namespace
allow-lists beyond `pull`.
**Gate to prove:** extend `crates/server/src/e2e/peer_mirror.rs` (or its sibling): a forged bundle is
refused, a hard delete on the origin is reconciled on the mirror.

### I — N10 WASM sinks & connectors (XL, policy) — card EP3

**v1 slice.** `describe()` gains `capabilities: {http: {hosts: [...], methods: [...]}, kv: bool}`;
the loader records them, `GET /plugins` shows them, and a module importing a host function it did not
declare fails to link (`hook_not_executable`). Host imports on the wasmtime `Linker`:
`pumper_http_request(ptr,len) -> u64` (JSON `{method,url,headers,body}` in, `{status,body}` out),
bounded by the manifest allow-list AND `[plugins] allow_http_hosts`, routed through the metered
`Fetcher` chokepoint so governor/host profiles/cost ledger see it; `pumper_kv_get/put` scoped to a
per-plugin namespace table. Sink: `plugin:<name>` branch in `webhook.rs::deliver` → `plugins.run(name,
envelope{delivery_id,event,body}, params)`; output `{delivered: bool, permanent?: bool, error?: string}`
mapped onto the existing tuple so retries/DLQ/replay are unchanged. Trigger slot `post_enqueue`
(after `enqueue_dedup` succeeds, gets the job id; failures are ledgered, never gate the hop). One
reference connector under `plugins-src/` built and verified by the same CI path as the trigger plugins.
Fail closed on any undeclared import. Coordinate with N09's host: if the component host landed, put the
imports on the SAME linker abstraction; do not fork a second host.
**Out of v1:** OAuth/secret injection into plugins, streaming bodies, a marketplace/registry.
**Gate to prove:** a unit test that an undeclared import fails to link; a sink e2e where a
`plugin:` sink reports `delivered=false, permanent=true` and lands in the DLQ like any other sink;
a fuel/latency benchmark note (one HTTP-calling plugin under the admission gate) in the doc.

### J — N15 Self-hosted agent loop (XL, policy) — card SE2

**v1 slice.** `[claude] self_hosted_tools = false` default. When on: the Claude subprocess is launched
with `--mcp-config` pointing at pumper's own `/mcp` and an allow-list that replaces the CLI's
`WebFetch`/`WebSearch` with pumper's `fetch` tool; a **job token** (random, single-job, in-memory,
expires with the job) is passed in the MCP config and required by the new `fetch` tool, which runs
through the job's own metered `AppContext::fetch` (governor, cache, profile, archive, VCR, cost ledger
all apply and spend lands on the job). Tier-3 prompt text in `fetcher.rs` tells the model to use
`fetch`. `wait_job`/`enqueue_job` unaffected. `GET /jobs/{id}/receipt` shows `self_hosted_fetches`.
**Out of v1:** `search` and `query_dataset` through the loop, tokens for non-Claude agents, per-call
budget beyond the job's.
**Gate to prove:** with the scripted researcher: a job under `self_hosted_tools = true` gets its MCP
config file written with the token; the `fetch` tool refuses a missing/expired token (401 via the
error map); a fetch through the tool is metered on the job's ledger.

## Carry-forward from wave 1 (assigned; see FIXES-WAVE-1.md §Carry-forward)

- **G (N03)** also lands: `routes/jobs.rs::enqueue_job` reads the `CallerPrincipal` extension and
  calls `Storage::enqueue_dedup_as` so `jobs.principal_id` / `cost_events.principal_id` are stamped;
  workflow step enqueues carry the run's principal. Add `principal=` to `GET /costs` and a
  `by_principal` block on `/economics` (core `summary_by_principal` exists). G's scope gains
  `routes/jobs.rs` and `routes/economics.rs`.
- **J (N15)** also lands: `AppContext::fetch`'s router consultation in `crates/core/src/app.rs`
  honours a learned `api_recipe` pin (today it branches only on `browser`). J's scope gains that
  one function.
- **Migrations**: master is at **0045**. Take 0046+ and expect the coordinator to renumber on
  collision — four of five wave-1 builders took 0041.
- **Config sections**: append at the very end of `Config` and of the file, after `WasmAppsConfig`
  / `RepairConfig`; the wave-1 merges conflicted in `config.rs` every time and were resolved by
  keeping both sides.

## Merge order (coordinator)

J (N15) → I (N10) → H (N16) → G (N03) → F (N01). `cargo check --workspace` after each; `just ci`
after all. MCP tool-table conflicts are resolved by the coordinator by keeping all three appends.
