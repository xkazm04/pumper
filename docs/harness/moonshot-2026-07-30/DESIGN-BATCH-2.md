# Batch 2 Design — Platform & Control Plane (2026-07-30)

> Branch: `vibeman/moonshot-exec-2026-07-30` (after Batch 1 commits). Items: M29+M27 (MCP program), M19 (GitOps reconciler), M21 (inbound ingress), M23 (durable execution), M04 (information economist).
> All five touch `crates/server` — run as **two sub-waves** to keep concurrent same-file edits rare. Agents DO NOT run git; orchestrator gates + commits per item. Shared rules = DESIGN-BATCH-1.md §Shared rules (read them).

## Sub-wave 2a (3 parallel agents)

### F — M19 Catalog GitOps reconciler
Follow server-api.md §Catalog-1 Path. Owns: `crates/core/src/catalog.rs`, `crates/server/src/scheduler.rs` (reconcile section), catalog route handlers (wherever /catalog/* lives), `catalog/README.md`, `config.toml` `[catalog]`.
- `ReconcilePlan { create, update, disable, orphan }` from diffing catalog live-rows vs `list_schedules()`.
- `GET /catalog/reconcile` = dry-run plan; `POST /catalog/reconcile` applies. Applied schedules tagged `managed_by="catalog"` (schema: reuse an existing schedules metadata column if one exists, else migration `0022_managed_by.sql`). NEVER touch schedules without the tag (hand-made stay sacred).
- Boot: dry-run + loud drift log; `[catalog] auto_reconcile=false` default-OFF flips to apply.
- Keep the drift-gate test GREEN (it may become "plan is empty" — only if that keeps both-direction coverage).

### G — M21 Inbound event ingress
Follow server-api.md §Events-1 Path. Owns: `crates/server/src/events.rs` (additive event kind), `webhook.rs` (factor/rename sign helper additively), NEW `routes/ingress.rs` (+ its registration line), storage ingress methods + migration `0020_ingress.sql`, trigger matching extension (`triggers.rs` `on: external` + source filter + JSON-path predicates via the existing filter parser).
- `IngressSource {id, name, secret, enabled}` CRUD; `POST /ingest/{id}` verifies `x-pumper-signature` (inverted `sign()`), body size-cap, rate-limit per source (simple token bucket in state is fine), emits `external` event onto the EventBus (visible in replay ring for free).
- Default-OFF: `[ingress] enabled=false`. First non-localhost write surface — say so in docs.
- Storage edits: append your methods at the END of the storage impl with a `// ── ingress ──` marker (agent H is also editing storage.rs — do not reformat anything).

### H — M23 Durable execution
Follow server-api.md §Worker-1 Path. Owns: `crates/server/src/worker.rs`, `progress.rs`, `crates/core/src/app.rs` (AppContext checkpoint/restore ONLY — agent in sub-wave 2b will touch a different section later, but in THIS wave you are the only app.rs editor), storage checkpoint methods + migration `0021_checkpoints.sql`, `crates/apps/crawl/**` (port its bespoke checkpoint to the new seam).
- `checkpoints` table keyed job_id (blob, attempt, updated_at, size-capped); writes guarded by the attempts-lineage rule (mirror `complete(job.id, job.attempts, ..)`).
- `ctx.checkpoint(state_json)` throttled like progress.rs; `ctx.restore()` populated on claim; cleared on complete. Poisoned-checkpoint escape: after `max_resume_failures` (default 3) start fresh.
- `drain()`: signal cooperative suspend via the existing cancel token before deadline → checkpoint instead of abandon.
- Port app_crawl to the seam (delete its private path — proves the API). app_research port DEFERRED to a follow-up (say so in reply).
- Storage edits: append at END of impl with `// ── checkpoints ──` marker.

## Sub-wave 2b (2 parallel agents, after 2a is committed)

### I — M29+M27 MCP server + agent-ready registry
Follow server-api.md §HTTP-1 and §Registry-1 Paths (M27 substrate first, then M29 serves it). Owns: `crates/core/src/app.rs` (AppManifest + `fn manifest()` default impl), `crates/server/src/registry.rs`, NEW `crates/server/src/mcp/` module, `main.rs` mount, jobs route enqueue validation, server `Cargo.toml` (+ rmcp, jsonschema — pin versions that exist; if rmcp's streamable-HTTP API differs from the sketch, adapt and note it), root Cargo.toml workspace-deps if needed.
- M27: `AppManifest { params_schema, examples, output_shape, cost_class, }` with derive-friendly default (name + default_params) so all apps compile; rich manifests for extractor, crawl, research, grants-gov, plugin; enqueue validates params against schema → 422 with pointer paths; `GET /apps?format=tools` emits MCP tool-definition JSON; test: every manifest example passes its own schema.
- M29: mount `/mcp` (streamable HTTP) on the existing Router; v1 tools: `list_apps`, `enqueue_job` (BEHIND `[mcp] allow_enqueue=false` default-OFF), `query_dataset` (reuse parse_filters), `search`; resources: catalog + manifests; EventBus replay-ring bridge → MCP notifications (if rmcp's subscription support is immature, ship tools+resources and leave notifications as a documented seam — do not fight the crate).
- `[mcp] enabled=false` default-OFF. Ship a `.mcp.json` snippet in docs/.
- Budget rail: enqueue tool enforces a per-session budget cap param.

### J — M04 Information economist
Follow runtime-core.md §App&Job-2 Path. Owns: `crates/core/src/costs.rs` (additive), worker yield capture (`worker.rs` — coordinate: sub-wave 2a's H owns worker.rs, you run AFTER it lands), storage `job_yield` methods + migration `0023_job_yield.sql`, NEW `routes/economics.rs`, scheduler advisory reads.
- Step 1-3 of the Path only (persist yield; `GET /economics` $/new-record etc.; ADVISORY planner report). Enforcement mode (step 4) DEFERRED — needs live yield history; leave the seam + config stub `[economics] enforce=false`.
- Claude-tier worth-it score from TierTrace cost vs records produced (step 5) — include in /economics.
- Per-app value weights: `[economics] weights` config map, default 1.0.

## Orchestrator protocol
Dispatch F,G,H parallel → gate+commit each → dispatch I,J parallel → gate+commit → full test sweep + FIXES-BATCH-2.md + ledgers.
Migration numbers reserved: 0020 ingress (G), 0021 checkpoints (H), 0022 managed_by (F, only if needed), 0023 job_yield (J). If master has since consumed a number, renumber upward and note it.
