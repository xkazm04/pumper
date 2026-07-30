# Moonshot Batch 2 — Platform & Control Plane (2026-07-30)

> 8 commits, 6 M-ids / 5 backlog items, two sub-waves (2a: 3 parallel agents; 2b: 2 parallel agents).
> Baseline preserved: tests 488/0 → **545/0**. Drift-gate + migrations inventory green (0021–0024 consumed).

## Commits

| Commit | Item | Summary |
|---|---|---|
| `3a6b017` | M19 | Catalog GitOps reconciler — ReconcilePlan, managed_by tagging, boot drift log, auto_reconcile default-OFF, force-gate on mass-disable |
| `3c15407` | M21 | Inbound ingress — POST /ingest/{id}, HMAC (+GitHub x-hub-signature-256), token-bucket, `external` events → trigger DAGs with JSON-path predicates, redelivery idempotency |
| `c6efbd3` | M23 | Durable execution — CheckpointSink + ctx.checkpoint/restore, lineage-guarded writes, poison escape, two-phase drain = suspend not abandon; crawl ported (~70 LOC bespoke path deleted) |
| `4dbe7da` `6cc603e` | — | 2a integration (shared storage/config/routes) + checkpoint tests + batch-3 design |
| `c5bd830` | M29+M27 | MCP server at /mcp (hand-written streamable-HTTP JSON-RPC; rmcp skipped for API churn) on agent-ready manifests (schema-validated enqueue, self-verifying examples) |
| `f182b4f` | M04 | Information economist — job_yield capture, GET /economics ($/new-record, Claude worth-it, deterministic advice), advisory-only |

## Verification

- `cargo test --workspace` 545/0 (516 after 2a; one orchestrator fix-forward: `job_yield` added to migrations inventory — agent J missed the test H had extended for 0021-0023).
- All new surfaces default-OFF: `[catalog] auto_reconcile`, `[ingress] enabled`, `[mcp] enabled` + `allow_enqueue`, `[economics] enforce`.

## Incidents

- Agent G (M21) died mid-run on a transient API auth error ("Not logged in") right before its gate. SendMessage-resume recovered it with full context — **resume, don't redispatch**.

## Deferred / open seams

- M23: app_research checkpoint port.
- M29: EventBus → MCP notifications (needs SSE half); research-specific tools (fetch_readable, deep_research) as future /mcp extensions.
- M04: enforcement mode (scheduler reads planner budgets) — intentionally gated on accumulated live yield history.
- M19: drift-gate ↔ reconciler consolidation left as-is (both green, complementary).

## Activation notes (operator)

- MCP: set `[mcp] enabled=true`, add `/mcp` to a client's `.mcp.json` (snippet in docs/features/mcp.md). Enqueue via agents additionally needs `allow_enqueue=true`.
- Ingress: `[ingress] enabled=true`, create a source (POST /ingress/sources), point GitHub webhook at /ingest/{id} — x-hub-signature-256 works directly.
- Reconciler: POST /catalog/reconcile (dry-run via GET first). Existing hand-made schedules are never touched.
