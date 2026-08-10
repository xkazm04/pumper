---
slug: datahub-governance-preview
type: perfect/direction
context: "[[datahub-bridge]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: b2a2e76
---

## What & why
Governance acts immediately and invisibly: first poll after `govern=true` disables
schedules with no dry-run (catalog reconcile and resilience both have preview surfaces;
governance has none), and actions live only in `GovernState.last` in memory — restart and
the record of why a schedule is off is gone. Nothing reaches the event bus.

## Evidence
- `datahub.rs:884-906` plan_govern_actions; `:939-941` in-memory last; `:1007-1030` disable
- No preview route; no `state.events` use anywhere in datahub.rs
- Pattern precedent: GET /enforcement/preview (round 5), GET /catalog/reconcile

## Acceptance criteria
- `GET /datahub/governance/preview`: what current remote state WOULD disable/pause/enqueue;
  gates nothing; works with govern=false (that's the point).
- Every executed action durably recorded: event-bus event + persisted trail (table or
  reuse an existing ledger idiom) carrying the remote evidence (deprecated flag / tag /
  assertion state) that justified it.
- Mock-HTTP tests (first non-pure tests in the module) over preview + the actuating path.
- OpenAPI/inventory green; datahub docs updated (coordinates with
  [[datahub-config-honesty]]).

## Risks / non-goals
- Non-goal: changing what governance DOES (that's [[datahub-governance-reversible]]).

## Build record
- Builder DH2 (opus), wave 2, verdict merge (pick pending P2 gate). `6ec3e8a`:
  GET /datahub/governance/preview (exact schedule ids + real idempotency keys +
  pause/resume sets; `poll_would_abort` collects read errors so a blind preview never
  reads as quiet; enforcement-preview idiom; `just datahub-preview` runner). Audit:
  migration 0037 `datahub_govern_actions` + `datahub_govern` JobEvent on the bus; 90-day
  retention pruned hourly from the poll ITSELF (worker.rs stayed out of scope — prune only
  grows while governance runs, which is more honest). Extracted disable_targets /
  govern_sync_key / audit_prune_due / read_govern_metas.
- Gates: worktree 1143/0 (at direction-2 commit).
