---
name: automation-api
type: perfect/context
group: HTTP API
category: api
opportunity: 6
last_proposed: 2026-08-12
cooldown_until: round-13
directions: ["[[enqueue-door-parity]]", "[[schedule-truth]]", "[[watch-honesty]]"]
alias_of_old_map: "[[http-api-routes]] (round-1 pass covered events/SSE; the rest of these routes post-date it)"
---

## Current state
Not yet scouted on the 46-map. Files: crates/server/src/routes/{schedules,triggers,
watches,events,ingress}.rs. events.rs + ingress.rs were hardened by round 10 (bounded
shutdown c9c2c68, error contract 0cfc366) and round 6 (replay defense f908903, trigger
ledger 5d99cc6). schedules.rs and watches.rs have never been swept on any map.

## Direction history
- (as http-api-routes r1: SSE resume/shutdown 5bdb7ae; via trigger-pipeline r6 + api-surface r10 for events/ingress.)
- 2026-08-12 (round 11, director-self-gated; very-thorough scout, key anchors Director-verified
  live): **ACCEPTED 3**: [[enqueue-door-parity]] (schedule/trigger doors bypass the params-schema
  422; scheduler replaces while HTTP merges) · [[schedule-truth]] (guard/health SQL divergence —
  bulk retry wedges firing while health says ok; last_run advances on misfire-skip) ·
  [[watch-honesty]] (virtual namespaces unwatchable while the fan-out looks for them; inverse
  dead watches accepted silently; unvalidated ?app=; no watch→deliveries path).
  **REJECTED-deferred (banked)**: trigger-ledger-completeness (eval_set_error rows unreachable —
  trigger_id "*" 404s; TRIGGER_OUTCOMES doc drift missing status_mismatch/eval_set_error with no
  pinning test; no ?outcome= filter) — real, lost the slot race to the three above.
  **REJECTED-deferred (banked, the context's next anchor)**: schedule-runs-ledger — the full
  decision ledger (trigger_runs sibling; scheduler has 5 log-only skip paths); L-sized,
  schedule-truth ships the predicate unification it needs first.
  **REJECTED-deferred (banked)**: automation /metrics series (no pumper_watches/pumper_triggers;
  schedule health states invisible to dashboards) — joins r10's banked metrics-hot-path anchor.
  Other recorded scout findings for future rounds: no ToSchema on any automation object (spec
  path-complete, shape-empty); schedules/watches zero HTTP e2e; no schedule PATCH (delete+
  recreate severs lineage); enabled-toggle lost updates; sinks dir outside retention; DELETE
  watch downgrades replays to unsigned; governance/cost-pause invisible on automation surfaces;
  duplicated URL validation; four list envelopes.

## Shipped
- (inherited via those contexts)
- 2026-08-12 (round 11) · [[enqueue-door-parity]] → `6c3f91a` — one shared params door
  (mcp::validate_app_params) behind all six work-creating doors, inventory-enforced
  (EXPECTED_VALIDATING_DOORS); schedules validate MERGED effective params (replace→merge
  resolved); trigger hops record bad_params; fire path skips legacy rows as
  health=invalid_params.
- 2026-08-12 (round 11) · [[schedule-truth]] → `85bb5f9`+`fb78574` — existential guard twin
  deleted; run_holds_slot(newest) backs guard AND health (retry-wedge dead, pinned by e2e);
  migration 0039 last_skipped_at/skipped_count; schedule_reference = MAX(last_run,
  last_skipped_at) shared by reconcile + project_next_run.
- 2026-08-12 (round 11) · [[watch-honesty]] → `5ee2462` — NamespaceIndex (registry + virtual
  seed + STORE + saved-search materialize); virtual namespaces watchable, traps refused with
  the landing namespace named; validated ?app= on watches (+ separate trigger_filter_values —
  ingress ids are not apps); GET /watches/{id}/deliveries; explicit-null last_delivery.
  NEW BANKED: `trades` virtual namespace is accepted but undeliverable — trades-common emits
  no index_datasets, so 5 apps' unified writes never enter the fan-out batch (fix lives in
  trades-common; S-sized). Also: VIRTUAL_NAMESPACES pins to the registry, not
  grants_common::UNIFIED_APP — a rename there fails no test.
