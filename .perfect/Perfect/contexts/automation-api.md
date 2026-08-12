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
