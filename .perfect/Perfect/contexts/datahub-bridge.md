---
name: datahub-bridge
type: perfect/context
group: Event Pipeline
category: lib
opportunity: 6
last_proposed: 2026-08-04
cooldown_until: 2026-08 +2 rounds
directions: ["[[datahub-governance-preview]]", "[[datahub-governance-reversible]]", "[[datahub-emission-lifecycle]]", "[[datahub-poll-mechanics]]", "[[datahub-config-honesty]]"]
---

## Current state (scouted 2026-08-04, HEAD 8adfc91)

One file: `crates/server/src/datahub.rs` (1317 lines, 20 pure-fn tests). Outbound: entity
batches (25) via openapi endpoint, lineage read-merge, M25 flows/jobs topology. Inbound
(M26 governance, opt-in `govern=false`): deprecation → disable app's catalog-managed
schedules; `cost:pause` tag → budget forced $0; failing assertions → sync job enqueue
(hour-bucketed idempotency).

**Top findings:**
1. **Governance acts with no preview, no durable audit, no bus event** — actions live only
   in memory (`GovernState.last`), lost on restart; first poll after `govern=true` acts
   immediately. (Contrast: catalog reconcile and resilience both have preview surfaces.)
2. **Re-enable fights the poll**: operator re-enables a schedule → next poll (≤300s)
   re-disables while the remote flag stands; no override. Blast radius: ONE deprecated
   dataset disables ALL the app's catalog schedules; one cost:pause tag zeroes every job's
   budget. Pause set FREEZES when DataHub is down (abort-before-recompute) — an app paused
   pre-outage stays $0 indefinitely; "fail-open" only holds across restart.
3. **Detached emission spawn**: bare tokio::spawn, no shutdown token/JoinHandle/overlap
   guard, no retry; single-slot `datahub_last` status — a success overwrites a failure
   seconds later. full_sync fully serial + re-entrant (documented race).
4. **Poll mechanics**: one serial GraphQL per dataset (60s client — worst case 20min/poll);
   `last_poll` stamped at tick START → overlapping polls race on paused_apps.
5. **Shipped config.toml has `enabled = true`** vs localhost:8080 — default checkout spawns
   an emission (with DB reads) after EVERY successful job and warns each time.
6. **Zero non-pure tests**: no HTTP mock anywhere; every line that writes external state
   (post_entities, govern_poll, full_sync, effective_budget) untested. No datahub step in
   smoke.
7. Docs: datahub.md pre-M25/M26 — the ENTIRE governance actuator undocumented in features;
   known-gap "trigger edges not emitted" now false.
8. Dead ends: fineGrainedLineages with upstreamType NONE renders no edge (annotation only);
   flows/trigger_edges counters returned but unread; governance never publishes to the bus.

## Direction history
- 2026-08-04 (round 7): presented 5, **accepted 5/5 clean sweep** — governance-preview
  (the enforcement-preview idiom, third acceptance of that pattern), governance-reversible,
  emission-lifecycle, poll-mechanics, config-honesty.

## Shipped
- [[datahub-emission-lifecycle]] → `27c5131` — tracked emissions, 409 sync guard,
  failure-visible status + counters, MockGms harness.
- [[datahub-poll-mechanics]] → `89a9b37` — bounded poll (4×10s), completion-gated,
  overlap impossible.
- [[datahub-config-honesty]] → `175ce65` — shipped OFF; actuator + blast radius documented.
- [[datahub-governance-preview]] → `b2a2e76` — preview + 0037 audit trail + bus events.
- [[datahub-governance-reversible]] → `93997ed` — transition semantics (0038 level memory),
  re-enable respected, blind pauses expire loudly.
