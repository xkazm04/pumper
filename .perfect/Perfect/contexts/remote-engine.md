---
name: remote-engine
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 4
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: crates/engine-remote/src/lib.rs. Remote fetch
tier added after round 3 (no inherited history). Delegation contract, auth posture,
and failure semantics unswept; routes/remote.rs (job-search-api context) is its API face.

## Direction history
- 2026-08-12 (round 11): scouted (medium); candidate directions EXIST — banked, not slated
  (cap). NOT covered yet. Anchors, strongest first:
  1. **profile-fabric-safety** (wrong-data class): profile-scoped fetches serialized to a peer
     that lacks the profile → engine-http creates the jar EMPTY (warn only), peer returns 200
     logged-out content, coordinator stores it as real records. The wire test PINS the leak as
     correct (lib.rs:309-341 asserts profile round-trips). Fix: keep profile fetches local, or
     /fetch-proxy refuses unknown profiles so the coordinator falls back.
  2. **failover-before-local**: one node per fetch then straight to local (lib.rs:177-189) —
     a dead peer sends a deterministic 1/N of traffic out the coordinator's own (blocked) IP
     and escalates the ladder to browser/Claude. Round-robin test never exercises a failing
     node. Per-node cooldown + try-remaining-peers.
  3. (lesser) [remote] max_body_bytes only sizes the transport cap; per-node config drift =
     per-node body limits, double-fetch cost.
  Related job-search-api anchors (route side): no target-URL constraint, no audit trail of
  proxied work, single shared secret for all peers.

## Shipped
- (none on this map)
