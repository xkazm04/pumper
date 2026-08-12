---
name: engine-contracts
type: perfect/context
group: Core Platform
category: lib
opportunity: 5
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: crates/core/src/{engine,plugin,search,config,error,
lru,jitter,lib}.rs — the trait/config/error substrate everything plugs into. Rounds 9–10
reworked big pieces incidentally (fetch chokepoint in engine.rs 6237cc8, Error::Transact
terminality 8e17ac7-adjacent 8e17ca7, prelude 684d2c7, error-code contract 0cfc366).
Remaining headroom: trait ergonomics, config validation/reporting, error taxonomy
completeness — but much was just harvested via other contexts.

## Direction history
- (rounds 9–10, incidental): chokepoint, terminal errors, prelude, error contract.

## Shipped
- (via app-runtime r9 / api-surface + browser-transact r10)
