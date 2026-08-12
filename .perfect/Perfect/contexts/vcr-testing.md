---
name: vcr-testing
type: perfect/context
group: Core Platform
category: lib
opportunity: 4
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: crates/core/src/vcr.rs, crates/core/src/testing.rs.
Round 9 shipped VCR attempt integrity (4e3647a — cassettes survive retries via the
checkpoint-coupled Fresh/Resume rule); round 10 extended testing.rs for transact types.
Remaining known gap from the round-4 queue: no worker-level round-trip determinism test.

## Direction history
- (round 9, via app-runtime): vcr-attempt-integrity shipped 4e3647a.

## Shipped
- (via app-runtime r9)
