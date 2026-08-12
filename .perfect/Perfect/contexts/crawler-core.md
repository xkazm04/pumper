---
name: crawler-core
type: perfect/context
group: Core Platform
category: lib
opportunity: 5
last_proposed: never
cooldown_until: —
directions: []
alias_of_old_map: "[[broad-crawler]] (round-2 pass covered these files)"
---

## Current state
Not yet scouted on the 46-map. Files: crates/core/src/crawl.rs, crates/core/src/simhash.rs.
Round 2 shipped banded SimHash + versioned checkpoint (4b085c3); round 4 generalized the
banded dup index (51ce092, 27x). Frontier policy, checkpoint evolution, and simhash
coupling to resilience (stored-simhash invalidation, see resilience/mod.rs:306) unswept
since.

## Direction history
- (as broad-crawler, round 2): 5/5 shipped — see [[broad-crawler]].

## Shipped
- (inherited — see [[broad-crawler]])
