---
name: http-engine
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 4
last_proposed: never
cooldown_until: —
directions: []
alias_of_old_map: "[[fetch-engines]] (round-3 pass covered this file)"
---

## Current state
Not yet scouted on the 46-map. Files: crates/engine-http/src/lib.rs. Round 3 shipped
body cap + timeout + Retry-After retries (709e84b) and proxy support (9d2044f) here;
fronted by cache + governor which round 9 bounded. Recently harvested.

## Direction history
- (as fetch-engines, round 3): 5/5 shipped — see [[fetch-engines]].

## Shipped
- (inherited — see [[fetch-engines]])
