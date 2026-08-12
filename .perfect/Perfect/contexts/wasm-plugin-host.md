---
name: wasm-plugin-host
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 5
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: crates/engine-wasm/src/lib.rs. wasmtime host with
CPU fuel + memory cap. Round 6 ACTIVATED trigger hooks through it (8adfc91: real-host
e2e, plugin_missing ledger outcome, honest InstancePre negative result kept as a
regression gate). Fail-open unknown-plugin path is documented behavior. Host hardening,
fuel calibration, plugin versioning/manifest all unswept.

## Direction history
- (round 6, via trigger-pipeline): activate-wasm-hooks shipped 8adfc91.

## Shipped
- (via trigger-pipeline r6 — 8adfc91)
