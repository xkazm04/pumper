---
name: plugin-runner
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 4
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: crates/apps/plugin/src/lib.rs, crates/apps/plugin/
src/observatory.rs. The WASM-plugin-driven scrape app. Round 9's chokepoint metered its
fetches (6237cc8); host-side activated r6. Observatory sub-surface never swept.

## Banked (r14, 2026-08-12 — anchor for this context's proposal pass)
The run door lies green: `run()` checks only that the `plugin` param is a string
(plugin/lib.rs:391, same at :673, :818) — no `ctx.plugins.has()`/list() validation —
so a typo'd plugin name yields an `{"error": "unknown plugin 'x'"}` record per URL,
`ran: 0`, and a SUCCEEDED job. Observatory mode DOES validate (observatory.rs:246-249)
— the asymmetry is the fix's shape. Also: errors swallowed into `{"error":…}` records
while engine-wasm/src/lib.rs:26-27 claims "extraction propagates the error" (doc-lie
half fixed by r14 wasm docs work if it lands; the door is this context's own).
Verified live 2026-08-12 by the r14 wasm verification scout.

## Direction history
- (round 9, via app-runtime): fetch chokepoint covered its call sites.
- r14 (2026-08-12): plugin-app run-door validation REJECTED-deferred at the
  wasm-plugin-host gate — it is THIS context's door; banked above as the anchor.

## Shipped
- (via app-runtime r9)
