---
name: wasm-plugin-examples
type: perfect/context
group: Event Pipeline
category: lib
opportunity: 3
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: plugins-src/{title-extractor,delta-slim,
trigger-gate,busyloop}/src/lib.rs. Example/production WASM plugins; trigger-gate +
delta-slim are the two `just plugins-install` installs (r6 activation). busyloop is a
fuel-cap test fixture. Mostly verdict-shaped.

## Direction history
- (round 6, via trigger-pipeline): activation covered trigger-gate/delta-slim.
- 2026-08-12 (round 11): scouted (medium); candidates exist — banked, not slated (cap). NOT
  covered yet. Anchors:
  1. **shipped-plugins-verified** (S/M): the two PRODUCTION plugins (trigger-gate, delta-slim)
     are never compiled or exercised by CI — plugins-src crates are workspace-detached, CI has
     no wasm32 target, and all 4 tests that touch a real artifact are #[ignore]d. A build break
     surfaces as a silent production behavior change (fail-open unknown-plugin path). Fix: CI
     wasm32 + plugins build + un-ignore against built artifacts.
  2. **trigger-gate-honest-across-source-kinds** (S): reads delta.count unwrap_or(0) vs
     min_count default 1 — on job/external triggers (no count field) it silently vetoes EVERY
     hop with a well-formed pass:false, worse than a crash (fail-open never engages); docs say
     "attachable to any source_kind". Needs (1)'s harness to be provable.
  3. (riders) title-extractor's describe() omits kind → invisible to GET /plugins?kind=
     filters and teaches the omission; README documents only the legacy ABI (no extract_v2/
     describe); busyloop is a doc prop no test consumes.
  ABI itself verified compatible (extract_v2 preferred, fallback works).

## Shipped
- (via trigger-pipeline r6)
