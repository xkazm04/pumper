---
slug: mpsv-feed-drift-honesty
type: perfect/direction
context: "[[czech-labor-market]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 19f37e2
---
## What & why
Two confirmed-unfixed bughunt findings (2026-07-14) plus the structural reason
they survived: (1) a renamed/missing `polozky` key parses to zero rows and
reports a CLEAN success (`stored: 0`) with nothing tombstoned — schema drift on
a ~300k-posting national feed is doubly invisible; (2) `region_agg`'s "true
regional salary distribution" silently drops every posting lacking a CZ-ISCO
code because the region roll-up sits AFTER the czisco early-continue — the
regional numbers are biased and nobody knows. And (3) run() has ZERO test
coverage in both apps (~49 tests, all pure helpers), which is exactly why both
bugs are structurally invisible to CI.

## Evidence
- Drift-to-silent-empty: mpsv-ispv/src/lib.rs:92-96
  (`get("polozky").and_then(as_array).unwrap_or_default()`), mpsv-vpm:1918-1995
  (`#[serde(default)] polozky`). Bughunt czech-labour-market-mpsv.md #3.
- region_agg bias: mpsv-vpm:411-414 czisco `continue` gates :432-451 region
  roll-up which needs no occupation code. Bughunt #2, confirmed unfixed.
- No run() coverage: all tests are pure-fn (:2239-3055); no mpsv file under
  crates/server/src/e2e/. Core fake-engine harness exists (core testing seam,
  r10) — verify suitability from app-crate tests.
- upsert_many (not sync_many) everywhere → a drifted empty run also never
  tombstones — double invisibility.

## Acceptance criteria
1. Missing/renamed `polozky` distinguished from present-but-empty in BOTH apps:
   drift → loud failure or an explicit drift flag + zero-write; never a clean
   `stored: 0` success. Extracted named predicate + `x_not_y` test.
2. Suspicious-empty guard: a feed that normally carries ~hundreds of thousands
   of postings arriving 0/near-0 does not silently republish aggregates —
   builder picks the mechanism (floor const, prior-run ratio, refuse-and-note)
   with reasoning; test.
3. region_agg counts ALL postings that have a region, including czisco-less
   ones: roll-up moved ahead of the gate (extracted function), delta proven by
   test; bughunt #2 closed by name.
4. First run()-level tests for both apps: a stubbed HTTP engine drives run()
   end-to-end (happy path + drift case). Use the existing core testing harness
   if usable from app crates; if a new fixture is needed in core, REPORT it
   (core is out of write set), don't fork a parallel harness.
5. The fix must not break legitimate small feeds (ISPV quarterly file is small
   — the floor must be per-app/per-feed, not global).

## Risks / non-goals
- No sync_many/tombstoning migration this round (bank the shape if confirmed
  worth it).
- ARES checkpoint logic untouched.

## Build record
(to fill during build)
