---
date: 2026-07-26
mode: scan
theme: testing-strategy
sub_agents_spawned: 4
findings_total: 8
findings_weak: 4
findings_strong: 4
executed: [1, 2, 3, 4]
codified: [5, 6, 7, 8]
queued_followups: 3
dropped: []
adrs_written: ["2026-07-26-server-test-seam", "2026-07-26-shared-test-harness", "2026-07-26-prose-only-bug-class-guards", "2026-07-26-ci-and-honest-test-gates", "2026-07-26-codify-testing-patterns"]
commits: [60b8650, c9bf2fd, 8885c96, 8c83f14, ed8674f, be7371b, a217596, 38f62f8, f7c7287]
branch: "(committed to master)"
---

# Architect scan — testing-strategy (2026-07-26)

First /architect run in pumper (skill adopted from personas this session).

## Sub-agent reports (summary)
- **Inventory/layer map** (smell 3/5): ~336 tests, 75% pure-unit; excellent storage suite;
  ZERO HTTP-layer and ZERO worker-loop tests; no doc/prop/snapshot/bench tests anywhere.
- **Fixture duplication** (smell 4/5): no shared test-support of any kind; fresh_db ×11,
  Dead-engine trios ×4, AppContext ×3; 41 leak-prone teardowns; AppState::init untestable monolith.
- **Regression guards** (smell 3/5): 15-class catalog → 8 guarded / 1 partial / 6 prose-only;
  guard coverage correlates ~perfectly with whether the fix was an extracted pure fn.
- **E2e gaps** (smell 4/5): composition layer untested at every joint; NO CI; one AppState seam
  unlocks 4 of the 5 highest-value missing tests.

## Findings → outcomes
1. **Server test seam** — executed → from_parts + run_one + reconcile(now) + 8-test e2e suite (38f62f8, f7c7287)
2. **Shared harness** — executed → pumper_core::testing, −193 net lines, tests pass unchanged (ed8674f, be7371b)
3. **Prose-only guards** — Tier-1 executed (8885c96) + HMAC contract in the e2e suite; tail queued
4. **CI + honest gates** — executed → 7 tests #[ignore]-gated, .github/workflows/ci.yml (60b8650, c9bf2fd)
5–8. **Strong patterns** — all codified (8c83f14) → harness-learnings + CLAUDE.md

## Conflict noted
harness-learnings called the dispatch_event / keyset_cursor conventions "structural" while
nothing enforced them — "structural" meant "true today". Both now have tests; the honest
meaning is codified in the inventory-as-test convention.

## Cross-references
- Committed the dormant concurrent session's datahub/routes-split/SDK work (a217596) to
  restore tree coherence — see Lessons/2026-07-26-architect for the entanglement rule.
