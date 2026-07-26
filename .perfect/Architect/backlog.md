# Architect Backlog

Durable queue of architectural decisions. Sorted by (reach × payoff) / (risk × effort).
Status values: `proposed | approved | in-progress | shipped | abandoned | blocked`.

## Pending
- **[2026-07-26] Tier-2/3 bug-class guards — the tail** — type: structural-bug-class, risk: 1, effort: m, payoff: 3, reach: 3 classes
  Remaining from [[Architect/decisions/2026-07-26-prose-only-bug-class-guards]]: (i) raw-engine
  metering test (the crawl app's `ctx.meter`/`learn_tier` lines are deletable with a green suite);
  (f) `sync_many` allowlist inventory test (EXPECTED-diff style over app sources);
  (a) extract + test the remaining silent-success predicates (8 apps still inline them).
  Status: proposed
- **[2026-07-26] fmt-the-world + CI fmt gate** — type: convention-gap, risk: 1, effort: s, payoff: 2, reach: whole repo
  The tree is not `cargo fmt --check` clean, so CI shipped without the gate. One dedicated
  fmt commit (when no other session is mid-flight), then add the gate to .github/workflows/ci.yml.
  Status: proposed
- **[2026-07-26] App-coverage tail: 11 zero-test app crates** — type: weak-pattern, risk: 1, effort: m, payoff: 3, reach: 11 crates
  ca-grants, census-nonemp (near-clone of tested census-density), connector-api-watch (331 lines),
  state-tax, trade-wages, valuation-multiples, research, watch, readable, mpsv-ispv. Pure parse
  fns sit untested (`ca_grants::record_key`, `watch::first_heading`). Follow extract-then-test;
  `TestContext` makes run-level tests ~3 lines of setup.
  Status: proposed

## Shipped
- **[2026-07-26] CI + honest env-gated tests** — [[Architect/decisions/2026-07-26-ci-and-honest-test-gates]] (commits 60b8650, c9bf2fd)
- **[2026-07-26] Tier-1 prose-only bug-class guards** — [[Architect/decisions/2026-07-26-prose-only-bug-class-guards]] (commit 8885c96; keyset tests rode a217596)
- **[2026-07-26] Shared test harness (pumper_core::testing)** — [[Architect/decisions/2026-07-26-shared-test-harness]] (commits ed8674f, be7371b)
- **[2026-07-26] Server test seam + e2e suite** — [[Architect/decisions/2026-07-26-server-test-seam]] (commits 38f62f8, f7c7287)
- **[2026-07-26] Codify 4 testing strong patterns** — [[Architect/decisions/2026-07-26-codify-testing-patterns]] (commit 8c83f14)

## Abandoned / Blocked
_None yet._
