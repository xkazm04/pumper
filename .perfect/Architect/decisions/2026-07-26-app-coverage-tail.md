---
date: 2026-07-26
slug: app-coverage-tail
status: shipped
type: weak-pattern
reach: "10 zero-test app crates"
risk: 1
effort: m
payoff: 3
branch: "(committed to master)"
commits: [31799a0, 9ee77c4]
related_scan: "[[Architect/scans/2026-07-26-testing-strategy]]"
---

# App-coverage tail: tests for the zero-test app crates

## Context
10 of 24 app crates have zero tests, uncorrelated with size or parse risk: ca-grants,
census-nonemp (near-clone of the 5-test census-density), connector-api-watch (331 lines),
state-tax, trade-wages, valuation-multiples, research, watch, readable, mpsv-ispv.
Pure helpers sit untested (`ca_grants::record_key` fallback ladder, `watch::first_heading`,
`watch::hex_sha256`); several silent-success predicates are inline in `run()` bodies.

## Decision
Per crate: test the pure helpers against realistic inline samples (repo convention — no
fixture files); where a guard predicate is inline in `run()` and trivially extractable,
extract it as a named private fn wired unchanged, then test it (the extract-then-test
rule). X-not-Y test naming. Behavior changes are OUT of scope — tests + behavior-
preserving extraction only.

## Consequences
### Positive
- The zero-test list empties; parse/normalize drift in the app fleet becomes visible.
### Negative / risks
- Test-only additions; extraction refactors carry a small wiring risk — mitigated by
  compiling + testing per crate and by the full-workspace sweep.

## Rollout
1. Batch A (economic/grants): ca-grants, census-nonemp, state-tax, trade-wages,
   valuation-multiples — validation: cargo test per crate
2. Batch B (content/watch): connector-api-watch, research, watch, readable, mpsv-ispv —
   validation: cargo test per crate
3. Full sweep: fmt --check, clippy at 36, workspace tests ≥349/0/7 — one commit per batch

## Acceptance criteria
- Every listed crate has ≥1 meaningful test; no crate's test is a tautology.
- Baselines hold (fmt clean, clippy 36, no test failures).

## Regression checklist
- [x] Full workspace suite (381/0/7; clippy 36; fmt clean) at baseline rate.

## Pre-flight baseline
Tree clean; clippy 36 warnings; tests 349 passed / 0 failed / 7 ignored (this session).
