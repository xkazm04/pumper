---
date: 2026-07-26
slug: ci-and-honest-test-gates
status: shipped
type: convention-gap
reach: "whole repo; 3 environment-dependent test files"
risk: 1
effort: s
payoff: 4
branch: "(committed to master)"
commits: [60b8650, c9bf2fd]
related_scan: "[[Architect/scans/2026-07-26-testing-strategy]]"
---

# CI + honest environment-gated tests

## Context
No `.github/` exists — "leave the repo green" is enforced only by discipline; the 336-test
net is advisory. Three tests block a clean headless run: `engine-browser/tests/render.rs`
(live Chrome + network, unmarked), `engine-wasm/tests/plugins.rs` (silently early-returns
green when `data/plugins/title.wasm` absent), `core/src/governor.rs:259` (wall-clock timing
assert — first flake on a loaded runner).

## Decision
Mark environment-dependent tests `#[ignore]` with reason strings; make the wasm skip loud
(ignore too — an early-return green is worse than a skip); keep the governor test but move
it to `#[ignore]` (timing-sensitive). Add a minimal GitHub Actions workflow: fmt check,
clippy, `cargo test --workspace` (browser tests now self-exclude via ignore).

## Consequences
### Positive
- Red tests can no longer land silently; env-gated tests stop lying in both directions.
### Negative / risks
- Ignored tests need a manual `cargo test -- --ignored` lane to still be run locally.
### Mitigations
- Document the `--ignored` lane in ONBOARDING/harness-learnings.

## Rollout
1. `#[ignore]` browser/wasm/governor env-dependent tests — validation: cargo test --workspace headless-green
2. `.github/workflows/ci.yml` (fmt, clippy, test) — validation: YAML lints, local command parity

## Acceptance criteria
- `cargo test --workspace` passes with no Chrome/network/plugin artifacts required.
- CI workflow runs the same commands a developer would.

## Regression checklist
- [ ] Ignored tests still pass under `cargo test -- --ignored` on this machine (Chrome present).
