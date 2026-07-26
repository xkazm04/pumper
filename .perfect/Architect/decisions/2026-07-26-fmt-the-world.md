---
date: 2026-07-26
slug: fmt-the-world
status: shipped
type: convention-gap
reach: "95 files / 3845 insertions"
risk: 1
effort: s
payoff: 2
branch: "(committed to master)"
commits: [a1afbf5, 6c5b4d3]
related_scan: "[[Architect/scans/2026-07-26-testing-strategy]]"
---

# fmt-the-world + CI fmt gate

## Context
The tree is not `cargo fmt --check` clean, so CI (shipped this run) had to omit the fmt
gate. Formatting drift accumulates per-session; a gate only works from a clean baseline,
and the baseline commit must land when no concurrent session is mid-flight — which is now
(tree fully clean after the architect run).

## Decision
One mechanical `cargo fmt` commit across the workspace, verified behavior-neutral
(check + clippy + full test suite at baseline), then enable `cargo fmt --check` in
`.github/workflows/ci.yml`.

## Consequences
### Positive
- Formatting stops being reviewable noise; the gate keeps it that way permanently.
### Negative / risks
- One churn commit touches many files (blame noise); any unpushed concurrent work will
  rebase across it.
### Mitigations
- Execute only on a verified-clean tree; formatting-only commit isolated from all logic.

## Rollout
1. `cargo fmt` workspace-wide — validation: `cargo fmt --check` clean, `cargo check`,
   `cargo clippy` at 36-warning baseline, `cargo test --workspace` at 349/0/7
2. Enable the fmt gate in ci.yml — validation: command parity with local

## Acceptance criteria
- `cargo fmt --check` exits 0.
- Test/clippy baselines unchanged (formatting is behavior-neutral).

## Regression checklist
- [x] Full workspace suite at baseline rate after fmt.
