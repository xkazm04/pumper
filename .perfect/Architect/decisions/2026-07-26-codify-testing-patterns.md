---
date: 2026-07-26
slug: codify-testing-patterns
status: shipped
type: codification
vehicle: docs-harness + docs-claude
parent_strong_pattern: "[[Architect/strong-patterns]] — inventory-as-test, x-not-y naming, deterministic time, extract-then-test"
related_scan: "[[Architect/scans/2026-07-26-testing-strategy]]"
commits: [8c83f14]
---

# Codify: the four testing-strategy strong patterns

## Why now
Identified this run; all four are load-bearing and the scan showed the cost of them
staying tribal (extract-then-test explains exactly which of 15 bug classes are guarded).
One combined mini-ADR for the four — they shipped as two doc edits in one commit.

## Vehicle and rationale
- Patterns 5–7 (inventory-as-test, x-not-y naming + //! docs, deterministic time)
  → `docs/harness/harness-learnings.md` "Testing conventions" section — loaded before
  large changes, the natural home for test-shape conventions.
- Pattern 8 (extract-then-test) → `.claude/CLAUDE.md` — a rule for every session's bug
  fixes, so it belongs in the always-loaded file; cross-referenced from harness-learnings.

## Rollback
Delete the two doc sections; the patterns remain noted in Architect/strong-patterns.md.
