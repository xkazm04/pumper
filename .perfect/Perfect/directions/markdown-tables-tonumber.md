---
slug: markdown-tables-tonumber
type: perfect/direction
context: "[[Declarative Extraction Engine]]"
lens: optimization
status: shipped
size: S
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: ebe5f89
---

## What & why
html_to_markdown flattens <table> entirely (td/th fall through generic child-walk) — tabular data (fee schedules, wage tables) turns to soup for clean-text datasets and Claude token budgets. And `to_number` strips every non-[0-9.-] char, silently corrupting ranges ("1-2" → -12). Emit pipe tables; parse the first valid number instead.

## Evidence
- No table handling: crates/core/src/markdown.rs:58 (tr as plain block), :41-126
- Lossy to_number: crates/core/src/extract.rs:238-243

## Acceptance criteria
- [ ] <table>/<tr>/<th>/<td> → GitHub pipe tables (header row from th when present; nested-block cells degrade gracefully — state the rule).
- [ ] to_number extracts the first valid decimal number (currency symbols/thousands separators still tolerated; "1-2" → 1, "$1,234.50" → 1234.5); behavior documented; existing transform tests updated + new cases.
- [ ] Existing markdown tests green; new table fixtures (simple, th-less, colspan-degraded).
- [ ] docs/features/extraction.md updated.

## Risks / non-goals
- to_number semantic change: audit existing apps' transform usage for reliance on old concatenation behavior (grep RuleSets in catalog/params) — report findings.

## Build record
(pending)
