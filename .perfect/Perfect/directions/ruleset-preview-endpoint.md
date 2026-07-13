---
slug: ruleset-preview-endpoint
type: perfect/direction
context: "[[Declarative Extraction Engine]]"
lens: api-ux
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 387a509
---

## What & why
No way to test a RuleSet without running a full job — typo'd selectors discovered after fetching everything. RuleSet::compile() is pure; expose `POST /extract/preview` {rules, html|url} → compile diagnostics + extracted values + per-field match report. Interactive rule authoring.

## Evidence
- Compile-only-at-job-run: crates/apps/extractor/src/lib.rs:44-45; pure compile fn extract.rs:101
- No preview route: routes.rs router table

## Acceptance criteria
- [ ] POST /extract/preview: body {rules, html} or {rules, url} (url fetched via the metered path with a small budget/no-claude strategy); 400 with compile diagnostics on bad rules (per-field error messages).
- [ ] Response: extracted values + per-field match report (depends on [[extraction-quality-signal]] — same wave, brief them together).
- [ ] OpenAPI annotation + EXPECTED inventory; error `code` conventions.
- [ ] docs/features/extraction.md + http-api.md updated.

## Risks / non-goals
- Non-goal: a stored/named RuleSet registry (future direction).

## Build record
(pending)
